require('dotenv').config();
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const { detectSpam } = require('./spam-filter');
const { classifyEvent } = require('./ai-classifier');
const { makeDecision } = require('./decision-engine');
const { connectProducer, publishReplyCommand, disconnectProducer } = require('./kafka-producer');
const { checkRateLimit } = require('./rate-limiter');

const kafka = new Kafka({
  clientId: 'core-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'core-service-group' });

// Dedup: store processed event_ids to avoid duplicates
const processedEventIds = new Set();
const MAX_DEDUP_SIZE = 10000;

const DECISION_TO_ACTION = {
  reply_positive: 'reply',
  reply_negative: 'reply',
  auto_reply: 'reply',
  hidden: 'hide',
  delete: 'delete',
  hidden_and_queued: 'hide',
};

// For message type, use send_message instead of reply
const DECISION_TO_MESSAGE_ACTION = {
  reply_positive: 'send_message',
  reply_negative: 'send_message',
  auto_reply: 'send_message',
};

const REPLY_TEXTS = {
  reply_positive: [
    'Cảm ơn bạn đã ủng hộ shop!',
    'Dạ shop cảm ơn bạn rất nhiều ạ!',
    'Cảm ơn bạn đã tin tưởng và sử dụng sản phẩm bên mình nhé!',
    'Shop rất vui khi nhận được phản hồi tuyệt vời từ bạn!',
  ],
  reply_negative: [
    'Rất xin lỗi vì trải nghiệm chưa tốt, bên mình sẽ kiểm tra ngay.',
    'Dạ shop thành thật xin lỗi vì sự bất tiện này. Bạn inbox để shop hỗ trợ ngay nhé.',
    'Thành thật xin lỗi bạn! Đội ngũ hỗ trợ sẽ liên hệ xử lý ngay lập tức ạ.',
    'Xin lỗi bạn vì sự cố này. Mong bạn thông cảm, shop sẽ kiểm tra và đền bù cho bạn.',
  ],
  auto_reply: [
    'Cảm ơn bạn đã quan tâm! Sản phẩm này đang có giá ưu đãi. Nhân viên sẽ IB tư vấn thêm cho bạn nhé!',
  ],
};

const pickRandom = (arr) => arr[Math.floor(Math.random() * arr.length)];

const run = async () => {
  await connectProducer();
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_events', fromBeginning: false });

  console.log('====================================================');
  console.log('[Core Service] Started successfully!');
  console.log('[Core Service] Listening on topic "raw_events"...');
  console.log('[Core Service] After processing, publishes to "reply_commands"');
  console.log('[Core Service] Rate Limiting: Max 20 events / sender / minute');
  console.log('====================================================');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = JSON.parse(message.value.toString());
        const eventId = event.commentId || event.messageId || event.postId || 'unknown';

        // === EVENT DEDUP ===
        if (processedEventIds.has(eventId)) {
          console.log(`[Core Service] SKIP - event ${eventId} already processed (dedup).`);
          return;
        }
        processedEventIds.add(eventId);
        if (processedEventIds.size > MAX_DEDUP_SIZE) {
          const first = processedEventIds.values().next().value;
          processedEventIds.delete(first);
        }

        console.log(`\n>> New event received: [${event.type}] - Content: "${event.content}"`);

        // === RATE LIMITING ===
        const rateResult = checkRateLimit(event.senderId);
        if (rateResult.limited) {
          console.log(`[Core Service] RATE LIMITED - sender ${event.senderId} (${rateResult.count} events/min) -> pending_review`);
          // Log but skip AI processing and auto reply
          return;
        }

        // Skip 'post' event type - no analysis needed
        if (event.type === 'post') {
          console.log('[Core Service] Event type "post" - skipping analysis.');
          return;
        }

        const spamResult = detectSpam(event.content);

        let aiResult = { intent: 'unknown', sentiment: 'neutral' };
        if (!spamResult.isSpam) {
          aiResult = await classifyEvent(event.content);
        }

        const decision = await makeDecision(event, spamResult, aiResult);

        console.log(`[Core Service] Decision: ${decision} | Intent: ${aiResult.intent} | Sentiment: ${aiResult.sentiment}`);

        // Determine action based on event type (comment -> reply, message -> send_message)
        let action;
        if (event.type === 'message' && DECISION_TO_MESSAGE_ACTION[decision]) {
          action = DECISION_TO_MESSAGE_ACTION[decision];
        } else {
          action = DECISION_TO_ACTION[decision];
        }

        if (!action || decision === 'no_action' || decision === 'notify_staff') {
          console.log(`[Core Service] No Kafka action for decision: ${decision}. Skipping.`);
          return;
        }

        let replyText = null;
        if (action === 'reply' || action === 'send_message') {
          const texts = REPLY_TEXTS[decision];
          replyText = texts ? pickRandom(texts) : 'Cảm ơn bạn đã liên hệ!';
        }

        // Build command message
        const command = {
          schema_version: 1,
          command_id: uuidv4(),
          event_id: eventId,
          action,
          target: {
            comment_id: event.commentId || null,
            sender_id: event.senderId || null,
            post_id: event.postId || null,
            type: event.type || 'comment',
          },
          reply_text: replyText,
          intent: aiResult.intent,
          sentiment: aiResult.sentiment,
          spam_result: spamResult,
          created_at: new Date().toISOString(),
        };

        // Publish to topic reply_commands
        await publishReplyCommand(command);

        console.log(`[Core Service] Published command [${command.action}] -> reply_commands`);

      } catch (error) {
        console.error('[Core Service] Error processing event:', error.message);
      }
    },
  });
};

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      await disconnectProducer();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
      await disconnectProducer();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

run().catch(console.error);
