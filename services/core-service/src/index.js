require('dotenv').config();
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const { detectSpam } = require('./spam-filter');
const { classifyEvent } = require('./ai-classifier');
const { makeDecision } = require('./decision-engine');
const { connectProducer, publishReplyCommand, disconnectProducer } = require('./kafka-producer');

const kafka = new Kafka({
  clientId: 'core-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'core-service-group' });

const DECISION_TO_ACTION = {
  reply_positive: 'reply',
  reply_negative: 'reply',
  auto_reply: 'reply',
  hidden: 'hide',
  delete: 'delete',
  hidden_and_queued: 'hide',
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
  console.log('[Core Service] Khởi động thành công!');
  console.log('[Core Service] Đang lắng nghe topic "raw_events"...');
  console.log('[Core Service] Sau xử lý sẽ publish tới "reply_commands"');
  console.log('====================================================');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = JSON.parse(message.value.toString());

        console.log(`\n>> Nhận event mới: [${event.type}] - Nội dung: "${event.content}"`);

        const spamResult = detectSpam(event.content);

        let aiResult = { intent: 'unknown', sentiment: 'neutral' };
        if (!spamResult.isSpam) {
          aiResult = await classifyEvent(event.content);
        }

        const decision = await makeDecision(event, spamResult, aiResult);

        console.log(`[Core Service] Decision: ${decision} | Intent: ${aiResult.intent} | Sentiment: ${aiResult.sentiment}`);

        const action = DECISION_TO_ACTION[decision];

        if (!action || decision === 'no_action' || decision === 'notify_staff') {
          console.log(`[Core Service] Không có hành động Kafka cho decision: ${decision}. Bỏ qua.`);
          return;
        }

        let replyText = null;
        if (action === 'reply') {
          const texts = REPLY_TEXTS[decision];
          replyText = texts ? pickRandom(texts) : 'Cảm ơn bạn đã liên hệ!';
        }

        // Build command message
        const command = {
          schema_version: 1,
          command_id: uuidv4(),
          event_id: event.eventId || event.commentId || event.senderId || 'unknown',
          action,
          target: {
            comment_id: event.commentId || null,
            sender_id: event.senderId || null,
            type: event.type || 'comment',
          },
          reply_text: replyText,
          intent: aiResult.intent,
          sentiment: aiResult.sentiment,
          spam_result: spamResult,
          created_at: new Date().toISOString(),
        };

        // Publish tới topic reply_commands
        await publishReplyCommand(command);

        console.log(`[Core Service] Đã publish command [${command.action}] -> reply_commands`);

      } catch (error) {
        console.error('[Core Service] Lỗi xử lý event:', error.message);
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
