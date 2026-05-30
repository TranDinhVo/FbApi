const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'backend-api-producer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1,
  retry: {
    retries: 5
  }
});

const connectProducer = async () => {
  await producer.connect();
  console.log('[Backend API Producer] Đã kết nối Kafka producer.');
};


const publishSendFailed = async (command, retryCount, errorMessage) => {
  const message = {
    schema_version: 1,
    command_id: command.command_id,
    event_id: command.event_id,
    retry_count: retryCount,
    last_error: errorMessage,
    payload: {
      action: command.action,
      target: command.target,
      reply_text: command.reply_text,
      intent: command.intent,
      sentiment: command.sentiment,
    },
    failed_at: new Date().toISOString(),
  };

  await producer.send({
    topic: 'send_failed',
    messages: [{ key: command.command_id, value: JSON.stringify(message) }],
  });

  console.log(`[Backend API Producer] Đã publish send_failed [${command.command_id}] -> topic "send_failed"`);
};

const disconnectProducer = async () => {
  await producer.disconnect();
};

module.exports = { connectProducer, publishSendFailed, disconnectProducer };
