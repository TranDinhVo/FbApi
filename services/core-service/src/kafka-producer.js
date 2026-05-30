const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'core-service-producer',
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
  console.log('[Core Service Producer] Kafka producer connected successfully.');
};

const publishReplyCommand = async (command) => {
  await producer.send({
    topic: 'reply_commands',
    messages: [
      {
        key: command.command_id,
        value: JSON.stringify(command),
      },
    ],
  });
  console.log(`[Core Service Producer] Published reply_command [${command.command_id}] -> topic "reply_commands"`);
};

const disconnectProducer = async () => {
  await producer.disconnect();
};

module.exports = { connectProducer, publishReplyCommand, disconnectProducer };
