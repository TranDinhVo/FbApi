const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'webhook-service',
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
  try {
    await producer.connect();
    console.log('[Kafka] Producer connected successfully');
  } catch (error) {
    console.error('[Kafka] Error connecting producer:', error);
    process.exit(1);
  }
};

const publishEvent = async (topic, eventData) => {
  try {
    await producer.send({
      topic,
      messages: [
        { value: JSON.stringify(eventData) },
      ],
    });
    console.log(`[Kafka] Event published to ${topic}`);
  } catch (error) {
    console.error(`[Kafka] Failed to publish event to ${topic}:`, error);
  }
};

module.exports = {
  connectProducer,
  publishEvent,
};
