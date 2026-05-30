const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'retry-service-producer',
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
  console.log('[Retry Service Producer] Connected to Kafka producer.');
};


const publishSendRetry = async (message) => {
  await producer.send({
    topic: 'send_retry',
    messages: [{ key: message.command_id, value: JSON.stringify(message) }],
  });
  console.log(`[Retry Service Producer] Published [${message.command_id}] -> topic "send_retry" (retry #${message.retry_count})`);
};


const publishDeadLetter = async (message) => {
  const deadLetter = {
    schema_version: 1,
    command_id: message.command_id,
    event_id: message.event_id,
    retry_count: message.retry_count,
    final_error: message.last_error || 'Facebook timeout after maximum retries',
    payload: message.payload,
    dead_at: new Date().toISOString(),
  };

  await producer.send({
    topic: 'dead_letter',
    messages: [{ key: deadLetter.command_id, value: JSON.stringify(deadLetter) }],
  });
  console.log(`[Retry Service Producer] Published [${deadLetter.command_id}] -> topic "dead_letter" (permanently failed)`);
};

const disconnectProducer = async () => {
  await producer.disconnect();
};

module.exports = { connectProducer, publishSendRetry, publishDeadLetter, disconnectProducer };
