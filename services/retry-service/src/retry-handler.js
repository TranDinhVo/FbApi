const { publishSendRetry, publishDeadLetter } = require('./kafka-producer');

const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);


const calcBackoffDelay = (retryCount) => {
  return 1000 * Math.pow(2, retryCount);
};


const handleRetry = async (failedMessage) => {
  const { command_id, event_id, retry_count, last_error, payload } = failedMessage;

  console.log(`\n[Retry Handler] Received failed message [${command_id}]`);
  console.log(`  retry_count : ${retry_count}`);
  console.log(`  last_error  : ${last_error}`);
  console.log(`  MAX_RETRIES : ${MAX_RETRIES}`);

  if (retry_count >= MAX_RETRIES) {
    console.log(`[Retry Handler] retry_count (${retry_count}) >= MAX_RETRIES (${MAX_RETRIES}) -> Dead Letter Queue`);
    await publishDeadLetter(failedMessage);
    return;
  }

  const delay = calcBackoffDelay(retry_count);
  console.log(`[Retry Handler] Will retry after ${delay}ms (retry #${retry_count})...`);

  await new Promise((resolve) => setTimeout(resolve, delay));

  const retryMessage = {
    schema_version: 1,
    command_id,
    event_id,
    action: payload?.action,
    target: payload?.target,
    reply_text: payload?.reply_text,
    intent: payload?.intent,
    sentiment: payload?.sentiment,
    retry_count,
    retried_at: new Date().toISOString(),
  };

  await publishSendRetry(retryMessage);
  console.log(`[Retry Handler] Resent -> send_retry [retry #${retry_count}]`);
};

module.exports = { handleRetry };
