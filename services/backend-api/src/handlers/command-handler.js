const circuitBreaker = require('../circuit-breaker');
const { publishSendFailed } = require('../kafka-producer');
const { checkIdempotencyKey, saveIdempotencyKey, saveComment } = require('../db');


const handleCommand = async (command) => {
  const { command_id, event_id, action, target, reply_text, intent, sentiment } = command;
  const retryCount = command.retry_count ?? 0;

  console.log(`\n[Command Handler] Processing command [${command_id}]`);
  console.log(`  action      : ${action}`);
  console.log(`  target      : ${target?.comment_id || target?.sender_id || 'N/A'}`);
  console.log(`  reply_text  : ${reply_text || 'N/A'}`);
  console.log(`  retry_count : ${retryCount}`);

  // === IDEMPOTENCY CHECK ===
  try {
    const alreadyProcessed = await checkIdempotencyKey(command_id);
    if (alreadyProcessed) {
      console.log(`[Command Handler] SKIP - command_id [${command_id}] already processed (idempotent).`);
      return;
    }
  } catch (dbError) {
    console.error(`[Command Handler] Idempotency check error:`, dbError.message);
    // Continue processing if DB fails, do not block pipeline
  }

  try {
    switch (action) {
      case 'reply': {
        if (!target?.comment_id) throw new Error('Missing comment_id for reply');
        await circuitBreaker.replyComment(target.comment_id, reply_text);
        console.log(`[Command Handler] SUCCESS - reply comment ${target.comment_id}`);
        break;
      }

      case 'send_message': {
        if (!target?.sender_id) throw new Error('Missing sender_id for message');
        await circuitBreaker.sendMessage(target.sender_id, reply_text);
        console.log(`[Command Handler] SUCCESS - send message to ${target.sender_id}`);
        break;
      }

      case 'hide': {
        if (!target?.comment_id) throw new Error('Missing comment_id for hide');
        await circuitBreaker.hideComment(target.comment_id);
        console.log(`[Command Handler] SUCCESS - hide comment ${target.comment_id}`);
        break;
      }

      case 'delete': {
        if (!target?.comment_id) throw new Error('Missing comment_id for delete');
        await circuitBreaker.deleteComment(target.comment_id);
        console.log(`[Command Handler] SUCCESS - delete comment ${target.comment_id}`);
        break;
      }

      case 'create_post': {
        const pageId = target?.page_id || 'me';
        await circuitBreaker.createPost(pageId, reply_text);
        console.log(`[Command Handler] SUCCESS - create post`);
        break;
      }

      default:
        console.warn(`[Command Handler] Unknown action: "${action}" - skipping`);
        return;
    }

    // === SAVE IDEMPOTENCY KEY AFTER SUCCESS ===
    try {
      await saveIdempotencyKey(command_id, 'success');
      console.log(`[Command Handler] Saved idempotency key [${command_id}]`);
    } catch (dbError) {
      console.error(`[Command Handler] Error saving idempotency key:`, dbError.message);
    }

    // Save comment data to DB
    if (target?.comment_id) {
      await saveComment({
        comment_id: target.comment_id,
        post_id: target.post_id || null,
        message: reply_text,
        intent: intent || null,
        sentiment: sentiment || null,
        status: action === 'reply' ? 'replied' : action === 'hide' ? 'hidden' : action,
      });
    }

  } catch (error) {
    console.error(`[Command Handler] FAILED - ${error.message}`);

    // Save idempotency key with status failed (to know it was attempted but errored)
    // DO NOT save - allow retry to try again

    // Publish to send_failed
    await publishSendFailed(command, retryCount + 1, error.message);
  }
};

module.exports = { handleCommand };
