const facebookApi = require('../facebook-api');
const { publishSendFailed } = require('../kafka-producer');


const handleCommand = async (command) => {
  const { command_id, event_id, action, target, reply_text } = command;
  // retry_count có trong send_retry messages; mặc định 0 nếu lần đầu
  const retryCount = command.retry_count ?? 0;

  console.log(`\n[Command Handler] Xử lý command [${command_id}]`);
  console.log(`  action      : ${action}`);
  console.log(`  comment_id  : ${target?.comment_id || 'N/A'}`);
  console.log(`  reply_text  : ${reply_text || 'N/A'}`);
  console.log(`  retry_count : ${retryCount}`);

  try {
    switch (action) {
      case 'reply': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để reply');
        await facebookApi.replyComment(target.comment_id, reply_text);
        console.log(`[Command Handler] SUCCESS - reply comment ${target.comment_id}`);
        break;
      }

      case 'hide': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để hide');
        await facebookApi.hideComment(target.comment_id);
        console.log(`[Command Handler] SUCCESS - hide comment ${target.comment_id}`);
        break;
      }

      case 'delete': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để delete');
        await facebookApi.deleteComment(target.comment_id);
        console.log(`[Command Handler]  SUCCESS - delete comment ${target.comment_id}`);
        break;
      }

      case 'create_post': {
        const pageId = target?.page_id || 'me';
        await facebookApi.createPost(pageId, reply_text);
        console.log(`[Command Handler] SUCCESS - create post`);
        break;
      }

      default:
        console.warn(`[Command Handler]  Unknown action: "${action}" - bỏ qua`);
        return;
    }
  } catch (error) {
    console.error(`[Command Handler] FAILED - ${error.message}`);

    // Publish tới send_failed với retry_count + 1
    await publishSendFailed(command, retryCount + 1, error.message);
  }
};

module.exports = { handleCommand };
