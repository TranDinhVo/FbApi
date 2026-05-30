const { publishEvent } = require('./kafka-producer');

const normalizeAndPublish = async (body) => {
  if (body.object === 'page') {
    for (const entry of body.entry) {
      const pageId = entry.id;
      const timeOfEvent = entry.time;

      if (entry.messaging) {
        for (const webhookEvent of entry.messaging) {
          const senderId = webhookEvent.sender.id;

          if (webhookEvent.message && !webhookEvent.message.is_echo && webhookEvent.message.text) {
            const normalizedEvent = {
              source: 'facebook',
              type: 'message',
              pageId,
              senderId,
              timestamp: timeOfEvent,
              content: webhookEvent.message.text,
              messageId: webhookEvent.message.mid,
              raw: webhookEvent,
              status: 'received'
            };

            await publishEvent('raw_events', normalizedEvent);
          }
        }
      }

      if (entry.changes) {
        for (const change of entry.changes) {
          if (change.field === 'feed') {
            if (change.value.item === 'comment' && change.value.verb === 'add') {
              if (change.value.from && String(change.value.from.id) === String(pageId)) continue;

              const normalizedEvent = {
                source: 'facebook',
                type: 'comment',
                pageId,
                senderId: change.value.from.id,
                senderName: change.value.from.name,
                timestamp: change.value.created_time,
                content: change.value.message,
                commentId: change.value.comment_id,
                postId: change.value.post_id,
                raw: change.value,
                status: 'received'
              };
              console.log(`[Webhook] New comment received from ${change.value.from.name}: "${change.value.message}"`);
              await publishEvent('raw_events', normalizedEvent);
            }
            else if (['status', 'post', 'photo', 'video', 'share'].includes(change.value.item) && change.value.verb === 'add') {
              const normalizedEvent = {
                source: 'facebook',
                type: 'post',
                pageId,
                senderId: change.value.from ? change.value.from.id : 'N/A',
                senderName: change.value.from ? change.value.from.name : 'Page',
                timestamp: change.value.created_time,
                content: change.value.message || "[No Text/Media only]",
                postId: change.value.post_id,
                raw: change.value,
                status: 'received'
              };
              console.log(`[Webhook] New post received: "${normalizedEvent.content}"`);
              await publishEvent('raw_events', normalizedEvent);
            }
          }
        }
      }
    }
  }
};

module.exports = { normalizeAndPublish };
