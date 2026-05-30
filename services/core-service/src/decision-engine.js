const makeDecision = async (event, spamResult, aiResult) => {
  console.log(`[Decision Engine] Processing [${event.type.toUpperCase()}] from ${event.senderName || event.senderId || 'Unknown'}`);

  if (spamResult.isSpam) {
    console.log(`=> ACTION: [HIDE COMMENT] - Reason: Spam / Contains link.`);
    return 'hidden';
  }

  if (aiResult.error === 'AI_NOT_CONFIGURED') {
    console.log(`=> ACTION: [SKIP] - Reason: AI API Key not configured.`);
    return 'no_action';
  }

  let intent = aiResult.intent;
  let sentiment = aiResult.sentiment;

  if (intent === 'unknown' && sentiment === 'neutral') {
    const textLower = event.content ? event.content.toLowerCase() : '';
    if (textLower.includes('giá') || textLower.includes('nhiêu') || textLower.includes('ib') || textLower.includes('chuẩn')) {
      intent = 'hỏi giá';
    } else if (textLower.includes('tốt') || textLower.includes('tuyệt') || textLower.includes('đẹp') || textLower.includes('cảm ơn') || textLower.includes('uy tín') || textLower.includes('chất lượng')) {
      sentiment = 'tích cực';
    } else if (textLower.includes('tệ') || textLower.includes('xấu') || textLower.includes('lỗi') || textLower.includes('thất vọng') || textLower.includes('bể') || textLower.includes('vỡ') || textLower.includes('hư') || textLower.includes('hỏng') || textLower.includes('rách')) {
      sentiment = 'tiêu cực';
    }
  }

  if (sentiment === 'tích cực') {
    console.log(`=> ACTION: [THANK] - Reason: Positive sentiment.`);
    return 'reply_positive';
  }

  if (sentiment === 'tiêu cực') {
    console.log(`=> ACTION: [APOLOGIZE] - Reason: Negative sentiment.`);
    return 'reply_negative';
  }

  if (intent === 'hỏi giá') {
    console.log(`=> ACTION: [AUTO-REPLY PRICE QUOTE] - Reason: Customer asking for price.`);
    return 'auto_reply';
  }

  console.log(`=> ACTION: [SKIP/NO ACTION] - Intent: ${intent}`);
  return 'no_action';
};

module.exports = { makeDecision };
