const makeDecision = async (event, spamResult, aiResult) => {
  console.log(`[Decision Engine] Đang xử lý [${event.type.toUpperCase()}] từ ${event.senderName}`);

  if (spamResult.isSpam) {
    console.log(`=> HÀNH ĐỘNG: [ẨN BÌNH LUẬN] - Lý do: Spam / Chứa liên kết.`);
    return 'hidden';
  }

  if (aiResult.error === 'AI_NOT_CONFIGURED') {
    console.log(`=> HÀNH ĐỘNG: [BỎ QUA] - Lý do: Chưa cấu hình API Key của AI.`);
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
    console.log(`=> HÀNH ĐỘNG: [CẢM ƠN] - Lý do: Cảm xúc tích cực.`);
    return 'reply_positive';
  }

  if (sentiment === 'tiêu cực') {
    console.log(`=> HÀNH ĐỘNG: [XIN LỖI] - Lý do: Cảm xúc tiêu cực.`);
    return 'reply_negative';
  }

  if (intent === 'hỏi giá') {
    console.log(`=> HÀNH ĐỘNG: [AUTO-REPLY BÁO GIÁ] - Lý do: Khách hỏi giá.`);
    return 'auto_reply';
  }

  console.log(`=> HÀNH ĐỘNG: [BỎ QUA/KHÔNG LÀM GÌ] - Ý định: ${intent}`);
  return 'no_action';
};

module.exports = { makeDecision };
