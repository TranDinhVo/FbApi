const detectSpam = (content) => {
  if (!content) return { isSpam: false, type: null };

  const hasLink = /(https?:\/\/[^\s]+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g.test(content);
  const suspiciousKeywords = ['trúng thưởng', 'đăng nhập để nhận', 'bấm vào link', 'kết bạn zalo'];
  const hasSuspiciousWords = suspiciousKeywords.some(keyword => content.toLowerCase().includes(keyword));

  if (hasLink && hasSuspiciousWords) {
    return { isSpam: true, type: 'malicious_link' };
  }

  if (hasLink) {
    return { isSpam: true, type: 'light_spam_link' };
  }

  const hasRepetitions = /(.{3,})\1{3,}/i.test(content);
  if (hasRepetitions) {
    return { isSpam: true, type: 'repeated_content' };
  }

  return { isSpam: false, type: null };
};

module.exports = { detectSpam };
