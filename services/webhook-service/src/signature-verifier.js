const crypto = require('crypto');

const verifySignature = (req, res, buf, encoding) => {
  const signature = req.headers['x-hub-signature-256'];
  if (!signature) {
    console.warn("Couldn't find 'x-hub-signature-256' in headers.");
    return;
  }

  const elements = signature.split('=');
  const signatureHash = elements[1];
  const expectedHash = crypto
    .createHmac('sha256', process.env.FB_APP_SECRET)
    .update(buf)
    .digest('hex');

  if (signatureHash !== expectedHash) {
    console.warn('[Webhook] Cảnh báo: Chữ ký không hợp lệ! (Có thể do bạn điền Token vào chỗ của App Secret). Tạm thời bỏ qua để test ngrok.');
  }
};

module.exports = { verifySignature };
