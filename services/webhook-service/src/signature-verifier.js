const crypto = require('crypto');

const verifySignature = (req, res, buf, encoding) => {
  const signature = req.headers['x-hub-signature-256'];
  if (!signature) {
    throw new Error("Missing x-hub-signature-256 header");
  }

  const elements = signature.split('=');
  const signatureHash = elements[1];
  const expectedHash = crypto
    .createHmac('sha256', process.env.FB_APP_SECRET)
    .update(buf)
    .digest('hex');

  if (signatureHash !== expectedHash) {
    throw new Error("Invalid HMAC-SHA256 signature");
  }
};

module.exports = { verifySignature };
