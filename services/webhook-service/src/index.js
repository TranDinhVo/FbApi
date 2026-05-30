require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { verifySignature } = require('./signature-verifier');
const { normalizeAndPublish } = require('./webhook-handler');
const { connectProducer } = require('./kafka-producer');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(bodyParser.json({ verify: verifySignature }));

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode && token) {
    if (mode === 'subscribe' && token === process.env.FB_VERIFY_TOKEN) {
      console.log('WEBHOOK_VERIFIED');
      res.status(200).send(challenge);
    } else {
      res.sendStatus(403);
    }
  } else {
    res.sendStatus(400);
  }
});

app.post('/webhook', async (req, res) => {
  const body = req.body;

  try {
    await normalizeAndPublish(body);

    res.status(200).send('EVENT_RECEIVED');
  } catch (error) {
    console.error('Error processing webhook event:', error);
    res.status(500).send('INTERNAL_SERVER_ERROR');
  }
});

app.listen(PORT, async () => {
  console.log(`Webhook Service is listening on port ${PORT}`);
  await connectProducer();
});
