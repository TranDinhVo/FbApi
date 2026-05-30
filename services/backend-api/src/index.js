require('dotenv').config();
const express = require('express');
const { connectProducer, disconnectProducer } = require('./kafka-producer');
const { connectConsumer, disconnectConsumer } = require('./kafka-consumer');
const { initDatabase } = require('./db');
const facebookApi = require('./facebook-api');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// =====================================================
// REST API for admin dashboard
// =====================================================

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'backend-api', timestamp: new Date().toISOString() });
});

// GET /posts - List page posts
app.get('/posts', async (req, res) => {
  try {
    const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
    const FB_API_VERSION = process.env.FB_API_VERSION || 'v19.0';
    const axios = require('axios');

    const response = await axios.get(
      `https://graph.facebook.com/${FB_API_VERSION}/me/feed`,
      { params: { access_token: PAGE_ACCESS_TOKEN, fields: 'id,message,created_time,from,full_picture', limit: 25 } }
    );

    res.json({
      success: true,
      data: response.data.data,
      paging: response.data.paging || null,
    });
  } catch (error) {
    console.error('[REST API] GET /posts error:', error.response?.data || error.message);
    res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data?.error?.message || error.message,
    });
  }
});

// POST /post - Create a new post
app.post('/post', async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) {
      return res.status(400).json({ success: false, error: 'Missing "message" field' });
    }

    const result = await facebookApi.createPost('me', message);
    console.log(`[REST API] POST /post - Created post: "${message.substring(0, 50)}..."`);

    res.json({ success: true, data: result });
  } catch (error) {
    console.error('[REST API] POST /post error:', error.response?.data || error.message);
    res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data?.error?.message || error.message,
    });
  }
});

// GET /comments - Get comments for a post
app.get('/comments', async (req, res) => {
  try {
    const { post_id } = req.query;
    if (!post_id) {
      return res.status(400).json({ success: false, error: 'Missing query param "post_id"' });
    }

    const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
    const FB_API_VERSION = process.env.FB_API_VERSION || 'v19.0';
    const axios = require('axios');

    const response = await axios.get(
      `https://graph.facebook.com/${FB_API_VERSION}/${post_id}/comments`,
      { params: { access_token: PAGE_ACCESS_TOKEN, fields: 'id,message,from,created_time', limit: 100 } }
    );

    res.json({
      success: true,
      data: response.data.data,
      paging: response.data.paging || null,
    });
  } catch (error) {
    console.error('[REST API] GET /comments error:', error.response?.data || error.message);
    res.status(error.response?.status || 500).json({
      success: false,
      error: error.response?.data?.error?.message || error.message,
    });
  }
});

// =====================================================
// Start system
// =====================================================

const run = async () => {
  console.log('====================================================');
  console.log('[Backend API] Starting...');
  console.log(`[Backend API] FAKE_MODE = ${process.env.FAKE_MODE === 'true' ? 'ON (simulating FB API)' : 'OFF (calling real FB API)'}`);
  console.log('====================================================');

  // Initialize database
  await initDatabase();

  // Connect Kafka
  await connectProducer();
  await connectConsumer();

  // Start REST API server
  app.listen(PORT, () => {
    console.log(`[Backend API] REST API server running on port ${PORT}`);
    console.log('[Backend API] Endpoints:');
    console.log('  GET  /health    - Health check');
    console.log('  GET  /posts     - List page posts');
    console.log('  POST /post      - Create a new post');
    console.log('  GET  /comments  - Get comments (query: post_id)');
  });

  console.log('[Backend API] Started successfully!');
  console.log('[Backend API] Consume: reply_commands, send_retry');
  console.log('[Backend API] Publish: send_failed (on error)');
};

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`\n[Backend API] process.on ${type}`);
      console.error(e);
      await disconnectConsumer();
      await disconnectProducer();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await disconnectConsumer();
      await disconnectProducer();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

run().catch(console.error);
