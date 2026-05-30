const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.DB_HOST || 'postgres',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'fb_api_db',
  user: process.env.DB_USER || 'fb_api_user',
  password: process.env.DB_PASSWORD || 'fb_api_password',
});

// Auto-create tables on startup
const initDatabase = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS idempotency_keys (
        command_id VARCHAR(100) PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) NOT NULL
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        comment_id VARCHAR(100) UNIQUE NOT NULL,
        post_id VARCHAR(100),
        message TEXT,
        intent VARCHAR(50),
        sentiment VARCHAR(20),
        status VARCHAR(20) DEFAULT 'received',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('[Database] Connected to PostgreSQL, tables created.');
  } catch (error) {
    console.error('[Database] Init error:', error.message);
    throw error;
  }
};

// Check if idempotency key exists
const checkIdempotencyKey = async (commandId) => {
  const result = await pool.query(
    'SELECT command_id FROM idempotency_keys WHERE command_id = $1',
    [commandId]
  );
  return result.rows.length > 0;
};

// Save idempotency key after success
const saveIdempotencyKey = async (commandId, status = 'processed') => {
  await pool.query(
    'INSERT INTO idempotency_keys (command_id, status) VALUES ($1, $2) ON CONFLICT (command_id) DO NOTHING',
    [commandId, status]
  );
};

// Save processed comment data
const saveComment = async (commentData) => {
  try {
    await pool.query(
      `INSERT INTO comments (comment_id, post_id, message, intent, sentiment, status)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (comment_id) DO UPDATE SET
         intent = EXCLUDED.intent,
         sentiment = EXCLUDED.sentiment,
         status = EXCLUDED.status`,
      [
        commentData.comment_id,
        commentData.post_id || null,
        commentData.message || null,
        commentData.intent || null,
        commentData.sentiment || null,
        commentData.status || 'processed',
      ]
    );
  } catch (error) {
    console.error('[Database] Error saving comment:', error.message);
  }
};

module.exports = { pool, initDatabase, checkIdempotencyKey, saveIdempotencyKey, saveComment };
