/**
 * Rate Limiter - Track comment frequency per sender.
 * If threshold exceeded (e.g. 20 comments / minute), mark as pending_review.
 */

const senderHistory = new Map(); // Map<senderId, timestamp[]>

const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX = 20; // Max 20 comments per minute

/**
 * Check if a sender is rate limited.
 * Returns { limited: boolean, count: number }
 */
const checkRateLimit = (senderId) => {
  if (!senderId) return { limited: false, count: 0 };

  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW_MS;

  // Get existing history or create new
  let history = senderHistory.get(senderId) || [];

  // Filter out old timestamps (outside the window)
  history = history.filter(ts => ts > windowStart);

  // Add new timestamp
  history.push(now);
  senderHistory.set(senderId, history);

  const limited = history.length > RATE_LIMIT_MAX;

  if (limited) {
    console.log(`[Rate Limiter] WARNING: sender ${senderId} sent ${history.length} comments in 1 minute -> pending_review`);
  }

  return { limited, count: history.length };
};

// Periodic memory cleanup (every 5 minutes)
setInterval(() => {
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW_MS;
  for (const [senderId, history] of senderHistory.entries()) {
    const filtered = history.filter(ts => ts > windowStart);
    if (filtered.length === 0) {
      senderHistory.delete(senderId);
    } else {
      senderHistory.set(senderId, filtered);
    }
  }
}, 5 * 60 * 1000);

module.exports = { checkRateLimit };
