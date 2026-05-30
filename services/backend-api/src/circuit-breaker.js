const CircuitBreaker = require('opossum');
const facebookApi = require('./facebook-api');

const BREAKER_OPTIONS = {
  timeout: 10000,          // 10 second timeout
  errorThresholdPercentage: 50,  // Open circuit when 50% requests fail
  resetTimeout: 30000,     // Wait 30 seconds before retry (half-open)
  volumeThreshold: 5,      // Minimum 5 requests before calculating threshold
};

// Create circuit breaker for each action
const replyBreaker = new CircuitBreaker(
  (commentId, replyText) => facebookApi.replyComment(commentId, replyText),
  BREAKER_OPTIONS
);

const hideBreaker = new CircuitBreaker(
  (commentId) => facebookApi.hideComment(commentId),
  BREAKER_OPTIONS
);

const deleteBreaker = new CircuitBreaker(
  (commentId) => facebookApi.deleteComment(commentId),
  BREAKER_OPTIONS
);

const createPostBreaker = new CircuitBreaker(
  (pageId, message) => facebookApi.createPost(pageId, message),
  BREAKER_OPTIONS
);

const sendMessageBreaker = new CircuitBreaker(
  (senderId, messageText) => facebookApi.sendMessage(senderId, messageText),
  BREAKER_OPTIONS
);

// Log circuit breaker events
const setupBreakerEvents = (breaker, name) => {
  breaker.on('open', () => {
    console.warn(`[Circuit Breaker] OPEN - ${name}: Temporarily stopped calling Facebook API (too many consecutive errors)`);
  });
  breaker.on('halfOpen', () => {
    console.log(`[Circuit Breaker] HALF-OPEN - ${name}: Retrying Facebook API call...`);
  });
  breaker.on('close', () => {
    console.log(`[Circuit Breaker] CLOSED - ${name}: Facebook API operating normally.`);
  });
  breaker.on('fallback', () => {
    console.warn(`[Circuit Breaker] FALLBACK - ${name}: Circuit is open, skipping request.`);
  });
};

setupBreakerEvents(replyBreaker, 'replyComment');
setupBreakerEvents(hideBreaker, 'hideComment');
setupBreakerEvents(deleteBreaker, 'deleteComment');
setupBreakerEvents(createPostBreaker, 'createPost');
setupBreakerEvents(sendMessageBreaker, 'sendMessage');

module.exports = {
  replyComment: (commentId, replyText) => replyBreaker.fire(commentId, replyText),
  hideComment: (commentId) => hideBreaker.fire(commentId),
  deleteComment: (commentId) => deleteBreaker.fire(commentId),
  createPost: (pageId, message) => createPostBreaker.fire(pageId, message),
  sendMessage: (senderId, messageText) => sendMessageBreaker.fire(senderId, messageText),
};
