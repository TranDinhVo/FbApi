const FAKE_MODE = process.env.FAKE_MODE === 'true';
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
const FB_API_VERSION = process.env.FB_API_VERSION || 'v19.0';
const FB_BASE_URL = `https://graph.facebook.com/${FB_API_VERSION}`;

const fakeSuccess = (action, details) => {
  console.log(`[Facebook API - FAKE] OK - ${action}:`, details);
  return { success: true, fake: true };
};

const fakeFail = (action, details) => {
  console.log(`[Facebook API - FAKE] SIMULATED FAILURE - ${action}:`, details);
  throw new Error(`Simulated Facebook timeout for action: ${action}`);
};


const realPost = async (url, body) => {
  const axios = require('axios');
  try {
    const response = await axios.post(url, { ...body, access_token: PAGE_ACCESS_TOKEN });
    return response.data;
  } catch (error) {
    if (error.response?.data) {
      console.error(`[Facebook API] API Error:`, JSON.stringify(error.response.data));
    }
    throw error;
  }
};


const replyComment = async (commentId, replyText) => {
  if (FAKE_MODE) {
    return fakeSuccess('replyComment', { commentId, replyText });
  }
  const url = `${FB_BASE_URL}/${commentId}/comments`;
  const result = await realPost(url, { message: replyText });
  console.log(`[Facebook API] Replied to comment ${commentId}`);
  return result;
};

const hideComment = async (commentId) => {
  if (FAKE_MODE) {
    return fakeSuccess('hideComment', { commentId });
  }
  const url = `${FB_BASE_URL}/${commentId}`;

  const axios = require('axios');
  const params = new URLSearchParams();
  params.append('is_hidden', 'true');
  params.append('access_token', PAGE_ACCESS_TOKEN);

  const response = await axios.post(url, params);

  console.log(`[Facebook API] Hidden comment ${commentId}`);
  return response.data;
};

const deleteComment = async (commentId) => {
  if (FAKE_MODE) {
    return fakeSuccess('deleteComment', { commentId });
  }
  const url = `${FB_BASE_URL}/${commentId}`;

  const axios = require('axios');
  const response = await axios.delete(url, {
    params: { access_token: PAGE_ACCESS_TOKEN }
  });

  console.log(`[Facebook API] Deleted comment ${commentId}`);
  return response.data;
};

const createPost = async (pageId, message) => {
  if (FAKE_MODE) {
    return fakeSuccess('createPost', { pageId, message });
  }
  const url = `${FB_BASE_URL}/${pageId}/feed`;
  const result = await realPost(url, { message });
  console.log(`[Facebook API] Created post for page ${pageId}`);
  return result;
};

const sendMessage = async (senderId, messageText) => {
  if (FAKE_MODE) {
    return fakeSuccess('sendMessage', { senderId, messageText });
  }
  const url = `${FB_BASE_URL}/me/messages`;
  const result = await realPost(url, {
    recipient: { id: senderId },
    message: { text: messageText },
  });
  console.log(`[Facebook API] Sent message to user ${senderId}`);
  return result;
};

module.exports = { replyComment, hideComment, deleteComment, createPost, sendMessage };
