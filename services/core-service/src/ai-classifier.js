require('dotenv').config();
const { OpenAI } = require('openai');
const { GoogleGenerativeAI } = require('@google/generative-ai');

let openai;
let genAI;

if (process.env.AI_PROVIDER === 'OPENAI' && process.env.OPENAI_API_KEY && !process.env.OPENAI_API_KEY.includes('your_')) {
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
} else if (process.env.AI_PROVIDER === 'GEMINI' && process.env.GEMINI_API_KEY && !process.env.GEMINI_API_KEY.includes('your_')) {
  genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
}

const classifyEvent = async (content) => {
  if (!content) return { intent: 'unknown', sentiment: 'neutral' };

  const prompt = `
Bạn là một trợ lý AI chuyên phân tích bình luận và tin nhắn từ khách hàng trên Facebook Page.
Nhiệm vụ của bạn là phân tích nội dung sau và trả về CHỈ MỘT chuỗi JSON với 2 trường "intent" (ý định) và "sentiment" (cảm xúc).
- "intent": có thể là "hỏi giá", "khiếu nại", "tương tác tích cực", "hỗ trợ", "khác".
- "sentiment": có thể là "tích cực", "tiêu cực", "trung tính".
Tuyệt đối không giải thích thêm, không markdown, chỉ trả về đúng định dạng JSON.

Nội dung: "${content}"
`;

  try {
    if (process.env.AI_PROVIDER === 'OPENAI' && openai) {
      const response = await openai.chat.completions.create({
        model: "gpt-3.5-turbo",
        messages: [{ role: "user", content: prompt }],
        response_format: { type: "json_object" }
      });
      return JSON.parse(response.choices[0].message.content);
    }

    if (process.env.AI_PROVIDER === 'GEMINI' && genAI) {
      const model = genAI.getGenerativeModel({
        model: "gemini-2.5-flash-lite",
        generationConfig: { responseMimeType: "application/json" }
      });
      const result = await model.generateContent(prompt);
      let text = result.response.text();
      text = text.replace(/```json/g, '').replace(/```/g, '').trim();
      return JSON.parse(text);
    }

    return { intent: 'unknown', sentiment: 'neutral', error: 'AI_NOT_CONFIGURED' };
  } catch (error) {
    console.error('[AI] Lỗi khi gọi AI API:', error.message);
    return { intent: 'unknown', sentiment: 'neutral', error: error.message };
  }
};

module.exports = { classifyEvent };
