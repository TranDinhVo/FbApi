# Hệ thống Quản lý Facebook Page Phân tán

> **Học phần:** Lập trình API  
> **Kiến trúc:** Event-driven Microservices với Kafka  
> **Stack:** Node.js, KafkaJS, Gemini AI, PostgreSQL, Prometheus, Docker Compose

---

## Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Danh sách Service và Port](#danh-sách-service-và-port)
- [Luồng dữ liệu chi tiết](#luồng-dữ-liệu-chi-tiết)
- [Kafka Topics](#kafka-topics)
- [Các tính năng chính](#các-tính-năng-chính)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Cấu hình môi trường](#cấu-hình-môi-trường)
- [Hướng dẫn cài đặt và chạy](#hướng-dẫn-cài-đặt-và-chạy)
- [Kiểm thử hệ thống](#kiểm-thử-hệ-thống)
- [Giám sát và Cảnh báo](#giám-sát-và-cảnh-báo)
- [Xử lý sự cố](#xử-lý-sự-cố)

---

## Tổng quan

Hệ thống xử lý sự kiện Facebook Page theo thời gian thực, sử dụng kiến trúc **Microservices hướng sự kiện (Event-driven)**. Khi có bình luận hoặc tin nhắn mới trên Facebook Page, hệ thống sẽ:

1. **Nhận và xác thực** webhook event từ Facebook (HMAC-SHA256)
2. **Chuẩn hóa** dữ liệu về schema nội bộ thống nhất
3. **Phân tích AI** - xác định ý định (intent) và cảm xúc (sentiment) bằng Gemini API
4. **Phát hiện spam** - nhận diện link độc hại, nội dung lặp
5. **Tự động phản hồi** - reply cảm ơn, xin lỗi, hoặc ẩn bình luận spam
6. **Xử lý lỗi** - retry tự động với exponential backoff, Dead Letter Queue

Mọi giao tiếp nội bộ giữa các service đều đi qua **Apache Kafka**. Không có service nào gọi trực tiếp service khác bằng HTTP.

---

## Kiến trúc hệ thống

```
┌──────────────┐     HTTP POST      ┌─────────────────────────┐
│              │ ──────────────────> │   Webhook Service       │
│  Facebook    │                    │   port: 3001             │
│  Page        │                    │   parse · normalize      │
│              │                    │   verify HMAC-SHA256     │
└──────────────┘                    └───────────┬─────────────┘
                                                │
                                         publish raw_events
                                                │
                                                v
                                    ┌───────────────────────┐
                                    │   Kafka Broker        │
                                    │   topic: raw_events   │
                                    └───────────┬───────────┘
                                                │
                                         consume raw_events
                                                │
                                                v
                                    ┌─────────────────────────┐
                                    │   Core Service           │
                                    │   port: 3002             │
                                    │   AI (Gemini) + Spam     │
                                    │   + Decision Engine      │
                                    └───────────┬─────────────┘
                                                │
                                      publish reply_commands
                                                │
                                                v
                                    ┌───────────────────────┐
                                    │   Kafka Broker        │
                                    │   topic: reply_commands│
                                    │   topic: send_retry   │
                                    └───────────┬───────────┘
                                                │
                                   consume reply_commands
                                   consume send_retry
                                                │
                                                v
  ┌──────────┐     check/save     ┌─────────────────────────┐
  │ Database │ <────────────────> │   Backend API            │
  │ Postgres │   idempotency key  │   port: 3000             │
  └──────────┘                    │   Send + Idempotency     │
                                  │   REST API (dashboard)   │
                                  └───────────┬─────────────┘
                                              │
                                       publish send_failed
                                       (khi gọi FB API lỗi)
                                              │
                                              v
                                  ┌───────────────────────┐
                                  │   Kafka Broker        │
                                  │   topic: send_failed  │
                                  └───────────┬───────────┘
                                              │
                                       consume send_failed
                                              │
                                              v
                                  ┌─────────────────────────┐
                                  │   Retry Service          │
                                  │   port: 3003             │
                                  │   exponential backoff    │
                                  └──────┬──────────┬───────┘
                                         │          │
                          (counter < N)  │          │  (counter >= N)
                        publish send_retry          publish dead_letter
                                         │          │
                                         v          v
                               ┌──────────┐  ┌──────────────────┐
                               │send_retry│  │   dead_letter     │
                               │-> Backend│  │   Prometheus      │
                               │  API     │  │   -> Alertmanager │
                               └──────────┘  │   -> Slack        │
                                             └──────────────────┘
```

---

## Danh sách Service và Port

| Service | Port | Vai trò |
|---------|------|---------|
| **Backend API** | `3000` | Service duy nhất gọi Facebook Graph API. Expose REST API cho dashboard. Kiểm tra idempotency trước khi gửi. |
| **Webhook Service** | `3001` | Nhận webhook từ Facebook, xác thực chữ ký HMAC-SHA256, normalize event và publish vào Kafka. |
| **Core Service** | `3002` | Consume `raw_events`, phân tích AI (intent + sentiment), phát hiện spam, ra quyết định hành động. |
| **Retry Service** | `3003` | Consume `send_failed`, retry với exponential backoff. Chuyển vào Dead Letter Queue khi vượt ngưỡng. |
| **Kafka UI** | `8080` | Giao diện web xem topic, message, consumer group. Dùng để debug và xử lý DLQ thủ công. |
| **Prometheus** | `9090` | Thu thập metric từ Kafka Exporter, đánh giá alert rule. |
| **Alertmanager** | `9093` | Nhận alert từ Prometheus, gửi cảnh báo qua Slack. |
| **PostgreSQL** | `5432` | Lưu idempotency key và lịch sử bình luận. |
| **Kafka Broker** | `9092` | Message broker trung tâm cho toàn bộ hệ thống. |
| **Kafka Exporter** | `9308` | Cầu nối Kafka -> Prometheus (expose metric offset, lag). |

---

## Luồng dữ liệu chi tiết

### Luồng chính (Happy Path)

```
Facebook Page -> [có comment mới]
       │
       v
1. Facebook gửi HTTP POST đến webhook-service:3001/webhook
2. webhook-service xác thực chữ ký HMAC-SHA256
3. webhook-service normalize payload -> schema chuẩn nội bộ
4. Publish vào Kafka topic "raw_events" -> trả 200 OK cho Facebook
       │
       v
5. core-service consume "raw_events"
6. Bước 1: Phát hiện spam (link, nội dung lặp, từ khóa nghi ngờ)
7. Bước 2: Gọi Gemini AI phân loại intent + sentiment
8. Bước 3: Decision Engine quyết định hành động
9. Publish command vào "reply_commands"
       │
       v
10. backend-api consume "reply_commands"
11. Kiểm tra idempotency key trong Database
12. Gọi Facebook Graph API (reply / hide / delete)
13. Lưu idempotency key -> Hoàn thành
```

### Luồng lỗi (Error Path)

```
backend-api gọi Facebook API -> THẤT BẠI
       │
       v
1. Publish vào "send_failed" (retry_count = 1)
       │
       v
2. retry-service consume "send_failed"
3. Kiểm tra retry_count < MAX_RETRIES (mặc định 3)?
       │
       ├── CÒN -> Chờ backoff (1s x 2^retry_count)
       │         -> Publish "send_retry"
       │         -> backend-api thử lại
       │
       └── HẾT -> Publish "dead_letter"
                -> Prometheus phát hiện offset tăng
                -> Alertmanager bắn cảnh báo Slack
                -> Admin xử lý thủ công qua Kafka UI
```

---

## Kafka Topics

| Topic | Producer | Consumer | Mô tả |
|-------|----------|----------|--------|
| `raw_events` | webhook-service | core-service | Event đã normalize từ Facebook webhook |
| `reply_commands` | core-service | backend-api | Lệnh hành động (reply, hide, delete) |
| `send_failed` | backend-api | retry-service | Lệnh thất bại cần retry |
| `send_retry` | retry-service | backend-api | Lệnh retry sau backoff |
| `dead_letter` | retry-service | *(không có consumer)* | Message thất bại vĩnh viễn. Prometheus giám sát offset. |

---

## Các tính năng chính

### 1. Xác thực Webhook (HMAC-SHA256)
- Mỗi request từ Facebook đều kèm header `X-Hub-Signature-256`
- Service tính HMAC-SHA256 từ body + `FB_APP_SECRET` và so sánh
- Request giả mạo bị từ chối ngay

### 2. Normalize Event
- Comment và Message từ Facebook có cấu trúc khác nhau
- Sau normalize, cả hai đều ra cùng một schema:
  ```json
  {
    "source": "facebook",
    "type": "comment | message | post",
    "pageId": "...",
    "senderId": "...",
    "content": "...",
    "commentId": "...",
    "postId": "...",
    "status": "received"
  }
  ```

### 3. Phân tích AI (Gemini API)
- **Intent:** hỏi giá, khiếu nại, tương tác tích cực, hỗ trợ, khác
- **Sentiment:** tích cực, trung tính, tiêu cực
- Có fallback bằng keyword matching khi AI không khả dụng

### 4. Phát hiện Spam
- Chứa link + từ khóa nghi ngờ -> `malicious_link`
- Chứa link đơn thuần -> `light_spam_link`
- Nội dung lặp lại nhiều lần -> `repeated_content`

### 5. Automation Rules (Decision Engine)
| Kết quả phân tích | Hành động | Ví dụ |
|--------------------|-----------|-------|
| Spam | Ẩn bình luận | Link quảng cáo, nội dung lặp |
| Sentiment tích cực | Reply cảm ơn | "Shop hỗ trợ rất nhanh" |
| Sentiment tiêu cực | Reply xin lỗi | "Mình chưa nhận được hàng" |
| Intent hỏi giá | Auto-reply báo giá | "Giá bao nhiêu vậy shop?" |
| Không xác định | Bỏ qua | Bình luận tag bạn bè |

### 6. Retry với Exponential Backoff
- Công thức: `delay = 1000ms x 2^(retry_count - 1)`
- Lần 1: chờ 1s -> Lần 2: chờ 2s -> Lần 3: chờ 4s
- Vượt `MAX_RETRIES` -> chuyển sang Dead Letter Queue

### 7. Idempotency
- Mỗi command có `command_id` duy nhất (UUID)
- Backend API kiểm tra `command_id` trong PostgreSQL trước khi gọi Facebook API
- Nếu đã xử lý -> bỏ qua, không gửi reply trùng lặp

### 8. Dead Letter Queue và Cảnh báo
- Topic `dead_letter` lưu message thất bại vĩnh viễn
- Prometheus theo dõi offset, khi tăng -> Alertmanager bắn Slack ngay
- Admin xem chi tiết message qua Kafka UI (`http://localhost:8080`)

---

## Cấu trúc thư mục

```
fb_api/
├── docker-compose.yml          # Cấu hình toàn bộ hệ thống
├── .env                        # Biến môi trường (token, API key)
├── .gitignore
├── README.md
│
├── services/
│   ├── webhook-service/        # Port 3001 - Nhận webhook Facebook
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.js                # Express server + webhook endpoint
│   │       ├── signature-verifier.js   # Xác thực HMAC-SHA256
│   │       ├── webhook-handler.js      # Normalize comment/message/post
│   │       └── kafka-producer.js       # Publish raw_events
│   │
│   ├── core-service/           # Port 3002 - AI + Decision Engine
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.js                # Kafka consumer + pipeline xử lý
│   │       ├── ai-classifier.js        # Gọi Gemini/OpenAI phân loại
│   │       ├── spam-filter.js          # Phát hiện spam
│   │       ├── decision-engine.js      # Ra quyết định hành động
│   │       └── kafka-producer.js       # Publish reply_commands
│   │
│   ├── backend-api/            # Port 3000 - Gọi Facebook Graph API
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.js                # Entry point
│   │       ├── facebook-api.js         # Wrapper gọi Facebook Graph API
│   │       ├── kafka-consumer.js       # Consume reply_commands + send_retry
│   │       ├── kafka-producer.js       # Publish send_failed
│   │       └── handlers/
│   │           └── command-handler.js  # Xử lý từng command (reply/hide/delete)
│   │
│   └── retry-service/          # Port 3003 - Retry + DLQ
│       ├── package.json
│       └── src/
│           ├── index.js                # Entry point
│           ├── retry-handler.js        # Logic exponential backoff
│           ├── kafka-consumer.js       # Consume send_failed
│           └── kafka-producer.js       # Publish send_retry / dead_letter
│
├── prometheus/
│   ├── prometheus.yml          # Cấu hình scrape metric
│   └── alert.rules.yml         # Luật cảnh báo (DLQ, broker down)
│
└── alertmanager/
    └── alertmanager.yml        # Route cảnh báo đến Slack
```

---

## Cấu hình môi trường

Hệ thống sử dụng **duy nhất một file `.env`** tại thư mục gốc. Các biến môi trường riêng cho từng container (như `KAFKA_GROUP_ID`) được cấu hình trong `docker-compose.yml`.

```env
# ==== KAFKA ====
KAFKA_BROKERS=localhost:9092

# ==== FACEBOOK API ====
PAGE_ACCESS_TOKEN=<your_page_access_token>
FB_APP_SECRET=<your_app_secret>
FB_VERIFY_TOKEN=my_verify_token_123
FB_API_VERSION=v19.0
FAKE_MODE=false                # true = giả lập Facebook API (không gọi thật)

# ==== AI CONFIG ====
AI_PROVIDER=GEMINI             # GEMINI hoặc OPENAI
GEMINI_API_KEY=<your_gemini_api_key>

# ==== RETRY SERVICE ====
MAX_RETRIES=3                  # Số lần retry tối đa trước khi vào DLQ

# ==== ALERTMANAGER ====
SLACK_WEBHOOK_URL=<your_slack_incoming_webhook_url>
```

| Biến | Bắt buộc | Mô tả |
|------|----------|--------|
| `PAGE_ACCESS_TOKEN` | Có | Token truy cập Facebook Page (lấy từ Facebook Developer) |
| `FB_APP_SECRET` | Có | App Secret để xác thực webhook HMAC-SHA256 |
| `FB_VERIFY_TOKEN` | Có | Token xác minh khi Facebook verify webhook endpoint |
| `GEMINI_API_KEY` | Có | API key của Google Gemini AI |
| `FAKE_MODE` | Không | `true` để test mà không gọi Facebook thật |
| `MAX_RETRIES` | Không | Mặc định `3`. Số lần retry trước khi vào Dead Letter Queue |
| `SLACK_WEBHOOK_URL` | Không | URL Incoming Webhook của Slack để nhận cảnh báo |

---

## Hướng dẫn cài đặt và chạy

### Yêu cầu trước

- **Docker Desktop** đã cài đặt và đang chạy
- **ngrok** (hoặc tool tương đương) để expose webhook ra internet cho Facebook gửi event

### Bước 1: Clone và cấu hình

```bash
# Clone project
git clone <repository-url>
cd fb_api

# Cấu hình biến môi trường
# Mở file .env và điền các giá trị thật
```

### Bước 2: Khởi động hệ thống

```bash
# Khởi động tất cả container
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

### Bước 3: Kiểm tra hệ thống đã sẵn sàng

| Kiểm tra | URL / Lệnh |
|----------|-------------|
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| Kafka Exporter metrics | http://localhost:9308/metrics |
| Prometheus targets | http://localhost:9090/targets (mục `kafka` phải **UP**) |
| Container logs | `docker-compose logs -f` |

### Bước 4: Expose webhook bằng ngrok

```bash
# Mở terminal mới
ngrok http 3001

# Copy URL ngrok (ví dụ: https://abc123.ngrok.io)
# Vào Facebook App Dashboard -> Webhooks -> Callback URL:
# https://abc123.ngrok.io/webhook
# Verify Token: my_verify_token_123
```

### Bước 5: Đăng ký nhận sự kiện Facebook

Trong Facebook App Dashboard:
1. Vào **Webhooks** -> Subscribe to **Page** events
2. Chọn các fields: `feed` (để nhận comment), `messages` (để nhận tin nhắn)
3. Vào **Page Settings** -> Chọn Page -> Subscribe

### Các lệnh thường dùng

```bash
# Xem log toàn bộ hệ thống
docker-compose logs -f

# Xem log một service cụ thể
docker-compose logs -f webhook-service
docker-compose logs -f core-service
docker-compose logs -f backend-api
docker-compose logs -f retry-service

# Restart một service
docker-compose restart backend-api

# Dừng hệ thống
docker-compose down

# Dừng và xóa toàn bộ dữ liệu (volume)
docker-compose down -v

# Rebuild khi thay đổi code
docker-compose up -d --build
```

---

## Kiểm thử hệ thống

### Test 1: Luồng chính - Comment mới -> Reply tự động

1. Vào Facebook Page, đăng một bài viết
2. Dùng tài khoản khác, bình luận: **"Shop ơi giá bao nhiêu?"**
3. Kiểm tra log:
   ```
   [Webhook]     Đã nhận Comment mới từ ...
   [Core Service] Decision: auto_reply | Intent: hỏi giá
   [Backend API]  SUCCESS - reply comment ...
   ```
4. Trên Facebook, bình luận sẽ được reply tự động

### Test 2: Phân tích cảm xúc

| Comment thử | Kết quả mong đợi |
|-------------|-------------------|
| "Shop hỗ trợ rất nhanh" | Sentiment: tích cực -> Reply cảm ơn |
| "Mình chưa nhận được hàng" | Sentiment: tiêu cực -> Reply xin lỗi |
| "Bài viết hay quá" | Sentiment: tích cực -> Reply cảm ơn |

### Test 3: Phát hiện spam

| Comment thử | Kết quả mong đợi |
|-------------|-------------------|
| "Bấm vào link http://scam.com" | Spam malicious_link -> Ẩn bình luận |
| "http://example.com" | Spam light_link -> Ẩn bình luận |
| "aaaaaaaaaaaaaaaa" (nội dung lặp) | Spam repeated -> Ẩn bình luận |

### Test 4: Retry và Dead Letter Queue

1. Bật `FAKE_MODE=true` trong `.env` (hoặc dùng token hết hạn)
2. Sửa `facebook-api.js` để giả lập lỗi liên tục
3. Gửi comment -> Kiểm tra log:
   ```
   [Backend API]    FAILED - Simulated Facebook timeout
   [Retry Handler]  Sẽ retry sau 1000ms (retry #1)
   [Retry Handler]  Sẽ retry sau 2000ms (retry #2)
   [Retry Handler]  Sẽ retry sau 4000ms (retry #3)
   [Retry Handler]  -> Dead Letter Queue
   ```
4. Mở Kafka UI -> Topic `dead_letter` -> Có message mới
5. Prometheus alert `DeadLetterQueueAlert` firing
6. Alertmanager gửi Slack (nếu đã cấu hình)

### Test 5: Idempotency

1. Gửi cùng một command 2 lần (simulate Kafka redeliver)
2. Backend API kiểm tra `command_id` trong Database
3. Lần 2 bị bỏ qua -> Không gửi reply trùng

---

## Giám sát và Cảnh báo

### Prometheus Alert Rules

| Alert | Điều kiện | Mức độ | Mô tả |
|-------|-----------|--------|--------|
| `DeadLetterQueueAlert` | Offset topic `dead_letter` tăng trong 5 phút | Critical | Có message mới vào DLQ |
| `KafkaBrokerDown` | Không có Kafka broker nào chạy | Critical | Kafka broker bị down |

### Alertmanager -> Slack

- Alert `critical` được gửi ngay qua Slack
- Khi vấn đề được giải quyết -> Slack nhận thông báo `RESOLVED`
- `repeat_interval: 1h` - không spam nếu lỗi kéo dài

### Dashboard và Debug

- **Kafka UI** (`http://localhost:8080`): Xem topic, message, consumer lag
- **Prometheus** (`http://localhost:9090`): Query metric, xem alert status
- **Alertmanager** (`http://localhost:9093`): Xem alert đang firing

---

## Xử lý sự cố

| Vấn đề | Nguyên nhân có thể | Giải pháp |
|--------|---------------------|-----------|
| Webhook không nhận event | ngrok chưa chạy hoặc URL sai | Kiểm tra ngrok, cập nhật URL trong Facebook App |
| Chữ ký HMAC không hợp lệ | `FB_APP_SECRET` sai | Kiểm tra lại App Secret trong Facebook Developer |
| AI trả về `unknown` | API key sai hoặc hết quota | Kiểm tra `GEMINI_API_KEY`, xem log lỗi |
| Reply không xuất hiện | `PAGE_ACCESS_TOKEN` hết hạn | Tạo lại token trong Facebook Developer |
| Kafka không kết nối | Kafka chưa ready | Chờ container `init-kafka` hoàn thành |
| Consumer lag cao | Service xử lý chậm | Kiểm tra log service, tăng partition nếu cần |
| Message vào DLQ | Facebook API lỗi liên tục | Kiểm tra token, rate limit, xem message trong Kafka UI |

### Tạo bảng Database thủ công (nếu cần)

```sql
-- Kết nối PostgreSQL
docker exec -it fb_api-postgres psql -U fb_api_user -d fb_api_db

-- Bảng idempotency key
CREATE TABLE IF NOT EXISTS idempotency_keys (
    command_id VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL
);

-- Bảng lưu lịch sử bình luận
CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    comment_id VARCHAR(100) UNIQUE NOT NULL,
    post_id VARCHAR(100) NOT NULL,
    message TEXT,
    intent VARCHAR(50),
    sentiment VARCHAR(20),
    status VARCHAR(20) DEFAULT 'received',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Giao thức giao tiếp

### Quy tắc chung

- Mọi giao tiếp nội bộ đều qua Kafka - không gọi HTTP giữa các service
- Chỉ **Backend API** được gọi Facebook Graph API
- Idempotency bắt buộc cho mọi consumer
- Retry có giới hạn (exponential backoff, tối đa N lần)
- Message vượt ngưỡng retry -> Dead Letter Queue (không retry vô hạn)

### Schema message mẫu

**raw_events** (webhook-service -> core-service):
```json
{
  "source": "facebook",
  "type": "comment",
  "pageId": "123456",
  "senderId": "789",
  "senderName": "Nguyễn Văn A",
  "content": "Shop ơi giá bao nhiêu?",
  "commentId": "123456_789",
  "postId": "123456_456",
  "status": "received"
}
```

**reply_commands** (core-service -> backend-api):
```json
{
  "schema_version": 1,
  "command_id": "uuid-v4",
  "event_id": "123456_789",
  "action": "reply",
  "target": {
    "comment_id": "123456_789",
    "sender_id": "789",
    "type": "comment"
  },
  "reply_text": "Cảm ơn bạn đã quan tâm! Nhân viên sẽ IB tư vấn thêm cho bạn nhé!",
  "intent": "hỏi giá",
  "sentiment": "trung tính"
}
```

**send_failed** (backend-api -> retry-service):
```json
{
  "schema_version": 1,
  "command_id": "uuid-v4",
  "event_id": "123456_789",
  "retry_count": 1,
  "last_error": "Request failed with status code 500",
  "payload": { "action": "reply", "target": {...}, "reply_text": "..." }
}
```

**dead_letter** (retry-service -> không có consumer):
```json
{
  "schema_version": 1,
  "command_id": "uuid-v4",
  "event_id": "123456_789",
  "retry_count": 3,
  "final_error": "Facebook timeout after maximum retries",
  "payload": { "action": "reply", "target": {...}, "reply_text": "..." },
  "dead_at": "2026-05-30T07:00:00.000Z"
}
```
