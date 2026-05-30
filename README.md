# Hệ thống Xử lý Sự kiện Facebook Webhook

Hệ thống xử lý sự kiện Facebook Page sử dụng kiến trúc Microservices dựa trên sự kiện (Event-driven) với Kafka. Hệ thống tích hợp AI để phân tích nội dung và có cơ chế Retry (backoff) tự động cho các lệnh thất bại.

## Kiến trúc hệ thống

Hệ thống bao gồm các thành phần chính:
1. **Webhook Service**: Tiếp nhận sự kiện từ Facebook, xác thực chữ ký và đẩy vào Kafka topic `raw_events`.
2. **Core Service**: Tiêu thụ `raw_events`, sử dụng AI (Gemini/OpenAI) để phân loại ý định (intent), cảm xúc (sentiment) và ra quyết định hành động.
3. **Backend API**: Thực thi các hành động (reply, hide, delete) bằng cách gọi trực tiếp Facebook Graph API.
4. **Retry Service**: Quản lý các lệnh bị lỗi, thực hiện gửi lại với cơ chế backoff (thời gian chờ tăng dần) trước khi đưa vào Dead Letter Queue.

## Danh sách dịch vụ và Port

| Dịch vụ | Port | Mô tả |
|---------|------|-------|
| Webhook Service | 3001 | Tiếp nhận webhook từ Facebook |
| Core Service | 3002 | Xử lý logic AI và ra quyết định |
| Backend API | 3000 | Gọi API Facebook Graph |
| Retry Service | 3003 | Xử lý retry và Dead Letter Queue |
| Kafka UI | 8080 | Giao diện quản lý Kafka |
| Prometheus | 9090 | Theo dõi thông số hệ thống |
| Alertmanager | 9093 | Quản lý cảnh báo |
| PostgreSQL | 5432 | Cơ sở dữ liệu lưu trữ |

## Cấu hình môi trường (.env)

Dự án sử dụng duy nhất một file `.env` tại thư mục gốc để quản lý cấu hình cho tất cả các dịch vụ. Các biến môi trường riêng biệt cho từng container (như `KAFKA_GROUP_ID`) được cấu hình trực tiếp trong file `docker-compose.yml`.

Nội dung file `.env` tham khảo:

```env
# KAFKA
KAFKA_BROKERS=localhost:9092

# FACEBOOK API
PAGE_ACCESS_TOKEN=your_page_access_token
FB_APP_SECRET=your_app_secret
FB_VERIFY_TOKEN=your_verify_token
FB_API_VERSION=v19.0
FAKE_MODE=false

# AI CONFIG
AI_PROVIDER=GEMINI
GEMINI_API_KEY=your_gemini_api_key

# RETRY CONFIG
MAX_RETRIES=3
```

## Hướng dẫn vận hành

Hệ thống được thiết kế để chạy hoàn toàn trên Docker Compose.

### 1. Khởi động hệ thống
Chạy lệnh sau tại thư mục gốc để khởi động tất cả các dịch vụ:
```bash
docker-compose up -d
```

### 2. Cập nhật cấu hình
Khi thay đổi nội dung file `.env` hoặc `docker-compose.yml`, cần chạy lệnh sau để Docker tái tạo container với cấu hình mới:
```bash
docker-compose up -d
```

### 3. Kiểm tra Log
Theo dõi log của tất cả các dịch vụ:
```bash
docker-compose logs -f
```
Hoặc theo dõi một dịch vụ cụ thể:
```bash
docker-compose logs -f backend-api
```

### 4. Dừng hệ thống
```bash
docker-compose down
```

## Luồng xử lý Retry và Dead Letter Queue

1. Khi **Backend API** gọi Facebook Graph API thất bại, thông điệp sẽ được đẩy vào topic `send_failed`.
2. **Retry Service** tiêu thụ thông điệp từ `send_failed` và tính toán thời gian chờ (backoff).
3. Sau thời gian chờ, thông điệp được đẩy vào topic `send_retry` để **Backend API** thử lại.
4. Nếu số lần thử lại vượt quá `MAX_RETRIES` (mặc định là 3), thông điệp sẽ được chuyển vào topic `dead_letter` để kiểm tra thủ công.
