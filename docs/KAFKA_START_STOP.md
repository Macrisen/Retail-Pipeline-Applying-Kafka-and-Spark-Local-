# Cách tắt và mở lại Kafka local

Cập nhật: 2026-09-12. Các lệnh dưới đây do bạn tự chạy trong terminal.

## Các thành phần đang mở

| Thành phần | Trạng thái tại thời điểm kiểm tra | Cách tắt |
|---|---|---|
| Kafka server | Chạy nền qua Homebrew, cổng 9092 và 9093 | `brew services stop kafka` |
| Kafbat UI | Chạy tại http://localhost:8080, PID lúc kiểm tra là 32256 | Tìm PID hiện tại rồi dùng `kill PID` |
| Console consumer | Terminal trong ảnh đang chờ sự kiện | Bấm vào đúng terminal rồi nhấn `Ctrl+C` |
| Console producer | Nếu còn dấu `>` thì đang chờ nhập | Bấm vào đúng terminal rồi nhấn `Ctrl+C` |
| Python producer | Tự kết thúc khi gửi đủ số dòng trong `--limit` | Nếu còn chạy, nhấn `Ctrl+C` |

PID có thể thay đổi sau mỗi lần khởi động. Không dùng lại PID cũ nếu chưa kiểm tra.
Terminal trở lại dấu nhắc shell như `%` nghĩa là chương trình chạy trực tiếp trong
terminal đã kết thúc; Kafka chạy nền vẫn có thể đang hoạt động.

PostgreSQL local không chạy dưới dạng Homebrew service tại thời điểm kiểm tra.
Database PostgreSQL tạm dùng cho bài kiểm thử trước đó đã được dừng và dọn sạch.

## Tắt khi nghỉ

1. Ở từng terminal producer/consumer đang chạy, nhấn `Ctrl+C`.
   Nếu Python producer đã báo hoàn tất và trở lại dấu `%`, không cần tắt thêm.

2. Tìm tiến trình Kafbat UI đang nghe cổng 8080:

   ```bash
   lsof -nP -iTCP:8080 -sTCP:LISTEN
   ```

   Đọc cột PID, kiểm tra tiến trình trước khi tắt (thay `PID` bằng số vừa tìm):

   ```bash
   ps -p PID -o pid=,command=
   ```

   Nếu lệnh hiển thị Java chạy `api-v1.5.0.jar` của Kafbat UI, tắt bằng:

   ```bash
   kill PID
   ```

   Nếu UI đang chạy trực tiếp trong terminal của bạn, có thể nhấn `Ctrl+C` ở
   terminal đó. Nếu `lsof` không trả kết quả thì không có dịch vụ nghe cổng này.

3. Tắt Kafka server:

   ```bash
   brew services stop kafka
   ```

   Lệnh này cũng hủy đăng ký tự khởi động Kafka khi đăng nhập.

4. Kiểm tra lại:

   ```bash
   brew services list
   lsof -nP -iTCP:8080 -iTCP:9092 -iTCP:9093 -sTCP:LISTEN
   ```

   Kafka không còn trạng thái `started`, và `lsof` không còn tiến trình nghe các
   cổng trên nếu mọi thành phần đã tắt.

Đóng tab trình duyệt không tắt Kafbat UI. Đóng terminal consumer không tắt Kafka
server chạy nền. Các thao tác dừng ở trên không xóa topic hay message đã lưu;
Kafka vẫn quản lý dữ liệu theo chính sách lưu trữ khi chạy lại.

## Mở lại lần sau

1. Khởi động Kafka:

   ```bash
   brew services start kafka
   ```

   Kafka chạy nền và được đăng ký tự khởi động khi đăng nhập.

2. Chuyển terminal đến thư mục dự án:

   ```bash
   cd "/Users/macrisen/Desktop/Kafka Real-Time Data Pipeline"
   ```

3. Nếu cổng 8080 chưa có Kafbat UI chạy, mở UI:

   ```bash
   sh .local/kafbat-ui/start.sh
   ```

   Giữ terminal này mở khi dùng UI. Nhấn `Ctrl+C` để tắt UI khi dùng xong.
   UI chưa được cấu hình tự khởi động khi đăng nhập.

4. Mở http://localhost:8080 trong trình duyệt.
   Chọn **retail-local → Topics → retail-order-events → Messages** để xem sự kiện.

## Chạy thử producer và consumer

Mở terminal riêng để xem các message mới:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic retail-order-events
```

Để đọc cả message đã lưu trước đó, chạy consumer với `--from-beginning`:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic retail-order-events --from-beginning
```

Ở một terminal khác, tại thư mục dự án, gửi 10 dòng CSV với tốc độ tối đa 2 dòng/giây:

```bash
python3 producer.py --limit 10 --rate 2
```

Mỗi lần chạy producer sẽ đọc lại CSV từ đầu và gửi thêm message vào Kafka.
Đọc bằng consumer không tự xóa message.

## Phạm vi hiện tại của dự án

- Kafka local, topic `retail-order-events` và Kafbat UI đã hoạt động.
- Producer đọc CSV đã được viết; dữ liệu thử đã xuất hiện trong consumer/UI.
- Kafka chưa được nối với Spark để ghi vào Bronze/Silver/Gold trên Supabase.
- Bước tiếp theo: triển khai Spark Structured Streaming đọc topic và kiểm tra
  dữ liệu đầu ra, rồi mới nối phần ghi dữ liệu vào các tầng.
