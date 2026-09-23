# Hướng dẫn kiểm thử Retail Kafka Pipeline

Tài liệu này ghi lại cách kiểm thử pipeline local theo luồng:

```text
CSV → Python Producer → Kafka → Spark → Bronze → Silver → Gold → CSV dashboard
```

Chạy các lệnh tại thư mục gốc dự án. Khi sao chép lệnh, giữ nguyên dấu gạch dưới
(`event_id`, không phải `event\_id`). Không nhập lại dấu nhắc `%` của terminal.

Thứ tự dựng database mới nằm trong `sql/README.md`. Các bước dưới đây kiểm tra
database local đã cài schema và procedure. Không chạy `02` trên database dùng
view `bronze.raw_sales` của file `13`.

## 1. Kiểm tra dịch vụ trước khi test

Khởi động và kiểm tra Kafka:

```bash
brew services start kafka
kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic retail-order-events
```

Đạt khi topic tồn tại, có `TopicId` và partition 0 có leader. Nếu báo không kết
nối được `localhost:9092`, kiểm tra `brew services list`, rồi khởi động lại Kafka.

Kiểm tra terminal đang kết nối đúng PostgreSQL:

```bash
psql -X -P pager=off -c "SELECT current_database(), current_user;"
```

Đạt khi trả về database đang chứa các schema `bronze`, `silver`, `gold`. Nếu psql
thử database mang tên user Mac, terminal đó chưa có đúng biến kết nối PostgreSQL.

## 2. Test producer gửi dữ liệu vào Kafka

Mở consumer quan sát ở terminal thứ nhất:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic retail-order-events
```

Ở terminal thứ hai, gửi 10 sự kiện:

```bash
python3 producer.py --limit 10 --rate 2
```

Đạt khi producer báo đã gửi đủ và consumer hiển thị JSON. Nhấn `Ctrl+C` để dừng
consumer. Đọc message không xóa message khỏi Kafka.

## 3. Test Kafka → Bronze ở chế độ preview

Chạy tối đa 1.000 message:

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  streaming/kafka_to_bronze.py --mode preview
```

Đạt khi có dòng `Batch preview committed`. Ý nghĩa:

- `read`: số message Spark đọc trong batch.
- `inserted`: số tọa độ Kafka mới được thêm vào Bronze.
- `already_present`: số message đã tồn tại trong Bronze.
- `inserted=0` không phải lỗi nếu batch đang đọc lại dữ liệu đã có.

Kiểm tra Bronze:

```bash
psql -X -P pager=off -c "
SELECT source_topic_id, partition_id, count(*) AS rows,
       min(kafka_offset) AS first_offset,
       max(kafka_offset) AS last_offset
FROM bronze.kafka_sales_events
WHERE topic = 'retail-order-events'
GROUP BY source_topic_id, partition_id;"
```

## 4. Test backfill toàn bộ Kafka và refresh Silver/Gold

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  streaming/kafka_to_bronze.py \
  --mode backfill \
  --max-offsets-per-trigger 50000
```

Đạt khi lệnh tự kết thúc và mỗi batch không rỗng được theo sau bởi:

```text
Silver → Gold refreshed successfully.
```

`50000` là giới hạn mỗi streaming batch. Tham số này không thay đổi checkpoint.
Nếu checkpoint bắt đầu từ offset 0 nhưng Bronze đã có dữ liệu, các batch đầu có
thể đều `inserted=0`. Code hiện vẫn chạy lại 12/08 cho các batch đó.

## 5. Test chất lượng Silver

```bash
psql -X -v ON_ERROR_STOP=1 -f sql/05_silver_quality_checks.sql
psql -X -v ON_ERROR_STOP=1 -c "CALL silver.prepare_gold_source();"
```

File 05 chỉ cài procedure; lệnh `CALL` mới chạy kiểm tra. Đạt khi có notice
`Silver PASS`. Validator kiểm tra dữ liệu rỗng, trùng
`event_id`, trường bắt buộc, số âm/NULL, timestamp, event type, thuộc tính đơn
hàng mâu thuẫn và đối chiếu Bronze với Silver. Nếu lỗi, sửa dữ liệu hoặc logic
nguồn; không xóa bảng để né validator.

`NOTICE: ... does not exist, skipping` từ việc dọn bảng tạm trước khi tạo lại
là bình thường. `ERROR`, transaction aborted hoặc thiếu `Silver PASS` thì
không tính là đạt.

## 6. Test load và chất lượng Gold

```bash
psql -X -v ON_ERROR_STOP=1 \
  -f sql/08_load_and_validate_gold.sql
```

Đạt khi có notice `Gold PASS`, trả số dòng của năm bảng Gold và kết thúc bằng
`COMMIT`. Kiểm tra nhanh:

```bash
psql -X -P pager=off -c "
SELECT 'fact_orders' AS table_name, count(*) FROM gold.fact_orders
UNION ALL
SELECT 'fact_sales_events', count(*) FROM gold.fact_sales_events;"
```

Không cộng `order_total_vnd` trên mọi dòng `fact_sales_events`, vì một đơn có
nhiều sự kiện. Dùng `gold.order_payment_metrics` cho dòng tiền.

## 7. Test chạy lại không tạo dữ liệu sai (idempotency)

Chỉ chạy sau khi Gold đã load thành công:

```bash
psql -X -v ON_ERROR_STOP=1 -f sql/09_test_gold_idempotency.sql
```

Đạt khi hiện:

```text
PASS: all five Gold tables unchanged after rerun
```

File test dùng transaction và rollback để gỡ trigger kiểm thử. Nếu phiên bị
`transaction aborted`, chạy `ROLLBACK;` trước khi thử lại.

## 8. Test duplicate Kafka/Bronze

Test tự động này cần PostgreSQL local và môi trường Python của Bronze:

```bash
.local/bronze-venv/bin/python tests/test_kafka_bronze.py
```

Đạt khi dòng cuối bắt đầu bằng `PASS`. Test xác nhận lần ghi đầu insert, chạy lại
không đổi dữ liệu, duplicate event ở Kafka vẫn được giữ raw, byte lỗi/NULL được
giữ nguyên và topic được tạo lại có Topic ID riêng.

Trong dữ liệu thực, Silver chỉ giữ một dòng cho mỗi `event_id`:

```bash
psql -X -P pager=off -c "
SELECT event_id, count(*)
FROM silver.sales_events
GROUP BY event_id
HAVING count(*) > 1;"
```

Đạt khi trả `(0 rows)`.

## 9. Test late-arriving event của bước 23

Test đã dùng order `ORD-LATE-5f5e472ad2cb`:

1. Gửi `ORDER_DELIVERED` lúc `20:17:33 UTC` trước.
2. Gửi `PAYMENT_CONFIRMED` có event time `19:17:33 UTC` sau.
3. Chạy backfill để đưa message mới qua Bronze, Silver và Gold.

File gửi payment đến muộn:

```bash
python3 .local/send_late_payment.py
```

Script dùng cố định `event_id`; không gửi lại nếu Kafka đã có message đó. Kiểm tra
trực tiếp Kafka trước khi nghi ngờ message bị mất:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic retail-order-events \
  --partition 0 \
  --offset OFFSET_CAN_KIEM_TRA \
  --max-messages 1 \
  --timeout-ms 15000
```

Sau backfill, kiểm tra kết quả:

```bash
psql -X -P pager=off <<'SQL'
SET TIME ZONE 'UTC';

SELECT event_type, event_time_utc, producer_time_utc, is_late_event
FROM gold.fact_sales_events
WHERE order_id = 'ORD-LATE-5f5e472ad2cb'
ORDER BY event_time_utc;

SELECT order_id, paid_at_utc, delivered_at_utc,
       latest_event_type, latest_event_at_utc
FROM gold.fact_orders
WHERE order_id = 'ORD-LATE-5f5e472ad2cb';
SQL
```

Dòng kết thúc `SQL` phải đứng riêng. Đạt khi:

- Có hai event: payment `19:17:33` và delivered `20:17:33`.
- Payment có `is_late_event = true`.
- `paid_at_utc` được điền là `19:17:33`.
- `latest_event_type` vẫn là `ORDER_DELIVERED` lúc `20:17:33`.

Điều này chứng minh Gold chọn trạng thái bằng `event_time_utc`, không dựa vào thứ
tự message đến Kafka.

## 10. Test watermark và window aggregation

Test riêng, không ghi Kafka hoặc PostgreSQL:

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit \
  --master 'local[2]' \
  streaming/test_watermark_windows.py
```

Test dùng window 5 phút và watermark delay 10 phút. Đạt khi dòng cuối là:

```text
PASS: window counts correct; late event accepted; closed-window event dropped.
```

Các trường hợp được kiểm tra:

- Event `10:14` đến sau nhưng cửa sổ `10:10–10:15` còn giữ state: được tính.
- Event `10:02` đến khi cửa sổ `10:00–10:05` đã chốt: bị bỏ qua.
- Các window hoàn tất có count đúng.

Đây là bài test Spark độc lập. Watermark/window chưa được tích hợp vào luồng
Kafka → Gold hiện tại.

## 11. Checkpoint và recovery – bước 25

Theo quyết định hiện tại, bài ngắt Spark giữa chừng rồi khởi động lại được **bỏ
qua**, không đánh dấu PASS. Không xóa thư mục `.local/checkpoints` vì checkpoint
đang giúp Spark tiếp tục từ vị trí đã xử lý.

## 12. Test dữ liệu xuất cho Power BI/Fabric

Xuất toàn bộ Gold trong một snapshot nhất quán:

```bash
mkdir -p dashboard
psql -X -v ON_ERROR_STOP=1 -f sql/14_export_gold_dashboard.sql
```

Đạt khi sáu lệnh `COPY` thành công và kết thúc bằng `COMMIT`. Kiểm tra file:

```bash
ls -lh dashboard/*.csv
wc -l dashboard/*.csv
```

`wc -l` bao gồm một dòng header, nên số dòng file bằng số record trong bảng cộng
một. Sáu file cần có:

```text
dim_date.csv
dim_customer.csv
dim_product.csv
fact_orders.csv
fact_sales_events.csv
order_payment_metrics.csv
```

Sau khi upload vào Power BI/Fabric, kiểm tra relationship, cardinality và tổng
doanh thu. Tổng doanh thu phải lấy từ `order_payment_metrics`, không cộng tất cả
event.

## 13. Bộ test đầy đủ trong database tạm

Khi cần regression test toàn bộ batch pipeline:

```bash
python3 tests/test_gold_pipeline.py
```

Test tự tạo PostgreSQL tạm, chạy lại Silver/Gold, kiểm tra idempotency, payment,
refund, UTC, dữ liệu không hợp lệ, late arrival và dọn database tạm. Đạt khi dòng
cuối là:

```text
Temporary PostgreSQL stopped and removed. ALL TESTS PASSED
```

## Checklist kết thúc

Phạm vi bằng chứng: `tests/test_gold_pipeline.py` kiểm tra batch SQL trong
PostgreSQL tạm; `tests/test_kafka_bronze.py` kiểm tra sink Bronze trong
PostgreSQL tạm. Chúng không tự chạy Kafka → Spark thật. Bài watermark/window
là test riêng, chưa được tích hợp vào pipeline chính. Kiểm thử ngắt Spark
giữa batch và tự khôi phục vẫn chưa hoàn thành. Khi ghi CV, không gọi các test
độc lập này là bằng chứng recovery end-to-end.

- [ ] Kafka server và topic hoạt động.
- [ ] Producer gửi JSON, consumer đọc được.
- [ ] Bronze có đúng Topic ID và offset mới nhất.
- [ ] Silver quality checks PASS, không trùng event_id.
- [ ] Gold load/validation PASS.
- [ ] Gold idempotency PASS.
- [x] Late-arriving event bước 23 PASS.
- [x] Watermark/window test riêng PASS.
- [ ] Checkpoint interruption bước 25: bỏ qua theo quyết định hiện tại.
- [x] Sáu file Gold cho dashboard đã xuất thành công.
