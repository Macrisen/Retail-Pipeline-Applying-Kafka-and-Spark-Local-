# Kafka → Spark → Bronze trên Supabase

Luồng này đọc topic chính `retail-order-events`. Topic v2 chỉ để thử partition.
Không chạy producer lại: các message hiện có trong Kafka sẽ được đọc từ đầu
khi luồng ghi Bronze chưa có checkpoint. `bronze.raw_sales`, Silver và Gold giữ nguyên.

## 1. Tạo bảng mới

Mở `sql/11_create_kafka_bronze.sql`, copy toàn bộ vào Supabase SQL Editor và Run
bằng role sở hữu database (thường là postgres).

Bảng `bronze.kafka_sales_events` giữ key/value dạng BYTEA để bảo toàn byte gốc,
kể cả JSON lỗi, ký tự không hợp lệ và message có value NULL. Chuyển JSON thành
cột, cast kiểu dữ liệu và loại trùng event_id sẽ thực hiện ở bước Silver sau.
Query Silver cũ chưa tự đọc bảng này: cần bổ sung bước ánh xạ nguồn sau khi nạp đạt.

## 2. Chuẩn bị Python

Tại thư mục gốc dự án:

```bash
python3 -m venv .local/bronze-venv
.local/bronze-venv/bin/python -m pip install -r streaming/requirements-bronze.txt
export PYSPARK_PYTHON="$PWD/.local/bronze-venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
```

Spark được cung cấp bởi `spark-submit`, không cần cài thêm pyspark vào venv.

## 3. Cấu hình kết nối trong terminal sẽ chạy Spark

Trong Supabase, mở **Connect → Session pooler**, lấy Host, Port và User thực tế.
Session pooler thường dùng port 5432; không dùng anon key/service_role key làm mật khẩu.
Thay hai giá trị placeholder dưới bằng thông tin của bạn:

```bash
export PGHOST='HOST_TRONG_CONNECT'
export PGPORT='5432'
export PGDATABASE='postgres'
export PGUSER='USER_TRONG_CONNECT'
export PGSSLMODE='require'
read -s 'PGPASSWORD?Nhập database password: '
export PGPASSWORD
```

Lệnh `read` trên dành cho zsh trên máy bạn. Mật khẩu nhập ẩn, không lưu vào code
hay lịch sử lệnh. Không dán password hoặc connection string có password vào chat.
Dùng owner/backend role có quyền với bảng; bảng bật RLS và không cấp quyền public API.

Nguồn hướng dẫn kết nối: https://supabase.com/docs/guides/database/connecting-to-postgres

## 4. Kiểm tra Kafka, rồi ghi thử tối đa 1.000 message

```bash
brew services start kafka
kafka-topics --bootstrap-server localhost:9092 --describe --topic retail-order-events
```

Luồng chấp nhận topic chính hiện có 1 partition. Nó không yêu cầu v2 hoặc 3 partition,
không tự tạo topic, và kiểm tra Topic ID cùng số partition trước mỗi lần ghi.
Không xóa/tạo lại topic, đổi số partition hoặc chạy nhiều writer cùng checkpoint
trong lúc nạp. Không cần chạy `read_kafka.py` song song để nạp Bronze.

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  streaming/kafka_to_bronze.py --mode preview
```

Preview ghi thật tối đa 1.000 message vào bảng mới rồi thoát. Không tạo checkpoint
streaming và không đánh dấu bỏ qua các dòng ngoài mẫu. Mẫu không bảo đảm thứ tự
toàn cục giữa nhiều partition. Chạy lại cùng mẫu: inserted=0 nếu đã được ghi hết.
Mỗi batch báo `committed` chỉ sau khi database commit thành công.

Trong Supabase SQL Editor:

```sql
SELECT source_topic_id, topic, partition_id,
       count(*) AS rows, min(kafka_offset) AS first_offset,
       max(kafka_offset) AS last_offset
FROM bronze.kafka_sales_events
GROUP BY source_topic_id, topic, partition_id
ORDER BY source_topic_id, partition_id;

-- CSV producer gửi UTF-8 nên có thể decode để xem; byte không hợp lệ sẽ báo lỗi.
SELECT kafka_offset, convert_from(message_key, 'UTF8') AS order_key,
       convert_from(raw_value, 'UTF8') AS raw_json
FROM bronze.kafka_sales_events
ORDER BY ingested_at DESC LIMIT 5;

SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
```

Kiểm tra dung lượng dự án trước khi ghi toàn bộ: bảng mới lưu thêm message ngoài
dữ liệu batch cũ. Con số trước đây trong kế hoạch là 457 MB, không phải dung lượng
hiện tại đã xác nhận. Đối chiếu giới hạn thực tế trên dashboard Supabase.

## 5. Nạp hết dữ liệu đang có

Sau khi mẫu đạt, dùng cùng terminal đã cấu hình kết nối:

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  streaming/kafka_to_bronze.py --mode backfill
```

Spark dùng availableNow, tối đa 1.000 message/batch, đọc phần dữ liệu khả dụng
tại lúc bắt đầu rồi tự thoát. Các dòng preview đã có không được ghi thêm lần nữa.
Khóa duy nhất: source_cluster + Topic ID + partition + offset. Sự kiện trùng
event_id nhưng khác offset vẫn được giữ ở Bronze.

Checkpoint tự chọn theo nguồn và database đích trong `.local/checkpoints/kafka-bronze-*`.
Không dùng lại checkpoint reader chỉ hiển thị trước đó. Dùng cùng checkpoint khi
khởi động lại sau lỗi. Nếu xóa dữ liệu trong bảng đích, checkpoint không tự biết
để nạp bù; cần kế hoạch replay riêng, không xóa bảng khi đang vận hành.

Với topic chính chứa đúng 300.000 message còn lưu, kết quả mong đợi là 300.000
dòng cho Topic ID đó. Kiểm tra số dòng thực tế; không suy từ số dòng CSV nếu topic
đã được gửi thêm, xóa/tạo lại hoặc hết thời gian lưu dữ liệu.

## 6. Chạy liên tục sau khi backfill đạt

Dùng lại lệnh trên, thay `--mode backfill` bằng `--mode stream`.
Checkpoint giữ nguyên nên chỉ đọc tiếp dữ liệu chưa hoàn tất. Nhấn Ctrl+C để dừng.
Không cần bật Kafbat UI để luồng hoạt động. Khi xong:

```bash
unset PGPASSWORD
```

## Phạm vi bảo đảm và kiểm thử

Mỗi batch COPY vào bảng tạm, kiểm tra xung đột rồi INSERT ON CONFLICT DO NOTHING
trong một transaction. Nếu ghi thất bại, lỗi truyền về Spark và transaction rollback.
Nếu database commit rồi Spark chưa lưu checkpoint, lần thử lại bỏ qua tọa độ đã có.
Nếu cùng tọa độ có payload khác, dừng để kiểm tra thay vì âm thầm bỏ qua.

Topic ID phân biệt topic bị xóa/tạo lại; source_cluster phải là nhãn ổn định và riêng
cho mỗi Kafka cluster. Cấu hình này dành cho Kafka local, một Spark driver ghi tuần tự
vào PostgreSQL; chưa phải thiết kế throughput lớn phân tán.

Chạy test database tạm khi cần (cần PostgreSQL local và psycopg):

```bash
.local/bronze-venv/bin/python tests/test_kafka_bronze.py
```

Test không kết nối Kafka hoặc Supabase. Luồng Kafka → Spark → Supabase cần được
xác nhận qua preview trên môi trường của bạn; chưa tự nạp dữ liệu cloud.
