# SQL của Retail Medallion Pipeline

## Dashboard 
[![Xem báo cáo PDF](./dataengineerdasboard(1).pdf)
## Chọn một trong hai nguồn Bronze

**Luồng chính hiện tại: Kafka → PostgreSQL local.** Trên database local mới,
chạy `01_create_schemas.sql` → `11_create_kafka_bronze.sql` →
`13_kafka_bronze_compat_view.sql` → `03_create_silver.sql` →
`05_silver_quality_checks.sql` → `06_review_gold_schema.sql` →
`07_install_gold_loader.sql`. File 05 chỉ **cài validator**, nên chạy được khi
Silver còn rỗng. Sau khi Silver có dữ liệu, gọi validator riêng hoặc chạy 08.

Sau khi cài đặt, `streaming/kafka_to_bronze.py` ghi raw message vào
`bronze.kafka_sales_events` và tự chạy `12_kafka_bronze_to_silver.sql` →
`08_load_and_validate_gold.sql` cho mỗi batch có message. Vì 08 gọi loader từ
07, và loader gọi validator từ 05, không cần chạy lại 05/06/07 sau mỗi batch.

**Luồng tái tạo batch CSV:** `01` → `02` → nhập `raw_sales.csv` một lần → `03`
→ `04` → `05` → `06` → `07` → `08`; chạy `09` khi muốn kiểm tra idempotency.
Trong luồng này `bronze.raw_sales` là **bảng**. Trong luồng Kafka, file 13 tạo
**view cùng tên** từ inbox Kafka. Không chạy `02` và `13` trên cùng database.

### Lần dựng Kafka database từ trống

1. Chạy nguyên các file `01`, `11`, `13`, `03`, `05`, `06`, `07` theo thứ tự.
2. Khởi động Kafka/Spark; Python tự chạy `12` rồi `08` cho batch đầu tiên.
3. Sau khi có dữ liệu, gọi `CALL silver.prepare_gold_source();` để kiểm tra
   Silver độc lập nếu cần; chạy `09` để kiểm tra Gold chạy lại không đổi.

`08` yêu cầu các procedure từ 05/07 đã tồn tại. Với database local đang chạy,
không dựng lại cấu trúc và không nhập CSV lần nữa. Xem
`docs/TESTING_GUIDE_VI.md` cho các lệnh kiểm tra.

### Thông báo `skipping`

`NOTICE: schema "pg_temp" does not exist, skipping` hoặc
`NOTICE: table "retail_expected_events" does not exist, skipping` đến từ
`DROP TABLE IF EXISTS` trong 05/07 khi bảng tạm chưa tồn tại. Đây là thông báo
bình thường; xác nhận thành công bằng `Silver PASS`, `Gold PASS` và `COMMIT`.
`ERROR`, transaction aborted hoặc thiếu `Gold PASS` thì phải kiểm tra nguyên
nhân. Giữ thao tác dọn bảng tạm để procedure an toàn khi được gọi nhiều lần
trong cùng transaction.

### Chính sách correction và chi phí refresh hiện tại

`04` và `12` dùng `ON CONFLICT (event_id) DO NOTHING`: event đã có trong
Silver không được cập nhật. Nếu Bronze chứa cùng `event_id` với nội dung khác,
validator 05 sẽ từ chối nạp Gold. **Chính sách hiện tại là phát hiện và dừng để
điều tra correction**, chưa hỗ trợ tự sửa event. Không đổi sang UPSERT khi chưa
có quy tắc chọn phiên bản và bài test tương ứng.

Mỗi batch Kafka hiện đọc lại toàn bộ inbox Bronze để đối chiếu Silver, rồi
Gold đối chiếu lại toàn bộ fact. Đây là giới hạn hiệu năng đã biết; chưa có
incremental refresh. Ghi Bronze, nạp Silver và nạp Gold cũng là các transaction
riêng. Nếu Gold lỗi sau khi Silver commit, xử lý lỗi rồi chạy lại 08.

### File ngoài đường chạy thường xuyên

- `00_inspect_schema.sql`: kiểm tra schema khi chẩn đoán.
- `09_test_gold_idempotency.sql`: bài test sau khi Gold đã nạp.
- `10_remove_duplicate_gold_indexes.sql`: migration dọn index trùng đã dùng
  một lần trên database cũ; giữ làm lịch sử, không chạy theo mỗi batch.
- `14_export_gold_dashboard.sql`: xuất snapshot CSV cho Power BI khi cần refresh.

## Ghi chú nhánh batch/Supabase cũ

Các script dùng **bronze.raw_sales**, **silver.sales_events** và **gold.*** theo bản kế hoạch và DDL đã cung cấp. Chúng không chuyển bảng sang `public`, không xóa dữ liệu Bronze/Silver và không kết nối Supabase tự động.

Hai CSV export cung cấp tên cột và giá trị mẫu, không cung cấp datatype/constraint thực trên server. Chạy `00_inspect_schema.sql` nếu muốn đối chiếu server trước. Silver phải có các cặp `event_date_utc`/`event_time_utc`, `producer_date_utc`/`producer_time_utc`, `loaded_date_utc`/`loaded_time_utc`. Ngày là chuỗi hoặc số **YYYYDDMM**, giờ chuyển được sang `TIME`.

## Chạy nhánh batch trong SQL Editor

Dùng role sở hữu các schema/bảng (thường là `postgres` trong SQL Editor). Mở từng file, copy toàn bộ vào SQL Editor và Run theo thứ tự. Chỉ tiếp tục khi file trước chạy thành công.

| File | Mục đích |
|---|---|
| `05_silver_quality_checks.sql` | Cài procedure kiểm tra Silver/Bronze; chưa gọi kiểm tra hoặc nạp Gold. |
| `06_review_gold_schema.sql` | Tạo bảng nếu chưa có; bảo đảm khóa sản phẩm `(product_id, category)` và timestamp Gold đúng DDL. |
| `07_install_gold_loader.sql` | Cài procedure nạp Gold và view số liệu thanh toán. Chưa nạp dữ liệu. |
| `08_load_and_validate_gold.sql` | Nạp dim trước fact, đối chiếu toàn bộ fact với nguồn trong cùng transaction; trả số dòng và tổng tiền. |
| `09_test_gold_idempotency.sql` | Gắn trigger kiểm tra trong transaction, chạy loader lại; chặn INSERT/DELETE/TRUNCATE hoặc UPDATE thay đổi giá trị trên cả 5 bảng, rồi rollback lần thử. |

Nếu transaction báo lỗi và phiên đang ở trạng thái aborted, chạy `ROLLBACK;` trước khi sửa nguyên nhân rồi chạy lại. Không dùng `DROP TABLE` hoặc `TRUNCATE` để bỏ qua lỗi.

Sau khi cài đặt, chỉ cần chạy lại file **08** để refresh Gold. File **09** phải chạy sau lần nạp thành công; thông báo mong đợi là `PASS: all five Gold tables unchanged after rerun`.

## Chuyển đổi và quy tắc

- Silver giữ nguyên. Ghép `YYYYDDMM` + `TIME` thành timestamp UTC cho Gold. Ví dụ `20262908` + `15:16:45` → `2026-08-29 15:16:45+00`.
- `make_date` kiểm tra ngày thực, không tự sửa ngày sai. `dim_date.date_key` là `YYYYMMDD`, `date_label` vẫn là `YYYYDDMM`; ngày được tạo liên tục từ ngày sự kiện sớm nhất tới ngày sự kiện/producer muộn nhất.
- `dim_customer`: một dòng/customer_id. `dim_product`: một dòng/(product_id, category). Các khóa surrogate hiện có được giữ nguyên.
- `fact_sales_events`: một dòng/event_id. `fact_orders`: một dòng/order_id, giữ cả đơn chưa thanh toán.
- Mốc tạo/thanh toán/gửi/giao dùng event-time sớm nhất của loại sự kiện tương ứng; mốc không có là NULL.
- Trạng thái cuối lấy event-time lớn nhất; nếu bằng nhau dùng producer-time rồi event_id làm tiêu chí phụ ổn định. Đây là tie-break kỹ thuật, không phải thứ tự ưu tiên nghiệp vụ.
- Loader cập nhật fact theo khóa chính; không tự xóa các dòng Gold không còn trong Silver. Nếu gặp dòng dư, phép đối chiếu làm rollback và yêu cầu kiểm tra nguồn.
- Chỉ một loader này chạy đồng thời nhờ advisory lock; không hỗ trợ các writer ngoài pipeline cùng sửa Gold. Procedure giữ khóa `SHARE` trên Bronze/Silver đến cuối transaction để nguồn ổn định; các thao tác ghi vào hai bảng này sẽ chờ load hoàn tất. Bài test chạy lại cũng khóa ghi Gold trong lúc so sánh. Không đổi isolation level trong SQL Editor vì phiên có thể đã chạy query trước đó.

## Kiểm tra chất lượng

Dừng trước khi nạp khi Silver rỗng, trùng event_id, thiếu mã bắt buộc, giá trị số NULL/âm không hợp lệ, timestamp thiếu/sai, producer-time trước event-time, loại sự kiện ngoài danh sách, thuộc tính một đơn mâu thuẫn, hoặc có nhiều payment/refund cho một đơn.

Đối chiếu **toàn bộ 17 trường nghiệp vụ** Silver với Bronze sau bỏ bản sao giống nhau và chuẩn hóa promo_code. Vì vậy còn phát hiện sai ngày khi ghép, mất sự kiện hoặc bản sao Bronze cùng event_id nhưng khác nội dung. `promo_code` rỗng hoặc chuỗi literal `null` được coi là SQL NULL; các mã/categorical còn lại phải khớp nguồn. Các phép chuẩn hóa bổ sung có chủ ý trong Silver cần được thêm vào quy tắc đối chiếu Bronze trước khi nạp.

Các con số 300.000 / 299.400 / 74.850 là mốc của CSV hiện tại, không được hard-code làm điều kiện chạy production. Dữ liệu mới hợp lệ vẫn nạp được khi Bronze và Silver đã đồng bộ.

## Tiền thanh toán và hoàn tiền

`gold.order_payment_metrics` trả một dòng/đơn:

- `gross_paid_vnd`: giá trị đơn nếu có `PAYMENT_CONFIRMED`, ngược lại 0.
- `refunded_vnd`: toàn bộ giá trị đơn nếu có `REFUND_ISSUED`, ngược lại 0.
- `net_paid_vnd`: gross trừ refund.
- `is_cancelled`: có `ORDER_CANCELLED` hoặc `CANCEL_ACKNOWLEDGED`.

Đây là số liệu dòng tiền theo mô hình synthetic, không mặc định là doanh thu kế toán. Giả định hoàn tiền toàn bộ, tối đa một payment và một refund cho một đơn. Hủy chưa hoàn tiền không tự trừ tiền đã nhận. Nếu có partial refund, nhiều lần thanh toán hoặc muốn ghi nhận doanh thu khi giao hàng, phải đổi quy tắc và mô hình trước.

Không cộng `order_total_vnd` trên mọi event. `paid_date_key` là ngày thanh toán gốc; không dùng nó để báo cáo ngày phát sinh hoàn tiền (cần lấy timestamp của refund event).

## Kiểm thử local

`python3 tests/test_gold_pipeline.py` dùng bộ PostgreSQL đã cài, tạo cluster tạm trong `/private/tmp`, không mở TCP, và tự dừng/xóa sau bài test. Cần quyền chạy PostgreSQL nếu môi trường sandbox chặn shared memory/socket.

Fixture Bronze lấy toàn bộ `raw_sales.csv`; Silver test được tạo theo cấu trúc mẫu ngày/giờ đã gửi. Bài test kiểm tra lần nạp đầu, chạy lại không đổi cả 5 bảng, tổng payment/refund độc lập từ nguồn, ngày UTC cụ thể, từ chối duplicate/ngày sai/số NULL/mất dòng, sự kiện đến muộn không làm lùi trạng thái và cài script lần hai.

Kết quả local không thay thế việc chạy trên Supabase: các CSV export là mẫu, chưa xác nhận dữ liệu và ràng buộc đầy đủ trên server.

## Cập nhật loader: bỏ qua UPDATE không cần thiết

File 07 dùng `WHERE ROW(...) IS DISTINCT FROM ROW(...)` cho `dim_date`,
`fact_sales_events` và `fact_orders`. Các cột được so sánh có xử lý NULL;
loader chỉ UPDATE khi ít nhất một giá trị khác. Dữ liệu mới vẫn INSERT,
khóa cũ có dữ liệu thay đổi vẫn UPDATE. Các dim customer/product đã bỏ qua
business key tồn tại từ trước.

Copy lại toàn bộ file 07 vào Supabase và chạy để thay procedure. Việc cài
procedure không nạp lại Gold. Bản sửa giảm UPDATE thừa, không thu hồi dung lượng
đã chiếm trước đó và không loại bỏ các bảng tạm của loader/file 09.

## File 09 không tạo bản sao Gold

File 09 hiện dùng trigger trong transaction để kiểm tra từng thay đổi thực tế,
không dùng checksum hoặc bản sao JSON. Mọi INSERT/DELETE/TRUNCATE hoặc UPDATE
làm đổi giá trị (kể cả khóa dim) đều làm test thất bại. Kiểm tra này nghiêm hơn
so sánh trạng thái cuối: đổi rồi đổi ngược lại cũng thất bại.

Chạy toàn bộ file, bao gồm ROLLBACK cuối cùng để gỡ trigger thử nghiệm.
Nếu lỗi làm transaction aborted trong phiên còn mở, chạy ROLLBACK.
Cần quyền tạo trigger trên Gold. Procedure 07 mới phải được cài trước.
Bản sửa chỉ loại bỏ hai bản sao Gold; staging của loader vẫn dùng tài nguyên,
không bảo đảm tránh mọi giới hạn hoặc timeout của Supabase.

## Scripts Bronze/Silver đã lưu từ SQL người dùng cung cấp

- `01_create_schemas.sql`: tạo bronze, silver, gold.
- `02_create_bronze.sql`: tạo raw_sales trực tiếp trong bronze, giữ thứ tự cột CSV.
- `03_create_silver.sql`: tạo cấu trúc cuối cùng, ngày CHAR(8) YYYYDDMM và giờ TIME,
  thay vì tạo timestamp rồi ALTER/UPDATE toàn bộ bảng.
- `04_bronze_to_silver.sql`: ghép thao tác clean/deduplicate với tách ngày/giờ ngay
  khi INSERT. Giữ quy tắc ON CONFLICT(event_id) DO NOTHING của query gốc.

Thứ tự dựng database thử nghiệm trống: 01 → 02 → nhập CSV một lần → 03 → 04
→ 05 → 06 → 07 → 08 → 09. 08 tự gọi validator từ 05. Không cần chạy lại các
bước này trên Supabase hiện tại.

Để nhập CSV bằng psql, mở kết nối tới database thử nghiệm từ thư mục gốc dự án,
kiểm tra `SELECT count(*) FROM bronze.raw_sales;` bằng 0 rồi chạy dòng meta-command:

```text
\copy bronze.raw_sales FROM 'raw_sales.csv' WITH (FORMAT csv, HEADER true)
```

Đây là lệnh psql, không chạy trong SQL Editor. Nó giữ nguyên cả bản sao trong CSV.
Chỉ nhập một lần cho bài test rebuild; chạy lại lệnh này sẽ thêm 300.000 dòng Bronze.
Lần thử lại transformation giữ nguyên Bronze và chạy 04, 05, 08, 09. Đây chưa phải
cơ chế nhập file Bronze idempotent: cần thêm batch/file tracking nếu muốn tự động
chạy lại cả bước nhập nguồn.

Ngày/giờ loaded dùng default khi INSERT bản ghi mới; khi chạy 04 lần hai, giá trị
loaded cũ không đổi. Query gốc chỉ chuẩn hóa promo_code rỗng thành NULL; file 04 giữ
nguyên quy tắc đó. Validator 05 còn chấp nhận chuỗi literal `null` từ export.

Nếu các dòng có cùng event_id nhưng payload khác nhau, file 04 giữ dòng producer
mới nhất như query gốc; nếu producer-time cũng bằng nhau, lựa chọn không được bảo
đảm. File 05 sẽ chặn các event_id mâu thuẫn từ Bronze trước khi nạp Gold. Dữ liệu
CSV hiện tại chỉ có bản sao giống nhau. Event đã tồn tại trong Silver không tự
được sửa bởi file 04; nếu nguồn có correction, cần quy tắc UPSERT riêng.

## Lịch sử chuyển đổi đã cung cấp

DDL gốc được lưu tại `docs/history/supabase_original_ddl.txt` để đối chiếu.
Các thao tác thủ công sau thuộc lịch sử, không nằm trong quy trình rebuild:

- Chuyển bảng có tên literal `public."bronze.raw_sales"` sang schema bronze rồi
  đổi tên thành raw_sales.
- Xóa `public.raw_sales` cũ.
- ALTER timestamp Silver thành TIME, lấy ngày/giờ lại từ Bronze.
- Tách loaded_at thành loaded_date_utc/loaded_time_utc rồi bỏ loaded_at.

Không chạy lại lệnh DROP TABLE hoặc migration lịch sử trên database đang dùng.
Các script mới tạo thẳng cấu trúc đích, không cần bảng cũ hoặc bước UPDATE khôi phục ngày.
