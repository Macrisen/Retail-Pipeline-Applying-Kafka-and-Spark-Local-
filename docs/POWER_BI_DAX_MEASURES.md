# Power BI DAX cho Retail Dashboard

Tên measure DAX được phép có khoảng trắng. Tạo từng measure bằng **New measure**,
dán một công thức mỗi lần. Không dán toàn bộ tài liệu vào một measure.

## Quan hệ cần có trước khi tạo biểu đồ

```text
dim_date[date_key]           1 → * order_payment_metrics[paid_date_key] (active)
dim_date[date_key]           1 → * fact_sales_events[event_date_key]    (active)
dim_customer[customer_key]   1 → * fact_orders[customer_key]
dim_customer[customer_key]   1 → * fact_sales_events[customer_key]
dim_product[product_key]     1 → * fact_sales_events[product_key]
fact_orders[order_id]        1 → 1 order_payment_metrics[order_id]
```

Đặt cross-filter của quan hệ `fact_orders` ↔ `order_payment_metrics` là **Both**
để slicer `city`, `device`, `payment_method` từ `fact_orders` lọc được các measure
tiền trong `order_payment_metrics`. Các quan hệ còn lại dùng **Single**.

## Calculated column (New column)

### Loại dữ liệu nghiệp vụ khỏi dữ liệu test

Snapshot Gold hiện có 74.850 đơn nghiệp vụ và 103 đơn test. Tạo ba calculated
column dưới đây, sau đó đặt cả ba thành **TRUE** trong Filters on all pages.

Trong `fact_orders`:

```DAX
Is Business Data =
LEFT(fact_orders[order_id], 7) = "ORD-000"
```

Trong `fact_sales_events`:

```DAX
Is Business Data =
LEFT(fact_sales_events[order_id], 7) = "ORD-000"
```

Trong `order_payment_metrics`:

```DAX
Is Business Data =
LEFT(order_payment_metrics[order_id], 7) = "ORD-000"
```

Các ID `ORD-LATE`, `ORD-TEST` và `RECOVERY-01` phục vụ kiểm thử pipeline nên
không đưa vào số liệu kinh doanh. Sau filter, payment nghiệp vụ trải từ
01/08/2026 đến 01/09/2026; chỉ có 8 payment rơi vào ngày 01/09 do đơn cuối tháng.
Không cần Year/Quarter slicer cho snapshot này vì toàn bộ dữ liệu vẫn thuộc Q3/2026.

### Cột hỗ trợ hiển thị

`Year Quarter` dưới đây chỉ để mở rộng sau này khi có nhiều quý; không dùng trong
dashboard hiện tại:

```DAX
Year Quarter =
FORMAT(dim_date[year_number], "0")
    & " Q"
    & FORMAT(dim_date[quarter_number], "0")
```

Tạo trong `dim_date` để hiển thị tuần trong tháng 8:

```DAX
Week of Month =
"Week " & FORMAT(INT((dim_date[day_of_month] - 1) / 7) + 1, "0")
```

Tạo trong bảng `fact_sales_events`:

```DAX
Producer Delay Minutes =
DATEDIFF(
    fact_sales_events[event_time_utc],
    fact_sales_events[producer_time_utc],
    MINUTE
)
```

## Page 1 — Sales Overview

```DAX
Total Orders =
DISTINCTCOUNT(fact_orders[order_id])
```

```DAX
Paid Orders =
CALCULATE(
    DISTINCTCOUNT(order_payment_metrics[order_id]),
    order_payment_metrics[has_payment] = "t"
)
```

```DAX
Gross Paid =
SUM(order_payment_metrics[gross_paid_vnd])
```

```DAX
Refunded Amount =
SUM(order_payment_metrics[refunded_vnd])
```

```DAX
Net Paid =
SUM(order_payment_metrics[net_paid_vnd])
```

```DAX
Average Order Value =
DIVIDE([Gross Paid], [Paid Orders], 0)
```

```DAX
Delivered Orders =
CALCULATE(
    DISTINCTCOUNT(fact_orders[order_id]),
    NOT ISBLANK(fact_orders[delivered_at_utc])
)
```

```DAX
Delivery Rate =
DIVIDE([Delivered Orders], [Total Orders], 0)
```

```DAX
Cancelled Orders =
CALCULATE(
    DISTINCTCOUNT(order_payment_metrics[order_id]),
    order_payment_metrics[is_cancelled] = "t"
)
```

```DAX
Cancellation Rate =
DIVIDE([Cancelled Orders], [Total Orders], 0)
```

## Page 2 — Product & Customer Analysis

```DAX
Unique Customers =
DISTINCTCOUNT(fact_orders[customer_key])
```

```DAX
Total Products =
DISTINCTCOUNT(dim_product[product_key])
```

```DAX
Sold Products =
CALCULATE(
    DISTINCTCOUNT(fact_sales_events[product_key]),
    fact_sales_events[event_type] = "PAYMENT_CONFIRMED"
)
```

```DAX
Units Sold =
CALCULATE(
    SUM(fact_sales_events[quantity]),
    fact_sales_events[event_type] = "PAYMENT_CONFIRMED"
)
```

```DAX
Product Gross Paid =
CALCULATE(
    SUM(fact_sales_events[order_total_vnd]),
    fact_sales_events[event_type] = "PAYMENT_CONFIRMED"
)
```

```DAX
Product Paid Orders =
CALCULATE(
    DISTINCTCOUNT(fact_sales_events[order_id]),
    fact_sales_events[event_type] = "PAYMENT_CONFIRMED"
)
```

```DAX
Average Quantity per Paid Order =
DIVIDE([Units Sold], [Product Paid Orders], 0)
```

```DAX
Customer Orders =
DISTINCTCOUNT(fact_orders[order_id])
```

```DAX
Customer Gross Paid =
[Gross Paid]
```

## Page 3 — Order & Streaming Operations

```DAX
Total Events =
DISTINCTCOUNT(fact_sales_events[event_id])
```

```DAX
Late Events =
CALCULATE(
    DISTINCTCOUNT(fact_sales_events[event_id]),
    fact_sales_events[is_late_event] = "t"
)
```

```DAX
Late Event Rate =
DIVIDE([Late Events], [Total Events], 0)
```

```DAX
Refunded Orders =
CALCULATE(
    DISTINCTCOUNT(order_payment_metrics[order_id]),
    order_payment_metrics[has_refund] = "t"
)
```

```DAX
Refund Rate =
DIVIDE([Refunded Orders], [Paid Orders], 0)
```

```DAX
Average Producer Delay Minutes =
AVERAGEX(
    FILTER(
        fact_sales_events,
        NOT ISBLANK(fact_sales_events[producer_time_utc])
    ),
    DATEDIFF(
        fact_sales_events[event_time_utc],
        fact_sales_events[producer_time_utc],
        MINUTE
    )
)
```

```DAX
Average Creation to Payment Hours =
AVERAGEX(
    FILTER(
        fact_orders,
        NOT ISBLANK(fact_orders[created_at_utc])
            && NOT ISBLANK(fact_orders[paid_at_utc])
    ),
    DATEDIFF(
        fact_orders[created_at_utc],
        fact_orders[paid_at_utc],
        MINUTE
    ) / 60.0
)
```

```DAX
Average Payment to Delivery Hours =
AVERAGEX(
    FILTER(
        fact_orders,
        NOT ISBLANK(fact_orders[paid_at_utc])
            && NOT ISBLANK(fact_orders[delivered_at_utc])
    ),
    DATEDIFF(
        fact_orders[paid_at_utc],
        fact_orders[delivered_at_utc],
        MINUTE
    ) / 60.0
)
```

```DAX
Paid Not Delivered Orders =
CALCULATE(
    DISTINCTCOUNT(fact_orders[order_id]),
    NOT ISBLANK(fact_orders[paid_at_utc]),
    ISBLANK(fact_orders[delivered_at_utc])
)
```

## Định dạng measure

Trong **Properties → Format**, đặt:

- `Gross Paid`, `Refunded Amount`, `Net Paid`, `Average Order Value`,
  `Product Gross Paid`, `Customer Gross Paid`: Currency hoặc custom VND,
  không cần số thập phân.
- `Delivery Rate`, `Cancellation Rate`, `Refund Rate`, `Late Event Rate`:
  Percentage, 1–2 decimal places.
- Các measure đếm: Whole number.
- Các measure thời gian: Decimal number, 1–2 decimal places.

## Hồ sơ dữ liệu thực tế đã kiểm tra

| Nội dung | Kết quả |
|---|---:|
| Đơn nghiệp vụ | 74.850 |
| Event nghiệp vụ | 299.400 |
| Phạm vi payment nghiệp vụ | 01/08–01/09/2026 |
| Đơn test ngoài dữ liệu chính | 103 |
| Payment nghiệp vụ ngày 01/09 | 8 |
| Đơn nghiệp vụ có nhiều product | 0 |

Sau khi loại dữ liệu test, các KPI chuẩn để đối chiếu là:

| KPI | Giá trị kỳ vọng |
|---|---:|
| Total Orders | 74.850 |
| Paid Orders | 74.850 |
| Delivered Orders | 71.293 |
| Cancelled Orders | 3.557 |
| Refunded Orders | 1.953 |
| Late Events | 5.896 |
| Gross Paid | 266.858.794.000 VND |
| Refunded Amount | 6.317.929.000 VND |
| Net Paid | 260.540.865.000 VND |

Dữ liệu chính tập trung trong tháng 8 và có 8 payment vào ngày 01/09, cùng thuộc
một quý và một năm. Vì vậy dashboard hiện tại phân tích theo **ngày**, **tuần**,
thành phố, danh mục, phương thức
thanh toán, thiết bị và traffic source. Year/Quarter chỉ có ích khi bổ sung thêm
dữ liệu lịch sử.

## Kiểm tra measure trước khi làm biểu đồ

Tạo một **Table visual**, thêm `dim_date[full_date]`, `[Gross Paid]`,
`[Refunded Amount]` và `[Net Paid]`. Nếu mỗi ngày hiện cùng một tổng, kiểm tra
lại active relationship:

```text
dim_date[date_key] → order_payment_metrics[paid_date_key]
```

Vì `paid_date_key` có thể trống cho đơn chưa thanh toán, các đơn đó không xuất hiện
trong phân tích doanh thu theo ngày; đây là hành vi đúng.

## Thiết kế Page 1 — Sales Overview

**Mục tiêu:** trả lời doanh thu bao nhiêu, thay đổi theo thời gian ra sao, tiền
hoàn ảnh hưởng thế nào và khu vực/phương thức thanh toán nào đóng góp nhiều nhất.

### Slicer phía trên

| Slicer | Field |
|---|---|
| Khoảng ngày | `dim_date[full_date]`, kiểu Between |
| Tuần | `dim_date[Week of Month]` |
| Thành phố | `fact_orders[city]` |
| Phương thức thanh toán | `fact_orders[payment_method]` |

### Hàng KPI

Tạo sáu **Card visual**:

| Card | Measure | Ý nghĩa |
|---|---|---|
| Total Orders | `[Total Orders]` | Tổng số đơn trong bộ lọc |
| Paid Orders | `[Paid Orders]` | Số đơn đã xác nhận thanh toán |
| Gross Paid | `[Gross Paid]` | Tổng tiền đã thu trước hoàn tiền |
| Refunded Amount | `[Refunded Amount]` | Tổng tiền đã hoàn |
| Net Paid | `[Net Paid]` | Tiền thu ròng sau hoàn |
| Average Order Value | `[Average Order Value]` | Giá trị trung bình mỗi đơn đã thanh toán |

### Visual 1 — Xu hướng doanh thu

Dùng **Line chart**:

```text
X-axis: dim_date[full_date]
Y-axis: [Gross Paid], [Net Paid]
Tooltips: [Paid Orders], [Refunded Amount]
```

Phân tích ngày có doanh thu cao/thấp và khoảng cách giữa Gross Paid với Net Paid.
Khoảng cách lớn nghĩa là hoàn tiền nhiều.

### Visual 2 — Đơn thanh toán theo ngày

Dùng **Clustered column chart**:

```text
X-axis: dim_date[full_date]
Y-axis: [Paid Orders]
Tooltips: [Gross Paid], [Average Order Value]
```

So sánh từng ngày trong tháng 8. Đối chiếu với đường doanh thu để biết ngày có
doanh thu cao là do số đơn nhiều hay do giá trị đơn trung bình lớn.

### Visual 3 — Doanh thu theo thành phố

Dùng **Clustered bar chart**:

```text
Y-axis: fact_orders[city]
X-axis: [Net Paid]
Tooltips: [Total Orders], [Average Order Value]
Sort: [Net Paid] descending
```

Phân tích thành phố tạo tiền thu ròng cao nhất. So sánh thêm số đơn và AOV để
biết doanh thu cao do nhiều đơn hay do giá trị đơn lớn.

### Visual 4 — Phương thức thanh toán

Dùng **Donut chart**:

```text
Legend: fact_orders[payment_method]
Values: [Paid Orders]
Tooltips: [Gross Paid], [Net Paid]
```

Phân tích tỷ trọng sử dụng của từng phương thức. Nếu cần so sánh chính xác hơn,
thay donut bằng bar chart.

### Bố cục Page 1

```text
[Title]                       [Date range] [Week] [City] [Payment]
[Orders] [Paid] [Gross] [Refund] [Net] [AOV]
[Revenue trend — rộng 2/3]          [Payment method]
[Paid orders by day]                [Revenue by city]
```

## Thiết kế Page 2 — Product & Customer Analysis

**Mục tiêu:** tìm danh mục/sản phẩm bán tốt, khách hàng có giá trị cao và kênh
tiếp cận mang lại nhiều đơn.

### Slicer phía trên

| Slicer | Field |
|---|---|
| Ngày | `dim_date[full_date]` |
| Danh mục | `dim_product[category]` |
| Thành phố | `fact_orders[city]` |
| Thiết bị | `fact_orders[device]` |
| Nguồn truy cập | `fact_orders[traffic_source]` |

### Hàng KPI

| Card | Measure |
|---|---|
| Unique Customers | `[Unique Customers]` |
| Sold Products | `[Sold Products]` |
| Units Sold | `[Units Sold]` |
| Product Gross Paid | `[Product Gross Paid]` |
| Avg Quantity/Order | `[Average Quantity per Paid Order]` |

### Visual 1 — Doanh thu theo danh mục

Dùng **Clustered bar chart**:

```text
Y-axis: dim_product[category]
X-axis: [Product Gross Paid]
Tooltips: [Units Sold]
Sort: [Product Gross Paid] descending
```

Phân tích danh mục tạo doanh thu thanh toán lớn nhất. Measure chỉ lấy event
`PAYMENT_CONFIRMED`, tránh cộng cùng một đơn ở mọi trạng thái.

### Visual 2 — Top 10 sản phẩm

Dùng **Clustered bar chart**:

```text
Y-axis: dim_product[product_id]
X-axis: [Units Sold]
Tooltips: dim_product[category], [Product Gross Paid]
Visual filter: Top N → Top 10 by [Units Sold]
```

Phân tích sản phẩm bán nhiều nhất và đối chiếu doanh thu để nhận ra sản phẩm bán
nhiều nhưng giá trị thấp hoặc bán ít nhưng giá trị cao.

### Visual 3 — Đơn hàng theo nguồn truy cập

Dùng **Clustered bar chart**:

```text
Y-axis: fact_orders[traffic_source]
X-axis: [Total Orders]
Tooltips: [Gross Paid], [Average Order Value]
```

Phân tích nguồn nào mang lại nhiều đơn và nguồn nào có giá trị đơn cao.

### Visual 4 — Đơn hàng theo thiết bị

Dùng **Donut chart**:

```text
Legend: fact_orders[device]
Values: [Total Orders]
Tooltips: [Gross Paid]
```

### Visual 5 — Top khách hàng

Dùng **Table visual**:

```text
dim_customer[customer_id]
[Customer Orders]
[Customer Gross Paid]
```

Đặt visual filter **Top N → Top 10 by `[Customer Gross Paid]`**, rồi sort giảm
dần. Bảng này xác định khách hàng có giá trị cao.

### Giới hạn phân tích sản phẩm

Dữ liệu hiện tại phù hợp khi một order/payment đại diện cho một product. Nếu sau
này một đơn có nhiều dòng sản phẩm, cần fact order-item riêng để phân bổ doanh
thu chính xác; không dùng `fact_orders[order_total_vnd]` cho từng sản phẩm.

### Bố cục Page 2

```text
[Title]                   [Date] [Category] [City] [Device] [Source]
[Customers] [Sold Products] [Units] [Product Gross] [Avg Qty]
[Revenue by category]          [Top 10 products]
[Traffic source] [Device]      [Top customers table]
```

## Thiết kế Page 3 — Order & Streaming Operations

**Mục tiêu:** theo dõi vòng đời đơn hàng, tỷ lệ giao/hủy/hoàn và chất lượng dữ
liệu streaming như late event và producer delay.

### Slicer phía trên

| Slicer | Field |
|---|---|
| Ngày sự kiện | `dim_date[full_date]` |
| Event type | `fact_sales_events[event_type]` |
| Thành phố | `fact_orders[city]` |
| Trạng thái mới nhất | `fact_orders[latest_event_type]` |
| Late event | `fact_sales_events[is_late_event]` |

### Hàng KPI

| Card | Measure |
|---|---|
| Delivery Rate | `[Delivery Rate]` |
| Cancellation Rate | `[Cancellation Rate]` |
| Refund Rate | `[Refund Rate]` |
| Late Events | `[Late Events]` |
| Avg Producer Delay | `[Average Producer Delay Minutes]` |
| Avg Payment → Delivery | `[Average Payment to Delivery Hours]` |

### Visual 1 — Số lượng theo event type

Dùng **Clustered column chart**:

```text
X-axis: fact_sales_events[event_type]
Y-axis: [Total Events]
Tooltips: [Late Events], [Late Event Rate]
```

Kiểm tra phân bố các giai đoạn của đơn. Chênh lệch lớn giữa payment và delivery
có thể cho thấy nhiều đơn chưa hoàn tất.

### Visual 2 — Trạng thái mới nhất của đơn

Dùng **Clustered bar chart**:

```text
Y-axis: fact_orders[latest_event_type]
X-axis: [Total Orders]
Sort: [Total Orders] descending
```

Phân tích backlog: nhiều đơn dừng ở created, paid hoặc shipped cần được chú ý.

### Visual 3 — Late event theo ngày

Dùng **Line chart**:

```text
X-axis: dim_date[full_date]
Y-axis: [Late Events]
Tooltips: [Total Events], [Late Event Rate]
```

Visual này dùng relationship theo `fact_sales_events[event_date_key]`. Theo dõi
ngày có tỷ lệ late event tăng bất thường.

### Visual 4 — Thời gian giao hàng theo thành phố

Dùng **Clustered bar chart**:

```text
Y-axis: fact_orders[city]
X-axis: [Average Payment to Delivery Hours]
Tooltips: [Delivered Orders], [Delivery Rate]
Sort: [Average Payment to Delivery Hours] descending
```

Thành phố có thời gian cao hơn cần xem xét vận chuyển. Measure chỉ tính đơn có
cả thời gian thanh toán và giao hàng.

### Visual 5 — Các event trễ nhất

Dùng **Table visual**:

```text
fact_sales_events[order_id]
fact_sales_events[event_type]
fact_sales_events[event_time_utc]
fact_sales_events[producer_time_utc]
fact_sales_events[Producer Delay Minutes]
fact_sales_events[is_late_event]
```

Sort `Producer Delay Minutes` giảm dần và đặt filter
`is_late_event = TRUE`. Đây là bảng điều tra lỗi/độ trễ của pipeline.

### Visual 6 — Funnel vòng đời đơn hàng

Dùng **Funnel chart**:

```text
Category: fact_sales_events[event_type]
Values: DISTINCTCOUNT of fact_sales_events[order_id]
```

Funnel chỉ có ý nghĩa rõ nếu sắp event theo trình tự nghiệp vụ. Nếu Power BI sắp
theo giá trị, dùng bar chart để tránh người xem hiểu nhầm thứ tự.

### Bố cục Page 3

```text
[Title]                    [Date] [Event] [City] [Status] [Late]
[Delivery%] [Cancel%] [Refund%] [Late] [Delay] [Delivery Hours]
[Event type counts]             [Latest order status]
[Late event trend]              [Delivery time by city]
[Late-event detail table — full width]
```

## Quy tắc đọc dashboard

- Page 1 dùng để đánh giá tiền và xu hướng bán hàng.
- Page 2 dùng để tìm sản phẩm, khách hàng và kênh có hiệu quả cao.
- Page 3 dùng để theo dõi vận hành đơn hàng và chất lượng streaming.
- Một slicer chỉ lọc đúng visual khi relationship tạo được đường truyền filter.
- Dùng `order_payment_metrics` cho Gross/Refund/Net; không cộng tiền trên tất cả
  event trong `fact_sales_events`.
