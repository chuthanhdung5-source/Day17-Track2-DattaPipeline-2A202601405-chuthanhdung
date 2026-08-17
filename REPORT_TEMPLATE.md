# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Cù Thành Dũng  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 22.4s
  run 2/3 … 22.0s
  run 3/3 … 21.6s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` bị nhân bản dữ liệu sau mỗi lần re-run (`38,750` hàng thay vì `12,480` hàng kỳ vọng), làm sai lệch phân bố dữ liệu huấn luyện. |
| **Nguyên nhân** | Incremental model `gold_training_set` không được khai báo `unique_key`. Khi thiếu `unique_key`, dbt mặc định generate ra câu lệnh `INSERT INTO`; do đó mỗi lượt chạy lại cùng một ngày/partition sẽ chèn nối tiếp dữ liệu thay vì ghi đè (`MERGE`), làm mất tính idempotent. Đồng thời Airflow DAG đặt `catchup=True` và không giới hạn `max_active_runs` khiến việc rerun bị kích hoạt song song. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`.<br>2. `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (FAIL) · sau: 12,480 hàng (✓ ok) · checksum 3 lượt: `8622572a97` (✓ trùng khớp 100%). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` chỉ thu được 8,645 hàng thay vì 9,100 hàng (thiếu 455 cặp `(event_date, customer_id)` ở các ngày quá khứ). |
| **P99 độ trễ đo được** | **3 ngày** |
| **Lookback đã chọn** | 3 ngày — vì phân bố độ trễ P99 của `bronze_events` cho thấy 99% các sự kiện đến muộn lọt vào kho trong vòng 3 ngày kể từ khi phát sinh. |
| **Nguyên nhân** | Điều kiện incremental ban đầu chỉ dùng `where event_date > (select max(event_date) from {{ this }})`. Khi một event xảy ra ở ngày quá khứ (ví dụ `event_date = 08-12`) nhưng bị trễ và được nạp ở ngày `08-15`, lúc này `max(event_date)` trong Gold đã là `08-14`, khiến điều kiện `event_date > max(event_date)` loại bỏ hoàn toàn các event đến muộn này. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_feature_daily.sql`: Nới rộng window thành `where event_date >= (select max(event_date) - interval '3' day from {{ this }})`.<br>2. Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để ghi đè dữ liệu khi tính toán lại lookback window. |
| **Bằng chứng** | trước: 8,645 hàng (thiếu 455 hàng) · sau: 9,100 hàng (✓ ok, 14 ngày × 650 customers). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời:** `max` độ trễ có thể bị kéo dài bởi các outlier cực đoan (ví dụ lỗi mạng đứt kết nối cả tháng). Nếu chọn lookback theo `max`, mọi lượt chạy daily sau này đều phải tính lại toàn bộ lịch sử nhiều tuần/tháng, làm tốn chi phí tính toán (CPU/IO) và kéo dài thời gian chạy đường ống. Chọn **P99** là sự đánh đổi tối ưu: bao phủ được 99% dữ liệu trễ thực tế với chi phí tài nguyên cố định chỉ là 3 ngày lookback.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6,606 bản ghi sai (`NULL` hoặc số ngoài 1..4), `quarantine_tickets` có 0 hàng thay vì 312 hàng. |
| **Nguyên nhân** | Phía backend nâng cấp schema evolution từ ngày 2026-08-10 sang gửi nhãn chuỗi (`urgent`, `high`, `medium`, `low`). Biểu thức `try_cast(priority_raw as integer)` ban đầu vừa biến nhãn chuỗi hợp lệ thành `NULL` (gây mất dữ liệu tốt), vừa bỏ sót các số rác ngoài khoảng contract (`0`, `5`, `-1`). Bảng quarantine chưa có điều kiện lọc (`where false`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`'1'`, `'2'`, `'3'`, `'4'`): Giữ nguyên, ép về số integer 1..4.<br>2. **Nhãn chữ (Schema Evolution)** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Map tương ứng về `1`, `2`, `3`, `4`.<br>3. **Giá trị hỏng** (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `null`): Ép về `NULL` để đẩy sang quarantine. |
| **Cách khắc phục** | 1. `dbt/macros/normalize_priority.sql`: Viết khối `CASE` xử lý đúng 3 nhóm giá trị.<br>2. `dbt/models/silver/silver_tickets.sql`: Lọc bản ghi rác (`WHERE normalize_priority IS NOT NULL`) **trước khi** dùng `row_number()` xếp hạng CDC để không làm mất ticket.<br>3. `dbt/models/silver/quarantine_tickets.sql`: Thêm `WHERE normalize_priority IS NULL`.<br>4. `dbt/models/silver/schema.yml`: Đặt `enforced: true` và thêm test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (✓ ok) · `dbt test` 11/11 pass (✓ ok) · `silver_tickets` đủ 12,480 ticket. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời:** 
> 1. **Nên xử lý ở tầng Silver**: Bronze đóng vai trò là "Data Lake / Landing Zone" lưu trữ trung thực mọi dữ liệu thô (raw & immutable). Nếu chặn ở Bronze, dữ liệu lỗi sẽ bị hủy bỏ hoàn toàn trước khi vào kho, làm mất dấu vết kiểm toán (auditability) và không thể điều tra nguyên nhân sự cố.
> 2. **Không để pipeline dừng**: Trong môi trường sản xuất, vài trăm bản ghi lỗi không nên có quyền dừng cả đường ống dữ liệu và làm gián đoạn việc phục vụ cho hàng trăm nghìn bản ghi hợp lệ khác (fault-tolerant pattern). Cơ chế Quarantine giúp cách ly riêng các bản ghi lỗi để đội vận hành xử lý sau, trong khi pipeline vẫn tiếp tục vận hành thông suốt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm (đã đạt điểm tối đa 100/100 ở 3 bài chính) |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra các incremental model đã khai báo `unique_key` và `incremental_strategy` phù hợp hay chưa để đảm bảo tính idempotent khi rerun. |
| 2 | Đánh giá phân bố độ trễ nạp dữ liệu thực tế (P99 latency) để thiết lập lookback window chính xác cho dữ liệu đến muộn. |
| 3 | Kiểm tra Data Contract, kiểm soát Schema Evolution và thiết lập bảng Quarantine cách ly bản ghi lỗi thay vì để pipeline bị crash. |
