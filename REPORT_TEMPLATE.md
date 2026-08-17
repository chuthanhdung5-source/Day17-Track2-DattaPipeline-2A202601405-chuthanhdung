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
| **Triệu chứng** | Bảng `gold_training_set` tăng lên 38.750 hàng (kỳ vọng 12.480) sau các lần chạy lại, xuất hiện bản ghi trùng lặp ticket. |
| **Nguyên nhân** | Incremental model `gold_training_set` không khai báo `unique_key`. Khi thiếu `unique_key`, dbt tự động tạo câu lệnh `INSERT INTO` ghi chèn dữ liệu cũ khi rerun thay vì `MERGE` ghi đè, làm mất tính idempotent. DAG Airflow đặt `catchup=True` làm kích hoạt chạy bù dồn dập các ngày quá khứ. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.<br>`dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng (FAIL) · Sau: 12.480 hàng (✓ ok) · Checksum 3 lượt: `8dd7c98653`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` đạt 8.645 hàng (kỳ vọng 9.100), thiếu 455 cặp `(event_date, customer_id)` ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **3 ngày** |
| **Lookback đã chọn** | 3 ngày — 99% các sự kiện nạp muộn trong `bronze_events` có độ trễ lọt vào kho trong vòng 3 ngày. |
| **Nguyên nhân** | Mệnh đề incremental dùng `where event_date > (select max(event_date) from {{ this }})`. Một event có `event_date` cũ (ví dụ 08-12) nạp trễ ở ngày 08-15 bị loại hoàn toàn do `max(event_date)` lúc này đã là 08-14. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: Sửa điều kiện thành `where event_date >= (select max(event_date) - interval '3' day from {{ this }})`, khai báo `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'`. |
| **Bằng chứng** | Trước: 8.645 hàng · Sau: 9.100 hàng (✓ ok, 14 ngày × 650 customer). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Nếu dùng `max`, một outlier trễ hàng tuần/tháng ép mọi lượt chạy daily phải tính toán lại toàn bộ lịch sử quá khứ, gây tốn chi phí IO/CPU và kéo dài thời gian pipeline. Dùng **P99** đảm bảo xử lý được 99% dữ liệu trễ với chi phí tính toán cố định 3 ngày lookback.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6.606 bản ghi sai (`NULL` hoặc ngoài 1..4). Bảng `quarantine_tickets` có 0 hàng (kỳ vọng 312). |
| **Nguyên nhân** | Backend thay đổi format dữ liệu từ ngày 2026-08-10 sang gửi nhãn chuỗi (`urgent`, `high`, `medium`, `low`). Biểu thức `try_cast(priority_raw as integer)` biến nhãn chuỗi thành `NULL` làm mất dữ liệu, đồng thời bỏ sót các số rác `0`, `5`, `-1`. Bảng quarantine chưa lọc dữ liệu (`where false`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`'1'`, `'2'`, `'3'`, `'4'`): Giữ nguyên.<br>2. **Nhãn chuỗi** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Ánh xạ về `1`, `2`, `3`, `4`.<br>3. **Dữ liệu lỗi** (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `null`): Ép về `NULL` để đẩy sang quarantine. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: Viết `CASE` xử lý 3 nhóm trên.<br>`dbt/models/silver/silver_tickets.sql`: Lọc `normalize_priority IS NOT NULL` **trước khi** `row_number()` xếp hạng.<br>`dbt/models/silver/quarantine_tickets.sql`: Lọc `normalize_priority IS NULL`.<br>`dbt/models/silver/schema.yml`: Đặt `enforced: true` và thêm test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (✓ ok) · `dbt test` 11/11 pass (✓ ok) · `silver_tickets` đủ 12.480 ticket. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Tầng xử lý**: Xử lý ở tầng Silver. Tầng Bronze là Landing Zone cần lưu trữ nguyên vẹn dữ liệu thô (immutable) để đảm bảo tính kiểm toán và phục vụ điều tra sự cố.
> 2. **Cơ chế chịu lỗi**: Không dừng pipeline khi gặp bản ghi lỗi vì vài trăm bản ghi hỏng không được chặn tiến trình xử lý hàng trăm nghìn bản ghi hợp lệ khác. Bảng Quarantine tiếp nhận bản ghi lỗi để xử lý sau mà không làm đứt đoạn pipeline.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm (đạt 100/100 ở 3 bài chính) |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra cấu hình `unique_key` và `incremental_strategy` của các incremental model để đảm bảo tính idempotent. |
| 2 | Đo phân bố độ trễ nạp dữ liệu (P99 latency) để xác định lookback window hợp lý cho dữ liệu đến muộn. |
| 3 | Kiểm tra Data Contract và cơ chế Quarantine cách ly dữ liệu lỗi để bảo vệ pipeline không bị dừng đột ngột. |
