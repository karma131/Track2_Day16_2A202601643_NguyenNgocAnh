# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Ngọc Anh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 73.4s
run 2/3 … 77.1s
run 3/3 … 65.6s

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
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Retry/Clear Task làm `gold_training_set` tăng số hàng dù pipeline không báo lỗi. |
| **Nguyên nhân** | Model incremental không có `unique_key` và strategy nên dbt append lại cùng entity. CDC còn có update, vì vậy một ticket có thể được đưa vào nhiều lượt. `catchup=True` và không giới hạn active run làm tăng khả năng replay/chạy chồng. |
| **Cách khắc phục** | `gold_training_set.sql`: `merge` theo `ticket_id`. DAG: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Sau sửa: 12.480 hàng; checksum 3 lượt đều `8dd7c98653`; không có ticket trùng. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng daily ổn định nhưng thiếu 455 tổ hợp ngày–khách hàng ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **2,7258 ngày** (xấp xỉ 2 ngày 17 giờ 25 phút). Max đo được: 2,9447 ngày. |
| **Lookback đã chọn** | 3 ngày — làm tròn lên từ P99 và vẫn bao phủ max của tập dữ liệu hiện tại. |
| **Nguyên nhân** | Filter `event_date > max(event_date)` chỉ nhận ngày mới; event xảy ra ở ngày cũ nhưng được ingest muộn không bao giờ được tính lại. |
| **Cách khắc phục** | Tính lại cửa sổ 3 ngày và `merge` theo khóa ghép `(event_date, customer_id)`. |
| **Bằng chứng** | Trước: 8.645 hàng; sau: 9.100 hàng; checksum 3 lượt đều `3db448685c`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 giới hạn chi phí tính lại trong điều kiện vận hành thông thường; dùng max dễ bị một outlier kéo cửa sổ quá rộng vĩnh viễn. Mỗi ngày lookback tăng thêm đều làm tăng dữ liệu phải scan, aggregate và merge ở mọi lượt chạy. Các trường hợp vượt SLA P99 nên được theo dõi và backfill có kiểm soát.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Sau 10/08 nguồn đổi priority từ số sang nhãn; pipeline vẫn chạy nhưng 6.606 giá trị Silver sai/NULL. |
| **Nguyên nhân** | `try_cast` biến nhãn hợp lệ thành NULL nhưng lại chấp nhận số ngoài miền như 0, 5, -1; contract bị tắt và quarantine luôn lọc `false`. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1..4`: giữ nguyên; `urgent/high/medium/low`: map về `1/2/3/4`; NULL, rỗng, P1, unknown hoặc số ngoài 1..4: quarantine. |
| **Cách khắc phục** | Dùng chung macro chuẩn hóa; loại record lỗi trước khi rank CDC; bật contract; thêm `not_null` và `accepted_values`; đưa record lỗi vào quarantine. |
| **Bằng chứng** | `quarantine_tickets` = 312; `silver_tickets` vẫn đủ 12.480 ticket; 11/11 dbt tests pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Bronze phải giữ payload gốc để có khả năng truy vết. Silver là nơi chuẩn hóa contract và tách record lỗi. Không nên dừng cả pipeline vì 312 CDC rows hỏng không liên quan đến hơn 130 nghìn event và 31.200 document chunks hợp lệ; quarantine tạo hàng đợi xử lý mà vẫn duy trì dịch vụ.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | A: 5.000 file nhỏ, không partition và filter bọc cột bằng `strftime`. B: commit offset trước khi ghi tạo at-most-once, crash làm mất batch. |
| **Cách khắc phục** | A: partition 14 ngày, sort theo khách hàng/thời gian, row group 2.048, predicate trực tiếp trên `event_date`. B: ghi transaction idempotent trước, commit offset sau; PK `event_id` và upsert `DO UPDATE`. |
| **Bằng chứng** | A: 5.000 → 14 file, 5.000.000 → 9.324 rows scanned (536,3×), hash không đổi. B: crash batch 7 rồi phục hồi đủ 20.000/20.000 event, không mất/trùng. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, khóa tự nhiên và semantics khi retry trước khi chọn incremental strategy. |
| 2 | So sánh event time với ingestion time, đo phân bố lateness trước khi đặt watermark/lookback. |
| 3 | Kiểm tra contract ở cả kiểu và miền giá trị; phân biệt schema evolution hợp lệ với record thực sự hỏng. |
