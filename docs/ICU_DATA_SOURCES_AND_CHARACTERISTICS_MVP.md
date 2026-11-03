# Data Sources & Data Characteristics — ICU Streaming MVP

> **Mục đích:** Nội dung thực tế cho mục **2.4 Data Sources** và **2.5 Data Characteristics** của báo cáo/video project. Phạm vi là **MVP hiện tại**, không phải toàn bộ dữ liệu mà MIMIC-IV có thể cung cấp hay các chức năng dự định phát triển.
>
> **Nguyên tắc quan trọng:** Các định danh bệnh nhân/lượt điều trị xuất phát từ **MIMIC-IV Clinical Demo 2.2**, còn dữ liệu sinh hiệu được phát vào Kafka khi demo là **dữ liệu synthetic do simulator tạo**, **không phải những phép đo thực được replay nguyên bản từ MIMIC-IV**.

## 2.4. Data Sources

### 2.4.1. Các nguồn đầu vào thực tế

| Nguồn | Dữ liệu sử dụng | Hình thức và tần suất | Vai trò trong MVP |
|---|---|---|---|
| **MIMIC-IV Clinical Demo 2.2 — reference subset** | Thông tin `patients`, `admissions`, `icustays`, danh mục `d_items`, `d_labitems` | Dữ liệu tĩnh, được chuẩn bị thành seed cho **4 lượt nằm ICU** | Cung cấp định danh hợp lệ, quan hệ bệnh nhân–lượt nhập viện–lượt ICU và metadata để ánh xạ dữ liệu mô phỏng. Không stream trực tiếp từ CSV MIMIC lúc demo. |
| **Synthetic bedside vital signs** | Sáu tín hiệu **HR, MAP, SBP, DBP, SpO₂, RR** của 4 giường | JSON dạng **LONG**: 1 bản tin / tín hiệu / thời điểm; chế độ 1 Hz, 100 Hz hoặc 200 Hz | **Nguồn streaming chính** của hệ thống. Simulator gửi vào Kafka RAW `icu.v2.chartevents`. |
| **Synthetic Lactate events** | Các phép đo lactate giả lập, gắn với lượt ICU | JSON sự kiện xét nghiệm, thưa hơn nhiều so với sinh hiệu; kịch bản chuẩn khoảng **240 giây/lần cho mỗi lượt ICU** | Simulator gửi vào `icu.v2.labevents`; Flink có nhánh kiểm tra dữ liệu lab, nhưng **chưa dùng lactate để tính SOFA hoặc kết luận sepsis**. |
| **Seeded historical lab records** | **8 hàng `labevents`** được nạp khi khởi tạo theo kiến trúc Spring Boot/Flyway đã audit | Dữ liệu lịch sử trong TimescaleDB/PostgreSQL, không phải stream liên tục | Phục vụ phần hiển thị/tra cứu lab lịch sử; **không đồng nghĩa** với việc dữ liệu lactate RAW từ Kafka đã được persist tự động. |

**Định danh bệnh nhân:** MVP mô phỏng **4 giường**, tương ứng các `stay_id` sau:

| Nhãn demo | `bed_id` dùng trong MVP | `stay_id` |
|---|---:|---:|
| P01 | 1 | `37323533` |
| P02 | 2 | `35727289` |
| P03 | 3 | `39864867` |
| P04 | 4 | `32359580` |

Các ID liên kết bệnh nhân/lượt điều trị dựa trên bộ demo; **nhãn P01–P04, số giường và diễn biến sinh hiệu là thiết kế mô phỏng của dự án**.

### 2.4.2. Dữ liệu đi vào pipeline như thế nào?

```text
MIMIC-IV Clinical Demo 2.2
    └── Reference seed: patients / admissions / icustays / item catalogs

Python ICU Simulator (4 virtual ICU beds)
    ├── Six synthetic vital signals ──> Kafka RAW: icu.v2.chartevents
    └── Sparse synthetic lactate ────> Kafka RAW: icu.v2.labevents
                                        │
                                        ▼
                               Apache Flink
                        validation / dedup / Event Time
                                        │
                           Kafka business outputs
                     icu.telemetry / icu.vitals.5m /
                                  icu.alerts
                              ┌─────────┴─────────┐
                              ▼                   ▼
                        Storage Writer       Spring Boot
                              ▼                   ▼
                         TimescaleDB         STOMP → React
                              └──> Spring REST → React (history)
```

**Lưu ý phân loại:** Các topic `icu.telemetry`, `icu.vitals.5m`, `icu.alerts` là **dữ liệu được pipeline tạo ra**, không phải ba nguồn dữ liệu y tế độc lập. RAW topic có thể giữ tên `v2` dù bản kiểm tra repository sau merge cho thấy **body schema version `3.0`**. Không suy luận schema version từ tên topic.

### 2.4.3. Dữ liệu nào *chưa* được sử dụng như một nguồn xử lý chính?

- **ECG/PPG/arterial waveform thực:** Chưa có pipeline waveform thực chạy xuyên suốt từ file WFDB tới serving/database. Chế độ 100/200 Hz ở đây là **tăng tần suất các giá trị sinh hiệu synthetic để kiểm thử tải**, không có nghĩa đang đọc ECG waveform thực 100/200 Hz.
- **Full MIMIC-IV `chartevents`/`labevents` lịch sử:** Không được đẩy toàn bộ vào Kafka trong lần chạy demo chuẩn.
- **Dữ liệu lâm sàng đủ để tính SOFA:** Chưa có đầy đủ PaO₂/FiO₂, platelets, bilirubin, GCS, vasopressor và urine output dưới dạng nguồn được tích hợp trong rule engine. **MVP không tính SOFA và không chẩn đoán sepsis.**

## 2.5. Data Characteristics

### 2.5.1. Quy mô và tần suất dữ liệu

Workload chuẩn: **4 bệnh nhân × 6 tín hiệu**; mỗi tín hiệu là một RAW Kafka message riêng tại mỗi tick. Các con số dưới đây là **tốc độ phát sinh lý thuyết theo cấu hình simulator**, không phải throughput đã benchmark hoặc SLA.

| Chế độ | Tần suất / tín hiệu / bệnh nhân | Tổng RAW vital messages/giây | Wide telemetry kỳ vọng tối đa* |
|---|---:|---:|---:|
| **Normal** | 1 Hz | **24 msg/s** | ~4 rows/s |
| **HF100** | 100 Hz | **2.400 msg/s** | ~400 rows/s |
| **HF200** | 200 Hz | **4.800 msg/s** | ~800 rows/s |

\* Khi 4 thành phần cần cho bản ghi WIDE (**HR, MAP, SBP, SpO₂**) được nhận, ghép và xử lý đầy đủ. Số row thực có thể thấp hơn do dữ liệu thiếu, lỗi, trễ hoặc giới hạn xử lý. **DBP và RR vẫn có trong RAW**, nhưng không phải 4 trường của bản ghi WIDE telemetry hiện tại; RR có thể được dùng để bổ sung ngữ cảnh alert.

- Ở chế độ **Normal trong 6 phút Event Time**, simulator tạo khoảng **8.640 RAW vital messages** (24 × 360), chưa tính lab hay quality probes; wide telemetry lý tưởng khoảng **1.440 bản ghi** (4 × 360).
- Lactate là chuỗi **sparse/irregular** so với sinh hiệu: khoảng 1 phép đo/4 phút/lượt ICU trong kịch bản mô phỏng, không phải 1 phép đo/giây.
- Tuỳ chọn `--speed` chỉ **nén thời gian chạy replay trên đồng hồ thực**; không làm thay đổi các mốc Event Time hay biến workload thành dữ liệu từ monitor thật.

### 2.5.2. Đặc trưng dữ liệu theo 5V

| Đặc trưng | Thực tế trong MVP | Hệ quả thiết kế Data Engineering |
|---|---|---|
| **Volume** | 4 giường, 6 loại vital; các bài HF có thể tạo hàng nghìn RAW messages/giây | Kafka tiếp nhận; Flink chuyển LONG → WIDE và tổng hợp; TimescaleDB lưu kết quả business thay vì lưu mọi RAW signal vào các bảng business. |
| **Velocity** | Normal 24 msg/s; HF200 4.800 msg/s; dashboard cần cập nhật gần thời gian thực | Dùng Kafka/Flink xử lý streaming; luồng phục vụ realtime qua Spring/STOMP **không chờ** Storage Writer ghi xong DB. |
| **Variety** | Reference data quan hệ; sinh hiệu JSON LONG; lactate thưa; đầu ra telemetry WIDE, sliding-window và alert | Chuẩn hóa bằng `stay_id`, ánh xạ signal/item, phân tách RAW topic và business topic theo mục đích. |
| **Veracity** | Có mẫu hợp lệ và quality probes: duplicate, invalid, out-of-order, late | Validation, deduplication, Watermark và DLQ giúp tránh đưa bản tin lỗi vào các phép tính/cảnh báo. |
| **Value** | Telemetry realtime, thống kê 5 phút, cảnh báo MAP thấp kéo dài, dữ liệu lịch sử | Hỗ trợ dashboard và kiểm tra pipeline. Đây là **proof of concept kỹ thuật**, chưa phải hệ thống ra quyết định lâm sàng đã thẩm định. |

### 2.5.3. Các tính chất streaming quan trọng

- **Event Time khác Processing Time:** Simulator gắn thời điểm đo; Kafka có thể nhận bản tin muộn hoặc sai thứ tự. Flink xử lý theo Event Time, dùng **Watermark tolerance 2 giây** trong thiết kế alert/pipeline đã tài liệu hóa.
- **Duplicate / invalid / late data:** Dự án có các quality probes, ví dụ P04 tạo sự kiện lặp, SpO₂ không hợp lệ và sự kiện tới muộn. Bản tin lỗi được cách ly trong `icu.deadletter` theo logic kiểm định; sự kiện out-of-order còn trong giới hạn có thể được chấp nhận.
- **Irregular sampling / missingness:** Vital được giả lập đều theo tick, nhưng có thể thiếu component hoặc bị loại; lactate thưa hơn nhiều. **Không coi missing là normal**, và không suy diễn có số đo tại các thời điểm không quan sát được.
- **LONG → WIDE:** Flink tạo bản ghi `icu.telemetry` theo `stay_id` và timestamp, với 4 trường chính: `heart_rate`, `map`, `sbp`, `spo2`.
- **Window statistics:** `icu.vitals.5m` dùng cửa sổ **5 phút, slide 1 phút**, tính mean theo từng signal; nhánh mở rộng TICKET-017 bổ sung variance cho bốn signal trên. Không nhầm variance với chuẩn sai (`standard deviation = sqrt(variance)`).
- **Early warning:** `HYPOTENSION_SUSTAINED` phụ thuộc MAP `<65 mmHg` với **30 giây accumulated low evidence**; HR/RR/SBP/SpO₂ là thông tin ngữ cảnh. **Không phải tiêu chuẩn xác nhận sepsis/SOFA**.

## Tóm tắt có thể đưa lên slide

**Slide — Data Sources**

- **MIMIC-IV Clinical Demo 2.2:** 4 lượt ICU và reference metadata; không dùng nguyên waveform/chartevents lịch sử làm live feed.
- **Synthetic streaming vitals:** 4 giường × 6 tín hiệu (HR, MAP, SBP, DBP, SpO₂, RR) → Kafka RAW JSON.
- **Sparse Lactate:** mô phỏng khoảng 240 giây/lần; phân biệt với 8 bản ghi lab seed phục vụ lịch sử.
- **Data outputs:** Flink tạo wide telemetry, cửa sổ 5 phút và MAP alert; không tính SOFA.

**Slide — Data Characteristics**

- **Velocity / Volume:** 24 RAW msg/s (Normal), 2.400 (HF100), 4.800 (HF200) — *theoretical input rates*.
- **Variety:** Reference relational + JSON vital LONG + sparse lab; Flink chuyển LONG → WIDE.
- **Veracity:** Duplicate, invalid, out-of-order, late → Validation / Dedup / Watermark / DLQ.
- **Time & Missingness:** Event Time, Watermark 2s, mẫu xét nghiệm thưa; missing không phải normal.
- **Value:** Realtime dashboard, 5m statistics, hypotension alert; demo kỹ thuật, không chẩn đoán sepsis.

---

### Phạm vi xác nhận nguồn

Nội dung dựa trên tài liệu template `DE_ICU_Streaming.pdf` (mục 2.4–2.5), các tài liệu pipeline/alert đã cung cấp, và báo cáo audit repository sau khi merge Spring Boot + React tại commit `b49e7cf`. Một số tài liệu cũ vẫn mô tả Gateway Python, RAW schema 2.0 hoặc 4 hypertables; các thông tin đó **không được coi là deployment hiện tại**. Phiên bản source code **sau TICKET-016/017 chưa được cung cấp để kiểm tra trực tiếp**: trước khi trình bày như kết quả đã kiểm thử, nhóm nên đối chiếu Kafka message hiện tại và kết quả test variance/runtime. Các tốc độ ghi trong bảng là từ cấu hình simulator, không phải số liệu benchmark đo được.
