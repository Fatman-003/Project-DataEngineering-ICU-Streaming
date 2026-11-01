# Đánh giá Spark Structured Streaming so với Flink

> **Cách dùng file này.** Số liệu được điền tự động (`python bench/summarize.py && python bench/report.py`).
> Các ô **«Nhóm viết»** là phần phân tích; theo yêu cầu liêm chính học thuật, nhóm tự viết lời văn
> cuối cùng. Các gạch đầu dòng «Gợi ý» chỉ liệt kê bằng chứng và câu hỏi cần trả lời, không phải
> kết luận. Viết vào `docs/spark/EVALUATION_TEMPLATE.md`, không viết vào file sinh ra.

## 1. Mục tiêu và phạm vi

Dự án có một pipeline chính chạy bằng PyFlink (`jobs/flink/`) và một pipeline thay thế chạy bằng
PySpark Structured Streaming (`jobs/spark/`). Cả hai đọc **cùng các topic RAW**, dùng **chung logic
Python** (validate, dedup, ghép wide, cửa sổ, rule MAP) và ghi output theo **cùng contract**. Spark
ghi vào các topic riêng `icu.alt.spark.*`. Vì logic nghiệp vụ là một, khác biệt đo được phản ánh
**engine và cách tổ chức xử lý**, không phải khác biệt thuật toán.

Ngoài phạm vi: thay Flink trong luồng chính, BR-6 (lab correlation), tầng lưu trữ.

## 2. Khung lý thuyết: native streaming và micro-batch

| Khía cạnh | Native streaming (Flink) | Micro-batch (Spark Structured Streaming) |
|---|---|---|
| Mô hình thực thi | Toán tử chạy liên tục, mỗi record đi qua pipeline ngay | Luồng dữ liệu được chia thành các batch nhỏ; mỗi batch là một job được lập kế hoạch rồi thực thi |
| Latency | Thấp, gần với thời gian xử lý một record | Có sàn bằng trigger cộng chi phí cố định của mỗi batch |
| Throughput | Tốt; chi phí mỗi record cố định | Có thể cao nhờ gom batch và xử lý vector (Arrow) |
| Thời gian sự kiện | Watermark lan truyền liên tục; có idleness | Watermark tính lại ở ranh giới batch, chung cho cả query |
| Dữ liệu trễ | Đưa ra side output một cách tường minh | Bị loại trong toán tử stateful; chỉ còn metric |
| State | Keyed state; timer theo event time hoặc processing time | State store theo batch; `applyInPandasWithState` / `transformWithState` |
| Chịu lỗi | Checkpoint bất đồng bộ bằng barrier (biến thể Chandy–Lamport) | Offset log + commit log + phiên bản state store theo batch |
| Batch và stream | DataStream / Table API; batch là trường hợp đặc biệt của stream | Cùng DataFrame API cho batch và stream; hệ sinh thái Spark rộng |
| Vận hành | Cluster riêng (JobManager / TaskManager) | Driver + executor; tích hợp tự nhiên với hệ sinh thái lakehouse |

## 3. Thiết lập thí nghiệm

- **Máy.** Một server Ubuntu 22.04, AMD EPYC 7313P (32 luồng), 251 GB RAM, Docker Engine 29.9.2.
  Mỗi topic có 1 partition, replication factor 1.
- **Phiên bản.** Flink 2.3.0 + PyFlink (Python 3.12.3), Kafka connector 5.0.0-2.2. Spark 4.1.3
  (Scala 2.13, Java 17) + PySpark (Python 3.12.15), pandas 2.3.3, pyarrow 22.0.0. Kafka 4.2.2.
- **Ngân sách tài nguyên bằng nhau** (`docker/compose.bench.yaml`, D13). Flink: JobManager
  0,5 CPU / 1 GB + TaskManager 1,5 CPU / 2,5 GB. Spark: 2 CPU / 3,5 GB. Song song tương đương
  parallelism 1: Spark `local[1]`, 1 shuffle partition.
- **Đồng hồ chung.** Mọi topic được tạo lại với `LogAppendTime` trước mỗi run, nên latency đo bằng
  thời gian của broker (§10.1 của plan). `T_ready` là thời điểm sớm nhất một engine lý tưởng có
  thể phát kết quả: telemetry = RAW thứ tư tới; cửa sổ / alert = RAW đầu tiên đẩy watermark qua mốc.
- **Mức tải** (D25). 24, 120 và 240 msg/s (`--speed 1/5/10`, 5 phút wall), đều dưới năng lực của
  engine chậm hơn. Các mức HF100/HF200 của plan vượt năng lực của Flink nên không dùng cho E2.
- **Số lần lặp** (D26). E1 chạy 3 lần; các thí nghiệm khác chạy **1 lần** cho mỗi cấu hình (mức so
  sánh demo). Phần lớn ô trong bảng vì vậy không có khoảng min–max.
- **Tham chiếu độc lập.** `bench/reference.py` tính kỳ vọng offline bằng Python thuần từ chính các
  bản ghi RAW (D23). Cả hai engine được so với tham chiếu này, không chỉ so với nhau.

## 4. Số liệu

Biểu đồ nằm trong [`bench/results/plots/`](../bench/results/plots/).

{{tables}}

### Vận hành

| | Flink | Spark |
|---|---|---|
| Container cho engine | 3 (JobManager, TaskManager, một container một lần để submit job) | 1 (`icu-spark`) |
| Kích thước image | 3,14 GB (`icu-pyflink`) | 2,91 GB (`icu-spark`) |
| Từ lúc reset topic tới khi sẵn sàng nhận dữ liệu (trung vị E2, s) | {{ops.flink.startup_s}} | {{ops.spark.startup_s}} |
| Code riêng của engine cho rule MAP (dòng không trống) | 72 (`flink_hypotension` + attach) | 133 (`alert_logic.py` 91 + `q_alert.py` 42) |
| Code riêng của engine cho dedup + phân loại thời gian + ghép wide | 102 (dedup 19, classifier 28, watermark 9, wide 46) | 206 (`wide_logic.py` 116, `q_wide.py` 44, `parse_step.py` 46) |
| Khi reset topic | Submit job mới (không restore) | **Phải xóa checkpoint**, vì offset cũ trỏ quá cuối topic mới (`failOnDataLoss`) |

Số dòng code đếm cả docstring, không đếm comment `#`. Code Spark còn phải tự lo phần timer, tự
serialize state và tự sắp thứ tự, những việc mà Flink cung cấp sẵn (D8, D17, D18).

## 5. Kiểm chứng giả thuyết

Đây là các **giả thuyết** nêu trước khi đo. Mỗi dòng ghi bằng chứng; kết luận (ủng hộ, bác bỏ hoặc
chưa đủ dữ liệu) do nhóm viết.

| # | Giả thuyết | Bằng chứng đo được | Kết luận |
|---|---|---|---|
| H1 | Latency cửa sổ/alert của Spark cao hơn Flink và tăng theo trigger interval | Cửa sổ p50 ở 24 msg/s: Flink {{e2.flink.default.normal.window.p50}} ms; Spark trigger mặc định {{e2.spark.default.normal.window.p50}} ms, trigger 1 s {{e2.spark.1s.normal.window.p50}} ms, trigger 5 s {{e2.spark.5s.normal.window.p50}} ms. Alert p50: Flink {{e2.flink.default.normal.alert.p50}} ms, Spark {{e2.spark.default.normal.alert.p50}} ms (mặc định) / {{e2.spark.5s.normal.alert.p50}} ms (5 s) | «Nhóm viết» |
| H2 | Ở chế độ catch-up, Spark đạt throughput ngang hoặc cao hơn PyFlink | E3: Flink {{e3.flink.eps}} event/s ({{e3.flink.seconds}} s), Spark {{e3.spark.eps}} event/s ({{e3.spark.seconds}} s); Spark xử lý cả backlog trong một micro-batch | «Nhóm viết» |
| H3 | Với dữ liệu đúng thứ tự, hai engine cho kết quả giống nhau; khác biệt chỉ ở ca late/duplicate | E1: precision/recall telemetry Flink {{e1.flink.telemetry.precision}}/{{e1.flink.telemetry.recall}}, Spark {{e1.spark.telemetry.precision}}/{{e1.spark.telemetry.recall}}; cửa sổ Flink {{e1.flink.windows.recall}}, Spark {{e1.spark.windows.recall}}; `message_id`/`alert_id` trùng. Khác biệt: event trễ (Flink DLQ `LATE_EVENT`, Spark loại im lặng) | «Nhóm viết» |
| H4 | Sau sự cố, cả hai không mất dữ liệu nhưng có bản ghi trùng (at-least-once) | E4: Flink output lại sau {{e4.flink.recovery_s}} s, {{e4.flink.duplicates}} bản ghi trùng, thiếu {{e4.flink.missing}}; Spark {{e4.spark.recovery_s}} s, {{e4.spark.duplicates}} trùng, thiếu {{e4.spark.missing}} | «Nhóm viết» |
| H5 | Tài nguyên khi tải thấp của Spark khác Flink | CPU trung bình ở 24 msg/s: Flink {{e2.flink.default.normal.cpu}}%, Spark mặc định {{e2.spark.default.normal.cpu}}%; RAM đỉnh Flink {{e2.flink.default.normal.mem}} MiB, Spark {{e2.spark.default.normal.mem}} MiB | «Nhóm viết» |
| H6 | Trigger nhỏ làm tăng số batch và CPU khi tải thấp | Ở 24 msg/s: trigger mặc định {{e2.spark.default.normal.batches}} batch / CPU {{e2.spark.default.normal.cpu}}%; 1 s {{e2.spark.1s.normal.batches}} batch / {{e2.spark.1s.normal.cpu}}%; 5 s {{e2.spark.5s.normal.batches}} batch / {{e2.spark.5s.normal.cpu}}% | «Nhóm viết» |

**Gợi ý khi viết kết luận.**

- **H1.** Latency cửa sổ của Spark gồm hai chặng: Q_wide khoảng {{e2.spark.default.normal.q_wide}} ms,
  rồi Q_window khoảng {{e2.spark.default.normal.q_window}} ms (đọc qua topic trung gian, D20). Chặng
  nào là do bản chất micro-batch, chặng nào là do kiến trúc? Với `local[1]`, bốn query dùng chung một
  luồng task: batch Q_wide trung vị {{e2.spark.default.normal.batch_ms}} ms, so với khoảng 400 ms khi
  chạy một mình (SPIKE_FINDINGS F1).
- **H1, đuôi latency của Flink.** Ở 240 msg/s (khoảng 56% năng lực), p95 telemetry của Flink tăng lên
  {{e2.flink.default.speed10.telemetry.p95}} ms, trong khi p50 là {{e2.flink.default.speed10.telemetry.p50}} ms.
  Nhóm chưa xác định nguyên nhân (giả thuyết: checkpoint 10 s hoặc GC). Nên nêu rõ đây là quan sát chưa
  giải thích.
- **H2.** Khoảng cách throughput một phần đến từ cách PyFlink truyền từng record qua cầu JVM↔Python,
  so với batch Arrow lớn của Spark. Kết quả này **không** đại diện cho Flink/Spark viết bằng Java/Scala.
- **H4.** Spark có 0 bản ghi trùng trong một lần chạy E4. Đó có thể là do thời điểm kill rơi vào khoảng
  giữa hai batch; về nguyên tắc sink của Spark vẫn là at-least-once. Với n = 1, không nên khái quát.
- **H5/H6.** Ở tải thấp, Spark với trigger mặc định chạy micro-batch liên tục nên tốn CPU gần như cố
  định, bất kể tải. Trigger 5 s giảm CPU khoảng 3,5 lần nhưng tăng latency cửa sổ lên khoảng 13 s.

## 6. Lợi và hại trong bối cảnh dự án

Mỗi khía cạnh ghi **bằng chứng** và **hệ quả cho BR**.

### BR-3 / BR-4: alert và dashboard dưới 2 giây
- Bằng chứng: alert p50 Flink {{e2.flink.default.normal.alert.p50}} / {{e2.flink.default.speed5.alert.p50}} / {{e2.flink.default.speed10.alert.p50}} ms;
  Spark (trigger mặc định) {{e2.spark.default.normal.alert.p50}} / {{e2.spark.default.speed5.alert.p50}} / {{e2.spark.default.speed10.alert.p50}} ms
  (ở 24 / 120 / 240 msg/s). Telemetry p95 Spark {{e2.spark.default.normal.telemetry.p95}} ms.
- Gợi ý: ở trigger nào (nếu có) Spark đáp ứng được ngưỡng 2 giây? Dashboard 5 phút có chấp nhận được
  latency cửa sổ vài giây không?
- «Nhóm viết»

### BR-1: chất lượng dữ liệu
- Bằng chứng: Spark ghi DLQ được cho parse/schema/value/duplicate/idempotency, cùng `dlq_id` với Flink.
  Event trễ bị Spark loại **trước** khi tới hàm stateful nên không vào DLQ được, chỉ còn
  `numRowsDroppedByWatermark` (D7, SPIKE_FINDINGS S2). Biên trễ lệch 1 ms (event trễ đúng 2.000 ms).
- Gợi ý: yêu cầu kiểm toán dữ liệu y tế có cần lưu vết từng event trễ không?
- «Nhóm viết»

### BR-7: chất lượng cảnh báo (episode/recovery)
- Bằng chứng: logic episode/recovery được dùng lại nguyên vẹn, và 27 test cảnh báo có sẵn (các ca A–K)
  pass trên Spark (D22). Nhưng phía Spark phải tự giả lập timer (D8), tự serialize state JSON, tự sắp
  thứ tự theo offset (D17), và tốn khoảng gấp đôi số dòng code riêng của engine (mục 4).
  `transformWithStateInPandas` có MapState/timer nhưng mỗi thao tác là một RPC Python↔JVM; trong spike
  nó không xử lý xong 10 nghìn event trong 30 phút (D18, S9).
- «Nhóm viết»

### BR-8: báo cáo theo ca (chưa làm)
- Gợi ý: cùng DataFrame API cho cả job batch đọc dữ liệu lịch sử (ví dụ từ Cassandra/TimescaleDB) và
  job streaming. Stack hiện tại của Flink chưa có phần này. Đây là lợi thế **tiềm năng**, chưa được
  đo trong đánh giá này.
- «Nhóm viết»

### Vận hành
- Bằng chứng: bảng "Vận hành" ở mục 4. Spark chạy trong 1 container, khởi động nhanh hơn
  ({{ops.spark.startup_s}} s so với {{ops.flink.startup_s}} s), nhưng phải xóa checkpoint mỗi khi reset
  topic. Spark UI và log progress JSONL cho metric theo từng batch.
- «Nhóm viết»

## 7. Giới hạn của đánh giá

- Chạy một node, mỗi topic 1 partition, parallelism 1. Không suy ra hành vi ở quy mô cluster.
- Cả hai bên đều dùng Python (PyFlink, PySpark), nên kết quả **không** đại diện cho Flink/Spark viết
  bằng Java/Scala. Đặc biệt, chi phí của cầu JVM↔Python chi phối cả latency lẫn throughput.
- Dữ liệu mô phỏng có tính tất định. Mức tải tối đa trong E2 là 240 msg/s (D25); E3 dùng 576 nghìn
  event thay vì 2,9 triệu.
- Hầu hết cấu hình chỉ chạy **một lần** (D26), nên không có khoảng tin cậy.
- Logic dùng chung từ `jobs/flink/`, nên ưu/nhược đo được là của engine và cách tổ chức, không phải
  của thuật toán.
- Cửa sổ của Spark đọc qua topic trung gian, nên có thêm một chặng Kafka (D20).
- Dedup của Spark gom theo `stay_id` thay vì toàn cục (D19); TTL dedup theo event time thay vì
  processing time (D11). Không ảnh hưởng kịch bản demo.
- Q_labs của Spark chỉ validate và đếm, không dedup, không ghi DLQ cho lab (D14, D24).

## 8. Tài liệu liên quan

- Kế hoạch: [`docs/plans/SPARK_ALTERNATIVE_PLAN.md`](plans/SPARK_ALTERNATIVE_PLAN.md)
- Quyết định thiết kế D1–D26: [`docs/spark/DECISIONS.md`](spark/DECISIONS.md)
- Kết quả spike S1–S9, F1–F2: [`docs/spark/SPIKE_FINDINGS.md`](spark/SPIKE_FINDINGS.md)
- Sử dụng công cụ AI: [`docs/spark/AI_USAGE.md`](spark/AI_USAGE.md)
- Số liệu thô đã tổng hợp: [`bench/results/summary.json`](../bench/results/summary.json)
