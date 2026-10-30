# Kế hoạch: Spark Structured Streaming làm giải pháp thay thế cho Flink

> File này viết cho **Claude Code** thực hiện trong repo
> `Fatman-003/Project-DataEngineering-ICU-Streaming`. Đặt file tại
> `docs/plans/SPARK_ALTERNATIVE_PLAN.md` và làm việc trên nhánh
> `feature/spark-alternative`.

## 0. Cách dùng file này

1. Đọc **toàn bộ** file trước khi viết code.
2. Làm theo thứ tự phase. Hết mỗi phase, kiểm tra tiêu chí "Hoàn thành khi", rồi **dừng lại tóm tắt cho người dùng** trước khi sang phase sau.
3. Mọi quyết định thiết kế không có sẵn trong file này ghi vào `docs/spark/DECISIONS.md`, mỗi mục gồm: bối cảnh, lựa chọn, lý do, phương án đã loại.
4. Nếu thực tế trong repo khác với giả định ở đây (tên file, tên hàm, hành vi), **không tự đoán**. Ghi lại khác biệt và hỏi người dùng.
5. Mục tiêu là một **phép so sánh có giá trị khoa học**, không phải làm Spark "thắng". Báo cáo đúng số đo, kể cả khi bất lợi cho Spark.

## 1. Bối cảnh và mục tiêu

### Hiện trạng (commit `b49e7cf`)

Giải pháp chính đang chạy:

```
run_icu_demo.py ─► Kafka RAW (icu.v2.chartevents, icu.v2.labevents)
                    └─► PyFlink job (jobs/flink/runtime_pipeline.py)
                         ├─► icu.telemetry      (wide sample: heart_rate/map/sbp/spo2)
                         ├─► icu.vitals.5m      (mean 5 phút, trượt 1 phút)
                         ├─► icu.alerts         (HYPOTENSION_SUSTAINED, 30 giây low evidence)
                         └─► icu.deadletter     (parse/schema/duplicate/late/idempotency)
                    business topics ─► icu-server (Spring, WebSocket) và icu-storage-writer (TimescaleDB)
```

Các đặc điểm của Flink job cần tái hiện:

- Parallelism 1, một slot, mỗi topic 1 partition.
- Watermark bounded out-of-orderness **2 giây**, idleness 5 giây.
- Dedup theo `event_id`, TTL 24 giờ.
- Kafka sink AT_LEAST_ONCE, checkpoint 10 giây.
- Tài nguyên: JobManager 1024m, TaskManager 2048m.

Một thành viên khác đang làm **Cassandra thay TimescaleDB**. Phần việc này không đụng tới tầng lưu trữ.

### Mục tiêu

- **G1.** Hiện thực một pipeline Spark Structured Streaming (PySpark) đọc **cùng RAW topic** và tạo output **cùng contract** với Flink, ghi vào các topic riêng `icu.alt.spark.*`.
- **G2.** Đo và so sánh Spark với Flink về correctness, latency, throughput, tài nguyên và khả năng phục hồi lỗi, trên cùng input có thể lặp lại.
- **G3.** Tạo dữ liệu, biểu đồ và khung phân tích lợi/hại để nhóm viết báo cáo.

### Ngoài phạm vi

- Không thay Flink trong luồng chính. Không đổi topic, contract hay code Flink.
- Không làm BR-6 (lab correlation). Lab chỉ được validate rồi đếm, giống Flink chỉ `print`.
- Không tối ưu Spark đến mức làm sai lệch so sánh (ví dụ viết lại logic bằng Scala trong khi Flink là PyFlink). Nếu muốn thử, làm thành thí nghiệm phụ có nhãn rõ ràng (E5).

## 2. Ràng buộc bất biến

1. **Không sửa** `jobs/flink/**`, `server/**`, `client/**`, `services/**`, các topic và contract hiện có. Ngoại lệ duy nhất: thêm service mới vào `docker/compose.yaml` dưới `profiles: ["spark"]`, để `docker compose up` mặc định vẫn y như cũ.
2. **Tái sử dụng logic thuần** từ `jobs/flink/` bằng import (đưa thư mục vào `PYTHONPATH` / `--py-files`), **không chép lại**. Các module này không import `pyflink` ở top-level (đã kiểm tra):
   - `contracts.py`: `parse_event` (JSON nghiêm ngặt), `SIGNALS`, `TOPICS`, `stay_key`
   - `validation.py`: `validate_event`, `parse_timestamp`, `ValidationError`
   - `dedup.py`: `dedup_identity`
   - `dlq.py`: `rejection`, `encode`
   - `event_time.py`: `event_timestamp_ms`, `classify_event`
   - `business_outputs.py`: `telemetry(event, values)` để format wide sample
   - `window_aggregates.py`: `aggregate_result`, `iso_timestamp`
   - `hypotension.py`: `bucket_add`, `median_map`, `advance_episode`, `alert_identity`
   - `runtime_config.py`: `WindowAlertConfig`, các hằng số ngưỡng
   - `business_contract.py`: `validate`, `headers`, `metadata`, `stable_uuid`

   Nhờ dùng chung logic, khác biệt đo được phản ánh **engine**, không phải khác biệt cài đặt.
3. **Output Spark phải đi qua được** `business_contract.validate(event, topic, key, headers, require_headers=True)`, với `topic` là tên business topic tương ứng. `message_id` / `alert_id` phải **trùng khớp** với Flink trên cùng input, vì chúng là UUIDv5 sinh từ identity.
4. **Pin chính xác mọi phiên bản** (image, jar, pip). Chọn bản phát hành **ít nhất 2 tuần tuổi**. Jar tải lúc build và kiểm tra SHA-256, theo đúng kiểu `docker/flink/Dockerfile`.
5. Mọi script mới viết bằng **Python, chạy được trên Windows/macOS/Linux**. Không thêm script PowerShell-only.
6. Không thêm dependency vào `requirements.txt` gốc nếu không cần cho host. Dependency của benchmark đặt trong `bench/requirements.txt`.
7. **Minh bạch AI.** Đề cương môn học yêu cầu ghi rõ công cụ AI và cách sử dụng. Thêm vào `docs/spark/AI_USAGE.md` danh sách phần do Claude Code tạo hoặc hỗ trợ; nhóm sẽ đưa nội dung này vào phần tài liệu tham khảo của báo cáo.

## 3. Kiến trúc mục tiêu

### 3.1 Sơ đồ

```
Kafka RAW (icu.v2.chartevents, icu.v2.labevents)    ← cùng input với Flink
   │
   ├─► Q_wide  : validate → dedup → late check → LONG→WIDE (emit khi đủ 4 field)
   │             ├─► icu.alt.spark.telemetry
   │             └─► icu.alt.spark.deadletter
   │
   ├─► Q_alert : validate → MAP hợp lệ → dedup → bucket 1s/median → episode
   │             └─► icu.alt.spark.alerts
   │
   └─► Q_labs  : validate labevents → đếm (tương đương Flink chỉ print)

icu.alt.spark.telemetry
   └─► Q_window: watermark 2s → window(5 phút, trượt 1 phút) theo stay_id → sum/count
                 └─► icu.alt.spark.vitals.5m
```

Tất cả query chạy trong **một SparkSession, một container** (`icu-spark`, `local[N]`). Mỗi query có checkpoint riêng trong volume `icu-spark-checkpoints`.

### 3.2 Đối chiếu Flink → Spark

| Bước | Flink hiện tại | Spark đề xuất | Ghi chú |
|---|---|---|---|
| Đọc Kafka | `KafkaSource` + deserializer giữ topic/partition/offset/key | `readStream.format("kafka")` có sẵn cột `topic, partition, offset, timestamp, key, value`; bật `includeHeaders=true` | Spark tự lưu offset trong checkpoint, không dùng group `icu-flink-v2`, nên chạy song song không ảnh hưởng Flink |
| Parse/validate | `ProcessFunction` Python | `mapInPandas` gọi `parse_event` + `validate_event` | Cùng chi phí Python ở hai bên |
| Dedup | Keyed state TTL 24h, duplicate → DLQ | Trong `applyInPandasWithState` của Q_wide và Q_alert: giữ `event_id` đã thấy, dọn theo timeout | `dropDuplicatesWithinWatermark` loại **im lặng**, không tạo DLQ, nên chỉ dùng trong thí nghiệm phụ |
| Event-time | `for_bounded_out_of_orderness(2s)` + idleness 5s | `withWatermark(event_ts, "2 seconds")` | Spark cập nhật watermark **giữa các micro-batch**, toàn query, không có idleness |
| Late event | `classify_event` với watermark hiện tại → DLQ | So timestamp với `state.getCurrentWatermarkMs()` trong hàm stateful; số dòng Spark tự loại lấy từ `numRowsDroppedByWatermark` | Xem spike S2 |
| LONG→WIDE | Keyed state, emit ngay khi đủ 4 field, timer chốt phần thiếu | `applyInPandasWithState` keyed `stay_id`, `GroupStateTimeout.EventTimeTimeout` | Emit ngay khi đủ là điều kiện để so latency công bằng |
| Cửa sổ 5m | `SlidingEventTimeWindows(5m, 1m)` trên wide | `groupBy(window(sample_ts, "5 minutes", "1 minute"), stay_id)`, output mode `append` | Tính `sum` và `count`, rồi format bằng `aggregate_result` |
| Alert MAP | `KeyedProcessFunction`, pending bucket, chốt bằng watermark | `applyInPandasWithState` keyed `stay_id`, giữ pending bucket trong state, chốt bucket khi watermark qua `bucket_end` | Tái sử dụng `bucket_add`, `median_map`, `advance_episode` |
| Sink | `KafkaSink` AT_LEAST_ONCE + header | `writeStream.format("kafka")` với cột `key, value, headers` | Cả hai đều at-least-once |
| Chịu lỗi | Checkpoint barrier mỗi 10s | Offset log + commit log + state store theo từng batch | So sánh ở E4 |

### 3.3 Khác biệt ngữ nghĩa cần ghi nhận (không phải lỗi)

- **Watermark theo batch.** Spark chỉ tính watermark mới khi kết thúc một micro-batch. Vì vậy cửa sổ đóng và bucket MAP được chốt ở **ranh giới batch**, tạo ra một "sàn latency" phụ thuộc trigger và thời gian chạy batch.
- **Dữ liệu trễ bị loại im lặng.** Spark loại event trễ trong toán tử stateful mà không có side output. Flink đẩy chúng vào DLQ. Spark ghi DLQ cho late được đến đâu, ghi nhận đến đó.
- **Không có idleness.** Khi input dừng, Spark không tự tiến watermark. Cửa sổ và bucket cuối có thể không bao giờ đóng. Flink cũng có hiện tượng tương tự khi dừng hẳn. Khi so sánh, loại phần "đuôi" theo định nghĩa ở mục 9.2.

## 4. Phase 0: Khảo sát và spike (bắt buộc làm trước)

### Việc cần làm

1. Đọc: `README.md`, `KAFKA_FLINK_FLOW.md`, `docs/KAFKA_MESSAGE_FORMATS.md`, `docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md`, toàn bộ `jobs/flink/*.py`, `scripts/demo/run_icu_demo.py`, `scripts/demo/high_frequency.py`, `scripts/demo/kafka_topics.py`, `docker/compose.yaml`, `docker/flink/Dockerfile`, `tests/test_map_operator.py`, `tests/test_alarm_quality.py`.

   Lưu ý: `KAFKA_FLINK_FLOW.md` còn mô tả Python Gateway cổng 8083. Gateway này **không còn trong compose**; Spring server đã thay. `scripts/demo/fast_verify.py` phụ thuộc Gateway cũ nên **không dùng làm công cụ đo**.

2. Chọn phiên bản Spark 4.x ổn định (Scala 2.13, Java 17), có `applyInPandasWithState`, Python `StreamingQueryListener` và cột `headers` cho Kafka sink. Ghi lý do chọn vào `DECISIONS.md`.

3. Viết các spike nhỏ trong `jobs/spark/spikes/` (chạy trong container, dùng topic tạm `icu.alt.spike.*`) để kiểm chứng các giả định sau. Ghi kết quả vào `docs/spark/SPIKE_FINDINGS.md`.

| Mã | Giả định cần kiểm chứng | Ảnh hưởng nếu sai |
|---|---|---|
| S1 | `mapInPandas` → `withWatermark` → `applyInPandasWithState(EventTimeTimeout)` chạy được trong một query, output mode `append` | Phải tách query qua topic trung gian |
| S2 | Hàm stateful có nhận được dòng có timestamp ≤ watermark không, hay Spark đã lọc trước? `numRowsDroppedByWatermark` có phản ánh đúng số dòng bị lọc? | Quyết định cách đếm/ghi DLQ cho late |
| S3 | Khi không còn dữ liệu mới, timeout event-time có chạy (no-data batch) để chốt bucket/wide sample bị thiếu? | Ảnh hưởng chốt phần đuôi |
| S4 | Kafka sink ghi được cột `headers` đúng định dạng, đọc lại qua `business_contract.validate(..., require_headers=True)` | Nếu không, ghi rõ khác biệt |
| S5 | `spark.sql.shuffle.partitions` nhỏ (ví dụ 2) cho latency tốt hơn rõ rệt so với mặc định 200 | Xác nhận cấu hình chuẩn |
| S6 | Thứ tự dòng trong một nhóm của `applyInPandasWithState` không được đảm bảo; phải tự sắp xếp theo `event_time` | Bắt buộc sort trong hàm |

### Hoàn thành khi

`SPIKE_FINDINGS.md` có kết luận cho S1–S6 kèm bằng chứng (log hoặc output). `DECISIONS.md` chốt kiến trúc cuối cùng. Nếu S1 sai, đề xuất phương án thay thế và **dừng lại hỏi người dùng**.

## 5. Phase 1: Hạ tầng

### Việc cần làm

1. **`docker/spark/Dockerfile`**
   - Base là image chính thức `apache/spark` có Python 3, pin tag và digest.
   - Bake sẵn jar `spark-sql-kafka-0-10_2.13` cùng phiên bản Spark và các phụ thuộc bắc cầu (`kafka-clients`, `commons-pool2`, `spark-token-provider-kafka-0-10`), kiểm tra SHA-256.
   - Pip pin: `pandas`, `pyarrow` (đúng dải Spark hỗ trợ), `PyYAML` (cho `registry.py`).
   - Copy `jobs/spark/`. Mount `jobs/flink/` read-only giống service Flink.

2. **Service `icu-spark`** trong `docker/compose.yaml`, `profiles: ["spark"]`:
   - Env: `KAFKA_BOOTSTRAP_SERVERS=icu-kafka:9094`, `ICU_PATIENT_CONFIG=/opt/icu/config/demo_patients.yaml`, `SPARK_TRIGGER` (mặc định rỗng = chạy batch ngay khi xong batch trước), `SPARK_MASTER=local[1]`, `SPARK_SHUFFLE_PARTITIONS=1`.
   - Volume: `icu-spark-checkpoints`, `icu-spark-metrics` (log progress dạng JSONL).
   - Mở `127.0.0.1:4040` cho Spark UI. Healthcheck dựa trên file ready mà driver ghi sau khi các query đã start.
   - `local[1]` với 1 shuffle partition là cấu hình **tương đương parallelism 1** của Flink. Cấu hình lớn hơn chỉ dùng ở E6.

3. **`docker/compose.bench.yaml`** (override, chỉ dùng khi benchmark): đặt `cpus` và `mem_limit` **bằng nhau về tổng ngân sách** cho Flink (JM + TM) và cho Spark. Ví dụ: Flink JM 0,5 CPU/1 GB + TM 1,5 CPU/2 GB; Spark 2 CPU/3 GB. Ghi ngân sách cuối cùng vào `DECISIONS.md`.

4. **`jobs/spark/topics.py`**: tạo 4 topic `icu.alt.spark.{telemetry,vitals.5m,alerts,deadletter}`, mỗi topic 1 partition, RF 1. Tái sử dụng helper trong `scripts/demo/kafka_topics.py` nếu phù hợp, không sửa file đó.

5. **`jobs/spark/metrics_listener.py`**: Python `StreamingQueryListener` ghi mỗi `QueryProgressEvent` vào `/opt/icu/metrics/<query>.jsonl`. Các trường cần có: `batchId`, `timestamp`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs.*`, `eventTime.watermark`, `stateOperators[].numRowsTotal`, `memoryUsedBytes`, `numRowsDroppedByWatermark`.

6. **`jobs/spark/config.py`**: đọc env, dựng SparkSession với các cấu hình bắt buộc:
   - `spark.sql.shuffle.partitions` = env (không để mặc định 200)
   - `spark.sql.session.timeZone=UTC`
   - `spark.sql.streaming.noDataMicroBatches.enabled=true`
   - Đường dẫn checkpoint theo từng query
   - Kafka source: `startingOffsets=earliest`, `failOnDataLoss=true`

### Hoàn thành khi

`docker compose -f docker/compose.yaml --profile spark --env-file .env up -d --build` chạy lên `icu-spark` healthy. `docker compose ... up -d` **không** có profile thì hệ thống y như trước. Spark UI mở được.

## 6. Phase 2: Q_wide (BR-1 + wide telemetry + DLQ)

### Việc cần làm (`jobs/spark/q_wide.py`)

1. Đọc `icu.v2.chartevents`, giữ `topic, partition, offset, key, value, headers, timestamp`.

2. `mapInPandas(parse_validate)`: với mỗi dòng, dựng `source` dict theo đúng shape mà `dlq.rejection` cần (xem `payload_bytes` và `rejection` trong `dlq.py`, và `KafkaMetadataDeserializer` của Flink). Output các cột:
   - `status` ∈ {`VALID`, `REJECTED`}
   - `stay_key`
   - `event_ts` (timestamp, từ `event_timestamp_ms`)
   - `event_json`, `source_json`, `dlq_json`

   Dòng không parse được dùng `stay_key = "__invalid__"` và `event_ts` = timestamp Kafka, để vẫn đi qua được bước stateful.

3. `withWatermark("event_ts", "2 seconds")` → `groupBy("stay_key").applyInPandasWithState(...)`, timeout event-time. State gồm:
   - `seen_ids`: map `event_id` → `event_ts` đã thấy, dọn bỏ khi cũ hơn watermark − 24h. Flink dùng TTL theo processing time; ghi khác biệt này vào `DECISIONS.md`.
   - `max_clean_ts`
   - `pending`: map `sample_time` → `{values, emitted, event}`, mô phỏng `flink_telemetry` trong `business_outputs.py`.

4. Logic trong hàm, sau khi **sort theo `event_ts`**:
   - `REJECTED` → emit DLQ.
   - `event_id` đã thấy → DLQ `DUPLICATE_EVENT` (cùng `error_type` / `error_message` như Flink dedup).
   - `classify_event(ts, watermark, max_clean_ts)` == `LATE_EVENT` → DLQ, nếu S2 cho phép quan sát dòng này.
   - Signal không thuộc `FIELDS` (DBP, RR) → bỏ qua, như Flink.
   - Ghép field theo đúng quy tắc của `flink_telemetry`, kể cả `min` khi trùng timestamp và `IDEMPOTENCY_CONFLICT` khi sửa sample đã phát. Đủ 4 field thì emit ngay bằng `business_outputs.telemetry(event, values)`.
   - Khi timeout hoặc watermark ≥ `sample_time`: chốt các sample chưa đủ field (null field thiếu), đúng như `on_timer` của Flink.
   - Đặt timeout tới `sample_time` nhỏ nhất còn pending.

5. Output có cột `kind` ∈ {`TELEMETRY`, `DLQ`}. Dùng `foreachBatch`, hoặc hai sink lọc theo `kind` nếu S1 cho phép, để ghi:
   - `icu.alt.spark.telemetry`: key `stay_id`, header từ `business_contract.headers`
   - `icu.alt.spark.deadletter`

   Nếu dùng `foreachBatch`, ghi nhận đây là **at-least-once** và ID của batch được dùng để làm gì.

6. **Q_labs**: đọc `icu.v2.labevents`, validate, đếm số hợp lệ / số bị từ chối vào metrics. Không ghi business topic.

### Hoàn thành khi

- Chạy `run_icu_demo.py --duration-minutes 6 --speed 8` (input cố định qua `--start-time`). Số telemetry của Spark bằng Flink, và **mọi `message_id` khớp**.
- DLQ có đủ các probe của P04: duplicate, invalid SpO2 = −5, late. Chênh lệch nào so với Flink phải giải thích được bằng mục 3.3.
- Unit test thuần trong `tests/spark/test_wide_logic.py` cho hàm stateful (tách logic khỏi API Spark để test không cần JVM), bao phủ các ca: đủ field, thiếu field, duplicate, conflict, late, sample đến khác thứ tự.

## 7. Phase 3: Q_window (BR-2)

### Việc cần làm (`jobs/spark/q_window.py`)

1. Đọc `icu.alt.spark.telemetry`, parse JSON thành các cột `stay_id`, `sample_ts`, `heart_rate`, `map`, `sbp`, `spo2`.

2. Tính cửa sổ:
   - `withWatermark("sample_ts", "2 seconds")`
   - `groupBy(window("sample_ts", "5 minutes", "1 minute"), "stay_id")`
   - Với mỗi field: `sum` và `count` (count chỉ đếm giá trị non-null)
   - Output mode `append`

3. Format bằng `aggregate_result(accumulator, stay, window_start_ms, window_end_ms, emitted_at_ms)`, với accumulator `{field: [sum, count]}` đúng shape của `window_aggregates.create_accumulator`. Ghi ra `icu.alt.spark.vitals.5m`.

4. Lý do đọc từ topic telemetry của Spark thay vì từ RAW: để **cùng ngữ nghĩa** với Flink, vốn tính cửa sổ trên wide stream. Bước này thêm một chặng Kafka; ghi vào `DECISIONS.md` và đo riêng ở E2 (tách latency Q_wide và Q_window).

### Hoàn thành khi

Trên input cố định, tập `(stay_id, window_end)` của Spark trùng Flink cho mọi cửa sổ "đóng được" (mục 9.2). Các `avg_*` lệch không quá `1e-9`, vì thứ tự cộng số thực có thể khác nhau. `message_id` khớp.

## 8. Phase 4: Q_alert (BR-3)

### Việc cần làm (`jobs/spark/q_alert.py`)

1. Đọc RAW chartevents → `mapInPandas(parse_validate)` → chỉ giữ `VALID` với `signal_name == "MAP"`.

2. `withWatermark("event_ts", "2 seconds")` → `applyInPandasWithState` keyed `stay_id`, timeout event-time. State gồm:
   - `seen_ids`
   - `pending_buckets`: `bucket_start_ms` → samples dict của `bucket_add`, kèm first/last observation
   - `episode`: state của `advance_episode`, lưu dạng JSON

3. Mỗi lần gọi:
   - Sort input, bỏ duplicate, bỏ dòng có timestamp ≤ watermark (theo S2).
   - Thêm vào bucket bằng `bucket_add`.
   - Chốt **theo thứ tự tăng dần** mọi bucket có `bucket_start + 1s ≤ watermark`. Với mỗi bucket: dựng `record` giống operator Flink (event có `valuenum = median_map(samples)`, cùng `first_observation_ms` / `last_observation_ms`), rồi gọi `advance_episode`. Alert sinh ra được emit.
   - Đặt timeout tới `bucket_end` nhỏ nhất còn pending.

   Phải đọc kỹ `flink_hypotension` trong `hypotension.py` (dòng 85–139) để dựng `record` **giống hệt**, kể cả trường nào được ghi đè trước khi gọi `advance_episode`.

4. Ghi ra `icu.alt.spark.alerts` với key `stay_id` và header.

### Hoàn thành khi

- Kịch bản chuẩn: P02 có đúng **một** alert với `alert_id`, `alert_time`, `trigger_value` khớp Flink. P03 không có alert.
- Chạy lại toàn bộ các ca A–H trong `ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md` qua logic Spark (unit test thuần, tái sử dụng fixture của `test_alarm_quality.py` nếu được) và cho cùng kết quả.

## 9. Phase 5: Ground truth và kiểm tra correctness

### 9.1 Ground truth độc lập (`bench/reference.py`)

Kiểm tra correctness không chỉ so Spark với Flink, mà so **cả hai** với một tham chiếu độc lập. Simulator có tính tất định, nên tính được kết quả mong đợi offline:

- Sinh lại toàn bộ event bằng `run_icu_demo.demo_events(...)`, hoặc `high_frequency.high_frequency_events(...)`, với cùng `start`.
- Áp dụng validate + dedup, rồi tính bằng Python thuần (vòng lặp đơn giản, không qua engine nào):
  - Wide telemetry theo `(stay_id, sample_time)`
  - Trung bình 5 phút trượt 1 phút theo định nghĩa `[window_end − 5m, window_end)`
  - Alert, bằng cách chạy tuần tự `advance_episode` trên các bucket đã sắp xếp
- Ca late/out-of-order phụ thuộc thời điểm đến thực tế. Tham chiếu đánh dấu những event đó là "phụ thuộc engine" và **không tính** vào tỷ lệ khớp chính; báo riêng.

### 9.2 Định nghĩa "cửa sổ đóng được"

Một cửa sổ (hoặc bucket MAP) được tính vào phép so khi tồn tại event RAW có `event_time ≥ window_end + 2s`. Đó là điều kiện để watermark đủ điều kiện đi qua mốc đóng. Các cửa sổ ở đuôi không thỏa điều kiện được báo riêng là "tail", không tính là lỗi.

### 9.3 Chỉ số correctness (`bench/compare.py`)

| Output | Chỉ số |
|---|---|
| Telemetry | Precision/recall của tập khóa `(stay_id, sample_time)`; tỷ lệ field khớp chính xác; `message_id` khớp |
| Vitals 5m | Precision/recall của `(stay_id, window_end)`; sai số tuyệt đối lớn nhất của `avg_*`; `message_id` khớp |
| Alerts | Khớp chính xác `(stay_id, alert_time, alert_id, trigger_value)` |
| DLQ | Số lượng theo `error_type` (PARSE/SCHEMA/DUPLICATE/LATE/IDEMPOTENCY_CONFLICT) so với tham chiếu; số late Spark loại im lặng (`numRowsDroppedByWatermark`) |
| Trùng lặp | Số bản ghi trùng `message_id` trên mỗi output topic (at-least-once) |

### Hoàn thành khi

`python bench/compare.py --run <dir>` in bảng và ghi `correctness.json`. Trên kịch bản chuẩn, cả Flink và Spark đạt recall = precision = 1,0 so với tham chiếu cho phần "đóng được", hoặc mọi chênh lệch đều có giải thích.

## 10. Phase 6: Benchmark

### 10.1 Nguyên tắc đo

1. **Đồng hồ chung.** Trước mỗi run, đặt `message.timestamp.type=LogAppendTime` cho mọi topic liên quan (RAW, business của Flink, `icu.alt.spark.*`) bằng AdminClient. Timestamp khi đó do broker gán, nên tránh được lệch giờ giữa host Windows và VM Docker. Ghi vào metadata của run.
2. **Định nghĩa latency không phụ thuộc engine.** Mọi latency được tính bằng `ts_output − T_sẵn_sàng`, trong đó `T_sẵn_sàng` là thời điểm sớm nhất mà một engine lý tưởng có thể phát kết quả:

| Output | `T_sẵn_sàng` |
|---|---|
| Telemetry đủ 4 field | `max(append_ts)` của 4 event RAW thành phần |
| Cửa sổ 5m | `append_ts` của event RAW chartevents đầu tiên có `event_time ≥ window_end + 2s` |
| Alert | `append_ts` của event RAW đầu tiên có `event_time ≥ alert_time + 1s + 2s` (bucket 1s + tolerance) |

   Với Flink, watermark bounded out-of-orderness bằng `max_ts − 2s − 1ms`. Kiểm tra lại định nghĩa trên với hành vi thực tế và ghi vào `DECISIONS.md` nếu cần chỉnh.

3. **Chạy riêng từng engine khi đo hiệu năng.** Engine kia được stop, để không tranh CPU. Correctness (E1) có thể chạy song song.
4. **Input lặp lại được.** Luôn dùng `--start-time` cố định. Trước mỗi run: reset topic, **xóa checkpoint Spark**, submit lại Flink job mới (không restore). Reset topic làm checkpoint cũ của Spark mất hiệu lực, vì offset cũ trỏ vượt quá topic mới.
5. **Lặp lại.** Mỗi cấu hình chạy ít nhất 3 lần, báo trung vị và khoảng min–max.
6. **Ghi cấu hình máy.** CPU, RAM, hệ điều hành, phiên bản Docker Desktop, tài nguyên cấp cho VM Docker.

### 10.2 Thí nghiệm

| Mã | Mục đích | Cách chạy | Chỉ số |
|---|---|---|---|
| **E1** Correctness | Hai engine cho cùng kết quả | Kịch bản chuẩn 6 phút có probe; chạy song song cả hai | Mục 9.3 |
| **E2** Latency theo tải | Latency ở các mức tải | Normal (24 msg/s), HF100 (2.400), HF200 (4.800); mỗi mức 5 phút. Flink một cấu hình; Spark với trigger {mặc định, 1s, 5s} | p50/p95/p99/max latency cho telemetry, cửa sổ, alert; tách latency của Q_wide và Q_window |
| **E3** Throughput tối đa | Engine xử lý nhanh nhất được bao nhiêu | Nạp trước khoảng 2,9 triệu event RAW (HF200 × 10 phút, sinh bằng `high_frequency_events` không giới hạn tốc độ), rồi start một engine từ `earliest`. Chỉ chạy Kafka + engine đó | Thời gian xử lý hết, event/giây; với Spark thêm `processedRowsPerSecond` từ progress |
| **E4** Phục hồi lỗi | Hành vi khi engine chết | Đang chạy HF100: `docker kill` TaskManager / `icu-spark`, rồi start lại | Thời gian đến output đầu tiên sau restart; số bản ghi trùng `message_id`; có mất bản ghi nào so với tham chiếu không |
| **E5** (tùy chọn) Chi phí Python | Lượng hóa chi phí UDF Python | Thay `mapInPandas` bằng validate thuần Spark SQL (`from_json` + điều kiện) | Throughput E3 của hai biến thể; liệt kê kiểm tra không làm được nếu không có Python (duplicate JSON key, NaN) |
| **E6** (tùy chọn) Mở rộng | Hành vi khi tăng song song | Topic RAW 4 partition (môi trường benchmark riêng), Spark `local[2]`/`local[4]` + shuffle partition tương ứng; Flink parallelism 2/4 nếu thời gian cho phép | Throughput E3 theo số core |

Thứ tự ưu tiên: E1 → E2 → E3 → E4. E5 và E6 chỉ làm nếu còn thời gian.

### 10.3 Công cụ (`bench/`)

- `bench/run.py`: điều phối một run. Các tham số: engine, profile tải, trigger, số lần lặp. Các bước: reset topic, đặt LogAppendTime, khởi động đúng engine, chạy producer, chờ drain, thu dữ liệu, gọi `compare.py` và `latency.py`, lưu vào `bench/results/<timestamp>_<engine>_<profile>_<trigger>/`.
- `bench/collect.py`: đọc từ `earliest` các topic RAW và output của engine được đo, lưu `raw.parquet` / `outputs.parquet` gồm `topic, offset, append_ts, key, payload`.
- `bench/latency.py`: tính latency theo bảng 10.1, xuất `latency.csv` và `latency_summary.json`.
- `bench/resources.py`: lấy `docker stats --no-stream --format json` mỗi giây cho container của engine được đo, xuất `resources.csv`.
- `bench/plots.py`: vẽ biểu đồ (matplotlib) vào `bench/results/plots/`:
  - CDF latency theo engine/tải
  - Boxplot p95 theo tải
  - Cột throughput E3
  - Đường CPU/RAM theo thời gian
  - Batch duration của Spark theo thời gian
- `bench/summarize.py`: gộp tất cả run thành `bench/results/summary.json` và `summary.md` (các bảng số để chép vào báo cáo).

Kết quả thô trong `bench/results/` được gitignore, **trừ** `summary.json`, `summary.md` và `plots/`.

### Hoàn thành khi

E1–E4 có đủ 3 lần chạy mỗi cấu hình. `summary.md` có bảng số liệu và biểu đồ được tạo lại bằng một lệnh.

## 11. Phase 7: Phân tích và báo cáo

Tạo `docs/SPARK_VS_FLINK_EVALUATION.md`. Phần số liệu điền tự động từ `summary.json`. Phần phân tích để **khung và gợi ý**; nhóm tự viết lời văn cuối cùng (yêu cầu liêm chính học thuật).

### 11.1 Khung lý thuyết chung (độc lập với dự án)

Trình bày trước, không gắn với ICU:

| Khía cạnh | Native streaming (Flink) | Micro-batch (Spark Structured Streaming) |
|---|---|---|
| Mô hình thực thi | Toán tử chạy liên tục, record đi qua pipeline ngay | Chia luồng thành các batch nhỏ, mỗi batch là một job có lập kế hoạch và thực thi |
| Latency | Thấp, gần mức xử lý một record | Có sàn bằng trigger + chi phí cố định mỗi batch |
| Throughput | Tốt; chi phí mỗi record cố định | Có thể cao nhờ gom batch và xử lý vector (Arrow) |
| Thời gian sự kiện | Watermark lan truyền liên tục; idleness | Watermark tính lại ở ranh giới batch, toàn query |
| Dữ liệu trễ | Side output tường minh | Bị loại trong toán tử stateful; chỉ có metric |
| State | Keyed state, timer theo event/processing time | State store theo batch; `applyInPandasWithState` / `transformWithState` |
| Chịu lỗi | Checkpoint không đồng bộ bằng barrier (biến thể Chandy–Lamport) | Offset log + commit log + state store version theo batch |
| Batch + stream | DataStream / Table API, batch là trường hợp đặc biệt của stream | Cùng DataFrame API cho batch và stream; hệ sinh thái Spark rộng |
| Vận hành | Cluster riêng (JM/TM) | Driver + executor; tích hợp tự nhiên với hệ sinh thái lakehouse |

### 11.2 Áp dụng vào dự án: giả thuyết cần kiểm chứng

Đây là **giả thuyết**. Báo cáo chỉ kết luận khi có số đo ủng hộ, và phải nêu rõ nếu số đo bác bỏ.

| # | Giả thuyết | Kiểm chứng bằng |
|---|---|---|
| H1 | Latency cửa sổ/alert của Spark cao hơn Flink, và tăng theo trigger interval | E2 |
| H2 | Ở chế độ catch-up, Spark đạt throughput ngang hoặc cao hơn PyFlink nhờ batch Arrow lớn so với bundle 50 record của PyFlink | E3 |
| H3 | Với dữ liệu đúng thứ tự, hai engine cho kết quả giống nhau; khác biệt chỉ nằm ở ca late/duplicate và ở khả năng quan sát chúng | E1 |
| H4 | Sau sự cố, cả hai đều không mất dữ liệu nhưng có bản ghi trùng (at-least-once); khóa idempotent của Writer xử lý được | E4 |
| H5 | Tài nguyên khi rảnh của Spark (driver JVM + Python worker) khác Flink (JM + TM + Python) | E2, `resources.csv` |
| H6 | Trigger nhỏ làm tăng số batch và chi phí CPU khi tải thấp | E2 |

### 11.3 Lợi và hại trong bối cảnh dự án

Với mỗi khía cạnh, ghi **bằng chứng** (số đo hoặc hành vi quan sát được) và **hệ quả cho BR**:

- **BR-3 / BR-4** (alert và dashboard dưới 2 giây): latency E2 có đáp ứng không, ở trigger nào?
- **BR-1** (chất lượng dữ liệu): khả năng ghi DLQ cho late/duplicate.
- **BR-7** (alarm quality): độ khó cài logic episode/recovery trong `applyInPandasWithState` so với `KeyedProcessFunction` (số dòng code, số chỗ phải xử lý thủ công).
- **BR-8** (báo cáo theo ca, chưa làm): lợi thế dùng **cùng API Spark** cho job batch đọc dữ liệu lịch sử. Đây là điểm cộng tiềm năng của Spark mà Flink không có sẵn trong stack hiện tại.
- **Vận hành:** số container, kích thước image, thời gian khởi động, độ phức tạp cấu hình, cách xử lý khi reset topic (checkpoint Spark mất hiệu lực).

### 11.4 Giới hạn của đánh giá (phải có trong báo cáo)

- Chạy một node trên laptop, mỗi topic 1 partition. Không suy ra hành vi ở quy mô cluster.
- Cả hai bên đều dùng Python (PyFlink, PySpark), nên kết quả **không** đại diện cho Flink/Spark viết bằng Java/Scala.
- Dữ liệu mô phỏng có tính tất định, tối đa 4.800 msg/s.
- Dùng chung logic Python từ `jobs/flink/`, nên ưu/nhược đo được là của engine và cách tổ chức, không phải của thuật toán.
- Cửa sổ Spark đọc từ topic trung gian, nên có thêm một chặng Kafka.

## 12. Bẫy thường gặp (kiểm tra trước khi báo xong)

- [ ] `spark.sql.shuffle.partitions` đã đặt nhỏ. Số này **cố định sau khi checkpoint được tạo**, đổi thì phải xóa checkpoint.
- [ ] Mỗi run benchmark xóa checkpoint Spark sau khi reset topic.
- [ ] Trong hàm stateful đã sort theo `event_ts`, không giả định thứ tự đến.
- [ ] Định dạng `sample_time`, `window_end`, `alert_time` giống hệt Flink (microsecond, hậu tố `Z`), vì chúng tham gia sinh `message_id`.
- [ ] `producer` của alert là `icu-alert-engine` (validator yêu cầu).
- [ ] Header Kafka đầy đủ theo `business_contract.headers`.
- [ ] Output Spark **không** ghi vào topic business chính. Writer và Spring server không đọc `icu.alt.spark.*`.
- [ ] Python driver và worker cùng phiên bản; `pyarrow` / `pandas` nằm trong dải Spark hỗ trợ.
- [ ] Spark container mount `config/demo_patients.yaml` và đặt `ICU_PATIENT_CONFIG`, nếu không `validation.py` sẽ đọc sai registry.
- [ ] Không ai xóa log progress JSONL giữa chừng một run.
- [ ] Test cũ vẫn pass: `cd tests && PYTHONPATH=../jobs/flink:../scripts/demo:../services/storage_writer:. python -m unittest` (các test cần dependency ngoài thì ghi nhận nếu không chạy được).

## 13. Định nghĩa hoàn thành toàn bộ

1. `docker compose --profile spark up` chạy được Spark song song Flink, không ảnh hưởng luồng chính.
2. E1 đạt: output Spark khớp tham chiếu và Flink cho phần đóng được, mọi chênh lệch được giải thích.
3. E2–E4 có dữ liệu 3 lần chạy; `summary.md` và biểu đồ tạo lại được bằng `python bench/summarize.py && python bench/plots.py`.
4. Có đủ các tài liệu: `docs/spark/SPIKE_FINDINGS.md`, `docs/spark/DECISIONS.md`, `docs/spark/AI_USAGE.md`, `docs/SPARK_VS_FLINK_EVALUATION.md` (số liệu đầy đủ, phần phân tích để khung cho nhóm viết).
5. README có thêm mục ngắn "Chạy giải pháp thay thế Spark" và "Chạy benchmark".

---
