# Giải pháp Spark: các quyết định thiết kế

Mỗi mục gồm bối cảnh, lựa chọn, lý do và phương án đã loại, theo mục 0.3 của
[`docs/plans/SPARK_ALTERNATIVE_PLAN.md`](../plans/SPARK_ALTERNATIVE_PLAN.md).
Bằng chứng spike nằm trong [`SPIKE_FINDINGS.md`](SPIKE_FINDINGS.md).

## D1. Baseline là HEAD `9c02f3e`, không phải `b49e7cf`

- **Bối cảnh.** Plan được viết theo `b49e7cf`. Sau đó có hai commit đổi contract
  Flink. `878e505` thêm context HR/RR/SBP/SpO2 theo bucket 1 giây vào
  `trigger_message` của alert. `9c02f3e` chuyển cửa sổ 5 phút sang accumulator
  Welford `[count, mean, M2]` và thêm các trường `var_*`.
- **Lựa chọn.** Tái hiện HEAD. Q_window tính `count`, `avg` và `var_pop` cho
  từng field. Q_alert giữ reservoir 1 giây cho 4 context signal bên cạnh MAP.
  (Người dùng đã xác nhận.)
- **Lý do.** Output phải khớp từng message với job Flink đang chạy, kể cả
  `trigger_message`. So với contract cũ thì E1 mất ý nghĩa.
- **Đã loại.** Làm đúng nguyên văn plan (sum/count, chỉ MAP): output Spark sẽ
  thiếu `var_*` và context, không bao giờ khớp được Flink.

## D2. Spark 4.1.3 (Scala 2.13, Java 17)

- **Bối cảnh.** Cần Spark 4.x có `applyInPandasWithState`, Python
  `StreamingQueryListener` và cột `headers` cho Kafka sink. Mọi bản pin phải ra
  đời ít nhất 2 tuần (ngày làm: 2026-10-30).
- **Lựa chọn.** `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu`, pin digest
  `sha256:9b0a6c2c860f5e7d18dd5270286fef32a09c7f5a7e2b0dbe5642de8a3a02ab3e`.
  Connector `spark-sql-kafka-0-10_2.13:4.1.3` (phát hành 2026-07-12) cùng
  `spark-token-provider-kafka-0-10_2.13:4.1.3`, `kafka-clients:3.9.1` và
  `commons-pool2:2.12.1` (đúng phiên bản trong POM của connector). Mọi jar đều
  kiểm SHA-256 lúc build. (Người dùng đã xác nhận.)
- **Lý do.** Là bản patch thứ 3 của dòng 4.1 nên ổn định hơn bản `.0`. Cả ba
  API cần dùng đều có từ 3.4/4.0.
- **Đã loại.** 4.1.4 và 4.2.1 (ra ngày 21–22/10/2026, chưa đủ 2 tuần); 4.2.0
  (bản đầu của dòng minor mới).

## D3. Python 3.12.15 trong image Spark

- **Bối cảnh.** Image chính thức có Python 3.10.12. Hàm `datetime.fromisoformat`
  của 3.10 không nhận hậu tố `Z` mà mọi `event_time` RAW đều dùng, nên
  `validation.parse_timestamp` dùng chung sẽ từ chối mọi event (SPIKE_FINDINGS F2).
  PyFlink chạy Python 3.12.3.
- **Lựa chọn.** Cài CPython 3.12.15 từ python-build-standalone bản `20261013`
  (pin SHA-256), và trỏ `PYSPARK_PYTHON` cùng `PYSPARK_DRIVER_PYTHON` vào đó để
  driver và worker cùng phiên bản.
- **Lý do.** Cùng minor version với PyFlink, nên logic dùng chung chạy giống
  hệt. Tarball đóng gói sẵn và có checksum nên pin chính xác được.
- **Đã loại.** PPA deadsnakes (không pin phiên bản ổn định được); dựng lại trên
  `python:3.12-slim` và tự cài Spark (plan yêu cầu base `apache/spark`); sửa
  code dùng chung cho hợp 3.10 (vi phạm ràng buộc bất biến của plan).

## D4. Gói Python: pandas 2.3.3, pyarrow 22.0.0, numpy 2.3.5

- **Bối cảnh.** PySpark 4.1.3 yêu cầu pandas ≥ 2.2.0, pyarrow ≥ 15.0.0 và
  numpy ≥ 1.22.
- **Lựa chọn.** pandas 2.3.3, pyarrow 22.0.0, numpy 2.3.5.
- **Lý do.** Đây là các bản cuối của những dòng phiên bản Spark 4.1 được phát
  triển cùng, và đã ra khoảng một năm. pandas 3.x đổi kiểu chuỗi mặc định và
  ngữ nghĩa copy, trong khi cầu nối Arrow của Spark 4.1 ra đời trước đó.
- **Đã loại.** pandas 3.0.x và pyarrow 25–26 (mới hơn ma trận kiểm thử của Spark 4.1).
- **Ghi chú.** Plan liệt kê PyYAML cho `registry.py`, nhưng file registry thực
  chất là tập con JSON của YAML và `registry.py` đọc bằng `json`, nên không cần
  gói YAML.

## D5. Nhãn output: `producer=icu-spark`, DLQ giữ nguyên

- **Bối cảnh.** Các hàm format dùng chung gắn cứng `producer="icu-flink"` cho
  telemetry/vitals, và `consumer_group="icu-flink-v2"` cho DLQ; group này tham
  gia sinh `dlq_id`. Alert phải giữ `producer="icu-alert-engine"` vì validator
  yêu cầu.
- **Lựa chọn.** Message telemetry và vitals của Spark đặt `producer="icu-spark"`;
  header Kafka được tính lại sau khi đổi. Envelope DLQ giữ nguyên như
  `dlq.rejection` tạo ra. (Người dùng đã xác nhận.)
- **Lý do.** `message_id` và `alert_id` không phụ thuộc `producer`, nên vẫn so
  khớp danh tính với Flink được trong khi nguồn gốc vẫn hiển thị rõ. Giữ nguyên
  envelope DLQ cho `dlq_id` trùng Flink với cùng offset nguồn và mã lỗi, nên ở E1
  chỉ cần join theo khóa.
- **Đã loại.** Đổi group của DLQ: phải dựng lại envelope ngoài `dlq.rejection`,
  tức chép lại bảng ánh xạ mã lỗi.

## D6. Kiến trúc cuối: một SparkSession, bốn query

- **Bối cảnh.** S1 đúng: chuỗi `mapInPandas → withWatermark →
  applyInPandasWithState(EventTimeTimeout)` chạy được trong một query.
- **Lựa chọn.** Giữ kiến trúc mục 3.1 của plan: Q_wide, Q_alert, Q_labs đọc RAW;
  Q_window đọc `icu.alt.spark.telemetry`. Q_wide ghi telemetry và DLQ bằng
  **một** Kafka sink, chọn topic theo cột `topic` của từng dòng thay cho
  `foreachBatch`. Cách này giữ đúng at-least-once của sink chuẩn và sẽ được kiểm
  chứng ở Phase 2.
- **Lý do.** Không cần topic trung gian ngoài chặng telemetry → window mà plan
  đã chủ định. Cột `topic` tránh phải chạy hai query cho cùng một toán tử stateful.
- **Đã loại.** `foreachBatch` (phải tự lo trùng lặp theo batch id); hai sink lọc
  theo `kind` (Spark sẽ chạy toán tử stateful hai lần, mỗi lần với checkpoint riêng).

## D7. Event trễ: Spark loại im lặng, đếm bằng metric

- **Bối cảnh.** S2: với `EventTimeTimeout`, Spark bỏ dòng có `event_ts ≤
  watermark` trước khi gọi hàm. Hàm không thấy dòng đó nên không ghi DLQ được.
  Watermark Spark lại là của batch trước.
- **Lựa chọn.** Q_wide và Q_alert dùng `EventTimeTimeout`. Dòng bị Spark loại
  được đếm bằng `stateOperators[].numRowsDroppedByWatermark` trong log progress
  JSONL. Trong hàm vẫn gọi `classify_event(ts, watermark, max_clean_ts)` cho các
  dòng lọt qua bộ lọc; trên thực tế nhánh `LATE_EVENT` sẽ không bao giờ xảy ra vì
  Spark đã lọc trước, nhưng vẫn giữ để logic khớp Flink.
- **Hệ quả cần báo cáo.** (1) Probe late của P04 không xuất hiện trong DLQ Spark;
  nó chỉ hiện trong metric. (2) Ở Flink, dedup chạy trước kiểm tra trễ, nên một
  bản trùng mà lại trễ sẽ thành `DUPLICATE_EVENT`; ở Spark nó bị loại như late
  trước khi tới dedup. (3) Biên trễ khác 1 ms (event trễ đúng 2000 ms: Spark loại,
  Flink nhận). Cả ba được xếp vào mục 3.3 "khác biệt ngữ nghĩa".
- **Đã loại.** `NoTimeout`/`ProcessingTimeTimeout` để hàm thấy được dòng trễ:
  chốt bucket sẽ phải chờ data mới của cùng key (sai lệch latency, đuôi không bao
  giờ đóng). Riêng `ProcessingTimeTimeout` còn làm Spark chạy no-data batch liên
  tục (đốt CPU) khi trigger mặc định. Một query "kiểm toán late" riêng: watermark
  và ranh giới batch của nó khác Q_wide, nên đếm không nhất quán.

## D8. Giả lập timer Flink bằng timeout Spark

- **Bối cảnh.** Timer event-time của Flink kích hoạt khi `wm_f ≥ T`, với
  `wm_f = max − 2001 ms`. S3: timeout Spark chỉ kích hoạt khi `wm_s > T`, với
  `wm_s = max − 2000 ms`.
- **Lựa chọn.** Hàm stateful đặt timeout ở mốc timer nhỏ nhất còn pending,
  đúng `T` mà Flink dùng (wide: `T = event_time`; MAP: `T = bucket_start + 999`),
  và mỗi lần được gọi thì chốt mọi mốc thỏa `T < wm_s`.
- **Lý do.** `wm_s > T` ⇔ `max − 2000 ≥ T + 1` ⇔ `wm_f ≥ T`. Điều kiện chốt
  **trùng khít** Flink trên cùng `max`; khác biệt duy nhất là thời điểm `max`
  được cập nhật (theo batch so với theo chu kỳ 200 ms).

## D9. `local[1]`, `spark.sql.shuffle.partitions=1`

- **Bối cảnh.** S5: 1 partition: 839 ms/batch; 2: 1.029 ms; 200 (mặc định): 42.519 ms.
- **Lựa chọn.** Cấu hình chuẩn `local[1]` + 1 shuffle partition, tương đương
  parallelism 1 của Flink. Cấu hình lớn hơn chỉ dùng ở E6.
- **Lưu ý vận hành.** Số partition gắn vào checkpoint; đổi số này thì phải xóa
  checkpoint.

## D10. `kafka.fetch.max.wait.ms=10` cho source Spark

- **Bối cảnh.** F1: mỗi micro-batch mất khoảng 500 ms chỉ để chờ fetch ở cuối
  range offset (mặc định `fetch.max.wait.ms=500`), kể cả query không dùng Python.
  Đặt `kafka.fetch.max.wait.ms=10` thì batch stateful giảm từ 829 ms xuống 402 ms.
  KafkaSource của Flink poll liên tục nên không chịu khoản chờ này cho mỗi bản ghi.
- **Lựa chọn.** Cấu hình chuẩn đặt `kafka.fetch.max.wait.ms=10` qua biến
  `SPARK_KAFKA_FETCH_MAX_WAIT_MS` (mặc định 10). Đây là cấu hình client Kafka
  thông thường, không phải viết lại logic. Ở E2, chạy thêm một biến thể dùng
  mặc định 500 ms để báo cáo minh bạch ảnh hưởng của nó. (Người dùng đồng ý
  tiếp tục theo đề xuất.)
- **Lý do.** Spark chỉ fetch khi range offset của batch có dữ liệu, nên giá trị
  nhỏ không gây poll bận khi rảnh.
- **Đã loại.** Giữ mặc định 500 ms: so sánh latency khi đó chủ yếu đo một tham
  số client chứ không đo engine.

## D11. TTL dedup theo event time

- **Bối cảnh.** Flink dùng TTL 24 giờ theo **processing time** cho state dedup.
  Hàm stateful Spark không có TTL theo processing time cho từng phần tử trong map.
- **Lựa chọn.** `seen_ids` lưu `event_id → event_ts`, và bỏ các mục cũ hơn
  `watermark − 24h` mỗi lần hàm chạy.
- **Lý do.** Trong mọi kịch bản demo và benchmark (tối đa vài chục phút event
  time), không mục nào hết hạn ở cả hai engine, nên kết quả dedup giống nhau.
  Khác biệt chỉ xuất hiện khi replay dữ liệu cũ hơn 24 giờ; ghi vào mục giới hạn.

## D12. Quy tắc key/header của sink viết lại trong `records.py`

- **Bối cảnh.** Mọi sink Flink dùng `DeadLetterSerializationSchema` (Java). Key
  là `stay_id`, nếu không có thì lần lượt `mapped_stay_id`, `kafka_key`, `dlq_id`;
  header lấy theo field. Python không import được code Java.
- **Lựa chọn.** `records.kafka_record` viết lại **riêng phần chọn key** (khoảng
  10 dòng). Header lấy từ `business_contract.headers`, vốn cho cùng tên, cùng thứ
  tự với Java. `records.source_record` dựng envelope nguồn giống
  `KafkaMetadataDeserializer` để `parse_and_validate`/`dlq.rejection` dùng chung
  nhận đúng input. Cả hai được unit test trong `tests/spark/test_records.py`.
- **Lý do.** Đây là phần duy nhất không có bản Python dùng chung; toàn bộ logic
  nghiệp vụ vẫn import từ `jobs/flink/`.

## D13. Ngân sách tài nguyên khi benchmark

- **Bối cảnh.** Plan yêu cầu tổng CPU/RAM bằng nhau cho hai engine
  (`docker/compose.bench.yaml`). Ví dụ trong plan: Flink JM 0,5 CPU/1 GB + TM
  1,5 CPU/2 GB; Spark 2 CPU/3 GB.
- **Lựa chọn.** Flink JM 0,5 CPU/1 GB + TM 1,5 CPU/2,5 GB; Spark 2 CPU/3,5 GB
  (driver `--driver-memory 2g`). Tổng mỗi bên: 2 CPU/3,5 GB.
- **Lý do.** TaskManager đặt `taskmanager.memory.process.size: 2048m`. Python
  worker là process riêng, được Flink tính vào managed memory nhưng kernel tính
  vào giới hạn container, nên giới hạn đúng 2 GB dễ bị OOM-kill. Spark được cộng
  thêm cùng mức để tổng hai bên vẫn bằng nhau. Sẽ kiểm tra không có OOM ở Phase 6.
- **Đã loại.** Đúng ví dụ của plan (Flink 3 GB, Spark 3 GB): TM không có khoảng dư.

## D14. Q_labs làm sớm ở Phase 1; đếm bằng `foreachBatch`

- **Bối cảnh.** Healthcheck của Phase 1 cần ít nhất một query thật đang chạy.
  Plan đặt Q_labs ở Phase 2.
- **Lựa chọn.** Làm Q_labs ngay ở Phase 1: `mapInPandas` gọi
  `parse_and_validate(source, "labevents")`, rồi `foreachBatch` ghi số đếm theo
  `status`/`error_type` của từng batch vào `/opt/icu/metrics/labs_counts.jsonl`.
  Không ghi business topic, đúng như Flink chỉ `print`.
- **Lý do.** `foreachBatch` chỉ dùng cho metric, không cho output business, nên
  không ảnh hưởng tính at-least-once của các topic so sánh.
- **Ghi chú.** Q_labs không dedup và không phân loại event-time như nhánh lab
  của Flink. Không có output business nên điều này không ảnh hưởng E1.

## D15. Tên module trong `jobs/spark/`

- **Bối cảnh.** `services/storage_writer/config.py` và `jobs/spark/config.py`
  cùng tên `config`. Khi test chèn cả hai thư mục vào `sys.path`, module này che
  module kia, làm 3 file test cũ lỗi import.
- **Lựa chọn.** Đặt tên `spark_config.py` và `spark_topics.py`. Các module còn
  lại (`records`, `metrics_listener`, `q_*`, `main`) không trùng tên nào trong repo.
