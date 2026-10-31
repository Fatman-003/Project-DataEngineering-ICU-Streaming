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

## D16. Dòng bị từ chối: `event_ts = NULL`, key `__invalid__`

- **Bối cảnh.** Plan đề xuất `event_ts` bằng timestamp Kafka cho dòng không parse
  được. Timestamp Kafka là giờ thực, còn event time của demo/benchmark nằm ở quá
  khứ, nên watermark sẽ nhảy lên giờ thực và loại toàn bộ dữ liệu sau đó (S7).
- **Lựa chọn.** Mọi dòng `REJECTED` (PARSE/SCHEMA/VALUE) có `event_ts = NULL` và
  key `__invalid__`. Hàm stateful phát DLQ ngay, không lưu state.
- **Lý do.** S7: `NULL` không bị loại như late và không làm watermark tiến, đúng như
  Flink tách dòng bị từ chối trước bước gán timestamp.

## D17. Xử lý theo thứ tự offset, không theo `event_ts`

- **Bối cảnh.** Plan (§6.4, bẫy §12) yêu cầu sort theo `event_ts` trong hàm
  stateful. Flink với parallelism 1 và 1 partition xử lý theo **thứ tự đến**. Dedup
  (bản đến trước được giữ), `IDEMPOTENCY_CONFLICT` (sửa sample đã phát) và
  `max_clean_ts` đều phụ thuộc thứ tự đến chứ không phụ thuộc event time.
- **Lựa chọn.** Sort theo `offset` của Kafka (S6: Spark không đảm bảo thứ tự).
  Việc chốt theo event time đã do điều kiện watermark quyết định (D8), nên không
  phụ thuộc thứ tự xử lý.
- **Lý do.** Sort theo `event_ts` có thể đảo bản gốc và bản trùng hoặc bản sửa, và
  khi đó ra DLQ khác Flink. Test parity `tests/spark/test_wide_logic.py` khẳng định
  kết quả trùng Flink.

## D18. Giữ `applyInPandasWithState`; không dùng `transformWithStateInPandas`

- **Bối cảnh.** `applyInPandasWithState` giữ **một object state cho mỗi key**, nên tập
  `event_id` dedup của một bệnh nhân bị serialize lại mỗi batch. API v2
  `transformWithStateInPandas` có `MapState`, timer và TTL, giống
  `KeyedProcessFunction` của Flink hơn.
- **Lựa chọn.** Giữ `applyInPandasWithState` như plan. State gồm 3 cột JSON/long:
  `seen` (`event_id → event_ts`), `pending` (`ts → samples`), `maximum`.
- **Lý do.** S9: JSON state ổn định khoảng 900 ms/batch tới 120 nghìn id; `MapState`
  của API v2 không xử lý xong nổi 10 nghìn event trong 30 phút vì mỗi thao tác là
  một RPC. Ngoài ra S8 cho thấy API v2 không có TTL ở chế độ `EventTime`, nên cũng
  không thay được TTL của Flink.
- **Hệ quả cần báo cáo.** Đây là giới hạn của **Python API** Spark 4.1.3. Bản Scala
  của `transformWithState` không có chi phí RPC này (mục giới hạn §11.4).

## D19. Dedup theo từng `stay_id` thay vì toàn cục

- **Bối cảnh.** Flink dedup bằng keyed state theo `chartevents:id:<event_id>`, tức
  là toàn cục. Spark gom state theo `stay_id` (D18).
- **Lựa chọn.** `seen` nằm trong state của từng `stay_id`.
- **Lý do và giới hạn.** `event_id` của simulator chứa `stay_id`, nên hai cách cho
  cùng kết quả. Chỉ khi hai bệnh nhân gửi trùng `event_id` thì Flink báo
  `DUPLICATE_EVENT` còn Spark không. Ghi vào mục giới hạn.

## D20. Q_window: aggregate native của Spark, đọc telemetry của Spark

- **Bối cảnh.** Flink tính cửa sổ trên wide stream bằng accumulator Welford
  `[count, mean, M2]` (`window_aggregates.py`), rồi `aggregate_result` suy ra
  `avg_*` và `var_* = M2 / count` (khi `count ≥ 2`).
- **Lựa chọn.** Q_window đọc `icu.alt.spark.telemetry` (thêm một chặng Kafka, đo
  riêng ở E2). Dùng `groupBy(window(sample_ts, "5 minutes", "1 minute"), stay_id)`
  với `count`, `avg`, `var_pop` native cho từng field, output mode `append`. Sau đó
  `window_logic.accumulator` dựng lại `[count, mean, var_pop × count]` và gọi
  `aggregate_result` dùng chung để format (`producer=icu-spark`).
- **Lý do.** Phần tính toán số học là của engine (đúng mục tiêu so sánh engine),
  còn mọi quy tắc contract (null khi thiếu, `var` chỉ khi `count ≥ 2`, danh tính
  `message_id`) vẫn dùng chung. Sai số so với Welford của Flink đo được tối đa
  khoảng `9e-14`, nằm trong ngưỡng `1e-9` của plan.
- **Đã loại.** Gom giá trị rồi gọi `add_record` trong Python: lặp lại thuật toán
  của Flink thay vì đo engine, và tốn thêm một bước Python.
- **Khác biệt có thể có.** Sample thiếu field mà Flink chốt bằng timer có
  timestamp `T ≤ watermark`. Ở Flink, sample đó vẫn vào các cửa sổ chưa đóng. Ở
  Spark, Q_window có watermark riêng dựa trên max `sample_ts` của topic telemetry,
  nên sample chốt muộn có thể bị loại nếu `T ≤ max − 2 s`. Kịch bản chuẩn không có
  sample thiếu field nên không gặp; báo riêng nếu E1–E4 gặp.

## D21. Số cửa sổ kỳ vọng của kịch bản chuẩn là 20, không phải 24

- **Bối cảnh.** README ghi "vitals_5m = 24" cho kịch bản 6 phút.
- **Quan sát.** Với `--start-time` cố định (dữ liệu 20:00:00–20:05:59), cả Flink và
  Spark đều đóng đúng 5 cửa sổ mỗi bệnh nhân (kết thúc 20:01 đến 20:05) = 20. Các
  cửa sổ kết thúc 20:06–20:10 là phần "tail" (§9.2), chỉ đóng khi có dữ liệu mới đẩy
  watermark qua. Con số 24 có lẽ đến từ lần chạy không cố định `--start-time`, khi
  đồng hồ demo nối tiếp dữ liệu cũ.
- **Hệ quả.** Phase 5 dùng định nghĩa "cửa sổ đóng được" thay cho con số trong README.

## D22. Q_alert: parse riêng, chỉ nhận dòng hợp lệ, không ghi DLQ

- **Bối cảnh.** Ở Flink, operator hypotension nhận stream `clean` (đã validate,
  dedup, phân loại thời gian) và lọc còn MAP + HR/RR/SBP/SpO2. Plan §3.1 cho Q_alert
  đọc RAW độc lập với Q_wide.
- **Lựa chọn.** Q_alert dùng chung bước `parse_step` với Q_wide. Dòng `REJECTED` bị
  lọc bằng `where(status = 'VALID')` trước `groupBy`. Dedup, kiểm tra trễ và lọc
  signal nằm trong `AlertLogic.accept`. Duplicate và late bị bỏ im lặng vì Q_wide
  đã ghi DLQ cho chúng; như vậy mỗi lỗi chỉ có một bản ghi DLQ, giống Flink.
- **Logic.** `AlertLogic` chép lại `process_element`/`on_timer` của
  `flink_hypotension`, gọi các hàm dùng chung `bucket_add`, `median_map` và
  `advance_episode`. Timer của bucket `T = start + 999` được chốt khi watermark Spark
  `> T` (D8). Episode lưu dạng JSON như Flink.
- **Kiểm chứng.** `tests/spark/test_alert_logic.py` chạy lại **nguyên văn** 27 test
  của `test_alarm_quality.py` và `test_hypotension_context.py` (gồm các ca A–K
  trong `ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md`) trên `AlertLogic`, thông qua
  một fixture có cùng giao diện với fixture Flink. Hai test bị bỏ qua vì chỉ kiểm
  tra cách nối dây của PyFlink (`MapStateDescriptor` TTL, `filter/key_by/uid`).
  `accept(..., deduplicate=False)` chỉ dùng cho fixture, để mô phỏng
  `process_element` khi không có operator dedup phía trước.
- **Chi phí.** Q_wide và Q_alert cùng parse/validate RAW bằng Python, nên chi phí
  này bị trả hai lần. Đây là hệ quả của kiến trúc nhiều query trong plan, và cần
  nêu khi so tài nguyên ở E2/E3.

## D23. Ground truth tính từ RAW đã thu, theo thứ tự offset

- **Bối cảnh.** Plan §9.1 đề xuất sinh lại event bằng `demo_events(...)`. Nhưng
  `run_demo` có `skip_missed_ticks`: khi producer bị trễ, nó **bỏ tick**, nên input
  thật có thể khác input sinh lại.
- **Lựa chọn.** `bench/reference.py` tính kỳ vọng từ chính các bản ghi RAW mà
  `bench/collect.py` đọc từ Kafka, theo thứ tự offset, bằng vòng lặp Python thuần
  (validate → dedup lần thấy đầu → ghép wide → trung bình/phương sai cửa sổ bằng
  `statistics` → `advance_episode` tuần tự). Nó không dùng watermark, timer hay code
  của engine nào. Unit test `tests/bench/test_reference.py` chạy cùng hàm trên output
  của generator và khẳng định các con số kỳ vọng (1440 / 20 / 1 alert P02).
- **Ngưỡng "đóng được" (§9.2) quy đổi theo watermark của cả hai engine.** Sample
  thiếu field ở `T`: cần event có `event_time ≥ T + 2001 ms`. Cửa sổ kết thúc ở
  `E`: `≥ E + 2000 ms`. Bucket MAP bắt đầu ở `S`: `≥ S + 3000 ms`. Sample đủ 4 field
  luôn được tính, vì cả hai engine phát ngay khi đủ.
- **Event phụ thuộc thứ tự đến.** Event có `event_time ≤ max(trước đó) − 2000 ms` có
  thể bị coi là trễ hoặc không, tùy watermark lúc nó đến. Reference loại chúng ra và
  báo riêng. Ví dụ đã gặp: Q_alert bật sau khi dữ liệu đã có, đọc catch-up trong một
  batch, nên chỉ 1/3 probe late bị loại (`dropped_by_watermark.alert = 1`).
- **Đã loại.** So Spark với Flink trực tiếp: không phát hiện được lỗi chung của cả
  hai engine.

## D24. Phạm vi so DLQ

- `bench/compare.py` chỉ so DLQ có nguồn `icu.v2.chartevents`. Flink còn validate,
  dedup và phân loại trễ cho labevents rồi ghi DLQ; Q_labs của Spark chỉ đếm (D14).
  Số DLQ lab của từng engine được in riêng (`labevents=`). Trong mọi kịch bản demo,
  lab đều hợp lệ nên số này bằng 0.

## D25. Mức tải E2/E4 và kích thước E3 theo năng lực đo được

- **Bối cảnh.** Plan đặt tải E2 ở 24, 2.400 (HF100) và 4.800 (HF200) msg/s, và E3
  nạp sẵn HF200 × 10 phút (khoảng 2,9 triệu event). Dưới ngân sách D13 (TaskManager
  1,5 CPU), job PyFlink chỉ xử lý được **khoảng 430 event RAW/s** (E3 r1: 576.001
  event trong 1.340 s; đo lại khi đang chạy: 428–443/s). Pipeline có 6 operator Python
  nối tiếp, mỗi bản ghi đi qua cầu JVM↔Python dưới dạng JSON. Ở HF100/HF200, Flink
  chỉ xả backlog được sau khoảng 28/56 phút, và "latency" khi đó là thời gian chờ
  trong hàng đợi, tăng tuyến tính theo thời gian chạy.
- **Lựa chọn** (người dùng duyệt). E2 dùng ba mức **bền vững** dưới năng lực của engine
  chậm hơn: `normal` 24 msg/s (6%), `speed5` 120 msg/s (28%), `speed10` 240 msg/s
  (56%). Hai mức sau tạo bằng `run_icu_demo.py --speed N`: event time chạy nhanh N lần,
  pacing vẫn theo thời gian thực, wall time vẫn 5 phút. Thêm **một** run quá tải HF100
  cho mỗi engine (r1) để minh họa. E3 giảm còn HF200 × 2 phút (576 nghìn event). E4
  chạy ở `speed10`.
- **Lý do.** Mục tiêu của E2 là latency xử lý, nên phải đo khi engine theo kịp tải. Năng
  lực tối đa đã được E3 đo riêng. Ở catch-up, Spark gom 576 nghìn event vào một
  micro-batch (Q_wide 61 s, Q_alert 92 s) và đạt khoảng 6.000 event/s, nên HF100 quá
  tải Flink nhưng không quá tải Spark. Đây chính là điểm minh họa cho H2.
- **Hệ quả.** Ở `speed5`/`speed10`, một giây wall chứa 5/10 giây event time, nên cửa
  sổ và bucket MAP đóng nhanh hơn tương ứng, và mỗi run có nhiều cửa sổ/alert hơn.
  Định nghĩa latency (§10.1, theo LogAppendTime) không đổi.
- **Đã loại.** Giữ HF100/HF200 cho cả 3 lần lặp (15–20 giờ máy, số liệu chủ yếu đo
  hàng đợi); bỏ giới hạn CPU (worker Python của mỗi engine đơn luồng, và hai engine
  không còn cùng ngân sách).

## D26. Ma trận rút gọn (`--lite`) cho mức so sánh demo

- **Bối cảnh.** Người dùng xác định mục tiêu là demo và so sánh ở mức vừa phải, không
  cần đủ 3 lần lặp cho mọi cấu hình như plan §10.1.5.
- **Lựa chọn.** `bench/run.py --lite`: một lần chạy cho mỗi cấu hình; E2 gồm 3 mức tải
  × (Flink, Spark trigger mặc định, Spark trigger 5 s); E3 và E4 mỗi engine một lần. Bỏ
  trigger 1 s, biến thể `fetch.max.wait` 500 ms và run quá tải HF100. Các run đã chạy
  trước đó (E1 × 3, Spark trigger 1 s ở tải normal) vẫn được giữ trong tổng hợp.
- **Hệ quả cho báo cáo.** Phần lớn ô trong bảng chỉ có n = 1, nên không có khoảng
  min–max. Báo cáo cần nêu đây là số đo một lần, không có khoảng tin cậy. Ma trận đầy
  đủ vẫn chạy được bằng `python bench/run.py --plan all`.
