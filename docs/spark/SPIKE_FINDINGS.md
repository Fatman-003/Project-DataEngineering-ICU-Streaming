# Kết quả spike Phase 0 (S1–S6)

Mã spike: [`jobs/spark/spikes/spike.py`](../../jobs/spark/spikes/spike.py). Mỗi spike
ghi input bằng Spark batch write vào topic tạm `icu.alt.spike.*`, điều khiển query
bằng `processAllAvailable()` và in dòng `EVIDENCE {...}`. Các trích dẫn dưới đây
lấy nguyên văn từ log chạy ngày 2026-10-30.

**Môi trường.** Ubuntu 22.04, AMD EPYC 7313P (32 luồng), 251 GB RAM, Docker Engine
29.9.2, Compose 5.6.1. Image `icu-spark` (Spark 4.1.3, Python 3.12.15, xem
[DECISIONS.md](DECISIONS.md) D2–D4), `local[1]`. Kafka `apache/kafka:4.2.2` của
compose, topic 1 partition.

**Chạy lại** (cần `icu-kafka` đang chạy và các topic `icu.alt.spike.*` đã tạo):

```bash
docker run --rm --network icu-net \
  -e ICU_PATIENT_CONFIG=/opt/icu/config/demo_patients.yaml \
  -v "$PWD/jobs/flink:/opt/icu/jobs/flink:ro" -v "$PWD/jobs/spark:/opt/icu/jobs/spark:ro" \
  -v "$PWD/config/demo_patients.yaml:/opt/icu/config/demo_patients.yaml:ro" \
  icu-spark spark-submit /opt/icu/jobs/spark/spikes/spike.py s1   # s1 | s4 | s5 | s5b
```

## Tóm tắt

| Mã | Giả định | Kết luận |
|---|---|---|
| S1 | `mapInPandas` → `withWatermark` → `applyInPandasWithState(EventTimeTimeout)` chạy trong một query, mode `append` | **Đúng** |
| S2 | Hàm stateful có thấy dòng có timestamp ≤ watermark không | **Không thấy.** Spark lọc trước khi gọi hàm; `numRowsDroppedByWatermark` đếm đúng |
| S3 | Timeout event-time chạy trong no-data batch khi hết input | **Đúng**, với điều kiện watermark **lớn hơn hẳn** timestamp timeout |
| S4 | Kafka sink ghi cột `headers`, đọc lại qua `validate(..., require_headers=True)` | **Đúng**, 5/5 message hợp lệ |
| S5 | `spark.sql.shuffle.partitions` nhỏ cho latency tốt hơn rõ rệt so với 200 | **Đúng**: 0,84 s so với 42,5 s mỗi batch |
| S6 | Thứ tự dòng trong nhóm không theo `event_time`, phải tự sort | **Đúng**: dòng đến theo thứ tự offset, không theo event time |

Phát hiện ngoài kế hoạch: F1 (Kafka `fetch.max.wait.ms` chiếm khoảng 500 ms mỗi
batch) và F2 (Python 3.10 của image không parse được hậu tố `Z`).

## S1. Chuỗi toán tử trong một query

Query: Kafka → `mapInPandas(parse_validate)` gọi `parse_event` + `validate_event`
dùng chung → `withWatermark("event_ts", "2 seconds")` →
`groupBy("stay_key").applyInPandasWithState(..., "append", EventTimeTimeout)` →
memory sink. Query chạy, sinh cả dòng từ dữ liệu (`CALL`) lẫn dòng từ timeout
(`TIMEOUT`):

```
EVIDENCE {"spike": "S1 rows after A", "kind": "CALL", "watermark_s": null, "arrival_order_s": [3.0, 0.0, 7.0, 1.0, 9.0, 2.0, 8.0, 4.0, 6.0, 5.0]}
EVIDENCE {"spike": "S1 rows after A", "kind": "TIMEOUT", "watermark_s": 7.0, "timed_out_pending_s": [0.0, 1.0, ..., 9.0]}
```

**Kết luận.** Không cần tách query qua topic trung gian cho Q_wide và Q_alert.

## S2. Dữ liệu trễ

Batch A gửi giây 0..9 nên watermark thành `9 − 2 = 7 s`. Batch B gửi 10, 11, 12
cùng ba dòng cũ: 3 s (trễ rõ ràng), 7 s (đúng bằng watermark) và 7,5 s (ngay sau
watermark).

```
EVIDENCE {"spike": "S2 batch B", "batchId": 3, "numInputRows": 6, "watermark": "2026-10-16T20:00:07.000Z", "numRowsDroppedByWatermark": [2]}
EVIDENCE {"spike": "S1 rows after B", "kind": "CALL", "watermark_s": 7.0, "arrival_order_s": [10.0, 11.0, 12.0, 7.5]}
```

**Kết luận.**

1. Với `EventTimeTimeout`, Spark bỏ dòng có `event_ts ≤ watermark` **trước khi**
   gọi hàm. Hàm không bao giờ thấy dòng 3 s và 7 s, nên **không thể ghi DLQ
   `LATE_EVENT`** cho chúng. Chỉ có metric `numRowsDroppedByWatermark` (= 2, đúng).
2. Biên khác Flink 1 ms. Watermark Spark là `max − 2000 ms`, watermark Flink là
   `max − 2000 − 1 ms`. Flink coi trễ khi `ts ≤ max − 2001`, Spark loại khi
   `ts ≤ max − 2000`. Riêng event trễ **đúng 2000 ms** sẽ bị Spark loại nhưng
   Flink nhận.
3. Trong hàm, `getCurrentWatermarkMs()` là watermark của **batch trước**
   (batch A thấy `null`, batch B thấy 7 s). Dòng trong cùng batch không làm
   watermark tiến trong lúc xử lý batch đó.

Hệ quả thiết kế: xem [DECISIONS.md](DECISIONS.md) D7.

## S3. Timeout khi hết input

Sau batch dữ liệu cuối, Spark chạy một no-data batch (batch 2 và 4) với watermark
mới, và timeout event-time kích hoạt trong batch đó:

```
EVIDENCE {"spike": "S3 batches", "batches": [{"batchId": 0, "numInputRows": 0, ...}, {"batchId": 1, "numInputRows": 10, "watermark": "1970-01-01T00:00:00.000Z"}, {"batchId": 2, "numInputRows": 0, "watermark": "2026-10-16T20:00:07.000Z"}, {"batchId": 3, "numInputRows": 6, ...}, {"batchId": 4, "numInputRows": 0, "watermark": "2026-10-16T20:00:10.000Z"}]}
EVIDENCE {"spike": "S1 rows after B", "kind": "TIMEOUT", "watermark_s": 10.0, "timed_out_pending_s": [7.5, 10.0, 11.0, 12.0]}
```

**Biên kích hoạt.** Chạy lại với timeout đặt đúng bằng watermark kế tiếp
(`max(pending) − 2 s`, tức 7 s rồi 10 s; log `s3-boundary`): **không có dòng
`TIMEOUT` nào**. Timeout chỉ chạy khi `watermark > T`.

**Kết luận.**

- Timeout chỉ chạy **một lần** sau batch dữ liệu cuối. Khi input dừng hẳn,
  watermark không tiến thêm, nên các bucket/sample có `T ≥ max − 2 s` không bao
  giờ được chốt. Đây là phần "tail" ở mục 9.2 của plan; Flink cũng vậy khi
  input dừng hẳn.
- Biên `watermark > T` của Spark trùng khít điều kiện timer của Flink (xem D8).

## S4. Header Kafka

`mapInPandas` dựng message telemetry bằng `business_outputs.telemetry` (với
`producer="icu-spark"`) và cột `headers: array<struct<key:string, value:binary>>`
từ `business_contract.headers`. Đọc lại topic bằng Spark batch và gọi
`validate(message, "icu.telemetry", key, headers, require_headers=True)`:

```
EVIDENCE {"spike": "S4 validate", "messages": 5, "results": ["OK", "OK", "OK", "OK", "OK"], "header_names": ["content-type", "schema-version", "message-id", "event-type", "producer"]}
```

**Kết luận.** Không có khác biệt định dạng so với `DeadLetterSerializationSchema`
của Flink.

## S5. Số shuffle partition

Query stateful của S1, 25 batch, mỗi batch 4 dòng, bỏ 5 batch đầu (khởi động).
Đo `durationMs.triggerExecution`:

| `spark.sql.shuffle.partitions` | Trung vị | p90 | Max |
|---|---|---|---|
| 1 | 839 ms | 853 ms | 862 ms |
| 2 | 1.029 ms | 1.044 ms | 1.074 ms |
| 200 (mặc định) | **42.519 ms** | 42.761 ms | 43.027 ms |

**Kết luận.** Với 200 partition, mỗi batch phải lập lịch 200 task stateful, mỗi
task có state store và Python worker riêng, dù gần như tất cả rỗng. Kết quả là
chậm hơn khoảng 50 lần. Cấu hình chuẩn dùng `1`, tương đương parallelism 1 của
Flink. Số này cố định sau khi checkpoint được tạo (bẫy §12).

## S6. Thứ tự trong nhóm

Batch A gửi giây 0..9 theo thứ tự xáo trộn. Hàm nhận đúng thứ tự gửi (thứ tự
offset), không được sắp theo `event_ts`:

```
"arrival_order_s": [3.0, 0.0, 7.0, 1.0, 9.0, 2.0, 8.0, 4.0, 6.0, 5.0]
```

**Kết luận.** Hàm stateful phải tự sort theo `(event_ts, offset)`. Với 1 partition
Kafka, thứ tự offset ổn định nên dùng `offset` làm khóa phụ là tất định.

## F1. `fetch.max.wait.ms` của Kafka consumer chiếm khoảng 500 ms mỗi batch

Ngay cả query stateless chỉ có `mapInPandas`, mỗi batch 4 dòng vẫn mất khoảng
540 ms ở `addBatch`. Tách nguyên nhân bằng 3 biến thể (trung vị
`triggerExecution`):

| Biến thể | `fetch.max.wait.ms` mặc định (500) | `kafka.fetch.max.wait.ms=10` |
|---|---|---|
| Chỉ JVM (`selectExpr`, không Python) | 593 ms | 98 ms |
| `mapInPandas` stateless | 593 ms | 142 ms |
| `applyInPandasWithState` (state HDFS) | 829 ms | 402 ms |
| `applyInPandasWithState` (state RocksDB) | 855 ms | chưa đo |

Biến thể "chỉ JVM" cũng chịu khoảng 500 ms, nên khoản này **không** do Python hay
state. Kafka consumer của Spark đọc đúng range offset của batch; ở cuối range,
lệnh fetch không có thêm dữ liệu nên broker giữ request tới `fetch.max.wait.ms`.

**Ước lượng chi phí cố định mỗi batch** (khi `fetch.max.wait.ms=10`): khoảng 50 ms
cho offset log + commit log, khoảng 45 ms cho Python `mapInPandas`, và khoảng
260 ms cho toán tử `applyInPandasWithState` (state store + Python worker).
RocksDB không nhanh hơn HDFS ở quy mô state này.

Cấu hình này cần quyết định: xem [DECISIONS.md](DECISIONS.md) D10.

## F2. Python của image không parse được `event_time`

`apache/spark:4.1.3-...-python3-ubuntu` dùng Python 3.10.12:

```
$ python3 -c 'from datetime import datetime; datetime.fromisoformat("2026-10-16T20:00:00Z")'
ValueError: Invalid isoformat string: '2026-10-16T20:00:00Z'
```

Nếu không xử lý, `validation.parse_timestamp` sẽ từ chối **mọi** event. Image
cài thêm Python 3.12.15 (D3); spike chạy trên interpreter đó.
