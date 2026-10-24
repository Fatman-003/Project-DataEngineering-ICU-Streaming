# Kafka message contract

Đây là contract hiện tại cho hai RAW input và bốn output của dự án. Mô tả
theo [contracts.py](../jobs/flink/contracts.py),
[business_contract.py](../jobs/flink/business_contract.py), các serializer
và consumer. Luồng xử lý ở [KAFKA_FLINK_FLOW.md](../KAFKA_FLINK_FLOW.md);
schema lưu trữ ở [DATABASE_DESIGN.md](DATABASE_DESIGN.md).

## Topic, key và phiên bản

| Topic | Kafka key UTF-8 | Schema Version / event type | Đích |
|---|---|---|---|
| `icu.v2.chartevents` | `stay_id` dạng chuỗi | `2.0`, không có `event_type` | Flink RAW input |
| `icu.v2.labevents` | `mapped_stay_id` dạng chuỗi | `2.0`, không có `event_type` | Flink RAW input |
| `icu.telemetry` | `stay_id` dạng chuỗi | `1.0` / `TELEMETRY_SAMPLE` | Gateway; `patient_telemetry_stream` |
| `icu.vitals.5m` | `stay_id` dạng chuỗi | `1.0` / `VITALS_WINDOW_5M` | Gateway; `vitals_sliding_window_5m` |
| `icu.alerts` | `stay_id` dạng chuỗi | `1.0` / hai event type bên dưới | Gateway; `sepsis_alert_events` |
| `icu.deadletter` | Key gốc nếu có, fallback `dlq_id` | `1.0` / `DLQ_EVENT` | Điều tra; không ghi business DB |

Bootstrap tạo mỗi topic với **1 partition, replication factor 1**. DLQ đặt
`max.message.bytes=8388608`; retention/cleanup kế thừa broker, không có policy
7/14/30 ngày riêng trong code. Kafka giữ thứ tự record trong một partition,
không sắp xếp lại theo Event Time. Các topic khác nhau không có thứ tự chung.

## RAW input được giữ nguyên

Simulator phát một signal trên mỗi RAW message. `icu.v2.chartevents` có đúng
các field: `schema_version`, `event_id`, `subject_id`, `hadm_id`, `stay_id`,
`bed_id`, `itemid`, `signal_name`, `event_time`, `valuenum`, `valueuom`.
`icu.v2.labevents` thay `stay_id` bằng `mapped_stay_id`, `signal_name` bằng
`lab_name`; các field còn lại giữ nguyên. Không thêm metadata business/header
bắt buộc của mục tiếp theo vào RAW.

Schema RAW yêu cầu chính xác `2.0` và đúng tập field, không nhận field thừa.
ID số là positive integer, bộ identity phải khớp P01–P04; `event_id` là chuỗi
không rỗng, không phải UUID bắt buộc. `event_time` phải có timezone, được
chuẩn hóa UTC khi xử lý. Giá trị/unit/itemid theo từ điển
[contracts.py](../jobs/flink/contracts.py); sáu signal HR/MAP/SBP/DBP/SpO2/RR
và lab Lactate vẫn giữ nguyên. JSON object/UTF-8/value/schema không hợp lệ,
duplicate và quá muộn đi vào DLQ; dữ liệu bất thường nhưng hợp lệ như MAP 62
vẫn tham gia xử lý. Lab hợp lệ được log, chưa có business lab projection.

## Quy ước chung cho business và DLQ

UTF-8, một JSON object/message, field `snake_case`; số phải là JSON number
hữu hạn, không nhận chuỗi số, boolean thay số, NaN/Infinity hoặc float tràn.
Không nhận JSON có field trùng. Consumer business bỏ qua field lạ, chấp nhận
`schema_version` dạng `1.<minor>` và từ chối major khác. Producer hiện phát
`1.0`. UUID canonical chữ thường, variant RFC 4122, version 4/5/7 được
validator hỗ trợ; các ID do Flink tạo hiện dùng **UUIDv5**, không yêu cầu UUIDv7.

| Metadata | Kiểu / bắt buộc | Ý nghĩa |
|---|---|---|
| `schema_version` | string / có | Phiên bản body, hiện `1.0` |
| `message_id` | UUID string / có | ID ổn định theo loại event và logical key |
| `event_type` | string enum / có | Khớp topic |
| `produced_at` | UTC timestamp string / có | Thời điểm tạo output, khác Event Time |
| `producer` | string không rỗng / có | `icu-flink`, `icu-alert-engine`, hoặc service phát event |
| `trace_id` | string / tùy chọn | Đúng 32 ký tự hex thường nếu có; không nhận null |

DLQ cũng có toàn bộ metadata này; `producer` của DLQ là tên group phát hiện
lỗi. Metadata không ghi vào bảng business. `message_id` không phải PK DB và
không có nghĩa là mỗi lần delivery phải sinh ID mới. Flink dùng UUIDv5 ổn
định để replay cùng logical event có cùng ID; `produced_at` có thể khác.

Timestamp business là RFC 3339 **đã chuẩn hóa UTC**, hậu tố `Z` hoặc `+00:00`,
ví dụ `2026-10-16T20:00:00.000000Z`. Offset khác UTC không được validator
business nhận trực tiếp. Telemetry giữ microsecond; bucket alert và window
được biểu diễn milliseconds. `stay_id` là JSON integer `0 < stay_id < 2^63`;
Gateway/Writer còn kiểm tra registry cố định của demo.

### Kafka headers

Giá trị header encode UTF-8. Serializer business/DLQ phát:

| Header | Bắt buộc | Giá trị |
|---|---|---|
| `content-type` | Có | `application/json; charset=utf-8` |
| `schema-version` | Có | Body `schema_version` |
| `message-id` | Có | Body `message_id` |
| `event-type` | Có | Body `event_type` |
| `producer` | Có | Body `producer` |
| `trace-id` | Khi body có `trace_id` | Body `trace_id` |
| `dlq-source-topic` | Chỉ DLQ | Body `source_topic` |
| `dlq-error-code` | Chỉ DLQ | Body `error_code` |

Gateway/Writer yêu cầu header metadata khớp body. Nếu key có mặt, validator
kiểm tra key khớp `str(stay_id)`; serializer dự án luôn gán key theo bảng.
Ví dụ JSON dưới đây là **body**; khi tự publish cần gắn cả key và headers.

## `icu.telemetry`: một sample wide

Flink ghép LONG → WIDE theo `stay_id + sample_time`. Ví dụ các RAW component
cùng timestamp của P02: HR 88, MAP 62, SBP 110, SpO2 97 → một body dưới đây.
DBP/RR vẫn nằm trong RAW, chưa thuộc wide payload hay bảng telemetry.

Mọi field metadata chung đều bắt buộc, trừ `trace_id`.

| Field riêng | Kiểu | Bắt buộc / ràng buộc |
|---|---|---|
| `sample_time` | UTC timestamp string | Có; Event Time của sample |
| `stay_id` | integer | Có; positive int64 |
| `heart_rate` | number hoặc null | Tùy chọn; 0..350 bpm |
| `map` | number hoặc null | Tùy chọn; 0..300 mmHg |
| `sbp` | number hoặc null | Tùy chọn; 0..350 mmHg |
| `spo2` | number hoặc null | Tùy chọn; 0..100 % |

Ít nhất một measurement khác null. Flink hiện phát đủ bốn key, field thiếu là
null. Đủ bốn component thì phát ngay; thiếu component thì chờ Watermark chốt.
Timestamp khác nhau trong cùng millisecond vẫn là sample riêng nếu khác
microsecond. Sample chỉ có DBP/RR không phát wide object rỗng.

Nếu nhiều giá trị cùng component đến trước khi phát, chọn minimum xác định.
Sau khi sample đã phát, component cùng giá trị không phát lại; giá trị khác
→ DLQ `IDEMPOTENCY_CONFLICT`. Component quá Watermark bị loại từ upstream
dưới dạng Late Event, không sửa sample đã chốt. Không có correction/upsert.

```json
{
  "schema_version": "1.0",
  "message_id": "41c7902d-9d89-5534-ae88-27ebef349413",
  "event_type": "TELEMETRY_SAMPLE",
  "produced_at": "2026-10-16T20:00:00.100Z",
  "producer": "icu-flink",
  "sample_time": "2026-10-16T20:00:00.000000Z",
  "stay_id": 35727289,
  "heart_rate": 88,
  "map": 62,
  "sbp": 110,
  "spo2": 97
}
```

`message_id` phụ thuộc stay và sample_time UTC canonical. Writer insert vào
`patient_telemetry_stream`, Idempotency bằng PK `(sample_time, stay_id)` với
`ON CONFLICT DO NOTHING`. Replay khác payload nhưng trùng PK cũng không
overwrite DB; không suy ra rằng Writer tự phát hiện mọi conflict.

## `icu.vitals.5m`: trung bình cửa sổ

`event_type=VITALS_WINDOW_5M`, producer hiện là `icu-flink`; metadata chung
áp dụng như trên. Group theo stay, window **5 phút**, slide **1 phút**.

| Field riêng | Kiểu | Bắt buộc / ràng buộc |
|---|---|---|
| `window_end` | UTC timestamp string | Có; align theo phút, giây/microsecond bằng 0 |
| `stay_id` | integer | Có; positive int64 |
| `avg_heart_rate` | number hoặc null | Tùy chọn; 0..350 |
| `avg_map` | number hoặc null | Tùy chọn; 0..300 |
| `avg_sbp` | number hoặc null | Tùy chọn; 0..350 |
| `avg_spo2` | number hoặc null | Tùy chọn; 0..100 |

Mean tính từ các wide sample trên `[window_end − 5 phút, window_end)`; mỗi
field có sum/count riêng, bỏ null. Ít nhất một `avg_*` khác null; cửa sổ rỗng
không phát. Watermark đóng cửa sổ; không sửa cửa sổ đã đóng bằng dữ liệu đến
muộn. Không có `window_start` trong body, có thể suy ra từ `window_end`.

```json
{
  "schema_version": "1.0",
  "message_id": "ca646370-551b-5fb1-9f22-d46b612828c8",
  "event_type": "VITALS_WINDOW_5M",
  "produced_at": "2026-10-16T20:05:02.000Z",
  "producer": "icu-flink",
  "window_end": "2026-10-16T20:05:00.000Z",
  "stay_id": 35727289,
  "avg_heart_rate": 88.0,
  "avg_map": 62.0,
  "avg_sbp": 110.0,
  "avg_spo2": 97.0
}
```

Ví dụ mean bằng sample khi cửa sổ chỉ có sample đó. UUIDv5 `message_id` theo
stay/window_end. Writer insert `vitals_sliding_window_5m`, PK
`(window_end, stay_id)`, `ON CONFLICT DO NOTHING`.

## `icu.alerts`: tạo và cập nhật alert

### `SEPSIS_ALERT_CREATED`

Metadata chung áp dụng; `producer` bắt buộc **`icu-alert-engine`**.

| Field riêng | Kiểu | Bắt buộc / ràng buộc |
|---|---|---|
| `alert_time` | UTC timestamp string | Có; Event Time của bucket kích hoạt |
| `alert_id` | UUID string | Có; engine tạo UUIDv5 ổn định theo episode |
| `stay_id` | integer | Có; positive int64 |
| `alert_type` | enum string | Có; ba giá trị bên dưới |
| `severity` | enum string | Có; `WARNING`, `CRITICAL` |
| `trigger_value` | number hoặc null | Có field; hữu hạn nếu không null |
| `trigger_message` | string | Có; 1..1024 ký tự |
| `status` | enum string | Có; `ACTIVE` hoặc `FALSE_ALARM` |

| `alert_type` hợp lệ | Tình trạng triển khai |
|---|---|
| `HYPOTENSION_SUSTAINED` | Đang phát: MAP <65, 30 giây low evidence; severity `CRITICAL`, status `ACTIVE` |
| `QSOFA_HIGH_RISK` | Chỉ hỗ trợ enum; chưa tính qSOFA, thiếu GCS |
| `SENSOR_DISCONNECTED` | Chỉ hỗ trợ enum; chưa có rule cross-signal phát alert |

Tên event/table dùng `SEPSIS`, nhưng rule MAP là cảnh báo sớm, **không phải
chẩn đoán sepsis**. Xem [rule chi tiết](ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md).
`alert_id` theo `(stay_id, HYPOTENSION_SUSTAINED, episode_start_ms)`;
`message_id` theo stay/episode start trong namespace UUIDv5 của contract.
Không có cooldown cố định 15 phút; recovery/gap quyết định episode tiếp theo.

```json
{
  "schema_version": "1.0",
  "message_id": "7f24bcf2-0b83-5615-bb3b-93a76daf9637",
  "event_type": "SEPSIS_ALERT_CREATED",
  "produced_at": "2026-10-16T20:00:33Z",
  "producer": "icu-alert-engine",
  "alert_id": "73b0eea0-566e-59d1-ba91-7e5060040141",
  "alert_time": "2026-10-16T20:00:30.000Z",
  "stay_id": 35727289,
  "alert_type": "HYPOTENSION_SUSTAINED",
  "severity": "CRITICAL",
  "trigger_value": 62,
  "trigger_message": "MAP < 65 mmHg: >= 30s accumulated low evidence",
  "status": "ACTIVE"
}
```

Đây là ví dụ episode low từ 20:00:00, độc lập với lịch P02 mặc định bắt đầu
low ở T+12. Writer insert `sepsis_alert_events` bằng PK `(alert_time, alert_id)`;
replay cùng khóa không thêm hàng. Gateway bỏ qua create cùng identity đã có.

### `SEPSIS_ALERT_STATUS_CHANGED`

Metadata chung áp dụng, gồm `trace_id` tùy chọn. `producer` chỉ nhận `icu-api`
hoặc `icu-alert-engine`. Các field riêng đều bắt buộc:

| Field | Kiểu / ràng buộc |
|---|---|
| `alert_time` | UTC timestamp, cùng instant với alert gốc |
| `alert_id` | UUID, khớp alert gốc |
| `stay_id` | Positive int64, khớp alert gốc |
| `status` | `ACKNOWLEDGED` hoặc `FALSE_ALARM` |

```json
{
  "schema_version": "1.0",
  "message_id": "c1960d80-a09b-57f6-9f16-da63772769df",
  "event_type": "SEPSIS_ALERT_STATUS_CHANGED",
  "produced_at": "2026-10-16T20:01:00Z",
  "producer": "icu-api",
  "alert_time": "2026-10-16T20:00:30.000Z",
  "alert_id": "73b0eea0-566e-59d1-ba91-7e5060040141",
  "stay_id": 35727289,
  "status": "ACKNOWLEDGED"
}
```

Writer update đúng `(alert_time, alert_id, stay_id)`, không insert mới.
Không tìm thấy → `ALERT_NOT_FOUND`/DLQ. Lặp cùng status là idempotent; Gateway
không broadcast lần thứ hai. Chưa có status version chống update cũ đến sau,
chưa có REST API/UI nhập status. FastVerify có producer kiểm thử status event;
`icu-api` là tên producer được contract chấp nhận, không phải API đã triển khai.
Recovery MAP không tự tạo status event hoặc xóa alert đã lưu.

## `icu.deadletter`: lỗi kỹ thuật và dữ liệu

`event_type=DLQ_EVENT`, schema `1.0`; producer là `consumer_group` phát hiện
lỗi. Có metadata chung và các field sau do helper hiện tại phát đầy đủ:

| Field | Kiểu / ý nghĩa |
|---|---|
| `dlq_id` | UUIDv5 ổn định theo source topic/partition/offset, group và error code |
| `failed_at` | UTC timestamp lúc tạo DLQ |
| `source_topic` | String; một trong hai RAW hoặc ba business topic bị từ chối |
| `source_partition` | Integer >=0 |
| `source_offset` | Integer >=0 |
| `kafka_key` | String hoặc null, key gốc dạng text |
| `consumer_group` | String: `icu-flink-v2`, `icu-ui-gateway-v2`, `icu-timescale-writer-v2` |
| `error_stage` | `DESERIALIZATION`, `VALIDATION`, `PROCESSING`, `STORAGE` |
| `error_code` | String mã lỗi theo bảng dưới |
| `error_message` | String mô tả, tối đa 1024 ký tự |
| `retryable` | Boolean; có thể thử lại sau khi xử lý nguyên nhân |
| `attempt` | Integer >=1; thường 1, DB hết retry là 8 |
| `original_headers` | Object string → string/null; header trùng tên được rút về giá trị cuối |
| `original_payload` | Object nếu parse được JSON object, ngược lại null |
| `raw_payload_base64` | Base64 byte gốc nếu không parse được object, ngược lại null |

Giữ nguyên giá trị của JSON object parse được để điều tra, không hứa giữ byte
serialization/whitespace ban đầu. Khi UTF-8/JSON hỏng, field trùng, số tràn
hoặc payload không phải object, base64 giữ byte gốc. Tombstone không có byte
payload nên hai trường payload có thể đều null. Key/header không phải UTF-8
được decode thay ký tự lỗi; cơ chế bảo toàn byte chính xác áp dụng cho payload
không parse được, không phải mọi key/header.

| Mã lỗi hiện được dùng | Ý nghĩa |
|---|---|
| `JSON_PARSE_ERROR`, `INVALID_UTF8` | Lỗi deserialization |
| `SCHEMA_VERSION_UNSUPPORTED` | RAW khác 2.0 hoặc business major không hỗ trợ |
| `MISSING_REQUIRED_FIELD`, `INVALID_FIELD_TYPE`, `INVALID_ENUM_VALUE` | Sai field/metadata/header/enum |
| `INVALID_TIMESTAMP`, `VALUE_OUT_OF_RANGE`, `EVENT_TYPE_MISMATCH` | Sai timestamp/range/event type |
| `IDEMPOTENCY_CONFLICT` | Component thay đổi sau khi wide đã phát |
| `PROCESSING_ERROR` | RAW duplicate/late; phân biệt bằng prefix `DUPLICATE_EVENT:` / `LATE_EVENT:` trong `error_message`, `retryable=false` |
| `ALERT_NOT_FOUND` | Status event không có alert tương ứng |
| `REFERENCE_NOT_FOUND` | Stay ngoài registry; Writer đánh dấu retryable, Gateway không |
| `DB_CONSTRAINT_ERROR` | Vi phạm constraint DB, không retry |
| `DB_WRITE_ERROR` | Lỗi DB tạm thời hết 8 lượt thử, retryable |

RAW schema/value lỗi có thể dùng mã tổng quát `INVALID_FIELD_TYPE` hoặc
`VALUE_OUT_OF_RANGE`; không hứa mã lỗi chi tiết cho mọi field RAW. Cùng một
business message có thể tạo DLQ riêng từ hai consumer group. DLQ không tự
subscribe chính nó và không ghi vào bất kỳ business hypertable nào.

```json
{
  "schema_version": "1.0",
  "message_id": "0b6817c8-89bf-5adb-8a26-ead0d774185f",
  "event_type": "DLQ_EVENT",
  "produced_at": "2026-10-16T20:00:01Z",
  "producer": "icu-flink-v2",
  "dlq_id": "a102808d-865e-51b3-bbdf-bd5707f25c61",
  "failed_at": "2026-10-16T20:00:01Z",
  "source_topic": "icu.v2.chartevents",
  "source_partition": 0,
  "source_offset": 120,
  "kafka_key": "35727289",
  "consumer_group": "icu-flink-v2",
  "error_stage": "DESERIALIZATION",
  "error_code": "JSON_PARSE_ERROR",
  "error_message": "PARSE_ERROR: invalid JSON",
  "retryable": false,
  "attempt": 1,
  "original_headers": {},
  "original_payload": null,
  "raw_payload_base64": "eyJiYWQiOg=="
}
```

`eyJiYWQiOg==` là byte của JSON chưa hoàn chỉnh `{"bad":`. `message_id` và
`dlq_id` đều ổn định nhưng là hai UUID khác nhau. Kafka key của ví dụ là
`35727289`; headers có thêm `dlq-source-topic` và `dlq-error-code`.

## Validation và xử lý lại

Gateway/Writer decode UTF-8, parse object, kiểm tra version/field/type/time/
enum/range, key và headers, rồi kiểm tra registry. Message sai được đưa DLQ.
Writer chỉ commit Kafka offset sau DB commit hoặc sau DLQ delivery được xác
nhận. Lỗi DB tạm thời retry có giới hạn; DLQ delivery lỗi không commit offset
gốc. `retryable=true` **không có nghĩa đã có auto replay DLQ**.

Không tự replay payload lỗi khi chưa sửa nguyên nhân; replay RAW quá cũ vẫn
có thể bị Watermark từ chối. Ranh giới thời gian, recovery và tính bất biến
của alert ở [ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md](ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md).
