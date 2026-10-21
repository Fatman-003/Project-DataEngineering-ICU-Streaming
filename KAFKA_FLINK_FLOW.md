# Luồng dữ liệu Kafka, Flink, Gateway và TimescaleDB

## Đường đi của dữ liệu

```text
                    DỮ LIỆU ICU ĐƯỢC TẠO RA
                         /          \
                        v            v
          icu.v2.chartevents      icu.v2.labevents
                        \            /
                         v          v
                            Flink
             kiểm tra / loại trùng / event-time
               /             |             \
              v              v              v
          CLEANED       VITALS 15M        ALERTS
               \            |             /
                \           |            /
                 v          v           v
                     Kafka business topics
                          /        \
                         v          v
                Realtime Gateway  Storage Writer
                         |          |
                         v          v
                  WebSocket / UI  TimescaleDB

  event sai định dạng / sai giá trị / trùng / quá muộn
                          |
                          v
                  icu.v2.deadletter
```

Simulator tạo dấu hiệu sinh tồn và xét nghiệm cho bốn bệnh nhân, Kafka nhận
dữ liệu RAW, Flink xử lý rồi ghi các topic nghiệp vụ. Gateway và Storage Writer
dùng consumer group riêng nên cùng nhận được business message mà không chờ
nhau. Alert đến WebSocket ngay cả khi đường ghi DB tạm ngừng.

## Nguồn định danh và dữ liệu mô phỏng

Bốn bộ `subject_id`/`hadm_id`/`stay_id` có thật trong MIMIC-IV Clinical Demo
2.2 được cố định tại [config/demo_patients.yaml](config/demo_patients.yaml).
`BED-01` đến `BED-04` là giường mô phỏng của dự án.

| Bệnh nhân / giường | `subject_id` | `hadm_id` | `stay_id` |
|---|---|---|---|
| P01 / BED-01 | 10039708 | 24928679 | 37323533 |
| P02 / BED-02 | 10019003 | 21457723 | 35727289 |
| P03 / BED-03 | 10014354 | 29600294 | 39864867 |
| P04 / BED-04 | 10007818 | 22987108 | 32359580 |

Giá trị đo được tạo theo kịch bản xác định, không phải quan sát gốc của MIMIC.
Các tín hiệu bedside là `HR`, `MAP`, `SBP`, `DBP`, `SpO2`, `RR`; lab hiện có là
Lactate. Mỗi giây logic tạo một tín hiệu luân phiên cho từng bệnh nhân, nên mỗi
tín hiệu cập nhật sau sáu giây. Lab thưa hơn, xuất hiện mỗi bốn phút logic.
Demo thường phát một giây logic mỗi giây thực; FastVerify phát 24 giây logic
mỗi giây thực nhưng giữ nguyên timestamp và ngưỡng cảnh báo.

## The seven topics — Bảy topic Kafka

Mỗi topic trong demo local có một partition, replication factor bằng một và
schema version `2.0`. Kafka key là chuỗi UTF-8 của định danh được nêu dưới đây.

| Topic | Kafka key | Dữ liệu và consumer |
|---|---|---|
| `icu.v2.chartevents` | `stay_id` | Telemetry numeric RAW từ simulator; Flink đọc |
| `icu.v2.labevents` | `mapped_stay_id` | Lab RAW (Lactate), thưa hơn vital; Flink đọc |
| `icu.v2.chartevents.cleaned` | `stay_id` | Telemetry hợp lệ sau Flink; Gateway và Storage Writer đọc |
| `icu.v2.labevents.cleaned` | `mapped_stay_id` | Lab hợp lệ sau Flink; Storage Writer đọc |
| `icu.v2.vitals.15m` | `stay_id` | Thống kê trượt 15 phút; Gateway và Storage Writer đọc |
| `icu.v2.alerts` | `stay_id` | Cảnh báo `SUSTAINED_HYPOTENSION`; Gateway nhận ngay, Storage Writer lưu độc lập |
| `icu.v2.deadletter` | Định danh stay nếu có, nếu không dùng key gốc hoặc `dlq_id` ổn định | Event bị từ chối kèm payload gốc và tọa độ Kafka để điều tra |

RAW `icu.v2.chartevents` chứa `event_id`, `subject_id`, `hadm_id`, `stay_id`,
`bed_id`, `itemid`, `signal_name`, `event_time`, `valuenum`, `valueuom` và
`schema_version`. RAW `icu.v2.labevents` dùng `mapped_stay_id` và `lab_name`;
đây là ánh xạ ICU stay của dự án vì lab MIMIC gốc không mang stay ID.

Hai topic `.cleaned` giữ định danh quan sát và thêm `quality_status`,
`ordering_status`, `processed_at`. Topic `icu.v2.vitals.15m` có `aggregate_id`,
`window_start`, `window_end`, `window_minutes`, `count`, `mean`, `min`, `max`,
sample variance (phương sai mẫu) trong `variance` và `emitted_at`. Topic alert
có `alert_id`, loại/mức độ, `trigger_time`, `episode_start`, giá trị/ngưỡng MAP,
trạng thái `ACTIVE` và thời điểm phát thực `alert_emitted_at`.

## What Flink does — Flink xử lý dữ liệu như thế nào

```text
icu.v2.chartevents -> parse JSON -> kiểm tra schema/identity/value
                   -> loại trùng -> event-time / ordering
                        |                   |
                        | bị từ chối        v
                        v           icu.v2.chartevents.cleaned
                icu.v2.deadletter       /             \
                                      v               v
                             thống kê 15m       trạng thái MAP
                                      |               |
                                      v               v
                             icu.v2.vitals.15m   icu.v2.alerts

icu.v2.labevents -> parse / kiểm tra / loại trùng / ordering
                   -> icu.v2.labevents.cleaned hoặc icu.v2.deadletter
```

Flink kiểm tra JSON, trường bắt buộc, bốn bộ định danh được phép, cặp
`itemid`/tín hiệu/đơn vị, timestamp, số hữu hạn và giới hạn kỹ thuật. Dedup
dùng stream cùng `event_id` trong managed state với TTL 24 giờ theo processing
time. Watermark được tạo với bounded out-of-order tolerance 30 giây; event tại
hoặc trước watermark hiệu lực lúc đến bị coi là quá muộn. Event cũ hơn mẫu
đã thấy nhưng còn sau watermark được gắn `OUT_OF_ORDER`. Đây không phải phép
so sánh đồng hồ thực lúc nhận với `event_time`; watermark hiệu lực mới là
ranh giới quyết định. Input rảnh không tự tạo quan sát giả. Checkpoint
bình thường chạy mỗi 10 giây; FastVerify dùng 2 giây để rút ngắn bước kiểm tra.

Event malformed, sai schema, sai giá trị, trùng hoặc quá muộn đi vào DLQ.
**DLQ là lỗi kỹ thuật/chất lượng dữ liệu, không phải nơi chứa mọi số đo bất
thường về lâm sàng.** `MAP=62` vẫn hợp lệ, đi vào CLEAN và tham gia logic
alert. `SpO2=-5` là sai giá trị và có thể vào DLQ. Event HR `-ooo` của P04 đến
khác thứ tự nhưng vẫn được chấp nhận.

Kafka sink có ngữ nghĩa at-least-once. `event_id`, `aggregate_id` và `alert_id`
ổn định giúp consumer xử lý lại an toàn; Gateway bỏ qua alert ID lặp.

## Thống kê duy nhất: cửa sổ 15 phút

Flink dùng cửa sổ event-time dài 15 phút, trượt mỗi phút; đầu cửa sổ được tính,
cuối cửa sổ không được tính. Mỗi cửa sổ bệnh nhân/tín hiệu có dữ liệu sẽ phát
`count`, `mean`, `min`, `max` và phương sai mẫu (`variance`). Với một mẫu,
`variance` là `null`. Các cửa sổ đầu có thể chưa đủ 15 phút quan sát; watermark
đóng cửa sổ, còn cửa sổ đang mở cần event sau đó mới phát. **Không có thống kê
5 phút** và TimescaleDB không tính lại thống kê này.

## Cảnh báo MAP: thứ tự, tính liên tục và thời gian xác nhận

Cấu hình gốc nằm ở [runtime_config.py](jobs/flink/runtime_config.py). Các giá
trị sau phục vụ mục đích riêng, không thay thế cho nhau:

| Giá trị | Ý nghĩa | Cách áp dụng |
|---|---|---|
| 30 giây | Out-of-order/lateness tolerance của stream | Tạo bounded watermark; event tại hoặc trước watermark hiệu lực đi vào DLQ dưới dạng `LATE_EVENT`. |
| 90 giây | Khoảng cách tối đa giữa hai quan sát MAP được chấp nhận | Tính bằng chênh lệch `event_time` của hai mẫu MAP liên tiếp; đúng 90 giây vẫn được phép, lớn hơn 90 giây phá vỡ tính liên tục. |
| 300 giây | Hypotension confirmation | Tổng low evidence của cùng episode phải đủ 300 giây event-time; không cộng khoảng recovery candidate. |
| 30 giây | Recovery confirmation | MAP ≥65 mở candidate; recovery đủ 30 giây mới kết thúc episode và re-arm. Đây là engineering debounce, riêng với tolerance out-of-order. |

Ví dụ hai mẫu MAP có event-time `10:00:00` và `10:00:06` cách nhau 6 giây về
quan sát. Mẫu sau đến sai thứ tự 20 giây có thể vẫn được chấp nhận nếu nó còn
sau watermark. Độ đến muộn và khoảng cách giữa hai mẫu là hai khái niệm khác nhau.

Chỉ **MAP được chấp nhận** điều khiển rule. HR, SpO2, RR, SBP hoặc DBP bình
thường không reset hay xác nhận recovery. MAP <65 cộng low evidence; MAP ≥65
mở `RECOVERY_CANDIDATE`. Nếu thấp lại trước 30 giây, episode được giữ nhưng
khoảng normal không tính là low time. Recovery đạt 30 giây mới xóa evidence
cũ và re-arm. Gap MAP >90 giây vẫn phá vỡ continuity, kể cả trong recovery.
Mỗi episode đủ điều kiện phát đúng một alert `ACTIVE`.

MAP đúng thứ tự cập nhật state theo từng quan sát. Accepted out-of-order MAP
xét lại phần managed history chưa chốt theo event-time; normal MAP thắng nếu
cùng timestamp. Event tại hoặc trước watermark đi vào `LATE_EVENT`/DLQ, không
đổi trạng thái realtime. Alert đã gửi vẫn bất biến, kể cả khi lịch sử còn
được chấp nhận tới sau đó; không có luồng correction/retraction hay resolution.
Hệ thống chọn ranh giới freshness để giữ cảnh báo kịp thời, không tái dựng
lâm sàng hoàn hảo cho dữ liệu lịch sử đến trễ tùy ý.

Ví dụ 150 giây low + 20 giây normal + 150 giây low đạt đủ evidence tại T+320,
không phải T+300. Chi tiết state machine, biên 29/30 giây và các cơ chế giảm
false alarm nằm ở [ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md](docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md).

```text
MAP đủ điều kiện -> Flink -> icu.v2.alerts -> Kafka -> Realtime Gateway
                                               -> WebSocket -> UI client
```

**300 giây là low evidence của episode**, tính bằng event-time. Khi một mẫu
MAP thấp chứng minh đủ evidence, Flink phát alert ngay. Watermark dùng để xét
lateness, rút gọn lịch sử và đóng cửa sổ; không bắt alert chờ thêm 30/90 giây.
P02 bắt đầu MAP thấp ở mốc logic `00:13`, đủ điều kiện ở `05:13`. P03 hồi phục
giữa chuỗi thấp rồi thấp lại; episode mới chưa đủ 300 giây nên không có alert.
FastVerify dùng lịch MAP riêng: P02 có các mẫu cách nhau đúng 90 giây, P04 có gap 91 giây,
recovery P03 ở `02:00` đến tại `02:10` rồi được duy trì đủ 30 giây để xác nhận,
và recovery P02 quá muộn đến sau alert.
Các ca này đi qua cùng Flink operator; chỉ lịch phát input kiểm thử thay đổi,
không rút ngắn các ngưỡng hoặc thêm thời gian chờ 30/90 giây.
Độ trễ vận chuyển là khoảng thời gian **sau
khi Flink phát**: Flink → Kafka → Gateway → WebSocket, không phải 300 giây xác
nhận. `alert_emitted_at`, `gateway_received_at` và `websocket_received_at` là
các mốc đồng hồ thực được FastVerify dùng để đo. Mục tiêu local dưới một giây,
giới hạn chấp nhận cứng dưới hai giây. Đường alert không chờ ghi DB.

## Gateway, snapshot và WebSocket

Consumer group `icu-ui-gateway-v2` đọc `icu.v2.chartevents.cleaned`,
`icu.v2.vitals.15m` và `icu.v2.alerts`. Gateway giữ vital/summary mới nhất
cùng tối đa 100 alert cho mỗi bệnh nhân; event cũ không ghi đè vital mới.

`http://localhost:8083/api/snapshot` là REST snapshot tại một thời điểm
(point-in-time); trang đã mở **không tự refresh**. Cập nhật realtime dùng
`ws://localhost:8083/ws`: client nhận `SNAPSHOT` trước, sau đó là
`VITAL_UPDATE`, `VITAL_15M` và `ALERT` trên cùng kết nối. Client chậm có thể
kết nối lại để lấy snapshot mới mà không chặn Kafka consumer. Khi Gateway khởi
động lại, nó đọc lại business topic còn trong Kafka; snapshot có thể chưa đầy
đủ trong lúc bắt kịp. Chưa có frontend hoàn chỉnh trong runtime này.

## Hai consumer group và đường lưu trữ

| Consumer group | Topic nhận | Mục đích |
|---|---|---|
| `icu-ui-gateway-v2` | `icu.v2.chartevents.cleaned`, `icu.v2.vitals.15m`, `icu.v2.alerts` | Trạng thái live cho WebSocket/snapshot |
| `icu-timescale-writer-v2` | `icu.v2.chartevents.cleaned`, `icu.v2.labevents.cleaned`, `icu.v2.vitals.15m`, `icu.v2.alerts` | Ghi bền vững vào TimescaleDB |

Hai group nhận cùng business message một cách độc lập. Storage Writer chỉ
commit Kafka offset **sau khi** transaction DB tương ứng đã commit. Nếu DB
ngừng hoạt động, lag của Writer tăng còn Gateway vẫn gửi cập nhật. Khi storage
trở lại, Writer đọc tiếp các message chưa commit; primary key vật lý giúp lần
ghi lại không tạo hàng trùng. Đây là xử lý at-least-once.

Bảng sau là ánh xạ bốn topic được lưu. RAW và `icu.v2.deadletter` **không**
được ghi vào các bảng nghiệp vụ chính của DB.

| Kafka business topic | TimescaleDB business hypertable | Khóa chống ghi trùng |
|---|---|---|
| `icu.v2.chartevents.cleaned` | `chartevents` | `(event_time, event_id)` |
| `icu.v2.labevents.cleaned` | `labevents` | `(event_time, event_id)` |
| `icu.v2.vitals.15m` | `vitals_15m` | `(window_end, aggregate_id)` |
| `icu.v2.alerts` | `alerts` | `(trigger_time, alert_id)` |

Năm bảng tham chiếu là `patients`, `admissions`, `icustays`, `d_items`,
`d_labitems`. [Seed](config/db_reference_seed.json) đã được kiểm tra với
MIMIC-IV Clinical Demo 2.2; giường và số đo stream vẫn là dữ liệu mô phỏng.
Docker volume `icu-timescale-data` giữ dữ liệu qua các lần chạy. Khởi động mặc
định xóa dữ liệu ở bốn bảng nghiệp vụ; `-KeepDatabaseData` giữ lại dữ liệu đó
và tiếp tục event clock sau số đo đã lưu. FastVerify luôn reset bốn bảng này.
Chi tiết ở [DATABASE_DESIGN.md](docs/DATABASE_DESIGN.md).

## Theo dõi hệ thống đang chạy

1. Chạy `.\run_icu_demo.ps1` để xem demo theo thời gian thực; thêm `-FastVerify`
   để kiểm tra cùng pipeline ở tốc độ 24× với các ca MAP continuity nêu trên.
   Startup reset bảy V2 topic, Gateway,
   Flink job cũ và dữ liệu runtime DB. Chỉ chạy một orchestrator mỗi lần.
2. Mở [Kafka UI](http://localhost:8082), chọn `ICU-LOCAL`, xem
   `icu.v2.chartevents`: P01–P04 phải có RAW event. Kiểm tra lab thưa ở
   `icu.v2.labevents`.
3. Xem `icu.v2.chartevents.cleaned` và `icu.v2.labevents.cleaned`: đối chiếu
   `event_id` với RAW, `quality_status=VALID`, `ordering_status` và
   `processed_at`. HR `-ooo` của P04 phải là `OUT_OF_ORDER` hợp lệ.
4. Xem `icu.v2.vitals.15m`: kiểm tra giường, tín hiệu, thời gian cửa sổ,
   `count`, `mean`, `min`, `max`, `variance`. Chỉ cửa sổ đã đóng và có dữ liệu
   mới xuất hiện.
5. Xem `icu.v2.alerts`: P02 có một `SUSTAINED_HYPOTENSION`, P03 không có.
   Phân biệt `trigger_time` theo event-time với `alert_emitted_at` thực.
6. Xem `icu.v2.deadletter`: P04 có event trùng, SpO2 sai và HR quá muộn;
   HR `-ooo` vẫn hợp lệ nên không vào DLQ.
7. Mở [Flink UI](http://localhost:8081): đúng một job ICU V2 ở trạng thái
   `RUNNING`, checkpoint tiếp tục thành công, record counter tăng và không có
   vòng lặp restart/failure. Đường logic là Kafka Source → Validation → Dedup
   → Event Time → CLEAN/DLQ, kèm nhánh 15m và MAP; nhãn operator thực tế có
   thể khác.
8. Mở [Gateway snapshot](http://localhost:8083/api/snapshot): có P01–P04,
   vital/summary thay đổi và alert P02. Muốn nhận event liên tục, kết nối
   WebSocket trước khi P02 đủ điều kiện, ví dụ trong browser developer tools:

   ```javascript
   const socket = new WebSocket('ws://localhost:8083/ws');
   socket.onmessage = event => console.log(JSON.parse(event.data));
   ```

   Kết nối muộn chỉ thấy alert đã xảy ra trong `SNAPSHOT`.
9. Trong Kafka UI, xem hai group ở bảng trên. `icu-timescale-writer-v2` cần
   một member, bốn assignment và lag cuối FastVerify về 0.
10. Vào DB bằng `docker exec -it icu-timescaledb psql -U icu_app -d icu`
    (đổi user/database theo `.env` nếu cần). So `event_id`, thời gian, stay và
    giá trị của một Kafka business message với đúng một hàng DB.
11. Chạy FastVerify lần nữa để thấy topic, Gateway và runtime DB được reset;
    năm bảng tham chiếu và Docker volume vẫn được giữ.

## Kiểm tra bằng SQL

```sql
SELECT COUNT(*) FROM patients;    -- 4
SELECT COUNT(*) FROM admissions;  -- 4
SELECT COUNT(*) FROM icustays;    -- 4
SELECT COUNT(*) FROM d_items;     -- 6
SELECT COUNT(*) FROM d_labitems;  -- 1
SELECT COUNT(*) FROM chartevents;
SELECT COUNT(*) FROM labevents;
SELECT COUNT(*) FROM vitals_15m;
SELECT COUNT(*) FROM alerts;
SELECT * FROM chartevents ORDER BY event_time DESC LIMIT 20;
SELECT * FROM labevents ORDER BY event_time DESC LIMIT 20;
SELECT * FROM vitals_15m ORDER BY window_end DESC LIMIT 20;
SELECT * FROM alerts ORDER BY trigger_time DESC;
```

Để theo dõi một event từ Kafka đến DB, lấy `event_id` trong
`icu.v2.chartevents.cleaned` rồi chạy:

```sql
SELECT * FROM chartevents WHERE event_id = '<event-id>';
```

Kết quả đúng một hàng chứng minh đường business Kafka → Storage Writer →
TimescaleDB. Có thể làm tương tự với `alert_id` của `icu.v2.alerts`:

```sql
SELECT * FROM alerts WHERE alert_id = '<alert-id>';
```
