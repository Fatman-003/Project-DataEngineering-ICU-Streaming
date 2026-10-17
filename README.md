# Hệ thống theo dõi ICU theo thời gian thực và cảnh báo sớm

## Tổng quan dự án

Dự án mô phỏng bốn giường ICU. Simulator gửi dấu hiệu sinh tồn và xét nghiệm
Lactate được tạo ra vào Kafka. Flink kiểm tra dữ liệu, loại bản ghi trùng, xử lý
theo event-time rồi tạo telemetry và lab đã làm sạch, thống kê trượt 15 phút và
cảnh báo hạ huyết áp kéo dài. Event sai định dạng, sai giá trị, trùng hoặc đến
quá muộn đi vào `icu.v2.deadletter`.

Hai consumer độc lập nhận các luồng nghiệp vụ: Realtime Gateway đẩy cập nhật
qua WebSocket, còn Storage Writer lưu dữ liệu đã được chấp nhận vào TimescaleDB.
Cảnh báo MAP hiện tại là **cảnh báo hạ huyết áp kéo dài**, không phải chẩn đoán
sepsis.

Bốn bộ định danh bệnh nhân, lần nhập viện và ICU stay lấy từ MIMIC-IV Clinical
Demo 2.2, được cố định trong [config/demo_patients.yaml](config/demo_patients.yaml).
Giường và số đo là dữ liệu mô phỏng, **không phải số đo gốc của MIMIC
(not original MIMIC observations)**.

## Kiến trúc hệ thống

```text
4 định danh ICU cố định + telemetry/lab được tạo ra
                         |
                         v
                    ICU Simulator
                         |
                         v
                  Kafka RAW topics
                         |
                         v
                       Flink
       kiểm tra / loại trùng / event-time
           thống kê 15m / cảnh báo MAP
                         |
                         v
                Kafka business topics
                   /             \
                  v               v
       Realtime Gateway      Storage Writer
                  |               |
                  v               v
            WebSocket/UI      TimescaleDB

  event sai / trùng / quá muộn -> icu.v2.deadletter
```

Gateway và Writer dùng hai Kafka consumer group riêng. Cảnh báo đi từ Flink
qua Kafka đến Gateway rồi WebSocket; đường realtime không chờ ghi database.
TimescaleDB giữ lịch sử để truy vấn và phân tích. Runtime hiện cung cấp REST và
WebSocket; chưa có ứng dụng frontend hoàn chỉnh.

## Các dịch vụ đang chạy

| Dịch vụ | Mục đích | Địa chỉ |
|---|---|---|
| Kafka | Broker nhận và phát stream | `localhost:9092` (trong Docker: `icu-kafka:9094`) |
| Flink UI | Theo dõi job và checkpoint | http://localhost:8081 |
| Kafka UI, cluster `ICU-LOCAL` | Xem topic, message và consumer group | http://localhost:8082 |
| Realtime Gateway | Trạng thái trực tiếp và WebSocket | http://localhost:8083 |
| Gateway snapshot | REST snapshot tại thời điểm gọi | http://localhost:8083/api/snapshot |
| Gateway WebSocket | Cập nhật liên tục | `ws://localhost:8083/ws` |
| TimescaleDB | Lưu lịch sử bền vững | `localhost:5432` |
| Storage Writer | Ghi từ Kafka vào database | Container nền `icu-storage-writer`, không mở cổng cho người dùng |

Cần có Docker Desktop, môi trường `.venv` cài
[requirements.txt](requirements.txt) và broker `icu-kafka` của dự án. File
[.env.example](.env.example) chứa giá trị demo; launcher tạo hoặc bổ sung file
`.env` bị Git bỏ qua khi cần. Image dự án được build và image TimescaleDB được
tải tự động nếu còn thiếu. Lần đầu có thể lâu hơn các lần chạy sau.

## Khởi động hệ thống

Trong PowerShell tại thư mục dự án, chạy:

```powershell
.\run_icu_demo.ps1
```

Lệnh này kiểm tra Docker và quyền sở hữu các cổng 9092, 8081, 8082, 8083,
5432; giữ nguyên Kafka broker hiện có; khởi động Flink, Kafka UI, Gateway,
TimescaleDB và Storage Writer; khởi tạo schema/seed; reset bảy V2 topic và bốn
bảng dữ liệu runtime; tạo đúng một Flink job mới; rồi chạy mô phỏng sáu phút
theo thời gian thực. Các service tiếp tục chạy sau khi simulator kết thúc.
Không cần tự chạy lệnh pull, build, migration, seed hay reset riêng.

| Chế độ | Lệnh | Kết quả |
|---|---|---|
| Demo thường | `.\run_icu_demo.ps1` | Sáu phút thực, bốn cập nhật vital mỗi giây, Kafka và dữ liệu runtime DB bắt đầu sạch |
| Kiểm tra nhanh | `.\run_icu_demo.ps1 -FastVerify` | Sáu phút logic phát ở tốc độ 24×; kiểm tra Kafka, Flink, Gateway, WebSocket, DB, lag và idempotency |
| Giữ lịch sử | `.\run_icu_demo.ps1 -KeepDatabaseData` | Giữ các hàng runtime DB cũ; Kafka và Gateway vẫn reset |

FastVerify dùng **cùng logic** và ngưỡng MAP dưới 65 trong ít nhất **300 giây
event-time**; nó chỉ tăng tốc phát event, không rút ngắn thời gian xác nhận.
Demo thường phát một giây logic trong một giây thực. Các tùy chọn demo khác gồm
`-Loop`, `-Raw`, `-DurationMinutes`, `-IntervalSeconds`, `-SkipBuild` và
`-SkipDockerStart`.

## Bốn giường bệnh demo

| Bệnh nhân / giường | Kịch bản |
|---|---|
| P01 / BED-01 | Dấu hiệu sinh tồn bình thường |
| P02 / BED-02 | MAP <65 kéo dài; dự kiến một cảnh báo `SUSTAINED_HYPOTENSION` |
| P03 / BED-03 | MAP thấp ngắn hạn rồi hồi phục; không phát cảnh báo kéo dài |
| P04 / BED-04 | Chủ yếu bình thường, có một số event trùng, sai giá trị, quá muộn và out-of-order vẫn chấp nhận được |

Sáu tín hiệu `HR`, `MAP`, `SBP`, `DBP`, `SpO2`, `RR` luân phiên; mỗi tín hiệu
được cập nhật sau sáu giây logic. P02 có MAP thấp đầu tiên ở mốc `00:13` và
đủ điều kiện ở `05:13`. Khoảng cách giữa các mẫu MAP hợp lệ không được quá
90 giây; MAP ≥65 sẽ kết thúc episode và cho phép cảnh báo mới. Gateway chuyển
cảnh báo ngay sau khi Flink xác nhận, không đợi thống kê hoặc truy vấn DB.

## Database và cách theo dõi

Năm bảng tham chiếu là `patients`, `admissions`, `icustays`, `d_items` và
`d_labitems`. Bốn bảng time-series là `chartevents`, `labevents`, `vitals_15m`
và `alerts`. Mặc định lần chạy mới chỉ xóa dữ liệu ở bốn bảng time-series; các
bảng tham chiếu và Docker volume `icu-timescale-data` vẫn được giữ. Dùng
`-KeepDatabaseData` để giữ cả các hàng runtime trước đó.

Thông tin đăng nhập nằm trong `.env` với các khóa `POSTGRES_DB`, `POSTGRES_USER`
và `POSTGRES_PASSWORD`; không đưa mật khẩu vào tài liệu. Với giá trị demo mặc
định, mở SQL shell bằng lệnh PowerShell sau:

```powershell
docker exec -it icu-timescaledb psql -U icu_app -d icu
```

Nếu đã đổi user hoặc database trong `.env`, sửa hai tham số tương ứng. Trong
`psql`, có thể kiểm tra nhanh:

```sql
SELECT COUNT(*) FROM chartevents;
SELECT * FROM alerts ORDER BY trigger_time DESC LIMIT 20;
```

Trình tự quan sát đơn giản: chạy `.\run_icu_demo.ps1`, mở
[Flink UI](http://localhost:8081) để xem job/checkpoint, mở
[Kafka UI](http://localhost:8082) để xem message và group, mở
[Gateway snapshot](http://localhost:8083/api/snapshot) để xem P01–P04, rồi
truy vấn DB bằng SQL shell ở trên. Snapshot chỉ là ảnh chụp tại thời điểm gọi;
cập nhật liên tục đi qua WebSocket.

## Tài liệu liên quan

[KAFKA_FLINK_FLOW.md](KAFKA_FLINK_FLOW.md) giải thích đường đi của bảy topic,
Flink, Gateway và Storage Writer. [docs/DATABASE_DESIGN.md](docs/DATABASE_DESIGN.md)
ghi chi tiết chín bảng, bốn hypertable, khóa, index và cách commit offset.

Kiểm thử tự động có thể chạy bằng:

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests
```

Các file MIMIC gốc chỉ được đọc khi tạo hoặc kiểm tra seed; mô phỏng hằng ngày
dùng [seed đã xác minh](config/db_reference_seed.json) và không phát lại số đo
gốc từ MIMIC.
