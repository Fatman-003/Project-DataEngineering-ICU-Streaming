# Demo theo dõi ICU theo thời gian thực

Dự án mô phỏng dấu hiệu sinh tồn và xét nghiệm của **4 bệnh nhân ICU**:
Simulator → Kafka RAW → Flink → Kafka business → Realtime Gateway/WebSocket
và Storage Writer/TimescaleDB. Flink kiểm tra dữ liệu, loại trùng, tính trung
bình cửa sổ **5 phút** và cảnh báo MAP thấp đủ **30 giây low evidence**.

Định danh lấy từ MIMIC-IV Clinical Demo 2.2; số đo là mô phỏng,
**không phải quan sát gốc của MIMIC (not original MIMIC observations)**.
Repo có sẵn cấu hình và seed; chạy demo không cần tải MIMIC gốc.

P01–P04 là `demo_id` trong cấu hình ứng dụng, dùng để tra patient trên Gateway;
DB `patients` chỉ dùng `subject_id`, không có cột `demo_id`. `bed_id` là số
nguyên cố định: P01=1, P02=2, P03=3, P04=4, thống nhất trong DB, RAW và API.

## Công nghệ và image

| Dịch vụ | Image gốc | Image/container của dự án |
|---|---|---|
| Kafka | `apache/kafka:4.2.2` | Container `icu-kafka` |
| Flink/PyFlink | `flink:2.3.0` | `icu-pyflink:2.3.0-kafka5.0.0-2.2-v2` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | Container `icu-kafka-ui` |
| TimescaleDB / PostgreSQL | `timescale/timescaledb:2.30.2-pg17` | Container `icu-timescaledb` |
| Realtime Gateway | `python:3.12.12-slim` | `icu-realtime-gateway:v2` |
| Storage Writer | `python:3.12.12-slim` | `icu-storage-writer:v1` |

Script build ba image `icu-*` từ Dockerfile với base image khóa bằng SHA256.
Kafka connector của Flink là `5.0.0-2.2`. Python image dùng bên trong container;
Python **3.12.x trên Windows** dùng chạy script và producer.

## Chuẩn bị lần đầu trên Windows

Mở PowerShell tại thư mục repo chứa `run_icu_demo.ps1`. Cần Docker Desktop
đã cài, chạy **Linux containers**, có Docker Compose và Python **3.12.x**.
Không cần cài Kafka, Flink hay PostgreSQL trực tiếp lên Windows.

**Đảm bảo ổ đĩa lưu image/cache/volume của Docker Desktop còn đủ dung lượng**;
nên dành ít nhất **20 GB trống** cho lần chuẩn bị đầu, nhiều hơn nếu giữ dữ
liệu HF lâu. Vị trí lưu Docker có thể khác thư mục repo. Repo cũng cần chỗ
cho `.venv`. Script tự tải/build image dịch vụ còn thiếu,
**không tự cài Docker Desktop và không tự tạo Kafka broker đầu tiên**.

**1. Kiểm tra môi trường.**

```powershell
docker info --format '{{.OSType}}'
docker compose version
python --version
```

Kết quả cần có `linux` và `Python 3.12.x`. Các cổng `9092`, `8081`, `8082`,
`8083`, `5432` cần trống hoặc đang thuộc đúng container của dự án.

**2. Tải Kafka image và tạo broker một lần.**

Chỉ khi máy chưa có container `icu-kafka`, chạy:

```powershell
docker pull apache/kafka:4.2.2
docker run -d --name icu-kafka -p 127.0.0.1:9092:9092 apache/kafka:4.2.2
docker ps --filter name=icu-kafka
```

`pull` tải image; `run` tạo broker từ image đó. Nếu container đã tồn tại và
đang dừng, dùng `docker start icu-kafka`. Các lần demo sau dùng lại broker này;
script chuẩn bị mạng `icu-streaming`, listener nội bộ và topic.

**3. Tạo môi trường Python.**

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Không cần kích hoạt `.venv`. Lần chạy đầu, launcher tự tạo `.env` từ
`.env.example`; container TimescaleDB khởi tạo database, user và password theo
cấu hình đó. Script tiếp tục tạo bảng và nạp seed. Đây là tài khoản kết nối
DB cục bộ cho các service, người xem demo không phải đăng nhập DB.
Nếu muốn đổi cấu hình, copy `.env.example` thành `.env` và sửa **trước lần
khởi tạo DB đầu tiên**; sửa password trong file sau đó không tự đổi password
đã lưu trong volume.

**4. Chạy demo.**

```powershell
.\run_icu_demo.ps1
```

Script tải/build các image còn thiếu, bật dịch vụ, chuẩn bị schema/seed,
reset dữ liệu demo, rồi chờ Flink có checkpoint và Gateway/Writer sẵn sàng.
Khi thấy **`ICU STREAMING SYSTEM READY`**, mở các tab bên dưới; producer bắt
đầu ngay sau đó. Lần đầu tải/build lâu hơn. Không cần chạy Compose, migration
hay seed riêng. `start_icu.ps1` là tiện ích bật dịch vụ/job trên môi trường
đã chuẩn bị; chạy demo đầy đủ chỉ cần `run_icu_demo.ps1`.

Nếu PowerShell chặn `.ps1`, dùng:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_icu_demo.ps1
```

## Chọn chế độ và dừng demo

| Chế độ | Lệnh | Mục đích |
|---|---|---|
| Normal | `.\run_icu_demo.ps1` | Demo thời gian thực, mặc định 6 phút |
| Liên tục | `.\run_icu_demo.ps1 -Loop` | Có thêm thời gian mở UI; `Ctrl+C` dừng producer |
| FastVerify | `.\run_icu_demo.ps1 -FastVerify` | Kiểm tra chức năng: 6 phút event-time phát ở tốc độ 24× |
| HF100 | `.\run_icu_demo.ps1 -HighFrequency 100 -DurationMinutes 1` | Workload tổng hợp 100 Hz/signal/patient trong 60 giây |
| HF200 | `.\run_icu_demo.ps1 -HighFrequency 200 -DurationMinutes 1` | Workload tổng hợp 200 Hz/signal/patient trong 60 giây |

Normal phát **cả 6 signal HR, MAP, SBP, DBP, SpO2, RR ở 1 Hz cho mỗi bệnh
nhân**: 4 × 6 = **24 RAW chartevents/giây**. Sáu signal của cùng patient/tick
có chung `event_time` nhưng là 6 Kafka message riêng. Flink ghép HR/MAP/SBP/
SpO2 thành khoảng **4 business telemetry/giây**. Lab Lactate phát thưa ở T+0
và mỗi 240 giây cho từng bệnh nhân; chưa có business lab output hay DB sink.

HF100/HF200 nhắm **2.400/4.800 RAW numeric event/giây**, ngoài lab và quality
probe thưa. Đây là tải benchmark tổng hợp, không đại diện tần suất lâm sàng
thực của mọi scalar vital. Summary báo delivered/failures, undelivered và
scheduling lag; **tốc độ producer không chứng minh throughput đầu cuối**.
Normal có thể bỏ tick đã trễ lịch để giữ nhịp live; HF giữ mọi tick và báo
lag nên thời gian thực có thể dài hơn thời lượng yêu cầu.

`-DurationMinutes (10 / 60)` chạy thử 10 giây. HF không kết hợp `-FastVerify`,
`-Loop`, `-Raw` hoặc thay đổi `-IntervalSeconds`. FastVerify dùng kịch bản cố
định, không kết hợp tùy chọn thay đổi duration/interval/loop/raw hay
`-KeepDatabaseData`; nó còn thử status update nên alert cuối có thể là
`ACKNOWLEDGED`. Đây là kiểm tra chức năng, không phải benchmark.

**Mỗi lần chạy mặc định reset 6 topic ứng dụng, 3 Consumer Group của dự án và
4 business hypertable**. `-KeepDatabaseData` giữ 4 bảng này và chọn event clock
sau telemetry đã lưu; Kafka/Gateway vẫn reset. Năm bảng tham chiếu và broker/
DB volume được giữ. Chỉ chạy một launcher tại một thời điểm.

Producer kết thúc hoặc `Ctrl+C` thì service vẫn mở để kiểm tra. Muốn dừng
hạ tầng sau khi producer đã dừng, chạy:

```powershell
docker compose -f docker/flink/compose.yaml --env-file .env stop
docker stop icu-kafka
```

## Theo dõi trực quan

Mở các địa chỉ trên máy chạy demo sau dòng READY; tab mở sẵn thì tải lại.

| Công cụ | Cách theo dõi |
|---|---|
| [Flink UI:8081](http://localhost:8081) | Job `ICU business telemetry, 5m windows and 30s hypotension` phải `RUNNING`; xem record counters, checkpoint, backpressure. |
| [Kafka UI:8082](http://localhost:8082) | `ICU-LOCAL` → Topics → Messages: RAW `icu.v2.chartevents`, `icu.v2.labevents`; outputs `icu.telemetry`, `icu.vitals.5m`, `icu.alerts`, `icu.deadletter`. Xem consumer lag của Writer. |
| [Gateway snapshot:8083](http://localhost:8083/api/snapshot) | JSON P01–P04; tải lại để xem số đo mới. [Health](http://localhost:8083/api/health) báo kết nối Kafka. Chưa có giao diện ICU dashboard ở địa chỉ gốc. |
| TimescaleDB:5432 | Mở `psql` theo lệnh bên dưới; chưa có DB dashboard riêng trong Compose. |

P02 bắt đầu MAP thấp tại T+12 và đủ low evidence ở **T+42 event-time**. Alert
chỉ xuất hiện sau khi bucket được Watermark chốt; P01/P03/P04 không dự kiến
có alert trong kịch bản mặc định. Output 5m xuất hiện khi cửa sổ đóng; không
cần đợi đủ 5 phút kể từ đầu demo mới có cửa sổ đầu tiên.

Muốn xem event liên tục, mở Developer Tools → Console tại trang Gateway:

```javascript
const socket = new WebSocket('ws://localhost:8083/ws');
socket.onmessage = event => console.log(JSON.parse(event.data));
```

Nhận `SNAPSHOT` trước, rồi `TELEMETRY_SAMPLE`, `VITALS_WINDOW_5M`, `ALERT`,
`ALERT_STATUS_CHANGED` nếu có. Snapshot HTTP không tự refresh. Kafka host dùng
`localhost:9092`; các container dùng listener nội bộ `icu-kafka:9094`.

## Kiểm tra dữ liệu lưu

Trong PowerShell khác, dùng user/DB demo mặc định; nếu đã sửa `.env`, thay
`-U` và `-d` cho khớp:

```powershell
docker exec -it icu-timescaledb psql -U icu_app -d icu
```

```sql
\dt
SELECT count(*) FROM patient_telemetry_stream;
SELECT count(*) FROM vitals_sliding_window_5m;
SELECT * FROM patient_telemetry_stream ORDER BY sample_time DESC LIMIT 8;
SELECT * FROM sepsis_alert_events ORDER BY alert_time DESC LIMIT 8;
SELECT 'chartevents' AS table_name, count(*) FROM chartevents
UNION ALL SELECT 'labevents', count(*) FROM labevents;
```

Hai bảng nguồn phải vẫn **0 hàng**; dữ liệu stream nằm trong business
hypertable. Thoát `psql` bằng `\q`.

## Đọc tiếp

- [Luồng Kafka/Flink và các consumer](KAFKA_FLINK_FLOW.md).
- [Kafka message contract và ví dụ JSON](docs/KAFKA_MESSAGE_FORMATS.md).
- [Thiết kế 11 bảng, khóa/index và SQL kiểm tra](docs/DATABASE_DESIGN.md).
- [Rule cảnh báo, recovery và giảm false alarm](docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md).
