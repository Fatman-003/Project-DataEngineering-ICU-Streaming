# Demo theo dõi ICU theo thời gian thực

Dự án mô phỏng dấu hiệu sinh tồn và xét nghiệm của bốn giường ICU.
Simulator phát dữ liệu vào Kafka; Flink kiểm tra, loại bản ghi trùng, tính
thống kê 15 phút và tạo cảnh báo MAP thấp kéo dài. Realtime Gateway cung cấp
snapshot/WebSocket, còn Storage Writer ghi kết quả vào TimescaleDB. Bản ghi
không hợp lệ đi vào `icu.v2.deadletter`. Dữ liệu đo là mô phỏng, **không phải
số đo gốc của MIMIC (not original MIMIC observations)**. Repo đã có cấu hình
bệnh nhân và seed cần cho demo; không cần tải bộ dữ liệu MIMIC gốc.

## Image Docker được dùng

| Dịch vụ | Image gốc | Image chạy trong dự án |
|---|---|---|
| Kafka | `apache/kafka:4.2.2` | Cùng image, container `icu-kafka` |
| Flink | `flink:2.3.0` | `icu-pyflink:2.3.0-kafka5.0.0-2.2-v2` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | Cùng image |
| TimescaleDB | `timescale/timescaledb:2.30.2-pg17` | Cùng image |
| Realtime Gateway | `python:3.12.12-slim` | `icu-realtime-gateway:v2` |
| Storage Writer | Dùng chung `python:3.12.12-slim` | `icu-storage-writer:v1` |

Ba image `icu-*` được build từ Dockerfile của repo khi chạy script. Dockerfile
khóa base image bằng SHA256 và Flink image cài Kafka connector `5.0.0-2.2`.

## Chạy lần đầu trên Windows

Mở PowerShell tại **thư mục gốc của repo** (nơi có `run_icu_demo.ps1`).
Cần Docker Desktop chạy ở chế độ **Linux containers** và Python **3.12**.
Không cần cài Flink, Kafka hoặc PostgreSQL trực tiếp lên Windows.

**Kiểm tra dung lượng trước khi bắt đầu:** ổ đĩa lưu dữ liệu của Docker
Desktop (image, build cache và volume) nên còn **ít nhất 20 GB trống**. Image
Docker không nhất thiết nằm trong thư mục repo; kiểm tra vị trí lưu dữ liệu
Docker Desktop nếu bạn đã đổi ổ đĩa mặc định. Thư mục repo cũng cần còn chỗ
cho `.venv` và các file phát sinh. `run_icu_demo.ps1` tự tải/build image còn
thiếu và khởi động dịch vụ, **không tự cài Docker Desktop**.

**1. Kiểm tra môi trường.**

```powershell
docker info --format '{{.OSType}}'
docker compose version
python --version
```

Lệnh đầu phải in `linux`; lệnh cuối phải in `Python 3.12.x`. Nếu chưa đúng,
mở Docker Desktop hoặc chọn bản Python 3.12 trước khi làm tiếp. Các cổng
`9092`, `8081`, `8082`, `8083` và `5432` không được ứng dụng khác chiếm.

**2. Tải Kafka image, rồi tạo broker một lần.**

Chỉ trên máy **chưa có** container `icu-kafka`, chạy lần lượt:

```powershell
docker pull apache/kafka:4.2.2
docker run -d --name icu-kafka -p 127.0.0.1:9092:9092 apache/kafka:4.2.2
docker ps --filter name=icu-kafka
```

Lệnh đầu tải image Kafka `4.2.2`; lệnh thứ hai tạo container broker từ image
đó; lệnh cuối phải hiện `icu-kafka` đang chạy. Nếu container đã tồn tại,
không chạy lại `docker run`; nếu nó đang dừng, chạy `docker start icu-kafka`.
`run_icu_demo.ps1` giữ nguyên broker này, tự thêm mạng/listener nội bộ và
tạo các topic cần thiết; script **không tạo broker mới**.

**3. Tạo môi trường Python cho script.**

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Không cần kích hoạt `.venv`: lệnh chạy demo sẽ dùng đúng Python trong thư mục
này. Khi chạy lần đầu, script tự tạo `.env` từ `.env.example`, khởi tạo
database/bảng và seed. `POSTGRES_DB`, `POSTGRES_USER` và `POSTGRES_PASSWORD`
là cấu hình database cục bộ cho TimescaleDB và Storage Writer; người chạy
demo không phải đăng nhập DB. Nếu muốn đổi mật khẩu demo, chạy
`Copy-Item .env.example .env` rồi sửa `POSTGRES_PASSWORD` trong `.env`
**trước lần khởi tạo database đầu tiên**. Sửa `.env` sau đó không tự đổi mật
khẩu đã lưu trong volume DB.

**4. Chạy demo.**

```powershell
.\run_icu_demo.ps1
```

Script tự tải các image dịch vụ còn thiếu, build image `icu-*`, khởi động
các service và chuẩn bị database. Sau đó nó reset dữ liệu demo, chờ
Flink/Gateway/Writer sẵn sàng rồi mới phát dữ liệu.
Lần đầu tải/build có thể lâu hơn; không cần chạy `docker compose up`,
migration hoặc seed riêng. Khi PowerShell in
`ICU STREAMING SYSTEM READY`, các UI đã mở được và simulator bắt đầu
ngay sau đó. Demo thường chạy khoảng **6 phút**.

Nếu PowerShell chặn file `.ps1`, dùng lệnh này trong cùng thư mục:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_icu_demo.ps1
```

Để nhóm có thêm thời gian quan sát, dùng `.\run_icu_demo.ps1 -Loop`;
nhấn `Ctrl+C` để dừng phát dữ liệu. `.\run_icu_demo.ps1 -FastVerify`
chạy kiểm tra đầu cuối nhanh, không phù hợp để xem trực tiếp. Mỗi lần chạy
mặc định reset dữ liệu demo V2 và bốn bảng runtime; tùy chọn
`-KeepDatabaseData` giữ lại dữ liệu runtime DB. Các service vẫn chạy sau
khi simulator kết thúc. `start_icu.ps1` chỉ bật service/job trên môi trường
đã chuẩn bị; dùng `run_icu_demo.ps1` cho demo đầy đủ.

## High-frequency benchmark mode

Sau khi hoàn tất các bước chuẩn bị ở trên, chọn một profile:

```powershell
.\run_icu_demo.ps1 -HighFrequency 100 -DurationMinutes 1
.\run_icu_demo.ps1 -HighFrequency 200 -DurationMinutes 1
```

Dùng lại `-DurationMinutes`: `1` = 60 giây; `(10 / 60)` = 10 giây để chạy
thử ngắn. Nếu bỏ tham số này, thời lượng vẫn là 6 phút. Mỗi lần chạy qua
script vẫn reset dữ liệu demo như chế độ thường, chờ dịch vụ sẵn sàng rồi
mới bắt đầu tính thời gian phát. Không kết hợp HF với `-FastVerify`,
`-Loop`, `-Raw` hoặc thay đổi `-IntervalSeconds`.

| Profile | Bệnh nhân × signal × tần suất mỗi signal | Tick | Chartevents mục tiêu |
|---|---|---|---|
| HF100 | 4 × 6 × 100 Hz | 10 ms | 2.400 event/giây; 144.000 event/60 giây |
| HF200 | 4 × 6 × 200 Hz | 5 ms | 4.800 event/giây; 288.000 event/60 giây |

Mỗi tick phát 24 Kafka message riêng cho HR, MAP, SBP, DBP, SpO2, RR của
bốn bệnh nhân; sáu signal cùng tick có cùng event-time. Lab vẫn phát thưa
mỗi 240 giây, gồm lần đầu ở T+0. Các mẫu duplicate/invalid/out-of-order/late
của P04 vẫn phát thưa; summary đếm lab và các event bổ sung riêng, ngoài
chartevents mục tiêu. Khi chậm lịch, producer giữ các tick chưa phát và báo
scheduling lag; thời gian chạy thực tế có thể dài hơn yêu cầu.

Đây là **workload tổng hợp để benchmark/stress pipeline**; 100/200 Hz không
đại diện cho tần suất lâm sàng thực của tất cả scalar vital sign.
`-FastVerify` kiểm tra chức năng với kịch bản tăng tốc; HF tạo tải ở nhịp
thời gian thực. Lệnh thường `.\run_icu_demo.ps1` vẫn xoay vòng từng signal.
Yêu cầu MAP thấp đủ 300 giây event-time và xác nhận hồi phục 30 giây giữ nguyên,
không chuyển thành số mẫu.

Summary báo số attempted/queued/delivered (từ Kafka delivery callback),
failures, undelivered, tốc độ thực tế và tỷ lệ đạt mục tiêu. Tốc độ producer
không chứng minh công suất bền vững của toàn pipeline. Khi benchmark, theo dõi
Kafka message rate/consumer lag ở Kafka UI; throughput, backpressure và
checkpoint ở Flink UI; lag nhóm `icu-timescale-writer-v2`, tốc độ ghi
TimescaleDB, CPU/RAM của container (`docker stats`). MAP operator hiện nén
phần lịch sử đã qua watermark, còn giữ từng mẫu MAP trong phần chưa finalized:
HF200 có thể giữ khoảng 24.000 mẫu cho bốn bệnh nhân khi tail khoảng 30 giây;
state có thể lớn hơn khi watermark chậm hoặc dừng. Dedup còn giữ state theo
từng event-ID với TTL 24 giờ nên cũng tăng theo lượng event. Đây chưa phải
kết quả đo bộ nhớ hay benchmark so sánh.

Để chỉ phát vào Kafka đã chuẩn bị, không chạy orchestration/reset/Flink API:

```powershell
.\.venv\Scripts\python.exe scripts/demo/run_icu_demo.py --high-frequency 100 --duration-minutes 1 --start-time 2026-10-16T20:00:00Z
```

Cùng profile, duration và `--start-time` sẽ tạo cùng payload/event-ID; không
dùng random nên không cần seed. Dùng input/topic state sạch khi so sánh các
stream processor; replay cùng ID lên pipeline đã chạy có thể bị dedup hoặc
phân loại late.

## Xem kết quả

Mở các địa chỉ sau trên **máy đang chạy demo** sau dòng
`ICU STREAMING SYSTEM READY`. Nếu đã mở tab từ trước, tải lại trang.

| Mục | Xem ở đâu |
|---|---|
| Flink job | [localhost:8081](http://localhost:8081): job ICU V2 ở trạng thái `RUNNING`; xem số bản ghi và checkpoint. |
| Kafka message | [localhost:8082](http://localhost:8082): chọn `ICU-LOCAL` → Topics → Messages. `icu.v2.chartevents` là dữ liệu vào; `icu.v2.chartevents.cleaned` là dữ liệu đã làm sạch; `icu.v2.alerts` là cảnh báo; `icu.v2.deadletter` là bản ghi bị loại. |
| Bốn giường ICU | [Gateway snapshot](http://localhost:8083/api/snapshot): trang JSON chứa P01–P04; tải lại để xem giá trị mới. Cổng 8083 không có dashboard ở địa chỉ gốc. |
| Database | Trong PowerShell khác: `docker exec -it icu-timescaledb psql -U icu_app -d icu`, rồi chạy `SELECT COUNT(*) FROM chartevents;` hoặc `SELECT * FROM alerts ORDER BY trigger_time DESC LIMIT 20;`. Nếu đã đổi user/DB trong `.env`, thay hai tham số cho khớp. |

Kafka dùng `localhost:9092`, TimescaleDB dùng `localhost:5432`. P02 dự kiến
có cảnh báo MAP thấp kéo dài gần phút **5:13** của demo thường; trước mốc đó
topic alerts có thể còn trống.

Nếu chưa hiểu dữ liệu đi qua Kafka, Flink và Gateway như thế nào, đọc
[KAFKA_FLINK_FLOW.md](KAFKA_FLINK_FLOW.md). Nếu cần hiểu chín bảng và cách
Storage Writer ghi dữ liệu, đọc [docs/DATABASE_DESIGN.md](docs/DATABASE_DESIGN.md).
Chi tiết rule cảnh báo và false-alarm handling:
[docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md](docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md).
