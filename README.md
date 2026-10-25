# Demo theo dõi ICU theo thời gian thực

Hệ thống streaming dấu hiệu sinh tồn ICU: **Simulator → Kafka → [Server → WebSocket → Client]**
và **[Storage Writer → TimescaleDB]**, kèm **Flink** xử lý business (lọc trùng, event-time,
cửa sổ 5 phút, cảnh báo MAP thấp 30 giây). Toàn bộ hạ tầng chạy bằng **một file
Docker Compose duy nhất**: `docker/compose.yaml`.

Định danh lấy từ MIMIC-IV Clinical Demo 2.2; số đo là mô phỏng, **không phải quan sát
gốc của MIMIC**. Seed reference nằm sẵn trong repo (`config/db_reference_seed.json`) —
chạy demo không cần tải MIMIC gốc.

## Kiến trúc

```
                         ┌────────────────────────── docker/compose.yaml ──────────────────────────┐
                         │                                                                          │
 simulate_stream.py ──► icu-kafka:9092/9094 ──► icu-server ──► WebSocket ──► icu-web (:3000)       │
 (Python, host)              │            (Spring :8080, GET + WS)                                │
                             │                                                                    │
                             ├──► icu-storage-writer ──► icu-db (TimescaleDB :5432, Flyway)       │
                             │                                                                    │
                             └──► icu-flink (jobmanager+taskmanager :8081)                        │
                                  windows 5m / alerts ──► icu.telemetry, icu.vitals.5m, icu.alerts│
                                                                                                  │
  icu-init (one-shot): tạo 6 topic + seed 4 bệnh nhân  ──  icu-kafka-ui (:8082)                   │
                         └──────────────────────────────────────────────────────────────────────────┘
```

| Dịch vụ | Image | Port host | Vai trò |
|---|---|---|---|
| `icu-kafka` | `apache/kafka:4.2.2` (KRaft 1 node) | `127.0.0.1:9092` | Broker; container dùng listener nội bộ `icu-kafka:9094` |
| `icu-kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | `127.0.0.1:8082` | Xem topic, consumer lag |
| `icu-db` | `timescale/timescaledb:2.30.2-pg17` | `127.0.0.1:5432` | Schema do **Flyway** (server) tạo; seed do `icu-init` nạp |
| `icu-server` | `icu-server:local` (Spring Boot 21) | `:8080` | REST GET + consumes Kafka → WebSocket `/ws` (STOMP) |
| `icu-web` | `icu-web:local` (React + Nginx) | `:3000` | UI ICU monitor |
| `icu-storage-writer` | `icu-storage-writer:v1` | — | Consume business topics → DB (idempotent) |
| `icu-flink-*` | `icu-pyflink:2.3.0-kafka5.0.0-2.2-v2` | `127.0.0.1:8081` | Job business telemetry + alert |
| `icu-init` | `icu-init:local` (one-shot) | — | Tạo topic + seed, xong tự thoát |
| `icu-flink-submit` | (one-shot) | — | Submit job Flink nếu chưa có |

## Chuẩn bị lần đầu

Cần Docker Desktop (Linux containers) + Python **3.12.x** trên host (cho simulator).
Compose tự build/tải image còn thiếu. Các cổng `9092, 5432, 8080, 8081, 8082, 3000` phải trống.

```powershell
docker info --format '{{.OSType}}'      # cần: linux
docker compose version
python --version                        # cần: 3.12.x

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Copy `.env.example` thành `.env` nếu chưa có (chứa credentials DB local demo).

## Chạy toàn bộ hệ thống

```powershell
docker compose -f docker/compose.yaml --env-file .env up -d --build
```

Một lệnh duy nhất — compose tự động:

1. Khởi động Kafka, TimescaleDB, Kafka UI, Flink (jobmanager + taskmanager)
2. `icu-server` boot → **Flyway** tạo schema V0–V5 (11 bảng, hypertable, index)
3. `icu-init` chờ Kafka + DB + server → tạo 6 topic (`icu.v2.chartevents`,
   `icu.v2.labevents`, `icu.telemetry`, `icu.vitals.5m`, `icu.alerts`, `icu.deadletter`)
   → seed reference (`patients=4, admissions=4, icustays=4, d_items=6, d_labitems=1, labevents=8`)
4. `icu-flink-submit` submit job `ICU business telemetry, 5m windows and 30s hypotension`
5. `icu-storage-writer` + `icu-server` consumers sẵn sàng

Khi tất cả container `Up/healthy` (kiểm tra `docker compose -f docker/compose.yaml ps`)
và Flink job `RUNNING` — mở UI và các công cụ theo dõi:

| URL | Nội dung |
|---|---|
| [localhost:3000](http://localhost:3000) | **UI ICU monitor** (vitals realtime, trend theo range, labs, alerts) |
| [localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) | REST API docs |
| [localhost:8081](http://localhost:8081) | Flink UI — job phải `RUNNING` |
| [localhost:8082](http://localhost:8082) | Kafka UI — topic, consumer lag |

## Chạy simulator — 2 chế độ

### 1. Full-flow qua Flink (khuyến nghị cho demo business)

`run_icu_demo.py` bắn **RAW** chartevents (6 signal × 4 bệnh nhân) + Lactate + edge cases
(duplicate / invalid / out-of-order / late) vào `icu.v2.*` → **Flink** validate, dedup,
tính cửa sổ 5 phút, đánh giá hypotension 30s → business topics → DB + UI:

```powershell
# Kịch bản chuẩn 6 phút event-time, tốc độ 8× (~45 giây wall)
.\.venv\Scripts\python.exe scripts\demo\run_icu_demo.py --duration-minutes 6 --speed 8

# Kết quả kỳ vọng: telemetry ~1440, vitals_5m = 24, alerts = 1 (P02 hypotension CRITICAL)
```

### 2. Telemetry-only load test (bypass Flink)

`simulate_stream.py` bắn thẳng business telemetry vào `icu.telemetry` — dùng để
load test đường Kafka → writer → DB → UI, **không sinh vitals_5m/alerts**:

```powershell
# 1 triệu events, trải 60 phút event-time, ~17 phút wall-time
.\.venv\Scripts\python.exe scripts\demo\simulate_stream.py --total 1000000 --rate 1000

# Biến thể
#   --rate 5000            → nhanh (~3.5 phút), phù hợp load test
#   --hypotension-rate 0.3 → 30% mẫu MAP thấp để thấy cảnh báo màu trên UI
#   --no-reset             → bắn thêm vào data đang có (mặc định tự reset)
#   --reset-only           → chỉ xóa data stream (Kafka topics + DB stream tables), không bắn
```

Cả hai chế độ mặc định (`--reset` bật): **xóa sạch data stream trước mỗi lần chạy** —
truncate 3 bảng stream trong DB + tạo lại 6 topic rỗng — đảm bảo mỗi phiên là dữ liệu
mới hoàn toàn. Reference data (4 bệnh nhân, labs seed) không bị đụng tới.

> Lưu ý: khi có Flink job đang chạy, reset topic khiến job tự rebalance/replay từ
> đầu topic mới — nếu thấy dữ liệu lạ, chạy `down -v` + `up -d` để reset triệt để
> (mọi thứ tự tái tạo lại).

## Kiểm tra dữ liệu

```powershell
docker exec icu-db psql -U icu_app -d icu
```

```sql
\dt
SELECT count(*) FROM patient_telemetry_stream;         -- data stream (simulator)
SELECT count(*) FROM vitals_sliding_window_5m;         -- Flink 5m windows
SELECT count(*) FROM sepsis_alert_events;              -- alerts
SELECT 'patients' t, count(*) FROM patients            -- reference seed = 4
UNION ALL SELECT 'labevents', count(*) FROM labevents; -- = 8
SELECT * FROM patient_telemetry_stream ORDER BY sample_time DESC LIMIT 8;
```

Consumer lag (writer phải về 0):

```powershell
docker exec icu-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group icu-timescale-writer-v2
```

## Dừng / reset

```powershell
# Dừng, giữ nguyên dữ liệu
docker compose -f docker/compose.yaml --env-file .env stop

# Xóa SẠCH toàn bộ (containers + volumes: Kafka, DB, Flink checkpoints)
docker compose -f docker/compose.yaml --env-file .env down -v

# Up lại từ đầu: schema Flyway + seed tự nạp lại qua icu-init
docker compose -f docker/compose.yaml --env-file .env up -d
```

Sau `down -v` **không cần** chạy seed/topic thủ công — `icu-init` lo toàn bộ.

## Ma trận hiển thị Trend (client)

| Range | Dữ liệu | Bucket | Hiển thị | Cập nhật |
|---|---|---|---|---|
| 5m | Raw 1 mẫu/giây | 1s | Line 1.5px | Live tick qua WS |
| 1h | time_bucket | 10s | Line mượt | Roll-up bucket + refetch 60s |
| 2h | time_bucket + min/max | 20s | Line + dải Min–Max | Bucket + refetch 60s |
| 12h | time_bucket | 120s | Macro trend | Load 1 lần + refetch 60s |

## Cấu trúc project

```
docker/compose.yaml          # Toàn bộ hạ tầng (kafka, ui, db, flink, server, client, writer, init)
docker/init/                 # One-shot: tạo topic + seed reference
docker/flink/Dockerfile      # PyFlink 2.3.0 + kafka connector 5.0.0-2.2
server/                      # Spring Boot: REST + Kafka consumer + WebSocket + Flyway (db/migration)
client/                      # React + Vite: ICU monitor UI
services/storage_writer/     # Python: Kafka → TimescaleDB (idempotent, DLQ)
jobs/flink/                  # PyFlink job: validate, dedup, 5m window, hypotension alert
scripts/demo/                # simulate_stream.py (simulator), seed_sources.py, kafka_topics.py
config/                      # db_reference_seed.json, demo_patients.yaml (4 bệnh nhân MIMIC demo)
docs/                        # Thiết kế DB, Kafka message formats, alert rules
```

## Đọc tiếp

- [Thiết kế 11 bảng, khóa/index và SQL kiểm tra](docs/DATABASE_DESIGN.md)
- [Kafka message contract và ví dụ JSON](docs/KAFKA_MESSAGE_FORMATS.md)
- [Rule cảnh báo, recovery và giảm false alarm](docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md)
- [Luồng Kafka/Flink chi tiết](KAFKA_FLINK_FLOW.md)
