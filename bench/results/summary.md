# Kết quả benchmark Spark vs Flink

Tự sinh bởi `python bench/summarize.py`. Mỗi ô là **trung vị (min–max)** qua các lần lặp; latency tính bằng ms theo §10.1 của plan.

## E1 — Correctness (kịch bản chuẩn 6 phút, hai engine chạy song song)

| Engine | Lần chạy | Precision / recall telemetry | Precision / recall cửa sổ | Alert khớp | DLQ (chartevents) | Bản ghi trùng |
|---|---|---|---|---|---|---|
| flink | 3 | 1.0000 / 1.0000 | 1.0000 / 1.0000 | 1/1, 1/1, 1/1 | {'DUPLICATE_EVENT': 1, 'VALUE_ERROR': 1, 'LATE_EVENT': 1} | 0 |
| spark | 3 | 1.0000 / 1.0000 | 1.0000 / 1.0000 | 1/1, 1/1, 1/1 | {'DUPLICATE_EVENT': 1, 'VALUE_ERROR': 1} | 0 |

## E2 — Latency theo tải (ms)

| Tải | Engine | Trigger | Telemetry p50 | Telemetry p95 | Cửa sổ p50 | Cửa sổ p95 | Alert p50 | Alert max | CPU TB (%) | RAM đỉnh (MiB) | Drain |
|---|---|---|---|---|---|---|---|---|---|---|---|
| normal | flink | default | 431 | 535 | 592 | 627 | 470 | 470 | 32 | 1,475 | ok |
| normal | spark | 1s | 1,205 | 2,003 | 3,730 | 3,943 | 2,809 | 2,809 | 166 | 2,032 | ok |
| normal | spark | 5s | 3,471 | 5,594 | 13,002 | 13,251 | 9,749 | 9,749 | 49 | 1,362 | ok |
| normal | spark | default | 1,494 | 2,121 | 3,948 | 4,414 | 2,394 | 2,394 | 174 | 1,626 | ok |

### E2 — Tách latency cửa sổ của Spark (Q_wide → topic → Q_window), p50 ms

| Tải | Trigger | Phần Q_wide | Phần Q_window | Batch Q_wide trung vị (ms) | Số batch |
|---|---|---|---|---|---|
| normal | 1s | 1,150 | 2,632 | 832 | 283 |
| normal | 5s | 2,528 | 10,466 | 824 | 61 |
| normal | default | 1,504 | 2,579 | 983 | 279 |

## E3 — Throughput tối đa (nạp sẵn HF200 × 2 phút)

| Engine | Thời gian tới output cuối (s) | Event/giây | CPU TB (%) | RAM đỉnh (MiB) | Drain |
|---|---|---|---|---|---|
| flink | 1,340.5 | 430 | 142 | 1,732 | ok |
| spark | 93.8 | 6,143 | 95 | 3,127 | ok |
