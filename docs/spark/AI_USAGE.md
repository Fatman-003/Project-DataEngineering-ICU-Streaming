# Sử dụng công cụ AI (giải pháp Spark)

Đề cương môn học yêu cầu ghi rõ công cụ AI và cách sử dụng. File này liệt kê các
phần của giải pháp Spark do một AI coding agent tạo ra hoặc hỗ trợ. Nhóm review,
chạy và chịu trách nhiệm cho mọi thay đổi, và tự viết lời văn cuối cùng của báo cáo.

- **Công cụ.** Claude Code (Anthropic), model Claude Opus 5.5, chạy trong repo
  dự án trên server Linux dùng để thử nghiệm.
- **Cách dùng.** Nhóm viết plan (`docs/plans/SPARK_ALTERNATIVE_PLAN.md`). Agent
  hiện thực theo từng phase, dừng lại sau mỗi phase để nhóm review, và hỏi lại
  nhóm mỗi khi repo khác với giả định của plan.

| Phase | Sản phẩm | Vai trò của AI | Vai trò của nhóm |
|---|---|---|---|
| Chuẩn bị | `server/Dockerfile`, `docker/flink/Dockerfile`, `docker/compose.yaml` (biến tùy chọn `MAVEN_MIRROR`) | Chẩn đoán lỗi 429 của Maven Central và viết bản sửa | Duyệt bản sửa |
| 0 | `docker/spark/Dockerfile` | Chọn và pin phiên bản (Spark, Python, jar, pip) kèm checksum | Chọn Spark 4.1.3 trong các phương án được đưa ra |
| 0 | `jobs/spark/spikes/spike.py` | Viết và chạy spike S1–S6, thêm phân tích F1 | Review bằng chứng |
| 0 | `docs/spark/SPIKE_FINDINGS.md`, `docs/spark/DECISIONS.md` | Soạn từ output spike | Quyết định D1 (baseline), D2 (phiên bản), D5 (nhãn), D10 (fetch wait) |
| 1 | `jobs/spark/{spark_config,spark_topics,records,metrics_listener,q_labs,main}.py`, service `icu-spark` trong `docker/compose.yaml`, `docker/compose.bench.yaml` | Viết code, build image, kiểm tra healthcheck và Spark UI | Review; duyệt ngân sách tài nguyên D13 |
| 1 | `tests/spark/test_records.py` | Viết test cho envelope nguồn, key/header sink và cấu hình | Review |
| 2 | `jobs/spark/spikes/spike_state.py`, `SPIKE_FINDINGS.md` S7–S9 | Viết và chạy spike bổ sung về state API | Review |
| 2 | `jobs/spark/{wide_logic,parse_step,q_wide}.py`, `tests/spark/test_wide_logic.py` | Viết logic Q_wide dùng lại module Flink, test parity với operator Flink thật, chạy so sánh trên Kafka | Review; quyết định D16–D19 |
| 3 | `jobs/spark/{window_logic,q_window}.py`, `tests/spark/test_window_logic.py` | Viết Q_window, test so với Welford của Flink, chạy so sánh catch-up và live | Review; D20–D21 |
| 4 | `jobs/spark/{alert_logic,q_alert}.py`, `tests/spark/test_alert_logic.py` | Viết Q_alert, chạy lại 27 test cảnh báo có sẵn trên logic Spark, so sánh live 3 kịch bản | Review; D22 |
| 5 | `bench/{collect,reference,compare}.py`, `bench/requirements.txt`, `tests/bench/test_reference.py` | Viết công cụ thu dữ liệu, ground truth độc lập và so sánh; chạy trên 3 kịch bản | Review; D23–D24 |
