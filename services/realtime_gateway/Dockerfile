FROM python:3.12.12-slim@sha256:f3fa41d74a768c2fce8016b98c191ae8c1bacd8f1152870a3f9f87d350920b7c
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY jobs/flink/ jobs/flink/
COPY config/demo_patients.yaml config/demo_patients.yaml
COPY services/realtime_gateway/ services/realtime_gateway/
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
CMD ["python", "services/realtime_gateway/gateway.py"]
