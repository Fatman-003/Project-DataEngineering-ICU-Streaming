FROM python:3.12.12-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.16.0 "psycopg[binary]==3.3.6"

COPY scripts/demo/kafka_topics.py /app/scripts/demo/kafka_topics.py
COPY scripts/demo/seed_sources.py /app/scripts/demo/seed_sources.py
COPY jobs/flink /app/jobs/flink
COPY config/db_reference_seed.json /app/config/db_reference_seed.json
COPY docker/init/init.py /app/init.py

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-B", "/app/init.py"]
