# Start everything again
docker compose up -d

# Verify running
docker ps


# Wikimedia Real-Time Streaming Pipeline

Real-time data pipeline ingesting live Wikipedia edits into PostgreSQL via Kafka.

## Architecture
Wikipedia SSE Stream → Kafka Producer → Kafka Topic → Kafka Consumer → PostgreSQL

## Stack
- Apache Kafka (KRaft mode)
- Python (kafka-python, psycopg2)
- PostgreSQL 15
- Docker

## Run Locally
```powershell
docker compose up -d
python src/producer.py
python src/consumer_postgres.py
