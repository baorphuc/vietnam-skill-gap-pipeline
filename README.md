# 🇻🇳 Vietnam Tech Skill Gap Analytics Pipeline

> A production-style data engineering project that ingests Vietnamese tech job postings in real-time, processes them through a Medallion architecture, and surfaces skill demand insights via an interactive dashboard.

## Architecture

```
ITviec Scraper
      │
      ▼
Kafka (topic: raw.jobs)
      │
      ▼
Spark Structured Streaming
      │
      ▼
MinIO (S3-compatible)
  bronze/
  ├── raw_json/         ← raw Kafka messages
  └── parsed_parquet/   ← schema-parsed
  silver/               ← cleaned + skill-normalized
  gold/                 ← aggregated metrics
      │
      ▼
DuckDB → Streamlit Dashboard

Airflow orchestrates batch jobs (Silver, Gold layers)
```

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Kafka 7.6 + Python scraper |
| Processing | Apache Spark 3.5 (Structured Streaming + Batch) |
| Storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow 2.8 |
| Serving | DuckDB + Streamlit |
| Skill NLP | Regex dict → ESCO mapping (SBERT) |

## Quick Start

### 1. Start the stack

```bash
cd infra
docker compose up -d
```

### 2. Verify everything is healthy

```bash
chmod +x scripts/verify_stack.sh
./scripts/verify_stack.sh
```

### 3. Send test messages

```bash
pip install kafka-python
python scripts/test_producer.py
```

## UI Endpoints

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Kafka UI *(tắt mặc định)* | http://localhost:8090 | — |

> **Máy RAM thấp (≤12GB):** Kafka UI đã bị comment out trong `docker-compose.yml` để tiết kiệm tài nguyên. Bỏ comment block `kafka-ui` khi cần xem messages trực quan, hoặc dùng `kafka-console-consumer` qua `docker exec` thay thế.

### Xem Kafka messages không cần UI

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw.jobs \
  --from-beginning
```

## Project Phases

- [x] **Week 1** — Docker stack: Kafka + Spark + MinIO + Airflow
- [ ] **Week 2** — ITviec scraper + Kafka producer
- [ ] **Week 3** — Spark Streaming → Bronze layer
- [ ] **Week 4** — Silver: clean + skill normalization
- [ ] **Week 5** — Gold metrics + DuckDB + Streamlit
- [ ] **Week 6** — Airflow DAGs + monitoring + README polish

## Gold Layer Metrics

- Top 20 skills by demand
- Skill demand trend (monthly)
- Salary distribution by skill
- Skill co-occurrence matrix
- Junior vs Senior skill gap
- Emerging skills (MoM growth %)

## Author

**Bao Phuc** — Information Systems, UIT  
GitHub: [@baorphuc](https://github.com/baorphuc) · LinkedIn: [baorphuc](https://www.linkedin.com/in/baorphuc/)
