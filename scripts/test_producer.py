"""
scripts/test_producer.py
Smoke test: gửi 3 job messages giả lên Kafka topic raw.jobs
Dùng để verify stack hoạt động trước khi viết scraper thật

Usage:
    pip install kafka-python
    python scripts/test_producer.py
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "raw.jobs"

SAMPLE_JOBS = [
    {
        "job_id": "itviec_001",
        "title": "Data Engineer",
        "company": "VNG Corporation",
        "location": "Ho Chi Minh City",
        "salary_raw": "2,000 - 3,500 USD",
        "skills_raw": "Python, Apache Spark, Kafka, Airflow, AWS S3, SQL",
        "level": "Senior",
        "source": "itviec",
        "crawled_at": datetime.utcnow().isoformat(),
    },
    {
        "job_id": "itviec_002",
        "title": "Junior Data Analyst",
        "company": "Tiki",
        "location": "Ho Chi Minh City",
        "salary_raw": "700 - 1,200 USD",
        "skills_raw": "SQL, Python, Power BI, Excel, Google Analytics",
        "level": "Junior",
        "source": "itviec",
        "crawled_at": datetime.utcnow().isoformat(),
    },
    {
        "job_id": "itviec_003",
        "title": "ML Engineer",
        "company": "Momo",
        "location": "Ho Chi Minh City",
        "salary_raw": "2,500 - 4,000 USD",
        "skills_raw": "Python, TensorFlow, Docker, Kubernetes, MLflow, SQL",
        "level": "Mid",
        "source": "itviec",
        "crawled_at": datetime.utcnow().isoformat(),
    },
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Sending {len(SAMPLE_JOBS)} test messages to topic '{TOPIC}'...\n")

    for job in SAMPLE_JOBS:
        future = producer.send(
            topic=TOPIC,
            key=job["job_id"],
            value=job,
        )
        record = future.get(timeout=10)
        print(f"✓ Sent: {job['job_id']} | {job['title']} @ {job['company']}")
        print(f"  → partition={record.partition}, offset={record.offset}\n")
        time.sleep(0.2)

    producer.flush()
    print("Done — check Kafka UI at http://localhost:8090")


if __name__ == "__main__":
    main()
