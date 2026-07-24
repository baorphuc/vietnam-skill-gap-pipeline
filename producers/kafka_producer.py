"""
producers/kafka_producer.py

BƯỚC 3 — Publish dữ liệu đã scrape (data/raw_jobs.json) lên Kafka.

Tách riêng khỏi itviec_scraper.py để:
  - Có thể re-run publish nhiều lần mà không cần scrape lại (tốn thời gian + risk bị block)
  - Dễ debug: xem trước data/raw_jobs.json trước khi đẩy vào pipeline

Usage:
    pip install kafka-python --break-system-packages
    python3 producers/kafka_producer.py
"""

import json
import os
import time

from kafka import KafkaProducer

# Chạy từ WSL host (ngoài Docker network) -> "localhost:29092" (port đã map ra host)
# Chạy TRONG container cùng Docker network (vd từ Airflow) -> "kafka:9092"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "raw.jobs"
INPUT_FILE = "data/raw_jobs.json"


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        jobs = json.load(f)

    print(f"Đọc {len(jobs)} jobs từ {INPUT_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    sent = 0
    failed = 0

    for job in jobs:
        try:
            future = producer.send(topic=TOPIC, key=job["job_id"], value=job)
            future.get(timeout=10)
            sent += 1
            if sent % 25 == 0:
                print(f"  → đã gửi {sent}/{len(jobs)}")
        except Exception as e:
            failed += 1
            print(f"  ✗ lỗi gửi {job.get('job_id')}: {e}")

    producer.flush()
    print(f"\n✅ Xong: {sent} thành công, {failed} thất bại → topic '{TOPIC}'")


if __name__ == "__main__":
    main()
