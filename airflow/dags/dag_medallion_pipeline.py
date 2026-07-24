"""
airflow/dags/dag_medallion_pipeline.py

DAG chính điều phối toàn bộ pipeline Vietnam Skill Gap:

    scrape_and_produce
            |
            v
      bronze_ingest  (Spark Structured Streaming, RUN_ONCE=true -> tự dừng)
            |
            v
      silver_transform (dedup + normalize)
            |
      +-----+-----+
      v           v
  gold_tier1   gold_tier2   (chạy song song, đều phụ thuộc Silver)

Cách chạy:
  - scrape_and_produce: chạy trực tiếp trong container Airflow (đã cài
    curl_cffi/beautifulsoup4/kafka-python qua airflow/Dockerfile).
  - bronze_ingest, silver_transform, gold_tier1, gold_tier2: đều là Spark
    job -> orchestrate bằng cách `docker exec` vào container spark-master
    (Airflow không tự chạy Spark, chỉ điều khiển container khác qua
    Docker socket đã mount — xem docker-compose.yml).

Schedule: chạy hàng tuần (Chủ nhật 1h sáng). Đặt catchup=False để không
backfill các lần chạy trong quá khứ khi mới bật DAG lần đầu.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = "docker exec spark-master /opt/spark/bin/spark-submit"
SPARK_PACKAGES_KAFKA = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
SPARK_PACKAGES_S3_ONLY = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
SPARK_CONF_LOWMEM = "--conf spark.executor.memory=1g --conf spark.executor.cores=1"

default_args = {
    "owner": "baorphuc",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="vietnam_skill_gap_medallion_pipeline",
    description="ITviec scrape -> Kafka -> Bronze -> Silver -> Gold (Tier1+2)",
    default_args=default_args,
    schedule_interval="0 1 * * 0",  # Chủ nhật 1h sáng hàng tuần
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["skill-gap", "medallion", "portfolio"],
) as dag:

    # ── Task 1: Scrape ITviec + publish lên Kafka ──
    # Chạy trực tiếp bằng python3 trong container Airflow (đã có curl_cffi,
    # beautifulsoup4, kafka-python cài sẵn qua airflow/Dockerfile).
    scrape_and_produce = BashOperator(
        task_id="scrape_and_produce",
        bash_command=(
            "cd /opt/producers && "
            "python3 itviec_scraper.py && "
            "python3 clean_existing_data.py && "
            "python3 kafka_producer.py"
        ),
    )

    # ── Task 2: Bronze — Spark Structured Streaming, RUN_ONCE=true ──
    # availableNow trigger: xử lý hết message đang có trong Kafka rồi tự
    # dừng (không chạy vô hạn), phù hợp với mô hình task của Airflow.
    # `docker exec -e RUN_ONCE=true spark-master ...` truyền env var vào
    # bên trong container spark-master cho lần chạy này.
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "docker exec -e RUN_ONCE=true spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES_KAFKA} "
            f"{SPARK_CONF_LOWMEM} "
            "/opt/spark_jobs/streaming_bronze.py"
        ),
    )

    # ── Task 3: Silver — dedup + normalize ──
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES_S3_ONLY} "
            f"{SPARK_CONF_LOWMEM} "
            f"/opt/spark_jobs/batch_silver.py"
        ),
    )

    # ── Task 4a: Gold Tier 1 ──
    gold_tier1 = BashOperator(
        task_id="gold_tier1",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES_S3_ONLY} "
            f"{SPARK_CONF_LOWMEM} "
            f"/opt/spark_jobs/batch_gold.py"
        ),
    )

    # ── Task 4b: Gold Tier 2 ──
    gold_tier2 = BashOperator(
        task_id="gold_tier2",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES_S3_ONLY} "
            f"{SPARK_CONF_LOWMEM} "
            f"/opt/spark_jobs/batch_gold_tier2.py"
        ),
    )

    scrape_and_produce >> bronze_ingest >> silver_transform >> [gold_tier1, gold_tier2]
