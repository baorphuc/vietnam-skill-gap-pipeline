"""
spark_jobs/streaming_bronze.py

Đọc job postings từ Kafka topic `raw.jobs` và ghi vào MinIO theo Bronze layer:
  - bronze/raw_json/       -> raw message JSON nguyên bản (append, 1 file/batch)
  - bronze/parsed_parquet/ -> đã parse theo schema cố định (Parquet, partition theo ngày)

Submit vào cluster đang chạy (spark-master:7077):

    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        /opt/spark_jobs/streaming_bronze.py

(Nếu image apache/spark không cache sẵn packages, lần chạy đầu sẽ tải qua
maven — cần mạng ra ngoài từ container.)

Env vars kỳ vọng có sẵn trong container (đặt trong docker-compose.yml):
    MINIO_ENDPOINT       (vd: http://minio:9000)
    MINIO_ACCESS_KEY
    MINIO_SECRET_KEY
    KAFKA_BOOTSTRAP      (vd: kafka:9092)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = "raw.jobs"

# RUN_ONCE=true -> dùng trigger(availableNow=True): xử lý hết data hiện có
# trong Kafka rồi TỰ DỪNG (thay vì chạy streaming vô hạn). Cần thiết để
# Airflow orchestrate job này như 1 task bình thường (có điểm kết thúc).
RUN_ONCE = os.environ.get("RUN_ONCE", "false").lower() == "true"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")  # KHÔNG có http:// —
# hadoop-aws 3.3.x bị hang/SSL-timeout dài nếu endpoint có scheme + ssl.enabled=false
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

BUCKET = "skillgap"
RAW_JSON_PATH = f"s3a://{BUCKET}/bronze/raw_json/"
PARSED_PARQUET_PATH = f"s3a://{BUCKET}/bronze/parsed_parquet/"
CHECKPOINT_ROOT = f"s3a://{BUCKET}/bronze/_checkpoints/"

# Schema khớp với danh sách field trong progress summary
JOB_SCHEMA = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("salary_raw", StringType(), True),
    StructField("skills_raw", StringType(), True),   # giữ raw (list/string) -> normalize ở Silver
    StructField("level", StringType(), True),
    StructField("months_experience", DoubleType(), True),
    StructField("employment_type", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("date_posted", StringType(), True),  # parse thành date ở Silver, tránh lỗi format lệch
    StructField("source", StringType(), True),
    StructField("url", StringType(), True),
    StructField("crawled_at", StringType(), True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("vietnam-skill-gap-bronze-streaming")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.sql.shuffle.partitions", "4")  # máy 12GB, giữ nhỏ
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # đổi "latest" sau khi backfill xong lần đầu
        .option("failOnDataLoss", "false")
        .load()
    )

    # value là bytes -> string JSON; giữ cả key/timestamp Kafka để trace
    raw_df = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # -----------------------------------------------------------------
    # Sink 1: raw_json — lưu nguyên bản, không parse, để có thể replay
    # Silver/Gold sau này nếu schema đổi mà không mất dữ liệu gốc
    # -----------------------------------------------------------------
    trigger_kwargs = {"availableNow": True} if RUN_ONCE else {"processingTime": "30 seconds"}
    print(f"Trigger mode: {'availableNow (chạy 1 lần rồi dừng)' if RUN_ONCE else 'continuous (30s/batch)'}")

    raw_query = (
        raw_df.writeStream
        .format("json")
        .option("path", RAW_JSON_PATH)
        .option("checkpointLocation", CHECKPOINT_ROOT + "raw_json/")
        .outputMode("append")
        .trigger(**trigger_kwargs)
        .start()
    )

    # -----------------------------------------------------------------
    # Sink 2: parsed_parquet — parse theo JOB_SCHEMA, partition theo ngày crawl
    # -----------------------------------------------------------------
    parsed_df = (
        raw_df
        .select(
            F.from_json(F.col("raw_json"), JOB_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn(
            "crawled_date",
            F.to_date(F.coalesce(F.col("crawled_at"), F.col("kafka_timestamp").cast("string")))
        )
        # bỏ record parse thất bại hoàn toàn (mọi field chính đều null)
        .filter(F.col("job_id").isNotNull() | F.col("title").isNotNull())
    )

    parsed_query = (
        parsed_df.writeStream
        .format("parquet")
        .option("path", PARSED_PARQUET_PATH)
        .option("checkpointLocation", CHECKPOINT_ROOT + "parsed_parquet/")
        .partitionBy("crawled_date")
        .outputMode("append")
        .trigger(**trigger_kwargs)
        .start()
    )

    if RUN_ONCE:
        # availableNow tự dừng khi hết data -> chờ CẢ 2 query kết thúc
        # (awaitAnyTermination sẽ thoát ngay khi 1 trong 2 xong, bỏ sót cái còn lại)
        raw_query.awaitTermination()
        parsed_query.awaitTermination()
        print("✅ RUN_ONCE hoàn thành — đã xử lý hết data hiện có trong Kafka.")
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
