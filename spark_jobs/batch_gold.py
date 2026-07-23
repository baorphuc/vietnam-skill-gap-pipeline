"""
spark_jobs/batch_gold.py

BƯỚC 4 (Week 5) — Gold layer: đọc Silver, tính Tier 1 metrics.

Tier 1 metrics (lần này):
  1. top_skills        — skill, số job yêu cầu, % trên tổng số job
  2. salary_by_skill    — skill, lương trung bình min/max, số job có lương

(Tier 2 — skill co-occurrence, Junior vs Senior gap, emerging skills theo
tháng — để sau khi dashboard chạy được, và cần crawl thêm nhiều đợt theo
thời gian mới có ý nghĩa cho phần "trend theo tháng".)

Chạy 1 lần (batch), đọc Silver, ghi đè Gold:

    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        /opt/spark_jobs/batch_gold.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")  # KHÔNG có http://
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

BUCKET = "skillgap"
SILVER_PATH = f"s3a://{BUCKET}/silver/jobs_clean/"
GOLD_TOP_SKILLS_PATH = f"s3a://{BUCKET}/gold/top_skills/"
GOLD_SALARY_BY_SKILL_PATH = f"s3a://{BUCKET}/gold/salary_by_skill/"

TOP_N_SKILLS = 20


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("vietnam-skill-gap-gold-batch")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đọc Silver từ {SILVER_PATH} ...")
    silver_df = spark.read.parquet(SILVER_PATH)
    total_jobs = silver_df.count()
    print(f"Tổng số job (Silver): {total_jobs}")

    # Explode skills: 1 row / skill / job — nền tảng cho cả 2 metric
    exploded_df = silver_df.select(
        "job_id", "skills", "salary_min", "salary_max", "has_salary", "level"
    ).withColumn("skill", F.explode("skills"))

    # ── Metric 1: Top skills demand ──
    top_skills_df = (
        exploded_df
        .groupBy("skill")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .withColumn("pct_of_jobs", F.round(F.col("job_count") / total_jobs * 100, 1))
        .orderBy(F.desc("job_count"))
        .limit(TOP_N_SKILLS)
    )

    print(f"\n=== Top {TOP_N_SKILLS} Skills ===")
    top_skills_df.show(TOP_N_SKILLS, truncate=False)

    # ── Metric 2: Salary by skill (chỉ tính trên job có salary công khai) ──
    salary_by_skill_df = (
        exploded_df
        .filter(F.col("has_salary"))
        .groupBy("skill")
        .agg(
            F.countDistinct("job_id").alias("job_count_with_salary"),
            F.round(F.avg("salary_min"), 0).alias("avg_salary_min"),
            F.round(F.avg("salary_max"), 0).alias("avg_salary_max"),
        )
        # chỉ giữ skill có đủ mẫu để trung bình có ý nghĩa (>=2 job)
        .filter(F.col("job_count_with_salary") >= 2)
        # loại "crossed range" — khi avg_min > avg_max, dấu hiệu min/max được
        # tính trung bình từ các job KHÁC NHAU (1 job chỉ có min, job khác chỉ
        # có max) do mẫu quá thưa, nên khoảng lương ra không có ý nghĩa thực tế
        .filter(
            F.col("avg_salary_min").isNull()
            | F.col("avg_salary_max").isNull()
            | (F.col("avg_salary_min") <= F.col("avg_salary_max"))
        )
        .orderBy(F.desc("job_count_with_salary"))
    )

    print("\n=== Salary by Skill (job_count_with_salary >= 2) ===")
    salary_by_skill_df.show(30, truncate=False)

    # ── Ghi ra Gold (coalesce(1) — bảng nhỏ, gộp thành 1 file cho dễ đọc
    # bằng DuckDB/Streamlit sau này) ──
    print(f"\nGhi top_skills → {GOLD_TOP_SKILLS_PATH}")
    top_skills_df.coalesce(1).write.mode("overwrite").parquet(GOLD_TOP_SKILLS_PATH)

    print(f"Ghi salary_by_skill → {GOLD_SALARY_BY_SKILL_PATH}")
    salary_by_skill_df.coalesce(1).write.mode("overwrite").parquet(GOLD_SALARY_BY_SKILL_PATH)

    print("\n✅ Hoàn thành Gold layer (Tier 1).")
    spark.stop()


if __name__ == "__main__":
    main()
