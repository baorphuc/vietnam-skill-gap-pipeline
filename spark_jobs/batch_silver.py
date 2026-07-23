"""
spark_jobs/batch_silver.py

BƯỚC 3 (Week 4) — Silver layer: đọc Bronze parsed_parquet, clean + normalize.

Xử lý:
  1. Dedup theo job_id — Bronze có thể có nhiều bản ghi trùng job_id
     (do Kafka không tự dedup theo key khi ghi, producer có thể chạy
     lại nhiều lần). Giữ bản ghi có crawled_at mới nhất.
  2. Chuẩn hóa skills_raw (string "python, sql, aws") -> skills (array<string>)
     dedup case-insensitive, canonical lowercase để Gold layer group chính xác
     (vd "AWS" và "aws" phải tính là 1 skill khi đếm top skills).
  3. Parse salary_raw (string tự do như "500 - 2,500 USD", "Up to $3000",
     null khi ẩn lương) -> salary_min, salary_max (double), currency (string).
  4. Parse date_posted -> proper DateType.
  5. Data quality flag: has_salary, skill_count.

Chạy 1 lần (batch), đọc TOÀN BỘ Bronze hiện có, ghi đè Silver:

    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        /opt/spark_jobs/batch_silver.py
"""

import os
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, DoubleType

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")  # KHÔNG có http://
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

BUCKET = "skillgap"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/parsed_parquet/"
SILVER_PATH = f"s3a://{BUCKET}/silver/jobs_clean/"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("vietnam-skill-gap-silver-batch")
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


# ---------------------------------------------------------------------------
# Salary parsing — dữ liệu gốc rất tự do (vd "500 - 2,500 USD", "Up to $3000",
# null khi công ty ẩn lương). Dùng UDF Python cho linh hoạt hơn regex SQL thuần.
# ---------------------------------------------------------------------------

SALARY_SCHEMA = StructType([
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("currency", StringType(), True),
])


def parse_salary(salary_raw):
    if not salary_raw:
        return (None, None, None)

    text = salary_raw.strip()
    currency = "USD" if ("$" in text or "USD" in text.upper()) else None

    # Bỏ ký tự tiền tệ + dấu phẩy ngăn cách nghìn để regex số dễ match
    cleaned = text.replace("$", "").replace(",", "")

    # Pattern "X - Y" (có thể có chữ USD kèm theo)
    range_match = re.search(r"([\d.]+)\s*-\s*([\d.]+)", cleaned)
    if range_match:
        return (float(range_match.group(1)), float(range_match.group(2)), currency or "USD")

    # Pattern "Up to X"
    upto_match = re.search(r"up to\s*([\d.]+)", cleaned, re.IGNORECASE)
    if upto_match:
        return (None, float(upto_match.group(1)), currency or "USD")

    # Pattern "From X"
    from_match = re.search(r"from\s*([\d.]+)", cleaned, re.IGNORECASE)
    if from_match:
        return (float(from_match.group(1)), None, currency or "USD")

    # Không parse được (vd "Thỏa thuận" lẽ ra đã bị lọc thành null ở Bronze,
    # nhưng phòng hờ record nào lọt qua)
    return (None, None, None)


parse_salary_udf = F.udf(parse_salary, SALARY_SCHEMA)


# ---------------------------------------------------------------------------
# Skill normalization — split string -> array, dedup case-insensitive,
# lowercase làm canonical form để Gold layer group chính xác.
# ---------------------------------------------------------------------------

def normalize_skills(skills_raw):
    if not skills_raw:
        return []
    parts = [s.strip().lower() for s in skills_raw.split(",") if s.strip()]
    # dedup giữ thứ tự xuất hiện đầu tiên
    seen = []
    for p in parts:
        if p not in seen:
            seen.append(p)
    return seen


normalize_skills_udf = F.udf(normalize_skills, ArrayType(StringType()))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đọc Bronze từ {BRONZE_PATH} ...")
    bronze_df = spark.read.parquet(BRONZE_PATH)
    total_bronze = bronze_df.count()
    print(f"Tổng số record Bronze (bao gồm duplicate): {total_bronze}")

    # ── Dedup theo job_id, giữ bản ghi có crawled_at mới nhất ──
    window_spec = Window.partitionBy("job_id").orderBy(F.col("crawled_at").desc())
    deduped_df = (
        bronze_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    total_deduped = deduped_df.count()
    print(f"Sau dedup theo job_id: {total_deduped} record unique "
          f"(loại bỏ {total_bronze - total_deduped} bản trùng)")

    # ── Normalize skills + salary ──
    silver_df = (
        deduped_df
        .withColumn("skills", normalize_skills_udf(F.col("skills_raw")))
        .withColumn("skill_count", F.size(F.col("skills")))
        .withColumn("salary_parsed", parse_salary_udf(F.col("salary_raw")))
        .withColumn("salary_min", F.col("salary_parsed.salary_min"))
        .withColumn("salary_max", F.col("salary_parsed.salary_max"))
        .withColumn("currency", F.col("salary_parsed.currency"))
        .withColumn("has_salary", F.col("salary_min").isNotNull() | F.col("salary_max").isNotNull())
        .withColumn(
            "date_posted_parsed",
            F.coalesce(F.to_date(F.col("date_posted")), F.to_date(F.col("crawled_at")))
        )
        .drop("salary_parsed", "skills_raw")
    )

    print("\nSample sau khi normalize:")
    silver_df.select(
        "job_id", "title", "level", "skills", "skill_count",
        "salary_min", "salary_max", "currency", "has_salary"
    ).show(5, truncate=60)

    print(f"\nGhi Silver → {SILVER_PATH}")
    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("level")
        .parquet(SILVER_PATH)
    )

    print(f"✅ Hoàn thành: {total_deduped} jobs unique → {SILVER_PATH}")

    # ── Quick data quality report ──
    print("\n=== Data Quality Report ===")
    print(f"Tổng jobs: {total_deduped}")
    print(f"Jobs có salary công khai: {silver_df.filter(F.col('has_salary')).count()}")
    print("Phân bố level:")
    silver_df.groupBy("level").count().orderBy(F.desc("count")).show()

    spark.stop()


if __name__ == "__main__":
    main()
