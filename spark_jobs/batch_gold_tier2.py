"""
spark_jobs/batch_gold_tier2.py

Gold layer Tier 2 metrics:
  1. skill_cooccurrence — cặp skill nào hay xuất hiện cùng nhau trong 1 job
     (chỉ tính trong phạm vi Top 20 skill để tránh bùng nổ số cặp)
  2. skill_gap_by_level  — % job Junior vs % job Senior yêu cầu từng skill,
     gap = pct_senior - pct_junior (dương = Senior cần nhiều hơn Junior)

LƯU Ý: "Emerging skills (MoM growth)" trong plan gốc CHƯA làm được ở bước
này — cần dữ liệu crawl nhiều đợt theo thời gian để tính % tăng trưởng
theo tháng. Hiện chỉ có 1 lần crawl (single snapshot), nên metric này để
lại cho sau khi có lịch crawl định kỳ qua Airflow (Week 6).

CẢNH BÁO DATA: Junior chỉ có 3 job trong dataset (n=3) — % theo Junior
rất nhiễu (mỗi job đổi ~33%). Số liệu mang tính tham khảo, không đại diện
thống kê đầy đủ.

Chạy 1 lần (batch), đọc Silver + Gold top_skills đã có, ghi thêm vào Gold:

    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        /opt/spark_jobs/batch_gold_tier2.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

BUCKET = "skillgap"
SILVER_PATH = f"s3a://{BUCKET}/silver/jobs_clean/"
GOLD_TOP_SKILLS_PATH = f"s3a://{BUCKET}/gold/top_skills/"
GOLD_COOCCURRENCE_PATH = f"s3a://{BUCKET}/gold/skill_cooccurrence/"
GOLD_LEVEL_GAP_PATH = f"s3a://{BUCKET}/gold/skill_gap_by_level/"

TOP_N_SKILLS = 20
MIN_JOB_COUNT_FOR_GAP = 3  # bỏ skill quá hiếm để tránh % nhiễu vô nghĩa


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("vietnam-skill-gap-gold-tier2-batch")
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

    print(f"Đọc Top {TOP_N_SKILLS} skills từ {GOLD_TOP_SKILLS_PATH} ...")
    top_skills_list = [
        row["skill"] for row in
        spark.read.parquet(GOLD_TOP_SKILLS_PATH).select("skill").collect()
    ]
    print(f"Top skills dùng cho co-occurrence: {top_skills_list}")

    exploded_df = (
        silver_df
        .select("job_id", "level", F.explode("skills").alias("skill"))
        .filter(F.col("skill").isin(top_skills_list))  # giới hạn phạm vi tránh bùng nổ cặp
    )

    # ------------------------------------------------------------------
    # Metric 1: Skill co-occurrence — self-join trên job_id, skill_a < skill_b
    # để mỗi cặp chỉ xuất hiện 1 lần (tránh (A,B) và (B,A) trùng nhau)
    # ------------------------------------------------------------------
    left = exploded_df.select(F.col("job_id"), F.col("skill").alias("skill_a"))
    right = exploded_df.select(F.col("job_id"), F.col("skill").alias("skill_b"))

    cooccurrence_df = (
        left.join(right, on="job_id")
        .filter(F.col("skill_a") < F.col("skill_b"))  # loại self-pair + trùng đảo ngược
        .groupBy("skill_a", "skill_b")
        .agg(F.countDistinct("job_id").alias("co_occurrence_count"))
        .filter(F.col("co_occurrence_count") >= 2)  # bỏ cặp chỉ gặp 1 lần, ít ý nghĩa
        .orderBy(F.desc("co_occurrence_count"))
    )

    print("\n=== Top 20 Skill Co-occurrence Pairs ===")
    cooccurrence_df.show(20, truncate=False)

    # ------------------------------------------------------------------
    # Metric 2: Skill gap Junior vs Senior
    # % job theo từng level yêu cầu skill X, rồi so sánh Senior - Junior
    # ------------------------------------------------------------------
    level_totals = (
        silver_df.groupBy("level").agg(F.countDistinct("job_id").alias("total_jobs_in_level"))
    )
    print("\nTổng job theo level (dùng làm mẫu số %):")
    level_totals.show()

    skill_level_counts = (
        exploded_df
        .groupBy("skill", "level")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .join(level_totals, on="level")
        .withColumn("pct_in_level", F.round(F.col("job_count") / F.col("total_jobs_in_level") * 100, 1))
    )

    junior_pct = (
        skill_level_counts.filter(F.col("level") == "Junior")
        .select(F.col("skill"), F.col("pct_in_level").alias("pct_junior"))
    )
    senior_pct = (
        skill_level_counts.filter(F.col("level") == "Senior")
        .select(F.col("skill"), F.col("pct_in_level").alias("pct_senior"))
    )
    # tổng job xuất hiện của skill (cả 3 level) để lọc bớt skill quá hiếm
    skill_total_jobs = (
        exploded_df.groupBy("skill").agg(F.countDistinct("job_id").alias("total_skill_jobs"))
    )

    gap_df = (
        senior_pct.join(junior_pct, on="skill", how="outer")
        .join(skill_total_jobs, on="skill")
        .fillna(0.0, subset=["pct_junior", "pct_senior"])
        .withColumn("gap_senior_minus_junior", F.round(F.col("pct_senior") - F.col("pct_junior"), 1))
        .filter(F.col("total_skill_jobs") >= MIN_JOB_COUNT_FOR_GAP)
        .orderBy(F.desc("gap_senior_minus_junior"))
    )

    print(f"\n=== Skill Gap: Senior vs Junior (⚠ Junior n=3, số liệu tham khảo) ===")
    gap_df.select("skill", "pct_junior", "pct_senior", "gap_senior_minus_junior", "total_skill_jobs").show(30, truncate=False)

    # ── Ghi ra Gold ──
    print(f"\nGhi skill_cooccurrence → {GOLD_COOCCURRENCE_PATH}")
    cooccurrence_df.coalesce(1).write.mode("overwrite").parquet(GOLD_COOCCURRENCE_PATH)

    print(f"Ghi skill_gap_by_level → {GOLD_LEVEL_GAP_PATH}")
    gap_df.select(
        "skill", "pct_junior", "pct_senior", "gap_senior_minus_junior", "total_skill_jobs"
    ).coalesce(1).write.mode("overwrite").parquet(GOLD_LEVEL_GAP_PATH)

    print("\n✅ Hoàn thành Gold layer Tier 2.")
    spark.stop()


if __name__ == "__main__":
    main()
