# airflow/Dockerfile
#
# Mở rộng image Airflow chính thức để:
#   1. Cài dependencies cho itviec_scraper.py / kafka_producer.py chạy
#      trực tiếp trong container Airflow (curl_cffi, beautifulsoup4, kafka-python)
#   2. Cài Docker CLI để Airflow có thể `docker exec` vào container
#      spark-master (Bronze/Silver/Gold jobs vẫn chạy bằng Spark thật,
#      Airflow chỉ điều phối — không tự chạy Spark trong chính nó)

FROM apache/airflow:2.8.1-python3.11

USER root

# Docker CLI (không cần Docker daemon riêng — dùng chung daemon của host
# qua /var/run/docker.sock được mount vào container, xem docker-compose.yml)
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
