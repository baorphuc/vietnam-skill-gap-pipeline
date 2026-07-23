"""
dashboard/app.py

BƯỚC 5 (Week 5) — Streamlit dashboard, đọc trực tiếp Parquet từ MinIO
bằng DuckDB (httpfs/S3 extension) — không cần copy file về local.

Chạy trực tiếp trên WSL (KHÔNG qua Docker):

    pip install streamlit duckdb pandas plotly --break-system-packages
    python3 -m streamlit run dashboard/app.py

Yêu cầu: MinIO đang chạy và expose port 9000 ra host (đã có sẵn trong
docker-compose.yml: "9000:9000"), nên Streamlit chạy trên WSL host truy
cập MinIO qua "localhost:9000" bình thường.
"""

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

MINIO_ENDPOINT = "localhost:9000"  # Streamlit chạy ngoài Docker network -> dùng localhost
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "skillgap"

st.set_page_config(
    page_title="Vietnam Tech Skill Gap Analytics",
    page_icon="📊",
    layout="wide",
)


@st.cache_resource
def get_duckdb_connection():
    """Tạo kết nối DuckDB với httpfs extension trỏ vào MinIO (S3-compatible)."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    return con


@st.cache_data(ttl=300)  # cache 5 phút — tránh query lại MinIO mỗi lần rerender
def load_top_skills() -> pd.DataFrame:
    con = get_duckdb_connection()
    return con.execute(
        f"SELECT * FROM read_parquet('s3://{BUCKET}/gold/top_skills/*.parquet') "
        f"ORDER BY job_count DESC"
    ).df()


@st.cache_data(ttl=300)
def load_salary_by_skill() -> pd.DataFrame:
    con = get_duckdb_connection()
    return con.execute(
        f"SELECT * FROM read_parquet('s3://{BUCKET}/gold/salary_by_skill/*.parquet') "
        f"ORDER BY job_count_with_salary DESC"
    ).df()


@st.cache_data(ttl=300)
def load_silver_summary() -> pd.DataFrame:
    """Đọc Silver để lấy các số liệu tổng quan (total jobs, companies, level split)."""
    con = get_duckdb_connection()
    return con.execute(
        f"SELECT job_id, company, level, has_salary "
        f"FROM read_parquet('s3://{BUCKET}/silver/jobs_clean/**/*.parquet')"
    ).df()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("📊 Vietnam Tech Skill Gap Analytics")
st.caption("Phân tích nhu cầu kỹ năng thị trường IT Việt Nam — dữ liệu từ ITviec")

try:
    with st.spinner("Đang tải dữ liệu từ MinIO..."):
        top_skills_df = load_top_skills()
        salary_df = load_salary_by_skill()
        silver_df = load_silver_summary()
except Exception as e:
    st.error(
        f"Không kết nối được MinIO ({MINIO_ENDPOINT}). "
        f"Kiểm tra Docker stack đang chạy: `docker compose ps`\n\nChi tiết lỗi: {e}"
    )
    st.stop()

# ── Metrics tổng quan ──
col1, col2, col3, col4 = st.columns(4)
col1.metric("Tổng số job", len(silver_df))
col2.metric("Công ty unique", silver_df["company"].nunique())
col3.metric("Job có lương công khai", int(silver_df["has_salary"].sum()))
col4.metric("Skills tracked", len(top_skills_df))

st.divider()

# ── Top Skills Demand ──
st.subheader("🔥 Top 20 Skills được yêu cầu nhiều nhất")
fig_skills = px.bar(
    top_skills_df,
    x="job_count",
    y="skill",
    orientation="h",
    text="pct_of_jobs",
    labels={"job_count": "Số lượng job", "skill": "Kỹ năng"},
    color="job_count",
    color_continuous_scale="Blues",
)
fig_skills.update_traces(texttemplate="%{text}%", textposition="outside")
fig_skills.update_layout(yaxis=dict(autorange="reversed"), height=600, showlegend=False)
st.plotly_chart(fig_skills, use_container_width=True)

st.divider()

# ── Salary by Skill ──
st.subheader("💰 Mức lương trung bình theo kỹ năng (USD/tháng)")
st.caption("Chỉ tính trên các job có công khai mức lương, tối thiểu 2 job/skill")

salary_display = salary_df.copy()
salary_display["salary_range"] = (
    salary_display["avg_salary_min"].astype(int).astype(str)
    + " - "
    + salary_display["avg_salary_max"].astype(int).astype(str)
)

fig_salary = px.bar(
    salary_display.sort_values("avg_salary_max", ascending=True),
    x="avg_salary_max",
    y="skill",
    orientation="h",
    text="salary_range",
    labels={"avg_salary_max": "Lương trung bình (USD/tháng)", "skill": "Kỹ năng"},
    color="avg_salary_max",
    color_continuous_scale="Greens",
)
fig_salary.update_traces(textposition="outside")
fig_salary.update_layout(height=700, showlegend=False)
st.plotly_chart(fig_salary, use_container_width=True)

st.divider()

# ── Level Distribution ──
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 Phân bố Level")
    level_counts = silver_df["level"].value_counts().reset_index()
    level_counts.columns = ["level", "count"]
    fig_level = px.pie(level_counts, names="level", values="count", hole=0.4)
    st.plotly_chart(fig_level, use_container_width=True)

with col_right:
    st.subheader("🏢 Top công ty đăng nhiều job nhất")
    top_companies = silver_df["company"].value_counts().head(10).reset_index()
    top_companies.columns = ["company", "job_count"]
    fig_companies = px.bar(
        top_companies.sort_values("job_count"),
        x="job_count",
        y="company",
        orientation="h",
    )
    fig_companies.update_layout(showlegend=False)
    st.plotly_chart(fig_companies, use_container_width=True)

st.divider()
st.caption(
    "Dữ liệu: ITviec (crawl qua curl_cffi) → Kafka → Spark Structured Streaming "
    "→ MinIO (Medallion: Bronze/Silver/Gold) → DuckDB → Streamlit"
)
