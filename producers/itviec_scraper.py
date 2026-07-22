"""
producers/itviec_scraper.py

BƯỚC 2 — Scraper chính thức cho ITviec.

Cách hoạt động:
  1. Fetch các trang listing (VD: /it-jobs/data-engineer?page=1,2,3...)
     → lấy job slug từ attribute data-search--job-selection-job-slug-value
     (mỗi trang có 20 job cards, đây là cách ITviec render list phía server)
  2. Với mỗi slug, fetch trang detail /it-jobs/{slug}
     → parse schema.org JobPosting JSON-LD (rất sạch, không cần đoán CSS class)
  3. Lưu kết quả ra file JSON local (data/raw_jobs.json)
     → bước sau (kafka_producer.py) sẽ đọc file này và publish lên Kafka

Categories mặc định: data-engineer, data-analyst, data-scientist, business-analyst
Mục tiêu: ~200-300 jobs

Usage:
    pip install curl_cffi beautifulsoup4 --break-system-packages
    python3 producers/itviec_scraper.py
"""

import json
import os
import random
import re
import time
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from curl_cffi import requests as cf_requests

# ── Config ───────────────────────────────────────────────────
BASE_URL = "https://itviec.com"
CATEGORIES = [
    "data-engineer",
    "data-analyst",
    "data-scientist",
    "business-analyst",
]
MAX_PAGES_PER_CATEGORY = 5      # ITviec ~20 jobs/page → 5 pages ≈ 100 jobs/category
TARGET_TOTAL_JOBS = 300
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "raw_jobs.json")

HEADERS = {
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

os.makedirs(OUTPUT_DIR, exist_ok=True)


def polite_sleep():
    """Random delay để tránh spam server — tôn trọng rate limit."""
    time.sleep(random.uniform(1.5, 3.0))


def fetch_html(url: str) -> str | None:
    try:
        resp = cf_requests.get(
            url, headers=HEADERS, impersonate="chrome124", timeout=20
        )
        if resp.status_code != 200:
            print(f"  ⚠ status {resp.status_code} for {url}")
            return None
        return resp.text
    except Exception as e:
        print(f"  ✗ error fetching {url}: {e}")
        return None


def get_job_slugs_from_listing(category: str, page: int) -> list[str]:
    """Lấy danh sách job slug từ 1 trang listing."""
    url = f"{BASE_URL}/it-jobs/{category}?page={page}"
    print(f"Listing page: {url}")
    html = fetch_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("[data-search--job-selection-job-slug-value]")
    slugs = [c.get("data-search--job-selection-job-slug-value") for c in cards]
    slugs = [s for s in slugs if s]
    print(f"  → found {len(slugs)} job slugs")
    return slugs


def extract_skills_from_text(text: str, skill_dict: set) -> list[str]:
    """Regex + dictionary matching đơn giản cho skill normalization (Phase 1)."""
    text_lower = text.lower()
    found = set()
    for skill in skill_dict:
        # \b để tránh match nhầm substring (vd: 'r' trong 'programmer')
        pattern = r"\b" + re.escape(skill.lower()) + r"\b"
        if re.search(pattern, text_lower):
            found.add(skill)
    return sorted(found)


# Dictionary skill cơ bản — mở rộng dần, sau này thay bằng ESCO mapping
SKILL_DICT = {
    "python", "sql", "java", "scala", "spark", "hadoop", "kafka", "airflow",
    "docker", "kubernetes", "aws", "azure", "gcp", "power bi", "tableau",
    "excel", "etl", "dbt", "snowflake", "databricks", "nosql", "mongodb",
    "postgresql", "mysql", "big data", "machine learning", "tensorflow",
    "pytorch", "pandas", "numpy", "data warehouse", "data modeling",
    "business intelligence", "git", "ci/cd", "linux", "terraform", "redis",
    "elasticsearch", "hive", "presto", "trino", "looker", "google analytics",
}


def parse_job_detail(slug: str) -> dict | None:
    """Fetch trang detail và parse schema.org JobPosting JSON-LD."""
    url = f"{BASE_URL}/it-jobs/{slug}"
    html = fetch_html(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")

    job_data = None
    for s in scripts:
        if not s.string:
            continue
        try:
            data = json.loads(s.string)
        except json.JSONDecodeError:
            continue
        if data.get("@type") == "JobPosting":
            job_data = data
            break

    if not job_data:
        print(f"  ⚠ no JobPosting JSON-LD found for {slug}")
        return None

    # ── Parse các field từ JSON-LD (defensive — ITviec không phải lúc nào
    # cũng trả về đúng type cho mọi job, vd job không yêu cầu kinh nghiệm
    # hoặc lương ẩn sẽ có structure khác) ──
    title = job_data.get("title", "") or ""

    org = job_data.get("hiringOrganization")
    company = org.get("name", "") if isinstance(org, dict) else ""

    locations = job_data.get("jobLocation") or []
    if not isinstance(locations, list):
        locations = [locations]
    location_names = []
    for loc in locations:
        if isinstance(loc, dict):
            addr = loc.get("address", {})
            if isinstance(addr, dict):
                location_names.append(addr.get("addressRegion", ""))
    location = ", ".join(filter(None, location_names))

    skills_raw = job_data.get("skills", "") or ""
    if not isinstance(skills_raw, str):
        skills_raw = ", ".join(str(s) for s in skills_raw) if isinstance(skills_raw, list) else str(skills_raw)

    salary_info = job_data.get("baseSalary")
    salary_raw = None  # None = không tiết lộ / cần chuẩn hóa ở Silver layer
    HIDDEN_SALARY_MARKERS = {
        "you'll love it", "thỏa thuận", "thoả thuận", "very attractive",
        "negotiable", "competitive",
    }
    if isinstance(salary_info, dict):
        salary_value = salary_info.get("value")
        raw_val = None
        if isinstance(salary_value, dict):
            raw_val = salary_value.get("value")
        elif salary_value:
            raw_val = salary_value
        if raw_val:
            raw_val_str = str(raw_val).strip()
            if any(marker in raw_val_str.lower() for marker in HIDDEN_SALARY_MARKERS):
                salary_raw = None  # placeholder, không phải số liệu thật
            else:
                salary_raw = raw_val_str

    description_html = job_data.get("description", "") or ""
    if not isinstance(description_html, str):
        description_html = str(description_html)
    description_text = BeautifulSoup(description_html, "html.parser").get_text(
        separator=" ", strip=True
    )

    experience = job_data.get("experienceRequirements")
    months_exp = None
    if isinstance(experience, dict):
        months_exp = experience.get("monthsOfExperience")

    # Infer level từ title + months experience (heuristic đơn giản)
    title_lower = title.lower()
    if "senior" in title_lower or "lead" in title_lower or (months_exp and months_exp >= 60):
        level = "Senior"
    elif "junior" in title_lower or "fresher" in title_lower or "intern" in title_lower:
        level = "Junior"
    else:
        level = "Mid"

    # Kết hợp skills_raw (ITviec cung cấp) + skills extract từ description (bổ sung)
    # Dedup case-insensitive để tránh 'AWS' và 'aws' bị tính là 2 skill khác nhau
    skills_from_desc = extract_skills_from_text(description_text, SKILL_DICT)
    itviec_skills = [s.strip() for s in skills_raw.split(",") if s.strip()]

    seen_lower = {}
    for s in itviec_skills + skills_from_desc:
        key = s.lower()
        if key not in seen_lower:
            seen_lower[key] = s  # giữ dạng viết đầu tiên gặp được
    all_skills = sorted(seen_lower.values(), key=str.lower)

    return {
        "job_id": slug,
        "title": title,
        "company": company,
        "location": location,
        "salary_raw": salary_raw,
        "skills_raw": ", ".join(all_skills),
        "level": level,
        "months_experience": months_exp,
        "employment_type": job_data.get("employmentType", ""),
        "industry": job_data.get("industry", ""),
        "date_posted": job_data.get("datePosted", ""),
        "source": "itviec",
        "url": url,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    all_jobs = {}  # dedup theo job_id (slug)

    for category in CATEGORIES:
        print(f"\n{'='*60}\nCategory: {category}\n{'='*60}")
        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            if len(all_jobs) >= TARGET_TOTAL_JOBS:
                break

            slugs = get_job_slugs_from_listing(category, page)
            if not slugs:
                print(f"  → không còn job ở trang {page}, dừng category này")
                break

            polite_sleep()

            for slug in slugs:
                if slug in all_jobs:
                    continue  # đã crawl rồi (job trùng giữa nhiều category)

                print(f"  Fetching detail: {slug}")
                try:
                    job = parse_job_detail(slug)
                except Exception as e:
                    print(f"    ✗ lỗi parse {slug}: {e}")
                    job = None
                if job:
                    all_jobs[slug] = job
                    print(f"    ✓ {job['title']} @ {job['company']} ({job['level']})")

                    # Auto-save mỗi 20 jobs để không mất tiến độ nếu crash
                    if len(all_jobs) % 20 == 0:
                        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                            json.dump(list(all_jobs.values()), f, ensure_ascii=False, indent=2)
                        print(f"    💾 auto-saved ({len(all_jobs)} jobs)")
                polite_sleep()

            if len(all_jobs) >= TARGET_TOTAL_JOBS:
                print(f"\n✅ Đạt target {TARGET_TOTAL_JOBS} jobs, dừng lại.")
                break

    jobs_list = list(all_jobs.values())
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(jobs_list, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ Hoàn thành: {len(jobs_list)} jobs → {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
