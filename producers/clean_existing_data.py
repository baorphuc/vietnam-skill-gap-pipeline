"""
producers/clean_existing_data.py

Chạy 1 LẦN để clean lại data/raw_jobs.json đã scrape trước đó
(không cần crawl lại ITviec — tiết kiệm thời gian + tránh risk bị block thêm)

Fix 2 vấn đề phát hiện được:
  1. salary_raw = "You'll love it" / "Thỏa thuận" / v.v. → chuẩn hóa thành None
     (đây là placeholder khi công ty ẩn lương, không phải số liệu thật)
  2. skills_raw bị duplicate do khác hoa/thường (AWS vs aws) → dedup case-insensitive

Usage:
    python3 producers/clean_existing_data.py
"""

import json

INPUT_FILE = "data/raw_jobs.json"
OUTPUT_FILE = "data/raw_jobs.json"  # ghi đè luôn, đã backup ở bước dưới

HIDDEN_SALARY_MARKERS = {
    "you'll love it", "thỏa thuận", "thoả thuận", "very attractive",
    "negotiable", "competitive",
}


def clean_salary(salary_raw):
    if not salary_raw:
        return None
    salary_lower = str(salary_raw).lower()
    if any(marker in salary_lower for marker in HIDDEN_SALARY_MARKERS):
        return None
    return salary_raw


def clean_skills(skills_raw):
    skills = [s.strip() for s in skills_raw.split(",") if s.strip()]
    seen_lower = {}
    for s in skills:
        key = s.lower()
        if key not in seen_lower:
            seen_lower[key] = s
    return ", ".join(sorted(seen_lower.values(), key=str.lower))


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        jobs = json.load(f)

    # Backup trước khi ghi đè
    backup_path = INPUT_FILE.replace(".json", "_backup.json")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"Backup gốc → {backup_path}")

    fixed_salary = 0
    fixed_skills = 0

    for job in jobs:
        old_salary = job.get("salary_raw")
        new_salary = clean_salary(old_salary)
        if new_salary != old_salary:
            fixed_salary += 1
        job["salary_raw"] = new_salary

        old_skills = job.get("skills_raw", "")
        new_skills = clean_skills(old_skills)
        if new_skills != old_skills:
            fixed_skills += 1
        job["skills_raw"] = new_skills

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    print(f"✅ Đã clean {len(jobs)} jobs:")
    print(f"   - {fixed_salary} jobs chuẩn hóa salary (placeholder → null)")
    print(f"   - {fixed_skills} jobs dedup skills (case-insensitive)")
    print(f"   → ghi đè {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
