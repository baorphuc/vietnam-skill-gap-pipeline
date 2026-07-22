"""
producers/inspect_page.py

BƯỚC 1 — Chạy TRƯỚC khi viết scraper chính thức.

ITviec có bot detection, requests thường bị block. Script này dùng
curl_cffi để impersonate browser thật (Chrome), lấy HTML mẫu của:
  1. Một trang listing (danh sách job)
  2. Một trang chi tiết job

Sau đó lưu ra file .html để mở bằng browser hoặc grep tìm class name
thật, từ đó viết CSS selector chính xác cho itviec_scraper.py.

Usage:
    pip install curl_cffi beautifulsoup4 --break-system-packages
    python3 producers/inspect_page.py
"""

import os
from curl_cffi import requests as cf_requests

OUTPUT_DIR = "debug_html"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LISTING_URL = "https://itviec.com/it-jobs/data-engineer"

HEADERS = {
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


def fetch(url: str, label: str):
    print(f"Fetching {label}: {url}")
    resp = cf_requests.get(
        url,
        headers=HEADERS,
        impersonate="chrome124",
        timeout=20,
    )
    print(f"  → status_code = {resp.status_code}, length = {len(resp.text)} chars")

    out_path = os.path.join(OUTPUT_DIR, f"{label}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(resp.text)
    print(f"  → saved to {out_path}\n")
    return resp.text


def main():
    listing_html = fetch(LISTING_URL, "listing_sample")

    import re
    matches = re.findall(r'href="(/it-jobs/[a-z0-9\-]+)"', listing_html)
    matches = [m for m in matches if m != "/it-jobs" and not m.startswith("/it-jobs/search")]

    if matches:
        detail_url = "https://itviec.com" + matches[0]
        fetch(detail_url, "detail_sample")
        print(f"Tổng cộng tìm thấy {len(set(matches))} job links trong listing (mẫu).")
    else:
        print("⚠ Không tìm thấy job link nào trong HTML — có thể bị block hoặc "
              "trang render bằng JS. Mở file listing_sample.html để kiểm tra thủ công.")

    print(f"\n✅ Xong. Mở 2 file trong '{OUTPUT_DIR}/' bằng browser hoặc:")
    print(f"   grep -o 'class=\"[^\"]*job[^\"]*\"' {OUTPUT_DIR}/listing_sample.html | sort -u")


if __name__ == "__main__":
    main()
