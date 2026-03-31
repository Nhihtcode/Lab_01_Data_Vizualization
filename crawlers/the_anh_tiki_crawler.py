"""
crawlers/tiki_crawler.py — Crawler sản phẩm Tiki
Cách dùng:
    python crawlers/tiki_crawler.py

Output: data/raw/fact_product_tiki_YYYYMMDD_{crawled_by}.csv
"""

import time
import random
import csv
from pathlib import Path
from datetime import datetime

import requests

# ── Import hàm dùng chung ────────────────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent))
from utils.helpers import (
    get_timestamp, parse_price,
    price_ends_with_9, get_price_bucket,
    make_product_id, make_shop_id, make_category_id,
)

# ════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH — mỗi thành viên chỉnh tại đây
# ════════════════════════════════════════════════════════════════════════════
PLATFORM_ID   = "tiki"
CRAWLED_BY    = "the_anh"       # ← đổi tên: thinh/tuan/y/the_anh/duong
CATEGORY_ID   = "1520"          # ← ID danh mục Tiki (lấy từ URL)
CATEGORY_NAME = "Làm Đẹp & Sức Khỏe"    # ← Tên danh mục
MAX_PAGES     = 10              # mỗi trang 40 sản phẩm → tối đa 400
SLEEP_MIN     = 1.5
SLEEP_MAX     = 3.5
# ════════════════════════════════════════════════════════════════════════════

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE = OUTPUT_DIR / f"fact_product_{PLATFORM_ID}_{TODAY}_{CRAWLED_BY}.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer":  "https://tiki.vn/",
    "Accept":   "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9",
}

# Cột theo đúng thứ tự trong fact_product (schema v2)
FIELDNAMES = [
    "product_id", "platform_id", "category_id", "shop_id",
    "product_name", "product_url",
    "price_current", "price_original", "discount_percent",
    "price_ends_with_9", "price_bucket",
    "sold_count", "stock", "rating", "review_count",
    "image_count", "has_video",
    "review_with_image_count", "five_star_with_image_count",
    "shipping_fee", "is_freeship", "estimated_delivery_days",
    "has_freeship_xtra_label", "has_coinback_label",
    "has_voucher_label", "promotion_label_count",
    "crawled_at", "crawled_by",
]


def fetch_products(category_id: str, page: int) -> list[dict]:
    """Gọi Tiki API lấy danh sách sản phẩm theo category."""
    url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    params = {
        "limit":    40,
        "page":     page + 1,
        "category": category_id,
        "sort":     "top_seller",
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if isinstance(data, dict):
        data = data.get("data", [])
    return data or []


def parse_item(item: dict) -> dict:
    """Chuyển một item Tiki thành row theo schema."""
    d = item

    raw_id      = str(d.get("id", ""))
    seller      = d.get("seller", {}) or {}
    raw_shop_id = str(seller.get("id", "unknown"))

    price_raw  = float(d.get("price") or 0)
    price_orig = float(d.get("list_price") or 0)

    discount = float(d.get("discount_rate") or 0)
    if discount == 0 and price_orig > price_raw > 0:
        discount = round((1 - price_raw / price_orig) * 100, 1)

    badges     = d.get("badges_new", []) or []
    badge_blob = " ".join(
        str(b.get("code", "")) + " " + str(b.get("text", ""))
        for b in badges if isinstance(b, dict)
    ).lower()
    has_freeship = "freeship" in badge_blob or bool(d.get("is_free_shipping"))
    has_coin     = "coin" in badge_blob or "xu" in badge_blob
    has_voucher  = "voucher" in badge_blob or "coupon" in badge_blob

    # sold_count
    raw_sold = d.get("quantity_sold")
    if isinstance(raw_sold, dict):
        sold = raw_sold.get("value", "")
    else:
        sold = raw_sold or ""

    # url
    url_path = str(d.get("url_path", "")).strip("/")
    product_url = f"https://tiki.vn/{url_path}" if url_path else f"https://tiki.vn/p{raw_id}.html"

    images = d.get("images", []) or []
    image_count = len(images) if images else (1 if d.get("thumbnail_url") else 0)

    return {
        "product_id":   make_product_id(PLATFORM_ID, raw_id),
        "platform_id":  PLATFORM_ID,
        "category_id":  make_category_id(PLATFORM_ID, CATEGORY_ID),
        "shop_id":      make_shop_id(PLATFORM_ID, raw_shop_id),
        "product_name": d.get("name", ""),
        "product_url":  product_url,
        "price_current":     price_raw,
        "price_original":    price_orig if price_orig > 0 else "",
        "discount_percent":  discount,
        "price_ends_with_9": price_ends_with_9(price_raw),
        "price_bucket":      get_price_bucket(price_raw),
        "sold_count":    sold,
        "stock":         d.get("inventory", ""),
        "rating":        round(float(d.get("rating_average") or 0), 2),
        "review_count":  d.get("review_count", ""),
        "image_count":                 image_count,
        "has_video":                   bool(d.get("video_url") or d.get("has_video")),
        "review_with_image_count":     "",
        "five_star_with_image_count":  "",
        "shipping_fee":           "",
        "is_freeship":            has_freeship,
        "estimated_delivery_days":"",
        "has_freeship_xtra_label": has_freeship,
        "has_coinback_label":      has_coin,
        "has_voucher_label":       has_voucher,
        "promotion_label_count":   int(has_freeship) + int(has_coin) + int(has_voucher),
        "crawled_at":  get_timestamp(),
        "crawled_by":  CRAWLED_BY,
    }


def main():
    print(f"[INFO] Bắt đầu cào: {CATEGORY_NAME} (category_id={CATEGORY_ID})")
    print(f"[INFO] Output → {OUTPUT_FILE}\n")

    total = 0
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for page in range(MAX_PAGES):
            try:
                items = fetch_products(CATEGORY_ID, page)
                if not items:
                    print(f"[INFO] Không còn sản phẩm ở trang {page}. Dừng.")
                    break

                for item in items:
                    row = parse_item(item)
                    writer.writerow(row)
                    total += 1

                print(f"  [Trang {page+1:02d}] +{len(items)} sản phẩm | Tổng: {total}")
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            except requests.RequestException as e:
                print(f"  [LỖI] Trang {page}: {e}")
                time.sleep(5)

    print(f"\n[DONE] Đã cào {total} sản phẩm → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
