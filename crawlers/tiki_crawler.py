"""
crawlers/tiki_crawler.py — Template crawler sản phẩm Tiki
Mỗi thành viên chỉnh lại category_id + crawled_by trước khi chạy.

Cách dùng:
    python crawlers/tiki_crawler.py

Output: data/raw/fact_product_tiki_YYYYMMDD_{crawled_by}.csv
"""

import time
import random
import csv
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

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
CRAWLED_BY    = "y"            # ← đổi thành tên của bạn: thinh/tuan/y/the_anh/duong
CATEGORY_ID   = "931"            # ← ID danh mục trên Tiki (lấy từ URL category)
CATEGORY_NAME = "Thời Trang Nữ"         # ← Tên danh mục
MAX_PAGES     = 10                # số trang cào (mỗi trang ~40 sản phẩm)
SLEEP_MIN     = 1.5               # delay ngẫu nhiên giữa request (giây)
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
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://tiki.vn/",
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
    """Gọi Tiki listing API để lấy danh sách sản phẩm theo category."""
    url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    params = {
        "limit": 40,
        "page": page,
        "category": category_id,
        "sort": "top_seller",
        "urlKey": quote(CATEGORY_NAME.lower().replace(" ", "-")),
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", []) or []


def parse_sold_count(raw_sold: dict | str | None) -> int | str:
    """Tiki trả sold dưới nhiều dạng, ưu tiên lấy numeric value nếu có."""
    if isinstance(raw_sold, dict):
        value = raw_sold.get("value")
        if isinstance(value, (int, float)):
            return int(value)
        text = str(raw_sold.get("text", "")).strip().lower().replace("đã bán", "").strip()
        if text.endswith("k"):
            numeric_part = text[:-1].replace(",", ".")
            try:
                return int(float(numeric_part) * 1000)
            except ValueError:
                pass
        digits = "".join(ch for ch in text if ch.isdigit())
        return int(digits) if digits else ""
    if isinstance(raw_sold, (int, float)):
        return int(raw_sold)
    return ""


def make_tiki_product_url(d: dict, raw_id: str) -> str:
    url_path = str(d.get("url_path", "")).strip("/")
    if url_path:
        return f"https://tiki.vn/{url_path}"
    url_key = d.get("url_key")
    if url_key:
        return f"https://tiki.vn/{url_key}.html"
    return f"https://tiki.vn/p{raw_id}.html"


def parse_item(item: dict) -> dict:
    """Chuyển một item từ API thành row theo schema."""
    d = item

    raw_id = str(d.get("id", ""))

    seller = d.get("seller", {}) if isinstance(d.get("seller", {}), dict) else {}
    raw_shop_id = str(seller.get("id", "")) or "unknown"

    price_raw = float(d.get("price") or 0)
    price_orig = float(d.get("list_price") or 0)

    discount = float(d.get("discount_rate") or 0)
    if discount < 0:
        discount = abs(discount)
    if discount == 0 and price_orig > 0 and price_orig > price_raw:
        discount = round((1 - price_raw / price_orig) * 100, 1)

    badges = d.get("badges_new", []) or []
    badge_names = []
    for b in badges:
        if isinstance(b, dict):
            badge_names.append(str(b.get("code", "")).lower())
            badge_names.append(str(b.get("text", "")).lower())
    badge_blob = " ".join(badge_names)

    has_fs_xtra = ("freeship" in badge_blob) or bool(d.get("is_free_shipping"))
    has_coin = ("coin" in badge_blob) or ("xu" in badge_blob)
    has_voucher = ("voucher" in badge_blob) or ("coupon" in badge_blob)

    rating = d.get("rating_average")
    if isinstance(rating, (int, float)):
        rating = round(float(rating), 2)
    else:
        rating = ""

    image_count = len(d.get("images", []) or [])
    if image_count == 0 and d.get("thumbnail_url"):
        image_count = 1

    return {
        "product_id":   make_product_id(PLATFORM_ID, raw_id),
        "platform_id":  PLATFORM_ID,
        "category_id":  make_category_id(PLATFORM_ID, CATEGORY_ID),
        "shop_id":      make_shop_id(PLATFORM_ID, raw_shop_id),
        "product_name": d.get("name", ""),
        "product_url":  make_tiki_product_url(d, raw_id),
        # Giá
        "price_current":     price_raw,
        "price_original":    price_orig if price_orig > 0 else "",
        "discount_percent":  discount,
        "price_ends_with_9": price_ends_with_9(price_raw),
        "price_bucket":      get_price_bucket(price_raw),
        # Hiệu suất
        "sold_count":    parse_sold_count(d.get("quantity_sold")),
        "stock":         d.get("inventory", ""),
        "rating":        rating,
        "review_count":  d.get("review_count", ""),
        # Media — điền sau khi vào trang sản phẩm (để trống nếu chưa cào detail)
        "image_count":                 image_count,
        "has_video":                   bool(d.get("video_url")) or bool(d.get("has_video")),
        "review_with_image_count":     "",   # cần cào trang detail riêng
        "five_star_with_image_count":  "",
        # Vận chuyển
        "shipping_fee":           "",
        "is_freeship":            has_fs_xtra,
        "estimated_delivery_days":"",
        # Nhãn KM
        "has_freeship_xtra_label": has_fs_xtra,
        "has_coinback_label":      has_coin,
        "has_voucher_label":       has_voucher,
        "promotion_label_count":   int(has_fs_xtra) + int(has_coin) + int(has_voucher),
        # Metadata
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

        for page in range(1, MAX_PAGES + 1):
            try:
                items = fetch_products(CATEGORY_ID, page)
                if not items:
                    print(f"[INFO] Không còn sản phẩm ở trang {page}. Dừng.")
                    break

                for item in items:
                    row = parse_item(item)
                    writer.writerow(row)
                    total += 1

                print(f"  [Trang {page:02d}] +{len(items)} sản phẩm | Tổng: {total}")
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            except requests.RequestException as e:
                print(f"  [LỖI] Trang {page}: {e}")
                time.sleep(5)

    print(f"\n[DONE] Đã cào {total} sản phẩm → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
