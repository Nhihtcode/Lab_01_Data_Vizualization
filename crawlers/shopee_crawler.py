"""
crawlers/shopee_crawler.py — Template crawler sản phẩm Shopee
Mỗi thành viên copy file này và chỉnh lại category_id + crawled_by

Cách dùng:
    python shopee_crawler.py

Output: data/raw/fact_product_shopee_YYYYMMDD_{crawled_by}.csv
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
PLATFORM_ID   = "shopee"
CRAWLED_BY    = "duong"            # ← đổi thành tên của bạn: thinh/tuan/y/the_anh/duong
CATEGORY_ID   = "11036971"        # ← ID danh mục trên Shopee (lấy từ URL)
CATEGORY_NAME = "Gia Dụng"         # ← Tên danh mục
MAX_PAGES     = 10                # số trang cào (mỗi trang ~60 sản phẩm)
SLEEP_MIN     = 3.0              # delay ngẫu nhiên giữa request (giây)
SLEEP_MAX     = 6.0             # delay ngẫu nhiên giữa request (giây)
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
    "Referer": "https://shopee.vn/",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "X-Api-Source": "pc",
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
    """Gọi Shopee API không chính thức để lấy danh sách sản phẩm."""
    url = "https://shopee.vn/api/v4/search/search_items"
    params = {
        "by": "pop",
        "categoryids": category_id,
        "limit": 60,
        "newest": page * 60,
        "order": "desc",
        "page_type": "search",
        "scenario": "PAGE_CATEGORY",
        "version": 2,
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", []) or []


def parse_item(item: dict) -> dict:
    """Chuyển một item từ API thành row theo schema."""
    d = item.get("item_basic", item)  # tuỳ version API Shopee

    raw_id      = str(d.get("itemid", ""))
    raw_shop_id = str(d.get("shopid", ""))

    price_raw  = d.get("price", 0) / 100_000     # Shopee trả giá đơn vị 1/100000 VNĐ
    price_orig = d.get("price_before_discount", 0) / 100_000

    discount = 0.0
    if price_orig > 0 and price_orig > price_raw:
        discount = round((1 - price_raw / price_orig) * 100, 1)

    labels: list = d.get("promotions", []) or []
    label_names  = [str(lb.get("promotion_label", "")).lower() for lb in labels]
    has_fs_xtra  = any("freeship" in ln for ln in label_names)
    has_coin     = any("coin" in ln or "xu" in ln for ln in label_names)
    has_voucher  = any("voucher" in ln or "giảm" in ln for ln in label_names)

    return {
        "product_id":   make_product_id(PLATFORM_ID, raw_id),
        "platform_id":  PLATFORM_ID,
        "category_id":  make_category_id(PLATFORM_ID, CATEGORY_ID),
        "shop_id":      make_shop_id(PLATFORM_ID, raw_shop_id),
        "product_name": d.get("name", ""),
        "product_url":  f"https://shopee.vn/product/{raw_shop_id}/{raw_id}",
        # Giá
        "price_current":     price_raw,
        "price_original":    price_orig if price_orig > 0 else "",
        "discount_percent":  discount,
        "price_ends_with_9": price_ends_with_9(price_raw),
        "price_bucket":      get_price_bucket(price_raw),
        # Hiệu suất
        "sold_count":    d.get("sold", ""),
        "stock":         d.get("stock", -1),
        "rating":        round(d.get("item_rating", {}).get("rating_star", 0), 2),
        "review_count":  d.get("item_rating", {}).get("rating_count", [0])[0] if isinstance(d.get("item_rating", {}).get("rating_count"), list) else d.get("cmt_count", ""),
        # Media — điền sau khi vào trang sản phẩm (để trống nếu chưa cào detail)
        "image_count":                 len(d.get("images", [])),
        "has_video":                   bool(d.get("video_info_list")),
        "review_with_image_count":     "",   # cần cào trang detail riêng
        "five_star_with_image_count":  "",
        # Vận chuyển
        "shipping_fee":           "",   # cần gọi API shipping (tuỳ chọn)
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