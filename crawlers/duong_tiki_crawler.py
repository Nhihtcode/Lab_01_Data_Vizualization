"""
crawlers/thinh_tiki_crawler.py — Crawler sản phẩm Tiki cho THỊNH
Danh mục: Nhà Cửa & Đời Sống (ID: 1883)

Vòng lặp 1: Gọi API danh sách (blocks/listings) lấy product_ids
Vòng lặp 2: Gọi API sản phẩm chi tiết (v2/products/{id}) và API đánh giá (v2/reviews?product_id={id})

Cách dùng:
    python crawlers/thinh_tiki_crawler.py
"""

import time
import random
import csv
from pathlib import Path
from datetime import datetime

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent))
from utils.helpers import (
    get_timestamp, parse_price,
    price_ends_with_9, get_price_bucket,
    make_product_id, make_shop_id, make_category_id,
)

# ════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH — THỊNH (TIKI)
# ════════════════════════════════════════════════════════════════════════════
PLATFORM_ID   = "tiki"
CRAWLED_BY    = "duong"
CATEGORY_ID   = "1882"               # Nhà Cửa & Đời Sống
CATEGORY_NAME = "Gia dụng"
MAX_PAGES     = 25                   # 25 trang * 40 SP = 1.000 SP (Vòng 1)
SLEEP_MIN     = 0.3                  # Thời gian sleep vòng 2
SLEEP_MAX     = 0.8
# ════════════════════════════════════════════════════════════════════════════

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d")
FACT_FILE = OUTPUT_DIR / f"fact_product_{PLATFORM_ID}_{TODAY}_{CRAWLED_BY}.csv"
SHOP_FILE = OUTPUT_DIR / f"dim_shop_{PLATFORM_ID}_{TODAY}_{CRAWLED_BY}.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://tiki.vn/",
}

FACT_FIELDNAMES = [
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

SHOP_FIELDNAMES = [
    "shop_id", "platform_id",
    "shop_name", "shop_type", "is_mall",
    "shop_rating", "follower_count",
    "response_rate", "response_time_hours", "prep_time_hours",
    "location", "total_products", "shop_url",
    "crawled_at",
]

# ═══════════════════════════════════════════════════════════════════════
# FETCH
# ═══════════════════════════════════════════════════════════════════════

def fetch_search_products(category_id: str, page: int) -> list[dict]:
    """Vòng 1: Tìm danh sách sản phẩm cơ bản."""
    url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    params = {
        "limit": 40,
        "include": "advertisement",
        "category": category_id,
        "page": page,
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", []) or []


def fetch_product_detail(product_id: str) -> dict:
    """Vòng 2: Gọi API chi tiết sản phẩm."""
    url = f"https://tiki.vn/api/v2/products/{product_id}"
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    return {}


def fetch_product_reviews(product_id: str) -> dict:
    """Vòng 2: Gọi API review sản phẩm."""
    url = "https://tiki.vn/api/v2/reviews"
    params = {"product_id": product_id}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    return {}


# ═══════════════════════════════════════════════════════════════════════
# PARSE
# ═══════════════════════════════════════════════════════════════════════

def parse_fact_product(basic: dict, detail: dict, reviews: dict) -> dict:
    # Lấy data detail ưu tiên hơn, nếu ko có thì fallback basic
    d = detail if detail else basic
    raw_id = str(d.get("id", basic.get("id", "")))
    
    seller = d.get("current_seller", {}) or d.get("seller", {}) or basic.get("seller", {}) or {}
    raw_shop_id = str(seller.get("id", ""))

    price_raw  = float(d.get("price", 0))
    price_orig = float(d.get("original_price", 0) or d.get("list_price", 0))
    discount = 0.0
    if price_orig > 0 and price_orig > price_raw:
        discount = round((1 - price_raw / price_orig) * 100, 1)

    # Khuyến mãi & freeship
    badges = d.get("badges", []) or d.get("badges_new", []) or []
    badges_str = str(badges).lower()
    has_fs_xtra = "freeship" in badges_str or d.get("is_freeship_xtra", False) or bool(d.get("asa_freeship_xtra"))
    has_coin    = "hoàn tiền" in badges_str or "astra" in badges_str
    has_voucher = d.get("seller_discount_available", False) or "giảm" in badges_str or bool(d.get("coupons"))

    # Media
    images = d.get("images", [])
    if not isinstance(images, list):
        if hasattr(images, "split"):
            images = images.split(",")
        else:
            images = []
    has_video = bool(d.get("video_url") or d.get("has_video"))
    
    # Hiệu suất bán hàng
    qty_sold = d.get("quantity_sold") or {}
    sold_count = qty_sold.get("value", d.get("all_time_quantity_sold", ""))
    stock_item = d.get("stock_item") or {}
    stock = stock_item.get("qty", -1)
    
    # Review
    rating = round(float(d.get("rating_average", 0)), 2)
    review_count = d.get("review_count", "")
    
    # Số liệu nâng cao từ API reviews
    review_with_image_count = ""
    five_star_with_image_count = ""
    if reviews:
        # Cố gắng lấy từ metadata có `has_photo`
        # Mặc định review API của Tiki có `stars` (1-5) để thống kê
        stars = reviews.get("stars", {}) or {}
        # Count review_photo
        stats = reviews.get("review_photo", {}) or {}
        review_with_image_count = stats.get("total", "")
        # Lấy count rating 5 sao
        star_5 = stars.get("5", {}) or {}
        if isinstance(star_5, dict):
            five_star_count = star_5.get("count", "")
        else:
            five_star_count = ""
        # Không có số ảnh riêng cho 5 sao, nên đành lấy five star proxy
        five_star_with_image_count = five_star_count if five_star_count else ""

    url_key = d.get("url_key", "")
    prod_url = f"https://tiki.vn/{url_key}-p{raw_id}.html" if url_key else f"https://tiki.vn/product-p{raw_id}.html"

    # Giao hàng & Vị trí
    est_delivery = ""
    shipping_fee = ""
    if "delivery_info" in d:
        # Trong API products detail, shipping đôi khi có fee
        delivery_info = d.get("delivery_info", {}) or {}
        est_delivery = str(delivery_info.get("delivery_promise", ""))
    
    # Tiki v2/products có thể có free_shipping / shipping fee rõ ràng hơn 
    # Nhưng nếu không có ta để trống
    
    return {
        "product_id":   make_product_id(PLATFORM_ID, raw_id),
        "platform_id":  PLATFORM_ID,
        "category_id":  make_category_id(PLATFORM_ID, CATEGORY_ID),
        "shop_id":      make_shop_id(PLATFORM_ID, raw_shop_id),
        "product_name": d.get("name", ""),
        "product_url":  prod_url,
        "price_current":     price_raw,
        "price_original":    price_orig if price_orig > 0 else "",
        "discount_percent":  discount,
        "price_ends_with_9": price_ends_with_9(price_raw),
        "price_bucket":      get_price_bucket(price_raw),
        "sold_count":    sold_count,
        "stock":         stock,
        "rating":        rating,
        "review_count":  review_count,
        "image_count":                len(images),
        "has_video":                  has_video,
        "review_with_image_count":    review_with_image_count,
        "five_star_with_image_count": five_star_with_image_count,
        "shipping_fee":            shipping_fee,
        "is_freeship":             has_fs_xtra,
        "estimated_delivery_days": est_delivery,
        "has_freeship_xtra_label": has_fs_xtra,
        "has_coinback_label":      has_coin,
        "has_voucher_label":       has_voucher,
        "promotion_label_count":   int(has_fs_xtra) + int(has_coin) + int(has_voucher),
        "crawled_at":  get_timestamp(),
        "crawled_by":  CRAWLED_BY,
    }

def parse_dim_shop(basic: dict, detail: dict) -> dict:
    d = detail if detail else basic
    seller = d.get("current_seller", {}) or d.get("seller", {}) or basic.get("seller", {}) or {}
    raw_shop_id = str(seller.get("id", ""))
    shop_name = seller.get("name", d.get("seller_name", ""))
    
    # Tiki Trading hoặc Official Store -> Mall
    is_tiki_trading = raw_shop_id == "1" or "tiki trading" in shop_name.lower()
    is_official = bool(seller.get("is_official_store", False))
    is_mall_flag = is_tiki_trading or is_official

    if is_tiki_trading:
        shop_type = "mall"
    elif is_official:
        shop_type = "official"
    else:
        shop_type = "individual"

    store_id = seller.get("store_id")
    shop_url = getattr(seller, "get", lambda k: "")("link", f"https://tiki.vn/cua-hang/{store_id if store_id else raw_shop_id}")
    
    # Location
    location = d.get("inventory_status", "")
    if location == "available" or location == "in_stock":
        location = "" # tiki không thường expose city như shopee trong model này

    # V2 products seller thường có thêm chỉ số (nếu có trên web)
    # Tuy nhiên nếu không có ta mặc định rỗng
    rating_star = ""
    follower = ""
    if "store_info" in seller:
        rating_star = seller["store_info"].get("rating_average", "")
        follower = seller["store_info"].get("followers", "")

    return {
        "shop_id":             make_shop_id(PLATFORM_ID, raw_shop_id),
        "platform_id":         PLATFORM_ID,
        "shop_name":           shop_name,
        "shop_type":           shop_type,
        "is_mall":             is_mall_flag,
        "shop_rating":         rating_star,
        "follower_count":      follower,
        "response_rate":       "",
        "response_time_hours": "",
        "prep_time_hours":     "",
        "location":            location,
        "total_products":      "",
        "shop_url":            shop_url,
        "crawled_at":          get_timestamp(),
    }


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    print(f"[INFO] Bắt đầu cào Tiki 2 VÒNG LẶP - Danh mục: {CATEGORY_NAME}")
    print(f"[INFO] CRAWLED_BY  : {CRAWLED_BY}")
    
    # -- VÒNG LẶP 1: Lấy danh sách ID cơ bản từ API Listings --
    print("\n" + "="*50)
    print("VÒNG LẶP 1: TÌM KIẾM DANH SÁCH SẢN PHẨM TRÊN LISTINGS")
    print("="*50)
    all_basic_items = []
    
    for page in range(1, MAX_PAGES + 1):
        try:
            items = fetch_search_products(CATEGORY_ID, page)
            if not items:
                print(f"[INFO] Không còn sản phẩm ở trang {page}. Dừng Vòng 1.")
                break
            
            all_basic_items.extend(items)
            print(f"  [Trang {page:02d}] Thu được {len(items):3d} IDs | Tổng cộng: {len(all_basic_items):4d} IDs")
            time.sleep(random.uniform(0.1, 0.4))
        except requests.RequestException as e:
            print(f"  [LỖI VÒNG 1] Trang {page}: {e}")
            time.sleep(2)

    total_basic = len(all_basic_items)
    print(f"[DONE VÒNG 1] Thu được tổng cộng {total_basic} IDs sản phẩm cơ bản.")
    
    if total_basic == 0:
        return

    # -- VÒNG LẶP 2: Lấy thông tin chi tiết từng sản phẩm và đánh giá --
    print("\n" + "="*50)
    print("VÒNG LẶP 2: LẤY CHI TIẾT TỒN KHO, SHOP VÀ REVIEWS")
    print("="*50)
    
    total_written = 0
    seen_shop_ids: set[str] = set()

    with (
        open(FACT_FILE, "w", newline="", encoding="utf-8-sig") as ff,
        open(SHOP_FILE, "w", newline="", encoding="utf-8-sig") as sf,
    ):
        fact_writer = csv.DictWriter(ff, fieldnames=FACT_FIELDNAMES)
        shop_writer = csv.DictWriter(sf, fieldnames=SHOP_FIELDNAMES)
        fact_writer.writeheader()
        shop_writer.writeheader()

        for idx, basic_item in enumerate(all_basic_items, 1):
            raw_id = str(basic_item.get("id", ""))
            if not raw_id:
                continue
                
            try:
                # Gọi 2 API bổ sung theo logic
                detail = fetch_product_detail(raw_id)
                reviews = fetch_product_reviews(raw_id)
                
                # Ghi fact_product
                fact_row = parse_fact_product(basic_item, detail, reviews)
                fact_writer.writerow(fact_row)
                total_written += 1
                
                # Ghi dim_shop
                raw_shop_id = fact_row["shop_id"]
                if raw_shop_id and raw_shop_id not in seen_shop_ids:
                    seen_shop_ids.add(raw_shop_id)
                    shop_row = parse_dim_shop(basic_item, detail)
                    shop_writer.writerow(shop_row)
                
                if idx % 10 == 0:
                    print(f"  [Vòng 2] Đã xử lý {idx:4d}/{total_basic:4d} sản phẩm...")
                
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
            except Exception as e:
                print(f"  [LỖI VÒNG 2] Không thể lấy chi tiết sản phẩm {raw_id}: {e}")
                time.sleep(1)

    print("\n" + "="*50)
    print(f"[DONE] fact_product : {total_written:,} sản phẩm → {FACT_FILE}")
    print(f"[DONE] dim_shop     : {len(seen_shop_ids):,} gian hàng → {SHOP_FILE}")


if __name__ == "__main__":
    main()