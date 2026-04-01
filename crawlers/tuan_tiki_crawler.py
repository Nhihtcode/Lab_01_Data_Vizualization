import time
import random
import csv
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor

# Import hàm dùng chung 
import sys
sys.path.insert(0, str(Path(__file__).parent))
try:
    from utils.helpers import (
        get_timestamp, parse_price,
        price_ends_with_9, get_price_bucket,
        make_product_id, make_shop_id, make_category_id,
    )
except ImportError:
    def get_timestamp(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def price_ends_with_9(p): return str(int(p)).rstrip('0').endswith('9') if p else False
    def get_price_bucket(p): return "0-500k" 
    def make_product_id(pl, rid): return f"{pl}_{rid}"
    def make_shop_id(pl, rid): return f"{pl}_tiki_trading" if str(rid) == "1" else f"{pl}_{rid}"
    def make_category_id(pl, rid): return f"{pl}_{rid}"


# CẤU HÌNH
PLATFORM_ID   = "tiki"
CRAWLED_BY    = "tuan"            
CATEGORIES = [
    {"id": "1789", "name": "Điện Thoại - Máy Tính Bảng"},
    {"id": "1815", "name": "Thiết Bị Số - Phụ Kiện Số"},
    {"id": "1846", "name": "Laptop - Máy Vi Tính"},
    {"id": "1801", "name": "Máy Ảnh - Máy Quay Phim"}
]

MAX_PAGES     = 10         
MAX_WORKERS   = 8          
# ════════════════════════════════════════════════════════════════════════════
TODAY = datetime.now().strftime("%Y%m%d")
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"fact_product_{PLATFORM_ID}_{TODAY}_{CRAWLED_BY}.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://tiki.vn/",
}

FIELDNAMES = [
    "product_id", "platform_id", "category_id", "shop_id", "product_name", "product_url",
    "price_current", "price_original", "discount_percent", "price_ends_with_9", "price_bucket",
    "sold_count", "stock", "rating", "review_count", "image_count", "has_video",
    "review_with_image_count", "five_star_with_image_count", "shipping_fee", "is_freeship", 
    "estimated_delivery_days", "has_freeship_xtra_label", "has_coinback_label",
    "has_voucher_label", "promotion_label_count", "crawled_at", "crawled_by"
]

# ══════════════════════
# CÁC HÀM GỌI API
# ══════════════════════

def fetch_listing(session, cat_id, page, cat_name):
    url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    params = {
        "limit": 40, "page": page, "category": cat_id,
        "sort": "top_seller", "urlKey": quote(cat_name.lower().replace(" ", "-")),
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        return resp.json().get("data", [])
    except: return []

def fetch_detail_and_reviews(session, pid):
    """Gói 2 API vào 1 lần xử lý để tối ưu luồng"""
    detail, reviews = {}, {}
    try:
        # 1. API Chi tiết sản phẩm
        d_resp = session.get(f"https://tiki.vn/api/v2/products/{pid}", timeout=10)
        if d_resp.status_code == 200: detail = d_resp.json()
        
        # 2. API Reviews (Lấy số liệu ảnh và sao)
        r_resp = session.get(f"https://tiki.vn/api/v2/reviews", params={"product_id": pid}, timeout=10)
        if r_resp.status_code == 200: reviews = r_resp.json()
    except: pass
    return detail, reviews

def parse_full_data(basic_item, detail, reviews, cat_id):
    d = detail if detail else basic_item
    pid = str(d.get("id", basic_item.get("id", "")))
    
    # 1. ĐỊNH DANH
    seller = d.get("current_seller") or d.get("seller") or basic_item.get("seller") or {}
    raw_shop_id = str(seller.get("id") or d.get("merchant_id") or "1")

    # 2. GIÁ CẢ 
    price_curr = float(d.get("price") or 0)
    price_orig = float(d.get("original_price") or d.get("list_price") or 0)
    disc_rate  = float(d.get("discount_rate") or 0)

    if disc_rate == 0 or price_orig <= price_curr:
        price_orig = price_curr
        disc_rate = 0
    
    # 3. KHUYẾN MÃI & BADGES 
    badges = d.get("badges_new", []) or d.get("badges", [])
    badge_str = str(badges).lower()
    has_fs = "freeship" in badge_str or bool(d.get("is_free_shipping"))
    has_coin = "hoàn tiền" in badge_str or "astra" in badge_str
    has_voucher = "voucher" in badge_str or bool(d.get("seller_discount_available"))

    # 4. REVIEW NÂNG CAO 
    rev_with_img = reviews.get("review_photo", {}).get("total", "")
    five_star_img = reviews.get("stars", {}).get("5", {}).get("count", "") if reviews else ""

    # 5. GIAO HÀNG 
    deliv = d.get("delivery_info", {}) or {}
    ship_fee = deliv.get("fee", "")
    est_day = deliv.get("delivery_promise", "")

    return {
        "product_id":   make_product_id(PLATFORM_ID, pid),
        "platform_id":  PLATFORM_ID,
        "category_id":  make_category_id(PLATFORM_ID, cat_id),
        "shop_id":      make_shop_id(PLATFORM_ID, raw_shop_id),
        "product_name": d.get("name", ""),
        "product_url":  f"https://tiki.vn/{d.get('url_path', f'p{pid}.html')}",
        "price_current":  price_curr,
        "price_original": round(price_orig) if price_orig > 0 else "",
        "discount_percent": disc_rate,
        "price_ends_with_9": price_ends_with_9(price_curr),
        "price_bucket":      get_price_bucket(price_curr),
        "sold_count":   d.get("quantity_sold", {}).get("value", 0) if isinstance(d.get("quantity_sold"), dict) else 0,
        "stock":        d.get("inventory", {}).get("quantity", "N/A") if isinstance(d.get("inventory"), dict) else "N/A",
        "rating":       d.get("rating_average") if float(d.get("rating_average", 0)) > 0 else "",
        "review_count": d.get("review_count") if d.get("review_count") != 0 else "",
        "image_count":  len(d.get("images", []) or [1]),
        "has_video":    bool(d.get("video_url") or d.get("has_video")),
        "review_with_image_count":    rev_with_img,
        "five_star_with_image_count": five_star_img,
        "shipping_fee":               ship_fee,
        "is_freeship":                has_fs,
        "estimated_delivery_days":    est_day,
        "has_freeship_xtra_label":    has_fs,
        "has_coinback_label":         has_coin,
        "has_voucher_label":          has_voucher,
        "promotion_label_count":      int(has_fs) + int(has_coin) + int(has_voucher),
        "crawled_at":   get_timestamp(),
        "crawled_by":   CRAWLED_BY,
    }

# ══════════════════
# MAIN
# ══════════════════

def process_product(session, item, cat_id):
    pid = item.get("id")
    detail, reviews = fetch_detail_and_reviews(session, pid)
    return parse_full_data(item, detail, reviews, cat_id)

def main():
    start_time = time.time()
    session = requests.Session()
    session.headers.update(HEADERS)
    
    print(f" Crawler Tiki Full Columns - Chế độ: Cào sạch danh mục")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for cat in CATEGORIES:
            cat_id, cat_name = cat["id"], cat["name"]
            print(f"\n Đang quét danh mục: {cat_name}")
            
            # VÒNG 1: LISTING (Cào đến khi hết trang) 
            basic_items = []
            current_page = 1
            
            while True:
                items = fetch_listing(session, cat_id, current_page, cat_name)
                
                # Điều kiện dừng: Nếu không còn sản phẩm ở trang hiện tại
                if not items:
                    print(f"   [!] Đã hết sản phẩm tại trang {current_page}. Chuyển sang xử lý chi tiết.")
                    break
                
                basic_items.extend(items)
                print(f"   [+] Đã lấy danh sách trang {current_page:03d} (Tổng: {len(basic_items)} SP)")
                
                current_page += 1
                # Nghỉ ngắn giữa các trang listing để tránh bị block
                time.sleep(random.uniform(0.3, 0.6))

            # VÒNG 2: DETAIL + REVIEWS (Chạy đa luồng) 
            if basic_items:
                print(f"   [!] Đang lấy dữ liệu chi tiết cho {len(basic_items)} SP bằng {MAX_WORKERS} luồng...")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(process_product, session, item, cat_id) for item in basic_items]
                    for i, future in enumerate(futures):
                        try:
                            writer.writerow(future.result())
                            if (i + 1) % 20 == 0:
                                print(f"       > Progress: {i+1}/{len(basic_items)}...")
                        except Exception as e:
                            print(f"       [LỖI] Không xử lý được sản phẩm thứ {i+1}: {e}")
            else:
                print(f"   [?] Danh mục này không có sản phẩm nào.")

    print(f"\n HOÀN THÀNH!")
    print(f" Tổng thời gian: {round(time.time() - start_time, 2)}s |  File: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()