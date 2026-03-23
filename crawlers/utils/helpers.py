"""
utils/helpers.py — Hàm dùng chung cho tất cả crawler
"""
from datetime import datetime, timezone, timedelta


VN_TZ = timezone(timedelta(hours=7))

# ── Các nhãn khuyến mãi hợp lệ (theo bridge_product_promotion) ──────────────
VALID_PROMOTION_LABELS = {
    "freeship_xtra",
    "coinback",
    "flash_sale",
    "voucher",
    "mall_sale",
}

# ── Phân khúc giá ────────────────────────────────────────────────────────────
PRICE_BUCKETS = [
    (0,        100_000,   "<100k"),
    (100_000,  500_000,   "100k-500k"),
    (500_000,  1_000_000, "500k-1M"),
    (1_000_000,5_000_000, "1M-5M"),
    (5_000_000,float("inf"), ">5M"),
]


def get_timestamp() -> str:
    """Trả về thời điểm hiện tại theo định dạng ISO 8601 +07:00."""
    return datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S+07:00")


def parse_price(raw: str) -> float | None:
    """
    Chuyển chuỗi giá sang float (VNĐ).
    Xử lý các định dạng thực tế từ Shopee, Lazada, Tiki:
      '1.299.000₫'  → 1299000.0
      '1,299,000đ'  → 1299000.0
      '1299000 VND' → 1299000.0
      '199.000 d'   → 199000.0
    """
    if not raw:
        return None
    cleaned = str(raw)
    # Bỏ các ký tự tiền tệ phổ biến
    for ch in ("VND", "vnd", "₫", "đ", "d", "D", "\xa0", "\u200b"):
        cleaned = cleaned.replace(ch, "")
    # Xác định dấu thập phân: nếu dấu ',' cuối cùng (ví dụ 1.299,00) → đổi sang '.'
    # Trường hợp thông thường VN: dấu '.' là phân cách nghìn
    cleaned = cleaned.replace(".", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def price_ends_with_9(price: float | None) -> bool:
    """True nếu đuôi giá kết thúc bằng chữ số 9."""
    if price is None:
        return False
    return str(int(price))[-1] == "9"


def get_price_bucket(price: float | None) -> str | None:
    """Phân loại giá vào bucket tương ứng."""
    if price is None:
        return None
    for low, high, label in PRICE_BUCKETS:
        if low <= price < high:
            return label
    return None


def make_product_id(platform_id: str, raw_id: str) -> str:
    """Tạo product_id chuẩn: '{platform_id}_{raw_id}'."""
    return f"{platform_id}_{raw_id}"


def make_shop_id(platform_id: str, raw_id: str) -> str:
    return f"{platform_id}_{raw_id}"


def make_category_id(platform_id: str, raw_id: str) -> str:
    return f"{platform_id}_{raw_id}"
