"""
notebooks/00_data_merge.ipynb — Gộp & validate dữ liệu từ tất cả thành viên
Chạy notebook này SAU KHI tất cả đã cào xong và đặt file vào data/raw/
"""
# Cell 1 — Import
import pandas as pd
import glob
from pathlib import Path

RAW_DIR = Path("../data/raw")
PROCESSED_DIR = Path("../data/processed")
PROCESSED_DIR.mkdir(exist_ok=True)

# Cell 2 — Gộp tất cả fact_product
files = list(RAW_DIR.glob("fact_product_*.csv"))
print(f"Tìm thấy {len(files)} file:")
for f in files:
    print(f"  {f.name}")

dfs = []
for f in files:
    df = pd.read_csv(f, encoding="utf-8-sig")
    dfs.append(df)

products = pd.concat(dfs, ignore_index=True)
print(f"\nTổng dòng trước khi xử lý: {len(products)}")

# Cell 3 — Kiểm tra & tiền xử lý cơ bản
# Loại trùng lặp (cùng product_id)
products = products.drop_duplicates(subset=["product_id"])
print(f"Sau loại trùng: {len(products)}")

# Kiểm tra cột bắt buộc
required_cols = ["product_id", "platform_id", "category_id", "shop_id",
                 "product_name", "price_current", "crawled_at", "crawled_by"]
for col in required_cols:
    null_count = products[col].isna().sum()
    print(f"  {col}: {null_count} null")

# Cell 4 — Tính lại các cột derived (nếu chưa có)
products["price_ends_with_9"] = products["price_current"].apply(
    lambda x: str(int(x))[-1] == "9" if pd.notna(x) and x > 0 else False
)

def get_bucket(p):
    if pd.isna(p): return None
    if p < 100_000: return "<100k"
    if p < 500_000: return "100k-500k"
    if p < 1_000_000: return "500k-1M"
    if p < 5_000_000: return "1M-5M"
    return ">5M"

products["price_bucket"] = products["price_current"].apply(get_bucket)

# Cell 5 — Lưu
out_path = PROCESSED_DIR / "fact_product_merged.csv"
products.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"\n✅ Đã lưu {len(products)} dòng → {out_path}")
print(f"\nThống kê crawled_by:")
print(products["crawled_by"].value_counts())
