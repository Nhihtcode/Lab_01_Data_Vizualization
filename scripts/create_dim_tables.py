# -*- coding: utf-8 -*-
"""
scripts/create_dim_tables.py — Tao cac bang Dimension
Chay sau khi da gop du lieu merged
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# Paths
BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RAW_DIR = BASE_DIR / "data" / "raw"

# Load merged data
merged_df = pd.read_csv(PROCESSED_DIR / "fact_product_merged.csv", encoding="utf-8-sig")

print("=" * 70)
print("[PREP] Phan tich du lieu merged - Chuan bi tao ban dimension")
print("=" * 70)

# ============================================================================
# 1. DIM_PLATFORM
# ============================================================================
print("\n[1] Tao DIM_PLATFORM")
print("-" * 70)

dim_platform = pd.DataFrame({
    "platform_id": ["tiki"],
    "platform_name": ["Tiki"],
    "base_url": ["https://tiki.vn"],
})

print(f"\nUnique platforms in data: {merged_df['platform_id'].unique()}")
print(f"[OK] Created {len(dim_platform)} platform(s)")
print(dim_platform)

platform_path = PROCESSED_DIR / "dim_platform.csv"
dim_platform.to_csv(platform_path, index=False, encoding="utf-8-sig")
print(f"\n[OK] Saved -> {platform_path}")

# ============================================================================
# 2. DIM_CATEGORY
# ============================================================================
print("\n\n[2] Tao DIM_CATEGORY")
print("-" * 70)

# Extract unique categories with their metadata
categories_data = merged_df[["category_id", "platform_id", "crawled_by"]].drop_duplicates()
print(f"\nUnique categories in data: {categories_data['category_id'].nunique()}")

# Map category names based on crawled_by
category_mapping = {
    "tiki_1883": ("Nha Cua & Doi Song", 1),
    "tiki_1882": ("Dien Gia Dung", 1),
    "tiki_1520": ("Lam Dep & Suc Khoe", 1),
    "tiki_931": ("Thoi Trang Nu", 1),
    "tiki_1789": ("Dien Thoai - May Tinh Bang", 1),
    "tiki_1815": ("Thiet Bi So - Phu Kien So", 1),
    "tiki_1846": ("Laptop - May Vi Tinh", 1),
    "tiki_1801": ("May Anh - May Quay Phim", 1),
}

dim_category_records = []
for cat_id in merged_df["category_id"].unique():
    category_name, level = category_mapping.get(cat_id, (cat_id, 1))
    platform_id = "tiki"
    
    dim_category_records.append({
        "category_id": cat_id,
        "platform_id": platform_id,
        "category_name": category_name,
        "parent_category_id": None,
        "level": level,
        "category_url": f"https://tiki.vn/cua-hang-{category_name.lower().replace(' ', '-')}",
    })

dim_category = pd.DataFrame(dim_category_records)
print(f"\n[OK] Created {len(dim_category)} categories")
print(dim_category)

category_path = PROCESSED_DIR / "dim_category.csv"
dim_category.to_csv(category_path, index=False, encoding="utf-8-sig")
print(f"\n[OK] Saved -> {category_path}")

# ============================================================================
# 3. DIM_SHOP (GỘP TỪ CÁC FILE CÓ SẴN + EXTRACT TỬ FACT)
# ============================================================================
print("\n\n[3] Tao DIM_SHOP")
print("-" * 70)

# Kiểm tra các dim_shop files có sẵn
dim_shop_files = list(RAW_DIR.glob("dim_shop_*.csv"))
print(f"\nTim thay {len(dim_shop_files)} existing dim_shop files:")
for f in dim_shop_files:
    print(f"  - {f.name}")

# Load và gộp các dim_shop files có sẵn
dim_shop_existing = []
for f in dim_shop_files:
    df = pd.read_csv(f, encoding="utf-8-sig")
    dim_shop_existing.append(df)
    print(f"  {f.name}: {len(df)} shops")

if dim_shop_existing:
    dim_shop = pd.concat(dim_shop_existing, ignore_index=True)
    dim_shop = dim_shop.drop_duplicates(subset=["shop_id"])
    print(f"\nSau merge & dedupe: {len(dim_shop)} unique shops")
else:
    print("\n[WARNING] No dim_shop files found, extracting from fact_product...")
    dim_shop = None

# Nếu thiếu shop info -> extract từ fact_product
if dim_shop is None or dim_shop.empty:
    print("\nRut trich shop info tu fact_product...")
    dim_shop_from_fact = merged_df[[
        "shop_id", "platform_id", "rating", "crawled_at"
    ]].drop_duplicates(subset=["shop_id"])
    
    dim_shop = pd.DataFrame({
        "shop_id": dim_shop_from_fact["shop_id"],
        "platform_id": dim_shop_from_fact["platform_id"],
        "shop_name": "Unknown",  # fallback
        "shop_type": "individual",  # fallback
        "is_mall": False,
        "shop_rating": dim_shop_from_fact["rating"],
        "follower_count": None,
        "response_rate": None,
        "response_time_hours": None,
        "prep_time_hours": None,
        "location": None,
        "total_products": None,
        "shop_url": None,
        "crawled_at": dim_shop_from_fact["crawled_at"],
    })
    print(f"[OK] Rut trich {len(dim_shop)} shops tu fact_product")
else:
    # Ensure all required columns exist
    required_cols = [
        "shop_id", "platform_id", "shop_name", "shop_type", "is_mall",
        "shop_rating", "follower_count", "response_rate", "response_time_hours",
        "prep_time_hours", "location", "total_products", "shop_url", "crawled_at"
    ]
    for col in required_cols:
        if col not in dim_shop.columns:
            dim_shop[col] = None

print(f"\n[OK] Final dim_shop: {len(dim_shop)} shops")
print(dim_shop.head(10))

shop_path = PROCESSED_DIR / "dim_shop.csv"
dim_shop.to_csv(shop_path, index=False, encoding="utf-8-sig")
print(f"\n[OK] Saved -> {shop_path}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 70)
print("[DATA] DIMENSION TABLES - CREATED SUCCESSFULLY")
print("=" * 70)
print(f"""
[OK] dim_platform.csv:  {len(dim_platform)} rows
[OK] dim_category.csv:  {len(dim_category)} rows
[OK] dim_shop.csv:      {len(dim_shop)} rows""")
