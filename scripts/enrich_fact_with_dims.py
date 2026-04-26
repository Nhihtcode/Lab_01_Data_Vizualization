"""
scripts/enrich_fact_with_dims.py — Add dimension attributes to fact table
This creates a denormalized view for easier dashboard queries
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

print("=" * 80)
print(" ENRICHING FACT_PRODUCT WITH DIMENSION ATTRIBUTES")
print("=" * 80)

# Load tables
fact_product = pd.read_csv(PROCESSED_DIR / "fact_product_merged.csv", encoding="utf-8-sig")
dim_platform = pd.read_csv(PROCESSED_DIR / "dim_platform.csv", encoding="utf-8-sig")
dim_category = pd.read_csv(PROCESSED_DIR / "dim_category.csv", encoding="utf-8-sig")
dim_shop = pd.read_csv(PROCESSED_DIR / "dim_shop.csv", encoding="utf-8-sig")

print(f"\n Loaded {len(fact_product)} products")

# ============================================================================
# JOIN với dim_platform
# ============================================================================
print("\n[1] JOIN with dim_platform...")
fact_product = fact_product.merge(
    dim_platform[["platform_id", "platform_name", "base_url"]],
    on="platform_id",
    how="left"
)
print(f"   Added: platform_name, base_url")

# ============================================================================
# JOIN với dim_category
# ============================================================================
print("[2] JOIN with dim_category...")
fact_product = fact_product.merge(
    dim_category[["category_id", "category_name", "level"]],
    on="category_id",
    how="left",
    suffixes=("", "_dim")
)
print(f"   Added: category_name, level")

# ============================================================================
# JOIN với dim_shop (LEFT JOIN - keep nulls for missing shops)
# ============================================================================
print("[3] JOIN with dim_shop...")
shop_to_join = dim_shop[[
    "shop_id", "shop_name", "shop_type", "is_mall", "shop_rating",
    "follower_count", "response_rate", "location", "total_products"
]]

fact_product = fact_product.merge(
    shop_to_join,
    on="shop_id",
    how="left",
    suffixes=("", "_dim")
)
print(f"   Added: shop_name, shop_type, is_mall, shop_rating, follower_count, etc.")

# Handle missing shop_name
fact_product["shop_name"] = fact_product["shop_name"].fillna("Unknown Shop")
fact_product["shop_type"] = fact_product["shop_type"].fillna("unknown")
fact_product["is_mall"] = fact_product["is_mall"].fillna(False)

print(f"\n[OK] Enriched {len(fact_product)} rows with dimension attributes")

# ============================================================================
# Show sample after JOIN
# ============================================================================
print("\n[LIST] Sample enriched data:")
sample_cols = [
    "product_id", "product_name", "category_name", "shop_name",
    "price_current", "sold_count", "rating", "shop_rating", "is_mall"
]
print(fact_product[sample_cols].head(10).to_string())

# ============================================================================
# Data Quality Check
# ============================================================================
print("\n\n[DATA] DATA QUALITY AFTER ENRICHMENT:")
print("-" * 80)

total_rows = len(fact_product)
missing_shops = fact_product[fact_product["shop_name"] == "Unknown Shop"]
print(f"\nRows without shop dimension info: {len(missing_shops)} ({len(missing_shops)/total_rows*100:.2f}%)")
print(f"  (These can still be analyzed, just without shop attributes)")

print(f"\nCategory coverage: {fact_product['category_name'].notna().sum()} / {total_rows}")
print(f"Platform coverage: {fact_product['platform_name'].notna().sum()} / {total_rows}")

# ============================================================================
# Reorganize columns for better readability
# ============================================================================
print("\n\n[PROCESS] Reorganizing columns...")

columns_order = [
    # Identifiers
    "product_id", "platform_id", "category_id", "shop_id",
    # Platform & Category names (from dimensions)
    "platform_name", "base_url", "category_name", "level",
    # Shop names (from dimension)
    "shop_name", "shop_type", "is_mall",
    # Product details
    "product_name", "product_url",
    # Pricing
    "price_current", "price_original", "discount_percent",
    "price_ends_with_9", "price_bucket",
    # Sales performance
    "sold_count", "stock", "rating", "review_count",
    # Media
    "image_count", "has_video",
    "review_with_image_count", "five_star_with_image_count",
    # Shipping
    "shipping_fee", "is_freeship", "estimated_delivery_days",
    # Promotions
    "has_freeship_xtra_label", "has_coinback_label", "has_voucher_label",
    "promotion_label_count",
    # Shop dimension attributes
    "shop_rating", "follower_count", "response_rate", "location",
    "total_products",
    # Metadata
    "crawled_at", "crawled_by",
]

# Only keep columns that exist
available_cols = [col for col in columns_order if col in fact_product.columns]
fact_product = fact_product[available_cols]

# ============================================================================
# Save enriched data
# ============================================================================
out_path = PROCESSED_DIR / "fact_product_enriched.csv"
fact_product.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"[OK] Saved enriched data -> {out_path}")

print(f"\nEnriched table shape: {fact_product.shape}")
print(f"Columns: {len(fact_product.columns)}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 80)
print("[DONE] ENRICHMENT COMPLETE")
print("=" * 80)
print("""
[OK] Created fact_product_enriched.csv with:
   • All fact_product columns
   • Platform name & URL (from dim_platform)
   • Category name & level (from dim_category)  
   • Shop attributes: name, type, rating, mall status, etc (from dim_shop)

This enriched table is optimized for:
  [OK] Dashboard visualizations (no extra JOINs needed)
  [OK] EDA & analysis queries
  [OK] Direct BI tool queries (Tableau, Power BI, etc)

Files Ready:
  [OK] data/processed/fact_product_merged.csv (base)
  [OK] data/processed/fact_product_enriched.csv (with dimensions)
  [OK] data/processed/dim_platform.csv
  [OK] data/processed/dim_category.csv
  [OK] data/processed/dim_shop.csv""")
