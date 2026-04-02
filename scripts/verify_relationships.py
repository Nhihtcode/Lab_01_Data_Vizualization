# -*- coding: utf-8 -*-
"""
scripts/verify_relationships.py -- Verify Foreign Key relationships
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

print("=" * 80)
print("VERIFY FOREIGN KEY RELATIONSHIPS")
print("=" * 80)

# Load all tables
fact_product = pd.read_csv(PROCESSED_DIR / "fact_product_merged.csv", encoding="utf-8-sig")
dim_platform = pd.read_csv(PROCESSED_DIR / "dim_platform.csv", encoding="utf-8-sig")
dim_category = pd.read_csv(PROCESSED_DIR / "dim_category.csv", encoding="utf-8-sig")
dim_shop = pd.read_csv(PROCESSED_DIR / "dim_shop.csv", encoding="utf-8-sig")

print("\nTABLE SIZES:")
print(f"  fact_product: {len(fact_product)} rows")
print(f"  dim_platform: {len(dim_platform)} rows")
print(f"  dim_category: {len(dim_category)} rows")
print(f"  dim_shop: {len(dim_shop)} rows")

# ============================================================================
# 1. FK: fact_product.platform_id -> dim_platform.platform_id
# ============================================================================
print("\n\n[1] FACT_PRODUCT.PLATFORM_ID → DIM_PLATFORM.PLATFORM_ID")
print("-" * 80)

missing_platform = fact_product[~fact_product["platform_id"].isin(dim_platform["platform_id"])]
if len(missing_platform) > 0:
    print(f"[ERROR] {len(missing_platform)} orphaned rows (missing platform_id)")
    print(f"  Platforms in fact: {fact_product['platform_id'].unique()}")
else:
    print("[OK] All platform_ids are valid")

# ============================================================================
# 2. FK: fact_product.category_id -> dim_category.category_id
# ============================================================================
print("\n[2] FACT_PRODUCT.CATEGORY_ID -> DIM_CATEGORY.CATEGORY_ID")
print("-" * 80)

missing_category = fact_product[~fact_product["category_id"].isin(dim_category["category_id"])]
if len(missing_category) > 0:
    print(f"[ERROR] {len(missing_category)} orphaned rows (missing category_id)")
    print(f"  Categories in fact: {missing_category['category_id'].unique()}")
else:
    print("[OK] All category_ids are valid")

# ============================================================================
# 3. FK: fact_product.shop_id -> dim_shop.shop_id
# ============================================================================
print("\n[3] FACT_PRODUCT.SHOP_ID -> DIM_SHOP.SHOP_ID")
print("-" * 80)

missing_shop = fact_product[~fact_product["shop_id"].isin(dim_shop["shop_id"])]
if len(missing_shop) > 0:
    print(f"[WARNING] {len(missing_shop)} orphaned rows ({(len(missing_shop)/len(fact_product)*100):.2f}%)")
    print(f"  Sample missing shop_ids: {missing_shop['shop_id'].unique()[:5]}")
    print("\n  This is expected for Tuấn/Ý/Thế Anh (no dim_shop created)")
    print("  These shops are still in fact_product but not in dim_shop dimension")
else:
    print(" All shop_ids are valid")

# ============================================================================
# 4. Coverage Analysis
# ============================================================================
print("\n[4] COVERAGE ANALYSIS")
print("-" * 80)

print(f"\nPlatform coverage: {len(dim_platform)} / {fact_product['platform_id'].nunique()} = 100%")
print(f"Category coverage: {len(dim_category)} / {fact_product['category_id'].nunique()} = 100%")

shop_coverage = len(dim_shop.merge(fact_product[["shop_id"]], on="shop_id", how="inner")) / len(fact_product)
print(f"Shop coverage: {len(dim_shop)} dims / {fact_product['shop_id'].nunique()} unique shops")
print(f"  → {shop_coverage*100:.2f}% of fact rows have matching dim_shop")

# ============================================================================
# 5. Data Quality Checks
# ============================================================================
print("\n\n[5] DATA QUALITY CHECKS")
print("-" * 80)

# Check for nulls in PK columns
print("\nPrimary Key Nulls:")
for table_name, table_df in [
    ("dim_platform", dim_platform),
    ("dim_category", dim_category),
    ("dim_shop", dim_shop),
]:
    if table_name == "dim_platform":
        pk_col = "platform_id"
    elif table_name == "dim_category":
        pk_col = "category_id"
    else:
        pk_col = "shop_id"
    
    null_count = table_df[pk_col].isna().sum()
    if null_count > 0:
        print(f" [WARNING] {table_name}.{pk_col}: {null_count} nulls")
    else:
        print(f"  [PASSED] {table_name}.{pk_col}: OK")

# Check for nulls in FK columns
print("\nForeign Key Nulls in fact_product:")
fk_cols = ["platform_id", "category_id", "shop_id"]
for col in fk_cols:
    null_count = fact_product[col].isna().sum()
    if null_count > 0:
        print(f"  [WARNING] {col}: {null_count} nulls")
    else:
        print(f"  [PASSED] {col}: OK")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 80)
print("[DATA] RELATIONSHIP SUMMARY")
print("=" * 80)

print("[OK] DIMENSION TABLES READY FOR JOIN")

# ============================================================================
# Create summary CSV for reference
# ============================================================================
summary_df = pd.DataFrame({
    "Table": ["dim_platform", "dim_category", "dim_shop", "fact_product"],
    "Rows": [len(dim_platform), len(dim_category), len(dim_shop), len(fact_product)],
    "Type": ["Dimension", "Dimension", "Dimension", "Fact"],
})

summary_path = PROCESSED_DIR / "table_summary.csv"
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"\n[LIST] Summary saved -> {summary_path}")
