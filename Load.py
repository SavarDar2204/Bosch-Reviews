import sqlite3
import pandas as pd

# =====================================================
# CONFIG
# =====================================================

DB_NAME = "bosch_dw.db"

PRODUCTS_CSV = "stg_products.csv"
REVIEWS_CSV = "stg_reviews.csv"

# =====================================================
# LOAD STAGING
# =====================================================

products = pd.read_csv(PRODUCTS_CSV)
reviews = pd.read_csv(REVIEWS_CSV)

# =====================================================
# DIM_PRODUCT
# =====================================================

products = products.rename(columns={
    "model_code": "product_id"
})

product_cols = [
    "product_id",
    "serie",
    "install_type",
    "energy_class",
    "avg_rating",
    "review_count",
    "is_discontinued",
]

product_cols.extend(
    sorted(
        c for c in products.columns
        if c.startswith("has_") and not c.endswith("evidence")
    )
)

product_cols = [c for c in product_cols if c in products.columns]

dim_product = products[product_cols].copy()

# sicurezza: un prodotto = una riga
dim_product = dim_product.drop_duplicates(subset=["product_id"])

# =====================================================
# FACT_REVIEW
# =====================================================

reviews = reviews.rename(columns={
    "queried_product_id": "product_id"
})

review_cols = [
    "review_id",
    "product_id",
    "review_product_id",
    "year",
    "month",
    "day",
    "time",
    "rating",
    "is_incentivized",
    "rating_programs",
    "rating_loading",
    "rating_washing",
    "rating_noise",
    "country",
    "age_midpoint",
    "household_size",
    "household",
    "usage_weekly",
    "title_en",
    "text_en",
]

review_cols = [c for c in review_cols if c in reviews.columns]

fact_review = reviews[review_cols].copy()

# =====================================================
# CHIAVE TECNICA (SURROGATE KEY)
# =====================================================

fact_review = fact_review.reset_index(drop=True)
fact_review.insert(0, "review_key", range(1, len(fact_review) + 1))

# =====================================================
# SQLITE LOAD
# =====================================================

conn = sqlite3.connect(DB_NAME)
conn.execute("PRAGMA foreign_keys = ON")

cur = conn.cursor()

# pulizia tabelle
cur.execute("DROP TABLE IF EXISTS product_db")
cur.execute("DROP TABLE IF EXISTS review_db")

# =====================================================
# DIM_PRODUCT TABLE
# =====================================================

dim_product.to_sql(
    "product_db",
    conn,
    if_exists="replace",
    index=False
)

# =====================================================
# FACT_REVIEW TABLE
# =====================================================

fact_review.to_sql(
    "review_db",
    conn,
    if_exists="replace",
    index=False
)

# =====================================================
# SUMMARY
# =====================================================

conn.commit()
conn.close()

print("=" * 50)
print("LOAD COMPLETATO")
print("=" * 50)
print(f"Prodotti  : {len(dim_product)}")
print(f"Recensioni: {len(fact_review)}")
print(f"DB        : {DB_NAME}")
print("=" * 50)