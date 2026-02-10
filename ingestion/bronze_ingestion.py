import pandas as pd
from pathlib import Path
from datetime import datetime

current_file = Path(__file__) #le fichier acctuel
project_root= current_file.parent.parent #le chemin parent du projet(pipeline0)
ingestion_ts= datetime.now()
ingestion_date= ingestion_ts.date()

def ingest_one(source_subdir: str, source_filename: str, raw_subdir: str, out_prefix: str, data_source: str):
    source_csv = project_root / "source_data" / source_subdir / source_filename
    if not source_csv.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {source_csv}")

    df = pd.read_csv(source_csv)

    raw_base_dir = project_root / "data-lake" / "raw" / raw_subdir
    output_dir = raw_base_dir / f"ingestion_date={ingestion_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{out_prefix}_{ingestion_ts.strftime('%Y%m%d_%H%M%S')}.parquet"

    df["ingestion_timestamp"] = ingestion_ts
    df["ingestion_date"] = ingestion_date
    df["source_file"] = source_filename
    df["data_source"] = data_source

    df.to_parquet(output_file, index=False)
    print(f"Parquet écrit : {output_file} ({len(df)} lignes)")

ingest_one("ecommerce", "olist_orders_dataset.csv", "ecommerce/orders", "orders", "ecommerce")
ingest_one("ecommerce", "olist_customers_dataset.csv", "ecommerce/customers", "customers", "ecommerce")
ingest_one("ecommerce", "olist_products_dataset.csv", "ecommerce/products", "products", "ecommerce")
ingest_one("ecommerce", "olist_sellers_dataset.csv", "ecommerce/sellers", "sellers", "ecommerce")

ingest_one("marketing", "marketing_campaign_dataset.csv", "marketing", "marketing", "marketing")

ingest_one("ecommerce", "olist_geolocation_dataset.csv", "ecommerce/geolocation", "geolocation", "ecommerce")
ingest_one("ecommerce", "olist_order_items_dataset.csv", "ecommerce/items", "items", "ecommerce")
ingest_one("ecommerce", "olist_order_payments_dataset.csv", "ecommerce/payments", "payments", "ecommerce")
ingest_one("ecommerce", "olist_order_reviews_dataset.csv", "ecommerce/reviews", "reviews", "ecommerce")
ingest_one("ecommerce", "product_category_name_translation.csv", "ecommerce/categories", "categories", "ecommerce")


