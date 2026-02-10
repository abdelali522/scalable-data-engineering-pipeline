import pandas as pd
from pathlib import Path


def find_project_root() -> Path:
    """Assume this script is in pipeline0/transformations/curated/ ."""
    current_file = Path(__file__).resolve()
    return current_file.parents[2]  # pipeline0

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_gold(df: pd.DataFrame, out_path: Path, also_csv: bool = True) -> None:
    """Write parquet (and optionally CSV) for BI tools."""
    ensure_dir(out_path.parent)
    df.to_parquet(out_path, index=False)
    if also_csv:
        df.to_csv(out_path.with_suffix(".csv"), index=False, encoding="utf-8")
    print(f" GOLD écrit : {out_path} ({len(df)} lignes)")

def to_dt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def to_num(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def safe_lower_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    return df



project_root = find_project_root()
staging = project_root / "data-lake" / "staging"
curated = project_root / "data-lake" / "curated"


def read_silver(domain: str, table: str) -> pd.DataFrame:
    p = staging / domain / table / f"{table}_silver.parquet"
    if not p.exists():
        raise FileNotFoundError(f"SILVER introuvable : {p}")
    return pd.read_parquet(p)


orders    = read_silver("ecommerce", "orders")
customers = read_silver("ecommerce", "customers")
items     = read_silver("ecommerce", "items")
payments  = read_silver("ecommerce", "payments")
reviews   = read_silver("ecommerce", "reviews")
products  = read_silver("ecommerce", "products")
sellers   = read_silver("ecommerce", "sellers")


geo_path = staging / "ecommerce" / "geolocation" / "geolocation_silver.parquet"
cat_path = staging / "ecommerce" / "categories" / "categories_silver.parquet"
geolocation = pd.read_parquet(geo_path) if geo_path.exists() else None
categories  = pd.read_parquet(cat_path) if cat_path.exists() else None

orders = safe_lower_str(orders, ["order_id", "customer_id", "order_status"])
orders = to_dt(orders, [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
])

customers = safe_lower_str(customers, ["customer_id", "customer_unique_id", "customer_city", "customer_state"])

if "customer_zip_code_prefix" in customers.columns:
    customers["customer_zip_code_prefix"] = customers["customer_zip_code_prefix"].astype("string").str.strip()

items = safe_lower_str(items, ["order_id", "product_id", "seller_id"])
items = to_num(items, ["price", "freight_value"])
if "order_item_id" in items.columns:
    items["order_item_id"] = pd.to_numeric(items["order_item_id"], errors="coerce")

payments = safe_lower_str(payments, ["order_id", "payment_type"])
payments = to_num(payments, ["payment_sequential", "payment_installments", "payment_value"])

reviews = safe_lower_str(reviews, ["review_id", "order_id"])
reviews = to_num(reviews, ["review_score"])
reviews = to_dt(reviews, ["review_creation_date", "review_answer_timestamp"])

products = safe_lower_str(products, ["product_id", "product_category_name"])
products = to_num(products, [
    "product_name_lenght", "product_description_lenght",
    "product_photos_qty", "product_weight_g",
    "product_length_cm", "product_height_cm", "product_width_cm"
])

sellers = safe_lower_str(sellers, ["seller_id", "seller_city", "seller_state"])
if "seller_zip_code_prefix" in sellers.columns:
    sellers["seller_zip_code_prefix"] = sellers["seller_zip_code_prefix"].astype("string").str.strip()

if geolocation is not None:
    geolocation = safe_lower_str(geolocation, ["geolocation_city", "geolocation_state"])
    if "geolocation_zip_code_prefix" in geolocation.columns:
        geolocation["geolocation_zip_code_prefix"] = geolocation["geolocation_zip_code_prefix"].astype("string").str.strip()
    geolocation = to_num(geolocation, ["geolocation_lat", "geolocation_lng"])

if categories is not None:
    categories = safe_lower_str(categories, ["product_category_name", "product_category_name_english"])


# Build a simple Date dimension

# For BI + matplotlib: use purchase timestamp
df_dates = orders[["order_purchase_timestamp"]].dropna().copy()
df_dates["date"] = df_dates["order_purchase_timestamp"].dt.date
dim_date = (
    df_dates.drop_duplicates(subset=["date"])
            .assign(
                date_key=lambda x: pd.to_datetime(x["date"]),
                year=lambda x: pd.to_datetime(x["date"]).dt.year,
                month=lambda x: pd.to_datetime(x["date"]).dt.month,
                day=lambda x: pd.to_datetime(x["date"]).dt.day,
                week=lambda x: pd.to_datetime(x["date"]).dt.isocalendar().week.astype(int),
                weekday=lambda x: pd.to_datetime(x["date"]).dt.dayofweek  # 0=Mon
            )[["date", "year", "month", "day", "week", "weekday"]]
            .sort_values("date")
)
write_gold(dim_date, curated / "ecommerce" / "dim_date.parquet")



orders_tmp = orders.copy()
orders_tmp["order_purchase_date"] = orders_tmp["order_purchase_timestamp"].dt.date
orders_by_day = (
    orders_tmp.dropna(subset=["order_purchase_date"])
              .groupby(["order_purchase_date", "order_status"])
              .size()
              .reset_index(name="orders_count")
              .sort_values(["order_purchase_date", "order_status"])
)
write_gold(orders_by_day, curated / "ecommerce" / "orders_by_day.parquet")

# Status share (pie/bar)
order_status_share = (
    orders["order_status"]
    .value_counts(dropna=False)
    .reset_index()
)
order_status_share.columns = ["order_status", "orders_count"]
order_status_share["orders_share"] = order_status_share["orders_count"] / order_status_share["orders_count"].sum()
write_gold(order_status_share, curated / "ecommerce" / "order_status_share.parquet")

#  Delivery KPIs overall
deliv = orders.dropna(subset=["order_purchase_timestamp", "order_delivered_customer_date"]).copy()
deliv["delivery_days"] = (deliv["order_delivered_customer_date"] - deliv["order_purchase_timestamp"]).dt.total_seconds() / 86400
deliv["is_late"] = deliv["order_estimated_delivery_date"].notna() & (deliv["order_delivered_customer_date"] > deliv["order_estimated_delivery_date"])
delivery_kpis = pd.DataFrame([{
    "delivered_orders": len(deliv),
    "avg_delivery_days": float(deliv["delivery_days"].mean()),
    "median_delivery_days": float(deliv["delivery_days"].median()),
    "late_orders": int(deliv["is_late"].sum()),
    "late_rate": float(deliv["is_late"].mean()),
}])
write_gold(delivery_kpis, curated / "ecommerce" / "delivery_kpis.parquet")

#  Revenue (approx) by day from items: sum(price + freight) by purchase_date
# Join items -> orders (to get purchase date)
items_orders = items.merge(
    orders[["order_id", "order_purchase_timestamp"]],
    on="order_id",
    how="inner"
)
items_orders["order_purchase_date"] = items_orders["order_purchase_timestamp"].dt.date
items_orders["item_total"] = (items_orders.get("price", 0) + items_orders.get("freight_value", 0))
revenue_by_day = (
    items_orders.dropna(subset=["order_purchase_date"])
               .groupby("order_purchase_date")["item_total"]
               .sum()
               .reset_index(name="revenue")
               .sort_values("order_purchase_date")
)
write_gold(revenue_by_day, curated / "ecommerce" / "revenue_by_day.parquet")

#  Average order value (AOV) by day: revenue / nb_orders_day
orders_count_by_day = (
    orders_tmp.dropna(subset=["order_purchase_date"])
              .groupby("order_purchase_date")["order_id"]
              .nunique()
              .reset_index(name="orders")
)
aov_by_day = revenue_by_day.merge(orders_count_by_day, on="order_purchase_date", how="left")
aov_by_day["aov"] = aov_by_day["revenue"] / aov_by_day["orders"].replace({0: pd.NA})
write_gold(aov_by_day, curated / "ecommerce" / "aov_by_day.parquet")

#  Payment mix by day (stacked bar)
payments_orders = payments.merge(
    orders[["order_id", "order_purchase_timestamp"]],
    on="order_id",
    how="inner"
)
payments_orders["order_purchase_date"] = payments_orders["order_purchase_timestamp"].dt.date
payment_mix_by_day = (
    payments_orders.dropna(subset=["order_purchase_date"])
                  .groupby(["order_purchase_date", "payment_type"])["payment_value"]
                  .sum()
                  .reset_index(name="payment_value_sum")
                  .sort_values(["order_purchase_date", "payment_type"])
)
write_gold(payment_mix_by_day, curated / "ecommerce" / "payment_mix_by_day.parquet")

#  Review score by day (trend)
reviews_orders = reviews.merge(
    orders[["order_id", "order_purchase_timestamp"]],
    on="order_id",
    how="inner"
)
reviews_orders["order_purchase_date"] = reviews_orders["order_purchase_timestamp"].dt.date
review_score_by_day = (
    reviews_orders.dropna(subset=["order_purchase_date", "review_score"])
                 .groupby("order_purchase_date")["review_score"]
                 .mean()
                 .reset_index(name="avg_review_score")
                 .sort_values("order_purchase_date")
)
write_gold(review_score_by_day, curated / "ecommerce" / "review_score_by_day.parquet")

#  Geographic segmentation (customers)

#  Customers by state (BI bar)
if "customer_state" in customers.columns and "customer_unique_id" in customers.columns:
    customers_by_state = (
        customers.groupby("customer_state")["customer_unique_id"]
                .nunique()
                .reset_index(name="customers_count")
                .sort_values("customers_count", ascending=False)
    )
    write_gold(customers_by_state, curated / "ecommerce" / "customers_by_state.parquet")

#  Orders by state (needs join orders->customers)
if "customer_state" in customers.columns:
    orders_customers = orders.merge(
        customers[["customer_id", "customer_state"]],
        on="customer_id",
        how="left"
    )
    orders_customers["order_purchase_date"] = orders_customers["order_purchase_timestamp"].dt.date
    orders_by_state = (
        orders_customers.dropna(subset=["customer_state"])
                       .groupby("customer_state")["order_id"]
                       .nunique()
                       .reset_index(name="orders_count")
                       .sort_values("orders_count", ascending=False)
    )
    write_gold(orders_by_state, curated / "ecommerce" / "orders_by_state.parquet")

#  Delivery KPIs by state (avg delivery + late rate)
if "customer_state" in customers.columns:
    deliv_state = deliv.merge(
        customers[["customer_id", "customer_state"]],
        on="customer_id",
        how="left"
    ).dropna(subset=["customer_state"])
    delivery_kpis_by_state = (
        deliv_state.groupby("customer_state")
                  .agg(
                      delivered_orders=("order_id", "nunique"),
                      avg_delivery_days=("delivery_days", "mean"),
                      late_rate=("is_late", "mean"),
                  )
                  .reset_index()
                  .sort_values("delivered_orders", ascending=False)
    )
    write_gold(delivery_kpis_by_state, curated / "ecommerce" / "delivery_kpis_by_state.parquet")

print("Toutes les tables GOLD principales ont été générées.")
