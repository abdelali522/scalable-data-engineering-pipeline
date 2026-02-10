import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("data-lake/curated/ecommerce/revenue_by_day.parquet")
df["order_purchase_date"] = pd.to_datetime(df["order_purchase_date"])

plt.plot(df["order_purchase_date"], df["revenue"])
plt.title("Revenue by day")
plt.xlabel("Date")
plt.ylabel("Revenue")
plt.show()
