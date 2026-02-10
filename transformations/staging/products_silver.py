import pandas as pd
from datetime import datetime
from pathlib import Path

project_root = None
current_file=Path(__file__).resolve()
for p in current_file.parents:
    if (p/ "data-lake").exists():
        project_root=p
        break

if project_root is None:
    raise FileNotFoundError("Racine du projet introuvable")

bronze_base_dir= project_root / "data-lake" / "raw" / "ecommerce" / "products"
silver_base_dir= project_root / "data-lake" / "staging" / "ecommerce" /"products"

if not bronze_base_dir.exists():
    raise FileNotFoundError(f"Fichier bronze : {bronze_base_dir}")
parquet_files= list(bronze_base_dir.rglob("*.parquet"))
if not parquet_files:
    raise FileNotFoundError("Aucun fichier bronze trouve dans la couche bronze")
dfs=[pd.read_parquet(p) for p in parquet_files]
df= pd.concat(dfs,ignore_index=True)
print(df.head())

silver_base_dir.mkdir(parents=True,exist_ok=True)
output_file= silver_base_dir/ f"products_silver.parquet"
df.to_parquet(output_file,index=False)
print(f"fichier silver ecrit")
