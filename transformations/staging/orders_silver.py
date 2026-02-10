import pandas as pd
from pathlib import Path
current_file = Path(__file__).resolve()


project_root = None
for p in current_file.parents:
    if (p/"data-lake").exists():
        project_root=p
        break
if project_root is None:
    raise FileNotFoundError("Racine du projet introuvable")

bronze_base_dir = project_root / "data-lake" / "raw" / "ecommerce" / "orders"
silver_base_dir = project_root / "data-lake" / "staging" / "ecommerce" / "orders"


if not bronze_base_dir.exists():
    raise FileNotFoundError(f"Dossier Bronze introuvable {bronze_base_dir}")


parquet_files = list(bronze_base_dir.rglob("*.parquet"))
# rglob ---> recherche recurcive dans tous les dossiers


if not parquet_files:
    raise FileNotFoundError("Aucun fichier Parquet trouvé dans la couche BRONZE")
dfs= [pd.read_parquet(p) for p in parquet_files]
df=pd.concat(dfs,ignore_index=True)
print(f"Nombre de ligne de BRONZE lues: {len(df)}")
print(df.head())
before = len(df)
df = df.drop_duplicates(subset=["order_id"])
after= len(df)
print(f"Deduplication:{before}--> {after} lignes")
silver_base_dir.mkdir(parents=True,exist_ok=True)
output_file = silver_base_dir/ "orders_silver.parquet"
df.to_parquet(output_file,index=False)
print(f"SILVER ecrit : {output_file}")






