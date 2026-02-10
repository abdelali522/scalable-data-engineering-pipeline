import pandas as pd
from pathlib import  Path

current_file= Path(__file__).resolve()

project_root= None
for p in current_file.parents:
    if(p/"data-lake").exists():
        project_root=p
        break
if project_root is None:
    raise FileNotFoundError("Racine du project introuvable")



bronze_base_dir= project_root/ "data-lake" / "raw" /"ecommerce" / "geolocation"
silver_base_dir= project_root / "data-lake" / "staging" / "ecommerce" / "geolocation"

if not bronze_base_dir.exists():
    raise FileNotFoundError(f"Fichier bronze introuvable: {bronze_base_dir}")

parquet_files= list(bronze_base_dir.rglob("*.parquet"))
if not parquet_files:
    raise FileNotFoundError("Aucun fichier Parquet trouvé dans la couche BRONZE")
dfs=[pd.read_parquet(p) for p in parquet_files]
df= pd.concat(dfs,ignore_index=True)
silver_base_dir.mkdir(parents=True,exist_ok=True)

output_file= silver_base_dir/ "geolocation_silver.parquet"

df.to_parquet(output_file,index=False)
print(f"Fichier silver cree : {output_file}")