from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import json

BASE_PATH = "dbfs:/medallion_runs"

def run_folder():
    try:
        rid = spark.conf.get("medallion.run_id")
        return f"{BASE_PATH}/{rid}"
    except:
        folders = dbutils.fs.ls(BASE_PATH)
        latest = sorted(folders, key=lambda f: f.name, reverse=True)[0]
        return latest.path.rstrip("/")

rf = run_folder()
bronze_path = f"{rf}/bronze.parquet"
silver_path = f"{rf}/silver.parquet"
print(f"[SILVER] run_folder={rf}")

bronze = spark.read.parquet(bronze_path)
w = Window.partitionBy("customer_id").orderBy(F.col("modified_date").desc(), F.col("ingest_time").desc())
silver = bronze.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

silver_count = silver.count()
silver.write.mode("overwrite").parquet(silver_path)
print(f"[SILVER] wrote {silver_path}, rows={silver_count}")

meta_path = f"{rf}/run_meta.json"
meta = json.loads(dbutils.fs.head(meta_path))
meta["silver_count"] = silver_count
dbutils.fs.put(meta_path, json.dumps(meta), overwrite=True)
print("[SILVER] done")
