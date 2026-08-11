from pyspark.sql import functions as F
from datetime import datetime
import json

BASE_PATH = "dbfs:/medallion_runs"
SRC_TABLE = "dev_catalog.bronze.Customer_Source"
CONTROL_TABLE = "dev_catalog.bronze.Control_Table"
TABLE_NAME_KEY = "Customer"

def now_id():
    return datetime.now().strftime("%Y%m%d%H%M%S")

def run_folder_for_new_run():
    rid = now_id()
    return rid, f"{BASE_PATH}/{rid}"

def write_json(path, obj):
    dbutils.fs.mkdirs(path.rsplit("/",1)[0])
    dbutils.fs.put(path, json.dumps(obj), overwrite=True)

run_id, run_folder = run_folder_for_new_run()
print(f"[BRONZE] run_id={run_id}, run_folder={run_folder}")

ctl = spark.table(CONTROL_TABLE).filter(F.col("Table_Name") == TABLE_NAME_KEY).select("Last_Load_Date").limit(1).collect()
last_load_date = ctl[0].Last_Load_Date if ctl and ctl[0].Last_Load_Date is not None else datetime(1970,1,1)

src = spark.table(SRC_TABLE).select(
    F.col("Customer_ID").alias("customer_id"),
    F.col("Customer_Name").alias("customer_name"),
    F.col("City").alias("city"),
    F.col("Phone").alias("phone"),
    F.col("Modified_Date").alias("modified_date")
)

bronze = src.filter(F.col("modified_date") > F.lit(last_load_date)) \
            .withColumn("ingest_time", F.current_timestamp()) \
            .withColumn("run_id", F.lit(run_id))

count = bronze.count()
print(f"[BRONZE] staged rows = {count}")

bronze_path = f"{run_folder}/bronze.parquet"
if count > 0:
    bronze.write.mode("overwrite").parquet(bronze_path)
    print(f"[BRONZE] wrote {bronze_path}")

meta = {"run_id": run_id, "timestamp": datetime.now().isoformat(), "bronze_count": count}
write_json(f"{run_folder}/run_meta.json", meta)
print("[BRONZE] done")
