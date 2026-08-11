from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import traceback

SRC_TABLE = "dev_catalog.bronze.Customer_Source"
DIM_TABLE = "dev_catalog.bronze.Dim_Customer"
CONTROL_TABLE = "dev_catalog.bronze.Control_Table"
ERROR_TABLE = "dev_catalog.bronze.Error_Log"
TABLE_NAME_KEY = "Customer"

run_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))

ctl_row = spark.table(CONTROL_TABLE).filter(F.col("Table_Name") == TABLE_NAME_KEY).select("Last_Load_Date").limit(1).collect()
last_load_date = ctl_row[0].Last_Load_Date if ctl_row and ctl_row[0].Last_Load_Date is not None else datetime(1970,1,1)

src = spark.table(SRC_TABLE).select(
    F.col("Customer_ID").alias("customer_id"),
    F.col("Customer_Name").alias("customer_name"),
    F.col("City").alias("city"),
    F.col("Phone").alias("phone"),
    F.col("Modified_Date").alias("modified_date")
)
staged = src.filter(F.col("modified_date") > F.lit(last_load_date)) \
            .withColumn("ingest_time", F.current_timestamp()) \
            .withColumn("source_run_id", F.lit(run_id))

if staged.rdd.isEmpty():
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS T
        USING (SELECT '{TABLE_NAME_KEY}' AS Table_Name) AS S
        ON T.Table_Name = S.Table_Name
        WHEN MATCHED THEN UPDATE SET Last_Load_Date = current_timestamp(), Load_Status = 'SUCCESS', Last_Run_ID = {run_id}
        WHEN NOT MATCHED THEN INSERT (Table_Name, Last_Load_Date, Load_Status, Last_Run_ID) VALUES ('{TABLE_NAME_KEY}', current_timestamp(), 'SUCCESS', {run_id})
    """)
else:
    w = Window.partitionBy("customer_id").orderBy(F.col("modified_date").desc(), F.col("ingest_time").desc())
    silver = staged.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    silver.createOrReplaceTempView("silver_stage")

    def latest_version(table_name):
        r = spark.sql(f"DESCRIBE HISTORY {table_name}").select("version").orderBy(F.col("version").desc()).limit(1).collect()
        return int(r[0].version) if r else None

    dim_version_pre = latest_version(DIM_TABLE)
    control_version_pre = latest_version(CONTROL_TABLE)

    try:
        spark.sql(f"""
        MERGE INTO {DIM_TABLE} AS Target
        USING (SELECT customer_id, city, phone FROM silver_stage) AS Source
        ON Target.Customer_ID = Source.customer_id AND Target.Current_Flag = 'Y'
        WHEN MATCHED AND (
            (Target.City IS NULL AND Source.city IS NOT NULL) OR (Target.City IS NOT NULL AND Source.city IS NULL) OR (Target.City <> Source.city)
            OR (Target.Phone IS NULL AND Source.phone IS NOT NULL) OR (Target.Phone IS NOT NULL AND Source.phone IS NULL) OR (Target.Phone <> Source.phone)
        )
        THEN UPDATE SET End_Date = date_sub(current_date(), 1), Current_Flag = 'N'
        """)

        spark.sql(f"""
        MERGE INTO {DIM_TABLE} AS Target
        USING (SELECT customer_id, customer_name, city, phone FROM silver_stage) AS Source
        ON Target.Customer_ID = Source.customer_id AND Target.Current_Flag = 'Y' AND (Target.City <=> Source.city) AND (Target.Phone <=> Source.phone)
        WHEN NOT MATCHED THEN
          INSERT (Customer_ID, Customer_Name, City, Phone, Start_Date, End_Date, Current_Flag)
          VALUES (Source.customer_id, Source.customer_name, Source.city, Source.phone, current_date(), NULL, 'Y')
        """)

        spark.sql(f"""
            MERGE INTO {CONTROL_TABLE} AS T
            USING (SELECT '{TABLE_NAME_KEY}' AS Table_Name) AS S
            ON T.Table_Name = S.Table_Name
            WHEN MATCHED THEN UPDATE SET Last_Load_Date = current_timestamp(), Load_Status = 'SUCCESS', Last_Run_ID = {run_id}
            WHEN NOT MATCHED THEN INSERT (Table_Name, Last_Load_Date, Load_Status, Last_Run_ID) VALUES ('{TABLE_NAME_KEY}', current_timestamp(), 'SUCCESS', {run_id})
        """)
    except Exception as exc:
        err_msg = str(exc)
        stack = traceback.format_exc()
        spark.createDataFrame([(err_msg, datetime.now(), run_id, stack)], ["Error_Message","Error_Date","Run_ID","Stack_Trace"]) \
             .write.format("delta").mode("append").saveAsTable(ERROR_TABLE)

        if dim_version_pre is not None:
            spark.sql(f"RESTORE TABLE {DIM_TABLE} TO VERSION AS OF {dim_version_pre}")

        if control_version_pre is not None:
            spark.sql(f"RESTORE TABLE {CONTROL_TABLE} TO VERSION AS OF {control_version_pre}")

        raise
