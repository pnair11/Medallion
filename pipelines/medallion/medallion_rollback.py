from datetime import datetime
import json

DIM_TABLE = "dbo.Dim_Customer"
CONTROL_TABLE = "dbo.Control_Table"
ERROR_TABLE = "dbo.Error_Log"

def current_version(table_name):
    rows = spark.sql(f"DESCRIBE HISTORY {table_name}").select("version").orderBy("version", ascending=False).limit(1).collect()
    return int(rows[0].version) if rows else None

dim_v = current_version(DIM_TABLE)
ctl_v = current_version(CONTROL_TABLE)

actions = []
if dim_v is not None and dim_v > 0:
    target = dim_v - 1
    spark.sql(f"RESTORE TABLE {DIM_TABLE} TO VERSION AS OF {target}")
    actions.append(f"{DIM_TABLE} -> v{target}")

if ctl_v is not None and ctl_v > 0:
    target = ctl_v - 1
    spark.sql(f"RESTORE TABLE {CONTROL_TABLE} TO VERSION AS OF {target}")
    actions.append(f"{CONTROL_TABLE} -> v{target}")

summary = f"manual rollback performed at {datetime.now().isoformat()}: " + "; ".join(actions)
spark.createDataFrame([(summary, datetime.now(), None, None)], ["Error_Message","Error_Date","Run_ID","Stack_Trace"]) \
     .write.format("delta").mode("append").saveAsTable(ERROR_TABLE)
