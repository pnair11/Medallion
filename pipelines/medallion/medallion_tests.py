from pyspark.sql import functions as F

DIM_TABLE = "dev_catalog.bronze.Dim_Customer"

dim = spark.table(DIM_TABLE)

null_count = dim.filter(F.col("Customer_ID").isNull()).count()
assert null_count == 0, f"TEST FAIL: {null_count} NULL Customer_ID rows"

multi_current = dim.filter(F.col("Current_Flag")=='Y') \
                   .groupBy("Customer_ID").count().filter(F.col("count")>1).count()
assert multi_current == 0, f"TEST FAIL: {multi_current} customers have >1 current row"

bad_dates = dim.filter(F.col("End_Date").isNotNull() & (F.col("Start_Date") > F.col("End_Date"))).count()
assert bad_dates == 0, f"TEST FAIL: {bad_dates} rows have Start_Date > End_Date"

invalid_flag = dim.filter(~(F.col("Current_Flag").isin('Y','N'))).count()
assert invalid_flag == 0, f"TEST FAIL: {invalid_flag} rows have invalid Current_Flag"

print("[TEST] all tests passed")
