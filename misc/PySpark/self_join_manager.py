from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ManagerChain").getOrCreate()

# Sample data
data = [
    (1, "Amit", 3),
    (2, "Neha", 3),
    (3, "RAJ", 4),
    (4, "Suresh", None),
]
emp_df = spark.createDataFrame(data, ["empid", "emp_name", "mgr_id"])

# Step 1: Use clear aliases for employees vs managers
emp_e = emp_df.alias("emp_e")
emp_m = emp_df.alias("emp_m")

# Step 2: mgr_1 = direct manager of employee
mgr_df1 = (
    emp_e
    .join(
        emp_m,
        col("emp_e.mgr_id") == col("emp_m.empid"),
        "left"
    )
    .select(
        col("emp_e.empid").alias("empid"),
        col("emp_e.emp_name").alias("emp_name"),
        col("emp_m.empid").alias("mgr_1"),
        col("emp_m.mgr_id").alias("mgr_1_mgr_id")   # important: keep mgr_id of mgr_1
    )
)

# Step 3: mgr_2 = manager of mgr_1 (i.e., manager whose empid = mgr_1_mgr_id)
mgr_df2 = (
    mgr_df1.alias("t1")
    .join(
        emp_m.alias("m2"),
        col("t1.mgr_1_mgr_id") == col("m2.empid"),
        "left"
    )
    .select(
        col("t1.empid"),
        col("t1.emp_name"),
        col("t1.mgr_1"),
        col("m2.empid").alias("mgr_2"),
        col("m2.mgr_id").alias("mgr_2_mgr_id")   # keep mgr_id of mgr_2
    )
)

# Step 4: mgr_3 = manager of mgr_2
final_df = (
    mgr_df2.alias("t2")
    .join(
        emp_m.alias("m3"),
        col("t2.mgr_2_mgr_id") == col("m3.empid"),
        "left"
    )
    .select(
        col("t2.empid"),
        col("t2.emp_name"),
        col("t2.mgr_1"),
        col("t2.mgr_2"),
        col("m3.empid").alias("mgr_3")
    )
)

final_df.show()