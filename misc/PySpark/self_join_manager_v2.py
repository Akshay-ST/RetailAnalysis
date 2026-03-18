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

# Step 1: mgr_1 = direct manager of employee
df = (
    emp_df.alias("e")
    .join(
        emp_df.alias("m1"),
        col("e.mgr_id") == col("m1.empid"),
        "left"
    )
    .select(
        col("e.empid").alias("empid"),
        col("e.emp_name").alias("emp_name"),
        col("m1.empid").alias("mgr_1"),
        col("m1.mgr_id").alias("mgr_1_mgr_id")   # keep mgr_id of mgr_1
    )
)

# Step 2: mgr_2 = manager of mgr_1 (i.e., mgr of RAJ → 4)
df = (
    df.alias("t1")
    .join(
        emp_df.alias("m2"),
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

# Step 3: mgr_3 = manager of mgr_2
result = (
    df.alias("t2")
    .join(
        emp_df.alias("m3"),
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

result.show()

result2 = (
    emp_df.alias("e")
    .join(emp_df.alias("m1"), col("e.mgr_id") == col("m1.empid"), "left")
    .join(emp_df.alias("m2"), col("m1.mgr_id") == col("m2.empid"), "left")
    .join(emp_df.alias("m3"), col("m2.mgr_id") == col("m3.empid"), "left")
    .select(
        col("e.empid"),
        col("e.emp_name"),
        col("m1.empid").alias("mgr_1"),
        col("m2.empid").alias("mgr_2"),
        col("m3.empid").alias("mgr_3"),
    )
)

result2.show()
