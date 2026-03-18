from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.appName("ConsecutiveFailures").getOrCreate()

# Your sample data (adding dates for full timestamps)
data = [
    (1, "2023-01-01 10:00:00", "FAILED"),
    (1, "2023-01-01 10:20:00", "FAILED"),
    (1, "2023-01-01 10:50:00", "FAILED"),
    (1, "2023-01-01 12:00:00", "FAILED"),
    (2, "2023-01-01 09:00:00", "FAILED"),
    (2, "2023-01-01 11:00:00", "FAILED"),
    (2, "2023-01-01 11:30:00", "FAILED"),
    (2, "2023-01-01 11:40:00", "SUCCESS")
]
transactions_df = spark.createDataFrame(data, ["custid", "txn_time", "status"])


# Convert to timestamp, filter failures, sort
df_failures = (transactions_df
    .withColumn("txn_ts", to_timestamp("txn_time"))
    .filter(col("status") == "FAILED")
    .orderBy("custid", "txn_ts")
)

# Window spec - partition by customer, order by time (default frame works for lag)
window_spec = Window.partitionBy("custid").orderBy("txn_ts")

# Process the data
df_result = (transactions_df
    .withColumn("txn_ts", to_timestamp("txn_time"))
    .filter(col("status") == "FAILED")
    .withColumn("lag1_time", lag("txn_ts", 1).over(window_spec))
    .withColumn("lag2_time", lag("txn_ts", 2).over(window_spec))
    .withColumn("gap1", col("txn_ts") - col("lag1_time"))  # Current - previous
    .withColumn("gap2", col("lag1_time") - col("lag2_time"))  # Previous - before previous
    .filter(
        col("lag2_time").isNotNull() &           # 3 consecutive exist
        (col("gap1") <= expr("interval 1 hour")) &  # Gap1 ≤ 1hr
        (col("gap2") <= expr("interval 1 hour"))    # Gap2 ≤ 1hr
    )
    .select("custid")
    .distinct()
)
df_result.show()
