from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, date_format
 
spark = SparkSession.builder.appName("RemoveTimePart").getOrCreate()
 
data = [(1, "2024-02-01 00:00:00"), (2, "2024-02-01 00:00:00"), (3, "2024-02-03 00:00:00"), (4, "2024-02-04 00:00:00",)]
columns = ["Order_id","Order_date"]
 
df = spark.createDataFrame(data, columns)
print(df)

# Convert the string column to a timestamp
timestamp_format = "yyyy-MM-dd HH:mm:ss"

df_with_timestamp = df.withColumn("timestamp_col", unix_timestamp(col("Order_date"), timestamp_format).cast("timestamp"))
#df_with_timestamp = df.withColumn("timestamp_col", to_date(col("Order_date")))
df_with_timestamp.show()

 
df_without_time = df_with_timestamp.withColumn("Order_date", date_format(col("timestamp_col"), "yyyy-MM-dd")).drop("timestamp_col")
df_without_time.show()