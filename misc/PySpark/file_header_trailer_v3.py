from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Split Data").getOrCreate()

# Read the file
df = spark.read.text("file1.txt")

# Add a row number column
df = df.withColumn("row_num", F.monotonically_increasing_id())

# Split the data into three parts
header_df = df.filter(df.row_num == 0)
data_df = df.filter((df.row_num > 0) & (df.row_num < df.count() - 3))
trailer_df = df.filter(df.row_num >= df.count() - 3)

# Write the data to separate files
header_df.select("value").coalesce(1).write.text("/data/fht_v3/header", mode="overwrite")
data_df.select("value").coalesce(1).write.text("/data/fht_v3/data", mode="overwrite")
trailer_df.select("value").coalesce(1).write.text("/data/fht_v3/trailer", mode="overwrite")

# Stop the SparkSession
spark.stop()