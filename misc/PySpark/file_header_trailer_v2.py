from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Split Data").getOrCreate()

# Read the file
df = spark.read.text("file1.txt")

# Split the data into three parts
header_df = df.limit(1)
data_df = df.filter(df.value.notin(["Header", "Trailer 3"]))
trailer_df = df.tail(3)

# Convert the trailer data to a DataFrame
trailer_df = spark.createDataFrame(trailer_df, ["value"])

# Write the data to separate files
header_df.coalesce(1).write.text("header", mode="overwrite")
data_df.coalesce(1).write.text("data", mode="overwrite")
trailer_df.coalesce(1).write.text("trailer", mode="overwrite")

# Stop the SparkSession
spark.stop()