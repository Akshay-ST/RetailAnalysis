from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Split Data").getOrCreate()

# Read the file
df = spark.read.text("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/file1.csv")

# Split the data into three parts
header_df = df.limit(1)
trailer_df = df.tail(3)
data_df = df.filter(df.value.notin([header_df.collect()[0][0]] + [row[0] for row in trailer_df]))


# Convert the trailer data to a DataFrame
trailer_df = spark.createDataFrame(trailer_df, ["value"])

# Write the data to separate files
header_df.coalesce(1).write.text("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v2/header", mode="overwrite")
data_df.coalesce(1).write.text("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v2/data", mode="overwrite")
trailer_df.coalesce(1).write.text("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v2/trailer", mode="overwrite")

# Stop the SparkSession
spark.stop()