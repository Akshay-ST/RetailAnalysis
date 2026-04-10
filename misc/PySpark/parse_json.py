from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("JsonParsing").getOrCreate()

# Sample data
data = [("Akshay", '{"street":"123 Main St", "city":"Pune"}'),
        ("Sachin", '{"street":"789 Main St", "city":"Mumbai"}')]

header = ["name", "address"]

# Define schema for the JSON column
json_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, header)

# Parse the JSON string in 'address' column
df_parsed = df.withColumn("address_json", from_json(col("address"), json_schema))

# Select and expand the fields
result_df = df_parsed.select(
    col("name"),
    col("address_json.street").alias("street"),
    col("address_json.city").alias("city")
)

# Show the result
result_df.show()