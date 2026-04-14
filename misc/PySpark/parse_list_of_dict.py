from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = SparkSession.builder \
        .appName("Explode") \
        .getOrCreate()


data = {
    "order_id": 101,
    "items": [
        {"product_id": 1, "price": 100},
        {"product_id": 2, "price": 200}
    ]
}

# Create DataFrame from input data
df = spark.createDataFrame([data])


# Explode the 'items' array to get one row per item
exploded_df = df.select(
    col("order_id"),
    explode(col("items")).alias("item")
)

# Select the required columns
result_df = exploded_df.select(
    col("order_id"),
    col("item.product_id"),
    col("item.price")
)

result_df.show()
"""
+--------+----------+-----+
|order_id|product_id|price|
+--------+----------+-----+
|     101|         1|  100|
|     101|         2|  200|
+--------+----------+-----+
"""