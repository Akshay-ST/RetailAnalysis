from pyspark.sql import SparkSession

print("creating Session")
spark = SparkSession.builder.getOrCreate()

orders_schema = "order_id int,order_date string,order_customer_id, int,order_status string"

print("Reading File")
orders_df = spark.read.format("csv").option("header", "true").schema(orders_schema).load("data/orders.csv")

print("Action Called")
print(orders_df.count())

print("data demo")
print(orders_df.show(10, truncate=False))

