from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':

    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("Streaming Application") \
            .config("spark.sql.shuffle.partitions",4) \
            .master("local[2]") \
            .getOrCreate()

    orders_schema = "order_id int, order_date date, order_customer_id int, order_status string"

# 1. Reading DATA
    orders_df = spark \
    .readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("path","data/inputdir") \
    .load()

# 2. Processing Logic
    orders_df.createOrReplaceTempView("orders")
    status_count_df = spark.sql("""
                                    select order_status, count(*) as count 
                                    from orders
                                    group by order_status
                                    """)

# 3. Wrtie to SINK
query = status_count_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation","checkpointdir1") \
    .start()
    
query.awaitTermination()
spark.stop()