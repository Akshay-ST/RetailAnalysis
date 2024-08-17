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
    completed_orders_df = spark.sql("""
                                    select * 
                                    from orders
                                    where order_status = 'COMPLETE'
                                    """)

# 3. Wrtie to SINK
query = completed_orders_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation","checkpointdir1") \
    .option("path","data/outputdir") \
    .start()
    
query.awaitTermination()