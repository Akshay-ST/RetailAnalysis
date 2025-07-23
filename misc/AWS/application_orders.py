import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct


if __name__ ==  "__main__" :
  
    spark = SparkSession\
        .builder\
        .appName("WordCount")\
        .getOrCreate()

    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
    else:
        print("S3 output location not specified printing top 10 results to output stream")

    region = os.getenv("AWS_REGION")
    orders_df = spark.read \
    .format("csv") \
    .option("inferSchema","True") \
    .option("header","True") \
    .load(input_path)
    
    # Aggregate data
    status_counts = orders_df.groupBy("order_status").count()


    # Show aggregated data
    status_counts.show(truncate=False)
    
    
    if output_path:
        status_counts.write.mode("overwrite").parquet(output_path)
        print("WordCount job completed successfully. Refer output at S3 path: " + output_path)
    else:
        counts_df.show(10, False)
        print("WordCount job completed successfully.")

    spark.stop()
