from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':

    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("Streaming Application") \
            .config("spark.sql.shuffle.partitions",4) \
            .master("local[2]") \
            .getOrCreate()
    
# 1. Reading DATA
    lines = spark \
    .readStream \
    .format("socket") \
    .option("host","localhost") \
    .option("port",9992) \
    .load()

# 2. Processing Logic
    words_df = lines.select(explode(split(lines.value," ")).alias("word"))
    word_count = words_df.groupBy("word").count()

# 3. Wrtie to SINK
query = word_count \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation","checkpointdir1") \
    .start()
    
query.awaitTermination()