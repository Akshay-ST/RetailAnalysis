from pyspark.sql import SparkSession

from pyspark.sql.functions import row_number, avg, round, col
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("Duplicates") \
        .getOrCreate()

data = [(1, "Alice", 5000), (2, "Bob", 6000), (3, "Charlie", 7000), (1, "Alice", 5000), (2, "Bob", 6000)]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)   
#df.show()

df_avg = df.groupBy("id") \
            .agg(round(avg("salary"), 2).alias("avg"))

df_avg.show()

window_spec = Window.partitionBy("id").orderBy("name")

# df_rn = df.withColumn("rn", row_number().over(window_spec))

#df_rn.show()

df_filtered = df.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn")


# df_filtered = df.withColumn("rn", row_number().over(window_spec)) \
#                 .select("id","salary", "rn") \
#                 .filter("rn = 1")

# df_filtered = df.withColumn("rn", row_number().over(window_spec)) \
#                 .filter("rn = 1")

# df_filtered = df.withColumn("rn", row_number().over(window_spec)) \
#                 .where("rn = 1")

df_filtered.show()
df.dropDuplicates().show()

spark.stop()