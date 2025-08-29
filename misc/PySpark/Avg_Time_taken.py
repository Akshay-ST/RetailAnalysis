from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize a SparkSession
# You can use `local[*]` to run on all available cores on your machine.
spark = SparkSession.builder \
    .appName("ActivityTable") \
    .getOrCreate()

# The data from the "Activity table" image.
# Each tuple represents a row in the DataFrame.
data = [
    (0, 0, "start", 0.712),
    (0, 0, "end", 1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end", 4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end", 1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end", 1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end", 4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end", 5.000)
]

# Define the schema for the DataFrame to ensure correct data types.
# This makes the DataFrame strongly typed, which is a best practice.
schema = StructType([
    StructField("machine_id", IntegerType(), True),
    StructField("process_id", IntegerType(), True),
    StructField("activity_type", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

df.printSchema()
df.show(truncate=False)

# Create time diff col

#df2 = df.withColumn("tdiff", when(df['activity_type'] = "start", ))

# Calculate the average time taken for each process on each machine. 
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql.functions import desc 
from pyspark.sql.functions import expr

window_spec =  Window.partitionBy("machine_id","process_id") \
              .orderBy(col("activity_type").desc()) \
              #.orderBy(desc("activity_type")) \
              #.rowsBetween(Window.currentRow, Window.unboundedFollowing)
              #.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing) \

df_w_end_time = data.withColumn("end_time", lead("timestamp").over(window_spec))
df_w_end_time.show(truncate=False)


df_w_start_end_time = df_w_end_time.withColumnRenamed("timestamp", "start_time") \
                        .where(col("end_time").isNotNull()) \
                        .drop("activity_type")
df_w_start_end_time.show(truncate=False)

df_w_avg_time = df_w_start_end_time.withColumn("tdiff", expr('end_time - start_time')) \
                    .groupBy("machine_id") \
                    .agg(avg("tdiff").alias("processing_time")) \
                    .orderBy("machine_id")

                    #.drop("process_id","start_time","end_time","tdiff") \ 
                    # Not needed as 

df_w_avg_time.show(truncate=False)