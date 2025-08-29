from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, first

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("AverageProcessingTime") \
    .getOrCreate()

# Create the initial DataFrame from the provided image data
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


df = spark.createDataFrame(data, ["machine_id", "process_id", "activity_type", "timestamp"])

# Step 1: Pivot the DataFrame to get 'start' and 'end' timestamps in separate columns
# We use the `first` aggregation function because there's only one start and one end for each process
pivoted_df = df.groupBy("machine_id", "process_id")\
                .pivot("activity_type") \
                .agg(first("timestamp"))

# Show the pivoted DataFrame to see the intermediate result
print("Pivoted DataFrame:")
pivoted_df.show()

# Step 2: Calculate the processing time for each process
# This is simply the difference between the 'end' and 'start' timestamps
processing_time_df = pivoted_df.withColumn("processing_time",col("end") - col("start") )

# Show the DataFrame with the calculated processing time
print("DataFrame with Processing Time:")
processing_time_df.show()

# Step 3: Group by machine_id and calculate the average processing time
average_time_df = processing_time_df.groupBy("machine_id") \
                    .agg(avg("processing_time").alias("average_processing_time"))

# Show the final result
print("Final result - Average processing time per machine:")
average_time_df.show()

# Stop the SparkSession
spark.stop()
