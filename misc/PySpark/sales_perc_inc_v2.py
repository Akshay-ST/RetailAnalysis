from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# Create a SparkSession
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

# Sample data
data = [
    ("Jan", 2022, 1000),
    ("Feb", 2022, 1200),
    ("Mar", 2022, 1500),
    ("Apr", 2022, 1800),
    ("May", 2022, 2000),
    ("Jun", 2022, 2200),
    ("Jul", 2022, 2500),
    ("Aug", 2022, 2800),
    ("Sep", 2022, 3000),
    ("Oct", 2022, 3200),
    ("Nov", 2022, 3500),
    ("Dec", 2022, 3800),
    ("Jan", 2023, 4000),
    ("Feb", 2023, 4200),
    ("Mar", 2023, 4500),
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Month", "Year", "Sales"])

df.show()

# Define a window specification
window_spec = Window.partitionBy("Year").orderBy(F.col("Month").asc())

# Calculate previous and next month sales
df_with_prev_next = df.withColumn("Prev_Month_Sales", F.lag("Sales").over(window_spec)) \
                      .withColumn("Next_Month_Sales", F.lead("Sales").over(window_spec))

df_with_prev_next.show()

# Calculate percentage growth
df_with_growth = df_with_prev_next.withColumn("% Growth", 
                                              F.when(F.col("Prev_Month_Sales").isNull(), None) \
                                               .otherwise(((F.col("Sales") - F.col("Prev_Month_Sales")) / F.col("Prev_Month_Sales")) * 100))

df_with_growth.show()

# Select required columns
result_df = df_with_growth.select("Year", "Month", "Sales", "Prev_Month_Sales", "Next_Month_Sales", "% Growth")

# Show the result
result_df.show()