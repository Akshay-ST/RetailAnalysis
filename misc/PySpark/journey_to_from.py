from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Delhi", "Hyderabad", 1),
    (1, "Hyderabad", "Chennai", 2),
    (1, "Chennai", "Coimbatore", 3),
    (2, "Mumbai", "Pune", 1),
    (2, "Pune", "Bangalore", 2)
]

columns = ["customer_id", "from_city", "to_city", "journey_order"]

df = spark.createDataFrame(data, columns)

df.show()


from pyspark.sql import functions as F
from pyspark.sql.window import Window
'''
w_start = Window.partitionBy("customer_id").orderBy("journey_order")
w_end = Window.partitionBy("customer_id").orderBy(F.col("journey_order").desc())

df_ranked = (
    df
    .withColumn("rn_start", F.row_number().over(w_start))
    .withColumn("rn_end", F.row_number().over(w_end))
)

result = (
    df_ranked
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("rn_start") == 1, F.col("from_city"))).alias("from_city"),
        F.max(F.when(F.col("rn_end") == 1, F.col("to_city"))).alias("to_city")
    )
)

result.show()
'''

# Step 1: get min and max journey per customer
df1 = (
    df.groupBy("customer_id")
    .agg(
        F.min("journey_order").alias("min_journey_id"),
        F.max("journey_order").alias("max_journey_id")
    )
)

# Step 2: join back
final_df = (
    df.join(df1, "customer_id", "inner")
    .groupBy("customer_id")
    .agg(
        F.max(
            F.when(
            F.col("journey_order") == F.col("min_journey_id"),
            F.col("from_city")
            )
        ).alias("start_city"),

        F.max(
            F.when(
            F.col("journey_order") == F.col("max_journey_id"),
            F.col("to_city")
            )
        ).alias("end_city")
    )
)

final_df.show()