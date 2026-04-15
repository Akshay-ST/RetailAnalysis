from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, floor, lit, rand

spark = SparkSession.builder.appName("Salt Example").getOrCreate()

num_salt = 5

# Example input DataFrames - replace these with your actual data sources.
df1 = spark.createDataFrame(
    [(1, "A"), (2, "B")],
    ["id", "value"]
)
df2 = spark.createDataFrame(
    [(1, "x"), (1, "y"), (2, "z")],
    ["id", "other"]
)

# Primary salting technique: add a salt to the smaller side and expand the larger side.
df1_salted = df1.withColumn("salt", floor(rand() * num_salt))

salt_df = spark.range(num_salt).withColumnRenamed("id", "salt")
df2_expanded = df2.crossJoin(salt_df)

# Join on the original key plus the salt column.
joined = df1_salted.join(df2_expanded, ["id", "salt"], "inner")

# Alternate approach: create a salted composite key on both sides.
df1_alt = df1.withColumn("salt", floor(rand() * num_salt))

df1_alt = df1_alt.withColumn("salted_key", concat(col("id"), lit("_"), col("salt")))

salt_df = spark.range(num_salt).withColumnRenamed("id", "salt")
df2_alt = df2.crossJoin(salt_df)
df2_alt = df2_alt.withColumn("salted_key", concat(col("id"), lit("_"), col("salt")))

joined_alt = df1_alt.join(df2_alt, "salted_key")


#Alternate
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, rand

spark = SparkSession.builder.appName("Salt Example").getOrCreate()

num_salt = 5

df1 = df1.withColumn("salt", floor(rand() * num_salt))

salt_df = spark.range(num_salt).withColumnRenamed("id","salt")

df2_expanded = df2.crossJoin(salt_df)
"""