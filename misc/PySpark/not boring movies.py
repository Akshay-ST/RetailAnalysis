from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder \
        .appName("Not Boring Movies") \
        .getOrCreate()


# Define the schema for the DataFrame
schema = StructType([
StructField("id", IntegerType(), True),
StructField("movie", StringType(), True),
StructField("description", StringType(), True),
StructField("rating", FloatType(), True)
])

# Your data
data = [
(1, "War", "great 3D", 8.9),
(2, "Science", "fiction", 8.5),
(3, "Irish", "boring", 6.2),
(4, "Ice song", "Fantasy", 8.6),
(5, "House card", "Interesting", 9.1)
]

df = spark.createDataFrame(data, schema=schema)
df.show()

odd_df = df.filter(col('id') % 2 == 1)
odd_df.show()

odd_df_filtered = odd_df.filter(col('description') != 'boring')
odd_df_filtered.show()