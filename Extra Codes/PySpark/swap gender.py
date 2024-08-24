from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import when

spark = SparkSession.builder \
        .appName("Swap Gender") \
        .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True),
StructField("sex", StringType(), True),
StructField("salary", IntegerType(), True),
])
# Define the data
data = [
(1, "A", "m", 2500),
(2, "B", "f", 1500),
(3, "C", "m", 5500),
(4, "D", "f", 500),
]

df = spark.createDataFrame(data, schema=schema)
df.show()

gender_swapped = df.withColumn('sex', when(df['sex'] == 'm','f').otherwise('m'))
gender_swapped.show()