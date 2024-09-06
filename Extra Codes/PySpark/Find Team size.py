from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
        .appName("Find Team Size") \
        .getOrCreate()

# Employee-Team data
data = [
(1, 8),
(2, 8),
(3, 8),
(4, 7),
(5, 9),
(6, 9)
]

# Define schema using StructType
schema = StructType([
    StructField("employee_id", IntegerType(),True),
    StructField("team_id", IntegerType(),True)
])

from pyspark.sql.functions import count

df = spark.createDataFrame(data,schema=schema)
df.show()

df_agg = df.groupBy("team_id") \
           .agg(count("team_id").alias("team_size"))
           
df_agg.show()

df_result = df.join(df_agg, "team_id", "inner") \
              .select("employee_id","team_size")
df_result.show()