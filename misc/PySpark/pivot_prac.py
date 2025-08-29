from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("ActivityTable") \
    .getOrCreate()

data = [
            ("IT","A",3000),
            ("IT","B",3000),
            ("HR","C",3000),
            ("HR","D",3000),
            ("FINANCE","E",3000),
            ("FINANCE","F",3000)
]
schema = StructType([
    StructField("dept", StringType(), True),
    StructField("emp", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show()
df.printSchema()

from pyspark.sql.functions import col, sum
df2 = df.groupBy() \
        .pivot("dept") \
        .agg(sum("salary").alias("Total_salary"))

#df2.show()

df3 = df.groupBy("dept") \
        .pivot("emp") \
        .agg(sum("salary").alias("Total_salary"))

df3.show()