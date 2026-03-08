from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, coalesce


spark = SparkSession \
    .builder \
    .appName("coalesce") \
    .getOrCreate()

data = [
    (None,    None,    "Pune"),
    (None,    " ",     "delhi"),
    (" ",     "mumbai", "banglore")
]

df = spark.createDataFrame(data, ["city1", "city2", "city3"])
df.show()

res = df.withColumn( "city",
        coalesce(
            when( col("city1") != " " , col("city1") ),
            when( col("city2") != " " , col("city2") ),
            when( col("city3") != " " , col("city3") )
        )
    ).drop("city1", "city2", "city3")
res.show()

spark.stop()