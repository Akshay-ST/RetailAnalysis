import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    [(1, 400), (2, 300)], ("id", "age")
)

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.id == 1]

df.mapInPandas(filter_func, schema=df.schema).show()