import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import ceil
from pyspark.sql.types import LongType

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
)

df.show()

def mean_func(key, pdf):
    return pd.DataFrame([key + (pdf.v.mean(),)])

df.groupby('id').applyInPandas(mean_func, schema="id long, v double").show()

def sum_func(key, pdf):
    return pd.DataFrame([key + (pdf.v.sum(),)])

#df.groupby(df.id, ceil(df.v/2)).applyInPandas(sum_func, schema="id long, v double").show()
df.groupby("id").applyInPandas(sum_func, schema="id long, v double").show()
