import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf,col
from pyspark.sql.types import LongType

spark = SparkSession.builder.getOrCreate()

def multiply_func(a,b):
    return a * b

multiply = pandas_udf(multiply_func, returnType=LongType())

x = pd.Series([1,2,3])
print(multiply_func(x,x))

df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

df.select(multiply(col("x"), col("x"))).show()
