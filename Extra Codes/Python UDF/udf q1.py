import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
)


@pandas_udf("double")
def mean_udf(v: pd.Series) -> float:
    return v.mean()


df.select(mean_udf(df['v'])).show()

df.groupby("id").agg(mean_udf(df["v"])).show()

w = Window \
    .partitionBy('id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()
