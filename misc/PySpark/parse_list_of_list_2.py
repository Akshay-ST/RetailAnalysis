from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, explode, col

spark = SparkSession.builder \
        .appName("Explode") \
        .getOrCreate()

data = [("Akshay", ["English", "Maths"], [60,70], 'Pass')]
df = spark.createDataFrame(data, ["name", "subj", "score", "result"])
df.show()

"""
+------+----------------+--------+------+
|  name|            subj|   score|result|
+------+----------------+--------+------+
|Akshay|[English, Maths]|[60, 70]|  Pass|
+------+----------------+--------+------+
"""

result = df.select(col("name"),
                   explode(arrays_zip(col("subj"),col("score"))).alias("Zipped"),
                   col("result")
                   ) \
            .select(col("name"),
                    col("zipped.subj").alias("Subject"),
                    col("zipped.score").alias("Score"),
                    col("result")
            )

result.show()

"""
+------+-------+-----+------+
|  name|Subject|Score|result|
+------+-------+-----+------+
|Akshay|English|   60|  Pass|
|Akshay|  Maths|   70|  Pass|
+------+-------+-----+------+
"""