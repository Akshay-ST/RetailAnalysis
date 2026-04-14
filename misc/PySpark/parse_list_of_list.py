
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, explode, col

spark = SparkSession.builder \
        .appName("Explode") \
        .getOrCreate()

data = [("A", ["x", "y"], [1, 2])]
df = spark.createDataFrame(data, ["id", "letters", "numbers"])
df.show()

"""
+---+-------+-------+
| id|letters|numbers|
+---+-------+-------+
|  A| [x, y]| [1, 2]|
+---+-------+-------+
"""

df.select(
    "id",
    explode("letters").alias("letter"),
    explode("numbers").alias("number")
).show()

"""
Cross Product
+---+------+------+
| id|letter|number|
+---+------+------+
|  A|     x|     1|
|  A|     x|     2|
|  A|     y|     1|
|  A|     y|     2|
+---+------+------+
"""

df.select(
    "id",
    explode(arrays_zip("letters", "numbers")).alias("zipped")
).show() 

"""
+---+------+
| id|zipped|
+---+------+
|  A|{x, 1}|
|  A|{y, 2}|
+---+------+
"""

df.select(
    "id",
    explode(arrays_zip("letters", "numbers")).alias("zipped")
).select(
    "id",
    col("zipped.letters").alias("letter"),
    col("zipped.numbers").alias("number")
).show(truncate=False)

#or
df.selectExpr(
    "id",
    "explode(arrays_zip(letters, numbers)) as zipped"
).selectExpr(
    "id",
    "zipped.letters as letter",
    "zipped.numbers as number"
).show()



#or
df.selectExpr(
    "id",
    "explode(arrays_zip(letters, numbers)) as t"
).select(
    "id",
    col("t.letters").alias("letter"),
    col("t.numbers").alias("number")
).show()

#or
df.selectExpr(
    "id", 
    "explode(arrays_zip(letters, numbers)) as t") \
  .select(
      "id", 
      "t.*") \
  .show()

#or
df.select(
    "id",
    explode(
        arrays_zip(
            col("letters").alias("letter"),
            col("numbers").alias("number")
        )
    ).alias("pair")
).select(
    "id",
    col("pair.letter"),
    col("pair.number")
).show(truncate=False)

#or
df.selectExpr(
    "id",
    "inline(arrays_zip(letters, numbers))"
).show()

"""
+---+------+------+
|id |letter|number|
+---+------+------+
|A  |x     |1     |
|A  |y     |2     |
+---+------+------+
"""