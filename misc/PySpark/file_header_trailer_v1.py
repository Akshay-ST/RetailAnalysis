from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Read input CSV (assuming comma delimiter, header=true)
df = (
    spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("misc/PySpark/data/file1.csv")
)

# File 1: Only header (empty data with header)
df.coalesce(1).write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v1/header_only")

# File 2: Only data rows (exclude header/trailer)
data_rows = df.filter(~F.col("Header").isin(["Trailer"]))
data_rows.coalesce(1).write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v1/data_only")

# File 3: Last 3 lines (using row_number)
w = Window.orderBy(F.monotonically_increasing_id())
last_3_rows = df.filter(F.row_number().over(w).isin([df.count()-2, df.count()-1, df.count()]))
last_3_rows.coalesce(1).write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/fht_v1/last_3_lines")

spark.stop()
