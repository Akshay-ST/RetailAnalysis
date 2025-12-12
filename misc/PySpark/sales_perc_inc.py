from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Example input DataFrame
data = [
    ("Jan", 2024, 1000),
    ("Feb", 2024, 1200),
    ("Mar", 2024, 1500),
    ("Apr", 2024, 1400),
]
columns = ["Month", "Year", "sales"]
df = spark.createDataFrame(data, columns)

df.show()

# If Month is a number string like "01", cast to int; if it is name ("Jan"), map it to number
month_order_expr = F.when(F.col("Month") == "Jan", 1) \
    .when(F.col("Month") == "Feb", 2) \
    .when(F.col("Month") == "Mar", 3) \
    .when(F.col("Month") == "Apr", 4) \
    .when(F.col("Month") == "May", 5) \
    .when(F.col("Month") == "Jun", 6) \
    .when(F.col("Month") == "Jul", 7) \
    .when(F.col("Month") == "Aug", 8) \
    .when(F.col("Month") == "Sep", 9) \
    .when(F.col("Month") == "Oct", 10) \
    .when(F.col("Month") == "Nov", 11) \
    .when(F.col("Month") == "Dec", 12)

print(month_order_expr)

# Define window by year and ordered by month
w = Window.partitionBy("Year").orderBy(month_order_expr)

result = (
    df
    .withColumn("prev_month_sales", F.lag("sales").over(w))
    .withColumn("next_month_sales", F.lead("sales").over(w))
    .withColumn(
        "pct_growth_from_prev",
        F.when(
            F.col("prev_month_sales").isNotNull() & (F.col("prev_month_sales") != 0),
            (F.col("sales") - F.col("prev_month_sales")) * 100.0 / F.col("prev_month_sales")
        )
    )
)

result.show()
