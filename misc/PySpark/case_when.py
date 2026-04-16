from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CaseWhenExamples").getOrCreate()

# Sample dataset for case-when examples
data = [
    (1, "Alice", 23, "North", 2500.0, None),
    (2, "Bob", 34, "South", 1800.0, "2024-01-10"),
    (3, "Carol", 29, "East", 3200.0, "2024-03-15"),
    (4, "David", 42, "West", 1200.0, None),
    (5, "Eva", 19, "North", 2100.0, "2024-02-05"),
]

columns = ["id", "name", "age", "region", "sales", "last_order_date"]
df = spark.createDataFrame(data, columns)

# Print the original dataset
print("Original dataset")
df.show(truncate=False)

# Example 1: Basic when/otherwise
# Assign an age group based on whether age is less than 25.
df1 = df.withColumn(
    "age_group",
    F.when(F.col("age") < 25, "young").otherwise("adult")
)

print("\nExample 1: Basic when/otherwise")
df1.select("name", "age", "age_group").show(truncate=False)

# Example 2: Chained when conditions
# Assign a category based on multiple age ranges.
df2 = df.withColumn(
    "age_category",
    F.when(F.col("age") < 25, "under_25")
     .when((F.col("age") >= 25) & (F.col("age") < 35), "25_to_34")
     .when((F.col("age") >= 35) & (F.col("age") < 45), "35_to_44")
     .otherwise("45_plus")
)

print("\nExample 2: Chained when conditions")
df2.select("name", "age", "age_category").show(truncate=False)

# Example 3: Logical combinations using & and |
# Decide bonus level based on sales and region.
df3 = df.withColumn(
    "bonus_level",
    F.when((F.col("sales") > 3000) | (F.col("region") == "North"), "high_bonus")
     .when((F.col("sales") >= 2000) & (F.col("region") != "West"), "medium_bonus")
     .otherwise("low_bonus")
)

print("\nExample 3: Logical combinations")
df3.select("name", "region", "sales", "bonus_level").show(truncate=False)

# Example 4: Null-safe handling with isNull / isNotNull
# Mark rows that have a missing last order date.
df4 = df.withColumn(
    "order_status",
    F.when(F.col("last_order_date").isNull(), "no_orders_yet").otherwise("has_orders")
)

#same dataframe with null-safe handling without using when/otherwise
df4_alt = df.withColumn(
    "order_status",
    F.expr("CASE WHEN last_order_date IS NULL THEN 'no_orders_yet' ELSE 'has_orders' END")
)           

#Null handling via dedicating function like coalesce or nvl 
df4_alt2 = df.withColumn(
    "order_status",
    F.coalesce(F.col("last_order_date"), F.lit("no_orders_yet"))
)
#using nvl function to handle nulls       
df4_alt2 = df.withColumn(
    "order_status",
    F.expr("NVL(last_order_date, 'no_orders_yet')") 
)
 

print("\nExample 4: Null-safe handling")
df4.select("name", "last_order_date", "order_status").show(truncate=False)

# Example 5: Reuse the same condition for multiple derived columns
condition = F.col("sales") >= 3000

df5 = df.withColumn(
    "premium_customer",
    F.when(condition, F.lit(True)).otherwise(F.lit(False))
).withColumn(
    "premium_label",
    F.when(condition, "premium").otherwise("standard")
)

print("\nExample 5: Reuse condition in multiple columns")
df5.select("name", "sales", "premium_customer", "premium_label").show(truncate=False)

# Example 6: SQL-style CASE WHEN expression
# Equivalent logic using expr instead of function chaining.
df6 = df.withColumn(
    "sales_level",
    F.expr(
        "CASE "
        "WHEN sales >= 3000 THEN 'top' "
        "WHEN sales >= 2000 THEN 'good' "
        "ELSE 'needs_improvement' "
        "END"
    )
)

print("\nExample 6: SQL-style CASE WHEN")
df6.select("name", "sales", "sales_level").show(truncate=False)

# Stop Spark session when done
spark.stop()
