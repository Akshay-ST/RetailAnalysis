"""
1. Load the following sample dataset into a PySpark DataFrame:
 
2. Perform the following operations:
 
a. Replace all NULL values in the Quantity column with 0.
 
b. Replace all NULL values in the Price column with the average price of the existing data.

c. Fill missing Sales_Date with a default value of '2025-01-01'.

d. Drop rows where the Product column is NULL.
 
e. Drop rows where all columns are NULL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col,when,avg, lit


columns = ["Sales_ID", "Product", "Quantity", "Price", "Region", "Sales_Date"]
data = [(1, "Laptop", 10, 50000, "North", "2025-01-01"),
 
        (2, "Mobile", None, 15000, "South", None),          
 
	    (3, "Tablet", 20, None, "West", "2025-01-03"),
 
        (4, "Desktop", 15, 30000, None, "2025-01-04"),
 
        (5, None, None, None, "East", "2025-01-05") ]
 

spark = SparkSession.builder.appName("Handle-NA").getOrCreate()
 
input_df =  spark.createDataFrame(data,columns)
input_df.show()
 
avg_price = input_df.select(avg(col("Price")).alias("avg_price")).collect()[0]["avg_price"]

nulls_removed_df = (
    input_df
    .withColumn("Quantity", when(col("Quantity").isNull(), 0).otherwise(col("Quantity")))
    .withColumn("Price", when(col("Price").isNull(), round(avg_price, 0) ).otherwise(col("Price")))
    .withColumn("Sales_Date", when(col("Sales_Date").isNull(), lit("2025-01-01")).otherwise(col("Sales_Date")))
)

null_rows_removed_df = (
    nulls_removed_df
    .filter(col("Product").isNotNull())
    .na.drop(how="all")
)

null_rows_removed_df.show()

#-------------------Alternatively
df = spark.createDataFrame(data, columns)

# 1) Replace NULL in Quantity with 0
df = df.fillna({"Quantity": 0})

# 2) Replace NULL in Price with average price of existing data
avg_price = df.select(avg(col("Price")).alias("avg_price")).collect()[0]["avg_price"]
df = df.fillna({"Price": avg_price})

# 3) Fill missing Sales_Date with default '2025-01-01'
df = df.fillna({"Sales_Date": "2025-01-01"})

# 4) Drop rows where Product is NULL
df = df.filter(col("Product").isNotNull())

# 5) Drop rows where all columns are NULL
df = df.na.drop(how="all")

df.show()

spark.stop()