"""
add new col = sales = quantity * price

aggregate by region and category, 
sum(sales) as total_sales
sum(quantity) as total_quantity
avg order value = sum(sales) / sum(quantity)


This is not a standard JSON parsing problem, 
but rather a transformation of a nested dictionary into a 
structured format (like a DataFrame) and then performing some aggregations on it.

correct json syntax is:
{"trx_id": "TRX001", "Region": "East", "Category": "Electronics", "quantity": 2, "price": 1200}
{"trx_id": "TRX002", "Region": "West", "Category": "Clothing", "quantity": 5, "price": 50}

sales_data = {
    "TRX001" : {"Region": "East", "Category": "Electronics", "quantity": 2, "price": 1200},
    "TRX002" : {"Region": "West", "Category": "Clothing", "quantity": 5, "price": 50},
    "TRX003" : {"Region": "North", "Category": "Home", "quantity": 1, "price": 300},
    "TRX004" : {"Region": "South", "Category": "Books", "quantity": 3, "price": 20},
    "TRX005" : {"Region": "East", "Category": "Clothing", "quantity": 4, "price": 80},
    "TRX006" : {"Region": "West", "Category": "Electronics", "quantity": 1, "price": 1500},
    "TRX007" : {"Region": "North", "Category": "Books", "quantity": 2, "price": 15},
    "TRX008" : {"Region": "South", "Category": "Home", "quantity": 3, "price": 250},
    "TRX009" : {"Region": "East", "Category": "Books", "quantity": 1, "price": 10},
    "TRX010" : {"Region": "West", "Category": "Clothing", "quantity": 6, "price": 60}
}

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# Step 1: Read file using Python
import json
with open("data/dict_to_json.json", "r") as f:
    data = json.load(f)

# Step 2: Convert dict → list of rows
rows = [
    {"trx_id": k, **v}
    for k, v in data.items()
]
print("Rows: ")
print(rows)
"""
Rows: 
[{'trx_id': 'TRX001', 'Region': 'East', 'Category': 'Electronics', 'quantity': 2, 'price': 1200}, 
{'trx_id': 'TRX002', 'Region': 'West', 'Category': 'Clothing', 'quantity': 5, 'price': 50},....]
"""

# Step 3: Create DataFrame
input_df = spark.createDataFrame(rows)

print("Input DataFrame: ")
input_df.show()

from pyspark.sql.functions import col, avg, sum, expr, round

temp_df = input_df.withColumn("sales", expr("price * quantity"))

print("DataFrame with Sales Column: ")
temp_df.show()

agg_region_level_df = temp_df.groupBy(col("region")) \
                                .agg(
                                    sum(col("sales")).alias("total_regional_sales"),
                                    sum(col("quantity")).alias("total_regional_quantity_sold"),
                                    round( sum(col("sales")) / sum(col("quantity")), 2 ).alias("avg_order_value_by_region")
                                )
print("Aggregated DataFrame at Region Level: ")
agg_region_level_df.show()

agg_category_level_df = temp_df.groupBy(col("category")) \
                                .agg(
                                    sum(col("sales")).alias("total_category_sales"),
                                    sum(col("quantity")).alias("total_category_quantity_sold"),
                                    round( sum(col("sales")) / sum(col("quantity")), 2 ).alias("avg_order_value_by_category")
                                )

print("Aggregated DataFrame at Category Level: ")
agg_category_level_df.show()



spark.stop()