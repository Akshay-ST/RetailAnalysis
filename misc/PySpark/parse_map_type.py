"""
-------------------------------------------------------------------

order_id | date | value	| prod_info | country  |

----------------------------------------------------------------- 

1        |2023-07-07 |300USD |{p1:30,p2:600,p3:50} 	|	US    |

2        |2023-07-06 |400USD |{p1:30,p21:600,p35:50}	|	ind   |

3        |2023-07-05 |500USD |{p13:30,p2:600,p37:50}	|	china |

4        |2023-07-04 |600USD |{p12:30,p23:600,p35:50}	|	canada|

-------------------------------------------------------------------

O/p: Find the following details about product 1. 

-----------------------------------

prod_id| total_count| total_value |

-----------------------------------

P1	   | 2			|60|
-----------------------------------

"""



# map keys / map values
#method 1
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, lit, map_keys, sum, count, expr, col


# Initialize Spark session
spark = SparkSession.builder.appName("ProductAnalysis").getOrCreate()

# Sample data as per your example
data = [
    (1, "2023-07-07", "300USD", {"p1": 30, "p2": 600, "p3": 50}, "US"),
    (2, "2023-07-06", "400USD", {"p1": 30, "p21": 600, "p35": 50}, "ind"),
    (3, "2023-07-05", "500USD", {"p13": 30, "p2": 600, "p37": 50}, "china"),
    (4, "2023-07-04", "600USD", {"p12": 30, "p23": 600, "p35": 50}, "canada")
]

df = spark.createDataFrame(data, ["order_id", "date", "value", "prod_info", "country"])

# Show initial DataFrame
print("Initial DataFrame:")
df.show(truncate=False)

# Assuming your DataFrame is named `df`
# Filter rows where prod_info contains 'p1'
filtered_df = df.filter(array_contains(map_keys(col("prod_info")), lit("p1")))

#filtered_df = df.filter(expr("array_contains(map_keys(prod_info), 'p1')"))

# Aggregating data for product 'p1'
result_df = filtered_df.groupBy(F.lit("P1").alias("prod_id")) \
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.regexp_extract(df.value, r'(\d+)', 1).cast("int")).alias("total_value")
    )

# Select and display results
print("method 1 result:")
result_df.show()




#method2
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType


# Sample data as per your example
data = [
    (1, "2023-07-07", "300USD", {"p1": 30, "p2": 600, "p3": 50}, "US"),
    (2, "2023-07-06", "400USD", {"p1": 30, "p21": 600, "p35": 50}, "ind"),
    (3, "2023-07-05", "500USD", {"p13": 30, "p2": 600, "p37": 50}, "china"),
    (4, "2023-07-04", "600USD", {"p12": 30, "p23": 600, "p35": 50}, "canada")
]

# Defining schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("value", StringType(), True),
    StructField("prod_info", MapType(StringType(), IntegerType()), True),
    StructField("country", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)


# Filter rows where product info contains 'p1'
from pyspark.sql.functions import explode

# Explode the prod_info map to have one row per product
exploded_df = df.selectExpr("*", "explode(prod_info) as (prod_id, product_value)")

# Filter for product 'p1'
p1_df = exploded_df.filter(col("prod_id") == "p1")

# Aggregate to get count and sum of values
result = p1_df.groupBy("prod_id").agg(
    count("*").alias("total_count"),
    sum("product_value").alias("total_value")
)

# Format the final output
print("method 2 result:")
result.show()

# Optional: To present in the exact format as in your output:
final_output = result.rdd.map(lambda row: f"P1 | {row['total_count']} | {row['total_value']}").collect()
print("Formatted output:")
print("prod_id | total_count | total_value")
for line in final_output:
    print(line)

