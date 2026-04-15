"""
Transactions table:

+ — — — + — — — — -+ — — — — — + — — — — + — — — 

| id | dept   	   | state    | amount | date |

+ — — — + — — — — -+ — — — — — + — — — — + — — — 

| 1  | IT 	   | approved | 1000   | 2018–12–18 |

| 2  | IT 	   | declined | 2000   | 2018–12–19 |

| 3  | IT 	   | approved | 2000   | 2019–01–01 |

| 4  | Fin     	   | approved | 2000   | 2019–01–07 |

+ — — — + — — — — -+ — — — — — + — — — — + — — —
 
 
Output:

+ — — — — — + — — — — -+ — — — — — — -+ — — — — — — — — + — — — — — — — — — — + — — — — — — — — — — — --------------+

| month   | dept      | trans_count    | approved_trans_cnt		 | total_amount    | approved_total_amount |

+ — — — — — + — — — — -+ — — — — — — -+ — — — — — — — — + — — — — — — — — — — + — — — — — — — — — — -------------— -+

| 2018–12 | IT 		  | 2			| 1 			| 3000 			  	 | 1000	|

| 2019–01 | IT 		  | 1 			| 1 			| 2000 			 	 | 2000	|

| 2019–01 | Fin		  | 1 			| 1 			| 2000 			 	 | 2000 |

+ — — — — — + — — — — -+ — — — — — — -+ — — — — — — — — + — — — — — — — — — — + — — — — — — — — — — — --------------+

"""
"""

from pyspark.sql import SparkSession
input_df = spark.createDataFrame(data, ["id", "dept", "state", "amount", "date" ])


from pyspark.sql.functions import expr, col

input_df_processed = input_df.withColumn("date", substring("date",0,7)) \
				.withColumnRenamed("date", "month") \
				.withColumn("approved_flag", when(col("state") == 'approved, 1).otherwise(0)


output_df = input_processed_df.groupBy("month","dept") \
				.agg(
				count("id").alias("trans_count"),
				count(expr("CASE WHEN approved_flag == 1 then id")).alias("approved_trans_count"),
				sum("amount").alias("total_amount"),
				sum(expr("CASE WHEN approved_flag == 1 then amount")).alias("approved_total_amount")
				) \
				.sort("month","dept")

output_df.show()	

------------------------------------------
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum, count

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionAnalysis").getOrCreate()

# Sample input DataFrame
data = [
    (1, 'IT', 'approved', 1000, '2018-12-18'),
    (2, 'IT', 'declined', 2000, '2018-12-19'),
    (3, 'IT', 'approved', 2000, '2019-01-01'),
    (4, 'Fin', 'approved', 2000, '2019-01-07')
]

columns = ['id', 'dept', 'state', 'amount', 'date']

transactions_df = spark.createDataFrame(data, columns)

# Extract year-month from date
transactions_df = transactions_df.withColumn(
    'month',
    substring(col('date'), 1, 7)
)

# Filter only relevant statuses if needed
# but here, we consider all transactions for counts and filter within aggregations

# Create a flag for approved transactions
transactions_df = transactions_df \
    .withColumn('is_approved',(col('state') == 'approved').cast('int'))

# Perform aggregations
result_df = transactions_df.groupBy('month', 'dept') \
    .agg(
        count('*').alias('trans_count'),
        sum('is_approved').alias('approved_trans_cnt'),
        sum('amount').alias('total_amount'),
        sum(col('amount') * col('is_approved')).alias('approved_total_amount')
    )

# Show result
result_df.show(truncate=False)
 