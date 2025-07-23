from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
        .appName("Dept Avg Exp") \
        .getOrCreate()

# Define the schema for the Project table
project_schema = StructType([
StructField("project_id", IntegerType(), True),
StructField("employee_id", IntegerType(), True)
])

# Define the data for the Project table
project_data = [(1, 1), (1, 2), (1, 3), (2, 1), (2,
4)]

# Define the schema for the Employee table
employee_schema = StructType([
StructField("employee_id", IntegerType(), True),
StructField("name", StringType(), True),
StructField("experience_years", IntegerType(),
True)
])

# Define the data for the Employee table
employee_data = [(1, "Khaled", 3), (2, "Ali", 2), (3,
"John", 1), (4, "Doe", 2)]

project_df = spark.createDataFrame(project_data, schema = project_schema)
emp_df = spark.createDataFrame(employee_data, schema=employee_schema)

joined_df = project_df.join(emp_df,"employee_id")

from pyspark.sql.functions import avg, round

result_df = joined_df.groupBy("project_id") \
                     .agg( round(avg("experience_years"), 2)  \
                            .alias("average_exp_years"))

result_df.show()

