from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('filter').getOrCreate()

Data =[("Alice", 30, "Hyd"), ("Bob", 35, "Pune"), ("Charlie",40,"Indore")]
 
columns = ["Name", "Age", "City"]

user_df = spark.createDataFrame(Data, columns)
user_df.show()
user_df.printSchema()

from pyspark.sql.functions import col,expr

result_df = user_df.filter(col('Age') > 30)

result_df.show()

spark.stop()