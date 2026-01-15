from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
        .builder \
        .appName('Filter') \
        .getOrCreate()

data = [('Akshay',28,'Pune'),('Divya',27,'Pune'),('Neeta',52,'Bhopal')]
columns = ['Name','Age','City']

user_df = spark.createDataFrame(data, columns)
user_df.show()
user_df.printSchema()

result_df = user_df.filter(col('Age')>30)

result_df.show()

spark.stop()