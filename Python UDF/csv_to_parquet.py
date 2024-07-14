from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName("CSV TO PARQuet") \
            .master("local") \
            .getOrCreate()
    
    students_df = spark.read \
                  .format("csv") \
                  .option("inferSchema", True) \
                  .option("header", True) \
                  .option("path","/home/akshay/Documents/#BIG DATA/weekly Practise/Assignments/w30-dataset/students_with_header.csv") \
                  .load()
    
    students_df.write \
    .partitionBy("year","subject") \
    .parquet("/home/akshay/Documents/#BIG DATA/weekly Practise/Assignments/w30-dataset/students_parquet")
