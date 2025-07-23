from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check Hive") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# List existing databases (if Hive support is working)
spark.sql("SHOW DATABASES").show()
