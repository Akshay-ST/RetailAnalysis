from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySQLConnection") \
    .config("spark.jars", "/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/spark-warehouse/mysql-connector-j-9.4.0.jar") \
    .master("local[*]") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .config("spark.hadoop.fs.AbstractFileSystem.hdfs.impl","org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl","org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://localhost:3306/Akshay"
table_name = "Employee"
properties = {
    "user": "root",
    "password": "123456789",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

df.show()

spark.stop()