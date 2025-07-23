from pyspark.sql import SparkSession

# Define your JARs
jars = ",".join([
    "/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/iceberg-hudi/jars/hudi-spark3-bundle_2.12-1.0.2.jar",
    "/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/iceberg-hudi/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
])

# SparkSession with Hive + Iceberg catalog
#.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
spark = SparkSession.builder \
    .appName("Iceberg with Hive Catalog") \
    .config("spark.jars", jars) \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/iceberg-hudi/employee_data.csv")
df.show(truncate=False)

# Create Iceberg table in default Hive DB
#spark.sql("create database if not exists iceberg_db")
#spark.sql("use iceberg_db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_catalog.default.employee_iceberg (
        id INT,
        name STRING,
        dept STRING,
        salary INT
    )
    USING iceberg
""")

# Insert data into the Iceberg table
df.writeTo("spark_catalog.default.employee_iceberg").append()

# Query the table
spark.sql("SELECT * FROM spark_catalog.default.employee_iceberg").show()
