from pyspark.sql import SparkSession

jars = ",".join([
    "jars/hudi-spark3-bundle_2.12-1.0.2.jar",
    "jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
])


spark = SparkSession.builder \
    .appName("Iceberg Example") \
    .config("spark.jars", jars) \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.option("header", True).csv("employee_data.csv")

# Create Iceberg table (using Spark SQL)
spark.sql("use iceberg_db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_db.employee_iceberg (
        id INT,
        name STRING,
        dept STRING,
        salary INT
    )
    USING iceberg
""")

# Insert data
df.writeTo("spark_catalog.iceberg_db.employee_iceberg").using("iceberg").tableProperty("format-version", "2").createOrReplace()

# Query Iceberg table
spark.sql("SELECT * FROM spark_catalog.iceberg_db.employee_iceberg").show()


# Update operation
spark.sql("""
    UPDATE spark_catalog.iceberg_db.employee_iceberg
    SET salary = 90000
    WHERE id = 2
""")
# Query after update
spark.sql("SELECT * FROM spark_catalog.iceberg_db.employee_iceberg").show()


#Iceberg time travel
# Show table history
spark.sql("SELECT * FROM spark_catalog.iceberg_db.employee_iceberg.history").show()

# Query a specific snapshot
spark.sql("""
  SELECT * FROM spark_catalog.iceberg_db.employee_iceberg
  VERSION AS OF 1234567890  -- replace with actual snapshot ID
""").show()
