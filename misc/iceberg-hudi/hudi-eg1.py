from pyspark.sql import SparkSession

# Path to your downloaded Hudi JAR
jars = ",".join([
    "jars/hudi-spark3-bundle_2.12-1.0.2.jar",
    "jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
])

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Hudi with Hive Catalog") \
    .config("spark.jars", jars) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .enableHiveSupport() \
    .getOrCreate()

# Load sample CSV
df = spark.read.option("header", True).csv("employee_data.csv")

# Target location for Hudi table on disk (local path)
hudi_table_path = "file:///tmp/hudi/hudi_employee_table"
table_name = "default.hudi_employee"

# Write to Hudi table
df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_employee") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "salary") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.hive_sync.enable", "true") \
    .option("hoodie.datasource.hive_sync.table", "hudi_employee") \
    .option("hoodie.datasource.hive_sync.database", "default") \
    .option("hoodie.datasource.hive_sync.mode", "hms") \
    .option("hoodie.datasource.hive_sync.support_timestamp", "true") \
    .option("hoodie.datasource.hive_sync.use_jdbc", "false") \
    .option("path", hudi_table_path) \
    .mode("overwrite") \
    .save()

# Query using Hive catalog (if sync was successful)
spark.sql("SELECT * FROM default.hudi_employee").show()
