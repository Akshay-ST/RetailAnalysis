from pyspark.sql import SparkSession

jars = ",".join([
    "jars/hudi-spark3-bundle_2.12-1.0.2.jar",
    "jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
])


spark = SparkSession.builder \
    .appName("Hudi Example") \
    .config("spark.jars", jars) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

df = spark.read.option("header", True).csv("employee_data.csv")

hudi_table_path = "file:///tmp/hudi_employee_table"

df.write.format("hudi") \
    .option("hoodie.table.name", "employee_hudi") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "salary") \
    .option("hoodie.datasource.write.operation", "insert") \
    .mode("overwrite") \
    .save(hudi_table_path)

# Query
spark.read.format("hudi").load(hudi_table_path).show()


#Update operation
update_df = spark.createDataFrame([
    (2, "Bob", "Engineering", 90000)
], ["id", "name", "dept", "salary"])

update_df.write.format("hudi") \
    .option("hoodie.table.name", "employee_hudi") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "salary") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save(hudi_table_path)


#time travel
# For Hudi time travel, use `as.of.instant` option
spark.read.format("hudi") \
    .option("as.of.instant", "20250711123000") \
    .load(hudi_table_path) \
    .show()
