from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
        .appName("Combine_files") \
        .master("local[*]") \
        .getOrCreate()

# Read File 1: Name, gender (pipe-delimited)
df1 = (
    spark.read
         .schema("name STRING, gender STRING")  # explicitly define schema 
         .option("header", "true")      # if first row has column names
         .option("delimiter", "|")      # pipe delimiter 
         .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/merge_file1.csv")
)

df1.show()

# Read File 2: Name, gender, age (pipe-delimited)
df2 = (
    spark.read
         .schema("name STRING, gender STRING, age INTEGER")  # explicitly define schema 
         .option("header", "true")
         .option("delimiter", "|")
         .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/merge_file2.csv")
)

df2.show()

# Union with different schemas (allow missing columns)
combined_df = df1.unionByName(df2, allowMissingColumns=True)  # [web:21][web:25][web:33]

combined_df.show()

# Write to single tilde-delimited file
(
    combined_df
    .coalesce(1)  # single output file
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", "~")          # tilde delimiter [web:26][web:35]
    .csv("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/PySpark/data/combined_tilde_v1")
)
