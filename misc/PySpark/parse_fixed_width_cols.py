from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, expr

spark = SparkSession.builder.appName("FixedWidthParsing").getOrCreate()

file_path = "data/fixed_width.txt"

lines = spark.read.text(file_path)
lines_padded = lines.withColumn("padded_line", expr("lpad(value, 15, ' ')" ) )


# Define your cols dictionary
cols = {
    "ID": [1, 3],
    "NAME": [4, 13],
    "GENDER": [14, 14]
}

id_start, id_end = cols["ID"]
name_start, name_end = cols["NAME"]
gender_start, gender_end = cols["GENDER"]

# Calculate lengths for substring extraction
id_length = id_end - id_start + 1
name_length = name_end - name_start + 1
gender_length = gender_end - gender_start + 1

# Extract columns using substring
parsed_df = lines_padded.select(
    trim(substring(col("padded_line"), id_start, id_length)).alias("ID"),
    trim(substring(col("padded_line"), name_start, name_length)).alias("NAME"),
    trim(substring(col("padded_line"), gender_start, gender_length)).alias("GENDER")
)

parsed_df.show(truncate=False)

# Save as Parquet
parsed_df.write.mode("overwrite").parquet("data/fixed_width/parquet_files")