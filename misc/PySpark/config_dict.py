from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Configs") \
    .getOrCreate()

configs = [
{"filepath": "data/file1.csv", "format": "csv", "options": {"header": "true"}},
{"filepath": "data/merge_file1.csv", "format": "csv", "options": {"header": "true", "delimiter": "|"}} 
]

# Initialize an empty list to hold the DataFrames
dataframes = []

# Loop through each configuration in the configs list
for config in configs:
    # Access the filepath and format
    filepath = config["filepath"]
    file_format = config["format"]
    options = config.get("options", {})
    
    # Start reading with spark.read
    df_reader = spark.read.format(file_format)
    
    # Add options to the reader
    for key, value in options.items():
        df_reader = df_reader.option(key, value)
    
    # Read the file
    df = df_reader.load(filepath)
    
    # Append the DataFrame to the list
    dataframes.append(df)

# If needed, you can union all DataFrames (assuming schema is compatible)
# combined_df = reduce(lambda df1, df2: df1.union(df2), dataframes)

for index, df in enumerate(dataframes):
    print(f"DataFrame {index + 1}:")
    df.show()