# PySpark Parsing Cheat Sheet

This cheat sheet covers various data parsing methodologies in PySpark, based on different data structures and formats. Each section demonstrates a specific parsing technique with code examples and key differences.

## 1. Parsing Configuration Dictionaries for Multiple File Reads

**Use Case**: Reading multiple files with different formats and options using a configuration list.

**Key Concept**: Loop through a list of dictionaries containing file paths, formats, and options to dynamically read DataFrames.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Configs").getOrCreate()

configs = [
    {"filepath": "data/file1.csv", "format": "csv", "options": {"header": "true"}},
    {"filepath": "data/merge_file1.csv", "format": "csv", "options": {"header": "true", "delimiter": "|"}}
]

dataframes = []
for config in configs:
    filepath = config["filepath"]
    file_format = config["format"]
    options = config.get("options", {})
    
    df_reader = spark.read.format(file_format)
    for key, value in options.items():
        df_reader = df_reader.option(key, value)
    
    df = df_reader.load(filepath)
    dataframes.append(df)
```

**Difference**: Unlike direct `spark.read` calls, this method allows dynamic configuration for multiple files in a loop.

## 2. Converting Dictionary of Dictionaries to DataFrame

**Use Case**: Transforming nested dictionary data into structured DataFrame rows for aggregation.

**Key Concept**: Flatten nested dict structure into list of tuples, then create DataFrame and perform aggregations.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, expr

spark = SparkSession.builder.getOrCreate()

sales_data = {
    "TRX001": {"Region": "East", "Category": "Electronics", "quantity": 2, "price": 1200},
    "TRX002": {"Region": "West", "Category": "Clothing", "quantity": 5, "price": 50},
    # ... more data
}

data = [
    (trx_id, details["Region"], details["Category"], details["quantity"], details["price"])
    for trx_id, details in sales_data.items()
]

input_df = spark.createDataFrame(data, ["trx_id", "Region", "Category", "quantity", "price"])

temp_df = input_df.withColumn("sales", expr("price * quantity"))

agg_df = temp_df.groupBy(col("Region")) \
    .agg(
        sum(col("sales")).alias("total_regional_sales"),
        sum(col("quantity")).alias("total_regional_quantity_sold"),
        round(sum(col("sales")) / sum(col("quantity")), 2).alias("avg_order_value_by_region")
    )
```

**Difference**: Converts in-memory dict to DataFrame vs. reading from external JSON file (see section 4).

## 3. Parsing Fixed-Width Text Files

**Use Case**: Reading fixed-width formatted text files where columns have fixed character positions.

**Key Concept**: Use `substring` function with predefined column positions to extract fields.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, expr

spark = SparkSession.builder.appName("FixedWidthParsing").getOrCreate()

lines = spark.read.text("data/fixed_width.txt")
lines_padded = lines.withColumn("padded_line", expr("lpad(value, 15, ' ')"))

cols = {
    "ID": [1, 3],
    "NAME": [4, 13],
    "GENDER": [14, 14]
}

parsed_df = lines_padded.select(
    trim(substring(col("padded_line"), 1, 3)).alias("ID"),
    trim(substring(col("padded_line"), 4, 10)).alias("NAME"),
    trim(substring(col("padded_line"), 14, 1)).alias("GENDER")
)
```

**Difference**: Unlike delimited files (CSV), fixed-width requires position-based parsing without delimiters.

## 4. Reading JSON File (Dictionary of Dictionaries)

**Use Case**: Reading JSON files containing nested dictionary structures.

**Key Concept**: Load JSON with Python's `json.load()`, then convert to list of dicts for DataFrame creation.

```python
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

with open("data/dict_to_json.json", "r") as f:
    data = json.load(f)

rows = [{"trx_id": k, **v} for k, v in data.items()]
input_df = spark.createDataFrame(rows)
```

**Difference**: External JSON file reading vs. in-memory dict conversion (see section 2). Uses Python JSON parsing before Spark.

## 5. Parsing JSON Strings in DataFrame Columns

**Use Case**: When DataFrame contains JSON strings that need to be parsed into structured columns.

**Key Concept**: Use `from_json` function with defined schema to parse JSON strings.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("JsonParsing").getOrCreate()

data = [("Akshay", '{"street":"123 Main St", "city":"Pune"}'),
        ("Sachin", '{"street":"789 Main St", "city":"Mumbai"}')]

df = spark.createDataFrame(data, ["name", "address"])

json_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True)
])

df_parsed = df.withColumn("address_json", from_json(col("address"), json_schema))

result_df = df_parsed.select(
    col("name"),
    col("address_json.street").alias("street"),
    col("address_json.city").alias("city")
)
```

**Difference**: Parses JSON within existing DataFrame columns vs. reading entire JSON files (see section 4).

## 6. Exploding List of Dictionaries

**Use Case**: When DataFrame contains arrays of dictionaries that need to be flattened into separate rows.

**Key Concept**: Use `explode` to create one row per array element, then access nested fields.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName("Explode").getOrCreate()

data = {
    "order_id": 101,
    "items": [
        {"product_id": 1, "price": 100},
        {"product_id": 2, "price": 200}
    ]
}

df = spark.createDataFrame([data])

exploded_df = df.select(
    col("order_id"),
    explode(col("items")).alias("item")
)

result_df = exploded_df.select(
    col("order_id"),
    col("item.product_id"),
    col("item.price")
)
```

**Difference**: Explodes complex objects (dicts) vs. simple arrays (see sections 7-8).

## 7. Exploding and Zipping Multiple Arrays

**Use Case**: When DataFrame has parallel arrays that need to be paired element-wise.

**Key Concept**: Use `arrays_zip` to combine corresponding elements from multiple arrays, then explode.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, explode, col

spark = SparkSession.builder.appName("Explode").getOrCreate()

data = [("Akshay", ["English", "Maths"], [60, 70], 'Pass')]
df = spark.createDataFrame(data, ["name", "subj", "score", "result"])

result = df.select(col("name"),
                   explode(arrays_zip(col("subj"), col("score"))).alias("Zipped"),
                   col("result")) \
          .select(col("name"),
                  col("zipped.subj").alias("Subject"),
                  col("zipped.score").alias("Score"),
                  col("result"))
```

**Difference**: Maintains element correspondence vs. cross product explosion (see section 8).

## 8. Exploding Arrays (Cross Product vs. Zip)

**Use Case**: Handling multiple arrays in DataFrame columns.

**Key Concepts**:
- **Cross Product**: Each element from first array pairs with every element from second array
- **Zip**: Corresponding elements from arrays are paired together

```python
# Cross Product (Cartesian product)
df.select(
    "id",
    explode("letters").alias("letter"),
    explode("numbers").alias("number")
).show()

# Zip (element-wise pairing)
df.select(
    "id",
    explode(arrays_zip("letters", "numbers")).alias("zipped")
).select(
    "id",
    col("zipped.letters").alias("letter"),
    col("zipped.numbers").alias("number")
).show()
```

**Difference**: Cross product creates all combinations (m×n rows) vs. zip creates paired rows (max(m,n) rows).

## 9. Working with Map Type Columns

**Use Case**: DataFrames with map columns containing key-value pairs.

**Key Concepts**: 

### Method 1: Filtering Using map_keys and array_contains

Use map functions like `map_keys`, `array_contains` for filtering specific keys in map data. Extract numeric values using regex if needed.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, map_keys, col, lit, count, sum, regexp_extract
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ProductAnalysis").getOrCreate()

# DataFrame with map column
df = spark.createDataFrame([
    (1, "2023-07-07", "300USD", {"p1": 30, "p2": 600, "p3": 50}, "US"),
    (2, "2023-07-06", "400USD", {"p1": 30, "p21": 600, "p35": 50}, "ind"),
    # ...
], ["order_id", "date", "value", "prod_info", "country"])

# Filter rows where map contains specific key
filtered_df = df.filter(array_contains(map_keys(col("prod_info")), lit("p1")))

# Aggregate data for product 'p1', extracting numeric values from strings using regex
result_df = filtered_df.groupBy(F.lit("P1").alias("prod_id")) \
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.regexp_extract(df.value, r'(\d+)', 1).cast("int")).alias("total_value")
    )

print("Method 1 result:")
result_df.show()
```

### Method 2: Exploding Map Columns

Explode the map into key-value pairs (one row per key), then filter and aggregate.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count, sum

spark = SparkSession.builder.appName("ProductAnalysis").getOrCreate()

# DataFrame with map column
df = spark.createDataFrame([
    (1, "2023-07-07", "300USD", {"p1": 30, "p2": 600, "p3": 50}, "US"),
    (2, "2023-07-06", "400USD", {"p1": 30, "p21": 600, "p35": 50}, "ind"),
    # ...
], ["order_id", "date", "value", "prod_info", "country"])

# Explode the prod_info map to have one row per product
exploded_df = df.selectExpr("*", "explode(prod_info) as (prod_id, product_value)")

# Filter for product 'p1'
p1_df = exploded_df.filter(col("prod_id") == "p1")

# Aggregate to get count and sum of values
result = p1_df.groupBy("prod_id").agg(
    count("*").alias("total_count"),
    sum("product_value").alias("total_value")
)

# Format the final output
print("Results:")
result.show()

# Optional: Custom formatting using RDD operations
final_output = result.rdd.map(lambda row: f"P1 | {row['total_count']} | {row['total_value']}").collect()
print("prod_id | total_count | total_value")
for line in final_output:
    print(line)
```

**Differences**:
- **Method 1**: Filters map using key existence; uses `regexp_extract` to parse numeric values from string columns (e.g., "300USD" → 300); better for extracting data from formatted strings
- **Method 2**: Explodes map into rows to access individual key-value pairs directly; better for aggregating values already stored as numbers in the map
- Map columns store key-value pairs vs. struct columns with fixed fields or array columns with ordered values

## Summary of Methodologies

| Method | Input Type | Output | Key Function | Use Case |
|--------|------------|--------|--------------|----------|
| Config Dict | List of dicts | Multiple DataFrames | `spark.read` with loop | Dynamic file reading |
| Dict of Dict | Nested dict | DataFrame rows | `createDataFrame` | In-memory dict conversion |
| Fixed Width | Text file | Structured columns | `substring` | Position-based parsing |
| JSON File | JSON file | DataFrame | `json.load` + `createDataFrame` | External JSON reading |
| JSON String | DataFrame column | Parsed columns | `from_json` | Inline JSON parsing |
| Explode Dict List | Array of dicts | Flattened rows | `explode` | Complex object flattening |
| Array Zip | Multiple arrays | Paired elements | `arrays_zip` + `explode` | Element-wise pairing |
| Array Cross Product | Multiple arrays | All combinations | Multiple `explode` | Cartesian product |
| Map Filter | Map column | Filtered rows | `map_keys`, `array_contains` | Key existence checks |
| Map Explode | Map column | Key-value rows | `explode` on map | Detailed aggregations |

## Key Differences and When to Use

- **File vs. In-Memory**: Use config dicts or JSON file reading for external data; use direct dict conversion for in-memory data
- **Structured vs. Unstructured**: Fixed-width for position-based data; JSON for hierarchical data
- **Pairing Strategy**: Use `arrays_zip` when elements correspond; use cross product when all combinations needed
- **Single vs. Multiple**: JSON strings for per-row parsing; explode methods for array expansion
- **Data Types**: Maps for key-value lookup; structs for fixed schema; arrays for ordered collections
- **Map Operations**: Use `map_keys`/`array_contains` for simple filtering; use `explode` for detailed aggregations and value extraction</content>
<parameter name="filePath">/home/akshay/Documents/Setups/1Projects/RetailAnalysis/misc/PySpark/Parsing_cheet_sheet_README.md