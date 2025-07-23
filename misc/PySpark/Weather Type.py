from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, when

spark = SparkSession.builder \
        .appName("Weather Type") \
        .getOrCreate()

 # Define the schema for the Weather DataFrame
weather_schema = StructType([
 StructField("country_id", IntegerType(), True),
 StructField("weather_state", IntegerType(), True),
 StructField("day", StringType(), True)
])
 
# Weather data
weather_data = [
 (2, 15, "2019-11-01"),
 (2, 12, "2019-10-28"),
 (2, 12, "2019-10-27"),
 (3, -2, "2019-11-10"),
 (3, 0, "2019-11-11"),
 (3, 3, "2019-11-12"),
 (5, 16, "2019-11-07"),
 (5, 18, "2019-11-09"),
 (5, 21, "2019-11-23"),
 (7, 25, "2019-11-28"),
 (7, 22, "2019-12-01"),
 (7, 20, "2019-12-02"),
 (8, 25, "2019-11-05"),
 (8, 27, "2019-11-15"),
 (8, 31, "2019-11-25"),
 (9, 7, "2019-10-23"),
 (9, 3, "2019-12-23")
]

countries_schema = StructType([
StructField("country_id", IntegerType(), True),
StructField("country_name", StringType(), True)
])

countries_data = [
(2, "USA"),
(3, "Australia"),
(7, "Peru"),
(5, "China"),
(8, "Morocco"),
(9, "Spain")
]

weather_df = spark.createDataFrame(weather_data,weather_schema)
weather_df.show()

countries_df = spark.createDataFrame(countries_data,countries_schema)
countries_df.show()

nov_weather_df = weather_df.filter( col('day').like('%-11-%'))
nov_weather_df.show()

joined_df = nov_weather_df.join(countries_df, "country_id", "inner") \
        .drop("country_id")

joined_df.show()

avg_df = joined_df.groupBy('country_name').agg(avg('weather_state').alias('avg_weather'))

result_df = avg_df.withColumn('Weather_type', when( col('avg_weather') <= 15, 'cold')
                                              .when( col('avg_weather') >= 25, 'hot')
                                              .otherwise('warm')).drop('avg_weather').sort('country_name')

result_df.show()
