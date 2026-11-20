from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_count").getOrCreate()

location_rdd = spark.sparkContext.textFile("/home/akshay/Documents/Setups/1Projects/RetailAnalysis/misc/PySpark/data/location1.csv")

mapped_rdd = location_rdd.map( lambda x: (x.split(",")[2] , 1 ) )

result = mapped_rdd.reduceByKey(lambda x,y:x+y)

result.collect()

result.stop()