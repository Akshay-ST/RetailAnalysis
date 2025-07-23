from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.\
	builder.\
	config("spark.ui.port","0").\
	config("spark.sql.warehouse.dir",f"/user/{username}/warehouse").\
	enableHiveSupport().\
	master('yarn').\
	getOrCreate()
	
print(spark)

