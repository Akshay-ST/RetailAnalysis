from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()


DATE_FORMAT = "yyyy-MM-dd"
future_date = "9999-12-31"
source_url = "/user/itv005857/scd_demo/source"
destination_url = "/user/itv005857/scd_demo/target"
primary_key = ["customerid"]
slowly_changing_cols = [ "email","phone","address", "city", "state", "zipcode"]
implementation_cols = ["effective_date","end_date","active_flag"]


customers_source_schema = "customerid long,firstname string, lastname string, email string, phone string, address string, city string, state string, zipcode long"

customers_target_schema = ("""customerid long,firstname string, lastname string, email string, phone string,
                           address string, city string, state string, zipcode long, customer_skey long, 
                           effective_date date, end_date date, active_flag boolean""")

customers_source_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(customers_source_schema) \
.load(source_url)

customers_source_df.show()

window_def = Window.orderBy("customerid")

enhanced_customers_source_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(customers_source_schema) \
.load(source_url) \
.withColumn("customer_skey",row_number().over(window_def)) \
.withColumn("effective_date",date_format(current_date(), DATE_FORMAT)) \
.withColumn("end_date",date_format(lit(future_date), DATE_FORMAT)) \
.withColumn("active_flag", lit(True))

enhanced_customers_source_df.show()

enhanced_customers_source_df.write.mode('overwrite') \
.option("header",True) \
.option("delimiter",",") \
.csv(destination_url)


customers_target_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(customers_target_schema) \
.load(destination_url)

customers_target_df.show()

customers_source_df.show()


max_sk = customers_target_df.agg({"customer_skey": "max"}).collect()[0][0]
print(max_sk)

active_customers_target_df = customers_target_df.where(col("active_flag")==True)

inactive_customers_target_df = customers_target_df.where(col("active_flag")==False)

active_customers_target_df.show()

active_customers_target_df.join(customers_source_df, "customerid" , "full_outer").show()


def column_renamer(df, suffix, append):
    if append:
        new_column_names = list(map(lambda x: x + suffix, df.columns))

    else:
        new_column_names = list(map(lambda x: x.replace(suffix, ""), df.columns))

    return df.toDF(*new_column_names)

def get_hash(df, keys_list):
    columns = [col(column) for column in keys_list]

    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit(1)))

active_customers_target_df_hash = column_renamer(get_hash(active_customers_target_df, slowly_changing_cols), suffix="_target", append=True)

customers_source_df_hash = column_renamer(get_hash(customers_source_df, slowly_changing_cols), suffix="_source", append=True)

active_customers_target_df_hash.show()

customers_source_df_hash.show()

merged_df = active_customers_target_df_hash.join(customers_source_df_hash, col("customerid_source") ==  col("customerid_target") , "full_outer") \
.withColumn("Action", when(col("hash_md5_source") == col("hash_md5_target")  , 'NOCHANGE')\
.when(col("customerid_source").isNull(), 'DELETE')\
.when(col("customerid_target").isNull(), 'INSERT')\
.otherwise('UPDATE'))

merged_df.show()


unchanged_records = column_renamer(merged_df.filter(col("action") == 'NOCHANGE'), suffix="_target", append=False).select(active_customers_target_df.columns)

unchanged_records.show()

insert_records = column_renamer(merged_df.filter(col("action") == 'INSERT'), suffix="_source", append=False) \
                .select(customers_source_df.columns)\
                .withColumn("row_number",row_number().over(window_def))\
                .withColumn("customer_skey",col("row_number")+ max_sk)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("end_date",date_format(lit(future_date),DATE_FORMAT))\
                .withColumn("active_flag", lit(True))\
                .drop("row_number")

insert_records.show()

max_sk = insert_records.agg({"customer_skey": "max"}).collect()[0][0]

print(max_sk)

update_records = column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_target", append=False)\
                .select(active_customers_target_df.columns)\
                .withColumn("end_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("active_flag", lit(False))\
            .unionByName(
            column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_source", append=False)\
                .select(customers_source_df.columns)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("end_date",date_format(lit(future_date),DATE_FORMAT))\
                .withColumn("row_number",row_number().over(window_def))\
                .withColumn("customer_skey",col("row_number")+ max_sk)\
                .withColumn("active_flag", lit(True))\
                .drop("row_number")
                )


update_records.show()

max_sk = update_records.agg({"customer_skey": "max"}).collect()[0][0]

print(max_sk)

delete_records = column_renamer(merged_df.filter(col("action") == 'DELETE'), suffix="_target", append=False)\
                .select(active_customers_target_df.columns)\
                .withColumn("end_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("active_flag", lit(False))

delete_records.show()

resultant_df = inactive_customers_target_df \
            .unionByName(unchanged_records)\
            .unionByName(insert_records)\
            .unionByName(update_records)\
            .unionByName(delete_records)

resultant_df.show()

spark.stop()
