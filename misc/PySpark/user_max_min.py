logs1 = [
    ["200", "user_1", "resource_5"],
    ["3", "user_1", "resource_1"],
    ["620", "user_1", "resource_1"],
    ["620", "user_3", "resource_1"],
    ["34", "user_6", "resource_2"],
    ["95", "user_9", "resource_1"],
    ["416", "user_6", "resource_1"],
    ["58523", "user_3", "resource_1"],
    ["53760", "user_3", "resource_3"],
    ["58522", "user_22", "resource_1"],
    ["100", "user_3", "resource_6"],
    ["400", "user_6", "resource_2"],
]
#
logs2 = [
    ["357", "user", "resource_2"],
    ["1262", "user", "resource_1"],
    ["1462", "user", "resource_2"],
    ["1060", "user", "resource_1"],
    ["756", "user", "resource_3"],
    ["1090", "user", "resource_3"],
]
#
logs3 = [
    ["300", "user_10", "resource_5"]
]
#
logs4 = [
    ["1", "user_96", "resource_5"],
    ["1", "user_10", "resource_5"],
    ["301", "user_11", "resource_5"],
    ["301", "user_12", "resource_5"],
    ["603", "user_12", "resource_5"],
    ["1603", "user_12", "resource_7"],
]
#
logs5 = [
    ["300", "user_1", "resource_3"],
    ["599", "user_1", "resource_3"],
    ["900", "user_1", "resource_3"],
    ["1199", "user_1", "resource_3"],
    ["1200", "user_1", "resource_3"],
    ["1201", "user_1", "resource_3"],
    ["1202", "user_1", "resource_3"]
]

logs = [logs1, logs2, logs3, logs4, logs5]

def spark_init():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("Logs_Analysis").getOrCreate()

def user_sessions(spark, logs):
    from pyspark.sql.functions import col, max, min

    data = logs
    header = ["time", "user_id", "resource_id"]
    
    input_df = spark.createDataFrame(data, header)
    
    temp_df = input_df.groupBy(col("user_id")).agg(
                max(col("time")).alias("max_time"),
                min(col("time")).alias("min_time")
            )
    temp_df.show()
    
def main():
    spark = spark_init()
    for i in range(5):
        print("Input logs ",i+1, user_sessions(spark, logs[i]))

if __name__ == "__main__":
    main()