
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col, to_timestamp, unix_timestamp,
max, avg, round, concat_ws
)

class ChargePointsETLJob:
    input_path = 'data/electric-chargepoints-2017.csv'
    output_path = 'data/chargepoints-2017-analysis'

    def __init__(self):
        self.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("ChargePointsETLJob")
            .getOrCreate()
        )
    # -------------------------
    # Extract
    # -------------------------
    def extract(self):
        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(self.input_path)
        )

    # -------------------------
    # Transform
    # -------------------------
    def transform(self, df):
        df = df.withColumn(
            "duration",
            col("PluginDuration").cast("double")
        )

        result_df = df.groupBy(
            col("CPID").alias("chargepoint_id")
        ).agg(
            round(max("duration"), 2).alias("max_duration"),
            round(avg("duration"), 2).alias("avg_duration")
        )

        return result_df
    
    # -------------------------
    # LOAD
    # -------------------------
    def load(self, df):
        (
            df.write
            .mode("overwrite")
            .option("header", True)
            .csv(self.output_path)
        )

    def run(self):
        self.load(self.transform(self.extract()))
        '''
        df = self.extract()
        transformed_df = self.transform(df)
        self.load(transformed_df)
        '''

# Run job
if __name__ == "__main__":
    job = ChargePointsETLJob()
    job.run()



'''

    # -------------------------
    # SOLUTION 2–7: Timestamp Based Approach (ITERATIONS)
    # -------------------------
    def transform_using_timestamps(self, df):

        # 🔹 Iteration Fix 1–4: Combine date + time
        df = df.withColumn(
            "start_ts",
            to_timestamp(concat_ws(" ", col("StartDate"), col("StartTime")))
        ).withColumn(
            "end_ts",
            to_timestamp(concat_ws(" ", col("EndDate"), col("EndTime")))
        )

        # 🔹 Iteration Fix 5–7: Explicit format (important)
        df = df.withColumn(
            "start_ts",
            to_timestamp(
                concat_ws(" ", col("StartDate"), col("StartTime")),
                "yyyy-MM-dd HH:mm"
            )
        ).withColumn(
            "end_ts",
            to_timestamp(
                concat_ws(" ", col("EndDate"), col("EndTime")),
                "yyyy-MM-dd HH:mm"
            )
        )

        # 🔹 Iteration Fix: Duration calculation
        df = df.withColumn(
            "duration",
            (col("end_ts").cast("long") - col("start_ts").cast("long")) / 3600
        )

        # 🔹 Aggregation
        result_df = df.groupBy(col("CPID").alias("chargepoint_id")).agg(
            round(max("duration"), 2).alias("max_duration"),
            round(avg("duration"), 2).alias("avg_duration")
        )

        return result_df

    # -------------------------
    # SOLUTION 8: CORRECT (Use PluginDuration)
    # -------------------------
    def transform_using_plugin_duration(self, df):

        df = df.withColumn(
            "duration",
            col("PluginDuration").cast("double")
        )

        result_df = df.groupBy(
            col("CPID").alias("chargepoint_id")
        ).agg(
            round(max("duration"), 2).alias("max_duration"),
            round(avg("duration"), 2).alias("avg_duration")
        )

        return result_df

    # -------------------------
    # RUN (choose approach)
    # -------------------------
    def run(self, use_plugin_duration=True):
        df = self.extract()

        if use_plugin_duration:
            result = self.transform_using_plugin_duration(df)
        else:
            result = self.transform_using_timestamps(df)

        self.load(result)
'''


