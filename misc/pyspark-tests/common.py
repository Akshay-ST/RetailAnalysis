import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Filter numeric column between 21 and 30
def filter_data_numerical(df, column_name):
    return df.filter(F.col(column_name).between(21, 30))


# Filter text column
def filter_data_text(df, column_name):
    return df.filter(F.col(column_name).isin("shreyas", "Jambure"))


# Add row_number using window function
def add_row_number(df, partition_cols, order_cols):
    order_exprs = [
        F.col(c).desc() if d.lower() == "desc" else F.col(c).asc()
        for c, d in order_cols
    ]

    w = Window.partitionBy(*partition_cols).orderBy(*order_exprs)

    return df.withColumn("row_number", F.row_number().over(w))


# Moving average over N rows
def add_moving_avg(df, partition_cols, order_cols, target_col, window_size):
    order_exprs = [
        F.col(c).desc() if d.lower() == "desc" else F.col(c).asc()
        for c, d in order_cols
    ]

    w = (
        Window
        .partitionBy(*partition_cols)
        .orderBy(*order_exprs)
        .rowsBetween(-window_size + 1, 0)
    )

    return df.withColumn(
        "moving_avg",
        F.round(F.avg(F.col(target_col)).over(w), 2)
    )
