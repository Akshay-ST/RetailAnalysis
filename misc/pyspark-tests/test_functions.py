from common import filter_data_numerical, add_moving_avg

# Test Case 1: Filter numeric values between 21 and 30
def test_filter_data_numerical(spark_session):

    sample_data = [
        {"name": "John", "age": 30},
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 35},
        {"name": "Eve", "age": 20},
    ]

    original_df = spark_session.createDataFrame(sample_data)

    transformed_df = filter_data_numerical(original_df, "age")

    expected_data = [
        {"name": "John", "age": 30},
        {"name": "Alice", "age": 25},
    ]

    expected_df = spark_session.createDataFrame(expected_data)

    assert transformed_df.collect() == expected_df.collect()


# Test Case 2: Moving average
def test_add_moving_avg_3(spark_session):

    sample_data = [
        {"id": 1, "grp": "A", "amount": 10},
        {"id": 2, "grp": "A", "amount": 20},
        {"id": 3, "grp": "A", "amount": 30},
        {"id": 4, "grp": "C", "amount": 40},
        {"id": 5, "grp": "B", "amount": 40},
        {"id": 6, "grp": "B", "amount": 60},
        {"id": 8, "grp": "B", "amount": 20},
        {"id": 7, "grp": "B", "amount": 10},
    ]

    original_df = spark_session.createDataFrame(sample_data)

    transformed_df = add_moving_avg(
        original_df,
        partition_cols=["grp"],
        order_cols=[("id", "asc")],
        target_col="amount",
        window_size=3
    )

    expected_data = [
        {"id": 1, "grp": "A", "amount": 10, "moving_avg": 10.00},
        {"id": 2, "grp": "A", "amount": 20, "moving_avg": 15.00},
        {"id": 3, "grp": "A", "amount": 30, "moving_avg": 20.00},
        {"id": 4, "grp": "C", "amount": 40, "moving_avg": 40.00},
        {"id": 5, "grp": "B", "amount": 40, "moving_avg": 40.00},
        {"id": 6, "grp": "B", "amount": 60, "moving_avg": 50.00},
        {"id": 8, "grp": "B", "amount": 20, "moving_avg": 30.00},
        {"id": 7, "grp": "B", "amount": 10, "moving_avg": 36.67},
    ]

    expected_df = spark_session.createDataFrame(expected_data)

    assert (
        transformed_df.orderBy("grp", "id").collect()
        == expected_df.orderBy("grp", "id").collect()
    )
