import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct


def calculate_aggregate_orders_data(data_source_orders, data_source_order_items, data_source_customers, output_uri):

    with SparkSession.builder.appName("Agg").getOrCreate() as spark:

        orders_schema = "order_id int, order_date date, customer_id int, order_status string"
        orders_df = spark.read.format("csv").schema(orders_schema).load(data_source_orders)

        order_items_schema = "order_item_id int, order_item_order_id int, order_item_product_id int,  order_item_quantity int, order_item_product_price float, order_item_subtotal float"
        order_items_df = spark.read.format("csv") \
                        .schema(order_items_schema).load(data_source_order_items)

        customer_schema = "customer_id int, customer_fname string, customer_lname string, email string, password string, address string, city string, state string, pincode string"
        customer_df = spark.read.format("csv") \
                      .schema(customer_schema).load(data_source_customers)

        joined_df = orders_df \
                    .join(order_items_df, orders_df["order_id"] == order_items_df["order_item_order_id"], "inner") \
                    .join(customer_df, orders_df.customer_id == customer_df.customer_id, "inner")

        joined_new_df = joined_df.select("order_id", "order_item_id", "order_item_product_id", "order_item_quantity",
                                         "order_item_product_price", "order_item_subtotal", "customer_fname",
                                         "customer_lname", "city", "state")

        aggregated_df = joined_new_df.groupBy("order_id", "customer_fname", "customer_lname", "city", "state") \
                        .agg(collect_list(struct("order_item_id", "order_item_product_id", "order_item_quantity","order_item_product_price","order_item_subtotal")) \
                        .alias("line_items")) \
                        .orderBy("order_id")

        aggregated_df.show(5,False)

        aggregated_df.write.mode("overwrite").parquet(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source_orders', help="Orders CSV URI")
    parser.add_argument('--data_source_order_items', help="Order Items CSV URI")
    parser.add_argument('--data_source_customers', help="Customers CSV URI")
    parser.add_argument('--output_uri', help="Output URI")

    args = parser.parse_args()

    calculate_aggregate_orders_data(args.data_source_orders, args.data_source_order_items,
                                    args.data_source_customers,args.output_uri)
