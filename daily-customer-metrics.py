from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, count, sum, avg, max, min

spark = SparkSession.builder.appName("EcommerceTransactionCustomerMetrics").getOrCreate()

# Load datasets
orders = spark.read.csv("s3://roy-music-kpis/ecommerce/orders.csv", header=True, inferSchema=True)
customers = spark.read.csv("s3://roy-music-kpis/ecommerce/customers.csv", header=True, inferSchema=True)

# Convert timestamp
orders = orders.withColumn("order_timestamp", to_timestamp(col("order_timestamp")))

# Join orders with customers
order_details = orders.join(customers, "customer_id")

# Daily order metrics
daily_metrics = (
    order_details
    .withColumn("order_date", date_format(col("order_timestamp"), "yyyy-MM-dd"))
    .groupBy("order_date")
    .agg(
        count("*").alias("total_orders"),
        sum("order_amount").alias("total_revenue"),
        avg("order_amount").alias("average_order_value"),
        max("order_amount").alias("max_order_value"),
        min("order_amount").alias("min_order_value"),
        sum("items_count").alias("total_items_sold")
    )
)

# Customer metrics
customer_metrics = (
    order_details
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_customer_orders"),
        sum("order_amount").alias("customer_total_revenue"),
        avg("order_amount").alias("avg_customer_spend"),
        sum("items_count").alias("total_items_bought"),
        max("order_amount").alias("max_single_order"),
        min("order_amount").alias("min_single_order")
    )
)

# Write outputs to S3
daily_metrics.write.parquet("s3://roy-music-kpis/ecommerce/output/daily_order_metrics", mode="overwrite")
customer_metrics.write.parquet("s3://roy-music-kpis/ecommerce/output/customer_metrics", mode="overwrite")

spark.stop()
