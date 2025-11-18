from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, sum, avg, max, min, countDistinct

spark = SparkSession.builder.appName("EcommerceRegionCategoryPerformance").getOrCreate()

# Load datasets
orders = spark.read.csv("s3://your-bucket/ecommerce/orders.csv", header=True, inferSchema=True)
customers = spark.read.csv("s3://your-bucket/ecommerce/customers.csv", header=True, inferSchema=True)
products = spark.read.csv("s3://your-bucket/ecommerce/products.csv", header=True, inferSchema=True)

# Convert timestamp
orders = orders.withColumn("order_timestamp", to_timestamp(col("order_timestamp")))

# Join orders -> customers -> products
data = orders.join(customers, "customer_id").join(products, "product_id")

# Region performance metrics
region_metrics = (
    data.groupBy("country")
    .agg(
        sum("order_amount").alias("region_total_revenue"),
        count("*").alias("region_total_orders"),
        avg("order_amount").alias("avg_order_value_in_region"),
        max("order_amount").alias("max_order_value_in_region"),
        min("order_amount").alias("min_order_value_in_region"),
        countDistinct("customer_id").alias("unique_customers_in_region"),
        countDistinct("product_id").alias("unique_products_sold_in_region")
    )
)

# Category performance metrics
category_metrics = (
    data.groupBy("category")
    .agg(
        sum("order_amount").alias("category_total_revenue"),
        count("*").alias("category_total_orders"),
        avg("order_amount").alias("avg_order_value_by_category"),
        max("order_amount").alias("max_order_value_by_category"),
        min("order_amount").alias("min_order_value_by_category")
    )
)

# Write outputs to S3
region_metrics.write.parquet("s3://your-bucket/ecommerce/output/region_performance_metrics", mode="overwrite")
category_metrics.write.parquet("s3://your-bucket/ecommerce/output/category_performance_metrics", mode="overwrite")

spark.stop()
