from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)


# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("EcommerceStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------
# PostgreSQL configuration
# -----------------------------
jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce"

jdbc_properties = {
    "user": "ecommerce",
    "password": "ecommerce",
    "driver": "org.postgresql.Driver"
}


# -----------------------------
# Kafka message schema
# -----------------------------
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_time", StringType(), False),
    StructField("status", StringType(), False),

    StructField("payment_id", IntegerType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_status", StringType(), False),
    StructField("payment_time", StringType(), False)
])


# -----------------------------
# Read from Kafka
# -----------------------------
orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)


# -----------------------------
# Convert Kafka JSON → columns
# -----------------------------
order_data = (
    orders
    .selectExpr("CAST(value AS STRING) AS json")
    .select(
        from_json(col("json"), order_schema).alias("data")
    )
    .select("data.*")
)


# -----------------------------
# Convert timestamp strings
# -----------------------------
order_data = (
    order_data
    .withColumn(
        "order_time",
        to_timestamp(col("order_time"))
    )
    .withColumn(
        "payment_time",
        to_timestamp(col("payment_time"))
    )
)


# -----------------------------
# Process each streaming batch
# -----------------------------
def write_to_postgres(batch_df, batch_id):

    print(f"\nProcessing batch: {batch_id}")

    # -------------------------
    # Orders table
    # -------------------------
    orders_df = batch_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_time",
        "status"
    )

    orders_df.write \
        .jdbc(
            url=jdbc_url,
            table="orders",
            mode="append",
            properties=jdbc_properties
        )

    print(f"Inserted orders for batch {batch_id}")


    # -------------------------
    # Payments table
    # -------------------------
    payments_df = batch_df.select(
        "payment_id",
        "order_id",
        "amount",
        "payment_method",
        "payment_status",
        "payment_time"
    )

    payments_df.write \
        .jdbc(
            url=jdbc_url,
            table="payments",
            mode="append",
            properties=jdbc_properties
        )

    print(f"Inserted payments for batch {batch_id}")


# -----------------------------
# Start streaming query
# -----------------------------
query = (
    order_data
    .writeStream
    .foreachBatch(write_to_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark-apps/checkpoint"
    )
    .start()
)

query.awaitTermination()