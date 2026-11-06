
import os
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    sum as spark_sum,
    coalesce,
    lit,
    when,
    trim,
    isnan,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ecommerce-events",
)

CHECKPOINT = os.getenv(
    "CHECKPOINT_LOCATION",
    "/opt/spark-checkpoints/streamforge-dev",
)

JDBC_URL = os.getenv(
    "JDBC_URL",
    "jdbc:postgresql://postgres:5432/streamforge",
)

DB_USER = os.getenv("DB_USER", "streamforge")
DB_PASSWORD = os.getenv("DB_PASSWORD", "streamforge_dev")


# --------------------------------------------------
# 2. EVENT SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("inventory_delta", IntegerType(), True),
])


# --------------------------------------------------
# 3. INITIALIZE SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("StreamForge-Kafka-Analytics")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 4. READ EVENTS FROM KAFKA
# --------------------------------------------------

print(
    f"StreamForge connecting to Kafka topic "
    f"'{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP}"
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# --------------------------------------------------
# 5. PARSE AND VALIDATE EVENTS
# --------------------------------------------------

valid_event_types = [
    "product_view",
    "add_to_cart",
    "purchase",
    "payment_failed",
    "inventory_update",
]

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(col("json_value"), schema).alias("event")
    )
    .select("event.*")
    .withColumn("event_id", trim(col("event_id")))
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("region", trim(col("region")))
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )
)

# Common validation rules
valid_events = parsed.filter(
    col("event_id").isNotNull()
    & (col("event_id") != "")
    & col("event_type").isin(valid_event_types)
    & col("region").isNotNull()
    & (col("region") != "")
    & col("event_timestamp").isNotNull()
)

# Purchase-specific validation
valid_events = valid_events.filter(
    (col("event_type") != "purchase")
    |
    (
        col("amount").isNotNull()
        & (~isnan(col("amount")))
        & (col("amount") >= 0)
    )
)

print("Event parsing and validation rules configured.")


# --------------------------------------------------
# 6. EVENT-TIME WINDOW AGGREGATION
# --------------------------------------------------

aggregated = (
    valid_events
    .withWatermark("event_timestamp", "5 seconds")
    .groupBy(
        window(col("event_timestamp"), "10 seconds"),
        col("event_type"),
        col("region"),
    )
    .agg(
        count(lit(1)).alias("event_count"),

        spark_sum(
            when(
                col("event_type") == "purchase",
                col("amount"),
            ).otherwise(lit(0.0))
        ).alias("gross_sales"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("region"),
        col("event_count"),
        coalesce(
            col("gross_sales"),
            lit(0.0),
        ).alias("gross_sales"),
    )
)


# --------------------------------------------------
# 7. WRITE AGGREGATIONS TO POSTGRESQL
# --------------------------------------------------


def write_to_postgres(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no completed windows yet.")
        return

    # Each micro-batch gets its own staging table.
    staging_table = f"streamforge_metrics_staging_{batch_id}"

    connection = None

    try:
        # Step 1: Write this micro-batch to its staging table.
        (
            batch_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", staging_table)
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        # Step 2: Connect to PostgreSQL.
        connection = psycopg2.connect(
            JDBC_URL.replace("jdbc:", ""),
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )

        connection.autocommit = False

        with connection.cursor() as cursor:

            # Step 3: Upsert staged records into final table.
            cursor.execute(f"""
                INSERT INTO streamforge_event_metrics (
                    window_start,
                    window_end,
                    event_type,
                    region,
                    event_count,
                    gross_sales
                )
                SELECT
                    window_start,
                    window_end,
                    event_type,
                    region,
                    event_count,
                    gross_sales
                FROM {staging_table}

                ON CONFLICT (
                    window_start,
                    window_end,
                    event_type,
                    region
                )
                DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    gross_sales = EXCLUDED.gross_sales;
            """)

            # Step 4: Drop staging table in the same transaction.
            cursor.execute(
                f"DROP TABLE {staging_table};"
            )

        # Step 5: Commit the upsert and cleanup together.
        connection.commit()

        print(
            f"Batch {batch_id}: successfully upserted "
            f"aggregation rows into PostgreSQL."
        )

    except Exception:
        if connection:
            connection.rollback()

        raise

    finally:
        if connection:
            connection.close()

# --------------------------------------------------
# 8. START THE STREAMING QUERY
# --------------------------------------------------

query = (
    aggregated.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print(
    "StreamForge is running. "
    "Waiting for Kafka events..."
)


# --------------------------------------------------
# 9. KEEP THE APPLICATION RUNNING
# --------------------------------------------------

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("Stopping StreamForge...")

finally:
    if query.isActive:
        query.stop()

    spark.stop()
