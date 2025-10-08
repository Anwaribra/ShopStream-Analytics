import json
import logging
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SparkKafkaToPostgresBronze")
        .getOrCreate()
    )


def run_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics_env = os.getenv("KAFKA_TOPICS", "customers,orders,products").strip()
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    topics = ",".join([t.strip() for t in topics_env.split(",") if t.strip()])

    # read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed = (
        df.select(
            F.col("topic").cast(StringType()).alias("topic"),
            F.col("key").cast(StringType()).alias("record_key"),
            F.col("value").cast(StringType()).alias("value_str"),
            F.col("partition").alias("partition"),
            F.col("offset").alias("offset"),
            (F.col("timestamp").cast("timestamp")).alias("event_ts"),
        )
        .withColumn("record_value", F.from_json("value_str", F.schema_of_json(F.lit("{}"))))
        .drop("value_str")
    )

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'shopstream_analytics')}"
    )

    db_properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }

    def foreach_batch_write(batch_df, batch_id):
        (
            batch_df
            .select(
                "topic",
                "record_key",
                F.to_json(F.col("record_value")).cast(StringType()).alias("record_value"),
                "partition",
                "offset",
                "event_ts",
            )
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.raw_events")
            .option("stringtype", "unspecified")
            .option("user", db_properties["user"])
            .option("password", db_properties["password"])
            .option("driver", db_properties["driver"])
            .mode("append")
            .save()
        )

    query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(foreach_batch_write)
        .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark/checkpoints/bronze_raw_events"))
        .start()
    )

    logger.info("Spark streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    run_stream()