import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from psycopg2.extras import Json


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "shopstream_analytics"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def ensure_bronze_schema_and_table(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.raw_events (
                id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                record_key TEXT,
                record_value JSONB NOT NULL,
                partition INT,
                "offset" BIGINT,
                event_ts TIMESTAMPTZ,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


def get_consumer(topics: list[str]) -> KafkaConsumer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "shopstream-postgres-loader")
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    return consumer


def insert_event(conn, topic: str, key: Optional[str], value: dict, partition: int, offset: int, timestamp_ms: int) -> None:
    event_ts = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.raw_events (topic, record_key, record_value, partition, "offset", event_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (topic, key, Json(value), partition, offset, event_ts),
        )


def run():
    topics_env = os.getenv("KAFKA_TOPICS", "customers,orders,products,events").strip()
    topics = [t.strip() for t in topics_env.split(",") if t.strip()]

    logger.info(f"Starting Postgres loader for topics: {topics}")

    consumer = get_consumer(topics)

    conn = get_db_connection()
    ensure_bronze_schema_and_table(conn)

    try:
        for msg in consumer:
            try:
                insert_event(
                    conn=conn,
                    topic=msg.topic,
                    key=msg.key,
                    value=msg.value,
                    partition=msg.partition,
                    offset=msg.offset,
                    timestamp_ms=msg.timestamp,
                )
            except Exception as e:
                logger.exception(f"Failed to insert message offset={msg.offset} topic={msg.topic}: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping Postgres loader...")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run()


