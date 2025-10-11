from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="medallion_pipeline_dag",
    description="Simple medallion architecture pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    params={
        "kafka_bootstrap": Param("kafka-shopstream:29092", type="string"),
        "topics": Param("customers,orders,products,events", type="string"),
        "postgres_host": Param("host.docker.internal", type="string"),
        "postgres_port": Param(5432, type="integer"),
        "postgres_db": Param("shopstream_analytics", type="string"),
        "postgres_user": Param("postgres", type="string"),
        "postgres_password": Param("2003", type="string"),
    },
    tags=["medallion", "pipeline", "bronze", "silver", "gold"],
) as dag:

    # Bronze Layer: Spark Streaming (Kafka → PostgreSQL)
    bronze_streaming = SparkSubmitOperator(
        task_id="bronze_streaming",
        name="BronzeStreaming",
        application="local:///opt/bitnami/spark/app/spark_streaming.py",
        packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
            "org.postgresql:postgresql:42.7.4",
        ],
        conf={
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
        },
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": "{{ params.kafka_bootstrap }}",
            "KAFKA_TOPICS": "{{ params.topics }}",
            "KAFKA_STARTING_OFFSETS": "earliest",
            "POSTGRES_HOST": "{{ params.postgres_host }}",
            "POSTGRES_PORT": "{{ params.postgres_port }}",
            "POSTGRES_DB": "{{ params.postgres_db }}",
            "POSTGRES_USER": "{{ params.postgres_user }}",
            "POSTGRES_PASSWORD": "{{ params.postgres_password }}",
            "SPARK_CHECKPOINT_DIR": "/opt/bitnami/spark/app/checkpoints",
        },
        master="spark://spark-master:7077",
        deploy_mode="cluster",
    )

    # Silver Layer: dbt transformations
    silver_transformations = DockerOperator(
        task_id="silver_transformations",
        image="python:3.9-slim",
        command=[
            "bash", "-c",
            """
            pip install dbt-postgres
            cd /opt/airflow/dags/dbt/shopstream_analytics
            dbt run --models customers products orders order_items events
            echo "Silver layer transformations completed"
            """
        ],
        auto_remove=True,
        network_mode="bridge",
        volumes=[
            "/opt/airflow/dags:/opt/airflow/dags",
        ],
        environment={
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt/shopstream_analytics",
        },
    )

    # Gold Layer: dbt transformations
    gold_transformations = DockerOperator(
        task_id="gold_transformations",
        image="python:3.9-slim",
        command=[
            "bash", "-c",
            """
            pip install dbt-postgres
            cd /opt/airflow/dags/dbt/shopstream_analytics
            dbt run --models dim_customers dim_products fct_orders fct_order_items agg_orders_daily agg_events_daily
            echo "Gold layer transformations completed"
            """
        ],
        auto_remove=True,
        network_mode="bridge",
        volumes=[
            "/opt/airflow/dags:/opt/airflow/dags",
        ],
        environment={
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt/shopstream_analytics",
        },
    )

    # Data quality tests
    data_quality_tests = DockerOperator(
        task_id="data_quality_tests",
        image="python:3.9-slim",
        command=[
            "bash", "-c",
            """
            pip install dbt-postgres
            cd /opt/airflow/dags/dbt/shopstream_analytics
            dbt test
            echo "Data quality tests completed"
            """
        ],
        auto_remove=True,
        network_mode="bridge",
        volumes=[
            "/opt/airflow/dags:/opt/airflow/dags",
        ],
        environment={
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt/shopstream_analytics",
        },
    )

    # Pipeline completion
    pipeline_complete = DockerOperator(
        task_id="pipeline_complete",
        image="python:3.9-slim",
        command="echo 'Medallion pipeline completed successfully'",
        auto_remove=True,
        network_mode="bridge",
    )

    # Define task dependencies: Bronze → Silver → Gold → Tests → Complete
    bronze_streaming >> silver_transformations >> gold_transformations >> data_quality_tests >> pipeline_complete
