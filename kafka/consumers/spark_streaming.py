import json
import logging
import os
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_config() -> Dict[str, Any]:
	return {
		'host': os.getenv('POSTGRES_HOST'),
		'port': os.getenv('POSTGRES_PORT'),
		'database': os.getenv('POSTGRES_DB'),
		'user': os.getenv('POSTGRES_USER'),
		'password': os.getenv('POSTGRES_PASSWORD')
	}

# Spark Session
# ----------------------------	
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()
	