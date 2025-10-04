# import json
# import logging
# from datetime import datetime
# from kafka import KafkaConsumer
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import os
# from dotenv import load_dotenv

# load_dotenv()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class PostgresLoaderConsumer:
#     def __init__(self, 
#                  bootstrap_servers='localhost:9092', 
#                  topics=['customers', 'orders', 'products', 'events'],
#                  group_id='postgres_loader_group'):
        
#         self.bootstrap_servers = bootstrap_servers
#         self.topics = topics
#         self.group_id = group_id
        
#         self.db_config = {
#             'host': os.getenv('POSTGRES_HOST'),
#             'port': os.getenv('POSTGRES_PORT'),
#             'database': os.getenv('POSTGRES_DB'),
#             'user': os.getenv('POSTGRES_USER'),
#             'password': os.getenv('POSTGRES_PASSWORD')
#         }
        
#         self.consumer = KafkaConsumer(
#             *topics,
#             bootstrap_servers=bootstrap_servers,
#             group_id=group_id,
#             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#             key_deserializer=lambda m: m.decode('utf-8') if m else None,
#             auto_offset_reset='earliest',
#             enable_auto_commit=True,
#             auto_commit_interval_ms=1000
#         )
        
#         self.test_db_connection()
    
#     def test_db_connection(self):
#         try:
#             conn = psycopg2.connect(**self.db_config)
#             conn.close()
#             logger.info("Connected to PostgreSQL")
#         except Exception as e:
#             logger.error(f"Database connection failed: {e}")
#             raise
    
#     def get_db_connection(self):
#         return psycopg2.connect(**self.db_config)
    
#     def load_to_postgres(self, topic, data):
#         conn = None
#         try:
#             conn = self.get_db_connection()
#             cursor = conn.cursor(cursor_factory=RealDictCursor)
            
#             table_map = {
#                 'customers': 'bronze.customers_raw',
#                 'orders': 'bronze.orders_raw',
#                 'products': 'bronze.products_raw'
#             }
            
#             table = table_map.get(topic)
#             if not table:
#                 logger.warning(f"Unknown topic: {topic}")
#                 return
            
#             id_field = f"{topic[:-1]}_id" if topic.endswith('s') else f"{topic}_id"
#             record_id = data.get(id_field)
#             created_at = data.get('created_at')
#             updated_at = data.get('updated_at')
            
#             insert_query = f"""
#                 INSERT INTO {table} ({id_field}, raw_data, created_at, updated_at)
#                 VALUES (%s, %s, %s, %s)
#                 ON CONFLICT ({id_field}) 
#                 DO UPDATE SET 
#                     raw_data = EXCLUDED.raw_data,
#                     updated_at = EXCLUDED.updated_at
#                 RETURNING id;
#             """
            
#             cursor.execute(insert_query, (
#                 record_id,
#                 json.dumps(data),
#                 created_at,
#                 updated_at
#             ))
            
#             result = cursor.fetchone()
#             conn.commit()
            
#             logger.info(f"Loaded {topic} record {record_id} to PostgreSQL")
#             return result['id']
            
#         except Exception as e:
#             if conn:
#                 conn.rollback()
#             logger.error(f"Error loading to PostgreSQL: {e}")
#             raise
#         finally:
#             if conn:
#                 conn.close()
    
#     def start_consuming(self):
#         logger.info(f"Starting PostgreSQL loader for topics: {self.topics}")
        
#         try:
#             for message in self.consumer:
#                 logger.info(f"Received: {message.topic}:{message.partition}:{message.offset}")
#                 self.load_to_postgres(message.topic, message.value)
                
#         except KeyboardInterrupt:
#             logger.info("Stopping PostgreSQL loader...")
#         except Exception as e:
#             logger.error(f"Error in PostgreSQL loader: {e}")
#         finally:
#             self.consumer.close()
#             logger.info("PostgreSQL loader stopped")

# if __name__ == "__main__":
#     consumer = PostgresLoaderConsumer()
#     consumer.start_consuming()
