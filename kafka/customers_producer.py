import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomersProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='customers'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.dummyjson_url = "https://dummyjson.com/users"
        
    def fetch_users_from_dummyjson(self, limit=100, skip=0):
        """Fetch users with pagination support"""
        try:
            response = requests.get(f"{self.dummyjson_url}?limit={limit}&skip={skip}")
            response.raise_for_status()
            data = response.json()
            return data.get('users', [])
        except requests.RequestException as e:
            logger.error(f"error fetching users: {e}")
            return []
    
    def fetch_multiple_user_pages(self, total_users=500):
        """Fetch multiple pages of users to increase data volume"""
        all_users = []
        page_size = 100  # DummyJSON max per page
        skip = 0
        
        while len(all_users) < total_users:
            users = self.fetch_users_from_dummyjson(limit=page_size, skip=skip)
            if not users:
                break
            
            all_users.extend(users)
            skip += page_size
            
            # Add small delay to respect API rate limits
            time.sleep(0.1)
            
            logger.info(f"Fetched {len(all_users)} users so far...")
        
        return all_users[:total_users]
    
    def transform_user_to_customer(self, user):

        return {
            "customer_id": str(user.get('id', '')),
            "first_name": user.get('firstName', ''),
            "last_name": user.get('lastName', ''),
            "email": user.get('email', ''),
            "phone": user.get('phone', ''),
            "username": user.get('username', ''),
            "password": user.get('password', ''),  
            "birth_date": user.get('birthDate', ''),
            "image": user.get('image', ''),
            "address": {
                "street": user.get('address', {}).get('address', ''),
                "city": user.get('address', {}).get('city', ''),
                "state": user.get('address', {}).get('state', ''),
                "postal_code": user.get('address', {}).get('postalCode', ''),
                "country": user.get('address', {}).get('country', '')
            },
            "bank": {
                "card_expire": user.get('bank', {}).get('cardExpire', ''),
                "card_number": user.get('bank', {}).get('cardNumber', ''),
                "card_type": user.get('bank', {}).get('cardType', ''),
                "currency": user.get('bank', {}).get('currency', ''),
                "iban": user.get('bank', {}).get('iban', '')
            },
            "company": {
                "department": user.get('company', {}).get('department', ''),
                "name": user.get('company', {}).get('name', ''),
                "title": user.get('company', {}).get('title', '')
            },
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
    
    def send_customer(self, customer):
        try:
            future = self.producer.send(
                self.topic,
                key=customer['customer_id'],
                value=customer
            )
            future.get(timeout=10)
            logger.info(f"Sent customer {customer['customer_id']} to topic {self.topic}")
        except Exception as e:
            logger.error(f"Error sending customer {customer['customer_id']}: {e}")
    
    def produce_customers(self, batch_size=100, delay_seconds=2, total_users_per_cycle=500):
        """Enhanced producer with pagination and higher volume"""
        logger.info(f"Starting customers producer for topic: {self.topic}")
        
        while True:
            try:
                # Fetch multiple pages of users for higher volume
                users = self.fetch_multiple_user_pages(total_users=total_users_per_cycle)
                
                if not users:
                    logger.warning("No users fetched from DummyJSON, retrying...")
                    time.sleep(delay_seconds)
                    continue
                
                # Process users in batches
                for i in range(0, len(users), batch_size):
                    batch = users[i:i + batch_size]
                    
                    for user in batch:
                        customer = self.transform_user_to_customer(user)
                        self.send_customer(customer)
                    
                    logger.info(f"Processed batch {i//batch_size + 1}: {len(batch)} customers")
                    
                    # Small delay between batches
                    time.sleep(0.5)
                
                logger.info(f"Completed cycle: {len(users)} customers from DummyJSON")
                time.sleep(delay_seconds)
                
            except KeyboardInterrupt:
                logger.info("Stopping customers producer...")
                break
            except Exception as e:
                logger.error(f"Error in customers producer: {e}")
                time.sleep(delay_seconds)
        
        self.producer.close()

if __name__ == "__main__":
    producer = CustomersProducer()
    producer.produce_customers(batch_size=100, delay_seconds=2, total_users_per_cycle=500)
