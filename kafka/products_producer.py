import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductsProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='products'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.dummyjson_url = "https://dummyjson.com/products"
        
    def fetch_products_from_dummyjson(self, limit=100, skip=0):
        """Fetch products with pagination support"""
        try:
            response = requests.get(f"{self.dummyjson_url}?limit={limit}&skip={skip}")
            response.raise_for_status()
            data = response.json()
            return data.get('products', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching products {e}")
            return []
    
    def fetch_multiple_product_pages(self, total_products=1000):
        """Fetch multiple pages of products to increase data volume"""
        all_products = []
        page_size = 100  # DummyJSON max per page
        skip = 0
        
        while len(all_products) < total_products:
            products = self.fetch_products_from_dummyjson(limit=page_size, skip=skip)
            if not products:
                break
            
            all_products.extend(products)
            skip += page_size
            
            # Add small delay to respect API rate limits
            time.sleep(0.1)
            
            logger.info(f"Fetched {len(all_products)} products so far...")
        
        return all_products[:total_products]
    
    def transform_product(self, product):

        return {
            "product_id": str(product.get('id', '')),
            "title": product.get('title', ''),
            "description": product.get('description', ''),
            "price": product.get('price', 0.0),
            "discount_percentage": product.get('discountPercentage', 0.0),
            "rating": product.get('rating', 0.0),
            "stock": product.get('stock', 0),
            "brand": product.get('brand', ''),
            "category": product.get('category', ''),
            "thumbnail": product.get('thumbnail', ''),
            "images": product.get('images', []),
            "sku": f"SKU-{product.get('id', '')}-{random.randint(1000, 9999)}",
            "weight": round(random.uniform(0.1, 10.0), 2),  
            "dimensions": {
                "length": round(random.uniform(5, 50), 1),
                "width": round(random.uniform(5, 50), 1),
                "height": round(random.uniform(5, 50), 1)
            },
            "tags": product.get('tags', []),
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
    
    def send_product(self, product):
        try:
            future = self.producer.send(
                self.topic,
                key=product['product_id'],
                value=product
            )
            future.get(timeout=10)
            logger.info(f"Sent product {product['product_id']} to topic {self.topic}")
        except Exception as e:
            logger.error(f"Error sending product {product['product_id']}: {e}")
    
    def produce_products(self, batch_size=100, delay_seconds=2, total_products_per_cycle=1000):
        """Enhanced producer with pagination and higher volume"""
        logger.info(f"Starting products producer for topic: {self.topic}")
        
        while True:
            try:
                # Fetch multiple pages of products for higher volume
                products = self.fetch_multiple_product_pages(total_products=total_products_per_cycle)
                
                if not products:
                    logger.warning("No products fetched")
                    time.sleep(delay_seconds)
                    continue

                # Process products in batches
                for i in range(0, len(products), batch_size):
                    batch = products[i:i + batch_size]
                    
                    for product in batch:
                        transformed_product = self.transform_product(product)
                        self.send_product(transformed_product)
                    
                    logger.info(f"Processed batch {i//batch_size + 1}: {len(batch)} products")
                    
                   
                    time.sleep(0.5)
                
                logger.info(f"Completed cycle: {len(products)} products from DummyJSON")
                time.sleep(delay_seconds)
                
            except KeyboardInterrupt:
                logger.info("Stopping products producer...")
                break
            except Exception as e:
                logger.error(f"Error in products producer: {e}")
                time.sleep(delay_seconds)
        
        self.producer.close()

if __name__ == "__main__":
    producer = ProductsProducer()
    producer.produce_products(batch_size=100, delay_seconds=2, total_products_per_cycle=1000)
