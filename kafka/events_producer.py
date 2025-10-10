import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventsProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='events'):

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.dummyjson_url = "https://dummyjson.com"
        
    def fetch_users_for_events(self, limit=50):

        try:
            response = requests.get(f"{self.dummyjson_url}/users?limit={limit}")
            response.raise_for_status()
            data = response.json()
            return data.get('users', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching users from DummyJSON: {e}")
            return []
    
    def fetch_products_for_events(self, limit=100):

        try:
            response = requests.get(f"{self.dummyjson_url}/products?limit={limit}")
            response.raise_for_status()
            data = response.json()
            return data.get('products', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching products from DummyJSON: {e}")
            return []
    
    def generate_event(self, user, product=None):

        event_types = [
            'page_view',
            'product_view',
            'add_to_cart',
            'remove_from_cart',
            'search',
            'filter',
            'checkout_start',
            'checkout_complete',
            'login',
            'logout',
            'signup',
            'newsletter_signup',
            'review_submitted',
            'wishlist_add',
            'wishlist_remove'
        ]
        
        event_type = random.choice(event_types)
        event_id = f"EVT-{int(time.time())}-{random.randint(1000, 9999)}"

        event = {
            "event_id": event_id,
            "event_type": event_type,
            "user_id": str(user.get('id', '')),
            "session_id": f"SESS-{random.randint(100000, 999999)}",
            "timestamp": datetime.now().isoformat(),
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15"
            ]),
            "referrer": random.choice([
                "https://google.com",
                "https://facebook.com",
                "https://twitter.com",
                "https://instagram.com",
                "direct",
                ""
            ]),
            "device_type": random.choice(['desktop', 'mobile', 'tablet']),
            "browser": random.choice(['chrome', 'firefox', 'safari', 'edge']),
            "os": random.choice(['windows', 'macos', 'linux', 'ios', 'android']),
            "country": user.get('address', {}).get('country', 'USA'),
            "city": user.get('address', {}).get('city', 'Unknown'),
            "created_at": datetime.now().isoformat()
        }

        if event_type in ['product_view', 'add_to_cart', 'remove_from_cart', 'wishlist_add', 'wishlist_remove'] and product:
            event["product_id"] = str(product.get('id', ''))
            event["product_name"] = product.get('title', '')
            event["product_category"] = product.get('category', '')
            event["product_price"] = product.get('price', 0.0)
            event["product_brand"] = product.get('brand', '')
            
            if event_type in ['add_to_cart', 'remove_from_cart']:
                event["quantity"] = random.randint(1, 5)
        
        elif event_type == 'search':
            search_terms = [
                'laptop', 'smartphone', 'headphones', 'camera', 'watch',
                'shoes', 'jacket', 'book', 'gaming', 'fitness'
            ]
            event["search_term"] = random.choice(search_terms)
            event["search_results_count"] = random.randint(0, 100)
        
        elif event_type == 'filter':
            event["filter_category"] = random.choice(['price', 'brand', 'rating', 'category'])
            event["filter_value"] = random.choice(['low', 'medium', 'high'])
        
        elif event_type == 'page_view':
            pages = [
                '/home', '/products', '/categories', '/cart', '/checkout',
                '/profile', '/orders', '/wishlist', '/search', '/about'
            ]
            event["page_url"] = random.choice(pages)
            event["page_title"] = random.choice([
                'Home', 'Products', 'Categories', 'Shopping Cart', 'Checkout',
                'Profile', 'Orders', 'Wishlist', 'Search Results', 'About Us'
            ])
        
        elif event_type == 'checkout_start':
            event["cart_value"] = round(random.uniform(50.0, 500.0), 2)
            event["cart_items_count"] = random.randint(1, 10)
        
        elif event_type == 'checkout_complete':
            event["order_id"] = f"ORD-{random.randint(10000, 99999)}"
            event["order_value"] = round(random.uniform(50.0, 500.0), 2)
            event["payment_method"] = random.choice(['credit_card', 'paypal', 'debit_card'])
        
        elif event_type == 'review_submitted' and product:
            event["product_id"] = str(product.get('id', ''))
            event["rating"] = random.randint(1, 5)
            event["review_text"] = random.choice([
                "Great product!", "Good quality", "Fast shipping", 
                "Could be better", "Excellent value", "Not what I expected"
            ])
        
        return event
    
    def send_event(self, event):

        try:
            future = self.producer.send(
                self.topic,
                key=event['event_id'],
                value=event
            )
            future.get(timeout=10)
            logger.info(f"Sent event {event['event_id']} ({event['event_type']}) to topic {self.topic}")
        except Exception as e:
            logger.error(f"Error sending event {event['event_id']}: {e}")
    
    def produce_events(self, events_per_batch=200, delay_seconds=1):
        logger.info(f"Starting events producer for topic: {self.topic}")

        users = self.fetch_users_for_events(limit=200)
        products = self.fetch_products_for_events(limit=500)
        
        if not users:
            logger.error("No users available for event generation")
            return
        
        logger.info(f"Loaded {len(users)} users and {len(products)} products for event generation")
        
        while True:
            try:
                for _ in range(events_per_batch):
                    user = random.choice(users)
                    product = random.choice(products) if products else None
                    
                    event = self.generate_event(user, product)
                    self.send_event(event)
                
                logger.info(f"Generated {events_per_batch} events")
                time.sleep(delay_seconds)
                
            except KeyboardInterrupt:
                logger.info("Stopping events producer...")
                break
            except Exception as e:
                logger.error(f"Error in events producer: {e}")
                time.sleep(delay_seconds)
        
        self.producer.close()

if __name__ == "__main__":
    producer = EventsProducer()
    producer.produce_events(events_per_batch=30, delay_seconds=3)
