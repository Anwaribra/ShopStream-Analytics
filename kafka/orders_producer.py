import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrdersProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='orders'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.dummyjson_url = "https://dummyjson.com/carts"
        
    def fetch_carts_from_dummyjson(self, limit=20):
        try:
            response = requests.get(f"{self.dummyjson_url}?limit={limit}")
            response.raise_for_status()
            data = response.json()
            return data.get('carts', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching carts {e}")
            return []
    
    def fetch_products_for_orders(self, product_ids):
        products = []
        for product_id in product_ids:
            try:
                response = requests.get(f"https://dummyjson.com/products/{product_id}")
                response.raise_for_status()
                product = response.json()
                products.append(product)
            except requests.RequestException as e:
                logger.error(f"Error fetching product {product_id}: {e}")
        return products
    
    def transform_cart_to_order(self, cart):

        order_id = f"ORD-{cart.get('id', random.randint(10000, 99999))}-{int(time.time())}"

        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        status = random.choice(statuses)

        payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']
        payment_method = random.choice(payment_methods)

        total = cart.get('total', 0)
        discounted_total = cart.get('discountedTotal', total)
        discount_amount = total - discounted_total

        shipping_address = {
            "street": f"{random.randint(1, 999)} Main St",
            "city": random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']),
            "state": random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']),
            "postal_code": f"{random.randint(10000, 99999)}",
            "country": "USA"
        }
        
        return {
            "order_id": order_id,
            "customer_id": str(cart.get('userId', random.randint(1, 100))),
            "order_date": datetime.now().isoformat(),
            "status": status,
            "payment_method": payment_method,
            "payment_status": random.choice(['pending', 'paid', 'failed', 'refunded']),
            "subtotal": total,
            "discount_amount": discount_amount,
            "tax_amount": round(total * 0.08, 2),  
            "shipping_cost": round(random.uniform(5.99, 25.99), 2),
            "total_amount": round(discounted_total + (total * 0.08) + random.uniform(5.99, 25.99), 2),
            "shipping_address": shipping_address,
            "billing_address": shipping_address, 
            "items": cart.get('products', []),
            "notes": random.choice([
                "Please leave package at front door",
                "Call before delivery",
                "Signature required",
                "Fragile - handle with care",
                ""
            ]),
            "tracking_number": f"TRK{random.randint(100000000, 999999999)}" if status in ['shipped', 'delivered'] else None,
            "estimated_delivery": (datetime.now() + timedelta(days=random.randint(1, 7))).isoformat(),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
    
    def send_order(self, order):

        try:
            future = self.producer.send(
                self.topic,
                key=order['order_id'],
                value=order
            )
            future.get(timeout=10)
            logger.info(f"Sent order {order['order_id']} to topic {self.topic}")
        except Exception as e:
            logger.error(f"Error sending order {order['order_id']}: {e}")
    
    def produce_orders(self, batch_size=5, delay_seconds=10):
        logger.info(f"Starting orders producer for topic: {self.topic}")
        while True:
            try:
                carts = self.fetch_carts_from_dummyjson(limit=batch_size)
                if not carts:
                    logger.warning("No carts fetched ")
                    time.sleep(delay_seconds)
                    continue
                for cart in carts:
                    order = self.transform_cart_to_order(cart)
                    self.send_order(order)
                logger.info(f"Processed {len(carts)} orders from DummyJSON")
                time.sleep(delay_seconds) # avoid rate limiting
            except KeyboardInterrupt:
                logger.info("Stopping orders producer...")
                break
            except Exception as e:
                logger.error(f"Error in orders producer: {e}")
                time.sleep(delay_seconds)
        
        self.producer.close()

if __name__ == "__main__":
    producer = OrdersProducer()
    producer.produce_orders(batch_size=10, delay_seconds=15)
