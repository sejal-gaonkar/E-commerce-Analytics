import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


# Kafka configuration
conf = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(conf)


# Product master data
# These prices match the products stored in PostgreSQL
products = {
    101: {"name": "Wireless Mouse", "price": 599.00},
    102: {"name": "Mechanical Keyboard", "price": 2499.00},
    103: {"name": "USB-C Cable", "price": 399.00},
    104: {"name": "Laptop Stand", "price": 1499.00},
    105: {"name": "Bluetooth Speaker", "price": 2999.00},
    106: {"name": "Smart Watch", "price": 4999.00},
    107: {"name": "Backpack", "price": 1799.00},
    108: {"name": "Power Bank", "price": 1999.00},
    109: {"name": "Headphones", "price": 3499.00},
    110: {"name": "Webcam", "price": 2799.00}
}


customer_ids = list(range(1, 11))
product_ids = list(products.keys())

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking"
]


# Start from 30061 so we don't conflict with our earlier test data
order_id = 30061


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Order {msg.key().decode()} sent to "
            f"partition {msg.partition()}"
        )


print("Real-time e-commerce order producer started...")
print("Press Ctrl+C to stop.\n")


try:

    while True:

        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)

        quantity = random.randint(1, 5)

        product = products[product_id]

        unit_price = product["price"]

        amount = round(quantity * unit_price, 2)

        current_time = datetime.now(timezone.utc).isoformat()

        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "amount": amount,
            "order_time": current_time,
            "status": "CONFIRMED",

            "payment_id": order_id + 50000,
            "payment_method": random.choice(payment_methods),
            "payment_status": "SUCCESS",
            "payment_time": current_time
        }

        producer.produce(
            topic="orders",
            key=str(order_id),
            value=json.dumps(order),
            callback=delivery_report
        )

        producer.poll(0)

        print(json.dumps(order, indent=2))

        order_id += 1

        # Generate one order every 2 seconds
        time.sleep(2)


except KeyboardInterrupt:

    print("\nStopping producer...")


finally:

    producer.flush()

    print("Producer stopped.")