import json
import random
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC      = "orders-stream"
CITIES     = ["Mumbai","Delhi","Bangalore","Pune","Hyderabad","Chennai","Kolkata"]
CATEGORIES = ["Electronics","Fashion","Grocery","Books","Sports","Beauty"]
GATEWAYS   = ["Razorpay","PayU","Paytm","UPI","Credit Card"]
STATUSES   = ["placed","confirmed","shipped"]

print("=" * 50)
print("SmartCommerce Kafka Producer")
print(f"Sending to topic: '{TOPIC}'")
print("Press Ctrl+C to stop")
print("=" * 50)

order_count = 0

while True:
    try:
        order = {
            "order_id":        str(uuid.uuid4()),
            "user_id":         str(uuid.uuid4()),
            "timestamp":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":          random.choices(STATUSES, weights=[50,30,20])[0],
            "city":            random.choice(CITIES),
            "category":        random.choice(CATEGORIES),
            "amount":          round(random.uniform(200, 8000), 2),
            "quantity":        random.randint(1, 5),
            "payment_gateway": random.choice(GATEWAYS),
        }

        producer.send(TOPIC, value=order)
        producer.flush()
        order_count += 1

        print(f"[{order_count:>4}] Sent | {order['city']:<12} | "
              f"{order['category']:<15} | Rs.{order['amount']:>8,.0f} | "
              f"{order['payment_gateway']}")

        time.sleep(1)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total sent: {order_count}")
        producer.close()
        break