import json
from collections import defaultdict
from kafka import KafkaConsumer

# Connect to Kafka
consumer = KafkaConsumer(
    "orders-stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",        # Only read new messages
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=-1,            # Run forever
    group_id="smartcommerce-consumer",
)

# Running stats
total_orders    = 0
total_revenue   = 0.0
city_revenue    = defaultdict(float)
category_orders = defaultdict(int)
gateway_count   = defaultdict(int)

print("=" * 60)
print("SmartCommerce Kafka Consumer — Live Dashboard")
print("Waiting for orders... (start producer.py in another terminal)")
print("=" * 60)

try:
    for message in consumer:
        order = message.value

        # Update running stats
        total_orders  += 1
        total_revenue += order["amount"]
        city_revenue[order["city"]]           += order["amount"]
        category_orders[order["category"]]    += 1
        gateway_count[order["payment_gateway"]] += 1

        # Print live dashboard every 5 orders
        if total_orders % 5 == 0:
            top_city     = max(city_revenue,    key=city_revenue.get)
            top_category = max(category_orders, key=category_orders.get)
            top_gateway  = max(gateway_count,   key=gateway_count.get)

            print(f"\n{'='*60}")
            print(f"   Orders received  : {total_orders:,}")
            print(f"   Total revenue    : ₹{total_revenue:>12,.0f}")
            print(f"   Top city         : {top_city}")
            print(f"    Top category     : {top_category}")
            print(f"   Top gateway      : {top_gateway}")
            print(f"   Avg order value  : ₹{total_revenue/total_orders:>10,.0f}")
            print(f"{'='*60}")

            print("\n  Revenue by City:")
            for city, rev in sorted(city_revenue.items(),
                                    key=lambda x: x[1], reverse=True):
                bar = "█" * int(rev / total_revenue * 30)
                print(f"    {city:<12} ₹{rev:>10,.0f}  {bar}")

        else:
            print(f"   Received | {order['city']:<12} | "
                  f"{order['category']:<15} | ₹{order['amount']:>8,.0f}")

except KeyboardInterrupt:
    print(f"\n Consumer stopped.")
    print(f"   Total processed: {total_orders:,} orders")
    print(f"   Total revenue:   ₹{total_revenue:,.0f}")
    consumer.close()