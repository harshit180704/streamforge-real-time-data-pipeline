"""Synthetic e-commerce Kafka producer. Run: python src/producer.py --events 30"""
import argparse
import json
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "ecommerce-events"
EVENT_TYPES = ["product_view", "add_to_cart", "purchase", "payment_failed", "inventory_update"]
PRODUCTS = [
    ("prod_1001", "Electronics"), ("prod_1002", "Electronics"),
    ("prod_2001", "Fashion"), ("prod_2002", "Fashion"),
    ("prod_3001", "Home"), ("prod_4001", "Books")
]
REGIONS = ["north", "south", "east", "west", "central"]
PAYMENT_METHODS = ["card", "upi", "net_banking", "wallet"]
running = True


def stop(_sig, _frame):
    global running
    running = False


def build_event():
    event_type = random.choices(EVENT_TYPES, weights=[35, 25, 20, 10, 10])[0]
    product_id, category = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    unit_price = round(random.uniform(199, 4999), 2)
    event = {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "user_id": f"user_{random.randint(1000, 1099)}",
        "product_id": product_id,
        "category": category,
        "region": random.choice(REGIONS),
        "quantity": quantity,
        "unit_price": unit_price,
        "amount": round(quantity * unit_price, 2),
        "currency": "INR"
    }
    if event_type in ("purchase", "payment_failed"):
        event["payment_method"] = random.choice(PAYMENT_METHODS)
        event["order_id"] = f"order_{uuid.uuid4().hex[:12]}"
    if event_type == "payment_failed":
        event["failure_reason"] = random.choice(
            ["insufficient_funds", "bank_timeout", "issuer_declined"]
        )
    if event_type == "inventory_update":
        event["inventory_delta"] = random.choice([-5, -2, 5, 10, 20])
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=30, help="Number of events; 0 means continuous")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    args = parser.parse_args()
    if args.events < 0 or args.interval < 0:
        parser.error("--events and --interval must be non-negative")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1
    )
    sent = 0
    print(f"Publishing to {TOPIC} at {BOOTSTRAP_SERVERS}; Ctrl+C to stop.")
    try:
        while running and (args.events == 0 or sent < args.events):
            event = build_event()
            metadata = producer.send(TOPIC, key=event["user_id"], value=event).get(timeout=30)
            sent += 1
            print(f"[{sent:04d}] partition={metadata.partition} offset={metadata.offset} "
                  f"type={event['event_type']} user={event['user_id']}")
            if args.events == 0 or sent < args.events:
                time.sleep(args.interval)
        producer.flush()
        print(f"Published {sent} event(s).")
    except KafkaError:
        raise
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
