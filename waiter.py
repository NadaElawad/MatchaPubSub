#!/usr/bin/env python3
"""
Matcha PubSub - Solo Waiter (Barista Service)

Role:
- Subscribes to 'matcha-orders' as the sole worker in 'matcha-waiter-group'.
- Prepares matcha orders one-by-one with realistic brewing steps.
- Publishes completed orders to 'matcha-ready' with key=order_id.
- Commits offsets so orders are never double-processed.
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from db import get_product_price, log_order


class Color:
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


running = True


def handle_shutdown(sig, frame):
    global running
    print(f"\n{Color.YELLOW}🧑‍🍳 Waiter is closing the matcha bar for today... Goodbye!{Color.RESET}")
    running = False


def main():
    parser = argparse.ArgumentParser(description="Solo Waiter / Barista Service")
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server (default: localhost:9092)",
    )
    parser.add_argument(
        "--orders-topic",
        default="matcha-orders",
        help="Incoming orders topic (default: matcha-orders)",
    )
    parser.add_argument(
        "--ready-topic",
        default="matcha-ready",
        help="Completed orders topic (default: matcha-ready)",
    )
    parser.add_argument(
        "--prep-time",
        type=float,
        default=10,
        help="Total preparation time in seconds per drink (default: 10)",
    )
    args = parser.parse_args()

    bootstrap_server = os.environ.get("BOOTSTRAP_SERVER", args.bootstrap_server)
    orders_topic = os.environ.get("ORDERS_TOPIC", args.orders_topic)
    ready_topic = os.environ.get("READY_TOPIC", args.ready_topic)
    prep_time = float(os.environ.get("PREP_TIME", args.prep_time))

    # Consumer configuration: single consumer in matcha-waiter-group
    consumer_conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": "matcha-waiter-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit after drink is prepared!
    }

    # Producer configuration: sends ready events
    producer_conf = {
        "bootstrap.servers": bootstrap_server,
        "client.id": "matcha-waiter-producer",
    }

    try:
        consumer = Consumer(consumer_conf)
        producer = Producer(producer_conf)
    except Exception as e:
        print(f"{Color.RED}Initialization failed: {e}{Color.RESET}")
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    consumer.subscribe([orders_topic])

    print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}│  🍵 MATCHA CAFÉ - SOLO WAITER (BARISTA) READY            │{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}│  Kafka Server : {bootstrap_server:<40} │{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}│  Orders Topic : {orders_topic:<40} │{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}│  Ready Topic  : {ready_topic:<40} │{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")
    print(f"{Color.DIM}Waiting for customers to place orders... (Press Ctrl+C to close shop)\n{Color.RESET}")

    step_delay = prep_time / 3.0
    orders_served = 0

    try:
        while running:
            # Poll for new incoming orders
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                    KafkaError._UNKNOWN_TOPIC,
                ):
                    time.sleep(0.5)
                    continue
                raise KafkaException(msg.error())

            # Parse order details
            try:
                order_data = json.loads(msg.value().decode("utf-8"))
            except Exception:
                order_data = {
                    "order_id": msg.key().decode("utf-8") if msg.key() else "UNKNOWN",
                    "client_name": "Customer",
                    "drink": msg.value().decode("utf-8", errors="replace"),
                }

            order_id = order_data.get("order_id", "UNKNOWN")
            client_name = order_data.get("client_name", "Anonymous")
            drink = order_data.get("drink", "Matcha Special")
            milk = order_data.get("milk", "Standard")
            sweetness = order_data.get("sweetness", "Normal")

            orders_served += 1
            print(f"{Color.BOLD}{Color.CYAN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
            print(f"{Color.BOLD}│ 📋 NEW TICKET #{orders_served:<3} | Order: {Color.YELLOW}{order_id:<12}{Color.RESET}{Color.BOLD}         │{Color.RESET}")
            print(f"{Color.BOLD}│ Customer : {Color.GREEN}{client_name:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
            print(f"{Color.BOLD}│ Drink    : {drink:<46}│{Color.RESET}")
            print(f"{Color.BOLD}│ Options  : {milk}, {sweetness} sweetness{' ' * max(0, 31 - len(milk) - len(sweetness))}│{Color.RESET}")
            print(f"{Color.BOLD}{Color.CYAN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

            # Simulate handcrafted matcha preparation steps
            time.sleep(step_delay)
            print(f"   {Color.GREEN}🍃 Step 1:{Color.RESET} Sifting ceremonial Uji matcha into chawan...")
            time.sleep(step_delay)
            print(f"   {Color.GREEN}🥣 Step 2:{Color.RESET} Whisking rapidly with bamboo chasen until silky foam forms...")
            time.sleep(step_delay)
            print(f"   {Color.GREEN}🥛 Step 3:{Color.RESET} Pouring fresh {milk} and ice into cup...")

            # Fetch price from products table
            price = get_product_price(drink)

            # Construct ready event
            ready_event = {
                "order_id": order_id,
                "client_name": client_name,
                "drink": drink,
                "milk": milk,
                "sweetness": sweetness,
                "price": price,
                "status": "READY",
                "prepared_by": "Kaito (Solo Waiter)",
                "ready_at": datetime.now(timezone.utc).isoformat(),
            }

            # Publish to 'matcha-ready' with key=order_id
            producer.produce(
                topic=ready_topic,
                key=order_id.encode("utf-8"),
                value=json.dumps(ready_event).encode("utf-8"),
            )
            producer.flush()

            # Record into PostgreSQL order_logs table
            log_order(ready_event)

            # Commit offset to Kafka to confirm order is fulfilled
            consumer.commit(msg)

            print(f"   {Color.BOLD}{Color.YELLOW}🔔 DING! Order {order_id} for {client_name} (${price:.2f}) is READY at the counter!{Color.RESET}\n")

    except KeyboardInterrupt:
        pass
    finally:
        print("\nCleaning up resources...")
        consumer.close()
        producer.flush()
        print(f"{Color.GREEN}Barista closed shop. Total orders completed today: {orders_served}{Color.RESET}")


if __name__ == "__main__":
    main()
