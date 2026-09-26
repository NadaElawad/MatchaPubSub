#!/usr/bin/env python3
"""
Matcha PubSub - Automated Dishwasher & Sanitizer Service

Role:
- Subscribes to 'matcha-cup-returns' topic to receive dirty cups returned by customers.
- Transitions cup status: WITH_CUSTOMER -> IN_DISHWASHER.
- Simulates automated industrial wash, sanitize, and drying cycle.
- Transitions cup status: IN_DISHWASHER -> CLEAN_ON_SHELF.
- Returns cups to circulation so the barista can brew more orders!
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import db


class Color:
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


running = True


def handle_shutdown(sig, frame):
    global running
    print(f"\n{Color.YELLOW}🫧 Dishwasher cycle stopping... Turning off water and power.{Color.RESET}")
    running = False


def main():
    parser = argparse.ArgumentParser(description="Matcha Café Automated Dishwasher")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--returns-topic", default="matcha-cup-returns", help="Cup returns topic")
    parser.add_argument("--clean-topic", default="matcha-cup-clean", help="Clean cups notification topic")
    parser.add_argument("--wash-time", type=float, default=4.0, help="Wash and sanitize time in seconds (default: 4.0)")
    args = parser.parse_args()

    bootstrap_server = os.environ.get("BOOTSTRAP_SERVER", args.bootstrap_server)
    returns_topic = os.environ.get("RETURNS_TOPIC", args.returns_topic)
    clean_topic = os.environ.get("CLEAN_TOPIC", args.clean_topic)
    wash_time = float(os.environ.get("WASH_TIME", args.wash_time))

    consumer_conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": "matcha-dishwasher-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "broker.address.family": "v4",
    }

    producer_conf = {
        "bootstrap.servers": bootstrap_server,
        "client.id": "matcha-dishwasher-producer",
        "broker.address.family": "v4",
    }

    try:
        consumer = Consumer(consumer_conf)
        producer = Producer(producer_conf)
    except Exception as e:
        print(f"{Color.RED}Initialization failed: {e}{Color.RESET}")
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    consumer.subscribe([returns_topic])

    print(f"\n{Color.BOLD}{Color.BLUE}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}│  🫧 MATCHA CAFÉ - DISHWASHER & SANITIZER READY           │{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}│  Kafka Server : {bootstrap_server:<40} │{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}│  Returns Topic: {returns_topic:<40} │{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}│  Wash Cycle   : {wash_time:.1f}s                                    │{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}╰──────────────────────────────────────────────────────────╯{Color.RESET}")
    print(f"{Color.DIM}Listening for dirty cups returned by customers at the counter...{Color.RESET}\n")

    step_delay = wash_time / 3.0
    cups_washed = 0

    try:
        while running:
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

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception:
                data = {"cup_code": msg.key().decode("utf-8") if msg.key() else "UNKNOWN"}

            cup_code = data.get("cup_code") or (msg.key().decode("utf-8") if msg.key() else "UNKNOWN")
            customer = data.get("customer_name") or data.get("client_name") or "A customer"
            order_id = data.get("order_id") or "UNKNOWN"

            cups_washed += 1
            print(f"{Color.BOLD}{Color.BLUE}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
            print(f"{Color.BOLD}│ 🍽️  DIRTY CUP RETURNED #{cups_washed:<2} | Cup: {Color.MAGENTA}{cup_code:<18}{Color.RESET}{Color.BOLD}│{Color.RESET}")
            print(f"{Color.BOLD}│ Returned By: {Color.CYAN}{customer:<44}{Color.RESET}{Color.BOLD}│{Color.RESET}")
            print(f"{Color.BOLD}│ Prior Order: {Color.YELLOW}{order_id:<44}{Color.RESET}{Color.BOLD}│{Color.RESET}")
            print(f"{Color.BOLD}{Color.BLUE}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

            # 1. Update database: WITH_CUSTOMER -> IN_DISHWASHER
            db.return_cup_to_dishwasher(cup_code, actor=f"Customer: {customer}")
            print(f"  {Color.BLUE}🚿 Step 1:{Color.RESET} Rinsing organic matcha residue from {Color.MAGENTA}{cup_code}{Color.RESET}...")
            time.sleep(step_delay)

            # 2. Sanitizing steam cycle
            print(f"  {Color.BLUE}🧼 Step 2:{Color.RESET} High-temperature sanitizing cycle (85°C steam)...")
            time.sleep(step_delay)

            # 3. Drying and shelving
            print(f"  {Color.BLUE}💨 Step 3:{Color.RESET} Hot air drying and inspection complete.")
            time.sleep(step_delay)

            # 4. Update database: IN_DISHWASHER -> CLEAN_ON_SHELF
            db.sanitize_and_shelve_cup(cup_code, actor="Automated Dishwasher")

            # 5. Announce clean cup on Kafka
            clean_event = {
                "cup_code": cup_code,
                "status": "CLEAN_ON_SHELF",
                "sanitized_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(
                topic=clean_topic,
                key=cup_code.encode("utf-8"),
                value=json.dumps(clean_event).encode("utf-8"),
            )
            producer.flush()

            # 6. Commit offset
            consumer.commit(msg)

            print(f"  {Color.BOLD}{Color.GREEN}✨ DING! {cup_code} is sanitized and back on the shelf, ready for the next drink!{Color.RESET}\n")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()
        print(f"\n{Color.GREEN}Dishwasher closed. Total cups washed today: {cups_washed}{Color.RESET}")


if __name__ == "__main__":
    main()
