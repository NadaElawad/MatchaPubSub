#!/usr/bin/env python3
"""
Kafka Producer Script for Learning & Prototyping.

Features:
- Interactive Mode: Type messages in real-time or send sample JSON orders.
- Automated Stream Mode: Generate a stream of realistic simulated events.
- Single Message Mode: Send a one-off message via CLI flag.
- Delivery Callbacks: Shows the partition and offset assigned to each message.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from confluent_kafka import Producer


# ANSI Color helpers for readable console output
class Color:
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


def delivery_report(err, msg):
    """Callback invoked when a message has been delivered or failed."""
    if err is not None:
        print(f"{Color.RED}[ERROR] Delivery failed: {err}{Color.RESET}", file=sys.stderr)
    else:
        key_str = f"key='{msg.key().decode()}' " if msg.key() else ""
        print(
            f"{Color.GREEN}✔ Delivered{Color.RESET} {key_str}"
            f"to topic {Color.CYAN}{msg.topic()}{Color.RESET} "
            f"[partition {Color.BOLD}{msg.partition()}{Color.RESET}] "
            f"at offset {Color.BOLD}{msg.offset()}{Color.RESET}"
        )


def generate_sample_order():
    """Generate a realistic sample event for a Matcha Café."""
    items = [
        ("Iced Matcha Latte", 6.50),
        ("Ceremonial Hot Matcha", 7.00),
        ("Strawberry Matcha Float", 7.50),
        ("Matcha Soft Serve", 4.50),
        ("Matcha Basque Cheesecake", 8.00),
        ("Matcha Espresso Fusion", 6.75),
    ]
    milks = ["Oat Milk", "Almond Milk", "Whole Milk", "Soy Milk"]
    customers = ["Maya", "Liam", "Kenji", "Zara", "Amina", "Oliver", "Chloe"]

    item_name, price = random.choice(items)
    order_id = f"ORD-{random.randint(1000, 9999)}"
    customer = random.choice(customers)

    return {
        "order_id": order_id,
        "customer": customer,
        "item": item_name,
        "price": price,
        "milk": random.choice(milks),
        "sweetness_level": random.choice(["0%", "25%", "50%", "75%", "100%"]),
        "status": "PLACED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Producer for learning and testing message flows."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="matcha-orders",
        help="Target topic name (default: matcha-orders)",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Automatically generate a stream of sample orders",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Number of messages to produce in auto mode (0 for infinite, default: 5)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between messages in auto mode (default: 1.0)",
    )
    parser.add_argument(
        "--message",
        type=str,
        default=None,
        help="Send a single message directly and exit",
    )
    parser.add_argument(
        "--key",
        type=str,
        default=None,
        help="Optional message key (determines partition routing)",
    )

    args = parser.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap_server,
        "client.id": "matcha-producer-py",
    }

    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"{Color.RED}Failed to initialize Producer: {e}{Color.RESET}")
        sys.exit(1)

    print(f"\n{Color.BOLD}🌿 Kafka Producer Started{Color.RESET}")
    print(f"Connecting to: {Color.CYAN}{args.bootstrap_server}{Color.RESET}")
    print(f"Target Topic:  {Color.CYAN}{args.topic}{Color.RESET}")

    # Mode 1: Single message mode
    if args.message is not None:
        key_bytes = args.key.encode("utf-8") if args.key else None
        value_bytes = args.message.encode("utf-8")
        producer.produce(
            args.topic,
            key=key_bytes,
            value=value_bytes,
            callback=delivery_report,
        )
        producer.flush()
        print(f"\n{Color.GREEN}Done!{Color.RESET}")
        return

    # Mode 2: Auto stream mode
    if args.auto:
        print(
            f"{Color.YELLOW}Generating sample messages "
            f"(count={args.count if args.count > 0 else 'infinite'}, delay={args.delay}s)...{Color.RESET}\n"
        )
        sent = 0
        try:
            while args.count == 0 or sent < args.count:
                order = generate_sample_order()
                key = order["order_id"]
                value = json.dumps(order)

                producer.produce(
                    args.topic,
                    key=key.encode("utf-8"),
                    value=value.encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                sent += 1
                time.sleep(args.delay)
        except KeyboardInterrupt:
            print(f"\n{Color.YELLOW}Streaming stopped by user.{Color.RESET}")

        print(f"\nFlushing pending messages...")
        producer.flush()
        print(f"{Color.GREEN}Completed sending {sent} messages.{Color.RESET}")
        return

    # Mode 3: Interactive mode
    print(f"\n{Color.BOLD}Interactive Mode Guide:{Color.RESET}")
    print(f" - Type any text to send it as a message.")
    print(f" - Type {Color.CYAN}sample{Color.RESET} to send an auto-generated JSON order.")
    print(f" - Type {Color.CYAN}key:value{Color.RESET} to send a message with a specific key.")
    print(f" - Type {Color.CYAN}exit{Color.RESET} or press {Color.CYAN}Ctrl+C{Color.RESET} to quit.\n")

    try:
        while True:
            try:
                line = input(f"{Color.BOLD}producer > {Color.RESET}").strip()
            except EOFError:
                break

            if not line:
                continue

            if line.lower() in ("exit", "quit"):
                break

            if line.lower() == "sample":
                order = generate_sample_order()
                key = order["order_id"]
                value = json.dumps(order, indent=2)
                print(f"{Color.DIM}Sending order:{Color.RESET}\n{value}")
                producer.produce(
                    args.topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(order).encode("utf-8"),
                    callback=delivery_report,
                )
            elif ":" in line:
                parts = line.split(":", 1)
                k = parts[0].strip()
                v = parts[1].strip()
                producer.produce(
                    args.topic,
                    key=k.encode("utf-8"),
                    value=v.encode("utf-8"),
                    callback=delivery_report,
                )
            else:
                producer.produce(
                    args.topic,
                    value=line.encode("utf-8"),
                    callback=delivery_report,
                )

            # Serve delivery callbacks from previous produce calls
            producer.poll(0)

    except KeyboardInterrupt:
        print(f"\n{Color.YELLOW}Exiting...{Color.RESET}")

    print("\nFlushing remaining messages...")
    producer.flush()
    print(f"{Color.GREEN}Producer closed.{Color.RESET}")


if __name__ == "__main__":
    main()
