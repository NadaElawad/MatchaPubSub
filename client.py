#!/usr/bin/env python3
"""
Matcha PubSub - Customer Client (Approach 1: Shared Pickup Counter)

Role:
- Places an order by producing to 'matcha-orders' with a unique order_id.
- Waits at the pickup counter by listening to 'matcha-ready'.
- Filters incoming messages by key (order_id):
  - If another customer's order is called, ignores it.
  - When their own order_id is called, grabs the matcha and enjoys!
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

CUSTOMERS = ["Maya", "Kenji", "Liam", "Zara", "Amina", "Oliver", "Chloe", "Sam", "Hossam"]


class Color:
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


MENU = [
    "Iced Ceremonial Matcha Latte",
    "Hot Uji Matcha Latte",
    "Strawberry Matcha Float",
    "Matcha Espresso Fusion",
    "Matcha Soft Serve",
]

MILKS = ["Oat Milk", "Almond Milk", "Whole Milk", "Soy Milk"]
SWEETNESS_LEVELS = ["0% (Unsweetened)", "25%", "50%", "75%", "100%"]


def prompt_user_order():
    """Interactive CLI menu if arguments were not provided."""
    print(f"\n{Color.BOLD}{Color.GREEN}🍵 Welcome to the Matcha Café!{Color.RESET}")
    name = input(f"{Color.CYAN}What is your name? {Color.RESET}").strip() or "Guest"

    print(f"\n{Color.BOLD}Select your drink:{Color.RESET}")
    for i, item in enumerate(MENU, 1):
        print(f"  [{i}] {item}")
    choice = input(f"Choice (1-{len(MENU)}): ").strip()
    try:
        drink = MENU[int(choice) - 1]
    except (ValueError, IndexError):
        drink = MENU[0]

    print(f"\n{Color.BOLD}Select milk preference:{Color.RESET}")
    for i, m in enumerate(MILKS, 1):
        print(f"  [{i}] {m}")
    m_choice = input(f"Choice (1-{len(MILKS)}): ").strip()
    try:
        milk = MILKS[int(m_choice) - 1]
    except (ValueError, IndexError):
        milk = MILKS[0]

    print(f"\n{Color.BOLD}Select sweetness level:{Color.RESET}")
    for i, s in enumerate(SWEETNESS_LEVELS, 1):
        print(f"  [{i}] {s}")
    s_choice = input(f"Choice (1-{len(SWEETNESS_LEVELS)}): ").strip()
    try:
        sweetness = SWEETNESS_LEVELS[int(s_choice) - 1]
    except (ValueError, IndexError):
        sweetness = SWEETNESS_LEVELS[2]

    return name, drink, milk, sweetness


def main():
    parser = argparse.ArgumentParser(description="Matcha Café Customer")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--orders-topic", default="matcha-orders")
    parser.add_argument("--ready-topic", default="matcha-ready")
    parser.add_argument("--name", type=str, default=None, help="Customer name")
    parser.add_argument("--drink", type=str, default=None, help="Drink name")
    parser.add_argument("--milk", type=str, default="Oat Milk", help="Milk choice")
    parser.add_argument("--sweetness", type=str, default="50%", help="Sweetness level")
    parser.add_argument("--timeout", type=int, default=60, help="Max wait time in seconds")
    args = parser.parse_args()

    bootstrap_server = os.environ.get("BOOTSTRAP_SERVER", args.bootstrap_server)
    orders_topic = os.environ.get("ORDERS_TOPIC", args.orders_topic)
    ready_topic = os.environ.get("READY_TOPIC", args.ready_topic)

    # Determine order details (interactive if TTY, otherwise randomized/env-driven)
    if args.name is None and args.drink is None:
        if sys.stdin.isatty():
            name, drink, milk, sweetness = prompt_user_order()
        else:
            name = os.environ.get("CUSTOMER_NAME", random.choice(CUSTOMERS))
            drink = os.environ.get("CUSTOMER_DRINK", random.choice(MENU))
            milk = os.environ.get("CUSTOMER_MILK", random.choice(MILKS))
            sweetness = os.environ.get("CUSTOMER_SWEETNESS", random.choice(SWEETNESS_LEVELS))
    else:
        name = args.name or os.environ.get("CUSTOMER_NAME", "Customer")
        drink = args.drink or os.environ.get("CUSTOMER_DRINK", random.choice(MENU))
        milk = args.milk or os.environ.get("CUSTOMER_MILK", "Oat Milk")
        sweetness = args.sweetness or os.environ.get("CUSTOMER_SWEETNESS", "50%")

    order_id = f"ORD-{random.randint(1000, 9999)}"

    # 1. Initialize Producer to submit the order
    producer = Producer({"bootstrap.servers": bootstrap_server})

    # 2. Initialize Consumer to listen at the pickup counter
    # We use a unique ad-hoc consumer group for this customer so it independently receives broadcasts
    client_group = f"customer-{order_id}-{uuid.uuid4().hex[:4]}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap_server,
        "group.id": client_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    # Subscribe to the pickup counter topic
    consumer.subscribe([ready_topic])

    # 3. Submit Order Event to orders topic
    order_event = {
        "order_id": order_id,
        "client_name": name,
        "drink": drink,
        "milk": milk,
        "sweetness": sweetness,
        "ordered_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
    print(f"{Color.BOLD}│ 🧾 ORDER PLACED AT REGISTER                              │{Color.RESET}")
    print(f"{Color.BOLD}│ Order ID : {Color.YELLOW}{order_id:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Customer : {Color.CYAN}{name:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Item     : {drink:<46}│{Color.RESET}")
    print(f"{Color.BOLD}│ Options  : {milk}, {sweetness} sweetness{' ' * max(0, 31 - len(milk) - len(sweetness))}│{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

    # Produce with key=order_id
    producer.produce(
        topic=orders_topic,
        key=order_id.encode("utf-8"),
        value=json.dumps(order_event).encode("utf-8"),
    )
    producer.flush()

    print(f"\n{Color.YELLOW}⏳ {name} is waiting by the pickup counter (listening to '{ready_topic}')...{Color.RESET}\n")

    # 4. Wait for our order to be called on 'matcha-ready'
    start_time = time.time()
    order_received = False

    try:
        while time.time() - start_time < args.timeout:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            announced_key = msg.key().decode("utf-8") if msg.key() else ""

            # Check if this announcement is for our order!
            if announced_key == order_id:
                ready_data = json.loads(msg.value().decode("utf-8"))
                prep_by = ready_data.get("prepared_by", "The Waiter")

                print(f"{Color.BOLD}{Color.GREEN}🎉 DING! Barista called: 'Order {order_id} for {name}!'{Color.RESET}")
                print(f"{Color.GREEN}   Prepared by : {prep_by}{Color.RESET}")
                print(f"{Color.GREEN}   Ready Drink : {drink} ({milk}, {sweetness}){Color.RESET}")
                print(f"\n{Color.BOLD}🍵 {name} picked up the matcha from the counter. Enjoy! 😋✨{Color.RESET}\n")
                order_received = True
                break
            else:
                # Another customer's order was called!
                try:
                    other_data = json.loads(msg.value().decode("utf-8"))
                    ready_at_str = other_data.get("ready_at")
                    # Only mention announcements that happened AFTER the customer arrived
                    if not ready_at_str or ready_at_str >= order_event["ordered_at"]:
                        other_customer = other_data.get("client_name", "someone else")
                        other_drink = other_data.get("drink", "a drink")
                        print(
                            f"  {Color.DIM}👀 {name} heard '{announced_key} for {other_customer} ({other_drink})' "
                            f"called at the counter. (Not mine, waiting...){Color.RESET}"
                        )
                except Exception:
                    pass

        if not order_received:
            print(f"{Color.RED}⏰ Waited {args.timeout}s, but order was not called. The waiter might be busy!{Color.RESET}")
            sys.exit(1)

    except KeyboardInterrupt:
        print(f"\n{Color.YELLOW}{name} stepped away from the counter.{Color.RESET}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
