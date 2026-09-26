#!/usr/bin/env python3
"""Matcha PubSub - Customer Client.

Provides both HTTP API and direct Kafka client interfaces to place orders,
poll fulfillment statuses, and simulate drinking and cup return workflows.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import random
import sys
import time
from typing import Any, Tuple
import uuid

try:
    import requests
except ImportError:
    requests = None

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("matcha.client")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


# ---------------------------------------------------------------------------
# Presets & Fallback Catalog
# ---------------------------------------------------------------------------
FALLBACK_CUSTOMERS: list[str] = [
    "Maya", "Kenji", "Liam", "Zara", "Amina", "Oliver", "Chloe", "Sam", "Hossam"
]

FALLBACK_MENU: list[dict[str, Any]] = [
    {"name": "Iced Ceremonial Matcha Latte", "price": 6.50, "category": "Drink"},
    {"name": "Hot Uji Matcha Latte", "price": 6.00, "category": "Drink"},
    {"name": "Strawberry Matcha Float", "price": 7.50, "category": "Drink"},
    {"name": "Matcha Espresso Fusion", "price": 6.75, "category": "Drink"},
    {"name": "Matcha Soft Serve", "price": 4.50, "category": "Dessert"},
]

FALLBACK_MILKS: list[str] = ["Oat Milk", "Almond Milk", "Whole Milk", "Soy Milk"]
FALLBACK_SWEETNESS: list[str] = ["0% (Unsweetened)", "25%", "50%", "75%", "100%"]

# Exported aliases for rush.py and external scripts
MENU: list[str] = [item["name"] for item in FALLBACK_MENU]
MILKS: list[str] = list(FALLBACK_MILKS)
SWEETNESS_LEVELS: list[str] = list(FALLBACK_SWEETNESS)


class Color:
    """Terminal ANSI escape codes for client interactions."""
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


def fetch_live_catalog(api_url: str) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Fetches live menu items and customization options from the Order API.

    Args:
        api_url: Base URL of the Matcha Order API service.

    Returns:
        tuple[list[dict[str, Any]], list[str], list[str]]: Products, milks, and sweetness levels.
    """
    if not requests:
        return FALLBACK_MENU, FALLBACK_MILKS, FALLBACK_SWEETNESS

    try:
        resp = requests.get(f"{api_url}/menu", timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            products = data.get("products", [])
            milks = data.get("available_milks", FALLBACK_MILKS)
            sweetness = data.get("available_sweetness", FALLBACK_SWEETNESS)
            if products:
                return products, milks, sweetness
    except Exception as exc:
        logger.debug("Could not fetch live catalog from %s: %s", api_url, exc)

    return FALLBACK_MENU, FALLBACK_MILKS, FALLBACK_SWEETNESS


def prompt_user_order(
    menu: list[dict[str, Any]],
    milks: list[str],
    sweetness_levels: list[str],
) -> tuple[str, str, str, str]:
    """Prompts the user interactively in the terminal to configure their order.

    Args:
        menu: Product list.
        milks: Allowed milk choices.
        sweetness_levels: Allowed sweetness choices.

    Returns:
        tuple[str, str, str, str]: (customer_name, drink, milk, sweetness)
    """
    print(f"\n{Color.BOLD}{Color.GREEN}🍵 Welcome to the Matcha Café!{Color.RESET}")
    name = input(f"{Color.CYAN}What is your name? {Color.RESET}").strip() or "Guest"

    print(f"\n{Color.BOLD}Select your item (Live Menu):{Color.RESET}")
    for i, item in enumerate(menu, 1):
        price_str = f"${item.get('price', 6.50):.2f}"
        cat_str = f"[{item.get('category', 'Drink')}]"
        print(f"  [{i}] {item['name']:<30} {price_str:>8}  {Color.DIM}{cat_str}{Color.RESET}")

    choice = input(f"Choice (1-{len(menu)}): ").strip()
    try:
        selected_item = menu[int(choice) - 1]
        drink = selected_item["name"]
    except (ValueError, IndexError):
        drink = menu[0]["name"]

    print(f"\n{Color.BOLD}Select milk preference:{Color.RESET}")
    for i, m in enumerate(milks, 1):
        print(f"  [{i}] {m}")
    m_choice = input(f"Choice (1-{len(milks)}): ").strip()
    try:
        milk = milks[int(m_choice) - 1]
    except (ValueError, IndexError):
        milk = milks[0]

    print(f"\n{Color.BOLD}Select sweetness level:{Color.RESET}")
    for i, s in enumerate(sweetness_levels, 1):
        print(f"  [{i}] {s}")
    s_choice = input(f"Choice (1-{len(sweetness_levels)}): ").strip()
    try:
        sweetness = sweetness_levels[int(s_choice) - 1]
    except (ValueError, IndexError):
        sweetness = sweetness_levels[2]

    return name, drink, milk, sweetness


def run_api_client(
    api_url: str,
    name: str,
    drink: str,
    milk: str,
    sweetness: str,
    timeout: float = 60.0,
) -> None:
    """Submits order through HTTP Order API and polls status.

    Args:
        api_url: Base Order API endpoint.
        name: Customer name.
        drink: Target drink item.
        milk: Milk option.
        sweetness: Sweetness option.
        timeout: Maximum seconds to wait for fulfillment.
    """
    if not requests:
        print(f"{Color.RED}Error: 'requests' package is required for API mode.{Color.RESET}")
        sys.exit(1)

    payload = {
        "customer_name": name,
        "drink_name": drink,
        "drink": drink,
        "milk": milk,
        "sweetness": sweetness,
    }

    print(f"\n{Color.CYAN}📡 Submitting order to Matcha Order API at {api_url}...{Color.RESET}")
    try:
        resp = requests.post(f"{api_url}/orders", json=payload, timeout=5.0)
    except requests.exceptions.ConnectionError:
        print(f"{Color.RED}❌ Could not connect to Order API at {api_url}. Is the service running?{Color.RESET}")
        sys.exit(1)

    if resp.status_code != 201:
        print(f"{Color.RED}❌ Order rejected ({resp.status_code}): {resp.text}{Color.RESET}")
        sys.exit(1)

    order_info = resp.json()
    order_id = order_info["order_id"]
    price = order_info["price"]

    print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
    print(f"{Color.BOLD}│ 🧾 ORDER CONFIRMED BY ORDER API                          │{Color.RESET}")
    print(f"{Color.BOLD}│ Order ID : {Color.YELLOW}{order_id:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Customer : {Color.CYAN}{name:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Item     : {drink:<46}│{Color.RESET}")
    print(f"{Color.BOLD}│ Price    : ${price:<45.2f}│{Color.RESET}")
    print(f"{Color.BOLD}│ Status   : {Color.MAGENTA}{order_info['status']:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")
    print(f"\n{Color.YELLOW}⏳ {name} is waiting for order status updates via API...{Color.RESET}\n")

    start_time = time.time()
    last_status = "PENDING"

    while time.time() - start_time < timeout:
        time.sleep(1.5)
        try:
            status_resp = requests.get(f"{api_url}/orders/{order_id}", timeout=3.0)
            if status_resp.status_code == 200:
                current_order = status_resp.json()
                current_status = current_order.get("status")

                if current_status == "PREPARING" and last_status != "PREPARING":
                    barista = current_order.get("prepared_by") or "The Barista"
                    print(f"  {Color.CYAN}🥣 {barista} started brewing your order!{Color.RESET}")
                    last_status = "PREPARING"

                elif current_status == "READY":
                    barista = current_order.get("prepared_by") or "The Barista"
                    cup_code = current_order.get("cup_code") or "Ceramic Cup"
                    print(f"{Color.BOLD}{Color.GREEN}🎉 DING! {barista} announced: 'Order {order_id} for {name} in {cup_code}!'{Color.RESET}")
                    print(f"{Color.GREEN}   Drink : {drink} ({milk}, {sweetness}){Color.RESET}")
                    print(f"\n{Color.BOLD}🍵 {name} picked up the freshly brewed matcha in {Color.MAGENTA}{cup_code}{Color.RESET}! Enjoy! 😋✨\n")

                    # Simulate drinking time
                    print(f"{Color.DIM}☕ {name} is sipping matcha at the café table...{Color.RESET}")
                    time.sleep(2.0)

                    # Return cup to dishwasher station via API
                    print(f"{Color.CYAN}🍽️ {name} finished the drink and returned {Color.MAGENTA}{cup_code}{Color.RESET}{Color.CYAN} to the dishwasher counter!{Color.RESET}\n")
                    try:
                        requests.post(f"{api_url}/cups/{cup_code}/return?customer_name={name}", timeout=3.0)
                    except Exception as ret_err:
                        logger.warning("Could not notify cup return via API: %s", ret_err)
                    return
        except Exception as exc:
            logger.debug("Order polling exception: %s", exc)

    print(f"{Color.RED}⏰ Order wait timeout exceeded ({timeout}s). Barista is still crafting your drink.{Color.RESET}")


def run_direct_kafka_client(
    bootstrap_server: str,
    orders_topic: str,
    ready_topic: str,
    name: str,
    drink: str,
    milk: str,
    sweetness: str,
    timeout: float = 60.0,
) -> None:
    """Direct Kafka communication fallback path."""
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

    order_id = f"ORD-{random.randint(1000, 9999)}"
    producer = Producer({"bootstrap.servers": bootstrap_server, "broker.address.family": "v4"})
    client_group = f"customer-{order_id}-{uuid.uuid4().hex[:4]}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap_server,
        "group.id": client_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "broker.address.family": "v4",
    })
    consumer.subscribe([ready_topic])

    order_event = {
        "order_id": order_id,
        "client_name": name,
        "drink_name": drink,
        "drink": drink,
        "milk": milk,
        "sweetness": sweetness,
        "ordered_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
    print(f"{Color.BOLD}│ 🧾 ORDER PLACED DIRECTLY VIA KAFKA                       │{Color.RESET}")
    print(f"{Color.BOLD}│ Order ID : {Color.YELLOW}{order_id:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Customer : {Color.CYAN}{name:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
    print(f"{Color.BOLD}│ Item     : {drink:<46}│{Color.RESET}")
    print(f"{Color.BOLD}│ Options  : {milk}, {sweetness} sweetness{' ' * max(0, 31 - len(milk) - len(sweetness))}│{Color.RESET}")
    print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

    producer.produce(
        topic=orders_topic,
        key=order_id.encode("utf-8"),
        value=json.dumps(order_event).encode("utf-8"),
    )
    producer.flush()

    print(f"\n{Color.YELLOW}⏳ {name} is waiting by the pickup counter (listening to '{ready_topic}')...{Color.RESET}\n")

    start_time = time.time()
    try:
        while time.time() - start_time < timeout:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            announced_key = msg.key().decode("utf-8") if msg.key() else ""
            if announced_key == order_id:
                ready_data = json.loads(msg.value().decode("utf-8"))
                prep_by = ready_data.get("prepared_by", "The Waiter")
                cup_code = ready_data.get("cup_code", "Ceramic Cup")
                print(f"{Color.BOLD}{Color.GREEN}🎉 DING! Barista called: 'Order {order_id} for {name} in {cup_code}!'{Color.RESET}")
                print(f"{Color.GREEN}   Prepared by : {prep_by}{Color.RESET}")
                print(f"{Color.GREEN}   Ready Drink : {drink} ({milk}, {sweetness}){Color.RESET}")
                print(f"\n{Color.BOLD}🍵 {name} picked up the matcha in {Color.MAGENTA}{cup_code}{Color.RESET}! Enjoy! 😋✨\n")

                time.sleep(2.0)

                returns_topic = "matcha-cup-returns"
                print(f"\n{Color.CYAN}🍽️ {name} finished the drink and returned {Color.MAGENTA}{cup_code}{Color.RESET}{Color.CYAN} to the dishwasher counter!{Color.RESET}\n")
                return_event = {
                    "cup_code": cup_code,
                    "customer_name": name,
                    "order_id": order_id,
                    "returned_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.produce(
                    topic=returns_topic,
                    key=cup_code.encode("utf-8"),
                    value=json.dumps(return_event).encode("utf-8"),
                )
                producer.flush()
                return
    finally:
        consumer.close()

    print(f"{Color.RED}⏰ Waited {timeout}s, but order was not called.{Color.RESET}")


def main() -> None:
    """CLI customer entrypoint with mode switching."""
    parser = argparse.ArgumentParser(description="Matcha Café Customer Client")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Order API URL")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--orders-topic", default="matcha-orders", help="Orders Kafka topic")
    parser.add_argument("--ready-topic", default="matcha-ready", help="Ready orders Kafka topic")
    parser.add_argument("--name", help="Customer name (skips interactive prompt)")
    parser.add_argument("--drink", help="Drink name (skips interactive prompt)")
    parser.add_argument("--milk", help="Milk preference (skips interactive prompt)")
    parser.add_argument("--sweetness", help="Sweetness level (skips interactive prompt)")
    parser.add_argument("--direct-kafka", action="store_true", help="Bypass API and publish directly to Kafka")
    parser.add_argument("--timeout", type=float, default=60.0, help="Max wait seconds for drink completion")
    args = parser.parse_args()

    api_url = os.environ.get("API_URL", args.api_url)
    bootstrap_server = os.environ.get("BOOTSTRAP_SERVER", args.bootstrap_server)
    orders_topic = os.environ.get("ORDERS_TOPIC", args.orders_topic)
    ready_topic = os.environ.get("READY_TOPIC", args.ready_topic)

    menu_items, milks, sweetness_levels = fetch_live_catalog(api_url)

    if args.name and args.drink:
        name = args.name
        drink = args.drink
        milk = args.milk or milks[0]
        sweetness = args.sweetness or sweetness_levels[2]
    else:
        name, drink, milk, sweetness = prompt_user_order(menu_items, milks, sweetness_levels)

    if args.direct-kafka:
        run_direct_kafka_client(
            bootstrap_server=bootstrap_server,
            orders_topic=orders_topic,
            ready_topic=ready_topic,
            name=name,
            drink=drink,
            milk=milk,
            sweetness=sweetness,
            timeout=args.timeout,
        )
    else:
        run_api_client(
            api_url=api_url,
            name=name,
            drink=drink,
            milk=milk,
            sweetness=sweetness,
            timeout=args.timeout,
        )


if __name__ == "__main__":
    main()
