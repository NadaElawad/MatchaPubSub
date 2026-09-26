#!/usr/bin/env python3
"""Matcha PubSub - Solo Waiter (Barista Worker Service).

Subscribes to 'matcha-orders' Kafka topic as the sole worker in 'matcha-waiter-group'.
Simulates realistic artisanal tea whisking and dessert plating steps, manages
finite ceramic cup assignments, transitions PostgreSQL order state, and publishes
ready events to 'matcha-ready'.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import signal
import sys
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
import db

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("matcha.waiter")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


class Color:
    """Terminal ANSI escape codes for beautiful barista console output."""
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


class SoloWaiterService:
    """Barista worker that fulfills incoming orders sequentially."""

    def __init__(
        self,
        bootstrap_server: str,
        orders_topic: str,
        ready_topic: str,
        prep_time: float,
    ) -> None:
        """Initializes the solo waiter service.

        Args:
            bootstrap_server: Kafka broker connection string.
            orders_topic: Incoming orders Kafka topic.
            ready_topic: Ready orders notification topic.
            prep_time: Total artisanal preparation delay in seconds per drink.
        """
        self.bootstrap_server = bootstrap_server
        self.orders_topic = orders_topic
        self.ready_topic = ready_topic
        self.prep_time = prep_time
        self.running = True
        self.orders_served = 0

        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_server,
            "group.id": "matcha-waiter-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        })

        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_server,
            "client.id": "matcha-waiter-producer",
            "broker.address.family": "v4",
        })

    def stop(self) -> None:
        """Signals the worker loop to terminate gracefully."""
        self.running = False

    def run(self) -> None:
        """Main event consumption loop."""
        self.consumer.subscribe([self.orders_topic])
        step_delay = self.prep_time / 3.0

        print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
        print(f"{Color.BOLD}{Color.GREEN}│  🍵 MATCHA CAFÉ - SOLO WAITER (BARISTA) READY            │{Color.RESET}")
        print(f"{Color.BOLD}{Color.GREEN}│  Kafka Server : {self.bootstrap_server:<40} │{Color.RESET}")
        print(f"{Color.BOLD}{Color.GREEN}│  Orders Topic : {self.orders_topic:<40} │{Color.RESET}")
        print(f"{Color.BOLD}{Color.GREEN}│  Ready Topic  : {self.ready_topic:<40} │{Color.RESET}")
        print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")
        print(f"{Color.DIM}Waiting for customers to place orders... (Press Ctrl+C to close shop)\n{Color.RESET}")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
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

                self._process_order_message(msg, step_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received.")
        finally:
            self._cleanup()

    def _process_order_message(self, msg: Message, step_delay: float) -> None:
        """Parses, claims cups, prepares, and dispatches a single order.

        Args:
            msg: Kafka message containing the order JSON payload.
            step_delay: Preparation sleep duration per step.
        """
        try:
            order_data: dict[str, Any] = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.warning("Malformed message payload: %s", exc)
            order_data = {
                "order_id": msg.key().decode("utf-8") if msg.key() else "UNKNOWN",
                "client_name": "Customer",
                "drink": msg.value().decode("utf-8", errors="replace"),
            }

        order_id = order_data.get("order_id", "UNKNOWN")
        client_name = order_data.get("client_name") or order_data.get("customer_name") or "Anonymous"

        raw_items = order_data.get("items")
        if not raw_items:
            drink_name = order_data.get("drink_name") or order_data.get("drink", "Matcha Special")
            milk = order_data.get("milk", "Oat Milk")
            sweetness = order_data.get("sweetness", "50%")
            product = db.get_product_by_name(drink_name)
            category = product.get("category", "Drink") if product else "Drink"
            raw_items = [{
                "drink_name": drink_name,
                "category": category,
                "milk": milk,
                "sweetness": sweetness,
                "quantity": 1,
                "price": float(order_data.get("price", db.DEFAULT_FALLBACK_PRICE)),
            }]

        items_to_prep: list[dict[str, Any]] = []
        claimed_cups: list[str] = []
        total_price: float = 0.0

        for it in raw_items:
            name = it.get("drink_name") or it.get("drink", "Matcha Special")
            product = db.get_product_by_name(name)
            category = product.get("category", "Drink") if product else it.get("category", "Drink")
            is_drink = (category.lower() == "drink")
            unit_price = float(product["price"]) if product else float(it.get("price", db.DEFAULT_FALLBACK_PRICE))
            qty = int(it.get("quantity", 1))

            for _ in range(qty):
                cup_code: str | None = None
                if is_drink:
                    cup_code = db.claim_cup_for_order(order_id, client_name)
                    wait_attempts = 0
                    while cup_code is None and self.running:
                        wait_attempts += 1
                        print(f"   {Color.RED}⚠️ [Cup Shortage] All cups in use! Waiting for clean cups...{Color.RESET}")
                        time.sleep(1.5)
                        cup_code = db.claim_cup_for_order(order_id, client_name)

                        # Auto-expedite if deadlock threatens service
                        if cup_code is None and wait_attempts >= 4:
                            print(f"   {Color.YELLOW}⚡ [Kitchen Expedite] Busser urgently collecting & washing finished cups...{Color.RESET}")
                            try:
                                with db.get_db_connection() as conn:
                                    with conn.cursor() as cur:
                                        cur.execute(
                                            """
                                            UPDATE cups SET status = 'CLEAN_ON_SHELF', current_order_id = NULL, current_customer = NULL
                                            WHERE cup_code IN (
                                                SELECT cup_code FROM cups 
                                                WHERE status != 'IN_BREWING' 
                                                ORDER BY updated_at ASC LIMIT 5
                                            );
                                            """
                                        )
                                        conn.commit()
                            except Exception as exp_err:
                                logger.warning("Kitchen expedite failed: %s", exp_err)
                            cup_code = db.claim_cup_for_order(order_id, client_name)

                    if not self.running:
                        return
                    if cup_code:
                        claimed_cups.append(cup_code)

                items_to_prep.append({
                    "drink_name": name,
                    "name": name,
                    "drink": name,
                    "category": category,
                    "is_drink": is_drink,
                    "quantity": 1,
                    "milk": it.get("milk") if is_drink else None,
                    "sweetness": it.get("sweetness") if is_drink else None,
                    "price": unit_price,
                    "cup_code": cup_code,
                })
                total_price += unit_price

        if not self.running:
            return

        self.orders_served += 1
        print(f"{Color.BOLD}{Color.CYAN}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
        print(f"{Color.BOLD}│ 📋 NEW TICKET #{self.orders_served:<3} | Order: {Color.YELLOW}{order_id:<12}{Color.RESET}{Color.BOLD} ({len(items_to_prep)} items)     │{Color.RESET}")
        print(f"{Color.BOLD}│ Customer : {Color.GREEN}{client_name:<46}{Color.RESET}{Color.BOLD}│{Color.RESET}")
        for idx, item in enumerate(items_to_prep, 1):
            iname = item["drink_name"]
            if item["is_drink"]:
                opt = f"{item['milk']}, {item['sweetness']}"
                print(f"{Color.BOLD}│  {idx}. {iname:<26} ☕ {Color.MAGENTA}{item['cup_code']}{Color.RESET}{Color.BOLD}   │{Color.RESET}")
                print(f"{Color.BOLD}│     {Color.DIM}{opt:<52}{Color.RESET}{Color.BOLD}│{Color.RESET}")
            else:
                print(f"{Color.BOLD}│  {idx}. {iname:<26} 🍽️ {Color.YELLOW}Plate{Color.RESET}{Color.BOLD}        │{Color.RESET}")
        print(f"{Color.BOLD}{Color.CYAN}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

        # Update database status to PREPARING
        db.update_order_status(order_id, db.OrderStatus.PREPARING, prepared_by="Kaito (Solo Waiter)")

        # Realistic artisanal preparation steps
        for item in items_to_prep:
            time.sleep(step_delay)
            if item["is_drink"]:
                print(f"   {Color.GREEN}🍵 Preparing {item['drink_name']}:{Color.RESET} Whisking Uji foam & pouring into {Color.MAGENTA}{item['cup_code']}{Color.RESET}...")
            elif item["category"].lower() == "dessert":
                print(f"   {Color.GREEN}🍦 Preparing {item['drink_name']}:{Color.RESET} Swirling soft serve into chilled glass dish...")
            else:
                print(f"   {Color.GREEN}🍰 Plating {item['drink_name']}:{Color.RESET} Slicing & garnishing on Mino-ware ceramic plate...")

        # Hand cups to customer
        for cup in claimed_cups:
            db.hand_cup_to_customer(cup, order_id=order_id, customer_name=client_name)

        summary_drink = ", ".join(f"{it.get('quantity', 1)}x {it.get('drink_name') or it.get('drink')}" for it in raw_items)

        ready_event = {
            "order_id": order_id,
            "client_name": client_name,
            "drink_name": summary_drink,
            "drink": summary_drink,
            "items": items_to_prep,
            "price": total_price,
            "cup_codes": claimed_cups,
            "cup_code": ", ".join(claimed_cups) if claimed_cups else None,
            "status": db.OrderStatus.READY,
            "prepared_by": "Kaito (Solo Waiter)",
            "ready_at": datetime.now(timezone.utc).isoformat(),
        }

        # Publish ready event to Kafka
        self.producer.produce(
            topic=self.ready_topic,
            key=order_id.encode("utf-8"),
            value=json.dumps(ready_event).encode("utf-8"),
        )
        self.producer.flush()

        # Update order in PostgreSQL
        db.log_order(ready_event)

        # Commit Kafka offset
        self.consumer.commit(msg)

        cup_info = f" in {', '.join(claimed_cups)}" if claimed_cups else ""
        print(f"   {Color.BOLD}{Color.YELLOW}🔔 DING! Order {order_id} for {client_name}{cup_info} (${total_price:.2f}) is READY at the counter!{Color.RESET}\n")

    def _cleanup(self) -> None:
        """Closes Kafka connections and prints closing stats."""
        print("\nCleaning up resources...")
        try:
            self.consumer.close()
            self.producer.flush(timeout=5.0)
        except Exception as exc:
            logger.warning("Error during waiter shutdown: %s", exc)
        print(f"{Color.GREEN}Barista closed shop. Total orders completed today: {self.orders_served}{Color.RESET}")


def main() -> None:
    """Parses command-line options and boots the waiter worker service."""
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
        default=10.0,
        help="Total preparation time in seconds per drink (default: 10)",
    )
    args = parser.parse_args()

    bootstrap_server = os.environ.get("BOOTSTRAP_SERVER", args.bootstrap_server)
    orders_topic = os.environ.get("ORDERS_TOPIC", args.orders_topic)
    ready_topic = os.environ.get("READY_TOPIC", args.ready_topic)
    prep_time = float(os.environ.get("PREP_TIME", args.prep_time))

    service = SoloWaiterService(
        bootstrap_server=bootstrap_server,
        orders_topic=orders_topic,
        ready_topic=ready_topic,
        prep_time=prep_time,
    )

    def handle_shutdown(sig: int, frame: Any) -> None:
        print(f"\n{Color.YELLOW}🧑‍🍳 Waiter is closing the matcha bar for today... Goodbye!{Color.RESET}")
        service.stop()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    service.run()


if __name__ == "__main__":
    main()
