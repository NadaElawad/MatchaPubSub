#!/usr/bin/env python3
"""Matcha PubSub - Automated Dishwasher & Sanitizer Service.

Subscribes to 'matcha-cup-returns' Kafka topic to receive used ceramic cups.
Simulates industrial sanitization cycles, transitions PostgreSQL cup records
from WITH_CUSTOMER -> IN_DISHWASHER -> CLEAN_ON_SHELF, and publishes clean
availability notifications to 'matcha-cup-clean'. Includes an auto-busser
background thread to prevent cup starvation during long operating periods.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import signal
import threading
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
import db

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("matcha.dishwasher")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


class Color:
    """Terminal ANSI escape codes for dishwasher console logs."""
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


class DishwasherService:
    """Automated sanitization worker and table clearing busser service."""

    def __init__(
        self,
        bootstrap_server: str,
        returns_topic: str,
        clean_topic: str,
        wash_time: float,
        busser_timeout_secs: int = 30,
    ) -> None:
        """Initializes the dishwasher service.

        Args:
            bootstrap_server: Kafka broker connection string.
            returns_topic: Used cup return event topic.
            clean_topic: Sanitized cup notification topic.
            wash_time: Total sanitization cycle duration in seconds.
            busser_timeout_secs: Max seconds a cup can sit uncollected at tables.
        """
        self.bootstrap_server = bootstrap_server
        self.returns_topic = returns_topic
        self.clean_topic = clean_topic
        self.wash_time = wash_time
        self.busser_timeout_secs = busser_timeout_secs
        self.running = True
        self.cups_washed = 0

        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_server,
            "group.id": "matcha-dishwasher-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        })

        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_server,
            "client.id": "matcha-dishwasher-producer",
            "broker.address.family": "v4",
        })

    def stop(self) -> None:
        """Signals the worker loops to terminate gracefully."""
        self.running = False

    def run(self) -> None:
        """Main service entrypoint: launches busser and consumes cup returns."""
        self.consumer.subscribe([self.returns_topic])

        print(f"\n{Color.BOLD}{Color.BLUE}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}│  🫧 MATCHA CAFÉ - DISHWASHER & SANITIZER READY           │{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}│  Kafka Server : {self.bootstrap_server:<40} │{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}│  Returns Topic: {self.returns_topic:<40} │{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}│  Wash Cycle   : {self.wash_time:.1f}s                                    │{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}╰──────────────────────────────────────────────────────────╯{Color.RESET}")
        print(f"{Color.DIM}Listening for dirty cups returned by customers at the counter...{Color.RESET}\n")

        # Start background busser thread
        busser_thread = threading.Thread(target=self._auto_busser_loop, daemon=True)
        busser_thread.start()

        step_delay = self.wash_time / 3.0

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

                self._process_cup_return(msg, step_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received.")
        finally:
            self._cleanup()

    def _auto_busser_loop(self) -> None:
        """Periodically recovers abandoned cups from tables to prevent starvation."""
        print(f"  {Color.DIM}🧹 [Auto-Busser Active] Tables automatically bussed after {self.busser_timeout_secs}s inactivity.{Color.RESET}")
        while self.running:
            time.sleep(5)
            try:
                with db.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT cup_code, current_customer, current_order_id
                            FROM cups
                            WHERE (status = 'WITH_CUSTOMER' AND updated_at < NOW() - (%s || ' seconds')::INTERVAL)
                               OR (status = 'IN_BREWING' AND updated_at < NOW() - INTERVAL '2 minutes')
                               OR (status = 'IN_DISHWASHER' AND updated_at < NOW() - INTERVAL '1 minute');
                            """,
                            (self.busser_timeout_secs,),
                        )
                        stale_cups = cur.fetchall()
                        for c_code, c_cust, c_order in stale_cups:
                            print(f"\n  {Color.BLUE}🧹 [Café Busser]{Color.RESET} Finished cup {Color.MAGENTA}{c_code}{Color.RESET} cleared from {c_cust}'s table (order: {c_order}).")
                            db.return_cup_to_dishwasher(c_code, actor="Café Staff (Table Clearing)")
                            time.sleep(1.5)
                            db.sanitize_and_shelve_cup(c_code, actor="Automated Dishwasher")
                            print(f"  {Color.GREEN}✨ [Dishwasher]{Color.RESET} {c_code} sanitized and returned to shelf!")

                            clean_event = {"cup_code": c_code, "status": db.CupStatus.CLEAN_ON_SHELF}
                            self.producer.produce(
                                topic=self.clean_topic,
                                key=c_code.encode("utf-8"),
                                value=json.dumps(clean_event).encode("utf-8"),
                            )
                            self.producer.flush()
            except Exception as exc:
                logger.warning("Auto-busser iteration warning: %s", exc)

    def _process_cup_return(self, msg: Message, step_delay: float) -> None:
        """Sanitizes an incoming dirty cup through the 3-step wash pipeline.

        Args:
            msg: Kafka return event message.
            step_delay: Duration per sanitization sub-step.
        """
        try:
            data: dict[str, Any] = json.loads(msg.value().decode("utf-8"))
        except Exception:
            data = {"cup_code": msg.key().decode("utf-8") if msg.key() else "UNKNOWN"}

        cup_code = data.get("cup_code") or (msg.key().decode("utf-8") if msg.key() else "UNKNOWN")
        customer = data.get("customer_name") or data.get("client_name") or "A customer"
        order_id = data.get("order_id") or "UNKNOWN"

        self.cups_washed += 1
        print(f"{Color.BOLD}{Color.BLUE}╭──────────────────────────────────────────────────────────╮{Color.RESET}")
        print(f"{Color.BOLD}│ 🍽️  DIRTY CUP RETURNED #{self.cups_washed:<2} | Cup: {Color.MAGENTA}{cup_code:<18}{Color.RESET}{Color.BOLD}│{Color.RESET}")
        print(f"{Color.BOLD}│ Returned By: {Color.CYAN}{customer:<44}{Color.RESET}{Color.BOLD}│{Color.RESET}")
        print(f"{Color.BOLD}│ Prior Order: {Color.YELLOW}{order_id:<44}{Color.RESET}{Color.BOLD}│{Color.RESET}")
        print(f"{Color.BOLD}{Color.BLUE}╰──────────────────────────────────────────────────────────╯{Color.RESET}")

        # 1. Update database: WITH_CUSTOMER -> IN_DISHWASHER
        db.return_cup_to_dishwasher(cup_code, actor=f"Customer: {customer}")
        print(f"  {Color.BLUE}🚿 Step 1:{Color.RESET} Rinsing organic matcha residue from {Color.MAGENTA}{cup_code}{Color.RESET}...")
        time.sleep(step_delay)

        # 2. High-temp sanitizing cycle
        print(f"  {Color.BLUE}🧼 Step 2:{Color.RESET} High-temperature sanitizing cycle (85°C steam)...")
        time.sleep(step_delay)

        # 3. Drying & Shelving
        print(f"  {Color.BLUE}💨 Step 3:{Color.RESET} Hot air drying and inspection complete.")
        time.sleep(step_delay)

        # 4. Update database: IN_DISHWASHER -> CLEAN_ON_SHELF
        db.sanitize_and_shelve_cup(cup_code, actor="Automated Dishwasher")

        # 5. Announce clean cup on Kafka
        clean_event = {
            "cup_code": cup_code,
            "status": db.CupStatus.CLEAN_ON_SHELF,
            "sanitized_at": datetime.now(timezone.utc).isoformat(),
        }
        self.producer.produce(
            topic=self.clean_topic,
            key=cup_code.encode("utf-8"),
            value=json.dumps(clean_event).encode("utf-8"),
        )
        self.producer.flush()

        # 6. Commit offset
        self.consumer.commit(msg)

        print(f"  {Color.BOLD}{Color.GREEN}✨ DING! {cup_code} is sanitized and back on the shelf, ready for the next drink!{Color.RESET}\n")

    def _cleanup(self) -> None:
        """Closes Kafka consumer and flushes producer."""
        try:
            self.consumer.close()
            self.producer.flush(timeout=5.0)
        except Exception as exc:
            logger.warning("Error during dishwasher shutdown: %s", exc)
        print(f"\n{Color.GREEN}Dishwasher closed. Total cups washed today: {self.cups_washed}{Color.RESET}")


def main() -> None:
    """Parses arguments and starts the dishwasher daemon."""
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
    busser_timeout = int(os.environ.get("CUP_BUSSER_TIMEOUT_SECONDS", "30"))

    service = DishwasherService(
        bootstrap_server=bootstrap_server,
        returns_topic=returns_topic,
        clean_topic=clean_topic,
        wash_time=wash_time,
        busser_timeout_secs=busser_timeout,
    )

    def handle_shutdown(sig: int, frame: Any) -> None:
        print(f"\n{Color.YELLOW}🫧 Dishwasher cycle stopping... Turning off water and power.{Color.RESET}")
        service.stop()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    service.run()


if __name__ == "__main__":
    main()
