#!/usr/bin/env python3
"""Matcha PubSub - Morning Rush Simulator.

Concurrently launches multiple client processes to simulate a sudden rush of
customers entering the café, exercising the waiter's FIFO queue and cup limits.
"""

from __future__ import annotations

import argparse
import random
import subprocess
import sys
import threading
import time

from client import MENU, MILKS, SWEETNESS_LEVELS

CUSTOMERS: list[str] = [
    "Maya", "Kenji", "Liam", "Zara", "Amina", "Oliver", "Chloe", "Sam"
]


def spawn_customer(name: str, delay: float) -> None:
    """Spawns an independent customer client subprocess after an initial delay.

    Args:
        name: Name of the customer.
        delay: Stagger delay in seconds before launching.
    """
    time.sleep(delay)
    drink = random.choice(MENU)
    milk = random.choice(MILKS)
    sweetness = random.choice(SWEETNESS_LEVELS)

    cmd = [
        sys.executable,
        "client.py",
        "--name", name,
        "--drink", drink,
        "--milk", milk,
        "--sweetness", sweetness,
    ]
    subprocess.run(cmd, check=False)


def run_rush(count: int) -> None:
    """Dispatches multiple concurrent customer orders.

    Args:
        count: Number of customers to simulate.
    """
    print(f"\n🏃‍♂️💨 MORNING RUSH: {count} customers are entering the Matcha Café!\n")

    threads: list[threading.Thread] = []
    selected_names = random.sample(CUSTOMERS, min(count, len(CUSTOMERS)))
    for i, name in enumerate(selected_names):
        t = threading.Thread(target=spawn_customer, args=(name, i * 0.5))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n✨ All rush hour customers have completed their orders! ✨\n")


def main() -> None:
    """CLI entrypoint for rush simulation."""
    parser = argparse.ArgumentParser(description="Simulate a rush of café customers")
    parser.add_argument("--count", type=int, default=4, help="Number of customers (default: 4)")
    args = parser.parse_args()

    run_rush(count=args.count)


if __name__ == "__main__":
    main()
