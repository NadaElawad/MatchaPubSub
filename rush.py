#!/usr/bin/env python3
"""
Matcha PubSub - Morning Rush Simulator

Spawns multiple customers concurrently to place orders at the Matcha Café.
Watch the solo waiter handle the queue one by one!
"""

import argparse
import random
import threading
import time
from client import MENU, MILKS, SWEETNESS_LEVELS, main as run_client

CUSTOMERS = ["Maya", "Kenji", "Liam", "Zara", "Amina", "Oliver", "Chloe", "Sam"]


def spawn_customer(name, delay):
    time.sleep(delay)
    import subprocess
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
    subprocess.run(cmd)


if __name__ == "__main__":
    import sys
    parser = argparse.ArgumentParser(description="Simulate a rush of café customers")
    parser.add_argument("--count", type=int, default=4, help="Number of customers (default: 4)")
    args = parser.parse_args()

    print(f"\n🏃‍♂️💨 MORNING RUSH: {args.count} customers are entering the Matcha Café!\n")

    threads = []
    selected_names = random.sample(CUSTOMERS, min(args.count, len(CUSTOMERS)))
    for i, name in enumerate(selected_names):
        # Stagger entrances slightly
        t = threading.Thread(target=spawn_customer, args=(name, i * 0.5))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n✨ All rush hour customers have been served! ✨\n")
