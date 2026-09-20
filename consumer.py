#!/usr/bin/env python3
"""
Kafka Consumer Script for Learning & Prototyping.

Features:
- Consumer Group Management: Join a consumer group and observe rebalancing.
- Offset Control: Read from beginning (--from-beginning) or only newly published messages.
- Pretty Formatting: Automatically parses and formats JSON payloads with colors.
- Clean Shutdown: Gracefully closes and commits offsets on Ctrl+C.
"""

import argparse
import json
import signal
import sys
import uuid
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING


# ANSI Color helpers for readable console output
class Color:
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


running = True


def handle_shutdown(sig, frame):
    """Graceful signal handler for Ctrl+C."""
    global running
    print(f"\n{Color.YELLOW}Shutting down consumer cleanly...{Color.RESET}")
    running = False


def format_payload(raw_bytes):
    """Attempt to format the payload as pretty JSON, otherwise return as string."""
    if raw_bytes is None:
        return f"{Color.DIM}(null){Color.RESET}"
    try:
        decoded = raw_bytes.decode("utf-8")
        parsed = json.loads(decoded)
        # Pretty indented JSON with colors
        formatted = json.dumps(parsed, indent=2)
        return f"{Color.GREEN}{formatted}{Color.RESET}"
    except (UnicodeDecodeError, json.JSONDecodeError):
        return f"{Color.CYAN}{raw_bytes.decode('utf-8', errors='replace')}{Color.RESET}"


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Consumer for learning and inspecting messages."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="matcha-orders",
        help="Topic to consume from (default: matcha-orders)",
    )
    parser.add_argument(
        "--group",
        default=None,
        help="Consumer group ID (default: matcha-consumer-group, or unique ad-hoc ID with --from-beginning)",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read messages from the beginning of the topic (offset 0)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum messages to receive before exiting (0 for continuous, default: 0)",
    )

    args = parser.parse_args()

    # Determine consumer group ID:
    # If --from-beginning is specified without an explicit group, use an ad-hoc group
    # to avoid waiting on rebalance timeouts or skipping committed offsets.
    if args.group:
        group_id = args.group
    elif args.from_beginning:
        group_id = f"adhoc-viewer-{uuid.uuid4().hex[:6]}"
    else:
        group_id = "matcha-consumer-group"

    offset_reset = "earliest" if args.from_beginning else "latest"

    conf = {
        "bootstrap.servers": args.bootstrap_server,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }

    try:
        consumer = Consumer(conf)
    except Exception as e:
        print(f"{Color.RED}Failed to initialize Consumer: {e}{Color.RESET}")
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    def print_assignment(c, partitions):
        print(f"{Color.MAGENTA}👥 Partitions assigned:{Color.RESET}")
        for p in partitions:
            print(f"   • Topic: {p.topic}, Partition: {p.partition}")

        if args.from_beginning:
            print(f"{Color.CYAN}⏪ Seeking all partitions to offset 0 (beginning)...{Color.RESET}")
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            c.assign(partitions)

    consumer.subscribe([args.topic], on_assign=print_assignment)

    print(f"\n{Color.BOLD}🍵 Kafka Consumer Started{Color.RESET}")
    print(f"Connecting to:    {Color.CYAN}{args.bootstrap_server}{Color.RESET}")
    print(f"Subscribed Topic: {Color.CYAN}{args.topic}{Color.RESET}")
    print(f"Consumer Group:   {Color.CYAN}{group_id}{Color.RESET}")
    print(f"Offset Reset:     {Color.YELLOW}{offset_reset}{Color.RESET}")
    print(f"Waiting for messages... (Press {Color.BOLD}Ctrl+C{Color.RESET} to exit)\n")

    message_count = 0

    try:
        while running:
            # Poll for new messages with a 1.0s timeout
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event (normal if reached latest offset)
                    print(
                        f"{Color.DIM}Reached end of partition {msg.partition()} "
                        f"at offset {msg.offset()}{Color.RESET}"
                    )
                else:
                    raise KafkaException(msg.error())
                continue

            message_count += 1
            key_text = msg.key().decode("utf-8") if msg.key() else "(no key)"
            timestamp_info = ""
            if msg.timestamp()[0] != 0:
                ts_ms = msg.timestamp()[1]
                timestamp_info = f" | ts={ts_ms}"

            print(f"{Color.BOLD}--------------------------------------------------{Color.RESET}")
            print(
                f"📥 {Color.BOLD}Message #{message_count}{Color.RESET} | "
                f"Topic: {Color.CYAN}{msg.topic()}{Color.RESET} | "
                f"Partition: {Color.MAGENTA}{msg.partition()}{Color.RESET} | "
                f"Offset: {Color.YELLOW}{msg.offset()}{Color.RESET}{timestamp_info}"
            )
            print(f"🔑 Key: {Color.BOLD}{key_text}{Color.RESET}")
            print(f"📦 Payload:")
            print(format_payload(msg.value()))
            print()

            if args.max_messages > 0 and message_count >= args.max_messages:
                print(f"{Color.GREEN}Reached target of {args.max_messages} messages.{Color.RESET}")
                break

    except Exception as e:
        print(f"{Color.RED}Error consuming message: {e}{Color.RESET}", file=sys.stderr)
    finally:
        print("Closing consumer and committing final offsets...")
        consumer.close()
        print(f"{Color.GREEN}Consumer closed cleanly. Total messages received: {message_count}{Color.RESET}")


if __name__ == "__main__":
    main()
