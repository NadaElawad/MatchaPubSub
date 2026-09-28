"""Real-Time Kafka Streaming Analytics Engine powered by Quix Streams.

This module provides stateful stream processing over Apache Kafka topics
(`matcha-orders` and `matcha-ready`), replacing high-latency, heavy PostgreSQL
aggregation queries (e.g., `generate_series`, `GROUP BY`, `SUM`) with
sub-millisecond in-memory materialized state.

Key Features:
  - Real-Time Popular Creations Leaderboard (Top drinks by volume and revenue).
  - Live KPI Counters (Total orders count, gross revenue, rolling prep velocity).
  - Tumbling 5-Minute Window Timeseries Buckets (Orders and revenue slices).
  - Thread-safe state snapshots consumed directly by FastAPI (/analytics).
  - Graceful fallback to PostgreSQL when the streaming engine is initializing.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import threading
import time
from typing import Any

from quixstreams import Application

logger = logging.getLogger("matcha.stream_analytics")

# Environment & Connection Constants
DEFAULT_BOOTSTRAP_SERVERS: str = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
DEFAULT_ORDERS_TOPIC: str = os.environ.get("ORDERS_TOPIC", "matcha-orders")
DEFAULT_READY_TOPIC: str = os.environ.get("READY_TOPIC", "matcha-ready")
MAX_HISTORY_BUCKETS: int = 24  # Keep up to 2 hours of 5-minute tumbling windows
MAX_PREP_SAMPLES: int = 100


@dataclass
class DrinkStat:
    """Aggregated sales metrics for an individual menu item."""

    name: str
    count: int = 0
    revenue: float = 0.0


@dataclass
class TimeBucket:
    """Aggregated metrics for a discrete 5-minute tumbling window."""

    bucket_time: datetime
    orders: int = 0
    revenue: float = 0.0


class QuixStreamAnalytics:
    """Thread-safe stream processing engine using Quix Streams."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        orders_topic: str = DEFAULT_ORDERS_TOPIC,
        ready_topic: str = DEFAULT_READY_TOPIC,
        consumer_group: str | None = None,
    ) -> None:
        """Initializes the Quix Streams real-time analytics engine.

        Args:
            bootstrap_servers: Kafka broker connection string.
            orders_topic: Topic containing submitted order events.
            ready_topic: Topic containing completed drink notifications.
            consumer_group: Optional Kafka consumer group identifier. Defaults to
                a unique session group ensuring full stream hydration from offset 0.
        """
        import uuid
        self.bootstrap_servers = bootstrap_servers
        self.orders_topic_name = orders_topic
        self.ready_topic_name = ready_topic
        self.consumer_group = consumer_group or f"matcha-analytics-{uuid.uuid4().hex[:8]}"

        # Thread synchronization
        self._lock = threading.RLock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._app: Application | None = None
        self._initialized = False

        # Materialized Streaming State
        self._total_orders: int = 0
        self._total_revenue: float = 0.0
        self._drink_stats: dict[str, DrinkStat] = {}
        self._time_buckets: dict[datetime, TimeBucket] = {}
        self._pending_orders: dict[str, float] = {}
        self._prep_durations: deque[float] = deque(maxlen=MAX_PREP_SAMPLES)
        self._avg_prep_time_sec: float = 42.0
        self._processed_events: int = 0
        self._last_event_time: datetime | None = None

    def start(self) -> None:
        """Launches the Quix Streams consumer application in a daemon thread."""
        with self._lock:
            if self._running:
                logger.warning("QuixStreamAnalytics is already running.")
                return

            self._running = True

            try:
                # Initialize Quix Streams Application
                self._app = Application(
                    broker_address=self.bootstrap_servers,
                    consumer_group=self.consumer_group,
                    auto_offset_reset="earliest",
                )
                # Bypass signal handlers so Quix runs safely within a daemon thread
                self._app._setup_signal_handlers = lambda: None  # type: ignore

                # Define topic streams
                orders_topic = self._app.topic(self.orders_topic_name)
                ready_topic = self._app.topic(self.ready_topic_name)

                # Build Streaming Topologies
                orders_sdf = self._app.dataframe(orders_topic)
                orders_sdf = orders_sdf.update(self._process_order_event)

                ready_sdf = self._app.dataframe(ready_topic)
                ready_sdf = ready_sdf.update(self._process_ready_event)

                # Spawn background processing thread
                self._thread = threading.Thread(
                    target=self._run_worker,
                    name="QuixStreamAnalyticsWorker",
                    daemon=True,
                )
                self._thread.start()
                logger.info(
                    "QuixStreamAnalytics engine started successfully on broker %s (topics: %s, %s)",
                    self.bootstrap_servers,
                    self.orders_topic_name,
                    self.ready_topic_name,
                )
            except Exception as exc:
                self._running = False
                logger.error("Failed to initialize QuixStreamAnalytics: %s", exc, exc_info=True)

    def _run_worker(self) -> None:
        """Internal worker target for the Quix Streams application."""
        if not self._app:
            return
        try:
            self._app.run()
        except Exception as exc:
            logger.error("QuixStreamAnalytics worker loop stopped with error: %s", exc)
        finally:
            with self._lock:
                self._running = False

    def stop(self) -> None:
        """Stops the Quix Streams engine cleanly."""
        with self._lock:
            if not self._running:
                return
            logger.info("Stopping QuixStreamAnalytics engine...")
            if self._app:
                try:
                    self._app.stop()
                except Exception as exc:
                    logger.debug("Error while stopping Quix app: %s", exc)
            self._running = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
            logger.info("QuixStreamAnalytics background thread joined.")

    def is_ready(self) -> bool:
        """Indicates whether the streaming engine is initialized and has hydrated data."""
        with self._lock:
            return self._running and (self._processed_events > 0 or self._initialized)

    def _parse_timestamp(self, ts_raw: Any) -> datetime:
        """Parses an ISO timestamp string or float into a timezone-aware datetime."""
        if isinstance(ts_raw, (int, float)):
            return datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        if isinstance(ts_raw, str):
            try:
                # Python 3.11+ fromisoformat handles standard UTC offsets (Z, +00:00)
                clean_str = ts_raw.replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _process_order_event(self, event: dict[str, Any]) -> None:
        """Processes an incoming order event from `matcha-orders`."""
        if not isinstance(event, dict):
            return

        with self._lock:
            self._processed_events += 1
            self._initialized = True
            order_id = str(event.get("order_id") or "")
            price = float(event.get("price") or 0.0)
            ordered_at_dt = self._parse_timestamp(event.get("ordered_at"))
            self._last_event_time = ordered_at_dt

            # 1. Update Global KPIs
            self._total_orders += 1
            self._total_revenue += price

            # 2. Track Pending Order for Prep Duration Calculation
            if order_id:
                self._pending_orders[order_id] = ordered_at_dt.timestamp()

            # 3. Update Popular Items Leaderboard
            items = event.get("items")
            if isinstance(items, list) and items:
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    item_name = it.get("name") or it.get("drink_name") or event.get("drink_name") or "Matcha Drink"
                    qty = int(it.get("quantity") or 1)
                    item_rev = float(it.get("line_total") or (float(it.get("price") or 0.0) * qty))
                    stat = self._drink_stats.setdefault(item_name, DrinkStat(name=item_name))
                    stat.count += qty
                    stat.revenue += item_rev
            else:
                drink_name = event.get("drink_name") or event.get("drink") or "Matcha Drink"
                stat = self._drink_stats.setdefault(drink_name, DrinkStat(name=drink_name))
                stat.count += 1
                stat.revenue += price

            # 4. Update Tumbling 5-Minute Window Bucket
            # Round down to the nearest 5-minute interval
            bucket_dt = ordered_at_dt.replace(
                minute=(ordered_at_dt.minute // 5) * 5,
                second=0,
                microsecond=0,
            )
            bucket = self._time_buckets.setdefault(
                bucket_dt, TimeBucket(bucket_time=bucket_dt)
            )
            bucket.orders += 1
            bucket.revenue += price

            # Purge stale time buckets older than MAX_HISTORY_BUCKETS (2 hours)
            cutoff = ordered_at_dt - timedelta(minutes=MAX_HISTORY_BUCKETS * 5)
            stale_keys = [k for k in self._time_buckets if k < cutoff]
            for k in stale_keys:
                self._time_buckets.pop(k, None)

    def _process_ready_event(self, event: dict[str, Any]) -> None:
        """Processes a completed drink ready event from `matcha-ready`."""
        if not isinstance(event, dict):
            return

        with self._lock:
            order_id = str(event.get("order_id") or "")
            ready_at_dt = self._parse_timestamp(event.get("ready_at"))

            if order_id and order_id in self._pending_orders:
                ordered_ts = self._pending_orders.pop(order_id)
                duration = ready_at_dt.timestamp() - ordered_ts
                # Filter out outlier delays (e.g. historical orders processed days later)
                if 2.0 <= duration <= 300.0:
                    self._prep_durations.append(duration)
                    if self._prep_durations:
                        self._avg_prep_time_sec = sum(self._prep_durations) / len(self._prep_durations)

    def get_realtime_metrics(self) -> dict[str, Any]:
        """Produces a real-time analytics snapshot from Kafka Streams state.

        Returns:
            dict[str, Any]: Aggregated chart series, popular creations, and KPI metrics.
        """
        with self._lock:
            # 1. Sorted Popular Items (Top 6)
            sorted_drinks = sorted(
                self._drink_stats.values(),
                key=lambda d: (d.count, d.revenue),
                reverse=True,
            )
            popular_items = [
                {
                    "name": d.name,
                    "count": d.count,
                    "revenue": round(d.revenue, 2),
                }
                for d in sorted_drinks[:6]
            ]

            # 2. Continuous 12-Bucket 5-Minute Slices for Chart Series (Past 60 Minutes)
            now_dt = datetime.now(timezone.utc).astimezone()
            aligned_now = now_dt.replace(
                minute=(now_dt.minute // 5) * 5,
                second=0,
                microsecond=0,
            )
            bucket_times = [
                aligned_now - timedelta(minutes=5 * i)
                for i in range(11, -1, -1)
            ]

            chart_series: list[dict[str, Any]] = []
            for b_dt in bucket_times:
                time_label = b_dt.strftime("%H:%M")
                # Look up matching bucket in time_buckets
                # Compare by year, month, day, hour, minute
                found_bucket = None
                for k, b in self._time_buckets.items():
                    k_local = k.astimezone()
                    if (
                        k_local.year == b_dt.year
                        and k_local.month == b_dt.month
                        and k_local.day == b_dt.day
                        and k_local.hour == b_dt.hour
                        and k_local.minute == b_dt.minute
                    ):
                        found_bucket = b
                        break

                chart_series.append({
                    "time": time_label,
                    "orders": found_bucket.orders if found_bucket else 0,
                    "revenue": round(found_bucket.revenue, 2) if found_bucket else 0.0,
                })

            return {
                "timeframe": "minutes",
                "time_label": "Past 60 Minutes (Kafka Streams 5-Min Slices)",
                "total_orders": self._total_orders,
                "total_revenue": round(self._total_revenue, 2),
                "avg_prep_time_sec": round(self._avg_prep_time_sec, 1),
                "popular_items": popular_items,
                "chart_series": chart_series,
                "engine": "Kafka Streams (Quix Streams 3.26)",
                "processed_events": self._processed_events,
                "is_stream_powered": True,
            }


# Singleton stream analytics instance for application-wide use
stream_analytics_engine = QuixStreamAnalytics()
