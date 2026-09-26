"""Database access layer and domain state machine for Matcha PubSub.

This module provides thread-safe connection pooling, atomic cup lifecycle
transitions via PostgreSQL concurrency primitives (e.g. FOR UPDATE SKIP LOCKED),
customer loyalty metrics, and real-time visualization state aggregation.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
import logging
import os
import threading
from typing import Any, Generator, Sequence

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("matcha.db")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# ---------------------------------------------------------------------------
# Enums and Domain Constants
# ---------------------------------------------------------------------------
class CupStatus(StrEnum):
    """Lifecycle statuses for ceramic reusable matcha cups."""
    CLEAN_ON_SHELF = "CLEAN_ON_SHELF"
    IN_BREWING = "IN_BREWING"
    WITH_CUSTOMER = "WITH_CUSTOMER"
    IN_DISHWASHER = "IN_DISHWASHER"


class OrderStatus(StrEnum):
    """Fulfillment lifecycle statuses for orders."""
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    READY = "READY"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class DiningOption(StrEnum):
    """Seating / dispatch choices for guest orders."""
    DINE_IN = "dine_in"
    TAKE_AWAY = "take_away"


DEFAULT_CUP_CAPACITY: int = 12
DEFAULT_FALLBACK_PRICE: float = 6.50
DINE_IN_DURATION_SECONDS: int = 45

# ---------------------------------------------------------------------------
# Database Connection Pool Management
# ---------------------------------------------------------------------------
_pool: pool.ThreadedConnectionPool | None = None
_pool_lock: threading.Lock = threading.Lock()


def get_pool() -> pool.ThreadedConnectionPool:
    """Lazily initializes and returns a ThreadedConnectionPool.

    Uses double-checked locking to ensure safe initialization across threads.

    Returns:
        pool.ThreadedConnectionPool: Active connection pool.
    """
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                host = os.environ.get("DB_HOST", "localhost")
                port = os.environ.get("DB_PORT", "5432")
                dbname = os.environ.get("DB_NAME", "matcha_cafe")
                user = os.environ.get("DB_USER", "barista")
                password = os.environ.get("DB_PASSWORD", "matchapassword")

                logger.info(
                    "Initializing ThreadedConnectionPool (host=%s, port=%s, db=%s, user=%s)",
                    host,
                    port,
                    dbname,
                    user,
                )
                _pool = pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=25,
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                )
    return _pool


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager for acquiring and returning a connection from the pool.

    Ensures that connections are rolled back on uncaught exceptions before
    being returned to the pool to prevent dirty transaction leakage.

    Yields:
        psycopg2.extensions.connection: A connection from the pool.

    Raises:
        Exception: Re-raises any database or execution exceptions.
    """
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def check_db_health() -> bool:
    """Verifies that PostgreSQL is reachable and responsive.

    Returns:
        bool: True if connection is responsive, False otherwise.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return True
    except Exception as exc:
        logger.error("Database health check failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Menu & Catalog Queries
# ---------------------------------------------------------------------------
def get_menu() -> list[dict[str, Any]]:
    """Fetches all in-stock products ordered by category and name.

    Returns:
        list[dict[str, Any]]: List of in-stock product items.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, category, price, description, in_stock
                    FROM products
                    WHERE in_stock = TRUE
                    ORDER BY category, name;
                    """
                )
                rows = cur.fetchall()
                for row in rows:
                    row["price"] = float(row["price"])
                return rows
    except Exception as exc:
        logger.error("get_menu failed: %s", exc, exc_info=True)
        return []


def get_product_by_name(drink_name: str) -> dict[str, Any] | None:
    """Fetches a single product by name (case-insensitive).

    Args:
        drink_name: The name of the product or drink.

    Returns:
        dict[str, Any] | None: Product details dictionary if found, else None.
    """
    if not drink_name:
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, category, price, description, in_stock
                    FROM products
                    WHERE LOWER(name) = LOWER(%s);
                    """,
                    (drink_name.strip(),),
                )
                row = cur.fetchone()
                if row:
                    row["price"] = float(row["price"])
                return row
    except Exception as exc:
        logger.error("get_product_by_name failed for '%s': %s", drink_name, exc)
        return None


def get_product_price(drink_name: str) -> float:
    """Fetches product price from products table with fallback.

    Args:
        drink_name: Name of the drink.

    Returns:
        float: Unit price of the product or fallback price.
    """
    product = get_product_by_name(drink_name)
    if product and product.get("price") is not None:
        return float(product["price"])
    return DEFAULT_FALLBACK_PRICE


# ---------------------------------------------------------------------------
# Order Lifecycle Management
# ---------------------------------------------------------------------------
def create_pending_order(
    order_id: str,
    customer_name: str,
    drink_name: str,
    milk: str | None,
    sweetness: str | None,
    price: float,
    ordered_at: str | datetime | None = None,
    items: list[dict[str, Any]] | None = None,
    cup_codes: list[str] | None = None,
    dining_option: str = DiningOption.TAKE_AWAY,
) -> bool:
    """Records an incoming order with status 'PENDING'.

    Args:
        order_id: Unique order identifier.
        customer_name: Guest name.
        drink_name: Summary drink description.
        milk: Milk customization choice.
        sweetness: Sweetness percentage choice.
        price: Total order monetary amount.
        ordered_at: Optional timestamp when order was placed.
        items: Optional structured list of line items.
        cup_codes: Optional allocated cup codes.
        dining_option: 'dine_in' or 'take_away'.

    Returns:
        bool: True on successful persistence, False on error.
    """
    try:
        items_json = json.dumps(items) if items is not None else None
        cup_code_str = ", ".join(cup_codes) if cup_codes else None
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_name, drink_name, milk, sweetness, price,
                        cup_code, cup_codes, items, status, ordered_at, ready_at, dining_option
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', COALESCE(%s, NOW()), NULL, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        items = COALESCE(EXCLUDED.items, orders.items),
                        dining_option = COALESCE(EXCLUDED.dining_option, orders.dining_option),
                        status = 'PENDING';
                    """,
                    (
                        order_id,
                        customer_name,
                        drink_name,
                        milk,
                        sweetness,
                        price,
                        cup_code_str,
                        cup_codes,
                        items_json,
                        ordered_at,
                        dining_option,
                    ),
                )
                conn.commit()
                return True
    except Exception as exc:
        logger.error("create_pending_order failed for %s: %s", order_id, exc, exc_info=True)
        return False


def update_order_status(
    order_id: str,
    status: str,
    prepared_by: str | None = None,
) -> bool:
    """Updates order status and optional preparer metadata.

    Args:
        order_id: Unique order identifier.
        status: New OrderStatus string (e.g. 'PREPARING', 'READY').
        prepared_by: Optional name of the barista/staff member.

    Returns:
        bool: True on successful update, False otherwise.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if prepared_by:
                    cur.execute(
                        "UPDATE orders SET status = %s, prepared_by = %s WHERE order_id = %s;",
                        (status, prepared_by, order_id),
                    )
                else:
                    cur.execute(
                        "UPDATE orders SET status = %s WHERE order_id = %s;",
                        (status, order_id),
                    )
                conn.commit()
                return True
    except Exception as exc:
        logger.error("update_order_status failed for %s: %s", order_id, exc)
        return False


def get_order(order_id: str) -> dict[str, Any] | None:
    """Fetches full order details and status by order_id.

    Args:
        order_id: Unique order identifier.

    Returns:
        dict[str, Any] | None: Order dictionary if found, else None.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT order_id, customer_name, drink_name, milk, sweetness, price,
                           cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at, dining_option
                    FROM orders
                    WHERE order_id = %s;
                    """,
                    (order_id,),
                )
                row = cur.fetchone()
                if row:
                    row["price"] = float(row["price"])
                    if row["ordered_at"]:
                        row["ordered_at"] = row["ordered_at"].isoformat()
                    if row["ready_at"]:
                        row["ready_at"] = row["ready_at"].isoformat()
                return row
    except Exception as exc:
        logger.error("get_order failed for %s: %s", order_id, exc)
        return None


def get_recent_orders(
    limit: int = 20,
    customer_name: str | None = None,
) -> list[dict[str, Any]]:
    """Fetches recent orders, optionally filtered by customer name.

    Args:
        limit: Maximum number of rows to retrieve.
        customer_name: Optional customer name for filtering.

    Returns:
        list[dict[str, Any]]: Ordered list of recent orders.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if customer_name:
                    cur.execute(
                        """
                        SELECT order_id, customer_name, drink_name, milk, sweetness, price,
                               cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at, dining_option
                        FROM orders
                        WHERE LOWER(customer_name) = LOWER(%s)
                        ORDER BY ordered_at DESC
                        LIMIT %s;
                        """,
                        (customer_name.strip(), limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT order_id, customer_name, drink_name, milk, sweetness, price,
                               cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at, dining_option
                        FROM orders
                        ORDER BY ordered_at DESC
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                rows = cur.fetchall()
                for r in rows:
                    r["price"] = float(r["price"])
                    if r["ordered_at"]:
                        r["ordered_at"] = r["ordered_at"].isoformat()
                    if r["ready_at"]:
                        r["ready_at"] = r["ready_at"].isoformat()
                return rows
    except Exception as exc:
        logger.error("get_recent_orders failed: %s", exc)
        return []


def get_customer_profile(customer_name: str) -> dict[str, Any] | None:
    """Fetches loyalty profile and discovered preferences for a customer.

    Args:
        customer_name: Customer name.

    Returns:
        dict[str, Any] | None: Profile record if found, else None.
    """
    if not customer_name:
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT name, favorite_drink, preferred_milk, preferred_sweetness,
                           total_spent, total_orders, last_visit
                    FROM customers
                    WHERE LOWER(name) = LOWER(%s);
                    """,
                    (customer_name.strip(),),
                )
                row = cur.fetchone()
                if row:
                    row["total_spent"] = float(row["total_spent"])
                    if row["last_visit"]:
                        row["last_visit"] = row["last_visit"].isoformat()
                return row
    except Exception as exc:
        logger.error("get_customer_profile failed for %s: %s", customer_name, exc)
        return None


def record_order(order_data: dict[str, Any]) -> bool:
    """Atomically commits a fulfilled order and updates customer loyalty.

    Performs:
    1. Upsert completed order to 'orders' with status 'READY'.
    2. Upsert customer profile: increment spend and order count.
    3. Recomputes customer preferences via CTE based on order history.

    Args:
        order_data: Complete dictionary representing the ready order event.

    Returns:
        bool: True on successful transaction, False otherwise.
    """
    order_id = order_data.get("order_id")
    customer_name = order_data.get("client_name") or order_data.get("customer_name")
    drink = order_data.get("drink") or order_data.get("drink_name")
    milk = order_data.get("milk")
    sweetness = order_data.get("sweetness")
    price = float(order_data.get("price", DEFAULT_FALLBACK_PRICE))
    cup_code = order_data.get("cup_code")
    cup_codes = order_data.get("cup_codes")
    items = order_data.get("items")
    items_json = json.dumps(items) if items is not None else None
    if not cup_code and cup_codes:
        cup_code = ", ".join(cup_codes)
    status = order_data.get("status", OrderStatus.READY)
    prepared_by = order_data.get("prepared_by", "Kaito (Solo Waiter)")
    ordered_at = order_data.get("ordered_at")
    ready_at = order_data.get("ready_at")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Upsert order
                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_name, drink_name, milk, sweetness, price,
                        cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), COALESCE(%s, NOW()))
                    ON CONFLICT (order_id) DO UPDATE SET
                        cup_code = COALESCE(EXCLUDED.cup_code, orders.cup_code),
                        cup_codes = COALESCE(EXCLUDED.cup_codes, orders.cup_codes),
                        items = COALESCE(EXCLUDED.items, orders.items),
                        status = EXCLUDED.status,
                        prepared_by = EXCLUDED.prepared_by,
                        ready_at = EXCLUDED.ready_at;
                    """,
                    (
                        order_id,
                        customer_name,
                        drink,
                        milk,
                        sweetness,
                        price,
                        cup_code,
                        cup_codes,
                        items_json,
                        status,
                        prepared_by,
                        ordered_at,
                        ready_at,
                    ),
                )

                # 2. Upsert customer profile
                cur.execute(
                    """
                    INSERT INTO customers (
                        name, favorite_drink, preferred_milk, preferred_sweetness,
                        total_spent, total_orders, last_visit, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 1, COALESCE(%s, NOW()), NOW()
                    )
                    ON CONFLICT (name) DO UPDATE SET
                        total_spent = customers.total_spent + EXCLUDED.total_spent,
                        total_orders = customers.total_orders + 1,
                        last_visit = EXCLUDED.last_visit,
                        updated_at = NOW();
                    """,
                    (customer_name, drink, milk, sweetness, price, ready_at),
                )

                # 3. Recalculate customer preferences
                cur.execute(
                    """
                    WITH fav AS (
                        SELECT drink_name
                        FROM orders
                        WHERE customer_name = %s
                        GROUP BY drink_name
                        ORDER BY COUNT(*) DESC, MAX(ready_at) DESC
                        LIMIT 1
                    ),
                    pref_milk AS (
                        SELECT milk
                        FROM orders, fav
                        WHERE customer_name = %s AND orders.drink_name = fav.drink_name AND milk IS NOT NULL
                        GROUP BY milk
                        ORDER BY COUNT(*) DESC, MAX(ready_at) DESC
                        LIMIT 1
                    ),
                    pref_sweet AS (
                        SELECT sweetness
                        FROM orders, fav
                        WHERE customer_name = %s AND orders.drink_name = fav.drink_name AND sweetness IS NOT NULL
                        GROUP BY sweetness
                        ORDER BY COUNT(*) DESC, MAX(ready_at) DESC
                        LIMIT 1
                    )
                    UPDATE customers
                    SET
                        favorite_drink = (SELECT drink_name FROM fav),
                        preferred_milk = COALESCE(
                            (SELECT milk FROM pref_milk),
                            (SELECT milk FROM orders WHERE customer_name = %s AND milk IS NOT NULL GROUP BY milk ORDER BY COUNT(*) DESC, MAX(ready_at) DESC LIMIT 1)
                        ),
                        preferred_sweetness = COALESCE(
                            (SELECT sweetness FROM pref_sweet),
                            (SELECT sweetness FROM orders WHERE customer_name = %s AND sweetness IS NOT NULL GROUP BY sweetness ORDER BY COUNT(*) DESC, MAX(ready_at) DESC LIMIT 1)
                        ),
                        updated_at = NOW()
                    WHERE name = %s;
                    """,
                    (customer_name, customer_name, customer_name, customer_name, customer_name, customer_name),
                )

                conn.commit()
                return True
    except Exception as exc:
        logger.error("record_order failed for %s: %s", order_id, exc, exc_info=True)
        return False


# Backward-compatible alias
log_order = record_order


# ---------------------------------------------------------------------------
# Cup Inventory & Lifecycle Management
# ---------------------------------------------------------------------------
def claim_cup_for_order(
    order_id: str,
    customer_name: str,
    actor: str = "Barista",
) -> str | None:
    """Atomically claims one clean cup from the shelf.

    Uses PostgreSQL concurrency primitive 'FOR UPDATE SKIP LOCKED' so concurrent
    baristas or workers never collide or double-claim cups.
    Transition: CLEAN_ON_SHELF -> IN_BREWING

    Args:
        order_id: The order identifier requiring a ceramic cup.
        customer_name: Name of the customer.
        actor: System entity triggering the action.

    Returns:
        str | None: The claimed cup code (e.g. 'CUP-01') or None if shelf is empty.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cups
                    SET 
                        status = 'IN_BREWING',
                        current_order_id = %s,
                        current_customer = %s,
                        total_uses = total_uses + 1,
                        updated_at = NOW()
                    WHERE id = (
                        SELECT id FROM cups
                        WHERE status = 'CLEAN_ON_SHELF'
                        ORDER BY id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING cup_code;
                    """,
                    (order_id, customer_name),
                )
                row = cur.fetchone()
                if row:
                    cup_code = row[0]
                    # Log audit event
                    cur.execute(
                        """
                        INSERT INTO cup_audit_log (cup_code, from_status, to_status, order_id, actor)
                        VALUES (%s, 'CLEAN_ON_SHELF', 'IN_BREWING', %s, %s);
                        """,
                        (cup_code, order_id, actor),
                    )
                    # Associate cup with order record
                    cur.execute(
                        "UPDATE orders SET cup_code = %s WHERE order_id = %s;",
                        (cup_code, order_id),
                    )
                    conn.commit()
                    return cup_code
                return None
    except Exception as exc:
        logger.error("claim_cup_for_order failed for %s: %s", order_id, exc, exc_info=True)
        return None


def hand_cup_to_customer(
    cup_code: str,
    order_id: str | None = None,
    customer_name: str | None = None,
    actor: str = "Barista",
) -> bool:
    """Transitions cup from IN_BREWING -> WITH_CUSTOMER when customer collects drink.

    Args:
        cup_code: Target ceramic cup identifier.
        order_id: Associated order ID.
        customer_name: Customer collecting the cup.
        actor: Staff or system entity.

    Returns:
        bool: True on success, False otherwise.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cups
                    SET 
                        status = 'WITH_CUSTOMER',
                        current_customer = COALESCE(%s, current_customer),
                        current_order_id = COALESCE(%s, current_order_id),
                        updated_at = NOW()
                    WHERE cup_code = %s;
                    """,
                    (customer_name, order_id, cup_code),
                )
                cur.execute(
                    """
                    INSERT INTO cup_audit_log (cup_code, from_status, to_status, order_id, actor)
                    VALUES (%s, 'IN_BREWING', 'WITH_CUSTOMER', %s, %s);
                    """,
                    (cup_code, order_id, actor),
                )
                conn.commit()
                return True
    except Exception as exc:
        logger.error("hand_cup_to_customer failed for %s: %s", cup_code, exc)
        return False


def return_cup_to_dishwasher(
    cup_code: str,
    actor: str = "Customer",
) -> bool:
    """Transitions cup from WITH_CUSTOMER -> IN_DISHWASHER.

    Args:
        cup_code: Cup being returned to the bussing / cleaning station.
        actor: Actor description (e.g. 'Customer: Maya').

    Returns:
        bool: True on success, False otherwise.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, current_order_id FROM cups WHERE cup_code = %s;",
                    (cup_code,),
                )
                row = cur.fetchone()
                from_status = row[0] if row else "WITH_CUSTOMER"
                order_id = row[1] if row else None

                cur.execute(
                    """
                    UPDATE cups
                    SET 
                        status = 'IN_DISHWASHER',
                        updated_at = NOW()
                    WHERE cup_code = %s;
                    """,
                    (cup_code,),
                )
                cur.execute(
                    """
                    INSERT INTO cup_audit_log (cup_code, from_status, to_status, order_id, actor)
                    VALUES (%s, %s, 'IN_DISHWASHER', %s, %s);
                    """,
                    (cup_code, from_status, order_id, actor),
                )
                conn.commit()
                return True
    except Exception as exc:
        logger.error("return_cup_to_dishwasher failed for %s: %s", cup_code, exc)
        return False


def sanitize_and_shelve_cup(
    cup_code: str,
    actor: str = "Dishwasher",
) -> bool:
    """Transitions cup from IN_DISHWASHER -> CLEAN_ON_SHELF.

    Resets active order assignments and stamps last_washed_at.

    Args:
        cup_code: Sanitized cup code.
        actor: Actor performing the sanitization.

    Returns:
        bool: True on success, False otherwise.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cups
                    SET 
                        status = 'CLEAN_ON_SHELF',
                        current_order_id = NULL,
                        current_customer = NULL,
                        last_washed_at = NOW(),
                        updated_at = NOW()
                    WHERE cup_code = %s;
                    """,
                    (cup_code,),
                )
                cur.execute(
                    """
                    INSERT INTO cup_audit_log (cup_code, from_status, to_status, actor)
                    VALUES (%s, 'IN_DISHWASHER', 'CLEAN_ON_SHELF', %s);
                    """,
                    (cup_code, actor),
                )
                conn.commit()
                return True
    except Exception as exc:
        logger.error("sanitize_and_shelve_cup failed for %s: %s", cup_code, exc)
        return False


def get_cup_inventory_summary() -> dict[str, Any]:
    """Returns high-level summary of all cups grouped by status in a single pass.

    Returns:
        dict[str, Any]: Partitioned cup inventory and status counts.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT cup_code, status, current_order_id, current_customer, total_uses, last_washed_at
                    FROM cups
                    ORDER BY cup_code ASC;
                    """
                )
                cups = cur.fetchall()

                shelf: list[str] = []
                brewing: list[str] = []
                customer: list[str] = []
                dishwasher: list[str] = []

                for c in cups:
                    if c["last_washed_at"]:
                        c["last_washed_at"] = c["last_washed_at"].isoformat()
                    st = c.get("status")
                    code = c.get("cup_code", "")
                    if st == CupStatus.CLEAN_ON_SHELF:
                        shelf.append(code)
                    elif st == CupStatus.IN_BREWING:
                        brewing.append(code)
                    elif st == CupStatus.WITH_CUSTOMER:
                        customer.append(code)
                    elif st == CupStatus.IN_DISHWASHER:
                        dishwasher.append(code)

                return {
                    "total_cups": len(cups),
                    "counts": {
                        "CLEAN_ON_SHELF": len(shelf),
                        "IN_BREWING": len(brewing),
                        "WITH_CUSTOMER": len(customer),
                        "IN_DISHWASHER": len(dishwasher),
                    },
                    "shelf_clean": shelf,
                    "in_brewing": brewing,
                    "with_customer": customer,
                    "in_dishwasher": dishwasher,
                    "cups": cups,
                }
    except Exception as exc:
        logger.error("get_cup_inventory_summary failed: %s", exc)
        return {"total_cups": 0, "counts": {}, "cups": []}


def get_cup_audit_trail(
    cup_code: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Fetches recent audit log entries for cup lifecycle transitions.

    Args:
        cup_code: Optional cup code filter.
        limit: Max rows to return.

    Returns:
        list[dict[str, Any]]: Audit rows with ISO-8601 formatted timestamps.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if cup_code:
                    cur.execute(
                        """
                        SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                        FROM cup_audit_log
                        WHERE cup_code = %s
                        ORDER BY id DESC
                        LIMIT %s;
                        """,
                        (cup_code, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                        FROM cup_audit_log
                        ORDER BY id DESC
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                rows = cur.fetchall()
                for r in rows:
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()
                return rows
    except Exception as exc:
        logger.error("get_cup_audit_trail failed: %s", exc)
        return []


def get_total_cups_count() -> int:
    """Returns the total number of cups registered in the café inventory.

    Returns:
        int: Total cup count (defaults to DEFAULT_CUP_CAPACITY if unavailable).
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM cups;")
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else DEFAULT_CUP_CAPACITY
    except Exception as exc:
        logger.warning("get_total_cups_count failed, falling back to %d: %s", DEFAULT_CUP_CAPACITY, exc)
        return DEFAULT_CUP_CAPACITY


# ---------------------------------------------------------------------------
# Attack on Titan Character Configuration & Fallbacks
# ---------------------------------------------------------------------------
AOT_CHARACTERS: list[dict[str, Any]] = [
    {
        "id": "eren",
        "name": "Eren Yeager",
        "title": "Cadet #15 • Attack Titan",
        "regiment": "Survey Corps",
        "avatar": "⚔️",
        "badge_color": "#2D6A4F",
        "favorite_drink": "Matcha Espresso Fusion",
        "quote": "I'll keep moving forward... until every cup of matcha is finished!",
        "side": "north",
        "seat_number": 1,
    },
    {
        "id": "mikasa",
        "name": "Mikasa Ackerman",
        "title": "Top Graduate • Elite Scout",
        "regiment": "Survey Corps",
        "avatar": "🧣",
        "badge_color": "#9B2226",
        "favorite_drink": "Iced Ceremonial Matcha Latte",
        "quote": "This world is cruel... but this warm matcha is very beautiful.",
        "side": "north",
        "seat_number": 2,
    },
    {
        "id": "armin",
        "name": "Armin Arlert",
        "title": "Master Strategist • Tactical Advisor",
        "regiment": "Survey Corps",
        "avatar": "📖",
        "badge_color": "#E9D8A6",
        "favorite_drink": "Hot Uji Matcha Latte",
        "quote": "Someone who cannot sacrifice anything can never brew something extraordinary.",
        "side": "north",
        "seat_number": 3,
    },
    {
        "id": "levi",
        "name": "Levi Ackerman",
        "title": "Captain • Humanity's Strongest",
        "regiment": "Special Operations Squad",
        "avatar": "🗡️",
        "badge_color": "#1B4332",
        "favorite_drink": "Hot Uji Matcha Latte",
        "quote": "Drink it without spilling. And make sure this cup is returned spotless.",
        "side": "north",
        "seat_number": 4,
    },
    {
        "id": "erwin",
        "name": "Erwin Smith",
        "title": "Commander • 13th Commander",
        "regiment": "Survey Corps",
        "avatar": "🎖️",
        "badge_color": "#005F73",
        "favorite_drink": "Strawberry Matcha Float",
        "quote": "My soldiers, rage! My soldiers, scream! My soldiers, WHISK!!",
        "side": "north",
        "seat_number": 5,
    },
    {
        "id": "hange",
        "name": "Hange Zoë",
        "title": "Section Commander • Titan Researcher",
        "regiment": "Survey Corps",
        "avatar": "🔬",
        "badge_color": "#CA6702",
        "favorite_drink": "Hot Uji Matcha Latte",
        "quote": "Look at the micro-bubbles in this ceremonial foam! It's a marvel of nature!",
        "side": "north",
        "seat_number": 6,
    },
    {
        "id": "sasha",
        "name": "Sasha Blouse",
        "title": "Scout • Food & Rations Specialist",
        "regiment": "Survey Corps",
        "avatar": "🥔",
        "badge_color": "#BB3E03",
        "favorite_drink": "Strawberry Matcha Float",
        "quote": "Matcha float, sweet strawberry froth, cheesecake... can I eat the ceramic cup too?!",
        "side": "south",
        "seat_number": 7,
    },
    {
        "id": "connie",
        "name": "Connie Springer",
        "title": "Cadet • Ragako Vanguard",
        "regiment": "Survey Corps",
        "avatar": "⚡",
        "badge_color": "#EE9B00",
        "favorite_drink": "Strawberry Matcha Float",
        "quote": "Lord Connie demands extra sweetness and extra froth in Wall Rose!",
        "side": "south",
        "seat_number": 8,
    },
    {
        "id": "jean",
        "name": "Jean Kirstein",
        "title": "Cadet • Squad Leader",
        "regiment": "Survey Corps",
        "avatar": "🐎",
        "badge_color": "#52796F",
        "favorite_drink": "Iced Ceremonial Matcha Latte",
        "quote": "I just want a comfortable life in the interior with a quality oat milk matcha.",
        "side": "south",
        "seat_number": 9,
    },
    {
        "id": "reiner",
        "name": "Reiner Braun",
        "title": "Cadet • Armored Vanguard",
        "regiment": "Warrior",
        "avatar": "🛡️",
        "badge_color": "#6C584C",
        "favorite_drink": "Matcha Espresso Fusion",
        "quote": "I'm tired of running... I just need a cup strong enough to fortify my armor.",
        "side": "south",
        "seat_number": 10,
    },
    {
        "id": "annie",
        "name": "Annie Leonhart",
        "title": "Military Police • Solitary Fighter",
        "regiment": "Military Police",
        "avatar": "🥋",
        "badge_color": "#94D2BD",
        "favorite_drink": "Matcha Soft Serve",
        "quote": "Leave me alone with my chilled parfait. Don't make me kick you.",
        "side": "south",
        "seat_number": 11,
    },
    {
        "id": "historia",
        "name": "Historia Reiss",
        "title": "Queen of the Walls • Royal Aegis",
        "regiment": "Royal Government",
        "avatar": "👑",
        "badge_color": "#E7C169",
        "favorite_drink": "Matcha Soft Serve",
        "quote": "By royal decree, every hardworking cadet shall receive ceremonial matcha!",
        "side": "south",
        "seat_number": 12,
    },
]


def _load_all_aot_characters() -> list[dict[str, Any]]:
    """Loads all Attack on Titan characters from local dataset.

    Returns:
        list[dict[str, Any]]: Complete character pool.
    """
    chars_file = os.path.join(os.path.dirname(__file__), "static", "characters.json")
    if os.path.exists(chars_file):
        try:
            with open(chars_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                result = []
                for idx, c in enumerate(data):
                    name = c.get("name") or f"Scout #{idx+1}"
                    img_url = c.get("image_url") or ""
                    filename = os.path.basename(img_url) if img_url else ""
                    local_file = os.path.join(
                        os.path.dirname(__file__), "static", "images", "characters", filename
                    )
                    local_path = f"/images/characters/{filename}" if os.path.exists(local_file) else img_url
                    result.append({
                        "id": str(c.get("id", idx)),
                        "name": name,
                        "image": local_path or img_url,
                    })
                if result:
                    return result
        except Exception as exc:
            logger.error("Error loading AoT characters from %s: %s", chars_file, exc)

    # Core 12 fallback
    return [
        {"id": c["id"], "name": c["name"], "image": f"/images/characters/{c['id']}.webp"}
        for c in AOT_CHARACTERS
    ]


_AOT_CHARACTER_POOL: list[dict[str, Any]] = _load_all_aot_characters()
_diner_assignments: dict[str, dict[str, Any]] = {}


def get_dining_action_label(
    drink_name: str,
    status: str,
    items: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Generates context-aware status badges and action labels for diners.

    Distinguishes beverages (e.g. 'Sipping Matcha 🍵') from desserts/pastries
    (e.g. 'Savoring Cheesecake 🍰').

    Args:
        drink_name: Order drink or item title.
        status: OrderStatus string.
        items: Optional structured order items list.

    Returns:
        tuple[str, str]: (status_badge, action_label)
    """
    dlower = (drink_name or "").lower()
    is_dessert = any(
        kw in dlower for kw in ("cheesecake", "soft serve", "cake", "pastry", "parfait")
    )
    if items and isinstance(items, list):
        for it in items:
            cat = it.get("category", "")
            if cat in ("Dessert", "Pastry") or not it.get("is_drink", True):
                is_dessert = True
                break

    if status == OrderStatus.PENDING:
        return ("pending", "Order in Queue 🍃")
    elif status == OrderStatus.PREPARING:
        return ("preparing", "Plating Dessert 🍽️" if is_dessert else "Whisking at Bar 🥣")
    else:  # READY or COMPLETED
        if "cheesecake" in dlower:
            return ("ready", "Savoring Cheesecake 🍰")
        elif "soft serve" in dlower:
            return ("ready", "Savoring Soft Serve 🍦")
        elif is_dessert:
            return ("ready", "Enjoying Dessert 🍧")
        elif "espresso" in dlower:
            return ("ready", "Sipping Espresso Fusion ☕")
        elif "float" in dlower:
            return ("ready", "Sipping Matcha Float 🍓")
        elif "iced" in dlower:
            return ("ready", "Sipping Iced Matcha 🧊")
        else:
            return ("ready", "Sipping Matcha 🍵")


# ---------------------------------------------------------------------------
# Real-Time Telemetry & Analytics
# ---------------------------------------------------------------------------
def get_analytics_breakdown(timeframe: str = "minutes") -> dict[str, Any]:
    """Aggregates timeseries metrics, cup logs, and throughput over a timeframe.

    Uses PostgreSQL generate_series() to ensure continuous timeline buckets
    with no gaps across minutes (60m), hours (24h), and days (7d).

    Args:
        timeframe: 'minutes', 'hours', or 'days'.

    Returns:
        dict[str, Any]: Aggregated chart series, audit events, and KPI metrics.
    """
    timeframe = timeframe.lower()
    if timeframe not in ("minutes", "hours", "days"):
        timeframe = "minutes"

    chart_series: list[dict[str, Any]] = []
    popular_items: list[dict[str, Any]] = []
    audit_events: list[dict[str, Any]] = []
    customer_leaderboard: list[dict[str, Any]] = []

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Base Metrics by Timeframe
                if timeframe == "minutes":
                    time_label = "Past 60 Minutes (5-Min Slices)"
                    cur.execute(
                        """
                        SELECT 
                            to_char(b.bucket, 'HH24:MI') AS time_bucket,
                            COUNT(o.order_id) AS orders_count,
                            COALESCE(SUM(o.price), 0) AS total_revenue
                        FROM generate_series(
                            date_trunc('hour', NOW()) + (date_part('minute', NOW())::int / 5 * 5) * INTERVAL '1 minute' - INTERVAL '55 minutes',
                            date_trunc('hour', NOW()) + (date_part('minute', NOW())::int / 5 * 5) * INTERVAL '1 minute',
                            INTERVAL '5 minutes'
                        ) AS b(bucket)
                        LEFT JOIN orders o ON (
                            o.ordered_at >= b.bucket 
                            AND o.ordered_at < b.bucket + INTERVAL '5 minutes'
                        )
                        GROUP BY b.bucket
                        ORDER BY b.bucket ASC;
                        """
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    cur.execute(
                        """
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 42.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '60 minutes';
                        """
                    )
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 42.0}

                elif timeframe == "hours":
                    time_label = "Past 24 Hours (Hourly Slices)"
                    cur.execute(
                        """
                        SELECT 
                            to_char(b.bucket, 'HH24:00') AS time_bucket,
                            COUNT(o.order_id) AS orders_count,
                            COALESCE(SUM(o.price), 0) AS total_revenue
                        FROM generate_series(
                            date_trunc('hour', NOW()) - INTERVAL '23 hours',
                            date_trunc('hour', NOW()),
                            INTERVAL '1 hour'
                        ) AS b(bucket)
                        LEFT JOIN orders o ON (
                            o.ordered_at >= b.bucket 
                            AND o.ordered_at < b.bucket + INTERVAL '1 hour'
                        )
                        GROUP BY b.bucket
                        ORDER BY b.bucket ASC;
                        """
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    cur.execute(
                        """
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 45.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '24 hours';
                        """
                    )
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 45.0}

                else:  # days
                    time_label = "Past 7 Days (Daily Slices)"
                    cur.execute(
                        """
                        SELECT 
                            to_char(b.bucket, 'Mon DD') AS time_bucket,
                            COUNT(o.order_id) AS orders_count,
                            COALESCE(SUM(o.price), 0) AS total_revenue
                        FROM generate_series(
                            date_trunc('day', NOW()) - INTERVAL '6 days',
                            date_trunc('day', NOW()),
                            INTERVAL '1 day'
                        ) AS b(bucket)
                        LEFT JOIN orders o ON (
                            o.ordered_at >= b.bucket 
                            AND o.ordered_at < b.bucket + INTERVAL '1 day'
                        )
                        GROUP BY b.bucket
                        ORDER BY b.bucket ASC;
                        """
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    cur.execute(
                        """
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 48.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '7 days';
                        """
                    )
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 48.0}

                # 2. Popular creations breakdown in this timeframe
                interval_clause = "60 minutes" if timeframe == "minutes" else ("24 hours" if timeframe == "hours" else "7 days")
                cur.execute(
                    f"""
                    SELECT 
                        drink_name,
                        COUNT(*) as count,
                        COALESCE(SUM(price), 0) as revenue
                    FROM orders
                    WHERE ordered_at >= NOW() - INTERVAL '{interval_clause}'
                    GROUP BY drink_name
                    ORDER BY count DESC
                    LIMIT 6;
                    """
                )
                for r in cur.fetchall():
                    popular_items.append({
                        "name": r["drink_name"],
                        "count": int(r["count"]),
                        "revenue": float(r["revenue"]),
                    })

                # 3. Cup status distribution right now
                inv = get_cup_inventory_summary()
                counts = inv.get("counts", {})

                # 4. Audit Log Events in this timeframe
                cur.execute(
                    f"""
                    SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                    FROM cup_audit_log
                    WHERE created_at >= NOW() - INTERVAL '{interval_clause}'
                    ORDER BY id DESC
                    LIMIT 30;
                    """
                )
                audit_rows = cur.fetchall()
                if not audit_rows:
                    cur.execute(
                        """
                        SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                        FROM cup_audit_log
                        ORDER BY id DESC
                        LIMIT 30;
                        """
                    )
                    audit_rows = cur.fetchall()

                for a in audit_rows:
                    time_display = a["created_at"].strftime("%H:%M:%S") if a["created_at"] else "Just now"
                    audit_events.append({
                        "id": a["id"],
                        "cup_code": a["cup_code"],
                        "from_status": a["from_status"] or "—",
                        "to_status": a["to_status"] or "UNKNOWN",
                        "previous_status": a["from_status"] or "—",
                        "new_status": a["to_status"] or "UNKNOWN",
                        "order_id": a["order_id"] or "—",
                        "actor": a["actor"] or "Café System",
                        "trigger_event": a["actor"] or "Café System",
                        "created_at": time_display,
                        "timestamp": time_display,
                        "date": a["created_at"].strftime("%b %d") if a["created_at"] else "",
                    })

                # 5. Customer Leaderboard
                cur.execute(
                    """
                    SELECT name, favorite_drink, total_spent, total_orders, last_visit
                    FROM customers
                    ORDER BY total_spent DESC
                    LIMIT 8;
                    """
                )
                for c in cur.fetchall():
                    customer_leaderboard.append({
                        "name": c["name"],
                        "favorite": c["favorite_drink"] or "Ceremonial Matcha",
                        "total_spent": float(c["total_spent"] or 0),
                        "total_orders": int(c["total_orders"] or 0),
                        "last_visit": c["last_visit"].strftime("%b %d, %H:%M") if c["last_visit"] else "Today",
                    })

                # 6. Active orders in flight (clean up any stale zombie orders older than 10 minutes)
                cur.execute(
                    """
                    UPDATE orders 
                    SET status = 'CANCELLED' 
                    WHERE status IN ('PENDING', 'PREPARING') 
                    AND ordered_at < NOW() - INTERVAL '10 minutes';
                    """
                )
                conn.commit()

                cur.execute(
                    """
                    SELECT COUNT(*) AS active 
                    FROM orders 
                    WHERE status IN ('PENDING', 'PREPARING')
                    AND ordered_at >= NOW() - INTERVAL '10 minutes';
                    """
                )
                active_row = cur.fetchone()
                active_orders = int(active_row["active"]) if active_row else 0

                return {
                    "timeframe": timeframe,
                    "time_label": time_label,
                    "total_orders": int(summary["total_orders"]),
                    "total_revenue": round(float(summary["total_revenue"]), 2),
                    "avg_prep_time_sec": round(float(summary["avg_prep_time_sec"]), 1),
                    "active_orders": active_orders,
                    "cups_in_circulation": counts.get("WITH_CUSTOMER", 0) + counts.get("IN_BREWING", 0),
                    "cup_counts": counts,
                    "chart_series": chart_series,
                    "popular_items": popular_items,
                    "audit_events": audit_events,
                    "customer_leaderboard": customer_leaderboard,
                }
    except Exception as exc:
        logger.error("get_analytics_breakdown failed: %s", exc, exc_info=True)
        return {
            "timeframe": timeframe,
            "time_label": "Analytics Unavailable",
            "total_orders": 0,
            "total_revenue": 0.0,
            "avg_prep_time_sec": 45.0,
            "active_orders": 0,
            "cups_in_circulation": 0,
            "cup_counts": {},
            "chart_series": [],
            "popular_items": [],
            "audit_events": [],
            "customer_leaderboard": [],
        }


def get_active_diners() -> dict[str, Any]:
    """Returns active dine-in customers for the Attack on Titan dining scene.

    Each customer is assigned a character from the AoT pool and a seat (1-12).
    Diners auto-vacate after DINE_IN_DURATION_SECONDS.

    Returns:
        dict[str, Any]: Live table seats, diners, and cup circulation metrics.
    """
    now = datetime.now(timezone.utc)

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Retrieve recent dine-in orders
                cur.execute(
                    """
                    SELECT order_id, customer_name, drink_name, items, status, price,
                           ordered_at, ready_at, dining_option
                    FROM orders
                    WHERE ordered_at > NOW() - INTERVAL '2 minutes'
                      AND (dining_option = 'dine_in' OR dining_option IS NULL)
                    ORDER BY ordered_at DESC
                    LIMIT 30;
                    """
                )
                recent_orders = cur.fetchall()

                # Calculate live cup circulation from database
                cur.execute("SELECT status, count(*) FROM cups GROUP BY status;")
                cup_rows = cur.fetchall()
                cup_counts = {r["status"]: r["count"] for r in cup_rows}
                clean_cups = cup_counts.get("CLEAN_ON_SHELF", 0) + cup_counts.get("CLEAN", 0)
                in_brewing = cup_counts.get("IN_BREWING", 0)
                with_customer = cup_counts.get("WITH_CUSTOMER", 0)
                in_dishwasher = cup_counts.get("IN_DISHWASHER", 0)
                total_cups = sum(cup_counts.values()) or DEFAULT_CUP_CAPACITY
                cups_in_circulation = in_brewing + with_customer + in_dishwasher

        # 1. Filter valid active dine-in orders within the dining stay duration
        valid_orders: list[tuple[dict[str, Any], float, float, datetime]] = []
        for order in recent_orders:
            if order.get("dining_option") == DiningOption.TAKE_AWAY:
                continue

            ordered_at = order.get("ordered_at")
            if ordered_at and hasattr(ordered_at, "tzinfo") and ordered_at.tzinfo is None:
                ordered_at = ordered_at.replace(tzinfo=timezone.utc)

            if not ordered_at:
                continue

            elapsed = (now - ordered_at).total_seconds()
            remaining = max(0.0, DINE_IN_DURATION_SECONDS - elapsed)
            if remaining > 0:
                valid_orders.append((order, elapsed, remaining, ordered_at))

        # 2. Reconcile _diner_assignments: Prune all departed or expired orders
        active_oids = {item[0]["order_id"] for item in valid_orders}
        for stale_oid in list(_diner_assignments.keys()):
            if stale_oid not in active_oids:
                _diner_assignments.pop(stale_oid, None)

        # 3. Determine currently occupied seats strictly from verified active diners
        occupied_seats: set[int] = {
            info["seat"] for info in _diner_assignments.values()
        }

        # 4. Sort chronologically so earliest arrivals retain/fill lower seat indices
        valid_orders.sort(key=lambda x: x[3])

        active_diners: list[dict[str, Any]] = []
        for order, elapsed, remaining, ordered_at in valid_orders:
            oid = order["order_id"]

            # Assign character and seat if first time seeing this order
            if oid not in _diner_assignments:
                h = int(hashlib.md5(oid.encode()).hexdigest(), 16)
                char_idx = h % len(_AOT_CHARACTER_POOL)
                char = _AOT_CHARACTER_POOL[char_idx]

                seat: int | None = None
                for s in range(1, 13):
                    if s not in occupied_seats:
                        seat = s
                        break
                if seat is None:
                    continue  # Table full

                _diner_assignments[oid] = {
                    "character": char,
                    "seat": seat,
                }
                occupied_seats.add(seat)

            assignment = _diner_assignments[oid]

            status_badge, action_label = get_dining_action_label(
                order["drink_name"], order["status"], order.get("items")
            )
            dlower = (order["drink_name"] or "").lower()
            is_dessert = any(
                kw in dlower for kw in ("cheesecake", "soft serve", "cake", "pastry", "parfait")
            )

            active_diners.append({
                "order_id": oid,
                "customer_name": order["customer_name"],
                "drink_name": order["drink_name"],
                "status": order["status"],
                "status_badge": status_badge,
                "action_label": action_label,
                "is_dessert": is_dessert,
                "price": float(order["price"]) if order["price"] else 0.0,
                "seat_number": assignment["seat"],
                "character_id": assignment["character"]["id"],
                "character_name": assignment["character"]["name"],
                "image": assignment["character"]["image"],
                "seconds_remaining": round(remaining),
                "elapsed_seconds": round(elapsed),
                "ordered_at": ordered_at.isoformat() if ordered_at else None,
            })

        # Build full 12-seat representation
        diner_by_seat = {d["seat_number"]: d for d in active_diners}
        seats = [
            {**diner_by_seat[s], "occupied": True} if s in diner_by_seat else {"seat_number": s, "occupied": False}
            for s in range(1, 13)
        ]

        return {
            "total_seats": 12,
            "occupied_count": len(active_diners),
            "diners": active_diners,
            "seats": seats,
            "cups": {
                "total": total_cups,
                "in_circulation": cups_in_circulation,
                "clean_on_shelf": clean_cups,
                "in_brewing": in_brewing,
                "with_customer": with_customer,
                "in_dishwasher": in_dishwasher,
            },
        }

    except Exception as exc:
        logger.error("get_active_diners failed: %s", exc, exc_info=True)
        return {
            "total_seats": 12,
            "occupied_count": 0,
            "diners": [],
            "seats": [{"seat_number": s, "occupied": False} for s in range(1, 13)],
            "cups": {
                "total": DEFAULT_CUP_CAPACITY,
                "in_circulation": 0,
                "clean_on_shelf": DEFAULT_CUP_CAPACITY,
                "in_brewing": 0,
                "with_customer": 0,
                "in_dishwasher": 0,
            },
        }


def get_restaurant_state() -> dict[str, Any]:
    """Returns visualization state for the Survey Corps Mess Hall.

    Aggregates:
    - 12 seats on the Grand Dining Table (6 North, 6 South)
    - Active occupants, allocated cups, and order statuses
    - Barista Prep Station & Captain Levi's Cleaning Station

    Returns:
        dict[str, Any]: Structured mess hall state payload.
    """
    try:
        inv = get_cup_inventory_summary()
        cups_list = inv.get("cups", [])
        counts = inv.get("counts", {})

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT order_id, customer_name, drink_name, items, price, status,
                           cup_codes, cup_code, ordered_at, ready_at
                    FROM orders
                    ORDER BY id DESC
                    LIMIT 25;
                    """
                )
                recent_orders = cur.fetchall()

        # Map cups currently held by customer or barista
        active_cups_by_holder: dict[str, list[str]] = {}
        for c in cups_list:
            if c["status"] in (CupStatus.WITH_CUSTOMER, CupStatus.IN_BREWING):
                holder = (c.get("current_customer") or "").strip()
                if holder:
                    active_cups_by_holder.setdefault(holder, []).append(c["cup_code"])

        # Construct 12 seat entities
        seats: list[dict[str, Any]] = []
        assigned_orders: set[str] = set()

        for idx, char in enumerate(AOT_CHARACTERS):
            seat_num = idx + 1
            char_name = char["name"]
            char_first = char["name"].split()[0]

            matched_order = None
            for ord_row in recent_orders:
                if ord_row["order_id"] in assigned_orders:
                    continue
                cust = (ord_row.get("customer_name") or "").strip()
                if cust and (cust.lower() == char_name.lower() or cust.lower() == char_first.lower() or char_first.lower() in cust.lower()):
                    matched_order = ord_row
                    assigned_orders.add(ord_row["order_id"])
                    break

            held_cups = active_cups_by_holder.get(char_name, []) or active_cups_by_holder.get(char_first, [])

            if matched_order and matched_order["status"] == OrderStatus.PREPARING:
                seat_status = "ORDER_PREPARING"
                status_label = "Awaiting Barista (Whisking...) 🥣"
                order_id = matched_order["order_id"]
                drink = matched_order["drink_name"]
                price = float(matched_order["price"])
                cup_display = ", ".join(held_cups) if held_cups else "Allocating..."
                is_active = True
            elif matched_order and matched_order["status"] == OrderStatus.PENDING:
                seat_status = "ORDER_PENDING"
                status_label = "Order in Queue 🍃"
                order_id = matched_order["order_id"]
                drink = matched_order["drink_name"]
                price = float(matched_order["price"])
                cup_display = "In Queue"
                is_active = True
            elif held_cups:
                seat_status = "DRINKING"
                status_label = "Sipping Ceremonial Matcha 🍵"
                order_id = matched_order["order_id"] if matched_order else "ORD-ACTIVE"
                drink = matched_order["drink_name"] if matched_order else char["favorite_drink"]
                price = float(matched_order["price"]) if matched_order else DEFAULT_FALLBACK_PRICE
                cup_display = ", ".join(held_cups)
                is_active = True
            elif matched_order and matched_order["status"] == OrderStatus.READY:
                seat_status = "ENJOYING"
                status_label = "Enjoying Fresh Order ✨"
                order_id = matched_order["order_id"]
                drink = matched_order["drink_name"]
                price = float(matched_order["price"])
                cup_display = matched_order["cup_code"] or "Delivered"
                is_active = True
            else:
                seat_status = "IDLE_CHATTING"
                status_label = "Relaxing in Mess Hall 🪑"
                order_id = None
                drink = char["favorite_drink"]
                price = 0.0
                cup_display = None
                is_active = False

            seats.append({
                "seat_number": seat_num,
                "side": char["side"],
                "character_id": char["id"],
                "character_name": char["name"],
                "title": char["title"],
                "regiment": char["regiment"],
                "avatar": char["avatar"],
                "badge_color": char["badge_color"],
                "quote": char["quote"],
                "favorite_drink": char["favorite_drink"],
                "image": f"/images/characters/{char['id']}.webp",
                "is_active": is_active,
                "status": seat_status,
                "status_label": status_label,
                "order_id": order_id,
                "drink_name": drink,
                "price": price,
                "cup_code": cup_display,
                "held_cups": held_cups,
            })

        north_seats = [s for s in seats if s["side"] == "north"]
        south_seats = [s for s in seats if s["side"] == "south"]
        occupied_count = sum(1 for s in seats if s["is_active"])

        preparing_orders = [
            {"order_id": o["order_id"], "customer": o["customer_name"], "drink": o["drink_name"], "status": o["status"]}
            for o in recent_orders if o["status"] in (OrderStatus.PENDING, OrderStatus.PREPARING)
        ]

        clean_shelf = [c["cup_code"] for c in cups_list if c["status"] == CupStatus.CLEAN_ON_SHELF]
        in_dishwasher = [c["cup_code"] for c in cups_list if c["status"] == CupStatus.IN_DISHWASHER]
        in_brewing = [c["cup_code"] for c in cups_list if c["status"] == CupStatus.IN_BREWING]

        cleaning_data = {
            "name": "Captain Levi's Disinfection Bay",
            "inspector": "Captain Levi (Humanity's Strongest Cleaner)",
            "cleanliness_rating": "100% Spotless • Approved by Levi",
            "clean_shelf": clean_shelf,
            "in_dishwasher": in_dishwasher,
            "total_clean": len(clean_shelf),
            "total_washing": len(in_dishwasher),
            "clean_cup_count": len(clean_shelf),
            "dishwasher_cups": [{"cup_code": cc} for cc in in_dishwasher],
        }

        barista_data = {
            "name": "Wall Rose Garrison Kitchen Rail",
            "barista": "Kaito (Solo Waiter / Scout Barista)",
            "status": "Brewing Ceremonial Uji Matcha 🍃",
            "active_orders": preparing_orders,
            "in_brewing_cups": in_brewing,
            "brewing_cups": [
                {"cup_code": cc, "drink": "Ceremonial Matcha"} for cc in in_brewing
            ] + [
                {"cup_code": o.get("order_id", "Queue"), "drink": o.get("drink", "Matcha")} for o in preparing_orders
            ],
        }

        return {
            "mess_hall": {
                "name": "Survey Corps Mess Hall (Wall Rose HQ)",
                "theme": "Attack on Titan",
                "table_name": "The Great Scout Dining Table",
                "total_seats": 12,
                "occupied_seats": occupied_count,
                "north_seats": north_seats,
                "south_seats": south_seats,
                "all_seats": seats,
            },
            "cups_rail": cups_list,
            "barista_station": barista_data,
            "cleaning_bay": cleaning_data,
            "cleaning_station": cleaning_data,
            "inventory_counts": counts,
        }
    except Exception as exc:
        logger.error("get_restaurant_state failed: %s", exc, exc_info=True)
        return {
            "mess_hall": {
                "name": "Survey Corps Mess Hall",
                "total_seats": 12,
                "occupied_seats": 0,
                "north_seats": [],
                "south_seats": [],
                "all_seats": [],
            },
            "barista_station": {"barista": "Kaito", "active_orders": []},
            "cleaning_station": {"inspector": "Captain Levi", "clean_shelf": []},
            "inventory_counts": {},
        }
