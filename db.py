"""
Matcha Café - PostgreSQL Database Helper Module
Production-ready with ThreadedConnectionPool, dynamic catalog queries, and order lifecycle tracking.
"""

import json
import os
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

_pool = None


def get_pool():
    """Lazily initialize and return a ThreadedConnectionPool."""
    global _pool
    if _pool is None or _pool.closed:
        host = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        dbname = os.environ.get("DB_NAME", "matcha_cafe")
        user = os.environ.get("DB_USER", "barista")
        password = os.environ.get("DB_PASSWORD", "matchapassword")

        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
    return _pool


@contextmanager
def get_db_connection():
    """Context manager for acquiring and returning a connection from the pool."""
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)


def check_db_health():
    """Verify that PostgreSQL is reachable and responsive."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return True
    except Exception as e:
        print(f"[DB Health Check Error] {e}")
        return False


def get_menu():
    """Fetch all in-stock products ordered by category and name."""
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
                for r in rows:
                    r["price"] = float(r["price"])
                return rows
    except Exception as e:
        print(f"[DB Error] get_menu failed: {e}")
        return []


def get_product_by_name(drink_name):
    """Fetch a single product by name (case-insensitive)."""
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
    except Exception as e:
        print(f"[DB Error] get_product_by_name failed for '{drink_name}': {e}")
        return None


def get_product_price(drink_name):
    """Fetch product price from products table with fallback."""
    product = get_product_by_name(drink_name)
    if product and product.get("price") is not None:
        return product["price"]
    return 6.50


def create_pending_order(order_id, customer_name, drink_name, milk, sweetness, price, ordered_at=None, items=None, cup_codes=None):
    """
    Records an incoming order with status 'PENDING' before the barista begins preparation.
    Supports single drinks and multi-item orders.
    """
    try:
        items_json = json.dumps(items) if items is not None else None
        cup_code_str = ", ".join(cup_codes) if cup_codes else None
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, ordered_at, ready_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', COALESCE(%s, NOW()), NULL)
                    ON CONFLICT (order_id) DO UPDATE SET
                        items = COALESCE(EXCLUDED.items, orders.items),
                        status = 'PENDING';
                    """,
                    (order_id, customer_name, drink_name, milk, sweetness, price, cup_code_str, cup_codes, items_json, ordered_at),
                )
                conn.commit()
                return True
    except Exception as e:
        print(f"[DB Error] Failed to create pending order {order_id}: {e}")
        return False


def update_order_status(order_id, status, prepared_by=None):
    """Update order status (e.g. PREPARING)."""
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
    except Exception as e:
        print(f"[DB Error] Failed to update order status for {order_id}: {e}")
        return False


def get_order(order_id):
    """Fetch order details and current status by order_id."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at
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
    except Exception as e:
        print(f"[DB Error] Failed to fetch order {order_id}: {e}")
        return None


def get_recent_orders(limit=20, customer_name=None):
    """Fetch recent orders, optionally filtered by customer."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if customer_name:
                    cur.execute(
                        """
                        SELECT order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at
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
                        SELECT order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at
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
    except Exception as e:
        print(f"[DB Error] Failed to fetch recent orders: {e}")
        return []


def get_customer_profile(customer_name):
    """Fetch loyalty profile and preferences for a customer."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT name, favorite_drink, preferred_milk, preferred_sweetness, total_spent, total_orders, last_visit
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
    except Exception as e:
        print(f"[DB Error] Failed to fetch customer {customer_name}: {e}")
        return None


def record_order(order_data):
    """
    Real-time transaction whenever an order is completed:
    1. Inserts or updates the completed order in 'orders' table (sets status='READY', ready_at=NOW()).
    2. Updates the 'customers' table immediately:
       - Increments total_spent by the drink price
       - Increments total_orders count
       - Dynamically calculates customer preferences (favorite drink, milk, sweetness)
       - Updates last_visit timestamp.
    """
    order_id = order_data.get("order_id")
    customer_name = order_data.get("client_name") or order_data.get("customer_name")
    drink = order_data.get("drink") or order_data.get("drink_name")
    milk = order_data.get("milk")
    sweetness = order_data.get("sweetness")
    price = float(order_data.get("price", 6.50))
    cup_code = order_data.get("cup_code")
    cup_codes = order_data.get("cup_codes")
    items = order_data.get("items")
    items_json = json.dumps(items) if items is not None else None
    if not cup_code and cup_codes:
        cup_code = ", ".join(cup_codes)
    status = order_data.get("status", "READY")
    prepared_by = order_data.get("prepared_by", "Kaito (Solo Waiter)")
    ordered_at = order_data.get("ordered_at")
    ready_at = order_data.get("ready_at")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Upsert order into orders table
                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, prepared_by, ordered_at, ready_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), COALESCE(%s, NOW()))
                    ON CONFLICT (order_id) DO UPDATE SET
                        cup_code = COALESCE(EXCLUDED.cup_code, orders.cup_code),
                        cup_codes = COALESCE(EXCLUDED.cup_codes, orders.cup_codes),
                        items = COALESCE(EXCLUDED.items, orders.items),
                        status = EXCLUDED.status,
                        prepared_by = EXCLUDED.prepared_by,
                        ready_at = EXCLUDED.ready_at;
                    """,
                    (order_id, customer_name, drink, milk, sweetness, price, cup_code, cup_codes, items_json, status, prepared_by, ordered_at, ready_at),
                )

                # 2. Upsert customer profile: increment total_spent and total_orders
                cur.execute(
                    """
                    INSERT INTO customers (
                        name, favorite_drink, preferred_milk, preferred_sweetness, total_spent, total_orders, last_visit, updated_at
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

                # 3. Recalculate customer preferences contextually for their favorite drink (Option 2)
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
                        )
                    WHERE name = %s;
                    """,
                    (customer_name, customer_name, customer_name, customer_name, customer_name, customer_name),
                )

                conn.commit()
                return True
    except Exception as e:
        print(f"[DB Error] Failed to record order & update customer {customer_name}: {e}")
        return False


# Alias for backward compatibility
log_order = record_order


# ==========================================
# 🍵 CUP INVENTORY & LIFECYCLE MANAGEMENT
# ==========================================

def claim_cup_for_order(order_id, customer_name, actor="Barista"):
    """
    Atomically claims one clean cup from the shelf.
    Uses 'FOR UPDATE SKIP LOCKED' so concurrent baristas never contend or double-claim.
    Transitions: CLEAN_ON_SHELF -> IN_BREWING
    Returns cup_code (e.g. 'CUP-01') or None if shelf is empty.
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
                    # Log audit trail
                    cur.execute(
                        """
                        INSERT INTO cup_audit_log (cup_code, from_status, to_status, order_id, actor)
                        VALUES (%s, 'CLEAN_ON_SHELF', 'IN_BREWING', %s, %s);
                        """,
                        (cup_code, order_id, actor),
                    )
                    # Associate cup with order in orders table
                    cur.execute(
                        "UPDATE orders SET cup_code = %s WHERE order_id = %s;",
                        (cup_code, order_id),
                    )
                    conn.commit()
                    return cup_code
                return None
    except Exception as e:
        print(f"[DB Error] claim_cup_for_order failed: {e}")
        return None


def hand_cup_to_customer(cup_code, order_id=None, customer_name=None, actor="Barista"):
    """
    Transitions cup from IN_BREWING -> WITH_CUSTOMER when customer collects drink.
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
    except Exception as e:
        print(f"[DB Error] hand_cup_to_customer failed for {cup_code}: {e}")
        return False


def return_cup_to_dishwasher(cup_code, actor="Customer"):
    """
    Transitions cup from WITH_CUSTOMER -> IN_DISHWASHER.
    Called when customer brings their used cup to the return station.
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
    except Exception as e:
        print(f"[DB Error] return_cup_to_dishwasher failed for {cup_code}: {e}")
        return False


def sanitize_and_shelve_cup(cup_code, actor="Dishwasher"):
    """
    Transitions cup from IN_DISHWASHER -> CLEAN_ON_SHELF.
    Resets current_order_id and current_customer, marks last_washed_at.
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
    except Exception as e:
        print(f"[DB Error] sanitize_and_shelve_cup failed for {cup_code}: {e}")
        return False


def get_cup_inventory_summary():
    """
    Returns high-level summary of all cups grouped by status.
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
                for c in cups:
                    if c["last_washed_at"]:
                        c["last_washed_at"] = c["last_washed_at"].isoformat()

                shelf = [c["cup_code"] for c in cups if c["status"] == "CLEAN_ON_SHELF"]
                brewing = [c["cup_code"] for c in cups if c["status"] == "IN_BREWING"]
                customer = [c["cup_code"] for c in cups if c["status"] == "WITH_CUSTOMER"]
                dishwasher = [c["cup_code"] for c in cups if c["status"] == "IN_DISHWASHER"]

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
    except Exception as e:
        print(f"[DB Error] get_cup_inventory_summary failed: {e}")
        return {"total_cups": 0, "counts": {}, "cups": []}


def get_cup_audit_trail(cup_code=None, limit=20):
    """Fetch recent audit log entries for cup lifecycle transitions."""
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
    except Exception as e:
        print(f"[DB Error] get_cup_audit_trail failed: {e}")
        return []


def get_total_cups_count():
    """Returns the total number of cups in the café pool (defaults to 12)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM cups;")
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 12
    except Exception:
        return 12

