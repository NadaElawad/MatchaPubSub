"""
Matcha Café - PostgreSQL Database Helper Module
Production-ready with ThreadedConnectionPool, dynamic catalog queries, and order lifecycle tracking.
"""

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
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


def create_pending_order(order_id, customer_name, drink_name, milk, sweetness, price, ordered_at=None, items=None, cup_codes=None, dining_option="take_away"):
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
                        order_id, customer_name, drink_name, milk, sweetness, price, cup_code, cup_codes, items, status, ordered_at, ready_at, dining_option
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', COALESCE(%s, NOW()), NULL, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        items = COALESCE(EXCLUDED.items, orders.items),
                        dining_option = COALESCE(EXCLUDED.dining_option, orders.dining_option),
                        status = 'PENDING';
                    """,
                    (order_id, customer_name, drink_name, milk, sweetness, price, cup_code_str, cup_codes, items_json, ordered_at, dining_option),
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


AOT_CHARACTERS = [
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


def get_analytics_breakdown(timeframe="minutes"):
    """
    Aggregates orders, revenue, cup metrics, and logs across minutes, hours, or days.
    """
    timeframe = timeframe.lower()
    if timeframe not in ("minutes", "hours", "days"):
        timeframe = "minutes"

    now = datetime.now(timezone.utc)
    chart_series = []
    popular_items = []
    audit_events = []
    customer_leaderboard = []

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Base Metrics by Timeframe
                if timeframe == "minutes":
                    time_label = "Past 60 Minutes (5-Min Slices)"
                    cur.execute("""
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
                    """)
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    # Summary cards for minutes
                    cur.execute("""
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 42.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '60 minutes';
                    """)
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 42.0}

                elif timeframe == "hours":
                    time_label = "Past 24 Hours (Hourly Slices)"
                    cur.execute("""
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
                    """)
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    cur.execute("""
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 45.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '24 hours';
                    """)
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 45.0}

                else:  # days
                    time_label = "Past 7 Days (Daily Slices)"
                    cur.execute("""
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
                    """)
                    rows = cur.fetchall()
                    for r in rows:
                        chart_series.append({
                            "time": r["time_bucket"],
                            "orders": int(r["orders_count"]),
                            "revenue": float(r["total_revenue"]),
                        })

                    cur.execute("""
                        SELECT 
                            COUNT(*) AS total_orders,
                            COALESCE(SUM(price), 0) AS total_revenue,
                            COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - ordered_at))), 48.0) AS avg_prep_time_sec
                        FROM orders
                        WHERE ordered_at >= NOW() - INTERVAL '7 days';
                    """)
                    summary = cur.fetchone() or {"total_orders": 0, "total_revenue": 0.0, "avg_prep_time_sec": 48.0}

                # 2. Popular creations breakdown in this timeframe
                interval_clause = "60 minutes" if timeframe == "minutes" else ("24 hours" if timeframe == "hours" else "7 days")
                cur.execute(f"""
                    SELECT 
                        drink_name,
                        COUNT(*) as count,
                        COALESCE(SUM(price), 0) as revenue
                    FROM orders
                    WHERE ordered_at >= NOW() - INTERVAL '{interval_clause}'
                    GROUP BY drink_name
                    ORDER BY count DESC
                    LIMIT 6;
                """)
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
                cur.execute(f"""
                    SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                    FROM cup_audit_log
                    WHERE created_at >= NOW() - INTERVAL '{interval_clause}'
                    ORDER BY id DESC
                    LIMIT 30;
                """)
                audit_rows = cur.fetchall()
                if not audit_rows:
                    cur.execute("""
                        SELECT id, cup_code, from_status, to_status, order_id, actor, created_at
                        FROM cup_audit_log
                        ORDER BY id DESC
                        LIMIT 30;
                    """)
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

                # 5. Customer Leaderboard (Top Scouts & Regulars)
                cur.execute("""
                    SELECT name, favorite_drink, total_spent, total_orders, last_visit
                    FROM customers
                    ORDER BY total_spent DESC
                    LIMIT 8;
                """)
                for c in cur.fetchall():
                    customer_leaderboard.append({
                        "name": c["name"],
                        "favorite": c["favorite_drink"] or "Ceremonial Matcha",
                        "total_spent": float(c["total_spent"] or 0),
                        "total_orders": int(c["total_orders"] or 0),
                        "last_visit": c["last_visit"].strftime("%b %d, %H:%M") if c["last_visit"] else "Today",
                    })

                # 6. Active orders in flight (clean up any stale zombie orders older than 10 minutes)
                cur.execute("""
                    UPDATE orders 
                    SET status = 'CANCELLED' 
                    WHERE status IN ('PENDING', 'PREPARING') 
                    AND ordered_at < NOW() - INTERVAL '10 minutes';
                """)
                conn.commit()

                cur.execute("""
                    SELECT COUNT(*) AS active 
                    FROM orders 
                    WHERE status IN ('PENDING', 'PREPARING')
                    AND ordered_at >= NOW() - INTERVAL '10 minutes';
                """)
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
    except Exception as e:
        print(f"[DB Error] get_analytics_breakdown: {e}")
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


# Characters available for random assignment to dine-in customers
def _load_all_aot_characters():
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
                    # Check if downloaded locally
                    local_file = os.path.join(os.path.dirname(__file__), "static", "images", "characters", filename)
                    local_path = f"/images/characters/{filename}" if os.path.exists(local_file) else img_url
                    result.append({
                        "id": str(c.get("id", idx)),
                        "name": name,
                        "image": local_path or img_url,
                    })
                if result:
                    return result
        except Exception as e:
            print(f"[AOT Character Load Error] {e}")

    # Fallback to core 12
    return [
        {"id": "eren", "name": "Eren Yeager", "image": "/images/characters/eren.webp"},
        {"id": "mikasa", "name": "Mikasa Ackerman", "image": "/images/characters/mikasa.webp"},
        {"id": "armin", "name": "Armin Arlert", "image": "/images/characters/armin.webp"},
        {"id": "levi", "name": "Levi Ackerman", "image": "/images/characters/levi.webp"},
        {"id": "erwin", "name": "Erwin Smith", "image": "/images/characters/erwin.webp"},
        {"id": "hange", "name": "Hange Zoë", "image": "/images/characters/hange.webp"},
        {"id": "sasha", "name": "Sasha Blouse", "image": "/images/characters/sasha.webp"},
        {"id": "connie", "name": "Connie Springer", "image": "/images/characters/connie.webp"},
        {"id": "jean", "name": "Jean Kirstein", "image": "/images/characters/jean.webp"},
        {"id": "reiner", "name": "Reiner Braun", "image": "/images/characters/reiner.webp"},
        {"id": "annie", "name": "Annie Leonhart", "image": "/images/characters/annie.webp"},
        {"id": "historia", "name": "Historia Reiss", "image": "/images/characters/historia.webp"},
    ]

_AOT_CHARACTER_POOL = _load_all_aot_characters()

# In-memory mapping of order_id -> assigned character + seat for the dining scene
_diner_assignments = {}

DINE_IN_DURATION_SECONDS = 45


def get_dining_action_label(drink_name: str, status: str, items=None) -> tuple:
    """
    Returns (status_badge, action_label) appropriately matching drinks vs desserts/pastries.
    Fixes bug where desserts were labeled 'sipping matcha'.
    """
    dlower = (drink_name or "").lower()
    is_dessert = ("cheesecake" in dlower or "soft serve" in dlower or "cake" in dlower 
                  or "pastry" in dlower or "parfait" in dlower)
    if items and isinstance(items, list):
        for it in items:
            cat = it.get("category", "")
            if cat in ("Dessert", "Pastry") or not it.get("is_drink", True):
                is_dessert = True
                break

    if status == "PENDING":
        return ("pending", "Order in Queue 🍃")
    elif status == "PREPARING":
        return ("preparing", "Plating Dessert 🍽️" if is_dessert else "Whisking at Bar 🥣")
    else:  # READY or DELIVERED
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


def get_active_diners():
    """
    Returns active dine-in customers for the visualization.
    Each customer is assigned a random AoT character from the 100+ character pool and a seat (1-12).
    Customers leave after DINE_IN_DURATION_SECONDS from order placement.
    """
    import hashlib

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get orders from the last 2 minutes that chose dine_in
                cur.execute("""
                    SELECT order_id, customer_name, drink_name, items, status, price,
                           ordered_at, ready_at, dining_option
                    FROM orders
                    WHERE ordered_at > NOW() - INTERVAL '2 minutes'
                      AND (dining_option = 'dine_in' OR dining_option IS NULL)
                    ORDER BY ordered_at DESC
                    LIMIT 30;
                """)
                recent_orders = cur.fetchall()

                # Calculate live cup circulation from database
                cur.execute("SELECT status, count(*) FROM cups GROUP BY status;")
                cup_rows = cur.fetchall()
                cup_counts = {r["status"]: r["count"] for r in cup_rows}
                clean_cups = cup_counts.get("CLEAN_ON_SHELF", 0) + cup_counts.get("CLEAN", 0)
                in_brewing = cup_counts.get("IN_BREWING", 0)
                with_customer = cup_counts.get("WITH_CUSTOMER", 0)
                in_dishwasher = cup_counts.get("IN_DISHWASHER", 0)
                total_cups = sum(cup_counts.values()) or 12
                cups_in_circulation = in_brewing + with_customer + in_dishwasher

        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)

        # Filter to dine-in orders only (explicitly excluding take_away)
        active_diners = []
        occupied_seats = {
            info["seat"] for info in _diner_assignments.values()
        }

        for order in recent_orders:
            # Skip if explicitly marked take_away
            if order.get("dining_option") == "take_away":
                continue

            oid = order["order_id"]
            ordered_at = order["ordered_at"]
            if ordered_at and hasattr(ordered_at, 'tzinfo') and ordered_at.tzinfo is None:
                ordered_at = ordered_at.replace(tzinfo=timezone.utc)

            if not ordered_at:
                continue

            elapsed = (now - ordered_at).total_seconds()
            remaining = max(0, DINE_IN_DURATION_SECONDS - elapsed)

            # Customer has left
            if remaining <= 0:
                _diner_assignments.pop(oid, None)
                continue

            # Assign character + seat if not already assigned
            if oid not in _diner_assignments:
                # Random/hash selection across the full 136-character AoT dataset
                h = int(hashlib.md5(oid.encode()).hexdigest(), 16)
                char_idx = h % len(_AOT_CHARACTER_POOL)
                char = _AOT_CHARACTER_POOL[char_idx]

                # Find an unoccupied seat
                seat = None
                for s in range(1, 13):
                    if s not in occupied_seats:
                        seat = s
                        break
                if seat is None:
                    continue  # table full

                _diner_assignments[oid] = {
                    "character": char,
                    "seat": seat,
                }

            assignment = _diner_assignments[oid]
            occupied_seats.add(assignment["seat"])

            status_badge, action_label = get_dining_action_label(
                order["drink_name"], order["status"], order.get("items")
            )
            dlower = (order["drink_name"] or "").lower()
            is_dessert = ("cheesecake" in dlower or "soft serve" in dlower or "cake" in dlower 
                          or "pastry" in dlower or "parfait" in dlower)

            active_diners.append({
                "order_id": oid,
                "customer_name": order["customer_name"],
                "drink_name": order["drink_name"],
                "status": order["status"],
                "status_badge": status_badge,
                "action_label": action_label,
                "is_dessert": is_dessert,
                "price": float(order["price"]) if order["price"] else 0,
                "seat_number": assignment["seat"],
                "character_id": assignment["character"]["id"],
                "character_name": assignment["character"]["name"],
                "image": assignment["character"]["image"],
                "seconds_remaining": round(remaining),
                "elapsed_seconds": round(elapsed),
                "ordered_at": ordered_at.isoformat() if ordered_at else None,
            })

        # Build seats array (12 seats, each empty or occupied)
        seats = []
        diner_by_seat = {d["seat_number"]: d for d in active_diners}
        for s in range(1, 13):
            if s in diner_by_seat:
                seats.append({**diner_by_seat[s], "occupied": True})
            else:
                seats.append({"seat_number": s, "occupied": False})

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
            }
        }

    except Exception as e:
        print(f"[DB Error] get_active_diners: {e}")
        return {
            "total_seats": 12,
            "occupied_count": 0,
            "diners": [],
            "seats": [{"seat_number": s, "occupied": False} for s in range(1, 13)],
            "cups": {
                "total": 12,
                "in_circulation": 0,
                "clean_on_shelf": 12,
                "in_brewing": 0,
                "with_customer": 0,
                "in_dishwasher": 0,
            }
        }


def get_restaurant_state():
    """
    Returns real-time visualization state of the Attack on Titan restaurant:
    - 12 seats on the Grand Survey Corps Mess Hall Table (6 North, 6 South)
    - Active occupants, allocated cups, and current order status
    - Barista Prep Station & Captain Levi's Cleaning & Sanitizing Bay
    """
    try:
        inv = get_cup_inventory_summary()
        cups_list = inv.get("cups", [])
        counts = inv.get("counts", {})

        # Fetch recent orders (last 2 hours) or any active orders
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT order_id, customer_name, drink_name, items, price, status, cup_codes, cup_code, ordered_at, ready_at
                    FROM orders
                    ORDER BY id DESC
                    LIMIT 25;
                """)
                recent_orders = cur.fetchall()

        # Map cups currently WITH_CUSTOMER or IN_BREWING
        active_cups_by_holder = {}
        for c in cups_list:
            if c["status"] in ("WITH_CUSTOMER", "IN_BREWING"):
                holder = (c.get("current_customer") or "").strip()
                if holder:
                    if holder not in active_cups_by_holder:
                        active_cups_by_holder[holder] = []
                    active_cups_by_holder[holder].append(c["cup_code"])

        # Build 12 seats
        seats = []
        assigned_orders = set()

        for idx, char in enumerate(AOT_CHARACTERS):
            seat_num = idx + 1
            char_name = char["name"]
            char_first = char["name"].split()[0]

            # Find matching order for this character or assign active order
            matched_order = None
            for ord_row in recent_orders:
                if ord_row["order_id"] in assigned_orders:
                    continue
                cust = (ord_row.get("customer_name") or "").strip()
                if cust and (cust.lower() == char_name.lower() or cust.lower() == char_first.lower() or char_first.lower() in cust.lower()):
                    matched_order = ord_row
                    assigned_orders.add(ord_row["order_id"])
                    break

            # Check if this character has active cups
            held_cups = active_cups_by_holder.get(char_name, []) or active_cups_by_holder.get(char_first, [])

            # Determine seat state
            if matched_order and matched_order["status"] == "PREPARING":
                seat_status = "ORDER_PREPARING"
                status_label = "Awaiting Barista (Whisking...) 🥣"
                order_id = matched_order["order_id"]
                drink = matched_order["drink_name"]
                price = float(matched_order["price"])
                cup_display = ", ".join(held_cups) if held_cups else "Allocating..."
                is_active = True
            elif matched_order and matched_order["status"] == "PENDING":
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
                price = float(matched_order["price"]) if matched_order else 6.50
                cup_display = ", ".join(held_cups)
                is_active = True
            elif matched_order and matched_order["status"] == "READY":
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

        # Barista Station
        preparing_orders = [
            {"order_id": o["order_id"], "customer": o["customer_name"], "drink": o["drink_name"], "status": o["status"]}
            for o in recent_orders if o["status"] in ("PENDING", "PREPARING")
        ]

        # Dishwasher Station
        clean_shelf = [c["cup_code"] for c in cups_list if c["status"] == "CLEAN_ON_SHELF"]
        in_dishwasher = [c["cup_code"] for c in cups_list if c["status"] == "IN_DISHWASHER"]
        in_brewing = [c["cup_code"] for c in cups_list if c["status"] == "IN_BREWING"]
        with_customer = [c["cup_code"] for c in cups_list if c["status"] == "WITH_CUSTOMER"]

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
    except Exception as e:
        print(f"[DB Error] get_restaurant_state: {e}")
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


