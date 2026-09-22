"""
Matcha Café - PostgreSQL Database Helper Module
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """Connect to PostgreSQL using environment variables or local defaults."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    dbname = os.environ.get("DB_NAME", "matcha_cafe")
    user = os.environ.get("DB_USER", "barista")
    password = os.environ.get("DB_PASSWORD", "matchapassword")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def get_product_price(drink_name):
    """Fetch product price from products table."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT price FROM products WHERE name = %s;", (drink_name,))
                row = cur.fetchone()
                if row:
                    return float(row[0])
    except Exception as e:
        print(f"[DB Warning] Could not fetch price for '{drink_name}': {e}")
    return 6.50  # Default fallback price


def record_order(order_data):
    """
    Real-time transaction whenever an order is completed:
    1. Inserts the completed order into the 'orders' table.
    2. Updates the 'customers' table immediately:
       - Increments total_spent by the drink price
       - Increments total_orders count
       - Dynamically calculates customer preferences (favorite drink, milk, sweetness)
       - Updates last_visit timestamp.
    """
    order_id = order_data.get("order_id")
    customer_name = order_data.get("client_name")
    drink = order_data.get("drink")
    milk = order_data.get("milk")
    sweetness = order_data.get("sweetness")
    price = float(order_data.get("price", 6.50))
    status = order_data.get("status", "READY")
    prepared_by = order_data.get("prepared_by", "Kaito (Solo Waiter)")
    ordered_at = order_data.get("ordered_at")
    ready_at = order_data.get("ready_at")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Insert order into orders table
                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_name, drink_name, milk, sweetness, price, status, prepared_by, ordered_at, ready_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING;
                    """,
                    (order_id, customer_name, drink, milk, sweetness, price, status, prepared_by, ordered_at, ready_at),
                )

                # 2. Upsert customer profile: increment total_spent and total_orders
                cur.execute(
                    """
                    INSERT INTO customers (
                        name, favorite_drink, preferred_milk, preferred_sweetness, total_spent, total_orders, last_visit, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 1, %s, NOW()
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
