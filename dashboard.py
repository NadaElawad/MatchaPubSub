#!/usr/bin/env python3
"""
Matcha Café - Manager & Analytics Dashboard

Queries PostgreSQL to display:
1. What we offer (Products catalog & pricing)
2. Customers table (Loyalty, total spent, preferences)
3. Total café metrics (Revenue, top-selling drinks)
"""

from db import get_db_connection


class Color:
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


def print_dashboard():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                print(f"\n{Color.BOLD}{Color.GREEN}╭──────────────────────────────────────────────────────────────────────────╮{Color.RESET}")
                print(f"{Color.BOLD}{Color.GREEN}│               🍵 MATCHA CAFÉ - DATABASE DASHBOARD & ANALYTICS            │{Color.RESET}")
                print(f"{Color.BOLD}{Color.GREEN}╰──────────────────────────────────────────────────────────────────────────╯{Color.RESET}\n")

                # 1. Products Table
                print(f"{Color.BOLD}📋 1. PRODUCTS & MENU CATALOG ('products' table):{Color.RESET}")
                print(f"{Color.DIM}{'─' * 74}{Color.RESET}")
                print(f" {'ID':<4} {'Item Name':<32} {'Category':<10} {'Price':<8} {'In Stock'}")
                print(f"{Color.DIM}{'─' * 74}{Color.RESET}")

                cur.execute("SELECT id, name, category, price, in_stock FROM products ORDER BY id;")
                for p_id, name, cat, price, in_stock in cur.fetchall():
                    stock_str = f"{Color.GREEN}Yes{Color.RESET}" if in_stock else f"{Color.RED}No{Color.RESET}"
                    print(f" {p_id:<4} {name:<32} {cat:<10} ${price:>5.2f}   {stock_str}")
                print()

                # 2. Customers Table
                print(f"{Color.BOLD}👥 2. CUSTOMERS & PREFERENCES ('customers' table):{Color.RESET}")
                print(f"{Color.DIM}{'─' * 74}{Color.RESET}")
                print(f" {'Name':<12} {'Spent':<10} {'Orders':<8} {'Favorite Drink':<28} {'Milk / Sweet'}")
                print(f"{Color.DIM}{'─' * 74}{Color.RESET}")

                cur.execute(
                    """
                    SELECT name, total_spent, total_orders, favorite_drink, preferred_milk, preferred_sweetness
                    FROM customers
                    ORDER BY total_spent DESC;
                    """
                )
                customers = cur.fetchall()
                if not customers:
                    print(f" {Color.DIM}(No customer profiles yet. Place an order to see real-time updates!){Color.RESET}")
                else:
                    for name, spent, count, fav, milk, sweet in customers:
                        fav_str = fav or "N/A"
                        pref_str = f"{milk or 'Any'}, {sweet or 'Any'}"
                        print(f" {Color.BOLD}{name:<12}{Color.RESET} {Color.GREEN}${spent:>6.2f}{Color.RESET}   {count:<8} {Color.CYAN}{fav_str:<28}{Color.RESET} {pref_str}")
                print()

                # 3. Overall Café Performance
                cur.execute("SELECT COALESCE(SUM(total_spent), 0), COALESCE(SUM(total_orders), 0) FROM customers;")
                total_rev, total_drinks = cur.fetchone()

                print(f"{Color.BOLD}💰 3. CAFÉ REVENUE & TOTALS:{Color.RESET}")
                print(f"   • Total Revenue : {Color.BOLD}{Color.GREEN}${total_rev:.2f}{Color.RESET}")
                print(f"   • Total Drinks  : {Color.BOLD}{Color.YELLOW}{total_drinks}{Color.RESET}")
                print()

                # 4. Cup Inventory & Tracking
                cur.execute("""
                    SELECT cup_code, status, current_order_id, current_customer, total_uses
                    FROM cups
                    ORDER BY cup_code ASC;
                """)
                cups = cur.fetchall()
                if cups:
                    print(f"{Color.BOLD}🍵 4. CUP INVENTORY & LIFECYCLE (Fixed Pool of {len(cups)}):{Color.RESET}")
                    print(f"{Color.DIM}{'─' * 74}{Color.RESET}")
                    print(f" {'Cup Code':<10} {'Status':<20} {'Current Holder / Order':<30} {'Uses'}")
                    print(f"{Color.DIM}{'─' * 74}{Color.RESET}")
                    for c_code, c_status, c_order, c_cust, c_uses in cups:
                        if c_status == "CLEAN_ON_SHELF":
                            status_str = f"{Color.GREEN}CLEAN_ON_SHELF{Color.RESET}"
                            holder = f"{Color.DIM}Ready on shelf{Color.RESET}"
                        elif c_status == "IN_BREWING":
                            status_str = f"{Color.YELLOW}IN_BREWING{Color.RESET}"
                            holder = f"Barista (Order {c_order})"
                        elif c_status == "WITH_CUSTOMER":
                            status_str = f"{Color.CYAN}WITH_CUSTOMER{Color.RESET}"
                            holder = f"{c_cust} ({c_order})"
                        elif c_status == "IN_DISHWASHER":
                            status_str = f"{Color.MAGENTA}IN_DISHWASHER{Color.RESET}"
                            holder = f"Washing & Sanitizing"
                        else:
                            status_str = c_status
                            holder = "-"
                        print(f" {Color.BOLD}{c_code:<10}{Color.RESET} {status_str:<30} {holder:<30} {c_uses}")
                    print()

    except Exception as e:
        print(f"[Error] Could not load dashboard: {e}")


if __name__ == "__main__":
    print_dashboard()
