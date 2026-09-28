"""Matcha PubSub - Order API Service (FastAPI).

This module provides the primary HTTP ingestion and state query interface for
the Matcha Café system. It handles:
- Dynamic catalog retrieval with authoritative pricing from PostgreSQL.
- Pydantic-based input validation and tray item normalization.
- Safe Kafka event production for decoupled worker consumption.
- Observability and health probes for Kubernetes orchestration.
- Live dining visualization state and timeseries telemetry.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import logging
import mimetypes
import os
import random
from typing import Any, AsyncGenerator

# Ensure explicit web audio MIME types for Safari & modern browsers
mimetypes.add_type("audio/wav", ".wav")
mimetypes.add_type("audio/mp4", ".m4a")

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import db

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("matcha.api")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# ---------------------------------------------------------------------------
# Service Configuration & Constants
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVER: str = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
ORDERS_TOPIC: str = os.environ.get("ORDERS_TOPIC", "matcha-orders")
READY_TOPIC: str = os.environ.get("READY_TOPIC", "matcha-ready")
RETURNS_TOPIC: str = os.environ.get("RETURNS_TOPIC", "matcha-cup-returns")

ALLOWED_MILKS: list[str] = [
    "Oat Milk",
    "Almond Milk",
    "Whole Milk",
    "Soy Milk",
    "None / Black",
]

ALLOWED_SWEETNESS: list[str] = [
    "0% (Unsweetened)",
    "25%",
    "50%",
    "75%",
    "100%",
]

kafka_producer: Producer | None = None


# ---------------------------------------------------------------------------
# Application Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and graceful shutdown.

    Initializes the shared Confluent Kafka Producer and verifies the
    PostgreSQL connection pool. Flushes buffered producer messages on shutdown.

    Args:
        app: The FastAPI application instance.

    Yields:
        None
    """
    global kafka_producer
    logger.info("Initializing Kafka producer (bootstrap.servers=%s)", BOOTSTRAP_SERVER)
    try:
        kafka_producer = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "client.id": "matcha-order-api",
            "retries": 5,
            "acks": "all",
            "broker.address.family": "v4",
        })
        logger.info("Kafka producer initialized successfully.")
    except Exception as exc:
        logger.warning("Could not initialize Kafka producer on startup: %s", exc)

    if db.check_db_health():
        logger.info("PostgreSQL database connection pool verified.")
    else:
        logger.warning("PostgreSQL database health check failed on startup.")

    yield

    if kafka_producer:
        logger.info("Flushing Kafka producer message queue before shutdown...")
        kafka_producer.flush(timeout=5.0)
    logger.info("Matcha Order API shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI Application Definition
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Matcha Café Order API",
    description="Production-grade API for ordering ceremonial matcha, tracking finite cup lifecycles, and telemetry.",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static and Asset Mounts
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    images_dir = os.path.join(static_dir, "images")
    if os.path.exists(images_dir):
        app.mount("/images", StaticFiles(directory=images_dir), name="images")


# ---------------------------------------------------------------------------
# Pydantic Schemas & DTOs
# ---------------------------------------------------------------------------
class ProductItem(BaseModel):
    """Catalog product representation."""
    id: int = Field(..., description="Unique product identifier")
    name: str = Field(..., description="Product name")
    category: str = Field(..., description="Product category (Drink, Dessert, Pastry)")
    price: float = Field(..., ge=0.0, description="Unit price in USD")
    description: str | None = Field(None, description="Tasting notes and ingredients")
    in_stock: bool = Field(True, description="Inventory availability flag")


class MenuResponse(BaseModel):
    """Complete live menu with available customizations."""
    products: list[ProductItem]
    available_milks: list[str]
    available_sweetness: list[str]


class OrderItemInput(BaseModel):
    """Line item in a customer order tray."""
    drink_name: str | None = Field(None, min_length=1, examples=["Strawberry Matcha Float"])
    drink: str | None = Field(None, min_length=1, examples=["Strawberry Matcha Float"])
    milk: str | None = Field("Oat Milk", examples=["Oat Milk"])
    sweetness: str | None = Field("50%", examples=["50%"])
    quantity: int = Field(1, ge=1, le=20, examples=[1])

    def get_drink_name(self) -> str:
        """Resolves the normalized item title."""
        return (self.drink_name or self.drink or "Matcha Special").strip()


class OrderCreateRequest(BaseModel):
    """Payload to create a new order."""
    customer_name: str = Field(..., min_length=1, max_length=100, examples=["Maya"])
    items: list[OrderItemInput] | None = None
    drink_name: str | None = Field(None, examples=["Strawberry Matcha Float"])
    drink: str | None = Field(None, examples=["Strawberry Matcha Float"])
    milk: str | None = Field("Oat Milk", examples=["Oat Milk"])
    sweetness: str | None = Field("50%", examples=["50%"])
    dining_option: str | None = Field("take_away", examples=["dine_in", "take_away"])


class OrderResponse(BaseModel):
    """Response returned upon successful order ingestion."""
    order_id: str
    customer_name: str
    drink_name: str
    drink: str | None = None
    items: list[dict[str, Any]] | None = None
    milk: str | None = None
    sweetness: str | None = None
    price: float
    status: str
    ordered_at: str
    dining_option: str = "take_away"
    message: str


class OrderStatusResponse(BaseModel):
    """Full detail response for an existing order."""
    order_id: str
    customer_name: str
    drink_name: str
    items: list[dict[str, Any]] | None = None
    milk: str | None = None
    sweetness: str | None = None
    price: float
    cup_code: str | None = None
    cup_codes: list[str] | None = None
    status: str
    prepared_by: str | None = None
    ordered_at: str | None = None
    ready_at: str | None = None
    dining_option: str | None = "take_away"


class HealthResponse(BaseModel):
    """Liveness and readiness probe response."""
    status: str
    database: str
    kafka: str
    timestamp: str


class SeatOrderRequest(BaseModel):
    """Payload for seat-specific dining room order."""
    seat_number: int = Field(..., ge=1, le=12)
    character_name: str | None = None
    drink_name: str | None = None
    milk: str | None = "Oat Milk"
    sweetness: str | None = "50%"


# ---------------------------------------------------------------------------
# API Routes: Core & Observability
# ---------------------------------------------------------------------------
@app.get("/", tags=["General"])
def root() -> Any:
    """Serves the web client application or welcoming metadata."""
    index_file = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {
        "message": "🍵 Welcome to the Matcha Café Order API!",
        "documentation": "/docs",
    }


@app.get("/api", tags=["General"])
def api_endpoints_index() -> dict[str, Any]:
    """Returns top-level endpoint directory and API discovery links."""
    return {
        "message": "🍵 Welcome to the Matcha Café Order API!",
        "documentation": "/docs",
        "endpoints": {
            "menu": "/menu",
            "cups": "/cups",
            "place_order": "POST /orders",
            "order_status": "/orders/{order_id}",
            "health": "/health",
            "restaurant_diners": "/restaurant/diners",
            "restaurant_state": "/restaurant/state",
            "analytics": "/analytics",
        },
    }


@app.get("/health", response_model=HealthResponse, tags=["Observability"])
def health_check() -> dict[str, str]:
    """Liveness & readiness health probe verifying PostgreSQL and Kafka."""
    db_ok = db.check_db_health()
    kafka_ok = kafka_producer is not None

    overall_status = "healthy" if (db_ok and kafka_ok) else "degraded"

    return {
        "status": overall_status,
        "database": "connected" if db_ok else "disconnected",
        "kafka": "connected" if kafka_ok else "unavailable",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/menu", response_model=MenuResponse, tags=["Menu & Products"])
def get_live_menu() -> dict[str, Any]:
    """Fetches the real-time product menu from the database."""
    products = db.get_menu()
    return {
        "products": products,
        "available_milks": ALLOWED_MILKS,
        "available_sweetness": ALLOWED_SWEETNESS,
    }


# ---------------------------------------------------------------------------
# API Routes: Order Ingestion & Lifecycle
# ---------------------------------------------------------------------------
@app.post(
    "/orders",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Orders"],
)
def place_order(order: OrderCreateRequest) -> dict[str, Any]:
    """Submits a new order to the café.

    Validates item availability and authoritative prices from PostgreSQL,
    persists the pending record, and produces an order event to Kafka.

    Args:
        order: Validated order request.

    Returns:
        dict[str, Any]: Ingested order response.

    Raises:
        HTTPException: On validation, capacity, or persistence failures.
    """
    global kafka_producer

    cust_name = order.customer_name.strip()
    if not cust_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Customer name is required and cannot be empty.",
        )

    # Normalize single drink vs multi-item list
    items_input: list[OrderItemInput] = []
    if order.items and len(order.items) > 0:
        items_input = order.items
    elif order.drink_name or order.drink:
        dname = order.drink_name or order.drink
        items_input = [
            OrderItemInput(
                drink_name=dname,
                drink=dname,
                milk=order.milk,
                sweetness=order.sweetness,
                quantity=1,
            )
        ]
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order must contain either 'items' list or a 'drink_name'.",
        )

    processed_items: list[dict[str, Any]] = []
    total_price: float = 0.0

    for item in items_input:
        target_name = item.get_drink_name()
        product = db.get_product_by_name(target_name)
        if not product:
            available = [p["name"] for p in db.get_menu()]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Item '{target_name}' is not on the menu. Available items: {', '.join(available)}",
            )

        if not product.get("in_stock", True):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Item '{product['name']}' is currently out of stock.",
            )

        is_drink = (product.get("category", "Drink").lower() == "drink")
        unit_price = float(product["price"])
        qty = max(1, item.quantity)
        line_total = unit_price * qty
        total_price += line_total

        processed_items.append({
            "drink_name": product["name"],
            "name": product["name"],
            "drink": product["name"],
            "category": product["category"],
            "price": unit_price,
            "quantity": qty,
            "line_total": line_total,
            "milk": item.milk if is_drink else None,
            "sweetness": item.sweetness if is_drink else None,
            "is_drink": is_drink,
        })

    # Validate finite ceramic cup limits
    drink_count = sum(it["quantity"] for it in processed_items if it.get("is_drink", False))
    max_cups = db.get_total_cups_count()
    if drink_count > max_cups:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Order exceeds café cup capacity. You requested {drink_count} drinks, "
                f"but our total café limit is {max_cups} cups per order."
            ),
        )

    order_id = f"ORD-{random.randint(1000, 9999)}"
    ordered_at = datetime.now(timezone.utc).isoformat()
    summary_drink = ", ".join(f"{it['quantity']}x {it['drink_name']}" for it in processed_items)
    dining_opt = order.dining_option or "take_away"

    # Persist pending order to database
    success = db.create_pending_order(
        order_id=order_id,
        customer_name=cust_name,
        drink_name=summary_drink,
        milk=processed_items[0]["milk"] if len(processed_items) == 1 else None,
        sweetness=processed_items[0]["sweetness"] if len(processed_items) == 1 else None,
        price=round(total_price, 2),
        ordered_at=ordered_at,
        items=processed_items,
        dining_option=dining_opt,
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record pending order in the database.",
        )

    # Publish order event to Kafka topic
    order_event = {
        "order_id": order_id,
        "client_name": cust_name,
        "customer_name": cust_name,
        "drink_name": summary_drink,
        "drink": summary_drink,
        "price": round(total_price, 2),
        "total_price": round(total_price, 2),
        "items": processed_items,
        "ordered_at": ordered_at,
        "dining_option": dining_opt,
    }

    if kafka_producer:
        try:
            kafka_producer.produce(
                topic=ORDERS_TOPIC,
                key=order_id.encode("utf-8"),
                value=json.dumps(order_event).encode("utf-8"),
            )
            kafka_producer.flush(2.0)
            logger.info("Published order event %s to topic %s", order_id, ORDERS_TOPIC)
        except Exception as exc:
            logger.error("Failed to publish order %s to Kafka: %s", order_id, exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Order recorded in database, but failed to queue to barista Kafka topic.",
            )
    else:
        logger.warning("Kafka producer not available; order %s recorded in database only.", order_id)

    return {
        "order_id": order_id,
        "customer_name": cust_name,
        "drink_name": summary_drink,
        "drink": summary_drink,
        "items": processed_items,
        "milk": processed_items[0]["milk"] if len(processed_items) == 1 else None,
        "sweetness": processed_items[0]["sweetness"] if len(processed_items) == 1 else None,
        "price": round(total_price, 2),
        "status": "PENDING",
        "ordered_at": ordered_at,
        "dining_option": dining_opt,
        "message": (
            f"Order {order_id} ({len(processed_items)} items) submitted to the barista. "
            "Sit back and relax while it is prepared!"
        ),
    }


@app.get("/orders/{order_id}", response_model=OrderStatusResponse, tags=["Orders"])
def get_order_status(order_id: str) -> dict[str, Any]:
    """Retrieves live status and fulfillment details for an order."""
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' was not found.",
        )
    return order


@app.get("/orders", response_model=list[OrderStatusResponse], tags=["Orders"])
def list_orders(
    customer: str | None = Query(None, description="Filter by customer name"),
    limit: int = Query(20, ge=1, le=100, description="Max orders to return"),
) -> list[dict[str, Any]]:
    """Lists recent orders with optional filtering by customer."""
    return db.get_recent_orders(limit=limit, customer_name=customer)


@app.get("/customers/{customer_name}", tags=["Customers"])
def get_customer_profile(customer_name: str) -> dict[str, Any]:
    """Fetches customer loyalty profile and dynamic preferences."""
    profile = db.get_customer_profile(customer_name)
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer '{customer_name}' has not placed any orders yet.",
        )
    return profile


# ---------------------------------------------------------------------------
# API Routes: Cup Inventory & Dishwashing
# ---------------------------------------------------------------------------
@app.get("/cups", tags=["Cups & Inventory"])
def get_cup_inventory() -> dict[str, Any]:
    """Returns the live cup inventory breakdown across shelf, barista, customer, and dishwasher."""
    return db.get_cup_inventory_summary()


@app.post("/cups/{cup_code}/return", tags=["Cups & Inventory"])
def return_cup(cup_code: str, customer_name: str | None = None) -> dict[str, str]:
    """Returns a used cup to the dishwasher station."""
    global kafka_producer
    event = {
        "cup_code": cup_code,
        "customer_name": customer_name or "Guest",
        "returned_at": datetime.now(timezone.utc).isoformat(),
    }

    success = db.return_cup_to_dishwasher(
        cup_code, actor=f"Customer: {customer_name or 'Guest'}"
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cup '{cup_code}' not found or could not be returned.",
        )

    if kafka_producer:
        try:
            kafka_producer.produce(
                topic=RETURNS_TOPIC,
                key=cup_code.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            kafka_producer.flush(2.0)
            logger.info("Published return event for %s to topic %s", cup_code, RETURNS_TOPIC)
        except Exception as exc:
            logger.error("Failed to publish cup return event for %s: %s", cup_code, exc)

    return {
        "cup_code": cup_code,
        "status": "IN_DISHWASHER",
        "message": f"Cup {cup_code} returned to the dishwasher station for sanitization.",
    }


# ---------------------------------------------------------------------------
# API Routes: Restaurant Visualization & Analytics
# ---------------------------------------------------------------------------
@app.get("/restaurant/diners", tags=["Restaurant Visualisation"])
def get_active_diners() -> dict[str, Any]:
    """Returns the live state of the 12-seat Mess Hall table and real-time cup metrics."""
    return db.get_active_diners()


@app.get("/restaurant/state", tags=["Restaurant Visualisation"])
def get_restaurant_realtime_state() -> dict[str, Any]:
    """Returns full Attack on Titan themed restaurant state."""
    return db.get_restaurant_state()


@app.get("/analytics", tags=["Analytics & Logs"])
def get_analytics(
    timeframe: str = Query("minutes", description="Aggregation timeframe: minutes, hours, or days"),
) -> dict[str, Any]:
    """Returns aggregated timeseries breakdown, popular items, and cup audit logs."""
    if timeframe not in ("minutes", "hours", "days"):
        timeframe = "minutes"
    return db.get_analytics_breakdown(timeframe)


@app.post("/restaurant/order-seat", tags=["Restaurant Visualisation"])
def order_for_seat(req: SeatOrderRequest) -> dict[str, Any]:
    """Places an order on behalf of a specific seat in the Mess Hall."""
    char = next((c for c in db.AOT_CHARACTERS if c["seat_number"] == req.seat_number), None)
    char_name = req.character_name or (char["name"] if char else f"Cadet #{req.seat_number}")
    dname = req.drink_name or (char["favorite_drink"] if char else "Strawberry Matcha Float")

    order_create = OrderCreateRequest(
        customer_name=char_name,
        drink_name=dname,
        drink=dname,
        milk=req.milk,
        sweetness=req.sweetness,
        dining_option="dine_in",
    )
    return place_order(order_create)


@app.post("/restaurant/simulate-rush", tags=["Restaurant Visualisation"])
def simulate_scout_rush(count: int = Query(default=5, ge=1, le=12)) -> dict[str, Any]:
    """Simulates a dining rush by creating concurrent orders for Scout Regiment cadets.

    Selects random cadets from the character pool and diverse items from the menu,
    placing dine-in orders strictly constrained to the number of available seats
    in the 12-seat Mess Hall so no cadets are served standing up without a seat.
    """
    diners_state = db.get_active_diners()
    occupied = diners_state.get("occupied_count", 0)
    total_seats = diners_state.get("total_seats", 12)
    available_seats = max(0, total_seats - occupied)

    if available_seats == 0:
        return {
            "message": "Mess Hall is at full capacity (12/12 seats occupied). No seats available for a Scout rush.",
            "count": 0,
            "orders": [],
            "available_seats": 0,
            "occupied_count": occupied,
            "total_seats": total_seats,
        }

    # Strictly limit orders to the number of physically available seats
    actual_count = min(count, available_seats)

    rush_orders: list[dict[str, Any]] = []
    pool = getattr(db, "_AOT_CHARACTER_POOL", db.AOT_CHARACTERS) or db.AOT_CHARACTERS

    # Filter out cadets who are already currently seated at the table
    seated_names = {
        (d.get("character_name") or "").strip().lower()
        for d in diners_state.get("diners", [])
        if d.get("character_name")
    } | {
        (d.get("customer_name") or "").strip().lower()
        for d in diners_state.get("diners", [])
        if d.get("customer_name")
    }
    available_pool = [
        c for c in pool
        if (c.get("name") or "").strip().lower() not in seated_names
    ] or pool
    sample_size = min(actual_count, len(available_pool))
    candidates = random.sample(available_pool, k=sample_size)

    menu = db.get_menu()
    available_items = [p for p in menu if p.get("in_stock", True)] or [
        {"name": "Hot Uji Matcha Latte", "category": "Drink"},
        {"name": "Iced Ceremonial Matcha Latte", "category": "Drink"},
        {"name": "Matcha Basque Cheesecake", "category": "Pastry"},
    ]

    for char in candidates:
        try:
            chosen = random.choice(available_items)
            item_name = chosen["name"]
            is_drink = chosen.get("category", "Drink").lower() == "drink"
            ord_req = OrderCreateRequest(
                customer_name=char["name"],
                drink_name=item_name,
                drink=item_name,
                milk="Oat Milk" if is_drink else None,
                sweetness="50%" if is_drink else None,
                dining_option="dine_in",
            )
            res = place_order(ord_req)
            rush_orders.append(res)
        except Exception as exc:
            logger.warning("Simulate rush order failed for %s: %s", char.get("name"), exc)

    return {
        "message": (
            f"Scout Regiment meal rush triggered for {len(rush_orders)} cadet(s) "
            f"({len(rush_orders)} of {available_seats} remaining seats filled)!"
        ),
        "count": len(rush_orders),
        "orders": rush_orders,
        "available_seats": available_seats - len(rush_orders),
        "occupied_count": occupied + len(rush_orders),
        "total_seats": total_seats,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
