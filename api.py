#!/usr/bin/env python3
"""
Matcha PubSub - Order API Service (FastAPI)

Role:
- Decouples client applications from raw Kafka brokers and direct database credentials.
- Dynamically queries PostgreSQL 'products' table to serve live menus and authoritatively validate prices.
- Validates order requests with Pydantic.
- Publishes validated order events to the Kafka 'matcha-orders' topic.
- Tracks order lifecycles (PENDING -> PREPARING -> READY) via PostgreSQL.
- Provides health checks (Liveness & Readiness) for production orchestrators (Kubernetes / Docker).
"""

import json
import os
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import db

# Configuration
BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "matcha-orders")
READY_TOPIC = os.environ.get("READY_TOPIC", "matcha-ready")

# Customizable options allowed across the café
ALLOWED_MILKS = ["Oat Milk", "Almond Milk", "Whole Milk", "Soy Milk", "None / Black"]
ALLOWED_SWEETNESS = ["0% (Unsweetened)", "25%", "50%", "75%", "100%"]

# Global Kafka Producer instance
kafka_producer: Optional[Producer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager: initialize connections on startup, clean up on shutdown."""
    global kafka_producer
    print(f"🚀 [API Startup] Connecting Kafka producer to {BOOTSTRAP_SERVER}...")
    try:
        kafka_producer = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "client.id": "matcha-order-api",
            "retries": 5,
            "acks": "all",
            "broker.address.family": "v4",
        })
        print("✅ [API Startup] Kafka producer ready.")
    except Exception as e:
        print(f"⚠️ [API Startup Warning] Could not initialize Kafka producer: {e}")

    # Verify DB connection pool
    if db.check_db_health():
        print("✅ [API Startup] PostgreSQL connection verified.")
    else:
        print("⚠️ [API Startup Warning] PostgreSQL check failed on startup.")

    yield

    # Shutdown
    if kafka_producer:
        print("🛑 [API Shutdown] Flushing Kafka producer queue...")
        kafka_producer.flush(timeout=5.0)
    print("👋 [API Shutdown] Matcha Order API shut down cleanly.")


app = FastAPI(
    title="Matcha Café Order API",
    description="Production-grade API for ordering matcha drinks, querying live menus, and tracking order progress.",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for web dashboards, mobile apps, or local frontend clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Pydantic Data Models ---

class ProductItem(BaseModel):
    id: int
    name: str
    category: str
    price: float
    description: Optional[str] = None
    in_stock: bool


class MenuResponse(BaseModel):
    products: List[ProductItem]
    available_milks: List[str]
    available_sweetness: List[str]


class OrderCreateRequest(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=100, examples=["Maya"])
    drink: str = Field(..., min_length=1, examples=["Strawberry Matcha Float"])
    milk: Optional[str] = Field("Oat Milk", examples=["Oat Milk"])
    sweetness: Optional[str] = Field("50%", examples=["50%"])


class OrderResponse(BaseModel):
    order_id: str
    customer_name: str
    drink: str
    milk: Optional[str]
    sweetness: Optional[str]
    price: float
    status: str
    ordered_at: str
    message: str


class OrderStatusResponse(BaseModel):
    order_id: str
    customer_name: str
    drink_name: str
    milk: Optional[str]
    sweetness: Optional[str]
    price: float
    cup_code: Optional[str] = None
    status: str
    prepared_by: Optional[str] = None
    ordered_at: Optional[str] = None
    ready_at: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    database: str
    kafka: str
    timestamp: str


# --- Endpoints ---

@app.get("/", tags=["General"])
def root():
    return {
        "message": "🍵 Welcome to the Matcha Café Order API!",
        "documentation": "/docs",
        "endpoints": {
            "menu": "/menu",
            "place_order": "POST /orders",
            "order_status": "/orders/{order_id}",
            "health": "/health",
        },
    }


@app.get("/health", response_model=HealthResponse, tags=["Observability"])
def health_check():
    """Liveness & Readiness probe verifying PostgreSQL and Kafka health."""
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
def get_live_menu():
    """
    Fetches the live menu directly from PostgreSQL 'products' table.
    Ensures clients always see real-time prices and stock availability.
    """
    products = db.get_menu()
    return {
        "products": products,
        "available_milks": ALLOWED_MILKS,
        "available_sweetness": ALLOWED_SWEETNESS,
    }


@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED, tags=["Orders"])
def place_order(order: OrderCreateRequest):
    """
    Submit a new matcha order:
    1. Validates that the requested drink exists in the database and is in stock.
    2. Resolves the authoritative price from PostgreSQL (preventing client price tampering).
    3. Persists the order to PostgreSQL with status 'PENDING'.
    4. Produces the order event to the Kafka 'matcha-orders' topic for the barista.
    """
    global kafka_producer

    # 1. Authoritatively validate product from PostgreSQL
    product = db.get_product_by_name(order.drink)
    if not product:
        available = [p["name"] for p in db.get_menu()]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Drink '{order.drink}' is not on the menu. Available items: {', '.join(available)}",
        )

    if not product.get("in_stock", True):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Drink '{product['name']}' is currently out of stock.",
        )

    authoritative_price = product["price"]
    order_id = f"ORD-{random.randint(1000, 9999)}"
    ordered_at = datetime.now(timezone.utc).isoformat()

    # 2. Persist order in DB as PENDING
    db.create_pending_order(
        order_id=order_id,
        customer_name=order.customer_name.strip(),
        drink_name=product["name"],
        milk=order.milk,
        sweetness=order.sweetness,
        price=authoritative_price,
        ordered_at=ordered_at,
    )

    # 3. Produce to Kafka topic
    order_event = {
        "order_id": order_id,
        "client_name": order.customer_name.strip(),
        "drink": product["name"],
        "milk": order.milk,
        "sweetness": order.sweetness,
        "price": authoritative_price,
        "ordered_at": ordered_at,
    }

    if kafka_producer:
        try:
            kafka_producer.produce(
                topic=ORDERS_TOPIC,
                key=order_id.encode("utf-8"),
                value=json.dumps(order_event).encode("utf-8"),
            )
            kafka_producer.flush(2.0)
        except Exception as e:
            print(f"[API Error] Failed to publish order {order_id} to Kafka: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Order recorded in database, but failed to queue to barista Kafka topic.",
            )
    else:
        print("[API Warning] Kafka producer not initialized; order recorded in DB only.")

    return {
        "order_id": order_id,
        "customer_name": order.customer_name.strip(),
        "drink": product["name"],
        "milk": order.milk,
        "sweetness": order.sweetness,
        "price": authoritative_price,
        "status": "PENDING",
        "ordered_at": ordered_at,
        "message": f"Order {order_id} submitted to the barista. Sit back and relax while it is prepared!",
    }


@app.get("/orders/{order_id}", response_model=OrderStatusResponse, tags=["Orders"])
def get_order_status(order_id: str):
    """
    Check the live status of an order (PENDING, PREPARING, or READY).
    Clients can poll this endpoint instead of requiring raw Kafka consumer listeners.
    """
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' was not found.",
        )
    return order


@app.get("/orders", response_model=List[OrderStatusResponse], tags=["Orders"])
def list_orders(
    customer: Optional[str] = Query(None, description="Filter by customer name"),
    limit: int = Query(20, ge=1, le=100, description="Max orders to return"),
):
    """List recent orders with optional filtering."""
    return db.get_recent_orders(limit=limit, customer_name=customer)


@app.get("/customers/{customer_name}", tags=["Customers"])
def get_customer_profile(customer_name: str):
    """Fetch customer loyalty metrics and automatically discovered preferences."""
    profile = db.get_customer_profile(customer_name)
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer '{customer_name}' has not placed any orders yet.",
        )
@app.get("/cups", tags=["Cups & Inventory"])
def get_cup_inventory():
    """Returns the live cup inventory breakdown (Shelf, Barista, Customer, Dishwasher)."""
    return db.get_cup_inventory_summary()


@app.post("/cups/{cup_code}/return", tags=["Cups & Inventory"])
def return_cup(cup_code: str, customer_name: Optional[str] = None):
    """
    Customer returns their used cup to the café dishwasher station.
    Emits an event to 'matcha-cup-returns' for the automated dishwasher worker.
    """
    global kafka_producer
    event = {
        "cup_code": cup_code,
        "customer_name": customer_name or "Guest",
        "returned_at": datetime.now(timezone.utc).isoformat(),
    }

    # Transition cup status in database
    success = db.return_cup_to_dishwasher(cup_code, actor=f"Customer: {customer_name or 'Guest'}")
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cup '{cup_code}' not found or could not be returned.",
        )

    # Publish return event to Kafka
    if kafka_producer:
        try:
            kafka_producer.produce(
                topic="matcha-cup-returns",
                key=cup_code.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            kafka_producer.flush(2.0)
        except Exception as e:
            print(f"[API Error] Failed to publish cup return event: {e}")

    return {
        "cup_code": cup_code,
        "status": "IN_DISHWASHER",
        "message": f"Cup {cup_code} returned to the dishwasher station for sanitization.",
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)

