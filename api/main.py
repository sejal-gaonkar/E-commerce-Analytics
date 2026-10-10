

import os
import psycopg
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(
    title="E-Commerce Analytics API",
    description="Real-time e-commerce order and payment analytics",
    version="1.0.0",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ecommerce:ecommerce@localhost:5432/ecommerce",
)


def get_connection():
    return psycopg.connect(DATABASE_URL)


@app.get("/")
def home():
    return {"message": "E-Commerce Analytics API is running"}


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except psycopg.Error:
        raise HTTPException(
            status_code=503, detail="Database unavailable"
        )


@app.get("/orders")
def get_orders(limit: int = Query(default=20, ge=1, le=100)):
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT order_id, customer_id, product_id,
                       quantity, amount, order_time, status
                FROM orders
                ORDER BY order_time DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()

        return [
            {
                "order_id": r[0],
                "customer_id": r[1],
                "product_id": r[2],
                "quantity": r[3],
                "amount": float(r[4]),
                "order_time": r[5].isoformat(),
                "status": r[6],
            }
            for r in rows
        ]
    except psycopg.Error:
        raise HTTPException(
            status_code=503, detail="Could not retrieve orders"
        )


@app.get("/analytics/revenue")
def get_revenue():
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(amount), 0),
                       COALESCE(AVG(amount), 0)
                FROM orders
                """
            ).fetchone()

        return {
            "total_orders": row[0],
            "total_revenue": float(row[1]),
            "average_order_value": round(float(row[2]), 2),
        }
    except psycopg.Error:
        raise HTTPException(
            status_code=503, detail="Could not calculate revenue"
        )


@app.get("/analytics/payments")
def get_payment_summary():
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT payment_status,
                       COUNT(*),
                       COALESCE(SUM(amount), 0)
                FROM payments
                GROUP BY payment_status
                ORDER BY payment_status
                """
            ).fetchall()

        return [
            {
                "payment_status": r[0],
                "count": r[1],
                "total_amount": float(r[2]),
            }
            for r in rows
        ]
    except psycopg.Error:
        raise HTTPException(
            status_code=503, detail="Could not retrieve payment analytics"
        )

