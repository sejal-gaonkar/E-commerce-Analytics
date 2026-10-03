import streamlit as st
import pandas as pd
import psycopg2


# -----------------------------
# Page configuration
# -----------------------------
st.set_page_config(
    page_title="E-commerce Analytics",
    page_icon="🛒",
    layout="wide"
)


# -----------------------------
# PostgreSQL connection
# -----------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce",
        user="ecommerce",
        password="ecommerce"
    )


conn = get_connection()


# -----------------------------
# Page title
# -----------------------------
st.title("🛒 Real-Time E-commerce Analytics")
st.caption("Data pipeline: Python → Kafka → Spark → PostgreSQL")


# -----------------------------
# Load summary metrics
# -----------------------------
summary_query = """
SELECT
    COUNT(*) AS total_orders,
    COALESCE(SUM(amount), 0) AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders;
"""

summary = pd.read_sql(summary_query, conn).iloc[0]


# -----------------------------
# KPI cards
# -----------------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Orders",
        f"{int(summary['total_orders']):,}"
    )

with col2:
    st.metric(
        "Total Revenue",
        f"₹{summary['total_revenue']:,.2f}"
    )

with col3:
    st.metric(
        "Unique Customers",
        f"{int(summary['unique_customers']):,}"
    )


st.divider()


# -----------------------------
# Revenue by product
# -----------------------------
st.subheader("Revenue by Product")

product_query = """
SELECT
    p.product_name,
    p.category,
    SUM(o.quantity) AS units_sold,
    SUM(o.amount) AS revenue
FROM orders o
JOIN products p
    ON o.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC;
"""

product_data = pd.read_sql(product_query, conn)

st.bar_chart(
    product_data.set_index("product_name")["revenue"]
)


# -----------------------------
# Two-column section
# -----------------------------
left, right = st.columns(2)


# -----------------------------
# Orders by city
# -----------------------------
with left:
    st.subheader("Orders by City")

    city_query = """
    SELECT
        c.city,
        COUNT(*) AS orders
    FROM orders o
    JOIN customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.city
    ORDER BY orders DESC;
    """

    city_data = pd.read_sql(city_query, conn)

    st.bar_chart(
        city_data.set_index("city")["orders"]
    )


# -----------------------------
# Payment methods
# -----------------------------
with right:
    st.subheader("Payment Methods")

    payment_query = """
    SELECT
        payment_method,
        COUNT(*) AS transactions
    FROM payments
    GROUP BY payment_method
    ORDER BY transactions DESC;
    """

    payment_data = pd.read_sql(payment_query, conn)

    st.bar_chart(
        payment_data.set_index("payment_method")["transactions"]
    )


st.divider()


# -----------------------------
# Recent orders
# -----------------------------
st.subheader("Recent Orders")

recent_query = """
SELECT
    o.order_id,
    c.name AS customer,
    p.product_name,
    p.category,
    o.quantity,
    o.amount,
    o.status,
    o.order_time
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
JOIN products p
    ON o.product_id = p.product_id
ORDER BY o.order_id DESC
LIMIT 15;
"""

recent_orders = pd.read_sql(recent_query, conn)

st.dataframe(
    recent_orders,
    use_container_width=True,
    hide_index=True
)