from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


def test_home():
    response = client.get("/")
    assert response.status_code == 200


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["database"] == "connected"


def test_orders():
    response = client.get("/orders?limit=5")
    assert response.status_code == 200
    assert len(response.json()) <= 5


def test_invalid_limit():
    response = client.get("/orders?limit=500")
    assert response.status_code == 422


def test_revenue():
    response = client.get("/analytics/revenue")
    assert response.status_code == 200
    assert "total_revenue" in response.json()