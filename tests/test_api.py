import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime, timezone
import json

# Patch environment before importing app to avoid pydantic_settings
import os
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PASSWORD"] = "pass"
os.environ["DOWNSTREAM_BROKER_HOST"] = "localhost"
os.environ["UPSTREAM_BROKER_HOST"] = "localhost"

from app import app, API_KEY
from db.repositories import WaitTimeRepository
from schemas import WaitTimeResponse

client = TestClient(app)


@pytest.fixture
def api_key_headers():
    """Fixture providing valid API key headers for authenticated endpoints"""
    return {"X-API-Key": API_KEY}


@pytest.fixture(autouse=True)
def mock_all_dependencies():
    """Mock all external dependencies (DB, MQTT, MapService)"""
    with patch("app.init_db", new_callable=AsyncMock), \
         patch("app.close_db", new_callable=AsyncMock), \
         patch("services.map_service.MapServiceClient"), \
         patch("consumer.RobustMQTTConsumer"), \
         patch("db.database.get_db") as mock_get_db, \
         patch("db.repositories.WaitTimeRepository.get_current_wait_time") as mock_get_wait, \
         patch("db.repositories.WaitTimeRepository.get_all_wait_times") as mock_get_all, \
         patch("db.repositories.POIRepository.get_all_pois") as mock_get_pois, \
         patch("db.repositories.POIRepository.get_poi_by_id") as mock_get_poi_by_id:

        # Default mock returns
        mock_get_wait.return_value = WaitTimeResponse(
            poi_id="test-poi",
            wait_minutes=5.0,
            confidence_lower=4.0,
            confidence_upper=6.0,
            status="medium",
            timestamp=datetime.now(timezone.utc)
        )
        mock_get_all.return_value = []
        mock_get_pois.return_value = []
        mock_get_poi_by_id.return_value = None

        yield


class TestHealth:
    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "waittime"


class TestWaitTimeEndpoints:
    def test_get_wait_time_success(self, api_key_headers):
        response = client.get("/api/waittime?poi=WC-Norte-L0-1", headers=api_key_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["poi_id"] == "test-poi"
        assert "wait_minutes" in data

    def test_get_wait_time_missing_poi(self, api_key_headers):
        response = client.get("/api/waittime", headers=api_key_headers)  # missing query param
        assert response.status_code == 422  # validation error

    def test_get_wait_time_unauthorized(self):
        response = client.get("/api/waittime?poi=WC-Norte-L0-1")  # no API key
        assert response.status_code == 401

    def test_get_wait_time_invalid_api_key(self):
        response = client.get("/api/waittime?poi=WC-Norte-L0-1", headers={"X-API-Key": "invalid"})
        assert response.status_code == 401

    def test_get_all_wait_times(self, api_key_headers):
        response = client.get("/api/waittime/all", headers=api_key_headers)
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestPOIEndpoints:
    def test_get_pois(self, api_key_headers):
        response = client.get("/api/pois", headers=api_key_headers)
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_get_pois_unauthorized(self):
        response = client.get("/api/pois")  # no API key
        assert response.status_code == 401

    def test_get_poi_by_id_not_found(self, api_key_headers):
        response = client.get("/api/poi/xyz", headers=api_key_headers)
        assert response.status_code == 404

    def test_get_poi_by_id_found(self, api_key_headers, monkeypatch):
        from schemas import POIInfo
        async def mock_get(*args, **kwargs):
            return POIInfo(id="xyz", name="X", poi_type="food", num_servers=4, service_rate=0.5)
        monkeypatch.setattr("db.repositories.POIRepository.get_poi_by_id", mock_get)
        response = client.get("/api/poi/xyz", headers=api_key_headers)
        assert response.status_code == 200
        assert response.json()["id"] == "xyz"


class TestDebugEndpoints:
    def test_debug_consumer_status(self, api_key_headers):
        response = client.get("/debug/consumer-status", headers=api_key_headers)
        assert response.status_code == 200
        assert "status" in response.json()

    def test_debug_consumer_status_unauthorized(self):
        response = client.get("/debug/consumer-status")
        assert response.status_code == 401


class TestPrivacyConsent:
    def test_log_user_consent_success(self):
        """Test successful consent logging (public)"""
        consent_data = {
            "user_id": "user_123",
            "action": "granted"
        }
        response = client.post(
            "/api/v1/privacy/consent",
            json=consent_data
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "logged"
        assert "timestamp" in data

    def test_log_user_consent_denied(self):
        """Test consent logging for denied action (public)"""
        consent_data = {
            "user_id": "user_456",
            "action": "denied"
        }
        response = client.post(
            "/api/v1/privacy/consent",
            json=consent_data
        )
        assert response.status_code == 200
        assert response.json()["status"] == "logged"

    def test_log_user_consent_missing_fields(self):
        """Test consent logging handles missing fields gracefully (public)"""
        consent_data = {}  # empty data
        response = client.post(
            "/api/v1/privacy/consent",
            json=consent_data
        )
        # Should succeed with defaults
        assert response.status_code == 200
        assert response.json()["status"] == "logged"