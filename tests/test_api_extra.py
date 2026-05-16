import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
from app import app, event_consumer

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_debug_consumer_status_not_init():
    with patch('app.event_consumer', None):
        response = client.get("/debug/consumer-status")
        assert response.status_code == 200
        assert response.json()["status"] == "not_initialized"

def test_debug_consumer_status_running():
    mock_consumer = MagicMock()
    mock_consumer.running = True
    mock_consumer.smoothers = {"POI-1": None}
    mock_consumer.queue_models = {"POI-1": None}
    
    with patch('app.event_consumer', mock_consumer):
        response = client.get("/debug/consumer-status")
        assert response.status_code == 200
        assert response.json()["status"] == "running"
        assert response.json()["active_pois"] == 1

@pytest.mark.asyncio
async def test_get_wait_time_404():
    # We need to mock the dependency get_db_session or the repository
    with patch('app.WaitTimeRepository') as mock_repo_class:
        mock_repo = AsyncMock()
        mock_repo.get_current_wait_time.return_value = None
        mock_repo_class.return_value = mock_repo
        
        response = client.get("/api/waittime?poi=Unknown", headers={"X-API-Key": "dragao_secret_key_2026"})
        assert response.status_code == 404

def test_unauthorized_access():
    response = client.get("/api/waittime?poi=POI-1")
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_log_user_consent():
    response = client.post(
        "/api/v1/privacy/consent", 
        json={"user_id": "user1", "action": "granted"},
        headers={"X-API-Key": "dragao_secret_key_2026"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "logged"

@pytest.mark.asyncio
async def test_seed_pois_from_map_service():
    from app import _seed_pois_from_map_service
    
    mock_pois = [{"id": "POI-1", "name": "Test", "type": "food", "num_servers": 1, "service_rate": 0.5}]
    
    with patch('app.MapServiceClient') as mock_client_class, \
         patch('app.get_db') as mock_get_db, \
         patch('app.POIRepository') as mock_repo_class:
        
        mock_client = AsyncMock()
        mock_client.fetch_pois.return_value = mock_pois
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_get_db.return_value.__aenter__.return_value = mock_db
        
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        
        await _seed_pois_from_map_service()
        
        assert mock_client.fetch_pois.called
        assert mock_repo.insert_poi.called
