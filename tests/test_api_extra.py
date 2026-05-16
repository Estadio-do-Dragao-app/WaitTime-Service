import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
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

@pytest.mark.asyncio
async def test_seed_pois_from_map_service_failure():
    from app import _seed_pois_from_map_service
    
    with patch('app.MapServiceClient') as mock_client_class:
        mock_client = AsyncMock()
        mock_client.fetch_pois.side_effect = Exception("Service Down")
        mock_client_class.return_value = mock_client
        
        # Should not raise exception, just log error/warning
        await _seed_pois_from_map_service()
        assert mock_client.fetch_pois.called

@pytest.mark.asyncio
async def test_lifespan():
    from app import lifespan
    mock_app = MagicMock()
    
    with patch('app.init_db', new_callable=AsyncMock) as mock_init, \
         patch('app.close_db', new_callable=AsyncMock) as mock_close, \
         patch('app._seed_pois_from_map_service', new_callable=AsyncMock) as mock_seed, \
         patch('app.EventConsumer') as mock_consumer_class, \
         patch('app.DataRetentionService') as mock_retention_class, \
         patch('asyncio.create_task') as mock_task:
        
        mock_consumer = MagicMock()
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        mock_retention = MagicMock()
        mock_retention.start = AsyncMock()
        mock_retention.stop = MagicMock()
        mock_retention_class.return_value = mock_retention
        
        # mock_task must return an awaitable
        future = asyncio.Future()
        future.set_result(None)
        mock_task.return_value = future
        
        async with lifespan(mock_app):
            assert mock_init.called
            assert mock_seed.called
            assert mock_consumer_class.called
            assert mock_retention_class.called
        
        assert mock_consumer.stop.called
        assert mock_retention.stop.called
        assert mock_close.called

@pytest.mark.asyncio
async def test_get_queue_state_debug_404():
    with patch('app.WaitTimeRepository') as mock_repo_class:
        mock_repo = AsyncMock()
        mock_repo.get_queue_state_raw.return_value = None
        mock_repo_class.return_value = mock_repo
        
        response = client.get("/debug/queue-state/Unknown")
        assert response.status_code == 404

@pytest.mark.asyncio
async def test_get_queue_state_debug_success():
    with patch('app.WaitTimeRepository') as mock_repo_class:
        mock_repo = AsyncMock()
        mock_repo.get_queue_state_raw.return_value = {"poi_id": "POI-1", "wait": 5}
        mock_repo_class.return_value = mock_repo
        
        response = client.get("/debug/queue-state/POI-1")
        assert response.status_code == 200
        assert response.json()["poi_id"] == "POI-1"
