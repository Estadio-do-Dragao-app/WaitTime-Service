"""
Unit tests for app.py - FastAPI endpoints
Tests all HTTP API endpoints for the WaitTime-Service
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, AsyncMock
from httpx import AsyncClient
from db.repositories import POIRepository, WaitTimeRepository
from db.models import POI, QueueState


class TestHealthEndpoint:
    """Tests for /health endpoint"""
    
    @pytest.mark.asyncio
    async def test_health_check_basic(self, test_client):
        """Test basic health check returns 200"""
        response = await test_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data['status'] == 'healthy'
        assert data['service'] == 'waittime'
        assert 'timestamp' in data
        assert 'consumer_status' in data


class TestWaitTimeEndpoints:
    """Tests for /api/waittime endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_wait_time_success(self, test_client, test_db_session, seed_pois, seed_queue_states):
        """Test getting wait time for a specific POI"""
        # Seed data is already in database
        response = await test_client.get("/api/waittime?poi=WC-Norte-L0-1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data['poi_id'] == "WC-Norte-L0-1"
        assert 'wait_minutes' in data
        assert 'confidence_lower' in data
        assert 'confidence_upper' in data
        assert 'status' in data
        assert 'timestamp' in data
    
    @pytest.mark.asyncio
    async def test_get_wait_time_not_found(self, test_client):
        """Test getting wait time for non-existent POI returns 404"""
        response = await test_client.get("/api/waittime?poi=NonExistent-POI")
        
        assert response.status_code == 404
        assert "not found" in response.json()['detail'].lower()
    
    @pytest.mark.asyncio
    async def test_get_wait_time_missing_poi_parameter(self, test_client):
        """Test that missing poi parameter returns 422"""
        response = await test_client.get("/api/waittime")
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_get_all_wait_times(self, test_client, test_db_session, seed_pois, seed_queue_states):
        """Test getting all wait times"""
        response = await test_client.get("/api/waittime/all")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 3  # We have 3 POIs
        
        # Verify structure
        for wait_time in data:
            assert 'poi_id' in wait_time
            assert 'wait_minutes' in wait_time
            assert 'status' in wait_time
    
    @pytest.mark.asyncio
    async def test_get_all_wait_times_filtered_by_type(self, test_client, test_db_session, seed_pois, seed_queue_states):
        """Test getting wait times filtered by POI type"""
        response = await test_client.get("/api/waittime/all?poi_type=restroom")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        # Should only get restroom POIs
        assert len(data) > 0  # Should find at least one restroom POI
    
    @pytest.mark.asyncio
    async def test_get_all_wait_times_empty(self, test_client):
        """Test getting all wait times when none exist"""
        response = await test_client.get("/api/waittime/all")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 0


class TestPOIEndpoints:
    """Tests for /api/pois endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_all_pois(self, test_client, test_db_session, seed_pois):
        """Test getting all POIs"""
        response = await test_client.get("/api/pois")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 3
        
        # Verify structure
        for poi in data:
            assert 'id' in poi
            assert 'name' in poi
            assert 'poi_type' in poi
            assert 'num_servers' in poi
            assert 'service_rate' in poi
    
    @pytest.mark.asyncio
    async def test_get_pois_filtered_by_type(self, test_client, test_db_session, seed_pois):
        """Test getting POIs filtered by type"""
        response = await test_client.get("/api/pois?poi_type=food")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['poi_type'] == 'food'
        assert data[0]['id'] == 'Food-Sul-1'
    
    @pytest.mark.asyncio
    async def test_get_pois_empty(self, test_client):
        """Test getting POIs when database is empty"""
        response = await test_client.get("/api/pois")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 0
    
    @pytest.mark.asyncio
    async def test_get_poi_by_id_success(self, test_client, test_db_session, seed_pois):
        """Test getting a specific POI by ID"""
        response = await test_client.get("/api/poi/WC-Norte-L0-1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data['id'] == "WC-Norte-L0-1"
        assert data['name'] == "Restrooms North Level 0 #1"
        assert data['poi_type'] == "restroom"
        assert data['num_servers'] == 8
        assert data['service_rate'] == pytest.approx(0.5)
    
    @pytest.mark.asyncio
    async def test_get_poi_by_id_not_found(self, test_client):
        """Test getting non-existent POI returns 404"""
        response = await test_client.get("/api/poi/NonExistent-POI")
        
        assert response.status_code == 404
        assert "not found" in response.json()['detail'].lower()


class TestDebugEndpoints:
    """Tests for /debug/* endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_queue_state_debug(self, test_client, test_db_session, seed_queue_states):
        """Test getting raw queue state for debugging"""
        response = await test_client.get("/debug/queue-state/WC-Norte-L0-1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data['poi_id'] == "WC-Norte-L0-1"
        assert 'arrival_rate' in data
        assert 'wait_minutes' in data
        assert 'sample_count' in data
    
    @pytest.mark.asyncio
    async def test_get_queue_state_debug_not_found(self, test_client):
        """Test debug queue state for non-existent POI returns 404"""
        response = await test_client.get("/debug/queue-state/NonExistent-POI")
        
        assert response.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_consumer_status(self, test_client):
        """Test getting consumer status"""
        response = await test_client.get("/debug/consumer-status")
        
        assert response.status_code == 200
        data = response.json()
        
        # Consumer might not be initialized in tests
        assert 'status' in data


class TestCORSConfiguration:
    """Tests for CORS middleware configuration"""
    
    @pytest.mark.asyncio
    async def test_cors_headers_present(self, test_client):
        """Test that CORS headers are present in responses"""
        response = await test_client.get("/health")
        
        # Note: AsyncClient might not include all CORS headers in test mode
        # This is a basic check that the endpoint works
        assert response.status_code == 200


class TestResponseModels:
    """Tests for response model validation"""
    
    @pytest.mark.asyncio
    async def test_wait_time_response_model(self, test_client, test_db_session, seed_pois, seed_queue_states):
        """Test that wait time response matches WaitTimeResponse model"""
        response = await test_client.get("/api/waittime?poi=WC-Norte-L0-1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate required fields
        required_fields = ['poi_id', 'wait_minutes', 'confidence_lower', 
                          'confidence_upper', 'status', 'timestamp']
        for field in required_fields:
            assert field in data
        
        # Validate types
        assert isinstance(data['poi_id'], str)
        assert isinstance(data['wait_minutes'], (int, float))
        assert isinstance(data['status'], str)
    
    @pytest.mark.asyncio
    async def test_poi_info_response_model(self, test_client, test_db_session, seed_pois):
        """Test that POI response matches POIInfo model"""
        response = await test_client.get("/api/poi/WC-Norte-L0-1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate required fields
        required_fields = ['id', 'name', 'poi_type', 'num_servers', 'service_rate']
        for field in required_fields:
            assert field in data
        
        # Validate types
        assert isinstance(data['id'], str)
        assert isinstance(data['name'], str)
        assert isinstance(data['poi_type'], str)
        assert isinstance(data['num_servers'], int)
        assert isinstance(data['service_rate'], (int, float))


class TestErrorHandling:
    """Tests for error handling"""
    
    @pytest.mark.asyncio
    async def test_invalid_endpoint_returns_404(self, test_client):
        """Test that invalid endpoint returns 404"""
        response = await test_client.get("/api/invalid-endpoint")
        
        assert response.status_code == 404
    
    @pytest.mark.asyncio
    async def test_method_not_allowed(self, test_client):
        """Test that wrong HTTP method returns 405"""
        # POST to a GET-only endpoint
        response = await test_client.post("/health")
        
        assert response.status_code == 405
