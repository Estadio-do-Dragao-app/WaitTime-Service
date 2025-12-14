"""
Unit tests for services/map_service.py
Tests MapServiceClient for fetching POI configurations
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
from services.map_service import MapServiceClient


class TestMapServiceClient:
    """Tests for MapServiceClient"""
    
    @pytest.mark.asyncio
    async def test_fetch_pois_success(self, mock_map_service_response):
        """Test successfully fetching POIs from MapService"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        # Mock httpx response
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_map_service_response
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response
            
            pois = await client.fetch_pois()
            
            assert len(pois) == 2
            assert pois[0]['id'] == "WC-Norte-L0-1"
            assert pois[1]['id'] == "Food-Sul-1"
            mock_get.assert_called_once_with("http://test-map-service:8000/pois")
    
    @pytest.mark.asyncio
    async def test_fetch_pois_with_missing_fields(self):
        """Test fetching POIs with missing num_servers/service_rate adds defaults"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        # POI missing num_servers and service_rate
        incomplete_pois = [
            {
                "id": "Incomplete-POI",
                "name": "Test POI",
                "type": "restroom"
                # Missing num_servers and service_rate
            }
        ]
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = incomplete_pois
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response
            
            pois = await client.fetch_pois()
            
            # Should have defaults added
            assert pois[0]['num_servers'] == 1
            assert pois[0]['service_rate'] == pytest.approx(0.5)
    
    @pytest.mark.asyncio
    async def test_fetch_pois_http_error(self):
        """Test handling HTTP errors when fetching POIs"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.side_effect = httpx.HTTPError("Connection failed")
            
            with pytest.raises(RuntimeError, match="MapService unavailable"):
                await client.fetch_pois()
    
    @pytest.mark.asyncio
    async def test_fetch_pois_timeout(self):
        """Test handling timeout when fetching POIs"""
        client = MapServiceClient(base_url="http://test-map-service:8000", timeout=1)
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.side_effect = httpx.TimeoutException("Timeout")
            
            with pytest.raises(RuntimeError, match="MapService unavailable"):
                await client.fetch_pois()
    
    @pytest.mark.asyncio
    async def test_fetch_poi_by_id_success(self):
        """Test successfully fetching a single POI by ID"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        poi_data = {
            "id": "WC-Norte-L0-1",
            "name": "Restrooms North Level 0 #1",
            "type": "restroom",
            "num_servers": 8,
            "service_rate": 0.5
        }
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = poi_data
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response
            
            poi = await client.fetch_poi_by_id("WC-Norte-L0-1")
            
            assert poi['id'] == "WC-Norte-L0-1"
            assert poi['num_servers'] == 8
            mock_get.assert_called_once_with("http://test-map-service:8000/pois/WC-Norte-L0-1")
    
    @pytest.mark.asyncio
    async def test_fetch_poi_by_id_not_found(self):
        """Test fetching non-existent POI raises error"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "404 Not Found",
                request=MagicMock(),
                response=MagicMock(status_code=404)
            )
            mock_get.return_value = mock_response
            
            with pytest.raises(RuntimeError, match="POI .* not found"):
                await client.fetch_poi_by_id("NonExistent-POI")
    
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test health check when service is available"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response
            
            is_healthy = await client.health_check()
            
            assert is_healthy is True
            mock_get.assert_called_once_with("http://test-map-service:8000/health")
    
    @pytest.mark.asyncio
    async def test_health_check_service_down(self):
        """Test health check when service is unavailable"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 503
            mock_get.return_value = mock_response
            
            is_healthy = await client.health_check()
            
            assert is_healthy is False
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self):
        """Test health check when connection fails"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            
            is_healthy = await client.health_check()
            
            assert is_healthy is False
    
    def test_client_initialization_default_url(self):
        """Test client uses default URL from settings"""
        with patch('services.map_service.settings') as mock_settings:
            mock_settings.MAP_SERVICE_URL = "http://default-map:8000"
            
            client = MapServiceClient()
            
            assert client.base_url == "http://default-map:8000"
    
    def test_client_initialization_custom_url(self):
        """Test client can use custom URL"""
        client = MapServiceClient(base_url="http://custom-map:9000")
        
        assert client.base_url == "http://custom-map:9000"
    
    def test_client_initialization_custom_timeout(self):
        """Test client can use custom timeout"""
        client = MapServiceClient(timeout=30)
        
        assert client.timeout == 30
    
    @pytest.mark.asyncio
    async def test_fetch_pois_validates_response_structure(self, mock_map_service_response):
        """Test that fetch_pois validates response has required queue fields"""
        client = MapServiceClient(base_url="http://test-map-service:8000")
        
        # Valid POI with all fields
        valid_poi = mock_map_service_response[0]
        
        # Invalid POI missing service_rate
        invalid_poi = {
            "id": "Invalid-POI",
            "name": "Invalid POI",
            "type": "restroom",
            "num_servers": 5
            # Missing service_rate
        }
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = [valid_poi, invalid_poi]
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response
            
            pois = await client.fetch_pois()
            
            # Both POIs should be returned
            assert len(pois) == 2
            
            # Invalid POI should have default service_rate
            invalid_poi_result = next(p for p in pois if p['id'] == 'Invalid-POI')
            assert invalid_poi_result['service_rate'] == pytest.approx(0.5)
