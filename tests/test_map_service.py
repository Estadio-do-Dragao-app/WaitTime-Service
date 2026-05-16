import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import httpx
from services.map_service import MapServiceClient

class TestMapServiceClient:
    @pytest.mark.asyncio
    async def test_fetch_pois_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "POI-1", "name": "Test POI", "type": "food", "num_servers": 1, "service_rate": 0.5}
        ]
        
        with patch('httpx.AsyncClient.get', return_value=mock_response):
            client = MapServiceClient()
            pois = await client.fetch_pois()
            assert len(pois) == 1
            assert pois[0]["id"] == "POI-1"

    @pytest.mark.asyncio
    async def test_fetch_pois_failure(self):
        with patch('httpx.AsyncClient.get', side_effect=httpx.HTTPError("Connection error")):
            client = MapServiceClient()
            with pytest.raises(RuntimeError):
                await client.fetch_pois()

    @pytest.mark.asyncio
    async def test_fetch_poi_by_id_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "POI-1", "name": "Detailed POI"}
        
        with patch('httpx.AsyncClient.get', return_value=mock_response):
            client = MapServiceClient()
            poi = await client.fetch_poi_by_id("POI-1")
            assert poi["id"] == "POI-1"

    @pytest.mark.asyncio
    async def test_health_check(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        with patch('httpx.AsyncClient.get', return_value=mock_response):
            client = MapServiceClient()
            assert await client.health_check() is True
        
        with patch('httpx.AsyncClient.get', side_effect=Exception()):
            client = MapServiceClient()
            assert await client.health_check() is False
