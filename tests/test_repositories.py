import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from db.repositories import POIRepository, WaitTimeRepository
from db.schemas import POI, QueueState
from datetime import datetime, timezone

class TestPOIRepository:
    @pytest.mark.asyncio
    async def test_get_poi_by_id(self):
        mock_session = AsyncMock(spec=AsyncSession)
        repo = POIRepository(mock_session)
        
        mock_poi = POI(id="POI-1", name="Test POI", poi_type="food", num_servers=4, service_rate=0.5)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_poi
        mock_session.execute.return_value = mock_result
        
        result = await repo.get_poi_by_id("POI-1")
        assert result.id == "POI-1"
        assert result.name == "Test POI"
        assert mock_session.execute.called

    @pytest.mark.asyncio
    async def test_insert_poi(self):
        mock_session = AsyncMock(spec=AsyncSession)
        repo = POIRepository(mock_session)
        
        poi_data = {
            "id": "POI-2",
            "name": "New POI",
            "type": "food",
            "num_servers": 2,
            "service_rate": 0.5
        }
        
        await repo.insert_poi(poi_data)
        assert mock_session.merge.called
        assert mock_session.commit.called

class TestWaitTimeRepository:
    @pytest.mark.asyncio
    async def test_get_current_wait_time(self):
        mock_session = AsyncMock(spec=AsyncSession)
        repo = WaitTimeRepository(mock_session)
        
        mock_state = QueueState(
            poi_id="POI-1", 
            arrival_rate=2.0,
            current_wait_minutes=10.0,
            status="medium",
            last_updated=datetime.now(timezone.utc)
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_state
        mock_session.execute.return_value = mock_result
        
        result = await repo.get_current_wait_time("POI-1")
        assert result.poi_id == "POI-1"
        assert result.wait_minutes == 10.0

    @pytest.mark.asyncio
    async def test_update_queue_state(self):
        mock_session = AsyncMock(spec=AsyncSession)
        repo = WaitTimeRepository(mock_session)
        
        await repo.update_queue_state(
            poi_id="POI-1",
            arrival_rate=2.0,
            wait_minutes=5.0,
            confidence_lower=4.0,
            confidence_upper=6.0,
            sample_count=10,
            status="low"
        )
        
        assert mock_session.merge.called
        assert mock_session.commit.called
