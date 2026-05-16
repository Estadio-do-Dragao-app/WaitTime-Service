import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from services.data_retention import DataRetentionService

class TestDataRetentionService:
    @pytest.mark.asyncio
    async def test_cleanup_old_data(self):
        service = DataRetentionService(retention_hours=24)
        
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_db.execute.return_value = mock_result
        
        with patch('services.data_retention.get_db') as mock_get_db:
            # Mock the async context manager
            mock_get_db.return_value.__aenter__.return_value = mock_db
            
            await service.cleanup_old_data()
            
            assert mock_db.execute.called

    @pytest.mark.asyncio
    async def test_service_start_stop(self):
        # Use a very short interval to avoid long sleep
        service = DataRetentionService(retention_hours=24, check_interval_hours=0.0001)
        
        with patch.object(service, 'cleanup_old_data', new_callable=AsyncMock) as mock_cleanup:
            # Start in a task
            task = asyncio.create_task(service.start())
            
            # Wait a bit for it to run at least once
            await asyncio.sleep(0.01)
            
            # Stop
            service.stop()
            
            # Wait for task to finish or cancel it
            try:
                await asyncio.wait_for(task, timeout=0.1)
            except asyncio.TimeoutError:
                task.cancel()
            
            assert mock_cleanup.called
