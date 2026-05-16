import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import json
from datetime import datetime, timezone

from consumer import RobustMQTTConsumer
from schemas import QueueEvent

class TestProcessQueueEvent:
    @pytest.mark.asyncio
    async def test_process_queue_event_success(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            consumer = RobustMQTTConsumer()
            consumer.running = True
            
            event = QueueEvent(
                event_id="e1",
                event_type="queue_update",
                location_type="BAR",
                location_id="bar_norte_1",
                queue_length=10,
                timestamp=datetime.now(timezone.utc),
                metadata={}
            )
            
            mock_poi = MagicMock()
            mock_poi.num_servers = 4
            mock_poi.service_rate = 0.5
            
            mock_repo_poi = AsyncMock()
            mock_repo_poi.get_poi_by_id.return_return_value = mock_poi
            
            mock_repo_wait = AsyncMock()
            mock_repo_wait.get_queue_state_raw.return_value = {"wait_minutes": 5.0}
            
            with patch('consumer.get_db') as mock_get_db, \
                 patch('consumer.POIRepository', return_value=mock_repo_poi), \
                 patch('consumer.WaitTimeRepository', return_value=mock_repo_wait), \
                 patch('consumer.QueueModel') as mock_model_class:
                
                mock_model = MagicMock()
                mock_model.calculate_wait_time.return_value = MagicMock(
                    wait_minutes=8.0,
                    confidence_lower=6.0,
                    confidence_upper=10.0,
                    status="medium"
                )
                mock_model_class.return_value = mock_model
                
                # Mock the MQTT publishing
                consumer.upstream_client.publish = MagicMock()
                
                await consumer._process_queue_event(event)
                
                # Verify DB calls
                mock_repo_wait.update_queue_state.assert_called_once()
                # Verify MQTT publish
                assert consumer.upstream_client.publish.called

    def test_get_stats(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            consumer = RobustMQTTConsumer()
            stats = consumer.get_stats()
            assert "messages_received" in stats
            assert stats["errors"] == 0

    @pytest.mark.asyncio
    async def test_stop(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            consumer = RobustMQTTConsumer()
            consumer.downstream_client = MagicMock()
            consumer.upstream_client = MagicMock()
            await consumer.stop()
            assert consumer.running is False
            assert consumer.downstream_client.loop_stop.called
            assert consumer.upstream_client.loop_stop.called

    @pytest.mark.asyncio
    async def test_handle_cantina_reconciliation(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            consumer = RobustMQTTConsumer()
            
            mock_repo_poi = AsyncMock()
            mock_repo_poi.get_poi_by_id.return_value = None # Force creation
            mock_repo_poi.session = MagicMock()
            mock_repo_poi.session.merge = AsyncMock()
            mock_repo_poi.session.commit = AsyncMock()
            
            mock_repo_wait = AsyncMock()
            
            result = MagicMock(
                wait_minutes=5.0,
                confidence_lower=4.0,
                confidence_upper=6.0,
                status="low"
            )
            
            consumer.upstream_client.publish = MagicMock()
            
            await consumer._handle_cantina_reconciliation(
                mock_repo_poi, mock_repo_wait,
                4, 0.5, 2.0, result, 10
            )
            
            # Should have processed 2 alternate IDs
            assert mock_repo_poi.session.merge.call_count == 2
            assert mock_repo_wait.update_queue_state.call_count == 2
            assert consumer.upstream_client.publish.call_count == 2
