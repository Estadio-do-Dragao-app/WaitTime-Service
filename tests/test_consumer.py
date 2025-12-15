"""
Tests for consumer.py - MQTT Event Consumer
Tests the MQTTEventConsumer class and its methods
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timezone


class TestMQTTEventConsumerInit:
    """Tests for MQTTEventConsumer initialization"""
    
    def test_consumer_initialization(self):
        """Test that consumer initializes with correct defaults"""
        with patch('consumer.mqtt.Client') as mock_mqtt, \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer(window_minutes=10)
            
            assert consumer.window_minutes == 10
            assert consumer.running is False
            assert consumer.stats['messages_received'] == 0
            assert consumer.stats['messages_published'] == 0
            assert consumer.stats['errors'] == 0
    
    def test_consumer_default_window_minutes(self):
        """Test default window_minutes is 5"""
        with patch('consumer.mqtt.Client') as mock_mqtt, \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            
            assert consumer.window_minutes == 5


class TestConvertFacilityId:
    """Tests for _convert_facility_id method"""
    
    def test_convert_bar_norte(self):
        """Test converting bar facility ID"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._convert_facility_id('BAR', 'bar_norte_1')
            
            assert result == 'Food-Norte-1'
    
    def test_convert_toilet_sul(self):
        """Test converting toilet/WC facility ID"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._convert_facility_id('TOILET', 'toilet_sul_2')
            
            assert result == 'WC-Sul-L0-2'
    
    def test_convert_wc_este(self):
        """Test converting wc facility ID"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._convert_facility_id('WC', 'wc_este')
            
            assert result == 'WC-Este-L0-1'
    
    def test_convert_empty_facility_id(self):
        """Test that empty facility_id returns None"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._convert_facility_id('BAR', '')
            
            assert result is None
    
    def test_convert_short_facility_id(self):
        """Test facility_id with less than 2 parts returns as-is"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._convert_facility_id('BAR', 'simple')
            
            assert result == 'simple'
    
    def test_convert_english_directions(self):
        """Test converting English direction names"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            
            # Test north -> Norte
            result = consumer._convert_facility_id('BAR', 'bar_north_1')
            assert result == 'Food-Norte-1'
            
            # Test south -> Sul
            result = consumer._convert_facility_id('BAR', 'bar_south_1')
            assert result == 'Food-Sul-1'
            
            # Test east -> Este
            result = consumer._convert_facility_id('BAR', 'bar_east_1')
            assert result == 'Food-Este-1'
            
            # Test west -> Oeste
            result = consumer._convert_facility_id('BAR', 'bar_west_1')
            assert result == 'Food-Oeste-1'


class TestIsSignificantChange:
    """Tests for _is_significant_change method"""
    
    def test_first_update_is_significant(self):
        """Test that first update (old_wait=None) is significant"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._is_significant_change(None, 5.0)
            
            assert result is True
    
    def test_zero_old_wait_with_new_above_threshold(self):
        """Test when old_wait is 0 and new_wait > 0.5"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._is_significant_change(0, 1.0)
            
            assert result is True
    
    def test_zero_old_wait_with_new_below_threshold(self):
        """Test when old_wait is 0 and new_wait <= 0.5"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            result = consumer._is_significant_change(0, 0.3)
            
            assert result is False
    
    def test_significant_percentage_change(self):
        """Test when change is >= 15%"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            # 20% change
            result = consumer._is_significant_change(10.0, 12.0)
            
            assert result is True
    
    def test_insignificant_percentage_change(self):
        """Test when change is < 15%"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            # 10% change
            result = consumer._is_significant_change(10.0, 11.0)
            
            assert result is False


class TestGetStats:
    """Tests for get_stats method"""
    
    def test_get_stats_returns_stats_dict(self):
        """Test that get_stats returns the stats dictionary"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            consumer.stats['messages_received'] = 100
            consumer.stats['messages_published'] = 50
            consumer.stats['errors'] = 5
            
            stats = consumer.get_stats()
            
            assert stats['messages_received'] == 100
            assert stats['messages_published'] == 50
            assert stats['errors'] == 5


class TestMQTTCallbacks:
    """Tests for MQTT callback methods"""
    
    def test_on_downstream_connect_success(self):
        """Test _on_downstream_connect with successful connection"""
        with patch('consumer.mqtt.Client') as mock_mqtt, \
             patch('consumer.asyncio.get_event_loop'), \
             patch('consumer.settings') as mock_settings:
            mock_settings.DOWNSTREAM_TOPIC_QUEUES = 'test/queues'
            mock_settings.DOWNSTREAM_TOPIC_ALL = 'test/all'
            
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_client = MagicMock()
            
            # Call callback with rc=0 (success)
            consumer._on_downstream_connect(mock_client, None, None, 0)
            
            # Verify subscriptions were made
            assert mock_client.subscribe.call_count == 2
    
    def test_on_downstream_connect_failure(self):
        """Test _on_downstream_connect with failed connection"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_client = MagicMock()
            
            # Call callback with rc=1 (failure)
            consumer._on_downstream_connect(mock_client, None, None, 1)
            
            # Verify no subscriptions were made
            mock_client.subscribe.assert_not_called()
    
    def test_on_upstream_connect_success(self):
        """Test _on_upstream_connect with successful connection"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_client = MagicMock()
            
            # Should not raise
            consumer._on_upstream_connect(mock_client, None, None, 0)
    
    def test_on_upstream_connect_failure(self):
        """Test _on_upstream_connect with failed connection"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_client = MagicMock()
            
            # Should not raise
            consumer._on_upstream_connect(mock_client, None, None, 1)


class TestOnDownstreamMessage:
    """Tests for _on_downstream_message callback"""
    
    def test_valid_queue_update_message(self):
        """Test processing valid queue_update message"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop') as mock_loop, \
             patch('consumer.asyncio.run_coroutine_threadsafe') as mock_run:
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            consumer.running = True
            
            mock_msg = MagicMock()
            mock_msg.payload.decode.return_value = '{"event_type": "queue_update", "location_id": "bar_norte_1"}'
            mock_msg.topic = 'test/topic'
            
            consumer._on_downstream_message(None, None, mock_msg)
            
            assert consumer.stats['messages_received'] == 1
            mock_run.assert_called_once()
    
    def test_valid_gate_passage_message(self):
        """Test processing valid gate_passage message"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop') as mock_loop, \
             patch('consumer.asyncio.run_coroutine_threadsafe') as mock_run:
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            consumer.running = True
            
            mock_msg = MagicMock()
            mock_msg.payload.decode.return_value = '{"event_type": "gate_passage", "gate": "A1"}'
            mock_msg.topic = 'test/topic'
            
            consumer._on_downstream_message(None, None, mock_msg)
            
            assert consumer.stats['messages_received'] == 1
            mock_run.assert_called_once()
    
    def test_invalid_json_message(self):
        """Test handling invalid JSON message"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            
            mock_msg = MagicMock()
            mock_msg.payload.decode.return_value = 'not valid json'
            mock_msg.topic = 'test/topic'
            
            # Should not raise
            consumer._on_downstream_message(None, None, mock_msg)
            
            assert consumer.stats['messages_received'] == 1
    
    def test_unknown_event_type(self):
        """Test handling unknown event type"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'), \
             patch('consumer.asyncio.run_coroutine_threadsafe') as mock_run:
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            
            mock_msg = MagicMock()
            mock_msg.payload.decode.return_value = '{"event_type": "unknown_type"}'
            mock_msg.topic = 'test/topic'
            
            consumer._on_downstream_message(None, None, mock_msg)
            
            # Should not schedule any async task for unknown types
            mock_run.assert_not_called()


class TestPublishWaittimeUpdate:
    """Tests for _publish_waittime_update method"""
    
    def test_publish_success(self):
        """Test successful publish"""
        with patch('consumer.mqtt.Client') as mock_mqtt_class, \
             patch('consumer.asyncio.get_event_loop'), \
             patch('consumer.settings') as mock_settings:
            mock_settings.UPSTREAM_TOPIC_PREFIX = 'waittime'
            
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_upstream = MagicMock()
            consumer.upstream_client = mock_upstream
            
            consumer._publish_waittime_update(
                poi_id='WC-Norte-L0-1',
                wait_minutes=5.5,
                confidence_lower=4.0,
                confidence_upper=7.0,
                status='medium',
                queue_length=10
            )
            
            mock_upstream.publish.assert_called_once()
            assert consumer.stats['messages_published'] == 1
    
    def test_publish_failure(self):
        """Test publish failure increments error count"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'), \
             patch('consumer.settings') as mock_settings:
            mock_settings.UPSTREAM_TOPIC_PREFIX = 'waittime'
            
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            mock_upstream = MagicMock()
            mock_upstream.publish.side_effect = Exception("Connection failed")
            consumer.upstream_client = mock_upstream
            
            consumer._publish_waittime_update(
                poi_id='WC-Norte-L0-1',
                wait_minutes=5.5,
                confidence_lower=4.0,
                confidence_upper=7.0,
                status='medium'
            )
            
            assert consumer.stats['errors'] == 1


class TestProcessGateEvent:
    """Tests for _process_gate_event method"""
    
    @pytest.mark.asyncio
    async def test_process_gate_event(self):
        """Test processing gate event"""
        with patch('consumer.mqtt.Client'), \
             patch('consumer.asyncio.get_event_loop'):
            from consumer import MQTTEventConsumer
            
            consumer = MQTTEventConsumer()
            
            event_data = {
                'gate': 'A1',
                'direction': 'entry'
            }
            
            # Should not raise
            await consumer._process_gate_event(event_data)


class TestEventConsumerAlias:
    """Test the EventConsumer alias"""
    
    def test_event_consumer_alias(self):
        """Test that EventConsumer is an alias for MQTTEventConsumer"""
        from consumer import EventConsumer, MQTTEventConsumer
        
        assert EventConsumer is MQTTEventConsumer
