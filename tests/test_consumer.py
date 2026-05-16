import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone

from consumer import RobustMQTTConsumer


class TestConvertFacilityId:
    """Tests for the ID conversion logic"""

    def setup_method(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            self.consumer = RobustMQTTConsumer()

    def test_poi_prefix_passthrough(self):
        """POI- prefixed IDs should be returned unchanged"""
        assert self.consumer._convert_facility_id("BAR", "POI-123") == "POI-123"

    def test_empty_facility_id_returns_none(self):
        assert self.consumer._convert_facility_id("BAR", "") is None

    def test_bar_norte_converts_to_food(self):
        result = self.consumer._convert_facility_id("BAR", "bar_norte_1")
        assert result == "Food-Norte-1"

    def test_toilet_sul_converts_to_wc(self):
        result = self.consumer._convert_facility_id("WC", "toilet_sul_2")
        assert result == "WC-Sul-L0-2"

    def test_wc_este_converts(self):
        result = self.consumer._convert_facility_id("WC", "wc_este_1")
        assert result == "WC-Este-L0-1"

    def test_restroom_north_converts_english(self):
        result = self.consumer._convert_facility_id("WC", "restroom_north_3")
        assert result == "WC-Norte-L0-3"

    def test_short_id_falls_through(self):
        """IDs with only one part should be returned as-is"""
        result = self.consumer._convert_facility_id("BAR", "bar")
        assert result == "bar"

    def test_no_number_defaults_to_1(self):
        result = self.consumer._convert_facility_id("BAR", "bar_norte")
        assert result == "Food-Norte-1"


class TestIsSignificantChange:
    """Tests for the change detection logic"""

    def setup_method(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            self.consumer = RobustMQTTConsumer()

    def test_first_measurement_always_significant(self):
        assert self.consumer._is_significant_change(None, 5.0) is True

    def test_no_change_not_significant(self):
        assert self.consumer._is_significant_change(5.0, 5.0) is False

    def test_old_zero_new_below_threshold_not_significant(self):
        assert self.consumer._is_significant_change(0.0, 0.3) is False

    def test_old_zero_new_above_threshold_significant(self):
        assert self.consumer._is_significant_change(0.0, 1.0) is True

    def test_large_percentage_change_significant(self):
        # 50% change - above 15% threshold
        assert self.consumer._is_significant_change(10.0, 15.0) is True

    def test_small_percentage_change_not_significant(self):
        # 5% change - below 15% threshold
        assert self.consumer._is_significant_change(10.0, 10.5) is False


class TestOnDownstreamConnect:
    """Tests for the MQTT connection callback"""

    def setup_method(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            self.consumer = RobustMQTTConsumer()

    def test_successful_connect_subscribes_to_topics(self):
        mock_client = MagicMock()
        self.consumer._on_downstream_connect(mock_client, None, None, 0)
        assert mock_client.subscribe.call_count == 2

    def test_failed_connect_does_not_subscribe(self):
        mock_client = MagicMock()
        self.consumer._on_downstream_connect(mock_client, None, None, 1)
        mock_client.subscribe.assert_not_called()


class TestOnDownstreamMessage:
    """Tests for the message routing logic"""

    def setup_method(self):
        with patch('consumer.mqtt.Client'), patch('consumer.asyncio.get_event_loop'):
            self.consumer = RobustMQTTConsumer()
            self.consumer.loop = MagicMock()

    def test_malformed_json_does_not_crash(self):
        msg = MagicMock()
        msg.payload = b"not-valid-json"
        msg.topic = "stadium/events/queues"
        # Should log and return without raising
        self.consumer._on_downstream_message(None, None, msg)

    def test_valid_queue_event_schedules_processing(self):
        import json
        from datetime import datetime, timezone

        payload = {
            "event_id": "e1",
            "event_type": "queue_update",
            "location_type": "BAR",
            "location_id": "bar_norte_1",
            "queue_length": 5,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {"cam": "123"}
        }
        msg = MagicMock()
        msg.payload = json.dumps(payload).encode()
        msg.topic = "stadium/events/queues"

        with patch('consumer.asyncio.run_coroutine_threadsafe') as mock_run:
            self.consumer._on_downstream_message(None, None, msg)
            mock_run.assert_called_once()

    def test_unknown_topic_is_ignored_gracefully(self):
        import json
        msg = MagicMock()
        msg.payload = json.dumps({"key": "val"}).encode()
        msg.topic = "stadium/events/unknown"
        # Should not raise
        self.consumer._on_downstream_message(None, None, msg)
