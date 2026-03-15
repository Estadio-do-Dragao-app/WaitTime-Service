"""
Unit tests for models.py
Tests Pydantic model validation and serialization
"""
import pytest
from datetime import datetime, timezone
from models import (
    QueueEvent,
    WaitTimeUpdate,
    WaitTimeResponse,
    POIInfo,
    QueueState
)


class TestQueueEvent:
    """Tests for QueueEvent model"""
    
    def test_valid_queue_event(self):
        """Test creating a valid queue event"""
        event = QueueEvent(
            poi_id="WC-Norte-L0-1",
            event_type="entry",
            count=1,
            camera_id="cam-101",
            timestamp=datetime.now(timezone.utc)
        )
        
        assert event.poi_id == "WC-Norte-L0-1"
        assert event.event_type == "entry"
        assert event.count == 1
        assert event.camera_id == "cam-101"
        assert isinstance(event.timestamp, datetime)
    
    def test_default_count(self):
        """Test default count value"""
        event = QueueEvent(
            poi_id="Food-Sul-1",
            event_type="exit",
            camera_id="cam-201",
            timestamp=datetime.now(timezone.utc)
        )
        
        assert event.count == 1
    
    def test_event_serialization(self):
        """Test event can be serialized to dict"""
        event = QueueEvent(
            poi_id="Store-Este-1",
            event_type="entry",
            count=2,
            camera_id="cam-301",
            timestamp=datetime(2025, 12, 14, 20, 0, 0, tzinfo=timezone.utc)
        )
        
        data = event.model_dump()
        assert data['poi_id'] == "Store-Este-1"
        assert data['event_type'] == "entry"
        assert data['count'] == 2


class TestWaitTimeUpdate:
    """Tests for WaitTimeUpdate model"""
    
    def test_valid_wait_time_update(self):
        """Test creating a valid wait time update"""
        update = WaitTimeUpdate(
            poi_id="WC-Norte-L0-1",
            wait_minutes=6.5,
            confidence_lower=5.0,
            confidence_upper=8.0,
            status="medium",
            timestamp=datetime.now(timezone.utc)
        )
        
        assert update.poi_id == "WC-Norte-L0-1"
        assert update.wait_minutes == pytest.approx(6.5)
        assert update.status == "medium"
    
    def test_to_broker_message_format(self):
        """Test conversion to broker message format"""
        timestamp = datetime(2025, 12, 14, 20, 30, 0, tzinfo=timezone.utc)
        update = WaitTimeUpdate(
            poi_id="Food-Sul-1",
            wait_minutes=4.567,
            confidence_lower=3.234,
            confidence_upper=5.987,
            status="low",
            timestamp=timestamp
        )
        
        message = update.to_broker_message()
        
        assert message['type'] == "waittime"
        assert message['poi'] == "Food-Sul-1"
        assert message['minutes'] == pytest.approx(4.6)
        assert message['ci95'] == pytest.approx([3.2, 6.0])
        assert message['status'] == "low"
        assert message['ts'] == timestamp.isoformat()
    
    def test_broker_message_rounding(self):
        """Test that values are properly rounded in broker message"""
        update = WaitTimeUpdate(
            poi_id="Test-POI",
            wait_minutes=3.14159,
            confidence_lower=2.71828,
            confidence_upper=4.66920,
            status="medium",
            timestamp=datetime.now(timezone.utc)
        )
        
        message = update.to_broker_message()
        
        assert message['minutes'] == pytest.approx(3.1)
        assert message['ci95'][0] == pytest.approx(2.7)
        assert message['ci95'][1] == pytest.approx(4.7)


class TestWaitTimeResponse:
    """Tests for WaitTimeResponse model"""
    
    def test_valid_wait_time_response(self):
        """Test creating a valid response"""
        response = WaitTimeResponse(
            poi_id="WC-Norte-L0-1",
            wait_minutes=5.5,
            confidence_lower=4.2,
            confidence_upper=6.8,
            status="medium",
            timestamp=datetime.now(timezone.utc)
        )
        
        assert response.poi_id == "WC-Norte-L0-1"
        assert response.wait_minutes == pytest.approx(5.5)
        assert response.status == "medium"
    
    def test_response_serialization(self):
        """Test response can be serialized for API"""
        timestamp = datetime(2025, 12, 14, 20, 0, 0, tzinfo=timezone.utc)
        response = WaitTimeResponse(
            poi_id="Food-Sul-1",
            wait_minutes=3.0,
            confidence_lower=2.5,
            confidence_upper=3.5,
            status="low",
            timestamp=timestamp
        )
        
        data = response.model_dump()
        
        assert data['poi_id'] == "Food-Sul-1"
        assert data['wait_minutes'] == pytest.approx(3.0)
        assert data['confidence_lower'] == pytest.approx(2.5)
        assert data['confidence_upper'] == pytest.approx(3.5)
        assert data['status'] == "low"


class TestPOIInfo:
    """Tests for POIInfo model"""
    
    def test_valid_poi_info(self):
        """Test creating valid POI info"""
        poi = POIInfo(
            id="WC-Norte-L0-1",
            name="Restrooms North Level 0 #1",
            poi_type="restroom",
            num_servers=8,
            service_rate=0.5
        )
        
        assert poi.id == "WC-Norte-L0-1"
        assert poi.name == "Restrooms North Level 0 #1"
        assert poi.poi_type == "restroom"
        assert poi.num_servers == 8
        assert poi.service_rate == pytest.approx(0.5)
    
    def test_poi_types(self):
        """Test different POI types"""
        types = ["restroom", "food", "store"]
        
        for poi_type in types:
            poi = POIInfo(
                id=f"Test-{poi_type}",
                name=f"Test {poi_type}",
                poi_type=poi_type,
                num_servers=4,
                service_rate=0.4
            )
            assert poi.poi_type == poi_type
    
    def test_poi_serialization(self):
        """Test POI can be serialized"""
        poi = POIInfo(
            id="Store-Este-1",
            name="Store East #1",
            poi_type="store",
            num_servers=2,
            service_rate=0.3
        )
        
        data = poi.model_dump()
        
        assert data['id'] == "Store-Este-1"
        assert data['name'] == "Store East #1"
        assert data['poi_type'] == "store"
        assert data['num_servers'] == 2
        assert data['service_rate'] == pytest.approx(0.3)


class TestQueueState:
    """Tests for QueueState model"""
    
    def test_valid_queue_state(self):
        """Test creating valid queue state"""
        state = QueueState(
            poi_id="WC-Norte-L0-1",
            arrival_rate=2.5,
            current_wait_minutes=5.0,
            confidence_lower=4.0,
            confidence_upper=6.0,
            last_updated=datetime.now(timezone.utc),
            sample_count=15,
            status="medium"
        )
        
        assert state.poi_id == "WC-Norte-L0-1"
        assert state.arrival_rate == pytest.approx(2.5)
        assert state.current_wait_minutes == pytest.approx(5.0)
        assert state.sample_count == 15
        assert state.status == "medium"
    
    def test_optional_confidence_intervals(self):
        """Test that confidence intervals can be None"""
        state = QueueState(
            poi_id="Test-POI",
            arrival_rate=1.0,
            current_wait_minutes=3.0,
            confidence_lower=None,
            confidence_upper=None,
            last_updated=datetime.now(timezone.utc),
            sample_count=5,
            status="low"
        )
        
        assert state.confidence_lower is None
        assert state.confidence_upper is None
    
    def test_status_values(self):
        """Test different status values"""
        statuses = ['low', 'medium', 'high', 'overloaded']
        
        for status in statuses:
            state = QueueState(
                poi_id="Test-POI",
                arrival_rate=1.0,
                current_wait_minutes=2.0,
                confidence_lower=1.5,
                confidence_upper=2.5,
                last_updated=datetime.now(timezone.utc),
                sample_count=10,
                status=status
            )
            assert state.status == status


class TestModelValidation:
    """Tests for model validation"""
    
    def test_queue_event_missing_required_fields(self):
        """Test that missing required fields raise validation error"""
        with pytest.raises(Exception):  # Pydantic ValidationError
            QueueEvent(
                poi_id="Test",
                # Missing event_type
                camera_id="cam-1",
                timestamp=datetime.now(timezone.utc)
            )
    
    def test_poi_info_invalid_num_servers(self):
        """Test validation with invalid num_servers"""
        # Should accept any integer, but test basic creation
        poi = POIInfo(
            id="Test",
            name="Test POI",
            poi_type="restroom",
            num_servers=0,  # Edge case
            service_rate=0.5
        )
        assert poi.num_servers == 0
