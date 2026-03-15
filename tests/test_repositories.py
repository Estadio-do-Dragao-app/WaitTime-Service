"""
Unit tests for db/repositories.py
Tests POI, WaitTime, and CameraEvent repositories
"""
import pytest
from datetime import datetime, timezone, timedelta
from db.repositories import POIRepository, WaitTimeRepository, CameraEventRepository
from models import QueueEvent, POIInfo, WaitTimeResponse


class TestPOIRepository:
    """Tests for POI repository operations"""
    
    @pytest.mark.asyncio
    async def test_insert_poi(self, test_db_session, sample_pois):
        """Test inserting a POI"""
        repo = POIRepository(test_db_session)
        poi_data = sample_pois[0]
        
        await repo.insert_poi(poi_data)
        
        # Verify it was inserted
        result = await repo.get_poi_by_id(poi_data['id'])
        assert result is not None
        assert result.id == poi_data['id']
        assert result.name == poi_data['name']
        assert result.poi_type == poi_data['type']
        assert result.num_servers == poi_data['num_servers']
        assert result.service_rate == poi_data['service_rate']
    
    @pytest.mark.asyncio
    async def test_insert_poi_updates_existing(self, test_db_session, sample_pois):
        """Test that insert_poi updates existing POI (merge behavior)"""
        repo = POIRepository(test_db_session)
        poi_data = sample_pois[0].copy()
        
        # Insert first time
        await repo.insert_poi(poi_data)
        
        # Update and insert again
        poi_data['name'] = "Updated Name"
        poi_data['num_servers'] = 10
        await repo.insert_poi(poi_data)
        
        # Verify update
        result = await repo.get_poi_by_id(poi_data['id'])
        assert result.name == "Updated Name"
        assert result.num_servers == 10
    
    @pytest.mark.asyncio
    async def test_get_poi_by_id_not_found(self, test_db_session):
        """Test getting non-existent POI returns None"""
        repo = POIRepository(test_db_session)
        
        result = await repo.get_poi_by_id("NonExistent-POI")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_all_pois(self, test_db_session, sample_pois):
        """Test getting all POIs"""
        repo = POIRepository(test_db_session)
        
        # Insert multiple POIs
        for poi_data in sample_pois:
            await repo.insert_poi(poi_data)
        
        # Get all
        all_pois = await repo.get_all_pois()
        
        assert len(all_pois) == len(sample_pois)
        assert all(isinstance(poi, POIInfo) for poi in all_pois)
    
    @pytest.mark.asyncio
    async def test_get_all_pois_filtered_by_type(self, test_db_session, sample_pois):
        """Test getting POIs filtered by type"""
        repo = POIRepository(test_db_session)
        
        # Insert multiple POIs
        for poi_data in sample_pois:
            await repo.insert_poi(poi_data)
        
        # Filter by restroom
        restrooms = await repo.get_all_pois(poi_type="restroom")
        
        assert len(restrooms) == 1
        assert restrooms[0].poi_type == "restroom"
        
        # Filter by food
        food_pois = await repo.get_all_pois(poi_type="food")
        
        assert len(food_pois) == 1
        assert food_pois[0].poi_type == "food"
    
    @pytest.mark.asyncio
    async def test_get_all_pois_empty(self, test_db_session):
        """Test getting all POIs when database is empty"""
        repo = POIRepository(test_db_session)
        
        all_pois = await repo.get_all_pois()
        
        assert all_pois == []


class TestWaitTimeRepository:
    """Tests for WaitTime repository operations"""
    
    @pytest.mark.asyncio
    async def test_update_queue_state(self, test_db_session):
        """Test updating queue state"""
        repo = WaitTimeRepository(test_db_session)
        
        await repo.update_queue_state(
            poi_id="WC-Norte-L0-1",
            arrival_rate=2.5,
            wait_minutes=5.0,
            confidence_lower=4.0,
            confidence_upper=6.0,
            sample_count=15,
            status="medium"
        )
        
        # Verify it was updated
        result = await repo.get_current_wait_time("WC-Norte-L0-1")
        
        assert result is not None
        assert result.poi_id == "WC-Norte-L0-1"
        assert result.wait_minutes == pytest.approx(5.0)
        assert result.confidence_lower == pytest.approx(4.0)
        assert result.confidence_upper == pytest.approx(6.0)
        assert result.status == "medium"
    
    @pytest.mark.asyncio
    async def test_update_queue_state_merge_behavior(self, test_db_session):
        """Test that updating existing queue state merges correctly"""
        repo = WaitTimeRepository(test_db_session)
        
        # First update
        await repo.update_queue_state(
            poi_id="Food-Sul-1",
            arrival_rate=1.0,
            wait_minutes=3.0,
            confidence_lower=2.5,
            confidence_upper=3.5,
            sample_count=10,
            status="low"
        )
        
        # Second update
        await repo.update_queue_state(
            poi_id="Food-Sul-1",
            arrival_rate=2.0,
            wait_minutes=6.0,
            confidence_lower=5.0,
            confidence_upper=7.0,
            sample_count=20,
            status="medium"
        )
        
        # Verify latest values
        result = await repo.get_current_wait_time("Food-Sul-1")
        
        assert result.wait_minutes == pytest.approx(6.0)
        assert result.status == "medium"
    
    @pytest.mark.asyncio
    async def test_get_current_wait_time_not_found(self, test_db_session):
        """Test getting wait time for non-existent POI returns None"""
        repo = WaitTimeRepository(test_db_session)
        
        result = await repo.get_current_wait_time("NonExistent-POI")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_all_wait_times(self, test_db_session):
        """Test getting all wait times"""
        repo = WaitTimeRepository(test_db_session)
        
        # Insert multiple queue states
        pois = ["WC-Norte-L0-1", "Food-Sul-1", "Store-Este-1"]
        
        for i, poi_id in enumerate(pois):
            await repo.update_queue_state(
                poi_id=poi_id,
                arrival_rate=float(i + 1),
                wait_minutes=float(i + 2),
                confidence_lower=float(i + 1),
                confidence_upper=float(i + 3),
                sample_count=10 + i,
                status="low"
            )
        
        # Get all
        all_wait_times = await repo.get_all_wait_times()
        
        assert len(all_wait_times) == len(pois)
        assert all(isinstance(wt, WaitTimeResponse) for wt in all_wait_times)
    
    @pytest.mark.asyncio
    async def test_get_all_wait_times_filtered_by_type(self, test_db_session, sample_pois):
        """Test getting wait times filtered by POI type"""
        poi_repo = POIRepository(test_db_session)
        wait_repo = WaitTimeRepository(test_db_session)
        
        # Insert POIs
        for poi_data in sample_pois:
            await poi_repo.insert_poi(poi_data)
        
        # Insert queue states
        for i, poi_data in enumerate(sample_pois):
            await wait_repo.update_queue_state(
                poi_id=poi_data['id'],
                arrival_rate=1.0,
                wait_minutes=float(i + 3),
                confidence_lower=2.0,
                confidence_upper=4.0,
                sample_count=10,
                status="low"
            )
        
        # Filter by restroom
        restroom_wait_times = await wait_repo.get_all_wait_times(poi_type="restroom")
        
        assert len(restroom_wait_times) == 1
        assert restroom_wait_times[0].poi_id == "WC-Norte-L0-1"
    
    @pytest.mark.asyncio
    async def test_get_queue_state_raw(self, test_db_session):
        """Test getting raw queue state as dict"""
        repo = WaitTimeRepository(test_db_session)
        
        await repo.update_queue_state(
            poi_id="Test-POI",
            arrival_rate=2.0,
            wait_minutes=4.0,
            confidence_lower=3.0,
            confidence_upper=5.0,
            sample_count=12,
            status="medium"
        )
        
        raw_state = await repo.get_queue_state_raw("Test-POI")
        
        assert raw_state is not None
        assert raw_state['poi_id'] == "Test-POI"
        assert raw_state['arrival_rate'] == pytest.approx(2.0)
        assert raw_state['wait_minutes'] == pytest.approx(4.0)
    
    @pytest.mark.asyncio
    async def test_get_queue_state_raw_not_found(self, test_db_session):
        """Test getting raw queue state for non-existent POI returns None"""
        repo = WaitTimeRepository(test_db_session)
        
        raw_state = await repo.get_queue_state_raw("NonExistent-POI")
        
        assert raw_state is None


class TestCameraEventRepository:
    """Tests for CameraEvent repository operations"""
    
    @pytest.mark.asyncio
    async def test_insert_event(self, test_db_session, sample_queue_events):
        """Test inserting a camera event"""
        repo = CameraEventRepository(test_db_session)
        
        event = QueueEvent(**sample_queue_events[0])
        await repo.insert_event(event)
        
        # Verify by querying events
        since = event.timestamp - timedelta(minutes=1)
        events = await repo.get_events_since(event.poi_id, since)
        
        assert len(events) == 1
        assert events[0].poi_id == event.poi_id
        assert events[0].event_type == event.event_type
    
    @pytest.mark.asyncio
    async def test_insert_multiple_events(self, test_db_session, sample_queue_events):
        """Test inserting multiple events"""
        repo = CameraEventRepository(test_db_session)
        
        for event_data in sample_queue_events:
            event = QueueEvent(**event_data)
            await repo.insert_event(event)
        
        # Get all events for WC-Norte-L0-1
        since = datetime.now(timezone.utc) - timedelta(hours=1)
        events = await repo.get_events_since("WC-Norte-L0-1", since)
        
        # Should have 2 events (entry and exit)
        assert len(events) == 2
    
    @pytest.mark.asyncio
    async def test_get_events_since_filters_by_time(self, test_db_session):
        """Test that get_events_since filters by timestamp"""
        repo = CameraEventRepository(test_db_session)
        
        now = datetime.now(timezone.utc)
        poi_id = "Test-POI"
        
        # Insert old event
        old_event = QueueEvent(
            poi_id=poi_id,
            event_type="entry",
            count=1,
            camera_id="cam-1",
            timestamp=now - timedelta(hours=2)
        )
        await repo.insert_event(old_event)
        
        # Insert recent event
        recent_event = QueueEvent(
            poi_id=poi_id,
            event_type="exit",
            count=1,
            camera_id="cam-2",
            timestamp=now - timedelta(minutes=5)
        )
        await repo.insert_event(recent_event)
        
        # Query events since 1 hour ago
        since = now - timedelta(hours=1)
        events = await repo.get_events_since(poi_id, since)
        
        # Should only get recent event
        assert len(events) == 1
        assert events[0].event_type == "exit"
    
    @pytest.mark.asyncio
    async def test_get_events_since_orders_by_time_desc(self, test_db_session):
        """Test that events are ordered by timestamp descending"""
        repo = CameraEventRepository(test_db_session)
        
        now = datetime.now(timezone.utc)
        poi_id = "Test-POI"
        
        # Insert events in order
        for i in range(3):
            event = QueueEvent(
                poi_id=poi_id,
                event_type="entry",
                count=1,
                camera_id=f"cam-{i}",
                timestamp=now - timedelta(minutes=i * 10)
            )
            await repo.insert_event(event)
        
        # Get events
        since = now - timedelta(hours=1)
        events = await repo.get_events_since(poi_id, since)
        
        # Verify descending order
        assert len(events) == 3
        for i in range(len(events) - 1):
            assert events[i].timestamp >= events[i + 1].timestamp
    
    @pytest.mark.asyncio
    async def test_cleanup_old_events(self, test_db_session):
        """Test cleaning up old events"""
        repo = CameraEventRepository(test_db_session)
        
        now = datetime.now(timezone.utc)
        poi_id = "Test-POI"
        
        # Insert old event (older than 24 hours)
        old_event = QueueEvent(
            poi_id=poi_id,
            event_type="entry",
            count=1,
            camera_id="cam-old",
            timestamp=now - timedelta(hours=25)
        )
        await repo.insert_event(old_event)
        
        # Insert recent event
        recent_event = QueueEvent(
            poi_id=poi_id,
            event_type="exit",
            count=1,
            camera_id="cam-recent",
            timestamp=now - timedelta(hours=1)
        )
        await repo.insert_event(recent_event)
        
        # Cleanup old events
        await repo.cleanup_old_events(older_than_hours=24)
        
        # Query all events
        since = now - timedelta(days=30)
        events = await repo.get_events_since(poi_id, since)
        
        # Should only have recent event
        assert len(events) == 1
        assert events[0].camera_id == "cam-recent"
    
    @pytest.mark.asyncio
    async def test_get_events_since_empty(self, test_db_session):
        """Test getting events when none exist"""
        repo = CameraEventRepository(test_db_session)
        
        since = datetime.now(timezone.utc) - timedelta(hours=1)
        events = await repo.get_events_since("NonExistent-POI", since)
        
        assert events == []
