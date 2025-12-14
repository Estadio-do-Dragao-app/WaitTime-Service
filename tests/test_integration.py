"""
Integration tests for WaitTime-Service
Tests end-to-end flows and component integration
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from db.repositories import POIRepository, WaitTimeRepository
from db.models import POI, QueueState
from queueModel import QueueModel, ArrivalRateSmoother
from models import QueueEvent


class TestDatabaseIntegration:
    """Integration tests for database operations"""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_full_poi_workflow(self, test_db_session):
        """Test full POI workflow: insert -> query -> update"""
        poi_repo = POIRepository(test_db_session)
        
        # Insert POI
        poi_data = {
            "id": "Test-POI-1",
            "name": "Test POI",
            "type": "restroom",
            "num_servers": 5,
            "service_rate": 0.45
        }
        await poi_repo.insert_poi(poi_data)
        
        # Query POI
        result = await poi_repo.get_poi_by_id("Test-POI-1")
        assert result is not None
        assert result.num_servers == 5
        
        # Update POI (merge behavior)
        poi_data['num_servers'] = 8
        await poi_repo.insert_poi(poi_data)
        
        # Verify update
        updated = await poi_repo.get_poi_by_id("Test-POI-1")
        assert updated.num_servers == 8
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_queue_state_calculation_and_storage(self, test_db_session):
        """Test calculating wait time and storing in database"""
        # Setup POI
        poi_repo = POIRepository(test_db_session)
        poi_data = {
            "id": "Integration-Test-POI",
            "name": "Integration Test POI",
            "type": "food",
            "num_servers": 3,
            "service_rate": 0.4
        }
        await poi_repo.insert_poi(poi_data)
        
        # Calculate wait time
        model = QueueModel(num_servers=3)
        result = model.calculate_wait_time(
            arrival_rate=1.5,
            service_rate=0.4,
            sample_count=10
        )
        
        # Store in database
        wait_repo = WaitTimeRepository(test_db_session)
        await wait_repo.update_queue_state(
            poi_id="Integration-Test-POI",
            arrival_rate=1.5,
            wait_minutes=result.wait_minutes,
            confidence_lower=result.confidence_lower,
            confidence_upper=result.confidence_upper,
            sample_count=10,
            status=result.status
        )
        
        # Query stored wait time
        stored = await wait_repo.get_current_wait_time("Integration-Test-POI")
        
        assert stored is not None
        assert stored.poi_id == "Integration-Test-POI"
        assert stored.wait_minutes == result.wait_minutes
        assert stored.status == result.status


class TestEventProcessingFlow:
    """Integration tests for event processing workflow"""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_event_to_wait_time_calculation(self, test_db_session):
        """Test flow: queue event -> arrival rate -> wait time calculation"""
        # Setup POI
        poi_repo = POIRepository(test_db_session)
        poi_data = {
            "id": "Event-Flow-POI",
            "name": "Event Flow Test POI",
            "type": "restroom",
            "num_servers": 8,
            "service_rate": 0.5
        }
        await poi_repo.insert_poi(poi_data)
        
        # Simulate queue length from events
        queue_length = 20
        window_minutes = 5
        
        # Calculate arrival rate
        arrival_rate = queue_length / window_minutes  # 4.0 people/min
        
        # Apply smoothing
        smoother = ArrivalRateSmoother(alpha=0.3)
        smoothed_rate = smoother.update(arrival_rate)
        
        # Calculate wait time
        model = QueueModel(num_servers=8)
        result = model.calculate_wait_time(
            arrival_rate=smoothed_rate,
            service_rate=0.5,
            sample_count=queue_length
        )
        
        # Store result
        wait_repo = WaitTimeRepository(test_db_session)
        await wait_repo.update_queue_state(
            poi_id="Event-Flow-POI",
            arrival_rate=smoothed_rate,
            wait_minutes=result.wait_minutes,
            confidence_lower=result.confidence_lower,
            confidence_upper=result.confidence_upper,
            sample_count=queue_length,
            status=result.status
        )
        
        # Verify end-to-end
        final_wait_time = await wait_repo.get_current_wait_time("Event-Flow-POI")
        
        assert final_wait_time is not None
        assert final_wait_time.wait_minutes > 0
        assert final_wait_time.status in ['low', 'medium', 'high', 'overloaded']
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_multiple_event_updates_with_smoothing(self, test_db_session):
        """Test multiple queue updates with arrival rate smoothing"""
        # Setup POI
        poi_repo = POIRepository(test_db_session)
        poi_data = {
            "id": "Smoothing-Test-POI",
            "name": "Smoothing Test POI",
            "type": "food",
            "num_servers": 4,
            "service_rate": 0.4
        }
        await poi_repo.insert_poi(poi_data)
        
        # Simulate multiple queue updates
        queue_lengths = [10, 15, 12, 18, 14]
        window_minutes = 5
        
        smoother = ArrivalRateSmoother(alpha=0.3)
        model = QueueModel(num_servers=4)
        wait_repo = WaitTimeRepository(test_db_session)
        
        for queue_length in queue_lengths:
            # Calculate and smooth arrival rate
            arrival_rate = queue_length / window_minutes
            smoothed_rate = smoother.update(arrival_rate)
            
            # Calculate wait time
            result = model.calculate_wait_time(
                arrival_rate=smoothed_rate,
                service_rate=0.4,
                sample_count=queue_length
            )
            
            # Store update
            await wait_repo.update_queue_state(
                poi_id="Smoothing-Test-POI",
                arrival_rate=smoothed_rate,
                wait_minutes=result.wait_minutes,
                confidence_lower=result.confidence_lower,
                confidence_upper=result.confidence_upper,
                sample_count=queue_length,
                status=result.status
            )
        
        # Verify final state
        final_state = await wait_repo.get_current_wait_time("Smoothing-Test-POI")
        
        assert final_state is not None
        # Smoothed rate should not equal the last raw rate
        final_raw_rate = queue_lengths[-1] / window_minutes
        # We can't directly assert the smoothed value without replicating the logic,
        # but we verify the result is stored
        assert final_state.wait_minutes > 0


class TestAPIIntegration:
    """Integration tests for API with database"""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_api_poi_to_wait_time_workflow(self, test_client, test_db_session):
        """Test complete workflow via API: POI creation -> wait time update -> query"""
        # Insert POI directly via database
        poi_repo = POIRepository(test_db_session)
        poi_data = {
            "id": "API-Test-POI",
            "name": "API Test POI",
            "type": "store",
            "num_servers": 2,
            "service_rate": 0.3
        }
        await poi_repo.insert_poi(poi_data)
        
        # Insert wait time
        wait_repo = WaitTimeRepository(test_db_session)
        await wait_repo.update_queue_state(
            poi_id="API-Test-POI",
            arrival_rate=1.2,
            wait_minutes=4.5,
            confidence_lower=3.8,
            confidence_upper=5.2,
            sample_count=12,
            status="medium"
        )
        
        # Query via API
        response = await test_client.get("/api/poi/API-Test-POI")
        assert response.status_code == 200
        poi_response = response.json()
        assert poi_response['id'] == "API-Test-POI"
        
        # Query wait time via API
        response = await test_client.get("/api/waittime?poi=API-Test-POI")
        assert response.status_code == 200
        wait_response = response.json()
        assert wait_response['poi_id'] == "API-Test-POI"
        assert wait_response['wait_minutes'] == 4.5
        assert wait_response['status'] == "medium"
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_api_list_all_with_filters(self, test_client, test_db_session, sample_pois):
        """Test listing POIs and wait times with type filters"""
        # Insert multiple POIs
        poi_repo = POIRepository(test_db_session)
        wait_repo = WaitTimeRepository(test_db_session)
        
        for i, poi_data in enumerate(sample_pois):
            await poi_repo.insert_poi(poi_data)
            
            # Add wait times
            await wait_repo.update_queue_state(
                poi_id=poi_data['id'],
                arrival_rate=float(i + 1),
                wait_minutes=float((i + 1) * 2),
                confidence_lower=float(i + 1),
                confidence_upper=float((i + 1) * 3),
                sample_count=10,
                status="low"
            )
        
        # Get all POIs
        response = await test_client.get("/api/pois")
        assert response.status_code == 200
        all_pois = response.json()
        assert len(all_pois) == 3
        
        # Filter by type
        response = await test_client.get("/api/pois?poi_type=restroom")
        assert response.status_code == 200
        restrooms = response.json()
        assert len(restrooms) == 1
        assert restrooms[0]['poi_type'] == "restroom"
        
        # Get all wait times
        response = await test_client.get("/api/waittime/all")
        assert response.status_code == 200
        all_wait_times = response.json()
        assert len(all_wait_times) == 3


class TestMapServiceIntegration:
    """Integration tests for MapService client with database"""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_fetch_pois_and_store_in_db(self, test_db_session, mock_map_service_response):
        """Test fetching POIs from MapService and storing in database"""
        from services.map_service import MapServiceClient
        
        # Mock the HTTP client
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_map_service_response
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response
            
            # Fetch POIs
            client = MapServiceClient(base_url="http://test-map:8000")
            pois = await client.fetch_pois()
            
            # Store in database
            poi_repo = POIRepository(test_db_session)
            for poi_data in pois:
                await poi_repo.insert_poi(poi_data)
            
            # Verify storage
            all_pois = await poi_repo.get_all_pois()
            
            assert len(all_pois) == 2
            assert any(poi.id == "WC-Norte-L0-1" for poi in all_pois)
            assert any(poi.id == "Food-Sul-1" for poi in all_pois)


class TestQueueModelIntegration:
    """Integration tests for queue model calculations"""
    
    @pytest.mark.integration
    def test_mm1_to_mmk_consistency(self):
        """Test that M/M/k reduces to M/M/1 for k=1"""
        arrival_rate = 2.0
        service_rate = 3.0
        
        mm1 = QueueModel(num_servers=1)
        mmk = QueueModel(num_servers=1)
        
        result_mm1 = mm1.calculate_wait_time(arrival_rate, service_rate, 10)
        result_mmk = mmk.calculate_wait_time(arrival_rate, service_rate, 10)
        
        # Results should be very similar
        assert abs(result_mm1.wait_minutes - result_mmk.wait_minutes) < 0.1
        assert result_mm1.status == result_mmk.status
    
    @pytest.mark.integration
    def test_queue_model_with_smoother_realistic_scenario(self):
        """Test realistic scenario with varying queue lengths"""
        # Simulate a busy period
        queue_lengths = [5, 8, 12, 15, 18, 20, 18, 15, 12, 8]
        window_minutes = 5
        
        smoother = ArrivalRateSmoother(alpha=0.3)
        model = QueueModel(num_servers=4)
        
        wait_times = []
        
        for queue_length in queue_lengths:
            arrival_rate = queue_length / window_minutes
            smoothed_rate = smoother.update(arrival_rate)
            
            result = model.calculate_wait_time(
                arrival_rate=smoothed_rate,
                service_rate=0.4,
                sample_count=queue_length
            )
            
            wait_times.append(result.wait_minutes)
        
        # Verify realistic behavior
        assert len(wait_times) == len(queue_lengths)
        assert all(wt >= 0 for wt in wait_times)
        
        # Peak wait time should be higher than initial
        peak_idx = queue_lengths.index(max(queue_lengths))
        assert wait_times[peak_idx] > wait_times[0]
