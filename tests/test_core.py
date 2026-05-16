import pytest
from datetime import datetime, timezone
from queueModel import QueueModel, ArrivalRateSmoother
from schemas import QueueEvent, WaitTimeUpdate, WaitTimeResponse, POIInfo


class TestQueueModel:
    def test_mm1_low_load(self):
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(1.0, 3.0, 10)
        assert result.wait_minutes > 0
        assert result.status == "low"
        assert 0 < result.utilization < 0.5

    def test_mm1_high_load(self):
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(2.5, 3.0, 10)
        assert result.status == "high"

    def test_mmk_reduces_wait(self):
        mm1 = QueueModel(1)
        mmk = QueueModel(3)
        r1 = mm1.calculate_wait_time(2.0, 1.0, 10)
        rk = mmk.calculate_wait_time(2.0, 1.0, 10)
        assert rk.wait_minutes < r1.wait_minutes

    def test_overloaded_returns_positive(self):
        model = QueueModel(1)
        result = model.calculate_wait_time(3.0, 3.0, 10)
        assert result.wait_minutes > 0
        assert result.status == "overloaded"

    def test_zero_arrival_rate_returns_low_status(self):
        model = QueueModel(1)
        result = model.calculate_wait_time(0.0, 3.0, 10)
        assert result.wait_minutes == 0.0
        assert result.status == "low"

    def test_zero_service_rate_raises_value_error(self):
        model = QueueModel(1)
        with pytest.raises(ValueError, match="Service rate must be positive"):
            model.calculate_wait_time(1.0, 0.0, 10)

    def test_mmk_overloaded_returns_positive(self):
        model = QueueModel(num_servers=2)
        result = model.calculate_wait_time(10.0, 1.0, 10)  # rho > 0.95
        assert result.wait_minutes > 0
        assert result.status == "overloaded"

    def test_mm1_medium_load(self):
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(1.8, 3.0, 10)  # rho ~0.6
        assert result.status == "medium"


class TestArrivalRateSmoother:
    def test_first_update(self):
        s = ArrivalRateSmoother(0.3)
        assert s.update(10.0) == 10.0

    def test_smoothing(self):
        s = ArrivalRateSmoother(0.3)
        s.update(10.0)
        assert s.update(20.0) == 0.3 * 20 + 0.7 * 10

    def test_get_rate_before_first_update_is_none(self):
        s = ArrivalRateSmoother(0.3)
        assert s.get_rate() is None

    def test_get_rate_after_update_is_set(self):
        s = ArrivalRateSmoother(0.3)
        s.update(5.0)
        assert s.get_rate() == 5.0


class TestQueueEventValidation:
    def test_valid_event(self):
        event = QueueEvent(
            event_id="e1",
            event_type="queue_update",
            location_type="BAR",
            location_id="bar_norte_1",
            queue_length=5,
            timestamp=datetime.now(timezone.utc),
            metadata={"cam": "123"}
        )
        assert event.location_id == "bar_norte_1"

    def test_missing_required_field(self):
        with pytest.raises(Exception):
            QueueEvent(event_id="e1")  # missing other required fields


class TestWaitTimeUpdate:
    def test_to_broker_message(self):
        update = WaitTimeUpdate(
            poi_id="WC-Norte-L0-1",
            wait_minutes=6.5,
            confidence_lower=5.0,
            confidence_upper=8.0,
            status="medium",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)
        )
        msg = update.to_broker_message()
        assert msg["type"] == "waittime"
        assert msg["minutes"] == 6.5
        assert msg["ci95"] == [5.0, 8.0]


class TestPOIInfo:
    def test_poi_creation(self):
        poi = POIInfo(id="p1", name="Test", poi_type="food", num_servers=4, service_rate=0.5)
        assert poi.num_servers == 4


class TestWaitTimeResponse:
    def test_response_creation(self):
        resp = WaitTimeResponse(
            poi_id="p1",
            wait_minutes=3.0,
            confidence_lower=2.5,
            confidence_upper=3.5,
            status="low",
            timestamp=datetime.now(timezone.utc)
        )
        assert resp.poi_id == "p1"
        assert resp.wait_minutes == 3.0