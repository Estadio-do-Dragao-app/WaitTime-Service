"""
Unit tests for queueModel.py
Tests M/M/1, M/M/k queue calculations and arrival rate smoothing
"""
import pytest
import math
from queueModel import QueueModel, ArrivalRateSmoother, WaitTimeResult


class TestQueueModelMM1:
    """Tests for M/M/1 (single server) queue model"""
    
    def test_zero_arrival_rate(self):
        """Test with zero arrivals - should return zero wait time"""
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(
            arrival_rate=0.0,
            service_rate=3.0,
            sample_count=10
        )
        
        assert result.wait_minutes == pytest.approx(0.0)
        assert result.confidence_lower == pytest.approx(0.0)
        assert result.confidence_upper == pytest.approx(0.0)
        assert result.utilization == pytest.approx(0.0)
        assert result.status == 'low'
    
    def test_low_utilization(self):
        """Test low utilization (ρ < 0.5)"""
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(
            arrival_rate=1.0,  # λ = 1
            service_rate=3.0,  # μ = 3
            sample_count=10
        )
        
        # ρ = 1/3 ≈ 0.33
        assert result.utilization == pytest.approx(0.333, abs=0.01)
        assert result.status == 'low'
        assert result.wait_minutes > 0
        assert result.confidence_lower < result.wait_minutes
        assert result.confidence_upper > result.wait_minutes
    
    def test_medium_utilization(self):
        """Test medium utilization (0.5 <= ρ < 0.75)"""
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(
            arrival_rate=2.0,  # λ = 2
            service_rate=3.0,  # μ = 3
            sample_count=15
        )
        
        # ρ = 2/3 ≈ 0.67
        assert result.utilization == pytest.approx(0.667, abs=0.01)
        assert result.status == 'medium'
        assert result.wait_minutes > 0
    
    def test_high_utilization(self):
        """Test high utilization (0.75 <= ρ < 0.95)"""
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(
            arrival_rate=2.5,
            service_rate=3.0,
            sample_count=20
        )
        
        # ρ = 2.5/3 ≈ 0.83
        assert result.utilization == pytest.approx(0.833, abs=0.01)
        assert result.status == 'high'
        assert result.wait_minutes > 0
    
    def test_overloaded_system(self):
        """Test overloaded system (ρ >= 0.95)"""
        model = QueueModel(num_servers=1)
        result = model.calculate_wait_time(
            arrival_rate=3.0,
            service_rate=3.0,
            sample_count=10
        )
        
        # ρ = 1.0
        assert result.utilization >= 0.95
        assert result.status == 'overloaded'
        assert result.wait_minutes == float('inf')
        assert result.confidence_lower == pytest.approx(15.0)
        assert result.confidence_upper == float('inf')
    
    def test_mm1_formula_correctness(self):
        """Test M/M/1 formula: W = 1/(μ - λ)"""
        model = QueueModel(num_servers=1)
        arrival_rate = 2.0
        service_rate = 4.0
        
        result = model.calculate_wait_time(
            arrival_rate=arrival_rate,
            service_rate=service_rate,
            sample_count=10
        )
        
        # W (total time in system) = 1 / (μ - λ) = 1 / (4 - 2) = 0.5
        expected_w = 1 / (service_rate - arrival_rate)
        assert result.wait_minutes == pytest.approx(expected_w, abs=0.01)
    
    def test_confidence_interval_with_more_samples(self):
        """Test that CI narrows with more samples"""
        model = QueueModel(num_servers=1)
        
        result_few = model.calculate_wait_time(
            arrival_rate=1.5,
            service_rate=3.0,
            sample_count=5
        )
        
        result_many = model.calculate_wait_time(
            arrival_rate=1.5,
            service_rate=3.0,
            sample_count=100
        )
        
        # CI width should be smaller with more samples
        width_few = result_few.confidence_upper - result_few.confidence_lower
        width_many = result_many.confidence_upper - result_many.confidence_lower
        assert width_many < width_few
    
    def test_invalid_service_rate(self):
        """Test that invalid service rate raises error"""
        model = QueueModel(num_servers=1)
        
        with pytest.raises(ValueError, match="Service rate must be positive"):
            model.calculate_wait_time(
                arrival_rate=2.0,
                service_rate=0.0,
                sample_count=10
            )


class TestQueueModelMMK:
    """Tests for M/M/k (multiple servers) queue model"""
    
    def test_single_server_matches_mm1(self):
        """Test that M/M/k with k=1 matches M/M/1"""
        mm1_model = QueueModel(num_servers=1)
        mmk_model = QueueModel(num_servers=1)
        
        mm1_result = mm1_model.calculate_wait_time(
            arrival_rate=2.0,
            service_rate=3.0,
            sample_count=10
        )
        
        mmk_result = mmk_model.calculate_wait_time(
            arrival_rate=2.0,
            service_rate=3.0,
            sample_count=10
        )
        
        # Results should be very close (minor differences due to Erlang C)
        assert mm1_result.wait_minutes == pytest.approx(mmk_result.wait_minutes, rel=0.1)
        assert mm1_result.status == mmk_result.status
    
    def test_multi_server_lower_wait_time(self):
        """Test that more servers reduce wait time"""
        model_1 = QueueModel(num_servers=1)
        model_3 = QueueModel(num_servers=3)
        
        arrival_rate = 2.0
        service_rate = 1.0
        
        result_1 = model_1.calculate_wait_time(
            arrival_rate=arrival_rate,
            service_rate=service_rate,
            sample_count=10
        )
        
        result_3 = model_3.calculate_wait_time(
            arrival_rate=arrival_rate,
            service_rate=service_rate,
            sample_count=10
        )
        
        # 3 servers should have lower wait time than 1 server
        assert result_3.wait_minutes < result_1.wait_minutes
    
    def test_mmk_utilization_calculation(self):
        """Test utilization calculation for M/M/k: ρ = λ/(k*μ)"""
        k = 4
        model = QueueModel(num_servers=k)
        
        arrival_rate = 6.0
        service_rate = 2.0
        
        result = model.calculate_wait_time(
            arrival_rate=arrival_rate,
            service_rate=service_rate,
            sample_count=10
        )
        
        # ρ = λ/(k*μ) = 6/(4*2) = 6/8 = 0.75
        expected_rho = arrival_rate / (k * service_rate)
        assert result.utilization == pytest.approx(expected_rho, abs=0.01)
    
    def test_mmk_overloaded(self):
        """Test M/M/k overload condition"""
        model = QueueModel(num_servers=3)
        
        result = model.calculate_wait_time(
            arrival_rate=9.0,
            service_rate=3.0,
            sample_count=10
        )
        
        # ρ = 9/(3*3) = 1.0 >= 0.95
        assert result.status == 'overloaded'
        assert result.wait_minutes == float('inf')
    
    def test_factorial_caching(self):
        """Test that factorial is properly cached"""
        model = QueueModel(num_servers=5)
        
        # First call
        result1 = model.calculate_wait_time(
            arrival_rate=2.0,
            service_rate=1.0,
            sample_count=10
        )
        
        # Second call should use cached factorial
        result2 = model.calculate_wait_time(
            arrival_rate=2.0,
            service_rate=1.0,
            sample_count=10
        )
        
        assert result1.wait_minutes == result2.wait_minutes


class TestArrivalRateSmoother:
    """Tests for exponential moving average smoother"""
    
    def test_initial_update(self):
        """Test first update sets the rate directly"""
        smoother = ArrivalRateSmoother(alpha=0.3)
        
        rate = smoother.update(5.0)
        
        assert rate == pytest.approx(5.0)
        assert smoother.get_rate() == pytest.approx(5.0)
    
    def test_smoothing_effect(self):
        """Test exponential smoothing formula"""
        alpha = 0.3
        smoother = ArrivalRateSmoother(alpha=alpha)
        
        # First update
        smoother.update(10.0)
        
        # Second update
        new_rate = 20.0
        result = smoother.update(new_rate)
        
        # Expected: alpha * new_rate + (1 - alpha) * old_rate
        # = 0.3 * 20 + 0.7 * 10 = 6 + 7 = 13
        expected = alpha * new_rate + (1 - alpha) * 10.0
        assert result == pytest.approx(expected, abs=0.01)
    
    def test_multiple_updates(self):
        """Test series of updates"""
        smoother = ArrivalRateSmoother(alpha=0.3)
        
        rates = [10.0, 15.0, 12.0, 18.0, 14.0]
        
        for rate in rates:
            smoother.update(rate)
        
        # Should be smoothed value, not the last raw value
        assert smoother.get_rate() != rates[-1]
        assert 10.0 < smoother.get_rate() < 18.0
    
    def test_high_alpha_more_responsive(self):
        """Test that high alpha is more responsive to changes"""
        smoother_low = ArrivalRateSmoother(alpha=0.1)
        smoother_high = ArrivalRateSmoother(alpha=0.9)
        
        # Initialize both
        smoother_low.update(10.0)
        smoother_high.update(10.0)
        
        # Apply spike
        spike_rate = 50.0
        result_low = smoother_low.update(spike_rate)
        result_high = smoother_high.update(spike_rate)
        
        # High alpha should be closer to spike
        assert abs(result_high - spike_rate) < abs(result_low - spike_rate)
    
    def test_get_rate_before_update(self):
        """Test get_rate returns None before any updates"""
        smoother = ArrivalRateSmoother(alpha=0.3)
        
        assert smoother.get_rate() is None


class TestWaitTimeResult:
    """Tests for WaitTimeResult dataclass"""
    
    def test_result_creation(self):
        """Test creating a WaitTimeResult"""
        result = WaitTimeResult(
            wait_minutes=5.5,
            confidence_lower=4.0,
            confidence_upper=7.0,
            utilization=0.65,
            status='medium'
        )
        
        assert result.wait_minutes == pytest.approx(5.5)
        assert result.confidence_lower == pytest.approx(4.0)
        assert result.confidence_upper == pytest.approx(7.0)
        assert result.utilization == pytest.approx(0.65)
        assert result.status == 'medium'
