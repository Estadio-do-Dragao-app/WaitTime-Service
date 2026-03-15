"""
Queue modeling using M/M/1 and M/M/k for wait time estimation
Based on queueing theory principles from the project spec
"""
import math
from typing import Tuple, Optional
from dataclasses import dataclass


@dataclass
class WaitTimeResult:
    """Result from wait time calculation"""
    wait_minutes: float
    confidence_lower: float
    confidence_upper: float
    utilization: float
    status: str  # 'low', 'medium', 'high', 'overloaded'


class QueueModel:
    """Wait time calculator using M/M/1 or M/M/k queueing models"""
    
    def __init__(self, num_servers: int = 1):
        """
        Args:
            num_servers: Number of parallel service points (k in M/M/k)
        """
        self.num_servers = num_servers
    
    def calculate_wait_time(
        self,
        arrival_rate: float,
        service_rate: float,
        sample_count: int = 10
    ) -> WaitTimeResult:
        """
        Calculate expected wait time using queueing theory
        
        Args:
            arrival_rate: λ (lambda) - arrivals per minute
            service_rate: μ (mu) - service rate per minute per server
            sample_count: number of observations (affects confidence)
            
        Returns:
            WaitTimeResult with wait time and confidence interval
        """
        # Handle edge cases
        if arrival_rate <= 0:
            return WaitTimeResult(
                wait_minutes=0.0,
                confidence_lower=0.0,
                confidence_upper=0.0,
                utilization=0.0,
                status='low'
            )
        
        if service_rate <= 0:
            raise ValueError("Service rate must be positive")
        
        if self.num_servers == 1:
            return self._calculate_mm1(arrival_rate, service_rate, sample_count)
        else:
            return self._calculate_mmk(arrival_rate, service_rate, sample_count)
    
    def _calculate_mm1(
        self,
        arrival_rate: float,
        service_rate: float,
        sample_count: int
    ) -> WaitTimeResult:
        """M/M/1 queue calculation (single server)"""
        
        # Traffic intensity (utilization)
        rho = arrival_rate / service_rate
        
        # Overloaded system check
        if rho >= 0.95:
            return WaitTimeResult(
                wait_minutes=float('inf'),
                confidence_lower=15.0,  # arbitrary high values
                confidence_upper=float('inf'),
                utilization=rho,
                status='overloaded'
            )
        
        # Average time in queue (Wq) - waiting before service
        Wq = rho / (service_rate * (1 - rho))
        
        # Average time in system (W) - waiting + service
        W = Wq + (1 / service_rate)
        
        # Calculate confidence interval based on sample size
        # Using approximate formula: CI ≈ W ± z * (W / sqrt(n))
        # For 95% CI, z ≈ 1.96
        z_score = 1.96
        margin = z_score * (W / math.sqrt(max(sample_count, 1)))
        
        ci_lower = max(0.0, W - margin)
        ci_upper = W + margin
        
        # Determine status
        if rho < 0.5:
            status = 'low'
        elif rho < 0.75:
            status = 'medium'
        else:
            status = 'high'
        
        return WaitTimeResult(
            wait_minutes=W,
            confidence_lower=ci_lower,
            confidence_upper=ci_upper,
            utilization=rho,
            status=status
        )
    
    def _calculate_mmk(
        self,
        arrival_rate: float,
        service_rate: float,
        sample_count: int
    ) -> WaitTimeResult:
        """M/M/k queue calculation (multiple servers)"""
        
        k = self.num_servers
        
        # System utilization
        rho = arrival_rate / (k * service_rate)
        
        if rho >= 0.95:
            return WaitTimeResult(
                wait_minutes=float('inf'),
                confidence_lower=15.0,
                confidence_upper=float('inf'),
                utilization=rho,
                status='overloaded'
            )
        
        # Erlang C formula for probability of waiting
        # This is a simplified approximation
        a = arrival_rate / service_rate  # total traffic
        
        # P0 calculation (probability of 0 customers)
        # Optimized with math.gamma for continuous or cached factorial if needed
        # But for small k, simple loop is fine. Using lru_cache on a helper would be better for high k.
        
        sum_term = sum((a ** n) / self._get_factorial(n) for n in range(k))
        last_term = (a ** k) / (self._get_factorial(k) * (1 - rho))
        P0 = 1 / (sum_term + last_term)
        
        # Probability of waiting (Erlang C)
        C = ((a ** k) / self._get_factorial(k)) * (1 / (1 - rho)) * P0
        
        # Average wait time in queue
        Wq = C / (k * service_rate - arrival_rate)
        
        # Average time in system
        W = Wq + (1 / service_rate)
        
        # Confidence interval
        z_score = 1.96
        margin = z_score * (W / math.sqrt(max(sample_count, 1)))
        
        ci_lower = max(0.0, W - margin)
        ci_upper = W + margin
        
        # Status based on utilization
        if rho < 0.5:
            status = 'low'
        elif rho < 0.75:
            status = 'medium'
        else:
            status = 'high'
        
        return WaitTimeResult(
            wait_minutes=W,
            confidence_lower=ci_lower,
            confidence_upper=ci_upper,
            utilization=rho,
            status=status
        )

    from functools import lru_cache

    @staticmethod
    @lru_cache(maxsize=128)
    def _get_factorial(n: int) -> int:
        """Cached factorial for repetitive M/M/k calculations"""
        return math.factorial(n)


class ArrivalRateSmoother:
    """Exponential Moving Average for smoothing arrival rates"""
    
    def __init__(self, alpha: float = 0.3):
        """
        Args:
            alpha: Smoothing factor (0 < alpha < 1)
                  Higher = more responsive to recent data
                  Lower = more smooth, less reactive
        """
        self.alpha = alpha
        self.current_rate = None
    
    def update(self, new_rate: float) -> float:
        """
        Update smoothed arrival rate with new observation
        
        Args:
            new_rate: Latest arrival rate measurement
            
        Returns:
            Smoothed arrival rate
        """
        if self.current_rate is None:
            self.current_rate = new_rate
        else:
            self.current_rate = (
                self.alpha * new_rate + 
                (1 - self.alpha) * self.current_rate
            )
        
        return self.current_rate
    
    def get_rate(self) -> Optional[float]:
        """Get current smoothed rate"""
        return self.current_rate


# Example usage
if __name__ == "__main__":
    # Single server example (M/M/1)
    model = QueueModel(num_servers=1)
    
    # 2 people/min arriving, 3 people/min service rate
    result = model.calculate_wait_time(
        arrival_rate=2.0,
        service_rate=3.0,
        sample_count=15
    )
    
    print(f"Wait time: {result.wait_minutes:.2f} minutes")
    print(f"CI 95%: [{result.confidence_lower:.2f}, {result.confidence_upper:.2f}]")
    print(f"Utilization: {result.utilization:.2%}")
    print(f"Status: {result.status}")
    
    # Multiple servers example (M/M/k)
    model_multi = QueueModel(num_servers=3)
    result_multi = model_multi.calculate_wait_time(
        arrival_rate=8.0,
        service_rate=3.0,
        sample_count=20
    )
    
    print(f"\nMulti-server wait time: {result_multi.wait_minutes:.2f} minutes")
    print(f"Status: {result_multi.status}")
    
    # EMA smoother example
    smoother = ArrivalRateSmoother(alpha=0.3)
    rates = [2.0, 2.5, 3.0, 2.8, 2.3]
    
    print("\nSmoothing arrival rates:")
    for rate in rates:
        smoothed = smoother.update(rate)
        print(f"Raw: {rate:.2f} → Smoothed: {smoothed:.2f}")
