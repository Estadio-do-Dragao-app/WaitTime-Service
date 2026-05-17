"""
Queue modeling using M/M/1 and M/M/k for wait time estimation
Based on queueing theory principles from the project spec
"""
import math
from functools import lru_cache
from typing import Optional
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
            arrival_rate: lambda - arrivals per minute
            service_rate: mu - service rate per minute per server
            sample_count: number of observations (affects confidence interval width)
            
        Returns:
            WaitTimeResult with wait time and confidence interval
        """
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
        
        rho = arrival_rate / service_rate
        
        # Overloaded system: use linear estimate as fallback
        if rho >= 0.95:
            wait = max(sample_count + 1, 1) / service_rate
            return WaitTimeResult(
                wait_minutes=float(wait),
                confidence_lower=wait * 0.8,
                confidence_upper=wait * 1.2,
                utilization=rho,
                status='overloaded'
            )
        
        # Average time in queue (wq) - waiting before service begins
        wq = rho / (service_rate * (1 - rho))
        
        # Average time in system (w) - waiting + service
        w = wq + (1 / service_rate)
        
        # 95% confidence interval: CI = w +/- z * (w / sqrt(n))
        z_score = 1.96
        margin = z_score * (w / math.sqrt(max(sample_count, 1)))
        
        ci_lower = max(0.0, w - margin)
        ci_upper = w + margin
        
        status = self._get_status(rho)
        
        return WaitTimeResult(
            wait_minutes=w,
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
        rho = arrival_rate / (k * service_rate)
        
        if rho >= 0.95:
            # Fallback: linear estimate (Current Queue / Aggregate Throughput)
            throughput = k * service_rate
            wait = max(sample_count + 1, 1) / throughput
            return WaitTimeResult(
                wait_minutes=float(wait),
                confidence_lower=wait * 0.8,
                confidence_upper=wait * 1.5,
                utilization=rho,
                status='overloaded'
            )
        
        # Erlang C formula for probability of waiting
        a = arrival_rate / service_rate  # total traffic intensity
        
        sum_term = sum(_factorial(n) and (a ** n) / _factorial(n) for n in range(k))
        last_term = (a ** k) / (_factorial(k) * (1 - rho))
        P0 = 1 / (sum_term + last_term)
        
        # Probability of waiting (Erlang C)
        C = ((a ** k) / _factorial(k)) * (1 / (1 - rho)) * P0
        
        # Average wait time in queue
        wq = C / (k * service_rate - arrival_rate)
        
        # Average time in system
        w = wq + (1 / service_rate)
        
        # 95% confidence interval
        z_score = 1.96
        margin = z_score * (w / math.sqrt(max(sample_count, 1)))
        
        ci_lower = max(0.0, w - margin)
        ci_upper = w + margin
        
        status = self._get_status(rho)
        
        return WaitTimeResult(
            wait_minutes=w,
            confidence_lower=ci_lower,
            confidence_upper=ci_upper,
            utilization=rho,
            status=status
        )

    @staticmethod
    def _get_status(rho: float) -> str:
        """Determine congestion status from utilization rate"""
        if rho < 0.5:
            return 'low'
        elif rho < 0.75:
            return 'medium'
        return 'high'


@lru_cache(maxsize=128)
def _factorial(n: int) -> int:
    """Cached factorial for repetitive M/M/k calculations"""
    return math.factorial(n)


class ArrivalRateSmoother:
    """Exponential Moving Average (EMA) for smoothing arrival rates"""
    
    def __init__(self, alpha: float = 0.3):
        """
        Args:
            alpha: Smoothing factor (0 < alpha < 1).
                   Higher values = more responsive to recent data.
                   Lower values = smoother, less reactive.
        """
        self.alpha = alpha
        self.current_rate = None
    
    def update(self, new_rate: float) -> float:
        """
        Update smoothed arrival rate with a new observation.
        
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
        """Get current smoothed rate (None if no observations yet)"""
        return self.current_rate
