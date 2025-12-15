"""
Pydantic schemas for Wait Time Service
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional


# ============ Incoming Events (from downstream broker) ============

class QueueEvent(BaseModel):
    """
    Event received from downstream broker (queue_events)
    Published by camera processing service
    """
    poi_id: str = Field(..., description="POI identifier")
    event_type: str = Field(..., description="entry or exit")
    count: int = Field(default=1, description="Number of people")
    camera_id: str = Field(..., description="Camera identifier")
    timestamp: datetime = Field(..., description="Event timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "poi_id": "Restroom-A3",
                "event_type": "entry",
                "count": 1,
                "camera_id": "cam-101",
                "timestamp": "2025-10-08T18:05:00Z"
            }
        }
    )


# ============ Outgoing Updates (to upstream broker) ============

class WaitTimeUpdate(BaseModel):
    """
    Wait time update published to upstream broker
    Consumed by Gateway/WebSocket service for client distribution
    """
    poi_id: str
    wait_minutes: float
    confidence_lower: float
    confidence_upper: float
    status: str  # low, medium, high, overloaded
    timestamp: datetime
    
    def to_broker_message(self) -> dict:
        """
        Convert to broker message format matching project spec:
        {
            "type": "waittime",
            "poi": "Restroom-A3",
            "minutes": 6.0,
            "ci95": [4.0, 8.5],
            "ts": "2025-10-08T18:06:00Z"
        }
        """
        return {
            "type": "waittime",
            "poi": self.poi_id,
            "minutes": round(self.wait_minutes, 1),
            "ci95": [
                round(self.confidence_lower, 1),
                round(self.confidence_upper, 1)
            ],
            "status": self.status,
            "ts": self.timestamp.isoformat()
        }


# ============ HTTP API Responses ============

class WaitTimeResponse(BaseModel):
    """Response for wait time queries"""
    poi_id: str
    wait_minutes: float
    confidence_lower: float
    confidence_upper: float
    status: str
    timestamp: datetime
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "poi_id": "Restroom-A3",
                "wait_minutes": 6.0,
                "confidence_lower": 4.0,
                "confidence_upper": 8.5,
                "status": "medium",
                "timestamp": "2025-10-08T18:06:00Z"
            }
        }
    )


class POIInfo(BaseModel):
    """POI (Point of Interest) information"""
    id: str
    name: str
    poi_type: str  # restroom, food, store
    num_servers: int
    service_rate: float
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "Restroom-A3",
                "name": "Restrooms Section A Level 3",
                "poi_type": "restroom",
                "num_servers": 8,
                "service_rate": 0.5
            }
        }
    )


# ============ Database Models ============

class QueueState(BaseModel):
    """Current state of a queue (from database)"""
    poi_id: str
    arrival_rate: float
    current_wait_minutes: float
    confidence_lower: Optional[float]
    confidence_upper: Optional[float]
    last_updated: datetime
    sample_count: int
    status: str
