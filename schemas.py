from datetime import datetime, timezone
from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, List, Optional
import uuid

class QueueEvent(BaseModel):
    """Event received from downstream broker"""
    event_id: str
    event_type: str = "queue_update"
    location_type: str
    location_id: str
    queue_length: int = Field(..., ge=0)
    timestamp: datetime
    metadata: Dict[str, str]

class WaitTimeUpdate(BaseModel):
    """Wait time update published to upstream broker"""
    poi_id: str
    wait_minutes: float = Field(..., ge=0)
    confidence_lower: float = Field(..., ge=0)
    confidence_upper: float = Field(..., ge=0)
    status: str  # low, medium, high, overloaded
    timestamp: datetime
    
    def to_broker_message(self) -> dict:
        return {
            "type": "waittime",
            "poi": self.poi_id,
            "minutes": round(self.wait_minutes, 1),
            "ci95": [round(self.confidence_lower, 1), round(self.confidence_upper, 1)],
            "status": self.status,
            "ts": self.timestamp.isoformat()
        }

class WaitTimeResponse(BaseModel):
    """Response for wait time queries"""
    poi_id: str
    wait_minutes: float = Field(..., ge=0)
    confidence_lower: float = Field(..., ge=0)
    confidence_upper: float = Field(..., ge=0)
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
    poi_type: str
    num_servers: int
    service_rate: float

class QueueState(BaseModel):
    """Current state of a queue (from database)"""
    poi_id: str
    arrival_rate: float = Field(..., ge=0)
    current_wait_minutes: float = Field(..., ge=0)
    confidence_lower: Optional[float] = Field(None, ge=0)
    confidence_upper: Optional[float] = Field(None, ge=0)
    last_updated: datetime
    sample_count: int = Field(..., ge=0)
    status: str
