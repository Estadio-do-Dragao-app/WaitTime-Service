"""
SQLAlchemy database models
"""
from sqlalchemy import Column, String, Float, Integer, DateTime, Index
from sqlalchemy.sql import func
from datetime import datetime, timezone

from db.database import Base


class POI(Base):
    """Point of Interest configuration"""
    __tablename__ = "pois"
    
    id = Column(String, primary_key=True)  # e.g., "Restroom-A3"
    name = Column(String, nullable=False)
    poi_type = Column(String, nullable=False)  # restroom, food, store
    num_servers = Column(Integer, nullable=False)
    service_rate = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "poi_type": self.poi_type,
            "num_servers": self.num_servers,
            "service_rate": self.service_rate
        }


class CameraEvent(Base):
    """Camera events (entry/exit)"""
    __tablename__ = "camera_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    poi_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False)  # entry or exit
    count = Column(Integer, default=1)
    camera_id = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('idx_poi_timestamp', 'poi_id', 'timestamp'),
    )


class QueueState(Base):
    """Current queue state and wait time for each POI"""
    __tablename__ = "queue_states"
    
    poi_id = Column(String, primary_key=True)
    arrival_rate = Column(Float, nullable=False)
    current_wait_minutes = Column(Float, nullable=False)
    confidence_lower = Column(Float)
    confidence_upper = Column(Float)
    sample_count = Column(Integer, default=0)
    status = Column(String, nullable=False)  # low, medium, high, overloaded
    last_updated = Column(DateTime(timezone=True), nullable=False)
    
    def to_dict(self):
        return {
            "poi_id": self.poi_id,
            "arrival_rate": self.arrival_rate,
            "wait_minutes": self.current_wait_minutes,
            "confidence_lower": self.confidence_lower,
            "confidence_upper": self.confidence_upper,
            "sample_count": self.sample_count,
            "status": self.status,
            "last_updated": self.last_updated.isoformat()
        }
