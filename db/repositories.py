"""
Database repositories for data access
"""
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import logging

from db.models import POI, CameraEvent, QueueState
from models import QueueEvent as QueueEventSchema, WaitTimeResponse, POIInfo

logger = logging.getLogger(__name__)


class POIRepository:
    """Repository for POI operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def insert_poi(self, poi_data: dict):
        """Insert or update POI configuration"""
        poi = POI(
            id=poi_data['id'],
            name=poi_data['name'],
            poi_type=poi_data['type'],
            num_servers=poi_data['num_servers'],
            service_rate=poi_data['service_rate']
        )
        await self.session.merge(poi)  # Insert or update
        await self.session.commit()
    
    async def get_poi_by_id(self, poi_id: str) -> Optional[POIInfo]:
        """Get POI by ID"""
        result = await self.session.execute(
            select(POI).where(POI.id == poi_id)
        )
        poi = result.scalar_one_or_none()
        
        if poi:
            return POIInfo(
                id=poi.id,
                name=poi.name,
                poi_type=poi.poi_type,
                num_servers=poi.num_servers,
                service_rate=poi.service_rate
            )
        return None
    
    async def get_all_pois(self, poi_type: Optional[str] = None) -> List[POIInfo]:
        """Get all POIs, optionally filtered by type"""
        query = select(POI)
        if poi_type:
            query = query.where(POI.poi_type == poi_type)
        
        result = await self.session.execute(query)
        pois = result.scalars().all()
        
        return [
            POIInfo(
                id=poi.id,
                name=poi.name,
                poi_type=poi.poi_type,
                num_servers=poi.num_servers,
                service_rate=poi.service_rate
            )
            for poi in pois
        ]


class CameraEventRepository:
    """Repository for camera event operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def insert_event(self, event: QueueEventSchema):
        """Insert camera event"""
        db_event = CameraEvent(
            poi_id=event.poi_id,
            event_type=event.event_type,
            count=event.count,
            camera_id=event.camera_id,
            timestamp=event.timestamp
        )
        self.session.add(db_event)
        await self.session.commit()
    
    async def get_events_since(
        self,
        poi_id: str,
        since: datetime
    ) -> List[QueueEventSchema]:
        """Get events for a POI since a specific time"""
        result = await self.session.execute(
            select(CameraEvent)
            .where(CameraEvent.poi_id == poi_id)
            .where(CameraEvent.timestamp >= since)
            .order_by(CameraEvent.timestamp.desc())
        )
        events = result.scalars().all()
        
        return [
            QueueEventSchema(
                poi_id=e.poi_id,
                event_type=e.event_type,
                count=e.count,
                camera_id=e.camera_id,
                timestamp=e.timestamp
            )
            for e in events
        ]
    
    async def cleanup_old_events(self, older_than_hours: int = 24):
        """Remove events older than specified hours"""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        
        await self.session.execute(
            delete(CameraEvent).where(CameraEvent.timestamp < cutoff)
        )
        await self.session.commit()
        logger.info(f"Cleaned up events older than {older_than_hours} hours")


class WaitTimeRepository:
    """Repository for wait time/queue state operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def update_queue_state(
        self,
        poi_id: str,
        arrival_rate: float,
        wait_minutes: float,
        confidence_lower: float,
        confidence_upper: float,
        sample_count: int,
        status: str
    ):
        """Update or insert queue state"""
        state = QueueState(
            poi_id=poi_id,
            arrival_rate=arrival_rate,
            current_wait_minutes=wait_minutes,
            confidence_lower=confidence_lower,
            confidence_upper=confidence_upper,
            sample_count=sample_count,
            status=status,
            last_updated=datetime.now(timezone.utc)
        )
        await self.session.merge(state)
        await self.session.commit()
    
    async def get_current_wait_time(self, poi_id: str) -> Optional[WaitTimeResponse]:
        """Get current wait time for a POI"""
        result = await self.session.execute(
            select(QueueState).where(QueueState.poi_id == poi_id)
        )
        state = result.scalar_one_or_none()
        
        if state:
            return WaitTimeResponse(
                poi_id=state.poi_id,
                wait_minutes=state.current_wait_minutes,
                confidence_lower=state.confidence_lower or 0.0,
                confidence_upper=state.confidence_upper or 0.0,
                status=state.status,
                timestamp=state.last_updated
            )
        return None
    
    async def get_all_wait_times(
        self,
        poi_type: Optional[str] = None
    ) -> List[WaitTimeResponse]:
        """Get all wait times, optionally filtered by POI type"""
        query = select(QueueState)
        
        if poi_type:
            query = (
                query.join(POI, QueueState.poi_id == POI.id)
                .where(POI.poi_type == poi_type)
            )
        
        result = await self.session.execute(query)
        states = result.scalars().all()
        
        return [
            WaitTimeResponse(
                poi_id=state.poi_id,
                wait_minutes=state.current_wait_minutes,
                confidence_lower=state.confidence_lower or 0.0,
                confidence_upper=state.confidence_upper or 0.0,
                status=state.status,
                timestamp=state.last_updated
            )
            for state in states
        ]
    
    async def get_queue_state_raw(self, poi_id: str) -> Optional[dict]:
        """Get raw queue state as dict (for debug endpoints)"""
        result = await self.session.execute(
            select(QueueState).where(QueueState.poi_id == poi_id)
        )
        state = result.scalar_one_or_none()
        
        return state.to_dict() if state else None
