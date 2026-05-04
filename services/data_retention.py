import asyncio
from datetime import datetime, timedelta, timezone
from sqlalchemy import delete
from db.database import get_db
from db.schemas import CameraEvent
from services.audit_logger import audit_logger

class DataRetentionService:
    def __init__(self, retention_hours=24, check_interval_hours=1):
        self.retention_hours = retention_hours
        self.check_interval_seconds = check_interval_hours * 3600
        self.running = False

    async def start(self):
        self.running = True
        audit_logger.info("Data Retention Service started. Retention: %d hours.", self.retention_hours)
        while self.running:
            try:
                await self.cleanup_old_data()
            except asyncio.CancelledError:
                audit_logger.info("Data Retention Service cancelled")
                raise
            except Exception as e:
                audit_logger.error("Error during data retention cleanup: %s", e)
            await asyncio.sleep(self.check_interval_seconds)

    def stop(self):
        self.running = False

    async def cleanup_old_data(self):
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        
        async with get_db() as db:
            # Delete CameraEvents older than cutoff_time
            stmt = delete(CameraEvent).where(CameraEvent.timestamp < cutoff_time)
            result = await db.execute(stmt)
            
            deleted_count = result.rowcount
            if deleted_count > 0:
                audit_logger.info("Storage Limitation Policy: Purged %d old camera events prior to %s", deleted_count, cutoff_time.isoformat())
