"""
Wait Time Service - FastAPI application
Consumes queue_events from downstream broker
Publishes waittime_updates to upstream broker
Provides HTTP API for queries
"""
from fastapi import FastAPI, Query, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional
import asyncio
import logging

from schemas import WaitTimeResponse, POIInfo
from db.database import get_db, init_db, close_db, get_db_session
from db.repositories import WaitTimeRepository, POIRepository
from consumer import RobustMQTTConsumer as EventConsumer
from services.map_service import MapServiceClient
from services.data_retention import DataRetentionService
from services.audit_logger import audit_logger

import os
import secrets
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Global event consumer instance
event_consumer: Optional[EventConsumer] = None
retention_service: Optional[DataRetentionService] = None

API_KEY_NAME = "X-API-Key"
API_KEY = os.getenv("API_KEY", "dragao_secret_key_2026")  # Load from env, fallback for dev
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header and secrets.compare_digest(api_key_header, API_KEY):
        return api_key_header
    raise HTTPException(
        status_code=401,
        detail="Unauthorized access - invalid or missing API key"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic"""
    global event_consumer
    
    # Startup
    logger.info("Starting Wait Time Service...")
    
    # Initialize database
    await init_db()
    logger.info("Database initialized")
    
    # Fetch POI configurations from MapService
    try:
        map_client = MapServiceClient()
        logger.info("Fetching POI configurations from MapService...")
        pois = await map_client.fetch_pois()
        
        # Store POIs in database
        async with get_db() as db:
            poi_repo = POIRepository(db)
            for poi_data in pois:
                await poi_repo.insert_poi(poi_data)
        
        logger.info(f"Loaded {len(pois)} POIs from MapService")
        
    except Exception as e:
        logger.error(f"Failed to fetch POIs from MapService: {e}")
        logger.warning("Starting service without POI data - will retry on first events")
    
    # Initialize event consumer (subscribes to broker)
    event_consumer = EventConsumer(window_minutes=5)
    
    # Initialize and start data retention service
    global retention_service
    retention_service = DataRetentionService(retention_hours=24, check_interval_hours=1)
    retention_task = asyncio.create_task(retention_service.start())
    
    # Start consuming events in background
    consumer_task = asyncio.create_task(event_consumer.start())
    
    logger.info("Wait Time Service ready - subscribed to queue_events")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Wait Time Service...")
    if event_consumer:
        await event_consumer.stop()
    if retention_service:
        retention_service.stop()
        
    consumer_task.cancel()
    retention_task.cancel()
    try:
        await consumer_task
        await retention_task
    except asyncio.CancelledError:  # NOSONAR - intentional during shutdown
        logger.info("Background tasks cancelled successfully")
    
    await close_db()
    logger.info("Database connections closed")


app = FastAPI(
    title="Wait Time Service",
    description="Calculates and publishes queue wait times for stadium POIs",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ HTTP API Endpoints ============
# Note: WebSocket is handled by another service (Gateway/WebSocket Service)
# This service only provides HTTP queries and publishes to broker

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    consumer_status = "connected" if event_consumer and event_consumer.running else "disconnected"
    
    return {
        "status": "healthy",
        "service": "waittime",
        "consumer_status": consumer_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/waittime", response_model=WaitTimeResponse)
async def get_wait_time(
    poi: str = Query(..., description="POI identifier (e.g., Restroom-A3)"),
    db: AsyncSession = Depends(get_db_session),
    api_key: str = Depends(get_api_key)
):
    """
    Get current wait time for a specific POI
    
    Example: GET /api/waittime?poi=Restroom-A3
    
    Response:
    {
        "poi_id": "Restroom-A3",
        "wait_minutes": 6.0,
        "confidence_lower": 4.0,
        "confidence_upper": 8.5,
        "status": "medium",
        "timestamp": "2025-10-08T18:06:00Z"
    }
    """
    repo = WaitTimeRepository(db)
    
    audit_logger.info(f"API Access: get_wait_time queried for {poi}")
    
    wait_time = await repo.get_current_wait_time(poi)
    
    if not wait_time:
        raise HTTPException(
            status_code=404,
            detail=f"POI '{poi}' not found or no wait time data available"
        )
    
    return wait_time


@app.get("/api/waittime/all", response_model=List[WaitTimeResponse])
async def get_all_wait_times(
    poi_type: Optional[str] = Query(
        None, 
        description="Filter by POI type (restroom, food, store)"
    ),
    db: AsyncSession = Depends(get_db_session),
    api_key: str = Depends(get_api_key)
):
    """
    Get wait times for all POIs, optionally filtered by type
    
    Example: GET /api/waittime/all?poi_type=restroom
    """
    repo = WaitTimeRepository(db)
    
    audit_logger.info("API Access: get_all_wait_times queried")
    
    wait_times = await repo.get_all_wait_times(poi_type=poi_type)
    
    return wait_times


@app.get("/api/pois", response_model=List[POIInfo])
async def get_pois(
    poi_type: Optional[str] = Query(None, description="Filter by type"),
    db: AsyncSession = Depends(get_db_session),
    api_key: str = Depends(get_api_key)
):
    """
    Get list of all POIs (Points of Interest)
    
    Example: GET /api/pois?poi_type=food
    """
    repo = POIRepository(db)
    
    pois = await repo.get_all_pois(poi_type=poi_type)
    
    return pois


@app.get("/api/poi/{poi_id}", response_model=POIInfo)
async def get_poi_details(
    poi_id: str,
    db: AsyncSession = Depends(get_db_session),
    api_key: str = Depends(get_api_key)
):
    """
    Get details for a specific POI
    
    Example: GET /api/poi/Restroom-A3
    """
    repo = POIRepository(db)
    
    poi = await repo.get_poi_by_id(poi_id)
    
    if not poi:
        raise HTTPException(
            status_code=404,
            detail=f"POI '{poi_id}' not found"
        )
    
    return poi

@app.post("/api/v1/privacy/consent")
async def log_user_consent(
    consent_data: dict,
    api_key: str = Depends(get_api_key)
):
    """
    Log user consent event for accountability
    """
    user_id = consent_data.get("user_id", "unknown")
    action = consent_data.get("action", "granted")
    
    audit_logger.info(f"PRIVACY: User {user_id} {action} consent for GPS tracking")
    
    return {"status": "logged", "timestamp": datetime.now(timezone.utc).isoformat()}


# ============ Admin/Debug Endpoints ============

@app.get("/debug/queue-state/{poi_id}")
async def get_queue_state_debug(
    poi_id: str,
    db: AsyncSession = Depends(get_db_session)
):
    """
    Debug endpoint to see raw queue state including arrival rates
    """
    repo = WaitTimeRepository(db)
    state = await repo.get_queue_state_raw(poi_id)
    
    if not state:
        raise HTTPException(status_code=404, detail="POI not found")
    
    return state


@app.get("/debug/consumer-status")
async def get_consumer_status():
    """
    Debug endpoint to check event consumer status
    """
    if not event_consumer:
        return {"status": "not_initialized"}
    
    return {
        "status": "running" if event_consumer.running else "stopped",
        "active_pois": len(event_consumer.smoothers),
        "queue_models": len(event_consumer.queue_models)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
