import pytest
import pytest_asyncio
import asyncio
from typing import AsyncGenerator, Generator
from unittest.mock import MagicMock
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool

from db.database import Base, get_db
from db import database
from app import app
from models import QueueEvent

# Use in-memory SQLite for tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="function")
async def db_engine():
    """Create async engine for the test session"""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    yield engine
    
    # Drop tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()

@pytest_asyncio.fixture
async def test_db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    """
    Get a test database session.
    This fixture creates a new session for each test and rolls back changes at the end.
    It also patches the application's session factory to use this test session.
    """
    # Create a new session factory bound to the test engine
    test_session_factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    
    # Create a session
    async with test_session_factory() as session:
        # Patch the global session factory in db.database
        # This ensures app code uses our test engine/session
        original_factory = database.async_session_factory
        database.async_session_factory = test_session_factory
        
        yield session
        
        # Rollback all changes after test
        await session.rollback()
        
        # Restore original factory
        database.async_session_factory = original_factory

@pytest_asyncio.fixture
async def test_client(test_db_session) -> AsyncGenerator[AsyncClient, None]:
    """Get a test client for the application"""
    # Create a client using the app
    # The dependency override isn't strictly necessary due to the patch in test_db_session,
    # but it's good practice just in case we used Depends(get_db)
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

# ==================== DATA FIXTURES ====================

@pytest.fixture
def sample_pois():
    """Sample POI data"""
    return [
        {
            "id": "WC-Norte-L0-1",
            "name": "Restrooms North Level 0 #1",
            "type": "restroom",
            "num_servers": 8,
            "service_rate": 0.5
        },
        {
            "id": "Food-Sul-1",
            "name": "Food Court South #1",
            "type": "food",
            "num_servers": 4,
            "service_rate": 0.3
        },
        {
            "id": "Store-Este-1",
            "name": "Official Store East",
            "type": "store",
            "num_servers": 2,
            "service_rate": 0.2
        }
    ]

@pytest.fixture
def sample_queue_events():
    """Sample queue events"""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    
    return [
        {
            "poi_id": "WC-Norte-L0-1",
            "event_type": "entry",
            "count": 1,
            "camera_id": "cam-01",
            "timestamp": now - timedelta(minutes=5)
        },
        {
            "poi_id": "WC-Norte-L0-1",
            "event_type": "exit",
            "count": 1,
            "camera_id": "cam-01",
            "timestamp": now - timedelta(minutes=1)
        }
    ]

@pytest.fixture
def mock_map_service_response(sample_pois):
    """Mock response from MapService"""
    return sample_pois[0:2]  # Return first two POIs

@pytest_asyncio.fixture
async def seed_pois(test_db_session, sample_pois):
    """Seed database with POIs"""
    from db.repositories import POIRepository
    repo = POIRepository(test_db_session)
    for poi in sample_pois:
        await repo.insert_poi(poi)
    await test_db_session.commit()
    return sample_pois

@pytest_asyncio.fixture
async def seed_queue_states(test_db_session, seed_pois):
    """Seed database with queue states"""
    from db.repositories import WaitTimeRepository
    repo = WaitTimeRepository(test_db_session)
    
    pois = seed_pois
    for i, poi in enumerate(pois):
        await repo.update_queue_state(
            poi_id=poi['id'],
            arrival_rate=1.0 + i,
            wait_minutes=5.0 * (i + 1),
            confidence_lower=4.0 * (i + 1),
            confidence_upper=6.0 * (i + 1),
            sample_count=10,
            status="medium"
        )
    await test_db_session.commit()
