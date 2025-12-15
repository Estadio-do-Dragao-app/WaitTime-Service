# tests/conftest.py
import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ['TESTING'] = 'True'
os.environ['LOG_LEVEL'] = 'CRITICAL'

from app import app
from sqlalchemy.pool import StaticPool
from db.database import Base, get_db
from db.models import POI, QueueState, CameraEvent

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    poolclass=StaticPool,
)

TestingSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

from unittest.mock import MagicMock, patch, AsyncMock
"""
@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()
"""

@pytest.fixture(scope="session", autouse=True)
def event_loop_policy():
    """Cleanup event loop policy to prevent hanging."""
    import asyncio
    policy = asyncio.get_event_loop_policy()
    yield
    try:
        loop = policy.get_event_loop()
        if loop.is_running():
            loop.stop()
        loop.close()
    except:
        pass

@pytest.fixture(scope="session", autouse=True)
def mock_app_dependencies():
    """
    Mock external dependencies used in app lifespan to prevent 
    production DB/Service connection attempts during testing.
    Autouse ensures this runs for the entire session.
    """
    with patch("app.init_db", new_callable=AsyncMock) as mock_init, \
         patch("app.close_db", new_callable=AsyncMock) as mock_close, \
         patch("app.MapServiceClient", new_callable=MagicMock) as mock_map_service, \
         patch("app.EventConsumer", new_callable=MagicMock) as mock_consumer:
        
        # Setup mocks
        mock_init.return_value = None
        mock_close.return_value = None
        
        # Mock MapService fetch_pois to return empty list or sample data to avoid error logs
        mock_map_instance = mock_map_service.return_value
        mock_map_instance.fetch_pois = AsyncMock(return_value=[])
        
        # Mock EventConsumer start/stop
        mock_consumer_instance = mock_consumer.return_value
        mock_consumer_instance.start = AsyncMock(return_value=None)
        mock_consumer_instance.stop = AsyncMock(return_value=None)
        mock_consumer_instance.running = True
        mock_consumer_instance.smoothers = {}
        mock_consumer_instance.queue_models = {}

        yield


@pytest_asyncio.fixture(scope="function")
async def test_db_session():
    """Sessão de banco para cada teste (cria e remove tabelas)."""
    # Create tables for this test
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with TestingSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

    # Drop tables after test
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest_asyncio.fixture(scope="function")
async def test_client(test_db_session):
    """Cliente de teste HTTP."""
    async def override_get_db():
        try:
            yield test_db_session
        finally:
            await test_db_session.rollback()
    
    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=10.0
    ) as ac:
        yield ac
    
    app.dependency_overrides.clear()

# Manter as fixtures originais que seus testes esperam:
@pytest_asyncio.fixture(scope="function")
async def seed_pois(test_db_session):
    """Popula o banco de dados com POIs de teste."""
    test_pois_data = [
        {
            "id": "WC-Norte-L0-1",
            "name": "Restrooms North Level 0 #1",
            "poi_type": "restroom",
            "num_servers": 8,
            "service_rate": 0.5
        },
        {
            "id": "WC-Sul-L1-1",
            "name": "Restrooms South Level 1 #1",
            "poi_type": "restroom",
            "num_servers": 6,
            "service_rate": 0.5
        },
        {
            "id": "Food-Sul-1",
            "name": "Food Court South #1",
            "poi_type": "food",
            "num_servers": 4,
            "service_rate": 0.4
        }
    ]
    
    for poi_data in test_pois_data:
        poi = POI(
            id=poi_data["id"],
            name=poi_data["name"],
            poi_type=poi_data["poi_type"],
            num_servers=poi_data["num_servers"],
            service_rate=poi_data["service_rate"]
        )
        test_db_session.add(poi)
    
    await test_db_session.flush()
    return test_pois_data

@pytest_asyncio.fixture(scope="function")
async def seed_queue_states(test_db_session):
    """Popula o banco de dados com estados de fila de teste."""
    test_states = [
        {
            "poi_id": "WC-Norte-L0-1",
            "arrival_rate": 2.5,
            "current_wait_minutes": 5.2,
            "confidence_lower": 4.0,
            "confidence_upper": 6.5,
            "sample_count": 50,
            "status": "medium"
        },
        {
            "poi_id": "WC-Sul-L1-1",
            "arrival_rate": 1.8,
            "current_wait_minutes": 3.1,
            "confidence_lower": 2.5,
            "confidence_upper": 3.8,
            "sample_count": 40,
            "status": "low"
        },
        {
            "poi_id": "Food-Sul-1",
            "arrival_rate": 3.2,
            "current_wait_minutes": 8.7,
            "confidence_lower": 7.0,
            "confidence_upper": 10.5,
            "sample_count": 30,
            "status": "high"
        }
    ]
    
    for state_data in test_states:
        state = QueueState(
            poi_id=state_data["poi_id"],
            arrival_rate=state_data["arrival_rate"],
            current_wait_minutes=state_data["current_wait_minutes"],
            confidence_lower=state_data["confidence_lower"],
            confidence_upper=state_data["confidence_upper"],
            sample_count=state_data["sample_count"],
            status=state_data["status"],
            last_updated=datetime.now(timezone.utc)
        )
        test_db_session.add(state)
    
    await test_db_session.flush()
    return test_states

@pytest.fixture
def sample_pois():
    """Fornece dados de POI de exemplo para teste."""
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
            "service_rate": 0.4
        },
        {
            "id": "Store-Este-1",
            "name": "Store East #1",
            "type": "store",
            "num_servers": 2,
            "service_rate": 0.3
        }
    ]

@pytest.fixture
def mock_map_service_response():
    """Fornece resposta mockada do MapService."""
    return [
        {
            "id": "WC-Norte-L0-1",
            "name": "Restrooms North Level 0 #1",
            "type": "restroom",
            "num_servers": 8,
            "service_rate": 0.5,
            "location": {"lat": 40.7128, "lng": -74.0060}
        },
        {
            "id": "Food-Sul-1",
            "name": "Food Court South #1",
            "type": "food",
            "num_servers": 4,
            "service_rate": 0.4,
            "location": {"lat": 40.7128, "lng": -74.0060}
        }
    ]

@pytest.fixture
def sample_queue_events():
    """Fornece eventos de fila de exemplo para teste."""
    base_time = datetime.now(timezone.utc)
    return [
        {
            "poi_id": "WC-Norte-L0-1",
            "event_type": "entry",
            "count": 2,
            "camera_id": "cam-123",
            "timestamp": base_time - timedelta(minutes=2)
        },
        {
            "poi_id": "WC-Norte-L0-1",
            "event_type": "exit",
            "count": 1,
            "camera_id": "cam-123",
            "timestamp": base_time - timedelta(minutes=1)
        },
        {
            "poi_id": "WC-Sul-L1-1",
            "event_type": "entry",
            "count": 3,
            "camera_id": "cam-456",
            "timestamp": base_time - timedelta(minutes=3)
        }
    ]

def pytest_configure(config):
    """Registra marcadores personalizados."""
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "slow: marca testes lentos"
    )
