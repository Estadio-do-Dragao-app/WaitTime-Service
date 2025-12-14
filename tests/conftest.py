import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

# Configuração para testes
os.environ['TESTING'] = 'True'
os.environ['LOG_LEVEL'] = 'WARNING'

from app import app
from db.database import Base, get_db

# Fixture para test_client corrigida
@pytest.fixture
async def test_client():
    """Cria um cliente de teste."""
    # Configura banco de dados em memória
    TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    TestingSessionLocal = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Cria tabelas
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    # Cria sessão
    session = TestingSessionLocal()
    
    async def override_get_db():
        try:
            yield session
        finally:
            pass
    
    # Substitui dependência
    app.dependency_overrides[get_db] = override_get_db
    
    # Cria cliente
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
    
    # Limpa
    app.dependency_overrides.clear()
    await session.close()
    await engine.dispose()

# Fixtures de dados (adicione as que faltam)
@pytest.fixture
def sample_pois():
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
    return {
        "pois": [
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
    }

@pytest.fixture
def sample_queue_events():
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

# Fixture de loop de eventos para pytest-asyncio
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()