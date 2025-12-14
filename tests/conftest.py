import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os
import sys

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configuração para testes
os.environ['TESTING'] = 'True'
os.environ['LOG_LEVEL'] = 'WARNING'

from app import app
from db.database import Base, get_db
from db.models import POI, QueueState, CameraEvent

# Configuração do banco de dados de teste
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", 
    "sqlite+aiosqlite:///:memory:"
)

# Cria engine de teste
test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestingSessionLocal = sessionmaker(
    test_engine, class_=AsyncSession, expire_on_commit=False
)

@pytest.fixture(scope="session")
def event_loop():
    """Cria uma instância do event loop para a sessão de teste."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function")
async def setup_database():
    """Configura o banco de dados para cada função de teste."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.fixture(scope="function")
async def test_db_session(setup_database):
    """Cria uma sessão de banco de dados fresca para cada teste."""
    async with TestingSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

@pytest.fixture(scope="function")
async def test_client(test_db_session):
    """Cria um cliente de teste que usa a sessão de banco de dados de teste."""
    
    async def override_get_db():
        try:
            yield test_db_session
        finally:
            await test_db_session.close()
    
    # Substitui a dependência do banco de dados
    app.dependency_overrides[get_db] = override_get_db
    
    # Cria cliente assíncrono - RETORNA O OBJETO, NÃO UM GENERATOR!
    async with AsyncClient(
        app=app, 
        base_url="http://testserver",
        timeout=30.0
    ) as client:
        yield client  # Isto retorna o objeto AsyncClient
    
    # Limpa as substituições
    app.dependency_overrides.clear()

@pytest.fixture(scope="function")
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
    
    await test_db_session.commit()
    return test_pois_data

@pytest.fixture(scope="function")
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
    
    await test_db_session.commit()
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

@pytest.fixture(autouse=True)
async def cleanup_database(test_db_session):
    """Limpa o banco de dados após cada teste."""
    yield
    try:
        # Remove em ordem para evitar problemas de chave estrangeira
        await test_db_session.execute("DELETE FROM camera_events")
        await test_db_session.execute("DELETE FROM queue_states")
        await test_db_session.execute("DELETE FROM pois")
        await test_db_session.commit()
    except Exception:
        await test_db_session.rollback()