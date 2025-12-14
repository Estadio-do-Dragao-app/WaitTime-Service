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

# Configurações de teste
os.environ['TESTING'] = 'True'
os.environ['LOG_LEVEL'] = 'WARNING'

# Importa apenas o que está disponível
from app import app
from models import WaitTimeResponse, POIInfo, QueueState
from db.database import Base, get_db
from db.repositories import WaitTimeRepository, POIRepository

# Configuração do banco de dados de teste
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", 
    "sqlite+aiosqlite:///./test.db?check_same_thread=false"
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

@pytest.fixture(scope="session")
async def setup_database():
    """Configura o banco de dados para os testes."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.fixture
async def test_db_session(setup_database):
    """Cria uma sessão de banco de dados fresca para cada teste."""
    async with TestingSessionLocal() as session:
        try:
            yield session
        finally:
            await session.rollback()
            await session.close()

@pytest.fixture
async def test_client(test_db_session):
    """Cria um cliente de teste que usa a sessão de banco de dados de teste."""
    
    # Função mock para substituir get_db
    async def override_get_db():
        try:
            yield test_db_session
        finally:
            pass  # Não feche aqui, deixe a fixture gerenciar
    
    # Substitui a dependência do banco de dados
    app.dependency_overrides[get_db] = override_get_db
    
    # Cria cliente assíncrono
    async with AsyncClient(
        app=app, 
        base_url="http://testserver",
        timeout=30.0
    ) as client:
        yield client
    
    # Limpa as substituições
    app.dependency_overrides.clear()

@pytest.fixture
async def seed_pois(test_db_session):
    """Popula o banco de dados com POIs de teste."""
    from models import POIInfo
    
    test_pois = [
        POIInfo(
            id="WC-Norte-L0-1",
            name="Restrooms North Level 0 #1",
            poi_type="restroom",
            num_servers=8,
            service_rate=0.5,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        ),
        POIInfo(
            id="WC-Sul-L1-1",
            name="Restrooms South Level 1 #1",
            poi_type="restroom",
            num_servers=6,
            service_rate=0.5,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        ),
        POIInfo(
            id="Food-Sul-1",
            name="Food Court South #1",
            poi_type="food",
            num_servers=4,
            service_rate=0.4,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
    ]
    
    for poi in test_pois:
        test_db_session.add(poi)
    
    await test_db_session.commit()
    return test_pois

@pytest.fixture
async def seed_queue_states(test_db_session):
    """Popula o banco de dados com estados de fila de teste."""
    from models import QueueState
    
    test_states = [
        QueueState(
            poi_id="WC-Norte-L0-1",
            arrival_rate=2.5,
            wait_minutes=5.2,
            confidence_lower=4.0,
            confidence_upper=6.5,
            sample_count=50,
            status="medium",
            last_updated=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc)
        ),
        QueueState(
            poi_id="WC-Sul-L1-1",
            arrival_rate=1.8,
            wait_minutes=3.1,
            confidence_lower=2.5,
            confidence_upper=3.8,
            sample_count=40,
            status="low",
            last_updated=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc)
        ),
        QueueState(
            poi_id="Food-Sul-1",
            arrival_rate=3.2,
            wait_minutes=8.7,
            confidence_lower=7.0,
            confidence_upper=10.5,
            sample_count=30,
            status="high",
            last_updated=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc)
        )
    ]
    
    for state in test_states:
        test_db_session.add(state)
    
    await test_db_session.commit()
    return test_states

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
    # Remove todos os dados após cada teste
    try:
        # Remove em ordem para evitar problemas de chave estrangeira
        # Verifica se as tabelas existem antes de tentar limpar
        from sqlalchemy import text
        
        # Tabelas que podem existir
        tables = ['camera_events', 'queue_states', 'poi_info']
        
        for table in tables:
            try:
                await test_db_session.execute(text(f"DELETE FROM {table}"))
            except Exception as e:
                # Tabela pode não existir, apenas continua
                pass
        
        await test_db_session.commit()
    except Exception:
        await test_db_session.rollback()