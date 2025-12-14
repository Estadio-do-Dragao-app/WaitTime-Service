# tests/conftest.py
import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ['TESTING'] = 'True'
os.environ['LOG_LEVEL'] = 'CRITICAL'  # Reduz logs durante testes

from app import app
from db.database import Base, get_db
from db.models import POI, QueueState, CameraEvent

# Usa SQLite em memória para testes
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

TestingSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

@pytest.fixture(scope="session")
def event_loop():
    """Event loop para sessão de teste."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session", autouse=True)
async def setup_database():
    """Cria tabelas uma vez por sessão de teste."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    yield
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()

@pytest.fixture(scope="function")
async def db_session():
    """Sessão de banco com rollback automático."""
    async with TestingSessionLocal() as session:
        # Transação aninhada para rollback fácil
        await session.begin_nested()
        
        try:
            yield session
        finally:
            await session.rollback()
            await session.close()

@pytest.fixture(scope="function")
async def client(db_session):
    """Cliente de teste HTTP."""
    async def override_get_db():
        try:
            yield db_session
        finally:
            await db_session.rollback()
    
    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=10.0
    ) as ac:
        yield ac
    
    app.dependency_overrides.clear()

# Fixtures de dados mantidas como antes...
# (seed_pois, seed_queue_states, etc.)

# Adicionar marcadores
def pytest_collection_modifyitems(config, items):
    """Adiciona marcador 'asyncio' automaticamente para testes async."""
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)

def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marca testes de integração")
    config.addinivalue_line("markers", "slow: marca testes lentos")
    config.addinivalue_line("markers", "database: testes que usam banco de dados")