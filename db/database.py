"""
Database connection and session management
"""
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import logging
import asyncio
from config.config import settings

logger = logging.getLogger(__name__)

# SQLAlchemy Base
Base = declarative_base()

# Async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.LOG_LEVEL == "DEBUG",
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=10
)

# Session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

@asynccontextmanager
async def get_db():
    """
    Async context manager for database sessions

    Usage:
        async with get_db() as db:
            result = await db.execute(...)
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("Database error")
            raise
        finally:
            await session.close()

async def get_db_session():
    """Generator for FastAPI Depends"""
    async with get_db() as session:
        yield session

def _read_sql_file(path: str) -> str:
    """Read SQL file synchronously (called via executor to avoid blocking event loop)"""
    with open(path, "r") as f:
        return f.read()

async def init_db():
    """Initialize database - create all tables and indices"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        try:
            loop = asyncio.get_event_loop()
            sql_script = await loop.run_in_executor(None, _read_sql_file, "db/indexes.sql")
            for statement in sql_script.split(";"):
                statement = statement.strip()
                if statement:
                    await conn.exec_driver_sql(statement)
            logger.info("Database indices created successfully")
        except FileNotFoundError:
            logger.warning("indexes.sql not found - skipping custom indices")
        except Exception:
            logger.exception("Failed to create indices")  # FIX: exception() em vez de error()

    logger.info("Database initialized successfully")

async def close_db():
    """Close database connections"""
    await engine.dispose()
    logger.info("Database connections closed")
