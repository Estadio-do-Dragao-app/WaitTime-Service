"""
Database connection and session management
"""
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import logging

from config.config import settings

logger = logging.getLogger(__name__)

# SQLAlchemy Base
Base = declarative_base()

# Async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.LOG_LEVEL == "DEBUG",
    pool_pre_ping=True,
    pool_size=20,        # Increased from 5 to handle more concurrent connections
    max_overflow=10      # Allow overflow for peak load
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


async def init_db():
    """Initialize database - create all tables and indices"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # Apply custom indices for performance
        try:
            with open("db/indexes.sql", "r") as f:
                sql_script = f.read()
            # Execute each statement separately (PostgreSQL doesn't support multiple in one execute)
            for statement in sql_script.split(";"):
                statement = statement.strip()
                if statement:
                    await conn.exec_driver_sql(statement)
            logger.info("Database indices created successfully")
        except FileNotFoundError:
            logger.warning("indexes.sql not found - skipping custom indices")
        except Exception as e:
            logger.error(f"Failed to create indices: {e}")
    
    logger.info("Database initialized successfully")


async def close_db():
    """Close database connections"""
    await engine.dispose()
    logger.info("Database connections closed")
