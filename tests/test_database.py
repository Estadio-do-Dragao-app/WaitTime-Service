"""
Tests for db/database.py - Database connection and session management
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock


class TestGetDb:
    """Tests for get_db context manager"""
    
    @pytest.mark.asyncio
    async def test_get_db_success(self, test_db_session):
        """Test successful database session"""
        # test_db_session fixture already tests basic get_db functionality
        assert test_db_session is not None
    
    @pytest.mark.asyncio
    async def test_get_db_commit_on_success(self):
        """Test that session commits on successful operation"""
        with patch('db.database.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            from db.database import get_db
            
            async with get_db() as session:
                pass  # Successful operation
            
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_db_rollback_on_error(self):
        """Test that session rolls back on error"""
        with patch('db.database.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            from db.database import get_db
            
            with pytest.raises(ValueError):
                async with get_db() as session:
                    raise ValueError("Test error")
            
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()


class TestInitDb:
    """Tests for init_db function"""
    
    @pytest.mark.asyncio
    async def test_init_db_creates_tables(self):
        """Test that init_db creates database tables"""
        with patch('db.database.engine') as mock_engine:
            mock_conn = AsyncMock()
            mock_engine.begin.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_engine.begin.return_value.__aexit__ = AsyncMock(return_value=None)
            
            from db.database import init_db
            
            await init_db()
            
            mock_conn.run_sync.assert_called_once()


class TestCloseDb:
    """Tests for close_db function"""
    
    @pytest.mark.asyncio
    async def test_close_db_disposes_engine(self):
        """Test that close_db disposes the engine"""
        with patch('db.database.engine') as mock_engine:
            mock_engine.dispose = AsyncMock()
            
            from db.database import close_db
            
            await close_db()
            
            mock_engine.dispose.assert_called_once()


class TestDatabaseConfiguration:
    """Tests for database configuration"""
    
    def test_base_is_declarative_base(self):
        """Test that Base is a declarative base"""
        from db.database import Base
        from sqlalchemy.orm import DeclarativeBase
        
        # Check Base has metadata
        assert hasattr(Base, 'metadata')
    
    def test_engine_configuration(self):
        """Test engine is configured"""
        from db.database import engine
        
        assert engine is not None
    
    def test_session_factory_exists(self):
        """Test session factory is configured"""
        from db.database import async_session_factory
        
        assert async_session_factory is not None
