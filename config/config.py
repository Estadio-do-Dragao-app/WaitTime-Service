# import os
# from dotenv import load_dotenv

# load_dotenv()

# class Config:
#     SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI') or 'postgresql://localhost/estadio_do_dragao'
    
#     # Additional optional configurations
#     SQLALCHEMY_TRACK_MODIFICATIONS = False
#     DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'


"""
Configuration for Wait Time Service
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings from environment variables"""
    
    # Service info
    SERVICE_NAME: str = "waittime-service"
    SERVICE_PORT: int = 8001
    
    # PostgreSQL Database
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "stadium_waittime"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    # Downstream Broker (receives queue_events from camera processors)
    DOWNSTREAM_BROKER_HOST: str = "localhost"
    DOWNSTREAM_BROKER_PORT: int = 5672
    DOWNSTREAM_BROKER_USER: str = "guest"
    DOWNSTREAM_BROKER_PASSWORD: str = "guest"
    DOWNSTREAM_EXCHANGE: str = "stadium.events"
    QUEUE_EVENTS_QUEUE: str = "queue_events"
    
    @property
    def DOWNSTREAM_BROKER_URL(self) -> str:
        return (
            f"amqp://{self.DOWNSTREAM_BROKER_USER}:{self.DOWNSTREAM_BROKER_PASSWORD}"
            f"@{self.DOWNSTREAM_BROKER_HOST}:{self.DOWNSTREAM_BROKER_PORT}/"
        )
    
    # Upstream Broker (publishes waittime_updates for clients)
    UPSTREAM_BROKER_HOST: str = "localhost"
    UPSTREAM_BROKER_PORT: int = 5672
    UPSTREAM_BROKER_USER: str = "guest"
    UPSTREAM_BROKER_PASSWORD: str = "guest"
    UPSTREAM_EXCHANGE: str = "stadium.updates"
    
    @property
    def UPSTREAM_BROKER_URL(self) -> str:
        return (
            f"amqp://{self.UPSTREAM_BROKER_USER}:{self.UPSTREAM_BROKER_PASSWORD}"
            f"@{self.UPSTREAM_BROKER_HOST}:{self.UPSTREAM_BROKER_PORT}/"
        )
    
    # Queue modeling parameters
    ARRIVAL_RATE_WINDOW_MINUTES: int = 5
    EMA_ALPHA: float = 0.3  # Smoothing factor for arrival rates
    SIGNIFICANT_CHANGE_THRESHOLD: float = 15.0  # Percent change to trigger publish
    
    # External Services
    MAP_SERVICE_URL: str = "http://mapservice:8000"
    MAP_SERVICE_TIMEOUT: int = 10
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
