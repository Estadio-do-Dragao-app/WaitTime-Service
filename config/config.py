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
    
    # MQTT Broker (Mosquitto) - receives queue events from simulator
    MQTT_BROKER_HOST: str = "mosquitto"
    MQTT_BROKER_PORT: int = 1883
    
    # MQTT Topics (matching simulator topics)
    MQTT_TOPIC_QUEUES: str = "stadium/events/queues"          # Subscribe: queue events
    MQTT_TOPIC_ALL_EVENTS: str = "stadium/events/all"         # Subscribe: all events
    MQTT_TOPIC_WAITTIME: str = "stadium/waittime/updates"     # Publish: wait time updates
    
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
