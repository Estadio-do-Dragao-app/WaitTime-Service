"""
Configuration for Wait Time Service
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    POSTGRES_PASSWORD: str = "postgres"  # NOSONAR - overridden by env var in production
    
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    # ==================== DOWNSTREAM BROKER (MQTT) ====================
    # Receives events FROM simulator
    DOWNSTREAM_BROKER_HOST: str = "mosquitto-downstream"
    DOWNSTREAM_BROKER_PORT: int = 1883
    
    # Topics to subscribe (from simulator)
    DOWNSTREAM_TOPIC_QUEUES: str = "stadium/events/queues"
    DOWNSTREAM_TOPIC_ALL: str = "stadium/events/all"
    
    # ==================== UPSTREAM BROKER (MQTT) ====================
    # Publishes wait times TO clients/apps
    UPSTREAM_BROKER_HOST: str = "mosquitto-upstream"
    UPSTREAM_BROKER_PORT: int = 1883
    
    # Topic prefix for publishing
    UPSTREAM_TOPIC_PREFIX: str = "stadium/waittime"
    
    # ==================== QUEUE MODEL PARAMETERS ====================
    ARRIVAL_RATE_WINDOW_MINUTES: int = 5
    EMA_ALPHA: float = 0.3
    SIGNIFICANT_CHANGE_THRESHOLD: float = 15.0
    
    # ==================== EXTERNAL SERVICES ====================
    MAP_SERVICE_URL: str = "http://mapservice:8000"  # NOSONAR
    MAP_SERVICE_TIMEOUT: int = 10
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    # ==================== MQTT SECURITY ====================
    # Credentials for the 'services' account created by generate_certs.sh
    MQTT_USER: str = "services"
    MQTT_PASS: str = "dragao_mqtt_2026"
    # Path to Root CA cert inside the container (empty = plaintext fallback)
    MQTT_CA_CERT: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )


settings = Settings()
