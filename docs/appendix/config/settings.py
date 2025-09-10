"""Configuration settings for the Real Estate Scraper Backend."""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class DatabaseSettings(BaseSettings):
    """Database configuration."""
    
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    username: str = Field(default="postgres", env="DB_USERNAME")
    password: str = Field(default="password", env="DB_PASSWORD")
    database: str = Field(default="real_estate_scraper", env="DB_DATABASE")
    
    model_config = {"extra": "ignore"}
    
    @property
    def database_url(self) -> str:
        """Construct database URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class ScraperSettings(BaseSettings):
    """Scraper configuration."""
    
    # Rate limiting
    requests_per_minute: int = Field(default=60, env="REQUESTS_PER_MINUTE")
    delay_between_requests: float = Field(default=2.0, env="DELAY_BETWEEN_REQUESTS")
    
    # Proxy settings
    use_proxy: bool = Field(default=True, env="USE_PROXY")
    proxy_list: Optional[List[str]] = Field(default=None, env="PROXY_LIST")
    
    # User agent rotation
    rotate_user_agents: bool = Field(default=True, env="ROTATE_USER_AGENTS")
    
    # Browser settings for Selenium
    headless_browser: bool = Field(default=True, env="HEADLESS_BROWSER")
    browser_timeout: int = Field(default=30, env="BROWSER_TIMEOUT")
    
    # Anti-detection measures
    random_delays: bool = Field(default=True, env="RANDOM_DELAYS")
    min_delay: float = Field(default=1.0, env="MIN_DELAY")
    max_delay: float = Field(default=5.0, env="MAX_DELAY")
    
    model_config = {"extra": "ignore"}


class APISettings(BaseSettings):
    """API configuration."""
    
    host: str = Field(default="0.0.0.0", env="API_HOST")
    port: int = Field(default=8000, env="API_PORT")
    debug: bool = Field(default=False, env="API_DEBUG")
    
    # Authentication
    secret_key: str = Field(default="your-secret-key-change-this", env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    
    # Rate limiting
    rate_limit_per_minute: int = Field(default=100, env="RATE_LIMIT_PER_MINUTE")
    
    model_config = {"extra": "ignore"}


class RedisSettings(BaseSettings):
    """Redis configuration for Celery."""
    
    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    db: int = Field(default=0, env="REDIS_DB")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    
    model_config = {"extra": "ignore"}
    
    @property
    def redis_url(self) -> str:
        """Construct Redis URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class Settings(BaseSettings):
    """Main application settings."""
    
    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")
    
    # Monitoring
    sentry_dsn: Optional[str] = Field(default=None, env="SENTRY_DSN")
    
    # Component settings
    database: DatabaseSettings = DatabaseSettings()
    scraper: ScraperSettings = ScraperSettings()
    api: APISettings = APISettings()
    redis: RedisSettings = RedisSettings()
    
    model_config = {"extra": "ignore", "env_file": ".env", "env_file_encoding": "utf-8"}


# Global settings instance
settings = Settings()

