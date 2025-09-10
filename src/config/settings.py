"""Configuration settings for the Real Estate Scraper."""

from typing import List, Optional, Dict
from pydantic_settings import BaseSettings
from pydantic import Field


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


class ETLSettings(BaseSettings):
    """ETL configuration."""
    
    # Output settings
    output_dir: str = Field(default="./data", env="OUTPUT_DIR")
    csv_filename: str = Field(default="properties.csv", env="CSV_FILENAME")
    
    # Data processing
    deduplicate_data: bool = Field(default=True, env="DEDUPLICATE_DATA")
    validate_data: bool = Field(default=True, env="VALIDATE_DATA")
    
    # Export settings
    export_format: str = Field(default="csv", env="EXPORT_FORMAT")
    csv_encoding: str = Field(default="utf-8", env="CSV_ENCODING")
    
    model_config = {"extra": "ignore"}


class Settings(BaseSettings):
    """Main application settings."""
    
    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")
    
    # Component settings
    scraper: ScraperSettings = ScraperSettings()
    etl: ETLSettings = ETLSettings()
    
    model_config = {"extra": "ignore", "env_file": ".env", "env_file_encoding": "utf-8"}


# Global settings instance
settings = Settings()

