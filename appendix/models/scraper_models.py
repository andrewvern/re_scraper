"""Scraper-related data models."""

from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel

Base = declarative_base()


class ScrapingStatus(str, Enum):
    """Scraping job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ScrapeJob(Base):
    """Scraping job model to track scraping tasks."""
    
    __tablename__ = "scrape_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(100), unique=True, nullable=False)  # Celery task ID
    data_source = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default=ScrapingStatus.PENDING)
    
    # Job configuration
    search_criteria = Column(JSON, nullable=True)  # Search parameters
    max_pages = Column(Integer, default=10)
    max_results = Column(Integer, nullable=True)
    
    # Progress tracking
    pages_scraped = Column(Integer, default=0)
    properties_found = Column(Integer, default=0)
    properties_saved = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    
    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    estimated_duration = Column(Integer, nullable=True)  # in seconds
    
    # Results and errors
    error_message = Column(Text, nullable=True)
    results_summary = Column(JSON, nullable=True)
    
    # Metadata
    created_by = Column(String(100), nullable=True)  # User or system identifier
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScrapeResult(Base):
    """Individual scraping result model."""
    
    __tablename__ = "scrape_results"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(100), nullable=False)  # Reference to ScrapeJob
    
    # Source information
    data_source = Column(String(50), nullable=False)
    source_url = Column(String(500), nullable=False)
    external_id = Column(String(100), nullable=False)
    
    # Raw data
    raw_data = Column(JSON, nullable=False)  # Original scraped data
    processed_data = Column(JSON, nullable=True)  # Cleaned/processed data
    
    # Processing status
    is_processed = Column(Boolean, default=False)
    is_saved_to_db = Column(Boolean, default=False)
    processing_errors = Column(JSON, nullable=True)
    
    # Quality metrics
    data_completeness_score = Column(Integer, nullable=True)  # 0-100
    confidence_score = Column(Integer, nullable=True)  # 0-100
    
    # Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Pydantic Models for API
class ScrapeJobSchema(BaseModel):
    """Scrape job schema for API responses."""
    
    id: Optional[int] = None
    job_id: str
    data_source: str
    status: ScrapingStatus
    search_criteria: Optional[Dict[str, Any]] = None
    max_pages: int = 10
    max_results: Optional[int] = None
    pages_scraped: int = 0
    properties_found: int = 0
    properties_saved: int = 0
    errors_count: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    estimated_duration: Optional[int] = None
    error_message: Optional[str] = None
    results_summary: Optional[Dict[str, Any]] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ScrapeResultSchema(BaseModel):
    """Scrape result schema for API responses."""
    
    id: Optional[int] = None
    job_id: str
    data_source: str
    source_url: str
    external_id: str
    raw_data: Dict[str, Any]
    processed_data: Optional[Dict[str, Any]] = None
    is_processed: bool = False
    is_saved_to_db: bool = False
    processing_errors: Optional[Dict[str, Any]] = None
    data_completeness_score: Optional[int] = None
    confidence_score: Optional[int] = None
    scraped_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ScrapeJobCreate(BaseModel):
    """Schema for creating scrape jobs."""
    
    data_source: str
    search_criteria: Optional[Dict[str, Any]] = None
    max_pages: int = 10
    max_results: Optional[int] = None
    created_by: Optional[str] = None


class ScrapeJobUpdate(BaseModel):
    """Schema for updating scrape jobs."""
    
    status: Optional[ScrapingStatus] = None
    pages_scraped: Optional[int] = None
    properties_found: Optional[int] = None
    properties_saved: Optional[int] = None
    errors_count: Optional[int] = None
    error_message: Optional[str] = None
    results_summary: Optional[Dict[str, Any]] = None

