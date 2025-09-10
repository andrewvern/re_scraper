"""Logging configuration and setup."""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
import structlog
from datetime import datetime

from ..config import settings


def setup_logging(log_file: Optional[str] = None, log_level: str = "INFO") -> None:
    """Set up structured logging for the application.
    
    Args:
        log_file: Optional log file path
        log_level: Logging level
    """
    # Use settings if parameters not provided
    if log_file is None:
        log_file = settings.log_file
    if log_level == "INFO":
        log_level = settings.log_level
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler with structured output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Console formatter
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if log file specified
    if log_file:
        # Create log directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        # File formatter (JSON for structured logging)
        file_formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "logger": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    # Sentry integration if configured
    if settings.sentry_dsn:
        try:
            import sentry_sdk
            from sentry_sdk.integrations.logging import LoggingIntegration
            from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
            
            sentry_logging = LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR
            )
            
            sentry_sdk.init(
                dsn=settings.sentry_dsn,
                integrations=[sentry_logging, SqlalchemyIntegration()],
                traces_sample_rate=0.1,
                environment=settings.environment
            )
            
            logging.info("Sentry logging integration initialized")
            
        except ImportError:
            logging.warning("Sentry SDK not installed, skipping Sentry integration")
    
    logging.info(f"Logging configured - Level: {log_level}, File: {log_file}")


class ScrapingLogger:
    """Specialized logger for scraping operations."""
    
    def __init__(self, scraper_name: str, job_id: Optional[str] = None):
        """Initialize scraping logger.
        
        Args:
            scraper_name: Name of the scraper
            job_id: Optional job ID for tracking
        """
        self.scraper_name = scraper_name
        self.job_id = job_id
        self.logger = structlog.get_logger(f"scraper.{scraper_name}")
        
        # Bind context
        if job_id:
            self.logger = self.logger.bind(job_id=job_id)
    
    def log_scrape_start(self, search_criteria: dict, max_pages: int):
        """Log start of scraping operation.
        
        Args:
            search_criteria: Search parameters
            max_pages: Maximum pages to scrape
        """
        self.logger.info(
            "Scraping started",
            search_criteria=search_criteria,
            max_pages=max_pages,
            scraper=self.scraper_name
        )
    
    def log_page_scraped(self, page_num: int, properties_found: int, url: str):
        """Log completion of a page scrape.
        
        Args:
            page_num: Page number
            properties_found: Number of properties found
            url: URL that was scraped
        """
        self.logger.info(
            "Page scraped",
            page_num=page_num,
            properties_found=properties_found,
            url=url,
            scraper=self.scraper_name
        )
    
    def log_property_processed(self, external_id: str, success: bool, errors: list = None):
        """Log processing of a single property.
        
        Args:
            external_id: External property ID
            success: Whether processing was successful
            errors: List of errors if any
        """
        if success:
            self.logger.debug(
                "Property processed successfully",
                external_id=external_id,
                scraper=self.scraper_name
            )
        else:
            self.logger.warning(
                "Property processing failed",
                external_id=external_id,
                errors=errors,
                scraper=self.scraper_name
            )
    
    def log_scrape_complete(self, total_pages: int, total_properties: int, 
                           processing_time: float, errors: int = 0):
        """Log completion of scraping operation.
        
        Args:
            total_pages: Total pages scraped
            total_properties: Total properties found
            processing_time: Total processing time in seconds
            errors: Number of errors encountered
        """
        self.logger.info(
            "Scraping completed",
            total_pages=total_pages,
            total_properties=total_properties,
            processing_time=processing_time,
            errors=errors,
            scraper=self.scraper_name
        )
    
    def log_rate_limit(self, retry_after: int):
        """Log rate limiting event.
        
        Args:
            retry_after: Seconds to wait before retry
        """
        self.logger.warning(
            "Rate limit encountered",
            retry_after=retry_after,
            scraper=self.scraper_name
        )
    
    def log_error(self, error: Exception, context: dict = None):
        """Log an error with context.
        
        Args:
            error: Exception that occurred
            context: Additional context information
        """
        self.logger.error(
            "Scraping error",
            error=str(error),
            error_type=type(error).__name__,
            context=context or {},
            scraper=self.scraper_name,
            exc_info=True
        )


class ETLLogger:
    """Specialized logger for ETL operations."""
    
    def __init__(self, process_name: str, batch_id: Optional[str] = None):
        """Initialize ETL logger.
        
        Args:
            process_name: Name of the ETL process
            batch_id: Optional batch ID for tracking
        """
        self.process_name = process_name
        self.batch_id = batch_id
        self.logger = structlog.get_logger(f"etl.{process_name}")
        
        if batch_id:
            self.logger = self.logger.bind(batch_id=batch_id)
    
    def log_batch_start(self, record_count: int, source: str):
        """Log start of batch processing.
        
        Args:
            record_count: Number of records in batch
            source: Source of the data
        """
        self.logger.info(
            "ETL batch started",
            record_count=record_count,
            source=source,
            process=self.process_name
        )
    
    def log_validation_results(self, total: int, valid: int, invalid: int, errors: list):
        """Log validation results.
        
        Args:
            total: Total records
            valid: Valid records
            invalid: Invalid records
            errors: List of validation errors
        """
        self.logger.info(
            "Validation completed",
            total_records=total,
            valid_records=valid,
            invalid_records=invalid,
            validation_rate=valid/total if total > 0 else 0,
            error_count=len(errors),
            process=self.process_name
        )
        
        if errors:
            self.logger.debug("Validation errors", errors=errors[:10])  # Log first 10 errors
    
    def log_transformation_results(self, input_count: int, output_count: int, 
                                 processing_time: float):
        """Log transformation results.
        
        Args:
            input_count: Number of input records
            output_count: Number of output records
            processing_time: Processing time in seconds
        """
        self.logger.info(
            "Transformation completed",
            input_records=input_count,
            output_records=output_count,
            processing_time=processing_time,
            process=self.process_name
        )
    
    def log_deduplication_results(self, input_count: int, unique_count: int, 
                                duplicates: int):
        """Log deduplication results.
        
        Args:
            input_count: Number of input records
            unique_count: Number of unique records
            duplicates: Number of duplicates found
        """
        duplication_rate = duplicates / input_count if input_count > 0 else 0
        
        self.logger.info(
            "Deduplication completed",
            input_records=input_count,
            unique_records=unique_count,
            duplicates=duplicates,
            duplication_rate=duplication_rate,
            process=self.process_name
        )
    
    def log_load_results(self, records_saved: int, errors: int, processing_time: float):
        """Log data loading results.
        
        Args:
            records_saved: Number of records saved
            errors: Number of errors
            processing_time: Processing time in seconds
        """
        self.logger.info(
            "Data loading completed",
            records_saved=records_saved,
            errors=errors,
            processing_time=processing_time,
            process=self.process_name
        )
    
    def log_batch_complete(self, total_time: float, success: bool, summary: dict):
        """Log completion of batch processing.
        
        Args:
            total_time: Total processing time
            success: Whether batch was successful
            summary: Summary statistics
        """
        self.logger.info(
            "ETL batch completed",
            total_time=total_time,
            success=success,
            summary=summary,
            process=self.process_name
        )


class APILogger:
    """Specialized logger for API operations."""
    
    def __init__(self):
        """Initialize API logger."""
        self.logger = structlog.get_logger("api")
    
    def log_request(self, method: str, path: str, user: str = None, 
                   query_params: dict = None):
        """Log API request.
        
        Args:
            method: HTTP method
            path: Request path
            user: Username if authenticated
            query_params: Query parameters
        """
        self.logger.info(
            "API request",
            method=method,
            path=path,
            user=user,
            query_params=query_params
        )
    
    def log_response(self, method: str, path: str, status_code: int, 
                    response_time: float, user: str = None):
        """Log API response.
        
        Args:
            method: HTTP method
            path: Request path
            status_code: HTTP status code
            response_time: Response time in seconds
            user: Username if authenticated
        """
        self.logger.info(
            "API response",
            method=method,
            path=path,
            status_code=status_code,
            response_time=response_time,
            user=user
        )
    
    def log_error(self, method: str, path: str, error: Exception, 
                 user: str = None, request_data: dict = None):
        """Log API error.
        
        Args:
            method: HTTP method
            path: Request path
            error: Exception that occurred
            user: Username if authenticated
            request_data: Request data for debugging
        """
        self.logger.error(
            "API error",
            method=method,
            path=path,
            error=str(error),
            error_type=type(error).__name__,
            user=user,
            request_data=request_data,
            exc_info=True
        )
