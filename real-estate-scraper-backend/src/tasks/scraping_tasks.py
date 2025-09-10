"""Celery tasks for web scraping operations."""

from typing import Dict, Any, List
from celery import current_task
import logging
from datetime import datetime

from .celery import celery_app
from ..scrapers import RedfinScraper, ZillowScraper, ApartmentsScraper
from ..etl.data_processor import DataProcessor
from ..database.connection import SessionLocal
from ..database.crud import ScrapeJobCRUD, ScrapeResultCRUD
from ..models.scraper_models import ScrapeJobUpdate, ScrapingStatus
from ..models.property_models import DataSource
from ..monitoring.logger import ScrapingLogger
from ..monitoring.metrics import track_scraping_metrics

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="src.tasks.scraping_tasks.scrape_properties")
@track_scraping_metrics("general")
def scrape_properties(self, data_source: str, search_criteria: Dict[str, Any], 
                     job_id: str = None, max_pages: int = 10) -> Dict[str, Any]:
    """Scrape properties from a data source.
    
    Args:
        self: Celery task instance
        data_source: Data source to scrape
        search_criteria: Search parameters
        job_id: Optional job ID for tracking
        max_pages: Maximum pages to scrape
        
    Returns:
        Dict[str, Any]: Scraping results
    """
    task_id = self.request.id
    scraping_logger = ScrapingLogger(data_source, job_id or task_id)
    
    # Database session
    db = SessionLocal()
    
    try:
        # Update job status to running
        if job_id:
            job_update = ScrapeJobUpdate(
                status=ScrapingStatus.RUNNING,
                pages_scraped=0,
                properties_found=0
            )
            ScrapeJobCRUD.update(db, job_id, job_update)
        
        scraping_logger.log_scrape_start(search_criteria, max_pages)
        
        # Initialize scraper
        scraper = _get_scraper(data_source)
        if not scraper:
            raise ValueError(f"Unknown data source: {data_source}")
        
        scraped_properties = []
        total_pages = 0
        errors = 0
        start_time = datetime.utcnow()
        
        # Add max_results to search criteria if not present
        if 'max_results' not in search_criteria:
            search_criteria['max_results'] = max_pages * 50  # Estimate
        
        with scraper:
            try:
                for i, property_data in enumerate(scraper.search_properties(search_criteria)):
                    if i % 50 == 0:  # Log every 50 properties
                        total_pages = (i // 50) + 1
                        scraping_logger.log_page_scraped(total_pages, 50, "search_results")
                        
                        # Update job progress
                        if job_id:
                            job_update = ScrapeJobUpdate(
                                pages_scraped=total_pages,
                                properties_found=i + 1
                            )
                            ScrapeJobCRUD.update(db, job_id, job_update)
                        
                        # Update task progress
                        self.update_state(
                            state='PROGRESS',
                            meta={
                                'current': i + 1,
                                'total': search_criteria.get('max_results', 1000),
                                'status': f'Scraped {i + 1} properties'
                            }
                        )
                    
                    try:
                        # Store raw scrape result
                        result_data = {
                            'job_id': job_id or task_id,
                            'data_source': data_source,
                            'source_url': property_data.get('listing_url', ''),
                            'external_id': property_data.get('external_id', ''),
                            'raw_data': property_data
                        }
                        
                        ScrapeResultCRUD.create(db, result_data)
                        scraped_properties.append(property_data)
                        
                        scraping_logger.log_property_processed(
                            property_data.get('external_id', 'unknown'),
                            True
                        )
                        
                    except Exception as e:
                        errors += 1
                        scraping_logger.log_property_processed(
                            property_data.get('external_id', 'unknown'),
                            False,
                            [str(e)]
                        )
                        
                        if errors > 10:  # Stop if too many consecutive errors
                            logger.warning(f"Too many errors ({errors}), stopping scraping")
                            break
                
            except Exception as e:
                scraping_logger.log_error(e, {"search_criteria": search_criteria})
                raise
        
        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        # Update job status to completed
        results_summary = {
            'total_properties': len(scraped_properties),
            'total_pages': total_pages,
            'processing_time': processing_time,
            'errors': errors
        }
        
        if job_id:
            job_update = ScrapeJobUpdate(
                status=ScrapingStatus.COMPLETED,
                properties_saved=len(scraped_properties),
                errors_count=errors,
                results_summary=results_summary
            )
            ScrapeJobCRUD.update(db, job_id, job_update)
        
        scraping_logger.log_scrape_complete(
            total_pages, len(scraped_properties), processing_time, errors
        )
        
        # Trigger data processing
        if scraped_properties:
            process_scraped_data.delay(job_id or task_id, scraped_properties)
        
        return {
            'status': 'completed',
            'properties_scraped': len(scraped_properties),
            'pages_scraped': total_pages,
            'processing_time': processing_time,
            'errors': errors,
            'job_id': job_id or task_id
        }
        
    except Exception as e:
        # Update job status to failed
        if job_id:
            job_update = ScrapeJobUpdate(
                status=ScrapingStatus.FAILED,
                error_message=str(e)
            )
            ScrapeJobCRUD.update(db, job_id, job_update)
        
        scraping_logger.log_error(e)
        raise
        
    finally:
        db.close()


@celery_app.task(bind=True, name="src.tasks.scraping_tasks.process_scraped_data")
def process_scraped_data(self, job_id: str, scraped_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process scraped data through ETL pipeline.
    
    Args:
        self: Celery task instance
        job_id: Job ID for tracking
        scraped_data: List of scraped property data
        
    Returns:
        Dict[str, Any]: Processing results
    """
    task_id = self.request.id
    logger.info(f"Processing {len(scraped_data)} scraped properties for job {job_id}")
    
    # Database session
    db = SessionLocal()
    
    try:
        # Initialize data processor
        processor = DataProcessor(db)
        
        # Process the data
        self.update_state(
            state='PROGRESS',
            meta={
                'current': 0,
                'total': len(scraped_data),
                'status': 'Starting data processing'
            }
        )
        
        results = processor.process_scraped_data(scraped_data, job_id)
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current': len(scraped_data),
                'total': len(scraped_data),
                'status': 'Data processing completed'
            }
        )
        
        logger.info(f"Data processing completed for job {job_id}: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error processing scraped data for job {job_id}: {e}")
        raise
        
    finally:
        db.close()


@celery_app.task(bind=True, name="src.tasks.scraping_tasks.scrape_property_details")
def scrape_property_details(self, data_source: str, property_url: str) -> Dict[str, Any]:
    """Scrape detailed information for a specific property.
    
    Args:
        self: Celery task instance
        data_source: Data source
        property_url: URL of the property page
        
    Returns:
        Dict[str, Any]: Property details
    """
    scraper = _get_scraper(data_source)
    if not scraper:
        raise ValueError(f"Unknown data source: {data_source}")
    
    try:
        with scraper:
            property_details = scraper.get_property_details(property_url)
            return property_details
            
    except Exception as e:
        logger.error(f"Error scraping property details from {property_url}: {e}")
        raise


def _get_scraper(data_source: str):
    """Get scraper instance for data source.
    
    Args:
        data_source: Data source name
        
    Returns:
        BaseScraper: Scraper instance
    """
    scraper_map = {
        DataSource.REDFIN: RedfinScraper,
        DataSource.ZILLOW: ZillowScraper,
        DataSource.APARTMENTS_COM: ApartmentsScraper,
        # String variants for flexibility
        "redfin": RedfinScraper,
        "zillow": ZillowScraper,
        "apartments_com": ApartmentsScraper,
        "apartments.com": ApartmentsScraper,
    }
    
    scraper_class = scraper_map.get(data_source)
    if scraper_class:
        return scraper_class()
    
    return None
