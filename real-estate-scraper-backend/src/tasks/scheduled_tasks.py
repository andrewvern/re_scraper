"""Scheduled Celery tasks for automated operations."""

from typing import Dict, Any, List
from datetime import datetime, timedelta
import logging

from .celery import celery_app
from .scraping_tasks import scrape_properties
from ..database.connection import SessionLocal
from ..database.crud import PropertyCRUD, ScrapeJobCRUD, ScrapeResultCRUD
from ..models.scraper_models import ScrapeJobCreate
from ..monitoring.logger import ETLLogger
from ..monitoring.metrics import metrics, update_system_metrics

logger = logging.getLogger(__name__)


@celery_app.task(name="src.tasks.scheduled_tasks.daily_scraping_job")
def daily_scraping_job(data_source: str, search_criteria: Dict[str, Any] = None) -> Dict[str, Any]:
    """Daily scheduled scraping job for a data source.
    
    Args:
        data_source: Data source to scrape
        search_criteria: Search parameters
        
    Returns:
        Dict[str, Any]: Job results
    """
    logger.info(f"Starting daily scraping job for {data_source}")
    
    # Default search criteria if none provided
    if not search_criteria:
        search_criteria = {
            "location": "San Francisco, CA",
            "max_results": 1000
        }
    
    # Database session
    db = SessionLocal()
    
    try:
        # Create scrape job record
        job_create = ScrapeJobCreate(
            data_source=data_source,
            search_criteria=search_criteria,
            max_pages=20,
            max_results=search_criteria.get('max_results', 1000),
            created_by="scheduled_task"
        )
        
        scrape_job = ScrapeJobCRUD.create(db, job_create)
        
        # Start scraping task
        result = scrape_properties.delay(
            data_source=data_source,
            search_criteria=search_criteria,
            job_id=scrape_job.job_id,
            max_pages=20
        )
        
        logger.info(f"Daily scraping job started for {data_source}, job_id: {scrape_job.job_id}")
        
        return {
            'status': 'started',
            'job_id': scrape_job.job_id,
            'task_id': result.id,
            'data_source': data_source,
            'search_criteria': search_criteria
        }
        
    except Exception as e:
        logger.error(f"Error starting daily scraping job for {data_source}: {e}")
        raise
        
    finally:
        db.close()


@celery_app.task(name="src.tasks.scheduled_tasks.update_stale_properties")
def update_stale_properties(hours: int = 24, max_properties: int = 100) -> Dict[str, Any]:
    """Update properties that haven't been scraped recently.
    
    Args:
        hours: Hours since last scrape
        max_properties: Maximum number of properties to update
        
    Returns:
        Dict[str, Any]: Update results
    """
    logger.info(f"Updating stale properties (older than {hours} hours)")
    
    db = SessionLocal()
    
    try:
        # Get stale properties
        stale_properties = PropertyCRUD.get_stale_properties(db, hours)
        
        if not stale_properties:
            logger.info("No stale properties found")
            return {
                'status': 'completed',
                'properties_found': 0,
                'properties_updated': 0
            }
        
        # Limit to max_properties
        properties_to_update = stale_properties[:max_properties]
        
        logger.info(f"Found {len(stale_properties)} stale properties, updating {len(properties_to_update)}")
        
        # Group by data source for efficient scraping
        properties_by_source = {}
        for prop in properties_to_update:
            source = prop.data_source
            if source not in properties_by_source:
                properties_by_source[source] = []
            properties_by_source[source].append(prop)
        
        update_tasks = []
        
        # Start update tasks for each data source
        for data_source, properties in properties_by_source.items():
            # Create search criteria based on property locations
            cities = list(set([prop.location.city for prop in properties if prop.location]))
            
            if cities:
                search_criteria = {
                    "location": cities[0],  # Use first city as primary location
                    "max_results": len(properties) * 2  # Get more to ensure we capture updates
                }
                
                # Start scraping task
                task = scrape_properties.delay(
                    data_source=data_source,
                    search_criteria=search_criteria,
                    max_pages=5
                )
                update_tasks.append(task.id)
        
        return {
            'status': 'started',
            'properties_found': len(stale_properties),
            'properties_to_update': len(properties_to_update),
            'update_tasks': update_tasks,
            'data_sources': list(properties_by_source.keys())
        }
        
    except Exception as e:
        logger.error(f"Error updating stale properties: {e}")
        raise
        
    finally:
        db.close()


@celery_app.task(name="src.tasks.scheduled_tasks.cleanup_old_data")
def cleanup_old_data(days_to_keep: int = 90) -> Dict[str, Any]:
    """Clean up old scraping data and results.
    
    Args:
        days_to_keep: Number of days of data to keep
        
    Returns:
        Dict[str, Any]: Cleanup results
    """
    logger.info(f"Cleaning up data older than {days_to_keep} days")
    
    etl_logger = ETLLogger("cleanup", "scheduled")
    db = SessionLocal()
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Clean up old scrape results
        old_results = db.query(ScrapeResultCRUD.__class__).filter(
            ScrapeResultCRUD.created_at < cutoff_date
        ).all()
        
        results_deleted = 0
        for result in old_results:
            try:
                db.delete(result)
                results_deleted += 1
            except Exception as e:
                logger.warning(f"Error deleting scrape result {result.id}: {e}")
        
        # Clean up old scrape jobs (keep the job records but clean up detailed results)
        old_jobs = db.query(ScrapeJobCRUD.__class__).filter(
            ScrapeJobCRUD.created_at < cutoff_date
        ).all()
        
        jobs_cleaned = 0
        for job in old_jobs:
            try:
                # Clear detailed results but keep job record for statistics
                job.results_summary = None
                job.search_criteria = None
                jobs_cleaned += 1
            except Exception as e:
                logger.warning(f"Error cleaning scrape job {job.id}: {e}")
        
        # Commit changes
        db.commit()
        
        # Log cleanup results
        cleanup_results = {
            'scrape_results_deleted': results_deleted,
            'scrape_jobs_cleaned': jobs_cleaned,
            'cutoff_date': cutoff_date.isoformat(),
            'days_kept': days_to_keep
        }
        
        etl_logger.log_batch_complete(
            total_time=0,  # Quick operation
            success=True,
            summary=cleanup_results
        )
        
        logger.info(f"Cleanup completed: {cleanup_results}")
        
        return {
            'status': 'completed',
            **cleanup_results
        }
        
    except Exception as e:
        logger.error(f"Error during data cleanup: {e}")
        etl_logger.log_batch_complete(0, False, {'error': str(e)})
        raise
        
    finally:
        db.close()


@celery_app.task(name="src.tasks.scheduled_tasks.collect_daily_metrics")
def collect_daily_metrics() -> Dict[str, Any]:
    """Collect and log daily metrics summary.
    
    Returns:
        Dict[str, Any]: Metrics summary
    """
    logger.info("Collecting daily metrics")
    
    db = SessionLocal()
    
    try:
        # Update system metrics
        update_system_metrics()
        
        # Get current metrics
        current_metrics = metrics.get_all_metrics()
        
        # Calculate daily statistics
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        
        # Get jobs from last 24 hours
        recent_jobs = ScrapeJobCRUD.get_recent_jobs(db, limit=1000)
        today_jobs = [job for job in recent_jobs if job.created_at.date() == today]
        yesterday_jobs = [job for job in recent_jobs if job.created_at.date() == yesterday]
        
        # Calculate metrics
        daily_summary = {
            'date': today.isoformat(),
            'jobs_today': len(today_jobs),
            'jobs_yesterday': len(yesterday_jobs),
            'completed_today': len([j for j in today_jobs if j.status == 'completed']),
            'failed_today': len([j for j in today_jobs if j.status == 'failed']),
            'properties_scraped_today': sum([j.properties_saved or 0 for j in today_jobs]),
            'system_metrics': {
                'cpu_usage': current_metrics.get('gauges', {}).get('system.cpu.usage', 0),
                'memory_usage': current_metrics.get('gauges', {}).get('system.memory.usage', 0),
                'disk_usage': current_metrics.get('gauges', {}).get('system.disk.usage', 0)
            },
            'api_metrics': {
                'total_requests': current_metrics.get('counters', {}).get('api.requests', 0),
                'error_requests': current_metrics.get('counters', {}).get('api.requests.error', 0),
                'avg_response_time': current_metrics.get('histograms', {}).get('api.request.duration', {}).get('mean', 0)
            }
        }
        
        # Store metrics (you could save to database or send to external service)
        logger.info(f"Daily metrics summary: {daily_summary}")
        
        # Set daily metrics as gauges
        metrics.set_gauge("daily.jobs.total", daily_summary['jobs_today'], {"date": today.isoformat()})
        metrics.set_gauge("daily.jobs.completed", daily_summary['completed_today'], {"date": today.isoformat()})
        metrics.set_gauge("daily.jobs.failed", daily_summary['failed_today'], {"date": today.isoformat()})
        metrics.set_gauge("daily.properties.scraped", daily_summary['properties_scraped_today'], {"date": today.isoformat()})
        
        return {
            'status': 'completed',
            'daily_summary': daily_summary
        }
        
    except Exception as e:
        logger.error(f"Error collecting daily metrics: {e}")
        raise
        
    finally:
        db.close()


@celery_app.task(name="src.tasks.scheduled_tasks.health_check")
def health_check() -> Dict[str, Any]:
    """Periodic health check task.
    
    Returns:
        Dict[str, Any]: Health status
    """
    logger.info("Running scheduled health check")
    
    try:
        # Check database connection
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        database_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        database_status = "unhealthy"
    
    # Check Redis connection
    try:
        from redis import Redis
        from ..config import settings
        
        redis_client = Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            password=settings.redis.password,
            socket_timeout=5
        )
        redis_client.ping()
        redis_status = "healthy"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "unhealthy"
    
    # Update system metrics
    update_system_metrics()
    
    health_status = {
        'timestamp': datetime.utcnow().isoformat(),
        'database': database_status,
        'redis': redis_status,
        'overall': "healthy" if database_status == "healthy" and redis_status == "healthy" else "unhealthy"
    }
    
    # Set health metrics
    metrics.set_gauge("health.database", 1 if database_status == "healthy" else 0)
    metrics.set_gauge("health.redis", 1 if redis_status == "healthy" else 0)
    metrics.set_gauge("health.overall", 1 if health_status['overall'] == "healthy" else 0)
    
    logger.info(f"Health check completed: {health_status}")
    
    return {
        'status': 'completed',
        'health': health_status
    }
