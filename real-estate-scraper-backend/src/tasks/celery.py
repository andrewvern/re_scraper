"""Celery configuration and setup."""

from celery import Celery
from celery.schedules import crontab
import logging

from ..config import settings

logger = logging.getLogger(__name__)

# Create Celery app
celery_app = Celery(
    "real_estate_scraper",
    broker=settings.redis.redis_url,
    backend=settings.redis.redis_url,
    include=[
        "src.tasks.scraping_tasks",
        "src.tasks.scheduled_tasks"
    ]
)

# Celery configuration
celery_app.conf.update(
    # Task routing
    task_routes={
        "src.tasks.scraping_tasks.*": {"queue": "scraping"},
        "src.tasks.scheduled_tasks.*": {"queue": "scheduled"},
    },
    
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    
    # Result backend settings
    result_expires=3600,  # 1 hour
    result_backend_max_retries=10,
    result_backend_retry_delay=1,
    
    # Worker settings
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=False,
    
    # Task execution
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    
    # Monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
    
    # Beat schedule
    beat_schedule={
        # Daily scraping jobs
        "daily-redfin-scraping": {
            "task": "src.tasks.scheduled_tasks.daily_scraping_job",
            "schedule": crontab(hour=2, minute=0),  # 2 AM daily
            "args": ("redfin",),
            "kwargs": {
                "search_criteria": {
                    "location": "San Francisco, CA",
                    "max_results": 1000
                }
            }
        },
        
        "daily-zillow-scraping": {
            "task": "src.tasks.scheduled_tasks.daily_scraping_job",
            "schedule": crontab(hour=3, minute=0),  # 3 AM daily
            "args": ("zillow",),
            "kwargs": {
                "search_criteria": {
                    "location": "San Francisco, CA",
                    "max_results": 1000
                }
            }
        },
        
        "daily-apartments-scraping": {
            "task": "src.tasks.scheduled_tasks.daily_scraping_job",
            "schedule": crontab(hour=4, minute=0),  # 4 AM daily
            "args": ("apartments_com",),
            "kwargs": {
                "search_criteria": {
                    "location": "San Francisco, CA",
                    "max_results": 1000
                }
            }
        },
        
        # Weekly data cleanup
        "weekly-data-cleanup": {
            "task": "src.tasks.scheduled_tasks.cleanup_old_data",
            "schedule": crontab(hour=1, minute=0, day_of_week=0),  # Sunday 1 AM
            "kwargs": {"days_to_keep": 90}
        },
        
        # Hourly stale property updates
        "hourly-stale-property-updates": {
            "task": "src.tasks.scheduled_tasks.update_stale_properties",
            "schedule": crontab(minute=0),  # Every hour
            "kwargs": {"max_properties": 100}
        },
        
        # Daily metrics collection
        "daily-metrics-collection": {
            "task": "src.tasks.scheduled_tasks.collect_daily_metrics",
            "schedule": crontab(hour=23, minute=30),  # 11:30 PM daily
        },
    },
    
    beat_schedule_filename="celerybeat-schedule"
)


# Error handling
@celery_app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    print(f"Request: {self.request!r}")
    return "Debug task completed"


# Task failure handler
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def reliable_task(self, *args, **kwargs):
    """Base task with automatic retry logic."""
    try:
        # Task logic here
        return "Task completed successfully"
    except Exception as exc:
        logger.error(f"Task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


# Celery signals
@celery_app.task(bind=True)
def on_task_failure(self, task_id, error, traceback):
    """Handle task failures."""
    logger.error(f"Task {task_id} failed: {error}")
    # Could send alerts here


# Configure logging for Celery
@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Set up periodic tasks after Celery configuration."""
    logger.info("Celery periodic tasks configured")


# Worker ready signal
@celery_app.on_after_finalize.connect
def setup_celery_logging(sender, **kwargs):
    """Set up logging when Celery is ready."""
    from ..monitoring.logger import setup_logging
    setup_logging()
    logger.info("Celery worker initialized")


if __name__ == "__main__":
    celery_app.start()
