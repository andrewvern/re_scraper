"""Scraping-related API routes."""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime

from ...database.connection import get_db
from ...database.crud import ScrapeJobCRUD, ScrapeResultCRUD
from ...models.scraper_models import (
    ScrapeJobSchema, ScrapeJobCreate, ScrapeJobUpdate,
    ScrapeResultSchema, ScrapingStatus
)
from ...models.property_models import DataSource
from ..auth import get_current_active_user, User

router = APIRouter()


class ScrapeJobRequest(BaseModel):
    """Scrape job request model."""
    data_source: DataSource
    search_criteria: Optional[Dict[str, Any]] = None
    max_pages: int = 10
    max_results: Optional[int] = None


class ScrapeJobsResponse(BaseModel):
    """Scrape jobs response model."""
    jobs: List[ScrapeJobSchema]
    total_count: int
    page: int
    page_size: int


class StartScrapeResponse(BaseModel):
    """Start scrape response model."""
    job_id: str
    status: str
    message: str


async def start_scraping_task(job_id: str, search_criteria: Dict[str, Any], data_source: str):
    """Background task to start scraping.
    
    Args:
        job_id: Scrape job ID
        search_criteria: Search parameters
        data_source: Data source to scrape
    """
    # This would integrate with Celery or other task queue
    # For now, just a placeholder
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Starting scraping task {job_id} for {data_source} with criteria: {search_criteria}")


@router.post("/jobs", response_model=StartScrapeResponse)
async def start_scrape_job(
    job_request: ScrapeJobRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Start a new scraping job.
    
    Args:
        job_request: Scrape job request
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        StartScrapeResponse: Job start response
    """
    # Create scrape job record
    job_create = ScrapeJobCreate(
        data_source=job_request.data_source,
        search_criteria=job_request.search_criteria,
        max_pages=job_request.max_pages,
        max_results=job_request.max_results,
        created_by=current_user.username
    )
    
    scrape_job = ScrapeJobCRUD.create(db, job_create)
    
    # Start background scraping task
    background_tasks.add_task(
        start_scraping_task,
        scrape_job.job_id,
        job_request.search_criteria or {},
        job_request.data_source
    )
    
    return StartScrapeResponse(
        job_id=scrape_job.job_id,
        status=scrape_job.status,
        message="Scraping job started successfully"
    )


@router.get("/jobs", response_model=ScrapeJobsResponse)
async def get_scrape_jobs(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Number of jobs per page"),
    status: Optional[ScrapingStatus] = Query(None, description="Filter by job status"),
    data_source: Optional[DataSource] = Query(None, description="Filter by data source"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get scraping jobs with pagination and filtering.
    
    Args:
        page: Page number
        page_size: Number of jobs per page
        status: Filter by job status
        data_source: Filter by data source
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        ScrapeJobsResponse: Paginated jobs response
    """
    # Get recent jobs (simplified - in production would add filtering)
    all_jobs = ScrapeJobCRUD.get_recent_jobs(db, limit=1000)
    
    # Apply filters
    filtered_jobs = all_jobs
    if status:
        filtered_jobs = [job for job in filtered_jobs if job.status == status]
    if data_source:
        filtered_jobs = [job for job in filtered_jobs if job.data_source == data_source]
    
    # Apply pagination
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paginated_jobs = filtered_jobs[start_idx:end_idx]
    
    # Convert to schemas
    job_schemas = [ScrapeJobSchema.from_orm(job) for job in paginated_jobs]
    
    return ScrapeJobsResponse(
        jobs=job_schemas,
        total_count=len(filtered_jobs),
        page=page,
        page_size=page_size
    )


@router.get("/jobs/{job_id}", response_model=ScrapeJobSchema)
async def get_scrape_job(
    job_id: str = Path(..., description="Scrape job ID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get a specific scraping job.
    
    Args:
        job_id: Scrape job ID
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        ScrapeJobSchema: Scrape job data
        
    Raises:
        HTTPException: If job not found
    """
    scrape_job = ScrapeJobCRUD.get_by_id(db, job_id)
    
    if not scrape_job:
        raise HTTPException(status_code=404, detail="Scrape job not found")
    
    return ScrapeJobSchema.from_orm(scrape_job)


@router.put("/jobs/{job_id}", response_model=ScrapeJobSchema)
async def update_scrape_job(
    job_id: str = Path(..., description="Scrape job ID"),
    job_update: ScrapeJobUpdate = ...,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Update a scraping job.
    
    Args:
        job_id: Scrape job ID
        job_update: Job update data
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        ScrapeJobSchema: Updated job
        
    Raises:
        HTTPException: If job not found
    """
    updated_job = ScrapeJobCRUD.update(db, job_id, job_update)
    
    if not updated_job:
        raise HTTPException(status_code=404, detail="Scrape job not found")
    
    return ScrapeJobSchema.from_orm(updated_job)


@router.post("/jobs/{job_id}/cancel")
async def cancel_scrape_job(
    job_id: str = Path(..., description="Scrape job ID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Cancel a scraping job.
    
    Args:
        job_id: Scrape job ID
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        dict: Success message
        
    Raises:
        HTTPException: If job not found or cannot be cancelled
    """
    scrape_job = ScrapeJobCRUD.get_by_id(db, job_id)
    
    if not scrape_job:
        raise HTTPException(status_code=404, detail="Scrape job not found")
    
    if scrape_job.status in [ScrapingStatus.COMPLETED, ScrapingStatus.CANCELLED, ScrapingStatus.FAILED]:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot cancel job in {scrape_job.status} status"
        )
    
    # Update job status to cancelled
    job_update = ScrapeJobUpdate(status=ScrapingStatus.CANCELLED)
    ScrapeJobCRUD.update(db, job_id, job_update)
    
    return {"message": "Scrape job cancelled successfully"}


@router.get("/jobs/{job_id}/results", response_model=List[ScrapeResultSchema])
async def get_scrape_results(
    job_id: str = Path(..., description="Scrape job ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get results for a scraping job.
    
    Args:
        job_id: Scrape job ID
        limit: Maximum number of results
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        List[ScrapeResultSchema]: Scrape results
        
    Raises:
        HTTPException: If job not found
    """
    # Verify job exists
    scrape_job = ScrapeJobCRUD.get_by_id(db, job_id)
    if not scrape_job:
        raise HTTPException(status_code=404, detail="Scrape job not found")
    
    # Get results
    results = ScrapeResultCRUD.get_by_job_id(db, job_id, limit=limit)
    return [ScrapeResultSchema.from_orm(result) for result in results]


@router.get("/jobs/active", response_model=List[ScrapeJobSchema])
async def get_active_jobs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get all currently active scraping jobs.
    
    Args:
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        List[ScrapeJobSchema]: Active scraping jobs
    """
    active_jobs = ScrapeJobCRUD.get_active_jobs(db)
    return [ScrapeJobSchema.from_orm(job) for job in active_jobs]


@router.get("/jobs/failed/{hours}", response_model=List[ScrapeJobSchema])
async def get_failed_jobs(
    hours: int = Path(..., ge=1, le=168, description="Hours to look back"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get failed scraping jobs within specified time period.
    
    Args:
        hours: Hours to look back
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        List[ScrapeJobSchema]: Failed scraping jobs
    """
    failed_jobs = ScrapeJobCRUD.get_failed_jobs(db, hours=hours)
    return [ScrapeJobSchema.from_orm(job) for job in failed_jobs]


@router.post("/jobs/{job_id}/retry")
async def retry_scrape_job(
    job_id: str = Path(..., description="Scrape job ID"),
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Retry a failed scraping job.
    
    Args:
        job_id: Scrape job ID
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        dict: Success message
        
    Raises:
        HTTPException: If job not found or cannot be retried
    """
    scrape_job = ScrapeJobCRUD.get_by_id(db, job_id)
    
    if not scrape_job:
        raise HTTPException(status_code=404, detail="Scrape job not found")
    
    if scrape_job.status != ScrapingStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry job in {scrape_job.status} status"
        )
    
    # Update job status to retrying
    job_update = ScrapeJobUpdate(
        status=ScrapingStatus.RETRYING,
        error_message=None
    )
    ScrapeJobCRUD.update(db, job_id, job_update)
    
    # Start background scraping task
    background_tasks.add_task(
        start_scraping_task,
        job_id,
        scrape_job.search_criteria or {},
        scrape_job.data_source
    )
    
    return {"message": "Scrape job retry started successfully"}


@router.get("/statistics")
async def get_scraping_statistics(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get scraping statistics.
    
    Args:
        hours: Hours to look back for statistics
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        dict: Scraping statistics
    """
    # Get recent jobs
    recent_jobs = ScrapeJobCRUD.get_recent_jobs(db, limit=1000)
    
    # Calculate statistics
    total_jobs = len(recent_jobs)
    
    status_counts = {}
    for status in ScrapingStatus:
        status_counts[status.value] = sum(1 for job in recent_jobs if job.status == status)
    
    # Data source breakdown
    source_counts = {}
    for source in DataSource:
        source_counts[source.value] = sum(1 for job in recent_jobs if job.data_source == source)
    
    # Calculate success rate
    completed_jobs = status_counts.get(ScrapingStatus.COMPLETED.value, 0)
    failed_jobs = status_counts.get(ScrapingStatus.FAILED.value, 0)
    total_finished = completed_jobs + failed_jobs
    success_rate = (completed_jobs / total_finished * 100) if total_finished > 0 else 0
    
    # Total properties scraped
    total_properties = sum(job.properties_saved for job in recent_jobs if job.properties_saved)
    
    return {
        "total_jobs": total_jobs,
        "status_breakdown": status_counts,
        "data_source_breakdown": source_counts,
        "success_rate": round(success_rate, 2),
        "total_properties_scraped": total_properties,
        "period_hours": hours
    }

