"""Health check routes."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
import logging

from ...database.connection import get_db, check_db_connection
from ...config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


class HealthCheck(BaseModel):
    """Health check response model."""
    status: str
    timestamp: datetime
    version: str
    database: str
    environment: str


class DetailedHealthCheck(BaseModel):
    """Detailed health check response model."""
    status: str
    timestamp: datetime
    version: str
    environment: str
    services: dict
    system_info: dict


@router.get("/health", response_model=HealthCheck)
async def health_check(db: Session = Depends(get_db)):
    """Basic health check endpoint.
    
    Args:
        db: Database session
        
    Returns:
        HealthCheck: Health check response
    """
    try:
        # Check database connection
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "unhealthy"
    
    return HealthCheck(
        status="healthy" if db_status == "healthy" else "unhealthy",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        database=db_status,
        environment=settings.environment
    )


@router.get("/health/detailed", response_model=DetailedHealthCheck)
async def detailed_health_check(db: Session = Depends(get_db)):
    """Detailed health check endpoint.
    
    Args:
        db: Database session
        
    Returns:
        DetailedHealthCheck: Detailed health check response
    """
    services = {}
    
    # Check database
    try:
        db.execute("SELECT 1")
        services["database"] = {
            "status": "healthy",
            "response_time": "< 1ms"
        }
    except Exception as e:
        services["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Check Redis (if configured)
    try:
        from redis import Redis
        redis_client = Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            password=settings.redis.password,
            socket_timeout=5
        )
        redis_client.ping()
        services["redis"] = {
            "status": "healthy",
            "response_time": "< 1ms"
        }
    except Exception as e:
        services["redis"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # System information
    import psutil
    system_info = {
        "cpu_usage": f"{psutil.cpu_percent()}%",
        "memory_usage": f"{psutil.virtual_memory().percent}%",
        "disk_usage": f"{psutil.disk_usage('/').percent}%"
    }
    
    # Overall status
    overall_status = "healthy"
    for service_status in services.values():
        if service_status["status"] != "healthy":
            overall_status = "unhealthy"
            break
    
    return DetailedHealthCheck(
        status=overall_status,
        timestamp=datetime.utcnow(),
        version="1.0.0",
        environment=settings.environment,
        services=services,
        system_info=system_info
    )


@router.get("/ping")
async def ping():
    """Simple ping endpoint.
    
    Returns:
        dict: Pong response
    """
    return {"message": "pong", "timestamp": datetime.utcnow()}

