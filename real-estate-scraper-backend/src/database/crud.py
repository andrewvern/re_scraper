"""CRUD operations for database models."""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc
from datetime import datetime, timedelta
import logging

from ..models.property_models import (
    Property, Location, PropertyListing, PropertyMetrics,
    PropertyCreate, PropertyUpdate, LocationSchema
)
from ..models.scraper_models import (
    ScrapeJob, ScrapeResult, ScrapingStatus,
    ScrapeJobCreate, ScrapeJobUpdate
)

logger = logging.getLogger(__name__)


class LocationCRUD:
    """CRUD operations for Location model."""
    
    @staticmethod
    def get_by_id(db: Session, location_id: int) -> Optional[Location]:
        """Get location by ID."""
        return db.query(Location).filter(Location.id == location_id).first()
    
    @staticmethod
    def get_by_address(db: Session, street_address: str, city: str, state: str, zip_code: str) -> Optional[Location]:
        """Get location by address components."""
        return db.query(Location).filter(
            and_(
                Location.street_address == street_address,
                Location.city == city,
                Location.state == state,
                Location.zip_code == zip_code
            )
        ).first()
    
    @staticmethod
    def create(db: Session, location_data: LocationSchema) -> Location:
        """Create a new location."""
        # Check if location already exists
        existing = LocationCRUD.get_by_address(
            db, 
            location_data.street_address,
            location_data.city,
            location_data.state,
            location_data.zip_code
        )
        
        if existing:
            return existing
        
        db_location = Location(**location_data.dict())
        db.add(db_location)
        db.commit()
        db.refresh(db_location)
        return db_location
    
    @staticmethod
    def search_by_area(db: Session, city: str, state: str, limit: int = 100) -> List[Location]:
        """Search locations by city and state."""
        return db.query(Location).filter(
            and_(
                Location.city.ilike(f"%{city}%"),
                Location.state.ilike(f"%{state}%")
            )
        ).limit(limit).all()


class PropertyCRUD:
    """CRUD operations for Property model."""
    
    @staticmethod
    def get_by_id(db: Session, property_id: int) -> Optional[Property]:
        """Get property by ID."""
        return db.query(Property).filter(Property.id == property_id).first()
    
    @staticmethod
    def get_by_external_id(db: Session, external_id: str, data_source: str) -> Optional[Property]:
        """Get property by external ID and data source."""
        return db.query(Property).filter(
            and_(
                Property.external_id == external_id,
                Property.data_source == data_source
            )
        ).first()
    
    @staticmethod
    def create(db: Session, property_data: PropertyCreate) -> Property:
        """Create a new property."""
        # Create location first
        location_data = property_data.location
        db_location = LocationCRUD.create(db, location_data)
        
        # Create property
        property_dict = property_data.dict(exclude={'location'})
        property_dict['location_id'] = db_location.id
        
        db_property = Property(**property_dict)
        db.add(db_property)
        db.commit()
        db.refresh(db_property)
        return db_property
    
    @staticmethod
    def update(db: Session, property_id: int, property_data: PropertyUpdate) -> Optional[Property]:
        """Update a property."""
        db_property = PropertyCRUD.get_by_id(db, property_id)
        if not db_property:
            return None
        
        update_data = property_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_property, key, value)
        
        db_property.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(db_property)
        return db_property
    
    @staticmethod
    def delete(db: Session, property_id: int) -> bool:
        """Delete a property."""
        db_property = PropertyCRUD.get_by_id(db, property_id)
        if not db_property:
            return False
        
        db.delete(db_property)
        db.commit()
        return True
    
    @staticmethod
    def search(
        db: Session,
        data_source: Optional[str] = None,
        property_type: Optional[str] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_bedrooms: Optional[int] = None,
        max_bedrooms: Optional[int] = None,
        min_bathrooms: Optional[float] = None,
        max_bathrooms: Optional[float] = None,
        min_sqft: Optional[int] = None,
        max_sqft: Optional[int] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip_code: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc"
    ) -> List[Property]:
        """Search properties with filters."""
        query = db.query(Property).join(Location)
        
        # Apply filters
        if data_source:
            query = query.filter(Property.data_source == data_source)
        
        if property_type:
            query = query.filter(Property.property_type == property_type)
        
        if min_price is not None:
            query = query.filter(Property.price >= min_price)
        
        if max_price is not None:
            query = query.filter(Property.price <= max_price)
        
        if min_bedrooms is not None:
            query = query.filter(Property.bedrooms >= min_bedrooms)
        
        if max_bedrooms is not None:
            query = query.filter(Property.bedrooms <= max_bedrooms)
        
        if min_bathrooms is not None:
            query = query.filter(Property.bathrooms >= min_bathrooms)
        
        if max_bathrooms is not None:
            query = query.filter(Property.bathrooms <= max_bathrooms)
        
        if min_sqft is not None:
            query = query.filter(Property.square_feet >= min_sqft)
        
        if max_sqft is not None:
            query = query.filter(Property.square_feet <= max_sqft)
        
        if city:
            query = query.filter(Location.city.ilike(f"%{city}%"))
        
        if state:
            query = query.filter(Location.state.ilike(f"%{state}%"))
        
        if zip_code:
            query = query.filter(Location.zip_code == zip_code)
        
        # Apply ordering
        if hasattr(Property, order_by):
            order_column = getattr(Property, order_by)
        elif hasattr(Location, order_by):
            order_column = getattr(Location, order_by)
        else:
            order_column = Property.created_at
        
        if order_direction.lower() == "desc":
            query = query.order_by(desc(order_column))
        else:
            query = query.order_by(asc(order_column))
        
        return query.offset(offset).limit(limit).all()
    
    @staticmethod
    def get_stale_properties(db: Session, hours: int = 24) -> List[Property]:
        """Get properties that haven't been scraped recently."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return db.query(Property).filter(
            Property.last_scraped < cutoff_time
        ).all()


class ListingCRUD:
    """CRUD operations for PropertyListing model."""
    
    @staticmethod
    def get_by_property_id(db: Session, property_id: int) -> List[PropertyListing]:
        """Get all listings for a property."""
        return db.query(PropertyListing).filter(
            PropertyListing.property_id == property_id
        ).order_by(desc(PropertyListing.created_at)).all()
    
    @staticmethod
    def get_latest_by_property_id(db: Session, property_id: int) -> Optional[PropertyListing]:
        """Get the latest listing for a property."""
        return db.query(PropertyListing).filter(
            PropertyListing.property_id == property_id
        ).order_by(desc(PropertyListing.created_at)).first()
    
    @staticmethod
    def create(db: Session, listing_data: Dict[str, Any]) -> PropertyListing:
        """Create a new property listing."""
        db_listing = PropertyListing(**listing_data)
        db.add(db_listing)
        db.commit()
        db.refresh(db_listing)
        return db_listing
    
    @staticmethod
    def update_price_history(db: Session, property_id: int, new_price: int) -> None:
        """Update price history for a property."""
        latest_listing = ListingCRUD.get_latest_by_property_id(db, property_id)
        
        if latest_listing and latest_listing.list_price != new_price:
            # Create price history entry
            price_change = {
                'date': datetime.utcnow().isoformat(),
                'old_price': latest_listing.list_price,
                'new_price': new_price,
                'change_amount': new_price - (latest_listing.list_price or 0),
                'change_percent': ((new_price - (latest_listing.list_price or 0)) / (latest_listing.list_price or 1)) * 100
            }
            
            # Update price history
            price_history = latest_listing.price_history or []
            price_history.append(price_change)
            latest_listing.price_history = price_history
            
            db.commit()


class MetricsCRUD:
    """CRUD operations for PropertyMetrics model."""
    
    @staticmethod
    def get_by_property_id(db: Session, property_id: int) -> Optional[PropertyMetrics]:
        """Get metrics for a property."""
        return db.query(PropertyMetrics).filter(
            PropertyMetrics.property_id == property_id
        ).first()
    
    @staticmethod
    def create_or_update(db: Session, property_id: int, metrics_data: Dict[str, Any]) -> PropertyMetrics:
        """Create or update property metrics."""
        existing = MetricsCRUD.get_by_property_id(db, property_id)
        
        if existing:
            # Update existing metrics
            for key, value in metrics_data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            existing.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(existing)
            return existing
        else:
            # Create new metrics
            metrics_data['property_id'] = property_id
            db_metrics = PropertyMetrics(**metrics_data)
            db.add(db_metrics)
            db.commit()
            db.refresh(db_metrics)
            return db_metrics


class ScrapeJobCRUD:
    """CRUD operations for ScrapeJob model."""
    
    @staticmethod
    def get_by_id(db: Session, job_id: str) -> Optional[ScrapeJob]:
        """Get scrape job by ID."""
        return db.query(ScrapeJob).filter(ScrapeJob.job_id == job_id).first()
    
    @staticmethod
    def create(db: Session, job_data: ScrapeJobCreate) -> ScrapeJob:
        """Create a new scrape job."""
        import uuid
        job_dict = job_data.dict()
        job_dict['job_id'] = str(uuid.uuid4())
        
        db_job = ScrapeJob(**job_dict)
        db.add(db_job)
        db.commit()
        db.refresh(db_job)
        return db_job
    
    @staticmethod
    def update(db: Session, job_id: str, job_data: ScrapeJobUpdate) -> Optional[ScrapeJob]:
        """Update a scrape job."""
        db_job = ScrapeJobCRUD.get_by_id(db, job_id)
        if not db_job:
            return None
        
        update_data = job_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_job, key, value)
        
        db_job.updated_at = datetime.utcnow()
        
        # Set timestamps based on status
        if job_data.status == ScrapingStatus.RUNNING and not db_job.started_at:
            db_job.started_at = datetime.utcnow()
        elif job_data.status in [ScrapingStatus.COMPLETED, ScrapingStatus.FAILED, ScrapingStatus.CANCELLED]:
            if not db_job.completed_at:
                db_job.completed_at = datetime.utcnow()
        
        db.commit()
        db.refresh(db_job)
        return db_job
    
    @staticmethod
    def get_recent_jobs(db: Session, limit: int = 50) -> List[ScrapeJob]:
        """Get recent scrape jobs."""
        return db.query(ScrapeJob).order_by(
            desc(ScrapeJob.created_at)
        ).limit(limit).all()
    
    @staticmethod
    def get_active_jobs(db: Session) -> List[ScrapeJob]:
        """Get currently active scrape jobs."""
        return db.query(ScrapeJob).filter(
            ScrapeJob.status.in_([ScrapingStatus.PENDING, ScrapingStatus.RUNNING, ScrapingStatus.RETRYING])
        ).all()
    
    @staticmethod
    def get_failed_jobs(db: Session, hours: int = 24) -> List[ScrapeJob]:
        """Get failed jobs within specified hours."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return db.query(ScrapeJob).filter(
            and_(
                ScrapeJob.status == ScrapingStatus.FAILED,
                ScrapeJob.created_at >= cutoff_time
            )
        ).all()


class ScrapeResultCRUD:
    """CRUD operations for ScrapeResult model."""
    
    @staticmethod
    def create(db: Session, result_data: Dict[str, Any]) -> ScrapeResult:
        """Create a new scrape result."""
        db_result = ScrapeResult(**result_data)
        db.add(db_result)
        db.commit()
        db.refresh(db_result)
        return db_result
    
    @staticmethod
    def get_by_job_id(db: Session, job_id: str, limit: int = 1000) -> List[ScrapeResult]:
        """Get scrape results for a job."""
        return db.query(ScrapeResult).filter(
            ScrapeResult.job_id == job_id
        ).limit(limit).all()
    
    @staticmethod
    def get_unprocessed(db: Session, limit: int = 100) -> List[ScrapeResult]:
        """Get unprocessed scrape results."""
        return db.query(ScrapeResult).filter(
            ScrapeResult.is_processed == False
        ).limit(limit).all()
    
    @staticmethod
    def mark_processed(db: Session, result_id: int, processed_data: Optional[Dict[str, Any]] = None) -> bool:
        """Mark a scrape result as processed."""
        db_result = db.query(ScrapeResult).filter(ScrapeResult.id == result_id).first()
        if not db_result:
            return False
        
        db_result.is_processed = True
        db_result.processed_at = datetime.utcnow()
        
        if processed_data:
            db_result.processed_data = processed_data
        
        db.commit()
        return True
    
    @staticmethod
    def mark_saved(db: Session, result_id: int) -> bool:
        """Mark a scrape result as saved to database."""
        db_result = db.query(ScrapeResult).filter(ScrapeResult.id == result_id).first()
        if not db_result:
            return False
        
        db_result.is_saved_to_db = True
        db.commit()
        return True
