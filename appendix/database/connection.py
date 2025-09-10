"""Database connection and session management."""

from typing import Generator
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import logging

from ..config import settings

logger = logging.getLogger(__name__)

# Create database engine
engine = create_engine(
    settings.database.database_url,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=settings.api.debug  # Enable SQL logging in debug mode
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """Get database session.
    
    Yields:
        Session: Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Initialize database with all tables."""
    try:
        # Import all models to ensure they are registered
        from ..models.property_models import Base as PropertyBase
        from ..models.scraper_models import Base as ScraperBase
        
        # Create all tables
        PropertyBase.metadata.create_all(bind=engine)
        ScraperBase.metadata.create_all(bind=engine)
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise


def drop_all_tables() -> None:
    """Drop all tables. Use with caution!"""
    try:
        # Import all models
        from ..models.property_models import Base as PropertyBase
        from ..models.scraper_models import Base as ScraperBase
        
        # Drop all tables
        PropertyBase.metadata.drop_all(bind=engine)
        ScraperBase.metadata.drop_all(bind=engine)
        
        logger.warning("All database tables dropped")
        
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise


def create_indexes() -> None:
    """Create additional database indexes for performance."""
    try:
        with engine.connect() as conn:
            # Indexes for properties table
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_properties_data_source 
                ON properties(data_source);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_properties_property_type 
                ON properties(property_type);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_properties_price 
                ON properties(price) WHERE price IS NOT NULL;
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_properties_bedrooms 
                ON properties(bedrooms) WHERE bedrooms IS NOT NULL;
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_properties_last_scraped 
                ON properties(last_scraped);
            """)
            
            # Indexes for locations table
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_locations_city_state 
                ON locations(city, state);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_locations_zip_code 
                ON locations(zip_code);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_locations_coordinates 
                ON locations(latitude, longitude) 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
            """)
            
            # Indexes for property listings
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_property_listings_status 
                ON property_listings(listing_status);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_property_listings_price 
                ON property_listings(list_price) WHERE list_price IS NOT NULL;
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_property_listings_date 
                ON property_listings(listing_date) WHERE listing_date IS NOT NULL;
            """)
            
            # Indexes for scrape jobs
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_jobs_status 
                ON scrape_jobs(status);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_jobs_data_source 
                ON scrape_jobs(data_source);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_jobs_created_at 
                ON scrape_jobs(created_at);
            """)
            
            # Indexes for scrape results
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_results_job_id 
                ON scrape_results(job_id);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_results_external_id 
                ON scrape_results(external_id);
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scrape_results_processed 
                ON scrape_results(is_processed, is_saved_to_db);
            """)
            
            conn.commit()
            
        logger.info("Database indexes created successfully")
        
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
        raise


def check_db_connection() -> bool:
    """Check if database connection is working.
    
    Returns:
        bool: True if connection is working, False otherwise
    """
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

