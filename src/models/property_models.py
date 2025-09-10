"""Property data models for real estate scraping."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, JSON, ForeignKey, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from pydantic import BaseModel, Field, validator


Base = declarative_base()


class PropertyType(int, Enum):
    """Property type enumeration using integers for pandas compatibility."""
    HOUSE = 1
    APARTMENT = 2
    CONDO = 3
    TOWNHOUSE = 4
    MULTI_FAMILY = 5
    LAND = 6
    COMMERCIAL = 7
    OTHER = 8


class ListingStatus(str, Enum):
    """Listing status enumeration."""
    ACTIVE = "active"
    PENDING = "pending"
    SOLD = "sold"
    OFF_MARKET = "off_market"
    COMING_SOON = "coming_soon"
    PRICE_REDUCED = "price_reduced"


class DataSource(str, Enum):
    """Data source enumeration."""
    REDFIN = "redfin"
    ZILLOW = "zillow"
    APARTMENTS_COM = "apartments_com"


# SQLAlchemy Models
# Location model removed - consolidated into Property model for pandas compatibility


class Property(Base):
    """Main property model - aligned with pandas schema."""
    
    __tablename__ = "properties"
    
    # Core identifiers matching pandas schema
    property_id = Column(BigInteger, primary_key=True, index=True)  # Changed from 'id' to 'property_id'
    listing_id = Column(BigInteger, nullable=True)  # New field
    mls_id = Column(String(100), nullable=True)  # New field
    
    # Status and pricing
    status = Column(String(50), nullable=True)  # New field
    price = Column(BigInteger, nullable=True)  # Changed to BigInteger for pandas Int64Dtype compatibility
    hoa_fee = Column(String(50), nullable=True)  # New field
    
    # Property measurements
    square_feet = Column(Float, nullable=True)  # Changed to Float
    lot_size = Column(Float, nullable=True)
    bedrooms = Column(Float, nullable=True)  # Changed to Float for pandas compatibility
    bathrooms = Column(Float, nullable=True)
    stories = Column(Float, nullable=True)  # Changed to Float
    year_built = Column(Float, nullable=True)  # Changed to Float
    
    # Location fields - consolidated
    location = Column(String(500), nullable=True)  # New consolidated location field
    address = Column(String(500), nullable=True)  # New field
    city = Column(String(100), nullable=True)  # New field
    state = Column(String(50), nullable=True)  # New field
    zip_code = Column(String(20), nullable=True)  # New field
    country_code = Column(String(10), nullable=True)  # New field
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    # URLs and description
    url = Column(String(500), nullable=True)  # New field
    description = Column(Text, nullable=True)
    
    # Property type as integer
    property_type = Column(Integer, nullable=True)  # Changed to Integer
    
    # Legacy fields for backward compatibility
    external_id = Column(String(100), nullable=True)  # Made nullable for compatibility
    data_source = Column(String(50), nullable=True)  # Made nullable
    
    # Additional features (kept for flexibility)
    features = Column(JSON, nullable=True)
    images = Column(JSON, nullable=True)
    
    # Relationships - updated to use new primary key
    listings = relationship("PropertyListing", back_populates="property")
    metrics = relationship("PropertyMetrics", back_populates="property")
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_scraped = Column(DateTime, default=datetime.utcnow)


class PropertyListing(Base):
    """Property listing information (time-sensitive data)."""
    
    __tablename__ = "property_listings"
    
    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(BigInteger, ForeignKey("properties.property_id"), nullable=False)
    
    # Listing details
    listing_status = Column(String(50), nullable=False)
    list_price = Column(Integer, nullable=True)  # in cents
    listing_date = Column(DateTime, nullable=True)
    days_on_market = Column(Integer, nullable=True)
    price_history = Column(JSON, nullable=True)  # Historical price changes
    
    # Agent/Broker info
    listing_agent = Column(String(255), nullable=True)
    brokerage = Column(String(255), nullable=True)
    
    # URLs and external references
    listing_url = Column(String(500), nullable=True)
    mls_number = Column(String(100), nullable=True)
    
    # Relationships
    property = relationship("Property", back_populates="listings")
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PropertyMetrics(Base):
    """Property market metrics and analytics."""
    
    __tablename__ = "property_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(BigInteger, ForeignKey("properties.property_id"), nullable=False)
    
    # Market metrics
    zestimate = Column(Integer, nullable=True)  # Zillow estimate in cents
    rent_zestimate = Column(Integer, nullable=True)  # Rent estimate in cents
    market_value = Column(Integer, nullable=True)  # Market value in cents
    
    # Neighborhood metrics
    median_home_value = Column(Integer, nullable=True)
    median_rent = Column(Integer, nullable=True)
    price_per_sqft_area = Column(Float, nullable=True)
    
    # Investment metrics
    cap_rate = Column(Float, nullable=True)
    cash_flow = Column(Integer, nullable=True)  # Monthly cash flow in cents
    roi = Column(Float, nullable=True)  # Return on investment percentage
    
    # Market trends
    appreciation_rate = Column(Float, nullable=True)  # Annual appreciation percentage
    rental_yield = Column(Float, nullable=True)
    
    # Additional metrics
    walk_score = Column(Integer, nullable=True)
    school_rating = Column(Float, nullable=True)
    crime_score = Column(Float, nullable=True)
    
    # Relationships
    property = relationship("Property", back_populates="metrics")
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Pydantic Models for API
class PropertySchema(BaseModel):
    """Property schema for API responses - aligned with pandas schema."""
    
    # Core identifiers matching pandas schema
    property_id: Optional[int] = None
    listing_id: Optional[int] = None
    mls_id: Optional[str] = None
    
    # Status and pricing
    status: Optional[str] = None
    price: Optional[int] = None  # Keep as integer for pandas Int64Dtype compatibility
    hoa_fee: Optional[str] = None
    
    # Property measurements
    square_feet: Optional[float] = None
    lot_size: Optional[float] = None
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    stories: Optional[float] = None
    year_built: Optional[float] = None
    
    # Location fields
    location: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # URLs and description
    url: Optional[str] = None
    description: Optional[str] = None
    
    # Property type as integer
    property_type: Optional[int] = None
    
    # Legacy fields for backward compatibility
    external_id: Optional[str] = None
    data_source: Optional[str] = None
    
    # Additional features
    features: Optional[Dict[str, Any]] = None
    images: Optional[List[str]] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_scraped: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class PropertyListingSchema(BaseModel):
    """Property listing schema for API responses."""
    
    id: Optional[int] = None
    property_id: Optional[int] = None
    listing_status: ListingStatus
    list_price: Optional[int] = None
    listing_date: Optional[datetime] = None
    days_on_market: Optional[int] = None
    price_history: Optional[List[Dict[str, Any]]] = None
    listing_agent: Optional[str] = None
    brokerage: Optional[str] = None
    listing_url: Optional[str] = None
    mls_number: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @validator('list_price', pre=True)
    def convert_price_to_dollars(cls, v):
        """Convert price from cents to dollars for API response."""
        if v is not None:
            return v / 100
        return v
    
    class Config:
        from_attributes = True


class PropertyMetricsSchema(BaseModel):
    """Property metrics schema for API responses."""
    
    id: Optional[int] = None
    property_id: Optional[int] = None
    zestimate: Optional[int] = None
    rent_zestimate: Optional[int] = None
    market_value: Optional[int] = None
    median_home_value: Optional[int] = None
    median_rent: Optional[int] = None
    price_per_sqft_area: Optional[float] = None
    cap_rate: Optional[float] = None
    cash_flow: Optional[int] = None
    roi: Optional[float] = None
    appreciation_rate: Optional[float] = None
    rental_yield: Optional[float] = None
    walk_score: Optional[int] = None
    school_rating: Optional[float] = None
    crime_score: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @validator('zestimate', 'rent_zestimate', 'market_value', 'median_home_value', 'median_rent', 'cash_flow', pre=True)
    def convert_price_to_dollars(cls, v):
        """Convert price from cents to dollars for API response."""
        if v is not None:
            return v / 100
        return v
    
    class Config:
        from_attributes = True


# Create/Update schemas
class PropertyCreate(BaseModel):
    """Schema for creating properties - aligned with pandas schema."""
    
    # Core identifiers
    listing_id: Optional[int] = None
    mls_id: Optional[str] = None
    
    # Status and pricing
    status: Optional[str] = None
    price: Optional[int] = None  # Keep as integer for pandas compatibility
    hoa_fee: Optional[str] = None
    
    # Property measurements
    square_feet: Optional[float] = None
    lot_size: Optional[float] = None
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    stories: Optional[float] = None
    year_built: Optional[float] = None
    
    # Location fields
    location: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # URLs and description
    url: Optional[str] = None
    description: Optional[str] = None
    
    # Property type as integer
    property_type: Optional[int] = None
    
    # Legacy fields for backward compatibility
    external_id: Optional[str] = None
    data_source: Optional[str] = None
    
    # Additional features
    features: Optional[Dict[str, Any]] = None
    images: Optional[List[str]] = None


class PropertyUpdate(BaseModel):
    """Schema for updating properties - aligned with pandas schema."""
    
    # Core identifiers
    listing_id: Optional[int] = None
    mls_id: Optional[str] = None
    
    # Status and pricing
    status: Optional[str] = None
    price: Optional[int] = None
    hoa_fee: Optional[str] = None
    
    # Property measurements
    square_feet: Optional[float] = None
    lot_size: Optional[float] = None
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    stories: Optional[float] = None
    year_built: Optional[float] = None
    
    # Location fields
    location: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # URLs and description
    url: Optional[str] = None
    description: Optional[str] = None
    
    # Property type as integer
    property_type: Optional[int] = None
    
    # Legacy fields for backward compatibility
    external_id: Optional[str] = None
    data_source: Optional[str] = None
    
    # Additional features
    features: Optional[Dict[str, Any]] = None
    images: Optional[List[str]] = None

