"""Property data models for real estate scraping."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


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


class PropertyModel(BaseModel):
    """Main property model - aligned with pandas schema."""
    
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
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    last_scraped: Optional[datetime] = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class PropertyListingModel(BaseModel):
    """Property listing information (time-sensitive data)."""
    
    id: Optional[int] = None
    property_id: Optional[int] = None
    listing_status: ListingStatus
    list_price: Optional[int] = None  # in cents
    listing_date: Optional[datetime] = None
    days_on_market: Optional[int] = None
    price_history: Optional[List[Dict[str, Any]]] = None  # Historical price changes
    listing_agent: Optional[str] = None
    brokerage: Optional[str] = None
    listing_url: Optional[str] = None
    mls_number: Optional[str] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class PropertyMetricsModel(BaseModel):
    """Property market metrics and analytics."""
    
    id: Optional[int] = None
    property_id: Optional[int] = None
    
    # Market metrics
    zestimate: Optional[int] = None  # Zillow estimate in cents
    rent_zestimate: Optional[int] = None  # Rent estimate in cents
    market_value: Optional[int] = None  # Market value in cents
    
    # Neighborhood metrics
    median_home_value: Optional[int] = None
    median_rent: Optional[int] = None
    price_per_sqft_area: Optional[float] = None
    
    # Investment metrics
    cap_rate: Optional[float] = None
    cash_flow: Optional[int] = None  # Monthly cash flow in cents
    roi: Optional[float] = None  # Return on investment percentage
    
    # Market trends
    appreciation_rate: Optional[float] = None  # Annual appreciation percentage
    rental_yield: Optional[float] = None
    
    # Additional metrics
    walk_score: Optional[int] = None
    school_rating: Optional[float] = None
    crime_score: Optional[float] = None
    
    # Metadata
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


"""
=== APPENDIX: Original Schema Classes ===

# Original API schema classes
class PropertySchema(BaseModel):
    # ... original code with API response schema ...

class PropertyListingSchema(BaseModel):
    # ... original code with API response schema ...

class PropertyMetricsSchema(BaseModel):
    # ... original code with API response schema ...
"""


"""
=== APPENDIX: Original Create/Update Schemas ===

# Original schema classes for creating and updating properties
class PropertyCreate(BaseModel):
    # ... original code for create schema ...

class PropertyUpdate(BaseModel):
    # ... original code for update schema ...
"""

