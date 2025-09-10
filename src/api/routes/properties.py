"""Property-related API routes."""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from pydantic import BaseModel

from ...database.connection import get_db
from ...database.crud import PropertyCRUD, LocationCRUD, ListingCRUD, MetricsCRUD
from ...models.property_models import (
    PropertySchema, PropertyCreate, PropertyUpdate, 
    PropertyListingSchema, PropertyMetricsSchema,
    DataSource, PropertyType
)
from ..auth import get_current_active_user, get_optional_user, User

router = APIRouter()


class PropertySearchResponse(BaseModel):
    """Property search response model."""
    properties: List[PropertySchema]
    total_count: int
    page: int
    page_size: int


class PropertyFilters(BaseModel):
    """Property filters model."""
    data_source: Optional[DataSource] = None
    property_type: Optional[PropertyType] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_bedrooms: Optional[int] = None
    max_bedrooms: Optional[int] = None
    min_bathrooms: Optional[float] = None
    max_bathrooms: Optional[float] = None
    min_sqft: Optional[int] = None
    max_sqft: Optional[int] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None


@router.get("/", response_model=PropertySearchResponse)
async def search_properties(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Number of properties per page"),
    data_source: Optional[DataSource] = Query(None, description="Filter by data source"),
    property_type: Optional[PropertyType] = Query(None, description="Filter by property type"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    min_bedrooms: Optional[int] = Query(None, ge=0, description="Minimum bedrooms"),
    max_bedrooms: Optional[int] = Query(None, ge=0, description="Maximum bedrooms"),
    min_bathrooms: Optional[float] = Query(None, ge=0, description="Minimum bathrooms"),
    max_bathrooms: Optional[float] = Query(None, ge=0, description="Maximum bathrooms"),
    min_sqft: Optional[int] = Query(None, ge=0, description="Minimum square feet"),
    max_sqft: Optional[int] = Query(None, ge=0, description="Maximum square feet"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    zip_code: Optional[str] = Query(None, description="Filter by ZIP code"),
    order_by: str = Query("created_at", description="Field to order by"),
    order_direction: str = Query("desc", regex="^(asc|desc)$", description="Order direction"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_user)
):
    """Search properties with filtering and pagination.
    
    Args:
        page: Page number (1-based)
        page_size: Number of properties per page
        data_source: Filter by data source
        property_type: Filter by property type
        min_price: Minimum price filter
        max_price: Maximum price filter
        min_bedrooms: Minimum bedrooms filter
        max_bedrooms: Maximum bedrooms filter
        min_bathrooms: Minimum bathrooms filter
        max_bathrooms: Maximum bathrooms filter
        min_sqft: Minimum square feet filter
        max_sqft: Maximum square feet filter
        city: City filter
        state: State filter
        zip_code: ZIP code filter
        order_by: Field to order by
        order_direction: Order direction (asc/desc)
        db: Database session
        current_user: Optional current user
        
    Returns:
        PropertySearchResponse: Search results with pagination
    """
    # Convert prices to cents for database query
    min_price_cents = int(min_price * 100) if min_price else None
    max_price_cents = int(max_price * 100) if max_price else None
    
    # Calculate offset
    offset = (page - 1) * page_size
    
    # Search properties
    properties = PropertyCRUD.search(
        db=db,
        data_source=data_source,
        property_type=property_type,
        min_price=min_price_cents,
        max_price=max_price_cents,
        min_bedrooms=min_bedrooms,
        max_bedrooms=max_bedrooms,
        min_bathrooms=min_bathrooms,
        max_bathrooms=max_bathrooms,
        min_sqft=min_sqft,
        max_sqft=max_sqft,
        city=city,
        state=state,
        zip_code=zip_code,
        limit=page_size,
        offset=offset,
        order_by=order_by,
        order_direction=order_direction
    )
    
    # Convert to response schemas
    property_schemas = [PropertySchema.from_orm(prop) for prop in properties]
    
    # For total count, we'd need a separate query in production
    # For now, estimate based on current page
    total_count = len(properties) + offset if len(properties) == page_size else offset + len(properties)
    
    return PropertySearchResponse(
        properties=property_schemas,
        total_count=total_count,
        page=page,
        page_size=page_size
    )


@router.get("/{property_id}", response_model=PropertySchema)
async def get_property(
    property_id: int = Path(..., description="Property ID"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_user)
):
    """Get a specific property by ID.
    
    Args:
        property_id: Property ID
        db: Database session
        current_user: Optional current user
        
    Returns:
        PropertySchema: Property data
        
    Raises:
        HTTPException: If property not found
    """
    property_obj = PropertyCRUD.get_by_id(db, property_id)
    
    if not property_obj:
        raise HTTPException(status_code=404, detail="Property not found")
    
    return PropertySchema.from_orm(property_obj)


@router.post("/", response_model=PropertySchema)
async def create_property(
    property_data: PropertyCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Create a new property.
    
    Args:
        property_data: Property creation data
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        PropertySchema: Created property
        
    Raises:
        HTTPException: If property already exists
    """
    # Check if property already exists
    existing_property = PropertyCRUD.get_by_external_id(
        db, property_data.external_id, property_data.data_source
    )
    
    if existing_property:
        raise HTTPException(
            status_code=400, 
            detail=f"Property with external_id {property_data.external_id} already exists"
        )
    
    # Create property
    new_property = PropertyCRUD.create(db, property_data)
    return PropertySchema.from_orm(new_property)


@router.put("/{property_id}", response_model=PropertySchema)
async def update_property(
    property_id: int = Path(..., description="Property ID"),
    property_data: PropertyUpdate = ...,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Update a property.
    
    Args:
        property_id: Property ID
        property_data: Property update data
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        PropertySchema: Updated property
        
    Raises:
        HTTPException: If property not found
    """
    updated_property = PropertyCRUD.update(db, property_id, property_data)
    
    if not updated_property:
        raise HTTPException(status_code=404, detail="Property not found")
    
    return PropertySchema.from_orm(updated_property)


@router.delete("/{property_id}")
async def delete_property(
    property_id: int = Path(..., description="Property ID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Delete a property.
    
    Args:
        property_id: Property ID
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        dict: Success message
        
    Raises:
        HTTPException: If property not found
    """
    success = PropertyCRUD.delete(db, property_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Property not found")
    
    return {"message": "Property deleted successfully"}


@router.get("/{property_id}/listings", response_model=List[PropertyListingSchema])
async def get_property_listings(
    property_id: int = Path(..., description="Property ID"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_user)
):
    """Get all listings for a property.
    
    Args:
        property_id: Property ID
        db: Database session
        current_user: Optional current user
        
    Returns:
        List[PropertyListingSchema]: Property listings
        
    Raises:
        HTTPException: If property not found
    """
    # Verify property exists
    property_obj = PropertyCRUD.get_by_id(db, property_id)
    if not property_obj:
        raise HTTPException(status_code=404, detail="Property not found")
    
    # Get listings
    listings = ListingCRUD.get_by_property_id(db, property_id)
    return [PropertyListingSchema.from_orm(listing) for listing in listings]


@router.get("/{property_id}/metrics", response_model=PropertyMetricsSchema)
async def get_property_metrics(
    property_id: int = Path(..., description="Property ID"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_user)
):
    """Get metrics for a property.
    
    Args:
        property_id: Property ID
        db: Database session
        current_user: Optional current user
        
    Returns:
        PropertyMetricsSchema: Property metrics
        
    Raises:
        HTTPException: If property or metrics not found
    """
    # Verify property exists
    property_obj = PropertyCRUD.get_by_id(db, property_id)
    if not property_obj:
        raise HTTPException(status_code=404, detail="Property not found")
    
    # Get metrics
    metrics = MetricsCRUD.get_by_property_id(db, property_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Property metrics not found")
    
    return PropertyMetricsSchema.from_orm(metrics)


@router.get("/external/{data_source}/{external_id}", response_model=PropertySchema)
async def get_property_by_external_id(
    data_source: DataSource = Path(..., description="Data source"),
    external_id: str = Path(..., description="External property ID"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_user)
):
    """Get a property by external ID and data source.
    
    Args:
        data_source: Data source
        external_id: External property ID
        db: Database session
        current_user: Optional current user
        
    Returns:
        PropertySchema: Property data
        
    Raises:
        HTTPException: If property not found
    """
    property_obj = PropertyCRUD.get_by_external_id(db, external_id, data_source)
    
    if not property_obj:
        raise HTTPException(
            status_code=404, 
            detail=f"Property not found with external_id {external_id} from {data_source}"
        )
    
    return PropertySchema.from_orm(property_obj)


@router.get("/stale/{hours}", response_model=List[PropertySchema])
async def get_stale_properties(
    hours: int = Path(..., ge=1, le=168, description="Hours since last scrape"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of properties"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Get properties that haven't been scraped recently.
    
    Args:
        hours: Hours since last scrape
        limit: Maximum number of properties to return
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        List[PropertySchema]: Stale properties
    """
    stale_properties = PropertyCRUD.get_stale_properties(db, hours)
    
    # Limit results
    limited_properties = stale_properties[:limit]
    
    return [PropertySchema.from_orm(prop) for prop in limited_properties]

