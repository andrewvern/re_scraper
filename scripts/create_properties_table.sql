-- Create properties table with pandas-compatible schema
-- This script creates the exact schema that matches your pandas DataFrame

-- Drop tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS property_metrics CASCADE;
DROP TABLE IF EXISTS property_listings CASCADE;
DROP TABLE IF EXISTS properties CASCADE;

-- Create the main properties table
CREATE TABLE properties (
    -- Core identifiers matching pandas schema
    property_id BIGSERIAL PRIMARY KEY,  -- Auto-incrementing primary key for pandas Int64Dtype
    listing_id BIGINT,                  -- pandas Int64Dtype
    mls_id VARCHAR(100),                -- pandas string
    
    -- Status and pricing
    status VARCHAR(50),                 -- pandas string
    price BIGINT,                       -- pandas Int64Dtype (store in cents or dollars as needed)
    hoa_fee VARCHAR(50),                -- pandas string
    
    -- Property measurements (all FLOAT for pandas compatibility)
    square_feet FLOAT,                  -- pandas float
    lot_size FLOAT,                     -- pandas float
    bedrooms FLOAT,                     -- pandas float (allows null and decimal values)
    bathrooms FLOAT,                    -- pandas float
    stories FLOAT,                      -- pandas float
    year_built FLOAT,                   -- pandas float
    
    -- Location fields - consolidated into individual columns
    location VARCHAR(500),              -- pandas string (consolidated location)
    address VARCHAR(500),               -- pandas string
    city VARCHAR(100),                  -- pandas string
    state VARCHAR(50),                  -- pandas string
    zip_code VARCHAR(20),               -- pandas string
    country_code VARCHAR(10),           -- pandas string
    latitude FLOAT,                     -- pandas float
    longitude FLOAT,                    -- pandas float
    
    -- URLs and description
    url VARCHAR(500),                   -- pandas string
    description TEXT,                   -- pandas string (unlimited length)
    
    -- Property type as integer for pandas Int64Dtype
    property_type INTEGER,              -- pandas Int64Dtype (1=house, 2=apartment, etc.)
    
    -- Legacy fields for backward compatibility
    external_id VARCHAR(100),           -- Legacy field
    data_source VARCHAR(50),            -- Legacy field
    
    -- Additional features (JSON for flexibility)
    features JSONB,                     -- Store additional property features
    images JSONB,                       -- Store image URLs as JSON array
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create property_listings table (related data)
CREATE TABLE property_listings (
    id SERIAL PRIMARY KEY,
    property_id BIGINT NOT NULL REFERENCES properties(property_id) ON DELETE CASCADE,
    
    -- Listing details
    listing_status VARCHAR(50) NOT NULL,
    list_price INTEGER,                 -- Store in cents
    listing_date TIMESTAMP WITH TIME ZONE,
    days_on_market INTEGER,
    price_history JSONB,                -- Historical price changes
    
    -- Agent/Broker info
    listing_agent VARCHAR(255),
    brokerage VARCHAR(255),
    
    -- URLs and external references
    listing_url VARCHAR(500),
    mls_number VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create property_metrics table (analytics data)
CREATE TABLE property_metrics (
    id SERIAL PRIMARY KEY,
    property_id BIGINT NOT NULL REFERENCES properties(property_id) ON DELETE CASCADE,
    
    -- Market metrics (store monetary values in cents)
    zestimate INTEGER,                  -- Zillow estimate in cents
    rent_zestimate INTEGER,             -- Rent estimate in cents
    market_value INTEGER,               -- Market value in cents
    
    -- Neighborhood metrics
    median_home_value INTEGER,
    median_rent INTEGER,
    price_per_sqft_area FLOAT,
    
    -- Investment metrics
    cap_rate FLOAT,                     -- Capitalization rate
    cash_flow INTEGER,                  -- Monthly cash flow in cents
    roi FLOAT,                          -- Return on investment percentage
    
    -- Market trends
    appreciation_rate FLOAT,            -- Annual appreciation percentage
    rental_yield FLOAT,
    
    -- Additional metrics
    walk_score INTEGER,
    school_rating FLOAT,
    crime_score FLOAT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_properties_property_id ON properties(property_id);
CREATE INDEX idx_properties_listing_id ON properties(listing_id);
CREATE INDEX idx_properties_mls_id ON properties(mls_id);
CREATE INDEX idx_properties_status ON properties(status);
CREATE INDEX idx_properties_price ON properties(price);
CREATE INDEX idx_properties_city_state ON properties(city, state);
CREATE INDEX idx_properties_location_coords ON properties(latitude, longitude);
CREATE INDEX idx_properties_property_type ON properties(property_type);
CREATE INDEX idx_properties_created_at ON properties(created_at);

-- Indexes for property_listings
CREATE INDEX idx_listings_property_id ON property_listings(property_id);
CREATE INDEX idx_listings_status ON property_listings(listing_status);
CREATE INDEX idx_listings_mls_number ON property_listings(mls_number);

-- Indexes for property_metrics
CREATE INDEX idx_metrics_property_id ON property_metrics(property_id);

-- Create triggers for automatic updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to all tables
CREATE TRIGGER update_properties_updated_at 
    BEFORE UPDATE ON properties 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_property_listings_updated_at 
    BEFORE UPDATE ON property_listings 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_property_metrics_updated_at 
    BEFORE UPDATE ON property_metrics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE properties IS 'Main property table with pandas-compatible schema';
COMMENT ON COLUMN properties.property_id IS 'Primary key matching pandas property_id';
COMMENT ON COLUMN properties.listing_id IS 'External listing identifier';
COMMENT ON COLUMN properties.mls_id IS 'MLS listing identifier';
COMMENT ON COLUMN properties.price IS 'Property price (store as appropriate for your use case)';
COMMENT ON COLUMN properties.property_type IS 'Property type as integer (1=house, 2=apartment, 3=condo, etc.)';
COMMENT ON COLUMN properties.location IS 'Consolidated location string for pandas compatibility';

-- Create a view that matches your pandas DataFrame columns exactly
CREATE VIEW pandas_properties_view AS
SELECT 
    property_id,
    listing_id,
    mls_id,
    status,
    price,
    hoa_fee,
    square_feet,
    lot_size,
    bedrooms,
    bathrooms,
    location,
    stories,
    address,
    city,
    state,
    zip_code,
    year_built,
    url,
    latitude,
    longitude,
    description,
    property_type,
    country_code
FROM properties
ORDER BY property_id;

COMMENT ON VIEW pandas_properties_view IS 'View that exactly matches pandas DataFrame column structure';

