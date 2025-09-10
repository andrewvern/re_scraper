-- Initialize database for Real Estate Scraper

-- Create database if it doesn't exist (this might not work in all PostgreSQL setups)
-- The database creation is typically handled by the POSTGRES_DB environment variable

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search
CREATE EXTENSION IF NOT EXISTS "btree_gin"; -- For better indexing

-- Create custom functions
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Function to calculate distance between coordinates
CREATE OR REPLACE FUNCTION calculate_distance(lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT)
RETURNS FLOAT AS $$
DECLARE
    radius FLOAT := 6371; -- Earth's radius in kilometers
    dlat FLOAT;
    dlon FLOAT;
    a FLOAT;
    c FLOAT;
BEGIN
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    a := sin(dlat/2) * sin(dlat/2) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2) * sin(dlon/2);
    c := 2 * asin(sqrt(a));
    RETURN radius * c;
END;
$$ LANGUAGE plpgsql;

-- Function to generate property hash for deduplication
CREATE OR REPLACE FUNCTION generate_property_hash(
    street_addr TEXT,
    city_name TEXT,
    state_name TEXT,
    zip_code TEXT
)
RETURNS TEXT AS $$
BEGIN
    RETURN md5(lower(trim(COALESCE(street_addr, '') || '|' || 
                          COALESCE(city_name, '') || '|' || 
                          COALESCE(state_name, '') || '|' || 
                          COALESCE(zip_code, ''))));
END;
$$ LANGUAGE plpgsql;

-- Create indexes for better performance (will be applied after tables are created)
-- Note: The actual tables will be created by SQLAlchemy/Alembic

-- Create a view for property search with location data
-- This will need to be created after the tables exist, so it's commented out here
-- CREATE VIEW property_search_view AS
-- SELECT 
--     p.*,
--     l.street_address,
--     l.city,
--     l.state,
--     l.zip_code,
--     l.latitude,
--     l.longitude,
--     l.neighborhood,
--     l.county
-- FROM properties p
-- JOIN locations l ON p.location_id = l.id;

