-- Migration script: Transform existing property model to pandas-compatible schema
-- WARNING: This will modify your existing data structure
-- ALWAYS backup your database before running this migration!

BEGIN;

-- Step 1: Create backup of existing data (optional but recommended)
-- CREATE TABLE properties_backup AS SELECT * FROM properties;
-- CREATE TABLE locations_backup AS SELECT * FROM locations;

-- Step 2: Add new columns to existing properties table
DO $$
BEGIN
    -- Core identifiers
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='listing_id') THEN
        ALTER TABLE properties ADD COLUMN listing_id BIGINT;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='mls_id') THEN
        ALTER TABLE properties ADD COLUMN mls_id VARCHAR(100);
    END IF;
    
    -- Status and pricing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='status') THEN
        ALTER TABLE properties ADD COLUMN status VARCHAR(50);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='hoa_fee') THEN
        ALTER TABLE properties ADD COLUMN hoa_fee VARCHAR(50);
    END IF;
    
    -- Location consolidation
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='location') THEN
        ALTER TABLE properties ADD COLUMN location VARCHAR(500);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='address') THEN
        ALTER TABLE properties ADD COLUMN address VARCHAR(500);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='city') THEN
        ALTER TABLE properties ADD COLUMN city VARCHAR(100);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='state') THEN
        ALTER TABLE properties ADD COLUMN state VARCHAR(50);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='zip_code') THEN
        ALTER TABLE properties ADD COLUMN zip_code VARCHAR(20);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='country_code') THEN
        ALTER TABLE properties ADD COLUMN country_code VARCHAR(10);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='latitude') THEN
        ALTER TABLE properties ADD COLUMN latitude FLOAT;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='longitude') THEN
        ALTER TABLE properties ADD COLUMN longitude FLOAT;
    END IF;
    
    -- URLs
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='url') THEN
        ALTER TABLE properties ADD COLUMN url VARCHAR(500);
    END IF;
END $$;

-- Step 3: Rename primary key column if needed
DO $$
BEGIN
    -- Check if we need to rename 'id' to 'property_id'
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='id') AND
       NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='property_id') THEN
        
        -- Rename the column
        ALTER TABLE properties RENAME COLUMN id TO property_id;
        
        -- Update foreign key references in related tables
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='property_listings') THEN
            ALTER TABLE property_listings DROP CONSTRAINT IF EXISTS property_listings_property_id_fkey;
            ALTER TABLE property_listings ADD CONSTRAINT property_listings_property_id_fkey 
                FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE;
        END IF;
        
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='property_metrics') THEN
            ALTER TABLE property_metrics DROP CONSTRAINT IF EXISTS property_metrics_property_id_fkey;
            ALTER TABLE property_metrics ADD CONSTRAINT property_metrics_property_id_fkey 
                FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE;
        END IF;
    END IF;
END $$;

-- Step 4: Update data types for pandas compatibility
-- Change integer columns to appropriate types
ALTER TABLE properties 
    ALTER COLUMN property_id TYPE BIGINT,
    ALTER COLUMN bedrooms TYPE FLOAT USING bedrooms::FLOAT,
    ALTER COLUMN stories TYPE FLOAT USING stories::FLOAT,
    ALTER COLUMN year_built TYPE FLOAT USING year_built::FLOAT,
    ALTER COLUMN price TYPE BIGINT USING price::BIGINT;

-- Step 5: Migrate data from locations table if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='locations') THEN
        -- Copy location data to properties table
        UPDATE properties SET
            address = locations.street_address,
            city = locations.city,
            state = locations.state,
            zip_code = locations.zip_code,
            country_code = COALESCE(locations.country, 'US'),
            latitude = locations.latitude,
            longitude = locations.longitude,
            location = CONCAT_WS(', ', locations.street_address, locations.city, locations.state, locations.zip_code)
        FROM locations 
        WHERE properties.location_id = locations.id;
        
        -- Remove the foreign key constraint
        ALTER TABLE properties DROP CONSTRAINT IF EXISTS properties_location_id_fkey;
        
        -- Drop the location_id column
        ALTER TABLE properties DROP COLUMN IF EXISTS location_id;
        
        -- Drop the locations table (backup recommended first!)
        -- DROP TABLE locations CASCADE;
    END IF;
END $$;

-- Step 6: Update property_type to integer enum
UPDATE properties SET property_type = 
    CASE 
        WHEN property_type = 'house' OR property_type = '1' THEN 1
        WHEN property_type = 'apartment' OR property_type = '2' THEN 2
        WHEN property_type = 'condo' OR property_type = '3' THEN 3
        WHEN property_type = 'townhouse' OR property_type = '4' THEN 4
        WHEN property_type = 'multi_family' OR property_type = '5' THEN 5
        WHEN property_type = 'land' OR property_type = '6' THEN 6
        WHEN property_type = 'commercial' OR property_type = '7' THEN 7
        ELSE 8  -- other
    END::INTEGER
WHERE property_type IS NOT NULL;

-- Change property_type column to integer
ALTER TABLE properties ALTER COLUMN property_type TYPE INTEGER USING property_type::INTEGER;

-- Step 7: Make external_id and data_source nullable for compatibility
ALTER TABLE properties 
    ALTER COLUMN external_id DROP NOT NULL,
    ALTER COLUMN data_source DROP NOT NULL;

-- Step 8: Create indexes for the new schema
CREATE INDEX IF NOT EXISTS idx_properties_listing_id ON properties(listing_id);
CREATE INDEX IF NOT EXISTS idx_properties_mls_id ON properties(mls_id);
CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status);
CREATE INDEX IF NOT EXISTS idx_properties_city_state ON properties(city, state);
CREATE INDEX IF NOT EXISTS idx_properties_location_coords ON properties(latitude, longitude);

-- Step 9: Update triggers for updated_at if they don't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $trigger$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$trigger$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_properties_updated_at ON properties;
CREATE TRIGGER update_properties_updated_at 
    BEFORE UPDATE ON properties 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMIT;

-- Verification queries
SELECT 'Migration completed successfully. Verifying schema...' as status;

-- Show the new table structure
\d properties;

-- Show sample data to verify migration
SELECT property_id, listing_id, mls_id, status, city, state, property_type 
FROM properties 
LIMIT 5;

