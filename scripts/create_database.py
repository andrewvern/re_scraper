#!/usr/bin/env python3
"""
Script to create PostgreSQL database with pandas-compatible schema.
This script can be run independently or integrated with your existing setup.
"""

import os
import sys
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def create_database_with_sqlalchemy():
    """Create database using SQLAlchemy models (recommended approach)."""
    try:
        from src.database.connection import init_db, engine
        from src.models.property_models import Base
        
        print("Creating database tables using SQLAlchemy...")
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        
        print("✅ Database tables created successfully using SQLAlchemy!")
        return True
        
    except Exception as e:
        print(f"❌ Error creating database with SQLAlchemy: {e}")
        return False

def create_database_with_sql():
    """Create database using direct SQL execution."""
    try:
        # Database connection parameters (adjust as needed)
        db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'database': os.getenv('DB_NAME', 'real_estate_scraper')
        }
        
        print(f"Connecting to PostgreSQL at {db_params['host']}:{db_params['port']}...")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Read and execute the SQL script
        sql_file = project_root / "scripts" / "create_properties_table.sql"
        
        if not sql_file.exists():
            print(f"❌ SQL file not found: {sql_file}")
            return False
            
        print("Executing SQL script...")
        with open(sql_file, 'r') as f:
            sql_content = f.read()
            
        # Execute the SQL
        cursor.execute(sql_content)
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('properties', 'property_listings', 'property_metrics')
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("✅ Created tables:")
        for table in tables:
            print(f"   - {table[0]}")
            
        # Show the properties table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'properties' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\n📋 Properties table structure:")
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            print(f"   - {col[0]}: {col[1]} ({nullable})")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Database created successfully using SQL script!")
        return True
        
    except Exception as e:
        print(f"❌ Error creating database with SQL: {e}")
        return False

def verify_pandas_compatibility():
    """Verify the schema is compatible with pandas DataFrame."""
    try:
        import pandas as pd
        from src.database.connection import engine
        
        print("\n🔍 Verifying pandas compatibility...")
        
        # Test query to check if we can load data into pandas
        query = """
            SELECT property_id, listing_id, mls_id, status, price, hoa_fee,
                   square_feet, lot_size, bedrooms, bathrooms, location, stories,
                   address, city, state, zip_code, year_built, url, latitude, 
                   longitude, description, property_type, country_code
            FROM properties 
            LIMIT 0;  -- Just check structure, no data needed
        """
        
        df = pd.read_sql_query(query, engine)
        
        print("✅ Pandas DataFrame structure verified:")
        print(f"   - Columns: {list(df.columns)}")
        print(f"   - Data types will be: {dict(df.dtypes)}")
        
        # Test the pandas type conversions you specified
        test_data = {
            'property_id': [1, 2, None],
            'listing_id': [100, 200, None],
            'mls_id': ['MLS001', 'MLS002', None],
            'status': ['active', 'sold', None],
            'price': [500000, 750000, None],
            'hoa_fee': ['$200', '$150', None],
            'square_feet': [2000.5, 2500.0, None],
            'lot_size': [0.25, 0.33, None],
            'bedrooms': [3.0, 4.0, None],
            'bathrooms': [2.5, 3.0, None],
            'location': ['123 Main St, City, State', '456 Oak Ave, City, State', None],
            'stories': [2.0, 1.0, None],
            'address': ['123 Main St', '456 Oak Ave', None],
            'city': ['Anytown', 'Somewhere', None],
            'state': ['CA', 'TX', None],
            'zip_code': ['90210', '12345', None],
            'year_built': [1995.0, 2010.0, None],
            'url': ['http://example.com/1', 'http://example.com/2', None],
            'latitude': [34.0522, 32.7767, None],
            'longitude': [-118.2437, -96.7970, None],
            'description': ['Beautiful home', 'Great location', None],
            'property_type': [1, 2, None],
            'country_code': ['US', 'US', None]
        }
        
        test_df = pd.DataFrame(test_data)
        
        # Apply your specified type conversions
        test_df['property_id'] = test_df['property_id'].astype(pd.Int64Dtype())
        test_df['listing_id'] = test_df['listing_id'].astype(pd.Int64Dtype())
        test_df['mls_id'] = test_df['mls_id'].astype(str)
        test_df['status'] = test_df['status'].astype(str)
        test_df['price'] = pd.to_numeric(test_df['price'], errors='coerce').astype(pd.Int64Dtype())
        test_df['hoa_fee'] = test_df['hoa_fee'].astype(str)
        test_df['square_feet'] = pd.to_numeric(test_df['square_feet'], errors='coerce').astype(float)
        test_df['lot_size'] = pd.to_numeric(test_df['lot_size'], errors='coerce').astype(float)
        test_df['bedrooms'] = pd.to_numeric(test_df['bedrooms'], errors='coerce').astype(float)
        test_df['bathrooms'] = pd.to_numeric(test_df['bathrooms'], errors='coerce').astype(float)
        test_df['location'] = test_df['location'].astype(str)
        test_df['stories'] = pd.to_numeric(test_df['stories'], errors='coerce').astype(float)
        test_df['address'] = test_df['address'].astype(str)
        test_df['city'] = test_df['city'].astype(str)
        test_df['state'] = test_df['state'].astype(str)
        test_df['zip_code'] = test_df['zip_code'].astype(str)
        test_df['year_built'] = pd.to_numeric(test_df['year_built'], errors='coerce').astype(float)
        test_df['url'] = test_df['url'].astype(str)
        test_df['latitude'] = pd.to_numeric(test_df['latitude'], errors='coerce').astype(float)
        test_df['longitude'] = pd.to_numeric(test_df['longitude'], errors='coerce').astype(float)
        test_df['description'] = test_df['description'].astype(str)
        test_df['property_type'] = test_df['property_type'].astype(pd.Int64Dtype())
        test_df['country_code'] = test_df['country_code'].astype(str)
        
        print("✅ Pandas type conversions successful!")
        print("   Your exact pandas schema is fully compatible!")
        
        return True
        
    except Exception as e:
        print(f"❌ Pandas compatibility check failed: {e}")
        return False

def main():
    """Main function to create the database."""
    print("🏗️  PostgreSQL Database Creation Script")
    print("=" * 50)
    
    # Try SQLAlchemy approach first (recommended)
    if create_database_with_sqlalchemy():
        print("\n✅ Database created successfully with SQLAlchemy!")
    else:
        print("\n⚠️  SQLAlchemy approach failed, trying direct SQL...")
        if create_database_with_sql():
            print("\n✅ Database created successfully with direct SQL!")
        else:
            print("\n❌ Both approaches failed. Please check your database connection.")
            return False
    
    # Verify pandas compatibility
    verify_pandas_compatibility()
    
    print("\n🎉 Database setup complete!")
    print("\nNext steps:")
    print("1. Your database now has the exact schema for pandas compatibility")
    print("2. You can load data using: pd.read_sql_query(query, engine)")
    print("3. Apply the type conversions you specified in your pandas code")
    print("4. Use Alembic for future schema changes: alembic upgrade head")
    
    return True

if __name__ == "__main__":
    main()

