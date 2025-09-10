"""Main data processing pipeline for scraped real estate data."""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging
from pathlib import Path

# Use absolute imports instead of relative ones
from src.etl.data_validator import DataValidator
from src.etl.data_transformer import DataTransformer
from src.etl.deduplication import DeduplicationEngine
from src.models.property_models import PropertyModel

logger = logging.getLogger(__name__)


class DataProcessor:
    """Main data processing pipeline for scraped real estate data."""
    
    def __init__(self, output_dir: str):
        """Initialize the data processor.
        
        Args:
            output_dir: Directory to store output CSV files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.validator = DataValidator()
        self.transformer = DataTransformer()
        self.deduplicator = DeduplicationEngine()
        
    def process_scraped_data(self, scraped_data: List[Dict[str, Any]], job_id: str) -> Dict[str, Any]:
        """Process a batch of scraped real estate data.
        
        Args:
            scraped_data: List of raw scraped property data
            job_id: Scraping job ID for tracking
            
        Returns:
            Dict[str, Any]: Processing results summary
        """
        logger.info(f"Processing {len(scraped_data)} scraped properties for job {job_id}")
        
        results = {
            'total_input': len(scraped_data),
            'processed': 0,
            'saved': 0,
            'duplicates': 0,
            'invalid': 0,
            'errors': [],
            'processing_time': 0
        }
        
        start_time = datetime.utcnow()
        
        try:
            # Convert to DataFrame for bulk processing
            df = pd.DataFrame(scraped_data)
            
            if df.empty:
                logger.warning("No data to process")
                return results
            
            # Step 1: Data validation
            logger.info("Step 1: Validating data")
            valid_df, validation_errors = self._validate_data(df)
            results['invalid'] = len(df) - len(valid_df)
            results['errors'].extend(validation_errors)
            
            if valid_df.empty:
                logger.warning("No valid data after validation")
                return results
            
            # Step 2: Data transformation and standardization
            logger.info("Step 2: Transforming and standardizing data")
            transformed_df = self._transform_data(valid_df)
            
            # Step 3: Deduplication
            logger.info("Step 3: Deduplicating data")
            unique_df, duplicate_count = self._deduplicate_data(transformed_df)
            results['duplicates'] = duplicate_count
            
            # Step 4: Enrichment (add calculated fields, geocoding, etc.)
            logger.info("Step 4: Enriching data")
            enriched_df = self._enrich_data(unique_df)
            
            # Step 5: Save to CSV
            logger.info("Step 5: Saving to CSV")
            saved_count = self.save_to_csv(enriched_df, job_id)
            results['saved'] = saved_count
            results['processed'] = len(enriched_df)
            
            # Calculate processing time
            end_time = datetime.utcnow()
            results['processing_time'] = (end_time - start_time).total_seconds()
            
            logger.info(f"Data processing completed: {results}")
            
        except Exception as e:
            logger.error(f"Error processing scraped data: {e}")
            results['errors'].append(f"Processing error: {str(e)}")
        
        return results
    
    def _validate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate scraped data and filter out invalid records.
        
        Args:
            df: DataFrame with scraped data
            
        Returns:
            Tuple[pd.DataFrame, List[str]]: Valid data and validation errors
        """
        validation_errors = []
        valid_indices = []
        
        for idx, row in df.iterrows():
            try:
                is_valid, errors = self.validator.validate_property_data(row.to_dict())
                if is_valid:
                    valid_indices.append(idx)
                else:
                    validation_errors.extend([f"Row {idx}: {error}" for error in errors])
            except Exception as e:
                validation_errors.append(f"Row {idx}: Validation error - {str(e)}")
        
        valid_df = df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()
        
        logger.info(f"Validation: {len(valid_df)}/{len(df)} records passed validation")
        
        return valid_df, validation_errors
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and standardize the data.
        
        Args:
            df: DataFrame with valid data
            
        Returns:
            pd.DataFrame: Transformed data
        """
        transformed_df = df.copy()
        
        # Apply transformations row by row
        for idx, row in transformed_df.iterrows():
            try:
                transformed_data = self.transformer.transform_property_data(row.to_dict())
                for key, value in transformed_data.items():
                    if key in transformed_df.columns:
                        transformed_df.at[idx, key] = value
            except Exception as e:
                logger.error(f"Error transforming row {idx}: {e}")
        
        # Apply DataFrame-level transformations
        transformed_df = self._apply_bulk_transformations(transformed_df)
        
        return transformed_df
    
    def _apply_bulk_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply bulk transformations using pandas operations.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # Price normalization (convert to cents if not already)
        if 'price' in df.columns:
            df['price'] = df['price'].apply(lambda x: int(x * 100) if x and x < 100000 else x)
        
        if 'rent_estimate' in df.columns:
            df['rent_estimate'] = df['rent_estimate'].apply(lambda x: int(x * 100) if x and x < 100000 else x)
        
        # Calculate price per square foot if missing
        if 'price' in df.columns and 'square_feet' in df.columns and 'price_per_sqft' not in df.columns:
            df['price_per_sqft'] = np.where(
                (df['price'].notna()) & (df['square_feet'].notna()) & (df['square_feet'] > 0),
                (df['price'] / 100) / df['square_feet'],  # Convert price back to dollars for calculation
                np.nan
            )
        
        # Standardize state abbreviations
        if 'state' in df.columns:
            df['state'] = df['state'].apply(self._standardize_state)
        
        # Clean and standardize zip codes
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].apply(self._clean_zip_code)
        
        # Add timestamp fields
        df['scraped_at'] = datetime.utcnow()
        df['processed_at'] = datetime.utcnow()
        
        return df
    
    def _standardize_state(self, state: str) -> str:
        """Standardize state names to abbreviations.
        
        Args:
            state: State name or abbreviation
            
        Returns:
            str: Standardized state abbreviation
        """
        if not state or pd.isna(state):
            return ""
        
        state = str(state).strip().upper()
        
        # State name to abbreviation mapping
        state_mapping = {
            'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
            'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
            'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
            'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
            'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
            'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
            'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
            'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
            'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
            'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
            'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
            'WISCONSIN': 'WI', 'WYOMING': 'WY'
        }
        
        return state_mapping.get(state, state)
    
    def _clean_zip_code(self, zip_code: str) -> str:
        """Clean and standardize zip codes.
        
        Args:
            zip_code: Raw zip code
            
        Returns:
            str: Cleaned zip code
        """
        if not zip_code or pd.isna(zip_code):
            return ""
        
        zip_code = str(zip_code).strip()
        
        # Extract first 5 digits
        import re
        match = re.search(r'(\d{5})', zip_code)
        return match.group(1) if match else zip_code
    
    def _deduplicate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Remove duplicate properties from the dataset.
        
        Args:
            df: DataFrame with transformed data
            
        Returns:
            Tuple[pd.DataFrame, int]: Unique data and duplicate count
        """
        initial_count = len(df)
        
        # Use deduplication engine
        unique_properties = []
        duplicate_count = 0
        
        for _, row in df.iterrows():
            property_data = row.to_dict()
            
            if not self.deduplicator.is_duplicate(property_data):
                unique_properties.append(property_data)
            else:
                duplicate_count += 1
        
        unique_df = pd.DataFrame(unique_properties) if unique_properties else pd.DataFrame()
        
        logger.info(f"Deduplication: {len(unique_df)}/{initial_count} unique properties")
        
        return unique_df, duplicate_count
    
    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the data with calculated fields and external data.
        
        Args:
            df: DataFrame with unique data
            
        Returns:
            pd.DataFrame: Enriched data
        """
        enriched_df = df.copy()
        
        # Calculate derived metrics
        enriched_df = self._calculate_property_metrics(enriched_df)
        
        # Add market analysis
        enriched_df = self._add_market_analysis(enriched_df)
        
        # Geocoding (if coordinates are missing)
        enriched_df = self._geocode_addresses(enriched_df)
        
        return enriched_df
    
    def _calculate_property_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate property metrics and investment indicators.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            pd.DataFrame: DataFrame with calculated metrics
        """
        # Calculate price per square foot if missing
        if 'price' in df.columns and 'square_feet' in df.columns:
            df['calculated_price_per_sqft'] = np.where(
                (df['price'].notna()) & (df['square_feet'].notna()) & (df['square_feet'] > 0),
                (df['price'] / 100) / df['square_feet'],
                np.nan
            )
        
        # Estimate rental yield for purchase properties
        if 'price' in df.columns and 'rent_estimate' in df.columns:
            df['estimated_rental_yield'] = np.where(
                (df['price'].notna()) & (df['rent_estimate'].notna()) & (df['price'] > 0),
                ((df['rent_estimate'] / 100) * 12) / (df['price'] / 100) * 100,
                np.nan
            )
        
        # Calculate property age
        if 'year_built' in df.columns:
            current_year = datetime.now().year
            df['property_age'] = np.where(
                df['year_built'].notna(),
                current_year - df['year_built'],
                np.nan
            )
        
        return df
    
    def _add_market_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add market-level analysis and comparisons.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            pd.DataFrame: DataFrame with market analysis
        """
        # Group by city for market analysis
        if 'city' in df.columns and 'price' in df.columns:
            # Calculate median price by city
            city_medians = df.groupby('city')['price'].median().to_dict()
            df['city_median_price'] = df['city'].map(city_medians)
            
            # Calculate price vs market median
            df['price_vs_market'] = np.where(
                (df['price'].notna()) & (df['city_median_price'].notna()) & (df['city_median_price'] > 0),
                ((df['price'] - df['city_median_price']) / df['city_median_price']) * 100,
                np.nan
            )
        
        return df
    
    def _geocode_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add geocoding for addresses missing coordinates.
        
        Args:
            df: DataFrame to geocode
            
        Returns:
            pd.DataFrame: DataFrame with geocoded addresses
        """
        # For now, just placeholder - in production, would integrate with geocoding service
        logger.info("Geocoding functionality placeholder - integrate with geocoding service")
        return df
    
    def save_to_csv(self, df: pd.DataFrame, job_id: str) -> int:
        """Save processed data to a CSV file.
        
        Args:
            df: DataFrame with processed data
            job_id: Scraping job ID
            
        Returns:
            int: Number of records saved
        """
        try:
            # Create output filename with timestamp and job_id
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'properties_{job_id}_{timestamp}.csv'
            output_path = self.output_dir / filename
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            saved_count = len(df)
            
            logger.info(f"Saved {saved_count} properties to {output_path}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving properties to CSV: {e}")
            return 0
    
    def process_single_property(self, property_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single property record.
        
        Args:
            property_data: Raw property data
            
        Returns:
            Optional[Dict[str, Any]]: Processed property data or None if invalid
        """
        try:
            # Validate
            is_valid, errors = self.validator.validate_property_data(property_data)
            if not is_valid:
                logger.warning(f"Invalid property data: {errors}")
                return None
            
            # Transform
            transformed_data = self.transformer.transform_property_data(property_data)
            
            # Check for duplicates
            if self.deduplicator.is_duplicate(transformed_data):
                logger.debug("Property is duplicate, skipping")
                return None
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error processing single property: {e}")
            return None

