"""Factory module for creating scraping and ETL components."""

from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, Any, List, Optional, Union

from ..models.property_models import PropertyModel, PropertyListingModel, PropertyMetricsModel
from ..scrapers.base_scraper import BaseScraper
from .data_processor import DataProcessor
from .data_validator import DataValidator
from .data_transformer import DataTransformer
from .deduplication import DeduplicationEngine

logger = logging.getLogger(__name__)


class ETLFactory:
    """Factory for creating and managing ETL components."""
    
    def __init__(self, output_dir: str):
        """Initialize the ETL factory.
        
        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create ETL components
        self.validator = DataValidator()
        self.transformer = DataTransformer()
        self.deduplicator = DeduplicationEngine()
        self.processor = DataProcessor(output_dir)
    
    def process_scraped_data(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], 
                           source: str) -> Dict[str, Any]:
        """Process scraped property data.
        
        Args:
            data: Single property dict or list of property dicts
            source: Name/identifier for the data source
            
        Returns:
            Dict[str, Any]: Processing results
        """
        if isinstance(data, dict):
            data = [data]
        
        # Generate job ID
        job_id = f"{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Process the data
        results = self.processor.process_scraped_data(data, job_id)
        
        logger.info(f"Processed {len(data)} properties from {source}")
        return results
    
    def save_to_csv(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], 
                    filename: Optional[str] = None) -> str:
        """Save property data directly to CSV.
        
        Args:
            data: Single property dict or list of property dicts
            filename: Optional filename, defaults to timestamp
            
        Returns:
            str: Path to saved CSV file
        """
        import pandas as pd
        
        if isinstance(data, dict):
            data = [data]
        
        # Create pandas DataFrame
        df = pd.DataFrame(data)
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"properties_{timestamp}.csv"
        
        # Ensure .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Save to CSV
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False)
        
        logger.info(f"Saved {len(data)} properties to {output_path}")
        return str(output_path)
    
    def validate_property(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single property.
        
        Args:
            data: Property data to validate
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, error_messages)
        """
        return self.validator.validate_property_data(data)
    
    def transform_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single property.
        
        Args:
            data: Property data to transform
            
        Returns:
            Dict[str, Any]: Transformed property data
        """
        return self.transformer.transform_property_data(data)
    
    def is_duplicate(self, data: Dict[str, Any]) -> bool:
        """Check if a property is a duplicate.
        
        Args:
            data: Property data to check
            
        Returns:
            bool: True if property is a duplicate
        """
        return self.deduplicator.is_duplicate(data)
