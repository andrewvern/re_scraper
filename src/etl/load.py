"""Load module for saving processed property data."""

from typing import Dict, Any, List, Optional
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class PropertyLoader:
    """Handles saving processed property data to various formats."""
    
    def __init__(self, output_dir: str = './data'):
        """Initialize the loader.
        
        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_to_csv(self, 
                   properties: List[Dict[str, Any]], 
                   filename: Optional[str] = None) -> str:
        """Save properties to a CSV file.
        
        Args:
            properties: List of property dictionaries
            filename: Optional filename (default: timestamp-based)
            
        Returns:
            str: Path to the saved file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'properties_{timestamp}.csv'
        
        # Ensure .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        output_path = self.output_dir / filename
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(properties)
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            
            logger.info(f"Saved {len(properties)} properties to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            raise
    
    def append_to_csv(self, 
                     properties: List[Dict[str, Any]], 
                     filepath: str,
                     deduplicate: bool = True) -> int:
        """Append properties to an existing CSV file.
        
        Args:
            properties: List of property dictionaries
            filepath: Path to the CSV file
            deduplicate: Whether to remove duplicates based on all columns
            
        Returns:
            int: Number of records added
        """
        try:
            filepath = Path(filepath)
            
            # Read existing data if file exists
            if filepath.exists():
                existing_df = pd.read_csv(filepath)
                new_df = pd.DataFrame(properties)
                
                # Combine data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Remove duplicates if requested
                if deduplicate:
                    original_len = len(combined_df)
                    combined_df = combined_df.drop_duplicates()
                    duplicates_removed = original_len - len(combined_df)
                    logger.info(f"Removed {duplicates_removed} duplicate records")
                
                # Save back to file
                combined_df.to_csv(filepath, index=False)
                
                records_added = len(new_df)
                logger.info(f"Appended {records_added} properties to {filepath}")
                return records_added
                
            else:
                # If file doesn't exist, create it
                return len(self.save_to_csv(properties, str(filepath)))
                
        except Exception as e:
            logger.error(f"Error appending to CSV: {e}")
            raise
    
    def save_to_json(self, 
                    properties: List[Dict[str, Any]], 
                    filename: Optional[str] = None) -> str:
        """Save properties to a JSON file.
        
        Args:
            properties: List of property dictionaries
            filename: Optional filename (default: timestamp-based)
            
        Returns:
            str: Path to the saved file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'properties_{timestamp}.json'
        
        # Ensure .json extension
        if not filename.endswith('.json'):
            filename += '.json'
        
        output_path = self.output_dir / filename
        
        try:
            # Convert to DataFrame and save as JSON
            df = pd.DataFrame(properties)
            df.to_json(output_path, orient='records', date_format='iso')
            
            logger.info(f"Saved {len(properties)} properties to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            raise
