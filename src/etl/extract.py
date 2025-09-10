"""Extract module for obtaining property data from various sources."""

from typing import Dict, Any, List
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class PropertyExtractor:
    """Handles extraction of property data from various sources."""
    
    def extract_from_scraper(self, scraper: Any, url: str) -> List[Dict[str, Any]]:
        """Extract properties using a scraper instance.
        
        Args:
            scraper: Scraper instance (Redfin, Zillow, etc.)
            url: URL to scrape
            
        Returns:
            List[Dict[str, Any]]: List of property data dictionaries
        """
        try:
            properties = scraper.scrape(url)
            logger.info(f"Extracted {len(properties)} properties from {url}")
            return properties
        except Exception as e:
            logger.error(f"Error extracting properties: {e}")
            return []
    
    def extract_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Extract properties from a file (CSV, JSON, etc.).
        
        Args:
            file_path: Path to the input file
            
        Returns:
            List[Dict[str, Any]]: List of property data dictionaries
        """
        import pandas as pd
        
        try:
            file_path = Path(file_path)
            
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
            elif file_path.suffix.lower() == '.json':
                df = pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
            
            properties = df.to_dict('records')
            logger.info(f"Extracted {len(properties)} properties from {file_path}")
            return properties
            
        except Exception as e:
            logger.error(f"Error extracting from file {file_path}: {e}")
            return []
    
    def extract_from_api(self, api_url: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Extract properties from an API endpoint.
        
        Args:
            api_url: API endpoint URL
            params: Optional query parameters
            
        Returns:
            List[Dict[str, Any]]: List of property data dictionaries
        """
        import requests
        
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            properties = data.get('properties', [])  # Adjust based on API response structure
            
            logger.info(f"Extracted {len(properties)} properties from API")
            return properties
            
        except Exception as e:
            logger.error(f"Error extracting from API {api_url}: {e}")
            return []
