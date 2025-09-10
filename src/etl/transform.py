"""Transform module for cleaning and standardizing property data."""

import re
from typing import Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class PropertyTransformer:
    """Handles transformation and standardization of property data."""
    
    def __init__(self):
        """Initialize the transformer with common patterns."""
        self.price_patterns = [
            r'\$?([\d,]+\.?\d*)',  # $1,500 or 1500
            r'([\d,]+\.?\d*)\s*(?:dollars?|usd|\$)'  # 1500 dollars
        ]
        
        self.sqft_patterns = [
            r'([\d,]+\.?\d*)\s*(?:sq\.?\s*ft\.?|sqft|square\s*feet)',
            r'([\d,]+\.?\d*)\s*sf'
        ]
    
    def transform_properties(self, properties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform a list of properties.
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            List[Dict[str, Any]]: Transformed properties
        """
        return [self.transform_property(prop) for prop in properties]
    
    def transform_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single property record.
        
        Args:
            data: Raw property data
            
        Returns:
            Dict[str, Any]: Transformed property data
        """
        transformed = data.copy()
        
        try:
            # Clean text fields
            transformed = self._clean_text_fields(transformed)
            
            # Transform prices
            transformed = self._transform_prices(transformed)
            
            # Transform measurements
            transformed = self._transform_measurements(transformed)
            
            # Standardize address
            transformed = self._standardize_address(transformed)
            
            # Add metadata
            transformed['processed_at'] = datetime.utcnow().isoformat()
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming property: {e}")
            return data
    
    def _clean_text_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize text fields."""
        text_fields = ['description', 'address', 'city', 'state']
        
        for field in text_fields:
            if field in data and data[field]:
                # Convert to string and clean
                text = str(data[field]).strip()
                
                # Remove HTML and extra whitespace
                text = re.sub(r'<[^>]+>', '', text)
                text = re.sub(r'\s+', ' ', text)
                
                data[field] = text
        
        return data
    
    def _transform_prices(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform price-related fields to numeric values."""
        price_fields = ['price', 'rent_estimate']
        
        for field in price_fields:
            if field in data and data[field] is not None:
                if isinstance(data[field], (int, float)):
                    continue
                    
                price_text = str(data[field])
                
                # Try to extract price using patterns
                for pattern in self.price_patterns:
                    match = re.search(pattern, price_text, re.IGNORECASE)
                    if match:
                        try:
                            price = float(match.group(1).replace(',', ''))
                            data[field] = int(price) if price.is_integer() else price
                            break
                        except (ValueError, IndexError):
                            continue
        
        return data
    
    def _transform_measurements(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform measurement fields (square feet, etc.)."""
        if 'square_feet' in data and data['square_feet']:
            if isinstance(data['square_feet'], str):
                for pattern in self.sqft_patterns:
                    match = re.search(pattern, data['square_feet'], re.IGNORECASE)
                    if match:
                        try:
                            sqft = float(match.group(1).replace(',', ''))
                            data['square_feet'] = int(sqft)
                            break
                        except (ValueError, IndexError):
                            continue
        
        return data
    
    def _standardize_address(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize address components."""
        # State abbreviation mapping
        state_mapping = {
            'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT',
            'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI',
            'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA'
            # Add more states as needed
        }
        
        if 'state' in data and data['state']:
            state = str(data['state']).strip().upper()
            data['state'] = state_mapping.get(state, state)
        
        if 'zip_code' in data and data['zip_code']:
            # Extract 5-digit zip code
            match = re.search(r'(\d{5})', str(data['zip_code']))
            if match:
                data['zip_code'] = match.group(1)
        
        return data
