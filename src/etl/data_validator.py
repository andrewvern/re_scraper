"""Data validation module for scraped real estate data."""

import re
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime
import logging

from ..models.property_models import PropertyType, DataSource

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates scraped real estate data for quality and completeness."""
    
    def __init__(self):
        """Initialize the data validator."""
        self.required_fields = ['data_source', 'external_id']
        self.optional_fields = [
            'property_type', 'bedrooms', 'bathrooms', 'square_feet',
            'price', 'rent_estimate', 'street_address', 'city', 'state', 'zip_code'
        ]
        
        # Validation rules
        self.validation_rules = {
            'price': {'min': 1000, 'max': 100000000},  # $1K to $100M
            'rent_estimate': {'min': 100, 'max': 50000},  # $100 to $50K/month
            'bedrooms': {'min': 0, 'max': 20},
            'bathrooms': {'min': 0, 'max': 20},
            'square_feet': {'min': 100, 'max': 100000},
            'lot_size': {'min': 0.01, 'max': 1000},  # acres
            'year_built': {'min': 1800, 'max': datetime.now().year + 5}
        }
    
    def validate_property_data(self, property_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single property record.
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Check required fields
            errors.extend(self._validate_required_fields(property_data))
            
            # Validate data types and formats
            errors.extend(self._validate_data_types(property_data))
            
            # Validate field values and ranges
            errors.extend(self._validate_field_values(property_data))
            
            # Validate address components
            errors.extend(self._validate_address(property_data))
            
            # Validate business rules
            errors.extend(self._validate_business_rules(property_data))
            
            is_valid = len(errors) == 0
            
            if not is_valid:
                logger.debug(f"Validation failed for property {property_data.get('external_id', 'unknown')}: {errors}")
            
            return is_valid, errors
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False, [f"Validation exception: {str(e)}"]
    
    def _validate_required_fields(self, data: Dict[str, Any]) -> List[str]:
        """Validate that required fields are present and not empty.
        
        Args:
            data: Property data
            
        Returns:
            List[str]: Error messages
        """
        errors = []
        
        for field in self.required_fields:
            if field not in data or data[field] is None or data[field] == '':
                errors.append(f"Required field '{field}' is missing or empty")
        
        return errors
    
    def _validate_data_types(self, data: Dict[str, Any]) -> List[str]:
        """Validate data types for fields.
        
        Args:
            data: Property data
            
        Returns:
            List[str]: Error messages
        """
        errors = []
        
        # String fields
        string_fields = ['external_id', 'data_source', 'property_type', 'description',
                        'street_address', 'city', 'state', 'zip_code', 'neighborhood']
        
        for field in string_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], str):
                    try:
                        data[field] = str(data[field])
                    except (ValueError, TypeError):
                        errors.append(f"Field '{field}' must be a string")
        
        # Numeric fields
        numeric_fields = ['price', 'rent_estimate', 'bedrooms', 'bathrooms', 
                         'square_feet', 'lot_size', 'year_built', 'garage_spaces',
                         'stories', 'latitude', 'longitude']
        
        for field in numeric_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], (int, float)):
                    try:
                        # Try to convert to float
                        if isinstance(data[field], str):
                            # Remove common formatting
                            cleaned = data[field].replace(',', '').replace('$', '').strip()
                            data[field] = float(cleaned)
                        else:
                            data[field] = float(data[field])
                    except (ValueError, TypeError):
                        errors.append(f"Field '{field}' must be numeric")
        
        # Boolean fields
        boolean_fields = ['pool', 'fireplace', 'basement']
        
        for field in boolean_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], bool):
                    if isinstance(data[field], str):
                        data[field] = data[field].lower() in ['true', '1', 'yes', 'on']
                    else:
                        data[field] = bool(data[field])
        
        return errors
    
    def _validate_field_values(self, data: Dict[str, Any]) -> List[str]:
        """Validate field values against business rules and ranges.
        
        Args:
            data: Property data
            
        Returns:
            List[str]: Error messages
        """
        errors = []
        
        # Validate against range rules
        for field, rules in self.validation_rules.items():
            if field in data and data[field] is not None:
                value = data[field]
                
                if 'min' in rules and value < rules['min']:
                    errors.append(f"Field '{field}' value {value} is below minimum {rules['min']}")
                
                if 'max' in rules and value > rules['max']:
                    errors.append(f"Field '{field}' value {value} is above maximum {rules['max']}")
        
        # Validate enums
        if 'data_source' in data:
            try:
                DataSource(data['data_source'])
            except ValueError:
                errors.append(f"Invalid data_source: {data['data_source']}")
        
        if 'property_type' in data and data['property_type']:
            try:
                PropertyType(data['property_type'])
            except ValueError:
                errors.append(f"Invalid property_type: {data['property_type']}")
        
        return errors
    
    def _validate_address(self, data: Dict[str, Any]) -> List[str]:
        """Validate address components.
        
        Args:
            data: Property data
            
        Returns:
            List[str]: Error messages
        """
        errors = []
        
        # Validate state format (should be 2-letter abbreviation)
        if 'state' in data and data['state']:
            state = str(data['state']).strip().upper()
            if len(state) != 2 or not state.isalpha():
                # Check if it's a full state name that can be converted
                if not self._is_valid_state_name(state):
                    errors.append(f"Invalid state format: {data['state']}")
        
        # Validate zip code format
        if 'zip_code' in data and data['zip_code']:
            zip_code = str(data['zip_code']).strip()
            if not re.match(r'^\d{5}(-\d{4})?$', zip_code):
                # Try to extract 5-digit zip
                zip_match = re.search(r'\d{5}', zip_code)
                if not zip_match:
                    errors.append(f"Invalid zip code format: {data['zip_code']}")
        
        # Validate coordinates if present
        if 'latitude' in data and data['latitude'] is not None:
            lat = data['latitude']
            if not (-90 <= lat <= 90):
                errors.append(f"Invalid latitude: {lat}")
        
        if 'longitude' in data and data['longitude'] is not None:
            lng = data['longitude']
            if not (-180 <= lng <= 180):
                errors.append(f"Invalid longitude: {lng}")
        
        return errors
    
    def _validate_business_rules(self, data: Dict[str, Any]) -> List[str]:
        """Validate business logic rules.
        
        Args:
            data: Property data
            
        Returns:
            List[str]: Error messages
        """
        errors = []
        
        # Price per sqft consistency check
        if all(field in data and data[field] for field in ['price', 'square_feet', 'price_per_sqft']):
            calculated_price_per_sqft = data['price'] / data['square_feet']
            reported_price_per_sqft = data['price_per_sqft']
            
            # Allow 10% variance
            if abs(calculated_price_per_sqft - reported_price_per_sqft) / reported_price_per_sqft > 0.1:
                errors.append(f"Price per sqft inconsistency: calculated {calculated_price_per_sqft:.2f}, reported {reported_price_per_sqft:.2f}")
        
        # Bedroom/bathroom reasonableness
        if 'bedrooms' in data and 'bathrooms' in data:
            bedrooms = data['bedrooms']
            bathrooms = data['bathrooms']
            
            if bedrooms and bathrooms:
                # Generally, bathrooms shouldn't be more than 2x bedrooms
                if bathrooms > bedrooms * 2:
                    errors.append(f"Unusual bedroom/bathroom ratio: {bedrooms} bedrooms, {bathrooms} bathrooms")
        
        # Square feet reasonableness for bedrooms
        if 'bedrooms' in data and 'square_feet' in data:
            bedrooms = data['bedrooms']
            sqft = data['square_feet']
            
            if bedrooms and sqft:
                sqft_per_bedroom = sqft / max(bedrooms, 1)
                
                # Each bedroom should generally have at least 70 sqft
                if sqft_per_bedroom < 70:
                    errors.append(f"Very small space per bedroom: {sqft_per_bedroom:.0f} sqft per bedroom")
                
                # Each bedroom shouldn't generally exceed 2000 sqft
                if sqft_per_bedroom > 2000:
                    errors.append(f"Very large space per bedroom: {sqft_per_bedroom:.0f} sqft per bedroom")
        
        # Year built reasonableness
        if 'year_built' in data and data['year_built']:
            year = data['year_built']
            current_year = datetime.now().year
            
            if year > current_year:
                errors.append(f"Future year built: {year}")
        
        return errors
    
    def _is_valid_state_name(self, state: str) -> bool:
        """Check if a state name is valid (full name or abbreviation).
        
        Args:
            state: State name or abbreviation
            
        Returns:
            bool: True if valid state
        """
        valid_states = {
            'AL', 'ALABAMA', 'AK', 'ALASKA', 'AZ', 'ARIZONA', 'AR', 'ARKANSAS',
            'CA', 'CALIFORNIA', 'CO', 'COLORADO', 'CT', 'CONNECTICUT', 'DE', 'DELAWARE',
            'FL', 'FLORIDA', 'GA', 'GEORGIA', 'HI', 'HAWAII', 'ID', 'IDAHO',
            'IL', 'ILLINOIS', 'IN', 'INDIANA', 'IA', 'IOWA', 'KS', 'KANSAS',
            'KY', 'KENTUCKY', 'LA', 'LOUISIANA', 'ME', 'MAINE', 'MD', 'MARYLAND',
            'MA', 'MASSACHUSETTS', 'MI', 'MICHIGAN', 'MN', 'MINNESOTA', 'MS', 'MISSISSIPPI',
            'MO', 'MISSOURI', 'MT', 'MONTANA', 'NE', 'NEBRASKA', 'NV', 'NEVADA',
            'NH', 'NEW HAMPSHIRE', 'NJ', 'NEW JERSEY', 'NM', 'NEW MEXICO', 'NY', 'NEW YORK',
            'NC', 'NORTH CAROLINA', 'ND', 'NORTH DAKOTA', 'OH', 'OHIO', 'OK', 'OKLAHOMA',
            'OR', 'OREGON', 'PA', 'PENNSYLVANIA', 'RI', 'RHODE ISLAND', 'SC', 'SOUTH CAROLINA',
            'SD', 'SOUTH DAKOTA', 'TN', 'TENNESSEE', 'TX', 'TEXAS', 'UT', 'UTAH',
            'VT', 'VERMONT', 'VA', 'VIRGINIA', 'WA', 'WASHINGTON', 'WV', 'WEST VIRGINIA',
            'WI', 'WISCONSIN', 'WY', 'WYOMING'
        }
        
        return state.upper() in valid_states
    
    def validate_batch(self, properties: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate a batch of properties and return summary statistics.
        
        Args:
            properties: List of property data dictionaries
            
        Returns:
            Dict[str, Any]: Validation summary
        """
        total_count = len(properties)
        valid_count = 0
        all_errors = []
        
        for i, property_data in enumerate(properties):
            is_valid, errors = self.validate_property_data(property_data)
            
            if is_valid:
                valid_count += 1
            else:
                all_errors.extend([f"Property {i}: {error}" for error in errors])
        
        return {
            'total_properties': total_count,
            'valid_properties': valid_count,
            'invalid_properties': total_count - valid_count,
            'validation_rate': valid_count / total_count if total_count > 0 else 0,
            'errors': all_errors
        }
    
    def get_data_quality_score(self, property_data: Dict[str, Any]) -> float:
        """Calculate a data quality score (0-100) for a property.
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            float: Quality score from 0 to 100
        """
        score = 0
        max_score = 100
        
        # Required fields (40 points)
        required_score = 0
        for field in self.required_fields:
            if field in property_data and property_data[field]:
                required_score += 20
        score += min(required_score, 40)
        
        # Important optional fields (30 points)
        important_fields = ['price', 'bedrooms', 'bathrooms', 'square_feet', 'street_address', 'city', 'state']
        optional_score = 0
        for field in important_fields:
            if field in property_data and property_data[field] is not None:
                optional_score += 4.3  # ~30/7
        score += min(optional_score, 30)
        
        # Additional details (20 points)
        detail_fields = ['description', 'year_built', 'lot_size', 'images', 'features']
        detail_score = 0
        for field in detail_fields:
            if field in property_data and property_data[field]:
                detail_score += 4  # 20/5
        score += min(detail_score, 20)
        
        # Coordinates (10 points)
        if 'latitude' in property_data and 'longitude' in property_data:
            if property_data['latitude'] and property_data['longitude']:
                score += 10
        
        return min(score, max_score)

