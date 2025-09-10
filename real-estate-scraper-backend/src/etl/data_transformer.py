"""Data transformation module for standardizing and cleaning scraped data."""

import re
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms and standardizes scraped real estate data."""
    
    def __init__(self):
        """Initialize the data transformer."""
        self.price_patterns = [
            r'\$?([\d,]+\.?\d*)',  # $1,500 or 1500
            r'([\d,]+\.?\d*)\s*(?:dollars?|usd|\$)',  # 1500 dollars
        ]
        
        self.sqft_patterns = [
            r'([\d,]+\.?\d*)\s*(?:sq\.?\s*ft\.?|sqft|square\s*feet)',
            r'([\d,]+\.?\d*)\s*sf',
        ]
        
        self.bed_patterns = [
            r'(\d+)\s*(?:bed|bedroom|br|bd)',
            r'(\d+)br',
            r'studio',  # Special case for studio
        ]
        
        self.bath_patterns = [
            r'([\d.]+)\s*(?:bath|bathroom|ba)',
            r'([\d.]+)ba',
        ]
    
    def transform_property_data(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform and standardize a single property record.
        
        Args:
            property_data: Raw property data
            
        Returns:
            Dict[str, Any]: Transformed property data
        """
        transformed = property_data.copy()
        
        try:
            # Clean and standardize text fields
            transformed = self._clean_text_fields(transformed)
            
            # Parse and standardize prices
            transformed = self._standardize_prices(transformed)
            
            # Parse property details from text
            transformed = self._extract_property_details(transformed)
            
            # Standardize address components
            transformed = self._standardize_address(transformed)
            
            # Clean and validate coordinates
            transformed = self._clean_coordinates(transformed)
            
            # Standardize boolean fields
            transformed = self._standardize_booleans(transformed)
            
            # Generate unique identifiers
            transformed = self._generate_identifiers(transformed)
            
            # Clean and format features
            transformed = self._standardize_features(transformed)
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming property data: {e}")
            return property_data
    
    def _clean_text_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize text fields.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with cleaned text fields
        """
        text_fields = ['description', 'street_address', 'city', 'neighborhood', 'county']
        
        for field in text_fields:
            if field in data and data[field]:
                # Convert to string and clean
                text = str(data[field]).strip()
                
                # Remove extra whitespace
                text = re.sub(r'\s+', ' ', text)
                
                # Remove HTML tags if present
                text = re.sub(r'<[^>]+>', '', text)
                
                # Clean up common formatting issues
                text = text.replace('\n', ' ').replace('\t', ' ')
                
                # Title case for addresses and names
                if field in ['street_address', 'city', 'neighborhood', 'county']:
                    text = self._title_case_address(text)
                
                data[field] = text
        
        return data
    
    def _title_case_address(self, address: str) -> str:
        """Apply proper title case to address components.
        
        Args:
            address: Address string
            
        Returns:
            str: Title-cased address
        """
        # Words that should remain lowercase (unless at start)
        lowercase_words = {'of', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'with'}
        
        # Words that should remain uppercase
        uppercase_words = {'NE', 'NW', 'SE', 'SW', 'N', 'S', 'E', 'W', 'US', 'USA'}
        
        words = address.split()
        result = []
        
        for i, word in enumerate(words):
            if word.upper() in uppercase_words:
                result.append(word.upper())
            elif i > 0 and word.lower() in lowercase_words:
                result.append(word.lower())
            else:
                result.append(word.capitalize())
        
        return ' '.join(result)
    
    def _standardize_prices(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and standardize price fields.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with standardized prices
        """
        price_fields = ['price', 'rent_estimate', 'list_price']
        
        for field in price_fields:
            if field in data and data[field] is not None:
                # If already numeric, ensure it's in the right format
                if isinstance(data[field], (int, float)):
                    # Assume prices under 10000 are in thousands
                    if data[field] < 10000 and field != 'rent_estimate':
                        data[field] = data[field] * 1000
                    continue
                
                # Parse from string
                price_text = str(data[field])
                parsed_price = self._parse_price(price_text)
                
                if parsed_price:
                    data[field] = parsed_price
                else:
                    # Try to extract from the text using patterns
                    for pattern in self.price_patterns:
                        match = re.search(pattern, price_text, re.IGNORECASE)
                        if match:
                            try:
                                price_str = match.group(1).replace(',', '')
                                price = float(price_str)
                                
                                # Assume prices under 10000 are in thousands (except rent)
                                if price < 10000 and field != 'rent_estimate':
                                    price = price * 1000
                                
                                data[field] = price
                                break
                            except (ValueError, IndexError):
                                continue
        
        return data
    
    def _parse_price(self, price_text: str) -> Optional[float]:
        """Parse price from text.
        
        Args:
            price_text: Price as text
            
        Returns:
            Optional[float]: Parsed price or None
        """
        if not price_text:
            return None
        
        # Remove common formatting
        cleaned = str(price_text).replace('$', '').replace(',', '').strip()
        
        # Handle ranges (take the first number)
        if '-' in cleaned:
            cleaned = cleaned.split('-')[0].strip()
        
        # Handle "per month" or similar
        if 'per' in cleaned.lower():
            cleaned = cleaned.split('per')[0].strip()
        
        # Handle "K" and "M" suffixes
        if cleaned.endswith('K') or cleaned.endswith('k'):
            try:
                return float(cleaned[:-1]) * 1000
            except ValueError:
                pass
        
        if cleaned.endswith('M') or cleaned.endswith('m'):
            try:
                return float(cleaned[:-1]) * 1000000
            except ValueError:
                pass
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def _extract_property_details(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract property details from description or other text fields.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with extracted details
        """
        # Source text for extraction
        text_sources = []
        for field in ['description', 'title', 'features_text']:
            if field in data and data[field]:
                text_sources.append(str(data[field]).lower())
        
        full_text = ' '.join(text_sources)
        
        if not full_text:
            return data
        
        # Extract bedrooms if not already present
        if 'bedrooms' not in data or not data['bedrooms']:
            bedrooms = self._extract_bedrooms(full_text)
            if bedrooms is not None:
                data['bedrooms'] = bedrooms
        
        # Extract bathrooms if not already present
        if 'bathrooms' not in data or not data['bathrooms']:
            bathrooms = self._extract_bathrooms(full_text)
            if bathrooms is not None:
                data['bathrooms'] = bathrooms
        
        # Extract square feet if not already present
        if 'square_feet' not in data or not data['square_feet']:
            sqft = self._extract_square_feet(full_text)
            if sqft is not None:
                data['square_feet'] = sqft
        
        # Extract features
        features = self._extract_features(full_text)
        if features:
            existing_features = data.get('features', {}) or {}
            existing_features.update(features)
            data['features'] = existing_features
        
        return data
    
    def _extract_bedrooms(self, text: str) -> Optional[int]:
        """Extract number of bedrooms from text.
        
        Args:
            text: Text to search
            
        Returns:
            Optional[int]: Number of bedrooms or None
        """
        # Check for studio first
        if 'studio' in text:
            return 0
        
        for pattern in self.bed_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match and pattern != r'studio':
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_bathrooms(self, text: str) -> Optional[float]:
        """Extract number of bathrooms from text.
        
        Args:
            text: Text to search
            
        Returns:
            Optional[float]: Number of bathrooms or None
        """
        for pattern in self.bath_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_square_feet(self, text: str) -> Optional[int]:
        """Extract square footage from text.
        
        Args:
            text: Text to search
            
        Returns:
            Optional[int]: Square footage or None
        """
        for pattern in self.sqft_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    sqft_str = match.group(1).replace(',', '')
                    return int(float(sqft_str))
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _extract_features(self, text: str) -> Dict[str, bool]:
        """Extract property features from text.
        
        Args:
            text: Text to search
            
        Returns:
            Dict[str, bool]: Extracted features
        """
        features = {}
        
        # Define feature keywords
        feature_keywords = {
            'pool': ['pool', 'swimming pool'],
            'garage': ['garage', 'parking', 'car port', 'carport'],
            'fireplace': ['fireplace', 'fire place'],
            'basement': ['basement', 'cellar'],
            'balcony': ['balcony', 'deck', 'patio'],
            'hardwood_floors': ['hardwood', 'wood floor', 'hardwood floor'],
            'stainless_steel': ['stainless steel', 'stainless'],
            'granite': ['granite', 'granite counter'],
            'washer_dryer': ['washer', 'dryer', 'laundry'],
            'dishwasher': ['dishwasher'],
            'air_conditioning': ['air conditioning', 'a/c', 'ac', 'central air'],
            'heating': ['heating', 'heat', 'furnace'],
            'walk_in_closet': ['walk-in closet', 'walk in closet'],
            'updated_kitchen': ['updated kitchen', 'modern kitchen', 'new kitchen'],
            'pet_friendly': ['pet friendly', 'pets allowed', 'dog friendly', 'cat friendly'],
            'furnished': ['furnished', 'fully furnished'],
            'gym': ['gym', 'fitness', 'exercise room'],
            'elevator': ['elevator', 'lift']
        }
        
        for feature, keywords in feature_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    features[feature] = True
                    break
        
        return features
    
    def _standardize_address(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize address components.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with standardized address
        """
        # Standardize state
        if 'state' in data and data['state']:
            data['state'] = self._standardize_state(str(data['state']))
        
        # Clean zip code
        if 'zip_code' in data and data['zip_code']:
            data['zip_code'] = self._clean_zip_code(str(data['zip_code']))
        
        # Standardize street address
        if 'street_address' in data and data['street_address']:
            data['street_address'] = self._standardize_street_address(str(data['street_address']))
        
        return data
    
    def _standardize_state(self, state: str) -> str:
        """Standardize state to 2-letter abbreviation.
        
        Args:
            state: State name or abbreviation
            
        Returns:
            str: Standardized state abbreviation
        """
        state = state.strip().upper()
        
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
        """Clean and standardize zip code.
        
        Args:
            zip_code: Raw zip code
            
        Returns:
            str: Cleaned zip code
        """
        # Extract 5-digit zip code
        match = re.search(r'(\d{5})', zip_code)
        return match.group(1) if match else zip_code.strip()
    
    def _standardize_street_address(self, address: str) -> str:
        """Standardize street address formatting.
        
        Args:
            address: Raw street address
            
        Returns:
            str: Standardized address
        """
        # Common abbreviation standardization
        abbreviations = {
            r'\bstreet\b': 'St',
            r'\bavenue\b': 'Ave',
            r'\bboulevard\b': 'Blvd',
            r'\bdrive\b': 'Dr',
            r'\broad\b': 'Rd',
            r'\blane\b': 'Ln',
            r'\bcircle\b': 'Cir',
            r'\bcourt\b': 'Ct',
            r'\bplace\b': 'Pl',
            r'\bterrace\b': 'Ter',
            r'\bparkway\b': 'Pkwy',
            r'\bnorth\b': 'N',
            r'\bsouth\b': 'S',
            r'\beast\b': 'E',
            r'\bwest\b': 'W',
            r'\bnortheast\b': 'NE',
            r'\bnorthwest\b': 'NW',
            r'\bsoutheast\b': 'SE',
            r'\bsouthwest\b': 'SW'
        }
        
        standardized = address
        for pattern, replacement in abbreviations.items():
            standardized = re.sub(pattern, replacement, standardized, flags=re.IGNORECASE)
        
        return standardized.strip()
    
    def _clean_coordinates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and validate coordinates.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with cleaned coordinates
        """
        for coord_field in ['latitude', 'longitude']:
            if coord_field in data and data[coord_field] is not None:
                try:
                    coord = float(data[coord_field])
                    
                    # Validate ranges
                    if coord_field == 'latitude' and not (-90 <= coord <= 90):
                        data[coord_field] = None
                    elif coord_field == 'longitude' and not (-180 <= coord <= 180):
                        data[coord_field] = None
                    else:
                        data[coord_field] = coord
                        
                except (ValueError, TypeError):
                    data[coord_field] = None
        
        return data
    
    def _standardize_booleans(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize boolean fields.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with standardized booleans
        """
        boolean_fields = ['pool', 'fireplace', 'basement', 'garage', 'pet_friendly']
        
        for field in boolean_fields:
            if field in data and data[field] is not None:
                value = data[field]
                
                if isinstance(value, bool):
                    continue
                elif isinstance(value, str):
                    data[field] = value.lower() in ['true', '1', 'yes', 'on', 'y']
                else:
                    data[field] = bool(value)
        
        return data
    
    def _generate_identifiers(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate unique identifiers for the property.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with generated identifiers
        """
        # Generate a hash-based identifier for deduplication
        address_components = [
            str(data.get('street_address', '')),
            str(data.get('city', '')),
            str(data.get('state', '')),
            str(data.get('zip_code', ''))
        ]
        
        address_string = '|'.join(address_components).lower().strip()
        if address_string:
            address_hash = hashlib.md5(address_string.encode()).hexdigest()[:12]
            data['address_hash'] = address_hash
        
        return data
    
    def _standardize_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize features data.
        
        Args:
            data: Property data
            
        Returns:
            Dict[str, Any]: Data with standardized features
        """
        if 'features' in data and data['features']:
            features = data['features']
            
            # Ensure it's a dictionary
            if isinstance(features, list):
                # Convert list to dictionary
                feature_dict = {}
                for item in features:
                    if isinstance(item, str):
                        feature_dict[item.lower().replace(' ', '_')] = True
                    elif isinstance(item, dict):
                        feature_dict.update(item)
                data['features'] = feature_dict
            elif isinstance(features, str):
                # Parse string features
                feature_dict = {}
                feature_list = features.split(',')
                for item in feature_list:
                    item = item.strip().lower().replace(' ', '_')
                    if item:
                        feature_dict[item] = True
                data['features'] = feature_dict
        
        return data
