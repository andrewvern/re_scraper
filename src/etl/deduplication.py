"""Deduplication engine for identifying and handling duplicate properties."""

import hashlib
import logging
from typing import Dict, Any, Optional, List, Tuple
from difflib import SequenceMatcher
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class DeduplicationEngine:
    """Engine for detecting and handling duplicate property records.
    
    This version uses a simple JSON file to store property fingerprints instead of a database.
    In production, this would be replaced with a proper database for better performance and reliability.
    """
    
    def __init__(self, data_dir: str = None):
        """Initialize the deduplication engine.
        
        Args:
            data_dir: Optional directory to store fingerprint data, defaults to temp directory
        """
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            import tempfile
            self.data_dir = Path(tempfile.gettempdir()) / "re_scraper"
            
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.fingerprint_file = self.data_dir / "property_fingerprints.json"
        self._load_fingerprints()
        self.similarity_threshold = 0.85  # Minimum similarity for duplicate detection
        
        # Weight different fields for similarity calculation
        self.field_weights = {
            'street_address': 0.30,
            'city': 0.15,
            'state': 0.10,
            'zip_code': 0.15,
            'bedrooms': 0.10,
            'bathrooms': 0.10,
            'square_feet': 0.10
        }
    
    def is_duplicate(self, property_data: Dict[str, Any]) -> bool:
        """Check if a property is a duplicate of an existing record.
        
        Args:
            property_data: Property data to check
            
        Returns:
            bool: True if the property is a duplicate
        """
        try:
            # First check: exact external ID match within same data source
            if self._is_exact_duplicate(property_data):
                logger.debug(f"Found exact duplicate: {property_data.get('external_id')}")
                return True
            
            # Second check: address-based similarity matching
            similar_properties = self._find_similar_properties(property_data)
            
            for similar_property in similar_properties:
                similarity_score = self._calculate_similarity(property_data, similar_property)
                
                if similarity_score >= self.similarity_threshold:
                    logger.debug(f"Found similar duplicate with score {similarity_score:.3f}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {e}")
            return False
    
    def _load_fingerprints(self):
        """Load property fingerprints from JSON file."""
        if self.fingerprint_file.exists():
            try:
                with open(self.fingerprint_file, 'r') as f:
                    self.fingerprints = set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading fingerprints: {e}")
                self.fingerprints = set()
        else:
            self.fingerprints = set()
    
    def _save_fingerprints(self):
        """Save property fingerprints to JSON file."""
        try:
            with open(self.fingerprint_file, 'w') as f:
                json.dump(list(self.fingerprints), f)
        except Exception as e:
            logger.error(f"Error saving fingerprints: {e}")

    def _is_exact_duplicate(self, property_data: Dict[str, Any]) -> bool:
        """Check for exact duplicates based on fingerprint.
        
        Args:
            property_data: Property data to check
            
        Returns:
            bool: True if exact duplicate exists
        """
        fingerprint = self.create_address_hash(property_data)
        
        if fingerprint in self.fingerprints:
            return True
        
        self.fingerprints.add(fingerprint)
        self._save_fingerprints()
        return False
    
    def _find_similar_properties(self, property_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find similar properties within the current batch.
        Since we're not using a database, this will return an empty list.
        In production, this would query similar properties from a database.
        
        Args:
            property_data: Property data to find similarities for
            
        Returns:
            List[Dict[str, Any]]: List of potentially similar properties
        """
        return []  # No similar properties without database
    
    def _calculate_similarity(self, property1: Dict[str, Any], property2: Dict[str, Any]) -> float:
        """Calculate similarity score between two properties.
        
        Args:
            property1: First property data
            property2: Second property data
            
        Returns:
            float: Similarity score between 0 and 1
        """
        total_score = 0.0
        total_weight = 0.0
        
        for field, weight in self.field_weights.items():
            field_similarity = self._calculate_field_similarity(
                property1.get(field), 
                property2.get(field),
                field
            )
            
            if field_similarity is not None:
                total_score += field_similarity * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _calculate_field_similarity(self, value1: Any, value2: Any, field_name: str) -> Optional[float]:
        """Calculate similarity between two field values.
        
        Args:
            value1: First value
            value2: Second value
            field_name: Name of the field being compared
            
        Returns:
            Optional[float]: Similarity score or None if comparison not possible
        """
        if value1 is None or value2 is None:
            return None
        
        # String fields (addresses, city, etc.)
        if field_name in ['street_address', 'city', 'state', 'zip_code']:
            return self._string_similarity(str(value1), str(value2))
        
        # Numeric fields
        elif field_name in ['bedrooms', 'bathrooms', 'square_feet', 'price']:
            return self._numeric_similarity(value1, value2, field_name)
        
        # Default: exact match
        else:
            return 1.0 if value1 == value2 else 0.0
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using sequence matching.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            float: Similarity score between 0 and 1
        """
        if not str1 or not str2:
            return 0.0
        
        # Normalize strings
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        if str1 == str2:
            return 1.0
        
        # Use SequenceMatcher for fuzzy string matching
        return SequenceMatcher(None, str1, str2).ratio()
    
    def _numeric_similarity(self, num1: Any, num2: Any, field_name: str) -> float:
        """Calculate numeric similarity with field-specific tolerance.
        
        Args:
            num1: First number
            num2: Second number
            field_name: Name of the field
            
        Returns:
            float: Similarity score between 0 and 1
        """
        try:
            n1 = float(num1)
            n2 = float(num2)
        except (TypeError, ValueError):
            return 0.0
        
        if n1 == n2:
            return 1.0
        
        # Define tolerance levels for different fields
        tolerance_map = {
            'bedrooms': 0,  # Exact match required
            'bathrooms': 0.5,  # 0.5 bathroom difference allowed
            'square_feet': 0.1,  # 10% difference allowed
            'price': 0.05  # 5% difference allowed
        }
        
        tolerance = tolerance_map.get(field_name, 0.1)
        
        if field_name in ['bedrooms']:
            # For discrete values, check exact match
            return 1.0 if abs(n1 - n2) <= tolerance else 0.0
        else:
            # For continuous values, calculate proportional similarity
            if n1 == 0 and n2 == 0:
                return 1.0
            
            max_val = max(abs(n1), abs(n2))
            if max_val == 0:
                return 1.0
            
            difference_ratio = abs(n1 - n2) / max_val
            
            if difference_ratio <= tolerance:
                return 1.0
            else:
                # Gradual decrease in similarity
                return max(0.0, 1.0 - (difference_ratio - tolerance) / (1.0 - tolerance))
    
    def find_duplicates_in_batch(self, properties: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        """Find duplicates within a batch of properties.
        
        Args:
            properties: List of property data dictionaries
            
        Returns:
            Dict[str, List[int]]: Groups of duplicate property indices
        """
        duplicate_groups = {}
        processed_indices = set()
        
        for i, prop1 in enumerate(properties):
            if i in processed_indices:
                continue
            
            duplicates = [i]
            
            for j, prop2 in enumerate(properties[i+1:], start=i+1):
                if j in processed_indices:
                    continue
                
                similarity = self._calculate_similarity(prop1, prop2)
                
                if similarity >= self.similarity_threshold:
                    duplicates.append(j)
                    processed_indices.add(j)
            
            if len(duplicates) > 1:
                group_key = f"group_{len(duplicate_groups)}"
                duplicate_groups[group_key] = duplicates
                processed_indices.update(duplicates)
        
        return duplicate_groups
    
    def merge_duplicate_properties(self, properties: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple duplicate property records into one.
        
        Args:
            properties: List of duplicate property data
            
        Returns:
            Dict[str, Any]: Merged property data
        """
        if not properties:
            return {}
        
        if len(properties) == 1:
            return properties[0]
        
        # Start with the first property as base
        merged = properties[0].copy()
        
        for prop in properties[1:]:
            merged = self._merge_two_properties(merged, prop)
        
        return merged
    
    def _merge_two_properties(self, prop1: Dict[str, Any], prop2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two property records, preferring more complete data.
        
        Args:
            prop1: First property data
            prop2: Second property data
            
        Returns:
            Dict[str, Any]: Merged property data
        """
        merged = prop1.copy()
        
        for key, value in prop2.items():
            # If the field is missing in prop1, take from prop2
            if key not in merged or merged[key] is None or merged[key] == '':
                merged[key] = value
            
            # For certain fields, prefer the more complete or recent data
            elif key in ['description'] and value and len(str(value)) > len(str(merged[key])):
                merged[key] = value
            
            elif key in ['images', 'features'] and value:
                # Merge lists and dictionaries
                if isinstance(merged[key], list) and isinstance(value, list):
                    merged[key] = list(set(merged[key] + value))
                elif isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key].update(value)
            
            elif key in ['price', 'rent_estimate'] and value:
                # For prices, take the more recent or non-zero value
                if not merged[key] or merged[key] == 0:
                    merged[key] = value
        
        return merged
    
    def get_duplicate_statistics(self, properties: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about duplicates in a dataset.
        
        Args:
            properties: List of property data
            
        Returns:
            Dict[str, Any]: Duplicate statistics
        """
        total_properties = len(properties)
        duplicate_groups = self.find_duplicates_in_batch(properties)
        
        duplicate_count = sum(len(group) for group in duplicate_groups.values())
        unique_count = total_properties - duplicate_count + len(duplicate_groups)
        
        return {
            'total_properties': total_properties,
            'duplicate_groups': len(duplicate_groups),
            'total_duplicates': duplicate_count,
            'unique_properties': unique_count,
            'duplication_rate': duplicate_count / total_properties if total_properties > 0 else 0
        }
    
    def create_address_hash(self, property_data: Dict[str, Any]) -> str:
        """Create a hash for address-based deduplication.
        
        Args:
            property_data: Property data
            
        Returns:
            str: Address hash
        """
        address_components = [
            str(property_data.get('street_address', '')).lower().strip(),
            str(property_data.get('city', '')).lower().strip(),
            str(property_data.get('state', '')).lower().strip(),
            str(property_data.get('zip_code', '')).strip()
        ]
        
        # Remove common variations
        normalized_address = ' '.join(address_components)
        normalized_address = normalized_address.replace('street', 'st')
        normalized_address = normalized_address.replace('avenue', 'ave')
        normalized_address = normalized_address.replace('boulevard', 'blvd')
        normalized_address = normalized_address.replace('drive', 'dr')
        normalized_address = normalized_address.replace('road', 'rd')
        
        return hashlib.md5(normalized_address.encode()).hexdigest()

