"""Redfin scraper implementation."""

import re
import json
from typing import Dict, Any, Generator, Optional, List
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScrapingError
from ..models.property_models import DataSource, PropertyType


class RedfinScraper(BaseScraper):
    """Scraper for Redfin real estate platform."""
    
    def __init__(self):
        super().__init__(DataSource.REDFIN)
        self.base_url = "https://www.redfin.com"
        self.search_url = "https://www.redfin.com/stingray/api/gis"
        
    def _build_search_url(self, search_criteria: Dict[str, Any]) -> str:
        """Build search URL with parameters.
        
        Args:
            search_criteria: Search parameters
            
        Returns:
            str: Complete search URL
        """
        # Extract search parameters
        location = search_criteria.get('location', '')
        min_price = search_criteria.get('min_price')
        max_price = search_criteria.get('max_price')
        bedrooms = search_criteria.get('bedrooms')
        bathrooms = search_criteria.get('bathrooms')
        property_types = search_criteria.get('property_types', [])
        
        # Base search parameters
        params = {
            'al': '1',  # Include active listings
            'market': 'san_francisco',  # Default market, should be dynamic
            'num_homes': '350',  # Results per request
            'page_number': '1',
            'sf': '1,2,3,5,6,7',  # Property types
            'status': '9',  # For sale
            'uipt': '1,2,3,4,5,6,7,8',  # UI property types
            'v': '8'  # API version
        }
        
        # Add price filters
        if min_price:
            params['min_price'] = str(int(min_price))
        if max_price:
            params['max_price'] = str(int(max_price))
            
        # Add bedroom filter
        if bedrooms:
            params['min_beds'] = str(bedrooms)
            
        # Add bathroom filter
        if bathrooms:
            params['min_baths'] = str(bathrooms)
        
        # Build query string
        query_params = '&'.join([f"{k}={v}" for k, v in params.items()])
        
        return f"{self.search_url}?{query_params}"
    
    def _parse_property_type(self, property_type_code: int) -> PropertyType:
        """Parse Redfin property type code to our enum.
        
        Args:
            property_type_code: Redfin property type code
            
        Returns:
            PropertyType: Mapped property type
        """
        type_mapping = {
            1: PropertyType.HOUSE,
            2: PropertyType.CONDO,
            3: PropertyType.TOWNHOUSE,
            4: PropertyType.MULTI_FAMILY,
            5: PropertyType.LAND,
            6: PropertyType.OTHER,
            7: PropertyType.OTHER,
            8: PropertyType.OTHER
        }
        return type_mapping.get(property_type_code, PropertyType.OTHER)
    
    def _extract_property_data(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and normalize property data from Redfin API response.
        
        Args:
            property_data: Raw property data from API
            
        Returns:
            Dict[str, Any]: Normalized property data
        """
        try:
            # Basic property information
            property_info = {
                'external_id': str(property_data.get('property_id', '')),
                'data_source': DataSource.REDFIN,
                'property_type': self._parse_property_type(property_data.get('property_type', 1)),
                'bedrooms': property_data.get('beds'),
                'bathrooms': property_data.get('baths'),
                'square_feet': property_data.get('sqft'),
                'lot_size': property_data.get('lot_size'),
                'year_built': property_data.get('year_built'),
                'price': property_data.get('price'),
                'price_per_sqft': property_data.get('price_per_sqft'),
                'description': property_data.get('listing_remarks', ''),
            }
            
            # Location information
            location_data = {
                'street_address': property_data.get('street_line', ''),
                'city': property_data.get('city', ''),
                'state': property_data.get('state_or_province', ''),
                'zip_code': property_data.get('postal_code', ''),
                'latitude': property_data.get('lat'),
                'longitude': property_data.get('lng'),
                'neighborhood': property_data.get('market_display_name', ''),
                'county': property_data.get('county_display_name', '')
            }
            
            # Listing information
            listing_info = {
                'listing_status': 'active',  # Redfin search typically returns active listings
                'list_price': property_data.get('price'),
                'days_on_market': property_data.get('dom'),
                'listing_url': f"{self.base_url}{property_data.get('url', '')}",
                'mls_number': property_data.get('mls_id', ''),
            }
            
            # Property features
            features = {}
            if property_data.get('garage'):
                features['garage'] = True
                property_info['garage_spaces'] = 1  # Default assumption
            
            if property_data.get('pool'):
                features['pool'] = True
                property_info['pool'] = True
                
            if property_data.get('fireplace'):
                features['fireplace'] = True
                property_info['fireplace'] = True
            
            # Additional features from property data
            if 'hoa_fee' in property_data:
                features['hoa_fee'] = property_data['hoa_fee']
                
            if 'stories' in property_data:
                property_info['stories'] = property_data['stories']
            
            property_info['features'] = features
            property_info['location'] = location_data
            property_info['listing'] = listing_info
            
            # Images
            images = []
            if 'photo_count' in property_data and property_data['photo_count'] > 0:
                # Redfin image URLs follow a pattern
                base_image_url = property_data.get('photo_url', '')
                if base_image_url:
                    images.append(base_image_url)
            
            property_info['images'] = images
            
            return property_info
            
        except Exception as e:
            self.logger.error(f"Error extracting property data: {e}")
            raise ScrapingError(f"Failed to extract property data: {e}")
    
    def search_properties(self, search_criteria: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Search for properties on Redfin.
        
        Args:
            search_criteria: Dictionary containing search parameters like:
                - location: str (city, state or zipcode)
                - min_price: float
                - max_price: float
                - bedrooms: int
                - bathrooms: int
                - property_types: List[str]
                - max_results: int
                
        Yields:
            Dict[str, Any]: Property data dictionaries
        """
        max_results = search_criteria.get('max_results', 1000)
        results_count = 0
        page = 1
        
        while results_count < max_results:
            try:
                # Update page number in search criteria
                current_criteria = search_criteria.copy()
                current_criteria['page'] = page
                
                url = self._build_search_url(current_criteria)
                self.logger.info(f"Searching Redfin page {page}: {url}")
                
                response = self.make_request(url)
                
                # Redfin returns JSON data
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    self.logger.error("Failed to parse JSON response from Redfin")
                    break
                
                # Extract properties from response
                homes = data.get('homes', [])
                if not homes:
                    self.logger.info("No more properties found")
                    break
                
                for home_data in homes:
                    if results_count >= max_results:
                        break
                    
                    try:
                        property_data = self._extract_property_data(home_data)
                        yield property_data
                        results_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error processing property: {e}")
                        continue
                
                # Check if there are more pages
                if len(homes) < 350:  # Less than full page means last page
                    break
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error searching Redfin page {page}: {e}")
                break
    
    def get_property_details(self, property_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific property.
        
        Args:
            property_url: URL of the Redfin property page
            
        Returns:
            Dict[str, Any]: Detailed property data
        """
        try:
            self.logger.info(f"Fetching property details from: {property_url}")
            
            response = self.make_request(property_url)
            soup = self.parse_html(response.text)
            
            # Extract property details from the page
            property_data = {}
            
            # Try to extract from JSON-LD structured data
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get('@type') == 'Product':
                        property_data.update(self._parse_json_ld(data))
                        break
                except json.JSONDecodeError:
                    continue
            
            # Extract additional details from HTML if JSON-LD not available
            if not property_data:
                property_data = self._parse_property_html(soup, property_url)
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error fetching property details from {property_url}: {e}")
            raise ScrapingError(f"Failed to fetch property details: {e}")
    
    def _parse_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse property data from JSON-LD structured data.
        
        Args:
            json_data: JSON-LD data
            
        Returns:
            Dict[str, Any]: Parsed property data
        """
        property_data = {
            'data_source': DataSource.REDFIN,
            'description': json_data.get('description', ''),
        }
        
        # Extract offers information (price, etc.)
        offers = json_data.get('offers', {})
        if offers and isinstance(offers, dict):
            price_text = offers.get('price', '')
            if price_text:
                property_data['price'] = self.clean_price(price_text)
        
        return property_data
    
    def _parse_property_html(self, soup: BeautifulSoup, property_url: str) -> Dict[str, Any]:
        """Parse property data from HTML when JSON-LD is not available.
        
        Args:
            soup: BeautifulSoup object of the property page
            property_url: URL of the property
            
        Returns:
            Dict[str, Any]: Parsed property data
        """
        property_data = {
            'data_source': DataSource.REDFIN,
            'listing_url': property_url,
        }
        
        # Extract basic information
        property_data['price'] = self.clean_price(
            self.safe_extract_text(soup, '.sale-price .price', '')
        )
        
        property_data['bedrooms'] = self._extract_number(
            self.safe_extract_text(soup, '.beds .value', '')
        )
        
        property_data['bathrooms'] = self._extract_number(
            self.safe_extract_text(soup, '.baths .value', '')
        )
        
        property_data['square_feet'] = self._extract_number(
            self.safe_extract_text(soup, '.sqft .value', '')
        )
        
        # Extract description
        description_elem = soup.select_one('.remarks')
        if description_elem:
            property_data['description'] = description_elem.get_text(strip=True)
        
        # Extract address
        address_elem = soup.select_one('.street-address')
        if address_elem:
            property_data['street_address'] = address_elem.get_text(strip=True)
        
        return property_data
    
    def _extract_number(self, text: str) -> Optional[int]:
        """Extract number from text string.
        
        Args:
            text: Text containing a number
            
        Returns:
            Optional[int]: Extracted number or None
        """
        if not text:
            return None
        
        # Remove non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', text)
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None
