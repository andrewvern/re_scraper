"""Zillow scraper implementation."""

import re
import json
from typing import Dict, Any, Generator, Optional, List
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScrapingError
from ..models.property_models import DataSource, PropertyType


class ZillowScraper(BaseScraper):
    """Scraper for Zillow real estate platform."""
    
    def __init__(self):
        super().__init__(DataSource.ZILLOW)
        self.base_url = "https://www.zillow.com"
        self.search_url = "https://www.zillow.com/homes/for_sale"
        
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
        page = search_criteria.get('page', 1)
        
        # Build URL with location
        if location:
            encoded_location = quote_plus(location)
            url = f"{self.search_url}/{encoded_location}_rb/"
        else:
            url = f"{self.search_url}/"
        
        # Build filter parameters
        filters = []
        
        if min_price:
            filters.append(f"price_min:{int(min_price)}")
        if max_price:
            filters.append(f"price_max:{int(max_price)}")
        if bedrooms:
            filters.append(f"beds_min:{bedrooms}")
        if bathrooms:
            filters.append(f"baths_min:{bathrooms}")
            
        # Add pagination
        if page > 1:
            filters.append(f"p:{page}")
        
        if filters:
            url += "?" + "&".join(filters)
        
        return url
    
    def _parse_property_type(self, property_type_text: str) -> PropertyType:
        """Parse Zillow property type text to our enum.
        
        Args:
            property_type_text: Zillow property type text
            
        Returns:
            PropertyType: Mapped property type
        """
        if not property_type_text:
            return PropertyType.OTHER
            
        text_lower = property_type_text.lower()
        
        if 'house' in text_lower or 'single' in text_lower:
            return PropertyType.HOUSE
        elif 'condo' in text_lower:
            return PropertyType.CONDO
        elif 'townhouse' in text_lower or 'townhome' in text_lower:
            return PropertyType.TOWNHOUSE
        elif 'apartment' in text_lower:
            return PropertyType.APARTMENT
        elif 'multi' in text_lower or 'duplex' in text_lower:
            return PropertyType.MULTI_FAMILY
        elif 'land' in text_lower or 'lot' in text_lower:
            return PropertyType.LAND
        else:
            return PropertyType.OTHER
    
    def _extract_property_data_from_card(self, property_card) -> Dict[str, Any]:
        """Extract property data from a property card element.
        
        Args:
            property_card: BeautifulSoup element representing a property card
            
        Returns:
            Dict[str, Any]: Normalized property data
        """
        try:
            property_data = {
                'data_source': DataSource.ZILLOW,
            }
            
            # Extract property URL and ID
            link_elem = property_card.select_one('a[href*="/homedetails/"]')
            if link_elem:
                relative_url = link_elem.get('href', '')
                property_data['listing_url'] = urljoin(self.base_url, relative_url)
                
                # Extract property ID from URL
                zpid_match = re.search(r'/(\d+)_zpid/', relative_url)
                if zpid_match:
                    property_data['external_id'] = zpid_match.group(1)
            
            # Extract price
            price_elem = property_card.select_one('.list-card-price')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                property_data['price'] = self.clean_price(price_text)
            
            # Extract address
            address_elem = property_card.select_one('.list-card-addr')
            if address_elem:
                address_text = address_elem.get_text(strip=True)
                property_data['street_address'] = address_text
                
                # Try to parse city, state from address
                address_parts = address_text.split(',')
                if len(address_parts) >= 2:
                    property_data['city'] = address_parts[-2].strip()
                    state_zip = address_parts[-1].strip().split()
                    if state_zip:
                        property_data['state'] = state_zip[0]
                        if len(state_zip) > 1:
                            property_data['zip_code'] = state_zip[1]
            
            # Extract bed/bath/sqft info
            details_elem = property_card.select_one('.list-card-details')
            if details_elem:
                details_text = details_elem.get_text(strip=True)
                
                # Parse beds
                bed_match = re.search(r'(\d+)\s*(?:bd|bed)', details_text, re.IGNORECASE)
                if bed_match:
                    property_data['bedrooms'] = int(bed_match.group(1))
                
                # Parse baths
                bath_match = re.search(r'([\d.]+)\s*(?:ba|bath)', details_text, re.IGNORECASE)
                if bath_match:
                    property_data['bathrooms'] = float(bath_match.group(1))
                
                # Parse square feet
                sqft_match = re.search(r'([\d,]+)\s*(?:sqft|sq\.?\s*ft)', details_text, re.IGNORECASE)
                if sqft_match:
                    sqft_text = sqft_match.group(1).replace(',', '')
                    property_data['square_feet'] = int(sqft_text)
            
            # Extract property type
            type_elem = property_card.select_one('.list-card-type')
            if type_elem:
                type_text = type_elem.get_text(strip=True)
                property_data['property_type'] = self._parse_property_type(type_text)
            else:
                property_data['property_type'] = PropertyType.HOUSE  # Default assumption
            
            # Extract listing status
            status_elem = property_card.select_one('.list-card-status')
            if status_elem:
                status_text = status_elem.get_text(strip=True).lower()
                if 'pending' in status_text:
                    property_data['listing_status'] = 'pending'
                elif 'sold' in status_text:
                    property_data['listing_status'] = 'sold'
                else:
                    property_data['listing_status'] = 'active'
            else:
                property_data['listing_status'] = 'active'
            
            # Extract images
            img_elem = property_card.select_one('.list-card-img img')
            if img_elem:
                img_src = img_elem.get('src') or img_elem.get('data-src')
                if img_src:
                    property_data['images'] = [img_src]
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error extracting property data from card: {e}")
            return {}
    
    def search_properties(self, search_criteria: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Search for properties on Zillow.
        
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
                self.logger.info(f"Searching Zillow page {page}: {url}")
                
                response = self.make_request(url)
                soup = self.parse_html(response.text)
                
                # Find property cards
                property_cards = soup.select('.list-card-info')
                
                if not property_cards:
                    self.logger.info("No property cards found on page")
                    # Try alternative selectors
                    property_cards = soup.select('.list-card')
                    if not property_cards:
                        self.logger.info("No more properties found")
                        break
                
                for card in property_cards:
                    if results_count >= max_results:
                        break
                    
                    try:
                        property_data = self._extract_property_data_from_card(card)
                        if property_data and property_data.get('external_id'):
                            yield property_data
                            results_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error processing property card: {e}")
                        continue
                
                # Check if there are more pages
                next_button = soup.select_one('.zsg-pagination .zsg-pagination-next')
                if not next_button or 'zsg-disabled' in next_button.get('class', []):
                    self.logger.info("No more pages available")
                    break
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error searching Zillow page {page}: {e}")
                break
    
    def get_property_details(self, property_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific property.
        
        Args:
            property_url: URL of the Zillow property page
            
        Returns:
            Dict[str, Any]: Detailed property data
        """
        try:
            self.logger.info(f"Fetching property details from: {property_url}")
            
            response = self.make_request(property_url)
            soup = self.parse_html(response.text)
            
            property_data = {
                'data_source': DataSource.ZILLOW,
                'listing_url': property_url,
            }
            
            # Extract property ID from URL
            zpid_match = re.search(r'/(\d+)_zpid/', property_url)
            if zpid_match:
                property_data['external_id'] = zpid_match.group(1)
            
            # Extract basic information
            property_data.update(self._parse_property_details(soup))
            
            # Try to extract from structured data
            structured_data = self._extract_structured_data(soup)
            if structured_data:
                property_data.update(structured_data)
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error fetching property details from {property_url}: {e}")
            raise ScrapingError(f"Failed to fetch property details: {e}")
    
    def _parse_property_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse property details from the main property page.
        
        Args:
            soup: BeautifulSoup object of the property page
            
        Returns:
            Dict[str, Any]: Parsed property data
        """
        property_data = {}
        
        # Extract price
        price_elem = soup.select_one('.notranslate')
        if price_elem:
            property_data['price'] = self.clean_price(price_elem.get_text(strip=True))
        
        # Extract address
        address_elem = soup.select_one('h1.ds-address-container')
        if address_elem:
            address_text = address_elem.get_text(strip=True)
            property_data['street_address'] = address_text
        
        # Extract bed/bath/sqft from summary
        summary_elem = soup.select_one('.ds-bed-bath-living-area')
        if summary_elem:
            summary_text = summary_elem.get_text(strip=True)
            
            # Parse beds
            bed_match = re.search(r'(\d+)\s*(?:bd|bed)', summary_text, re.IGNORECASE)
            if bed_match:
                property_data['bedrooms'] = int(bed_match.group(1))
            
            # Parse baths
            bath_match = re.search(r'([\d.]+)\s*(?:ba|bath)', summary_text, re.IGNORECASE)
            if bath_match:
                property_data['bathrooms'] = float(bath_match.group(1))
            
            # Parse square feet
            sqft_match = re.search(r'([\d,]+)\s*(?:sqft|sq\.?\s*ft)', summary_text, re.IGNORECASE)
            if sqft_match:
                sqft_text = sqft_match.group(1).replace(',', '')
                property_data['square_feet'] = int(sqft_text)
        
        # Extract property type
        type_elem = soup.select_one('.ds-property-type')
        if type_elem:
            type_text = type_elem.get_text(strip=True)
            property_data['property_type'] = self._parse_property_type(type_text)
        
        # Extract year built
        year_elem = soup.select_one('.ds-year-built')
        if year_elem:
            year_text = year_elem.get_text(strip=True)
            year_match = re.search(r'(\d{4})', year_text)
            if year_match:
                property_data['year_built'] = int(year_match.group(1))
        
        # Extract lot size
        lot_elem = soup.select_one('.ds-lot-size')
        if lot_elem:
            lot_text = lot_elem.get_text(strip=True)
            lot_match = re.search(r'([\d.]+)', lot_text)
            if lot_match:
                property_data['lot_size'] = float(lot_match.group(1))
        
        # Extract description
        description_elem = soup.select_one('.ds-overview-section')
        if description_elem:
            property_data['description'] = description_elem.get_text(strip=True)
        
        # Extract Zestimate
        zestimate_elem = soup.select_one('.zestimate')
        if zestimate_elem:
            zestimate_text = zestimate_elem.get_text(strip=True)
            property_data['zestimate'] = self.clean_price(zestimate_text)
        
        return property_data
    
    def _extract_structured_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract data from JSON-LD structured data.
        
        Args:
            soup: BeautifulSoup object of the property page
            
        Returns:
            Dict[str, Any]: Structured data
        """
        property_data = {}
        
        # Find JSON-LD scripts
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
                if isinstance(data, list):
                    data = data[0] if data else {}
                
                if data.get('@type') == 'Product':
                    # Extract from product schema
                    property_data['description'] = data.get('description', '')
                    
                    offers = data.get('offers', {})
                    if offers:
                        price = offers.get('price')
                        if price:
                            property_data['price'] = float(price)
                
                break
                
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        
        return property_data

