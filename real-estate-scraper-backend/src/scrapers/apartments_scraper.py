"""Apartments.com scraper implementation."""

import re
import json
from typing import Dict, Any, Generator, Optional, List
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScrapingError
from ..models.property_models import DataSource, PropertyType


class ApartmentsScraper(BaseScraper):
    """Scraper for Apartments.com rental platform."""
    
    def __init__(self):
        super().__init__(DataSource.APARTMENTS_COM)
        self.base_url = "https://www.apartments.com"
        self.search_url = "https://www.apartments.com"
        
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
        page = search_criteria.get('page', 1)
        
        # Build URL with location
        if location:
            # Clean location for URL
            location_clean = location.lower().replace(' ', '-').replace(',', '')
            url = f"{self.search_url}/{location_clean}/"
        else:
            url = f"{self.search_url}/apartments/"
        
        # Build query parameters
        params = []
        
        if min_price:
            params.append(f"min={int(min_price)}")
        if max_price:
            params.append(f"max={int(max_price)}")
        if bedrooms:
            if bedrooms == 1:
                params.append("bb=1")
            elif bedrooms == 2:
                params.append("bb=2")
            elif bedrooms == 3:
                params.append("bb=3")
            elif bedrooms >= 4:
                params.append("bb=4")
        
        # Add pagination
        if page > 1:
            params.append(f"p={page}")
        
        if params:
            url += "?" + "&".join(params)
        
        return url
    
    def _extract_property_data_from_card(self, property_card) -> Dict[str, Any]:
        """Extract property data from a property card element.
        
        Args:
            property_card: BeautifulSoup element representing a property card
            
        Returns:
            Dict[str, Any]: Normalized property data
        """
        try:
            property_data = {
                'data_source': DataSource.APARTMENTS_COM,
                'property_type': PropertyType.APARTMENT,  # Default for apartments.com
                'listing_status': 'active',
            }
            
            # Extract property URL and ID
            link_elem = property_card.select_one('a.property-link')
            if link_elem:
                relative_url = link_elem.get('href', '')
                property_data['listing_url'] = urljoin(self.base_url, relative_url)
                
                # Extract property ID from URL or data attributes
                property_id = property_card.get('data-listingid') or property_card.get('data-propertyid')
                if property_id:
                    property_data['external_id'] = str(property_id)
                else:
                    # Try to extract from URL
                    id_match = re.search(r'/(\d+)/?$', relative_url)
                    if id_match:
                        property_data['external_id'] = id_match.group(1)
            
            # Extract property name/title
            name_elem = property_card.select_one('.property-name, .property-title')
            if name_elem:
                property_data['property_name'] = name_elem.get_text(strip=True)
            
            # Extract address
            address_elem = property_card.select_one('.property-address')
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
            
            # Extract price range
            price_elem = property_card.select_one('.property-pricing, .rent-range')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                
                # Handle price ranges like "$1,200 - $1,800"
                price_match = re.search(r'\$?([\d,]+)', price_text)
                if price_match:
                    min_price = self.clean_price(price_match.group(1))
                    property_data['rent_estimate'] = min_price
                    property_data['price'] = min_price  # For apartments, price is rent
                
                # Try to extract max price for ranges
                range_match = re.search(r'\$?([\d,]+)\s*-\s*\$?([\d,]+)', price_text)
                if range_match:
                    min_price = self.clean_price(range_match.group(1))
                    max_price = self.clean_price(range_match.group(2))
                    # Use average of range
                    if min_price and max_price:
                        property_data['rent_estimate'] = (min_price + max_price) / 2
                        property_data['price'] = property_data['rent_estimate']
            
            # Extract bed/bath info
            bed_bath_elem = property_card.select_one('.bed-bath, .property-beds')
            if bed_bath_elem:
                bed_bath_text = bed_bath_elem.get_text(strip=True)
                
                # Parse bedrooms
                bed_match = re.search(r'(\d+)\s*(?:bed|bd|bedroom)', bed_bath_text, re.IGNORECASE)
                if bed_match:
                    property_data['bedrooms'] = int(bed_match.group(1))
                elif 'studio' in bed_bath_text.lower():
                    property_data['bedrooms'] = 0
                
                # Parse bathrooms
                bath_match = re.search(r'([\d.]+)\s*(?:bath|ba|bathroom)', bed_bath_text, re.IGNORECASE)
                if bath_match:
                    property_data['bathrooms'] = float(bath_match.group(1))
            
            # Extract square feet
            sqft_elem = property_card.select_one('.property-sqft, .sqft')
            if sqft_elem:
                sqft_text = sqft_elem.get_text(strip=True)
                sqft_match = re.search(r'([\d,]+)', sqft_text)
                if sqft_match:
                    sqft_clean = sqft_match.group(1).replace(',', '')
                    property_data['square_feet'] = int(sqft_clean)
            
            # Extract availability
            availability_elem = property_card.select_one('.availability, .available-date')
            if availability_elem:
                availability_text = availability_elem.get_text(strip=True)
                if 'now' in availability_text.lower() or 'available' in availability_text.lower():
                    property_data['available_now'] = True
            
            # Extract amenities/features
            amenities_elem = property_card.select_one('.property-amenities, .amenities')
            if amenities_elem:
                amenities_text = amenities_elem.get_text(strip=True)
                features = {}
                
                if 'pool' in amenities_text.lower():
                    features['pool'] = True
                    property_data['pool'] = True
                if 'gym' in amenities_text.lower() or 'fitness' in amenities_text.lower():
                    features['gym'] = True
                if 'parking' in amenities_text.lower() or 'garage' in amenities_text.lower():
                    features['parking'] = True
                if 'laundry' in amenities_text.lower():
                    features['laundry'] = True
                if 'pet' in amenities_text.lower():
                    features['pet_friendly'] = True
                
                property_data['features'] = features
            
            # Extract images
            img_elem = property_card.select_one('.property-photo img, .photo img')
            if img_elem:
                img_src = img_elem.get('src') or img_elem.get('data-src')
                if img_src:
                    property_data['images'] = [img_src]
            
            # Extract rating if available
            rating_elem = property_card.select_one('.property-rating, .rating')
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                rating_match = re.search(r'([\d.]+)', rating_text)
                if rating_match:
                    property_data['rating'] = float(rating_match.group(1))
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error extracting property data from card: {e}")
            return {}
    
    def search_properties(self, search_criteria: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Search for properties on Apartments.com.
        
        Args:
            search_criteria: Dictionary containing search parameters like:
                - location: str (city, state or zipcode)
                - min_price: float
                - max_price: float
                - bedrooms: int
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
                self.logger.info(f"Searching Apartments.com page {page}: {url}")
                
                response = self.make_request(url)
                soup = self.parse_html(response.text)
                
                # Find property cards - try multiple selectors
                property_cards = soup.select('.property-card, .listingCard, .placard')
                
                if not property_cards:
                    self.logger.info("No property cards found on page")
                    # Try alternative selectors
                    property_cards = soup.select('[data-listingid], [data-propertyid]')
                    if not property_cards:
                        self.logger.info("No more properties found")
                        break
                
                self.logger.info(f"Found {len(property_cards)} property cards on page {page}")
                
                for card in property_cards:
                    if results_count >= max_results:
                        break
                    
                    try:
                        property_data = self._extract_property_data_from_card(card)
                        if property_data and (property_data.get('external_id') or property_data.get('listing_url')):
                            yield property_data
                            results_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error processing property card: {e}")
                        continue
                
                # Check if there are more pages
                next_button = soup.select_one('.next, .paging-next, [aria-label="Next"]')
                if not next_button or 'disabled' in next_button.get('class', []):
                    self.logger.info("No more pages available")
                    break
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error searching Apartments.com page {page}: {e}")
                break
    
    def get_property_details(self, property_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific property.
        
        Args:
            property_url: URL of the Apartments.com property page
            
        Returns:
            Dict[str, Any]: Detailed property data
        """
        try:
            self.logger.info(f"Fetching property details from: {property_url}")
            
            response = self.make_request(property_url)
            soup = self.parse_html(response.text)
            
            property_data = {
                'data_source': DataSource.APARTMENTS_COM,
                'property_type': PropertyType.APARTMENT,
                'listing_url': property_url,
            }
            
            # Extract property details
            property_data.update(self._parse_property_details(soup))
            
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
        
        # Extract property name
        name_elem = soup.select_one('.property-title, .propertyName')
        if name_elem:
            property_data['property_name'] = name_elem.get_text(strip=True)
        
        # Extract address
        address_elem = soup.select_one('.property-address, .propertyAddress')
        if address_elem:
            property_data['street_address'] = address_elem.get_text(strip=True)
        
        # Extract price ranges from unit listings
        unit_cards = soup.select('.unit-card, .unit-listing, .rentInfoDetail')
        prices = []
        bedrooms_list = []
        bathrooms_list = []
        sqft_list = []
        
        for unit in unit_cards:
            # Extract price
            price_elem = unit.select_one('.unit-price, .rent-range, .rentInfo')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price = self.clean_price(price_text)
                if price:
                    prices.append(price)
            
            # Extract bed/bath info
            bed_bath_elem = unit.select_one('.unit-bed-bath, .bed-bath')
            if bed_bath_elem:
                bed_bath_text = bed_bath_elem.get_text(strip=True)
                
                bed_match = re.search(r'(\d+)\s*(?:bed|bd)', bed_bath_text, re.IGNORECASE)
                if bed_match:
                    bedrooms_list.append(int(bed_match.group(1)))
                elif 'studio' in bed_bath_text.lower():
                    bedrooms_list.append(0)
                
                bath_match = re.search(r'([\d.]+)\s*(?:bath|ba)', bed_bath_text, re.IGNORECASE)
                if bath_match:
                    bathrooms_list.append(float(bath_match.group(1)))
            
            # Extract square feet
            sqft_elem = unit.select_one('.unit-sqft, .sqft')
            if sqft_elem:
                sqft_text = sqft_elem.get_text(strip=True)
                sqft_match = re.search(r'([\d,]+)', sqft_text)
                if sqft_match:
                    sqft = int(sqft_match.group(1).replace(',', ''))
                    sqft_list.append(sqft)
        
        # Calculate averages/ranges
        if prices:
            property_data['rent_estimate'] = sum(prices) / len(prices)
            property_data['price'] = property_data['rent_estimate']
            property_data['min_rent'] = min(prices)
            property_data['max_rent'] = max(prices)
        
        if bedrooms_list:
            property_data['min_bedrooms'] = min(bedrooms_list)
            property_data['max_bedrooms'] = max(bedrooms_list)
            property_data['bedrooms'] = max(set(bedrooms_list), key=bedrooms_list.count)  # Most common
        
        if bathrooms_list:
            property_data['min_bathrooms'] = min(bathrooms_list)
            property_data['max_bathrooms'] = max(bathrooms_list)
            property_data['bathrooms'] = max(set(bathrooms_list), key=bathrooms_list.count)
        
        if sqft_list:
            property_data['min_square_feet'] = min(sqft_list)
            property_data['max_square_feet'] = max(sqft_list)
            property_data['square_feet'] = sum(sqft_list) // len(sqft_list)  # Average
        
        # Extract description
        description_elem = soup.select_one('.property-description, .description')
        if description_elem:
            property_data['description'] = description_elem.get_text(strip=True)
        
        # Extract amenities
        amenity_elems = soup.select('.amenity-item, .amenity, .feature-item')
        features = {}
        amenities = []
        
        for amenity in amenity_elems:
            amenity_text = amenity.get_text(strip=True).lower()
            amenities.append(amenity_text)
            
            if 'pool' in amenity_text:
                features['pool'] = True
                property_data['pool'] = True
            if 'gym' in amenity_text or 'fitness' in amenity_text:
                features['gym'] = True
            if 'parking' in amenity_text or 'garage' in amenity_text:
                features['parking'] = True
            if 'laundry' in amenity_text:
                features['laundry'] = True
            if 'pet' in amenity_text:
                features['pet_friendly'] = True
            if 'dishwasher' in amenity_text:
                features['dishwasher'] = True
            if 'air conditioning' in amenity_text or 'a/c' in amenity_text:
                features['air_conditioning'] = True
        
        property_data['features'] = features
        property_data['amenities'] = amenities
        
        # Extract contact information
        phone_elem = soup.select_one('.phone-number, .contact-phone')
        if phone_elem:
            property_data['contact_phone'] = phone_elem.get_text(strip=True)
        
        # Extract rating
        rating_elem = soup.select_one('.property-rating, .rating-value')
        if rating_elem:
            rating_text = rating_elem.get_text(strip=True)
            rating_match = re.search(r'([\d.]+)', rating_text)
            if rating_match:
                property_data['rating'] = float(rating_match.group(1))
        
        # Extract images
        img_elems = soup.select('.property-photo img, .gallery img')
        images = []
        for img in img_elems:
            img_src = img.get('src') or img.get('data-src')
            if img_src and img_src not in images:
                images.append(img_src)
        
        if images:
            property_data['images'] = images
        
        return property_data
