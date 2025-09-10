"""Base scraper class with anti-detection measures and common functionality."""

import random
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generator
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from fake_useragent import UserAgent

from ..config import settings
from ..models.property_models import DataSource


class ScrapingError(Exception):
    """Custom exception for scraping errors."""
    pass


class RateLimitError(ScrapingError):
    """Exception raised when rate limit is exceeded."""
    pass


class BaseScraper(ABC):
    """Base scraper class with anti-detection and common functionality."""
    
    def __init__(self, data_source: DataSource):
        """Initialize the base scraper.
        
        Args:
            data_source: The data source this scraper targets
        """
        self.data_source = data_source
        self.logger = logging.getLogger(f"{__name__}.{data_source}")
        
        # User agent rotation
        self.ua = UserAgent()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        
        # Rate limiting
        self.requests_per_minute = settings.scraper.requests_per_minute
        self.delay_between_requests = settings.scraper.delay_between_requests
        self.last_request_time = 0
        self.request_count = 0
        self.start_time = time.time()
        
        # Browser setup
        self.driver = None
        self.session = None
        
        # Proxy setup
        self.proxies = []
        self.current_proxy_index = 0
        if settings.scraper.use_proxy and settings.scraper.proxy_list:
            self.proxies = settings.scraper.proxy_list
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        if settings.scraper.rotate_user_agents:
            return random.choice(self.user_agents)
        return self.user_agents[0]
    
    def _get_next_proxy(self) -> Optional[Dict[str, str]]:
        """Get the next proxy in rotation."""
        if not self.proxies:
            return None
        
        proxy = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
        
        return {
            'http': proxy,
            'https': proxy
        }
    
    def _apply_rate_limiting(self):
        """Apply rate limiting to prevent being blocked."""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self.start_time >= 60:
            self.request_count = 0
            self.start_time = current_time
        
        # Check if we've exceeded rate limit
        if self.request_count >= self.requests_per_minute:
            sleep_time = 60 - (current_time - self.start_time)
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.request_count = 0
                self.start_time = time.time()
        
        # Apply delay between requests
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.delay_between_requests:
            sleep_time = self.delay_between_requests - time_since_last
            time.sleep(sleep_time)
        
        # Apply random delay if enabled
        if settings.scraper.random_delays:
            random_delay = random.uniform(
                settings.scraper.min_delay, 
                settings.scraper.max_delay
            )
            time.sleep(random_delay)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _setup_session(self) -> requests.Session:
        """Set up a requests session with headers and proxy."""
        session = requests.Session()
        
        # Set headers
        session.headers.update({
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Set proxy if available
        proxy = self._get_next_proxy()
        if proxy:
            session.proxies.update(proxy)
        
        return session
    
    def _setup_browser(self) -> webdriver.Chrome:
        """Set up a Chrome browser with anti-detection measures."""
        chrome_options = Options()
        
        # Basic options
        if settings.scraper.headless_browser:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Anti-detection measures
        chrome_options.add_argument(f"--user-agent={self._get_random_user_agent()}")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")  # Can be removed if JS is needed
        
        # Proxy setup for browser
        proxy = self._get_next_proxy()
        if proxy and 'http' in proxy:
            proxy_address = proxy['http'].replace('http://', '')
            chrome_options.add_argument(f"--proxy-server=http://{proxy_address}")
        
        # Window size randomization
        window_sizes = [
            (1920, 1080), (1366, 768), (1440, 900), (1536, 864), (1280, 720)
        ]
        width, height = random.choice(window_sizes)
        chrome_options.add_argument(f"--window-size={width},{height}")
        
        driver = webdriver.Chrome(options=chrome_options)
        
        # Execute script to hide webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Set timeouts
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(settings.scraper.browser_timeout)
        
        return driver
    
    def get_session(self) -> requests.Session:
        """Get or create a requests session."""
        if not self.session:
            self.session = self._setup_session()
        return self.session
    
    def get_browser(self) -> webdriver.Chrome:
        """Get or create a browser instance."""
        if not self.driver:
            self.driver = self._setup_browser()
        return self.driver
    
    def make_request(self, url: str, **kwargs) -> requests.Response:
        """Make a rate-limited HTTP request with anti-detection measures.
        
        Args:
            url: The URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            requests.Response: The response object
            
        Raises:
            ScrapingError: If the request fails
        """
        self._apply_rate_limiting()
        
        session = self.get_session()
        
        try:
            response = session.get(url, timeout=30, **kwargs)
            
            # Check for rate limiting
            if response.status_code == 429:
                self.logger.warning(f"Rate limited by {self.data_source}")
                raise RateLimitError("Rate limit exceeded")
            
            # Check for blocking
            if response.status_code == 403:
                self.logger.warning(f"Access forbidden by {self.data_source}")
                # Try with new session and proxy
                self.session = None
                session = self.get_session()
                response = session.get(url, timeout=30, **kwargs)
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            raise ScrapingError(f"Request failed: {e}")
    
    def parse_html(self, html: str) -> BeautifulSoup:
        """Parse HTML content with BeautifulSoup.
        
        Args:
            html: HTML content to parse
            
        Returns:
            BeautifulSoup: Parsed HTML object
        """
        return BeautifulSoup(html, 'html.parser')
    
    def safe_extract_text(self, element, selector: str, default: str = "") -> str:
        """Safely extract text from an element using CSS selector.
        
        Args:
            element: BeautifulSoup element or WebDriver element
            selector: CSS selector
            default: Default value if not found
            
        Returns:
            str: Extracted text or default value
        """
        try:
            if hasattr(element, 'select_one'):  # BeautifulSoup
                found = element.select_one(selector)
                return found.get_text(strip=True) if found else default
            else:  # Selenium WebElement
                found = element.find_element(By.CSS_SELECTOR, selector)
                return found.text.strip() if found else default
        except Exception:
            return default
    
    def safe_extract_attribute(self, element, selector: str, attribute: str, default: str = "") -> str:
        """Safely extract an attribute from an element.
        
        Args:
            element: BeautifulSoup element or WebDriver element
            selector: CSS selector
            attribute: Attribute name
            default: Default value if not found
            
        Returns:
            str: Extracted attribute value or default value
        """
        try:
            if hasattr(element, 'select_one'):  # BeautifulSoup
                found = element.select_one(selector)
                return found.get(attribute, default) if found else default
            else:  # Selenium WebElement
                found = element.find_element(By.CSS_SELECTOR, selector)
                return found.get_attribute(attribute) or default
        except Exception:
            return default
    
    def clean_price(self, price_text: str) -> Optional[float]:
        """Clean and convert price text to float.
        
        Args:
            price_text: Raw price text
            
        Returns:
            Optional[float]: Cleaned price or None if invalid
        """
        if not price_text:
            return None
        
        # Remove common price formatting
        cleaned = price_text.replace('$', '').replace(',', '').replace('+', '').strip()
        
        # Handle ranges (take the first number)
        if '-' in cleaned:
            cleaned = cleaned.split('-')[0].strip()
        
        # Handle "per month" or similar suffixes
        if 'per' in cleaned.lower():
            cleaned = cleaned.split('per')[0].strip()
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse price: {price_text}")
            return None
    
    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error closing browser: {e}")
            finally:
                self.driver = None
        
        if self.session:
            try:
                self.session.close()
            except Exception as e:
                self.logger.error(f"Error closing session: {e}")
            finally:
                self.session = None
    
    @abstractmethod
    def search_properties(self, search_criteria: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Search for properties based on criteria.
        
        Args:
            search_criteria: Dictionary containing search parameters
            
        Yields:
            Dict[str, Any]: Property data dictionaries
        """
        pass
    
    @abstractmethod
    def get_property_details(self, property_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific property.
        
        Args:
            property_url: URL of the property page
            
        Returns:
            Dict[str, Any]: Detailed property data
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

