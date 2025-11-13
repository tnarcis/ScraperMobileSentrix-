"""
TXParts Scraper Engine

Specialized scraper for txparts.com with proper title, price, and image extraction.
This engine is automatically used when scraping txparts.com URLs.

Author: Arslan
Created for: TXParts
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urljoin, urlparse

@dataclass
class Item:
    """Product item data structure (compatible with main scraper_engine)"""
    title: str = ""
    url: str = ""
    image_url: str = ""
    original: float = 0.0
    discounted: float = 0.0
    original_formatted: str = "$0.00"
    discounted_formatted: str = "$0.00"
    site: str = ""
    sku: str = ""
    stock_status: str = "In Stock"
    extra: dict = field(default_factory=dict)

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_price_number(price_str: str) -> float:
    """Extract numeric price from string like '$14.49' or '14.49'"""
    if not price_str:
        return 0.0
    clean = re.sub(r'[^\d.]', '', str(price_str))
    try:
        return float(clean) if clean else 0.0
    except (ValueError, TypeError):
        return 0.0

def fmt_price(val: float) -> str:
    """Format price as currency string"""
    return f"${val:.2f}"

def build_session(retries: int = 2, verify_ssl: bool = True) -> tuple:
    """Build HTTP session with retry logic"""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    
    retry_strategy = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.verify = verify_ssl
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    return session, False

def get_html(session, url: str) -> Optional[str]:
    """Fetch HTML content from URL"""
    try:
        response = session.get(url, timeout=30)  # Consistent 30s timeout
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[txparts] Failed to fetch {url}: {e}")
        return None

def extract_products_from_page(soup, base_url: str) -> List[Item]:
    """
    Extract all products from a TXParts category page
    
    TXParts Structure:
    - Product images in <div class="flipper">
    - Product titles and prices after the flipper
    - Links with /product/ in href
    """
    items = []
    
    # Find all product links (excluding stretched-link which is for the image)
    product_links = soup.find_all('a', href=lambda x: x and '/product/' in x)
    
    # Group links - TXParts has 2 links per product (image link + title link)
    seen_urls = {}
    
    for link in product_links:
        url = urljoin(base_url, link.get('href', ''))
        
        if url in seen_urls:
            continue
        
        # Create item
        item = Item()
        item.site = "txparts.com"
        item.url = url
        
        # Get title from link text
        title_text = clean_text(link.get_text())
        if title_text and len(title_text) > 5:  # Valid title
            item.title = title_text
        
        # If no title yet, try to find it nearby
        if not item.title:
            # Look for title in parent or sibling elements
            parent = link.parent
            if parent:
                # Try finding a text node with product name
                for elem in parent.find_all(['h6', 'h5', 'h4', 'a']):
                    text = clean_text(elem.get_text())
                    if text and len(text) > 10 and '/product/' in elem.get('href', ''):
                        item.title = text
                        break
        
        # Find price - look in parent container
        price_found = False
        search_parent = link.parent
        attempts = 0
        while search_parent and attempts < 5:
            # Look for price pattern
            price_match = re.search(r'\$(\d+\.?\d*)', search_parent.get_text())
            if price_match:
                price_val = parse_price_number(price_match.group())
                if price_val > 0:
                    item.original = price_val
                    item.discounted = price_val
                    item.original_formatted = fmt_price(price_val)
                    item.discounted_formatted = fmt_price(price_val)
                    price_found = True
                    break
            search_parent = search_parent.parent
            attempts += 1
        
        # Find image - look backwards for flipper div with image OR any img in parent
        img_found = False
        if link.parent:
            # Look for previous sibling or parent with flipper class
            container = link.parent.parent if link.parent.parent else link.parent
            flipper = container.find('div', class_='flipper') if container else None
            
            if not flipper:
                # Try finding in previous siblings
                for prev_sibling in link.parent.find_previous_siblings():
                    flipper = prev_sibling.find('div', class_='flipper')
                    if flipper:
                        break
            
            if flipper:
                img = flipper.find('img')
                if img:
                    img_url = img.get('src') or img.get('data-src') or ''
                    if img_url:
                        item.image_url = urljoin(base_url, img_url)
                        img_found = True
            
            # If no flipper found, search for any product image in parent/grandparent
            if not img_found and link.parent:
                # Search in parent
                search_container = link.parent
                for _ in range(3):  # Check up to 3 levels up
                    if search_container:
                        img = search_container.find('img', src=lambda x: x and 'admin.txparts.com' in x)
                        if img:
                            img_url = img.get('src') or img.get('data-src') or ''
                            if img_url:
                                item.image_url = urljoin(base_url, img_url)
                                img_found = True
                                break
                        search_container = search_container.parent
        
        # Only add item if we have at least title or URL
        if item.title or (item.url and price_found):
            # If no title, use a default from URL
            if not item.title:
                # Extract title from URL
                url_parts = item.url.split('/')
                if url_parts:
                    slug = url_parts[-1]
                    item.title = slug.replace('-', ' ').title()
            
            seen_urls[url] = True
            items.append(item)
    
    return items

def scrape_category_page(session, url: str, rules: dict, logger=None) -> List[Item]:
    """
    Scrape a single category page from txparts.com
    """
    items = []
    
    html = get_html(session, url)
    if not html:
        if logger:
            logger.warning(f"[txparts] Failed to fetch HTML from {url}")
        return items
    
    soup = BeautifulSoup(html, 'lxml')
    
    # Extract products
    items = extract_products_from_page(soup, url)
    
    if logger:
        logger.info(f"[txparts] Found {len(items)} products on page: {url}")
    
    # Apply discount rules
    for item in items:
        if rules.get('percent_off', 0) > 0:
            discount_amount = item.original * (rules['percent_off'] / 100.0)
            item.discounted = max(0, item.original - discount_amount)
            item.discounted_formatted = fmt_price(item.discounted)
        
        if rules.get('absolute_off', 0) > 0:
            item.discounted = max(0, item.discounted - rules['absolute_off'])
            item.discounted_formatted = fmt_price(item.discounted)
    
    return items

def scrape_url(session, url: str, rules: dict, crawl_pagination: bool = True,
               max_pages: int = 20, delay_ms: int = 200, logger=None) -> List[Item]:
    """
    Main entry point for scraping TXParts URLs
    """
    if logger:
        logger.info(f"[txparts] Starting scrape of: {url}")
    
    # TXParts doesn't have traditional pagination on category pages
    # All products are loaded on a single page
    items = scrape_category_page(session, url, rules, logger)
    
    return items
