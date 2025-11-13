"""
XCellParts Scraper Engine v8

Specialized scraper for xcellparts.com with whole-site discovery, pagination, and hardened networking.
Integrates with the main scraping framework and Results dashboard.

Features:
- WooCommerce category discovery
- Complete pagination coverage
- Anti-bot measures and Cloudflare bypass
- Structured logging and error handling
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict
from urllib.parse import urljoin, urlparse
from scraper_engine import (
    build_session, get_html_with_timing, retry_with_curl_cffi, 
    detect_cloudflare, CategoryInfo, ScrapeResult, find_next_page_url,
    parse_xcell_products, discover_xcell_categories, PARSER
)
from logger import log_scrape_start, log_scrape_page, log_scrape_complete, log_scrape_error, log_discovery

# Use the main Item class from scraper_engine for consistency
from scraper_engine import Item

# Legacy Item class for backward compatibility
@dataclass
class LegacyItem:
    """Legacy product item data structure"""
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
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_price_number(price_str: str) -> float:
    """Extract numeric price from string like '$12.17' or '12.17'"""
    if not price_str:
        return 0.0
    # Remove currency symbols, commas, spaces
    clean = re.sub(r'[^\d.]', '', str(price_str))
    try:
        return float(clean) if clean else 0.0
    except (ValueError, TypeError):
        return 0.0

def fmt_price(val: float, currency: str = "USD") -> str:
    """Format price as currency string"""
    if currency == "CAD":
        return f"CA${val:.2f}"
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
    
    return session, False  # False = not using curl_cffi

def get_html(session, url: str) -> Optional[str]:
    """Fetch HTML content from URL"""
    try:
        response = session.get(url, timeout=30)  # Increased from 30 to 30 (keep consistent)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[xcell] Failed to fetch {url}: {e}")
        return None

def extract_product_from_listing(product_elem, base_url: str) -> Optional[Item]:
    """
    Extract product data from a product listing element on xcellparts.com
    
    HTML Structure (Actual from xcellparts.com):
    <li class="product">
        <a class="woocommerce-LoopProduct-link" href="PRODUCT_URL">
            <img src="IMAGE_URL">
            <h2 class="woocommerce-loop-product__title">TITLE</h2>
        </a>
        <span class="price">
            <span class="woocommerce-Price-amount amount">$12.17</span>
        </span>
    </li>
    """
    try:
        item = Item()
        item.site = "xcellparts.com"
        
        # Extract title - xcellparts has title directly in h2 (not inside a link)
        title_elem = (
            product_elem.select_one('h2.woocommerce-loop-product__title') or
            product_elem.select_one('.woocommerce-loop-product__title') or
            product_elem.select_one('h2')
        )
        
        if title_elem:
            item.title = clean_text(title_elem.get_text())
        
        # Extract URL - separate from title
        link_elem = (
            product_elem.select_one('a.woocommerce-LoopProduct-link') or
            product_elem.select_one('a[href*="/product/"]') or
            product_elem.find('a', href=True)
        )
        
        if link_elem:
            item.url = urljoin(base_url, link_elem.get('href', ''))
        
        # If still no title, try getting from link or img alt
        if not item.title and link_elem:
            img = link_elem.find('img', alt=True)
            if img:
                item.title = clean_text(img.get('alt', ''))
        
        if not item.title or not item.url:
            return None
        
        # Extract image URL
        img_elem = product_elem.select_one('img')
        if img_elem:
            # XCellParts uses data-src for lazy loading, but falls back to src
            img_url = img_elem.get('data-src') or img_elem.get('src') or ''
            if img_url:
                item.image_url = urljoin(base_url, img_url)
        
        # Extract price
        price_elem = (
            product_elem.select_one('.price .woocommerce-Price-amount') or
            product_elem.select_one('.woocommerce-Price-amount') or
            product_elem.select_one('.price .amount') or
            product_elem.select_one('.price')
        )
        
        if price_elem:
            price_text = clean_text(price_elem.get_text())
            price_val = parse_price_number(price_text)
            item.original = price_val
            item.discounted = price_val
            item.original_formatted = fmt_price(price_val)
            item.discounted_formatted = fmt_price(price_val)
        
        # Check stock status - xcellparts shows "out-of-stock" class or text
        if (product_elem.select_one('.out-of-stock') or 
            'outofstock' in ' '.join(product_elem.get('class', [])).lower() or
            'OUT OF STOCK' in product_elem.get_text().upper()):
            item.stock_status = "Out of Stock"
        
        return item if item.title and item.url else None
        
    except Exception as e:
        print(f"[xcell] Failed to parse product element: {e}")
        return None

def scrape_category_page(session, url: str, rules: dict, logger=None) -> List[Item]:
    """
    Scrape a single category page from xcellparts.com
    
    Args:
        session: HTTP session
        url: Category page URL
        rules: Discount rules (percent_off, absolute_off)
        logger: Optional logger instance
    
    Returns:
        List of Item objects
    """
    items = []
    
    html = get_html(session, url)
    if not html:
        if logger:
            logger.warning(f"[xcell] Failed to fetch HTML from {url}")
        return items
    
    soup = BeautifulSoup(html, 'lxml')
    
    # XCellParts uses WooCommerce structure
    # Products are in <ul class="products"> with <li class="product"> items
    product_containers = soup.select('ul.products li.product') or soup.select('.products .product')
    
    if logger:
        logger.info(f"[xcell] Found {len(product_containers)} products on page: {url}")
    
    for product_elem in product_containers:
        item = extract_product_from_listing(product_elem, url)
        if item:
            # Apply discount rules
            if rules.get('percent_off', 0) > 0:
                discount_amount = item.original * (rules['percent_off'] / 100.0)
                item.discounted = max(0, item.original - discount_amount)
                item.discounted_formatted = fmt_price(item.discounted)
            
            if rules.get('absolute_off', 0) > 0:
                item.discounted = max(0, item.discounted - rules['absolute_off'])
                item.discounted_formatted = fmt_price(item.discounted)
            
            items.append(item)
    
    return items

def find_next_page_url(soup, current_url: str) -> Optional[str]:
    """
    Find the next pagination page URL
    
    XCellParts uses WooCommerce pagination:
    <nav class="woocommerce-pagination">
        <a class="next page-numbers" href="NEXT_PAGE_URL">Next</a>
    </nav>
    """
    # Try to find "Next" link
    next_link = (
        soup.select_one('a.next.page-numbers') or
        soup.select_one('.woocommerce-pagination a.next') or
        soup.select_one('.pagination .next') or
        soup.find('a', string=re.compile(r'Next|→', re.I))
    )
    
    if next_link and next_link.get('href'):
        return urljoin(current_url, next_link['href'])
    
    return None

def scrape_category_all_pages(session, url: str, rules: dict, max_pages: int = 20, delay_ms: int = 200, logger=None) -> List[Item]:
    """
    Scrape all pagination pages from a category
    
    Args:
        session: HTTP session
        url: Initial category URL
        rules: Discount rules
        max_pages: Maximum pages to scrape
        delay_ms: Delay between page requests (milliseconds)
        logger: Optional logger
    
    Returns:
        Combined list of items from all pages
    """
    all_items = []
    current_url = url
    page_num = 1
    
    while current_url and page_num <= max_pages:
        if logger:
            logger.info(f"[xcell] Scraping page {page_num}/{max_pages}: {current_url}")
        
        # Scrape current page
        page_items = scrape_category_page(session, current_url, rules, logger)
        all_items.extend(page_items)
        
        if not page_items:
            if logger:
                logger.info(f"[xcell] No items found on page {page_num}, stopping pagination")
            break
        
        # Get HTML to find next page
        html = get_html(session, current_url)
        if not html:
            break
        
        soup = BeautifulSoup(html, 'lxml')
        next_url = find_next_page_url(soup, current_url)
        
        if not next_url:
            if logger:
                logger.info(f"[xcell] No more pages found after page {page_num}")
            break
        
        current_url = next_url
        page_num += 1
        
        # Delay before next request
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
    
    if logger:
        logger.info(f"[xcell] Total items scraped: {len(all_items)} from {page_num} pages")
    
    return all_items

def scrape_url(session, url: str, rules: dict, crawl_pagination: bool = True, 
               max_pages: int = 20, delay_ms: int = 200, logger=None) -> List[Item]:
    """
    Main entry point for scraping XCellParts URLs
    
    Args:
        session: HTTP session
        url: Product or category URL
        rules: Discount rules (percent_off, absolute_off)
        crawl_pagination: Whether to follow pagination
        max_pages: Max pages to crawl
        delay_ms: Delay between requests
        logger: Optional logger
    
    Returns:
        List of Item objects
    """
    if logger:
        logger.info(f"[xcell] Starting scrape of: {url}")
    
    # Determine if it's a category or product page
    if '/product-category/' in url:
        # Category page
        if crawl_pagination:
            return scrape_category_all_pages(session, url, rules, max_pages, delay_ms, logger)
        else:
            return scrape_category_page(session, url, rules, logger)
    elif '/product/' in url:
        # Single product page - scrape as category with 1 item
        items = scrape_category_page(session, url, rules, logger)
        return items[:1] if items else []
    else:
        # Try as category by default
        if logger:
            logger.warning(f"[xcell] URL type unclear, trying as category: {url}")
        return scrape_category_page(session, url, rules, logger)

# ========== V8 ENHANCED FUNCTIONS ==========

def discover_all_categories(base_url: str = "https://xcellparts.com/") -> List[CategoryInfo]:
    """
    Discover all XCellParts categories using v8 enhanced discovery
    """
    return discover_xcell_categories(base_url)

def scrape_category_v8(category_url: str, max_pages: int = 50) -> ScrapeResult:
    """
    Enhanced category scraping with v8 features:
    - Improved pagination detection
    - Cloudflare bypass
    - Structured logging
    - Timing metrics
    """
    from scraper_engine import scrape_category_with_pagination
    return scrape_category_with_pagination(category_url, 'xcellparts', max_pages)

def scrape_all_discovered_categories(max_pages_per_category: int = 10, 
                                   max_categories: int = 100,
                                   allowed_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Complete site scrape: discover all categories and scrape each one
    """
    results = {
        'categories_found': 0,
        'categories_scraped': 0,
        'total_items': 0,
        'cf_detected': False,
        'errors': [],
        'categories_targeted': 0,
        'target_urls': []
    }
    
    try:
        # Step 1: Discover categories
        log_discovery('xcellparts', 0, phase='start')
        categories = discover_all_categories()
        results['categories_found'] = len(categories)

        def normalize_url(url: str) -> str:
            return (url or '').strip().rstrip('/').lower()

        allowed_lookup = {
            normalize_url(url): url.strip()
            for url in (allowed_urls or [])
            if isinstance(url, str) and url.strip()
        }

        if allowed_lookup:
            existing_lookup = {
                normalize_url(cat.url): cat
                for cat in categories
            }

            filtered: List[CategoryInfo] = []
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

            for normalized, original_url in allowed_lookup.items():
                match = existing_lookup.get(normalized)
                if match:
                    filtered.append(match)
                else:
                    slug_part = original_url.rstrip('/').split('/')[-1]
                    label_hint = clean_text(slug_part.replace('-', ' ')) or 'Custom Category'
                    filtered.append(CategoryInfo(
                        site='xcellparts',
                        url=original_url,
                        brand=None,
                        label_text=label_hint,
                        discovered_at=timestamp
                    ))

            categories = filtered

        if not categories:
            results['errors'].append('No categories discovered')
            return results
        
        # Step 2: Scrape each category
        all_items = []
        scraped_count = 0
        
        limited_categories = categories[:max_categories] if max_categories else categories
        results['categories_targeted'] = len(limited_categories)
        results['target_urls'] = [cat.url for cat in limited_categories]

        for i, category in enumerate(limited_categories):
            if i > 0:
                time.sleep(1)  # Rate limiting between categories
            
            try:
                log_scrape_start('xcellparts', category.url, category=category.label_text)
                scrape_result = scrape_category_v8(category.url, max_pages_per_category)
                
                all_items.extend(scrape_result.items)
                scraped_count += 1
                
                if scrape_result.cf_detected:
                    results['cf_detected'] = True
                
                log_scrape_complete('xcellparts', scrape_result.total_pages, len(scrape_result.items))
                
            except Exception as e:
                error_msg = f"Failed to scrape category {category.url}: {str(e)}"
                results['errors'].append(error_msg)
                log_scrape_error('xcellparts', category.url, error_msg)
        
        results['categories_scraped'] = scraped_count
        results['total_items'] = len(all_items)
        
        # Step 3: Save to database if available
        try:
            from database import results_db_manager
            
            # Save items using the existing structure
            history_id = str(int(time.time() * 1000))  # Timestamp in ms
            urls = [cat.url for cat in limited_categories[:scraped_count]]
            rules = {'site': 'xcellparts', 'max_pages': max_pages_per_category}
            
            # Convert v8 Items to legacy format for database compatibility
            legacy_items = []
            for item in all_items:
                legacy_item = {
                    'url': item.url,
                    'site': item.site,
                    'title': item.title,
                    'price_value': item.price_value,
                    'price_currency': item.price_currency or 'USD',
                    'price_text': item.price_text,
                    'discounted_value': item.discounted_value,
                    'discounted_formatted': item.discounted_formatted,
                    'original_formatted': item.original_formatted,
                    'source': item.source,
                    'image_url': item.image_url
                }
                legacy_items.append(legacy_item)
            
            results_db_manager.save_fetch_history(history_id, urls, legacy_items, rules)
            results['saved_to_db'] = True
            
        except Exception as e:
            results['errors'].append(f"Database save failed: {str(e)}")
        
        return results
        
    except Exception as e:
        results['errors'].append(f"Overall scraping failed: {str(e)}")
        return results

def get_site_stats() -> Dict[str, Any]:
    """Get scraping statistics for XCellParts"""
    try:
        from database import results_db_manager
        return results_db_manager.get_totals('xcellparts')
    except Exception as e:
        return {'error': str(e)}
