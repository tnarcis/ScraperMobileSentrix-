"""
MobileSentrix Scraper Engine v8
===============================
Complete whole-site scraping with category discovery and pagination.
This module handles:
- Whole-site category discovery (mobile nav + desktop + sitemap fallbacks)
- HTTP session management with anti-bot measures
- HTML parsing and data extraction
- Product and category page scraping with full pagination
- Parallel URL processing
- Cloudflare detection and bypass
"""

from bs4 import BeautifulSoup
import requests
import re
import json
import time
import random
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse
from collections import deque
import html
from dataclasses import dataclass
from typing import List, Optional, Tuple, Set, Dict, Any, Deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from logger import log_scrape_start, log_scrape_page, log_scrape_complete, log_scrape_error, log_discovery, log_cf_detected

# Optional curl_cffi for better Cloudflare bypass
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except Exception:
    HAS_CURL = False

# Check for lxml parser (faster than html.parser)
try:
    import lxml
    PARSER = 'lxml'
except ImportError:
    PARSER = 'html.parser'

# -------- Data Classes --------

@dataclass
class Item:
    """Represents a scraped product item"""
    url: str
    site: str
    title: str
    price_value: Optional[float]
    price_currency: Optional[str]
    price_text: str
    discounted_value: Optional[float]
    discounted_formatted: str
    original_formatted: str
    source: str
    image_url: str
    stock_status: Optional[str] = None
    sku: Optional[str] = None
    category_path: Optional[str] = None
    scraped_at: Optional[str] = None

@dataclass
class CategoryInfo:
    """Represents a discovered category"""
    site: str
    url: str
    brand: Optional[str]
    label_text: str
    discovered_at: str

@dataclass
class ScrapeResult:
    """Result of a scraping operation with metadata"""
    items: List[Item]
    next_page_url: Optional[str]
    total_pages: int
    cf_detected: bool
    ttfb_ms: float
    total_ms: float
    status_code: int


# -------- Text & Price Utilities --------

MONEY_RE = re.compile(r'([\$£€]|CA\$)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2})?)')
CURRENCY_SYMBOLS = {'USD': '$', 'CAD': 'CA$', 'usd': '$', 'cad': 'CA$'}


def clean_text(s: Optional[str]) -> str:
    """Clean and normalize text by removing extra whitespace"""
    if not s:
        return ""
    return re.sub(r'\s+', ' ', s).strip()


def host_currency(host: str) -> str:
    """Detect currency based on hostname (CA domains = CAD, others = USD)"""
    host = (host or '').lower()
    if host.endswith('.ca') or host.startswith('ca.') or '.ca.' in host:
        return 'CAD'
    return 'USD'


def parse_price_number(text: str) -> Optional[float]:
    """Extract numeric price value from text"""
    if not text:
        return None
    m = MONEY_RE.search(text)
    if not m:
        return None
    num = m.group(2).replace(',', '')
    try:
        return float(num)
    except Exception:
        return None


def fmt_price(val: Optional[float], currency: Optional[str], host: str) -> str:
    """Format price with appropriate currency symbol"""
    if val is None:
        return ""
    sym = CURRENCY_SYMBOLS.get((currency or '').upper()) or CURRENCY_SYMBOLS.get(host_currency(host)) or '$'
    return f"{sym}{val:,.2f}"


# -------- Stock Detection Helpers --------

STOCK_TEXT_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\b(not\s+available|unavailable|out\s+of\s+stock|sold\s+out|no\s+stock|currently\s+unavailable|temporarily\s+unavailable|stock\s*:?(?:\s|\s*&nbsp;)?no)\b", re.IGNORECASE), 'out_of_stock'),
    (re.compile(r"\b(back[\s-]?order(?:ed)?|backordered|awaiting\s+stock|ships\s+in\s+\d+|ships\s+within\s+\d+|special\s+order)\b", re.IGNORECASE), 'back_order'),
    (re.compile(r"\b(pre[\s-]?order|coming\s+soon|pre\s+order)\b", re.IGNORECASE), 'preorder'),
    (re.compile(r"\b(low\s+stock|limited\s+stock|few\s+left|only\s+\d+\s+(?:left|remaining)|almost\s+gone|last\s+(?:few|units)|limited\s+availability)\b", re.IGNORECASE), 'limited'),
    (re.compile(r"\b(in\s*stock|available\s+now|ready\s+to\s+ship|ships\s+today|available\s+for\s+immediate|stock\s*:?(?:\s|\s*&nbsp;)?yes|qty\s+available|stock\s+available)\b", re.IGNORECASE), 'in_stock'),
]

AVAILABILITY_URL_MAP = {
    'schema.org/instock': 'in_stock',
    'schema.org/outofstock': 'out_of_stock',
    'schema.org/limitedavailability': 'limited',
    'schema.org/backorder': 'back_order',
    'schema.org/preorder': 'preorder',
    'schema.org/discontinued': 'discontinued'
}

STOCK_STATUS_SELECTORS = (
    '[data-stock-status]',
    '[data-stock]',
    '[data-availability]',
    '.stock-status',
    '.stock',
    '.availability',
    '.availability span',
    '.product-stock',
    '.stock-status-label',
    '.stock-label',
    '.stock-message',
    '.stock-info',
    '.inventory-status',
    '.availability-value',
    '.availability-indicator',
    '.availability-message',
    '.item-stock',
    '.stock-qty',
    '.qty-status',
    '.qty-indicator'
)


def normalize_stock_status(value: Optional[Any]) -> Optional[str]:
    """Normalize raw stock text or boolean into a canonical status string."""
    if value is None:
        return None

    if isinstance(value, bool):
        return 'in_stock' if value else 'out_of_stock'

    cleaned = clean_text(str(value))
    if not cleaned:
        return None

    lowered = cleaned.lower()
    collapsed = lowered.replace('-', ' ').replace('_', ' ')

    if 'not available' in collapsed or 'unavailable' in collapsed or 'no availability' in collapsed:
        return 'out_of_stock'

    direct_map = {
        'in stock': 'in_stock',
        'instock': 'in_stock',
        'available': 'in_stock',
        'available now': 'in_stock',
        'yes': 'in_stock',
        'y': 'in_stock',
        'stock yes': 'in_stock',
        'out of stock': 'out_of_stock',
        'sold out': 'out_of_stock',
        'no': 'out_of_stock',
        'n': 'out_of_stock',
        'stock no': 'out_of_stock',
        'backorder': 'back_order',
        'back order': 'back_order',
        'backordered': 'back_order',
        'preorder': 'preorder',
        'pre order': 'preorder',
        'limited availability': 'limited',
        'low stock': 'limited',
        'limited stock': 'limited'
    }

    if collapsed in direct_map:
        return direct_map[collapsed]

    for pattern, status in STOCK_TEXT_PATTERNS:
        if pattern.search(lowered):
            return status

    return None


def _candidate_stock_strings(element: Optional[BeautifulSoup]) -> List[str]:
    candidates: List[str] = []
    if not element:
        return candidates

    for attr in ('data-stock-status', 'data-stock', 'data-availability', 'data-in-stock', 'data-instock',
                 'aria-label', 'title', 'data-original-title'):
        val = element.attrs.get(attr)
        if val:
            candidates.append(val)

    text_value = clean_text(element.get_text(separator=' '))
    if text_value:
        candidates.append(text_value)

    return candidates


def extract_stock_status_from_element(element: Optional[BeautifulSoup]) -> Optional[str]:
    for candidate in _candidate_stock_strings(element):
        status = normalize_stock_status(candidate)
        if status:
            return status
    return None


def extract_stock_status_from_container(container: Optional[BeautifulSoup]) -> Optional[str]:
    if container is None:
        return None

    direct = extract_stock_status_from_element(container)
    if direct:
        return direct

    for selector in STOCK_STATUS_SELECTORS:
        element = container.select_one(selector)
        if not element:
            continue
        status = extract_stock_status_from_element(element)
        if status:
            return status

    bulk_text = clean_text(container.get_text(separator=' '))
    return normalize_stock_status(bulk_text)


def stock_status_from_availability(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 'in_stock' if value else 'out_of_stock'
    if isinstance(value, str):
        lowered = value.lower()
        for key, status in AVAILABILITY_URL_MAP.items():
            if key in lowered:
                return status
        return normalize_stock_status(value)
    return None


def stock_status_from_offers(offers: Optional[Any]) -> Optional[str]:
    if isinstance(offers, dict):
        availability = offers.get('availability') or offers.get('availabilityText')
        status = stock_status_from_availability(availability)
        if status:
            return status

        for key in ('inStock', 'isInStock', 'available', 'availabilityValue'):
            if key in offers:
                status = normalize_stock_status(offers[key])
                if status:
                    return status

        nested = offers.get('offers')
        if nested:
            return stock_status_from_offers(nested)

    if isinstance(offers, list):
        for entry in offers:
            status = stock_status_from_offers(entry)
            if status:
                return status

    return None


def extract_stock_status_from_page(soup: BeautifulSoup) -> Optional[str]:
    meta = soup.select_one('meta[itemprop="availability"], meta[property="product:availability"]')
    if meta and meta.get('content'):
        status = stock_status_from_availability(meta['content'])
        if status:
            return status

    link = soup.select_one('link[itemprop="availability"]')
    if link and link.get('href'):
        status = stock_status_from_availability(link['href'])
        if status:
            return status

    attr_element = soup.find(attrs={'data-stock-status': True})
    if attr_element and attr_element.get('data-stock-status'):
        status = normalize_stock_status(attr_element.get('data-stock-status'))
        if status:
            return status

    for selector in STOCK_STATUS_SELECTORS:
        element = soup.select_one(selector)
        if not element:
            continue
        status = extract_stock_status_from_element(element)
        if status:
            return status

    return None


# -------- User Agents for Rotation --------

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

def get_random_user_agent() -> str:
    """Get random user agent for requests"""
    return random.choice(USER_AGENTS)

def detect_cloudflare(html: str, url: str) -> bool:
    """Detect Cloudflare protection in HTML content - only actual blocking pages"""
    if not html:
        return False
    
    html_lower = html.lower()
    
    # Only detect actual Cloudflare challenge/blocking pages
    cf_blocking_indicators = [
        'checking your browser before accessing',
        'cf-browser-verification', 
        'cf-error-details',
        '__cf_chl_jschl_tk__',
        'challenge-platform',
        'please wait while we check your browser'
    ]
    
    # Must have multiple indicators or specific blocking text
    blocking_count = sum(1 for indicator in cf_blocking_indicators if indicator in html_lower)
    
    return blocking_count >= 1 and (
        'checking your browser' in html_lower or 
        'challenge-platform' in html_lower or
        'cf-browser-verification' in html_lower
    )

def apply_rules(price: Optional[float], percent_off: float, absolute_off: float):
    """Apply discount rules to a price"""
    if price is None:
        return None
    p = float(price)
    if percent_off and percent_off > 0:
        p *= (1 - percent_off/100.0)
    if absolute_off and absolute_off > 0:
        p -= absolute_off
    return round(p + 1e-9, 2)


# -------- HTTP Session Management --------

def build_session(retries: int = 3, verify_ssl: bool = True, use_curl: bool = False):
    """
    Build hardened HTTP session with anti-bot measures.
    Returns (session, is_curl_session)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # Fixed working user agent
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    
    # TEMPORARY FIX: Use minimal session configuration that works with MobileSentrix
    # The site appears to block complex sessions with many headers
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    s = requests.Session()
    
    # Use ONLY the minimal headers that work
    minimal_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    s.headers.update(minimal_headers)
    
    # Exponential backoff retry strategy
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(['GET', 'HEAD', 'OPTIONS'])
    )
    
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    s.verify = verify_ssl
    
    return s, False


def get_html_with_timing(sess, url: str, timeout: Tuple[int, int] = (10, 30)) -> Dict[str, Any]:
    """
    Fetch HTML from URL with timing and Cloudflare detection.
    Returns dict with: {url, html, ttfb_ms, total_ms, status_code, cf_detected}
    """
    start_time = time.time()
    ttfb_time = None

    try:
        # Keep throughput high while avoiding burst traffic that may trigger bot detection
        time.sleep(random.uniform(0.02, 0.08))  # 20-80ms jitter

        response = sess.get(url, timeout=timeout, allow_redirects=True)
        ttfb_time = time.time()

        response.raise_for_status()
        html = response.text
        total_time = time.time()

        cf_detected = detect_cloudflare(html, url)

        result = {
            'url': response.url,
            'html': html,
            'ttfb_ms': round((ttfb_time - start_time) * 1000, 2),
            'total_ms': round((total_time - start_time) * 1000, 2),
            'status_code': response.status_code,
            'cf_detected': cf_detected
        }

        if cf_detected:
            log_cf_detected(urlparse(url).netloc, url)

        return result

    except Exception as exc:
        total_time = time.time()
        return {
            'url': url,
            'html': '',
            'ttfb_ms': round(((ttfb_time or total_time) - start_time) * 1000, 2),
            'total_ms': round((total_time - start_time) * 1000, 2),
            'status_code': 0,
            'cf_detected': False,
            'error': str(exc)
        }

def get_html(sess, url: str, timeout: int = 30) -> Tuple[str, str]:
    """Legacy function - fetch HTML from URL. Returns (final_url, html_content)"""
    result = get_html_with_timing(sess, url, (10, timeout))
    return result['url'], result['html']

def retry_with_curl_cffi(url: str, original_error: str = None) -> Dict[str, Any]:
    """Retry request using curl_cffi for Cloudflare bypass"""
    if not HAS_CURL:
        return {'url': url, 'html': '', 'cf_detected': True, 'error': 'curl_cffi not available'}
    
    try:
        sess, _ = build_session(use_curl=True)
        return get_html_with_timing(sess, url)
    except Exception as e:
        return {'url': url, 'html': '', 'cf_detected': True, 'error': f'curl_cffi failed: {str(e)}'}

def get_html_safe(sess, url: str, delay_ms: int):
    """Fetch HTML with delay and error handling. Returns (url, html) or (None, error)"""
    if delay_ms:
        time.sleep(delay_ms / 1000.0)
    try:
        return get_html(sess, url)
    except Exception as e:
        return None, f'{type(e).__name__}: {e}'

# -------- WHOLE-SITE CATEGORY DISCOVERY --------

def discover_mobilesentrix_categories(base_url: str = "https://www.mobilesentrix.com/") -> List[CategoryInfo]:
    """
    Discover all brand/model categories from MobileSentrix using multiple methods:
    1. Mobile navigation (primary)
    2. Desktop navigation (fallback)
    3. Sitemap (fallback)
    """
    categories = []
    discovered_urls = set()
    
    sess, _ = build_session()
    
    try:
        # Method 1: Mobile navigation (primary method)
        log_discovery('mobilesentrix', 0, method='mobile_nav_start')
        result = get_html_with_timing(sess, base_url)
        
        if result['html'] and not result['cf_detected']:
            soup = BeautifulSoup(result['html'], PARSER)
            
            # Mobile mega-menu: #nav-mobile.mobile-nav.dynamicMenu
            mobile_nav = soup.select_one('#nav-mobile.mobile-nav.dynamicMenu')
            if mobile_nav:
                # Brand buckets: li.apple.mac-enable, li.samsung, etc.
                brand_items = mobile_nav.select('li[class*="apple"], li[class*="samsung"], li[class*="motorola"], li[class*="google"], li[class*="lg"], li[class*="sony"], li[class*="htc"], li[class*="blackberry"]')
                
                for brand_item in brand_items:
                    brand_classes = brand_item.get('class', [])
                    brand_name = None
                    
                    # Extract brand name from CSS classes
                    for cls in brand_classes:
                        if cls.lower() in ['apple', 'samsung', 'motorola', 'google', 'lg', 'sony', 'htc', 'blackberry']:
                            brand_name = cls.capitalize()
                            break
                    
                    # Look for expanded menu: ul.level0.slayouts-menu
                    expanded_menu = brand_item.select_one('ul.level0.slayouts-menu')
                    if expanded_menu:
                        # Find leaf category links: ul.sview-inul > ul.sview-row a.nlabel-f[href]
                        category_links = expanded_menu.select('ul.sview-inul ul.sview-row a.nlabel-f[href]')
                        
                        for link in category_links:
                            href = link.get('href')
                            if href and href.startswith('/'):
                                full_url = urljoin(base_url, href)
                                if full_url not in discovered_urls:
                                    discovered_urls.add(full_url)
                                    categories.append(CategoryInfo(
                                        site='mobilesentrix',
                                        url=full_url,
                                        brand=brand_name,
                                        label_text=clean_text(link.get_text()),
                                        discovered_at=time.strftime('%Y-%m-%d %H:%M:%S')
                                    ))
        
        # Method 2: Desktop navigation fallback
        if len(categories) < 10:  # If mobile nav didn't yield enough results
            desktop_links = soup.select('nav a[href*="/replacement-parts/"], .menu a[href*="/replacement-parts/"], .navigation a[href*="/replacement-parts/"]')
            for link in desktop_links:
                href = link.get('href')
                if href and '/replacement-parts/' in href:
                    full_url = urljoin(base_url, href)
                    if full_url not in discovered_urls:
                        discovered_urls.add(full_url)
                        categories.append(CategoryInfo(
                            site='mobilesentrix',
                            url=full_url,
                            brand='Unknown',
                            label_text=clean_text(link.get_text()),
                            discovered_at=time.strftime('%Y-%m-%d %H:%M:%S')
                        ))
        
        # Method 3: Sitemap fallback
        if len(categories) < 5:  # If still not enough categories
            try:
                sitemap_url = urljoin(base_url, '/sitemap.xml')
                sitemap_result = get_html_with_timing(sess, sitemap_url)
                
                if sitemap_result['html']:
                    root = ET.fromstring(sitemap_result['html'])
                    
                    # Find all <loc> elements
                    for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                        url = loc.text
                        if url and '/replacement-parts/' in url:
                            if url not in discovered_urls:
                                discovered_urls.add(url)
                                categories.append(CategoryInfo(
                                    site='mobilesentrix',
                                    url=url,
                                    brand='Sitemap',
                                    label_text=url.split('/')[-1].replace('-', ' ').title(),
                                    discovered_at=time.strftime('%Y-%m-%d %H:%M:%S')
                                ))
                                
            except Exception as e:
                log_scrape_error('mobilesentrix', sitemap_url, f'Sitemap parsing failed: {str(e)}')
        
        log_discovery('mobilesentrix', len(categories))
        return categories
        
    except Exception as e:
        log_scrape_error('mobilesentrix', base_url, f'Category discovery failed: {str(e)}')
        return []

def discover_xcell_categories(base_url: str = "https://xcellparts.com/") -> List[CategoryInfo]:
    """
    Discover categories from XCellParts (WooCommerce)
    """
    categories = []
    discovered_urls = set()
    
    sess, _ = build_session()
    
    try:
        log_discovery('xcellparts', 0, method='woocommerce_start')
        result = get_html_with_timing(sess, base_url)
        
        if result['html'] and not result['cf_detected']:
            soup = BeautifulSoup(result['html'], PARSER)
            
            # WooCommerce category navigation
            category_selectors = [
                '.product-categories a',
                '.widget_product_categories a',
                'nav a[href*="/product-category/"]',
                'nav a[href*="/shop/"]',
                '.menu-item a[href*="/product-category/"]'
            ]
            
            for selector in category_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and ('/product-category/' in href or '/shop/' in href):
                        full_url = urljoin(base_url, href)
                        if full_url not in discovered_urls:
                            discovered_urls.add(full_url)
                            categories.append(CategoryInfo(
                                site='xcellparts',
                                url=full_url,
                                brand='WooCommerce',
                                label_text=clean_text(link.get_text()),
                                discovered_at=time.strftime('%Y-%m-%d %H:%M:%S')
                            ))
        
        log_discovery('xcellparts', len(categories))
        return categories
        
    except Exception as e:
        log_scrape_error('xcellparts', base_url, f'XCell category discovery failed: {str(e)}')
        return []

def save_discovered_categories(categories: List[CategoryInfo], db_manager):
    """Save discovered categories to database"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Create categories table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discovered_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT NOT NULL,
                url TEXT NOT NULL,
                brand TEXT,
                label_text TEXT,
                discovered_at DATETIME,
                scraped_at DATETIME,
                UNIQUE(site, url)
            )
        ''')
        
        for cat in categories:
            cursor.execute('''
                INSERT OR REPLACE INTO discovered_categories
                (site, url, brand, label_text, discovered_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (cat.site, cat.url, cat.brand, cat.label_text, cat.discovered_at))
        
        conn.commit()
        return len(categories)
        
    except Exception as e:
        print(f"Error saving categories: {e}")
        return 0

# -------- PAGINATION & PRODUCT PARSING --------

def find_next_page_url(soup: BeautifulSoup, current_url: str, site: str) -> Optional[str]:
    """Find next page URL using multiple strategies"""
    
    # Strategy 1: Look for "Next" links
    next_selectors = []
    if site == 'mobilesentrix':
        next_selectors = [
            '.pages-item-next a',
            'a.action.next',
            '.pagination a.next',
            '.pager .next a',
            'a[title*="Next"]'
        ]
    elif site == 'xcellparts':
        next_selectors = [
            '.woocommerce-pagination .next',
            'a[rel="next"]',
            '.pagination .next a',
            '.nav-links .next'
        ]
    
    for selector in next_selectors:
        next_link = soup.select_one(selector)
        if next_link:
            href = next_link.get('href')
            if href:
                return urljoin(current_url, href)
    
    # Strategy 2: Parameter increment fallback
    parsed = urlparse(current_url)
    query_params = parse_qs(parsed.query)
    
    # Try common pagination parameters
    for param in ['p', 'page', 'paged']:
        if param in query_params:
            try:
                current_page = int(query_params[param][0])
                next_page = current_page + 1
                
                # Update query parameters
                new_params = query_params.copy()
                new_params[param] = [str(next_page)]
                
                new_query = urlencode(new_params, doseq=True)
                new_url = urlunparse((
                    parsed.scheme, parsed.netloc, parsed.path,
                    parsed.params, new_query, parsed.fragment
                ))
                return new_url
            except (ValueError, IndexError):
                continue
    
    return None


def probe_infinite_scroll_next_page(
    sess,
    current_url: str,
    site: str,
    page_num: int,
    visited_urls: Set[str],
    seen_product_urls: Set[str]
) -> Optional[Dict[str, Any]]:
    """Attempt to discover next-page URLs when pagination is driven by infinite scroll."""
    parsed = urlparse(current_url)
    base_query = parse_qs(parsed.query)

    pagination_params = ['p', 'page', 'paged']

    for param in pagination_params:
        candidate_query = {key: value[:] for key, value in base_query.items()}
        candidate_query[param] = [str(page_num + 1)]

        candidate_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(candidate_query, doseq=True),
            parsed.fragment
        ))

        if candidate_url in visited_urls:
            continue

        fetch_start = time.time()
        result = get_html_with_timing(sess, candidate_url)
        fetch_time = (time.time() - fetch_start) * 1000

        if result.get('error') or not result.get('html') or result.get('status_code', 0) != 200:
            continue

        parse_start = time.time()
        soup = BeautifulSoup(result['html'], PARSER)

        if site == 'mobilesentrix':
            candidate_items = parse_mobilesentrix_products(soup, candidate_url)
        elif site == 'xcellparts':
            candidate_items = parse_xcell_products(soup, candidate_url)
        else:
            candidate_items = []

        parse_time = (time.time() - parse_start) * 1000

        has_new = any(item.url and item.url not in seen_product_urls for item in candidate_items)

        if not candidate_items or not has_new:
            continue

        return {
            'url': candidate_url,
            'result': result,
            'soup': soup,
            'items': candidate_items,
            'fetch_time': fetch_time,
            'parse_time': parse_time,
            'cf_detected': result.get('cf_detected', False)
        }

    return None


def _normalize_infinite_url(candidate: Optional[str], base_url: str) -> Optional[str]:
    if not candidate:
        return None

    cleaned = html.unescape(candidate).replace('\\/', '/').strip()
    if not cleaned:
        return None

    try:
        return urljoin(base_url, cleaned)
    except Exception:
        return None


LOAD_MORE_ATTR_MAP = {
    '[data-load-more-url]': 'data-load-more-url',
    '[data-loadmore-url]': 'data-loadmore-url',
    '[data-next-url]': 'data-next-url',
    '[data-url][data-role*="load"]': 'data-url',
    '[data-href][data-role*="load"]': 'data-href'
}

LOAD_MORE_REGEXES = [
    re.compile(r'"loadMoreUrl"\s*:\s*"([^"]+)"', re.IGNORECASE),
    re.compile(r'"nextUrl"\s*:\s*"([^"]+)"', re.IGNORECASE),
    re.compile(r'\'loadMoreUrl\'\s*:\s*\'([^\']+)\'', re.IGNORECASE),
    re.compile(r'data-load-more-url="([^"]+)"', re.IGNORECASE)
]


def extract_infinite_scroll_urls(soup: BeautifulSoup, current_url: str) -> List[str]:
    """Grab potential load-more endpoints from DOM/script hints."""
    urls: List[str] = []

    for selector, attr in LOAD_MORE_ATTR_MAP.items():
        for element in soup.select(selector):
            raw_value = element.attrs.get(attr) or element.attrs.get('data-href') or element.attrs.get('href')
            normalized = _normalize_infinite_url(raw_value, current_url)
            if normalized and normalized not in urls:
                urls.append(normalized)

    for script in soup.find_all('script'):
        text = script.string or script.get_text() or ''
        if not text:
            continue
        for pattern in LOAD_MORE_REGEXES:
            for match in pattern.findall(text):
                normalized = _normalize_infinite_url(match, current_url)
                if normalized and normalized not in urls:
                    urls.append(normalized)

    return urls


def fetch_infinite_scroll_page(sess, target_url: str, site: str) -> Optional[Dict[str, Any]]:
    """Fetch an infinite-scroll batch and return parsed items plus follow-up URLs."""
    try:
        fetch_start = time.time()
        result = get_html_with_timing(sess, target_url)
        fetch_time = (time.time() - fetch_start) * 1000
    except Exception as fetch_error:
        log_scrape_error(site, target_url, f"Load-more fetch failed: {fetch_error}")
        return None

    if result.get('error') or not result.get('html'):
        return None

    body = (result.get('html') or '').strip()
    fragment_html = None
    extra_urls: List[str] = []
    payload = None

    if body.startswith('{'):
        try:
            payload = json.loads(body)
        except Exception:
            payload = None

    if isinstance(payload, dict):
        fragment_html = payload.get('items_html') or payload.get('html') or payload.get('content')
        for key in ('next_url', 'nextUrl', 'loadMoreUrl', 'load_more_url'):
            normalized = _normalize_infinite_url(payload.get(key), target_url)
            if normalized and normalized not in extra_urls:
                extra_urls.append(normalized)

        for key in ('additional_urls', 'more_urls', 'urls'):
            values = payload.get(key)
            if isinstance(values, list):
                for val in values:
                    normalized = _normalize_infinite_url(val, target_url)
                    if normalized and normalized not in extra_urls:
                        extra_urls.append(normalized)

    parse_source = fragment_html if fragment_html else body
    parse_start = time.time()
    soup = BeautifulSoup(parse_source, PARSER)

    if site == 'mobilesentrix':
        items = parse_mobilesentrix_products(soup, target_url)
    elif site == 'xcellparts':
        items = parse_xcell_products(soup, target_url)
    else:
        items = []

    parse_time = (time.time() - parse_start) * 1000

    dom_urls = extract_infinite_scroll_urls(soup, target_url)
    for candidate in dom_urls:
        if candidate not in extra_urls:
            extra_urls.append(candidate)

    return {
        'url': target_url,
        'result': result,
        'soup': soup,
        'items': items,
        'fetch_time': result.get('ttfb_ms', fetch_time),
        'parse_time': parse_time,
        'cf_detected': result.get('cf_detected', False),
        'extra_load_more': extra_urls
    }

def parse_mobilesentrix_products(soup: BeautifulSoup, page_url: str) -> List[Item]:
    """Parse product listings from MobileSentrix page"""
    items = []
    
    # DEBUG: Log what we're parsing
    log_scrape_start('mobilesentrix', page_url, debug_message="Starting to parse products")
    
    # Get breadcrumbs for category path
    category_path = ""
    breadcrumbs = soup.select('.breadcrumbs a')
    if breadcrumbs:
        category_path = " > ".join([clean_text(bc.get_text()) for bc in breadcrumbs[1:]])  # Skip "Home"
    
    # Product listing container - use broader selectors that actually work
    container = soup.select_one('.products-grid, .category-products, .products-list')
    print(f"DEBUG: Primary container found: {container is not None}")
    
    if not container:
        # Fallback to even broader containers
        container = soup.select_one('ul.product-listing, .product-items, .products')
        print(f"DEBUG: Fallback container found: {container is not None}")
    
    # DEBUG: Try to find ANY container that might have products
    if not container:
        all_containers = soup.select('ul, .products, .category, .listing')
        print(f"DEBUG: Found {len(all_containers)} potential containers")
        for i, cont in enumerate(all_containers[:5]):  # Show first 5
            print(f"DEBUG: Container {i}: {cont.name} classes={cont.get('class', [])}")
    
    if not container:
        print(f"DEBUG: No container found for {page_url}")
        return items
    
    # Product cards: try multiple selectors in order of preference
    cards = container.select('li.item')
    print(f"DEBUG: Found {len(cards)} product cards with selector 'li.item'")
    
    # If no cards found with li.item, try broader selectors
    if not cards:
        cards = container.select('.item')
        print(f"DEBUG: Found {len(cards)} cards with broader '.item' selector")
    
    if not cards:
        cards = container.select('li')
        print(f"DEBUG: Found {len(cards)} cards with generic 'li' selector")
    
    # DEBUG: Show what we found
    if cards:
        print(f"DEBUG: Sample card structure: {cards[0].name} classes={cards[0].get('class', [])}")
    else:
        print(f"DEBUG: No product cards found in container!")
        print(f"DEBUG: Container HTML preview: {str(container)[:200]}...")
    
    for card in cards:
        try:
            # URL: first a[href] in card
            url_element = card.select_one('a[href]')
            if not url_element:
                continue
            
            product_url = urljoin(page_url, url_element.get('href'))
            
            # Title: from anchor title attribute or visible text
            title = url_element.get('title') or clean_text(url_element.get_text())
            if not title:
                title_element = card.select_one('.product-name a, .product-title, h2, h3')
                if title_element:
                    title = clean_text(title_element.get_text())
            
            # Image: img[data-src] → fallback img[src]
            image_url = ""
            img_element = card.select_one('img[data-src]')
            if img_element:
                image_url = img_element.get('data-src')
            else:
                img_element = card.select_one('img[src]')
                if img_element:
                    image_url = img_element.get('src')
            
            if image_url:
                image_url = urljoin(page_url, image_url)
            
            # Price parsing
            price_value = None
            price_currency = "USD"
            discounted_value = None
            original_formatted = ""
            discounted_formatted = ""
            price_text = ""
            
            # Price block: .price-qty-block
            price_block = card.select_one('.price-qty-block')
            if price_block:
                # Discounted price: ins .amount or .price .amount
                discount_element = price_block.select_one('ins .amount, .price .amount')
                if discount_element:
                    price_text = clean_text(discount_element.get_text())
                    price_value = parse_price_number(price_text)
                    discounted_value = price_value
                    discounted_formatted = price_text
                
                # Original price: del .amount
                original_element = price_block.select_one('del .amount')
                if original_element:
                    original_formatted = clean_text(original_element.get_text())
                    if not price_value:  # If no discounted price found
                        price_value = parse_price_number(original_formatted)
                        price_text = original_formatted
            
            # Fallback price parsing
            if not price_value:
                price_element = card.select_one('.price, .amount, .cost')
                if price_element:
                    price_text = clean_text(price_element.get_text())
                    price_value = parse_price_number(price_text)

            stock_status = extract_stock_status_from_container(card)
            
            # Create item - FIXED VERSION
            # Fix common issues: ensure we always have basic title and URL
            if not title and url_element:
                title = clean_text(url_element.get_text()) or "Product Title Not Found"
            
            if not title:
                title = "Unknown Product"
                
            if product_url and product_url.startswith('http'):
                try:
                    items.append(Item(
                        url=product_url,
                        site='mobilesentrix',
                        title=title or "Unknown Product",
                        price_value=price_value,
                        price_currency=price_currency or "USD",
                        price_text=price_text or "",
                        discounted_value=discounted_value,
                        discounted_formatted=discounted_formatted or "",
                        original_formatted=original_formatted or "",
                        source=page_url,
                        image_url=image_url or "",
                        stock_status=stock_status,
                        sku=None,
                        category_path=category_path or "",
                        scraped_at=time.strftime('%Y-%m-%d %H:%M:%S')
                    ))
                except Exception as item_error:
                    # Fallback: create minimal item
                    items.append(Item(
                        url=product_url,
                        site='mobilesentrix', 
                        title=title or "Product",
                        price_value=0.0,
                        price_currency="USD",
                        price_text="",
                        discounted_value=None,
                        discounted_formatted="",
                        original_formatted="",
                        source=page_url,
                        image_url="",
                        stock_status=stock_status,
                        sku=None,
                        category_path="",
                        scraped_at=time.strftime('%Y-%m-%d %H:%M:%S')
                    ))
                
        except Exception as e:
            log_scrape_error('mobilesentrix', page_url, f'Error parsing product card: {str(e)}')
            print(f"DEBUG: ❌ Exception in card parsing: {e}")
            continue
    
    print(f"DEBUG: parse_mobilesentrix_products returning {len(items)} items from {page_url}")
    return items

def parse_xcell_products(soup: BeautifulSoup, page_url: str) -> List[Item]:
    """Parse product listings from XCellParts page"""
    items = []
    
    # Get breadcrumbs for category path
    category_path = ""
    breadcrumbs = soup.select('.woocommerce-breadcrumb a')
    if breadcrumbs:
        category_path = " > ".join([clean_text(bc.get_text()) for bc in breadcrumbs[1:]])  # Skip "Home"
    
    # Product cards: li.product, .type-product
    cards = soup.select('li.product, .type-product, .product-item')
    
    for card in cards:
        try:
            # URL: from product link
            url_element = card.select_one('a[href]')
            if not url_element:
                continue
            
            product_url = urljoin(page_url, url_element.get('href'))
            
            # Title: h2.woocommerce-loop-product__title, .product-title a
            title = ""
            title_element = card.select_one('h2.woocommerce-loop-product__title, .product-title a, .product-name a, h3')
            if title_element:
                title = clean_text(title_element.get_text())
            
            # Image
            image_url = ""
            img_element = card.select_one('.attachment-woocommerce_thumbnail, img')
            if img_element:
                image_url = img_element.get('src') or img_element.get('data-src')
                if image_url:
                    image_url = urljoin(page_url, image_url)
            
            # Price parsing
            price_value = None
            price_currency = "USD"
            discounted_value = None
            original_formatted = ""
            discounted_formatted = ""
            price_text = ""
            
            # WooCommerce price structure: ins .amount (discount) / del .amount (original)
            discount_element = card.select_one('ins .amount, .price ins')
            if discount_element:
                price_text = clean_text(discount_element.get_text())
                price_value = parse_price_number(price_text)
                discounted_value = price_value
                discounted_formatted = price_text
            
            original_element = card.select_one('del .amount, .price del')
            if original_element:
                original_formatted = clean_text(original_element.get_text())
                if not price_value:  # If no discounted price
                    price_value = parse_price_number(original_formatted)
                    price_text = original_formatted
            
            # Fallback to any price element
            if not price_value:
                price_element = card.select_one('.price .amount, .price, .cost')
                if price_element:
                    price_text = clean_text(price_element.get_text())
                    price_value = parse_price_number(price_text)

            stock_status = extract_stock_status_from_container(card)
            
            # Create item
            if title and product_url:
                items.append(Item(
                    url=product_url,
                    site='xcellparts',
                    title=title,
                    price_value=price_value,
                    price_currency=price_currency,
                    price_text=price_text,
                    discounted_value=discounted_value,
                    discounted_formatted=discounted_formatted,
                    original_formatted=original_formatted,
                    source=page_url,
                    image_url=image_url,
                    stock_status=stock_status,
                    sku=None,
                    category_path=category_path,
                    scraped_at=time.strftime('%Y-%m-%d %H:%M:%S')
                ))
                
        except Exception as e:
            log_scrape_error('xcellparts', page_url, f'Error parsing product card: {str(e)}')
            continue
    
    return items

def scrape_category_with_pagination(category_url: str, site: str, max_pages: Optional[int] = None) -> ScrapeResult:
    """
    Scrape a single category with full pagination coverage.
    When max_pages is None we continue until pagination is exhausted.
    """
    all_items = []
    visited_urls = set()
    current_url = category_url
    page_num = 1
    cf_detected = False
    total_fetch_time = 0
    total_parse_time = 0
    prefetched_data: Optional[Dict[str, Any]] = None
    seen_product_urls: Set[str] = set()
    load_more_queue: Deque[str] = deque()
    load_more_seen: Set[str] = set()
    
    sess, is_curl = build_session()
    
    log_scrape_start(site, category_url, max_pages=max_pages if max_pages is not None else 'unlimited')
    
    while current_url and (max_pages is None or page_num <= max_pages):
        if current_url in visited_urls:
            break  # Avoid infinite loops
        
        visited_urls.add(current_url)
        
        try:
            if prefetched_data and prefetched_data.get('url') == current_url:
                result = prefetched_data['result']
                soup = prefetched_data['soup']
                page_items = prefetched_data['items']
                fetch_time = prefetched_data.get('fetch_time', result.get('ttfb_ms', 0))
                parse_time = prefetched_data.get('parse_time', 0)
                prefetched_data = None

                if site == 'mobilesentrix':
                    new_candidates = extract_infinite_scroll_urls(soup, current_url)
                    for candidate in new_candidates:
                        if candidate and candidate not in load_more_seen and candidate not in visited_urls:
                            load_more_seen.add(candidate)
                            load_more_queue.append(candidate)
            else:
                # Fetch page with timing
                fetch_start = time.time()
                result = get_html_with_timing(sess, current_url)
                fetch_time = (time.time() - fetch_start) * 1000

                if result.get('error'):
                    log_scrape_error(site, current_url, result['error'])
                    break

                if result['cf_detected']:
                    cf_detected = True
                    # Retry with curl_cffi
                    result = retry_with_curl_cffi(current_url)
                    if result.get('error'):
                        log_scrape_error(site, current_url, f"CF bypass failed: {result['error']}")
                        break

                soup = BeautifulSoup(result['html'], PARSER)

                # Extract products based on site
                parse_start = time.time()
                if site == 'mobilesentrix':
                    page_items = parse_mobilesentrix_products(soup, current_url)
                elif site == 'xcellparts':
                    page_items = parse_xcell_products(soup, current_url)
                else:
                    page_items = []

                parse_time = (time.time() - parse_start) * 1000

                if site == 'mobilesentrix':
                    new_candidates = extract_infinite_scroll_urls(soup, current_url)
                    for candidate in new_candidates:
                        if candidate and candidate not in load_more_seen and candidate not in visited_urls:
                            load_more_seen.add(candidate)
                            load_more_queue.append(candidate)

            total_fetch_time += result.get('ttfb_ms', fetch_time)
            total_parse_time += parse_time

            unique_page_items = []
            for item in page_items:
                if item.url and item.url not in seen_product_urls:
                    seen_product_urls.add(item.url)
                    unique_page_items.append(item)

            if not unique_page_items:
                break

            all_items.extend(unique_page_items)

            # Log page completion
            log_scrape_page(
                site,
                current_url,
                page_num,
                len(unique_page_items),
                result.get('ttfb_ms', fetch_time),
                parse_time,
                0,
                result.get('cf_detected', False)
            )
            
            # Find next page
            next_url = find_next_page_url(soup, current_url, site)

            if not next_url and site == 'mobilesentrix' and load_more_queue:
                while load_more_queue and not next_url:
                    candidate_url = load_more_queue.popleft()
                    if candidate_url in visited_urls:
                        continue

                    prefetch_page = fetch_infinite_scroll_page(sess, candidate_url, site)
                    if not prefetch_page or not prefetch_page.get('items'):
                        continue

                    prefetched_data = prefetch_page
                    next_url = prefetch_page['url']

                    for extra_url in prefetch_page.get('extra_load_more', []) or []:
                        if extra_url and extra_url not in load_more_seen and extra_url not in visited_urls:
                            load_more_seen.add(extra_url)
                            load_more_queue.append(extra_url)

                    if prefetch_page.get('cf_detected'):
                        cf_detected = True

                    break
            
            # Heuristic check: if we got exactly common page sizes, probe next page
            if not next_url:
                next_page_data = probe_infinite_scroll_next_page(
                    sess,
                    current_url,
                    site,
                    page_num,
                    visited_urls,
                    seen_product_urls
                )
                if next_page_data:
                    next_url = next_page_data['url']
                    prefetched_data = next_page_data
                    if next_page_data.get('cf_detected'):
                        cf_detected = True
            
            current_url = next_url
            page_num += 1
            
            # Stop if no items found (empty page)
            if len(unique_page_items) == 0:
                break
                
        except Exception as e:
            log_scrape_error(site, current_url, f"Page scraping failed: {str(e)}")
            break
    
    log_scrape_complete(site, page_num - 1, len(all_items))
    
    return ScrapeResult(
        items=all_items,
        next_page_url=None,
        total_pages=page_num - 1,
        cf_detected=cf_detected,
        ttfb_ms=total_fetch_time / max(1, page_num - 1),
        total_ms=total_fetch_time + total_parse_time,
        status_code=200 if all_items else 0
    )


# -------- HTML Parsing Utilities --------

def find_jsonld_products(soup: BeautifulSoup) -> List[dict]:
    """Extract Product schema from JSON-LD structured data"""
    out = []
    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(tag.string or tag.get_text() or '')
        except Exception:
            continue
        
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = data
        else:
            continue
        
        for obj in candidates:
            if isinstance(obj, dict) and (obj.get('@type') == 'Product'):
                out.append(obj)
            if isinstance(obj, dict) and isinstance(obj.get('@graph'), list):
                for g in obj['@graph']:
                    if isinstance(g, dict) and g.get('@type') == 'Product':
                        out.append(g)
    
    return out


def price_from_offers(offers) -> Tuple[Optional[float], Optional[str]]:
    """Extract price and currency from offers object in JSON-LD"""
    if isinstance(offers, dict):
        price = offers.get('price')
        currency = offers.get('priceCurrency')
        try:
            return float(price), currency
        except Exception:
            return parse_price_number(str(price)), currency
    
    if isinstance(offers, list):
        for off in offers:
            v, c = price_from_offers(off)
            if v is not None:
                return v, c
    
    return None, None


def extract_title(soup: BeautifulSoup) -> str:
    """Extract product title from page"""
    for sel in ['h1.page-title .base', 'span[data-ui-id="page-title-wrapper"]', 'h1.product', 'h1']:
        el = soup.select_one(sel)
        if el:
            t = clean_text(el.get_text())
            if t:
                return t
    
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get('content'):
        return clean_text(og['content'])
    
    return ""


def extract_canonical_or_og_url(soup: BeautifulSoup, fallback: str) -> str:
    """Extract canonical URL or og:url meta tag"""
    can = soup.select_one('link[rel="canonical"]')
    if can and can.get('href'):
        return can['href']
    
    og = soup.select_one('meta[property="og:url"]')
    if og and og.get('content'):
        return og['content']
    
    return fallback


def extract_price(soup: BeautifulSoup) -> Tuple[Optional[float], str, str]:
    """
    Extract price from page using various selectors.
    Returns (price_value, currency, source_selector)
    """
    el = soup.select_one('[data-price-amount]')
    if el and el.get('data-price-amount'):
        try:
            v = float(el['data-price-amount'])
            return v, '', 'data-price-amount'
        except Exception:
            pass
    
    for sel in [
        'span.price-final_price [data-price-amount]',
        'span.price-final_price span.price',
        'div.price-box [data-price-amount]',
        'div.price-box span.price',
        'span[id^="product-price-"] [data-price-amount]',
        'span[id^="product-price-"] span.price',
        'span.price',
        '[class*="price"]', '[id*="price"]'
    ]:
        for e in soup.select(sel):
            txt = clean_text(e.get_text())
            v = parse_price_number(txt)
            if v is not None:
                return v, '', sel
    
    return None, '', ''


def extract_image_url(container: BeautifulSoup) -> str:
    """Extract image URL from container element"""
    for sel in ['img[data-src]', 'img[srcset]', 'img[src]']:
        el = container.select_one(sel)
        if not el:
            continue
        return el.get('data-src') or el.get('src') or ''
    return ''


def is_product_page(soup: BeautifulSoup) -> bool:
    """Detect if page is a product detail page"""
    return bool(soup.select_one('h1.page-title, h1.product')) or bool(find_jsonld_products(soup))


def is_category_page(soup: BeautifulSoup) -> bool:
    """Detect if page is a category/listing page"""
    return bool(soup.select_one('ul.product-listing li.item')) or \
           bool(soup.select_one('ol.products li.product-item')) or \
           bool(soup.select_one('div.product-item-info, div.product-card, li.product'))


def find_next_page_url_legacy(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """
    Find next page URL for pagination.
    Handles both traditional pagination links and query parameter pagination.
    """
    # Check for traditional pagination links first
    cand = soup.select_one('li.pages-item-next a, a.action.next, a[rel="next"], .pages .next')
    if cand and cand.get('href'):
        return urljoin(base_url, cand['href'])
    
    # For MobileSentrix specifically, try to detect pagination patterns
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)
    
    # Get current page number (default to 1)
    current_page = int(query_params.get('p', ['1'])[0])
    
    # Look for pagination indicators in the page
    products = soup.select('ul.product-listing li.item')
    
    if products:
        # Look for pagination info or toolbar
        toolbar = soup.select('.toolbar-amount, .limiter, .pages-items, .pagination')
        if toolbar:
            toolbar_text = ' '.join([t.get_text() for t in toolbar])
            # Check for patterns like "1-48 of 150"
            if 'of' in toolbar_text.lower():
                match = re.search(r'(\d+)-(\d+)\s+of\s+(\d+)', toolbar_text)
                if match:
                    end_item = int(match.group(2))
                    total_items = int(match.group(3))
                    if end_item < total_items:
                        # There are more items, construct next page URL
                        query_params['p'] = [str(current_page + 1)]
                        new_query = urlencode(query_params, doseq=True)
                        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, 
                                         parsed.params, new_query, parsed.fragment))
        
        # If no clear pagination indicator but we have products, try next page anyway
        if len(products) >= 20:
            query_params['p'] = [str(current_page + 1)]
            new_query = urlencode(query_params, doseq=True)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, 
                             parsed.params, new_query, parsed.fragment))
    
    return None


# -------- Main Scraping Functions --------

def scrape_product(sess, final_url: str, html: str, rules: Dict) -> List[Item]:
    """
    Scrape a single product page.
    Returns list with one Item.
    """
    host = urlparse(final_url).hostname or ''
    soup = BeautifulSoup(html, PARSER)
    final_url = extract_canonical_or_og_url(soup, final_url)

    title = ""
    price_val = None
    currency = None
    source = "product"
    
    # Try JSON-LD first
    stock_status = None

    jl = find_jsonld_products(soup)
    if jl:
        for obj in jl:
            if not title:
                candidate_title = clean_text(obj.get('name') or '')
                if candidate_title:
                    title = candidate_title

            pv, cur = price_from_offers(obj.get('offers'))
            if pv is not None and price_val is None:
                price_val, currency, source = pv, cur, "jsonld"

            if stock_status is None:
                stock_status = stock_status_from_offers(obj.get('offers'))

            if title and price_val is not None and stock_status:
                break

    # Fallback to HTML extraction
    if not title:
        title = extract_title(soup)
    if price_val is None:
        pv, cur, src = extract_price(soup)
        price_val, currency = pv, cur or currency
        if pv is not None:
            source = src

    # Extract image
    img = ''
    gal = soup.select_one('.gallery-placeholder, .product.media, .fotorama, .product-image')
    img = extract_image_url(gal or soup)

    # Apply discount rules
    percent_off = float(rules.get('percent_off') or 0.0)
    absolute_off = float(rules.get('absolute_off') or 0.0)
    final_price = apply_rules(price_val, percent_off, absolute_off)

    if stock_status is None:
        stock_status = extract_stock_status_from_page(soup)

    return [Item(
        url=final_url,
        site=host,
        title=title or '',
        price_value=price_val,
        price_currency=currency or host_currency(host),
        price_text='' if price_val is not None else 'price_not_found_or_hidden',
        discounted_value=final_price,
        discounted_formatted=fmt_price(final_price, currency, host) if final_price is not None else '',
        original_formatted=fmt_price(price_val, currency, host),
        source=source,
        image_url=img,
        stock_status=stock_status
    )]


def scrape_category_page(sess, final_url: str, html: str, rules: Dict) -> List[Item]:
    """
    Scrape a category/listing page (single page, no pagination).
    Returns list of Items found on the page.
    """
    host = urlparse(final_url).hostname or ''
    soup = BeautifulSoup(html, PARSER)
    out: List[Item] = []

    # Find product cards
    cards = soup.select('ul.product-listing li.item')
    if not cards:
        cards = soup.select('ol.products li.product-item, div.product-item-info, div.product-card, li.product')

    percent_off = float(rules.get('percent_off') or 0.0)
    absolute_off = float(rules.get('absolute_off') or 0.0)

    for card in cards:
        a = card.select_one('a[href]')
        if not a:
            continue
        
        title = clean_text(a.get_text())
        href = a.get('href') or ''
        prod_url = urljoin(final_url, href)
        image = extract_image_url(card)

        # Extract price
        price_val = None
        price_text = ''
        pel = card.select_one('[data-price-amount]')
        if pel and pel.get('data-price-amount'):
            try:
                price_val = float(pel['data-price-amount'])
            except Exception:
                price_val = None
        
        if price_val is None:
            pt_el = card.select_one('.price, .price-final_price .price, [class*="price"]')
            price_text = clean_text(pt_el.get_text()) if pt_el else ''
            price_val = parse_price_number(price_text)

        final_price = apply_rules(price_val, percent_off, absolute_off)
        stock_status = extract_stock_status_from_container(card)

        out.append(Item(
            url=prod_url,
            site=host,
            title=title or '',
            price_value=price_val,
            price_currency=host_currency(host),
            price_text=price_text if price_val is None else '',
            discounted_value=final_price,
            discounted_formatted=fmt_price(final_price, None, host) if final_price is not None else '',
            original_formatted=fmt_price(price_val, None, host),
            source='category-card',
            image_url=image,
            stock_status=stock_status
        ))

    return out


def scrape_category_all_pages(sess, start_url: str, rules: Dict, max_pages: int = 10,  # Reduced default from 20 to 10
                              delay_ms: int = 50, logger=None):
    """
    Scrape a category with pagination support.
    Automatically follows 'Next' links up to max_pages.
    Tracks seen products to avoid duplicates.
    """
    items: List[Item] = []
    seen_urls: Set[str] = set()
    seen_products: Set[str] = set()
    url = start_url
    pages = 0
    consecutive_empty_pages = 0
    
    # Stop if: reached max_pages OR found 2 consecutive pages with NO new items
    while url and pages < max_pages and consecutive_empty_pages < 2:
        pages += 1
        if logger:
            logger.info(f"Scraping page {pages}: {url}")
        
        pair = get_html_safe(sess, url, delay_ms)
        if pair[0] is None:
            # Fetch failed - add error item
            items.append(Item(
                url=url,
                site=urlparse(url).hostname or '',
                title='',
                price_value=None,
                price_currency=None,
                price_text=f'fetch_failed: {pair[1]}',
                discounted_value=None,
                discounted_formatted='',
                original_formatted='',
                source='error',
                image_url=''
            ))
            break
            
        final_url, html = pair
        soup = BeautifulSoup(html, PARSER)
        
        # Get products from this page
        page_items = scrape_category_page(sess, final_url, html, rules)
        
        # Check for new products
        new_products_found = 0
        for item in page_items:
            if item.url not in seen_products:
                seen_products.add(item.url)
                items.append(item)
                new_products_found += 1
        
        if logger:
            logger.info(f"Page {pages}: Found {len(page_items)} total items, {new_products_found} new items")
        
        # If no new products found, increment counter
        if new_products_found == 0:
            consecutive_empty_pages += 1
            if logger:
                logger.info(f"No new products on page {pages}. Empty pages count: {consecutive_empty_pages}")
        else:
            consecutive_empty_pages = 0
        
        # Mark this URL as seen
        seen_urls.add(final_url)
        
        # Find next page
        nxt = find_next_page_url_legacy(soup, final_url)
        
        # Free memory
        del html
        del soup
        
        if not nxt or nxt in seen_urls:
            if logger:
                logger.info(f"No more pages found or URL already visited")
            break
            
        url = nxt
    
    if logger:
        logger.info(f"Finished scraping after {pages} pages. Total unique items: {len(items)}")
    
    return items


def scrape_url(sess, url: str, rules: Dict, crawl_pagination: bool, 
              max_pages: int, delay_ms: int, logger=None) -> List[Item]:
    """
    Main entry point for scraping a URL.
    Automatically detects if it's a product or category page.
    """
    pair = get_html_safe(sess, url, delay_ms=0)
    if pair[0] is None:
        return [Item(
            url=url,
            site=urlparse(url).hostname or '',
            title='',
            price_value=None,
            price_currency=None,
            price_text=f'fetch_failed: {pair[1]}',
            discounted_value=None,
            discounted_formatted='',
            original_formatted='',
            source='error',
            image_url=''
        )]
    
    final_url, html = pair
    soup = BeautifulSoup(html, PARSER)
    
    if is_product_page(soup):
        return scrape_product(sess, final_url, html, rules)
    
    if is_category_page(soup):
        if crawl_pagination:
            return scrape_category_all_pages(sess, final_url, rules, 
                                            max_pages=max_pages, delay_ms=delay_ms, logger=logger)
        return scrape_category_page(sess, final_url, html, rules)
    
    # Default: treat as product page
    return scrape_product(sess, final_url, html, rules)


def scrape_urls_parallel(urls: List[str], rules: Dict, crawl_pagination: bool, 
                        max_pages: int, delay_ms: int, retries: int, 
                        verify_ssl: bool, use_curl: bool, max_workers: int = 3,
                        logger=None, session_factory=None) -> List[Item]:
    """
    Scrape multiple URLs in parallel for faster processing.
    Each URL gets its own session to avoid threading conflicts.
    """
    all_items: List[Item] = []
    
    def acquire_session():
        if session_factory is not None:
            return session_factory()
        sess, _ = build_session(retries=retries, verify_ssl=verify_ssl, use_curl=use_curl)
        return sess

    def scrape_single_url(url: str) -> List[Item]:
        sess = acquire_session()
        try:
            return scrape_url(sess, url, rules, crawl_pagination, max_pages, delay_ms, logger)
        finally:
            try:
                sess.close()
            except Exception:
                pass
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all URL scraping tasks
        future_to_url = {executor.submit(scrape_single_url, url): url for url in urls}
        
        # Collect results as they complete
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                items = future.result()
                all_items.extend(items)
                if logger:
                    logger.info(f"Completed scraping {url}: {len(items)} items")
            except Exception as e:
                if logger:
                    logger.error(f"Error scraping {url}: {e}")
                # Add error item
                all_items.append(Item(
                    url=url,
                    site=urlparse(url).hostname or '',
                    title='',
                    price_value=None,
                    price_currency=None,
                    price_text=f'parallel_scrape_failed: {e}',
                    discounted_value=None,
                    discounted_formatted='',
                    original_formatted='',
                    source='error',
                    image_url=''
                ))
    
    return all_items
