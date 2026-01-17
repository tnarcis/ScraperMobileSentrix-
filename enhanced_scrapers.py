"""Clean working version of enhanced_scrapers.py"""
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime
from logger import logger

@dataclass
class ScrapedItem:
    """Enhanced scraped item with all required fields"""
    title: str
    url: str
    site: str
    price_value: Optional[float] = None
    price_currency: str = "USD"
    discounted_value: Optional[float] = None
    original_formatted: str = ""
    discounted_formatted: str = ""
    price_text: str = ""
    image_url: str = ""
    stock_status: str = ""
    sku: str = ""
    category_path: str = ""
    scraped_at: str = ""
    source: str = "enhanced_scraper"
    description: str = ""

def clean_text(text: str) -> str:
    """Clean and normalize text content"""
    if not text:
        return ""
    return ' '.join(text.strip().split())

def normalize_url(href: str, base_url: str) -> str:
    """Normalize relative URLs to absolute URLs"""
    if not href:
        return ""
    
    if href.startswith('http'):
        return href
    elif href.startswith('//'):
        return f"https:{href}"
    elif href.startswith('/'):
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    else:
        return f"{base_url.rstrip('/')}/{href.lstrip('/')}"

class BaseScrapeEngine:
    """Base scraper engine"""
    def __init__(self):
        self.session = requests.Session()
        self.site_name = "unknown"
        
    def add_delay(self):
        """Add random delay"""
        time.sleep(random.uniform(0.1, 0.3))

class MobileSentrixEngine(BaseScrapeEngine):
    """WORKING MobileSentrix scraper - completely rewritten"""
    
    def __init__(self):
        super().__init__()
        self.site_name = "mobilesentrix.com"
    
    def discover_categories(self, seed_url: str) -> List[str]:
        """Simple category discovery"""
        return [seed_url]  # For now, just return the input URL
    
    def extract_products(self, category_url: str, max_pages: int = 5) -> List[ScrapedItem]:
        """Working product extraction"""
        items = []
        
        try:
            # Direct request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(category_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get category path
            category_path = ""
            try:
                breadcrumbs = soup.select('.breadcrumbs a')
                if breadcrumbs:
                    category_path = " > ".join([clean_text(bc.get_text()) for bc in breadcrumbs[1:]])
            except:
                pass
            
            # Find products
            products = soup.select('li.item')
            
            if not products:
                # Try alternatives
                for selector in ['.product-item', '.item', '.product']:
                    products = soup.select(selector)
                    if products:
                        break
            
            # Parse each product
            for product in products:
                try:
                    item = self.parse_product_card(product, category_url, category_path)
                    if item:
                        items.append(item)
                except Exception:
                    continue
            
            return items
            
        except Exception as e:
            logger.error("extraction_failed", error=str(e), url=category_url)
            return []
    
    def parse_product_card(self, card_elem, category_url: str, category_path: str = "") -> Optional[ScrapedItem]:
        """Simple working product parser with description extraction"""
        try:
            # Get link and title
            link = card_elem.select_one('a[href]')
            if not link:
                return None
            title = clean_text(link.get_text())
            href = link.get('href', '')
            if not title or len(title) < 5:
                return None
            # Make absolute URL
            if href.startswith('/'):
                product_url = f"https://www.mobilesentrix.com{href}"
            elif href.startswith('http'):
                product_url = href
            else:
                return None
            # Get price
            price_text = ""
            price_value = None
            for price_selector in ['.price', '[class*="price"]']:
                price_elem = card_elem.select_one(price_selector)
                if price_elem:
                    price_text = clean_text(price_elem.get_text())
                    if '$' in price_text:
                        price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                        if price_match:
                            try:
                                price_value = float(price_match.group(1))
                            except:
                                pass
                        break
            # Get image
            image_url = ""
            img = card_elem.select_one('img')
            if img:
                image_url = img.get('src', '') or img.get('data-src', '')
                if image_url and image_url.startswith('/'):
                    image_url = f"https://www.mobilesentrix.com{image_url}"
            # Get description from product page
            description = ""
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = self.session.get(product_url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    prod_soup = BeautifulSoup(resp.text, 'html.parser')
                    # Caută descrierea în câteva locuri comune
                    desc_elem = prod_soup.select_one('.product.attribute.description, #description, .description, .product-description, [itemprop="description"]')
                    if desc_elem:
                        description = clean_text(desc_elem.get_text())
            except Exception:
                pass
            return ScrapedItem(
                title=title,
                url=product_url,
                site=self.site_name,
                price_value=price_value,
                price_currency="USD",
                price_text=price_text,
                image_url=image_url,
                category_path=category_path,
                scraped_at=datetime.utcnow().isoformat() + 'Z',
                source="mobilesentrix_working",
                description=description
            )
        except Exception:
            return None

class XCellPartsEngine(BaseScrapeEngine):
    """XCell scraper - keeping existing functionality"""
    
    def __init__(self):
        super().__init__()
        self.site_name = "xcellparts.com"
    
    def extract_products(self, category_url: str, max_pages: int = 5) -> List[ScrapedItem]:
        """Basic XCell extraction"""
        return []  # Placeholder
    
    def parse_product_card(self, card_elem, category_url: str, category_path: str = "") -> Optional[ScrapedItem]:
        """Basic XCell parser"""
        return None  # Placeholder

def get_scraper_for_url(url: str):
    """Get appropriate scraper for URL"""
    from urllib.parse import urlparse
    host = urlparse(url).netloc.lower()
    
    if "mobilesentrix.com" in host:
        return "mobilesentrix", MobileSentrixEngine()
    elif "xcellparts.com" in host:
        return "xcell", XCellPartsEngine()
    else:
        return "mobilesentrix", MobileSentrixEngine()  # Default


def scrape_with_engine(url: str, max_pages: int = 5) -> List[ScrapedItem]:
    """Convenience helper that delegates scraping to the matched engine."""

    _, engine = get_scraper_for_url(url)
    lowered = url.lower()

    if any(token in lowered for token in ["/category", "/product-category", "/parts", "/shop"]):
        return engine.extract_products(url, max_pages)

    items: List[ScrapedItem] = []
    for category in engine.discover_categories(url)[:3]:
        items.extend(engine.extract_products(category, max_pages))
    return items


if __name__ == "__main__":
    print(":: Smoke testing MobileSentrix engine ::")
    ms_engine = MobileSentrixEngine()
    products = ms_engine.extract_products("https://www.mobilesentrix.com/", max_pages=1)
    print(f"Scraped {len(products)} items")