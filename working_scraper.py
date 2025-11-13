"""Working MobileSentrix Scraper - Complete Rewrite"""
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
import re
import time

@dataclass
class WorkingItem:
    title: str
    url: str
    site: str = "mobilesentrix.com"
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
    source: str = "working_scraper"

def clean_text(text: str) -> str:
    """Clean text content"""
    if not text:
        return ""
    return ' '.join(text.strip().split())

def parse_price(text: str) -> tuple[Optional[float], str]:
    """Parse price from text"""
    if not text:
        return None, "USD"
    
    # Extract price using regex
    price_match = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
    if price_match:
        try:
            return float(price_match.group(1)), "USD"
        except ValueError:
            pass
    return None, "USD"

def working_scraper(url: str) -> List[WorkingItem]:
    """Working MobileSentrix scraper that actually works"""
    print(f"Scraping: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    try:
        # Make request
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        print(f"Response: {response.status_code}, Content: {len(response.text):,} chars")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find products
        products = soup.select('li.item')
        print(f"Found {len(products)} product containers")
        
        if not products:
            print("No li.item found, trying alternatives...")
            for selector in ['.product-item', '.item', '.product']:
                products = soup.select(selector)
                if products:
                    print(f"Found {len(products)} with selector: {selector}")
                    break
        
        items = []
        for i, product in enumerate(products):
            try:
                # Get title and link
                link = product.select_one('a[href]')
                if not link:
                    continue
                
                title = clean_text(link.get_text())
                href = link.get('href', '')
                
                if not title or len(title) < 5:
                    continue
                
                # Make absolute URL
                if href.startswith('/'):
                    product_url = f"https://www.mobilesentrix.com{href}"
                elif href.startswith('http'):
                    product_url = href
                else:
                    continue
                
                # Get price
                price_text = ""
                price_value = None
                
                # Try multiple price selectors
                for price_selector in ['.price', '[class*="price"]', '.cost', '.amount']:
                    price_elem = product.select_one(price_selector)
                    if price_elem:
                        price_text = clean_text(price_elem.get_text())
                        if '$' in price_text:
                            price_value, _ = parse_price(price_text)
                            break
                
                # Get image
                image_url = ""
                img = product.select_one('img')
                if img:
                    image_url = img.get('src', '') or img.get('data-src', '')
                    if image_url and image_url.startswith('/'):
                        image_url = f"https://www.mobilesentrix.com{image_url}"
                
                # Create item
                item = WorkingItem(
                    title=title,
                    url=product_url,
                    price_text=price_text,
                    price_value=price_value,
                    price_currency="USD",
                    image_url=image_url,
                    scraped_at=datetime.utcnow().isoformat() + 'Z'
                )
                
                items.append(item)
                
            except Exception as e:
                print(f"Error parsing product {i+1}: {e}")
                continue
        
        print(f"Successfully extracted {len(items)} items")
        return items
        
    except Exception as e:
        print(f"Scraping error: {e}")
        return []

def test_working_scraper():
    """Test the working scraper"""
    test_url = "https://www.mobilesentrix.com/replacement-parts/samsung/galaxy-s-series/galaxy-s25-edge"
    
    print("=== TESTING WORKING SCRAPER ===")
    start_time = time.time()
    
    items = working_scraper(test_url)
    
    elapsed = time.time() - start_time
    print(f"\nRESULT: {len(items)} items in {elapsed:.1f} seconds")
    
    if items:
        print("\nFirst 3 items:")
        for i, item in enumerate(items[:3], 1):
            print(f"{i}. {item.title[:60]}...")
            print(f"   Price: {item.price_text} (${item.price_value})")
            print(f"   URL: {item.url[:60]}...")
            print()
    else:
        print("❌ NO ITEMS FOUND")
    
    return items

if __name__ == "__main__":
    test_working_scraper()