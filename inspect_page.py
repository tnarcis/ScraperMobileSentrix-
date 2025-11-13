#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import time

def inspect_mobilesentrix_page():
    """Inspect the actual HTML structure of MobileSentrix pages"""
    
    test_url = "https://www.mobilesentrix.com/replacement-parts/apple/iphone-parts/iphone-4-4s"
    
    print(f"🔍 Inspecting: {test_url}")
    
    try:
        # Make request with proper headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(test_url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            print(f"📄 Page Title: {soup.title.string if soup.title else 'No title'}")
            
            # Look for product containers
            print("\n🔍 Looking for product containers...")
            
            # Check various container possibilities
            selectors_to_check = [
                'ul.product-listing',
                '.products-grid',
                '.product-list',
                '.category-products',
                '.products',
                'ul[class*="product"]',
                'div[class*="product"]',
                'main',
                '#main',
                '.container'
            ]
            
            for selector in selectors_to_check:
                containers = soup.select(selector)
                if containers:
                    print(f"  ✅ Found {len(containers)} containers with selector: {selector}")
                    for i, container in enumerate(containers[:2]):  # Show first 2
                        print(f"     Container {i}: classes={container.get('class', [])}")
                        
                        # Look for product items within
                        items = container.select('li, .product, .item, [class*="product"]')
                        print(f"     Contains {len(items)} potential product items")
                        
                        if items:
                            print(f"     Sample item classes: {items[0].get('class', [])}")
                else:
                    print(f"  ❌ No containers found with: {selector}")
            
            # Look for any elements that might contain product info
            print(f"\n🏷️ Looking for price indicators...")
            price_elements = soup.select('[class*="price"], .price, [class*="cost"], [class*="dollar"]')
            print(f"Found {len(price_elements)} elements with price-related classes")
            
            if price_elements:
                for i, elem in enumerate(price_elements[:3]):
                    print(f"  Price element {i}: {elem.name} class={elem.get('class')} text='{elem.get_text().strip()[:50]}'")
            
            print(f"\n🏷️ Looking for product titles...")
            title_elements = soup.select('[class*="title"], .title, h1, h2, h3, h4')
            print(f"Found {len(title_elements)} potential title elements")
            
            if title_elements:
                for i, elem in enumerate(title_elements[:5]):
                    text = elem.get_text().strip()
                    if text and len(text) > 10:
                        print(f"  Title {i}: {elem.name} class={elem.get('class')} text='{text[:100]}'")
            
        else:
            print(f"❌ Failed to fetch page: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    inspect_mobilesentrix_page()