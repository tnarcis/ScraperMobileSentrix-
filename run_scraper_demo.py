from enhanced_scrapers import scrape_with_engine

url = "https://www.mobilesentrix.eu/hello-philips-screwdriver-type-a?___store=default"
products = scrape_with_engine(url, max_pages=1)
for p in products:
    print(f"Title: {p.title}\nDescription: {p.description}\nPrice: {p.price_text}\nImage: {p.image_url}\nURL: {p.url}\n---")
