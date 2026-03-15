"""
Debug script - test cloudscraper vs requests
"""
import requests
import cloudscraper
import time

TEST_URL = "https://www.glamira.bg/glamira-earring-corliss.html?alloy=white-585&diamond=diamond-zirconia&stone2=diamond-sapphire"

# Test 1: requests thường (để so sánh)
print("=== Test 1: requests thường ===")
try:
    resp = requests.get(TEST_URL, timeout=10)
    print(f"Status: {resp.status_code}")
except Exception as e:
    print(f"Error: {e}")
print()

# Test 2: cloudscraper
print("=== Test 2: cloudscraper ===")
try:
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    resp2 = scraper.get(TEST_URL, timeout=15)
    print(f"Status: {resp2.status_code}")
    print(f"Final URL: {resp2.url}")
    if resp2.status_code == 200:
        # Thử parse product name
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp2.text, "html.parser")
        h1 = soup.select_one("h1.page-title span.base")
        print(f"Product name: {h1.get_text(strip=True) if h1 else 'NOT FOUND'}")
        print(f"HTML snippet: {resp2.text[:300]}")
except Exception as e:
    print(f"Error: {e}")