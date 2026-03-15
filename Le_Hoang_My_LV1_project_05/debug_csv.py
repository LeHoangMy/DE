"""
Test so sánh: có header vs không có header
"""
import csv
import requests
import time

INPUT_CSV = "data/product_urls.csv"

# Đọc 1 URL
with open(INPUT_CSV, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    row = next(reader)
    url = row["url"]

print(f"URL: {url}\n")

# Test 1: không có header gì hết
print("=== Test 1: Không header ===")
resp = requests.get(url, timeout=10)
print(f"Status: {resp.status_code}")
time.sleep(5)

# Test 2: chỉ User-Agent
print("\n=== Test 2: Chỉ User-Agent ===")
resp = requests.get(url, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}, timeout=10)
print(f"Status: {resp.status_code}")
time.sleep(5)

# Test 3: không header lần 2
print("\n=== Test 3: Không header lần 2 ===")
resp = requests.get(url, timeout=10)
print(f"Status: {resp.status_code}")