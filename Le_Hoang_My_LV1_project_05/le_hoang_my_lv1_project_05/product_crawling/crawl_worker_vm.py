"""
Step 6: Product Data Crawler - VM Worker
Chạy: python3 crawl_worker.py --part part1
"""

import argparse
import csv
import json
import logging
import os
import re
import time
import random
from urllib.parse import urlparse, urlunparse, parse_qs
from itertools import cycle

import requests
from bs4 import BeautifulSoup

# ── Parse argument --part ─────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--part", type=str, required=True)
args = parser.parse_args()
PART = args.part

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
INPUT_CSV         = os.path.expanduser(f"~/data/parts/product_urls_{PART}.csv")
OUTPUT_CSV        = os.path.expanduser(f"~/data/product_data_{PART}.csv")
FAILED_CSV        = os.path.expanduser(f"~/data/product_data_failed_{PART}.csv")

REQUEST_DELAY_MIN = 3.0
REQUEST_DELAY_MAX = 6.0
REQUEST_TIMEOUT   = 10
MAX_RETRIES       = 3

# ── 20 Proxies từ 2 accounts ─────────────────────────────────
PROXIES = [
    # Account 1: smwspceb / 05my16oetpx4
    "http://smwspceb:05my16oetpx4@31.59.20.176:6754",
    "http://smwspceb:05my16oetpx4@23.95.150.145:6114",
    "http://smwspceb:05my16oetpx4@198.23.239.134:6540",
    "http://smwspceb:05my16oetpx4@45.38.107.97:6014",
    "http://smwspceb:05my16oetpx4@107.172.163.27:6543",
    "http://smwspceb:05my16oetpx4@198.105.121.200:6462",
    "http://smwspceb:05my16oetpx4@64.137.96.74:6641",
    "http://smwspceb:05my16oetpx4@216.10.27.159:6837",
    "http://smwspceb:05my16oetpx4@142.111.67.146:5611",
    "http://smwspceb:05my16oetpx4@191.96.254.138:6185",
    # Account 3: qhrdfjgj / agj1xh8u39ur
    "http://qhrdfjgj:agj1xh8u39ur@31.59.20.176:6754",
    "http://qhrdfjgj:agj1xh8u39ur@23.95.150.145:6114",
    "http://qhrdfjgj:agj1xh8u39ur@198.23.239.134:6540",
    "http://qhrdfjgj:agj1xh8u39ur@45.38.107.97:6014",
    "http://qhrdfjgj:agj1xh8u39ur@107.172.163.27:6543",
    "http://qhrdfjgj:agj1xh8u39ur@198.105.121.200:6462",
    "http://qhrdfjgj:agj1xh8u39ur@64.137.96.74:6641",
    "http://qhrdfjgj:agj1xh8u39ur@216.10.27.159:6837",
    "http://qhrdfjgj:agj1xh8u39ur@142.111.67.146:5611",
    "http://qhrdfjgj:agj1xh8u39ur@191.96.254.138:6185",
]
proxy_cycle = cycle(PROXIES)

def get_next_proxy() -> dict:
    p = next(proxy_cycle)
    return {"http": p, "https": p}

# ─────────────────────────────────────────────────────────────
OUTPUT_FIELDS = [
    "product_id", "product_name", "product_name_en",
    "price_current", "price_original", "price_min", "price_max", "currency",
    "gender", "alloy", "stone", "diamond", "url_params",
    "source_url", "crawled_url", "is_english",
]
FAILED_FIELDS = ["product_id", "source_url", "fail_reason"]

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser(f"~/crawl_{PART}.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# URL UTILITIES
# ─────────────────────────────────────────────────────────────

def normalize_to_com(url: str) -> str:
    parsed = urlparse(url)
    new_netloc = "www.glamira.com"
    path = parsed.path
    lang_prefix = re.match(r'^/([a-z]{2})(/.*)', path)
    if lang_prefix:
        path = lang_prefix.group(2)
    return urlunparse((parsed.scheme, new_netloc, path, parsed.params, parsed.query, parsed.fragment))


def parse_url_params(url: str) -> dict:
    IGNORE_PARAMS = {"gclid", "fbclid", "utm_source", "utm_medium", "utm_campaign", "s"}
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=False)
    result = {}
    for key, values in params.items():
        if key in IGNORE_PARAMS:
            continue
        val = values[0].strip()
        if val:
            result[key] = val
    return result


def parse_name_from_slug(url: str) -> str | None:
    try:
        path = urlparse(url).path
        slug = path.split("/")[-1]
        slug = re.sub(r'\.html$', '', slug)
        if not slug:
            return None
        return slug.replace("-", " ").title()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# HTTP FETCH
# ─────────────────────────────────────────────────────────────

def fetch_page(url: str) -> tuple[requests.Response | None, str | None]:
    url = normalize_to_com(url)
    for attempt in range(1, MAX_RETRIES + 1):
        proxies = get_next_proxy()
        try:
            resp = requests.get(url, proxies=proxies, timeout=REQUEST_TIMEOUT, allow_redirects=True)

            if resp.status_code == 200:
                final_url = resp.url
                if ".html" not in urlparse(final_url).path and ".html" in urlparse(url).path:
                    return None, "redirect"
                return resp, None
            elif resp.status_code == 404:
                return None, "url_error_404"
            elif resp.status_code >= 500:
                log.warning(f"  Server error {resp.status_code}, attempt {attempt}/{MAX_RETRIES}")
                time.sleep(REQUEST_DELAY_MIN * attempt)
                continue
            else:
                return None, f"url_error_{resp.status_code}"

        except requests.exceptions.Timeout:
            log.warning(f"  Timeout attempt {attempt}/{MAX_RETRIES}")
            time.sleep(REQUEST_DELAY_MIN * attempt)
        except requests.exceptions.ConnectionError:
            log.warning(f"  Connection error attempt {attempt}/{MAX_RETRIES}")
            time.sleep(REQUEST_DELAY_MIN * attempt)
        except Exception as e:
            log.warning(f"  Unexpected error: {e}")
            return None, "url_error_unknown"

    return None, "url_error_timeout"


# ─────────────────────────────────────────────────────────────
# HTML PARSING
# ─────────────────────────────────────────────────────────────

def parse_product_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "product_name":   None,
        "price_current":  None,
        "price_original": None,
        "price_range":    None,
        "currency":       None,
        "gender":         None,
    }

    h1 = soup.select_one("h1.page-title span.base")
    if h1:
        result["product_name"] = h1.get_text(strip=True)

    special_price = soup.select_one("span.special-price")
    if special_price:
        meta_price = special_price.select_one("meta[itemprop='price']")
        if meta_price:
            result["price_current"] = meta_price.get("content")

    if result["price_current"] is None:
        final_price = soup.select_one("span[data-price-type='finalPrice']")
        if final_price:
            result["price_current"] = final_price.get("data-price-amount")

    if result["price_current"] is None:
        meta_price = soup.select_one("meta[itemprop='price']")
        if meta_price:
            result["price_current"] = meta_price.get("content")

    currency_match = re.search(r'"priceCurrency"\s*:\s*"([A-Z]{3})"', html)
    if currency_match:
        result["currency"] = currency_match.group(1)

    old_price_span = soup.select_one("span[id^='old-price-'][data-price-type='oldPrice']")
    if old_price_span:
        result["price_original"] = old_price_span.get("data-price-amount")

    price_range_span = soup.select_one("span.price-range")
    if price_range_span:
        minprice = price_range_span.get("data-minprice", "")
        maxprice = price_range_span.get("data-maxprice", "")
        def clean_price(p):
            p = re.sub(r'(\d)[,\.](\d{3})(?!\d)', r'\1\2', p)
            p = p.replace(',', '.')
            p = re.sub(r'[^\d.\- ]', '', p)
            p = p.lstrip('.')
            return p.strip()
        min_clean = clean_price(minprice)
        max_clean = clean_price(maxprice)
        if min_clean and max_clean:
            result["price_range"] = f"{min_clean} - {max_clean}"

    gender_match = re.search(r'"suggestedGender"\s*:\s*"https://schema\.org/([^"]+)"', html)
    if gender_match:
        result["gender"] = gender_match.group(1)

    return result


# ─────────────────────────────────────────────────────────────
# MAIN CRAWLER
# ─────────────────────────────────────────────────────────────

def crawl_products(test_mode: bool = False, test_limit: int = 20):
    rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    done_ids = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done_ids.add(r["product_id"])
    if os.path.exists(FAILED_CSV):
        with open(FAILED_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done_ids.add(r["product_id"])
    if done_ids:
        log.info(f"⏩ Resume: đã xử lý {len(done_ids)} sản phẩm, skip qua")

    rows = [r for r in rows if r["product_id"] not in done_ids]

    if test_mode:
        rows = rows[:test_limit]
        log.info(f"🧪 TEST MODE: crawl {len(rows)} sản phẩm")

    log.info(f"📋 [{PART}] Còn lại cần crawl: {len(rows)}")

    if not rows:
        log.info("✅ Tất cả đã crawl xong!")
        return

    is_new = len(done_ids) == 0
    out_f    = open(OUTPUT_CSV,  "a" if not is_new else "w", newline="", encoding="utf-8")
    failed_f = open(FAILED_CSV,  "a" if not is_new else "w", newline="", encoding="utf-8")
    writer        = csv.DictWriter(out_f,    fieldnames=OUTPUT_FIELDS)
    failed_writer = csv.DictWriter(failed_f, fieldnames=FAILED_FIELDS)
    if is_new:
        writer.writeheader()
        failed_writer.writeheader()

    success_count = 0
    failed_count  = 0

    try:
        for i, row in enumerate(rows, 1):
            product_id = row["product_id"]
            source_url = row["url"]

            log.info(f"[{i}/{len(rows)}] product_id={product_id}")

            url_params      = parse_url_params(source_url)
            url_params_json = json.dumps(url_params) if url_params else None
            product_name_en = parse_name_from_slug(source_url)

            log.info(f"  🔗 Crawling: {source_url}")
            resp, fail_reason = fetch_page(source_url)

            if resp is None:
                log.warning(f"  ❌ FAILED: {fail_reason}")
                failed_writer.writerow({
                    "product_id":  product_id,
                    "source_url":  source_url,
                    "fail_reason": fail_reason,
                })
                failed_count += 1
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
                continue

            parsed = parse_product_page(resp.text)

            if parsed["product_name"] is None:
                log.warning(f"  ❌ no_product_name")
                failed_writer.writerow({
                    "product_id":  product_id,
                    "source_url":  source_url,
                    "fail_reason": "no_product_name",
                })
                failed_count += 1
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
                continue

            price_min, price_max = None, None
            if parsed["price_range"]:
                parts = parsed["price_range"].split(" - ")
                if len(parts) == 2:
                    price_min = parts[0].strip()
                    price_max = parts[1].strip()

            writer.writerow({
                "product_id":      product_id,
                "product_name":    parsed["product_name"],
                "product_name_en": product_name_en,
                "price_current":   parsed["price_current"],
                "price_original":  parsed["price_original"],
                "price_min":       price_min,
                "price_max":       price_max,
                "currency":        parsed["currency"],
                "gender":          parsed["gender"],
                "alloy":           url_params.get("alloy"),
                "stone":           url_params.get("stone"),
                "diamond":         url_params.get("diamond"),
                "url_params":      url_params_json,
                "source_url":      source_url,
                "crawled_url":     source_url,
                "is_english":      False,
            })

            log.info(f"  ✅ {parsed['product_name']} | {parsed['price_current']} {parsed['currency']}")
            success_count += 1
            out_f.flush()
            failed_f.flush()

            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    finally:
        out_f.close()
        failed_f.close()

    log.info(f"\n{'='*50}")
    log.info(f"✅ [{PART}] Success : {success_count}")
    log.info(f"❌ [{PART}] Failed  : {failed_count}")


if __name__ == "__main__":
    crawl_products(test_mode=False)
