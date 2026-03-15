"""
Step 6: Product Data Crawler
=============================
Flow:
  1. Đọc product_urls.csv
  2. Parse url_params từ query string
  3. Normalize URL → glamira.com, fallback về URL gốc nếu .com fail
  4. Crawl HTML: product_name, price_current, price_original,
     price_range, currency, gender
  5. Lưu product_data.csv + product_data_failed.csv
"""

import csv
import json
import logging
import os
import re
import time
import random
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
INPUT_CSV        = "data/product_urls.csv"
OUTPUT_CSV       = "data/product_data.csv"
FAILED_CSV       = "data/product_data_failed.csv"

REQUEST_DELAY_MIN = 3.0   # giây delay tối thiểu
REQUEST_DELAY_MAX = 6.0   # giây delay tối đa (random)
REQUEST_TIMEOUT  = 10    # giây timeout
MAX_RETRIES      = 3     # số lần retry

OUTPUT_FIELDS = [
    "product_id", "product_name", "product_name_en",
    "price_current", "price_original", "price_min", "price_max", "currency",
    "gender", "alloy", "stone", "diamond", "url_params",
    "source_url", "crawled_url", "is_english",
]

FAILED_FIELDS = ["product_id", "source_url", "fail_reason"]

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("product_crawler.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# URL UTILITIES
# ──────────────────────────────────────────────────────────────

def normalize_to_com(url: str) -> str:
    """
    Đổi domain glamira.XX → www.glamira.com
    Xử lý cả trường hợp có language path: glamira.be/fr/... → glamira.com/...
    Giữ nguyên path .html và query string
    """
    parsed = urlparse(url)
    
    # Đổi domain → www.glamira.com
    new_netloc = "www.glamira.com"
    
    # Xử lý path có language prefix: /fr/..., /de/..., /nl/...
    # VD: /fr/bague-pour-femmes.html → /bague-pour-femmes.html
    path = parsed.path
    lang_prefix = re.match(r'^/([a-z]{2})(/.*)', path)
    if lang_prefix:
        path = lang_prefix.group(2)
    
    new_url = urlunparse((
        parsed.scheme,
        new_netloc,
        path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))
    return new_url


def parse_url_params(url: str) -> dict:
    """
    Extract query string params từ URL
    VD: ?alloy=red-750&diamond=aquamarine → {"alloy": "red-750", "diamond": "aquamarine"}
    Bỏ qua các tracking params: gclid, fbclid, utm_*
    """
    IGNORE_PARAMS = {"gclid", "fbclid", "utm_source", "utm_medium", "utm_campaign", "s"}
    
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=False)
    
    result = {}
    for key, values in params.items():
        if key in IGNORE_PARAMS:
            continue
        # parse_qs trả về list, lấy value đầu tiên
        val = values[0].strip()
        if val:
            result[key] = val
    
    return result


# ──────────────────────────────────────────────────────────────
# HTTP FETCH
# ──────────────────────────────────────────────────────────────

def create_session():
    """Tạo requests session đơn giản - glamira không block requests thường"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
    return session


def parse_name_from_slug(url: str) -> str | None:
    """
    Parse product name từ URL slug.
    VD: https://www.glamira.fr/glamira-pendant-viktor.html?alloy=yellow-375
        → "Glamira Pendant Viktor"
    """
    try:
        path = urlparse(url).path
        slug = path.split("/")[-1]
        slug = re.sub(r'\.html$', '', slug)
        if not slug:
            return None
        name = slug.replace("-", " ").title()
        return name
    except Exception:
        return None


def fetch_page(url: str) -> tuple[requests.Response | None, str | None]:
    """
    Fetch URL với retry.
    Returns: (response, fail_reason)
    - Success: (response, None)
    - Fail:    (None, fail_reason)
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            
            if resp.status_code == 200:
                # Kiểm tra có bị redirect sang trang khác không
                final_url = resp.url
                parsed_original = urlparse(url)
                parsed_final    = urlparse(final_url)
                
                # Nếu path thay đổi hoàn toàn (không còn .html) → có thể redirect về homepage
                if ".html" not in parsed_final.path and ".html" in parsed_original.path:
                    return None, "redirect"
                
                return resp, None
            
            elif resp.status_code == 404:
                return None, "url_error_404"
            
            elif resp.status_code >= 500:
                log.warning(f"  Server error {resp.status_code}, attempt {attempt}/{MAX_RETRIES}: {url}")
                time.sleep(REQUEST_DELAY_MIN * attempt)
                continue
            
            else:
                return None, f"url_error_{resp.status_code}"
        
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout attempt {attempt}/{MAX_RETRIES}: {url}")
            time.sleep(REQUEST_DELAY_MIN * attempt)
        
        except requests.exceptions.ConnectionError:
            log.warning(f"  Connection error attempt {attempt}/{MAX_RETRIES}: {url}")
            time.sleep(REQUEST_DELAY_MIN * attempt)
        
        except Exception as e:
            log.warning(f"  Unexpected error: {e}")
            return None, "url_error_unknown"
    
    return None, "url_error_timeout"


# ──────────────────────────────────────────────────────────────
# HTML PARSING
# ──────────────────────────────────────────────────────────────

def parse_product_page(html: str) -> dict:
    """
    Parse HTML → extract tất cả fields cần thiết.
    Trả về dict với None nếu field không tìm thấy.
    """
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "product_name":   None,
        "price_current":  None,
        "price_original": None,
        "price_range":    None,
        "currency":       None,
        "gender":         None,
    }

    # ── product_name ──────────────────────────────────────────
    # <h1 class="page-title"><span class="base">Viktor Men's Pendant</span></h1>
    h1 = soup.select_one("h1.page-title span.base")
    if h1:
        result["product_name"] = h1.get_text(strip=True)

    # ── price_current ─────────────────────────────────────────
    # Lấy từ meta tag bên trong span.special-price (có discount)
    special_price = soup.select_one("span.special-price")
    if special_price:
        meta_price = special_price.select_one("meta[itemprop='price']")
        if meta_price:
            result["price_current"] = meta_price.get("content")

    # Fallback: lấy từ span[data-price-type="finalPrice"] nếu không có special-price
    if result["price_current"] is None:
        final_price = soup.select_one("span[data-price-type='finalPrice']")
        if final_price:
            result["price_current"] = final_price.get("data-price-amount")

    # Fallback cuối: bất kỳ meta[itemprop="price"] nào
    if result["price_current"] is None:
        meta_price = soup.select_one("meta[itemprop='price']")
        if meta_price:
            result["price_current"] = meta_price.get("content")

    # ── currency ──────────────────────────────────────────────
    # Nằm trong JSON-LD: "priceCurrency":"EUR"
    currency_match = re.search(r'"priceCurrency"\s*:\s*"([A-Z]{3})"', html)
    if currency_match:
        result["currency"] = currency_match.group(1)

    # ── price_original ────────────────────────────────────────
    # <span id="old-price-XXXXX" data-price-amount="463" data-price-type="oldPrice">
    old_price_span = soup.select_one("span[id^='old-price-'][data-price-type='oldPrice']")
    if old_price_span:
        result["price_original"] = old_price_span.get("data-price-amount")

    # ── price_range ───────────────────────────────────────────
    # <span class="price-range" data-minprice="150,00 €" data-maxprice="2 140,00 €">
    price_range_span = soup.select_one("span.price-range")
    if price_range_span:
        minprice = price_range_span.get("data-minprice", "")
        maxprice = price_range_span.get("data-maxprice", "")
        def clean_price(p):
            # Bước 1: bỏ separator hàng nghìn (1.234,56 hoặc 1,234.56)
            p = re.sub(r'(\d)[,\.](\d{3})(?!\d)', r'\1\2', p)
            # Bước 2: đổi dấu phẩy thập phân → dấu chấm
            p = p.replace(',', '.')
            # Bước 3: chỉ giữ lại số, dấu chấm, dấu gạch ngang, space
            p = re.sub(r'[^\d.\- ]', '', p)
            # Bước 4: bỏ dấu chấm thừa ở đầu
            p = p.lstrip('.')
            return p.strip()
        min_clean = clean_price(minprice)
        max_clean = clean_price(maxprice)
        if min_clean and max_clean:
            result["price_range"] = f"{min_clean} - {max_clean}"

    # ── gender ────────────────────────────────────────────────
    # Lấy từ schema.org JSON-LD: "suggestedGender":"https://schema.org/Male"
    gender_match = re.search(r'"suggestedGender"\s*:\s*"https://schema\.org/([^"]+)"', html)
    if gender_match:
        result["gender"] = gender_match.group(1)  # "Male", "Female", "Unisex"...

    return result


# ──────────────────────────────────────────────────────────────
# MAIN CRAWLER
# ──────────────────────────────────────────────────────────────

def crawl_products(test_mode: bool = True, test_limit: int = 20):
    """
    Main function: đọc CSV → crawl → lưu kết quả
    Resume: tự động skip các product_id đã crawl thành công
    """
    # Đọc input
    rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    # ── Resume: load các product_id đã xử lý (cả success lẫn fail) ──
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

    # Filter ra những sản phẩm chưa crawl
    rows = [r for r in rows if r["product_id"] not in done_ids]

    if test_mode:
        rows = rows[:test_limit]
        log.info(f"🧪 TEST MODE: crawl tiếp {len(rows)} sản phẩm")

    log.info(f"📋 Còn lại cần crawl: {len(rows)}")

    if not rows:
        log.info("✅ Tất cả đã crawl xong!")
        return

    # Khởi tạo output files — append nếu đã có, write mới nếu chưa
    is_new = len(done_ids) == 0
    out_f    = open(OUTPUT_CSV, "a" if not is_new else "w", newline="", encoding="utf-8")
    failed_f = open(FAILED_CSV, "a" if not is_new else "w", newline="", encoding="utf-8")

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
            
            # ── Parse url_params và product_name_en từ URL gốc ─
            url_params = parse_url_params(source_url)
            url_params_json = json.dumps(url_params) if url_params else None
            product_name_en = parse_name_from_slug(source_url)

            # ── Dùng thẳng source URL, chưa normalize .com ───
            crawled_url = source_url
            is_english  = False  # tạm thời chưa xử lý

            log.info(f"  🔗 Crawling: {source_url}")

            resp, fail_reason = fetch_page(source_url)

            # ── Nếu fail ──────────────────────────────────────
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

            # ── Parse HTML ────────────────────────────────────
            parsed = parse_product_page(resp.text)
            
            # Nếu không có product_name → fail
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

            # ── Tách price_min / price_max từ price_range ────
            price_min, price_max = None, None
            if parsed["price_range"]:
                parts = parsed["price_range"].split(" - ")
                if len(parts) == 2:
                    price_min = parts[0].strip()
                    price_max = parts[1].strip()

            # ── Ghi kết quả ───────────────────────────────────
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
                "crawled_url":     crawled_url,
                "is_english":      is_english,
            })
            
            log.info(f"  ✅ {parsed['product_name']} | {parsed['price_current']} {parsed['currency']}")
            success_count += 1

            # Flush để không mất data nếu crash giữa chừng
            out_f.flush()
            failed_f.flush()
            
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    finally:
        out_f.close()
        failed_f.close()

    log.info(f"\n{'='*50}")
    log.info(f"✅ Success : {success_count}")
    log.info(f"❌ Failed  : {failed_count}")
    log.info(f"📁 Output  : {OUTPUT_CSV}")
    log.info(f"📁 Failed  : {FAILED_CSV}")


# ──────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # test_mode=True để test 20 sản phẩm trước
    # Khi ok rồi đổi thành test_mode=False
    crawl_products(test_mode=True, test_limit=3)