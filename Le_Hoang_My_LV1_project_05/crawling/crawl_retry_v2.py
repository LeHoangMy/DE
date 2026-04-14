"""
Step 6 Retry - Crawl bằng catalog URL + parse react_data
URL pattern: https://www.glamira.com/catalog/product/view/id/{product_id}
Fallback: .co.uk → .com.au
Output:
  - ~/data/product_data_v2.csv       (schema giống cũ)
  - ~/data/product_data_v2_raw.csv   (product_id + full react_data JSON)
  - ~/data/product_data_v2_failed.csv
"""

import csv
import json
import logging
import os
import re
import time
import random
import threading
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
DATA_DIR     = os.path.expanduser("~/data")
OUTPUT_CSV   = os.path.join(DATA_DIR, "product_data_v2.csv")
RAW_CSV      = os.path.join(DATA_DIR, "product_data_v2_raw.csv")
FAILED_CSV   = os.path.join(DATA_DIR, "product_data_v2_failed.csv")
SUCCESS_CSV  = os.path.join(DATA_DIR, "product_data.csv")

REQUEST_DELAY_MIN = 3.0
REQUEST_DELAY_MAX = 6.0
REQUEST_TIMEOUT   = 15
MAX_RETRIES       = 3
NUM_THREADS       = 3

DOMAIN_FALLBACKS = [
    "www.glamira.com",
    "www.glamira.co.uk",
    "www.glamira.com.au",
]

# ── 10 Proxies - account mới: yeaeklvg ───────────────────────
PROXIES = [
    "http://yeaeklvg:khnu95avthol@31.59.20.176:6754",
    "http://yeaeklvg:khnu95avthol@23.95.150.145:6114",
    "http://yeaeklvg:khnu95avthol@198.23.239.134:6540",
    "http://yeaeklvg:khnu95avthol@45.38.107.97:6014",
    "http://yeaeklvg:khnu95avthol@107.172.163.27:6543",
    "http://yeaeklvg:khnu95avthol@198.105.121.200:6462",
    "http://yeaeklvg:khnu95avthol@64.137.96.74:6641",
    "http://yeaeklvg:khnu95avthol@216.10.27.159:6837",
    "http://yeaeklvg:khnu95avthol@142.111.67.146:5611",
    "http://yeaeklvg:khnu95avthol@191.96.254.138:6185",
]

# Thread-safe proxy cycle
_proxy_lock = threading.Lock()
_proxy_cycle = cycle(PROXIES)

def get_next_proxy():
    with _proxy_lock:
        p = next(_proxy_cycle)
    return {"http": p, "https": p}

# ─────────────────────────────────────────────────────────────
OUTPUT_FIELDS = [
    "product_id", "product_name", "product_name_en",
    "price_current", "price_original", "price_min", "price_max", "currency",
    "gender", "alloy", "stone", "diamond", "url_params",
    "source_url", "crawled_url", "is_english",
]
RAW_FIELDS    = ["product_id", "crawled_url", "react_data"]
FAILED_FIELDS = ["product_id", "crawled_url", "fail_reason"]

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "crawl_v2.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# THREAD-SAFE WRITE LOCK
# ─────────────────────────────────────────────────────────────
_write_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# LOAD FAILED IDs
# ─────────────────────────────────────────────────────────────

def load_failed_ids(success_ids: set) -> set:
    failed_ids = set()
    for i in range(1, 6):
        path = os.path.join(DATA_DIR, f"product_data_failed_part{i}.csv")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                pid = str(r.get("product_id", "")).strip()
                if pid and pid not in success_ids:
                    failed_ids.add(pid)
    return failed_ids


# ─────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────

def fetch_catalog_url(product_id: str) -> tuple[str | None, str | None]:
    for domain in DOMAIN_FALLBACKS:
        url = f"https://{domain}/catalog/product/view/id/{product_id}"
        for attempt in range(1, MAX_RETRIES + 1):
            proxies = get_next_proxy()
            try:
                resp = requests.get(url, proxies=proxies, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if resp.status_code == 200:
                    if "var react_data" in resp.text:
                        return resp.text, resp.url
                    else:
                        log.warning(f"  {domain}: 200 nhưng không có react_data")
                        break
                elif resp.status_code == 404:
                    log.warning(f"  {domain}: 404")
                    break
                else:
                    log.warning(f"  {domain}: {resp.status_code} attempt {attempt}/{MAX_RETRIES}")
                    time.sleep(2 * attempt)
            except requests.exceptions.Timeout:
                log.warning(f"  {domain}: Timeout attempt {attempt}/{MAX_RETRIES}")
                time.sleep(2 * attempt)
            except requests.exceptions.ConnectionError:
                log.warning(f"  {domain}: ConnectionError attempt {attempt}/{MAX_RETRIES}")
                time.sleep(2 * attempt)
            except Exception as e:
                log.warning(f"  {domain}: {e}")
                break
    return None, None


# ─────────────────────────────────────────────────────────────
# PARSE react_data
# ─────────────────────────────────────────────────────────────

def extract_react_data(html: str) -> dict | None:
    match = re.search(r'var react_data\s*=\s*(\{.*?\});\s*</script>', html, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except Exception as e:
        log.warning(f"  JSON parse error: {e}")
        return None


def parse_react_data(data: dict) -> dict:
    result = {
        "product_name":   data.get("name"),
        "price_current":  data.get("price"),
        "price_min":      data.get("min_price"),
        "price_max":      data.get("max_price"),
        "currency":       None,
        "gender":         data.get("gender"),
        "alloy":          None,
        "stone":          None,
        "diamond":        None,
        "price_original": None,
    }

    pp = data.get("product_price", {})
    result["currency"] = pp.get("currencyCode")

    prices = pp.get("prices", {})
    old_amt   = prices.get("oldPrice",   {}).get("amount")
    final_amt = prices.get("finalPrice", {}).get("amount")
    if old_amt and final_amt and float(old_amt) != float(final_amt):
        result["price_original"] = str(old_amt)

    for opt in data.get("options", []):
        group  = opt.get("group")
        part   = opt.get("part_type", "")
        values = opt.get("values", [])
        default_val = next((v for v in values if v.get("is_default")), None)
        if not default_val and values:
            default_val = values[0]
        if not default_val:
            continue
        sku = default_val.get("sku", "")
        if group == "alloy":
            result["alloy"] = sku
        elif group == "stone":
            if part == "stone1":
                result["stone"] = sku
            elif part == "stone2":
                result["diamond"] = sku

    return result


# ─────────────────────────────────────────────────────────────
# PROCESS ONE PRODUCT
# ─────────────────────────────────────────────────────────────

def process_product(pid: str, idx: int, total: int,
                    writer, raw_writer, failed_writer,
                    out_f, raw_f, failed_f,
                    counter: dict):
    log.info(f"[{idx}/{total}] product_id={pid}")
    catalog_url = f"https://www.glamira.com/catalog/product/view/id/{pid}"

    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    html, crawled_url = fetch_catalog_url(pid)

    if html is None:
        log.warning(f"  ❌ [{pid}] all domains failed")
        with _write_lock:
            failed_writer.writerow({"product_id": pid, "crawled_url": catalog_url, "fail_reason": "all_domains_failed"})
            failed_f.flush()
            counter["failed"] += 1
        return

    react_data = extract_react_data(html)
    if react_data is None:
        log.warning(f"  ❌ [{pid}] no react_data")
        with _write_lock:
            failed_writer.writerow({"product_id": pid, "crawled_url": crawled_url, "fail_reason": "no_react_data"})
            failed_f.flush()
            counter["failed"] += 1
        return

    parsed = parse_react_data(react_data)

    if not parsed["product_name"]:
        log.warning(f"  ❌ [{pid}] no product_name")
        with _write_lock:
            failed_writer.writerow({"product_id": pid, "crawled_url": crawled_url, "fail_reason": "no_product_name"})
            failed_f.flush()
            counter["failed"] += 1
        return

    with _write_lock:
        writer.writerow({
            "product_id":      pid,
            "product_name":    parsed["product_name"],
            "product_name_en": parsed["product_name"],
            "price_current":   parsed["price_current"],
            "price_original":  parsed["price_original"],
            "price_min":       parsed["price_min"],
            "price_max":       parsed["price_max"],
            "currency":        parsed["currency"],
            "gender":          parsed["gender"],
            "alloy":           parsed["alloy"],
            "stone":           parsed["stone"],
            "diamond":         parsed["diamond"],
            "url_params":      None,
            "source_url":      catalog_url,
            "crawled_url":     crawled_url,
            "is_english":      True,
        })
        raw_writer.writerow({
            "product_id":  pid,
            "crawled_url": crawled_url,
            "react_data":  json.dumps(react_data, ensure_ascii=False),
        })
        out_f.flush()
        raw_f.flush()
        counter["success"] += 1

    log.info(f"  ✅ [{pid}] {parsed['product_name']} | {parsed['price_current']} {parsed['currency']}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main(test_mode=False, test_limit=10):
    # 1. Load success ids
    success_ids = set()
    if os.path.exists(SUCCESS_CSV):
        with open(SUCCESS_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                success_ids.add(str(r["product_id"]).strip())
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                success_ids.add(str(r["product_id"]).strip())
    log.info(f"✅ Success product_ids (skip): {len(success_ids)}")

    # 2. Load failed ids
    failed_ids = load_failed_ids(success_ids)
    log.info(f"❌ Failed product_ids cần retry: {len(failed_ids)}")

    # 3. Skip đã xử lý trong v2_failed
    done_ids = set()
    if os.path.exists(FAILED_CSV):
        with open(FAILED_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done_ids.add(str(r.get("product_id", "")).strip())

    todo = [pid for pid in sorted(failed_ids) if pid not in done_ids]
    log.info(f"⏩ Skip {len(done_ids)}, còn lại: {len(todo)}")

    if test_mode:
        todo = todo[:test_limit]
        log.info(f"🧪 TEST MODE: {len(todo)} products")

    if not todo:
        log.info("✅ Xong hết rồi!")
        return

    is_new_out    = not os.path.exists(OUTPUT_CSV)
    is_new_raw    = not os.path.exists(RAW_CSV)
    is_new_failed = not os.path.exists(FAILED_CSV)

    out_f    = open(OUTPUT_CSV, "a" if not is_new_out    else "w", newline="", encoding="utf-8")
    raw_f    = open(RAW_CSV,    "a" if not is_new_raw    else "w", newline="", encoding="utf-8")
    failed_f = open(FAILED_CSV, "a" if not is_new_failed else "w", newline="", encoding="utf-8")

    writer        = csv.DictWriter(out_f,    fieldnames=OUTPUT_FIELDS)
    raw_writer    = csv.DictWriter(raw_f,    fieldnames=RAW_FIELDS)
    failed_writer = csv.DictWriter(failed_f, fieldnames=FAILED_FIELDS)

    if is_new_out:    writer.writeheader()
    if is_new_raw:    raw_writer.writeheader()
    if is_new_failed: failed_writer.writeheader()

    counter = {"success": 0, "failed": 0}
    total   = len(todo)

    log.info(f"🚀 Bắt đầu crawl {total} products với {NUM_THREADS} threads")

    try:
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = {
                executor.submit(
                    process_product,
                    pid, idx, total,
                    writer, raw_writer, failed_writer,
                    out_f, raw_f, failed_f,
                    counter
                ): pid
                for idx, pid in enumerate(todo, 1)
            }
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    future.result()
                except Exception as e:
                    log.error(f"  💥 [{pid}] Unexpected error: {e}")
                    with _write_lock:
                        failed_writer.writerow({"product_id": pid, "crawled_url": "", "fail_reason": str(e)})
                        failed_f.flush()
                        counter["failed"] += 1
    finally:
        out_f.close()
        raw_f.close()
        failed_f.close()

    log.info(f"\n{'='*50}")
    log.info(f"✅ Success : {counter['success']}")
    log.info(f"❌ Failed  : {counter['failed']}")


if __name__ == "__main__":
    main(test_mode=False)
