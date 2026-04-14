# Project 05 — Glamira Web Crawling & Data Collection

> Crawl product data from [glamira.com](https://www.glamira.com), store in MongoDB, and export to CSV for downstream pipeline.

---

## 📁 Project Structure

```
LE_HOANG_MY_LV1_PROJECT_05/
├── data/                          # Local data files (gitignored)
│   ├── product_urls.csv           # All product URLs collected
│   ├── product_data.csv           # Crawled product data (merged)
│   ├── product_data_final.csv     # Final deduplicated success (18,007 records)
│   └── product_data_failed_final.csv  # Final deduplicated failed (1,410 records)
├── docs/
│   └── data_dictionary.md         # Schema & field descriptions
├── le_hoang_my_lv1_project_05/
│   ├── ip_processing/
│   │   ├── ip_processor.py        # IP geolocation processing (local)
│   │   └── ip_processor_vm.py     # IP geolocation processing (VM)
│   ├── product_crawling/
│   │   ├── collect_urls.py        # Step 5: Collect product URLs from sitemap/category
│   │   ├── crawl_products.py      # Step 6 v1: Crawl product details (simple requests)
│   │   ├── crawl_worker_vm.py     # Step 6 v2: Crawl with rotating proxy, 5 workers
│   │   ├── crawl_retry_v2_3threads.py  # Step 6 v3: Retry failed IDs, 3 threads
│   │   └── crawl_retry_v2_singlethread.py  # Step 6 v3 alt: Single thread version
│   └── utils/
│       └── db.py                  # MongoDB connection helper
├── notebooks/                     # EDA & exploration notebooks
├── .env.example                   # Environment variables template
├── .gitignore
├── pyproject.toml
├── poetry.lock
└── README.md
```

---

## 🔧 Environment Setup

### Prerequisites
- Python 3.10+
- [Poetry](https://python-poetry.org/)
- GCP account with a running VM (e2-medium recommended)
- MongoDB 6.x on VM

### 1. Clone & Install Dependencies

```bash
git clone <repo-url>
cd LE_HOANG_MY_LV1_PROJECT_05

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your values
```

`.env.example`:
```env
MONGO_URI=mongodb://localhost:27017
MONGO_DB=glamira
WEBSHARE_USER=your_proxy_user
WEBSHARE_PASS=your_proxy_pass
```

---

## ☁️ GCP Setup (Step 1–3)

### Step 1 — Create GCP Project

```bash
# Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install

# Login
gcloud auth login

# Create project
gcloud projects create glamira-project-05 --name="Glamira Project 05"

# Set active project
gcloud config set project glamira-project-05

# Enable billing (required for VM & GCS)
# → Go to: https://console.cloud.google.com/billing
```

### Step 2 — Create GCS Bucket

```bash
# Create bucket (raw layer)
gcloud storage buckets create gs://glamira-raw-data \
    --location=asia-southeast1 \
    --storage-class=STANDARD

# Verify bucket created
gcloud storage buckets list

# Test upload
gcloud storage cp data/product_urls.csv gs://glamira-raw-data/raw/
```

### Step 3 — Create & Configure VM

```bash
# Create VM instance
gcloud compute instances create glamira-vm \
    --zone=asia-southeast1-b \
    --machine-type=e2-medium \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB

# SSH into VM
gcloud compute ssh glamira-vm --zone=asia-southeast1-b

# --- Inside VM ---

# Update packages
sudo apt update && sudo apt upgrade -y

# Install Python
sudo apt install python3 python3-pip python3-venv -y

# Install MongoDB
curl -fsSL https://www.mongodb.org/static/pgp/server-6.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-6.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] \
    https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list

sudo apt update
sudo apt install -y mongodb-org

# Start MongoDB
sudo systemctl start mongod
sudo systemctl enable mongod

# Verify MongoDB running
sudo systemctl status mongod
mongosh --eval "db.runCommand({ connectionStatus: 1 })"
```

---

## 🕷️ Crawling Pipeline (Step 4–6)

### Step 4–5 — Collect Product URLs

```bash
# Run URL collection (scrapes sitemap + category pages)
python le_hoang_my_lv1_project_05/product_crawling/collect_urls.py
# Output: data/product_urls.csv (~19,417 product IDs)
```

### Step 6 — Crawl Product Data

**Method 1 — Simple requests (local, no proxy):**
```bash
python le_hoang_my_lv1_project_05/product_crawling/crawl_products.py
```

**Method 2 — Retry failed IDs, 3 threads (on VM):**
```bash
# Upload script to VM
gcloud compute scp le_hoang_my_lv1_project_05/product_crawling/crawl_retry_v2_3threads.py \
    glamira-vm:~/crawl_retry_v2.py --zone=asia-southeast1-b

# Run with nohup
nohup python3 crawl_retry_v2.py > ~/data/crawl_v2_nohup.log 2>&1 &
tail -f ~/data/crawl_v2.log
```

### Download Results from VM

```bash
# Download final files to local
gcloud compute scp glamira-vm:/home/lhmy11297/data/product_data_final.csv \
    "data/product_data_final.csv" --zone=asia-southeast1-b

gcloud compute scp glamira-vm:/home/lhmy11297/data/product_data_failed_final.csv \
    "data/product_data_failed_final.csv" --zone=asia-southeast1-b
```

---

## 📊 Final Results

| Metric | Value |
|---|---|
| Total product IDs | 19,417 |
| ✅ Success (distinct) | 18,007 |
| ❌ Failed (distinct) | 1,410 |
| Failed reason | Mostly true 404 (product removed) |

---

## 🗂️ Data Schema

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for full schema.

Key fields in `product_data_final.csv`:

| Field | Type | Description |
|---|---|---|
| product_id | string | Unique product identifier |
| product_name | string | Product name (original language) |
| product_name_en | string | Product name (English) |
| price_current | float | Current price |
| price_original | float | Original price (if on sale) |
| price_min | float | Minimum configurable price |
| price_max | float | Maximum configurable price |
| currency | string | Currency code (USD, GBP, AUD) |
| gender | string | Target gender |
| alloy | string | Metal alloy SKU |
| stone | string | Primary stone SKU |
| diamond | string | Diamond SKU |
| source_url | string | Original URL |
| crawled_url | string | Actual URL crawled (after fallback) |
| is_english | bool | Whether product name is in English |

---

## ⚠️ Anti-bot Notes

Glamira uses session-based bot detection:
- ✅ Plain `requests.get()` → 200 OK
- ❌ `requests.Session()` → 403 Forbidden
- ❌ Too many rapid requests → IP rate limit

**Solution:** Use rotating proxies (Webshare) + random delay 3–6s per request.