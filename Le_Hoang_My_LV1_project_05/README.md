# Glamira Data Engineering Project

End-to-end data engineering pipeline built on GCP, covering web crawling, data pipeline, dimensional modeling, and business intelligence dashboards.

---

## Project Overview

| Folder | Topic | Stack |
|--------|-------|-------|
| `crawling/` | Web Crawling | Python, MongoDB, GCP VM, Webshare Proxy |
| `gcs_storage/` | Data Pipeline & Storage | GCS, BigQuery, Cloud Functions, Python |
| `transformation/` | Data Transformation & Visualization | dbt, BigQuery, Looker Studio |

---

## Architecture

```
Glamira Website
      ↓ (crawl)
  MongoDB (VM)
      ↓ (export)
  GCS Bucket: raw-glamira-data-mie/
      ↓ (Cloud Function trigger)
  BigQuery: glamira_raw (raw layer)
      ↓ (dbt transform)
  BigQuery: glamira_dev (mart layer)
      ↓ (visualize)
  Looker Studio Dashboard
```

---

## Folder Structure

```
LE_HOANG_MY_LV1_PROJECT/
├── README.md
├── .gitignore
├── .env.example
├── crawling/
│   ├── crawl_worker.py
│   ├── crawl_retry_v2.py
│   └── requirements.txt
├── gcs_storage/
│   ├── export_to_gcs.py
│   ├── export_summary_v2.py
│   ├── cloud_function/
│   │   └── main.py
│   └── schemas/
│       ├── product_schema.json
│       ├── ip_locations_schema.json
│       └── summary_schema_v2.json
└── transformation/
    └── glamira_dbt/
        ├── dbt_project.yml
        ├── packages.yml
        └── models/
            ├── staging/
            │   ├── sources.yml
            │   ├── schema.yml
            │   ├── stg_summary.sql
            │   ├── stg_product.sql
            │   └── stg_ip_locations.sql
            ├── intermediate/
            │   └── int_summary_enriched.sql
            ├── core/
            │   ├── schema.yml
            │   ├── dim_customer.sql
            │   ├── dim_device.sql
            │   ├── dim_date.sql
            │   ├── dim_location.sql
            │   ├── dim_product.sql
            │   └── dim_option.sql
            └── mart/
                ├── schema.yml
                └── fact_sales_order.sql
```

---

## Crawling

### Objective
Crawl ~19,417 product URLs from Glamira website to collect product data (name, price, material, stone, diamond).

### Approach
- **v1** (`crawl_worker.py`): Parse HTML tags. 5 parallel workers with Webshare rotating proxy.
- **v2** (`crawl_retry_v2.py`): Discovered `var react_data` JSON embedded in HTML. Fallback chain: `.com` → `.co.uk` → `.com.au`

### Results
- Success: 13,322 products
- Failed: 6,095 (mostly 404)
- Total: 19,417 URLs

### Setup
```bash
pip install -r crawling/requirements.txt
cp .env.example .env
python crawling/crawl_retry_v2.py
```

---

## GCS Storage

### Objective
Build automated ETL pipeline: MongoDB → GCS → BigQuery with Cloud Function trigger.

### Pipeline Flow
```
MongoDB
  ↓ export_to_gcs.py / export_summary_v2.py
  ↓ [2 parallel workers, batch 200k rows]
GCS: raw-glamira-data-mie/
  ├── product_data/    (18,007 rows)
  ├── ip_locations/    (3.2M rows)
  └── summary_v2/      (42M rows, 205 files)
       ↓ Cloud Function trigger_bigquery_load
BigQuery: glamira-analytics.glamira_raw
  ├── product_data
  ├── ip_locations
  └── summary_v2
```

### Key Decisions
- **JSONL** over CSV/PARQUET: handles nested arrays
- **Batch size 200k**: balances RAM on 4GB VM
- **Dedup in dbt**: raw layer stores as-is
- **Cloud Function**: event-driven auto-load on GCS upload

### Setup
```bash
pip install pymongo google-cloud-storage

# Export product_data and ip_locations
python gcs_storage/export_to_gcs.py

# Export summary (42M rows) - run overnight
nohup python gcs_storage/export_summary_v2.py > ~/data/export_v2_nohup.log 2>&1 &

# Deploy Cloud Function
gcloud functions deploy trigger_bigquery_load \
  --runtime python311 \
  --trigger-resource raw-glamira-data-mie \
  --trigger-event google.storage.object.finalize \
  --source gcs_storage/cloud_function/ \
  --entry-point trigger_bigquery_load \
  --region asia-southeast1 \
  --project glamira-analytics
```

---

## Transformation

### Objective
Build dimensional model with dbt and create BI dashboards in Looker Studio.

### Data Model (Star Schema)

```
glamira_raw.summary_v2   ──► stg_summary ──►
                                              int_summary_enriched ──► dim_customer
glamira_raw.ip_locations ──► stg_ip_locations ──► dim_location    ──► dim_device
                                                                   ──► dim_date     ──► fact_sales_order
glamira_raw.product_data ──► stg_product ──► dim_product          ──► dim_option
```

### Tables

| Table | Grain | Rows (approx) |
|-------|-------|---------------|
| `fact_sales_order` | 1 product line item per order | ~27K |
| `dim_customer` | 1 logged-in user | ~17K |
| `dim_device` | 1 device_id | ~7.9M |
| `dim_date` | 1 date | ~1K |
| `dim_location` | 1 (country, city, region) combo | ~10K |
| `dim_product` | 1 product | ~18K |
| `dim_option` | 1 (option_id, value_id) combo | ~8.5K |

### Key Design Decisions
- **Surrogate keys**: `MOD(ABS(FARM_FINGERPRINT(...)), 10000000000)`
- **Guest checkout**: `customer_key = -1`
- **Price**: converted to USD using static exchange rates
- **Dedup**: `ROW_NUMBER() OVER (PARTITION BY ...)` in staging/core
- **int_summary_enriched**: joins IP → location_key before dims/fact

### Setup
```bash
pip install dbt-bigquery
cd transformation/glamira_dbt

# Configure BigQuery connection
mkdir -p ~/.dbt
nano ~/.dbt/profiles.yml

# Run models
dbt run
dbt test

# View docs
dbt docs generate
dbt docs serve --port 8090
```

### Dashboard (Looker Studio)

**Revenue Analysis:**
- Total Revenue YTD (card)
- Average Order Value — AOV (card)
- Total Orders YTD (card)
- Total Quantity YTD (card)

**Geographic Distribution:**
- Revenue by Country (geo map)
- Top 10 Countries by Revenue + Order Count (bar)
- Top 10 Cities by Revenue + Order Count (filter by country)

**Time-based Trends:**
- Revenue by Month with Year (bar)
- Orders by Day of Week (heatmap)
- YoY/MoM Growth — Revenue + Orders

**Product Performance:**
- Top 10 Products by Revenue
- Top 10 Products by Quantity
- Revenue by Gender (Men/Women/Unisex)
- Top Products by Utilization Rate (price / price_max)

---

## GCP Infrastructure

| Resource | Value |
|----------|-------|
| VM | `glamira-vm`, `asia-southeast1-b`, e2-medium |
| GCS Bucket | `raw-glamira-data-mie` |
| BigQuery Project | `glamira-analytics` |
| BigQuery Dataset (raw) | `glamira_raw` |
| BigQuery Dataset (mart) | `glamira_dev` |
| Cloud Function | `trigger_bigquery_load`, `asia-southeast1` |

---

## Environment Variables

```bash
# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=glamira

# Webshare Proxy
WEBSHARE_USER=your_proxy_username
WEBSHARE_PASS=your_proxy_password

# GCP
GCP_PROJECT_ID=glamira-analytics
GCS_BUCKET_NAME=raw-glamira-data-mie
GCP_ZONE=asia-southeast1-b
VM_NAME=glamira-vm
```

---

## .gitignore

```
.env
data/
*.csv
*.jsonl
*.parquet
*.log
__pycache__/
*.pyc
target/
dbt_packages/
profiles.yml
```
