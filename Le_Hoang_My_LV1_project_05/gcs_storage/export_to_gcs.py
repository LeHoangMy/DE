"""
Project 06 - Step 1: Export MongoDB → PARQUET → GCS
Source : MongoDB glamira.product_data
Destination: gs://raw-glamira-data-mie/product_data/
"""

import logging
import os
from datetime import datetime

import pandas as pd
import pymongo
from google.cloud import storage

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MONGO_URI   = "mongodb://localhost:27017"
MONGO_DB    = "glamira"
MONGO_COL   = "product_data"

BUCKET_NAME = "raw-glamira-data-mie"
GCS_FOLDER  = "product_data"
LOCAL_TMP   = os.path.expanduser("~/data/export_tmp")

BATCH_SIZE  = 5000

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.expanduser("~/data/export_to_gcs.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def export_to_gcs(test_mode=False, test_limit=10):
    os.makedirs(LOCAL_TMP, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Connect MongoDB
    log.info("📦 Connecting to MongoDB...")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        col = client[MONGO_DB][MONGO_COL]
        total = col.count_documents({})
        log.info(f"✅ Total documents: {total}")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        log.error(f"❌ Cannot connect to MongoDB: {e}")
        return

    if test_mode:
        total = test_limit
        log.info(f"🧪 TEST MODE: only {test_limit} documents")

    # 2. Connect GCS
    log.info("☁️  Connecting to GCS...")
    try:
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(BUCKET_NAME)
        log.info(f"✅ Bucket: {BUCKET_NAME}")
    except Exception as e:
        log.error(f"❌ Cannot connect to GCS bucket: {e}")
        client.close()
        return

    # 3. Export theo batch
    uploaded_files = []
    failed_batches = []
    offset = 0
    batch_num = 1

    while offset < total:
        limit = min(BATCH_SIZE, total - offset)
        log.info(f"📤 Batch {batch_num}: offset={offset}, limit={limit}")

        # Extract & Convert
        local_path = None
        try:
            cursor = col.find({}, {"_id": 0}).skip(offset).limit(limit)
            docs = list(cursor)
            if not docs:
                break

            df = pd.DataFrame(docs)

            # Ép kiểu numeric cho price fields
            for col_name in ["price_current", "price_original", "price_min", "price_max"]:
                if col_name in df.columns:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

            # Ép kiểu bool cho is_english
            if "is_english" in df.columns:
                df["is_english"] = df["is_english"].map(
                    lambda x: True if str(x).lower() in ("true", "1") else False
                )

            log.info(f"  Rows: {len(df)}")

            filename = f"product_data_batch{batch_num:03d}_{timestamp}.parquet"
            local_path = os.path.join(LOCAL_TMP, filename)
            df.to_parquet(local_path, index=False, engine="pyarrow")
            log.info(f"  💾 Saved local: {local_path}")

        except Exception as e:
            log.error(f"  ❌ Batch {batch_num} extract/convert failed: {e}")
            failed_batches.append(batch_num)
            offset += limit
            batch_num += 1
            continue

        # Upload to GCS
        try:
            progress = min(100, round((offset + limit) / total * 100))
            gcs_path = f"{GCS_FOLDER}/{filename}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            log.info(f"  ☁️  Uploaded: gs://{BUCKET_NAME}/{gcs_path} [{progress}%]")
            uploaded_files.append(f"gs://{BUCKET_NAME}/{gcs_path}")

        except Exception as e:
            log.error(f"  ❌ Batch {batch_num} upload failed: {e}")
            failed_batches.append(batch_num)

        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

        offset += limit
        batch_num += 1

    client.close()

    log.info("=" * 50)
    log.info(f"✅ Uploaded: {len(uploaded_files)} files")
    if failed_batches:
        log.error(f"❌ Failed batches: {failed_batches}")
    else:
        log.info("🎉 All batches uploaded successfully!")


if __name__ == "__main__":
    export_to_gcs(test_mode=False)
