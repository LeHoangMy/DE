"""
Export MongoDB collection: summary → JSONL → GCS
- 2 parallel processes (optimal for 2-CPU VM)
- Batch size: 200,000 (~205 files total)
- Resume capability per worker
- Fixed: option object → flatten to option_* fields
- Fixed: REPEATED fields always exported as array

FULL MODE: GCS_FOLDER = "summary_v2", ranges = [(0, mid), (mid, total)]
"""

import json
import logging
import os
import multiprocessing
from datetime import datetime

import pymongo
from google.cloud import storage

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MONGO_URI   = "mongodb://localhost:27017"
MONGO_DB    = "glamira"
MONGO_COL   = "summary"

BUCKET_NAME = "raw-glamira-data-mie"
GCS_FOLDER  = "summary_v2"          # ✅ FULL MODE
LOCAL_TMP   = os.path.expanduser("~/data/export_tmp")

BATCH_SIZE  = 200000                 # ✅ ~205 files tổng
NUM_WORKERS = 2

# Dynamic option keys → flatten thành option_*
OPTION_DYNAMIC_KEYS = {
    "alloy":         "option_alloy",
    "diamond":       "option_diamond",
    "shapediamond":  "option_shapediamond",
    "Kollektion":    "option_kollektion",
    "category id":   "option_category_id",
    "finish":        "option_finish",
    "kollektion_id": "option_kollektion_id",
    "pearlcolor":    "option_pearlcolor",
    "price":         "option_price",
    "stone":         "option_stone",
}

# ─────────────────────────────────────────────────────────────
# LOGGING (per worker)
# ─────────────────────────────────────────────────────────────
def get_logger(worker_id):
    os.makedirs(os.path.expanduser("~/data"), exist_ok=True)
    logger = logging.getLogger(f"worker_{worker_id}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(
            os.path.expanduser(f"~/data/export_summary_v2_worker{worker_id}.log"),
            encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter("%(asctime)s [W%(name)s] [%(levelname)s] %(message)s"))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [W%(name)s] [%(levelname)s] %(message)s"))
        logger.addHandler(sh)
    return logger


# ─────────────────────────────────────────────────────────────
# PROGRESS HELPERS
# ─────────────────────────────────────────────────────────────
def get_progress_file(worker_id):
    return os.path.expanduser(f"~/data/summary_v2_progress_w{worker_id}.json")


def load_progress(worker_id):
    path = get_progress_file(worker_id)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"last_offset": None, "uploaded_count": 0}


def save_progress(worker_id, last_offset, uploaded_count):
    path = get_progress_file(worker_id)
    with open(path, "w") as f:
        json.dump({"last_offset": last_offset, "uploaded_count": uploaded_count}, f)


def clear_progress(worker_id):
    path = get_progress_file(worker_id)
    if os.path.exists(path):
        os.remove(path)


# ─────────────────────────────────────────────────────────────
# SERIALIZE
# ─────────────────────────────────────────────────────────────
REPEATED_FIELDS = {"option", "cart_products"}


def ensure_array(val):
    """Đảm bảo value là list. '', None, {} → []"""
    if isinstance(val, list):
        return val
    if not val:
        return []
    if isinstance(val, dict):
        return [val]
    return [val]


def serialize_doc(doc):
    result = {}
    for k, v in doc.items():
        if k == "_id":
            continue
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif isinstance(v, list):
            result[k] = [serialize_doc(i) if isinstance(i, dict) else i for i in v]
        elif isinstance(v, dict):
            # Xử lý option object dạng dynamic keys → flatten thành option_*
            if k == "option":
                for dyn_key, col_name in OPTION_DYNAMIC_KEYS.items():
                    if dyn_key in v:
                        result[col_name] = str(v[dyn_key]) if v[dyn_key] is not None else None
                # Nếu option là dict (k/v dynamic) thì không export dưới dạng array
                result[k] = []
            else:
                result[k] = serialize_doc(v)
        else:
            result[k] = v

    # Đảm bảo REPEATED fields luôn là array
    for field in REPEATED_FIELDS:
        if field in result:
            result[field] = ensure_array(result[field])
        else:
            result[field] = []

    return result


# ─────────────────────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────────────────────
def worker_export(worker_id, range_start, range_end, timestamp):
    log = get_logger(worker_id)
    os.makedirs(LOCAL_TMP, exist_ok=True)

    progress = load_progress(worker_id)
    start_offset   = progress["last_offset"] if progress["last_offset"] is not None else range_start
    uploaded_count = progress["uploaded_count"]

    if start_offset >= range_end:
        log.info(f"✅ Already completed range [{range_start:,} → {range_end:,}]")
        return

    log.info(f"🚀 Worker {worker_id} starting: [{range_start:,} → {range_end:,}]")
    if start_offset > range_start:
        log.info(f"♻️  Resuming from offset {start_offset:,} (uploaded: {uploaded_count})")

    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    col    = client[MONGO_DB][MONGO_COL]
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)

    failed_batches = []
    offset         = start_offset
    batch_num      = uploaded_count + 1

    while offset < range_end:
        limit      = min(BATCH_SIZE, range_end - offset)
        local_path = os.path.join(LOCAL_TMP, f"tmp_w{worker_id}_b{batch_num:04d}.jsonl")
        gcs_path   = f"{GCS_FOLDER}/summary_w{worker_id}_batch{batch_num:04d}_{timestamp}.jsonl"

        try:
            log.info(f"📥 Batch {batch_num}: offset={offset:,}, limit={limit:,}")
            cursor = col.find({}, {"_id": 0}).skip(offset).limit(limit)

            with open(local_path, "w", encoding="utf-8") as f:
                count = 0
                for doc in cursor:
                    f.write(json.dumps(serialize_doc(doc), ensure_ascii=False) + "\n")
                    count += 1

            log.info(f"✅ Written {count:,} rows → {local_path}")

            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            log.info(f"☁️  Uploaded → gs://{BUCKET_NAME}/{gcs_path}")

            os.remove(local_path)

            offset         += limit
            uploaded_count += 1
            batch_num      += 1
            save_progress(worker_id, offset, uploaded_count)

        except Exception as e:
            log.error(f"❌ Batch {batch_num} failed: {e}")
            failed_batches.append(batch_num)
            if os.path.exists(local_path):
                os.remove(local_path)
            offset    += limit
            batch_num += 1

    client.close()
    log.info(f"🏁 Worker {worker_id} done. Uploaded: {uploaded_count} files")
    if failed_batches:
        log.error(f"❌ Failed batches: {failed_batches}")
    else:
        clear_progress(worker_id)
        log.info("🎉 Worker completed successfully!")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.expanduser("~/data"), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [MAIN] [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.expanduser("~/data/export_summary_v2_main.log"), encoding="utf-8"),
            logging.StreamHandler(),
        ]
    )
    log = logging.getLogger("main")

    log.info("📦 Connecting to MongoDB to get total count...")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    total  = client[MONGO_DB][MONGO_COL].count_documents({})
    client.close()
    log.info(f"✅ Total documents: {total:,}")

    # ✅ FULL MODE
    mid    = total // NUM_WORKERS
    ranges = [(0, mid), (mid, total)]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    log.info(f"🚀 Launching {NUM_WORKERS} workers:")
    for i, (s, e) in enumerate(ranges):
        log.info(f"  Worker {i}: [{s:,} → {e:,}] ({e-s:,} docs)")
    log.info(f"📁 GCS folder: {GCS_FOLDER}")
    log.info(f"📦 Batch size: {BATCH_SIZE:,}")

    processes = []
    for worker_id, (range_start, range_end) in enumerate(ranges):
        p = multiprocessing.Process(
            target=worker_export,
            args=(worker_id, range_start, range_end, timestamp)
        )
        p.start()
        processes.append(p)
        log.info(f"✅ Worker {worker_id} started (PID: {p.pid})")

    for p in processes:
        p.join()

    log.info("=" * 50)
    log.info("🎉 All workers finished!")
    log.info(f"📁 GCS path: gs://{BUCKET_NAME}/{GCS_FOLDER}/")


if __name__ == "__main__":
    main()
