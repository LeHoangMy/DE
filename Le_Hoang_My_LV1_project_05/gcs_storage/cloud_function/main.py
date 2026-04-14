"""
Cloud Function: trigger_bigquery_load
Triggered when a new .jsonl file is uploaded to GCS bucket.
Detects folder → maps to correct BQ table → starts load job.
"""

from google.cloud import bigquery

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
PROJECT_ID = "glamira-analytics"
DATASET_ID = "glamira_raw"

# GCS folder → BQ table mapping
FOLDER_TABLE_MAP = {
    "product_data": "product_data",
    "ip_locations": "ip_locations",
    "summary":      "summary",
}


# ─────────────────────────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────────────────────────
def trigger_bigquery_load(event, context):
    """
    Triggered by GCS object finalize event.
    event: dict with 'bucket' and 'name' keys
    context: Cloud Functions metadata
    """
    bucket_name = event.get("bucket")
    file_name   = event.get("name")

    print(f"📁 New file detected: gs://{bucket_name}/{file_name}")

    # 1. Only process .jsonl files
    if not file_name.endswith(".jsonl"):
        print(f"⏭️  Skipping non-JSONL file: {file_name}")
        return

    # 2. Detect folder
    parts = file_name.split("/")
    if len(parts) < 2:
        print(f"⚠️  Cannot detect folder from path: {file_name}")
        return

    folder = parts[0]

    # 3. Map folder → BQ table
    table_name = FOLDER_TABLE_MAP.get(folder)
    if not table_name:
        print(f"⏭️  Folder '{folder}' not mapped to any BQ table, skipping.")
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    gcs_uri  = f"gs://{bucket_name}/{file_name}"

    print(f"🚀 Loading: {gcs_uri} → {table_id}")

    # 4. Start BQ load job
    try:
        bq_client  = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
            max_bad_records=100,
            autodetect=False,
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
        )
        print(f"⏳ Job ID: {load_job.job_id} — waiting...")
        load_job.result()

        table = bq_client.get_table(table_id)
        print(f"✅ Done! File: {file_name} | Rows in table: {table.num_rows:,}")

    except Exception as e:
        print(f"❌ Failed to load {file_name}: {e}")
        raise
