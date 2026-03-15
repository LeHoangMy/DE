"""
Standalone IP Location Processor - chạy trên VM
"""
import csv
import os
from datetime import datetime
from IP2Location import IP2Location
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"))

def get_db():
    client = MongoClient(
        os.getenv("MONGO_URI"),
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=None,
        connectTimeoutMS=30000,
    )
    return client[os.getenv("MONGO_DB")]

IP2LOC_DB_PATH = os.path.expanduser("~/data/IP-COUNTRY-REGION-CITY.BIN")

def process_ip_locations(test_mode=False, limit=1000):
    db = get_db()
    col = db['summary']

    # 1. Lấy unique IPs
    print("🔍 Fetching unique IPs...")
    pipeline = [
        {"$group": {"_id": "$ip"}},
        {"$match": {"_id": {"$ne": None, "$ne": ""}}},
    ]
    if test_mode:
        pipeline.append({"$limit": limit})
        print(f"   🧪 TEST MODE — {limit} IPs only")

    unique_ips = [doc["_id"] for doc in col.aggregate(pipeline, allowDiskUse=True)]
    print(f"   Found {len(unique_ips)} unique IPs")

    # 2. Init IP2Location
    ip_reader = IP2Location(IP2LOC_DB_PATH)
    print("✅ IP2Location DB loaded")

    # 3. Process
    results_csv  = []  # cho CSV: có processed_at + error
    results_mongo = []  # cho MongoDB: chỉ data địa lý

    print("⚙️  Processing...")
    for i, ip in enumerate(unique_ips):
        try:
            rec = ip_reader.get_all(ip)
            results_csv.append({
                "ip":           ip,
                "country_code": rec.country_short,
                "country_name": rec.country_long,
                "region":       rec.region,
                "city":         rec.city,
                "processed_at": datetime.utcnow().isoformat(),
                "error":        "",
            })
            results_mongo.append({
                "ip":           ip,
                "country_code": rec.country_short,
                "country_name": rec.country_long,
                "region":       rec.region,
                "city":         rec.city,
            })
        except Exception as e:
            results_csv.append({
                "ip":           ip,
                "country_code": "",
                "country_name": "",
                "region":       "",
                "city":         "",
                "processed_at": datetime.utcnow().isoformat(),
                "error":        str(e),
            })

        if (i + 1) % 1000 == 0:
            print(f"   {i+1}/{len(unique_ips)}...")

    success = sum(1 for r in results_csv if not r["error"])
    print(f"\n✅ Success: {success} | ❌ Failed: {len(results_csv) - success}")

    # 4. Lưu CSV
    out_path = os.path.expanduser("~/data/ip_locations.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["ip", "country_code", "country_name", "region", "city", "processed_at", "error"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results_csv)
    print(f"✅ Saved CSV: {out_path}")

    # 5. Lưu MongoDB (chỉ khi không phải test)
    if not test_mode and results_mongo:
        db['ip_locations'].drop()
        db['ip_locations'].insert_many(results_mongo)
        db['ip_locations'].create_index("ip", unique=True)
        print(f"✅ Saved {len(results_mongo)} docs to MongoDB 'ip_locations'")

if __name__ == "__main__":
    process_ip_locations(test_mode=False)
