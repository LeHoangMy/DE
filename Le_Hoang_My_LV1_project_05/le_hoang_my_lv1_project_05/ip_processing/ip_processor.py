import csv
import os
from datetime import datetime
from IP2Location import IP2Location
from le_hoang_my_lv1_project_05.utils.db import get_db

IP2LOC_DB_PATH = os.path.join(os.path.dirname(__file__), "../../data/IP-COUNTRY-REGION-CITY.BIN")

def process_ip_locations(test_mode=True, limit=1000):
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
        print(f"   🧪 TEST MODE — using {limit} IPs only")

    unique_ips = [doc["_id"] for doc in col.aggregate(pipeline)]
    print(f"   Found {len(unique_ips)} unique IPs")

    # 2. Init IP2Location
    ip_reader = IP2Location(IP2LOC_DB_PATH)
    print("✅ IP2Location DB loaded")

    # 3. Process
    results = []
    print("⚙️  Processing...")
    for i, ip in enumerate(unique_ips):
        try:
            rec = ip_reader.get_all(ip)
            results.append({
                "ip": ip,
                "country_code": rec.country_short,
                "country_name": rec.country_long,
                "region": rec.region,
                "city": rec.city,
                "processed_at": datetime.utcnow()
            })
        except Exception as e:
            results.append({
                "ip": ip,
                "error": str(e),
                "processed_at": datetime.utcnow()
            })

        if (i + 1) % 100 == 0:
            print(f"   {i+1}/{len(unique_ips)}...")

    # 4. Preview
    print(f"\n📊 Processed {len(results)} IPs")
    print("\n🔎 Sample results:")
    for r in results[:5]:
        print(f"   {r}")

    success = sum(1 for r in results if "error" not in r)
    print(f"\n✅ Success: {success} | ❌ Failed: {len(results) - success}")

    # 5. Lưu CSV luôn dù test hay không
    csv_path = os.path.join(os.path.dirname(__file__), "../../data/ip_locations.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["ip", "country_code", "country_name", "region", "city", "processed_at", "error"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Saved CSV to {csv_path}")

    # 6. Chỉ lưu MongoDB khi không phải test
    if not test_mode:
        db['ip_locations'].drop()
        db['ip_locations'].insert_many(results)
        db['ip_locations'].create_index("ip", unique=True)
        print(f"✅ Saved to MongoDB 'ip_locations'")

if __name__ == "__main__":
    process_ip_locations(test_mode=True, limit=1000)