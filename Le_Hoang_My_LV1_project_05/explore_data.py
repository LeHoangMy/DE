# explore_data.py
from le_hoang_my_lv1_project_05.utils.db import get_db

db = get_db()
col = db['summary']

# 1. Xem các loại collection - dùng aggregate với limit thay vì distinct
print("=== CÁC LOẠI COLLECTION (event types) ===")
pipeline = [
    {"$group": {"_id": "$collection", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
event_types = list(col.aggregate(pipeline))
for et in event_types:
    print(f"  - {et['_id']}: {et['count']:,} documents")

# 2. Xem fields của từng event type - chỉ lấy 1 sample thôi
print("\n=== FIELDS CỦA TỪNG EVENT TYPE ===")
for et in event_types:
    event_name = et['_id']
    sample = col.find_one({"collection": event_name})
    if sample:
        print(f"\n[{event_name}]")
        for key, val in sample.items():
            print(f"  {key}: {type(val).__name__} = {str(val)[:80]}")