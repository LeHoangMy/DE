import csv
import os
from le_hoang_my_lv1_project_05.utils.db import get_db

EVENTS_WITH_PRODUCT_ID = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed",
]

def collect_product_urls(test_mode=True, limit=100):
    db = get_db()
    col = db['summary']

    product_map = {}  # {product_id: url}

    # Nhóm 1: product_id + current_url
    for event_type in EVENTS_WITH_PRODUCT_ID:
        # Dùng aggregation thay vì find() → nhanh hơn, không bị cursor timeout
        pipeline = [
            {"$match": {"collection": event_type}},
            {"$project": {"product_id": 1, "viewing_product_id": 1, "current_url": 1}},
        ]
        if test_mode:
            pipeline.append({"$limit": limit})

        cursor = col.aggregate(pipeline, allowDiskUse=True)

        for doc in cursor:
            pid = str(doc.get("product_id") or doc.get("viewing_product_id") or "").strip()
            url = str(doc.get("current_url") or "").strip()
            if pid and url and pid not in product_map:
                product_map[pid] = url

        print(f"✅ {event_type}: {len(product_map)} unique products so far")

    # Nhóm 2: viewing_product_id + referrer_url
    pipeline = [
        {"$match": {"collection": "product_view_all_recommend_clicked"}},
        {"$project": {"viewing_product_id": 1, "referrer_url": 1}},
    ]
    if test_mode:
        pipeline.append({"$limit": limit})

    cursor = col.aggregate(pipeline, allowDiskUse=True)

    for doc in cursor:
        pid = str(doc.get("viewing_product_id") or "").strip()
        url = str(doc.get("referrer_url") or "").strip()
        if pid and url and pid not in product_map:
            product_map[pid] = url

    print(f"✅ product_view_all_recommend_clicked: {len(product_map)} unique products so far")
    print(f"\n📊 Total unique products: {len(product_map)}")

    # Lưu CSV
    csv_path = os.path.join(os.path.dirname(__file__), "../../data/product_urls.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "url"])
        writer.writeheader()
        for pid, url in product_map.items():
            writer.writerow({"product_id": pid, "url": url})

    print(f"✅ Saved to {csv_path}")
    return product_map

if __name__ == "__main__":
    collect_product_urls(test_mode=False)