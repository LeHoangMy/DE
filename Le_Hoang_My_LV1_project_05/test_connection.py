from le_hoang_my_lv1_project_05.utils.db import get_db

db = get_db()

# Thêm vào test_connection.py, thay phần sample
# test_connection.py
# test_connection.py
try:
    db.client.server_info()
    
    summary_count = db['summary'].count_documents({})
    print(f"📊 summary: {summary_count} documents")
    
    sample = db['summary'].find_one()
    if sample:
        print(f"📄 Sample keys: {list(sample.keys())}")
        print(f"📄 Sample data: {sample}")

except Exception as e:
    print(f"❌ Lỗi: {e}")