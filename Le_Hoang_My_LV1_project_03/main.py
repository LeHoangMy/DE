"""
Main entry point - E-commerce Data Generator
Run: poetry run python main.py
"""
import sys
from pathlib import Path

# Add src to path nếu cần
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from le_hoang_my_lv1_project_03.db.pipeline import EcommerceDataPipeline
from le_hoang_my_lv1_project_03.config.config import DB_CONFIG, DATA_COUNTS


def full_mode():
    """FULL MODE - Generate và insert tất cả dữ liệu"""
    pipeline = EcommerceDataPipeline(DB_CONFIG)
    
    try:
        pipeline.create_all_schemas()
        pipeline.generate_and_insert_all(DATA_COUNTS)
        pipeline.view_statistics()
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        raise
    finally:
        pipeline.close()


def main():
    """Main function"""
    print("\n" + "="*80)
    print("E-COMMERCE DATA GENERATOR")
    print("="*80)
    print("\n🎯 Chọn chế độ:")
    print("\n1. FULL MODE - Generate tất cả dữ liệu")
    print("2. QUICK TEST - Test từng bảng riêng lẻ (run: poetry run python tests/quick_test.py)")
    print("0. Thoát")
    print("\n" + "-"*80)
    
    mode = input("\nChọn mode (0-2): ").strip()
    
    if mode == '1':
        print("\n🚀 Bắt đầu FULL MODE...")
        print(f"Số lượng dữ liệu: {DATA_COUNTS}")
        confirm = input("\nXác nhận tiếp tục? (y/n): ").strip().lower()
        
        if confirm == 'y':
            full_mode()
        else:
            print("\n❌ Đã hủy!")
    
    elif mode == '2':
        print("\n💡 Để chạy quick test, sử dụng:")
        print("   poetry run python tests/quick_test.py")
    
    elif mode == '0':
        print("\n👋 Tạm biệt!")
    
    else:
        print("\n❌ Lựa chọn không hợp lệ!")


if __name__ == "__main__":
    main()