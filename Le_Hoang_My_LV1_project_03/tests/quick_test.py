"""
Quick test cho từng bảng riêng lẻ
"""
from le_hoang_my_lv1_project_03.db.connection import DatabaseManager
from le_hoang_my_lv1_project_03.db.schemas import TableSchema
from le_hoang_my_lv1_project_03.db.inserters import DataInserter
from le_hoang_my_lv1_project_03.db.generators import *
from le_hoang_my_lv1_project_03.config.config import DB_CONFIG


class QuickTest:
    """Quick test individual tables"""
    
    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.db.connect()
        self.inserter = DataInserter(self.db)
        
        # Initialize generators
        self.generators = {
            'brand': BrandGenerator(),
            'category': CategoryGenerator(),
            'seller': SellerGenerator(),
            'product': ProductGenerator(),
            'orders': OrderGenerator(),
            'orders_item': OrderItemGenerator(),
            'promotion': PromotionGenerator(),
            'promotion_product': PromotionProductGenerator()
        }
    
    def _create_table(self, table_name: str):
        """Tạo table nếu chưa tồn tại"""
        schemas = TableSchema.get_all_schemas()
        if table_name in schemas:
            self.db.execute_query(schemas[table_name])
            print(f"✓ Đã tạo/kiểm tra bảng '{table_name}'")
    
    def _preview(self, table_name: str, ids: list = None, limit: int = 5):
        """Preview dữ liệu vừa insert"""
        if ids and len(ids) > 0:
            id_list = ','.join(map(str, ids[:limit]))
            query = f"SELECT * FROM {table_name} WHERE {table_name}_id IN ({id_list})"
        else:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        result = self.db.execute_query(query, fetch=True)
        
        if result:
            print(f"\n📋 Preview {len(result)} records:")
            for row in result:
                print(f"   {row}")
        print()
    
    def test_brand(self, num: int = 10):
        """Test bảng brand"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: BRAND ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('brand')
        brands = self.generators['brand'].generate(num)
        ids = self.inserter.insert_data('brand', brands)
        self._preview('brand', ids)
        
        return ids
    
    def test_category(self, num: int = 10):
        """Test bảng category"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: CATEGORY ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('category')
        categories = self.generators['category'].generate(num)
        ids = self.inserter.insert_data('category', categories)
        self._preview('category', ids)
        
        return ids
    
    def test_seller(self, num: int = 10):
        """Test bảng seller"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: SELLER ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('seller')
        sellers = self.generators['seller'].generate(num)
        ids = self.inserter.insert_data('seller', sellers)
        self._preview('seller', ids)
        
        return ids
    
    def test_product(self, num: int = 10, brand_ids=None, category_ids=None, seller_ids=None):
        """Test bảng product (tự tạo dependencies nếu cần)"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: PRODUCT ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('product')
        
        # Tạo dependencies nếu chưa có
        if not brand_ids:
            print("→ Tạo brands phụ thuộc...")
            brand_ids = self.test_brand(5)
        
        if not category_ids:
            print("→ Tạo categories phụ thuộc...")
            category_ids = self.test_category(5)
        
        if not seller_ids:
            print("→ Tạo sellers phụ thuộc...")
            seller_ids = self.test_seller(5)
        
        products = self.generators['product'].generate(
            num,
            brand_ids=brand_ids,
            category_ids=category_ids,
            seller_ids=seller_ids
        )
        ids = self.inserter.insert_data('product', products)
        self._preview('product', ids)
        
        return ids
    
    # def test_customer(self, num: int = 10):
    #     """Test bảng customer"""
    #     print(f"\n{'='*60}")
    #     print(f"🧪 TESTING: CUSTOMER ({num} records)")
    #     print(f"{'='*60}\n")
        
    #     self._create_table('customer')
    #     customers = self.generators['customer'].generate(num)
    #     ids = self.inserter.insert_data('customer', customers)
    #     self._preview('customer', ids)
        
    #     return ids
    
    def test_order(self, num: int = 10, seller_ids=None):
        """Test bảng orders"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: ORDERS ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('orders')
        
        if not seller_ids:
            print("→ Tạo seller phụ thuộc...")
            seller_ids = self.test_seller(5)
        
        orders = self.generators['orders'].generate(num, seller_ids=seller_ids)
        ids = self.inserter.insert_data('orders', orders)
        self._preview('orders', ids)
        
        return ids
    
    # def test_order_item(self, num_orders: int = 5):
    #     """Test bảng order_item"""
    #     print(f"\n{'='*60}")
    #     print(f"🧪 TESTING: ORDER_ITEM")
    #     print(f"{'='*60}\n")
        
    #     self._create_table('order_item')
        
    #     print("→ Tạo orders phụ thuộc...")
    #     order_ids = self.test_order(num_orders)
        
    #     print("→ Tạo products phụ thuộc...")
    #     product_ids = self.test_product(10)
        
    #     order_items = self.generators['order_item'].generate(num_orders, seller_ids=seller_ids)
    #     order_items = self.generators['order_item'].generate(
    #         order_ids=order_ids,
    #         product_ids=product_ids
    #     )
    #     self.inserter.insert_data('order_item', order_items, return_ids=False)
    #     ids = self.inserter.insert_data('order_item', order_items)
    #     self._preview('order_item', ids)

    def test_order_item(self, num: int = 20, orders_ids=None, product_ids=None):
        """Test bảng order_item"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: ORDER_ITEMS ({num} records)")
        print(f"{'='*60}\n")
        
        # 1. Tạo bảng nếu chưa có
        self._create_table('orders_item')
        
        # 2. Kiểm tra và tạo dữ liệu phụ thuộc nếu thiếu
        if not orders_ids:
            print("→ Không có order_ids, đang tạo orders phụ thuộc...")
            orders_ids = self.test_order(10) # Tạo 10 đơn hàng mẫu
            
        if not product_ids:
            print("→ Không có product_ids, đang lấy hoặc tạo product mẫu...")
            # Giả sử bạn đã có hàm test_product, nếu chưa hãy thay bằng hàm tương ứng
            product_ids = self.test_product(10) 

        # 3. Sinh dữ liệu giả (Generator)
        # Lưu ý: Generator của bạn cần nhận order_ids và product_ids để gán ngẫu nhiên
        order_items = self.generators['orders_item'].generate(
            num, 
            orders_ids=orders_ids, 
            product_ids=product_ids
        )
        
        # 4. Insert vào Database
        ids = self.inserter.insert_data('orders_item', order_items)
        
        # 5. Xem trước kết quả
        self._preview('orders_item', ids)
        
        return ids
    
    def test_promotion(self, num: int = 5):
        """Test bảng promotion"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: PROMOTION ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('promotion')
        promotions = self.generators['promotion'].generate(num)
        ids = self.inserter.insert_data('promotion', promotions)
        self._preview('promotion', ids)
        
        return ids
    
    def test_promotion_product(self, num: int = 20):
        """Test bảng promotion_product"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: PROMOTION_PRODUCT ({num} records)")
        print(f"{'='*60}\n")
        
        self._create_table('promotion_product')
        
        print("→ Tạo promotions phụ thuộc...")
        promotion_ids = self.test_promotion(3)
        
        print("→ Tạo products phụ thuộc...")
        product_ids = self.test_product(10)
        
        promo_products = self.generators['promotion_product'].generate(
            num,
            promotion_ids=promotion_ids,
            product_ids=product_ids
        )
        self.inserter.insert_data('promotion_product', promo_products, return_ids=False)
        self._preview('promotion_product')
    
    def close(self):
        """Đóng kết nối"""
        self.db.close()


def quick_test_menu():
    """Menu tương tác cho quick test"""
    tester = QuickTest()
    
    try:
        print("\n" + "="*80)
        print("🚀 QUICK TEST MENU - Chọn bảng muốn test")
        print("="*80)
        print("\n1. Brand")
        print("2. Category")
        print("3. Seller")
        print("4. Product (+ dependencies)")
        print("5. Orders (+ sellers)")
        print("6. Order Item (+ orders, products)")
        print("7. Promotion")
        print("8. Promotion Product (+ promotion, products)")
        print("0. Thoát")
        print("\n" + "-"*80)
        
        choice = input("\nChọn số (0-8): ").strip()
        
        if choice == '1':
            num = int(input("Số lượng brands (mặc định 10): ") or "10")
            tester.test_brand(num)
        
        elif choice == '2':
            num = int(input("Số lượng categories (mặc định 10): ") or "10")
            tester.test_category(num)
        
        elif choice == '3':
            num = int(input("Số lượng sellers (mặc định 10): ") or "10")
            tester.test_seller(num)
        
        elif choice == '4':
            num = int(input("Số lượng products (mặc định 10): ") or "10")
            tester.test_product(num)
        
        elif choice == '5':
            num = int(input("Số lượng orders (mặc định 10): ") or "10")
            tester.test_order(num)
        
        elif choice == '6':
            num = int(input("Số lượng orders (mặc định 5): ") or "5")
            tester.test_order_item(num)
        
        elif choice == '7':
            num = int(input("Số lượng promotions (mặc định 5): ") or "5")
            tester.test_promotion(num)
        
        elif choice == '8':
            num = int(input("Số lượng mappings (mặc định 20): ") or "20")
            tester.test_promotion_product(num)
        
        elif choice == '0':
            print("\n👋 Thoát!")
            return
        
        else:
            print("\n❌ Lựa chọn không hợp lệ!")
        
        print("\n✅ Test hoàn tất!\n")
        
    finally:
        tester.close()


if __name__ == "__main__":
    quick_test_menu()