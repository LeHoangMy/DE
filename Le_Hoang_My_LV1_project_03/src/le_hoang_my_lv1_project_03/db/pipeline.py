"""
Main data pipeline - Orchestrate toàn bộ quy trình
"""
from le_hoang_my_lv1_project_03.db.connection import DatabaseManager
from le_hoang_my_lv1_project_03.db.schemas import TableSchema
from le_hoang_my_lv1_project_03.db.inserters import DataInserter
from le_hoang_my_lv1_project_03.db.generators import (
    BrandGenerator,
    CategoryGenerator,
    SellerGenerator,
    ProductGenerator,
    OrderGenerator,
    OrderItemGenerator,
    PromotionGenerator,
    PromotionProductGenerator
)


class EcommerceDataPipeline:
    """Pipeline chính để generate và insert toàn bộ dữ liệu"""
    
    def __init__(self, db_config: dict):
        self.db_manager = DatabaseManager(db_config)
        self.inserter = None
        
        # Initialize all generators
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
    
    def create_all_schemas(self):
        """Tạo tất cả schemas theo thứ tự dependency"""
        print("\n" + "="*80)
        print("📋 TẠO DATABASE SCHEMAS")
        print("="*80 + "\n")
        
        self.db_manager.connect()
        
        schemas = TableSchema.get_all_schemas()
        table_order = TableSchema.get_table_order()
        
        for table_name in table_order:
            if table_name in schemas:
                self.db_manager.execute_query(schemas[table_name])
                print(f"✓ Đã tạo bảng '{table_name}'")
        
        print("\n")
    
    def generate_and_insert_all(self, counts: dict):
        """
        Generate và insert tất cả dữ liệu theo đúng thứ tự dependencies
        
        Args:
            counts: Dict chứa số lượng records cho mỗi bảng
        """
        print("="*80)
        print("🚀 GENERATE VÀ INSERT DỮ LIỆU")
        print("="*80 + "\n")
        
        self.inserter = DataInserter(self.db_manager)
        
        # 1. BRANDS (không phụ thuộc)
        print("1️⃣ BRANDS")
        brands = self.generators['brand'].generate(counts.get('brands', 20))
        brand_ids = self.inserter.insert_data('brand', brands)
        
        # 2. CATEGORIES (tự tham chiếu)
        print("\n2️⃣ CATEGORIES")
        categories = self.generators['category'].generate(counts.get('categories', 10))
        category_ids = self.inserter.insert_data('category', categories)
        
        # 3. SELLERS (không phụ thuộc)
        print("\n3️⃣ SELLERS")
        sellers = self.generators['seller'].generate(counts.get('sellers', 25))
        seller_ids = self.inserter.insert_data('seller', sellers)
        
        # 4. CUSTOMERS (không phụ thuộc)
        # print("\n4️⃣ CUSTOMERS")
        # customers = self.generators['customer'].generate(counts.get('customers', 500))
        # customer_ids = self.inserter.insert_data('customer', customers)
        
        # 5. PRODUCTS (phụ thuộc brands, categories, sellers)
        print("\n5️⃣ PRODUCTS")
        products = self.generators['product'].generate(
            counts.get('products', 200),
            brand_ids=brand_ids,
            category_ids=category_ids,
            seller_ids=seller_ids
        )
        product_ids = self.inserter.insert_data('product', products)
        
        # 6. PROMOTIONS (không phụ thuộc)
        print("\n6️⃣ PROMOTIONS")
        promotions = self.generators['promotion'].generate(counts.get('promotions', 10))
        promotion_ids = self.inserter.insert_data('promotion', promotions)
        
        # 7. ORDERS (phụ thuộc customers) - SỬ DỤNG BULK INSERT
        print("\n7️⃣ ORDERS (bulk insert)")
        num_orders = counts.get('orders', 10000)
        orders = self.generators['orders'].generate(num_orders, seller_ids=seller_ids)
        
        # Không cần return IDs nếu dùng bulk insert
        self.inserter.bulk_insert('orders', orders)
        
        # Lấy order IDs từ database
        orders_ids_result = self.db_manager.execute_query(
            "SELECT orders_id FROM orders ORDER BY orders_id",
            fetch=True
        )
        orders_ids = [row[0] for row in orders_ids_result]
        print(f"  → Đã lấy {len(orders_ids):,} order IDs từ database")
        
        # 8. ORDER ITEMS (phụ thuộc orders, products) - BULK INSERT
        print("\n8️⃣ ORDER ITEMS (bulk insert)")
        order_items = self.generators['orders_item'].generate(
            orders_ids=orders_ids,
            product_ids=product_ids
        )
        self.inserter.bulk_insert('orders_item', order_items)
        
        # 9. PROMOTION PRODUCTS (phụ thuộc promotions, products)
        print("\n9️⃣ PROMOTION PRODUCTS")
        promo_products = self.generators['promotion_product'].generate(
            counts.get('promotion_products', 100),
            promotion_ids=promotion_ids,
            product_ids=product_ids
        )
        self.inserter.insert_data('promotion_product', promo_products, return_ids=False)
        
        print("\n" + "="*80)
        print("✅ HOÀN THÀNH TẤT CẢ!")
        print("="*80 + "\n")
    
    def view_statistics(self):
        """Xem thống kê dữ liệu trong database"""
        print("="*80)
        print("📊 THỐNG KÊ DỮ LIỆU")
        print("="*80 + "\n")
        
        tables = TableSchema.get_table_order()
        
        for table_name in tables:
            count = self.db_manager.get_table_count(table_name)
            print(f"{table_name.capitalize():<20}: {count:>10,} records")
        
        print("\n" + "="*80 + "\n")
    
    def close(self):
        """Đóng kết nối database"""
        self.db_manager.close()