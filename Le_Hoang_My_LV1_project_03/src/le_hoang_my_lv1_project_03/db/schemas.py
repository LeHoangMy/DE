"""
Database table schemas
"""


class TableSchema:
    """Định nghĩa schemas cho tất cả các bảng"""
    
    BRAND = """
        CREATE TABLE IF NOT EXISTS brand (
            brand_id SERIAL PRIMARY KEY,
            brand_name VARCHAR(100) NOT NULL,
            country VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    CATEGORY = """
        CREATE TABLE IF NOT EXISTS category (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(100) NOT NULL,
            parent_category_id INTEGER REFERENCES category(category_id),
            level SMALLINT DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    SELLER = """
        CREATE TABLE IF NOT EXISTS seller (
            seller_id SERIAL PRIMARY KEY,
            seller_name VARCHAR(150) NOT NULL,
            join_date DATE NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 5),
            country VARCHAR(50) NOT NULL
        )
    """
    
    PRODUCT = """
        CREATE TABLE IF NOT EXISTS product (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(200) NOT NULL,
            category_id INTEGER REFERENCES category(category_id),
            brand_id INTEGER REFERENCES brand(brand_id),
            seller_id INTEGER REFERENCES seller(seller_id),
            price DECIMAL(12,2) NOT NULL,
            discount_price DECIMAL(12,2),
            stock_qty INTEGER DEFAULT 0,
            rating FLOAT CHECK (rating >= 0 AND rating <= 5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    """
    
    
    
    ORDERS = """
        CREATE TABLE IF NOT EXISTS orders (
            orders_id SERIAL PRIMARY KEY,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            seller_id INTEGER REFERENCES seller(seller_id),
            status VARCHAR(20) ,
            total_amount DECIMAL(12,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    ORDER_ITEM = """
        CREATE TABLE IF NOT EXISTS orders_item (
            orders_item_id SERIAL PRIMARY KEY,
            orders_id INTEGER REFERENCES orders(orders_id),
            product_id INTEGER REFERENCES product(product_id),
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(12,2) NOT NULL,
            subtotal DECIMAL(12,2) NOT NULL
        )
    """
    
    PROMOTION = """
        CREATE TABLE IF NOT EXISTS promotion (
            promotion_id SERIAL PRIMARY KEY,
            promotion_name VARCHAR(100) NOT NULL,
            promotion_type VARCHAR(50) NOT NULL,
            discount_type VARCHAR(20) NOT NULL,
            discount_value NUMERIC(10,2) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL
        )
    """
    
    PROMOTION_PRODUCT = """
        CREATE TABLE IF NOT EXISTS promotion_product (
            promo_product_id SERIAL PRIMARY KEY,
            promotion_id INTEGER REFERENCES promotion(promotion_id),
            product_id INTEGER REFERENCES product(product_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    @classmethod
    def get_all_schemas(cls) -> dict:
        """
        Trả về tất cả schemas theo thứ tự dependency
        
        Returns:
            Dict với key là tên bảng, value là SQL schema
        """
        return {
            'brand': cls.BRAND,
            'category': cls.CATEGORY,
            'seller': cls.SELLER,
            'product': cls.PRODUCT,
            'orders': cls.ORDERS,
            'order_item': cls.ORDER_ITEM,
            'promotion': cls.PROMOTION,
            'promotion_product': cls.PROMOTION_PRODUCT
        }
    
    @classmethod
    def get_table_order(cls) -> list:
        """
        Trả về thứ tự tạo bảng (theo dependencies)
        
        Returns:
            List tên bảng theo thứ tự
        """
        return [
            'brand',
            'category',
            'seller',
            'product',
            'promotion',
            'orders',
            'orders_item',
            'promotion_product'
        ]