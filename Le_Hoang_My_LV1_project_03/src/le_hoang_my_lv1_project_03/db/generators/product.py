"""
Product data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class ProductGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng product"""
    
    PRODUCT_PREFIXES = [
        'iPhone', 'Samsung', 'Laptop', 'Áo', 'Quần', 'Giày',
        'Tủ lạnh', 'Máy giặt', 'Điều hòa', 'TV', 'Tai nghe',
        'Đồng hồ', 'Túi xách', 'Bàn', 'Ghế', 'Đèn'
    ]
    
    def generate(
        self, 
        num: int = 200, 
        brand_ids: List[int] = None,
        category_ids: List[int] = None,
        seller_ids: List[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Generate products
        
        Args:
            num: Số lượng products
            brand_ids: List brand IDs
            category_ids: List category IDs
            seller_ids: List seller IDs
            
        Returns:
            List of product dicts
        """
        # Validate foreign keys
        self._validate_foreign_keys('brand_ids', brand_ids)
        self._validate_foreign_keys('category_ids', category_ids)
        self._validate_foreign_keys('seller_ids', seller_ids)
        
        products = []
        
        for _ in range(num):
            # Generate price
            price = round(random.uniform(100000, 50000000), 2)
            
            # Generate discount (0.7-1.0 của price)
            discount_multiplier = round(random.uniform(0.7, 1.0), 2)
            discount_price = round(price * discount_multiplier, 2)
            
            products.append({
                'product_name': f"{random.choice(self.PRODUCT_PREFIXES)} {self.fake.catch_phrase().title()}",
                'category_id': random.choice(category_ids),
                'brand_id': random.choice(brand_ids),
                'seller_id': random.choice(seller_ids),
                'price': price,
                'discount_price': discount_price,
                'stock_qty': random.randint(0, 500),
                'rating': round(random.uniform(3.0, 5.0), 1),
                'created_at': self.fake.date_time_between(start_date='-3y', end_date='now'),
                'is_active': random.choice([True, True, True, False])  # 75% active
            })
        
        return products