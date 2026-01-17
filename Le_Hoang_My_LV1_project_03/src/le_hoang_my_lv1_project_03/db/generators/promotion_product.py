"""
Promotion-Product mapping data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class PromotionProductGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng promotion_product"""
    
    def generate(
        self,
        num: int = 100,
        promotion_ids: List[int] = None,
        product_ids: List[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Generate promotion-product mappings (random mapping)
        
        Args:
            num: Số lượng mappings
            promotion_ids: List promotion IDs
            product_ids: List product IDs
            
        Returns:
            List of promotion_product dicts
        """
        self._validate_foreign_keys('promotion_ids', promotion_ids)
        self._validate_foreign_keys('product_ids', product_ids)
        
        promo_products = []
        
        for _ in range(num):
            promo_products.append({
                'promotion_id': random.choice(promotion_ids),
                'product_id': random.choice(product_ids),
                'created_at': self.fake.date_time_this_year()
            })
        
        return promo_products