"""
Seller data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class SellerGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng seller"""
    
    SELLER_TYPES = ['Official', 'Marketplace']
    
    def generate(self, num: int = 25, **kwargs) -> List[Dict[str, Any]]:
        """
        Generate sellers (Vietnam-based sellers)
        
        Args:
            num: Số lượng sellers
            
        Returns:
            List of seller dicts
        """
        sellers = []
        
        for _ in range(num):
            sellers.append({
                'seller_name': self.fake.company(),
                'join_date': self.fake.date_between(start_date='-4y', end_date='today'),
                'seller_type': random.choice(self.SELLER_TYPES),
                'rating': round(random.uniform(0.0, 5.0), 1),
                'country': 'Vietnam'
            })
        
        return sellers