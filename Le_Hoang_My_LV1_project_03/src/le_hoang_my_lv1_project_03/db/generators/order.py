"""
Order data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class OrderGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng orders"""
    
    ORDER_STATUSES = ['PLACED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED','RETURNED']
    
    def generate(
        self, 
        num: int = 10000,
        seller_ids: List[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Generate orders (daily distributed)
        
        Args:
            num: Số lượng orders
            seller_ids: List seller IDs
            
        Returns:
            List of order dicts
        """
        self._validate_foreign_keys('seller_ids', seller_ids)
        
        orders = []
        
        for _ in range(num):
            orders.append({
                'order_date': self.fake.date_time_between(start_date='-1y', end_date='now'),
                'seller_id': random.choice(seller_ids),
                'status': random.choice(self.ORDER_STATUSES),
                'total_amount': round(random.uniform(50000, 10000000), 2),
                'created_at': self.fake.date_time_between(start_date='-1y', end_date='now')
            })
        
        return orders