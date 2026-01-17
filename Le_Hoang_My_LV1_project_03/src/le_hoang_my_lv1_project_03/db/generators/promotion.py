"""
Promotion data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random
from datetime import timedelta


class PromotionGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng promotion"""
    
    PROMOTION_TYPES = ['product', 'category', 'seller', 'flash_sale']
    DISCOUNT_TYPES = ['percentage', 'fixed_amount']
    
    CAMPAIGN_NAMES = [
        '9.9 Mega Sale',
        'Flash Sale 12.12',
        'Black Friday',
        'Summer Sale',
        'New Year Promotion',
        'Back to School',
        'Tet Holiday Sale',
        'Singles Day 11.11'
    ]
    
    def generate(self, num: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Generate promotions (campaign events)
        
        Args:
            num: Số lượng promotions
            
        Returns:
            List of promotion dicts
        """
        promotions = []
        
        for i in range(num):
            start_date = self.fake.date_between(start_date='-1y', end_date='+30d')
            # Campaign kéo dài 3-50 ngày
            end_date = start_date + timedelta(days=random.randint(3, 50))
            
            discount_type = random.choice(self.DISCOUNT_TYPES)
            
            # Generate discount value based on type
            if discount_type == 'percentage':
                discount_value = round(random.uniform(10.0, 50.0), 2)  # 10%-50%
            else:
                discount_value = round(random.uniform(10000, 500000), 2)  # Fixed VND
            
            promotions.append({
                'promotion_name': random.choice(self.CAMPAIGN_NAMES),
                'promotion_type': random.choice(self.PROMOTION_TYPES),
                'discount_type': discount_type,
                'discount_value': discount_value,
                'start_date': start_date,
                'end_date': end_date
            })
        
        return promotions