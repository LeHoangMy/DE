"""
Brand data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any


class BrandGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng brand"""
    
    def generate(self, num: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """
        Generate brands
        
        Args:
            num: Số lượng brands (mặc định 20)
            
        Returns:
            List of brand dicts
        """
        brands = []
        
        for _ in range(num):
            brands.append({
                
                'brand_name': self.fake.company(),
                'country': self.fake.country(),
                'created_at':self.fake.date_time_this_decade()
                # 'created_at': self.fake.date_time_between(start_date='-5y', end_date='now')
            })
        
        return brands