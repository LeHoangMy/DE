"""
Order item data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class OrderItemGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng order_item"""
    
    def generate(
        self,
        num: int = 5, # Thêm tham số num, mặc định là 5
        orders_ids: List[int] = None,
        product_ids: List[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Generate order items (each order has 2-5 items)
        
        Args:
            order_ids: List order IDs
            product_ids: List product IDs
            
        Returns:
            List of order_item dicts
        """
        self._validate_foreign_keys('orders_ids', orders_ids)
        self._validate_foreign_keys('product_ids', product_ids)
        
        order_items = []
        
        for orders_id in orders_ids:
            # Mỗi order có 2-5 items
            num_items = random.randint(2, 5)
            selected_products = random.sample(
                product_ids, 
                min(num_items, len(product_ids))
            )
            
            for product_id in selected_products:
                quantity = random.randint(1, 5)
                unit_price = round(random.uniform(50000, 5000000), 2)
                
                order_items.append({
                    'orders_id': orders_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'subtotal': round(quantity * unit_price, 2)
                })
        
        return order_items