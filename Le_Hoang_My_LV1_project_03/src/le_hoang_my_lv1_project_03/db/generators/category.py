"""
Category data generator
"""
from .base import BaseGenerator
from typing import List, Dict, Any
import random


class CategoryGenerator(BaseGenerator):
    """Generate dữ liệu cho bảng category với hierarchy"""

    CATEGORY_MAP = {
        "Electronics": [
            "Mobile Phones", "Laptops", "Tablets",
            "TV & Audio", "Cameras", "Accessories"
        ],
        "Fashion": [
            "Men Clothing", "Women Clothing",
            "Shoes", "Bags", "Accessories"
        ],
        "Home & Kitchen": [
            "Furniture", "Cookware",
            "Home Decor", "Appliances"
        ],
        "Books": [
            "Fiction", "Non-fiction",
            "Education", "Comics"
        ],
        "Sports": [
            "Fitness", "Outdoor",
            "Team Sports", "Sportswear"
        ],
        "Toys": [
            "Educational Toys", "Action Figures", "Puzzles"
        ],
        "Beauty": [
            "Skincare", "Makeup", "Hair Care", "Fragrance"
        ],
        "Food": [
            "Snacks", "Beverages", "Frozen Food", "Organic Food"
        ]
    }

    def generate(self, num: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Generate categories với parent-child relationship

        Args:
            num: Tổng số categories (main + sub)

        Returns:
            List of category dicts
        """
        categories: List[Dict[str, Any]] = []

        # 1️⃣ Tạo MAIN categories (level = 1)
        main_categories = list(self.CATEGORY_MAP.keys())
        num_main = min(len(main_categories), num)

        for i in range(num_main):
            categories.append({
                "category_name": main_categories[i],
                "parent_category_id": None,
                "level": 1,
                "created_at": self.fake.date_time_this_year()
            })

        # Nếu đã đủ num thì return luôn
        if len(categories) >= num:
            return categories[:num]

        # 2️⃣ Tạo SUB categories (level = 2)
        remaining = num - len(categories)

        # Flatten (main, sub) pairs
        sub_candidates = []
        for idx, main_cat in enumerate(main_categories[:num_main], start=1):
            for sub_cat in self.CATEGORY_MAP[main_cat]:
                sub_candidates.append((idx, sub_cat))

        # Lấy đúng số lượng cần
        random.shuffle(sub_candidates)
        selected_subs = sub_candidates[:remaining]

        for parent_id, sub_name in selected_subs:
            categories.append({
                "category_name": sub_name,
                "parent_category_id": parent_id,
                "level": 2,
                "created_at": self.fake.date_time_this_year()
            })

        return categories
