"""
Base generator class
"""
from faker import Faker
from faker_commerce import Provider
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseGenerator(ABC):
    """Base class cho tất cả data generators"""
    
    def __init__(self, locale=['en_US', 'vi_VN']):
        """
        Args:
            locale: List các locale, ưu tiên locale đầu tiên
        """
        self.fake = Faker(locale)
        # self.fake.add_provider(Provider)
        Faker.seed(42)  # Để data reproducible
    
    @abstractmethod
    def generate(self, num: int, **kwargs) -> List[Dict[str, Any]]:
        """
        Generate data cho một bảng
        
        Args:
            num: Số lượng records cần generate
            **kwargs: Các tham số bổ sung (ví dụ: foreign keys)
            
        Returns:
            List of dicts chứa data
        """
        pass
    
    def _validate_foreign_keys(self, fk_name: str, fk_list: List[int]):
        """Validate foreign keys không rỗng"""
        if not fk_list:
            raise ValueError(f"Foreign key '{fk_name}' không được rỗng!")