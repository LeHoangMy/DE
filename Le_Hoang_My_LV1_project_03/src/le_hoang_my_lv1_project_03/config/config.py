import os
from configparser import ConfigParser

def load_config(filename='database.ini', section='postgresql'):
    # Lấy đường dẫn tuyệt đối của thư mục chứa file config.py hiện tại
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Kết hợp với tên file để tạo đường dẫn chính xác tới database.ini
    file_path = os.path.join(current_dir, filename)
    
    parser = ConfigParser()
    parser.read(file_path)

    if parser.has_section(section):
        params = parser.items(section)
        return {param[0]: param[1] for param in params}
    else:
        # Báo lỗi rõ ràng hơn để bạn biết nó đang tìm ở đâu
        raise Exception(f'Section {section} not found in the file: {file_path}')
    
DB_CONFIG = load_config()
DB_CONFIG['options'] = '-c client_encoding=UTF8'

    
# Data volume configuration
DATA_COUNTS = {
    'brands': 20,
    'categories': 10,
    'sellers': 25,
    'products': 200,
    'promotions': 10,
    'promotion_products': 100,
    'orders': 100,
    'order_items': 100
}