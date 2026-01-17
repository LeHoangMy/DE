# import psycopg2
# from le_hoang_my_lv1_project_03.config.config import load_config

# def connect(config):
#     """ Connect to the PostgreSQL database server """
#     try:
#         # connecting to the PostgreSQL server
#         with psycopg2.connect(**config) as conn:
#             print('Connected to the PostgreSQL server.')
#             return conn
#     except (psycopg2.DatabaseError, Exception) as error:
#         print(error)


# if __name__ == '__main__':
#     config = load_config()
#     connect(config)

import psycopg2
from typing import Optional

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.conn = None
    
    def connect(self):
        try:
            # self.config['options'] = '-c client_encoding=UTF8'

            self.conn = psycopg2.connect(**self.config)
            # self.conn.set_client_encoding('UTF8')
            print('Connected to the PostgreSQL server.')
            return self.conn
        except Exception as e:
            print(f"Lỗi kết nối: {e}")
            return None
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def execute_query(self, query, params=None, fetch=False):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            self.conn.commit()
            return cursor
        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi query: {e}")
            return None
        finally:
            cursor.close()
    def table_exists(self, table_name: str) -> bool:
        """Kiểm tra table có tồn tại không"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """
        result = self.execute_query(query, (table_name,), fetch=True)
        return result[0][0] if result else False
    
    def get_table_count(self, table_name: str) -> int:
        """Lấy số lượng records trong table"""
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query, fetch=True)
        return result[0][0] if result else 0