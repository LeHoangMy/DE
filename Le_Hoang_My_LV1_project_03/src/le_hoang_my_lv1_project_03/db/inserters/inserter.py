"""
Data inserter - Insert dữ liệu vào database
"""
from typing import List, Dict, Any


class DataInserter:
    """Insert dữ liệu vào database với batch support"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def insert_data(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        return_ids: bool = True,
        batch_size: int = 1000
    ) -> List[int]:
        """
        Insert dữ liệu vào bảng và trả về IDs
        
        Args:
            table_name: Tên bảng
            data_list: List of dicts chứa data
            return_ids: True nếu cần trả về IDs
            batch_size: Số records mỗi batch
            
        Returns:
            List of inserted IDs (hoặc empty list nếu return_ids=False)
        """
        if not data_list:
            print(f"⚠ Không có dữ liệu để insert vào '{table_name}'")
            return []
        
        # Lấy columns từ dict đầu tiên
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Tạo query
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            {f'RETURNING {table_name}_id' if return_ids else ''}
        """
        
        inserted_ids = []
        cursor = self.db.conn.cursor()
        
        try:
            # Insert từng batch
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                
                for data in batch:
                    values = tuple(data[col] for col in columns)
                    cursor.execute(insert_query, values)
                    
                    if return_ids:
                        result = cursor.fetchone()
                        if result:
                            inserted_ids.append(result[0])
                
                # Commit mỗi batch
                self.db.conn.commit()
                
                if (i + batch_size) % 10000 == 0:
                    print(f"  → Đã insert {min(i + batch_size, len(data_list))}/{len(data_list)} records...")
            
            print(f"✓ Đã insert {len(data_list):,} records vào bảng '{table_name}'")
            
        except Exception as e:
            self.db.conn.rollback()
            print(f"✗ Lỗi khi insert vào '{table_name}': {e}")
            raise
        finally:
            cursor.close()
        
        return inserted_ids
    
    def bulk_insert(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        batch_size: int = 5000
    ):
        """
        Bulk insert sử dụng executemany (nhanh hơn nhưng không return IDs)
        
        Args:
            table_name: Tên bảng
            data_list: List of dicts chứa data
            batch_size: Số records mỗi batch
        """
        if not data_list:
            print(f"⚠ Không có dữ liệu để insert vào '{table_name}'")
            return
        
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
        """
        
        cursor = self.db.conn.cursor()
        
        try:
            # Insert từng batch
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                values_list = [
                    tuple(data[col] for col in columns)
                    for data in batch
                ]
                
                cursor.executemany(insert_query, values_list)
                self.db.conn.commit()
                
                print(f"  → Đã insert {min(i + batch_size, len(data_list)):,}/{len(data_list):,} records...")
            
            print(f"✓ Bulk insert hoàn tất {len(data_list):,} records vào '{table_name}'")
            
        except Exception as e:
            self.db.conn.rollback()
            print(f"✗ Lỗi bulk insert vào '{table_name}': {e}")
            raise
        finally:
            cursor.close()