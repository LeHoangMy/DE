import asyncio
import aiohttp
import random
import json
import time
import csv
from pathlib import Path
from typing import Dict, Any, List, Tuple, Set

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"

# ========== CONFIG CRAWLING ==========
INPUT_CSV = "../Data/products_200k_tiki.csv"
LIMIT_IDS = 200000 # Số lượng ID tối đa muốn tải trong mỗi lần chạy
CONCURRENCY = 50  
MAX_RETRY = 7       
TIMEOUT = 10
BASE_BACKOFF = 2.0 
SAVE_FOLDER = "output_2k"
BATCH_SIZE = 1000   # Kích thước Batch (1000 ID OK mỗi file JSON)
FAIL_ID_FILE = f"{SAVE_FOLDER}/fail_ids.csv"
STATS_RESULT_FILE = f"{SAVE_FOLDER}/stats_result.txt"
# ======================================

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
}

PRODUCT_QUEUE = asyncio.Queue() 

# --- HÀM TẢI VÀ LỌC ID ---

def load_ids(offset_count: int) -> List[int]:
    """Tải LIMIT_IDS từ file CSV, bắt đầu sau offset_count dòng."""
    ids = []
    skipped_count = 0
    
    try:
        with open(INPUT_CSV, "r", encoding="utf-8") as f:
            next(f) # Bỏ qua hàng header
            for line in f:
                if skipped_count < offset_count:
                    # Bỏ qua các ID đã được xử lý (làm offset)
                    skipped_count += 1
                    continue
                
                # Bắt đầu thu thập ID
                pid_str = line.strip().split(",")[0]
                if pid_str.isdigit():
                    ids.append(int(pid_str))
                    if len(ids) >= LIMIT_IDS:
                        break
                        
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {INPUT_CSV}. Dừng chương trình.")
    return ids


def load_last_partial_batch() -> Tuple[int, List[Dict[str, Any]]]:
    """
    Tìm file JSON có index lớn nhất. Nếu nó chưa đầy BATCH_SIZE, 
    trả về index và nội dung của nó. Ngược lại, trả về index mới.
    """
    output_path = Path(SAVE_FOLDER)
    max_index = 0
    last_batch_data = []
    last_batch_path = None
    
    for file_path in output_path.glob('products_*.json'):
        try:
            index_str = file_path.stem.split('_')[-1]
            index = int(index_str)
            if index > max_index:
                max_index = index
                last_batch_path = file_path
        except ValueError:
            continue
            
    if last_batch_path and last_batch_path.exists():
        try:
            with open(last_batch_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Nếu file cuối cùng chưa đạt BATCH_SIZE (chưa đầy), ta điền vào nó
            if len(data) < BATCH_SIZE:
                last_batch_data = data
                return max_index, last_batch_data
                
            # Nếu file đã đầy, ta bắt đầu file mới (index + 1)
            return max_index + 1, []
            
        except Exception as e:
            print(f"Cảnh báo: Không thể đọc file JSON {last_batch_path}. Bắt đầu từ file mới. {e}")
            
    # Nếu không tìm thấy file nào, bắt đầu từ index 1
    return 1, []


def load_completed_ids(partial_batch_data: List[Dict[str, Any]]) -> Set[int]:
    """Tải tất cả ID đã được xử lý (OK/404/FAIL) từ các file đầu ra."""
    completed_ids = set()
    output_path = Path(SAVE_FOLDER)
    
    # 1. Tải ID từ các file JSON đã lưu (ID OK)
    for json_file in output_path.glob('products_*.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if item and item.get('id') is not None:
                        completed_ids.add(int(item['id']))
        except Exception as e:
            continue
            
    # 2. Loại bỏ ID trong batch cuối cùng (vì ta sẽ ghi đè lại nó)
    for item in partial_batch_data:
        if item and item.get('id') is not None:
            completed_ids.remove(int(item['id']))
            
    # 3. Tải ID từ file thất bại (ID FAIL/404)
    fail_file = Path(FAIL_ID_FILE)
    if fail_file.exists():
        try:
            with open(fail_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                for row in reader:
                    if row and row[0].isdigit():
                        completed_ids.add(int(row[0]))
        except Exception as e:
            print(f"Cảnh báo: Không thể đọc file FAIL ID {FAIL_ID_FILE}. {e}")
            
    return completed_ids

# --- HÀM TRÍCH XUẤT VÀ FETCH ---

def extract_product_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """Trích xuất các thông tin cần thiết từ dữ liệu JSON Tiki."""
    image_urls = []
    if data.get('images'):
        for img in data['images']:
            if img.get('base_url'):
                image_urls.append(img['base_url'])

    return {
        'id': data.get('id'),
        'name': data.get('name'),
        'url_key': data.get('url_key'),
        'price': data.get('price'),
        'description': data.get('description', ''),
        'images_url': image_urls
    }


async def fetch_product(session: aiohttp.ClientSession, pid: int) -> Tuple[Any, str]:
    """Fetch sản phẩm và xử lý retry."""
    url = API_URL.format(pid)
    attempt = 0
    error_type = "Unknown"

    while attempt < MAX_RETRY:
        attempt += 1

        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                status = resp.status

                if status == 200:
                    data = await resp.json()
                    extracted_data = extract_product_info(data)
                    return extracted_data, "OK"

                if status == 404:
                    return None, "404"

                if status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        wait_time = int(float(retry_after))
                    else:
                        wait_time = BASE_BACKOFF ** attempt * random.uniform(1.0, 2.0)

                    await asyncio.sleep(wait_time)
                    continue

                await asyncio.sleep(BASE_BACKOFF ** attempt)
                continue

        except Exception as e:
            error_type = type(e).__name__
            await asyncio.sleep(BASE_BACKOFF ** attempt * random.uniform(1.0, 2.0))

    return None, f"FAIL ({error_type})"


async def save_fail_ids(fail_ids: List[Tuple[int, str]]):
    """Lưu danh sách ID thất bại vào file CSV."""
    if not fail_ids:
        return
        
    mode = 'a'
    with open(FAIL_ID_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if f.tell() == 0:
             writer.writerow(['pid', 'status/error'])
             
        for pid, error in fail_ids:
            writer.writerow([pid, error])


async def worker(name, session, queue, stats: Dict[str, Any]) -> Dict[str, Any]:
    """Worker chuyên fetch dữ liệu và đẩy OK vào PRODUCT_QUEUE. 
    Lưu 404 và FAIL vào local_stats['fail_ids']."""
    
    # local_stats sẽ được merge vào stats cuối cùng
    local_stats = {'ok': 0, '404': 0, 'fail_ids': []}
    
    while True:
        pid = await queue.get()
        if pid is None:
            queue.task_done()
            break

        await asyncio.sleep(random.uniform(0.01, 0.05))

        # Gọi hàm fetch_product để lấy dữ liệu
        data, status = await fetch_product(session, pid)

        if status == "OK":
            local_stats['ok'] += 1
            await PRODUCT_QUEUE.put(data) 
            
        elif status == "404":
            local_stats['404'] += 1
            # 💡 FIX: Thêm ID 404 vào danh sách fail_ids để được lưu vào CSV
            # Lưu với trạng thái "404" để phân biệt rõ ràng
            local_stats['fail_ids'].append((pid, "404")) 
            
        else: # FAIL (Bao gồm cả lỗi 429 liên tục, Timeout, Connection Errors)
            # Trạng thái status lúc này là chuỗi 'FAIL (ErrorType)'
            local_stats['fail_ids'].append((pid, status))
            
            # Chỉ tăng tổng số lỗi không phục hồi (FAIL) trên toàn cục
            async with stats['lock']:
                stats['total_fail'] += 1 

        queue.task_done()
        
    return local_stats


async def batch_saver(stats: Dict[str, Any], initial_index: int, initial_buffer: List[Dict[str, Any]]):
    """
    Worker chuyên lưu batch. Đảm bảo mỗi file JSON đủ BATCH_SIZE.
    """
    
    batch_buffer = initial_buffer
    batch_index = initial_index 
    batch_time_start = time.time()
    
    if batch_buffer:
        print(f"\n[RESUME] Tiếp tục điền vào Batch {batch_index:03d}. Hiện có {len(batch_buffer)}/{BATCH_SIZE} ID.")
    
    while True:
        try:
            # Dùng timeout để kiểm tra xem workers đã xong chưa
            product_data = await asyncio.wait_for(PRODUCT_QUEUE.get(), timeout=1) 
        except asyncio.TimeoutError:
            if stats['workers_done']:
                break
            continue

        batch_buffer.append(product_data)

        # Lưu file chỉ khi buffer đạt BATCH_SIZE (1000 ID)
        if len(batch_buffer) == BATCH_SIZE:
            
            file_index = batch_index
            out_file = f"{SAVE_FOLDER}/products_{file_index:03d}.json"

            end_time = time.time()
            elapsed = end_time - batch_time_start
            
            ok_count = len(batch_buffer) 
            
            async with stats['lock']:
                stats['total_ok'] += ok_count
                
                batch_info = {
                    'batch': file_index, 
                    'time': f"{elapsed:.2f}s", 
                    'ok': ok_count,
                    '404': 0, 
                    'fail': 0 
                }
                stats['batches'].append(batch_info)

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(batch_buffer, f, ensure_ascii=False, indent=2)

            print(f"\n[SAVED - FULL] Batch {file_index} ĐÃ ĐẦY ({len(batch_buffer)} IDs) → {out_file}")

            # Reset để chuẩn bị cho batch tiếp theo
            batch_buffer.clear()
            batch_index += 1
            batch_time_start = time.time()
            
            PRODUCT_QUEUE.task_done()
        else:
            PRODUCT_QUEUE.task_done()

    # Xử lý batch cuối cùng (chưa đầy 1000)
    if batch_buffer:
        file_index = batch_index
        out_file = f"{SAVE_FOLDER}/products_{file_index:03d}.json"
        elapsed = time.time() - batch_time_start
        ok_count = len(batch_buffer) 

        async with stats['lock']:
            # Tính số ID mới được thêm vào trong lần chạy này
            ok_newly_added = ok_count - len(initial_buffer) if file_index == initial_index else ok_count
            stats['total_ok'] += ok_newly_added

            batch_info = {
                'batch': file_index, 
                'time': f"{elapsed:.2f}s", 
                'ok': ok_count, 
                'ok_new': ok_newly_added, 
                '404': 0, 
                'fail': 0
            }
            stats['batches'].append(batch_info)
        
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(batch_buffer, f, ensure_ascii=False, indent=2)

        print(
            f"\n[SAVED - PARTIAL] Batch {file_index} ({len(batch_buffer)} ID) → {out_file}\n"
            f"  Đã thêm {ok_newly_added} ID mới. Lưu file dở dang để chạy tiếp."
        )
    
    return


def format_stats_report(stats: Dict[str, Any], run_mode: str) -> str:
    report = []
    
    if run_mode == "APPEND":
        report.append("\n\n" + "="*50)
        report.append("  BÁO CÁO CHẠY TIẾP TỤC (APPEND MODE)")
        report.append("="*50)
    else:
        report.append("="*50)
        report.append("       BÁO CÁO CRAWL CUỐI CÙNG (NEW RUN)")
        report.append("="*50)

    report.append(f"Tổng số ID đã hoàn thành (Offset): {stats['completed_ids_on_start']} ID")
    report.append(f"Số ID cần xử lý trong lần chạy này: {stats['total_ids']}")
    report.append(f"Tổng thời gian chạy: {stats['total_time']:.2f} giây")
    report.append("-" * 50)
    report.append(f"✅ ID OK (Lưu Data): {stats['total_ok']}")
    
    total_404_final = sum(b['404'] for b in stats['worker_stats'])
    total_fail_final = stats['total_fail'] 
    
    report.append(f"❌ ID Lỗi (Đã lưu vào {FAIL_ID_FILE}): {total_fail_final}")
    report.append(f"❓ ID 404 (Không tìm thấy): {total_404_final}")
    report.append("-" * 50)
    report.append("CHI TIẾT THỐNG KÊ THEO BATCH (ID OK):")
    
    header = ["Batch", "OK (Total)", "OK (New)", "Time"]
    data_rows = [header]
    for batch in stats['batches']:
        new_ok_count = batch.get('ok_new', batch['ok']) 
        data_rows.append([batch['batch'], batch['ok'], new_ok_count, batch['time']])
        
    for row in data_rows:
        report.append(f"| {str(row[0]).ljust(5)} | {str(row[1]).ljust(10)} | {str(row[2]).ljust(10)} | {str(row[3]).ljust(10)} |")
    
    report.append("="*50)
    report.append("DONE.")
    
    return "\n".join(report)


# --- HÀM CHÍNH ---

async def crawl_main():
    start_time_total = time.time()
    
    Path(SAVE_FOLDER).mkdir(exist_ok=True)
    
    # 1. Tải dữ liệu batch cuối cùng và xác định index khởi đầu
    next_batch_index, initial_batch_buffer = load_last_partial_batch()
    
    # 2. Tải ID đã hoàn thành và lọc ID cần chạy
    completed_ids = load_completed_ids(initial_batch_buffer)
    completed_ids_on_start = len(completed_ids)
    
    # 3. Xác định chế độ ghi file thống kê (W=Ghi đè, A=Thêm vào)
    file_write_mode = "w"
    run_mode_name = "OVERWRITE"
    if next_batch_index > 1:
        file_write_mode = "a"
        run_mode_name = "APPEND"

    # 4. Tính toán offset và tải block ID tiếp theo
    offset = completed_ids_on_start 
    all_ids = load_ids(offset) 
    initial_total_ids = len(all_ids)
    
    if initial_total_ids == 0:
        print("\n==================================")
        print(f"Đã đạt đến cuối file CSV ({INPUT_CSV}). Không còn ID để crawl.")
        return

    ids_to_run = [pid for pid in all_ids if pid not in completed_ids]
    total_ids = len(ids_to_run)
    
    if total_ids == 0:
        print("\n==================================")
        print("TẤT CẢ ID TRONG BLOCK HIỆN TẠI ĐÃ ĐƯỢC XỬ LÝ. BẮT ĐẦU CHUYỂN BLOCK TIẾP THEO.")
        return # Nếu tất cả ID trong block 2000 đã xong, người dùng cần chạy lại để load block tiếp.

    print(f"Tổng số ID đã hoàn thành (Offset): {completed_ids_on_start} ID")
    print(f"Đã tải {initial_total_ids} ID tiếp theo từ file CSV.")
    
    if initial_batch_buffer:
        print(f"Sẽ tiếp tục điền vào file {next_batch_index:03d}.json.")
    else:
        print(f"Bắt đầu ghi file JSON từ index: {next_batch_index:03d}.")
        
    print(f"Chế độ ghi thống kê: {run_mode_name}")
    print(f"Cần xử lý lại {total_ids} ID — bắt đầu crawl với {CONCURRENCY} workers...")

    # Khởi tạo Queue và Stats
    queue = asyncio.Queue()
    for pid in ids_to_run:
        queue.put_nowait(pid)
    
    stats_lock = asyncio.Lock()
    stats = {
        'initial_total_ids': initial_total_ids,
        'completed_ids_on_start': completed_ids_on_start,
        'total_ids': total_ids,
        'total_time': 0,
        'total_ok': 0, 
        'total_404': 0, 
        'total_fail': 0,
        'batches': [],
        'worker_stats': [],
        'lock': stats_lock,
        'workers_done': False 
    }

    # Khởi động Workers và Saver
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2) 
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        
        saver_task = asyncio.create_task(batch_saver(stats, next_batch_index, initial_batch_buffer))
        
        workers = [
            asyncio.create_task(
                worker(
                    f"W{i}", session, queue, stats
                )
            )
            for i in range(CONCURRENCY)
        ]

        await queue.join()

        for _ in workers:
            queue.put_nowait(None)
        
        workers_results = await asyncio.gather(*workers)

    # Tổng hợp kết quả
    for result in workers_results:
        stats['worker_stats'].append(result)
        stats['total_404'] += result['404']
        await save_fail_ids(result['fail_ids'])
        
    stats['workers_done'] = True
    await saver_task
    stats['total_time'] = time.time() - start_time_total
    
    
    # Lưu báo cáo cuối cùng
    report_content = format_stats_report(stats, run_mode_name)
    
    try:
        with open(STATS_RESULT_FILE, file_write_mode, encoding="utf-8") as f:
            f.write(report_content)
            
        print("\n" + "="*50)
        print(f"BÁO CÁO CRAWL ĐÃ LƯU VÀO: {STATS_RESULT_FILE}")
        print(f"Chế độ ghi: {run_mode_name}")
        print(f"Tổng thời gian chạy: {stats['total_time']:.2f} giây")
        print("="*50 + "\nDONE.")
    except Exception as e:
        print(f"LỖI khi lưu file thống kê: {e}")


if __name__ == "__main__":
    asyncio.run(crawl_main())