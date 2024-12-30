
import psycopg2
import time
from datetime import datetime
import sys


#   Lấy chuỗi kết nối từ db cloud
def query_database(db_config, variable):
    conn = None
    try:
        with open('Querydb/query2.txt', 'r') as file:
            query = file.read()

        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            cursor.execute(query, (variable,))
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Error querying database: {e}")
        return []
    finally:
        if conn:
            conn.close()  # Ngắt kết nối

# Lấy tất cả chuỗi kết nối từ dbcloud


#    Đọc danh sách MST từ file mst.txt.
def read_mst_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return [line.strip() for line in file.readlines()]
    except FileNotFoundError:
        print(f"File {file_path} không tồn tại.")
        return []


#Duyệt qua danh sách MST và kiểm tra kết nối cơ sở dữ liệu,
def get_connections_from_mst_results(mst_list, db_config):
    connections = []
    for mst in mst_list:
        results = query_database(db_config, mst)
        if results:
            connections.append({'mst': mst, 'results': results})
        else:
            write_to_log_no_timestamp(f"Error: No results found for MST {mst}",'log_db/log_connection.txt')
    return connections



def query_database1(db_config):
    with open("Querydb/query1.txt", "r") as query_file:
        query = query_file.read()  # Đọc câu truy vấn từ file

    conn = None
    try:
        conn = psycopg2.connect(**db_config)  # Kết nối đến database
        with conn.cursor() as cursor:
            cursor.execute(query)  # Thực thi câu truy vấn
            results = cursor.fetchall()  # Lấy tất cả kết quả
            return results
    except Exception as e:
        print(f"Error querying database: {e}")
        return []  # Trả về danh sách rỗng nếu có lỗi
    finally:
        if conn:
            conn.close()  # Đảm bảo ngắt kết nối

def get_connections_from_mst_results1(db_config):
    connections = []  # Khởi tạo danh sách chứa các kết nối

    # Gọi hàm query_database1 để lấy kết quả
    results = query_database1(db_config)

    if results:  # Kiểm tra nếu có kết quả trả về từ query
        connections.append({'results': results})  # Đưa kết quả vào danh sách connections
    else:
        print("No results found.")  # In ra thông báo nếu không có kết quả
        write_to_log_no_timestamp(f"Error: No results found for MST : {results}" , 'log_db/log_connection.txt')

    return connections  # Trả về danh sách kết nối


# hàm cho file chạy 2
#
#
#
#
##

#Chuyển định dạng chuỗi
# Đọc file config.txt
def load_config1(file_path):
    config = {}
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"')  # Loại bỏ dấu ngoặc kép
                config[key] = value
    return config

# Hàm chuyển đổi định dạng kết nối
def convert_connect_string(raw_string):
    parts = raw_string.split(';')
    converted = []
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            key = key.strip().lower()
            value = value.strip()
            if key == 'database':
                key = 'dbname'
            elif key == 'user id':
                key = 'user'
            converted.append(f'{key}={value}')
    return ' '.join(converted)

# Hàm thay thế giá trị host
def replace_hosts(raw_string, config):
    if config.get("THUC_THI") == 'C':
        raw_string = raw_string.replace(config["HOST_MASTER_1"], config["HOST_SLAVE_1"])
        raw_string = raw_string.replace(config["HOST_MASTER_2"], config["HOST_SLAVE_2"])
    return raw_string




#  Ghi thông điệp vào file log
def write_to_log(message, log_filename='log.txt'):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_filename, 'a', encoding='utf-8') as log_file:
        log_file.write(f'[{timestamp}] {message}\n')

 #  Ghi thông điệp vào file log Không kèm thời gian
def write_to_log_no_timestamp(message, log_filename='log.txt'):
    with open(log_filename, 'a', encoding='utf-8') as log_file:
        log_file.write(f'{message}\n')


#Tiền trình thực hiện
def show_progress_bar(iteration, total, start_time):
    percent_complete = (iteration / total) * 100
    elapsed_time = time.time() - start_time
    remaining_time = (elapsed_time / iteration) * (total - iteration) if iteration > 0 else 0
    remaining_minutes = remaining_time // 60
    remaining_seconds = remaining_time % 60
    sys.stdout.write(f'\rProgress: {percent_complete:.2f}% | Elapsed: {elapsed_time:.2f}s | Estimated Remaining: {remaining_minutes:.0f}m {remaining_seconds:.0f}s')
    sys.stdout.flush()

#Đọc lệnh SQL từ file
def read_query_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f'File "{file_path}" không tồn tại.')
        return None

#thực thi lệnh vàtrarar về kết uqar
def execute_sql_query(connect_string, tenant_id, query):
    try:
        conn = psycopg2.connect(connect_string)
        cursor = conn.cursor()
        cursor.execute(query, (tenant_id,))
        result = cursor.fetchall()
        return result
    except Exception as e:
        return str(e)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Đọc danh sách MST từ file 'mst.txt'
def read_mst_from_file1(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.readlines()  # Đọc tất cả dòng trong file và trả về dưới dạng danh sách
    except Exception as e:
        print(f"Error reading MST file: {e}")


