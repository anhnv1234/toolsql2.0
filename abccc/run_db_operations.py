from concurrent.futures import ThreadPoolExecutor, as_completed
from db_utils import load_config1, convert_connect_string,replace_hosts, get_connections_from_mst_results1, write_to_log_no_timestamp, get_connections_from_mst_results, read_mst_from_file, write_to_log, convert_connect_string, show_progress_bar, read_query_from_file, execute_sql_query
from Config.config import db_config
import time
from tqdm import tqdm
config_file_path = "Config/config.txt"

config = {
    "HOST_MASTER_1": "10.10.12.4",
    "HOST_MASTER_2": "10.10.12.21",
    "HOST_SLAVE_1": "10.10.12.5",
    "HOST_SLAVE_2": "10.10.12.22",
    "THUC_THI": 'C'  # Giá trị có thể là 'C' hoặc 'K'
}

# Hàm để thực hiện một kết nối và truy vấn
def process_connection(conn_info, mst, query_from_file, progress_bar):
    raw_connect_string = conn_info[1]
    tenant_id = conn_info[2]  # tenant_id là giá trị UUID

    # Chuyển đổi chuỗi kết nối
    if config.get("THUC_THI") == 'C':
        # Sửa lỗi logic thay thế
        connect_string1 = raw_connect_string.replace(config["HOST_MASTER_1"], config["HOST_SLAVE_1"])
        connect_string1 = connect_string1.replace(config["HOST_MASTER_2"], config["HOST_SLAVE_2"])
        connect_string = convert_connect_string(connect_string1)
    else:
        connect_string = convert_connect_string(raw_connect_string)

    # Câu lệnh SQL kiểm tra TenantCompaniesId
    sql_query = '''
    SELECT "Id" FROM "MInvoice"."TenantCompanies" WHERE "TenantId" = %s;
    '''

    # Thực thi câu lệnh SQL kiểm tra TenantCompaniesId
    result = execute_sql_query(connect_string, tenant_id, sql_query)

    tenant_valid = False
    if isinstance(result, list):
        for row in result:
            if row[0] == tenant_id:  # Cột thứ 2 chứa TenantCompaniesId
                tenant_valid = True
                write_to_log(f'MST {mst} is a valid connection with TenantId: {tenant_id} for {connect_string}','log_db/log_checkconnect.txt')
                break

    # Nếu kết nối hợp lệ, thực hiện câu lệnh tiếp theo từ file abc.txt
    if tenant_valid and query_from_file:
        query_result = execute_sql_query(connect_string, tenant_id, query_from_file)
        if isinstance(query_result, list):
            for row in query_result:
                write_to_log(str(row),'log_db/log_results.txt')
    else:
        # Nếu TenantId không khớp, in thông báo lỗi vào log
        write_to_log(f'Error: Invalid TenantId {tenant_id} for MST {mst}', 'log_db/log_checkconnect.txt')

    # Cập nhật tiến trình
    progress_bar.update(1)  # Cập nhật thanh tiến trình sau mỗi tác vụ hoàn thành


# Hàm chính để xử lý kết nối từ MST file
def main():
    mst_list = read_mst_from_file('Config/mst1.txt')
    # Kiểm tra nếu file 'mst.txt' không rỗng
    if mst_list:
        connections = get_connections_from_mst_results(mst_list, db_config)  # Nếu có MST, sử dụng hàm này
    else:
        connections = get_connections_from_mst_results1(db_config)  # Nếu MST trống, sử dụng hàm này

    all_results = []

    # Lấy kết quả vào mảng
    for connection in connections:
        all_results.extend(connection['results'])

    connections = all_results

    # Câu lệnh SQL kiểm tra TenantCompaniesId
    query_from_file = read_query_from_file('Querydb/query.txt')

    # Tạo thanh tiến trình sử dụng tqdm
    total_connections = len(connections)
    progress_bar = tqdm(total=total_connections, desc="Processing connections", ncols=100, ascii=True)

    # Cấu hình số luồng
    num_threads = 10  # Thay đổi số luồng bạn muốn chạy song song

    # Tạo ThreadPoolExecutor với số lượng luồng bạn muốn
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Tạo một list các task để xử lý các kết nối
        futures = []

        for i, conn_info in enumerate(connections):
            mst = conn_info[3]
            future = executor.submit(process_connection, conn_info, mst, query_from_file, progress_bar)
            futures.append(future)

        # Đợi tất cả các task hoàn thành và xử lý kết quả
        for future in as_completed(futures):
            future.result()  # Đây là nơi kết quả của mỗi task được trả về

    # Đóng thanh tiến trình khi hoàn thành tất cả các tác vụ
    progress_bar.close()

    print('\nAll tasks completed')

if __name__ == '__main__':
    main()
