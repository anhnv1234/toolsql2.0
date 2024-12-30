# toolsql2.0
# config
cấu hình DB trong file Config/config.txt
cấu hình các mst để quét trong Config/mst1.txt  (Nếu trong file không có giá trị sẽ chạy toàn DB)
# log_db
log_checkconnect : Kiểm tra mst và tenant_id có hợp lệ hay không và trả ra chuỗi kết nối 
log_result : Trả ra kết quả khi thực thiện query
#Querydb
query.txt : Chua cấu lệnh để thực hiện chính
#db_untils.py : Chưa các hàm, thư viện 
#run_db_operations.py : Hàm chạy chính
(code chạy nhiều luồng, có thể thay đổi số lồng chạy ở num_threads = 10  )
(chuyển giữa db master và slave bạn có thể thay đổi ở "THUC_THI": 'C'  (C là có chuyển về Slave , K là là chạy trên master)  )
