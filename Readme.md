DDos type:
HTTP GET/POST Flood: Kẻ tấn công gửi hàng loạt request vào một trang cụ thể (thường là trang chủ hoặc trang tìm kiếm) để làm tràn ngập hàng đợi xử lý của Web Server.

Search/Heavy Query Attack: Tập trung vào các API yêu cầu xử lý Database nặng. Ví dụ: Liên tục gọi API tìm kiếm với các từ khóa phức tạp để làm treo cơ sở dữ liệu.

Slowloris (Slow HTTP): Mở kết nối nhưng gửi dữ liệu cực kỳ chậm, mục đích là giữ chân các "worker thread" của server lâu nhất có thể cho đến khi server không còn chỗ cho người khác.

Login/Brute Force Flood: Đánh vào trang đăng nhập. Loại này vừa là DDoS, vừa là tấn công dò mật khẩu.

Application Vulnerability Exploitation: Tìm ra một tính năng bị lỗi logic (ví dụ: upload file không giới hạn) để làm đầy ổ cứng server.


user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1 Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0
