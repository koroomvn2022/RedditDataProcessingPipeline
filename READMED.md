# Reddit Data Pipeline với AWS Glue

## Giới Thiệu

Dự án này tập trung vào việc thu thập dữ liệu từ Reddit thông qua API, lưu dữ liệu thành file CSV, tải lên S3 và sử dụng AWS Glue để thực hiện công việc ETL. Mục tiêu chính là luyện tập quá trình thu thập và xử lý dữ liệu từ Reddit.

## Quy Trình

1. **Đăng ký API Reddit**: Đăng ký API trên Reddit để có được `client_id`, `client_secret`, và `user_agent`.
2. **Thu thập dữ liệu từ Reddit**: Sử dụng thư viện `praw` để lấy top các bài post của Reddit trong 1 ngày.
3. **Xử lý dữ liệu**: Thay đổi dữ liệu và lấy các cột cần thiết, sau đó lưu lại dữ liệu thành file CSV.
4. **Tải dữ liệu lên S3**: Tạo ra bucket và tải file CSV lên S3.
5. **Xử lý dữ liệu với AWS Glue**: Sử dụng AWS Glue để đọc dữ liệu trong bucket, thực hiện biến đổi và lưu vào trong Data Catalog trong AWS Glue.

## Công Nghệ

- **Reddit API**: Được sử dụng để thu thập dữ liệu từ Reddit.
- **praw**: Thư viện Python được sử dụng để tương tác với Reddit API.
- **CSV**: Định dạng dữ liệu được lưu trữ sau khi thu thập.
- **Amazon S3**: Dịch vụ lưu trữ đám mây được sử dụng để lưu trữ dữ liệu.
- **AWS Glue**: Dịch vụ ETL được sử dụng để xử lý dữ liệu.


