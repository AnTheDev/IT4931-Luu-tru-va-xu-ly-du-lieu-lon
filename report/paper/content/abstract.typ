Dữ liệu liên quan đến ngành điện ảnh từ các nền tảng trực tuyến như The Movie Database đang gia tăng nhanh chóng về khối lượng và tần suất cập nhật. Việc xử lý hiệu quả nguồn dữ liệu bán cấu trúc này đòi hỏi một kiến trúc lưu trữ và xử lý dữ liệu phù hợp, đảm bảo khả năng cập nhật thời gian thực song hành với độ chính xác lâu dài. Báo cáo này trình bày việc thiết kế và triển khai hệ thống thu thập, lưu trữ, phân tích dữ liệu phim dựa trên mô hình Kiến trúc Lambda.

Hệ thống được thiết kế phân tầng gồm các thành phần:
- *Tầng thu thập*: Sử dụng module Crawler viết bằng ngôn ngữ Python để kéo dữ liệu từ TMDB API, áp dụng phương pháp chia truy vấn theo năm nhằm kiểm soát giới hạn tần suất và giảm mức tiêu thụ RAM. Dữ liệu được đẩy song song vào Apache Kafka và lưu trữ làm Master Dataset trên MinIO Object Storage.
- *Tầng Tốc độ*: Sử dụng Spark Structured Streaming để tiêu thụ và biến đổi dòng dữ liệu từ Kafka theo thời gian thực, áp dụng cơ chế Watermarking để xử lý dữ liệu trễ và ghi kết quả vào MongoDB, Elasticsearch.
- *Tầng Lô*: Sử dụng Spark SQL chạy định kỳ để đọc Master Dataset từ MinIO, thực hiện các phép toán phân tích như Window Functions, Pivot và Self-joins.
- *Tầng Phục vụ*: Xây dựng API Gateway bằng Flask tích hợp bộ nhớ đệm LRU với TTL 60 giây để phục vụ dữ liệu hợp nhất cho Metabase Dashboard.

Hệ thống được đóng gói bằng Docker và điều phối trên cụm Kubernetes của DigitalOcean sử dụng Strimzi Kafka Operator. Kết quả thực nghiệm cho thấy hệ thống đáp ứng các yêu cầu về thời gian thực của luồng nóng và tính nhất quán của luồng lạnh, đồng thời kiểm soát hiệu quả việc sử dụng tài nguyên phần cứng.
