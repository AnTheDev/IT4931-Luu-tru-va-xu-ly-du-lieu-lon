= Tổng quan và đặt vấn đề

== Bối cảnh và lý do chọn đề tài

Ngành công nghiệp giải trí và điện ảnh hiện đại tạo ra lượng dữ liệu lớn từ nhiều nền tảng trực tuyến như The Movie Database, IMDb hay Netflix. Dữ liệu này bao gồm thông tin tĩnh (phim, diễn viên) và các luồng thông tin động (lượt đánh giá, mức độ phổ biến, doanh thu). Việc thu thập, lưu trữ và khai thác nguồn dữ liệu bán cấu trúc này đặt ra các yêu cầu kỹ thuật tương tự như các bài toán dữ liệu lớn:

1. *Tốc độ*: Dữ liệu được cập nhật liên tục thông qua API, đòi hỏi hệ thống có khả năng xử lý thời gian thực để phản ánh các xu hướng mới, thay vì chỉ dựa vào mô hình xử lý lô có độ trễ cao.
2. *Đa dạng*: Dữ liệu từ API tồn tại ở định dạng JSON bán cấu trúc, chứa các cấu trúc lồng nhau (thể loại, công ty sản xuất), yêu cầu quy trình chuẩn hóa và trích xuất phù hợp.
3. *Giá trị*: Việc phân tích đa chiều (doanh thu theo thể loại, tỷ suất sinh lời theo năm, xu hướng theo thập kỷ) yêu cầu sự kết hợp giữa các kho lưu trữ dữ liệu thô và các cơ sở dữ liệu phục vụ truy vấn.

Do đó, đề tài *"Hệ thống thu thập, lưu trữ, phân tích và xử lý dữ liệu phim"* được thực hiện nhằm nghiên cứu và triển khai một giải pháp tích hợp cho phép tự động hóa vòng đời của dữ liệu phim từ thu thập, lưu trữ đến trực quan hóa.

== Mục tiêu của dự án

Báo cáo này tập trung vào các mục tiêu sau:
- *Thiết kế luồng dữ liệu*: Áp dụng mô hình Kiến trúc Lambda để phân tách luồng nóng và luồng lạnh.
- *Xây dựng Module Crawler*: Sử dụng `ThreadPoolExecutor` và phương pháp chia truy vấn theo năm nhằm kiểm soát giới hạn tần suất của API TMDB.
- *Xử lý dữ liệu thời gian thực và xử lý lô*:
  - Sử dụng *Spark Structured Streaming* để tiêu thụ dữ liệu từ Kafka, làm phẳng cấu trúc, tính toán thống kê và đẩy vào MongoDB/Elasticsearch.
  - Sử dụng *Spark SQL* xử lý định kỳ Master Dataset trên MinIO để thực hiện các phân tích chuyên sâu (Window Functions, Pivot tables, Self-joins).
- *Triển khai trên Kubernetes*: Đóng gói ứng dụng dưới dạng container và vận hành trên môi trường Kubernetes để đánh giá tính khả thi và khả năng vận hành của hệ thống.

== Phạm vi và Giới hạn đề tài

- *Nguồn dữ liệu*: Sử dụng API miễn phí từ The Movie Database, tập trung vào thực thể Phim (Movies) và Diễn viên (Actors).
- *Công nghệ sử dụng*: Apache Kafka (chế độ KRaft), Apache Spark (3.5.1), MinIO, MongoDB, Elasticsearch, Metabase, Docker và Kubernetes.
- *Giới hạn*:
  - *Hạ tầng vận hành*: Hệ thống được triển khai trên môi trường đám mây sử dụng cụm Kubernetes của DigitalOcean. Để tối ưu hóa hiệu năng và chi phí tài nguyên, Spark được cấu hình chạy ở chế độ Local Mode (`local[*]`) bên trong từng pod ứng dụng riêng biệt thay vì khởi tạo một cụm Spark phân tán lớn riêng.
  - Tần suất thu thập dữ liệu phụ thuộc vào chính sách giới hạn của tài khoản TMDB miễn phí.

= Khái quát công nghệ sử dụng

== Apache Kafka
Apache Kafka là hệ thống phân phối thông điệp dạng publish-subscribe phân tán, hỗ trợ truyền phát sự kiện với thông lượng cao. Trong hệ thống này, Kafka đóng vai trò là hàng đợi thông điệp làm vùng đệm giữa module thu thập dữ liệu và tầng xử lý thời gian thực. Để phù hợp với tài nguyên thử nghiệm hạn chế, Kafka được triển khai ở chế độ KRaft (Kafka Raft Metadata Mode), không sử dụng Apache Zookeeper nhằm giảm mức tiêu thụ bộ nhớ RAM.

== Apache Spark
Apache Spark là framework tính toán phân tán trong bộ nhớ. Báo cáo sử dụng hai thành phần cốt lõi của Spark:
- *Spark Structured Streaming*: Xử lý dòng dữ liệu thời gian thực dưới dạng bảng tăng trưởng vô hạn, hỗ trợ cơ chế Watermarking để xử lý dữ liệu trễ.
- *Spark SQL*: Thực hiện các truy vấn có cấu trúc trên Data Lake bằng SQL hoặc DataFrame API phục vụ cho Batch Layer.

== MinIO (Object Storage)
MinIO là kho lưu trữ đối tượng tương thích với chuẩn API Amazon S3. MinIO hoạt động hiệu quả trong môi trường container và Kubernetes. Trong hệ thống, MinIO đóng vai trò là hồ dữ liệu lưu trữ dữ liệu thô dạng JSON và Master Dataset dạng CSV (`tmdb.csv`) làm nguồn đầu vào cho Batch Layer.

== Metabase
Metabase là công cụ Business Intelligence mã nguồn mở, hỗ trợ kết nối trực tiếp với các cơ sở dữ liệu để thực hiện các truy vấn kéo thả trực quan và hiển thị kết quả trên các Dashboard.

== MongoDB
MongoDB là cơ sở dữ liệu NoSQL hướng tài liệu lưu trữ dữ liệu dưới dạng BSON. Nhờ lược đồ linh hoạt và hiệu năng đọc ghi cao, MongoDB được sử dụng làm Serving Store lưu trữ kết quả đầu ra của cả Batch Layer và Speed Layer.

== Elasticsearch
Elasticsearch là công cụ tìm kiếm và phân tích phân tán dựa trên thư viện Lucene, tối ưu cho việc tìm kiếm toàn văn. Elasticsearch hoạt động song song với MongoDB trong Serving Layer để hỗ trợ các truy vấn tìm kiếm phim theo từ khóa.

== Docker và Kubernetes
Docker đóng gói ứng dụng thành các Container độc lập, đảm bảo tính nhất quán của môi trường chạy. Kubernetes điều phối các Container này hoạt động cùng nhau.
