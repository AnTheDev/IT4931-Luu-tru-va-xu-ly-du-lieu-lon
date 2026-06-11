= Bài học kinh nghiệm

Trong quá trình thiết kế và vận hành thử nghiệm hệ thống xử lý dữ liệu phim, một số kinh nghiệm kỹ thuật được ghi nhận:

== Tối ưu hóa tài nguyên trong thu thập dữ liệu
- *Bối cảnh*: Phiên bản thử nghiệm đầu tiên sử dụng Apache Spark (PySpark) chạy liên tục để làm Producer kéo dữ liệu từ API TMDB và đẩy vào Kafka/MinIO.
- *Thách thức*: Tiến trình Spark (Driver và Executor) tiêu thụ tối thiểu 1.5GB RAM trên pod cho các tác vụ HTTP request cơ bản. Điều này gây quá tải tài nguyên trên cụm Kubernetes, dẫn đến nguy cơ các pod cơ sở dữ liệu và Kafka bị dừng hoạt động do lỗi OOM.
- *Giải pháp & Kết quả*: Module thu thập được chuyển đổi sang viết bằng ngôn ngữ Python thuần sử dụng thư viện `requests` và `kafka-python`, chạy dưới dạng một container độc lập. Giải pháp này giúp giảm lượng RAM tiêu thụ của tiến trình thu thập dữ liệu từ 1.5GB xuống dưới 100MB, ổn định việc phân phối tài nguyên trên cụm.
- *Bài học rút ra*: Cần đánh giá đúng khối lượng công việc để chọn công cụ phù hợp. Hạn chế sử dụng các công cụ tính toán phân tán cho các tác vụ I/O nhẹ đầu vào.

== Xử lý dữ liệu đến muộn
- *Bối cảnh*: Dữ liệu truyền từ Ingestion Layer sang Kafka và được tiêu thụ bởi Speed Layer có thể bị xáo trộn thứ tự thời gian do trễ mạng hoặc trễ lập lịch của container.
- *Thách thức*: Các bản ghi đến muộn nằm ngoài cửa sổ thời gian hiện tại có thể bị loại bỏ hoặc gây sai số cho các phép thống kê.
- *Giải pháp*: Áp dụng cơ chế Watermarking trong Spark Structured Streaming. Dòng dữ liệu trích xuất từ Kafka được cấu hình `withWatermark("event_time", "10 minutes")` trong `enhanced_speed_layer.py`. Spark Streaming sẽ duy trì trạng thái của các cửa sổ thời gian cũ thêm 10 phút để tiếp nhận dữ liệu trễ trước khi chốt kết quả ghi xuống MongoDB.
- *Bài học rút ra*: Watermarking là cơ chế cần thiết để đảm bảo tính nhất quán và chính xác của dữ liệu trong các hệ thống xử lý dòng thời gian thực.

== Tách biệt lớp phục vụ và bộ nhớ đệm
- *Bối cảnh*: Metabase Dashboard và các yêu cầu từ phía client liên tục thực hiện truy vấn dữ liệu thống kê từ MongoDB với tần suất cao.
- *Thách thức*: Việc truy vấn trực tiếp xuống cơ sở dữ liệu MongoDB ReplicaSet cho các tác vụ thống kê phức tạp ở mỗi lượt tải trang gây tăng thời gian phản hồi của API và tăng tải CPU của database.
- *Giải pháp*: Xây dựng Flask API làm API Gateway trung gian và tích hợp bộ lọc cache LRU với thời gian sống TTL 60 giây. Các yêu cầu giống nhau từ người dùng sẽ được trả về trực tiếp từ bộ nhớ RAM của API Gateway, giảm số lượng truy vấn trực tiếp xuống MongoDB.
- *Bài học rút ra*: Việc sử dụng API Gateway kết hợp với bộ nhớ đệm giúp giảm tải cho cơ sở dữ liệu và cải thiện tốc độ phản hồi của hệ thống.

= Nhận xét, đánh giá hướng phát triển

== Nhận xét và đánh giá hệ thống

Hệ thống được đánh giá dựa trên các ưu điểm và hạn chế kỹ thuật sau:

=== Ưu điểm
- *Tuân thủ kiến trúc Lambda*: Phân tách rõ ràng hai luồng xử lý luồng nóng độ trễ thấp cập nhật xu hướng tức thì và luồng lạnh độ chính xác cao xử lý các phép toán phức tạp trên toàn bộ lịch sử.
- *Kiểm soát tài nguyên tốt*: Việc sử dụng module Crawler bằng Python thuần, cấu hình Kafka chế độ KRaft và áp dụng LRU Caching ở Serving Layer giúp hệ thống vận hành ổn định trên tài nguyên thử nghiệm hạn chế.
- *Tính nhất quán dữ liệu*: Áp dụng kỹ thuật Watermarking giúp kiểm soát dữ liệu đến muộn. Cơ chế ghi đè upsert (`idFieldList` trong MongoDB và `es.mapping.id` trong Elasticsearch) đảm bảo tính chất idempotent.

=== Hạn chế
- *Mô hình đơn bản sao của một số dịch vụ*: Mặc dù hệ thống được triển khai trên cụm Production đa node (3 nodes), các dịch vụ lưu trữ và thông điệp như MongoDB, Elasticsearch và Kafka hiện tại chỉ được cấu hình chạy với 1 replica để tối ưu chi phí tài nguyên (tổng chi phí cụm được giới hạn). Do đó, các tính năng như MongoDB Sharding hay Kafka Replication Factor > 1 chưa được cấu hình ở mức tối đa để thử nghiệm khả năng tự động khôi phục lỗi nâng cao.
- *Chưa áp dụng mô hình học máy*: Các phân tích của Batch Layer hiện tại tập trung vào thống kê mô tả (Top Trending, Top Rating, ROI, Pivot tables) sử dụng Spark SQL. Hệ thống chưa tích hợp các mô hình Học máy để đưa ra các gợi ý cá nhân hóa.
- *Giới hạn tốc độ thu thập*: Tần suất thu thập dữ liệu bị giới hạn bởi chính sách Rate Limit của tài khoản TMDB miễn phí.

== Hướng phát triển đề tài

Các hướng nghiên cứu và phát triển tiếp theo của đề tài bao gồm:

=== Tích hợp các thuật toán phân tích nâng cao
- *Áp dụng Spark MLlib*: Xây dựng module huấn luyện mô hình gợi ý phim chạy định kỳ trong Batch Layer bằng thuật toán gợi ý ALS để đưa ra danh sách phim cá nhân hóa dựa trên lịch sử đánh giá của người dùng.
- *Xử lý ngôn ngữ tự nhiên*: Tích hợp mô hình phân tích cảm xúc để phân tích nội dung các bình luận phim thu thập được, hỗ trợ đánh giá chất lượng phim.

=== Nâng cấp và mở rộng hạ tầng đám mây
- *Tự động co giãn theo tải thực tế*: Cấu hình Horizontal Pod Autoscaler (HPA) cho Serving Layer và Speed Layer để tự động tăng giảm số lượng bản sao pod dựa trên mức sử dụng CPU và lưu lượng truy cập thực tế.
- *Nâng cao tính sẵn sàng cao (High Availability)*: Tăng số lượng replica của Kafka (Replication Factor > 1), MongoDB (ReplicaSet từ 1 lên 3 nodes) và Elasticsearch cluster để chịu lỗi tốt hơn trong môi trường Production.
- *Hệ thống giám sát và cảnh báo*: Tích hợp Prometheus và Grafana để theo dõi các chỉ số vận hành của cụm DOKS như CPU, RAM, Network Lag của Kafka Consumer và tốc độ ghi của Spark Streaming.

=== Cải thiện giao diện và tính năng tìm kiếm
- *Xây dựng ứng dụng Frontend*: Phát triển giao diện người dùng hoàn chỉnh bằng ReactJS hoặc Flutter để người dùng cuối tương tác trực tiếp với hệ thống.
- *Tối ưu hóa tìm kiếm*: Nâng cấp cấu hình Elasticsearch kết hợp tính năng Autocomplete, hỗ trợ gợi ý sửa lỗi chính tả khi người dùng tìm kiếm phim.
