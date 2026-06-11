= Kết luận

Báo cáo đã trình bày thiết kế và triển khai thực nghiệm hệ thống xử lý dữ liệu lớn toàn diện dựa trên nền tảng *Kiến trúc Lambda*. Các mục tiêu đặt ra của đề tài đã bước đầu được hiện thực hóa.

Kết quả đạt được của dự án bao gồm:
1. *Triển khai mô hình Lambda*: Phân tách thành công hai luồng xử lý luồng nóng thông qua Apache Kafka @kafka_doc và Spark Structured Streaming @spark_doc, song hành cùng luồng xử lý lạnh thông qua MinIO Object Storage @minio_doc và Spark SQL. Thiết kế này giúp cập nhật dữ liệu phim thời gian thực song song với việc duy trì độ tin cậy và tính nhất quán trên toàn bộ dữ liệu lịch sử.
2. *Tối ưu hóa quy trình Ingestion*: Chuyển đổi từ Spark Producer sang module Crawler Python thuần sử dụng cơ chế đa luồng giúp kiểm soát RAM tiêu thụ của Pod Ingestion xuống dưới 100MB.
3. *Đảm bảo tính nhất quán dữ liệu*: Áp dụng kỹ thuật Watermarking để xử lý dữ liệu trễ, kết hợp với các ràng buộc ghi đè upsert giúp đảm bảo tính chất idempotent trong MongoDB @mongo_doc và Elasticsearch.
4. *Xây dựng Serving Layer và BI Dashboard*: Phát triển API Gateway (Flask) tích hợp cơ chế Caching (LRU + TTL 60 giây) để giảm thời gian phản hồi truy vấn. Đồng thời, thiết lập các biểu đồ phân tích trực quan hóa (thống kê theo thể loại, tỷ lệ ngôn ngữ, YoY Financials, pivot tables theo quý) trên Metabase @metabase_doc.
5. *Đóng gói và điều phối Cloud-native*: Đóng gói toàn bộ các dịch vụ bằng Docker @docker_doc và điều phối trên hạ tầng đám mây của DigitalOcean thông qua cụm Kubernetes @k8s_doc với Strimzi Kafka Operator.

Đề tài đã cung cấp giải pháp thực tế để xử lý dữ liệu bán cấu trúc từ API trực tuyến, vận hành trên hạ tầng đám mây thực tế và tối ưu hóa tài nguyên. Đây là nền tảng để nghiên cứu các hướng nâng cấp tiếp theo như tích hợp các thuật toán gợi ý học máy cá nhân hóa (Collaborative Filtering), thiết lập cơ chế tự động co giãn tải (Auto-scaling) hoặc nâng cao tính sẵn sàng (High Availability) cho các kho lưu trữ dữ liệu.
