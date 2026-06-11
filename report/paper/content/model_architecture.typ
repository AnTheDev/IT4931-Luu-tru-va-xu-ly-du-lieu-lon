= Kiến trúc và thiết kế hệ thống

== Kiến trúc tổng thể

Hệ thống được thiết kế và triển khai tuân thủ mô hình *Kiến trúc Lambda* nhằm tối ưu hóa việc xử lý đồng thời dữ liệu lịch sử và dữ liệu thời gian thực. Hệ thống chia thành hai luồng xử lý riêng biệt:

- *Luồng lạnh*: Dữ liệu thô sau khi thu thập được lưu trữ bền vững trên MinIO ở dạng CSV/JSON đóng vai trò là Master Dataset. Một Spark Job được lên lịch chạy định kỳ để đọc toàn bộ dữ liệu lịch sử, thực hiện các phép biến đổi phức tạp có độ chính xác cao và ghi kết quả vào MongoDB (các collection dạng `batch_*`) và Elasticsearch.
- *Luồng nóng*: Dữ liệu thời gian thực được đẩy qua Apache Kafka làm vùng đệm. Spark Structured Streaming liên tục tiêu thụ dữ liệu từ các topic, áp dụng các phép tính toán tức thời (sliding window, watermarking) và ghi kết quả vào MongoDB (các collection dạng `speed_*`) và Elasticsearch.
- *Tầng phục vụ*: API Gateway viết bằng Flask tiếp nhận yêu cầu từ client, thực hiện truy vấn và hợp nhất dữ liệu từ cả hai lớp Batch và Speed, đồng thời cung cấp tính năng tìm kiếm qua Elasticsearch.

Bảng bên dưới so sánh các đặc tính vận hành giữa hai lớp xử lý dữ liệu:

#align(center)[
  #table(
    columns: (1.2fr, 2fr, 2fr),
    align: (center, left, left),
    stroke: 0.5pt + luma(120),
    fill: (col, row) => if row == 0 { rgb("e6f2ff") } else { none },
    [*Đặc trưng*], [*Batch Layer (Luồng lạnh)*], [*Speed Layer (Luồng nóng)*],
    [Độ trễ xử lý], [Cao (định kỳ mỗi 6 hoặc 12 giờ)], [Thấp (xử lý dòng thời gian thực tính bằng giây)],
    [Nguồn dữ liệu], [Master Dataset trên MinIO (`tmdb.csv`)], [Apache Kafka topic (`movie`)],
    [Công cụ tính toán], [Spark SQL (Window Functions, Joins, Pivots)], [Spark Structured Streaming (foreachBatch, Watermarking)],
    [Nơi lưu trữ kết quả], [MongoDB (`batch_*` collections) và Elasticsearch], [MongoDB (`speed_*` collections) và Elasticsearch],
    [Mục tiêu phân tích], [Thống kê lịch sử toàn diện, chính xác cao], [Thống kê và phát hiện xu hướng tức thời]
  )
]

== Các thành phần chi tiết

=== Nguồn dữ liệu
Hệ thống sử dụng các API REST từ TMDB. Class `MovieDB` trong module Crawler được xây dựng để gọi hai endpoint chính:
- `/discover/movie`: Trích xuất danh sách các phim kèm thông tin chi tiết (ngân sách, doanh thu, đánh giá, thể loại, quốc gia sản xuất).
- `/person/popular`: Trích xuất danh sách các diễn viên đang được quan tâm nhiều nhất.

=== Hàng đợi thông điệp
Apache Kafka được triển khai ở chế độ KRaft (không cần Zookeeper) trong Kubernetes namespace `bigdata` dưới tên cụm `kafka-cluster`. Hệ thống sử dụng 2 topic chính:
- `movie`: Chứa luồng dữ liệu phim dạng JSON gửi từ Crawler.
- `actor`: Chứa luồng dữ liệu diễn viên dạng JSON.

=== Hồ dữ liệu
MinIO được cấu hình ở chế độ Standalone để lưu trữ dữ liệu thô. Lớp Ingestion thực hiện lưu dữ liệu phim dưới hai định dạng:
- Các tệp JSON thô phân tách theo năm và trang (`raw/movies/YYYY/page_X.json`) nhằm mục đích lưu trữ lưu trữ lâu dài.
- Tệp CSV hợp nhất (`tmdb.csv`) đóng vai trò là Master Dataset của Batch Layer.

=== Xử lý thời gian thực
Sử dụng Spark Structured Streaming trong file `enhanced_speed_layer.py`. Lớp này thực hiện đọc dữ liệu từ Kafka topic `movie`, phân tích cú pháp JSON theo schema định sẵn, áp dụng Watermarking trễ 10 phút để kiểm soát dữ liệu đến muộn, và thực hiện:
- Lưu dữ liệu phim thời gian thực vào MongoDB collection `speed_movies` và Elasticsearch index `speed-movie`.
- Tính toán các chỉ số thống kê thể loại thời gian thực (`speed_genre_stats`, `speed_genre_ranked`), thống kê theo năm phát hành (`speed_year_stats`), theo ngôn ngữ gốc (`speed_language_stats`) và bảng xếp hạng phim hot (`speed_top_movies`).

=== Xử lý theo lô
Chương trình `enhanced_batch_layer.py` được triển khai dưới dạng Kubernetes CronJob để chạy định kỳ. Lớp này đọc dữ liệu lịch sử từ tệp `tmdb.csv` trên MinIO, thực hiện:
- Các phép biến đổi dữ liệu nhiều tầng và lọc bỏ các bản ghi lỗi.
- *Broadcast Joins* kết hợp với các bảng tra cứu thể loại, ngôn ngữ để làm giàu thông tin dữ liệu mà không cần thực hiện Shuffle lớn giữa các node.
- *Self-Join* bằng thuật toán Sort-Merge Join để phát hiện các cặp phim tương đồng (cùng thể loại và năm phát hành, chênh lệch điểm đánh giá < 0.5).
- *Window Functions* (`rank`, `row_number`, `dense_rank`, `lag`, `lead`, `ntile`) để phân tích xếp hạng phim, xếp hạng đạo diễn và phân tích tăng trưởng YoY.
- *Pivot & Unpivot Tables* để chuyển đổi dữ liệu phục vụ trực quan hóa chéo (Ví dụ: Thập kỷ x Xếp hạng, Quý x Nhóm ngân sách).
- Lưu trữ kết quả đầu ra vào MongoDB (`batch_*`) và Elasticsearch (`batch-movie`).

=== Lớp phục vụ
Ứng dụng Flask trong `enhanced_serving_layer.py` thực hiện:
- Tiếp nhận các request HTTP từ Client.
- Thực hiện thuật toán hợp nhất dữ liệu từ luồng lạnh (`batch_*`) và luồng nóng (`speed_*`). Khi có dữ liệu trùng lặp về ID phim hoặc thống kê thể loại, dữ liệu thời gian thực từ Speed Layer sẽ ghi đè lên các trường thông tin tương ứng để đảm bảo tính cập nhật nhất.
- Bộ nhớ đệm LRU Cache với cơ chế thread-safe và size-bounded, thiết lập thời gian hết hạn TTL là 60 giây, giúp giảm tải truy vấn trực tiếp đến cơ sở dữ liệu MongoDB.
- Kết nối song song với Elasticsearch làm công cụ tìm kiếm chính (Full-text Search), tự động hạ cấp (fallback) về MongoDB regex search khi Elasticsearch gặp sự cố.

== Sơ đồ luồng dữ liệu

Quy trình dịch chuyển và biến đổi của dữ liệu trong hệ thống diễn ra như sau:

1. *Ingestion*: Crawler gửi yêu cầu HTTP đến API TMDB -> Dữ liệu trả về được định dạng lại -> Đẩy vào Kafka topic `movie` (Hot Path) đồng thời lưu vào MinIO bucket `datalake/tmdb.csv` (Cold Path).
2. *Streaming Path*: Spark Structured Streaming đọc từ Kafka -> Phân tích JSON -> Lọc trùng lặp bằng `dropDuplicates` dựa trên `event_time` và `id` -> Tính toán thống kê trượt -> Ghi dữ liệu vào MongoDB collection `speed_movies` và Elasticsearch index `speed-movie`.
3. *Batch Path*: Định kỳ, CronJob khởi chạy Spark SQL -> Đọc dữ liệu từ MinIO -> Áp dụng các UDF để làm sạch và phân nhóm -> Thực hiện Broadcast Joins và Window Analytics -> Ghi đè vào các collection `batch_*` trong MongoDB.
4. *Serving Path*: Client gọi API `/api/movies` -> Serving Layer kiểm tra Cache. Nếu có dữ liệu hợp lệ trong TTL -> Trả về lập tức. Nếu không -> Gọi MongoDB -> Hợp nhất dữ liệu (ưu tiên speed) -> Trả về cho client và cập nhật vào Cache. Nếu client thực hiện tìm kiếm qua `/api/movies/search` -> Thực hiện gọi Elasticsearch -> Trả về kết quả tìm kiếm toàn văn.
