= Kết quả thực nghiệm

== Trạng thái hạ tầng trên Kubernetes

Trạng thái cụm Kubernetes trong namespace `bigdata` được kiểm tra qua lệnh `kubectl get pods -n bigdata` cho thấy các thành phần dịch vụ vận hành ổn định trên đám mây:

- *Nhóm dịch vụ cơ sở dữ liệu và lưu trữ*:
  - Cơ sở dữ liệu MongoDB (`mongodb-0`) chạy ổn định ở dạng StatefulSet đơn bản sao kèm ổ đĩa lưu trữ đám mây.
  - Elasticsearch (`elasticsearch-0`) và MinIO (`minio-0`) hoạt động ở chế độ Standalone làm Serving Store và hồ chứa dữ liệu Master.
- *Nhóm thông điệp*: Cụm Kafka KRaft với một broker (`kafka-cluster-dual-role-0`) được quản lý bởi Strimzi Operator.
- *Nhóm ứng dụng Lambda*:
  - `ingestion-*`: Pod hoạt động liên tục để crawler dữ liệu từ API và đẩy vào Kafka/MinIO.
  - `speed-layer-*`: Spark Structured Streaming xử lý dòng dữ liệu thời gian thực.
  - `serving-layer-*`: Hai Pod Flask chạy phía sau Load Balancer đám mây để phục vụ API.
  - `metabase-*`: Pod chạy dịch vụ Metabase phục vụ trực quan hóa dữ liệu.
  - `batch-layer-*` và `movie-batch-*`: Các CronJob chạy định kỳ để xử lý và đồng bộ dữ liệu theo lô.

== Thu thập dữ liệu (Kafka & MinIO)

Quy trình thu thập dữ liệu được kiểm thử và cho kết quả chính xác:
- Dữ liệu phim thô từ TMDB API được Crawler ghi nhận dưới dạng JSON và lưu trên hồ chứa MinIO. Ví dụ dữ liệu của năm 2025, trang 1 được lưu tại: `datalake/raw/movies/2025/page_1.json`.
- Dữ liệu phim sau đó được Crawler biến đổi dạng phẳng và ghi tiếp (append) vào tệp Master Dataset `tmdb.csv` lưu trên MinIO.
- Đồng thời, bản ghi phim và diễn viên được gửi dưới dạng thông điệp JSON vào Kafka topic `movie` và `actor` để luồng nóng tiêu thụ.

== Phân tích thời gian thực (Speed Layer)

Spark Structured Streaming tiêu thụ dữ liệu từ Kafka topic `movie`, tiến hành chuẩn hóa kiểu dữ liệu số cho `popularity`, `vote_average`, `revenue`, `budget`, `runtime`, phân loại mức độ phổ biến (`Viral`, `Trending`, `Popular`), phân loại đánh giá (`Excellent`, `Good`), làm phẳng trường thể loại (`genres_flat`) và ghi vào MongoDB.

Một bản ghi lưu trong MongoDB collection `speed_movies` có cấu trúc dạng:
```json
{
  "_id": ObjectId("694a6e8e077e7cce0998430a"),
  "id": "83533",
  "title": "Avatar: Fire and Ash",
  "vote_average": 7.3,
  "vote_count": 489,
  "popularity": 531.105,
  "release_date": "2025-12-17",
  "revenue": 347000000,
  "budget": 400000000,
  "runtime": 198,
  "genres_flat": "Science Fiction, Adventure, Fantasy",
  "popularity_category": "Viral",
  "rating_category": "Good",
  "processed_at": ISODate("2025-12-23T10:51:14Z"),
  "layer": "speed"
}
```

== API lớp phục vụ (Serving Layer)

Flask API phục vụ các dữ liệu JSON đã được định dạng và hợp nhất. Dưới đây là bảng tổng hợp các route API chính được Serving Layer hỗ trợ để phục vụ Client và Metabase Dashboard:

#align(center)[
  #table(
    columns: (1.8fr, 0.7fr, 1.8fr, 3.5fr),
    align: (center, center, center, left),
    stroke: 0.5pt + luma(120),
    fill: (col, row) => if row == 0 { rgb("e6f2ff") } else { none },
    [*Endpoint API*], [*Method*], [*Nguồn cơ sở dữ liệu*], [*Vai trò chính*],
    [/api/health], [GET], [MongoDB (Admin)], [Kiểm tra tình trạng kết nối MongoDB],
    [/api/metrics], [GET], [MongoDB (Tất cả)], [Lấy số lượng bản ghi của các collection],
    [/api/movies], [GET], [MongoDB (batch_movies + speed_movies)], [Lấy danh sách phim phân trang, hợp nhất luồng nóng/lạnh],
    [/api/movies/search], [GET], [Elasticsearch (MongoDB fallback)], [Tìm kiếm phim toàn văn theo từ khóa],
    [/api/movies/\<id\>], [GET], [MongoDB (batch_movies + speed_movies)], [Lấy thông tin chi tiết của bộ phim theo ID],
    [/api/stats/genres], [GET], [MongoDB (batch_genre_stats + speed_genre_stats)], [Lấy thống kê phim theo thể loại (được cache 60s)],
    [/api/stats/years], [GET], [MongoDB (batch_year_stats + speed_year_stats)], [Lấy thống kê phim theo năm phát hành (được cache 60s)],
    [/api/stats/languages], [GET], [MongoDB (batch_language_stats + speed_language_stats)], [Lấy thống kê phim theo ngôn ngữ gốc (được cache 60s)],
    [/api/stats/directors], [GET], [MongoDB (batch_director_stats)], [Lấy thống kê về các đạo diễn (được cache 120s)],
    [/api/top/movies], [GET], [MongoDB (batch_top_movies + speed_top_movies)], [Lấy danh sách các phim có điểm đánh giá cao nhất],
    [/api/top/profitable], [GET], [MongoDB (batch_top_movies)], [Lấy danh sách phim sinh lời cao nhất (ROI)],
    [/api/top/popular], [GET], [MongoDB (speed_top_movies + batch_top_movies)], [Lấy danh sách phim phổ biến nhất (ưu tiên luồng nóng)],
    [/api/lambda/status], [GET], [MongoDB (batch_movies + speed_movies)], [Xem trạng thái và tổng số bản ghi của hai luồng xử lý]
  )
]

Ví dụ phản hồi từ API hợp nhất danh sách phim (`/api/movies?limit=1`):
```json
{
  "success": true,
  "layer": "serving_layer",
  "timestamp": "2025-12-23T12:01:22",
  "data": [
    {
      "id": "1130276",
      "title": "Succubus",
      "vote_average": 8.8,
      "popularity": 48.0,
      "revenue": 0,
      "budget": 0,
      "profit": 0,
      "release_year": 2024,
      "primary_genre": "Horror",
      "rating_category": "Excellent",
      "source_layer": "batch"
    }
  ]
}
```

== Bảng điều khiển trực quan hóa (Metabase Dashboard)

Metabase kết nối với MongoDB để tạo các biểu đồ phân tích phục vụ báo cáo:

1. *Biểu đồ thống kê dữ liệu phim*: Bảng chi tiết hiển thị danh sách phim kèm các thông số đánh giá, mức độ phổ biến, ngày phát hành từ MongoDB.
2. *Biểu đồ tỷ lệ ngôn ngữ gốc*: Biểu đồ hình tròn thể hiện thị phần ngôn ngữ. Phim tiếng Anh chiếm tỷ lệ 41.7%, tiếp theo là tiếng Nhật (11.1%), tiếng Pháp (5.7%) và tiếng Trung (4.8%).
3. *Biểu đồ độ phổ biến của các thể loại phim*: Biểu đồ bánh donut thể hiện số lượng phim theo thể loại, trong đó thể loại Drama dẫn đầu với hơn 22%, theo sau là Comedy (13.3%) và Thriller (9.9%).
4. *Biểu đồ tương quan doanh thu và ngân sách theo năm*: Biểu đồ cột nhóm so sánh tổng doanh thu và tổng ngân sách qua các năm (2022 - 2025).
5. *Biểu đồ xu hướng thể loại theo thời gian*: Biểu đồ đường biểu diễn sự thay đổi về độ phổ biến trung bình của các thể loại phim qua từng năm phát hành.
6. *Bảng chuyển trục ngân sách trung bình theo quý*: Bảng ma trận phân loại doanh thu trung bình của các nhóm phim (Mega Blockbuster, Blockbuster, Medium Budget...) phân tách theo từng quý phát hành.
