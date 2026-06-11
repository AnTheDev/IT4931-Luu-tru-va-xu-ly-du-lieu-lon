= Chi tiết triển khai

== Mã nguồn của hệ thống
Mã nguồn và các tệp cấu hình Kubernetes được lưu trữ trên GitHub tại:
#link("https://github.com/tvgx/20251-IT4931-N31/tree/main")[GitHub Repository - IT4931 Nhóm 22]

== Yêu cầu hệ thống

Hệ thống được thiết kế để triển khai và vận hành trên môi trường điện toán đám mây với các công cụ quản lý sau:
- *DigitalOcean Kubernetes Service*: Cụm Kubernetes được quản lý gồm 3 worker nodes cấu hình `s-4vcpu-8gb` (tổng tài nguyên 12 vCPUs và 24GiB RAM) chạy trên khu vực `sgp1` (Singapore).
- *DigitalOcean Container Registry*: Registry dùng để lưu trữ và quản lý các Docker image của dự án.
- *Kubectl*: Công cụ dòng lệnh CLI để tương tác trực tiếp với API Server của cụm Kubernetes.
- *Helm*: Công cụ quản lý gói ứng dụng để thiết lập Strimzi Kafka Operator.
- *Doctl*: Công cụ dòng lệnh CLI của DigitalOcean để quản lý tài nguyên đám mây.

== Chiến lược triển khai

Hệ thống được xây dựng và triển khai theo trình tự các bước như sau:

=== Khởi tạo cụm và kết nối Registry
Khởi tạo cụm Kubernetes trên DigitalOcean với cấu hình tự động co giãn từ 2 đến 4 nodes:
```bash
doctl kubernetes cluster create bigdata-cluster \
  --region sgp1 \
  --version latest \
  --node-pool "name=worker-pool;size=s-4vcpu-8gb;count=3;auto-scale=true;min-nodes=2;max-nodes=4"
```

Tạo registry để lưu trữ container image:
```bash
doctl registry create my-bigdata-registry --subscription-tier basic
```

Tích hợp registry với cụm Kubernetes để các worker nodes có quyền kéo image:
```bash
doctl kubernetes cluster registry add bigdata-cluster
```

=== Xây dựng và đóng gói ứng dụng
Đăng nhập vào registry từ máy local để thực hiện đẩy image:
```bash
doctl registry login
```

Xây dựng Docker image cho ứng dụng và đẩy lên registry:
```bash
docker build -t registry.digitalocean.com/my-bigdata-registry/bigdata-lambda:latest .
docker push registry.digitalocean.com/my-bigdata-registry/bigdata-lambda:latest
```

=== Tự động hóa quy trình CI/CD qua GitHub Actions
Quy trình triển khai của hệ thống được tự động hóa hoàn toàn thông qua GitHub Actions (cấu hình tại tệp `.github/workflows/deploy.yml`). Mỗi khi mã nguồn mới được đẩy lên nhánh `main`, hệ thống tự động thực hiện:
1. Đăng nhập vào registry đám mây của DigitalOcean.
2. Xây dựng Docker image cho kiến trúc `linux/amd64` và gắn hai thẻ: `latest` và Git Commit SHA hiện tại, sau đó đẩy lên registry.
3. Sinh tệp chứa biến môi trường bí mật `k8s/02-secret.yaml` từ tệp cấu hình mẫu `k8s/02-secret.yaml.template` bằng lệnh `envsubst`, lấy giá trị an toàn từ GitHub Secrets.
4. Cập nhật thẻ image mới nhất trong cấu hình Kustomize theo Git Commit SHA để đảm bảo tính nhất quán.
5. Triển khai các tài nguyên lên cụm Kubernetes bằng lệnh `kubectl apply -k k8s/` và kiểm tra trạng thái hoạt động của các pod.

=== Triển khai hạ tầng và ứng dụng
1. *Tạo Namespace*: Tạo namespace `bigdata` để cô lập tài nguyên:
```bash
kubectl apply -f k8s/00-namespace.yaml
```
2. *Triển khai Kafka*: Cài đặt Strimzi Operator qua Helm và định nghĩa cụm Kafka KRaft:
```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-operator strimzi/strimzi-kafka-operator \
  --namespace bigdata \
  --set watchNamespaces="{bigdata}"

kubectl apply -f k8s/kafka/kafka-cluster.yaml
kubectl apply -f k8s/kafka/kafka-topics.yaml
kubectl -n bigdata wait kafka/kafka-cluster --for=condition=Ready --timeout=600s
```
3. *Triển khai cơ sở dữ liệu và lưu trữ*: Triển khai các cấu hình MongoDB standalone, Elasticsearch, MinIO và Metabase bằng Kustomize:
```bash
kubectl apply -k k8s/
```
4. *Khởi tạo tài nguyên*:
  - *Tạo Bucket trên MinIO*: Sử dụng port-forward để truy cập MinIO Console tại `http://localhost:9001` bằng tài khoản quản trị, tạo bucket tên là `datalake`.
  - *Tạo User cho MongoDB*: Tạo tài khoản và cấp quyền cho Spark ứng dụng ghi dữ liệu:
```bash
kubectl exec -it mongodb-0 -n bigdata -- mongosh admin -u admin -p adminPass --eval "db.createUser({ user: 'spark-user', pwd: 'spark-pass', roles: [{ role: 'readWrite', db: 'BIGDATA' }] })"
```

=== Truy cập và kiểm tra hệ thống
Để tương tác với Serving Layer và Metabase từ bên ngoài Internet, hệ thống sử dụng dịch vụ Load Balancer của DigitalOcean được tích hợp sẵn:
```bash
# Lấy địa chỉ IP công cộng của Serving Layer
kubectl -n bigdata get svc serving-layer

# Lấy địa chỉ IP công cộng của Metabase Dashboard
kubectl -n bigdata get svc metabase
```

Bảng dưới đây tổng hợp các workload được triển khai trong namespace `bigdata`:

#align(center)[
  #table(
    columns: (1.5fr, 1.2fr, 0.8fr, 3fr),
    align: (center, center, center, left),
    stroke: 0.5pt + luma(120),
    fill: (col, row) => if row == 0 { rgb("e6f2ff") } else { none },
    [*Tên Workload*], [*Loại*], [*Replicas*], [*Vai trò chính*],
    [ingestion], [Deployment], [1], [Crawler dữ liệu từ TMDB API và đẩy vào Kafka/MinIO],
    [speed-layer], [Deployment], [1], [Spark Structured Streaming xử lý dòng thời gian thực],
    [batch-layer], [CronJob], [1 (định kỳ)], [Spark SQL xử lý Master Dataset định kỳ trên MinIO],
    [serving-layer], [Deployment], [2], [Flask API Gateway phục vụ dữ liệu hợp nhất cho client],
    [metabase], [Deployment], [1], [Metabase phục vụ trực quan hóa dữ liệu phim],
    [kafka-cluster], [StatefulSet], [1], [Kafka KRaft Broker lưu trữ hàng đợi thông điệp],
    [mongodb], [StatefulSet], [1], [MongoDB lưu trữ Serving Store của hai luồng nóng/lạnh],
    [minio], [StatefulSet], [1], [MinIO Object Storage làm hồ chứa dữ liệu Master],
    [elasticsearch], [StatefulSet], [1], [Elasticsearch phục vụ tìm kiếm toàn văn]
  )
]


== Một số kỹ thuật nâng cao áp dụng trong mã nguồn

Trong quá trình xây dựng hệ thống, các kỹ thuật lập trình sau được áp dụng để kiểm soát hiệu năng và bộ nhớ:

1. *Tối ưu hóa tầng thu thập*: Để giảm mức tiêu thụ RAM trên Kubernetes Node, thay vì chạy một tiến trình Spark Job để kéo dữ liệu API, hệ thống sử dụng script thu thập tinh gọn sử dụng thư viện `requests` và `kafka-python` thuần. Điều này giúp giảm RAM tiêu thụ từ 1.5GB xuống dưới 100MB cho Pod Ingestion.
2. *Stateful Deduplication & Watermarking trong Streaming*: Trong `enhanced_speed_layer.py`, dữ liệu phim nhận từ Kafka được lọc trùng bằng hàm `dropDuplicates(["id"])` trong một cửa sổ trượt. Việc áp dụng `withWatermark("event_time", "10 minutes")` cho phép Spark Structured Streaming duy trì trạng thái của các phim nhận được trong vòng 10 phút để loại bỏ các bản ghi trùng lặp do độ trễ truyền phát.
3. *Broadcast Joins*: Trong `enhanced_batch_layer.py`, do các bảng tra cứu thể loại và ngôn ngữ có kích thước nhỏ (< 1MB), hệ thống áp dụng hàm `broadcast()` để sao chép các bảng này đến các Executor, tránh giai đoạn Shuffle dữ liệu qua mạng và giảm thời gian thực thi của Batch Job.
4. *Self-Join tối ưu để phát hiện phim tương đồng*: Nhóm thực hiện Self-Join trên tập dữ liệu phim lớn để tìm các phim có sự tương đồng. Hệ thống lọc trước các cột cần thiết, áp dụng Sort-Merge Join trên khóa `release_year` và `primary_genre`, và đặt điều kiện `id_a < id_b` nhằm tránh lặp lại các phép so sánh trùng lặp.
5. *Serving Caching*: Serving Layer sử dụng Flask tích hợp một decorator `@cached` thread-safe. Decorator này sử dụng `OrderedDict` để cài đặt bộ nhớ đệm LRU giới hạn kích thước tối đa 1000 phần tử, kèm theo thời gian sống TTL 60 giây. Các yêu cầu trùng lặp sẽ được phản hồi từ RAM thay vì truy vấn xuống MongoDB.
