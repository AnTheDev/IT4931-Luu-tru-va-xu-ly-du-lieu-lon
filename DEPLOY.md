# Triển khai Lambda Architecture lên DigitalOcean Kubernetes (DOKS)

Hướng dẫn deploy pipeline Big Data xử lý dữ liệu phim TMDB theo kiến trúc **Lambda** lên **DigitalOcean**.

---

## 1. Kiến trúc tổng thể

```
                                  ┌──────────────────────────────────────────────┐
                                  │            DOKS (namespace: bigdata)           │
                                  │                                                │
  TMDB API ──► [ingestion]───────►│  Kafka (Strimzi)        MinIO (data lake)      │
  (Deployment)  crawl + produce   │   topic: movie ┐        s3a://datalake/tmdb.csv│
                                  │                │                  │            │
                                  │      ┌─────────┘                  │            │
                                  │      ▼                            ▼            │
                                  │  [speed-layer]            [batch-layer]        │
                                  │  Spark Streaming          Spark CronJob (6h)   │
                                  │  (Deployment)             [movie-batch] (12h)  │
                                  │      │                            │            │
                                  │      ▼                            ▼            │
                                  │   MongoDB  speed_*  ◄──────►  batch_*  + Elasticsearch
                                  │      │                            │            │
                                  │      └────────────┬───────────────┘            │
                                  │                   ▼                            │
                                  │            [serving-layer]  Flask API          │
                                  │            (Deployment x2)                     │
                                  │                   │      [metabase]            │
                                  └───────────────────┼──────────┼────────────────┘
                                                      ▼          ▼
                                              DO Load Balancer (public IP)
                                              REST API          Dashboard
```

| Thành phần | Loại workload | Image | Ghi chú |
|---|---|---|---|
| ingestion | Deployment | `bigdata-lambda` | `data_ingestion_producer.py`, chạy liên tục |
| speed-layer | Deployment + PVC | `bigdata-lambda` | `enhanced_speed_layer.py`, Spark Streaming |
| batch-layer | CronJob (6h) | `bigdata-lambda` | `enhanced_batch_layer.py`, đọc MinIO CSV |
| movie-batch | CronJob (12h) | `bigdata-lambda` | `movie_batch.py`, đọc Kafka |
| actor-batch | CronJob (suspended) | `bigdata-lambda` | `actor_batch.py` (chưa có producer) |
| serving-layer | Deployment + LB | `bigdata-lambda` | `enhanced_serving_layer.py`, Flask |
| metabase | Deployment + LB | `metabase/metabase` | BI dashboard |
| Kafka | Strimzi CR | — | cluster `kafka-cluster`, KRaft |
| MongoDB | StatefulSet + PVC | `mongo:7` | serving store |
| Elasticsearch | StatefulSet + PVC | `elasticsearch:8.15` | full-text |
| MinIO | StatefulSet + PVC | `minio` | data lake (S3) |

> **Spark chạy local mode** (`MASTER=local[*]`) ngay trong từng pod — không cần cụm Spark riêng.

---

## 2. Yêu cầu công cụ (máy local)

```bash
# doctl - CLI của DigitalOcean
#   macOS:  brew install doctl
#   Windows (winget): winget install --id DigitalOcean.doctl
# kubectl, docker, kustomize (kustomize đã tích hợp trong kubectl >= 1.14)

doctl auth init        # dán Personal Access Token (DO -> API -> Tokens)
```

---

## 3. Tạo Container Registry (DOCR)

```bash
# Tạo registry (tên phải là duy nhất toàn cầu trong tài khoản bạn)
doctl registry create my-bigdata-registry --subscription-tier basic
```

> Ghi nhớ tên registry — ví dụ `my-bigdata-registry`. Image sẽ là
> `registry.digitalocean.com/my-bigdata-registry/bigdata-lambda`.

---

## 4. Tạo cụm Kubernetes (DOKS)

Tổng RAM cần (lúc cao điểm): speed 4Gi + batch 4Gi + ES 2.5Gi + Kafka 2Gi + Mongo 2Gi + MinIO 1Gi + serving/metabase ~3Gi ≈ **18–20Gi**.

Khuyến nghị: **3 node `s-4vcpu-8gb`** (24Gi RAM tổng).

```bash
doctl kubernetes cluster create bigdata-cluster \
  --region sgp1 \
  --version latest \
  --node-pool "name=worker-pool;size=s-4vcpu-8gb;count=3;auto-scale=true;min-nodes=2;max-nodes=4"

# kubeconfig được tự động merge; kiểm tra:
kubectl get nodes
```

---

## 5. Cho phép DOKS kéo image từ DOCR

```bash
# Gắn registry vào cụm (tự tạo imagePullSecret cho mọi namespace)
doctl kubernetes cluster registry add bigdata-cluster
```

> Nếu bước này không tự áp dụng cho namespace `bigdata`, thêm thủ công:
> ```bash
> kubectl get namespace bigdata || kubectl create namespace bigdata
> doctl registry kubernetes-manifest | kubectl apply -n bigdata -f -
> kubectl patch serviceaccount default -n bigdata \
>   -p '{"imagePullSecrets":[{"name":"registry-my-bigdata-registry"}]}'
> ```

---

## 6. Build & push image lên DOCR

```bash
# Từ thư mục gốc dự án
chmod +x scripts/build-and-push.sh
./scripts/build-and-push.sh my-bigdata-registry latest
```

(Trên Windows PowerShell có thể chạy trực tiếp các lệnh trong script, hoặc dùng Git Bash/WSL.)

---

## 7. Cài Strimzi Kafka Operator

```bash
kubectl create namespace bigdata --dry-run=client -o yaml | kubectl apply -f -

# Cài operator qua Helm (chỉ watch namespace bigdata)
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-operator strimzi/strimzi-kafka-operator \
  --namespace bigdata \
  --set watchNamespaces="{bigdata}"

# Chờ operator sẵn sàng
kubectl -n bigdata rollout status deploy/strimzi-cluster-operator
```

---

## 8. Cập nhật cấu hình trước khi apply

1. **Image** — sửa `k8s/kustomization.yaml`:
   ```yaml
   images:
     - name: bigdata-lambda
       newName: registry.digitalocean.com/my-bigdata-registry/bigdata-lambda
       newTag: latest
   ```

2. **Secrets** — sửa `k8s/02-secret.yaml`, thay tất cả `REPLACE_*` / `ChangeMe-*`:
   - `TMDB_BEARER_TOKEN`: token v4 từ https://www.themoviedb.org/settings/api
   - Mật khẩu Mongo phải **trùng nhau** ở `CONNECTION_STRING`, `mongodb-secret`.
   - Mật khẩu MinIO phải **trùng** ở `bigdata-secrets` và `minio-secret`.

---

## 9. Deploy

```bash
# 9.1 - Hạ tầng + ứng dụng (mọi thứ trừ Kafka)
kubectl apply -k k8s/

# 9.2 - Kafka (sau khi operator đã chạy)
kubectl apply -f k8s/kafka/kafka-cluster.yaml
kubectl apply -f k8s/kafka/kafka-topics.yaml

# Chờ Kafka ready
kubectl -n bigdata wait kafka/kafka-cluster --for=condition=Ready --timeout=600s
```

---

## 10. Kiểm tra

```bash
kubectl -n bigdata get pods
kubectl -n bigdata get pvc
kubectl -n bigdata get svc          # lấy EXTERNAL-IP của serving-layer & metabase

# Log từng tầng
kubectl -n bigdata logs -f deploy/ingestion
kubectl -n bigdata logs -f deploy/speed-layer
kubectl -n bigdata logs -f deploy/serving-layer
```

Lấy IP public:

```bash
SERVING_IP=$(kubectl -n bigdata get svc serving-layer -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl http://$SERVING_IP/api/health
curl http://$SERVING_IP/api/lambda/status
curl "http://$SERVING_IP/api/movies?limit=5"
```

Metabase: mở `http://<EXTERNAL-IP-metabase>` → tạo admin → thêm data source MongoDB:
- Host: `mongodb.bigdata.svc.cluster.local`, Port `27017`, DB `BIGDATA`, user/pass như secret, `authSource=admin`.

---

## 11. Chạy batch job thủ công (không chờ lịch cron)

```bash
kubectl -n bigdata create job --from=cronjob/batch-layer batch-manual-1
kubectl -n bigdata create job --from=cronjob/movie-batch movie-batch-manual-1
kubectl -n bigdata logs -f job/batch-manual-1
```

---

## 12. Thứ tự khởi động & lưu ý vận hành

- **Storage (Mongo/MinIO/ES) + Kafka phải Ready trước** khi ingestion/speed bắt đầu ghi. Các pod app sẽ `CrashLoopBackOff` vài lần lúc đầu rồi tự ổn định khi dependency lên — đây là hành vi bình thường.
- **batch-layer** cần file `tmdb.csv` đã tồn tại trong MinIO. Hãy để `ingestion` chạy một lúc (tạo `s3a://datalake/tmdb.csv`) trước khi batch-layer có dữ liệu.
- **actor-batch** đang `suspend: true` vì source chưa có producer ghi vào topic `actor`. Khi bổ sung actor producer, đổi `suspend: false`.
- **Checkpoint** của speed-layer nằm trên PVC `speed-checkpoint` → recover được khi pod restart.

---

## 13. Dọn dẹp (tránh tốn phí)

```bash
kubectl delete -k k8s/
kubectl delete -f k8s/kafka/
helm uninstall strimzi-operator -n bigdata
# Xoá Load Balancer + Volume còn sót (DO tính phí theo giờ)
kubectl -n bigdata delete svc serving-layer metabase
doctl kubernetes cluster delete bigdata-cluster
doctl registry delete my-bigdata-registry
```

> ⚠️ Kiểm tra lại mục **Volumes** và **Load Balancers** trên DigitalOcean console — chúng KHÔNG tự xoá khi xoá cụm và vẫn bị tính phí.

---

## 14. Chi phí ước tính (tham khảo)

| Hạng mục | Đơn giá | Ghi chú |
|---|---|---|
| DOKS 3× s-4vcpu-8gb | ~$48/node/tháng | ~$144/tháng |
| 2× Load Balancer | ~$12/cái/tháng | ~$24/tháng |
| Block storage (~70Gi) | ~$0.10/Gi/tháng | ~$7/tháng |
| DOCR basic | ~$5/tháng | |

Tổng ~**$180/tháng** nếu chạy 24/7. Để tiết kiệm cho đồ án: scale node pool xuống/destroy cụm khi không dùng, hoặc đổi 2 Load Balancer → 1 ingress-nginx.
