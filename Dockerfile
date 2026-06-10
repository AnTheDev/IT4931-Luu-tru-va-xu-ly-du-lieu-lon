# ============================================================================
# Big Data Lambda Architecture - All-in-one image
# Mọi workload (ingestion / speed / batch / serving) dùng chung image này,
# chỉ khác `command` khi chạy trên Kubernetes.
# ============================================================================
FROM python:3.11-slim-bookworm

# --- Java runtime cho PySpark (Spark 3.5.1 hỗ trợ Java 17) ---
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV SPARK_LOCAL_IP=127.0.0.1
ENV HOME=/root

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Cài Python deps (tách layer để tận dụng cache) ---
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# --- Pre-warm Ivy cache: tải sẵn toàn bộ connector JAR vào image ---
# Tránh việc mỗi pod phải tải lại jar từ Maven lúc khởi động (chậm & dễ lỗi mạng).
RUN python - <<'PY'
from pyspark.sql import SparkSession
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.500",
]
spark = (SparkSession.builder
         .master("local[1]")
         .appName("warm-ivy-cache")
         .config("spark.jars.packages", ",".join(packages))
         .config("spark.sql.warehouse.dir", "/tmp/warehouse")
         .getOrCreate())
print("Ivy cache warmed for:", packages)
spark.stop()
PY

# --- Copy source code ---
COPY app/ /app/

# Thư mục checkpoint mặc định cho Spark Structured Streaming
RUN mkdir -p /tmp/checkpoint

# Serving Layer chạy ở cổng 5000 (các workload khác bỏ qua)
EXPOSE 5000

# Default command (override bằng `command` trong từng Deployment/CronJob)
CMD ["python", "enhanced_serving_layer.py"]
