#!/usr/bin/env bash
# ============================================================================
# Build image và push lên DigitalOcean Container Registry (DOCR).
# Yêu cầu: đã cài `doctl` và `docker`, đã `doctl auth init`.
#
# Cách dùng:
#   ./scripts/build-and-push.sh <TEN_REGISTRY> [TAG]
# Ví dụ:
#   ./scripts/build-and-push.sh my-bigdata-registry v1
# ============================================================================
set -euo pipefail

REGISTRY_NAME="${1:?Thiếu tên registry. Dùng: ./build-and-push.sh <registry> [tag]}"
TAG="${2:-latest}"
IMAGE="registry.digitalocean.com/${REGISTRY_NAME}/bigdata-lambda:${TAG}"

echo "==> Đăng nhập DOCR"
doctl registry login

echo "==> Build image: ${IMAGE}"
# --platform linux/amd64 để chắc chắn chạy được trên node DOKS (kể cả khi build từ Mac ARM)
docker build --platform linux/amd64 -t "${IMAGE}" .

echo "==> Push image"
docker push "${IMAGE}"

echo "==> Xong: ${IMAGE}"
echo "    Cập nhật k8s/kustomization.yaml -> images.newName = registry.digitalocean.com/${REGISTRY_NAME}/bigdata-lambda"
