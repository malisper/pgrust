#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${1:-ea-$(date +%F)}"
IMAGE_NAME="${IMAGE_NAME:-pgrust-early-access}"
PLATFORM="${PLATFORM:-linux/arm64}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/target/early-access}"
SMOKE_CONTAINER="${SMOKE_CONTAINER:-pgrust-early-access-smoke}"
SMOKE_PORT="${SMOKE_PORT:-5544}"

archive_basename="${IMAGE_NAME}-${VERSION}-${PLATFORM//\//-}"
archive_path="$OUTPUT_DIR/${archive_basename}.tar.gz"
checksum_path="${archive_path}.sha256"

mkdir -p "$OUTPUT_DIR"

docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true
trap 'docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true' EXIT

echo "Building $IMAGE_NAME:$VERSION for $PLATFORM"
docker buildx build \
    --platform "$PLATFORM" \
    -f "$ROOT_DIR/Dockerfile.early-access" \
    -t "$IMAGE_NAME:$VERSION" \
    -t "$IMAGE_NAME:latest" \
    --load \
    "$ROOT_DIR"

echo "Smoke testing container startup on localhost:$SMOKE_PORT"
docker run -d --rm \
    --name "$SMOKE_CONTAINER" \
    -p "${SMOKE_PORT}:5432" \
    "$IMAGE_NAME:$VERSION" >/dev/null

sleep 2
logs="$(docker logs "$SMOKE_CONTAINER" 2>&1 || true)"
if [[ "$logs" != *"pgrust: listening on 0.0.0.0:5432"* ]]; then
    printf '%s\n' "$logs"
    echo "Smoke test failed: container did not reach listening state" >&2
    exit 1
fi

docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true
trap - EXIT

echo "Writing $archive_path"
docker save "$IMAGE_NAME:$VERSION" "$IMAGE_NAME:latest" | gzip > "$archive_path"

if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$archive_path" > "$checksum_path"
else
    sha256sum "$archive_path" > "$checksum_path"
fi

cat <<EOF
Created:
  $archive_path
  $checksum_path

Share with evaluator:
  gunzip -c $(basename "$archive_path") | docker load
  docker run --rm -p 5432:5432 $IMAGE_NAME:$VERSION
EOF
