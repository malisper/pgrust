#!/usr/bin/env bash
# Deploy the pgrust wasm demo to an S3 + CloudFront static site.
#
# Required env vars:
#   PGRUST_DEMO_BUCKET        S3 bucket name for the demo (e.g. "my-pgrust-demo")
#   PGRUST_CLOUDFRONT_ID      CloudFront distribution id to invalidate
# Optional env vars:
#   AWS_PROFILE               aws cli profile to use (default: current shell default)
#   PGRUST_DEMO_URL           public URL printed at end of deploy (cosmetic)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$SCRIPT_DIR"

: "${PGRUST_DEMO_BUCKET:?set PGRUST_DEMO_BUCKET to the target S3 bucket}"
: "${PGRUST_CLOUDFRONT_ID:?set PGRUST_CLOUDFRONT_ID to the CloudFront distribution id}"

./build.sh

BUILD_COMMIT="${PGRUST_DEMO_COMMIT:-$(git -C "$ROOT" rev-parse HEAD)}"
BUILD_AT="${PGRUST_DEMO_BUILT_AT:-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}"

tag_deployed_object() {
  local key="$1"
  aws s3api put-object-tagging \
    --bucket "$PGRUST_DEMO_BUCKET" \
    --key "$key" \
    --tagging "TagSet=[{Key=commit,Value=$BUILD_COMMIT},{Key=built_at,Value=$BUILD_AT}]" \
    >/dev/null
}

# Pass 1: everything except .wasm — let S3 auto-detect Content-Type.
aws s3 sync . "s3://${PGRUST_DEMO_BUCKET}/" \
  --delete \
  --exclude "build.sh" \
  --exclude "deploy.sh" \
  --exclude ".DS_Store" \
  --exclude "*.rs" \
  --exclude "target/*" \
  --exclude "*.wasm"

# Pass 2: .wasm files with explicit Content-Type.
# Browsers need `application/wasm` for streaming compile (WebAssembly.compileStreaming).
# S3 MIME auto-detection is unreliable for .wasm.
aws s3 sync . "s3://${PGRUST_DEMO_BUCKET}/" \
  --exclude "*" \
  --include "*.wasm" \
  --content-type "application/wasm"

tag_deployed_object "index.html"
tag_deployed_object "main.js"
tag_deployed_object "pkg/pgrust.js"
tag_deployed_object "pkg/pgrust_bg.wasm"

aws cloudfront create-invalidation \
  --distribution-id "$PGRUST_CLOUDFRONT_ID" \
  --paths "/*" \
  >/dev/null
echo "Invalidated CloudFront distribution $PGRUST_CLOUDFRONT_ID"

echo "Deployed to ${PGRUST_DEMO_URL:-s3://$PGRUST_DEMO_BUCKET}"
