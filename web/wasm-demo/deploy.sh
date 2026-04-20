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
cd "$(dirname "$0")"

: "${PGRUST_DEMO_BUCKET:?set PGRUST_DEMO_BUCKET to the target S3 bucket}"
: "${PGRUST_CLOUDFRONT_ID:?set PGRUST_CLOUDFRONT_ID to the CloudFront distribution id}"

./build.sh

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

aws cloudfront create-invalidation \
  --distribution-id "$PGRUST_CLOUDFRONT_ID" \
  --paths "/*" \
  >/dev/null
echo "Invalidated CloudFront distribution $PGRUST_CLOUDFRONT_ID"

echo "Deployed to ${PGRUST_DEMO_URL:-s3://$PGRUST_DEMO_BUCKET}"
