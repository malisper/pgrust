#!/usr/bin/env bash
# Deploy the pgrust wasm demo to https://pgrust.com (CloudFront + S3).
#
# Requires AWS_PROFILE=mfa (or another profile with S3 + CloudFront access
# on account 149051628381). Uses terraform output from pgrust/domains/ to
# locate the CloudFront distribution for invalidation.

set -euo pipefail
cd "$(dirname "$0")"

BUCKET="pgrust"

./build.sh

# Pass 1: everything except .wasm — let S3 auto-detect Content-Type.
AWS_PROFILE=mfa aws s3 sync . "s3://${BUCKET}/" \
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
AWS_PROFILE=mfa aws s3 sync . "s3://${BUCKET}/" \
  --exclude "*" \
  --include "*.wasm" \
  --content-type "application/wasm"

# Invalidate CloudFront so edge caches pick up the new build.
DIST_ID="${PGRUST_CLOUDFRONT_ID:-}"
if [[ -z "$DIST_ID" ]]; then
  DIST_ID="$(cd ../../domains && AWS_PROFILE=mfa terraform output -raw cloudfront_distribution_id 2>/dev/null || true)"
fi

if [[ -n "$DIST_ID" ]]; then
  AWS_PROFILE=mfa aws cloudfront create-invalidation \
    --distribution-id "$DIST_ID" \
    --paths "/*" \
    >/dev/null
  echo "Invalidated CloudFront distribution $DIST_ID"
else
  echo "Warning: could not determine CloudFront distribution id; skipping invalidation."
  echo "Set PGRUST_CLOUDFRONT_ID or run from a tree where pgrust/domains/ terraform state is accessible."
fi

echo "Deployed to https://pgrust.com"
