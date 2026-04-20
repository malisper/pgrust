---
name: pgrust-wasm-deploy
description: Deploy the pgrust wasm demo to the configured S3 + CloudFront site. Use this when the user asks to publish, push, or redeploy the browser demo or wasm app. Single script handles build + sync + CloudFront invalidation.
---

# pgrust wasm deploy

Use this skill when the user asks to deploy or redeploy the browser wasm demo.

## Default workflow

The deploy script reads all target info from environment variables:

```bash
PGRUST_DEMO_BUCKET=your-bucket \
PGRUST_CLOUDFRONT_ID=EXXXXXXXXXXXXX \
AWS_PROFILE=your-profile \
./web/wasm-demo/deploy.sh
```

That script does the full flow:

1. `./web/wasm-demo/build.sh` — rebuilds the wasm bundle.
2. `aws s3 sync web/wasm-demo/ s3://$PGRUST_DEMO_BUCKET/ --delete` — pushes files, excluding scripts and build artifacts.
3. A second sync pass with `--content-type application/wasm` for `.wasm` files (browsers need this for `WebAssembly.compileStreaming`).
4. `aws cloudfront create-invalidation --distribution-id $PGRUST_CLOUDFRONT_ID --paths /*` — busts edge caches.

## Required environment

- `PGRUST_DEMO_BUCKET` — S3 bucket name for the demo.
- `PGRUST_CLOUDFRONT_ID` — CloudFront distribution id to invalidate.
- `AWS_PROFILE` (optional) — aws cli profile to use.

## Verification

```bash
curl -sI "${PGRUST_DEMO_URL:?}" | head -5              # 2xx via CloudFront
curl -sI "${PGRUST_DEMO_URL:?}/pkg/pgrust_bg.wasm" \
  | grep -i content-                                   # application/wasm
```

Open the site and run a query end-to-end.

## Notes

- Do not bypass `deploy.sh` — syncing without re-setting wasm content-type breaks the demo.
- Do not bypass CloudFront invalidation — edge caches will serve stale content.
- Avoid reverting unrelated local changes in `web/wasm-demo`; deploy the requested state.
