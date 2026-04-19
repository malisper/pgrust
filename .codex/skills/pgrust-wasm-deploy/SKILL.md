---
name: pgrust-wasm-deploy
description: Deploy the pgrust wasm demo to https://pgrust.com (CloudFront + S3). Use this when the user asks to publish, push, or redeploy the browser demo or wasm app. Single script handles build + sync + CloudFront invalidation.
---

# Pgrust Wasm Deploy

Use this skill when the user asks to deploy or redeploy the browser wasm demo.

## Default workflow

Run the deploy script:

```bash
./web/wasm-demo/deploy.sh
```

That script does the full flow:

1. `./web/wasm-demo/build.sh` — rebuilds the wasm bundle
2. `aws s3 sync web/wasm-demo/ s3://pgrust/ --delete` — pushes files, excluding scripts and build artifacts
3. Second sync pass with `--content-type application/wasm` for `.wasm` files (browsers need this for `WebAssembly.compileStreaming`)
4. `aws cloudfront create-invalidation --paths /*` — busts edge caches

The script uses `AWS_PROFILE=mfa` internally and reads the CloudFront distribution id from `pgrust/domains/` terraform output.

## Target

- Site: `https://pgrust.com`
- Bucket: `s3://pgrust` (us-west-2, private — read through CloudFront OAC)
- CDN: CloudFront (distribution id in `pgrust/domains/` terraform output)
- DNS / TLS / redirect infra: managed in `pgrust/domains/` (terraform)

## Verification

```bash
curl -sI https://pgrust.com | head -5                              # 2xx via CloudFront
curl -sI https://pgrust.com/pkg/pgrust_bg.wasm | grep -i content-  # application/wasm
```

Open the site and run a query end-to-end.

## Notes

- Do not bypass `deploy.sh` — syncing without re-setting wasm content-type breaks the demo.
- Do not bypass CloudFront invalidation — edge caches will serve stale content.
- Infra changes (not content) live in `pgrust/domains/main.tf`. See `pgrust/domains/README.md`.
- Avoid reverting unrelated local changes in `web/wasm-demo`; deploy the requested state.
- If `aws` fails due to missing MFA session, `aws sts get-session-token` into the `mfa` profile and retry.
