---
name: pgrust-wasm-deploy
description: Deploy the pgrust wasm demo to the pgrust S3 bucket when the user asks to publish, push, or redeploy the browser demo or wasm app. Use this for the static site under web/wasm-demo and the S3 bucket s3://pgrust.
---

# Pgrust Wasm Deploy

Use this skill when the user asks to deploy or redeploy the browser wasm demo.

## Target

- Bucket: `s3://pgrust`
- Region: `us-west-2`
- Site root: bucket root
- Website config: `index.html` for both index and error documents

The deployed files mirror `web/wasm-demo/`:
- root files like `index.html`, `main.js`, `env.js`, `README.md`, `build.sh`
- generated browser package under `web/wasm-demo/pkg/`

## Default workflow

1. Check whether the worktree is already dirty in `web/wasm-demo`.
2. If the user wants the current checked-out demo published, deploy those files as-is.
3. If the user wants a fresh build, run:

```bash
./web/wasm-demo/build.sh
```

4. Sync the site:

```bash
aws s3 sync web/wasm-demo/ s3://pgrust/ --delete
```

5. Re-upload the wasm binary with the correct MIME type. Do this even after `sync`:

```bash
aws s3 cp web/wasm-demo/pkg/pgrust_bg.wasm s3://pgrust/pkg/pgrust_bg.wasm --content-type application/wasm
```

## Verification

Run:

```bash
aws s3 ls s3://pgrust/ --recursive
aws s3api head-object --bucket pgrust --key pkg/pgrust_bg.wasm
```

The wasm object must report:
- `ContentType: application/wasm`

Optional metadata check for JS:

```bash
aws s3api head-object --bucket pgrust --key main.js
```

## Notes

- Do not guess a different bucket or prefix; this demo currently deploys to bucket root.
- Avoid reverting unrelated local changes in `web/wasm-demo`; deploy the requested state.
- If `aws` fails in the sandbox, rerun with network escalation.
