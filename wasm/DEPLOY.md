# Deploying pgrust.com

**There is no staging environment.** Production is one S3 bucket (`s3://pgrust`)
behind one CloudFront distribution (`E8737IN03F13`); any upload IS a production
publish. "Staging" is a local directory assembled by `deploy.sh stage` and
verified with headless Chrome on localhost. A production push requires
Michael's explicit go per release.

## 1. Build (release module only)

```sh
PGRUST_WASM_PROFILE=wasm-release wasm/wasm-build.sh   # postgres.wasm (~46 MB)
wasm/build.sh                                  # assets/ + .br/.gz variants
```

`deploy.sh stage` refuses a >100 MB `postgres.wasm` — the ~225 MB dev-profile
module stalls the worker (a known trap once misdiagnosed as a stdin bug).

## 2. Stage + verify locally

```sh
wasm/deploy.sh stage            # assembles wasm/deploy-staging/
wasm/deploy.sh verify           # gates the BUNDLE in headless Chrome:
                                          #   default URL (real psql), ?client=js,
                                          #   and the no-JSPI Safari fallback
```

The full pre-deploy gate on the source tree is `wasm/wasm-psql-web-e2e.sh`
(four legs; run it before staging).

## 3. Production push (needs Michael's go)

```sh
PGRUST_DEPLOY_CONFIRM=yes wasm/deploy.sh push wasm/deploy-staging --yes
```

What it does (the fabled AGENTS.md runbook, collapsed to the single-asset-leg
layout):
- `postgres.wasm`, `psql.wasm`, `vfs.img`: uploads the **brotli bytes** under
  the raw object name with `Content-Encoding: br`, `Cache-Control: public,
  max-age=86400`;
- `vfs.json`, `pgrust-hero.png`: raw, same cache policy;
- `index.html` + all site JS: raw, `Cache-Control: no-cache`;
- `aws cloudfront create-invalidation --distribution-id E8737IN03F13 --paths "/*"`.

`_serve-for-testing.mjs` and the `.br`/`.gz` sidecars are never uploaded as
objects of their own.

## 4. Post-push verification

```sh
curl -s https://pgrust.com/assets/postgres.wasm | brotli -d | wc -c   # = local raw size
```

macOS `curl --compressed` returning 0 bytes is a documented false alarm —
always fetch raw bytes and decompress yourself. Then load https://pgrust.com/
in Chrome (expect the real psql banner) and in Safari (expect the JS REPL with
the one-line JSPI fallback note).

## Hard rules

- IAM: `s3:ListAllMyBuckets` / `cloudfront:ListDistributions` are DENIED;
  per-object `cp` and `create-invalidation` are ALLOWED. Don't "test" with ls.
- Never serve with `Cross-Origin-Embedder-Policy: require-corp` (breaks the
  hosted analytics script).
- Never `wasm-opt` the module (binaryen re-encodes wasm-EH; JavaScriptCore
  rejects it).
- On main the site lives at `wasm/` (the old lane worktrees keep
  `tools/wasm-web/`) — run deploy.sh from the tree you intend to publish.
