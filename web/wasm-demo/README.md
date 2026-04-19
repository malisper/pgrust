# pgrust wasm demo

The thing at [pgrust.com](https://pgrust.com). A browser-based sql repl that runs pgrust compiled to webassembly — no server, no backend, just rust shipped to the browser.

## Local dev

Install once:

```bash
rustup target add wasm32-unknown-unknown
cargo install -f wasm-bindgen-cli --version 0.2.117   # must match crate version
```

Build:

```bash
./web/wasm-demo/build.sh
```

Outputs `web/wasm-demo/pkg/pgrust.js` and `web/wasm-demo/pkg/pgrust_bg.wasm`.

Run:

```bash
python3 -m http.server 8000
# then open http://localhost:8000/web/wasm-demo/
```

## How it works

- Engine calls `Database::open_ephemeral()` — everything's in memory. Reload / `Reset Database` wipes it.
- The demo js splits input on top-level semicolons (quoting-aware) and feeds one statement at a time to `WasmEngine::execute(sql)`.
- Browser API: `WasmEngine.new(poolSize?)`, `execute(sql)`, `reset(poolSize?)`.

## Production site

```
https://www.pgrust.com  →  CloudFront Function 301 redirect  →  https://pgrust.com
https://pgrust.com      →  Route 53 ALIAS  →  CloudFront  →  (OAC)  →  s3://pgrust
```

- **Bucket:** `pgrust` in `us-west-2`. Private — CloudFront reads via Origin Access Control.
- **CDN:** CloudFront, TLS via ACM (cert in `us-east-1`), HTTP→HTTPS redirect, www→apex redirect.
- **DNS:** Route 53 zone `pgrust.com` (id `Z0795405IN00H8WFZEZA`), nameservers handled at Amazon Registrar automatically.
- **Analytics:** PostHog, already wired in `index.html`. Fires automatically in prod.

All of the above is managed in `pgrust/domains/` as terraform. See `pgrust/domains/README.md` for one-time setup and how to change it.

## Deploy

Push the current `web/wasm-demo/` tree to production:

```bash
./web/wasm-demo/deploy.sh
```

That script:

1. Runs `build.sh` to rebuild the wasm bundle.
2. Syncs `web/wasm-demo/` → `s3://pgrust/` with `--delete` so removed files drop off the live site.
3. Re-uploads any `.wasm` file with `Content-Type: application/wasm` (critical — browsers block `WebAssembly.compileStreaming` without it).
4. Invalidates the CloudFront distribution so edge caches pick up the new build.

Requires `AWS_PROFILE=mfa` to be set (the script does this internally — just make sure the profile exists with an active MFA session).

First-time CloudFront invalidations are free. After 1000/month they cost $0.005 each, which at normal deploy cadence is $0.

## Adding files to the site

Drop files into `web/wasm-demo/` and `./deploy.sh` picks them up. Stuff that already works by convention:

- `robots.txt`, `sitemap.xml` — root-level static files
- static assets in subfolders — reference with absolute paths (`/foo.svg`)
- favicon — currently an inline SVG data URL in `index.html`; replace with a real file + `<link rel="icon" href="/favicon.svg">` if you make one

Files that shouldn't be deployed (`build.sh`, `deploy.sh`, `*.rs`, `target/`, `.DS_Store`) are already excluded.

## Verifying a deploy

```bash
curl -sI https://pgrust.com | head -5                              # should be 2xx via CloudFront
curl -sI https://www.pgrust.com | grep -i location                 # 301 → https://pgrust.com/
curl -sI https://pgrust.com/pkg/pgrust_bg.wasm | grep -i content-  # application/wasm
```

Then open the site in a browser and run a query end-to-end.

## Troubleshooting

**Wasm won't load / "Incorrect response MIME type"**
The wasm file is missing `Content-Type: application/wasm`. Re-run `./deploy.sh` — the second pass explicitly sets the type. If you manually `aws s3 cp` a wasm file, pass `--content-type application/wasm`.

**Edge still serving old content**
CloudFront invalidation ran but browser cache is stale. Hard reload (`Cmd+Shift+R`) or check in an incognito window.

**DNS not resolving after infra changes**
Negative caching on resolvers (1.1.1.1 / 8.8.8.8) can hold NXDOMAIN up to 15 min. `dig @ns-657.awsdns-18.net pgrust.com A +short` queries authoritatively and bypasses the cache.

**`deploy.sh` says "could not determine CloudFront distribution id"**
You're running from a tree where `pgrust/domains/` terraform state isn't accessible, or `AWS_PROFILE=mfa` isn't set. Either fix the environment or set `PGRUST_CLOUDFRONT_ID` explicitly.

## Migrating off AWS later

If this ever moves to Cloudflare Pages / Vercel:

1. Create the new project pointing at this repo (build command: `./web/wasm-demo/build.sh`, publish dir: `web/wasm-demo/`).
2. Edit `pgrust/domains/main.tf` — swap the CloudFront + ACM + S3 resources for CNAME records pointing at the new host.
3. `terraform apply`; DNS flips; tear down old CloudFront / ACM / bucket.
4. `deploy.sh` becomes obsolete — git push = deploy.

No lock-in in the current setup: no Lambda@Edge, no signed URLs, no AWS-specific features beyond a plain static CDN.
