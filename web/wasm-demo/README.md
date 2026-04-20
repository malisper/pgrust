# pgrust wasm demo

The thing at [pgrust.com](https://pgrust.com). A browser-based SQL REPL that runs pgrust compiled to WebAssembly — no server, no backend, just Rust shipped to the browser.

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

- Engine calls `Database::open_ephemeral()` — everything's in memory. Reload / "Reset Database" wipes it.
- The demo JS splits input on top-level semicolons (quoting-aware) and feeds one statement at a time to `WasmEngine::execute(sql)`.
- Browser API: `WasmEngine.new(poolSize?)`, `execute(sql)`, `reset(poolSize?)`.

## Production site

The live demo is a static S3 bucket fronted by CloudFront. `web/wasm-demo/` is the publish root. Analytics via PostHog (key baked into `index.html`).

## Deploy

The deploy script reads all target info from environment variables:

```bash
PGRUST_DEMO_BUCKET=your-bucket \
PGRUST_CLOUDFRONT_ID=EXXXXXXXXXXXXX \
AWS_PROFILE=your-profile \
./web/wasm-demo/deploy.sh
```

That script:

1. Runs `build.sh` to rebuild the wasm bundle.
2. Syncs `web/wasm-demo/` → `s3://$PGRUST_DEMO_BUCKET/` with `--delete` so removed files drop off the live site.
3. Re-uploads any `.wasm` file with `Content-Type: application/wasm` (critical — browsers block `WebAssembly.compileStreaming` without it).
4. Invalidates the CloudFront distribution so edge caches pick up the new build.

## Adding files to the site

Drop files into `web/wasm-demo/` and `./deploy.sh` picks them up. Stuff that already works by convention:

- `robots.txt`, `sitemap.xml` — root-level static files
- static assets in subfolders — reference with absolute paths (`/foo.svg`)
- favicon — currently an inline SVG data URL in `index.html`; replace with a real file + `<link rel="icon" href="/favicon.svg">` if you make one

Files that shouldn't be deployed (`build.sh`, `deploy.sh`, `*.rs`, `target/`, `.DS_Store`) are already excluded.

## Verifying a deploy

```bash
curl -sI "${PGRUST_DEMO_URL:?}" | head -5                              # 2xx via CloudFront
curl -sI "${PGRUST_DEMO_URL:?}/pkg/pgrust_bg.wasm" | grep -i content-  # application/wasm
```

Then open the site in a browser and run a query end-to-end.

## Troubleshooting

**Wasm won't load / "Incorrect response MIME type"**
The wasm file is missing `Content-Type: application/wasm`. Re-run `./deploy.sh` — the second pass explicitly sets the type. If you manually `aws s3 cp` a wasm file, pass `--content-type application/wasm`.

**Edge still serving old content**
CloudFront invalidation ran but browser cache is stale. Hard reload (`Cmd+Shift+R`) or check in an incognito window.

**`deploy.sh` aborts with "set PGRUST_DEMO_BUCKET" or "set PGRUST_CLOUDFRONT_ID"**
Those are required. The script refuses to run without them so you don't accidentally deploy to the wrong place.

## Migrating off AWS later

If this ever moves to Cloudflare Pages / Vercel:

1. Create the new project pointing at this repo (build command: `./web/wasm-demo/build.sh`, publish dir: `web/wasm-demo/`).
2. Flip DNS to point at the new host.
3. `deploy.sh` becomes obsolete — git push = deploy.

No lock-in in the current setup: no Lambda@Edge, no signed URLs, no AWS-specific features beyond a plain static CDN.
