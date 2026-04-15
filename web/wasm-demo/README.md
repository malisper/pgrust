# pgrust wasm demo

## Prerequisites

Work from the repo root:

```bash
cd provo
```

Install the wasm target if needed:

```bash
rustup target add wasm32-unknown-unknown
```

Install the wasm bindgen CLI once. It must match the crate version in this repo.
At the moment that is `0.2.117`:

```bash
cargo install -f wasm-bindgen-cli --version 0.2.117
```

## Build

Build the browser package:

```bash
./web/wasm-demo/build.sh
```

That generates:

- `web/wasm-demo/pkg/pgrust.js`
- `web/wasm-demo/pkg/pgrust_bg.wasm`

## Run

Serve the repo root with any static file server. The demo expects to be loaded
from the repository tree so the generated `pkg/` files resolve correctly.

For example:

```bash
python3 -m http.server 8000
```

Then open:

```text
http://localhost:8000/web/wasm-demo/
```

## Notes

- The engine uses `Database::open_ephemeral()`, so all data is in-memory.
- Press `Reset Database` or reload the page to start from a clean database.
- The textarea runs one SQL statement at a time under the hood by splitting on
  top-level semicolons in the demo JS.
- The browser API currently exposes `WasmEngine` with:
  - `new(poolSize?)`
  - `execute(sql)`
  - `reset(poolSize?)`
