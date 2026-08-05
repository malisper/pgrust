# pgrust wasm webapp

Runs the pgrust-fast single-user `postgres.wasm` module **in the browser**
(and in Node), over an in-memory VFS holding an `initdb`'d datadir. Type SQL
into an interactive `pgrust=#` terminal, get a psql-style table back —
PostgreSQL, reimplemented in Rust, compiled to WebAssembly, executing
client-side.

Lineage: this site is the pgrust.com demo ported from the original pgrust
(fabled) repo's `wasm/` (taken at fabled main `e9b8881283`). The
UI/REPL/format layers are carried over intact; the **host layer is new**: the
old build targeted `wasm32/64-unknown-unknown` and spoke a custom
`pgvfs::host_*` ABI (27 imports + 161 `env::*` trap stubs, `pgrust-harness.js`),
while pgrust-fast builds `postgres.wasm` for **wasm32-wasip1** with
panic=unwind lowered through standard Wasm exception handling — so the module
imports exactly the **33 `wasi_snapshot_preview1` functions** and nothing
else, implemented here in `pgrust-wasi.js`.

This is the browser counterpart to the `wasm/wasm-boot-e2e.sh` wasmtime
host: same argv/GUC/env contract, same C-initdb'd datadir, plus a packed
in-memory VFS instead of a preopened host directory.

## The site

`index.html` is the **pgrust interactive database site**: a full-viewport
terminal REPL with an accent-orange `pgrust=#` prompt, a sidebar of runnable
example queries (warm-up / data & json / recursive CTEs), an About + Roadmap
panel, and header/footer chrome. It is plain HTML/CSS/JS (no framework):

- `repl.js` — the terminal logic (banner, input + history, statement extraction
  on `;`, `pgrust-#` continuation prompts, psql-style meta-commands, and the
  table/raw/error/command/notice renderer). Mobile-first affordances are kept
  from the original: viewport meta, sidebar collapses behind a "Try example"
  toggle on narrow screens, focus management avoids popping the on-screen
  keyboard on touch devices.
- `backend.js` — bridges the documented `window.pgrust.setBackend({ exec })`
  hook to **our** wasm engine: each SQL statement the REPL extracts is run
  through the long-lived `worker.js`, and the raw single-user `printtup` stream
  is parsed into structured `{ columns, rows, aligns }` (numeric columns
  right-aligned) so the design's own renderer draws the table. Errors come from
  the backend's stderr; non-SELECT statements get a psql-style command tag.

Clicking an **example** types it into the session and runs it against the real
engine. State **persists** across statements/examples (one long-lived datadir)
until you press **reset**.

## Status

Verified in Node (v25), Chrome, and WebKit/JavaScriptCore. **One asset leg
serves every engine**: the wasm32-wasip1 module uses the standardized exnref
wasm-EH encoding plus baseline post-MVP features (bulk-memory, reference-types,
multivalue, nontrapping-fptoint, sign-ext, extended-const — **no SIMD, no
memory64, no threads**), all accepted by current Chrome, Firefox, and
Safari/iOS Safari. The old desktop-wasm64 / Safari-wasm32 dual-asset split is
gone, and the old wasm32 float8/Datum limitation is fixed (the ILP32 Datum
audit in the wasm boot lane), so float8 examples run everywhere.

- **Module size**: ~44 MB raw (cargo `wasm-release` profile), ~8.9 MB
  brotli-compressed for deploy; the packed VFS image is ~41 MB raw / ~1.9 MB
  brotli. First load streams + compiles; later loads reuse the worker's
  IndexedDB compiled-module cache.
- **Peak linear memory**: ~118 MiB per statement run (with the default
  `shared_buffers=32MB`; ~220 MiB at the initdb default 128MB) — inside iOS
  Safari budgets.
- **Do not `wasm-opt` the module**: binaryen (tested v130, `-Os
  --all-features`) re-encodes the EH instructions in a form V8 accepts but
  JavaScriptCore rejects (`throw_ref expected an exception reference`). The
  shipped asset is the rustc-emitted module.
- **Two engine modes** over the one asset (worker.js picks automatically;
  `?engineMode=wire|single` overrides):
  * **`wire` — protocol session** (default where the engine has JSPI:
    Chrome/Edge-class V8, Node ≥ 24): ONE long-lived `postgres --stdio-wire`
    instance serves a real pgwire session over the stdin/stdout host pipes
    (`wiresession.js` — the browser port of the `wasm/wasm-net-e2e.sh`
    wasmtime driver). The worker encodes Query frames and parses backend
    messages (`wire.js`); **session state persists across statements** —
    temp tables, prepared statements, and transactions span REPL lines. The
    blocking primitive is JSPI: the guest's between-statements blocking
    stdin read returns a Promise (fd_read wrapped in
    `WebAssembly.Suspending`, `_start` in `WebAssembly.promising`) and the
    guest suspends until the next frame arrives.
  * **`single` — per-statement** (the fallback; feature detection sends all
    WebKit/Safari incl. iOS here — no JSPI in WebKit as of mid-2026): each
    statement runs `postgres --single` to completion in a fresh module
    instance over the SHARED long-lived VFS, exactly the original model —
    datadir writes persist REPL-style, session state does not. pgrust-fast's
    `--single` exits through a completed shutdown checkpoint, so every next
    boot is a normal clean restart.
- **Persistence across page reloads** (the `persist` toggle): the worker
  snapshots the whole datadir VFS to the **Origin Private File System** after
  each completed statement (`snapshot.js`; wire mode runs `CHECKPOINT` first
  so a restore replays only the WAL tail — psql client mode keys off the
  server's `ReadyForQuery` instead, see below) and restores the newest valid
  snapshot at boot. Snapshot-class by design — sync access handles are
  worker-only and per-write OPFS I/O is not portable to WebKit; the
  two-slot + generation + CRC format means a torn write never eats the last
  good snapshot, a corrupt snapshot falls back to a fresh datadir, and a
  quota failure just turns persistence off with a note. **reset** clears the
  stored snapshots too.

### Two clients: the REAL psql (default) and the JS REPL (`?client=js`)

Independently of the engine mode, the terminal can be driven by **the real
Rust `psql`** (`crates/bin/psql`, the byte-identical-to-PGDG-psql-18 client)
compiled to the same `wasm32-wasip1` target and cross-piped to
`postgres --stdio-wire` **inside the page** — no server, no network:

- **default** (`psql`, wherever the engine has JSPI) — `psql.wasm` owns the
  terminal. The bytes you see are psql's own stdout: its banner, its
  `postgres=#` / `postgres(#` continuation prompts, its `\d`/`\dt`/`\l`/
  `\conninfo` output, its `ERROR:` + `LINE n:` caret blocks. `\c otherdb` is
  a REAL reconnect (psql sends Terminate; the session manager respawns a
  server instance on the SAME VFS, so database state persists across the
  reconnect).
- `?client=js` (opt-out) — the JS REPL: statement extraction in JS and the
  structured `{columns, rows}` renderer.
- **No-JSPI fallback**: client selection is feature-detected (`wiresession.js
  jspiSupported()` — `WebAssembly.Suspending`/`promising`, never UA
  sniffing). WebKit/Safari (no JSPI as of mid-2026) lands on the JS REPL
  automatically, with one subtle terminal note ("this browser has no JSPI, so
  the JS REPL emulates psql here") — not an error.

**Terminal fidelity** (matched against PGDG psql 18.4 driven over a real
PTY): in psql mode, input history is STATEMENT-grained like readline's — a
multi-line statement is ONE Up-arrow entry with its newlines preserved (the
input is a textarea for exactly this), backslash commands are their own
entries even at a continuation prompt, and a statement aborted mid-entry
(`\r` buffer reset) leaves no history entry. The JS REPL keeps its historical
line-grained behavior. Drag-selecting scrollback text keeps the selection
alive for Cmd/Ctrl-C (the click-to-refocus handler yields to a live
selection, and Ctrl-C with a selection is copy, not interrupt).

**Both toolbar buttons work in both clients.**

- `reset` — psql mode reuses the `\c` machinery against a PRISTINE VFS
  instead of the same one: the worker retires psql FIRST (closing its stdin is
  psql's normal EOF exit, whose `PQfinish` Terminate takes the server down
  through its shutdown checkpoint), only THEN swaps in a fresh datadir, and
  finally boots a new psql over it. Reversing that order would leave psql
  talking to a backend whose datadir changed underneath it. The page holds the
  new psql's output until it has cleared the scrollback, so what lands is the
  REAL psql's banner and prompt. No page reload — the compiled modules stay
  warm — and no orphaned instance: `PsqlSession.stop()` reports whether both
  guests actually exited, and the page says so when one had to be abandoned.
- `persist` — the OPFS snapshot mechanism is the SAME (it serializes the VFS
  psql's server instance is using). Only the trigger differs: with no
  statement boundaries visible to the worker, snapshots key off the server's
  `ReadyForQuery`. Nothing can tear — `serializeVfs()` is synchronous, so a
  snapshot is the datadir at one instant with the guest's writes in issue
  order, and restoring it is an ordinary crash recovery replaying the WAL
  (psql owns the connection, so there is no `CHECKPOINT` to inject; committed
  work is durable without one).

  **Loss window — closed by a durability gate**: when persist is on, the
  bytes carrying a `ReadyForQuery` are WITHHELD from psql until the snapshot
  covering that statement has flushed to OPFS (`PsqlSession`'s delivery
  chain; the js client gets the same contract by snapshotting before the
  worker posts a result). So a result can only appear on screen once the
  state that produced it is durable — "you saw it, it's durable". A reload
  the instant output paints cannot lose that statement; a reload BEFORE the
  output paints loses only a statement whose completion was never reported,
  the same contract as killing any database client mid-statement. The cost is
  ~100ms of prompt latency per statement while persist is on (the ~40 MB
  image write); persist off has no gate and no latency. `pagehide`/
  `visibility:hidden` additionally ask the worker for one final best-effort
  flush. The gate reloads immediately after an INSERT paints (20ms poll, no
  snapshot-note wait) and requires the row to survive.

Topology (`psqlsession.js`): psql fd 0/1/2 = the terminal, fd 4 = read from
server, fd 5 = write to server; fd 3 stays the WASI `/` preopen. Both guests
block on `fd_read` under a JSPI `Suspending` wrapper, so the two instances
suspend on each other inside one worker thread.

Verified in headless Chrome by `wasm/wasm-psql-web-e2e.sh` (four legs
PASS): the battery page (`test/psql-e2e.html`, incl. cross-database `\c`); the
real site page at its DEFAULT URL (`test/psql-site-shot.mjs` — keystrokes
through `repl.js`, a multi-line sidebar example over psql's continuation
prompts, four consecutive `reset`s each proven pristine, `persist` surviving a
page reload issued immediately after a write, and screenshots); the SAME
driver against the `?client=js` opt-out; and the Safari-fallback leg (JSPI
deleted from the page world → the default URL feature-detects its way to the
JS REPL and prints the fallback note). Ground truth without a
browser: `run-node-psql.mjs`, and the
fidelity gate `crates/bin/psql/gate/run-wasm-gate.sh` (wasm psql byte-identical
to native psql on the same corpus).

Known: with the **unoptimized dev-profile** `postgres.wasm` (~225 MB, as
opposed to the shipped `wasm-release` ~46 MB module) the battery stalls
mid-run in the browser while the same battery passes in Node — the parked
increment-3 symptom. It does not reproduce with the shipped asset, and
delaying every server→psql chunk by 50 ms (which forces psql to lose the
read/arrival race on every message) does not reproduce it either, so it is
not a pipe-readiness race in the wiring; treat it as a
resource/size property of that dev asset and always gate on the release module.

## Files

| File | Purpose |
|------|---------|
| `pgrust-wasi.js` | The host harness: in-memory VFS + the 33 `wasi_snapshot_preview1` imports the module declares + module run. Hand-rolled (no vendored WASI shim — the subset is small and the VFS/packed-image model already existed here). Protocol mode adds a streaming-stdin seam: with a `stdinStream`, `fd_read(0)` on an empty queue returns a Promise (the JSPI suspension point) and `poll_oneoff` answers stdin readiness truthfully so the guest's emulated-noblock probes stay honest. |
| `wire.js` | pgwire frame codec: frontend encoders (Startup/Query/Terminate), incremental backend-message reader, structured parser, and the canonicalization that must stay byte-identical to `scripts/pgwire_stdio_driver.py`. |
| `wiresession.js` | The long-lived `--stdio-wire` session: instantiates the module with `fd_read` Suspending-wrapped and `_start` promising-wrapped, pumps handshake/query/terminate cycles. |
| `snapshot.js` | Durable datadir snapshots: VFS serialize/restore (magic + generation + CRC32), two-slot torn-write-safe store logic, and the OPFS (`createSyncAccessHandle`) adapter. |
| `format.js` | Parses the single-user `printtup` debug output into a psql-style ASCII table. |
| `worker.js` | Web Worker that fetches the module + VFS and runs statements off the UI thread over ONE long-lived datadir VFS. Wire engine: one live protocol session; single engine: one `--single` run per statement. Owns the persist toggle mechanics (snapshot after each statement, restore at boot, corrupt/quota fallbacks). |
| `index.html` | The interactive-database site: full-viewport terminal + examples/About/Roadmap sidebar + header/footer. |
| `repl.js` | The terminal logic: banner, input + history, statement extraction, meta-commands, table/raw/error/command/notice rendering. |
| `backend.js` | Bridges the `window.pgrust.setBackend({ exec })` hook to our engine: routes each statement through `worker.js` and parses the raw `printtup` stream into structured `{ columns, rows, aligns }`. |
| `run-node.mjs` | Node CLI runner (ground-truth, no browser needed). |
| `serve.mjs` | Tiny static server (correct wasm MIME, precompressed assets, range requests). |
| `pack-vfs.mjs` | Packs a directory tree into `vfs.img` + `vfs.json`. |
| `build.sh` | Assembles the asset directory (wasm module + packed VFS; mints the datadir with C initdb when not given one). |
| `verify.html` | Headless-driver page: runs two queries through the real worker, writes VERIFY_PASS/FAIL to `document.title`. |
| `test/decode-utf8.test.mjs` | Node unit test (`node wasm/test/decode-utf8.test.mjs`): streaming UTF-8 chunk decoding round-trips multi-byte characters split across output chunks (public issue #34). |
| `psqlsession.js` | `?client=psql`: the real `psql.wasm` cross-piped to `postgres --stdio-wire` instance(s) over host pipes (fd 4/5), with the psql→server frame splitter that spots `Terminate` and respawns the server on the same VFS so `\c` is a real reconnect; the server→psql splitter that spots `ReadyForQuery` (the persist cadence); and `stop()`, the psql-first teardown behind `reset`. |
| `run-node-psql.mjs` | Node CLI harness for the two-instance psql topology (script mode, no browser) — the ground truth behind `crates/bin/psql/gate/run-wasm-gate.sh`. |
| `test/psql-e2e.html` + `test/run-browser-e2e.mjs` | Browser battery for `?client=psql`: the page decides its own verdict and POSTs it to the driver's listener (DOM polling races the async battery). |
| `test/psql-site-shot.mjs` | CDP driver for the REAL site page: types statements into the page's own input, clicks a multi-line sidebar example, clicks `reset` (four times, each proven to give a pristine DB) and `persist` (across a page reload issued immediately after a write), and writes PNG screenshots. `--client psql` (default; drives the bare URL), `--client js` (the opt-out), or `--nojspi` (Safari-fallback simulation). |
| `assets/` | Build artifacts (gitignored except `psql.wasm`): `postgres.wasm`, `vfs.img`, `vfs.json`, plus compressed variants; `psql.wasm` (~440 KB) IS committed. |

## How to run

### 1. Build the assets

The binary assets are **not committed**. Regenerate them:

```bash
# optimized module (native-inert cargo profile; ~44MB):
PGRUST_WASM_PROFILE=wasm-release wasm/wasm-build.sh
# pack module + C-initdb'd datadir + share tree into assets/:
wasm/build.sh
```

`build.sh` mints the datadir itself with the C PostgreSQL 18 `initdb` (probes
`/opt/homebrew/bin`; override with `PGINSTALL`, or supply `PGRUST_DATADIR` /
`PGRUST_SHARE` directly). The share tree is copied with `cp -RL` — homebrew's
timezone entries are absolute symlinks and the VFS has none.

### 2a. Node (ground truth — no browser)

```bash
echo "SELECT 1;" | node wasm/run-node.mjs
node wasm/run-node.mjs --sql "SELECT pi();"
node wasm/run-node.mjs --sql "SELECT 1" --raw   # raw single-user output
```

The repo gate `wasm/wasm-web-e2e.sh` drives this runner over the boot
lane's query battery and byte-compares the raw stream against the NATIVE
`postgres --single` on an identical fresh datadir.

### 2b. Browser

```bash
node wasm/serve.mjs 8080
# open http://localhost:8080/              # the REAL Rust psql drives the terminal (default)
# open http://localhost:8080/?client=js    # the legacy JS REPL (opt-out; also the Safari path)
```

Headless gates for the psql client (both legs in Chrome, second one also
writes a screenshot):

```bash
wasm/wasm-psql-web-e2e.sh                        # both legs
SHOT_OUT=/tmp/psql.png wasm/wasm-psql-web-e2e.sh # pick the screenshot path
```

Do not serve or deploy this page with `Cross-Origin-Embedder-Policy: require-corp`.
The app does not need cross-origin isolation, and COEP blocks PostHog's hosted
analytics script.

Type SQL at the `pgrust=#` prompt and press **Enter** (end a statement with `;`
to run it; an unterminated statement continues on a `pgrust-#` line). Or click
an **example** in the sidebar to type + run it. `\?` lists the psql-style
meta-commands; **clear ^L** clears the screen; **reset** restores a fresh
datadir.

## How it works (the import surface)

The module is a `wasm32-wasip1` build (pinned nightly, `-Zbuild-std` with
`panic=unwind` + `+exception-handling`, 64MiB shadow stack — see
`wasm/wasm-build.sh`). Its complete import surface is
`wasi_snapshot_preview1`:

- **fs**: `path_open`, `fd_close`, `fd_read`, `fd_write`, `fd_pread`,
  `fd_pwrite`, `fd_seek`, `fd_sync`, `fd_datasync`, `fd_filestat_get`,
  `fd_filestat_set_size`, `fd_fdstat_get`, `fd_fdstat_set_flags`,
  `fd_readdir`, `path_filestat_get`, `path_create_directory`,
  `path_remove_directory`, `path_unlink_file`, `path_rename`,
  `path_readlink` — all against the in-memory VFS, preopened as `/` (fd 3)
  with the datadir at `/pgdata` and the share tree at `/share`.
- **stdio**: fd 0 feeds the SQL statement text; fd 1/2 are captured for
  `format.js` (results) and diagnostics (ERROR/NOTICE lines).
- **process/env**: `args_get`/`args_sizes_get` supply
  `postgres --single -D /pgdata -c max_stack_depth=60000 -c io_method=sync
  -c autovacuum=off -c wal_sync_method=fdatasync -c shared_buffers=32MB
  postgres`; `environ_get` supplies `USER=postgres`,
  `PGRUST_TZDIR=/share/timezone`, `PGRUST_PGSHAREDIR=/share`,
  `PGRUST_RUNTIME=0`; `proc_exit` is raised as a typed JS exception carrying
  the exit code (the module's wasm-EH cleanup pads rethrow foreign exceptions,
  so it propagates out intact).
- **misc**: `clock_time_get`, `random_get`, `sched_yield`, `poll_oneoff`
  (clock subscriptions report as immediately fired — a COEP-less browser
  worker has no blocking primitive; `--single` never sleeps on this path),
  `sock_recv`/`sock_send` (NOTSUP; never called in `--single`).
