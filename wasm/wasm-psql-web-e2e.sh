#!/usr/bin/env bash
# wasm-psql-web-e2e.sh — browser gate for the REAL Rust psql (psql.wasm)
# driving postgres.wasm in a page worker (increment 3 of the Rust-psql plan).
#
# Four legs, all in headless Chrome against wasm/assets:
#   1. test/run-browser-e2e.mjs  — the listener-verdict battery page
#      (multi-database \c reconnect, meta-commands, error path)
#   2. test/psql-site-shot.mjs   — the REAL site page at its DEFAULT URL
#      (index.html — real psql is the default client now): keystrokes through
#      repl.js -> worker -> psql.wasm, a multi-line sidebar example (psql
#      continuation prompts), the toolbar's RESET (pristine datadir with no
#      page reload, four times in a row) and PERSIST (OPFS snapshot surviving
#      a page reload issued IMMEDIATELY after a write), plus PNG screenshots
#   3. the same driver against ?client=js (the JS REPL opt-out) — an
#      unregression check that it still boots, resets and persists
#   4. the Safari-fallback leg: JSPI deleted from the page world before any
#      script runs; the DEFAULT URL must feature-detect its way to the JS
#      REPL and print the subtle fallback note
#
# Requires: assets built (wasm/build.sh, incl. assets/psql.wasm),
# Chrome (override with CHROME=...), Node >= 24 (JSPI).
#
# Env: SHOT_OUT / JSREPL_OUT / NOJSPI_OUT (screenshot paths), PORT_BASE (default 8093).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
WEB="$HERE"
PORT_BASE="${PORT_BASE:-8093}"
SHOT_OUT="${SHOT_OUT:-${TMPDIR:-/tmp}/psql-site.png}"
JSREPL_OUT="${JSREPL_OUT:-${TMPDIR:-/tmp}/jsrepl.png}"
NOJSPI_OUT="${NOJSPI_OUT:-${TMPDIR:-/tmp}/nojspi-fallback.png}"

for f in postgres.wasm vfs.img vfs.json psql.wasm; do
  [[ -f "$WEB/assets/$f" ]] || { echo "wasm-psql-web-e2e: missing assets/$f — run wasm/build.sh" >&2; exit 2; }
done

echo "=== leg 1: psql battery page (test/psql-e2e.html) ==="
node "$WEB/test/run-browser-e2e.mjs" --port "$PORT_BASE" --timeout 240

echo
echo "=== leg 2: the site page, DEFAULT client = real psql (index.html) ==="
node "$WEB/test/psql-site-shot.mjs" --port "$((PORT_BASE + 2))" --timeout 240 --out "$SHOT_OUT" --client psql

echo
echo "=== leg 3: the site page, JS REPL opt-out (index.html?client=js) ==="
node "$WEB/test/psql-site-shot.mjs" --port "$((PORT_BASE + 4))" --timeout 240 --out "$JSREPL_OUT" --client js

echo
echo "=== leg 4: Safari-fallback (default URL, JSPI removed) ==="
node "$WEB/test/psql-site-shot.mjs" --port "$((PORT_BASE + 6))" --timeout 240 --out "$NOJSPI_OUT" --nojspi

echo
echo "wasm-psql-web-e2e: ALL FOUR LEGS PASS"
echo "  psql-client screenshots: ${SHOT_OUT%.png}-*.png / $SHOT_OUT"
echo "  js-client screenshots:   ${JSREPL_OUT%.png}-*.png / $JSREPL_OUT"
echo "  nojspi screenshot:       $NOJSPI_OUT"
