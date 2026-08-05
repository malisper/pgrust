#!/usr/bin/env bash
# deploy.sh — stage, verify, and (on explicit go) publish the pgrust.com site.
#
#   wasm/deploy.sh stage  [BUNDLE_DIR]   # assemble the deploy bundle
#   wasm/deploy.sh verify [BUNDLE_DIR]   # gate the BUNDLE in headless Chrome
#   wasm/deploy.sh push   [BUNDLE_DIR]   # UPLOAD to production (guarded)
#
# BUNDLE_DIR defaults to wasm/deploy-staging (untracked). There is NO
# hosted staging environment: production is s3://pgrust behind CloudFront
# E8737IN03F13, one bucket, one distribution — any upload IS a production
# publish. `stage` + `verify` are therefore fully local; `push` refuses to run
# unless PGRUST_DEPLOY_CONFIRM=yes AND --yes are both given (Michael's explicit
# go per release — see the never-push-public rule).
#
# What ships (and what must never ship):
#   index.html + the site's JS modules, verify.html, pgrust-hero.png,
#   assets/{postgres.wasm,psql.wasm,vfs.img,vfs.json}. Big assets upload as
#   their BROTLI bytes with Content-Encoding: br (CloudFront serves them
#   as-is); HTML/JS upload raw with no-cache so edits take effect on the next
#   invalidation. serve.mjs is copied into the bundle as _serve-for-testing.mjs
#   for local verification and is EXCLUDED from the upload, as are the raw
#   .br/.gz sidecars of anything uploaded pre-encoded.
#
# Deploy environment notes (from the fabled AGENTS.md runbook):
#   - IAM: s3:ListAllMyBuckets / cloudfront:ListDistributions are DENIED;
#     per-object cp and create-invalidation are ALLOWED. Do not "test" with ls.
#   - Verify brotli objects by fetching raw bytes and `brotli -d`; macOS
#     `curl --compressed` returning 0 bytes is a known false alarm.
#   - Never serve with Cross-Origin-Embedder-Policy: require-corp.
#   - Never wasm-opt the module (binaryen re-encodes wasm-EH; JSC rejects it).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CMD="${1:-}"
BUNDLE="${2:-$HERE/deploy-staging}"
BUCKET="s3://pgrust"
DISTRIBUTION="E8737IN03F13"

PAGE_FILES=(index.html repl.js backend.js format.js worker.js wire.js
            wiresession.js snapshot.js psqlsession.js pgrust-wasi.js
            verify.html)
ASSET_FILES=(postgres.wasm psql.wasm vfs.img vfs.json pgrust-hero.png)

stage() {
  for f in "${PAGE_FILES[@]}"; do
    [[ -f "$HERE/$f" ]] || { echo "deploy: missing $f" >&2; exit 2; }
  done
  for f in "${ASSET_FILES[@]}"; do
    [[ -f "$HERE/assets/$f" ]] || { echo "deploy: missing assets/$f — run build.sh" >&2; exit 2; }
  done
  # Refuse to ship a dev-profile module (the ~225 MB trap: it stalls the
  # worker and reads as a stdin-readiness bug). The release module is ~46 MB.
  local wasm_bytes
  wasm_bytes=$(wc -c < "$HERE/assets/postgres.wasm")
  if (( wasm_bytes > 100000000 )); then
    echo "deploy: assets/postgres.wasm is ${wasm_bytes} bytes — that is the DEV module; build wasm-release" >&2
    exit 2
  fi

  # Assemble aside and swap in two quick steps, so a server already running
  # out of $BUNDLE (a live localhost testing session) never sees a
  # half-copied tree — only the old bundle or the new one.
  local staging="$BUNDLE.new.$$"
  rm -rf "$staging"
  mkdir -p "$staging/assets"
  for f in "${PAGE_FILES[@]}"; do cp "$HERE/$f" "$staging/$f"; done
  for f in "${ASSET_FILES[@]}"; do
    cp "$HERE/assets/$f" "$staging/assets/$f"
    for ext in br gz; do
      [[ -f "$HERE/assets/$f.$ext" ]] && cp "$HERE/assets/$f.$ext" "$staging/assets/$f.$ext"
    done
  done
  # Local-verification server only — the leading underscore marks it
  # not-content; push() excludes it explicitly as well.
  cp "$HERE/serve.mjs" "$staging/_serve-for-testing.mjs"
  rm -rf "$BUNDLE"
  mv "$staging" "$BUNDLE"
  echo "deploy: staged $(du -sh "$BUNDLE" | cut -f1) at $BUNDLE"
  echo "deploy: next — $0 verify $BUNDLE"
}

verify() {
  [[ -d "$BUNDLE" ]] || { echo "deploy: no bundle at $BUNDLE — run stage first" >&2; exit 2; }
  local S="${SHOT_DIR:-${TMPDIR:-/tmp}}"
  echo "=== bundle gate: default client (real psql) ==="
  node "$HERE/test/psql-site-shot.mjs" --root "$BUNDLE" --port "${PORT_BASE:-8791}" \
       --timeout 240 --out "$S/deploy-psql.png" --client psql
  echo
  echo "=== bundle gate: ?client=js opt-out ==="
  node "$HERE/test/psql-site-shot.mjs" --root "$BUNDLE" --port "$(( ${PORT_BASE:-8791} + 2 ))" \
       --timeout 240 --out "$S/deploy-jsrepl.png" --client js
  echo
  echo "=== bundle gate: Safari fallback (no JSPI) ==="
  node "$HERE/test/psql-site-shot.mjs" --root "$BUNDLE" --port "$(( ${PORT_BASE:-8791} + 4 ))" \
       --timeout 240 --out "$S/deploy-nojspi.png" --nojspi
  echo
  echo "deploy: BUNDLE VERIFIED (screenshots under $S/deploy-*.png)"
}

push() {
  local flag="${1:-}"
  [[ -d "$BUNDLE" ]] || { echo "deploy: no bundle at $BUNDLE — run stage first" >&2; exit 2; }
  if [[ "${PGRUST_DEPLOY_CONFIRM:-}" != "yes" || "$flag" != "--yes" ]]; then
    cat >&2 <<'EOF'
deploy: REFUSING to push. This publishes to PRODUCTION pgrust.com (there is no
staging bucket). Production deploys need Michael's explicit go, then:

  PGRUST_DEPLOY_CONFIRM=yes wasm/deploy.sh push [BUNDLE_DIR] --yes
EOF
    exit 3
  fi
  command -v aws >/dev/null || { echo "deploy: no aws CLI" >&2; exit 2; }

  # 1. Big assets: upload the BROTLI bytes under the raw object name with
  #    Content-Encoding: br (CloudFront serves the header through). Cacheable
  #    for a day; the invalidation below handles same-day re-deploys.
  for f in postgres.wasm psql.wasm vfs.img; do
    local_src="$BUNDLE/assets/$f.br"
    [[ -f "$local_src" ]] || { echo "deploy: missing $f.br (run build.sh with compression)" >&2; exit 2; }
    ctype=application/octet-stream; [[ "$f" == *.wasm ]] && ctype=application/wasm
    aws s3 cp "$local_src" "$BUCKET/assets/$f" \
      --content-encoding br --content-type "$ctype" \
      --cache-control "public, max-age=86400"
  done
  aws s3 cp "$BUNDLE/assets/vfs.json" "$BUCKET/assets/vfs.json" \
    --content-type "application/json; charset=utf-8" --cache-control "public, max-age=86400"
  aws s3 cp "$BUNDLE/assets/pgrust-hero.png" "$BUCKET/assets/pgrust-hero.png" \
    --content-type image/png --cache-control "public, max-age=86400"

  # 2. HTML/JS: raw, no-cache, so the next invalidation takes effect at once.
  for f in "${PAGE_FILES[@]}"; do
    ctype='text/javascript; charset=utf-8'; [[ "$f" == *.html ]] && ctype='text/html; charset=utf-8'
    aws s3 cp "$BUNDLE/$f" "$BUCKET/$f" --content-type "$ctype" --cache-control no-cache
  done

  # 3. Invalidate everything (assets changed too).
  aws cloudfront create-invalidation --distribution-id "$DISTRIBUTION" --paths "/*"
  echo "deploy: PUSHED to $BUCKET + invalidated $DISTRIBUTION"
  echo "deploy: verify with: curl -s https://pgrust.com/assets/postgres.wasm | brotli -d | wc -c"
}

case "$CMD" in
  stage)  stage ;;
  verify) verify ;;
  push)   push "${3:-}" ;;
  *) sed -n '2,14p' "$0"; exit 2 ;;
esac
