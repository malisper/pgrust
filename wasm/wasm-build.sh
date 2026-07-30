#!/usr/bin/env bash
# P5 WS-TOOLCHAIN blocking gate (wasm/p5-toolchain): the enumerated workspace
# subset compiles to wasm32-wasip1 WITH UNWINDS ENABLED (panic=unwind lowered
# through Wasm exception handling), and the catch_unwind smoke actually
# catches under wasmtime's exceptions proposal.
#
# Exclusion ledger: wasm/wasm-crate-ledger.md (ratchet-only — it may
# only shrink). Every workspace member is either BUILT here or LISTED there;
# a member that is neither fails this gate loudly (no silent drops: the
# include set is computed as members-minus-ledger, so an unledgered breakage
# breaks the build).
#
# Toolchain pin lives ONLY here (the repo's rust-toolchain.toml and native
# profiles are untouched): nightly-2026-07-17 is the first validated nightly
# whose wasm-EH linkage resolves the `__cpp_exception` tag — the 2026-04-26
# nightly compiles but FAILS TO LINK panic=unwind wasm (undefined
# __cpp_exception from every throwing object incl. libstd/libunwind).
#
# Usage:
#   wasm/wasm-build.sh              # full gate: crate subset + smoke build
#   PGRUST_WASM_RUN_SMOKE=1 wasm/wasm-build.sh   # also RUN smoke (needs wasmtime >= 46 w/ exceptions)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

TOOLCHAIN="${PGRUST_WASM_TOOLCHAIN:-nightly-2026-07-17}"
TARGET=wasm32-wasip1
LEDGER="$ROOT/wasm/wasm-crate-ledger.md"

# re2 is a build.rs probe; force the stub engine deterministically on wasm.
export PGRUST_FORCE_NO_RE2=1
# panic=unwind + Wasm EH codegen for every unit, including build-std units.
# 64MiB shadow stack: dev-profile frames are huge and the boot harness pins
# max_stack_depth=60000kB (matching the native e2e); the 1MiB link default
# turns legitimate executor recursion into "stack depth limit exceeded".
# Any PGRUST_WASM_RUSTFLAGS override MUST carry all three flags.
export RUSTFLAGS="${PGRUST_WASM_RUSTFLAGS:--C panic=unwind -C target-feature=+exception-handling -C link-arg=-zstack-size=67108864}"

if ! rustup toolchain list | grep -q "^${TOOLCHAIN}"; then
    echo "wasm-build: installing pinned toolchain ${TOOLCHAIN}" >&2
    rustup toolchain install "${TOOLCHAIN}" >/dev/null
fi
rustup component add --toolchain "${TOOLCHAIN}" rust-src >/dev/null 2>&1 || true
# build-std rebuilds std, but the SELF-CONTAINED sysroot objects (crt1,
# wasi-libc, libunwind) ship in the target's rust-std component — linking
# fails without it.
rustup target add --toolchain "${TOOLCHAIN}" "${TARGET}" >/dev/null 2>&1 || true

[ -f "$LEDGER" ] || { echo "wasm-build: FAIL — ledger missing: $LEDGER" >&2; exit 1; }

# Excluded crate names: first column of the ledger's `| crate | ... |` rows.
EXCLUDED=$(awk -F'|' '/^\|/ {gsub(/[[:space:]]/,"",$2); if ($2 != "" && $2 != "crate" && $2 !~ /^-+$/) print $2}' "$LEDGER" | sort -u)

MEMBERS=$(cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import json,sys
for p in json.load(sys.stdin)["packages"]: print(p["name"])' | sort -u)

# Ledger staleness: every excluded name must still be a workspace member.
STALE=$(comm -23 <(echo "$EXCLUDED") <(echo "$MEMBERS") || true)
if [ -n "$STALE" ]; then
    echo "wasm-build: FAIL — ledger lists non-members (stale rows):" >&2
    echo "$STALE" >&2
    exit 1
fi

INCLUDE=$(comm -23 <(echo "$MEMBERS") <(echo "$EXCLUDED"))
N_MEMBERS=$(echo "$MEMBERS" | wc -l | tr -d ' ')
N_EXCLUDED=$(echo "$EXCLUDED" | wc -l | tr -d ' ')
N_INCLUDE=$(echo "$INCLUDE" | wc -l | tr -d ' ')
echo "wasm-build: ${N_INCLUDE}/${N_MEMBERS} workspace crates in the wasm set (${N_EXCLUDED} ledgered out; ratchet-only)"

PKG_ARGS=""
for p in $INCLUDE; do PKG_ARGS="$PKG_ARGS -p $p"; done

# shellcheck disable=SC2086
cargo +"${TOOLCHAIN}" check --target "$TARGET" -Zbuild-std=std,panic_unwind $PKG_ARGS

echo "wasm-build: crate-subset compile OK (panic=unwind, +exception-handling)"

# Codegen + LINK leg (the F1 remedy): the postgres binary must actually link
# for wasip1 — cargo check proves neither monomorphization-time const evals
# nor linkage. Skippable for quick iterations with PGRUST_WASM_SKIP_LINK=1.
#
# PGRUST_WASM_PROFILE selects the cargo profile for this leg. The default dev
# profile is the gate's fast path; PGRUST_WASM_PROFILE=wasm-release builds the
# optimized module the web demo (wasm) ships — ~44MB vs the ~217MB
# dev binary (the profile is native-inert: nothing native selects it).
PROFILE="${PGRUST_WASM_PROFILE:-dev}"
case "$PROFILE" in
    dev) PROFILE_DIR=debug ;;
    *)   PROFILE_DIR="$PROFILE" ;;
esac
if [ "${PGRUST_WASM_SKIP_LINK:-0}" != "1" ]; then
    cargo +"${TOOLCHAIN}" build --target "$TARGET" -Zbuild-std=std,panic_unwind -p main_main --bin postgres --profile "$PROFILE"
    BIN_WASM="$ROOT/target/${TARGET}/${PROFILE_DIR}/postgres.wasm"
    [ -f "$BIN_WASM" ] || { echo "wasm-build: FAIL — postgres.wasm not produced" >&2; exit 1; }
    echo "wasm-build: postgres.wasm linked ($(du -h "$BIN_WASM" | cut -f1), profile $PROFILE)"
else
    echo "wasm-build: bin link SKIPPED (PGRUST_WASM_SKIP_LINK=1)"
fi

# Toolchain-validation smoke: catch_unwind must CATCH under a Wasm
# exception-handling runtime, proving the unwind story is real.
(
    cd "$ROOT/wasm/wasm-unwind-smoke"
    cargo +"${TOOLCHAIN}" build --target "$TARGET" -Zbuild-std=std,panic_unwind --release
)
SMOKE_WASM="$ROOT/wasm/wasm-unwind-smoke/target/${TARGET}/release/wasm-unwind-smoke.wasm"
[ -f "$SMOKE_WASM" ] || { echo "wasm-build: FAIL — smoke wasm not produced" >&2; exit 1; }

if [ "${PGRUST_WASM_RUN_SMOKE:-0}" = "1" ]; then
    command -v wasmtime >/dev/null || { echo "wasm-build: FAIL — PGRUST_WASM_RUN_SMOKE=1 but wasmtime not installed" >&2; exit 1; }
    OUT=$(wasmtime run -W exceptions=y "$SMOKE_WASM")
    echo "$OUT"
    echo "$OUT" | grep -q "VERDICT: unwind-smoke PASS" || { echo "VERDICT: wasm-build FAIL (unwind smoke did not catch)" >&2; exit 1; }
else
    echo "wasm-build: smoke built (set PGRUST_WASM_RUN_SMOKE=1 with wasmtime installed to execute it)"
fi

echo "VERDICT: wasm-build PASS (${N_INCLUDE}/${N_MEMBERS} crates @ ${TOOLCHAIN}, panic=unwind)"
