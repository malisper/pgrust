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
# PGRUST_WASM_TARGET selects the wasm target. Default wasm32-wasip1 (the
# gate's target; unchanged). wasm32-wasip1-threads is the SPIKE arm
# (spike/wasip1-threads): wasi-libc pthreads over a SHARED imported memory,
# so the guest's `wasi` `thread-spawn` import is the host's job and every
# instance must be handed the same WebAssembly.Memory.
TARGET="${PGRUST_WASM_TARGET:-wasm32-wasip1}"
LEDGER="$ROOT/wasm/wasm-crate-ledger.md"

# PGRUST_WASM_FEATURES selects WHICH SUBSYSTEMS THE BINARY LINKS. The names are
# main_main's features, which forward to seams_init's (crates/_support/seams_init
# Cargo.toml has the full list and the reasoning). `full` is the default and is
# the build that existed before the gates: every contrib, every subsystem group.
# `browser` is the web-demo profile -- every subsystem group still on, but only
# the four contribs the demo uses. The link leg below always passes
# --no-default-features, so this variable is the ONLY thing that decides the set.
FEATURES="${PGRUST_WASM_FEATURES:-full}"

# re2 is a build.rs probe; force the stub engine deterministically on wasm.
export PGRUST_FORCE_NO_RE2=1

# The MAIN shadow stack (-zstack-size), which is a different thing per target.
#
# wasm32-wasip1: the guest's session runs ON the main stack (`--single`,
# `--stdio-wire`), so it needs the whole budget the boot harness pins with
# max_stack_depth=60000kB (matching the native e2e). Dev-profile frames are
# huge and the 1MiB link default turns legitimate executor recursion into
# "stack depth limit exceeded". 64MiB.
#
# wasm32-wasip1-threads: nothing that runs SQL is on the main stack. A wire
# session gets its own 64MiB thread (tcop/postgres/src/stdio_wire.rs) and every
# postmaster child gets child_thread_stack_size(); the main thread only boots
# the process and then runs the postmaster loop. On this target the stack is
# not address space either — it is bytes of the ONE shared linear memory, sat
# on for the life of the instance — so 16MiB, which the postmaster lane, both
# tablespace proofs and the browser Speedtest/Concurrency Suites all run under.
if [ "$TARGET" = "wasm32-wasip1-threads" ]; then
    DEFAULT_STACK_SIZE=16777216
else
    DEFAULT_STACK_SIZE=67108864
fi
STACK_SIZE="${PGRUST_WASM_STACK_SIZE:-$DEFAULT_STACK_SIZE}"

# panic=unwind + Wasm EH codegen for every unit, including build-std units.
# Any PGRUST_WASM_RUSTFLAGS override MUST carry all three flags.
export RUSTFLAGS="${PGRUST_WASM_RUSTFLAGS:--C panic=unwind -C target-feature=+exception-handling -C link-arg=-zstack-size=${STACK_SIZE}}"

# wasm32-wasip1-threads pins --import-memory --shared-memory in its target
# spec, so the MODULE's declared memory limits must match the limits of the
# WebAssembly.Memory the JS host creates and passes in as an import. Pin them
# here rather than leaving wasm-ld's derived initial size to drift: 256MiB
# initial (static data plus the main shadow stack above must fit under it, and
# a smaller claim than 256MiB has been measured as a boot that either traps on
# an out-of-bounds access or takes the renderer down while it grows) and the
# 4GiB wasm32 ceiling as the maximum. Both are 64KiB multiples.
if [ "$TARGET" = "wasm32-wasip1-threads" ]; then
    RUSTFLAGS="$RUSTFLAGS -C link-arg=--initial-memory=${PGRUST_WASM_INITIAL_MEMORY:-268435456} -C link-arg=--max-memory=${PGRUST_WASM_MAX_MEMORY:-4294967296}"
    export RUSTFLAGS
fi

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
# --------------------------------------------------------------------------
# Exclusion proof. A feature that is merely *declared off* proves nothing: the
# crate can still be in the module because some OTHER crate in main_main's graph
# depends on it (contribs that core's index machinery pulls in directly, for
# instance). So before the link, ask cargo what it actually resolved:
#
#   * which seams_init features this profile turned ON (cargo tree -f '{f}'),
#   * which of seams_init's optional dependencies those features enable,
#   * which crates are in the link graph at all (cargo tree, forward).
#
# Optional deps the profile does NOT enable and that are NOT in the graph are
# the real exclusions -- the N in the line this prints. Ones the profile does
# not enable but that are in the graph anyway are reported as "still linked"
# with the crate that pulls them: declared, not removed, not fought (the gate
# still stops seams_init installing their seams). Then the hard assertion: no
# crate counted as excluded may appear in the graph. `cargo tree` needs no
# -Zbuild-std -- it resolves, it does not compile.
prove_feature_exclusion() {
    if [ "$FEATURES" = "full" ]; then
        echo "wasm-build: profile full excludes 0 crates (every gate on; nothing to prove)"
        return 0
    fi

    local tree graph enabled
    tree=$(cargo +"${TOOLCHAIN}" tree --target "$TARGET" -p main_main \
        --no-default-features --features "$FEATURES" -e normal --prefix none -f '{p}|{f}')
    graph=$(printf '%s\n' "$tree" | awk '{print $1}' | LC_ALL=C sort -u)
    enabled=$(printf '%s\n' "$tree" | awk -F'|' '/^seams_init / && !seen {e=$2; seen=1} END {print e}')
    [ -n "$enabled" ] || { echo "wasm-build: FAIL - seams_init not in the graph for features '$FEATURES'" >&2; exit 1; }

    # Optional deps of seams_init, as PACKAGE names (the aliases in the manifest
    # are not always the package name: contrib_cube is the `cube` package), split
    # into the ones "$enabled" turns on and the ones it does not.
    local off
    off=$(SEAMS_ENABLED="$enabled" python3 - "$ROOT/crates/_support/seams_init/Cargo.toml" <<'PYEOF'
import os, re, sys
manifest = sys.argv[1]
root = os.path.dirname(manifest)
text = open(manifest).read()
optional, feats, section = {}, {}, None
for line in text.split("\n"):
    if line.startswith("["):
        section = line.strip()
        continue
    if section == "[dependencies]":
        m = re.match(r'\s*([A-Za-z0-9_-]+)\s*=\s*\{([^}]*)\}\s*$', line)
        if m and "optional = true" in m.group(2):
            p = re.search(r'path\s*=\s*"([^"]+)"', m.group(2)).group(1)
            name = re.search(r'^\s*name\s*=\s*"([^"]+)"', open(os.path.join(root, p, "Cargo.toml")).read(), re.M).group(1)
            optional[m.group(1)] = name
# [features]: `name = [ ... ]`, one entry per line or inline
for m in re.finditer(r'^([A-Za-z0-9_-]+)\s*=\s*\[(.*?)\]', text[text.index("[features]"):], re.M | re.S):
    feats[m.group(1)] = re.findall(r'"([^"]+)"', m.group(2))
on = set()
for f in os.environ["SEAMS_ENABLED"].split(","):
    f = f.strip()
    for entry in feats.get(f, []):
        if entry.startswith("dep:"):
            on.add(entry[4:])
print("\n".join(sorted(optional[a] for a in optional if a not in on)))
PYEOF
)

    local excluded="" still=""
    for c in $off; do
        if printf '%s\n' "$graph" | grep -Fxq -- "$c"; then still="$still $c"; else excluded="$excluded $c"; fi
    done
    excluded=$(echo $excluded); still=$(echo $still)
    local n_excluded=0
    [ -n "$excluded" ] && n_excluded=$(echo "$excluded" | wc -w | tr -d ' ')
    echo "wasm-build: profile $FEATURES excludes $n_excluded crates: $excluded"
    if [ -n "$still" ]; then
        echo "wasm-build: profile $FEATURES leaves $(echo "$still" | wc -w | tr -d ' ') gated crates linked (another crate in the graph depends on them; their seams are still not installed):"
        for c in $still; do
            echo "    $c <- $(cargo +"${TOOLCHAIN}" tree --target "$TARGET" -i "$c" -p main_main --no-default-features --features "$FEATURES" -e normal --prefix none --depth 1 2>/dev/null | tail -n +2 | awk '{print $1}' | LC_ALL=C sort -u | tr '\n' ' ')"
        done
    fi
    # The assertion: nothing counted as excluded may be in the graph.
    local bad=0
    for c in $excluded; do
        if printf '%s\n' "$graph" | grep -Fxq -- "$c"; then
            echo "wasm-build: FAIL - $c is counted as excluded but IS in the link graph:" >&2
            cargo +"${TOOLCHAIN}" tree --target "$TARGET" -i "$c" -p main_main \
                --no-default-features --features "$FEATURES" -e normal >&2 || true
            bad=1
        fi
    done
    [ "$bad" = "0" ] || exit 1
    echo "wasm-build: exclusion proof OK (no excluded crate is reachable in the link graph)"
}

PROFILE="${PGRUST_WASM_PROFILE:-dev}"
case "$PROFILE" in
    dev) PROFILE_DIR=debug ;;
    *)   PROFILE_DIR="$PROFILE" ;;
esac
if [ "${PGRUST_WASM_SKIP_LINK:-0}" != "1" ]; then
    prove_feature_exclusion
    cargo +"${TOOLCHAIN}" build --target "$TARGET" -Zbuild-std=std,panic_unwind -p main_main --bin postgres --profile "$PROFILE" \
        --no-default-features --features "$FEATURES"
    BIN_WASM="$ROOT/target/${TARGET}/${PROFILE_DIR}/postgres.wasm"
    [ -f "$BIN_WASM" ] || { echo "wasm-build: FAIL — postgres.wasm not produced" >&2; exit 1; }
    echo "wasm-build: postgres.wasm linked ($(du -h "$BIN_WASM" | cut -f1), profile $PROFILE, features $FEATURES)"

    # Binaryen pass — release profile only (dev builds are untouched), opt out
    # with PGRUST_WASM_OPT=0. `[profile.wasm-release]` is `lto = false` with
    # `codegen-units = 16`, so the link leaves duplicate function bodies
    # behind; `wasm-opt -Oz` finds them and takes ~27% off the raw bytes
    # (measured in pglite-v-pgrust docs/results/2026-09-08-wasm-size-map.md §9).
    if [ "$PROFILE" = "wasm-release" ] && [ "${PGRUST_WASM_OPT:-1}" != "0" ]; then
        # The feature list is spelled out ONCE, here, and is deliberately NOT
        # `--all-features`: the release profile strips the module's
        # `target_features` section so wasm-opt cannot detect what the module
        # uses, and `--all-features` re-encodes the import section into
        # something V8 refuses to compile ("unknown import kind 0x7e"). The
        # list is the names build's `target_features`; only threads differs
        # between the two targets.
        WASM_OPT_FEATURES=()
        if [ "$TARGET" = "wasm32-wasip1-threads" ]; then
            WASM_OPT_FEATURES+=(--enable-threads)
        fi
        WASM_OPT_FEATURES+=(
            --enable-bulk-memory
            --enable-bulk-memory-opt
            --enable-call-indirect-overlong
            --enable-exception-handling
            --enable-extended-const
            --enable-multivalue
            --enable-mutable-globals
            --enable-nontrapping-float-to-int
            --enable-reference-types
            --enable-sign-ext
        )
        if ! command -v wasm-opt >/dev/null; then
            echo "wasm-build: FAIL — profile $PROFILE runs wasm-opt but Binaryen is not on PATH (install it, or set PGRUST_WASM_OPT=0 to ship the unoptimised module)" >&2
            exit 1
        fi
        WASM_OPT_BEFORE=$(wc -c < "$BIN_WASM")
        WASM_OPT_T0=$SECONDS
        wasm-opt -Oz "${WASM_OPT_FEATURES[@]}" "$BIN_WASM" -o "$BIN_WASM.opt"
        mv "$BIN_WASM.opt" "$BIN_WASM"
        WASM_OPT_AFTER=$(wc -c < "$BIN_WASM")
        echo "wasm-build: wasm-opt -Oz ${WASM_OPT_BEFORE} -> ${WASM_OPT_AFTER} bytes (-$(( (WASM_OPT_BEFORE - WASM_OPT_AFTER) * 100 / WASM_OPT_BEFORE ))%) in $((SECONDS - WASM_OPT_T0))s ($(wasm-opt --version))"
    fi
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

echo "VERDICT: wasm-build PASS (${N_INCLUDE}/${N_MEMBERS} crates @ ${TOOLCHAIN}, panic=unwind, features ${FEATURES})"
