#!/usr/bin/env bash
# pg-regress-fast.sh — run PostgreSQL's own regression suite (src/test/regress)
# against a pgrust-fast server with the REAL pg_regress binary in
# installcheck mode (--use-existing --host/--port), NATIVE parallel-group
# schedule: each "test: a b c" line in the schedule runs its tests
# concurrently (pg_regress's own dispatcher), exactly as real pg_regress and
# C postgres do. Group order/membership from the schedule file is preserved
# byte-for-byte (never reordered — group order encodes real dependencies,
# e.g. test_setup first). Historically this runner exploded every group into
# one test per schedule line (full serial: 229 steps instead of ~80 groups),
# which is the dominant reason a from-scratch sweep took 4-8h instead of the
# few minutes C pg_regress needs for the same 229 files.
#
# Runs the whole schedule in ONE invocation. If the pgrust postmaster dies
# mid-schedule, falls back to segmented resume: each resumed segment gets a
# fresh cluster and prepends test_setup (without it resumed segments lose the
# tenk1/onek fixtures and understate results by ~650 lines). Because tests
# within a group now run concurrently, a crash can leave partial/missing
# output for any member of the group that was in flight — there is no
# meaningful single-test "victim"/"killer" distinction anymore, so the whole
# GROUP containing the crash is the resume boundary (retried in full; a fresh
# cluster + test_setup makes this correctness-safe, just occasionally
# redundant work on the rare crash path). Groups fully before the crashed
# group are trusted as-is (pg_regress already gave each of their tests a real
# per-test result, in its own results/<test>.out file, independent of how
# many tests ran concurrently in that group).
#
# Usage:
#   scripts/pg-regress-fast.sh [test names...]
#     no args -> the full schedule (parallel_schedule), native groups
#     with args -> an explicit test list, run as one group (still concurrent)
#
# Environment (all optional):
#   PGRUST_BIN    server binary   (default: target/fast-profile/postgres, then
#                                  target/release/postgres, target/debug/postgres)
#   PGINSTALL     C PostgreSQL 18 bin dir for initdb/psql
#                                 (default: /tmp/pgrust_pginstall/bin, /opt/homebrew/bin,
#                                  /usr/lib/postgresql/18/bin)
#   PG_REGRESS    pg_regress binary
#                                 (default: ../pgrust/postgres-18.3/src/test/regress/pg_regress)
#   REGRESS_SRC   regression corpus dir with sql/ + expected/
#                                 (default: ../pgrust-fabled/vendor/postgres-src/src/test/regress)
#   DLPATH        dir holding regress.{dylib,so} for :libdir
#                                 (default: ../pgrust/postgres-18.3/src/test/regress)
#   PORT          server port     (default: 54551)
#   OUTDIR        output dir. Default: a fresh unique dir under the shared
#                 scratch root (scripts/lib/scratch.sh), REMOVED at exit —
#                 set OUTDIR (caller-owned, kept) or KEEP_DATADIR=1 to keep
#                 artifacts. Segment datadirs (segN/dd, ~0.3G each) are
#                 always torn down at exit regardless of OUTDIR ownership
#                 unless KEEP_DATADIR=1.
#   SCHEDULE      schedule file to take the test list from (default: parallel_schedule)
#   TEST_TIMEOUT  per-test psql timeout, seconds (default: 180)
#   MAX_SEGMENTS  resume-segment cap (default: 60)
#   NO_BUILD      skip cargo build of PGRUST_BIN when set
#   NO_SCORE      skip the classify/query-score post-passes when set
#   PROFILE       cargo profile for the default build (default: fast-profile)
#   REGRESS_OVERLAY  dir of annotated regress .sql copies for the order-aware
#                    comparator (default: $REPO/regress/overlay/sql; see the
#                    "order-aware comparator" block below). REGRESS_ORDER=off
#                    disables the overlay + comparator entirely.
#
# Exit: 0 iff every test passed. Artifacts in $OUTDIR:
#   segN/ per segment (results/, regression.diffs, server.log, status.tsv),
#   merged_status.tsv (test, status, segN, [kill label]), status.tsv (merged),
#   classified.tsv + query_score.tsv from the post-passes.
set -u

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO/scripts/lib/scratch.sh"
WORK_ROOT="$(cd "$REPO/../.." 2>/dev/null && pwd)"
case "$REPO" in */.claude/worktrees/*) WORK_ROOT="$(cd "$REPO/../../../.." && pwd)" ;; esac

PGINSTALL="${PGINSTALL:-}"
for cand in "$PGINSTALL" /tmp/pgrust_pginstall/bin /opt/homebrew/bin /usr/lib/postgresql/18/bin; do
    [ -n "$cand" ] && [ -x "$cand/initdb" ] && PGBIN="$cand" && break
done
[ -n "${PGBIN:-}" ] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL)"; exit 2; }
"$PGBIN/initdb" --version | grep -q ' 18\.' || { echo "initdb at $PGBIN is not v18"; exit 2; }

# collate.linux.utf8's skip gate needs de_DE/en_US/sv_SE/tr_TR UTF-8 system
# locales in pg_collation, which initdb imports from what the pod has
# installed. The fleet harness runs this script as pg (no apt); there the
# install must come from E2E_APT=locales-all (root phase) or the job image.
if [ "$(uname -s)" = Linux ] && ! locale -a 2>/dev/null | grep -qx 'tr_TR.utf8'; then
    if [ "$(id -u)" = 0 ] && command -v apt-get >/dev/null; then
        echo "installing locales-all (locale-gated regress suites)"
        apt-get install -y -qq locales-all >/dev/null 2>&1 \
            || echo "locales-all install failed; locale-gated suites will skip"
    else
        echo "WARNING: tr_TR.utf8 locale missing (not root; cannot install)" \
             "— collate.linux.utf8 will take its skip path"
    fi
fi

PG_REGRESS="${PG_REGRESS:-$WORK_ROOT/pgrust/postgres-18.3/src/test/regress/pg_regress}"
REGRESS_SRC="${REGRESS_SRC:-$WORK_ROOT/pgrust-fabled/vendor/postgres-src/src/test/regress}"
DLPATH="${DLPATH:-$WORK_ROOT/pgrust/postgres-18.3/src/test/regress}"
[ -x "$PG_REGRESS" ] || { echo "pg_regress not found at $PG_REGRESS (set PG_REGRESS)"; exit 2; }
[ -d "$REGRESS_SRC/sql" ] && [ -d "$REGRESS_SRC/expected" ] || {
    echo "regression corpus not found at $REGRESS_SRC (set REGRESS_SRC)"; exit 2; }

PROFILE="${PROFILE:-fast-profile}"
if [ -z "${PGRUST_BIN:-}" ]; then
    if [ -z "${NO_BUILD:-}" ]; then
        (cd "$REPO" && cargo build --profile "$PROFILE" -p main_main --bin postgres) || exit 2
    fi
    case "$PROFILE" in dev) PGRUST_BIN="$REPO/target/debug/postgres" ;; *) PGRUST_BIN="$REPO/target/$PROFILE/postgres" ;; esac
fi
[ -x "$PGRUST_BIN" ] || { echo "no server binary at $PGRUST_BIN"; exit 2; }

PORT="${PORT:-54551}"
if [ -z "${OUTDIR:-}" ]; then
    # Defaulted -> we own it: unique under the scratch root, removed at exit.
    OUTDIR="$(scratch_datadir pgrust-fast-regress)"
    scratch_adopt "$OUTDIR"
fi
# Segment datadirs are the heavy residue (~0.3G each); tear them down at exit
# even when the caller owns OUTDIR (glob expands at cleanup time).
scratch_adopt "$OUTDIR/seg*/dd"
TEST_TIMEOUT="${TEST_TIMEOUT:-180}"
MAX_SEGMENTS="${MAX_SEGMENTS:-60}"
rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

# --- order-aware comparator overlay (lane-executor-v2 design §7) -------------
# Per-query sort-mode annotations (`-- pgrust:rowsort`, `-- pgrust:stable-tie`)
# live in annotated COPIES of regress .sql files under $REGRESS_OVERLAY — the
# vendor corpus at $REGRESS_SRC is external and never modified. When overlay
# files exist we build a shadow inputdir in $OUTDIR/corpus: symlinks to every
# vendor entry, with each (verified) overlay .sql copied over its counterpart.
# psql echoes the annotation comments into results/<t>.out, so an annotated
# test ALWAYS fails pg_regress's raw byte diff; the order-compare post-pass at
# the bottom re-adjudicates such failures per query (unannotated queries stay
# byte-exact; annotated ones compare as row multisets) and flips the test to
# ok iff the whole file matches under those rules. With no overlay files (or
# REGRESS_ORDER=off) this is all inert: inputdir is $REGRESS_SRC verbatim and
# the post-pass never runs — byte-identical behavior to the pre-comparator
# harness.
OVERLAY_DIR="${REGRESS_OVERLAY:-$REPO/regress/overlay/sql}"
INPUTDIR="$REGRESS_SRC"
OVERLAY_ACTIVE=""
if [ "${REGRESS_ORDER:-on}" != off ] && ls "$OVERLAY_DIR"/*.sql >/dev/null 2>&1; then
    mkdir -p "$OUTDIR/corpus/sql"
    for e in "$REGRESS_SRC"/*; do
        b="$(basename "$e")"
        [ "$b" = sql ] && continue
        ln -s "$e" "$OUTDIR/corpus/$b"
    done
    for f in "$REGRESS_SRC"/sql/*; do
        ln -s "$f" "$OUTDIR/corpus/sql/$(basename "$f")"
    done
    for ov in "$OVERLAY_DIR"/*.sql; do
        t="$(basename "$ov" .sql)"
        vend="$REGRESS_SRC/sql/$t.sql"
        if [ ! -f "$vend" ]; then
            echo "WARNING: overlay $ov has no vendor counterpart; ignored"
            continue
        fi
        # Guard: overlay must be the vendor file plus whole-line annotations,
        # nothing else. A drifted overlay is skipped (fail-safe: vendor file
        # runs, test compares byte-exact as before).
        if ! cmp -s <(grep -vE '^[[:space:]]*--[[:space:]]*pgrust:' "$ov") "$vend"; then
            echo "WARNING: overlay $ov differs from vendor $t.sql beyond" \
                 "pgrust annotations (stale copy?); ignored"
            continue
        fi
        rm -f "$OUTDIR/corpus/sql/$t.sql"
        cp "$ov" "$OUTDIR/corpus/sql/$t.sql"
        OVERLAY_ACTIVE=1
    done
    if [ -n "$OVERLAY_ACTIVE" ]; then
        INPUTDIR="$OUTDIR/corpus"
        echo "order-compare: overlay active ($OVERLAY_DIR -> $INPUTDIR)"
    fi
fi

if [ $# -gt 0 ]; then
    printf '%s\n' "$@" > "$OUTDIR/all_tests.txt"
else
    # Preserve each schedule "test:" line as one GROUP (space-separated test
    # names run concurrently by pg_regress) — do not flatten to one test per
    # line. Order and grouping come verbatim from the schedule file; never
    # reordered here.
    SCHED_SRC="${SCHEDULE:-$REGRESS_SRC/parallel_schedule}"
    grep '^test:' "$SCHED_SRC" | sed -E 's/^test:[[:space:]]*//; s/[[:space:]]+/ /g; s/[[:space:]]*$//' \
        | grep -v '^$' > "$OUTDIR/all_tests.txt"
fi

cat > "$OUTDIR/launch.sh" <<EOF
#!/usr/bin/env bash
exec perl -e 'alarm shift @ARGV; exec @ARGV' "$TEST_TIMEOUT" "\$@"
EOF
chmod +x "$OUTDIR/launch.sh"

ulimit -s 65520 2>/dev/null
for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
    [ -d "$tz" ] && export PGRUST_TZDIR="$tz" && export PGRUST_PGSHAREDIR="$(dirname "$tz")" && break
done

SRV=""
trap '[ -n "$SRV" ] && kill -9 $SRV 2>/dev/null; scratch_cleanup' EXIT

run_segment() { # $1 segdir (contains schedule)
    local SEGDIR="$1" DD="$1/dd" SOCK="$1/sock" LOG="$1/server.log"
    mkdir -p "$SOCK" "$SEGDIR/results"

    "$PGBIN/initdb" -D "$DD" --no-locale --encoding=UTF8 -U postgres -A trust >"$SEGDIR/initdb.log" 2>&1 \
        || { echo "initdb failed; see $SEGDIR/initdb.log"; return 2; }

    seg_start_server() {
        # check_max_stack_depth rejects values above rlimit-512kB; derive from
        # the actual ulimit (crash-restart-smoke precedent) — pod stack
        # rlimits can sit below 60512kB.
        local stack_kb msd
        stack_kb=$(ulimit -s)
        case "$stack_kb" in
            unlimited|'') msd=60000 ;;
            *) msd=$(( stack_kb - 2048 )); [ "$msd" -gt 60000 ] && msd=60000; [ "$msd" -lt 2048 ] && msd=2048 ;;
        esac
        # REGRESS_SERVER_EXTRA_C: extra "-c name=value" server flags (word-split
        # deliberately) — lane-gates pins pgrust.parallel_engine=legacy here
        # post-M5-flip (its charter is the serial-lane census; the M5 engine
        # surface has its own batteries).
        (RUST_MIN_STACK=134217728 RUST_BACKTRACE=1 LLVM_PROFILE_FILE="${COV_PROFILE_DIR:-/tmp/covprof}/server-%p.profraw" exec "$PGRUST_BIN" -D "$DD" -k "$SOCK" -p "$PORT" \
            -c listen_addresses= -c "max_stack_depth=$msd" -c io_method="${REGRESS_IO_METHOD:-sync}" \
            ${REGRESS_SERVER_EXTRA_C:-} \
            >>"$LOG" 2>&1) &
        SRV=$!
    }
    seg_wait_ready() {
        local ready=""
        for _ in $(seq 1 60); do
            kill -0 $SRV 2>/dev/null || break
            if "$PGBIN/psql" -h "$SOCK" -p "$PORT" -U postgres -X -c 'SELECT 1' >/dev/null 2>&1; then
                ready=1; break
            fi
            sleep 1
        done
        [ -n "$ready" ] || { echo "server did not become ready; log tail:"; tail -30 "$LOG"; return 2; }
    }

    seg_start_server
    seg_wait_ready || return 2

    # Native CREATE DATABASE first; fall back to seeding with the C binary in
    # the same datadir (disk-compatible) if the native path fails.
    if "$PGBIN/psql" -h "$SOCK" -p "$PORT" -U postgres -X -v ON_ERROR_STOP=1 \
            -c 'CREATE DATABASE regression' >"$SEGDIR/createdb.log" 2>&1; then
        echo "regression database: created natively by $PGRUST_BIN"
        echo native > "$SEGDIR/createdb.mode"
    else
        echo "native CREATE DATABASE failed (see $SEGDIR/createdb.log); falling back to C seed"
        echo c-seed > "$SEGDIR/createdb.mode"
        kill -TERM $SRV 2>/dev/null; for _ in $(seq 1 60); do kill -0 $SRV 2>/dev/null || break; sleep 1; done; kill -9 $SRV 2>/dev/null; wait $SRV 2>/dev/null; SRV=""
        rm -f "$DD/postmaster.pid"
        "$PGBIN/pg_ctl" -D "$DD" -o "-k $SOCK -p $PORT -c listen_addresses=" -w -l "$SEGDIR/c_seed.log" start >/dev/null 2>&1 \
            || { echo "C seed postmaster failed; see $SEGDIR/c_seed.log"; return 2; }
        "$PGBIN/psql" -h "$SOCK" -p "$PORT" -U postgres -X -c 'DROP DATABASE IF EXISTS regression' >/dev/null 2>&1
        "$PGBIN/createdb" -h "$SOCK" -p "$PORT" -U postgres regression \
            || { "$PGBIN/pg_ctl" -D "$DD" -m fast stop >/dev/null 2>&1; echo "createdb regression failed"; return 2; }
        "$PGBIN/pg_ctl" -D "$DD" -m fast -w stop >/dev/null 2>&1
        seg_start_server
        seg_wait_ready || return 2
    fi

    (cd "$SEGDIR" && "$PG_REGRESS" \
        --use-existing \
        --host="$SOCK" --port="$PORT" --user=postgres \
        --bindir="$PGBIN" \
        --inputdir="$INPUTDIR" \
        --expecteddir="$REGRESS_SRC" \
        --outputdir="$SEGDIR" \
        --schedule="$SEGDIR/schedule" \
        --dlpath="$DLPATH" \
        --launcher="$OUTDIR/launch.sh" \
        2>&1 | tee "$SEGDIR/pg_regress.log" | grep -E '^(ok|not ok) ')

    kill -TERM $SRV 2>/dev/null; for _ in $(seq 1 60); do kill -0 $SRV 2>/dev/null || break; sleep 1; done; kill -9 $SRV 2>/dev/null; wait $SRV 2>/dev/null; SRV=""

    # pg_regress separates test number from name with "-" for serial steps
    # and "+" for parallel-group members — accept both.
    grep -E '^(ok|not ok) ' "$SEGDIR/pg_regress.log" \
        | sed -E 's/^ok +[0-9]+ +[-+] +([a-zA-Z0-9_.]+).*/\1\tok/; s/^not ok +[0-9]+ +[-+] +([a-zA-Z0-9_.]+).*/\1\tFAILED/' \
        > "$SEGDIR/status.tsv" || true
    return 0
}

echo "server: $PGRUST_BIN"
echo "corpus: $REGRESS_SRC"
echo "outdir: $OUTDIR"

cp "$OUTDIR/all_tests.txt" "$OUTDIR/remaining.txt"
: > "$OUTDIR/merged_status.tsv"
seg=1
while [ -s "$OUTDIR/remaining.txt" ] && [ "$seg" -le "$MAX_SEGMENTS" ]; do
    SEGDIR="$OUTDIR/seg$seg"
    mkdir -p "$SEGDIR"
    { if [ "$seg" -gt 1 ] && ! head -1 "$OUTDIR/remaining.txt" | grep -qx test_setup; then
          echo "test: test_setup"
      fi
      sed 's/^/test: /' "$OUTDIR/remaining.txt"; } > "$SEGDIR/schedule"
    echo "=== segment $seg: $(wc -l < "$OUTDIR/remaining.txt" | tr -d ' ') groups / $(awk '{n+=NF} END{print n+0}' "$OUTDIR/remaining.txt") tests ==="
    run_segment "$SEGDIR" || exit 2
    # The segment's cluster is dead and never reused (each resume segment
    # gets a fresh one); drop its datadir now to bound peak disk.
    [ -n "${KEEP_DATADIR:-}" ] || rm -rf "$SEGDIR/dd"

    # Crash scan, group-granular (each remaining.txt line is a concurrent
    # pg_regress group, per the native-schedule fix above): a group is "bad"
    # if any of its member tests has no result file, or shows a connection
    # failure/loss. Concurrent execution means we can no longer attribute a
    # single victim/killer test within a group, so the whole bad group is the
    # resume boundary — groups before it are trusted (pg_regress already gave
    # each of their tests a real per-test result).
    crashed_line=""
    while IFS= read -r line; do
        bad=0
        for t in $line; do
            f="$SEGDIR/results/$t.out"
            if [ ! -f "$f" ]; then bad=1; break; fi
            if grep -qE 'server closed the connection unexpectedly|no connection to the server|connection to server .* failed' "$f"; then
                bad=1; break
            fi
        done
        [ "$bad" = 1 ] && { crashed_line="$line"; break; }
    done < "$OUTDIR/remaining.txt"

    emit_line() { # $1 group line (space-separated tests), $2 kill label
        local line="$1" label="$2" t st
        for t in $line; do
            st=$(awk -F'\t' -v t="$t" '$1==t{print $2}' "$SEGDIR/status.tsv" | head -1)
            printf '%s\t%s\tseg%d%s\n' "$t" "${st:-NORESULT}" "$seg" "$label" >> "$OUTDIR/merged_status.tsv"
        done
    }

    if [ -z "$crashed_line" ]; then
        while IFS= read -r line; do emit_line "$line" ""; done < "$OUTDIR/remaining.txt"
        : > "$OUTDIR/remaining.txt"
        break
    fi
    echo "postmaster died in group: [$crashed_line]; resuming"
    {
        echo "--- crash forensics seg$seg: group=[$crashed_line] ---"
        echo "--- server.log tail ---"
        tail -n 120 "$SEGDIR/server.log" 2>/dev/null
        if command -v addr2line >/dev/null && grep -q 'terminated by signal' "$SEGDIR/server.log"; then
            echo "--- symbolized signal frames (addr2line) ---"
            tail -n 120 "$SEGDIR/server.log" | grep -oE '\(\+0x[0-9a-f]+\)' | tr -d '(+)' \
                | while read -r off; do
                    printf '%s  %s\n' "$off" \
                        "$(addr2line -e "$PGRUST_BIN" -f -C -i "$off" 2>/dev/null | paste -sd'  <-  ' -)"
                done
        fi
    } >> "$OUTDIR/crash-forensics.txt"
    while IFS= read -r line; do
        [ "$line" = "$crashed_line" ] && break
        emit_line "$line" ""
    done < "$OUTDIR/remaining.txt"
    awk -v c="$crashed_line" '$0==c{f=1} f{print}' "$OUTDIR/remaining.txt" > "$OUTDIR/remaining.next"
    mv "$OUTDIR/remaining.next" "$OUTDIR/remaining.txt"
    seg=$((seg+1))
done
if [ -s "$OUTDIR/remaining.txt" ]; then
    echo "segment cap ($MAX_SEGMENTS) hit with $(wc -l < "$OUTDIR/remaining.txt" | tr -d ' ') tests unrun"
fi

cut -f1,2 "$OUTDIR/merged_status.tsv" > "$OUTDIR/status.tsv"

# --- order-compare post-pass (lane-executor-v2 design §7) --------------------
# For each FAILED test whose sql carries pgrust sort-mode annotations,
# re-adjudicate results/<t>.out against expected/<t>*.out with per-query sort
# modes applied to BOTH sides. On success flip the test to ok in both status
# files (the raw regression.diffs entry remains for forensics). Anything the
# comparator cannot segment stays exact-compared inside it, so this can only
# forgive order, never content.
if [ -n "$OVERLAY_ACTIVE" ] && command -v python3 >/dev/null; then
    : > "$OUTDIR/order-reconciled.txt"
    while IFS="$(printf '\t')" read -r t st segref; do
        [ "$st" = FAILED ] || continue
        ovsql="$OUTDIR/corpus/sql/$t.sql"
        if [ ! -f "$OVERLAY_DIR/$t.sql" ] || [ ! -f "$ovsql" ] \
            || ! grep -qE '^[[:space:]]*--[[:space:]]*pgrust:(rowsort|stable-tie)([[:space:]]|$)' "$ovsql"; then
            # No overlay/annotations: a plain byte-exact miss the comparator
            # never sees. Surface a bounded per-test extract in the STREAMED
            # log — GL-LANEGATES-REBASE-1 found 4 failing no-overlay suites
            # with ZERO diff evidence in the fleet log, because the whole-run
            # regression.diffs display below only fires at <=4 total failures.
            d="$OUTDIR/$segref/regression.diffs"
            if [ -f "$d" ]; then
                echo "--- $t (no overlay) regression.diffs extract (first ${ORDER_RESIDUAL_LINES:-120} lines) ---"
                awk -v t="$t" '/^diff /{p = ($0 ~ ("/" t "\\.out( |$)"))} p' "$d" \
                    | head -n "${ORDER_RESIDUAL_LINES:-120}"
            fi
            continue
        fi
        res="$OUTDIR/$segref/results/$t.out"
        [ -f "$res" ] || continue
        exps=""
        for v in "$REGRESS_SRC/expected/$t.out" "$REGRESS_SRC/expected/$t"_[0-9]*.out; do
            [ -f "$v" ] && exps="$exps $v"
        done
        [ -n "$exps" ] || continue
        # On failure the comparator prints its POST-NORMALIZATION residual
        # (annotation echoes stripped, sort modes applied) into the STREAMED
        # log — the diff it actually adjudicated. The raw expected-vs-results
        # diff is deliberately NOT printed anymore: for annotated tests it is
        # dominated by echoed `-- pgrust:*` comment lines the comparator
        # strips before comparing, which made the 2026-07-21 lane-gates red
        # read as an "echo artifact" while the real content (plan-shape
        # diffs) sat past the 120-line truncation window.
        # shellcheck disable=SC2086
        if python3 "$REPO/scripts/regress-order-compare.py" \
                --sql "$ovsql" --actual "$res" --expected $exps \
                --residual "${ORDER_RESIDUAL_LINES:-120}"; then
            echo "$t" >> "$OUTDIR/order-reconciled.txt"
            echo "order-compare: $t reconciled ok (diff was order-only under per-query sort modes)"
        else
            echo "order-compare: $t still FAILED after applying sort modes"
        fi
    done < "$OUTDIR/merged_status.tsv"
    if [ -s "$OUTDIR/order-reconciled.txt" ]; then
        for f in status.tsv merged_status.tsv; do
            awk -F'\t' -v OFS='\t' 'NR==FNR{ok[$1]=1; next}
                 ok[$1] && $2=="FAILED" {$2="ok"} {print}' \
                "$OUTDIR/order-reconciled.txt" "$OUTDIR/$f" > "$OUTDIR/$f.new" \
                && mv "$OUTDIR/$f.new" "$OUTDIR/$f"
        done
    fi
fi
echo
echo "segments used: $seg   merged status: $OUTDIR/merged_status.tsv"
[ -f "$OUTDIR/crash-forensics.txt" ] && cat "$OUTDIR/crash-forensics.txt"
for m in "$OUTDIR"/seg*/createdb.mode; do
    [ -f "$m" ] || continue
    echo "createdb $(basename "$(dirname "$m")"): $(cat "$m")$(grep -m1 'GUC slot' "$(dirname "$m")/createdb.log" 2>/dev/null | sed 's/^/  /')"
done
awk -F'\t' '{c[$2]++; n++} END {printf "files: ok=%d FAILED=%d total=%d\n", c["ok"], c["FAILED"], n}' "$OUTDIR/status.tsv"

nfail=$(awk -F'\t' '$2!="ok"' "$OUTDIR/status.tsv" | wc -l | tr -d ' ')
if [ "$nfail" -ge 1 ] && [ "$nfail" -le "${DIFF_HEAD_MAX_FILES:-4}" ]; then
    for d in "$OUTDIR"/seg*/regression.diffs; do
        [ -s "$d" ] || continue
        echo "--- $(basename "$(dirname "$d")") regression.diffs (first ${DIFF_HEAD_LINES:-700} lines) ---"
        head -n "${DIFF_HEAD_LINES:-700}" "$d"
    done
fi

if [ -z "${NO_SCORE:-}" ] && command -v python3 >/dev/null; then
    python3 "$REPO/scripts/regress-classify.py" "$OUTDIR" --src "$REGRESS_SRC" || true
    python3 "$REPO/scripts/regress-query-score.py" "$OUTDIR" --src "$REGRESS_SRC" \
        --classified "$OUTDIR/classified.tsv" || true
fi

awk -F'\t' '$2!="ok"{exit 1}' "$OUTDIR/status.tsv" && [ ! -s "$OUTDIR/remaining.txt" ]
