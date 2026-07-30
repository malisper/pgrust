#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# build-pgo.sh — build the benchmark binary from source, with a CLEAN profile.
#
# RUN THIS ON THE CLICKBENCH BOX. It needs the loaded hits dataset.
#
# WHAT THIS IS FOR
#   Profile-guided optimisation means the compiler was handed a recorded
#   profile of the program running a training workload. The training workload
#   is therefore part of the artifact — and if a system trains on the queries
#   it is scored on, the score measures specialisation, not general speed.
#
#   The shipped binary DID train on the official ClickBench 43-query vector
#   (see README, "Binary provenance"). This script rebuilds it against a
#   corpus that provably does not, so you can benchmark a binary you built
#   yourself, from source you can read, with training queries mechanically
#   proven disjoint from the benchmark.
#
# THE PIPELINE (both families, matching the official retrain's leg structure)
#   0 preflight  toolchain, headers, disk, RAM, source
#   1 lint       prove training corpus INTERSECT published queries = empty
#   2 corpus     deterministically expand the corpus
#   3 instrument cargo build --profile dist  -Cprofile-generate
#   4 train      run BOTH families against the instrumented binary:
#                  transactional: point/range/scan/projection, writes, COPY,
#                                 prepared protocol, concurrent clients
#                  analytical   : the clean analytics vector, serial engine
#                                 and runtime engine, over the real hits data
#   5 merge      llvm-profdata merge
#   6 final      cargo build --profile dist  -Cprofile-use
#   7 verify     RE2 linkage, sha256, manifest
#
#   BOTH families matter. An analytics-only profile produces a binary that
#   legitimately under-performs on OLTP relative to the shipped artifact — a
#   discrepancy the pipeline itself would have manufactured. One build serves
#   both benchmarks; copy it to the OLTP server when you are done (see README).
#
# TRAINING DATA SAFETY
#   Stage 4 trains against a COPY of the ClickBench datadir, never the audited
#   one. The transactional legs create their own fixture schema, and the
#   ClickBench `data_size` axis measures the whole datadir — training in place
#   would corrupt both the bank and a scored axis. The copy is deleted after.
#
# COST
#   Two fat-LTO builds of a large Rust workspace. Budget 2.5-5 hours wall on
#   this 16 vCPU box, most of it in the two serial LTO links, plus ~20-40 min
#   of training. Disk needed: ~60 GB. It is resumable: --from <stage>.
#
# USAGE
#   ./build-pgo.sh --lint-only          # just the proof; no toolchain needed
#   ./build-pgo.sh --preflight          # check this box can do the build
#   ./build-pgo.sh                      # the whole pipeline
#   ./build-pgo.sh --from final         # resume at a stage
#   ./build-pgo.sh --jobs 8             # limit build parallelism
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGO="$HERE/pgo"
[ -f "$HERE/hosts.env" ] && . "$HERE/hosts.env"

WORK="${WORK:-$HOME/audit/build}"
SRC="${PGRUST_SRC:-$WORK/src}"
CORPUS="$WORK/corpus"
PROFRAW="$WORK/profraw"
PROFDATA="$WORK/pgrust.profdata"
TRAINDD="$WORK/traindd"
OUTBIN="$WORK/postgres"
LOG="$WORK/build.log"
SRCDD="${PGDATA_CB:-/data/clickbench/pgdata}"
JOBS="${JOBS:-$(nproc)}"
EXPECT_SHA="${PGRUST_GATED_SHA:-3e6cb16b1d22f6c754451dcc1f82c007934732fb}"
TRAIN_PORT=5599
TRAIN_SOCK="$WORK/sock"

FROM=preflight; ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --lint-only) ONLY=lint ;;
    --preflight) ONLY=preflight ;;
    --from)      shift; FROM="${1:-}" ;;
    --jobs)      shift; JOBS="${1:-}" ;;
    --src)       shift; SRC="${1:-}" ;;
    -h|--help)   sed -n '2,56p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac; shift
done

mkdir -p "$WORK"
say(){ echo "$*" | tee -a "$LOG"; }
hdr(){ say ""; say "=============================================================================="; say "$*"; say "=============================================================================="; }
die(){ say ""; say "!! $*"; exit 1; }

order="preflight lint corpus instrument train merge final verify"
want(){
  local s seen=0
  for s in $order; do
    [ "$s" = "$FROM" ] && seen=1
    [ "$s" = "$1" ] && { [ $seen = 1 ] && return 0 || return 1; }
  done
  return 1
}

hdr "pgrust PGO build — clean training corpus"
say "  work dir : $WORK"
say "  source   : $SRC"
say "  corpus   : $PGO/corpus/"
say "  jobs     : $JOBS"
say "  started  : $(date -u +%FT%TZ)"

# ---------------------------------------------------------------- 0 preflight
if want preflight || [ "$ONLY" = preflight ]; then
hdr "STAGE 0 — preflight"
ok=1
chk(){ printf '  %-34s %s\n' "$1" "$2" | tee -a "$LOG"; }

if command -v cargo >/dev/null 2>&1; then
  rv=$(rustc --version 2>/dev/null | awk '{print $2}')
  want_rv=$(awk -F'"' '/^channel/{print $2}' "$SRC/rust-toolchain.toml" 2>/dev/null)
  chk "rustc" "$rv${want_rv:+  (toolchain file pins $want_rv)}"
  if [ -n "$want_rv" ] && [ "$rv" != "$want_rv" ]; then
    chk "" "!! version mismatch — the pinned toolchain exists for a reason"
    chk "" "   (LLVM 21 SIGTRAPs on aarch64 thin-LTO; 1.96.0/LLVM 22 required)"; ok=0
  fi
else chk "rustc" "MISSING — install rustup, then 'rustup toolchain install'"; ok=0; fi

pd=$(command -v llvm-profdata 2>/dev/null || ls "$HOME"/.rustup/toolchains/*/lib/rustlib/*/bin/llvm-profdata 2>/dev/null | head -1)
if [ -n "$pd" ]; then chk "llvm-profdata" "$pd"
else chk "llvm-profdata" "MISSING — 'rustup component add llvm-tools'"; ok=0; fi

if [ -f /usr/include/re2/re2.h ]; then chk "re2 headers" "/usr/include/re2/re2.h"
else chk "re2 headers" "MISSING — 'sudo dnf install -y re2-devel abseil-cpp-devel'"; ok=0; fi
command -v pkg-config >/dev/null 2>&1 || { chk "pkg-config" "MISSING"; ok=0; }

if [ -d "$SRC/.git" ]; then
  s=$(git -C "$SRC" rev-parse HEAD 2>/dev/null); chk "source sha" "$s"
  [ "$s" = "$EXPECT_SHA" ] || chk "" "note: differs from the audited artifact's sha $EXPECT_SHA"
elif [ -f "$SRC/Cargo.toml" ]; then chk "source" "$SRC (no git metadata)"
else chk "source" "MISSING at $SRC — see README 'Building from source'"; ok=0; fi

avail=$(df -BG --output=avail "$WORK" | tail -1 | tr -dc '0-9')
chk "disk free at $WORK" "${avail} GB (need ~60)"
[ "${avail:-0}" -lt 60 ] && ok=0
chk "RAM" "$(awk '/MemTotal/{print int($2/1048576)}' /proc/meminfo) GB (fat-LTO link is the peak; 24+ recommended)"
chk "cpus" "$(nproc)"
if [ -d "$SRCDD" ]; then chk "clickbench datadir" "$SRCDD"
else chk "clickbench datadir" "MISSING at $SRCDD — stage 4 needs it"; ok=0; fi

say ""
[ $ok = 1 ] && say "  preflight OK" || die "preflight FAILED — fix the items above."
[ "$ONLY" = preflight ] && exit 0
fi

# --------------------------------------------------------------------- 1 lint
if want lint || [ "$ONLY" = lint ]; then
hdr "STAGE 1 — prove the corpus does not overlap the benchmarks"
say "  The lint canonicalises every training statement and every published"
say "  measurement statement and asserts the sets are disjoint."
say ""
bash "$PGO/lint-training-overlap.sh" 2>&1 | tee -a "$LOG" \
  || die "LINT FAILED — corpus overlaps a scored query family. Refusing to build."
say ""
say "  Read the counts: a lint that examined nothing would also pass."
say "  Confirm it can fail:"
say "    bash $PGO/lint-training-overlap.sh $PGO/denylist/analytics-official.sql   # must exit 1"
[ "$ONLY" = lint ] && { say ""; say "Lint-only mode. Done."; exit 0; }
fi

# ------------------------------------------------------------------- 2 corpus
if want corpus; then
hdr "STAGE 2 — expand the corpus"
rm -rf "$CORPUS"
bash "$PGO/gen-corpus.sh" "$CORPUS" 2>&1 | tee -a "$LOG" || die "corpus generation failed"
CH=$(find "$CORPUS" -type f | LC_ALL=C sort | xargs cat | sha256sum | cut -d' ' -f1)
echo "$CH" > "$WORK/corpus.sha256"
say ""
say "  corpus sha256: $CH"
say "  A PGO artifact is reproducible from (source sha, corpus sha). Keep this."
say "  files: $(ls "$CORPUS" | tr '\n' ' ')"
fi

# --------------------------------------------------------------- 3 instrument
if want instrument; then
hdr "STAGE 3 — instrumented build (-Cprofile-generate)"
say "  fat LTO, codegen-units=1. First of two long builds."
rm -rf "$PROFRAW"; mkdir -p "$PROFRAW"
t0=$(date +%s)
( cd "$SRC" && RUSTFLAGS="-Cprofile-generate=$PROFRAW" \
    cargo build --profile dist --locked --bin postgres -j "$JOBS" ) 2>&1 | tail -25 | tee -a "$LOG"
t1=$(date +%s)
INSTR="$SRC/target/dist/postgres"
[ -x "$INSTR" ] || die "instrumented build failed (no $INSTR)"
cp -f "$INSTR" "$WORK/postgres.instr"
say ""
say "  instrumented build: $(( (t1-t0)/60 )) min, $(stat -c %s "$INSTR") bytes"
fi

# -------------------------------------------------------------------- 4 train
if want train; then
hdr "STAGE 4 — training run (transactional + analytical)"
INSTR="$WORK/postgres.instr"
[ -x "$INSTR" ] || die "no instrumented binary at $INSTR (run stage 3)"

say "  Copying the ClickBench datadir so training never touches the audited"
say "  bank (transactional legs create schema, and data_size is a scored axis)."
sudo rm -rf "$TRAINDD"
sudo cp -a "$SRCDD" "$TRAINDD" || die "datadir copy failed"
sudo chown -R "$(id -u):$(id -g)" "$TRAINDD"
chmod 700 "$TRAINDD"
mkdir -p "$TRAIN_SOCK"
cat >> "$TRAINDD/postgresql.conf" <<EOF
shared_buffers=2GB
max_connections=50
fsync=off
full_page_writes=off
EOF

export LLVM_PROFILE_FILE="$PROFRAW/pgrust-%p-%m.profraw"
say "  booting instrumented server on port $TRAIN_PORT ..."
"$INSTR" -D "$TRAINDD" -k "$TRAIN_SOCK" -p "$TRAIN_PORT" -c listen_addresses='' \
  >>"$WORK/train-server.log" 2>&1 &
for _ in $(seq 1 300); do
  psql -h "$TRAIN_SOCK" -p "$TRAIN_PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
psql -h "$TRAIN_SOCK" -p "$TRAIN_PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 \
  || { tail -20 "$WORK/train-server.log"; die "instrumented server did not start"; }
P(){ psql -h "$TRAIN_SOCK" -p "$TRAIN_PORT" -U postgres -v ON_ERROR_STOP=0 "$@"; }

say ""
VACUOUS=0
say "  --- transactional family ---"
say "  These legs create their own fixture schema and share no table or column"
say "  name with either published transactional rig (see the taint map)."
P -d postgres -q -f "$CORPUS/oltp-schema.sql" >>"$LOG" 2>&1 \
  || say "  !! oltp-schema.sql reported errors — see $LOG"
# Consume EVERY transactional leg the corpus emits, rather than a hardcoded
# list. gen-corpus.sh has emitted legs this script did not know about (oltp-ext
# among them); a hardcoded list silently trains on less than the corpus
# provides, and nothing would have reported the omission.
oltp_legs=0
for f in "$CORPUS"/oltp-*.sql; do
  [ -f "$f" ] || continue
  leg=$(basename "$f" .sql)
  case "$leg" in oltp-schema) continue ;; esac   # DDL, already applied above
  t=$(date +%s)
  before=$(grep -ci 'ERROR:' "$LOG" 2>/dev/null || echo 0)
  P -d postgres -q -f "$f" >>"$LOG" 2>&1
  after=$(grep -ci 'ERROR:' "$LOG" 2>/dev/null || echo 0)
  errs=$(( after - before ))
  stmts=$(grep -c ';' "$f" 2>/dev/null || echo 0)
  say "    $leg: $(( $(date +%s)-t ))s  ($stmts statements, $errs errors)"
  # A leg whose statements all error trains nothing while still "succeeding".
  if [ "$stmts" -gt 0 ] && [ "$errs" -ge "$stmts" ]; then
    say "    !! $leg produced an error for every statement — it trained NOTHING."
    say "    !! Almost always a missing fixture: check oltp-schema.sql applied."
    VACUOUS=1
  fi
  T_OLTP=$(( ${T_OLTP:-0} + $(date +%s) - t ))
  oltp_legs=$(( oltp_legs + 1 ))
done
[ "$oltp_legs" -gt 0 ] || { say "  !! no transactional legs found in $CORPUS"; VACUOUS=1; }
if command -v pgbench >/dev/null 2>&1; then
  for pb in oltp-prep-point oltp-prep-const oltp-prep-write; do
    [ -f "$CORPUS/$pb.pgbench" ] || continue
    pgbench -h "$TRAIN_SOCK" -p "$TRAIN_PORT" -U postgres -d postgres \
      -M prepared -n -c 2 -j 2 -t 2000 -f "$CORPUS/$pb.pgbench" >>"$LOG" 2>&1
    say "    $pb (prepared protocol): done"
  done
  pgbench -h "$TRAIN_SOCK" -p "$TRAIN_PORT" -U postgres -d postgres \
    -n -c 4 -j 4 -T 30 -f "$CORPUS/oltp-prep-point.pgbench" >>"$LOG" 2>&1
  say "    concurrent 4-client phase: done"
else
  say "  !! pgbench not found — prepared-protocol and concurrent legs SKIPPED."
  say "  !! The resulting profile is weaker on the protocol paths than the"
  say "  !! shipped artifact's. Install postgresql client tools and re-run."
fi

say ""
say "  --- analytical family (over the real hits data) ---"
for vec in analytics-serial analytics-runtime; do
  [ -f "$CORPUS/$vec.sql" ] || continue
  t=$(date +%s); P -d test -q -f "$CORPUS/$vec.sql" >>"$LOG" 2>&1
  T_ANA=$(( ${T_ANA:-0} + $(date +%s) - t ))
  say "    $vec: $(( $(date +%s)-t ))s"
done

say ""
say "  clean shutdown (profile data is flushed at exit) ..."
pid=$(head -1 "$TRAINDD/postmaster.pid" 2>/dev/null)
[ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null
for _ in $(seq 1 180); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
n=$(ls "$PROFRAW"/*.profraw 2>/dev/null | wc -l)
say "  profraw files: $n"
[ "$n" -gt 0 ] || die "no profile data produced — the training run recorded nothing"
if [ "${VACUOUS:-0}" = 1 ]; then
  die "the transactional family trained nothing (see above). Refusing to
     continue: a profile built from analytics alone produces a binary that
     legitimately under-performs on OLTP, and the resulting comparison would
     be an artefact of this pipeline rather than a property of the engine."
fi
# Report the balance between the two families. It is not required to be even,
# but a wildly analytics-dominated profile is worth knowing about before you
# attribute an OLTP difference to the engine.
say "  training balance: transactional ${T_OLTP:-?}s vs analytical ${T_ANA:-?}s"
sudo rm -rf "$TRAINDD"
say "  training datadir removed; audited bank untouched"
fi

# -------------------------------------------------------------------- 5 merge
if want merge; then
hdr "STAGE 5 — merge profile data"
pd=$(command -v llvm-profdata 2>/dev/null || ls "$HOME"/.rustup/toolchains/*/lib/rustlib/*/bin/llvm-profdata 2>/dev/null | head -1)
[ -n "$pd" ] || die "llvm-profdata not found"
"$pd" merge -o "$PROFDATA" "$PROFRAW"/*.profraw 2>&1 | tee -a "$LOG" || die "profdata merge failed"
[ -s "$PROFDATA" ] || die "merged profdata is empty"
say "  $PROFDATA  ($(stat -c %s "$PROFDATA") bytes)"
fi

# -------------------------------------------------------------------- 6 final
if want final; then
hdr "STAGE 6 — optimised build (-Cprofile-use)"
say "  the second long build."
[ -s "$PROFDATA" ] || die "no profdata at $PROFDATA (run stages 4-5)"
t0=$(date +%s)
( cd "$SRC" && RUSTFLAGS="-Cprofile-use=$PROFDATA" \
    cargo build --profile dist --locked --bin postgres -j "$JOBS" ) 2>&1 | tail -25 | tee -a "$LOG"
t1=$(date +%s)
FIN="$SRC/target/dist/postgres"
[ -x "$FIN" ] || die "profile-use build failed"
cp -f "$FIN" "$OUTBIN"; chmod +x "$OUTBIN"
say ""
say "  optimised build: $(( (t1-t0)/60 )) min"
fi

# ------------------------------------------------------------------- 7 verify
if want verify; then
hdr "STAGE 7 — verify and record"
[ -x "$OUTBIN" ] || die "no output binary at $OUTBIN"
BSHA=$(sha256sum "$OUTBIN" | cut -d' ' -f1)
echo "$BSHA" > "$OUTBIN.sha256"

# A binary built without libre2 silently falls back to the Spencer regexp
# engine and answers regexp-heavy queries differently.
if [ -x "$SRC/scripts/check-re2-linkage.sh" ]; then
  if bash "$SRC/scripts/check-re2-linkage.sh" "$OUTBIN" >>"$LOG" 2>&1; then say "  RE2 linkage: PRESENT"
  else die "RE2 linkage check FAILED — this binary answers regexp queries differently"; fi
else
  strings "$OUTBIN" 2>/dev/null | grep -qi 're2' && say "  RE2 linkage: string evidence present" \
    || say "  !! RE2 linkage: NO evidence — treat regexp-heavy results with suspicion"
fi

"$OUTBIN" --version 2>&1 | head -1 | sed 's/^/  version: /' | tee -a "$LOG"

{
  echo "pgrust PGO build manifest"
  echo "built_utc=$(date -u +%FT%TZ)"
  echo "host=$(uname -srm)  cpus=$(nproc)"
  echo "source_sha=$(git -C "$SRC" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "rustc=$(rustc --version 2>/dev/null)"
  echo "profile=dist (fat LTO, codegen-units=1)"
  echo "corpus_sha256=$(cat "$WORK/corpus.sha256" 2>/dev/null)"
  echo "profdata_sha256=$(sha256sum "$PROFDATA" 2>/dev/null | cut -d' ' -f1)"
  echo "binary=$OUTBIN"
  echo "binary_sha256=$BSHA"
  echo "training=clean corpus (analytics + transactional); lint-proven disjoint"
} > "$WORK/build-manifest.txt"

say ""
say "  binary  : $OUTBIN"
say "  sha256  : $BSHA"
say "  manifest: $WORK/build-manifest.txt"
say ""
say "  Benchmark it — ClickBench:"
say "    ./run-clickbench.sh --binary $OUTBIN"
say "  And OLTP (one build serves both; copy it to the server):"
say "    scp $OUTBIN ${AUDIT_USER:-<user>}@${OLTP_SERVER_PRIVATE:-<server>}:/tmp/postgres.mine"
say "    ssh ${AUDIT_USER:-<user>}@${OLTP_SERVER_PRIVATE:-<server>} \\"
say "        'sudo install -m755 /tmp/postgres.mine /opt/pgrust/bin/postgres.mine'"
say "    ./run-oltp-ro.sh --pgrust-binary /opt/pgrust/bin/postgres.mine"
say ""
say "  finished $(date -u +%FT%TZ)"
fi
