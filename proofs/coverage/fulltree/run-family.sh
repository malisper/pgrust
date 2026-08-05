#!/bin/bash
# run-family.sh FAMILY TIMEOUT — one family of the full-tree Kani coverage
# capture, end to end:
#   1. wipe stale kanicov_* dirs for the family (stale kaniraw would show up
#      as census orphans);
#   2. pass 1: run-kani-coverage.sh over the family joblist (load-gated);
#   3. if pass 1 recorded NAME-UNRESOLVED rows with a unique did-you-mean
#      candidate, pass 2 re-runs exactly those with the resolved override;
#   4. reconcile passes into census/census-<fam>.tsv (last disposition wins);
#   5. copy the family's kaniraw JSONs (dir structure preserved) into
#      kaniraw/<fam>/ and DELETE the family target dir (600-800MB each);
#   6. commit + push the census + kaniraw (incremental banking: an overnight
#      run that dies loses at most the in-flight family).
set -u
FAM=$1; T=$2
HERE="$(cd "$(dirname "$0")" && pwd)"       # proofs/coverage/fulltree
COV="$HERE/.."                               # proofs/coverage
PROOFS="$HERE/../.."                         # proofs
REPO="$(cd "$PROOFS/.." && pwd)"
LOGD="$HERE/logs/$FAM"
mkdir -p "$LOGD" "$HERE/kaniraw"
LOADGATE=${LOADGATE:-32}

echo "== [$(date +%H:%M:%S)] family $FAM (timeout ${T}s) =="

# 1. wipe stale kanicov output for this family — UNLESS a checkpoint census
# from a crashed run exists (its kaniraw in target/ is the banked evidence for
# rows already dispositioned; resume must keep it)
if ! ls "$LOGD"/census.a*.tsv >/dev/null 2>&1; then
    find "$PROOFS/$FAM/target/kani" -type d -name 'kanicov_*' -prune -exec rm -rf {} + 2>/dev/null
else
    echo "-- checkpoint censuses found; resuming (kanicov in target/ preserved)"
fi

# 2. pass 1, with retry-on-missing: sibling lanes' broad `pkill -f` patterns
# have SIGTERM'd this runner mid-family (observed rc=143 twice, banking
# partial censuses). Re-run the not-yet-dispositioned remainder up to 3x.
CENSUSES=()
# checkpoint censuses from a previous (crashed) invocation seed the
# disposition set: rows already RAN are not re-solved.
for ck in "$LOGD"/census.a*.tsv; do
    [ -f "$ck" ] || continue
    mv "$ck" "$ck.ckpt"
    CENSUSES+=("$ck.ckpt")
done
RC1=-
missing_left=0
for attempt in 1 2 3; do
    JL="$LOGD/joblist.a$attempt.tsv"
    if [ ${#CENSUSES[@]} -eq 0 ]; then
        cp "$HERE/joblists/$FAM.tsv" "$JL"
    else
        python3 "$HERE/reconcile-census.py" "${CENSUSES[@]}" > "$LOGD/census.sofar.tsv"
        awk -F'\t' 'NR==FNR { if (FNR>1) seen[$2]=1; next }
                    NF>=2 && $1!="" && !seen[$2]' \
            "$LOGD/census.sofar.tsv" "$HERE/joblists/$FAM.tsv" > "$JL"
        if [ ! -s "$JL" ]; then missing_left=0; break; fi
        echo "-- pass $attempt: $(wc -l < "$JL") harnesses still need a disposition"
    fi
    "$COV/run-kani-coverage.sh" --joblist "$JL" \
        --census "$LOGD/census.a$attempt.tsv" --timeout "$T" --log "$LOGD" \
        --load-gate "$LOADGATE" > "$LOGD/runner.a$attempt.out" 2>&1
    RC1=$?
    CENSUSES+=("$LOGD/census.a$attempt.tsv")
    python3 "$HERE/reconcile-census.py" "${CENSUSES[@]}" > "$LOGD/census.sofar.tsv"
    missing_left=$(awk -F'\t' 'NR==FNR { if (FNR>1) seen[$2]=1; next }
                 NF>=2 && $1!="" && !seen[$2] {n++} END{print n+0}' \
                 "$LOGD/census.sofar.tsv" "$HERE/joblists/$FAM.tsv")
    [ "$missing_left" -eq 0 ] && break
done

# 3. pass 2 for uniquely-resolvable NAME-UNRESOLVED rows
awk -F'\t' '$7=="FAILED-TO-RUN" && $6 ~ /^NAME-UNRESOLVED did-you-mean:/ {
    # base name of the suite harness (strip any module path)
    nb=split($2, bseg, "::"); base=bseg[nb];
    n=split($6, a, "did-you-mean:"); m=split(a[2], c, " ");
    hit=""; nhit=0;
    for (i=1; i<=m; i++) {
        k=split(c[i], seg, "::");
        if (seg[k]==base) { hit=c[i]; nhit++; }
    }
    if (nhit==1) printf "%s\t%s\t%s\n", $1, $2, hit;
}' "$LOGD/census.sofar.tsv" > "$LOGD/joblist.fix.tsv"
if [ -s "$LOGD/joblist.fix.tsv" ]; then
    echo "-- pass 2: $(wc -l < "$LOGD/joblist.fix.tsv") name-resolved re-runs"
    "$COV/run-kani-coverage.sh" --joblist "$LOGD/joblist.fix.tsv" \
        --census "$LOGD/census.p2.tsv" --timeout "$T" --log "$LOGD" \
        --load-gate "$LOADGATE" > "$LOGD/runner.p2.out" 2>&1
    CENSUSES+=("$LOGD/census.p2.tsv")
fi

# 4. reconcile
python3 "$HERE/reconcile-census.py" "${CENSUSES[@]}" > "$HERE/census/census-$FAM.tsv"

# 5. bank kaniraw, drop the target dir
rm -rf "$HERE/kaniraw/$FAM"
if [ -d "$PROOFS/$FAM/target/kani" ]; then
    ( cd "$PROOFS/$FAM/target/kani" && \
      find . -name '*kaniraw.json' | while read -r f; do
          mkdir -p "$HERE/kaniraw/$FAM/$(dirname "$f")"
          cp "$f" "$HERE/kaniraw/$FAM/$f"
      done )
fi
NRAW=$(find "$HERE/kaniraw/$FAM" -name '*kaniraw.json' 2>/dev/null | wc -l | tr -d ' ')
rm -rf "$PROOFS/$FAM/target"

# 6. commit + push
awk -F'\t' 'NR>1{c[$7]++} END{printf "RAN=%d WALLED=%d FAILED=%d NOFLAGS=%d\n", c["RAN"], c["WALLED"], c["FAILED-TO-RUN"], c["NOFLAGS"]}' \
    "$HERE/census/census-$FAM.tsv" | tee "$LOGD/tally.txt"
cd "$REPO"
git add "proofs/coverage/fulltree/census/census-$FAM.tsv" "proofs/coverage/fulltree/kaniraw/$FAM" 2>/dev/null
git commit -q -m "coverage(fulltree): $FAM census + kaniraw ($(cat "$LOGD/tally.txt"), $NRAW kaniraw)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" && git push -q origin proofs/coverage-fulltree
echo "== [$(date +%H:%M:%S)] $FAM done: $(cat "$LOGD/tally.txt") kaniraw=$NRAW rc1=$RC1 missing=$missing_left =="
[ "$missing_left" -eq 0 ] || exit 1
exit 0
