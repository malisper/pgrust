#!/bin/bash
# run-kani-coverage.sh — run Kani proof harnesses under source coverage and
# emit a FAIL-CLOSED census of what was actually measured.
#
# Usage:
#   run-kani-coverage.sh --joblist FILE --census FILE [--timeout S] [--log DIR]
#   run-kani-coverage.sh --family FAM --census FILE [--timeout S] HARNESS...
#
# Joblist is TSV: family <TAB> suite_harness [<TAB> kani_harness_override]
# The third column overrides harness-name qualification (SUITE.tsv stores some
# rows unqualified; e.g. the `bool` family's harnesses live in `mod harnesses`,
# not `mod proofs`). Absent, the name is used as-is if already qualified, else
# prefixed with `proofs::`.
#
# Each harness runs in its own invocation with that row's exact SUITE.tsv flags
# plus `--coverage -Z source-coverage`. kanicov output accumulates under
# proofs/<family>/target/kani/*/kanicov_*/.
#
# WHY THE CENSUS EXISTS (2026-07-30 hardening)
# --------------------------------------------
# A harness that fails to RUN emits no kaniraw. A pipeline that globs kaniraw at
# the end cannot tell that apart from a harness that ran and covered nothing, so
# the failure reads as legitimately-uncovered code: a silent, confident
# undercount. Not hypothetical — under a naive `proofs::` prefix all 20 `bool`
# harnesses exited rc=1 with "Failed to match the following harness(es)" and
# produced zero coverage (SMOKE-RESULT.md §7 blocker 2). Same gate-blindness
# class as the 96 harnesses that never ran under `--exact`.
#
# Two independent guards, both fail-closed:
#   1. PREFLIGHT — every harness name is resolved against `cargo kani list` for
#      its family BEFORE running. A name that does not resolve is recorded
#      FAILED-TO-RUN/NAME-UNRESOLVED, never silently skipped. If `kani list`
#      itself fails for a family, preflight degrades to SKIPPED for that family
#      and guard 2 carries the weight — a listing failure must not remove rows
#      from the denominator.
#   2. POSTCONDITION — after each run, at least one NEW kaniraw file whose
#      mangled name ends in the harness's terminal segment must exist. rc=0 with
#      no artifact is FAILED-TO-RUN, not zero coverage.
#
# Exit status: 0 only if every job ended RAN. Any WALLED / FAILED-TO-RUN /
# NOFLAGS row makes this exit 1, and merge-coverage.py additionally refuses to
# produce a summary for such a census unless the row is waived by name with a
# stated reason in --allow-unmeasured.
#
# Census TSV columns (header written on creation; merge-coverage.py reads it):
#   family  suite_harness  kani_harness  rc  wall_s  verdict  status  kaniraw_new
# status is one of RAN | WALLED | FAILED-TO-RUN | NOFLAGS.
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE="$DIR/../SUITE.tsv"
JOBS=""; CENSUS=""; T=900; LOGDIR="${TMPDIR:-/tmp}"; FAMILY=""; PREFLIGHT=1
LOADGATE=""
ARGV_HARNESSES=()

while [ $# -gt 0 ]; do
    case "$1" in
        --joblist)  JOBS=$2; shift 2;;
        --census)   CENSUS=$2; shift 2;;
        --timeout)  T=$2; shift 2;;
        --log)      LOGDIR=$2; shift 2;;
        --family)   FAMILY=$2; shift 2;;
        --load-gate) LOADGATE=$2; shift 2;;
        --no-preflight) PREFLIGHT=0; shift;;
        -h|--help)  sed -n '2,48p' "$0"; exit 0;;
        *)          ARGV_HARNESSES+=("$1"); shift;;
    esac
done

if [ -z "$CENSUS" ]; then
    echo "FATAL: --census FILE is required. A coverage capture without a" >&2
    echo "census cannot distinguish 'covered nothing' from 'never ran'." >&2
    exit 2
fi
mkdir -p "$(dirname "$CENSUS")" "$LOGDIR"

# Build the joblist from --family + argv if that form was used.
if [ -z "$JOBS" ]; then
    if [ -z "$FAMILY" ] || [ ${#ARGV_HARNESSES[@]} -eq 0 ]; then
        echo "FATAL: need --joblist FILE, or --family FAM plus harness names" >&2
        exit 2
    fi
    JOBS="$LOGDIR/joblist-$FAMILY-$$.tsv"
    : > "$JOBS"
    for H in "${ARGV_HARNESSES[@]}"; do
        printf '%s\t%s\t\n' "$FAMILY" "$H" >> "$JOBS"
    done
fi

printf 'family\tsuite_harness\tkani_harness\trc\twall_s\tverdict\tstatus\tkaniraw_new\n' > "$CENSUS"

# ---- preflight: cargo kani list per family, cached -------------------------
# `list` rejects verification flags but needs the family's -Z features, so only
# the -Z tokens are forwarded — and the UNION of them across ALL the family's
# SUITE rows, because rows differ (bool carries `-Z stubbing` on some rows only,
# and a stub attribute without that feature is a hard compile error, which
# looked exactly like a name defect on the first cut of this script).
#
# A listing that does not come back as a well-formed kani table is treated as
# UNAVAILABLE, never as "no harnesses" — parsing rustc's error output would
# otherwise reject every name in the family, and a preflight that fails
# everything is as useless as one that passes everything. Unavailable degrades
# to PREFLIGHT-SKIPPED (guard 2 still applies); it never drops rows.
LIST_FAMS=""; LIST_BAD=""
list_for() {  # $1 = family; prints names one per line; rc 1 if unavailable
    local fam=$1 zflags raw out lrc
    case " $LIST_BAD " in *" $fam "*) return 1;; esac
    case " $LIST_FAMS " in *" $fam "*) cat "$LOGDIR/kanilist-$fam.txt"; return 0;; esac
    zflags=$(awk -F'\t' -v f="$fam" '$1==f {print $3}' "$SUITE" \
             | tr ' ' '\n' | awk '/^-Z$/{z=1;next} z==1{print $0; z=0}' \
             | sort -u | awk '{printf "-Z %s ", $0}')
    raw="$LOGDIR/kanilist-$fam.raw"
    # shellcheck disable=SC2086
    ( cd "$DIR/../$fam" && timeout 900 cargo kani list $zflags ) > "$raw" 2>&1
    lrc=$?
    out="$LOGDIR/kanilist-$fam.txt"
    if [ $lrc -eq 0 ] && grep -qE '^\| *Total' "$raw"; then
        awk -F'|' 'NF>3 {gsub(/^[ \t]+|[ \t]+$/,"",$4);
                         if ($4 ~ /^[A-Za-z_][A-Za-z0-9_:]*$/ &&
                             $4!="Harness" && $4!="Harnesses" && $4!="Contract")
                             print $4}' "$raw" | sort -u > "$out"
        if [ -s "$out" ]; then
            LIST_FAMS="$LIST_FAMS $fam"
            cat "$out"; return 0
        fi
    fi
    LIST_BAD="$LIST_BAD $fam"
    echo "PREFLIGHT-SKIPPED $fam: 'cargo kani list $zflags' did not produce a" \
         "harness table (rc=$lrc, see $raw); relying on the kaniraw" \
         "postcondition" >&2
    return 1
}

n_total=0; n_ran=0; n_walled=0; n_failed=0; n_noflags=0
record() {  # family suite_h kani_h rc wall verdict status nraw
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$CENSUS"
}

while IFS=$'\t' read -r FAM H KHIN; do
    [ -z "${FAM:-}" ] && continue
    case "$FAM" in \#*) continue;; family) continue;; esac
    n_total=$((n_total+1))
    FLAGS=$(awk -F'\t' -v f="$FAM" -v h="$H" '$1==f && $2==h {print $3; exit}' "$SUITE")
    if [ -z "$FLAGS" ]; then
        record "$FAM" "$H" "-" "-" "0" "NO-SUITE-ROW" "NOFLAGS" "0"
        n_noflags=$((n_noflags+1))
        echo "NOFLAGS $FAM/$H: no SUITE.tsv row" >&2
        continue
    fi
    if [ -n "${KHIN:-}" ]; then KH="$KHIN"
    else case "$H" in *::*) KH="$H";; *) KH="proofs::$H";; esac; fi

    # ---- guard 1: name resolves before we spend a solve on it
    if [ "$PREFLIGHT" = "1" ]; then
        if NAMES=$(list_for "$FAM"); then
            if ! printf '%s\n' "$NAMES" | grep -qxF "$KH"; then
                SUG=$(printf '%s\n' "$NAMES" | grep -F "${KH##*::}" | head -3 | tr '\n' ' ')
                record "$FAM" "$H" "$KH" "-" "0" \
                    "NAME-UNRESOLVED${SUG:+ did-you-mean:${SUG% }}" "FAILED-TO-RUN" "0"
                n_failed=$((n_failed+1))
                echo "PREFLIGHT FAIL $FAM/$H: '$KH' is not a harness in this" \
                     "crate.${SUG:+ Candidates: $SUG}" >&2
                continue
            fi
        fi
    fi

    # ---- load gate: serial, load-aware capture (--load-gate MAX). Sleeps
    # while the 1-min load average exceeds MAX; solves on a contended host
    # only distort walls (walls are recorded with their measured timeout).
    if [ -n "$LOADGATE" ]; then
        while :; do
            L1=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}')
            [ -z "$L1" ] && break
            if awk -v l="$L1" -v m="$LOADGATE" 'BEGIN{exit !(l<=m)}'; then break; fi
            echo "LOADGATE: 1-min load $L1 > $LOADGATE; sleeping 60s before $FAM/$KH" >&2
            sleep 60
        done
    fi

    LOG="$LOGDIR/kanicov-$FAM-${KH//:/_}.log"
    MARK="$LOGDIR/.kanicov-mark-$$"
    : > "$MARK"
    case "$FLAGS" in *--exact*) EX="";; *) EX="--exact";; esac
    start=$(date +%s)
    # shellcheck disable=SC2086
    ( cd "$DIR/../$FAM" && exec timeout "$T" cargo kani $FLAGS --coverage \
        -Z source-coverage --harness "$KH" $EX ) > "$LOG" 2>&1
    rc=$?
    end=$(date +%s)
    verdict=$(grep -Eo "VERIFICATION:- [A-Z]+" "$LOG" | tail -1)
    verdict=${verdict:-NO-VERDICT}

    # ---- guard 2: a NEW kaniraw naming this harness must exist
    BASE=${KH##*::}
    NRAW=$(find "$DIR/../$FAM/target/kani" -name "*_kaniraw.json" -newer "$MARK" 2>/dev/null \
           | grep -cE "[0-9]${BASE}_kaniraw\.json$")
    rm -f "$MARK"

    if [ "$rc" -eq 124 ]; then
        record "$FAM" "$H" "$KH" "$rc" "$((end-start))" "WALLED-TIMEOUT-${T}s" "WALLED" "$NRAW"
        n_walled=$((n_walled+1))
        echo "WALLED $FAM/$KH after ${T}s (log: $LOG)" >&2
    elif [ "$rc" -ne 0 ] || [ "$NRAW" -eq 0 ]; then
        why="$verdict"
        [ "$NRAW" -eq 0 ] && why="$why+NO-KANIRAW"
        record "$FAM" "$H" "$KH" "$rc" "$((end-start))" "$why" "FAILED-TO-RUN" "$NRAW"
        n_failed=$((n_failed+1))
        echo "FAILED-TO-RUN $FAM/$KH rc=$rc $why (log: $LOG)" >&2
        grep -E "error(\[|:)|Failed to match|CBMC failed|Failed Checks|out of memory" \
            "$LOG" | head -3 | sed 's/^/    /' >&2
    else
        record "$FAM" "$H" "$KH" "$rc" "$((end-start))" "$verdict" "RAN" "$NRAW"
        n_ran=$((n_ran+1))
    fi
done < "$JOBS"

acct=$((n_ran+n_walled+n_failed+n_noflags))
echo
echo "== run-kani-coverage census =="
echo "  jobs considered:            $n_total"
echo "  ran (kaniraw produced):     $n_ran"
echo "  walled (timeout ${T}s):     $n_walled"
echo "  failed to run (UNMEASURED): $n_failed"
echo "  no SUITE row:               $n_noflags"
echo "  census: $CENSUS"
if [ "$acct" -ne "$n_total" ]; then
    echo
    echo "CENSUS FAIL: ran($n_ran)+walled($n_walled)+failed($n_failed)+noflags($n_noflags)"
    echo "  = $acct != $n_total jobs — dispositions were lost; this run certifies nothing."
    exit 1
fi
if [ $((n_walled+n_failed+n_noflags)) -gt 0 ]; then
    echo
    echo "INCOMPLETE CAPTURE: $((n_walled+n_failed+n_noflags)) of $n_total jobs produced"
    echo "  no coverage. Those are UNMEASURED, not uncovered. merge-coverage.py will"
    echo "  refuse a summary unless each is waived by name in --allow-unmeasured."
    exit 1
fi
echo "  all $n_total jobs measured."
exit 0
