#!/usr/bin/env bash
# run-suite.sh — unified proof-suite runner for proofs/SUITE.tsv.
#
# Usage:   ./run-suite.sh <tier>   tier in {per-commit, release-gate, all,
#                                           measure}
#
# Tier selection (rows are picked by the `tier` column of SUITE.tsv, except
# `measure`, which selects by the `expected` column):
#   per-commit    tier == per-commit, plus every defect-witness row
#                 (the must-fail negative controls guard against a
#                 vacuous rig, so they ride along with every gate).
#   release-gate  per-commit + release-gate + defect-witness rows.
#   all           every GATING row, including calibration. Rows with
#                 expected=unmeasured (the dark-harness sweep) are NOT run
#                 by `all` — 700+ harnesses of unknown solve time do not
#                 belong in a gate; they are counted and reported only.
#   measure       ONLY rows with expected=unmeasured, any tier: the opt-in
#                 dark-harness measurement sweep. Runs each harness, records
#                 the real verdict as `unmeasured-<verdict>`, and NEVER
#                 gates (exit code ignores these rows). Greens are emitted
#                 as promotion candidates (see below).
#
# MEMORY PROTOCOL (mandatory): harnesses run STRICTLY SERIALLY — exactly one
# kani/cbmc solve at a time. Each harness is wrapped in `timeout` plus an RSS
# watchdog that polls the harness process tree every 15s and kills it if the
# tree's summed RSS exceeds 6 GiB. Never parallelize this script and never
# run it while another kani job is live on the box.
#
# Outputs a human scoreboard on stdout and machine-readable rows to
# proofs/suite-results.tsv. Exit status is nonzero if any green-expected
# harness fails/times out, if any must-fail harness verifies
# SUCCESSFUL (vacuity — a broken gate, the worst outcome), or if any row
# carries a tier outside the vocabulary (BAD-MANIFEST-TIER: such a row is
# selected by no tier and would otherwise be dropped silently).
#
# Outcome vocabulary (suite-results.tsv `outcome` column):
#   pass              expected green, VERIFICATION:- SUCCESSFUL
#   fail              expected green, VERIFICATION:- FAILED / no verdict
#   expected-fail-ok  must-fail control produced VERIFICATION:- FAILED
#   vacuous-pass      must-fail control verified SUCCESSFUL (BROKEN GATE)
#   timeout           killed by `timeout`
#   rss-kill          killed by the 6 GiB RSS watchdog
#   wall-ok           wall-recorded harness; any terminal outcome is
#                     informational (recorded, never gates)
#   skipped-missing   expected=missing row (adjudicated-absent harness,
#                     check-suite-names.py rule 2): skipped WITHOUT invoking
#                     kani — `--harness` on a nonexistent name can only
#                     fail. Never affects the exit code.
#   unmeasured-*      expected=unmeasured row under `measure`: the real
#                     verdict, recorded but never gating —
#                     unmeasured-green / unmeasured-failed /
#                     unmeasured-timeout / unmeasured-rss-kill.
#                     unmeasured-green rows are appended to
#                     proofs/suite-promotion-candidates.tsv with their
#                     measured wall time: candidates for promotion to
#                     expected=green with a tier from the measured time.
#                     Failures are the triage queue.
#
# "CBMC failed with status 15" is a killed solver, not a verdict
# (see text-cmp/run-all.sh); such runs are retried once automatically.

set -u

PROOFS_DIR=$(cd "$(dirname "$0")" && pwd)
# SUITE_TSV / RESULTS_TSV overridable from the environment for rig testing
# (run a hand-built manifest without touching the real one).
SUITE_TSV="${SUITE_TSV:-$PROOFS_DIR/SUITE.tsv}"
RESULTS_TSV="${RESULTS_TSV:-$PROOFS_DIR/suite-results.tsv}"
PROMO_TSV="${PROMO_TSV:-$PROOFS_DIR/suite-promotion-candidates.tsv}"

RSS_LIMIT_KB=$((6 * 1024 * 1024))   # 6 GiB, in KiB as reported by ps -o rss=
RSS_POLL_S=15
DEFAULT_TIMEOUT_S=600               # harnesses with no documented time
MIN_TIMEOUT_S=60
MAX_TIMEOUT_S=900

usage() {
    echo "usage: $0 <per-commit|release-gate|all|measure>" >&2
    exit 2
}

[ $# -eq 1 ] || usage
TIER="$1"
case "$TIER" in
    per-commit|release-gate|all|measure) ;;
    *) usage ;;
esac

command -v timeout >/dev/null 2>&1 || {
    echo "FATAL: 'timeout' not found on PATH (install coreutils)" >&2
    exit 2
}
[ -f "$SUITE_TSV" ] || {
    echo "FATAL: $SUITE_TSV not found" >&2
    exit 2
}

# Known tier vocabulary. A tier value outside this set is a HARD ERROR, not a
# silent omission: an unrecognized tier is matched by no arm of row_selected()
# below, so the row would be selected by no tier and never run. That is how
# eight must-fail negative controls (tier=control) sat silently disabled while
# every gate reported green — the vacuity guards were not in any gate at all.
# See proofs/lint-suite-rows.py (check TIER) for the authoring-time version.
tier_known() { # $1 = row tier
    case "$1" in
        per-commit|release-gate|calibration|defect-witness|unmeasured) return 0 ;;
        *) return 1 ;;
    esac
}

# Does a manifest row belong to the requested run? Gating tiers select by
# the `tier` column and EXCLUDE expected=unmeasured rows (700+ dark
# harnesses of unknown solve time never ride a gate); `measure` selects
# EXACTLY the expected=unmeasured rows, whatever their tier column says.
row_selected() { # $1 = row tier, $2 = row expected
    if [ "$TIER" = measure ]; then
        [ "$2" = unmeasured ]
        return
    fi
    [ "$2" = unmeasured ] && return 1
    case "$TIER" in
        per-commit)   [ "$1" = per-commit ] || [ "$1" = defect-witness ] ;;
        release-gate) [ "$1" = per-commit ] || [ "$1" = release-gate ] \
                          || [ "$1" = defect-witness ] ;;
        all)          true ;;
    esac
}

# Recursively collect a pid and all its descendants.
descendants() { # $1 = pid
    echo "$1"
    local child
    for child in $(pgrep -P "$1" 2>/dev/null); do
        descendants "$child"
    done
}

# Summed RSS (KiB) of a process tree.
tree_rss_kb() { # $1 = root pid
    local pids total
    pids=$(descendants "$1")
    # shellcheck disable=SC2086
    total=$(ps -o rss= -p $(echo $pids | tr ' ' ',') 2>/dev/null \
                | awk '{s+=$1} END {print s+0}')
    echo "${total:-0}"
}

# Run one harness under timeout + RSS watchdog, strictly in the foreground
# of this loop (serial). Sets: RUN_RC, RUN_WALL, RUN_RSSKILL, RUN_OUT (file).
run_one() { # $1 = crate dir, $2 = flags string, $3 = harness, $4 = timeout_s
    local dir="$1" flags="$2" harness="$3" tmo="$4"
    RUN_OUT=$(mktemp "${TMPDIR:-/tmp}/suite-harness.XXXXXX")
    RUN_RSSKILL=0
    local t0 t1 pid
    t0=$(date +%s)
    (
        cd "$dir" || exit 97
        # shellcheck disable=SC2086
        exec timeout "$tmo" cargo kani $flags --harness "$harness"
    ) >"$RUN_OUT" 2>&1 &
    pid=$!
    # RSS watchdog: poll the harness process tree every $RSS_POLL_S seconds.
    while kill -0 "$pid" 2>/dev/null; do
        local waited=0
        while [ "$waited" -lt "$RSS_POLL_S" ]; do
            kill -0 "$pid" 2>/dev/null || break 2
            sleep 1
            waited=$((waited + 1))
        done
        local rss
        rss=$(tree_rss_kb "$pid")
        if [ "$rss" -gt "$RSS_LIMIT_KB" ]; then
            RUN_RSSKILL=1
            # Kill the whole harness tree (kani drives child cbmc/kissat
            # processes; the root pid alone is not enough).
            # shellcheck disable=SC2046
            kill -KILL $(descendants "$pid") 2>/dev/null
            break
        fi
    done
    wait "$pid"
    RUN_RC=$?
    t1=$(date +%s)
    RUN_WALL=$((t1 - t0))
}

# ---------------------------------------------------------------------------

printf 'family\tharness\ttier\texpected\toutcome\twall_s\tverdict\n' \
    >"$RESULTS_TSV"

n_pass=0 n_fail=0 n_xfail_ok=0 n_vacuous=0 n_timeout=0 n_rsskill=0 n_wall=0
n_missing_crate=0 n_bad_tier=0 n_skipped_missing=0 n_dark_skipped=0
n_unm_green=0 n_unm_notgreen=0
suite_rc=0

if [ "$TIER" = measure ]; then
    printf 'family\tharness\twall_s\n' >"$PROMO_TSV"
fi

echo "== proof suite: tier=$TIER  (strictly serial; RSS cap 6 GiB) =="

while IFS=$'\t' read -r family harness flags expected tier time_s notes; do
    # Skip header and blank/comment lines.
    [ -n "${family:-}" ] || continue
    case "$family" in family|\#*) continue ;; esac

    # A tier outside the vocabulary belongs to no gate tier: fail loudly here
    # rather than dropping the row on the floor (see tier_known()).
    if ! tier_known "$tier"; then
        echo "BAD-MANIFEST-TIER  $family/$harness tier='$tier' matches no" \
             "selection arm — the row would run in NO tier" >&2
        n_bad_tier=$((n_bad_tier + 1))
        suite_rc=1
        continue
    fi

    # Dark rows (expected=unmeasured) are excluded from every gating tier;
    # count what `all` leaves behind so the omission is visible, not silent.
    if [ "$TIER" = all ] && [ "$expected" = unmeasured ]; then
        n_dark_skipped=$((n_dark_skipped + 1))
    fi

    row_selected "$tier" "$expected" || continue

    # expected=missing: adjudicated-absent harness (owned by
    # check-suite-names.py rule 2 — it ERRORS if the name starts resolving).
    # Skip WITHOUT invoking kani: `--harness` on a nonexistent name can only
    # fail. Recorded, never gates.
    if [ "$expected" = missing ]; then
        outcome=skipped-missing
        n_skipped_missing=$((n_skipped_missing + 1))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$family" "$harness" "$tier" "$expected" "$outcome" 0 \
            "NOT-RUN (adjudicated-absent harness)" >>"$RESULTS_TSV"
        printf '%-16s %-42s %-16s %4ss  %s\n' \
            "$family" "$harness" "$outcome" 0 "NOT-RUN"
        continue
    fi

    crate_dir="$PROOFS_DIR/$family"
    if [ ! -d "$crate_dir" ]; then
        echo "MISSING-CRATE  $family/$harness (skipped)"
        n_missing_crate=$((n_missing_crate + 1))
        suite_rc=1
        continue
    fi

    # Per-harness timeout: 10x the documented solve time, clamped to
    # [MIN_TIMEOUT_S, MAX_TIMEOUT_S]; DEFAULT_TIMEOUT_S when unmeasured.
    case "$time_s" in
        ''|'?') tmo=$DEFAULT_TIMEOUT_S ;;
        *)
            tmo=$(awk -v t="$time_s" -v lo="$MIN_TIMEOUT_S" \
                      -v hi="$MAX_TIMEOUT_S" \
                      'BEGIN { x = int(t*10)+1; if (x<lo) x=lo;
                               if (x>hi) x=hi; print x }')
            ;;
    esac

    attempt=1
    while :; do
        run_one "$crate_dir" "$flags" "$harness" "$tmo"
        # Killed-solver artifact, not a verdict: retry once.
        if grep -q "CBMC failed with status 15" "$RUN_OUT" \
                && [ "$attempt" -eq 1 ] && [ "$RUN_RSSKILL" -eq 0 ]; then
            echo "RETRY  $family/$harness (CBMC status 15 = killed solver)"
            rm -f "$RUN_OUT"
            attempt=2
            continue
        fi
        break
    done

    verdict=$(grep 'VERIFICATION:' "$RUN_OUT" | tail -1 | tr -d '\t')
    ok=0;  grep -q 'VERIFICATION:- SUCCESSFUL' "$RUN_OUT" && ok=1
    bad=0; grep -q 'VERIFICATION:- FAILED'     "$RUN_OUT" && bad=1
    rm -f "$RUN_OUT"

    outcome=""
    if [ "$RUN_RSSKILL" -eq 1 ]; then
        outcome=rss-kill
    elif [ "$RUN_RC" -eq 124 ] && [ "$ok" -eq 0 ] && [ "$bad" -eq 0 ]; then
        outcome=timeout
    fi

    case "$expected" in
        green)
            if [ -z "$outcome" ]; then
                if [ "$ok" -eq 1 ]; then outcome=pass; else outcome=fail; fi
            fi
            case "$outcome" in
                pass) n_pass=$((n_pass + 1)) ;;
                timeout) n_timeout=$((n_timeout + 1)); suite_rc=1 ;;
                rss-kill) n_rsskill=$((n_rsskill + 1)); suite_rc=1 ;;
                *) n_fail=$((n_fail + 1)); suite_rc=1 ;;
            esac
            ;;
        must-fail)
            # The control must produce a FAILED verdict. A SUCCESSFUL
            # verdict means the rig can no longer see the planted
            # divergence: vacuity, a broken gate.
            if [ -z "$outcome" ]; then
                if [ "$bad" -eq 1 ]; then
                    outcome=expected-fail-ok
                elif [ "$ok" -eq 1 ]; then
                    outcome=vacuous-pass
                else
                    outcome=fail
                fi
            fi
            case "$outcome" in
                expected-fail-ok) n_xfail_ok=$((n_xfail_ok + 1)) ;;
                vacuous-pass) n_vacuous=$((n_vacuous + 1)); suite_rc=1 ;;
                timeout) n_timeout=$((n_timeout + 1)); suite_rc=1 ;;
                rss-kill) n_rsskill=$((n_rsskill + 1)); suite_rc=1 ;;
                *) n_fail=$((n_fail + 1)); suite_rc=1 ;;
            esac
            ;;
        wall-recorded)
            # Informational: recorded, never gates. A wall harness that
            # suddenly solves is worth a look but not a failure.
            outcome=wall-ok
            n_wall=$((n_wall + 1))
            ;;
        unmeasured)
            # Dark harness under `measure`: record the REAL verdict, never
            # gate. Green => promotion candidate (promote the row to
            # expected=green with a tier from the measured wall time);
            # anything else => triage queue. suite_rc untouched.
            if [ -z "$outcome" ]; then
                if [ "$ok" -eq 1 ]; then
                    outcome=unmeasured-green
                else
                    outcome=unmeasured-failed
                fi
            else
                outcome=unmeasured-$outcome   # -timeout / -rss-kill
            fi
            if [ "$outcome" = unmeasured-green ]; then
                n_unm_green=$((n_unm_green + 1))
                printf '%s\t%s\t%s\n' "$family" "$harness" "$RUN_WALL" \
                    >>"$PROMO_TSV"
            else
                n_unm_notgreen=$((n_unm_notgreen + 1))
            fi
            ;;
        *)
            echo "BAD-MANIFEST-ROW  $family/$harness expected='$expected'"
            outcome=fail
            n_fail=$((n_fail + 1))
            suite_rc=1
            ;;
    esac

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$family" "$harness" "$tier" "$expected" "$outcome" "$RUN_WALL" \
        "${verdict:-NO-VERDICT}" >>"$RESULTS_TSV"
    printf '%-16s %-42s %-16s %4ss  %s\n' \
        "$family" "$harness" "$outcome" "$RUN_WALL" "${verdict:-NO-VERDICT}"
done <"$SUITE_TSV"

echo
echo "== scoreboard (tier=$TIER) =="
echo "  pass:              $n_pass"
echo "  fail:              $n_fail"
echo "  expected-fail-ok:  $n_xfail_ok"
echo "  vacuous-pass:      $n_vacuous   (must-fail control verified: BROKEN GATE)"
echo "  timeout:           $n_timeout"
echo "  rss-kill:          $n_rsskill"
echo "  wall-recorded:     $n_wall"
[ "$n_skipped_missing" -gt 0 ] && echo "  skipped-missing:   $n_skipped_missing   (adjudicated-absent harnesses; never run, never gate)"
[ "$n_dark_skipped" -gt 0 ] && echo "  dark rows skipped: $n_dark_skipped   (expected=unmeasured; run them with: $0 measure)"
[ "$n_missing_crate" -gt 0 ] && echo "  missing-crate:     $n_missing_crate"
[ "$n_bad_tier" -gt 0 ] && echo "  bad-manifest-tier: $n_bad_tier   (row in NO gate tier: BROKEN GATE)"
if [ "$TIER" = measure ]; then
    echo "  unmeasured-green:  $n_unm_green   (promotion candidates)"
    echo "  unmeasured-other:  $n_unm_notgreen   (failed/timeout/rss-kill: triage queue)"
    echo
    echo "== promotion candidates =="
    echo "  $n_unm_green harness(es) solved green; promote to expected=green"
    echo "  with a tier from the measured wall time. List: $PROMO_TSV"
fi
echo "  results:           $RESULTS_TSV"
exit "$suite_rc"
