#!/usr/bin/env bash
# Run deterministic SQLancer seeds and preserve per-seed logs for triage.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACT_ROOT="${PGRUST_SQLANCER_TRIAGE_DIR:-${TMPDIR:-/tmp}/pgrust-sqlancer-triage-$(date -u +%Y%m%dT%H%M%SZ)}"
BASE_PORT="${PGRUST_SQLANCER_TRIAGE_BASE_PORT:-55433}"
SEED_START="${PGRUST_SQLANCER_TRIAGE_SEED_START:-1}"
SEED_COUNT="${PGRUST_SQLANCER_TRIAGE_SEED_COUNT:-5}"
SUMMARY_FILE="$ARTIFACT_ROOT/summary.tsv"

if [[ $# -gt 0 ]]; then
    SEEDS=("$@")
elif [[ -n "${PGRUST_SQLANCER_SEEDS:-}" ]]; then
    # shellcheck disable=SC2206
    SEEDS=(${PGRUST_SQLANCER_SEEDS})
else
    SEEDS=()
    for ((i = 0; i < SEED_COUNT; i++)); do
        SEEDS+=("$((SEED_START + i))")
    done
fi

mkdir -p "$ARTIFACT_ROOT"
printf "seed\tstatus\texit_code\tblocker\tartifact_dir\n" >"$SUMMARY_FILE"

extract_blocker() {
    local log_file="$1"
    awk '
        /^java[.]lang[.]AssertionError:/ {
            sub(/^java[.]lang[.]AssertionError: */, "")
            print
            exit
        }
        /^Caused by:/ {
            sub(/^Caused by: */, "")
            print
            exit
        }
        /^pgrust server / {
            print
            exit
        }
        /^ERROR:/ {
            print
            exit
        }
    ' "$log_file" | tr '\t' ' '
}

echo "Writing SQLancer triage artifacts to $ARTIFACT_ROOT"
echo "Summary: $SUMMARY_FILE"

index=0
for seed in "${SEEDS[@]}"; do
    seed_dir="$ARTIFACT_ROOT/seed-$seed"
    data_dir="$seed_dir/data"
    server_log="$seed_dir/server.log"
    sqlancer_log="$seed_dir/sqlancer.log"
    blocker_file="$seed_dir/blocker.txt"
    port="$((BASE_PORT + index))"
    index="$((index + 1))"

    rm -rf "$seed_dir"
    mkdir -p "$seed_dir"

    echo "== SQLancer seed $seed on port $port =="
    (
        cd "$PGRUST_DIR" || exit 1
        PGRUST_SQLANCER_SEED="$seed" \
            PGRUST_SQLANCER_PORT="$port" \
            PGRUST_SQLANCER_DATA_DIR="$data_dir" \
            PGRUST_SQLANCER_LOG_FILE="$server_log" \
            ./scripts/run_sqlancer_smoke.sh
    ) >"$sqlancer_log" 2>&1
    exit_code="$?"

    if [[ "$exit_code" -eq 0 ]]; then
        status="pass"
        blocker=""
        : >"$blocker_file"
    else
        status="fail"
        blocker="$(extract_blocker "$sqlancer_log")"
        if [[ -z "$blocker" ]]; then
            blocker="see sqlancer.log"
        fi
        printf "%s\n" "$blocker" >"$blocker_file"
    fi

    printf "%s\t%s\t%s\t%s\t%s\n" "$seed" "$status" "$exit_code" "$blocker" "$seed_dir" >>"$SUMMARY_FILE"
    echo "seed $seed: $status ($blocker)"
done

echo "Done. Summary: $SUMMARY_FILE"
