#!/usr/bin/env bash
# Merge per-shard fleet measure-sweep results into one TSV + promotion file,
# with a completeness floor: exactly one row per requested dark harness
# (gate-blindness law: a shard that produced no output is RED, not skipped).
#
# usage: SHA=<full-sha> ./merge-results.sh <job1> <job2> ...
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROOFS=$(cd "$HERE/.." && pwd)
SHA="${SHA:?set SHA=<full 40-char sha>}"
BUCKET="${BUCKET:-pgrust-fleet-results-149051628381}"
PROFILE="${AWS_PROFILE_OPT:-mfa}"
OUT="$HERE/merged-results.tsv"
PROMO="$HERE/suite-promotion-candidates.tsv"
WANT="$HERE/.want-rows.tsv"

# The requested set: every expected=unmeasured row of SUITE.tsv.
awk -F'\t' '$1!="family" && $1!~/^#/ && $4=="unmeasured" {print $1"\t"$2}' \
    "$PROOFS/SUITE.tsv" | sort > "$WANT"
N_WANT=$(wc -l < "$WANT" | tr -d ' ')

printf 'family\tharness\toutcome\twall_s\ttimeout_s\tverdict\tjob\n' > "$OUT"
printf 'family\tharness\twall_s\n' > "$PROMO"

rc=0
for job in "$@"; do
    d="$HERE/shards/$job"
    mkdir -p "$d"
    aws s3 cp --profile "$PROFILE" --only-show-errors \
        "s3://$BUCKET/kani-suite/$SHA/$job/suite-results.tsv" "$d/" || true
    if [ ! -s "$d/suite-results.tsv" ]; then
        echo "RED: shard $job produced NO suite-results.tsv (gate-blindness law)" >&2
        rc=1
        continue
    fi
    aws s3 cp --profile "$PROFILE" --only-show-errors \
        "s3://$BUCKET/kani-suite/$SHA/$job/suite-promotion-candidates.tsv" "$d/" || true
    aws s3 cp --profile "$PROFILE" --only-show-errors \
        "s3://$BUCKET/kani-suite/$SHA/$job/telemetry-summary.txt" "$d/" || true
    # results columns: family harness tier expected outcome wall_s timeout_s verdict log
    awk -F'\t' -v OFS='\t' -v job="$job" \
        'NR>1 {print $1,$2,$5,$6,$7,$8,job}' "$d/suite-results.tsv" >> "$OUT"
    [ -s "$d/suite-promotion-candidates.tsv" ] && \
        tail -n +2 "$d/suite-promotion-candidates.tsv" >> "$PROMO"
done

# Completeness floor: one row per requested harness, no dupes, no strays.
GOT="$HERE/.got-rows.tsv"
tail -n +2 "$OUT" | cut -f1,2 | sort > "$GOT"
if ! diff "$WANT" "$GOT" > "$HERE/.completeness.diff"; then
    echo "RED: completeness violation (requested vs merged keys):" >&2
    head -40 "$HERE/.completeness.diff" >&2
    rc=1
fi
N_GOT=$(wc -l < "$GOT" | tr -d ' ')

echo "== measure sweep merge =="
echo "requested dark rows: $N_WANT   merged result rows: $N_GOT"
tail -n +2 "$OUT" | cut -f3 | sort | uniq -c | sort -rn
echo "promotion candidates (unmeasured-green): $(( $(wc -l < "$PROMO") - 1 ))"
exit "$rc"
