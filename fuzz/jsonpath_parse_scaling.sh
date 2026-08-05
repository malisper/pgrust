#!/usr/bin/env bash
# jsonpath parse-cost scaling law, REAL PostgreSQL side.
#
# Companion to the two #[ignore]d tests in core/src/jsonpath_diff.rs
# (timing_slow_unit_attribution, timing_scaling_family / timing_files), which
# measure the SAME synthetic family against the in-harness C oracle (vendored
# 18.3) and shipped Rust. This script measures real PostgreSQL 18.3 in docker
# so the three engines are comparable on identical bytes.
#
# Usage:
#   fuzz/jsonpath_parse_scaling.sh [container]     # default: laneaa-pg183
#   docker run -d --rm --name laneaa-pg183 -e POSTGRES_HOST_AUTH_METHOD=trust \
#       postgres:18.3
#
# Family: $ ? (@ like_regex "(<UNIT>{N})+") where UNIT is the repeated unit
# minimized out of fleet slow-unit 899856ad3a5f72f09a52598b9bc434076004cd93
# (campaign pgrust-fuzz-campaign-1785518461-61c1-18958). The cost is
# PATTERN COMPILATION inside the parse (both makeItemLikeRegex in C and
# make_item_like_regex in Rust call pg_regcomp during parsing), so the growth
# below is a property of PostgreSQL's own regex compiler, paid by BOTH engines.
set -euo pipefail
CONTAINER=${1:-laneaa-pg183}
UNIT='^^^^|\\\\\?\^^^\\Y||pawt@r'

run_one() {
  local text="$1" label="$2"
  local b64
  b64=$(printf '%s' "$text" | base64 | tr -d '\n')
  {
    echo '\timing on'
    echo "select (convert_from(decode('$b64','base64'),'UTF8')::jsonpath) is not null;"
  } | docker exec -i "$CONTAINER" psql -q -U postgres -f - 2>&1 |
    awk -v l="$label" -v n="${#text}" '
      /^Time:/ { t=$2 }
      /ERROR/  { err=$0 }
      END { printf "%-16s len=%-6d PG=%10.3f ms  %s\n", l, n, t, (err==""?"ok":err) }'
}

echo "== real PostgreSQL 18.3 ($CONTAINER) =="
docker exec -i "$CONTAINER" psql -qtA -U postgres -c 'select version()'

for n in ${NS:-1 2 4 8 16 32 64 128 256 1024}; do
  pat=''
  for ((i = 0; i < n; i++)); do pat+="$UNIT"; done
  run_one "\$ ? (@ like_regex \"($pat)+\")" "N=$n"
done

# The original fleet slow-unit's exact source text (committed).
SLOW="$(dirname "$0")/testdata/jsonpath-slow/slow-unit-899856ad-text.bin"
[ -f "$SLOW" ] && run_one "$(cat "$SLOW")" "fleet-slow-unit"
