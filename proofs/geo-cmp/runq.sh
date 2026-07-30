#!/bin/bash
# runq.sh <cap> <extra-args...> -- <harness...>: serial run-one.sh loop, verdict summary
CAP=$1; shift
EXTRA=()
while [ "$1" != "--" ]; do EXTRA+=("$1"); shift; done
shift
for H in "$@"; do
  ./run-one.sh "ext2::proofs_ext2::$H" "$CAP" "${EXTRA[@]}" >/dev/null 2>&1
  RC=$?
  LOG="/tmp/geo-cmp-logs/ext2::proofs_ext2::$H.log"
  V=$(grep -E "VERIFICATION:" "$LOG" | tail -1)
  T=$(grep -E "Verification Time:" "$LOG" | tail -1)
  echo "$H rc=$RC ${V:-NO-VERDICT} ${T:-}"
done
