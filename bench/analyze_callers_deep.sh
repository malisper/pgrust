#!/bin/bash
# Show the caller chain (up to 3 levels) for a given function/syscall.
# Usage: ./analyze_callers_deep.sh <pattern> [stacks_file] [depth]
# Example: ./analyze_callers_deep.sh __open /tmp/dtrace_insert_stacks.out 3

PATTERN="${1:?Usage: ./analyze_callers_deep.sh <pattern> [stacks_file] [depth]}"
FILE="${2:-/tmp/dtrace_insert_stacks.out}"
DEPTH="${3:-3}"

awk -v pattern="$PATTERN" -v depth="$DEPTH" '
/^$/ {
    if (top ~ pattern && chain != "") chains[chain] += count
    top = ""; chain = ""; count = 0; line = 0; next
}
/^[[:space:]]+[0-9]+$/ { count = $1; next }
/`/ {
    gsub(/^[[:space:]]+/, "")
    sub(/\+0x.*/, "")
    line++
    if (line == 1) top = $0
    if (line >= 2 && line <= depth + 1) {
        if (chain != "") chain = chain " <- "
        chain = chain $0
    }
}
END {
    for (c in chains) printf "%6d  %s\n", chains[c], c
}
' "$FILE" | sort -rn | head -15
