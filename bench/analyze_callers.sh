#!/bin/bash
# Show the immediate callers of a given function/syscall in dtrace stacks.
# Usage: ./analyze_callers.sh <pattern> [stacks_file]
# Example: ./analyze_callers.sh __open /tmp/dtrace_insert_stacks.out

PATTERN="${1:?Usage: ./analyze_callers.sh <pattern> [stacks_file]}"
FILE="${2:-/tmp/dtrace_insert_stacks.out}"

awk -v pattern="$PATTERN" '
/^$/ {
    if (top ~ pattern && caller != "") callers[caller] += count
    top = ""; caller = ""; count = 0; line = 0; next
}
/^[[:space:]]+[0-9]+$/ { count = $1; next }
/`/ {
    gsub(/^[[:space:]]+/, "")
    sub(/\+0x.*/, "")
    line++
    if (line == 1) top = $0
    if (line == 2) caller = $0
}
END {
    for (c in callers) printf "%6d  %s\n", callers[c], c
}
' "$FILE" | sort -rn | head -15
