#!/bin/bash
# Show full call stacks for a given syscall/function pattern.
# Usage: ./analyze_stacks.sh <pattern> [stacks_file] [max_stacks]
# Example: ./analyze_stacks.sh __open
#          ./analyze_stacks.sh "write$" /tmp/dtrace_insert_stacks.out 10

PATTERN="${1:?Usage: ./analyze_stacks.sh <pattern> [stacks_file] [max_stacks]}"
FILE="${2:-/tmp/dtrace_stacks.out}"
MAX="${3:-5}"

awk -v pattern="$PATTERN" -v max="$MAX" '
/^$/ {
    if (top ~ pattern && stack != "") {
        print count, stack
        print "---"
        printed++
    }
    top=""; stack=""; count=0; next
}
/^[[:space:]]+[0-9]+$/ { count=$1; next }
/`/ {
    gsub(/^[[:space:]]+/, "")
    sub(/\+0x.*/, "")
    if (top == "") top = $0
    stack = stack "\n  " $0
}
END {
    if (printed == 0) print "No stacks matching pattern: " pattern
}
' "$FILE" | head -$(( (MAX + 1) * 20 ))
