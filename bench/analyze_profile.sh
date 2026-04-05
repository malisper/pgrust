#!/bin/bash
# Analyze dtrace stack samples — shows self-time by function.
# Usage: ./analyze_profile.sh [stacks_file]
# Default: /tmp/dtrace_stacks.out

FILE="${1:-/tmp/dtrace_stacks.out}"

echo "=== Self-time by function ==="
awk '
BEGIN { top = "" }
/^$/ {
    if (top != "" && count > 0) { self_samples[top] += count; total += count }
    top = ""; count = 0; next
}
/^[[:space:]]+[0-9]+$/ { count = $1; next }
/`/ { if (top == "") { gsub(/^[[:space:]]+/, ""); sub(/\+0x.*/, ""); top = $0 } }
END {
    for (fn in self_samples) printf "%6d %5.1f%%  %s\n", self_samples[fn], self_samples[fn]*100/total, fn
}
' "$FILE" | sort -rn | head -30 | awk '
BEGIN { cumulative = 0 }
{
    pct = $2
    gsub(/%/, "", pct)
    cumulative += pct
    printf "%s %s  %7.1f%%  %s\n", $1, $2, cumulative, substr($0, index($0,$3))
}' | awk '{ printf "%-8s %-8s %-10s %s\n", $1, $2, $3, $4 }'

echo ""
echo "=== Syscall callers ==="
awk '
/^$/ {
    if (top ~ /libsystem_kernel/ && caller != "") callers[top " <- " caller] += count
    top=""; caller=""; count=0; line=0; next
}
/^[[:space:]]+[0-9]+$/ { count=$1; next }
/`/ {
    gsub(/^[[:space:]]+/, "")
    sub(/\+0x.*/, "")
    line++
    if (line == 1) top = $0
    if (line == 2) caller = $0
}
END {
    for (c in callers) print callers[c], c
}
' "$FILE" | sort -rn | head -15
