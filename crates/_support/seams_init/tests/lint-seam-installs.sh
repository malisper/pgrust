#!/usr/bin/env bash
# lint-seam-installs.sh — CI differ for the seam-install bug class
# (notes/seam-audit.md, branch audit/seam-installs).
#
# THE BUG CLASS THIS WOULD HAVE CAUGHT: the subtrans boot bug — a crate ports
# a C function (StartupSUBTRANS/CheckPointSUBTRANS/TruncateSUBTRANS), exports
# it, a seam! slot exists for it, but nothing ever ::set()s the slot, so every
# is_installed()-guarded caller silently skips FOREVER (StartupSUBTRANS was
# skipped on every boot until fix/subtrans-restore-boot). The same sweep also
# caught count_user_backends (unguarded call() → panic on any CONNECTION
# LIMIT role login; fixed by the seamfix lane).
#
# What it re-derives (the audit's core diff), mechanically, no compilation:
#   CHECK 1 — PORTED-NOT-INSTALLED: a seam! slot with NO production ::set()
#       anywhere whose name matches an exported production `pub fn` (name
#       normalization: lowercase, underscores stripped — startup_subtrans ==
#       StartupSUBTRANS). This is the exact subtrans class. FAIL unless the
#       slot is allowlisted (reason should reference an in-flight fix).
#   CHECK 2 — UNINSTALLED-REFERENCED: a seam! slot with NO production ::set()
#       but WITH production call()/call_if()/is_installed() references — an
#       unported-feature slot that either panics on use or silently skips.
#       Every such slot must be classified in the allowlist (the audit's
#       INTENTIONAL / DEAD / SKIP / PANIC-ON-USE table seeds it). A NEW entry
#       here means someone added a caller for an unported seam: classify it
#       deliberately or port+install it.
#   CHECK 3 — INIT-SEAMS-ORPHAN: a crate defines a NON-EMPTY init_seams()
#       that no production code invokes (seams_init::init_all() is the
#       closure) — the whole-crate variant of the class.
#
# Production code = crates/**/*.rs minus */tests/* dirs, *test* basenames,
# */benches/*, and brace-matched `#[cfg(test)]` items (same exclusions as the
# audit). tap! slots are exempt (optional consumers by design; call_if no-ops).
# Known limitation (from the audit): slot names are tracked unqualified, so
# same-named slots in two seam crates share one installed/uninstalled verdict
# (all 5 existing collisions are installed in production).
#
# Allowlist: crates/_support/seams_init/tests/lint-seam-installs.allow (override: LINT_SEAM_ALLOWLIST).
# Tab-separated: <slot | crate:name> <class> <reason...>. Stale entries (no
# longer violations) WARN but do not fail. Run with
# LINT_SEAM_ALLOWLIST=/dev/null to see every open instance.
#
# Standalone:   crates/_support/seams_init/tests/lint-seam-installs.sh          (exit 0 = clean)
# Unit-shaped:  cargo test -p seams_init --test lint_seam_installs
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
ALLOWLIST="${LINT_SEAM_ALLOWLIST:-$REPO/crates/_support/seams_init/tests/lint-seam-installs.allow}"
SEAMS_INIT="$REPO/crates/_support/seams_init/src/lib.rs"
[ -f "$SEAMS_INIT" ] || { echo "lint-seam-installs: $SEAMS_INIT not found"; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/lint-seam-installs.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT INT TERM

# --- production file list ------------------------------------------------
find "$REPO/crates" -name '*.rs' \
    ! -path '*/tests/*' ! -path '*/benches/*' \
    ! -name '*test*' \
    > "$TMP/files" || exit 2

# --- one awk pass: typed records ------------------------------------------
# DECL <slot> <file>     seam_core::seam!( ... pub fn <slot> ... )
# TAP  <slot> <file>     seam_core::tap!( ... )
# SET  <ident> <file>    <ident>::set( | <ident>::install(   (production)
# REF  <ident> <file>    <ident>::call( | ::call_if( | ::is_installed(
# FN   <name> <file>     pub [pub(crate)] fn <name> outside seam!/tap! blocks
# INIT <file>            non-empty `pub fn init_seams()` body
cat > "$TMP/scan.awk" <<'AWK'
    function emit_matches(line, re, tag,   s, m) {
        s = line
        while (match(s, re)) {
            m = substr(s, RSTART, RLENGTH)
            sub(/::.*/, "", m)
            if (m ~ /^[A-Za-z_][A-Za-z0-9_]*$/) print tag, m, FILENAME
            s = substr(s, RSTART + RLENGTH)
        }
    }
    function brace_delta(line,   t, d) {
        t = line; d = gsub(/\{/, "", t); d -= gsub(/\}/, "", t); return d
    }
    function paren_delta(line,   t, d) {
        t = line; d = gsub(/\(/, "", t); d -= gsub(/\)/, "", t); return d
    }
    FNR == 1 { skip = 0; pend = 0; inseam = 0; ininit = 0; initbody = 0 }
    {
        FILE = FILENAME
        line = $0
        sub(/\/\/.*/, "", line)   # strip line comments (string-safe enough here)

        # -- cfg(test) item skipping (brace-matched) --
        if (skip > 0) { skip += brace_delta(line); next }
        if (pend) {
            if (line ~ /\{/) { skip = brace_delta(line); pend = 0; if (skip <= 0) skip = 0; next }
            if (line ~ /;[ \t]*$/) { pend = 0; next }
            next
        }
        if (line ~ /^[ \t]*#\[cfg\(test\)\]/) { pend = 1; next }

        # -- seam!/tap! declaration blocks (paren-matched) --
        if (inseam) {
            if (inseam == 1 && match(line, /pub fn [A-Za-z_][A-Za-z0-9_]*/)) {
                n = substr(line, RSTART + 7, RLENGTH - 7)
                print declkind, n, FILE
                inseam = 2
            }
            seamdepth += paren_delta(line)
            if (seamdepth <= 0) inseam = 0
            next
        }
        if (match(line, /seam_core::(seam|tap)!\(/)) {
            declkind = (substr(line, RSTART, RLENGTH) ~ /tap/) ? "TAP" : "DECL"
            seamdepth = paren_delta(line)
            inseam = 1
            if (match(line, /pub fn [A-Za-z_][A-Za-z0-9_]*/)) {
                n = substr(line, RSTART + 7, RLENGTH - 7)
                print declkind, n, FILE
                inseam = 2
            }
            if (seamdepth <= 0) inseam = 0
            next
        }

        # -- non-empty init_seams() body detection --
        if (ininit) {
            initdepth += brace_delta(line)
            t = line; gsub(/[ \t{}]/, "", t)
            if (length(t) > 0) initbody = 1
            if (initdepth <= 0) { if (initbody) print "INIT", "-", FILE; ininit = 0 }
            # fall through: body lines still carry SET/REF records
        }
        if (line ~ /pub fn init_seams[ \t]*\(/) {
            ininit = 1; initbody = 0
            initdepth = brace_delta(line)
            t = line; sub(/.*\{/, "", t); gsub(/[ \t}]/, "", t)
            if (length(t) > 0) initbody = 1
            if (initdepth <= 0 && line ~ /\{/) { if (initbody) print "INIT", "-", FILE; ininit = 0 }
        }

        # -- installs / references / candidate impls --
        # (regexes as strings: a /literal/ passed as a function arg is $0~/re/)
        emit_matches(line, "[A-Za-z_][A-Za-z0-9_]*::(set|install)\\(", "SET")
        emit_matches(line, "[A-Za-z_][A-Za-z0-9_]*::(call|call_if|is_installed)\\(", "REF")
        if (match(line, /pub(\(crate\))? fn [A-Za-z_][A-Za-z0-9_]*/)) {
            n = substr(line, RSTART, RLENGTH)
            sub(/^pub(\(crate\))? fn /, "", n)
            if (n != "init_seams") print "FN", n, FILE
        }
    }
AWK
tr '\n' '\0' < "$TMP/files" | xargs -0 awk -f "$TMP/scan.awk" > "$TMP/records"

awk '$1=="DECL"{print $2}' "$TMP/records" | sort -u > "$TMP/decl"
awk '$1=="TAP"{print $2}'  "$TMP/records" | sort -u > "$TMP/tap"
awk '$1=="SET"{print $2}'  "$TMP/records" | sort -u > "$TMP/set"
awk '$1=="REF"{print $2}'  "$TMP/records" | sort -u > "$TMP/ref"
awk '$1=="FN"{print $2, $3}' "$TMP/records" | sort -u > "$TMP/fns"
awk '$1=="INIT"{print $3}'  "$TMP/records" | sort -u > "$TMP/initfiles"

comm -23 "$TMP/decl" "$TMP/set" > "$TMP/uninstalled"

echo "lint-seam-installs: $(wc -l < "$TMP/decl" | tr -d ' ') seam! slots," \
     "$(wc -l < "$TMP/uninstalled" | tr -d ' ') without a production install"

# --- allowlist -------------------------------------------------------------
touch "$TMP/allow"
if [ -f "$ALLOWLIST" ]; then
    grep -v '^[ \t]*#' "$ALLOWLIST" | awk -F'\t' 'NF>=1 && $1!=""' > "$TMP/allow"
fi
allowed() { awk -F'\t' -v k="$1" '$1==k{found=1} END{exit !found}' "$TMP/allow"; }

fail=0
viol=0

# --- CHECK 1: ported-but-not-installed (the subtrans class) ----------------
# normalized pub fn index: norm -> "Name file" (first hit wins)
awk '{ n = tolower($1); gsub(/_/, "", n); if (!(n in seen)) { seen[n] = $1 " " $2 } }
     END { for (k in seen) print k "\t" seen[k] }' "$TMP/fns" > "$TMP/fnnorm"

while IFS= read -r slot; do
    norm=$(printf '%s' "$slot" | tr 'A-Z' 'a-z' | tr -d '_')
    hit=$(awk -F'\t' -v n="$norm" '$1==n{print $2; exit}' "$TMP/fnnorm")
    [ -n "$hit" ] || continue
    if allowed "$slot"; then
        echo "$slot" >> "$TMP/allow_used"
        continue
    fi
    echo "VIOLATION(ported-not-installed): seam '$slot' has a ported implementation" \
         "(${hit% *} in ${hit#* }) but no production ::set() — the subtrans bug class." \
         "Install it from the owning crate's init_seams() (closure: seams_init::init_all)."
    viol=$((viol+1)); fail=1
done < "$TMP/uninstalled"

# --- CHECK 2: uninstalled slot with production references ------------------
while IFS= read -r slot; do
    grep -q "^$slot\$" "$TMP/ref" || continue
    norm=$(printf '%s' "$slot" | tr 'A-Z' 'a-z' | tr -d '_')
    awk -F'\t' -v n="$norm" '$1==n{exit 1}' "$TMP/fnnorm" || continue  # counted by CHECK 1
    if allowed "$slot"; then
        echo "$slot" >> "$TMP/allow_used"
        continue
    fi
    echo "VIOLATION(uninstalled-referenced): seam '$slot' has production callers but no" \
         "implementation or install — is_installed() guards skip silently; unguarded" \
         "call() panics. Port+install it, or classify it in $(basename "$ALLOWLIST")."
    viol=$((viol+1)); fail=1
done < "$TMP/uninstalled"

# --- CHECK 3: non-empty init_seams() outside the init_all() closure --------
while IFS= read -r initfile; do
    d=$(dirname "$initfile")
    while [ "$d" != "$REPO" ] && [ ! -f "$d/Cargo.toml" ]; do d=$(dirname "$d"); done
    [ -f "$d/Cargo.toml" ] || continue
    crate=$(awk -F'"' '/^name[ \t]*=/{print $2; exit}' "$d/Cargo.toml" | tr '-' '_')
    [ -n "$crate" ] || continue
    [ "$crate" = "seams_init" ] && continue
    if grep -q "${crate}::init_seams()" "$SEAMS_INIT"; then continue; fi
    # invoked from some other production file (nested init patterns)?
    if tr '\n' '\0' < "$TMP/files" | xargs -0 grep -l "${crate}::init_seams()" 2>/dev/null \
            | grep -qv "^$d/"; then
        continue
    fi
    if allowed "crate:$crate"; then
        echo "crate:$crate" >> "$TMP/allow_used"
        continue
    fi
    echo "VIOLATION(init-seams-orphan): $crate defines a non-empty init_seams()" \
         "($initfile) that seams_init::init_all() never calls — its ::set()s never run."
    viol=$((viol+1)); fail=1
done < "$TMP/initfiles"

# --- stale allowlist entries (warn only) ------------------------------------
touch "$TMP/allow_used"
while IFS=$'\t' read -r key _rest; do
    [ -n "$key" ] || continue
    grep -q "^$key\$" "$TMP/allow_used" && continue
    echo "WARN(stale-allowlist): '$key' is no longer a violation — remove its row from $(basename "$ALLOWLIST")."
done < "$TMP/allow"

if [ $fail -eq 0 ]; then
    echo "lint-seam-installs PASS (0 violations; $(wc -l < "$TMP/allow" | tr -d ' ') allowlisted)"
else
    echo "lint-seam-installs FAIL ($viol violations)"
fi
exit $fail
