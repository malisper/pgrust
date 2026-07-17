#!/usr/bin/env bash
# lint-determinism.sh — the DST P0 determinism-fencing ratchet.
# Authority: docs/design/dst-and-wasm.md (branch docs/dst-wasm-scoping @
# 0d76f50dd), P0 phasing row + §3.3 blocking census. Precedent:
# crates/_support/seams_init/tests/lint-seam-installs.sh (the seam-install lint family).
#
# WHAT IT FENCES: every raw nondeterminism primitive the DST program must
# eventually virtualize. Six categories, each a per-line grep over production
# code, diffed against a budgeted allowlist:
#   fs        raw std::fs / File:: / OpenOptions / read_dir / fsync-family IO
#             (sanctioned surface: the fd/vfd layer — the P1 Vfs choke, §2.1)
#   time      SystemTime / Instant / UNIX_EPOCH / .elapsed( / duration_since
#             (sanctioned: waiter::clock::WaiterClock + timestamp_seams, §2.2)
#   rand      raw OS entropy: getrandom/getentropy/urandom, rand::/OsRng/
#             StdRng/thread_rng/from_entropy. Calls to pg_strong_random are
#             NOT flagged — that crate IS the sanctioned entropy funnel
#             (§2.3); only its internals appear in the ledger.
#   spawn     thread::spawn / thread::Builder
#             (sanctioned: launch_backend's spawner seam — the spawn door, §3.3)
#   env       std::env::var/var_os/vars/vars_os
#             (sanctioned-to-be: the P2 knobs registry, §2.5)
#   blocking  raw std::sync::Condvar / mpsc / crossbeam channels / thread::park
#             outside the waiter/pg_barrier/runtime hubs (§3.3 token invariant;
#             conversion precedent: runtime/src/rg.rs onto the Waiter)
#
# PRODUCTION CODE = crates/**/*.rs minus tests|test|benches|examples dirs,
# tests.rs/test.rs/*_test(s).rs/build.rs basenames, brace-matched
# #[cfg(test)] / #[cfg(any(test,..))] / #[cfg(all(test,..))] items, and
# comments — the 2026-07-17 census methodology (census regen @ origin/main
# 3fee87ff096c), which the allowlist was seeded from. A "site" = one
# surviving source line matching a category pattern.
#
# THE RATCHET LAW: allowlist budgets may only shrink; rows may only be
# deleted. A new raw site fails this lint. The fix is to route it through
# the sanctioned surface for its category — or, if a reviewer deliberately
# accepts a new raw site, add a row carrying a "DST-REVIEW(<who>): <why>"
# marker (the lint recognizes marker rows and reports them as NOTEs so every
# train diff shows them). Rows whose sites vanished get a stale WARN: delete
# them. Rows whose sites shrank get a shrink WARN: lower the budget.
#
# Output ordering is deterministic (LC_ALL=C sorted; violations, then
# warnings, then notes, then fixed-order per-category summary).
#
# Allowlist: crates/_support/seams_init/tests/lint-determinism.allow (override: LINT_DETERMINISM_ALLOWLIST).
# Scan root: the repo (override: LINT_DETERMINISM_TREE=<dir containing crates/>).
# Regeneration (audit/reseed under explicit charter ONLY — never routine; it
# resurrects deleted rows and drops annotations):
#     crates/_support/seams_init/tests/lint-determinism.sh --regen > crates/_support/seams_init/tests/lint-determinism.allow
#
# SE note: the single-executor branch's delta lives in
# scripts/lint-determinism.se-delta.md (reference only, NOT enforced here).
#
# Standalone:   crates/_support/seams_init/tests/lint-determinism.sh            (exit 0 = clean)
# Unit-shaped:  cargo test -p seams_init --test lint_determinism
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
TREE="${LINT_DETERMINISM_TREE:-$REPO}"
ALLOWLIST="${LINT_DETERMINISM_ALLOWLIST:-$REPO/crates/_support/seams_init/tests/lint-determinism.allow}"
MODE="check"
[ "${1:-}" = "--regen" ] && MODE="regen"

[ -d "$TREE/crates" ] || { echo "lint-determinism: $TREE/crates not found"; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/lint-determinism.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT INT TERM

# --- production file list (census methodology) ------------------------------
find "$TREE/crates" -type f -name '*.rs' \
    ! -path '*/tests/*' ! -path '*/test/*' ! -path '*/benches/*' \
    ! -path '*/examples/*' ! -path '*/target/*' \
    ! -name 'tests.rs' ! -name 'test.rs' ! -name '*_test.rs' ! -name '*_tests.rs' \
    ! -name 'build.rs' \
    | LC_ALL=C sort > "$TMP/files" || exit 2

# --- scanner: one "category<TAB>relpath" record per surviving line ----------
cat > "$TMP/scan.awk" <<'AWK'
function brace_delta(line,   t, d) {
    t = line
    # string-literal contents must not perturb brace matching
    gsub(/"([^"\\]|\\.)*"/, "\"\"", t)
    d = gsub(/\{/, "", t); d -= gsub(/\}/, "", t)
    return d
}
FNR == 1 { skip = 0; pend = 0; inblk = 0; rel = substr(FILENAME, length(tree) + 1) }
{
    line = $0

    # ---- comments (left-to-right: whichever of "//" or "/*" comes first
    #      wins, so a "/*" inside a line comment cannot open a phantom
    #      block; line-preserving) ----
    if (inblk) {
        p = index(line, "*/")
        if (p == 0) next
        line = substr(line, p + 2); inblk = 0
    }
    res = ""
    while (1) {
        li = index(line, "//"); bi = index(line, "/*")
        if (li > 0 && (bi == 0 || li < bi)) { line = substr(line, 1, li - 1); break }
        if (bi == 0) break
        res = res substr(line, 1, bi - 1)
        line = substr(line, bi + 2)
        q = index(line, "*/")
        if (q == 0) { inblk = 1; line = ""; break }
        line = substr(line, q + 2)
    }
    line = res line

    # ---- #[cfg(test)]-fenced items (brace-matched; seam-lint pattern) ----
    if (skip > 0) { skip += brace_delta(line); next }
    if (pend) {
        if (line ~ /\{/) { skip = brace_delta(line); pend = 0; if (skip <= 0) skip = 0; next }
        if (line ~ /;[ \t]*$/) { pend = 0; next }
        next
    }
    if (line ~ /#\[[ \t]*cfg[ \t]*\([ \t]*((any|all)[ \t]*\([ \t]*)?test[^A-Za-z0-9_]/) {
        if (line ~ /\{/) { skip = brace_delta(line); if (skip <= 0) skip = 0 }
        else pend = 1
        next
    }

    # ---- category patterns (census parity; >=1 match on a line = 1 site) ----
    if (line ~ /(^|[^A-Za-z0-9_])std::fs($|[^A-Za-z0-9_:])|(^|[^A-Za-z0-9_])fs::[A-Za-z_]|File::(open|create|create_new|options)($|[^A-Za-z0-9_])|(^|[^A-Za-z0-9_])OpenOptions($|[^A-Za-z0-9_])|(^|[^:A-Za-z0-9_])(read_to_string|create_dir|create_dir_all|remove_file|remove_dir|remove_dir_all|read_dir|hard_link|symlink_metadata|set_permissions)($|[^A-Za-z0-9_])|\.metadata\(|\.(sync_all|sync_data|read_exact_at|write_all_at)\(/)
        print "fs\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(SystemTime|Instant|UNIX_EPOCH)($|[^A-Za-z0-9_])|\.elapsed\(|(^|[^A-Za-z0-9_])duration_since($|[^A-Za-z0-9_])/)
        print "time\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(getrandom|thread_rng|OsRng|StdRng|from_entropy|getentropy)($|[^A-Za-z0-9_])|(^|[^A-Za-z0-9_])rand::|urandom/)
        print "rand\t" rel
    if (line ~ /thread::spawn|thread::Builder/)
        print "spawn\t" rel
    if (line ~ /env::(var|var_os|vars|vars_os)[ \t]*\(/)
        print "env\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(Condvar|mpsc)($|[^A-Za-z0-9_])|crossbeam_channel|crossbeam::channel|thread::park/)
        print "blocking\t" rel
}
AWK

tr '\n' '\0' < "$TMP/files" \
    | xargs -0 awk -v tree="$TREE/" -f "$TMP/scan.awk" \
    | LC_ALL=C sort | uniq -c \
    | awk '{ printf "%s\t%s\t%d\n", $2, $3, $1 }' \
    | LC_ALL=C sort > "$TMP/scan"

# --- regen mode: emit a fresh allowlist body to stdout ----------------------
if [ "$MODE" = "regen" ]; then
    echo "# lint-determinism.allow — REGENERATED LEDGER (annotations lost; reseed"
    echo "# only under explicit charter — the ratchet law says rows only die)."
    echo "# Row format (tab-separated): <category>\t<file>\t<max-sites>[\t<annotation>]"
    for cat in fs time rand spawn env blocking; do
        echo ""
        echo "# ==== $cat ===="
        if [ "$cat" = "fs" ]; then
            awk -F'\t' -v cat="$cat" '
                $1 == cat {
                    crate = $2
                    if (index(crate, "/src/") > 0) sub(/\/src\/.*/, "", crate)
                    else sub(/\/[^\/]*$/, "", crate)
                    rows[NR] = cat "\t" $2 "\t" $3
                    crateof[NR] = crate
                    csum[crate] += $3
                }
                END {
                    prev = ""
                    for (i = 1; i <= NR; i++) {
                        if (!(i in rows)) continue
                        if (crateof[i] != prev) {
                            printf "# --- %s (%d sites) ---\n", crateof[i], csum[crateof[i]]
                            prev = crateof[i]
                        }
                        print rows[i]
                    }
                }
            ' "$TMP/scan"
        else
            awk -F'\t' -v cat="$cat" '$1 == cat { print $1 "\t" $2 "\t" $3 }' "$TMP/scan"
        fi
    done
    exit 0
fi

# --- allowlist parse + validation --------------------------------------------
[ -f "$ALLOWLIST" ] || { echo "lint-determinism: allowlist $ALLOWLIST not found"; exit 2; }
awk -F'\t' -v out="$TMP/allow" '
    /^[ \t]*(#|$)/ { next }
    {
        cat = $1; path = $2; budget = $3; annot = (NF >= 4 ? $4 : "")
        if (cat !~ /^(fs|time|rand|spawn|env|blocking)$/ || path == "" \
            || budget !~ /^[0-9]+$/ || budget + 0 < 1) {
            printf "CONFIG-ERROR: malformed allowlist row (%s:%d): %s\n", FILENAME, FNR, $0
            err = 1; next
        }
        key = cat "\t" path
        if (key in seen) {
            printf "CONFIG-ERROR: duplicate allowlist row for [%s] %s (%s:%d)\n", cat, path, FILENAME, FNR
            err = 1; next
        }
        seen[key] = 1
        if (annot ~ /DST-REVIEW/ && annot !~ /DST-REVIEW\([^)]+\):/) {
            printf "CONFIG-ERROR: malformed review marker on [%s] %s (want \"DST-REVIEW(<who>): <why>\"): %s\n", cat, path, annot
            err = 1; next
        }
        print cat "\t" path "\t" budget "\t" annot > out
    }
    END { exit err }
' "$ALLOWLIST"
[ $? -eq 0 ] || { echo "lint-determinism CONFIG-FAIL (bad allowlist)"; exit 2; }
touch "$TMP/allow"

# --- diff scan vs allowlist ---------------------------------------------------
# (rows are tagged A/S rather than split across two awk input files: the
#  FNR==NR idiom silently misassigns when the first file is empty)
ALLOWBASE=$(basename "$ALLOWLIST")
{ sed 's/^/A\t/' "$TMP/allow"; sed 's/^/S\t/' "$TMP/scan"; } \
| awk -F'\t' -v allowbase="$ALLOWBASE" -v counts="$TMP/counts" -v summ="$TMP/summ" '
    BEGIN {
        guide["fs"]       = "route it through the fd/vfd layer (the P1 Vfs choke, docs/design/dst-and-wasm.md #2.1)"
        guide["time"]     = "route it through waiter::clock::WaiterClock / timestamp_seams (dst-and-wasm.md #2.2)"
        guide["rand"]     = "route it through pg_strong_random (dst-and-wasm.md #2.3)"
        guide["spawn"]    = "route it through launch_backend'"'"'s spawner seam (dst-and-wasm.md #3.3)"
        guide["env"]      = "route it through the knobs registry (dst-and-wasm.md #2.5)"
        guide["blocking"] = "convert it onto Waiter/eventcount per the runtime/src/rg.rs precedent (dst-and-wasm.md #3.3)"
        viol = 0; warn = 0
    }
    $1 == "A" { budget[$2 "\t" $3] = $4 + 0; annot[$2 "\t" $3] = $5; next }
    $1 == "S" {
        k = $2 "\t" $3; n[k] = $4 + 0
        scnt[$2] += $4; sfiles[$2]++
    }
    END {
        for (k in n) {
            split(k, a, "\t"); cat = a[1]; path = a[2]
            if (!(k in budget)) {
                printf "1\tVIOLATION(new-site): [%s] %s carries %d raw %s site(s) with no allowlist row — %s, or add a \"DST-REVIEW(<who>): <why>\" row to %s.\n", \
                       cat, path, n[k], cat, guide[cat], allowbase
                viol++
            } else if (n[k] > budget[k]) {
                printf "1\tVIOLATION(ratchet): [%s] %s raw %s sites grew %d -> %d (budgets may only shrink) — %s, or add a DST-REVIEW marker to the row in %s.\n", \
                       cat, path, cat, budget[k], n[k], guide[cat], allowbase
                viol++
            } else if (n[k] < budget[k]) {
                printf "2\tWARN(shrink-budget): [%s] %s now has %d raw %s site(s) < budget %d — lower the budget (ratchet law).\n", \
                       cat, path, n[k], cat, budget[k]
                warn++
            }
        }
        for (k in budget) {
            split(k, a, "\t"); cat = a[1]; path = a[2]
            acnt[cat] += budget[k]; afiles[cat]++
            if (!(k in n)) {
                printf "2\tWARN(stale-allowlist): [%s] %s — no raw %s sites remain; delete the row from %s.\n", \
                       cat, path, cat, allowbase
                warn++
            }
            if (annot[k] ~ /DST-REVIEW\(/) {
                printf "3\tNOTE(review-marker): [%s] %s — %s\n", cat, path, annot[k]
            }
        }
        print viol "\t" warn > counts
        norder = split("fs time rand spawn env blocking", ord, " ")
        for (i = 1; i <= norder; i++) {
            cat = ord[i]
            printf "%s: %d sites / %d files (allowlist %d/%d)\n", \
                   cat, scnt[cat] + 0, sfiles[cat] + 0, acnt[cat] + 0, afiles[cat] + 0 >> summ
        }
    }
' | LC_ALL=C sort | cut -f2-

viol=$(cut -f1 "$TMP/counts"); warn=$(cut -f2 "$TMP/counts")
echo "lint-determinism summary:"
sed 's/^/  /' "$TMP/summ"
if [ "$viol" -eq 0 ]; then
    echo "lint-determinism PASS (0 violations; $warn warning(s))"
    exit 0
else
    echo "lint-determinism FAIL ($viol violation(s); $warn warning(s))"
    exit 1
fi
