#!/usr/bin/env bash
# lint-determinism.sh — the DST P0 determinism-fencing ratchet.
# Authority: docs/design/dst-and-wasm.md (branch docs/dst-wasm-scoping @
# 0d76f50dd), P0 phasing row + §3.3 blocking census. Precedent:
# crates/_support/seams_init/tests/lint-seam-installs.sh (the seam-install lint family).
#
# WHAT IT FENCES: every raw nondeterminism primitive the DST program must
# eventually virtualize. Ten categories, each a per-line grep over production
# code, diffed against a budgeted allowlist:
#   fs        raw std::fs / File:: / OpenOptions / read_dir / fsync-family IO,
#             `use std::fs::{...}` / `use std::fs::*` import declarations, and
#             the libc fs/fd syscall surface (libc::open/read/write/pread/
#             fsync/stat/rename/fcntl/dup/stdio-FILE*/…, incl. the
#             libc::syscall escape hatch)
#             (sanctioned surface: the fd/vfd layer — the P1 Vfs choke, §2.1)
#   time      SystemTime / Instant / UNIX_EPOCH / .elapsed( / duration_since,
#             plus libc::clock_gettime / gettimeofday / time
#             (sanctioned: pg_clock (s2.2, the P2 authority) + the waiter
#             park hook + the timestamp_seams API shell)
#   rand      raw OS entropy: getrandom/getentropy/urandom, rand::/OsRng/
#             StdRng/thread_rng/from_entropy (bare-name patterns, so
#             libc::getrandom / libc::getentropy are caught too). Calls to
#             pg_strong_random are NOT flagged — that crate IS the sanctioned
#             entropy funnel (§2.3, the P2 EntropySource seam); only its raw
#             internals (src/os.rs, the OsEntropy getentropy+urandom ladder)
#             appear in the ledger.
#   spawn     thread::spawn / thread::Builder / libc::fork / pthread_create
#             (sanctioned: launch_backend's spawner seam — the spawn door, §3.3)
#   env       std::env::var/var_os/vars/vars_os/set_var/remove_var — at call
#             sites AND at `use std::env::var`-style import declarations —
#             plus `use std::env::{...}` / `::*` groups and
#             libc::getenv/setenv/unsetenv/putenv
#             (sanctioned-to-be: the P2 knobs registry, §2.5)
#   blocking  raw std::sync::Condvar / mpsc / crossbeam channels / thread::park,
#             plus thread::sleep and libc::nanosleep/usleep/sleep (sleeps are
#             invisible to a virtual clock), outside the waiter/pg_barrier/
#             runtime hubs (§3.3 token invariant; conversion precedent:
#             runtime/src/rg.rs onto the Waiter)
#
# WIDENED BLOCKING ARMS (2026-07-17d, the P3 §1.3 layer-2 widening —
# docs/design/dst-p3-scheduler.md @ docs/dst-p3-design; census on
# dst/substrate @ e1a7e7b47, the §7 Wave-1 entry gate). These close the
# review-proven census blind spots: each is an OS-decided wake a token
# holder can block on that matched NO pattern above. Kept as separate
# categories (not folded into `blocking`) so the ladder's 84->43 blocking
# arithmetic stays a stable, auditable series:
#   join      zero-arg .join() — the JoinHandle::join / ScopedJoinHandle::
#             join call shape (thread joins take no argument; separator
#             joins do). Name-based like .metadata(: a same-named non-thread
#             join (the lifecycle armed join, runtime/src/sched.rs) carries
#             an annotated ledger row. (P3 §3 row 14: v1 = per-site
#             join-bounded-after-wake proof; v2 = sched::join_scheduled)
#   barrier   std::sync::Barrier — the direct path, `use std::sync::{..
#             Barrier ..}` import groups, and bare Barrier::new (name-based:
#             pg_barrier::Barrier constructions flag too and are ledgered —
#             that type IS the sanctioned Waiter-backed barrier, riding the
#             C1/C9 chokes). A raw std Barrier wait blocks in a futex with
#             the holder still counted Running: the sim wedge is watchdog-
#             invisible (P3 §3 row 18 — found by review, not census)
#   once      get_or_init / get_or_try_init / call_once — racing lazy-init:
#             losers block raw while the winner runs the init closure; an
#             init closure that crosses a choke point wedges a token-holding
#             loser (P3 §3 row 19; the once-ledger classifies every init
#             closure — pure/env-read vs choke-crossing)
#   scope     std::thread::scope gangs — scoped children are spawns outside
#             the spawn door and scope end is an implicit raw join (P3 §3
#             row 15). Own category rather than a spawn-pattern widening so
#             the seeded spawn numbers stay stable; this makes row 15's
#             "scope sites are enumerable by lint" claim actually true.
#
# WHAT IS DELIBERATELY NOT LINTED — LOCK WAITS. Contended Mutex::lock /
# RwLock::read/write waits are blocking, but they are NOT statically
# lintable: .lock() is ubiquitous (712 bare .lock() lines across 144 prod
# files at the 2026-07-17d census) and nearly every one is a short,
# uncontended critical section. Whether a lock wait blocks is a dynamic
# property (contention + what the holder does while holding it), not a
# lexical one — a lock arm would be 100% ledger, 0% ratchet. That class is
# layer 1's job (dst-p3-scheduler.md §1.3): the sim_sync re-export shims +
# the TLS RAW_LOCK_DEPTH counter asserted zero at every yield_point, which
# turns the watchdog-invisible park-holding-a-raw-lock wedge into a
# pinpointed abort.
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
# marker. Marker discipline on NEW/RAISED rows is enforced by HUMAN REVIEW
# of allowlist diffs — this lint cannot distinguish a new row from a seeded
# one. What the lint machine-enforces: malformed DST-REVIEW text, duplicate
# rows, and budget<1 are CONFIG-FAILs (exit 2), and every marker row is
# surfaced as a NOTE on every run so train diffs carry exceptions visibly.
# Rows whose sites vanished get a stale WARN: delete them. Rows whose sites
# shrank get a shrink WARN: lower the budget.
#
# ACCEPTED LIMITS of a grep-family ratchet (ledgered 2026-07-17, review
# dst/p0-fencing-lint; each was weighed and deliberately not chased):
#   * a "site" is a matched LINE: two calls joined onto one line count once,
#     so line-joining refactors can consume ratchet headroom invisibly;
#   * scope is crates/ only — bench/rig crates inside crates/ ARE scanned
#     (ledgered as such), but fuzz/, scripts/, and generated code outside
#     crates/ sit outside the fence (non-product);
#   * aliased imports (`use std::fs as f`, `use std::env::var as getenv`)
#     count once at the import line; call sites through the alias are not
#     individually counted (call-count does not scale with use);
#   * macro-generated sites count at the macro definition only;
#   * nested block comments (/* /* */ */) are closed at the first */;
#   * multi-line and raw string literals are not tracked across lines — a
#     "//", "/*", or brace inside one can perturb the scan (single-line
#     normal strings and char literals ARE handled correctly);
#   * spaced path tokens (`std :: fs :: rename`) are not matched;
#   * method-name patterns (.metadata( / .elapsed( / .sync_all( …) are
#     name-based: a same-named method on a non-fs type will flag — ledger
#     the row with an explanatory annotation, or rename the method;
#   * #[cfg(...)] attrs containing not( are NEVER treated as test-only
#     (prod-safe: cfg(not(test)) code stays censused; the cost is that
#     any(not(test),…)-style test code would be counted — ledger it);
#   * out-of-P0-scope primitives are not fenced here: std::process::Command,
#     HashMap RandomState seeding/iteration order, the mmap family,
#     socket/network IO, and the epoll/kevent poll layer (P1+ per
#     dst-and-wasm.md; the poll layer lives inside the runtime hubs).
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
# (plus "poison<TAB>relpath" when scan state is still open at a file's EOF)
cat > "$TMP/scan.awk" <<'AWK'
function brace_delta(line,   t, d) {
    t = line
    # char-literal contents first ('"' / '{' / '}' must not perturb string
    # or brace state), then string-literal contents
    gsub(/'([^'\\]|\\.)'/, "''", t)
    gsub(/"([^"\\]|\\.)*"/, "\"\"", t)
    d = gsub(/\{/, "", t); d -= gsub(/\}/, "", t)
    return d
}
# strip comments from one line, string/char-literal aware: a "//" or "/*"
# inside a string or char literal is NOT a comment start (and cannot truncate
# the line or open a phantom block). Uses/updates the global inblk. Known
# limits (see header): nested block comments, multi-line/raw strings.
function strip_code(line,   out, i, n, c, c2, q) {
    if (inblk) {
        q = index(line, "*/")
        if (q == 0) return ""
        line = substr(line, q + 2); inblk = 0
    }
    if (index(line, "//") == 0 && index(line, "/*") == 0) return line
    out = ""; i = 1; n = length(line)
    while (i <= n) {
        c = substr(line, i, 1)
        if (c == "\"") {
            out = out c; i++
            while (i <= n) {
                c = substr(line, i, 1)
                if (c == "\\") { out = out substr(line, i, 2); i += 2; continue }
                out = out c; i++
                if (c == "\"") break
            }
            continue
        }
        if (c == "'") {
            if (substr(line, i + 1, 1) == "\\" && substr(line, i + 3, 1) == "'") {
                out = out substr(line, i, 4); i += 4; continue
            }
            if (substr(line, i + 1, 1) != "\\" && substr(line, i + 2, 1) == "'") {
                out = out substr(line, i, 3); i += 3; continue
            }
            out = out c; i++; continue
        }
        c2 = substr(line, i, 2)
        if (c2 == "//") break
        if (c2 == "/*") {
            q = index(substr(line, i + 2), "*/")
            if (q == 0) { inblk = 1; break }
            i = i + q + 3
            continue
        }
        out = out c; i++
    }
    return out
}
# test-only cfg attr? Positional (test first) or test anywhere in an
# any()/all() list — but an attr containing not( is NEVER test-only
# (cfg(not(test)) is production code; prod-safe conservatism, see header).
function is_cfg_test(line) {
    if (line ~ /#\[[ \t]*cfg[ \t]*\([ \t]*((any|all)[ \t]*\([ \t]*)?test[^A-Za-z0-9_]/) return 1
    if (line ~ /#\[[ \t]*cfg[ \t]*\([ \t]*(any|all)[ \t]*\(/ \
        && line !~ /not[ \t]*\(/ \
        && line ~ /#\[[ \t]*cfg[ \t]*\([^]]*[(, \t]test[ \t]*[,)]/) return 1
    return 0
}
FNR == 1 {
    if (rel != "" && (inblk || skip > 0)) print "poison\t" rel
    skip = 0; pend = 0; inblk = 0
    rel = substr(FILENAME, length(tree) + 1)
}
{
    line = strip_code($0)

    # ---- #[cfg(test)]-fenced items (brace-matched; seam-lint pattern) ----
    if (skip > 0) { skip += brace_delta(line); next }
    if (pend) {
        if (line ~ /\{/) { skip = brace_delta(line); pend = 0; if (skip <= 0) skip = 0; next }
        if (line ~ /;[ \t]*$/) { pend = 0; next }
        next
    }
    if (is_cfg_test(line)) {
        if (line ~ /\{/) { skip = brace_delta(line); if (skip <= 0) skip = 0 }
        else pend = 1
        next
    }

    # ---- category patterns (census parity; >=1 match on a line = 1 site) ----
    if (line ~ /(^|[^A-Za-z0-9_])std::fs($|[^A-Za-z0-9_:])|(^|[^A-Za-z0-9_])fs::[A-Za-z_{*]|File::(open|create|create_new|options)($|[^A-Za-z0-9_])|(^|[^A-Za-z0-9_])OpenOptions($|[^A-Za-z0-9_])|(^|[^:A-Za-z0-9_])(read_to_string|create_dir|create_dir_all|remove_file|remove_dir|remove_dir_all|read_dir|hard_link|symlink_metadata|set_permissions)($|[^A-Za-z0-9_])|\.metadata\(|\.(sync_all|sync_data|read_exact_at|write_all_at)\(/ \
        || line ~ /libc::(open|openat|creat|close|read|readv|pread|preadv|write|writev|pwrite|pwritev|fsync|fdatasync|syncfs|sync_file_range|copy_file_range|fallocate|posix_fallocate|posix_fadvise|unlink|unlinkat|rename|renameat|link|linkat|symlink|symlinkat|readlink|readlinkat|mkdir|mkdirat|rmdir|stat|fstat|lstat|fstatat|statvfs|fstatvfs|truncate|ftruncate|lseek|access|faccessat|chmod|fchmod|chown|fchown|utime|utimes|futimens|realpath|opendir|readdir|closedir|umask|fcntl|flock|dup|dup2|fopen|fdopen|fclose|fread|fwrite|fseek|fseeko|ftell|ftello|fflush|setvbuf|syscall)($|[^A-Za-z0-9_])/)
        print "fs\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(SystemTime|Instant|UNIX_EPOCH)($|[^A-Za-z0-9_])|\.elapsed\(|(^|[^A-Za-z0-9_])duration_since($|[^A-Za-z0-9_])|libc::(clock_gettime|gettimeofday|time)($|[^A-Za-z0-9_])/)
        print "time\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(getrandom|thread_rng|OsRng|StdRng|from_entropy|getentropy)($|[^A-Za-z0-9_])|(^|[^A-Za-z0-9_])rand::|urandom/)
        print "rand\t" rel
    if (line ~ /thread::spawn|thread::Builder|libc::(fork|pthread_create)($|[^A-Za-z0-9_])/)
        print "spawn\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])env::(var|var_os|vars|vars_os|set_var|remove_var)($|[^A-Za-z0-9_])|(^|[^A-Za-z0-9_])env::[{*]|libc::(getenv|setenv|unsetenv|putenv)($|[^A-Za-z0-9_])/)
        print "env\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(Condvar|mpsc)($|[^A-Za-z0-9_])|crossbeam_channel|crossbeam::channel|thread::park|thread::sleep|libc::(nanosleep|usleep|sleep)($|[^A-Za-z0-9_])/)
        print "blocking\t" rel

    # ---- widened blocking arms (P3 s1.3 layer 2; dst-p3-scheduler.md) ----
    if (line ~ /\.join\(\)/)
        print "join\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])std::sync::Barrier($|[^A-Za-z0-9_])|use[ \t]+std::sync::\{[^}]*Barrier|(^|[^A-Za-z0-9_])Barrier::new($|[^A-Za-z0-9_])/)
        print "barrier\t" rel
    if (line ~ /(^|[^A-Za-z0-9_])(get_or_init|get_or_try_init|call_once)($|[^A-Za-z0-9_])/)
        print "once\t" rel
    if (line ~ /thread::scope/)
        print "scope\t" rel

    # ---- rawsync (permit-s1 WS-SYNC; permit-scheduler.md s2 + contract s2.5) ----
    # Raw sync TYPE imports/paths outside pgsync: std::sync Mutex/RwLock/
    # Condvar/Barrier/Once/OnceLock/mpsc (single paths, inline paths, and
    # use-group imports), parking_lot, crossbeam sync/channel. LEXICAL, one
    # row per file — the deliberately-unlinted lock-WAIT reasoning (see the
    # header) is about call sites; the single-lock-library rule makes the
    # TYPE import checkable. Arc/Weak/atomic stay std by crate law and are
    # NOT flagged. crates/_support/pgsync is the sanctioned home (the only
    # cfg site) and is excluded by path.
    if (rel !~ /^crates\/_support\/pgsync\//) {
        if (line ~ /(^|[^A-Za-z0-9_])std::sync::(OnceLock|Once|Mutex|RwLock|Condvar|Barrier|mpsc)($|[^A-Za-z0-9_])/ \
            || line ~ /use[ \t]+std::sync::\{[^}]*(OnceLock|Once|Mutex|RwLock|Condvar|Barrier|mpsc)/ \
            || line ~ /(^|[^A-Za-z0-9_])parking_lot::/ \
            || line ~ /crossbeam_channel|crossbeam::channel|crossbeam::sync|crossbeam_utils::sync/)
            print "rawsync\t" rel
    }
}
END { if (rel != "" && (inblk || skip > 0)) print "poison\t" rel }
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
    awk -F'\t' '$1 == "poison" { printf "# SCAN-POISON: %s — scanner state still open at EOF; counts below may be incomplete\n", $2 }' "$TMP/scan"
    for cat in fs time rand spawn env blocking join barrier once scope rawsync; do
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
        if (cat !~ /^(fs|time|rand|spawn|env|blocking|join|barrier|once|scope|rawsync)$/ || path == "" \
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
        guide["time"]     = "route it through pg_clock (dst-and-wasm.md #2.2: mono_ns/wall_* leaf API; the waiter park hook and timestamp_seams API shell delegate to it)"
        guide["rand"]     = "route it through pg_strong_random (dst-and-wasm.md #2.3)"
        guide["spawn"]    = "route it through launch_backend'"'"'s spawner seam (dst-and-wasm.md #3.3)"
        guide["env"]      = "route it through the knobs registry (dst-and-wasm.md #2.5)"
        guide["blocking"] = "convert it onto Waiter/eventcount per the runtime/src/rg.rs precedent (dst-and-wasm.md #3.3)"
        guide["join"]     = "prove it join-bounded-after-wake in the P3 row-14 ledger (the target cannot need the run token to reach exit), or convert to sched::join_scheduled when P3 wave 3 lands (dst-p3-scheduler.md #3 row 14)"
        guide["barrier"]  = "use pg_barrier::Barrier (Waiter-backed, rides the C1/C9 chokes) — a raw std::sync::Barrier wait wedges the sim watchdog-invisibly (dst-p3-scheduler.md #3 row 18)"
        guide["once"]     = "classify the init closure in the P3 row-19 once-ledger: pure/env-read closures get a proof row; choke-crossing closures hoist to boot or convert to a Waiter-backed once-guard (dst-p3-scheduler.md #3 row 19)"
        guide["scope"]    = "register the gang: ScopeEnter/ScopeExit + route scoped children through sim_scope_spawn (dst-p3-scheduler.md #3 row 15)"
        guide["rawsync"]  = "import the type from pgsync, THE single lock library (permit-scheduler.md #2: call sites carry no world cfg; pgsync is the only cfg site)"
        viol = 0; warn = 0
    }
    $1 == "A" { budget[$2 "\t" $3] = $4 + 0; annot[$2 "\t" $3] = $5; next }
    $1 == "S" {
        if ($2 == "poison") { poison[$3] = 1; next }
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
        for (p in poison) {
            printf "2\tWARN(scan-poison): %s — scanner state still open at EOF (unclosed block comment or unbalanced #[cfg(test)] braces); code after the poison point was NOT scanned. Fix the source shape (or the scanner) before trusting this file'"'"'s counts.\n", p
            warn++
        }
        print viol "\t" warn > counts
        norder = split("fs time rand spawn env blocking join barrier once scope rawsync", ord, " ")
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
