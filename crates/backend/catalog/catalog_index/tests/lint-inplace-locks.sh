#!/usr/bin/env bash
# lint-inplace-locks.sh — the standing guard for GL-INPLACE-1 defect (A):
# every TRANSACTIONAL updater of a pg_class row must hold LOCKTAG_TUPLE at
# InplaceUpdateTupleLock across the update, wherever C holds it.
#
# THE BUG CLASS THIS EXISTS TO CATCH. pg_class rows are written two ways.
# VACUUM/ANALYZE/CREATE INDEX write relpages, reltuples, relallvisible,
# relallfrozen, relhasindex and -- crucially -- relfrozenxid and relminmxid
# IN PLACE, under a buffer content lock. DDL (RENAME, SET SCHEMA, SET
# TABLESPACE, SET (...), REINDEX, relispartition) writes the same row
# TRANSACTIONALLY: read the tuple, build a new version, heap_update. If the
# transactional writer reads the tuple before an inplace writer commits and
# writes its new version after, the inplace write is SILENTLY DISCARDED.
# PostgreSQL 17 fixed that by making both sides take LOCKTAG_TUPLE at
# InplaceUpdateTupleLock (src/backend/access/heap/README.tuplock, section
# "Locking to write inplace-updated tables"). This port shipped the inplace
# half and dropped the transactional half at 7 sites, which loses
# relfrozenxid/relminmxid advances -- not a transient wrong answer but a
# DURABLE wraparound-safety regression, since clog can then be truncated past
# a value the catalog no longer reflects.
#
# WHY A LINT AND NOT JUST A FIX: the failure is silent on both sides. A
# missing lock produces no error, no assertion (C's heap_update assert at
# heapam.c:4241 has no counterpart here), and no test failure -- only a
# catalog that drifts. So the fix has to come with a tripwire.
#
# WHAT IT CHECKS
#   CHECK 1 -- LOCK PRESENT: every census row marked DIVERGENT-FIXED must
#       still contain both LockTuple and UnlockTuple at InplaceUpdateTupleLock
#       in the named function. Catches a refactor quietly dropping one.
#   CHECK 2 -- C-EXACT UNCHANGED: every census row marked C-EXACT (C uses the
#       plain, unlocked SearchSysCacheCopy1 there, so locking would be a
#       divergence) must NOT have grown a lock. Keeps the asymmetry honest and
#       documents it; C's own ATExecSetRelOptions locks the main relation's row
#       and not the toast relation's.
#   CHECK 3 -- NO 8th SITE: the number of CatalogTupleUpdate call sites in each
#       censused file must match the recorded count, AND no file outside the
#       census may both name pg_class and call CatalogTupleUpdate. Either one
#       means a pg_class writer was added or moved: classify it against C and
#       update the census. This is the check that actually prevents an 8th
#       unlocked updater, which is the whole point.
#
# The census is embedded below rather than kept in a .allow file on purpose:
# each row carries its C ANCHOR and, for C-EXACT rows, the mechanism (C's own
# unlocked fetch). That is the heapfree lane's R1 rule -- an allowlist must
# record the excluding mechanism, never a milestone that will stop being true.
#
# Standalone:   crates/backend/catalog/catalog_index/tests/lint-inplace-locks.sh        (exit 0 = clean)
# Unit-shaped:  cargo test -p catalog_index --test lint_inplace_locks
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$REPO" || exit 2
# POSIX grep + find only, no ripgrep: the fleet test pods do not ship rg, and a
# lint that cannot run is a lint that silently stops guarding. (Learned the
# expensive way -- CONFIRM take-1 red on "rg not found".) Skipping when a tool
# is missing would be worse still: that is the vacuous pass this repo's
# gate-blindness ledger is about, so a missing tool must FAIL, never skip.
command -v grep >/dev/null || { echo "lint-inplace-locks: grep not found"; exit 2; }

# Count matching lines in a file; always numeric, 0 when none (grep -c exits 1).
count_in() { grep -c -- "$1" "$2" 2>/dev/null || true; }

fail=0
say() { printf '%s\n' "$*"; }
bad() { say "VIOLATION: $*"; fail=1; }

# ---------------------------------------------------------------------------
# CENSUS. One row per Rust function that transactionally updates a pg_class
# row. Fields: verdict | file | rust_fn | c_anchor | note
#   DIVERGENT-FIXED = C takes InplaceUpdateTupleLock; we now do too.
#   C-EXACT         = C does NOT take it there; neither may we.
# ---------------------------------------------------------------------------
CENSUS=$(cat <<'ROWS'
DIVERGENT-FIXED|crates/backend/catalog/catalog_index/src/reindex.rs|RelationSetNewRelfilenumber|relcache.c:3820+3949 SearchSysCacheLockedCopy1/UnlockTuple|writes relfrozenxid AND relminmxid
DIVERGENT-FIXED|crates/backend/catalog/catalog_index/src/reindex.rs|SetRelationTableSpacePgClass|tablecmds.c:3765+3777 via index.c:3774 reindex_index|second in-tree copy of SetRelationTableSpace
DIVERGENT-FIXED|crates/backend/commands/tablecmds/src/alter.rs|SetRelationTableSpace|tablecmds.c:3765+3777|ALTER TABLE ... SET TABLESPACE
DIVERGENT-FIXED|crates/backend/commands/tablecmds/src/rename.rs|RenameRelationInternal|tablecmds.c:4297+4326|ALTER TABLE ... RENAME
DIVERGENT-FIXED|crates/backend/commands/tablecmds/src/setrelopts.rs|update_one|tablecmds.c:16666+16773 main rel only|toast arm is unlocked in C (:16790) -- locktup=false
DIVERGENT-FIXED|crates/backend/commands/tablecmds/src/namespace.rs|AlterRelationNamespaceInternal|tablecmds.c:19065+19099+19113|ALTER TABLE ... SET SCHEMA
DIVERGENT-FIXED|crates/backend/commands/indexcmds/src/define.rs|update_relispartition|indexcmds.c:4582+4589|IndexSetParentIndex
C-EXACT|crates/backend/commands/tablecmds/src/alter.rs|ATExecAddColumn|tablecmds.c:7360 SearchSysCacheCopy1|C fetches UNLOCKED here; relnatts only
C-EXACT|crates/backend/commands/tablecmds/src/partition.rs|SetRelationHasSubclass|tablecmds.c:3662 SearchSysCacheCopy1|C fetches UNLOCKED
C-EXACT|crates/backend/commands/tablecmds/src/owner.rs|ATExecChangeOwner|tablecmds.c:16088 SearchSysCache1|C fetches UNLOCKED
C-EXACT|crates/backend/commands/matview/src/lib.rs|SetMatViewPopulatedState|matview.c:92 SearchSysCacheCopy1|C fetches UNLOCKED
C-EXACT|crates/backend/rewrite/rewrite_define/src/lib.rs|SetRelationRuleStatus|rewriteSupport.c:63 SearchSysCacheCopy1|C fetches UNLOCKED
C-EXACT|crates/backend/commands/trigger/src/catalog.rs|set_relation_has_triggers|trigger.c:1017 SearchSysCacheCopy1|C fetches UNLOCKED
C-EXACT|crates/backend/statistics/stats_import/src/relation_stats.rs|relation_statistics_update|relation_stats.c:142 SearchSysCache1|C fetches UNLOCKED
ROWS
)

# Files that contain at least one pg_class transactional update, with the
# total number of catalog_indexing::CatalogTupleUpdate call sites in the file
# (ALL catalogs, not just pg_class -- a coarse but drift-proof counter).
# A mismatch is not automatically a bug; it means "reclassify and update me".
FILECOUNTS=$(cat <<'ROWS'
7|crates/backend/catalog/aclchk/src/grant.rs
3|crates/backend/catalog/catalog_heap/src/create.rs
1|crates/backend/catalog/catalog_heap/src/drop.rs
2|crates/backend/catalog/catalog_heap/src/partition.rs
6|crates/backend/catalog/catalog_index/src/concurrent.rs
2|crates/backend/catalog/catalog_index/src/lib.rs
3|crates/backend/catalog/catalog_index/src/reindex.rs
1|crates/backend/catalog/catalog_toasting/src/lib.rs
2|crates/backend/catalog/pg_attrdef/src/lib.rs
6|crates/backend/catalog/pg_constraint/src/lib.rs
3|crates/backend/catalog/pg_depend/src/lib.rs
1|crates/backend/catalog/pg_shdepend/src/lib.rs
2|crates/backend/catalog/pg_type/src/lib.rs
3|crates/backend/commands/alter/src/lib.rs
2|crates/backend/commands/cluster/src/command.rs
3|crates/backend/commands/cluster/src/lib.rs
2|crates/backend/commands/comment/src/lib.rs
5|crates/backend/commands/foreigncmds/src/lib.rs
2|crates/backend/commands/indexcmds/src/define.rs
1|crates/backend/commands/matview/src/lib.rs
3|crates/backend/commands/policy/src/lib.rs
2|crates/backend/commands/publicationcmds/src/lib.rs
1|crates/backend/commands/sequence/src/lib.rs
1|crates/backend/commands/statscmds/src/lib.rs
9|crates/backend/commands/tablecmds/src/alter.rs
7|crates/backend/commands/tablecmds/src/attach.rs
1|crates/backend/commands/tablecmds/src/constraints.rs
1|crates/backend/commands/tablecmds/src/fk.rs
1|crates/backend/commands/tablecmds/src/namespace.rs
2|crates/backend/commands/tablecmds/src/owner.rs
1|crates/backend/commands/tablecmds/src/partition.rs
1|crates/backend/commands/tablecmds/src/rename.rs
1|crates/backend/commands/tablecmds/src/setrelopts.rs
3|crates/backend/commands/trigger/src/catalog.rs
3|crates/backend/commands/typecmds/src/alter.rs
4|crates/backend/rewrite/rewrite_define/src/lib.rs
1|crates/backend/statistics/stats_import/src/relation_stats.rs
ROWS
)

# Slice a function body out of a Rust file: from the `fn NAME` line to the
# first line that is exactly a closing brace at column 0 (crate-level items in
# this tree always close that way).
fn_body() { # $1=file $2=fn
    awk -v want="$2" '
        index($0, "fn " want "<") || index($0, "fn " want "(") { on=1 }
        on { print }
        on && /^}/ { exit }
    ' "$1"
}

# --- CHECK 1 + 2 -----------------------------------------------------------
n_div=0; n_exact=0
while IFS='|' read -r verdict file fn anchor note; do
    [ -n "${verdict:-}" ] || continue
    if [ ! -f "$file" ]; then bad "census row names a missing file: $file ($fn)"; continue; fi
    body=$(fn_body "$file" "$fn")
    if [ -z "$body" ]; then
        bad "census row $verdict $file::$fn -- function not found (renamed? re-classify against $anchor)"
        continue
    fi
    has_lock=$(printf '%s\n' "$body" | grep -c 'LockTuple(.*InplaceUpdateTupleLock' || true)
    has_unlock=$(printf '%s\n' "$body" | grep -c 'UnlockTuple(.*InplaceUpdateTupleLock' || true)
    has_lock=${has_lock:-0}; has_unlock=${has_unlock:-0}
    case "$verdict" in
      DIVERGENT-FIXED)
        n_div=$((n_div+1))
        [ "${has_lock:-0}" -ge 1 ] || bad "CHECK1 $file::$fn lost its LockTuple(InplaceUpdateTupleLock) -- C takes it at $anchor. A transactional pg_class writer without it silently discards concurrent relfrozenxid/relminmxid advances."
        [ "${has_unlock:-0}" -ge 1 ] || bad "CHECK1 $file::$fn locks but never unlocks (C unlocks at $anchor)"
        ;;
      C-EXACT)
        n_exact=$((n_exact+1))
        if [ "${has_lock:-0}" -ge 1 ]; then
            bad "CHECK2 $file::$fn grew an InplaceUpdateTupleLock, but C does not take one there ($anchor). Adding it is a divergence, not a fix -- if C changed, move this row to DIVERGENT-FIXED with the new anchor."
        fi
        ;;
      *) bad "census row has unknown verdict '$verdict' ($file::$fn)";;
    esac
done <<<"$CENSUS"

# --- CHECK 3 ---------------------------------------------------------------
declare -a censused=()
while IFS='|' read -r want file; do
    [ -n "${want:-}" ] || continue
    censused+=("$file")
    if [ ! -f "$file" ]; then bad "CHECK3 censused file vanished: $file"; continue; fi
    got=$(count_in 'catalog_indexing::CatalogTupleUpdate' "$file"); got=${got:-0}
    if [ "$got" != "$want" ]; then
        bad "CHECK3 $file has ${got:-0} CatalogTupleUpdate sites, census says $want. A pg_class writer may have been added or removed: classify the delta against the C original and update the census in this script."
    fi
done <<<"$FILECOUNTS"

# Any file outside the census that both names pg_class and updates a catalog.
while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in *tests/*|*/benches/*|*test*.rs) continue;; esac
    grep -q 'catalog_indexing::CatalogTupleUpdate' "$f" 2>/dev/null || continue
    known=0
    for c in "${censused[@]}"; do [ "$c" = "$f" ] && { known=1; break; }; done
    [ "$known" = 1 ] && continue
    bad "CHECK3 unclassified pg_class-adjacent updater: $f names pg_class and calls CatalogTupleUpdate but is not in the census. If it writes a pg_class row, check whether C's counterpart takes InplaceUpdateTupleLock and add a row; if it does not, add it with the count only."
done < <(find crates -name '*.rs' -type f -print0 2>/dev/null \
         | xargs -0 grep -lE 'RELATION_RELATION_ID|RelationRelationId' 2>/dev/null | sort)

if [ "$fail" = 0 ]; then
    say "lint-inplace-locks: PASS ($n_div locked pg_class updaters, $n_exact C-exact-unlocked, ${#censused[@]} files counted)"
else
    say "lint-inplace-locks: FAIL -- see VIOLATION lines above (notes/GL-INPLACE-1-letter.md has the full enumeration)"
fi
exit $fail
