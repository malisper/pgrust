# Audit: backend-commands-cluster

- **Unit:** `backend-commands-cluster` (`src/backend/commands/cluster.c`)
- **Branch:** `port/backend-commands-cluster`
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent re-derivation from the C (`../pgrust/postgres-18.3/src/backend/commands/cluster.c`),
the c2rust rendering (`../pgrust/c2rust-runs/backend-commands-cluster/src/cluster.rs`),
and the port (`crates/backend-commands-cluster`, `-seams`, `crates/types-cluster`,
`crates/types-rusage`). Does not trust the port's own comments or self-review.

This is the re-audit after the prior FAIL (findings S1 + D1, recorded below);
both are confirmed resolved this round. Logic was already MATCH on every
function; this pass re-derives the resolved areas plus a fresh logic spot-check.

## 1. Function inventory

cluster.c defines exactly 13 named functions (confirmed by enumerating the C and
cross-checking c2rust's `cluster.rs`):

`cluster` (107), `cluster_multiple_rels` (263), `cluster_rel` (311),
`check_index_is_clusterable` (494), `mark_index_clustered` (554),
`rebuild_relation` (629), `make_new_heap` (705), `copy_table_data` (831),
`swap_relation_files` (1063), `finish_heap_swap` (1445),
`get_tables_to_cluster` (1643), `get_tables_to_cluster_partitioned` (1697),
`cluster_is_permitted_for_relation` (1745).

The other defs c2rust kept in `cluster.rs` (`MemoryContextSwitchTo`,
`BoolGetDatum`, `ObjectIdGetDatum`, `GETSTRUCT`, `table_endscan`,
`table_relation_copy_for_cluster`) are static-inline header wrappers, not
cluster.c-owned. No `#if`-gated function is absent from the build config. Every
cluster.c function is present in the port.

## 2. Per-function table

| # | C function (cluster.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `cluster` (107) | `cluster` | MATCH | Option-parse loop, single-rel vs multi-rel split, AccessExclusiveLock acquire, RELATION_IS_OTHER_TEMP reject, index-by-indisclustered scan / by-name lookup, partitioned branch, `PreventInTransactionBlock`, RECHECK/RECHECK_ISCLUSTERED bits, cleanup. `cluster_context` modeled as `mcx` (PortalContext lifetime); MemoryContextDelete = drop. SQLSTATEs SYNTAX_ERROR / FEATURE_NOT_SUPPORTED / UNDEFINED_OBJECT match. |
| 2 | `cluster_multiple_rels` (263) | `cluster_multiple_rels` | MATCH | PopActiveSnapshot + Commit, per-rel Start/PushSnapshot/table_open(AccessExclusiveLock)/cluster_rel/Pop/Commit. |
| 3 | `cluster_rel` (311) | `cluster_rel` + `cluster_rel_body` | MATCH | `goto out` modeled as body fn + unconditional `out:` cleanup (`AtEOXact_GUC`, `SetUserIdAndSecContext`, `pgstat_progress_end_command`) on both Ok and Err. Recheck legs, shared-catalog/other-temp rejects, CheckTableNotInUse, index validity, unpopulated-matview skip, predicate-lock transfer, rebuild_relation. SECURITY_RESTRICTED_OPERATION = 0x0002. |
| 4 | `check_index_is_clusterable` (494) | `check_index_is_clusterable` | MATCH | **Re-derived this round, line-by-line.** rd_index NULL / wrong indrelid (WRONG_OBJECT_TYPE), amclusterable, partial-index (indpred), indisvalid checks (all FEATURE_NOT_SUPPORTED); index_close(NoLock). Message text + SQLSTATEs exact. |
| 5 | `mark_index_clustered` (554) | `mark_index_clustered` | MATCH | Partitioned reject, already-clustered short-circuit, per-index deform/clear-or-set/reform/CatalogTupleUpdate, invalid-index elog, post-alter hook. cache-miss + invalid index = `elog_error` (XX000). |
| 6 | `rebuild_relation` (629) | `rebuild_relation` | MATCH | mark_index_clustered, remember relpersistence/is_system_catalog, make_new_heap, table_open new, copy_table_data, close all keep-lock, finish_heap_swap (`check_constraints=false`, `is_internal=true`). |
| 7 | `make_new_heap` (705) | `make_new_heap` | MATCH (SEAMED core) | reloptions fetch, pg_temp vs same namespace, `pg_temp_%u` name (NAMEDATALEN truncation), heap create, CCI, TOAST table create if `reltoastrelid` valid. 20-arg `heap_create_with_catalog(...)` collapses to `heap_create_with_catalog_transient` seam carrying the fixed-arg specialization (constant call-site args, not lost logic). Thin delegate. |
| 8 | `copy_table_data` (831) | `copy_table_data` | MATCH | **rusage path re-derived this round.** toast lock-to-EOXact, swap-by-content rd_toastoid set, vacuum_get_cutoffs + FreezeLimit/MultiXactCutoff max-clamp, use_sort only for BTREE_AM_OID(403), three log branches, AM copy, rd_toastoid reset, relpages/reltuples update, pg_class self-update suppression, CCI. The `ereport(elevel, ...)` msg/detail strings are cluster.c's own literals rendered in-crate; `pg_rusage_show(&ru0)` is the real `types_rusage::PgRUsage` passed by value through a thin seam. |
| 9 | `swap_relation_files` (1063) | `swap_relation_files` | MATCH | non-mapped swap of relfilenode/reltablespace/relam/relpersistence (+toast links if !by_content); mapped-rel backstop elogs; relfilelocator subid block (SEAMED, §3); relfrozenxid/relminmxid set for non-index; stats swap; pg_class update vs CacheInvalidate; changeDependencyFor !=1 elogs; post-alter hooks; toast by-content recursion / by-link dependency rewrite; toast-index recursion. All elog text + XX000 match. |
| 10 | `finish_heap_swap` (1445) | `finish_heap_swap` | MATCH | progress phases 5/6/7; swap_relation_files(target_is_pg_class = OIDOldHeap==1259); CacheInvalidateCatalog if system; reindex_flags SUPPRESS_INDEX_USE|CHECK_CONSTRAINTS|FORCE_*; reindex_relation; pg_class self relfrozenxid fixup; performDeletion(DROP_RESTRICT, PERFORM_DELETION_INTERNAL=0x0001); mapped-table removal loop; toast rename + ResetRelRewrite (CCI before) when !by_content; RelationClearMissing if !system. |
| 11 | `get_tables_to_cluster` (1643) | `get_tables_to_cluster` | MATCH | indisclustered scan batched into `scan_indisclustered` seam (systable_scan precedent); per-row aclcheck + push stay in-crate. List allocated in `mcx`. |
| 12 | `get_tables_to_cluster_partitioned` (1697) | `get_tables_to_cluster_partitioned` | MATCH | find_all_inheritors(NoLock), IndexGetRelation, leaf-only (RELKIND_INDEX) filter, per-leaf aclcheck, push. |
| 13 | `cluster_is_permitted_for_relation` (1745) | `cluster_is_permitted_for_relation` | MATCH | pg_class_aclcheck(ACL_MAINTAIN)==OK -> true; else WARNING "permission denied to cluster ... skipping it" -> false. |

Constants spot-checked against headers: BTREE_AM_OID=403, RelationRelationId=1259,
SECURITY_RESTRICTED_OPERATION=0x0002, PERFORM_DELETION_INTERNAL=0x0001,
PROGRESS_CLUSTER_PHASE_{SWAP=5,REBUILD=6,FINAL=7}, REINDEX_REL_* — all correct.

## 3. Seam audit

**Owned seam crate (by C-source coverage):** the unit's only C file is
`cluster.c`, so the single owned seam crate is
`crates/backend-commands-cluster-seams`. It declares 5 inward seams
(`cluster_rel`, `check_index_is_clusterable`, `mark_index_clustered`,
`make_new_heap`, `finish_heap_swap`); `seams_install.rs` `init_seams()` installs
all 5 with nothing but `set()` calls (verified — no logic in the installer), and
`seams-init/src/lib.rs` calls `backend_commands_cluster::init_seams()`. No
uninstalled owned seam.

**Inward-seam bodies:** the five installed functions are the real in-crate
implementations — none is a body replaced by a call to "somewhere else".

**Outward seams** are now per-owner `-seams` crate declarations only (panic
until the owner lands). No branching, node construction, or computation lives in
any seam path — `copy_table_data` pre-renders the log/detail strings in-crate
(cluster.c's own format literals) and the `ereport_msg` seam only emits at the
given elevel; the relfilelocator subid block batches the C
`relation_open`/`rd_*Subid` copy/`RelationAssumeNewRelfilelocator`/close into one
relcache-owner seam. Thin.

### Finding S1 (prior FAIL) — RESOLVED

The previous round bundled ~40 outward seams into one consumer-owned grab-bag
crate `backend-commands-cluster-deps-seams`. That crate no longer exists. The
cluster crate's `Cargo.toml` now depends on per-owner `-seams` crates, one per
owning unit (`backend-catalog-namespace-seams`, `backend-access-table-table-seams`,
`backend-access-index-indexam-seams`, `backend-utils-cache-relcache-seams`,
`backend-utils-cache-syscache-seams`, `backend-catalog-indexing-seams`,
`backend-utils-cache-inval-seams`, `backend-catalog-catalog-seams`,
`backend-catalog-heap-seams`, `backend-catalog-toasting-seams`,
`backend-access-common-toastdesc-seams`, `backend-catalog-index-seams`,
`backend-catalog-pg-inherits-seams`, `backend-commands-tablecmds-seams`,
`backend-commands-vacuum-seams`, `backend-optimizer-plan-planner-seams`,
`backend-storage-lmgr-predicate-seams`, `backend-access-heap-heapam-seams`,
`backend-access-tableam-seams`, `backend-utils-cache-relmapper-seams`,
`backend-catalog-objectaccess-seams`, `backend-utils-adt-catalog-perm-seams`,
`backend-utils-activity-small-seams`, `backend-utils-time-snapmgr-seams`,
`backend-utils-misc-clean-seams`, `backend-utils-error-elog-seams`,
`backend-catalog-pg-depend-seams`, `backend-catalog-dependency-seams`,
`backend-storage-lmgr-lmgr-seams`, `backend-utils-init-miscinit-seams`,
`backend-utils-misc-guc-file-seams`, `backend-utils-cache-lsyscache-seams`,
`backend-tcop-postgres-seams`, `backend-access-transam-xact-seams`,
`backend-parser-small1-seams`). AGENTS.md §2 satisfied. **Resolved.**

## 3b. Design conformance

### Finding D1 (prior FAIL — introduced opacity) — RESOLVED

`types_cluster::PgRusageToken(pub u64)` no longer exists. `PGRUsage` is now the
real spelled-out struct `types_rusage::PgRUsage { tv, ru_utime, ru_stime:
Timeval }`, carrying exactly the three `struct timeval`s `pg_rusage_show` ever
reads (verified against `utils/pg_rusage.h` and `pg_rusage.c`). The
`backend-utils-misc-clean-seams` declarations pass it by value:
`pg_rusage_init() -> PgRUsage` returns the populated struct the caller owns, and
`pg_rusage_show(mcx, ru0: PgRUsage) -> PgResult<PgString>` renders it. cluster.c
owns the struct (`PGRUsage ru0;` stack local); the seam only crosses to the
unported `backend-utils-misc-clean` owner (`pg_rusage.c`) for the
`getrusage`/`gettimeofday` OS snapshot + delta formatting — the genuine
owner-side work. No handle-newtype, no stand-in. types.md rule 6 satisfied.
**Resolved.**

No other 3b violations: allocating seams take `Mcx` + return `PgResult`; no
shared statics for per-backend globals; no ambient-global value getters; no
locks held across `?` without guards (toast `LockRelationOid` uses a `.keep()`
guard to mirror lock-to-EOXact); no registry side tables; no unledgered
divergence markers. Build green (`cargo build -p backend-commands-cluster`).

## 4. Verdict

**PASS.** All 13 cluster.c functions are logic-MATCH (two acceptable
inward-batched seams, one acceptable fixed-arg seam specialization, all outward
seams thin). The two prior merge-blocking design findings are confirmed
resolved: S1 (per-owner seam split) and D1 (real `PgRUsage` struct, no
introduced opacity). No MISSING/PARTIAL/DIVERGES. Set the `CATALOG.tsv` row to
`audited`; the crate may merge.
