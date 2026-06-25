# Audit: backend-utils-activity-xact

- **Unit:** `backend-utils-activity-xact` (`src/backend/utils/activity/pgstat_xact.c`, PostgreSQL 18.3)
- **Crates:** `crates/backend-utils-activity-xact`, `crates/backend-utils-activity-shmem-seams` (new seam-declaration crate)
- **Branch:** `port/backend-utils-activity-xact`
- **C source:** `../pgrust/postgres-18.3/src/backend/utils/activity/pgstat_xact.c` (387 lines)
- **c2rust:** `../pgrust/c2rust-runs/backend-utils-activity-xact/src/pgstat_xact.rs`
- **Auditor:** independent re-derivation from C + c2rust; constants verified against
  `include/utils/pgstat_kind.h`, `include/access/xact.h`, `include/utils/elog.h`/`types-error`.

## Function inventory and verdicts

The C file defines 12 functions (10 externs, 2 statics). c2rust kept all 12; no
`#if`-conditional bodies exist in this file.

| # | C function (pgstat_xact.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `AtEOXact_PgStat` (l.40) | `AtEOXact_PgStat` (l.90) | MATCH | DB hook seam first, then relations + dropped-stats only when stack non-empty, then `pgStatXactStack = NULL` (clear), then `pgstat_clear_snapshot()`. Asserts (`nest_level == 1`, `prev == NULL`) are `debug_assert`s (`stack.len() == 1` is the `prev == NULL` analogue). On `Err` from dropped-stats, the clear and snapshot reset are skipped — same as C's longjmp before `pgStatXactStack = NULL`. |
| 2 | `AtEOXact_PgStat_DroppedStats` (static, l.67) | `AtEOXact_PgStat_DroppedStats` (l.122) | MATCH | Front-to-back drain; commit drops non-create items, abort drops create items; `not_freed_count` → `pgstat_request_entry_refs_gc()`; item removed *after* the drop call, so an `Err` leaves the in-flight item queued exactly like C's longjmp before `dclist_delete_from`. Empty-list early exit is the loop's immediate break (gc check still skipped: count is 0). |
| 3 | `AtEOSubXact_PgStat` (l.113) | `AtEOSubXact_PgStat` (l.168) | MATCH | Pop iff `top.nest_level >= nestDepth` (one node, as C), delinked before the relation/dropped-stats calls; popped node dropped at scope end = `pfree(xact_state)`. |
| 4 | `AtEOSubXact_PgStat_DroppedStats` (static, l.136) | `AtEOSubXact_PgStat_DroppedStats` (l.188) | MATCH | Empty early-return before parent ensure; `pgstat_get_xact_stack_level(nestDepth - 1)` = `ensure_xact_stack_level` (parent is the stack top since the child was already delinked, same as the C pointer); item removed before the branch (`pop_front` = `dclist_delete_from` at loop head); abort+create → drop_entry, commit → push to parent tail (order preserved), else free. gc request on `not_freed_count > 0`. |
| 5 | `AtPrepare_PgStat` (l.191) | `AtPrepare_PgStat` (l.241) | MATCH | Relations hook only when stack non-empty; asserts as `debug_assert`s. |
| 6 | `PostPrepare_PgStat` (l.211) | `PostPrepare_PgStat` (l.262) | MATCH | Relations hook when non-empty, then stack clear, then `pgstat_clear_snapshot()`. Infallible like the C (relations hook frees only). |
| 7 | `pgstat_get_xact_stack_level` (l.238) | `pgstat_get_xact_stack_level` / `ensure_xact_stack_level` (l.292/296) | MATCH | New node pushed iff top is absent or `top.nest_level != nest_level` — only the current top is consulted, as in C. `MemoryContextAlloc` OOM modeled as `try_reserve` → `ereport(ERROR)`-shaped `PgError` (ERRCODE_OUT_OF_MEMORY). The C's `first = NULL` field belongs to pgstat_relation.c's per-level state (see seam audit). Returns `()` instead of the node pointer: the stack is crate-private and every caller addresses the stack top, which is the node C returns. |
| 8 | `pgstat_get_transactional_drops` (l.272) | `pgstat_get_transactional_drops` (l.333) | MATCH | NULL stack → 0 items (empty `PgVec`); commit skips create items, abort skips non-create items; array capacity = full pending count (C pallocs the same over-size); iterates only the top node, as C. Asserts as `debug_assert`s. |
| 9 | `pgstat_execute_transactional_drops` (l.314) | `pgstat_execute_transactional_drops` (l.371) | MATCH | `(ndrops, items)` = slice; empty early-return; per-item drop_entry; gc on `not_freed_count > 0`; `is_redo` unused in the C body too (confirmed in source and c2rust). |
| 10 | `create_drop_transactional_internal` (static, l.335) | `create_drop_transactional_internal` (l.393) | MATCH | `GetCurrentTransactionNestLevel()` via seam; ensure level; item fields `kind as int`, `dboid`, `objid_lo/hi` = the single `u64`; tail push. C allocates the item before ensuring the level — only OOM-ordering differs, both surface the same out-of-memory ERROR. |
| 11 | `pgstat_create_transactional` (l.361) | `pgstat_create_transactional` (l.431) | MATCH | Existence probe (`pgstat_get_entry_ref(..., false, NULL) != NULL`) via seam; on hit, `ereport(WARNING, ...)` with identical message text (`resetting existing statistics for kind %s, db=%u, oid=%llu` — `Oid = u32`, `objid: u64` Display reproduce `%u`/`PRIu64`), severity WARNING (19), default sqlstate 01000 matching elog.c's default for warnings; then `pgstat_reset` via seam; then internal create item. Location metadata says line 365 vs c2rust's 368 (start vs end of the multi-line `ereport`) — cosmetic, not behavioral. |
| 12 | `pgstat_drop_transactional` (l.384) | `pgstat_drop_transactional` (l.455) | MATCH | One-line delegate with `is_create = false`. |

### State model

- `static PgStat_SubXactStatus *pgStatXactStack` (backend-local) →
  `thread_local! PG_STAT_XACT_STACK: RefCell<Vec<PgStat_SubXactStatus>>`; the C's
  `prev`-linked stack is the Vec order (top = last). Empty Vec = C's NULL.
- `PgStat_PendingDroppedStatsItem`'s intrusive `dlist_node` is the containing
  `VecDeque`; `dclist` count/order semantics (tail push, front-to-back iterate,
  delete-from) are reproduced by `push_back`/`pop_front`.
- `xl_xact_stats_item { int kind; Oid dboid; uint32 objid_lo, objid_hi }`
  (verified in `access/xact.h:282-293`) → `types_core::xact::XlXactStatsItem
  { kind: i32, dboid: Oid, objid: u64 }`; every C use recombines
  `(hi << 32) | lo` into a u64 (l.80, 153, 324) or splits it (l.347-348), so the
  single u64 is loss-free. Round-trip covered by a unit test.
- `PgStat_Kind` = `uint32` verified against `utils/pgstat_kind.h:17`.
- `TopTransactionContext` residency: the C uses it purely for lifetime (freed at
  the same teardown points this crate owns); the thread-local owns the data and
  the same functions free it. `try_reserve` → out-of-memory ERROR models
  `MemoryContextAlloc`'s failure path.
- `PgStat_SubXactStatus.first` (the per-relation `PgStat_TableXactStatus`
  chain) is pgstat_relation.c's data; that unit models it in its own per-level
  state and keeps the levels in sync by calling this crate's public
  `pgstat_get_xact_stack_level`. The relation hooks therefore cross as
  scalar-only seams (see below).

## Seam audit

`init_seams()` is empty (this unit owns no seam crate; nothing to install) and
is called by `seams-init::init_all()`. No `set()` call exists outside tests
(tests install thread-dispatching stubs once per process, the established repo
pattern). All outward seams:

| Seam | Crate | C callee | Thin? | Justification |
|---|---|---|---|---|
| `get_current_transaction_nest_level` | backend-access-transam-xact-seams | `GetCurrentTransactionNestLevel()` (xact.c) | yes — nullary scalar read | real cycle: xact.c calls `AtEOXact_PgStat`/`AtEOSubXact_PgStat`/`AtPrepare_PgStat`/`PostPrepare_PgStat`/`pgstat_get_transactional_drops` in this unit |
| `pgstat_clear_snapshot` | backend-utils-activity-pgstat-seams | `pgstat_clear_snapshot()` (pgstat.c) | yes | pgstat.c unit unported; pgstat.c ↔ pgstat_xact.c are one `pgstat_internal.h` cluster (pgstat.c's snapshot/reset machinery calls back into per-kind code that reaches the xact stack) |
| `pgstat_reset` | backend-utils-activity-pgstat-seams | `pgstat_reset(kind, dboid, objid)` (pgstat.c) | yes | as above |
| `pgstat_get_kind_name` | backend-utils-activity-pgstat-seams | `(pgstat_get_kind_info(kind))->name` (pgstat.c) | yes — returns the static name only, the single field the consumer reads | as above |
| `pgstat_drop_entry` | backend-utils-activity-shmem-seams (new) | `pgstat_drop_entry()` (pgstat_shmem.c) | yes | pgstat_shmem.c unported; shmem ↔ xact mutual reach through the pgstat core |
| `pgstat_request_entry_refs_gc` | backend-utils-activity-shmem-seams | `pgstat_request_entry_refs_gc()` (pgstat_shmem.c) | yes — nullary | as above |
| `pgstat_get_entry_ref_exists` | backend-utils-activity-shmem-seams | `pgstat_get_entry_ref(kind, dboid, objid, false, NULL) != NULL` (pgstat_shmem.c) | yes — pointer→bool is result conversion; the consumer uses only the NULL-ness | as above |
| `at_eoxact_pgstat_database` | backend-utils-activity-stat-seams | `AtEOXact_PgStat_Database()` (pgstat_database.c) | yes | real cycle: pgstat_relation.c calls `pgstat_get_xact_stack_level` in this unit |
| `at_eoxact_pgstat_relations` | backend-utils-activity-stat-seams | `AtEOXact_PgStat_Relations(xact_state, isCommit)` | yes — `xact_state` carries only the `first` chain (relation-owned state) plus asserted-constant `nest_level == 1`, so only `isCommit` crosses | as above |
| `at_eosubxact_pgstat_relations` | backend-utils-activity-stat-seams | `AtEOSubXact_PgStat_Relations(xact_state, isCommit, nestDepth)` | yes — same `first`-chain reasoning; scalars cross | as above |
| `at_prepare_pgstat_relations` | backend-utils-activity-stat-seams | `AtPrepare_PgStat_Relations(xact_state)` | yes | as above |
| `post_prepare_pgstat_relations` | backend-utils-activity-stat-seams | `PostPrepare_PgStat_Relations(xact_state)` | yes | as above |

Fallibility of seam signatures checked against the C callees:
`AtEOXact_PgStat_Database` and `AtEOXact_PgStat_Relations` are pure counter
arithmetic (infallible — verified in pgstat_database.c / pgstat_relation.c);
`AtEOSubXact_PgStat_Relations` can reach `MemoryContextAlloc` (PgResult);
`AtPrepare_PgStat_Relations` reaches `RegisterTwoPhaseRecord`'s repalloc
(PgResult); `PostPrepare_PgStat_Relations` frees only (infallible);
`pgstat_clear_snapshot`/`pgstat_request_entry_refs_gc` are infallible in C.

No seam contains branching, node construction, or computation. No function
body was replaced by a seam call to its own logic: all 12 bodies live in this
crate.

Findings: none.

## Build / tests

- `cargo build` clean; `cargo test -p backend-utils-activity-xact`: 11/11 pass
  (commit/abort drop selection, gc-request on not-freed, reset-existing
  warning path, subxact abort/commit promotion, drops filtering, execute
  drops, 2PC hooks, objid hi/lo round-trip).
- `cargo test --workspace`: no failures.

## Verdict

**PASS** — all 12 functions MATCH; seams are thin, justified, and correctly
wired; `seams-init` calls the (empty) `init_seams()`. `CATALOG.tsv` row set to
`audited`.
