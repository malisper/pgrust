# Audit: backend-commands-matview

Unit: `backend-commands-matview` (`src/backend/commands/matview.c`, 980 lines).
Crates: `backend-commands-matview`, `backend-commands-matview-seams`,
`backend-commands-matview-deps-seams`, `types-matview`.

Audit is independent of the port: function inventory derived from the C source
and cross-checked against `c2rust-runs/backend-commands-matview/src/matview.rs`;
constants verified against PostgreSQL 18.3 headers and the c2rust rendering.

## Function inventory (16 C functions: 5 extern + 11 static)

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `SetMatViewPopulatedState` | matview.c 78-110 | lib.rs 84-106 | MATCH | Assert relkind, syscache-copy update of `relispopulated`, CCI. pg_class update + relkind/relid reads bundled to frontier read-seams; `cache lookup failed` elog fires on `false` return. |
| 2 | `ExecRefreshMatView` | matview.c 120-140 | lib.rs 113-147 | MATCH | lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock; RangeVarGetRelidExtended (maintains-table callback folded into seam); tail-calls RefreshMatViewByOid with is_create=false. |
| 3 | `RefreshMatViewByOid` | matview.c 164-394 | lib.rs 155-401 | MATCH | All guard branches with identical predicates/SQLSTATEs: not-matview (0A000), CONCURRENTLY-not-populated (0A000), CONCURRENTLY+WITH-NO-DATA (42601), missing/too-many rules + not-SELECT-INSTEAD + not-single-action (internal elog), CONCURRENTLY-no-unique-index (55000). SRO/GUC-nest/RestrictSearchPath setup+teardown order preserved. PG_TRY/PG_CATCH depth restore modeled by match arm. CommandTag set on the FINISH path. (CommandTag value bug fixed — see Findings.) |
| 4 | `refresh_matview_datafill` | matview.c 404-462 | lib.rs 410-472 | MATCH | copy/AcquireRewriteLocks/QueryRewrite bundled; `list_length!=1` internal elog with correct CREATE/REFRESH string; CHECK_FOR_INTERRUPTS; pg_plan_query; PushCopiedSnapshot+bump; CreateQueryDesc; ExecutorStart/Run; es_processed; Finish/End/Free; PopActiveSnapshot. |
| 5 | `CreateTransientRelDestReceiver` | matview.c 464-477 | lib.rs 482-484 | SEAMED | palloc0 + vtable wiring of DR_transientrel owned by runtime (DestReceiver/DestTransientRel unported). Thin delegate. |
| 6 | `transientrel_startup` | matview.c 482-503 | lib.rs 492-498 | SEAMED | table_open + private-field fill + bulk-insert state + target-block assert touch unported table-AM/DR_transientrel internals. Thin delegate. |
| 7 | `transientrel_receive` | matview.c 508-531 | lib.rs 506-509 | SEAMED | table_tuple_insert on private fields; returns C `true`. Thin delegate. |
| 8 | `transientrel_shutdown` | matview.c 536-548 | lib.rs 517-519 | SEAMED | FreeBulkInsertState/table_finish_bulk_insert/table_close. Thin delegate. |
| 9 | `transientrel_destroy` | matview.c 553-557 | lib.rs 527-529 | SEAMED | pfree(self). Thin delegate. |
| 10 | `make_temptable_name_n` | matview.c 570-579 | lib.rs 538-548 | MATCH | initStringInfo + append + `appendStringInfo("_%d", n)`; format `"_{n}"` identical. |
| 11 | `refresh_by_match_merge` | matview.c 613-897 | lib.rs 557-866 | MATCH | All 9 SPI calls with correct result-code checks (ANALYZE / dup-check SELECT / CREATE TEMP / ALTER / INSERT diff / ANALYZE diff / DELETE / INSERT / DROP). SQL text byte-identical to C format strings. SRO toggle around CREATE TEMP TABLE preserved. Per-unique-index equality-qual loop: opclass/opfamily/get_opfamily_member resolution bundled to `index_match_merge_quals`; `opUsedForQual[attnum-1]` de-dup, `" AND "` join, and `generate_operator_clause` emission stay in-crate. `foundUniqueIndex` error (0A000), Open/Close maintenance bracketing, table/index closes, SPI_finish present. (SPI_OK_FINISH value bug fixed — see Findings.) `relnatts` bound-checked against MaxHeapAttributeNumber (1600). |
| 12 | `refresh_by_heap_swap` | matview.c 904-909 | lib.rs 874-882 | SEAMED | finish_heap_swap with fixed flags (false,false,true,true) + RecentXmin/ReadNextMultiXactId read by runtime; variable args cross seam. cluster.c unported. Thin delegate. |
| 13 | `is_usable_unique_index` | matview.c 914-949 | lib.rs 890-923 | MATCH | indisunique && indimmediate && indisvalid && predicate==NIL && indnatts>0; per-key `attnum<=0 -> false`; else true/false. pg_index reads bundled to `index_usability_info`; predicate logic in-crate. Exact. |
| 14 | `MatViewIncrementalMaintenanceIsEnabled` | matview.c 963-967 | lib.rs 931-934 | MATCH | `depth > 0`. |
| 15 | `OpenMatViewIncrementalMaintenance` | matview.c 969-973 | lib.rs 941-944 | MATCH | `depth++`. |
| 16 | `CloseMatViewIncrementalMaintenance` | matview.c 975-980 | lib.rs 951-958 | MATCH | `depth--`; debug_assert `>= 0`. |

`matview_maintenance_depth` (static, matview.c 56) → per-backend `thread_local!`
`Cell<i32>` — correct for a single-threaded backend (not a shared static).

## Constants (verified against headers / c2rust)

| Constant | C value | Port (pre-fix) | Post-fix | Source |
|----------|---------|----------------|----------|--------|
| SPI_OK_SELECT | 5 | 5 | 5 | spi.h:86 |
| SPI_OK_INSERT | 7 | 7 | 7 | spi.h:88 |
| SPI_OK_DELETE | 8 | 8 | 8 | spi.h:89 |
| SPI_OK_UTILITY | 4 | 4 | 4 | spi.h:85 |
| **SPI_OK_FINISH** | **2** | **1 (WRONG)** | **2** | spi.h:83 / c2rust 3663 |
| SECURITY_RESTRICTED_OPERATION | 0x2 | 0x2 | 0x2 | miscadmin.h:319 |
| SECURITY_LOCAL_USERID_CHANGE | 0x1 | 0x1 | 0x1 | miscadmin.h:318 |
| RelationRelationId | 1259 | 1259 | 1259 | pg_class.h:32 |
| RELKIND_MATVIEW | 'm' | 'm' | 'm' | pg_class.h:172 |
| RELPERSISTENCE_TEMP | 't' | 't' | 't' | pg_class.h:180 |
| MaxHeapAttributeNumber | 1600 | 1600 | 1600 | htup_details.h:48 |
| **CMDTAG_REFRESH_MATERIALIZED_VIEW** | **169** | **101 (WRONG)** | **169** | cmdtaglist.h (170th PG_CMDTAG) / c2rust 3235 |
| **CMDTAG_SELECT** | **179** | **111 (WRONG)** | **179** | cmdtaglist.h (180th PG_CMDTAG) / c2rust 3225 |
| CMDTAG_UNKNOWN | 0 | 0 | 0 | cmdtaglist.h (1st) |
| ERRCODE_FEATURE_NOT_SUPPORTED | 0A000 | 0A000 | — | error.rs:69 |
| ERRCODE_CARDINALITY_VIOLATION | 21000 | 21000 | — | error.rs:96 |
| ERRCODE_SYNTAX_ERROR | 42601 | 42601 | — | error.rs:258 |
| ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE | 55000 | 55000 | — | error.rs:323 |

## Seam audit

- 9 inward seams declared in `backend-commands-matview-seams`; all 9 installed by
  `init_seams()`, which contains only `set()` calls. `seams-init::init_all()`
  calls `backend_commands_matview::init_seams()` (seams-init/src/lib.rs:23). No
  uninstalled seams, no `set()` outside the owner.
- Outward seams in `backend-commands-matview-deps-seams` cover relcache/table-AM/
  lock, pg_class update, RangeVar resolution, userid/sec-context/GUC, table
  maintenance/tablespace/heap-swap, pgstat, rewrite/plan/executor/snapshot, the
  transientrel provider callbacks, and SPI/ruleutils. Every owner is genuinely
  unported, so each seam panics until its owner lands (mirror-PG-and-panic).
- Frontier read-bundles (`matview_rel_info`, `index_usability_info`,
  `index_match_merge_quals`, `update_pg_class_populated`) mirror inline relcache/
  catalog reads + genuine cross-subsystem syscache resolution. The branching
  logic that consumes them stays in-crate — no computation leaked into a seam.

## Design conformance

- Opaque handles carry the semantic opacity of the C `void`-free pointers into
  unported executor/planner/rewriter objects matview never inspects — not invented.
- `matview_maintenance_depth` is a per-backend thread-local, not a shared static.
- Allocating paths take `Mcx` and return `PgResult`; OOM surfaced via `mcx.oom`.
- No registry-shaped side tables, no locks held across `?`, no ambient-global
  seams, no unledgered divergence markers.

## Findings (fixed this round, then re-audited from scratch)

1. **`SPI_OK_FINISH` = 1, must be 2** (`backend-commands-matview/src/lib.rs:55`).
   spi.h:82-83 define `SPI_OK_CONNECT 1`, `SPI_OK_FINISH 2`; c2rust line 3663
   confirms 2. The port transcribed CONNECT's value. Effect: the final
   `SPI_finish() != SPI_OK_FINISH` check in `refresh_by_match_merge` would wrongly
   raise "SPI_finish failed" on every concurrent refresh. **Fixed to 2.**
2. **`CommandTag::REFRESH_MATERIALIZED_VIEW` = 101 / `SELECT` = 111, must be
   169 / 179** (`types-matview/src/lib.rs:46,48`). The CommandTag enum is generated
   positionally from cmdtaglist.h; REFRESH is the 170th PG_CMDTAG (index 169),
   SELECT the 180th (index 179), confirmed by c2rust lines 3235/3225. Effect:
   `RefreshMatViewByOid` stamped QueryCompletion with the wrong command-tag value.
   **Fixed to 169 / 179.**

Both fixed functions re-audited from scratch after the fix: `RefreshMatViewByOid`'s
qc.set path and `refresh_by_match_merge`'s SPI_finish check now match the C and
c2rust exactly. Crates rebuild clean.

## Verdict: PASS

All 16 functions MATCH or SEAMED (per step-3 rules). Zero seam findings.
Two transcribed-constant bugs found and fixed; re-audit clean.
