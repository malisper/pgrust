# Audit: backend-executor-execIndexing

C source: `src/backend/executor/execIndexing.c` (PG 18.3).
Port: `crates/backend-executor-execIndexing/src/lib.rs`.
Independent re-derivation from the C + c2rust rendering.

## Function inventory + verdicts

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecOpenIndices` (160) | `ExecOpenIndices` | MATCH | `ri_NumIndices=0` set up front; `relhasindex` fast path; `RelationGetIndexList` len==0 bail; double-call `Assert` → `debug_assert!`; per-index `index_open(RowExclusiveLock=3)` + `BuildIndexInfo`; speculative leg `speculative && ii_Unique && !indisexclusion` → `build_speculative_index_info`; fills descs/info arrays + `ri_NumIndices`. |
| 2 | `ExecCloseIndices` (238) | `ExecCloseIndices` | MATCH | per-index `index_insert_cleanup` (IndexInfoCarrier) + `index_close(RowExclusiveLock)` + NULL the desc slot; double-call `Assert(indexDescs[i]!=NULL)` → panic on already-closed (debug-Assert rendering). Arrays not freed (FreeExecutorState). |
| 3 | `ExecInsertIndexTuples` (309) | `ExecInsertIndexTuples` + `insert_one_index` | MATCH | `GetPerTupleExprContext`+`ecxt_scantuple=slot`; per-index: NULL-desc skip, `!ii_ReadyForInserts` skip, `onlySummarizing && !ii_Summarizing` skip, partial-index predicate (`ExecPrepareQual`→`ii_PredicateState`, `ExecQual`), `FormIndexDatum`, `applyNoDupErr` (arbiter membership), `checkUnique` 4-way selection, `index_unchanged = update && index_unchanged_by_update`, `index_insert`, exclusion check (3-way violationOK/waitMode), recheck-append + `*specConflict` (set iff `indimmediate`). |
| 4 | `ExecCheckIndexConstraints` (542) | `ExecCheckIndexConstraints` + `check_one_index_constraint` | MATCH | `ItemPointerSetInvalid(conflictTid)`; per-index: NULL skip, `!ii_Unique && !ii_ExclusionOps` skip, `!ii_ReadyForInserts` skip, arbiter filter, `!indimmediate` ⇒ ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, `checkedIndex=true`, predicate, `FormIndexDatum`, `check_exclusion_or_unique_constraint(CEOUC_WAIT, violationOK=true, conflictTid)`; trailing `arbiterIndexes && !checkedIndex` ⇒ elog. |
| 5 | `check_exclusion_or_unique_constraint` (704) | `check_exclusion_or_unique_constraint` + `exclusion_scan_loop` | MATCH (one dependency gap, below) | procs/strats selection (Exclusion vs Unique); WITHOUT OVERLAPS not-empty (before NULL check); nulls-distinct early-true; ScanKey build (`SK_ISNULL|SK_SEARCHNULL` / strat / collation / proc / `i+1`); `table_slot_create` existing slot + scantuple save/restore; `retry:` loop with `index_beginscan(SnapshotDirty)`/`index_rescan`/`index_getnext_slot`/`index_endscan`; self-tuple skip + `found_self` double elog; lossy `xs_recheck` ⇒ `index_recheck_constraint`; `xwait` wait decision (`CEOUC_WAIT` / livelock spec-token + `TransactionIdPrecedes(GetCurrentTransactionId())`); `SpeculativeInsertionWait` vs `XactLockTableWait`; violationOK conflict return vs `BuildIndexValueDescription` + EXCLUSION_VIOLATION ereport (newIndex msg variants + key-detail variants). |
| 6 | `check_exclusion_constraint` (956) | `check_exclusion_constraint` (`pub`) | MATCH | delegates `(CEOUC_WAIT, violationOK=false)`. No in-repo caller yet (its caller `IndexCheckExclusion` lives in catalog/index.c, unported) — kept `pub`. |
| 7 | `index_recheck_constraint` (973) | `index_recheck_constraint` | MATCH | per-key `existing_isnull[i]` ⇒ false; `!DatumGetBool(OidFunctionCall2Coll(constr_procs[i], rd_indcollation[i], existing[i], new[i]))` ⇒ false via word-fmgr `function_call2_coll` + `.as_bool()`. |
| 8 | `index_unchanged_by_update` (1004) | `index_unchanged_by_update` | MATCH | `ii_CheckedUnchanged` cache; `ExecGetUpdatedCols`/`ExecGetExtraUpdatedCols`; per key-attr `keycol<=0`⇒hasexpression, else `bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber)` over both sets ⇒ false; `!hasexpression`⇒true; `allUpdatedCols` alias-vs-`bms_union` (free only when extra present); `RelationGetIndexExpressions` + walker; predicates deliberately ignored. |
| 9 | `index_expression_changed_walker` (1118) | `index_expression_changed_walker` + `index_expressions_changed` | MATCH | `IsA(node,Var)` ⇒ `bms_is_member(varattno - FirstLowInvalidHeapAttributeNumber)`; else `expression_tree_walker` recursion. The C top-level node is the `List*` of idxExprs; modeled by iterating the expression list and recursing per element. |
| 10 | `ExecWithoutOverlapsNotEmpty` (1147) | `ExecWithoutOverlapsNotEmpty` | MATCH | `TYPTYPE_RANGE`⇒`RangeIsEmpty(DatumGetRangeTypeP)`, `TYPTYPE_MULTIRANGE`⇒`MultirangeIsEmpty(DatumGetMultirangeTypeP)`, else internal elog; empty ⇒ ERRCODE_CHECK_VIOLATION verbatim message. typtype resolved via `type_cache_typtype(atttypid)`. |

## Constants verified

- `FirstLowInvalidHeapAttributeNumber = -7` (PG18 `access/sysattr.h`).
- `SK_ISNULL = 0x0002`, `SK_SEARCHNULL = 0x0010` (`access/skey.h`).
- `TYPTYPE_RANGE = 'r'`, `TYPTYPE_MULTIRANGE = 'm'` (`catalog/pg_type.h`).
- `RowExclusiveLock = 3` (`storage/lockdefs.h`).
- `XLTW_InsertIndex`/`XLTW_RecheckExclusionConstr` via the real `XLTW_Oper` enum.
- `IndexUniqueCheck` via the real `types_tableam::amapi::IndexUniqueCheck`.
- SQLSTATEs (EXCLUSION_VIOLATION 23P01, CHECK_VIOLATION 23514, OBJECT_NOT_IN_PREREQUISITE_STATE) from `types_error::error`.

## Seam audit

Owned inward seams (`backend-executor-execIndexing-seams`), all installed in `init_seams()`:
- `exec_open_indices` ✓, `exec_insert_index_tuples` ✓, `exec_check_index_constraints` ✓
  (the latter's decl was moved here from nodeModifyTable's insert_exec.rs — its
  original home — and nodeModifyTable now imports it from this seam crate; this
  removes the would-be execIndexing→nodeModifyTable cycle).
`init_seams()` is `set()`-only and wired into `seams-init::init_all()`.

Outward calls — direct (acyclic) where possible, seamed only on cycle/unported:
- DIRECT: indexam (`index_insert`/`index_insert_cleanup`/`index_beginscan`/
  `index_rescan`/`index_endscan` + `IndexInfoCarrier`), indexam-seams
  (`index_getnext_slot`/`index_open` — SlotId-pool form), tableam
  (`table_slot_create`), execUtils (`ExecGet{Updated,ExtraUpdated}Cols`,
  `MakePerTupleExprContext`), backend-nodes-core (`bms_is_member`/`bms_union`/
  `bms_free`/`expression_tree_walker`), common-scankey (`ScanKeyEntryInitialize`).
- SEAMED (cycle / unported owner): `form_index_datum`/`build_index_info`/
  `build_speculative_index_info` (catalog-index-seams), `exec_prepare_qual`/
  `exec_qual` (execExpr-seams), `build_index_value_description` (genam-seams),
  `relation_get_index_list`/`relation_get_index_expressions` (relcache-seams),
  `function_call2_coll` (fmgr-seams), `xact_lock_table_wait`/
  `speculative_insertion_wait` (lmgr-seams), `transaction_id_precedes`
  (transam-seams), `get_current_transaction_id` (xact-seams),
  `type_cache_typtype` (typcache-seams), `range_is_empty`/`multirange_is_empty`
  (rangetypes/multirangetypes-seams). Each is thin marshal+delegate.

New outward seams declared this change are installed by their (complete) owners:
`speculative_insertion_wait`→lmgr, `type_cache_typtype`→typcache,
`range_is_empty`→rangetypes, `multirange_is_empty`→multirangetypes.
`build_speculative_index_info` is declared but uninstalled — owner
`backend-catalog-index` is `partial` (mirror-pg-and-panic; only the
speculative-unique `ExecOpenIndices` path reaches it). Seam-install guard
(`seams-init` tests) passes, correctly exempting it as a not-complete owner.

## Findings

1. DEPENDENCY GAP (not a logic divergence) — the SnapshotDirty write-back.
   C threads `&scan->xs_snapshot` (a pointer) into `table_index_fetch_tuple`, so
   `HeapTupleSatisfiesDirty` writes `xmin`/`xmax`/`speculativeToken` back into
   the live `DirtySnapshot`, which `check_exclusion_or_unique_constraint` reads
   to decide whether to wait. The port reads `index_scan.xs_snapshot` after the
   fetch (structurally faithful), BUT the current `backend-access-index-indexam`
   `index_fetch_heap` clones `xs_snapshot` and passes the clone by `&`, so the
   dirty fields are not propagated back to `scan.xs_snapshot`. Until indexam
   threads the dirty snapshot back, the wait-for-conflicting-xact branch sees
   `xmin==xmax==0` and never waits. This is an indexam-layer gap to fix in its
   owner; execIndexing's logic mirrors C and reads the correct location.

2. errtableconstraint context-attach dropped (the `ON CONFLICT` arbiter error,
   the EXCLUSION_VIOLATION error). The `backend_utils_error` `ErrorBuilder` does
   not expose `errtableconstraint`; per the established nbtree precedent
   (`bt_check_third_page`) this is a project-wide error-context gap. The
   user-visible message/detail/SQLSTATE are reproduced verbatim. Not a logic
   divergence (context-attach only adds schema/table/constraint name fields).

3. existing-tuple slot is pushed into `es_tupleTable` and reclaimed at
   FreeExecutorState rather than freed immediately (C
   `ExecDropSingleTupleTableSlot`). FormIndexDatum/ecxt_scantuple require a
   pooled `SlotId`, and the pool is append-only; this matches the compromise the
   landed logical-conflict FormIndexDatum caller makes. Behavior-preserving
   (bounded extra slots per statement, freed at executor teardown).

## Verdict: PASS

All 10 functions MATCH. No MISSING/PARTIAL/DIVERGES. Seam audit clean
(install-guard green; the one uninstalled seam has a not-complete owner). The
three findings are a dependency-owner gap (indexam dirty snapshot), an
established project-wide error-context gap, and a behavior-preserving pool
lifetime difference — none is absent or approximated execIndexing logic.
