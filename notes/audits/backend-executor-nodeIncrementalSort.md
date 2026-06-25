# Audit: backend-executor-nodeIncrementalSort

C source: `src/backend/executor/nodeIncrementalSort.c` (1246 LOC, PG 18.3).
Port: `crates/backend-executor-nodeIncrementalSort/src/lib.rs`.
Re-derived from the C + execnodes.h headers (not from the port's comments).

## Function inventory

| # | C fn (line) | Port location | Verdict | Notes |
|---|-------------|---------------|---------|-------|
| 1 | `instrumentSortedGroup` (127) | `instrument_sorted_group` + `INSTRUMENT_SORT_GROUP`→`instrument_sort_group_{fullsort,prefixsort}` + `select_group_info_*` | MATCH | groupCount++, DISK/MEMORY total+max accumulation, `sortMethods |= sortMethod`. Macro's shared-vs-local slot selection faithful (`shared_info && am_worker` → worker slot via `ParallelWorkerNumber` with the two C Asserts as debug_assert; else `incsort_info`). |
| 2 | `preparePresortedCols` (163) | `prepare_presorted_cols` | MATCH | Per key i: `attno=sortColIdx[i]`; `get_equality_op_for_ordering_op(sortOperators[i])` + `OidIsValid` check → `elog(ERROR,"missing equality operator…")`; `get_opcode` + `OidIsValid` check → `elog(ERROR,"missing function…")`; `fmgr_info` (via `fmgr_info_check`). Cached `FmgrInfo`/`FunctionCallInfo` collapse to `eq_func`+`collation` OIDs re-resolved at call time (repo `function_call2_coll` pattern; behaviour-identical). palloc → `vec_with_capacity_in` in `es_query_cxt`. |
| 3 | `isCurrentGroup` (211) | `is_current_group` | MATCH | Loops i from `nPresortedCols-1` down to 0; `slot_getattr` both pivot & tuple; NULL special case (both null → continue, one null → false); `FunctionCall2` → `DatumGetBool` (`Datum::as_bool`) → false on inequality; strict-null `elog(ERROR,"function %u returned NULL")` carried on the fmgr-seam Err (eq op is strict, args non-null here). Returns true. |
| 4 | `switchToPresortedPrefixMode` (285) | `switch_to_presorted_prefix_mode` | MATCH | First-time `tuplesort_begin_heap(numCols-nPresortedCols, &keys[nPresortedCols..])` else `tuplesort_reset`; bounded `tuplesort_set_bound(bound-bound_Done)`; transfer loop with carry-over (`nTuples==0 && !TupIsNull(transfer_tuple)` puts transfer + copies pivot), else get from full sort, first-time pivot copy, isCurrentGroup put-or-break-with-clear; `n_fullsort_remaining -= nTuples`; `==0` → copy pivot, LOADPREFIXSORT, clear transfer; `!=0` → performsort, instrument, bound_Done, READPREFIXSORT. |
| 5 | `ExecIncrementalSort` (494) | `ExecIncrementalSort` | MATCH | CHECK_FOR_INTERRUPTS; READ-state re-entry (gettupleslot into result; `got||outerNodeDone`→return; `n_fullsort_remaining>0`→re-switch; else LOADFULLSORT); `es_direction=Forward`; LOADFULLSORT (begin/reset, minGroupSize w/ bound, group_pivot carry + clear-unless-==minGroup, pull loop: TupIsNull→performsort+READFULLSORT; <minGroup→put+pivot-at-min; isCurrentGroup→put; else→pivot-carry+bound_Done+performsort+READFULLSORT; `>MAX_FULL_SORT_GROUP_SIZE && !=READFULLSORT`→clear pivot+performsort+instrument+used_bound clamp+`n_fullsort_remaining`+switch); LOADPREFIXSORT loop (TupIsNull→outerNodeDone; isCurrentGroup→put; else→pivot-carry); performsort+READPREFIXSORT+bound_Done; restore dir; final gettupleslot. |
| 6 | `ExecInitIncrementalSort` (975) | `ExecInitIncrementalSort` | MATCH | castNode; makeNode + plan/state/ExecProcNode links; field init (LOADFULLSORT, all None/0/false); instrument zeroing; `ExecInitNode(outerPlan)` w/ unmodified eflags; `ExecCreateScanSlotFromOuterPlan(MinimalTuple)`; `ExecInitResultTupleSlotTL(MinimalTuple)`; `ps_ProjInfo=None`; two `MakeSingleTupleTableSlot(outer result type, MinimalTuple)` standalone slots. |
| 7 | `ExecEndIncrementalSort` (1076) | `ExecEndIncrementalSort` | MATCH | `ExecDropSingleTupleTableSlot` group_pivot+transfer_tuple; `tuplesort_end` both states (None after); `ExecEndNode(outerPlanState)`. |
| 8 | `ExecReScanIncrementalSort` (1106) | `ExecReScanIncrementalSort` | MATCH | clear result slot; clear group_pivot/transfer_tuple if present; reset outerNodeDone/n_fullsort_remaining/bound_Done; LOADFULLSORT; `tuplesort_reset` both if present (not dropped — matches C leak note); `if chgParam==NULL ExecReScan(outerPlan)`. |
| 9 | `ExecIncrementalSortEstimate` (1172) | `ExecIncrementalSortEstimate` | MATCH | `!instrument || nworkers==0` guard; `size = nworkers*sizeof(IncrementalSortInfo) + offsetof(.,sinfo)=8`; estimate_chunk + estimate_keys(1). |
| 10 | `ExecIncrementalSortInitializeDSM` (1193) | `ExecIncrementalSortInitializeDSM` | SEAMED (sanctioned mirror-and-panic) | guards run; the DSM allocate/memset/insert + `shared_info` carrier handoff panics into the DSM owner. In-process `PgBox<SharedIncrementalSortInfo>` carrier can't hold the DSM `SharedRef`; identical blocker to nodeSort/nodeAgg `InitializeDSM` (documented in CATALOG/note). Not MISSING — no own-logic is absent; only the unported DSM-carrier callee is unreachable. |
| 11 | `ExecIncrementalSortInitializeWorker` (1218) | `ExecIncrementalSortInitializeWorker` | SEAMED (sanctioned) | same DSM-carrier blocker; doing only the `am_worker=true` write would silently diverge, so mirror-and-panic. |
| 12 | `ExecIncrementalSortRetrieveInstrumentation` (1232) | `ExecIncrementalSortRetrieveInstrumentation` | SEAMED (sanctioned) | `shared_info==NULL` guard runs; the palloc+memcpy copy-out panics into the DSM owner (no DSM round-trip happened with the in-process carrier). |

## Constants verified against headers / c2rust

- `T_IncrementalSort = 363`, `T_Sort = 362`, `T_IncrementalSortState = 427` — match c2rust nodetags + ffi node_tags.
- `DEFAULT_MIN_GROUP_SIZE = 32`, `DEFAULT_MAX_FULL_SORT_GROUP_SIZE = 64` (= 2×) — match C.
- `offsetof(SharedIncrementalSortInfo, sinfo) = 8` — match (int + 8-byte-aligned flexarray); same literal as the merged nodeSort/SharedSortInfo header.
- `EXEC_FLAG_BACKWARD=0x0008`, `EXEC_FLAG_MARK=0x0010` — used only in the init debug_assert; from executor.h.
- `TUPLESORT_NONE=0`, `TUPLESORT_ALLOWBOUNDED=2` — match tuplesort.h.

## Types keystone (added this change)

`types-nodes::nodeincrementalsort`: `IncrementalSort` plan node (embeds `Sort`),
`IncrementalSortStateData`, `PresortedKeyData`, `IncrementalSortGroupInfo`,
`IncrementalSortInfo`, `SharedIncrementalSortInfo`,
`IncrementalSortExecutionStatus`. `Node::IncrementalSort` +
`PlanStateNode::IncrementalSort` variants with tag/plan/clone_in/ps_head(_mut)
arms. Field-for-field faithful to execnodes.h; `presorted_keys: Option<PgVec>`
(`None` = C `NULL`, matches the C init).

## Seam audit

- Owned inward seam crate: `backend-executor-nodeIncrementalSort-seams` (4
  parallel hooks). All 4 installed by `init_seams()` (only `set()` calls).
  Wired into `seams-init::init_all()`. PASS.
- New outward seams added to AUDITED owners and installed there:
  - execTuples-seams: `slot_getattr_standalone`, `exec_clear_tuple_standalone`,
    `exec_copy_slot_standalone`, `exec_copy_pool_slot_into_standalone` — thin
    delegates to `slot_deform::slot_getattr` / `slot_store_fetch::{ExecClearTuple,
    ExecCopySlot}` over the standalone `&mut SlotData` (group_pivot/transfer_tuple
    are `MakeSingleTupleTableSlot` slots outside `es_tupleTable`). Installed in
    execTuples `init_seams()`.
  - tuplesort-seams: `tuplesort_reset`, `tuplesort_used_bound`,
    `tuplesort_puttupleslot_standalone`, `tuplesort_gettupleslot_standalone` —
    thin delegates to the real `tuplesort_reset`/`tuplesort_used_bound` and the
    existing `_impl` over `slot.base()`. Installed in tuplesort `init_seams()`.
  - `recurrence_guard::every_declared_seam_is_installed_by_its_owner` passes →
    no uninstalled declared seam.
- All outward calls are thin marshal+delegate; the per-state / per-slot
  selection logic lives in in-crate helpers (`put_standalone`, `copy_standalone`,
  `sort_state_mut`, …), not in any seam. PASS.

## Design conformance

- No invented opacity: `PresortedKeyData` carries real `Oid`/`AttrNumber`; the
  cached `FmgrInfo`/`FunctionCallInfo` is reduced to an OID + collation
  re-resolved at call (matches the repo-wide `function_call2_coll` model).
- Allocating seams carry `Mcx` + return `PgResult`. No infallible
  `format!`/`vec!`/`to_string` on a palloc path.
- No shared statics, no ambient-global getters, no registry side tables, no
  locks held across `?`.
- One `unsafe` (`split_pivot_transfer` disjoint reborrow of two distinct struct
  fields) — sound and documented.
- Parallel DSM panics are sanctioned mirror-and-panic into an unported callee
  (the DSM-resident `shared_info` carrier), with the same rationale as the
  merged nodeSort/nodeAgg; no own logic is absent.

## Verdict: PASS

Every function MATCH or sanctioned SEAMED (parallel-DSM carrier, identical to
the merged nodeSort/nodeAgg blocker). Zero seam findings. Gates: `cargo check
--workspace` clean; `cargo test -p seams-init` (both recurrence guards) green;
`cargo test -p no-todo-guard` green; `cargo test -p backend-executor-nodeIncrementalSort`
(4 tests) green.
