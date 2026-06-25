# Audit: backend-executor-nodeMemoize

- **Unit:** backend-executor-nodeMemoize
- **Branch:** port/backend-executor-nodeMemoize (re-port of the ExecInitMemoize builders)
- **c_sources:** `src/backend/executor/nodeMemoize.c`
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 1M)
- **Verdict: PASS** — all 19 functions logic-MATCH, including the six
  `ExecInitMemoize` builders that were previously `unimplemented!()` panic stubs
  (a prior false green; see F4) and are now ported in-crate over real owner
  seams. `nodeMemoize-seams` holds only the 4 inward parallel entry points (all
  installed); every outward call routes to its owner `-seams` crate.

Independent function-by-function re-audit per
`.claude/skills/audit-crate/SKILL.md`, re-derived from the sources. This audit
supersedes the prior FAIL (commit `865cda1c`), which confirmed logic parity but
FAILed on seam-ownership conformance.

Sources compared:
- C: `../pgrust/postgres-18.3/src/backend/executor/nodeMemoize.c`
- c2rust: `../pgrust/c2rust-runs/backend-executor-nodeMemoize/src/nodeMemoize.rs`
- Port: `crates/backend-executor-nodeMemoize/src/lib.rs`,
  `crates/backend-executor-nodeMemoize-seams/src/lib.rs`,
  `crates/types-nodes/src/nodememoize.rs`

`cargo check --workspace` and `cargo test --workspace` are both green.

## 1. Function inventory

`nodeMemoize.c` defines 19 distinct functions (the two `SH_DECLARE` forward
declarations are the same functions defined later). The `simplehash.h`
expansions are substrate generated from a header, legitimately replaced by the
owned `MemoizeCache`, and are not audited as nodeMemoize.c functions.

## 2. Per-function table

| # | C function | Verdict | Notes |
|---|---|---|---|
| 1 | `MemoizeHash_hash` | **MATCH** | In-crate. `pg_rotate_left32`/XOR/`murmurhash32` accumulation owned here. Binary mode → `datum::datum_image_hash` leaf (adt-datum owner) keyed off owned `key_attrs[i]`; non-binary → reads `hashfunctions[i].fn_oid` in-crate, calls `fmgr::function_call1_coll` (fmgr owner) and applies `DatumGetUInt32` (`.as_u32()`) in-crate. Marshaling in-crate, only the engine call seamed. |
| 2 | `MemoizeHash_equal` | **MATCH** | In-crate. Binary per-key compare via `datum::datum_image_eq` (adt-datum owner) with null-ness mismatch break; non-binary via `execExpr::exec_qual` (execExpr owner). Table-slot deform marshaling in-crate. |
| 3 | `build_hash_table` | MATCH | `size==0 -> 1024`; pre-reserve clamped to `MAX_CACHE_SLOTS`. |
| 4 | `prepare_probe_slot` | **MATCH** | In-crate. `key==None` per-key eval via `execExpr::exec_eval_expr_switch_context` (execExpr owner); `Some` deform via in-crate helper. Control flow owned here. |
| 5 | `entry_purge_tuples` | MATCH | `CACHE_TUPLE_BYTES` sum, drain, `complete=false`, `mem_used -= freed`. |
| 6 | `remove_cache_entry` | MATCH | purge, subtract `EMPTY_ENTRY_MEMORY_BYTES`, delete slot. |
| 7 | `cache_purge_all` | MATCH | `evictions=members`, drop cache, reset, `mem_used=0`. |
| 8 | `cache_reduce_memory` | MATCH | peak update, LRU-head eviction loop, specialkey flag, re-find sanity check. |
| 9 | `cache_lookup` | MATCH | probe, hash, chain match, LRU move_tail, miss-path key alloc, over-budget reduce. |
| 10 | `cache_store_tuple` | MATCH | copy minimal tuple, `mem_used += CACHE_TUPLE_BYTES`, append, over-budget reduce, OOM discipline. |
| 11 | `ExecMemoize` | MATCH | all five states reproduced; stats; `CHECK_FOR_INTERRUPTS` via `tcop_postgres::check_for_interrupts` (tcop owner) + reset via `execUtils::reset_expr_context`; outer fetch via `execProcnode::exec_proc_node` (execProcnode owner) + `execTuples::exec_fetch_slot_minimal_tuple`; result store/clear via `execTuples` owner. |
| 12 | `ExecInitMemoize` | MATCH | castNode + 6 in-crate builders (rows 12a–12f) drive the C control flow in order: makeNode → links/expr-context/outer-init via execUtils/execProcnode/execTuples owners → ExecInitResultTupleSlotTL/ExecCreateScanSlotFromOuterPlan → mstatus=LOOKUP → nkeys/hashkeydesc/slots → collations copy → per-key loop (`lsyscache::get_op_hash_functions`/`get_opcode`, in-crate `hashfunctions[i].fn_oid`, `execExpr::exec_init_expr`) → ExecBuildParamSetEqual → mem_limit=`get_hash_memory_limit` (nodeHash) → reset cursors/flags/stats → hashtable=NULL. |
| 12a | `make_memoize_state` | **MATCH** | `makeNode(MemoizeState)` — boxes a zeroed `MemoizeScanState` (default `ScanState`/`PlanState` heads, empty owned vectors) in `es_query_cxt`. |
| 12b | `init_plan_state_links` | **MATCH** | `ss.ps.plan = (Plan*)node` (the shared `&Node`), `ss.ps.ExecProcNode = ExecMemoize` (the `exec_memoize_node` callback), `plan_node_id` copied. |
| 12c | `init_hashkeydesc_and_slots` | **MATCH** | `hashkeydesc = ExecTypeFromExprList(param_exprs)` (execTuples owner seam); per-key `key_attrs` distilled from `TupleDescCompactAttr` (attbyval/attlen); `tableslot`/`probeslot = MakeSingleTupleTableSlot(hashkeydesc, MinimalTuple/Virtual)` (execTuples owner) into the EState pool; `param_exprs`/`hashfunctions`/value-null mirrors presized. |
| 12d | `build_cache_eq_expr` | **MATCH** | `cache_eq_expr = ExecBuildParamSetEqual(hashkeydesc, MinimalTuple, Virtual, eqfuncoids, node->collations, param_exprs, parent)` via execExpr owner seam. |
| 12e | `deform_key_params` | **MATCH** | `ExecStoreMinimalTuple(params, tableslot, false)` + `slot_getattr` for the first `numkeys` attrs (execTuples owner seams), returning the deformed `(Datum,isnull)`. |
| 12f | `copy_probe_slot_minimal_tuple` | **MATCH** | virtual-store the probe values/nulls into `probeslot` + `ExecStoreVirtualTuple` (`store_virtual_values` execTuples seam) then `ExecCopySlotMinimalTuple` (`exec_fetch_slot_minimal_tuple`). |
| 13 | `ExecEndMemoize` | MATCH | memory-accounting walk; parallel worker copyback; drop cache; outer `exec_end_node`. |
| 14 | `ExecReScanMemoize` | MATCH | mstatus=LOOKUP, reset, `chgParam==NULL -> exec_re_scan` (execAmi owner), `bms_nonempty_difference` (nodes-core owner) -> purge. |
| 15 | `ExecEstimateCacheEntryOverheadBytes` | MATCH | `sizeof` math (LP64 24/24/16). |
| 16 | `ExecMemoizeEstimate` | MATCH | early-out; `mul_size`/`add_size` overflow-checked; toc estimate. |
| 17 | `ExecMemoizeInitializeDSM` | MATCH | DSM chunk canonical store via `sup::memoize_initialize_dsm_shared_info` (execParallel-support owner). |
| 18 | `ExecMemoizeInitializeWorker` | MATCH | `shm_toc_lookup` + attach via `sup::memoize_initialize_worker_shared_info`. |
| 19 | `ExecMemoizeRetrieveInstrumentation` | MATCH | `shared_info` early-out; copy-out via `sup::memoize_retrieve_shared_info`. |

All 19 functions are logic-MATCH; the refactor changed only the seam dispatch
surface and moved node-side marshaling in-crate.

## 3. Seam audit (PASS)

**Owned seam crate:** `crates/backend-executor-nodeMemoize-seams` (maps to
nodeMemoize.c). It now declares **exactly 4** seams — the inward parallel
entry points (`exec_memoize_estimate`, `exec_memoize_initialize_dsm`,
`exec_memoize_initialize_worker`, `exec_memoize_retrieve_instrumentation`) —
and `init_seams()` installs exactly those 4 (one `set()` each, nothing else).
`seams-init` calls `backend_executor_nodeMemoize::init_seams()`. This matches
the settled sibling convention (`nodeSort-seams` holds only its 4 entry points;
`nodeMergejoin` reaches downward through owner `-seams`).

### F2 (mislocated / duplicate declarations) — RESOLVED

Every outward call now routes to its owner `-seams` crate, with node-side
marshaling in the node crate:
- **lsyscache:** `get_op_hash_functions` / `get_opcode` consumed from
  `backend-utils-cache-lsyscache-seams` (the prior duplicates deleted).
- **datum:** `datum_image_hash` / `datum_image_eq` added to
  `backend-utils-adt-datum-seams` and consumed there.
- **fmgr:** `function_call1_coll` (OID-keyed leaf) added to
  `backend-utils-fmgr-fmgr-seams`; `DatumGetUInt32` applied in-crate.
  `fmgr_info` collapses to the in-crate `hashfunctions[i].fn_oid = oid` write
  (the owned `FmgrInfo` carries only the OID — no seam needed).
- **nodeHash:** `get_hash_memory_limit` added to
  `backend-executor-nodeHash-seams`.
- **execExpr:** `exec_init_expr` / `exec_eval_expr_switch_context` / `exec_qual`.
- **execProcnode:** `exec_init_node` / `exec_proc_node` / `exec_end_node`.
- **execUtils:** `exec_assign_expr_context` / `reset_expr_context` /
  `exec_create_scan_slot_from_outer_plan`.
- **execTuples:** `exec_init_result_tuple_slot_tl` / `exec_clear_tuple` /
  `exec_force_store_minimal_tuple` / `exec_fetch_slot_minimal_tuple`.
- **execAmi:** `exec_re_scan`.
- **tcop:** `check_for_interrupts` (the established convention used by every
  sibling node).
- **execParallel-support:** the `memoize_*` accessor family (mirroring the
  existing `sort_*` family) — the entry points hold an opaque `PlanStateHandle`
  the executor owns, so the node's own field reads/writes (`instrument`,
  `shared_info`) are marshaled there exactly as nodeSort does.

### F3 (node-aware seams reshaped) — RESOLVED

The node-aware seams that took `&mut MemoizeScanState` are reshaped: the
node-side marshaling (per-key indexing, slot/expr field selection, result
placement) lives in-crate, and only the genuine engine call is seamed over the
owner's real type (e.g. `exec_eval_expr_switch_context(&ExprState, EcxtId,
estate)`, `exec_qual(&ExprState, EcxtId, estate)`, `function_call1_coll(Oid,
collation, Datum)`).

### F4 (the six `ExecInitMemoize` builders were `unimplemented!()` stubs) — RESOLVED

The prior committed state had the six builders (`make_memoize_state`,
`init_plan_state_links`, `init_hashkeydesc_and_slots`, `build_cache_eq_expr`,
`deform_key_params`, `copy_probe_slot_minimal_tuple`) as in-crate
`unimplemented!()` panic stubs, rationalized as "mirror PG and panic". Per the
audit rules a function whose own body is replaced by a panic is **MISSING**, not
a deferral — the prior PASS was a false green. They are now ported in-crate as
the C `ExecInitMemoize`/`prepare_probe_slot` control flow, routing through real
owner seams:

- `make_memoize_state` / `init_plan_state_links` are pure in-crate node
  construction + PlanState wiring (the node is now in the `Node`/`PlanStateNode`
  enums, so `ss.ps.plan = Some(&Node)` and the `ExecProcNode = exec_memoize_node`
  callback are real).
- `ExecTypeFromExprList` (added to `backend-executor-execTuples-seams`, installed
  by execTuples pointing at its real `ExecTypeFromExprList`), and
  `MakeSingleTupleTableSlot` (already declared+installed) build `hashkeydesc` and
  the `tableslot`/`probeslot` (now real `SlotId`s in the EState pool, mirroring
  C `MemoizeState.tableslot/probeslot`).
- `ExecBuildParamSetEqual` (added to `backend-executor-execExpr-seams`) compiles
  `cache_eq_expr`. execExpr is the unported keystone, so this owner seam panics
  until execExpr lands — a genuine unported *callee*, which is acceptable.
- the minimal-tuple deform/form route through the execTuples owner seams
  (`exec_force_store_minimal_tuple` + `slot_getattr_by_id`; `store_virtual_values`
  (added) + `exec_fetch_slot_minimal_tuple`) against the real `tableslot`/
  `probeslot`. These execTuples slot ops panic until execTuples lands its slot
  payload model — again a genuine unported callee, not absent logic.

The added owner seams are thin marshal + delegate (no branching/computation in
the seam path); the C control flow lives in the node crate.

## 4. Constants verified

- NodeTags: `T_Memoize=361`, `T_MemoizeState=425`.
- `sizeof` (LP64): `MemoizeEntry=24`, `MemoizeKey=24`, `MemoizeTuple=16`.
- `sizeof(MemoizeInstrumentation)=40`, `offsetof(SharedMemoizeInfo,sinstrument)=8`.
- `EXEC_FLAG_BACKWARD=0x0008`, `EXEC_FLAG_MARK=0x0010`.

## 5. Verdict

**PASS.** All 19 functions logic-MATCH. The six `ExecInitMemoize` builders are
now genuinely ported in-crate (the C `ExecInitMemoize`/`prepare_probe_slot`
control flow) over real owner seams (execTuples `ExecTypeFromExprList` /
`MakeSingleTupleTableSlot` / minimal-tuple store-fetch / `store_virtual_values`,
execExpr `ExecBuildParamSetEqual`), replacing the prior `unimplemented!()` stubs
(F4). The node now carries real `hashkeydesc`/`tableslot`/`probeslot` mirroring
the C `MemoizeState`. `nodeMemoize-seams` holds only the 4 inward parallel entry
points, all installed by `init_seams()`. Every outward call routes to its owner
`-seams` crate (the new `ExecTypeFromExprList`/`store_virtual_values` installed
by execTuples; `ExecBuildParamSetEqual` panics until the execExpr keystone lands
— a genuine unported callee). F2, F3, F4 resolved. `cargo check --workspace` and
`cargo test --workspace` green.
