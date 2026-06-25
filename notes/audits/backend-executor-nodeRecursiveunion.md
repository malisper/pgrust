# Audit: backend-executor-nodeRecursiveunion

- C source: `src/backend/executor/nodeRecursiveunion.c` (PG 18.3)
- c2rust:  `../pgrust/c2rust-runs/backend-executor-nodeRecursiveunion/src/nodeRecursiveunion.rs`
- Port:    `crates/backend-executor-nodeRecursiveunion/src/lib.rs`
- Branch:  `port/backend-executor-nodeRecursiveunion`
- Verdict: **PASS**

This audit is independent: every claim re-derived from the C and c2rust
sources, not from the port's comments or self-review.

## 1. Function inventory

The C TU defines exactly five functions. c2rust additionally renders a few
inlined header helpers (`PointerGetDatum`, `newNode`, `ExecProcNode`) that are
not TU functions — they are macros/inline accessors and are not separate units
to port; the port handles each at its call site (slot ids, arena alloc,
seam `exec_proc_node`).

| # | C function | C lines | Port location | Verdict |
|---|------------|---------|---------------|---------|
| 1 | `build_hash_table` (static) | 31–59 | `lib.rs:118 build_hash_table` | MATCH |
| 2 | `ExecRecursiveUnion` (static) | 80–173 | `lib.rs:208 ExecRecursiveUnion` (+ `exec_recursive_union_node:190` callback wrapper, `lookup_hash_entry:366`, `inner_chgparam_add_wtparam:382` helpers) | MATCH |
| 3 | `ExecInitRecursiveUnion` | 179–276 | `lib.rs:410 ExecInitRecursiveUnion` | MATCH |
| 4 | `ExecEndRecursiveUnion` | 284–302 | `lib.rs:535 ExecEndRecursiveUnion` | MATCH |
| 5 | `ExecReScanRecursiveUnion` | 310–344 | `lib.rs:589 ExecReScanRecursiveUnion` | MATCH |

## 2. Per-function notes

### build_hash_table — MATCH
- `Assert(node->numCols > 0)` / `Assert(node->numGroups > 0)` → `debug_assert!`.
- `desc = ExecGetResultType(outerPlanState(rustate))` → seam
  `execTuples::exec_get_result_type` on the lefttree head; cloned into the
  per-query mcx (owned-tree faithfully copies the aliased TupleDesc).
- `BuildTupleHashTable(...)` → seam `execGrouping::build_tuple_hash_table` with
  all 14 args in order: parent (None), desc, inputOps (from
  `exec_get_common_child_slot_ops`, defaulting to MinimalTuple when None,
  matching the C "NULL means any slot type"), numCols, dupColIdx, eqfuncoids,
  hashfunctions, dupCollations, numGroups, additionalsize **0**, metacxt =
  `es_query_cxt`, tablecxt = tableContext, tempcxt = tempContext,
  use_variable_hash_iv **false**. Constants and arg order verified against C.

### ExecRecursiveUnion — MATCH
- `CHECK_FOR_INTERRUPTS()` → seam `tcop_postgres::check_for_interrupts` first.
- Phase 1 (`!recursing`): outer ExecProcNode loop; `TupIsNull` break
  (`tup_is_null` = None or `TTS_EMPTY`); `numCols>0` ⇒ LookupTupleHashEntry,
  `MemoryContextReset(tempContext)`, `continue` if `!isnew`; else
  `tuplestore_puttupleslot(working_table, slot)` + `return slot`. After loop:
  `recursing = true`.
- Phase 2: inner ExecProcNode loop. On `TupIsNull`: break if
  `intermediate_empty`; else `tuplestore_clear(working_table)`, swap
  working↔intermediate (C three-assignment swap == `mem::swap`),
  `intermediate_empty = true`, `chgParam |= wtParam`, `continue`. On non-null:
  `numCols>0` dedup (reset tempContext, continue if seen), `intermediate_empty
  = false`, `tuplestore_puttupleslot(intermediate_table, slot)`, `return slot`.
  The C `else` block falls through to the loop top with no `continue`; the
  Rust structure (non-`else`, returns/continues inside) is behaviorally
  identical.
- Final `return NULL` → `Ok(None)`.
- The `&isnew` out-param and ignored entry/`NULL` hash arg match: the helper
  returns only `isnew` and discards the entry (C reads only `isnew`).

### ExecInitRecursiveUnion — MATCH
- `Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)))` → `debug_assert!`.
- State init: plan back-link set, `ExecProcNode = ExecRecursiveUnion`
  (callback wrapper), nullable fields default to None/empty,
  `recursing=false`, `intermediate_empty=true`.
- `working_table`/`intermediate_table` = `tuplestore_begin_heap(false, false,
  work_mem)`; `work_mem` via `globals::work_mem` seam.
- `numCols>0` ⇒ create tempContext "RecursiveUnion" and tableContext
  "RecursiveUnion hash table" (AllocSetContextCreate == mcx `new_child`).
- wtParam slot publish via `worktablescan::publish_wtparam_slot` (the C
  `prmdata = &es_param_exec_vals[wtParam]; prmdata->value =
  PointerGetDatum(rustate); isnull=false`; the `Assert(execPlan==NULL)` lives
  in the owner seam). Cross-node channel owned by nodeWorktablescan — correct
  ownership, panics until that owner lands.
- `Assert(node->plan.qual == NIL)` → `debug_assert!(qual.is_none())`.
- `ExecInitResultTypeTL(&ps)` seam; `ps_ProjInfo = NULL` → None.
- Child init order outer-then-inner via `execProcnode::exec_init_node`.
- `numCols>0` ⇒ `execTuplesHashPrepare` seam fills eqfuncoids/hashfunctions,
  then `build_hash_table`. Order matches C (prepare before build).

### ExecEndRecursiveUnion — MATCH
- `tuplestore_end` on both tables (via Option::take).
- `if (tempContext) MemoryContextDelete` / `if (tableContext) ...` → drop via
  `Option::take` (owned mcx drop == MemoryContextDelete); the hashtable lives
  in tableContext and is dropped with it (`hashtable.take()`).
- `ExecEndNode(outer)` / `ExecEndNode(inner)` seams. The C dereferences
  unconditionally; the port guards with `if let Some` — equivalent because a
  RecursiveUnion always has both children initialized (and a NULL child would
  be a C segfault, never a different behavior).

### ExecReScanRecursiveUnion — MATCH
- `innerPlan->chgParam |= wtParam` via `inner_chgparam_add_wtparam`.
- `if (outerPlan->chgParam == NULL) ExecReScan(outerPlan)` →
  `exec_re_scan` seam guarded by `chgParam.is_none()`.
- `if (tableContext) MemoryContextReset` → `table_ctx.reset()`.
- `if (numCols>0) ResetTupleHashTable` seam.
- `recursing=false`, `intermediate_empty=true`,
  `tuplestore_clear` on both tables.

## 3. Seams & wiring

- **Owned inbound seams:** none. No `backend-executor-nodeRecursiveunion-seams`
  crate maps to this unit's only C file. The node is reached through executor
  dispatch (execProcnode), which can depend on this crate directly without a
  cycle. Therefore `init_seams() {}` (empty) is correct, and it is still wired
  into `seams-init::init_all()` (`crates/seams-init/src/lib.rs:77`).
- **recurrence_guard:** both guard tests pass —
  `every_seam_installing_crate_is_wired_into_init_all` and
  `every_declared_seam_is_installed_by_its_owner`.
- **Outward seams** (all real `::call` into named unported owners; thin
  marshal + delegate, no logic in the seam path):
  - `execProcnode`: exec_init_node / exec_proc_node / exec_end_node
  - `execAmi`: exec_re_scan
  - `execGrouping`: exec_tuples_hash_prepare / build_tuple_hash_table /
    lookup_tuple_hash_entry / reset_tuple_hash_table
  - `execTuples`: exec_init_result_type_tl / exec_get_result_type
  - `execUtils`: exec_get_common_child_slot_ops
  - `bitmapset` (nodes/core): bms_add_member
  - `tcop_postgres`: check_for_interrupts
  - `globals` (utils-init-small): work_mem
  - `tuplestore` (sort/storage): tuplestore_begin_heap / _puttupleslot /
    _clear / _end
  - `worktablescan`: publish_wtparam_slot (owned by nodeWorktablescan, the
    cross-node RecursiveUnionState channel). RE-AUDITED post merge of main
    (1cd724bf): this seam now lives in main's LANDED
    `backend-executor-nodeWorktablescan-seams` crate (not a branch-local stub),
    declared right beside its recovery mirror `resolve_rustate`. The C deposit
    `prmdata = &(estate->es_param_exec_vals[node->wtParam]); Assert(execPlan ==
    NULL); prmdata->value = PointerGetDatum(rustate); prmdata->isnull = false;`
    is the WorkTableScan-owned param-slot alias channel; both ends (deposit +
    recovery) belong to that owner's state model, so both are seams there.
    publish_wtparam_slot has no installer yet (panics) — identical status to the
    already-landed resolve_rustate. Thin marshal+delegate (rustate/estate/wtParam
    are the exact C operands; zero logic). One call site, no E0428 duplicate.
    The duplicate `RecursiveUnionStateData` main carried in
    `types-nodes::nodeworktablescan` was collapsed to re-export the canonical
    `noderecursiveunion::RecursiveUnionStateData` (this crate's owned model).
- No own-logic stubs; `grep` confirms zero `todo!`/`unimplemented!`/
  `unreachable!`. The only panics are infallible-cast guards (`castNode`,
  NULL plan back-link) and `expect` on always-set children — all mirror C
  paths that cannot diverge.

## 3b. Design conformance — PASS
- Allocating helpers (build_hash_table, ExecInit) take `Mcx`/`es_query_cxt`
  and return `PgResult`; no allocation outside an mcx.
- No invented opacity: RecursiveUnion / RecursiveUnionStateData are real
  structs in `types-nodes::noderecursiveunion`; T_RecursiveUnion=336,
  T_RecursiveUnionState=399 verified against the c2rust NodeTag table
  (lines 215, 278).
- No shared statics for per-backend state; no ambient-global seams; no locks
  across `?`; no registry side tables; no unledgered divergence markers.

## 4. Gates

- `cargo check --workspace`: clean (warnings only, no errors).
- `cargo test -p backend-executor-nodeRecursiveunion`: ok (0 tests; unit has
  no harness-runnable surface — pure seam delegation).
- `cargo test -p seams-init`: ok (2 passed — both recurrence_guard tests).

## Verdict: PASS
Every function MATCH; zero seam findings; guard + gates green. CATALOG row set
to `audited`.
