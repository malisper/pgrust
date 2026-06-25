# Audit: backend-executor-nodeAgg

- **Verdict:** PASS (after one fix-and-re-audit round)
- **Date:** 2026-06-13
- **Model:** Claude Fable 5
- **Branch:** fix/execgrouping-reconcile
- **C source:** `src/backend/executor/nodeAgg.c` (PG 18.3)
- **c2rust:** `c2rust-runs/backend-executor-nodeAgg/src/nodeAgg.rs`
- **Port:** `crates/backend-executor-nodeAgg/` (8 family modules) +
  `crates/backend-executor-nodeAgg-pq-seams/` (owned seam crate)

## Context

This audit accompanies the `execGrouping-seams` API-fork reconciliation. The
ported nodeAgg crate originally consumed a `TupleHashTableHandle(pub usize)`
opaque-handle variant of the execGrouping seams. That fork was retired in favor
of the single CANONICAL execGrouping seam API operating on the **real**
`TupleHashTable` / `TupleHashEntryData` structs (execGrouping.c exposes the full
`TupleHashTableData`/`TupleHashEntryData` definitions in execnodes.h, so
opacity-inherited mandates real structs, not handles). nodeAgg's hash-grouping
call sites were rewritten onto the canonical `&mut TupleHashTable` API; this
audit verifies both the reconciled seam usage and the full per-function logic.

This is an INDEPENDENT re-audit: every function was re-derived from the C and
headers, not from the port's comments or its prior self-review. The previous
in-tree audit (2026-06-12, on the scaffold branch) recorded a PASS that was NOT
reproducible — it transcribed `BLCKSZ` as `0x8000` and fabricated a seam-wiring
check. The findings below were caught here, fixed, and re-derived clean.

## Function inventory and verdicts

nodeAgg.c defines 58 functions (lines 371–455 are forward-declaration
prototypes, not definitions). All 58 are accounted for across the 8 family
modules (`node_lifecycle`, `transition`, `finalize`, `sorted_grouping`,
`hash_grouping`, `spill`, `aggapi`, `exec_init_agg`) and cross-checked against
the c2rust rendering.

The port follows **mirror-PG-and-panic**: each function's own nodeAgg control
flow (branches, loop bounds, field assignments, arithmetic, error predicates) is
present, and a `panic!()` stands in at the first unported *callee/dependency* in
C source order. Per the repo's `mirror-PG-and-panic` rule, these are acceptable
(MATCH-with-panic / SEAMED) — absent nodeAgg logic would be a FAIL, an
unavailable cross-crate callee guarded by a faithful panic is not.

Acceptable panic boundaries (own logic intact up to the panic), by dependency:

- **fmgr `FunctionCallInvoke`/`InitFunctionCallInfoData`/`ExecAggCopyTransValue`:**
  `advance_transition_function`, `finalize_aggregate`,
  `finalize_partialaggregate`, `process_ordered_aggregate_single/multi`.
- **execGrouping entry "additional" layout + execTuples slot machinery (the
  explicitly-unported surface):** `initialize_hash_entry`, `lookup_hash_entries`,
  `build_hash_table`, `prepare_hash_slot`, `agg_refill_hash_table`,
  `agg_retrieve_hash_table_in_memory`, `hashagg_spill_tuple`,
  `prepare_projection_slot` (all_grouped_cols branch).
- **execExpr (`ExecBuildAggTrans`/`ExecEvalExpr`):** `advance_aggregates`,
  `hashagg_recompile_expressions`, `project_aggregates`.
- **Shared `Node`-vocabulary blockers (`Var`/`Aggref`/`T_Agg` +
  `expression_tree_walker`, absent from the shared enum; inventing the opacity
  is forbidden by types.md 6-7):** `find_cols`, `find_cols_walker`,
  `find_hash_columns`.
- **fmgr call-frame back-reference (`FunctionCallInfoBaseData` is a trimmed stub
  without the `context` field):** `AggCheckCallContext`, `AggGetAggref`,
  `AggGetTempMemoryContext`, `AggStateIsShared`, `AggRegisterCallback`.
- **execUtils EcxtId-pooled ExprContext:** `ExecInitAgg` (own grouping-set
  counting / numPhases/numHashes / field init present up to the boundary).
- **transam-parallel `shm_toc`:** `ExecAggInitializeDSM/Worker`,
  `ExecAggRetrieveInstrumentation` (own offset/nworkers arithmetic present).

Full MATCH (no panic, or only infallible seams): `select_current_set`,
`initialize_phase`, `fetch_input_tuple`, `initialize_aggregate(s)`, `ExecAgg`,
`agg_retrieve_direct`, `build_hash_tables`, `hash_agg_check_limits`,
`hash_agg_enter_spill_mode`, `hash_choose_num_buckets`,
`hash_choose_num_partitions`, `agg_fill_hash_table`, `agg_retrieve_hash_table`,
`hashagg_batch_new`, `hashagg_batch_read`, `hashagg_finish_initial_spills`,
`hashagg_spill_init`, `hashagg_spill_finish`, `hashagg_reset_spill_state`,
`finalize_aggregates`, `ExecEndAgg`, `ExecReScanAgg`, `ExecAggEstimate`,
`build_pertrans_for_aggref`.

No function drops its own nodeAgg logic where a legitimate unported-callee stop
had not been reached.

### Spot-checks (re-derived in full)

1. `advance_transition_function` (C 708 / transition.rs:219) — MATCH. Inclusive
   `1..=numTransInputs` null-arg loop, `noTransValue` first-input datumCopy
   branch, `transValueIsNull` early return; panic at fmgr `FunctionCallInvoke`.
2. `finalize_aggregate` (C 1045 / finalize.rs:68) — MATCH. Direct-args loop,
   `OidIsValid(finalfn_oid)`, remaining-args null-fill `while i < num_final_args`
   (exclusive bound), strict-and-anynull short-circuit, no-finalfn else branch.
3. Hash-grouping (`lookup_hash_entries` C 2181 / hash_grouping.rs; the canonical
   `lookup_tuple_hash_entry(&mut table, slot, estate, cb) -> (isnew, hash)` call)
   — MATCH-with-panic at the `TupleHashEntryGetAdditional` layout boundary.
4. Spill partitioning (`hashagg_spill_init` C 2984 / spill.rs:382) — full MATCH:
   `shift = 32 - used_bits - partition_bits`, `mask = (npartitions-1) << shift`
   when `shift < 32` else 0; HLL init per partition.

## Seam audit

**Owned seam crate:** `backend-executor-nodeAgg-pq-seams` (4 declarations:
`exec_agg_estimate`, `exec_agg_initialize_dsm`, `exec_agg_initialize_worker`,
`exec_agg_retrieve_instrumentation`). All four are installed by
`aggapi::init_seams()` (only `set()` calls), reached from
`backend_executor_nodeAgg::init_seams()`, which `seams-init::init_all()` now
calls (see S-1). No `set()` outside the owner. Shims are thin marshal+delegate.

**Outward execGrouping seam calls** — verified against the reconciled canonical
API (`backend-executor-execGrouping-seams/src/lib.rs`). The firing call sites —
`reset_tuple_hash_table(&mut **ht)`, `lookup_tuple_hash_entry(&mut **ht, slot,
estate, &mut closure)`, `init_tuple_hash_iterator(&mut **ht)` — all match the
canonical `&mut TupleHashTable` signatures. `build_tuple_hash_table` and
`scan_tuple_hash_table` are not reached (their callers panic earlier at the
execTuples slot / entry-layout boundary), correctly seam-and-panicked.

## Findings (all fixed, re-derived clean)

- **C-1 (was merge-blocking):** `HASHAGG_READ_BUFFER_SIZE` /
  `HASHAGG_WRITE_BUFFER_SIZE` were `0x8000` (32768); C (nodeAgg.c:307-308) is
  `BLCKSZ` = 8192 (`0x2000`) — a 4× error feeding spill memory budgeting
  (`hash_agg_set_limits` C:1840, `hash_agg_update_metrics` C:1969,
  `hash_choose_num_partitions` C:2098) and `LogicalTapeRewindForRead` (C:3221).
  **Fixed:** both now `= types_core::BLCKSZ`. The four consumers (spill.rs:71,
  224-226, 494) re-derived against C and confirmed correct.
- **C-2 (was should-fix):** `CHUNKHDRSZ` was `16`; `sizeof(MemoryChunk)` in a
  standard non-`MEMORY_CONTEXT_CHECKING` 64-bit build is a single
  `uint64 hdrmask` = 8 bytes (memutils_memorychunk.h header comment: it
  "becomes 16 bytes" only under CHECKING). Feeds `hash_agg_entry_size`.
  **Fixed:** `CHUNKHDRSZ = 8`.
- **S-1 (was merge-blocking):** `seams-init::init_all()` did not call
  `backend_executor_nodeAgg::init_seams()` (the crate was not even a dependency
  of `seams-init`), so the four `nodeAgg-pq-seams` were never installed.
  **Fixed:** added the dependency and the `init_seams()` line.

## Design conformance

No invented opacity (the crossing hash-table type is the real `TupleHashTable`;
no `usize` stand-ins remain). Allocating seams carry `Mcx` + `PgResult`. No
shared statics for per-backend globals, no ambient-global seams, no locks held
across `?`, no registry side-tables, no unledgered divergence markers. The
`grouped_cols` alias is an owned `clone_in(es_query_cxt)` deep copy
(semantically equivalent, consistent with the owned model). Clean.

## Gate

`cargo check --workspace` and `cargo test --workspace` both green after the
fixes (no errors; only pre-existing warnings in unrelated crates).
