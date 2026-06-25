# Audit: backend-executor-nodeBitmapIndexscan

Independent function-by-function audit of the port of
`src/backend/executor/nodeBitmapIndexscan.c` against the original C and the
c2rust rendering (`../pgrust/c2rust-runs/backend-executor-nodeBitmapIndexscan/`).

- C source: `../pgrust/postgres-18.3/src/backend/executor/nodeBitmapIndexscan.c`
- Port: `crates/backend-executor-nodeBitmapIndexscan/src/lib.rs`
- Owned seam crate: `crates/backend-executor-nodeBitmapIndexscan-seams/src/lib.rs`

## Function inventory (every C definition)

| # | C function | C lines | Port location | Verdict | Notes |
|---|-----------|---------|---------------|---------|-------|
| 1 | `ExecBitmapIndexScan` (static pro-forma stub) | 37-42 | `exec_proc_node_trampoline` (lib.rs:642) + installed at lib.rs:310 | MATCH | C does `elog(ERROR, "BitmapIndexScan node does not support ExecProcNode call convention")`. Port returns `Err(elog(<same message>))`. Same severity (ERROR) and message. |
| 2 | `MultiExecBitmapIndexScan` | 48-121 | `MultiExecBitmapIndexScan` (lib.rs:76) | MATCH | See detail below. |
| 3 | `ExecReScanBitmapIndexScan` | 130-168 | `ExecReScanBitmapIndexScan` (lib.rs:180) | MATCH | See detail below. |
| 4 | `ExecEndBitmapIndexScan` | 174-214 | `ExecEndBitmapIndexScan` (lib.rs:239) | MATCH | See detail below. |
| 5 | `ExecInitBitmapIndexScan` | 222-343 | `ExecInitBitmapIndexScan` (lib.rs:284) | MATCH | See detail below. |
| 6 | `ExecBitmapIndexScanEstimate` | 352-368 | `ExecBitmapIndexScanEstimate` (lib.rs:443) | MATCH | Early-return predicate `!instrument || nworkers==0` preserved; size = `offsetof(..,winstrument) + nworkers*sizeof(IndexScanInstrumentation)` via `shared_info_size`; estimate_chunk + estimate_keys(1) delegated to shm_toc seam. |
| 7 | `ExecBitmapIndexScanInitializeDSM` | 376-397 | `ExecBitmapIndexScanInitializeDSM` (lib.rs:470) | MATCH | Same early-return predicate; allocates a fully-zeroed `nworkers`-slot SharedIndexScanInstrumentation with `num_workers` set; alloc+insert under `plan_node_id` delegated to shm_toc seam (mirrors shm_toc_allocate + shm_toc_insert + memset(0) + num_workers). |
| 8 | `ExecBitmapIndexScanInitializeWorker` | 405-415 | `ExecBitmapIndexScanInitializeWorker` (lib.rs:508) | MATCH | Early-return `!instrument`; `shm_toc_lookup(plan_node_id, noError=false)` delegated to shm_toc seam. |
| 9 | `ExecBitmapIndexScanRetrieveInstrumentation` | 423-437 | `ExecBitmapIndexScanRetrieveInstrumentation` (lib.rs:539) | MATCH | NULL-SharedInfo early return; size uses `SharedInfo->num_workers`; palloc+memcpy modeled as deep-clone of exactly `num_workers` slots into a fresh mcx allocation (`clone_shared_info`). |

C helpers rendered by c2rust (`newNode`, `list_nth`, `list_nth_cell`,
`exec_rt_fetch`) are inlined-from-headers, not nodeBitmapIndexscan.c definitions;
their behavior is reproduced via `BitmapIndexScanState::make_boxed_in`
(makeNode) and the `exec_rt_fetch_rellockmode` execUtils seam.

## Detailed re-derivation of the cores

### MultiExecBitmapIndexScan (C 48-121)
- `nTuples = 0` -> `n_tuples = 0.0`. MATCH.
- `if (instrument) InstrStartNode(...)` -> `if let Some(instr) ... instr_start_node`. MATCH.
- runtime/array-key not-ready guard `!RuntimeKeysReady && (NumRuntimeKeys!=0 || NumArrayKeys!=0)`
  -> identical predicate; `ExecReScan((PlanState*)node)` dispatches to
  `ExecReScanBitmapIndexScan` (the node's only rescan), port calls it directly,
  then `doscan = biss_RuntimeKeysReady`; else `doscan = true`. MATCH (the
  empty-array-key => stays-false => doscan=false case is preserved).
- result-bitmap handoff: `if (biss_result) { tbm = biss_result; biss_result = NULL; }`
  -> `node.biss_result.take()`; else `tbm_create(work_mem * 1024,
  isshared ? es_query_dsa : NULL)` -> `globals::work_mem * 1024`, `plan_isshared`,
  dsa selection identical, tbm allocated in `es_query_cxt`. MATCH (the `(Size)1024`
  unsigned multiply mirrored by `wrapping_mul(1024)`).
- scan loop `while (doscan)`: `nTuples += index_getbitmap(scandesc, tbm)`,
  `CHECK_FOR_INTERRUPTS()`, `doscan = ExecIndexAdvanceArrayKeys(...)`,
  `if (doscan) index_rescan(biss_ScanDesc, biss_ScanKeys, biss_NumScanKeys, NULL, 0)`
  -> all four steps in order via indexam/tcop/nodeIndexscan seams; index_rescan
  carried by `index_rescan_bis` (reads ScanKeys/NumScanKeys/NULL/0 from node). MATCH.
- `if (instrument) InstrStopNode(instrument, nTuples)` -> MATCH.
- `return (Node *) tbm` -> returns the real `PgBox<TIDBitmap>`. MATCH.

### ExecReScanBitmapIndexScan (C 130-168)
- `if (econtext) ResetExprContext(econtext)` -> `reset_expr_context` via execUtils. MATCH.
- `if (NumRuntimeKeys!=0) ExecIndexEvalRuntimeKeys(...)`. MATCH.
- `if (NumArrayKeys!=0) RuntimeKeysReady = ExecIndexEvalArrayKeys(...) else RuntimeKeysReady = true`. MATCH.
- `if (RuntimeKeysReady) index_rescan(...)`. MATCH.
- (The `econtext` non-null `ok_or_else` guards model the C invariant that
  NumRuntimeKeys/NumArrayKeys != 0 implies a runtime context was assigned; they
  never change behaviour on valid state.)

### ExecEndBitmapIndexScan (C 174-214)
- parallel-worker stat copy-back: `if (biss_SharedInfo != NULL && IsParallelWorker())`
  -> `biss_SharedInfo.is_some() && is_parallel_worker()`. MATCH.
- body: `winstrument = &SharedInfo->winstrument[ParallelWorkerNumber];
  winstrument->nsearches += node->biss_Instrument.nsearches` -> delegated to
  `parallel::accumulate_shared_index_searches(shared, nsearches)`. The
  `ParallelWorkerNumber` index and `Assert(ParallelWorkerNumber <= num_workers)`
  are owned by the parallel crate (it owns `ParallelWorkerNumber`); thin
  marshal+delegate, no logic in this crate. MATCH.
- `if (indexScanDesc) index_endscan(indexScanDesc)` -> `biss_ScanDesc.take()` +
  `index_endscan`. MATCH.
- `if (indexRelationDesc) index_close(indexRelationDesc, NoLock)` -> `index_rel.close(NoLock)`,
  NoLock == 0. MATCH.

### ExecInitBitmapIndexScan (C 222-343)
- `Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)))`: constants verified
  against executor.h — BACKWARD=0x0008, MARK=0x0010 (lib.rs:53-55). `debug_assert!`. MATCH.
- makeNode + `ss.ps.plan`/`ss.ps.state`/`ExecProcNode = ExecBitmapIndexScan`. MATCH
  (state back-link is executor-owned; ExecProcNode is the pro-forma trampoline).
- `biss_result = NULL`, `ss_currentRelation = NULL`, `ss_currentScanDesc = NULL`. MATCH.
- EXPLAIN_ONLY early return: `EXEC_FLAG_EXPLAIN_ONLY == 0x0001` verified. MATCH.
- `lockmode = exec_rt_fetch(scan.scanrelid, estate)->rellockmode;
  biss_RelationDesc = index_open(node->indexid, lockmode)`. MATCH (rellockmode
  fetch via execUtils seam; index_open via indexam seam).
- `biss_RuntimeKeysReady=false; biss_RuntimeKeys=NULL; biss_NumRuntimeKeys=0`. MATCH.
- `ExecIndexBuildScanKeys(... isorderby=false ...)` -> `exec_index_build_scan_keys_bis`,
  isorderby false. MATCH.
- runtime-key exprcontext swap (`stdecontext` save, `ExecAssignExprContext`,
  set `biss_RuntimeContext`, restore `ps_ExprContext`) -> reproduced verbatim;
  else branch sets `biss_RuntimeContext=NULL`. MATCH.
- `biss_ScanDesc = index_beginscan_bitmap(RelationDesc, es_snapshot, &biss_Instrument, NumScanKeys)`. MATCH.
- `if (NumRuntimeKeys==0 && NumArrayKeys==0) index_rescan(...)`. MATCH.
- `return indexstate`. MATCH.

## Seam audit

Owned seam crate (maps to nodeBitmapIndexscan.c):
`backend-executor-nodeBitmapIndexscan-seams` declares four parallel-executor
entry seams over execParallel's opaque handles:
`exec_bitmapindexscan_estimate`, `_initialize_dsm`, `_initialize_worker`,
`_retrieve_instrumentation`. **All four are installed** by this crate's
`init_seams()` (lib.rs:668-679). The bridges panic at the
`PlanStateHandle -> BitmapIndexScanState` resolution frontier, which execParallel
owns and has not yet wired — mirror-PG-and-panic on an unported callee, not
absent own-logic. The real node-level logic lives in the public
`ExecBitmapIndexScan*` functions.

Outward seam calls are all real cross-crate dependencies and thin
marshal+delegate (no branching/computation in seam paths):
- indexam: `index_open`, `index_close`, `index_beginscan_bitmap`,
  `index_getbitmap`, `index_endscan`, `index_rescan_bis`.
- tidbitmap: `tbm_create`.
- nodeIndexscan: `exec_index_build_scan_keys_bis`,
  `exec_index_eval_runtime_keys_bis`, `exec_index_eval_array_keys_bis`,
  `exec_index_advance_array_keys_bis`.
- execUtils: `reset_expr_context`, `exec_rt_fetch_rellockmode`,
  `exec_assign_expr_context`.
- instrument: `instr_start_node`, `instr_stop_node`.
- parallel: `is_parallel_worker`, `accumulate_shared_index_searches`.
- shm_toc: `pcxt_nworkers`, `estimate_chunk_and_key`,
  `toc_allocate_and_insert_bitmap_instr`, `toc_lookup_bitmap_instr`.
- globals/tcop: `work_mem`, `check_for_interrupts`.

Wiring: `seams-init::init_all()` calls
`backend_executor_nodeBitmapIndexscan::init_seams()` (seams-init/src/lib.rs:71).

## Design conformance
- No invented opacity: real `BitmapIndexScanState`/`TIDBitmap`/
  `SharedIndexScanInstrumentation` types are used; handle opacity at the
  execParallel frontier is inherited from execParallel.
- Allocating paths carry `Mcx` and return `PgResult` (`tbm_create`,
  `new_zeroed_winstrument`/`clone_shared_info` use `try_reserve_exact` + `mcx.oom`).
- No own-logic stubs, no `todo!()`/`unimplemented!()`, no deferred/SEAMED-equivalent
  escape of in-crate logic.

## Gates
- `cargo check --workspace`: pass (only pre-existing unrelated warnings in
  backend-access-common-printtup).
- `cargo test -p backend-executor-nodeBitmapIndexscan`: pass (0 tests).
- `cargo test -p seams-init`: pass; `recurrence_guard::every_seam_installing_crate_is_wired_into_init_all`
  and `every_declared_seam_is_installed_by_its_owner` both green.

## Verdict: PASS

Every function MATCH (or SEAMED per step-3 rules with all owned seams installed).
Zero seam findings, zero design findings.

## Re-audit 2026-06-13 — tidbitmap `tbm_create` contract rewire (post-main merge)

Context: nodeBitmapAnd/nodeBitmapOr landed on main first and superseded the
`tbm_create` seam contract. This branch originally declared a duplicate
`tbm_create(maxbytes, dsa) -> PgResult<TIDBitmap>` seam (E0428 vs main's
`tbm_create<'mcx>(mcx, maxbytes, dsa) -> PgResult<PgBox<'mcx, TIDBitmap>>`).
Merging main dropped the branch's duplicate decl and the branch's stale
`provide_tbm_create`; the owner (`backend-nodes-core::tidbitmap`) keeps the
landed mcx-first PgBox-returning provider.

Rewired call site (`MultiExecBitmapIndexScan`, src/lib.rs:132):

- C (nodeBitmapIndexscan.c:91): `tbm = tbm_create(work_mem * (Size) 1024,
  ((BitmapIndexScan *) node->ss.ps.plan)->isshared ?
  node->ss.ps.state->es_query_dsa : NULL);` — `tbm_create` palloc's the bitmap
  in `CurrentMemoryContext`, which during `MultiExec` is the per-query context.
- Port: `tidbitmap::tbm_create::call(estate.es_query_cxt, maxbytes, dsa)?` —
  passes the per-query context (`es_query_cxt: Mcx`) as the seam's `mcx`
  (faithful to the C `CurrentMemoryContext` palloc target), the same
  `work_mem*1024` maxbytes, and the same `isshared ? es_query_dsa : None` dsa.
  The seam now boxes into `mcx` and returns the `PgBox<'mcx, TIDBitmap>`
  directly, so the prior in-crate `mcx::alloc_in(es_query_cxt, bitmap)` wrap is
  gone (the box now happens owner-side, identical placement). Verdict: **MATCH**.
- Matches the sibling landed consumers (nodeBitmapOr src/lib.rs:211,
  nodeBitmapAnd) that already call the mcx-first contract.

No other tidbitmap seam is consumed by this crate (only `tbm_create`). No E0428
duplicate seams remain (single `tbm_create` decl in tidbitmap-seams; the
`backend-nodes-core::tidbitmap` `pub fn tbm_create` is the in-crate constructor,
not a seam decl). No re-introduced divergence; no owner-side seam change needed.

Re-gate: `cargo check --workspace` clean; `cargo test -p
backend-executor-nodeBitmapIndexscan -p backend-nodes-core -p
backend-nodes-core-tidbitmap-seams -p seams-init` all pass incl. both recurrence
guards. Verdict: **PASS**.
