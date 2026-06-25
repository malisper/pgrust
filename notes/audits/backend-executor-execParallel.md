# Audit: backend-executor-execParallel

C source: `src/backend/executor/execParallel.c` (PostgreSQL 18.3, 1531 lines).
c2rust: `c2rust-runs/backend-executor-execParallel/src/execParallel.rs`.
Port: `crates/backend-executor-execParallel/src/lib.rs`,
`crates/types-execparallel/src/lib.rs`,
`crates/backend-executor-execParallel-seams/src/lib.rs`,
`crates/backend-executor-execParallel-support-seams/src/lib.rs`.

## Function inventory (20 definitions — every C function gets a row)

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecSerializePlan` (146) | lib.rs:152 | MATCH | copyObject + resjunk-clear + dummy PlannedStmt build + parallel-safe subplan NULL-hole filter + nodeToString. PlannedStmt is a neighbor-owned node; field-fill + subplan filter delegated to `sup::build_serializable_plannedstmt` (see seam note). |
| 2 | `ExecParallelEstimate` (233) | lib.rs:174 | MATCH | NodeTag dispatch over 14 tags; parallel_aware gate on the same 6 tags (Seq/Foreign/Append/Custom/BitmapHeap/HashJoin); recurse via planstate_children short-circuit. Matches C tree-walker. |
| 3 | `EstimateParamExecSpace` (319) | lib.rs:240 | MATCH | `sz = sizeof(int)` init, bms_next_member loop, add_size(paramid) + add_size(datumEstimateSpace). typByVal/typLen resolution folded into `param_exec_value` (incl. no-OID by-value fallback). |
| 4 | `SerializeParamExecParams` (363) | lib.rs:266 | MATCH | dsa_allocate(EstimateParamExecSpace), write nparams (i32), per-param write paramid + datumSerialize; returns handle. |
| 5 | `RestoreParamExecParams` (418) | lib.rs:303 | MATCH | read nparams, loop reading paramid + datumRestore, write back value/isnull, execPlan=NULL (via set_param_exec_value). |
| 6 | `ExecParallelInitializeDSM` (447) | lib.rs:323 | MATCH | instrumentation slot fill, nnodes++, NodeTag dispatch (same 14 tags, same parallel_aware gates as Estimate), recurse. |
| 7 | `ExecParallelSetupTupleQueues` (547) | lib.rs:395 | MATCH | nworkers==0 → empty; allocate or lookup tqueuespace; per-worker shm_mq_create at offset i*SIZE, set receiver=MyProc, attach; insert key when !reinitialize. |
| 8 | `ExecInitParallelPlan` (599) | lib.rs:447 | MATCH (fixed) | Full DSM sizing/population sequence verified field-by-field; instrumentation_len/instrument_offset/MAXALIGN math matches C:709-716; nnodes consistency error. **Constant fix applied — see Findings.** |
| 9 | `ExecParallelCreateReaders` (890) | lib.rs:693 | MATCH | nworkers_launched; if >0 alloc reader array, set_handle(bgwhandle), CreateTupleQueueReader. |
| 10 | `ExecParallelReinitialize` (916) | lib.rs:722 | MATCH | ExecSetParamPlanMulti, ReinitializeParallelDSM, SetupTupleQueues(true), free old param_exec, re-serialize, ReInitializeDSM walk. |
| 11 | `ExecParallelReInitializeDSM` (965) | lib.rs:780 | MATCH | NodeTag dispatch; reinit only on 8 parallel_aware nodes; BitmapIndex/Hash/Sort/IncrSort/Memoize explicitly no-op; recurse. |
| 12 | `ExecParallelRetrieveInstrumentation` (1035) | lib.rs:840 | MATCH | linear search for plan_node_id (elog ERROR "plan node %d not found"), InstrAggNode each worker slot into node, store worker_instrument in per-query cxt, node-type retrieve dispatch, recurse. |
| 13 | `ExecParallelRetrieveJitInstrumentation` (1116) | lib.rs:908 | MATCH | alloc-if-needed + InstrJitAgg accumulate (accum_es_jit_worker_instr), build per-worker detail, set_worker_jit_instrument. |
| 14 | `ExecParallelFinish` (1156) | lib.rs:936 | MATCH | finished no-op guard; detach tqueues + free; destroy readers + free; WaitForParallelWorkersToFinish; InstrAccumParallelQuery per worker; finished=true. |
| 15 | `ExecParallelCleanup` (1209) | lib.rs:983 | MATCH | retrieve instrumentation + jit; dsa_free param_exec; dsa_detach; DestroyParallelContext; pei dropped (pfree). |
| 16 | `ExecParallelGetReceiver` (1245) | lib.rs:1021 | MATCH | lookup TUPLE_QUEUE, offset by ParallelWorkerNumber*SIZE, set sender=MyProc, CreateTupleQueueDestReceiver(attach). |
| 17 | `ExecParallelGetQueryDesc` (1261) | lib.rs:1043 | MATCH | lookup QUERY_TEXT/PLANNEDSTMT/PARAMLISTINFO, stringToNode, RestoreParamList, CreateQueryDesc(GetActiveSnapshot, InvalidSnapshot). |
| 18 | `ExecParallelReportInstrumentation` (1293) | lib.rs:1081 | MATCH | InstrEndLoop, linear search (elog ERROR), Assert(IsParallelWorker)+Assert(wnum<num_workers), InstrAggNode slot read-modify-write, recurse. |
| 19 | `ExecParallelInitializeWorker` (1334) | lib.rs:1131 | MATCH | NodeTag dispatch (same 14 tags + gates as InitializeDSM), recurse. |
| 20 | `ParallelQueryMain` (1429) | lib.rs:1193 | MATCH | full worker driver: fixed state, receiver, instrumentation/jit lookup, QueryDesc, debug_query_string, pgstat, dsa_attach, ExecutorStart(jitFlags/eflags), es_query_dsa, RestoreParamExecParams, InitializeWorker, SetTupleBound, InstrStart, ExecutorRun(count clamp), Finish, buffer/WAL usage, ReportInstrumentation, JIT instr, ExecutorEnd, dsa_detach, FreeQueryDesc, rDestroy. |

Macros (not functions): `GetInstrumentationArray`, `DsaPointerIsValid`, `OidIsValid`,
`GetPerTupleExprContext` — handled inline / via seams. No omitted definitions.

## Constants verified against headers

- 10 `PARALLEL_KEY_*` magic numbers (0xE0…01 .. 0A) — match execParallel.c:58-67.
- `PARALLEL_TUPLE_QUEUE_SIZE` = 65536 — match :69.
- `PGJIT_NONE` = 0 — verified jit/jit.h:19.
- `ForwardScanDirection` = 1 — verified access/sdir.h:28.
- `offsetof(SharedExecutorInstrumentation, plan_node_id)` = 16 (4×i32) — verified.
- `offsetof(SharedJitInstrumentation, jit_instr)` = MAXALIGN(4) = 8 — verified.
- `LWTRANCHE_PARALLEL_QUERY_DSA` = 71 — verified via header enum walk
  (NUM_INDIVIDUAL_LWLOCKS=54 + offset) and c2rust (`= 71`).

## Findings and fixes

### Finding 1 (DIVERGES → fixed): wrong LWTRANCHE_PARALLEL_QUERY_DSA value

The port hardcoded `const LWTRANCHE_PARALLEL_QUERY_DSA: i32 = 56;` (lib.rs:98).
The correct build-derived value is **71** (54 individual LWLocks + tranche
offset; confirmed by both the lwlock.h enum ordering and the c2rust rendering
`pub const LWTRANCHE_PARALLEL_QUERY_DSA: BuiltinTrancheIds = 71`). `56` is
actually `LWTRANCHE_SUBTRANS_BUFFER` — a transcribed-constant corruption, exactly
the failure mode the audit guards against. The value is passed to
`dsa_create_in_place`, so the leader would have created its parallel-query DSA
area under the wrong tranche label.

Fix: removed the hardcoded literal and imported the canonical
`types_storage::LWTRANCHE_PARALLEL_QUERY_DSA` (= 71, computed from
`NUM_INDIVIDUAL_LWLOCKS`), adding `types-storage` to the crate's deps. Re-audited
the use site (ExecInitParallelPlan lib.rs:656) from scratch against C:840-842 —
now MATCH. Build clean.

### Seam note (not a finding): build_serializable_plannedstmt

`sup::build_serializable_plannedstmt` carries the dummy-PlannedStmt field-fill and
the parallel-safe-subplan NULL-hole filter from ExecSerializePlan. `PlannedStmt`,
`Plan`, and `TargetEntry` are nodes owned by the (unported) nodes/planner
subsystem; this crate holds only the opaque `PlannedStmtHandle`, so the
node-field writes genuinely cannot live here yet. The dominant content is
node construction over a neighbor-owned type — the sanctioned "function owned by
an unported neighbor" form. The one genuine execParallel decision inside it (the
`subplan && !subplan->parallel_safe → NULL` filter) rides along with that node
build; acceptable as SEAMED for now, but it should collapse back into this crate
when the nodes subsystem lands.

## Seam / wiring audit

- 6 inward entry-point seams declared in `-seams`
  (ExecInitParallelPlan, ExecParallelCreateReaders, ExecParallelReinitialize,
  ExecParallelFinish, ExecParallelCleanup, ParallelQueryMain). All 6 installed
  by `init_seams()` (lib.rs:137-144), which contains only `set()` calls.
- `seams-init::init_all()` calls `backend_executor_execParallel::init_seams()`
  (seams-init/src/lib.rs:18). Verified.
- Outward seams (transam-parallel, shm-mq, mmgr-dsa, tqueue, the 14 per-node
  `Exec*` method seam crates, and the support seam) are thin marshal+delegate.
  Each dispatch arm performs one call; all branching (NodeTag dispatch,
  parallel_aware gates, instrumentation/jit conditionals, count clamping,
  nnodes-consistency check, no-op-on-twice guards) stays in this crate.
- `planstate_children` returns walker-order children; NULL filtering is the
  owner's responsibility (matches `planstate_tree_walker` not invoking the
  walker meaningfully on NULL). Top-level NULL guards in the C walkers cover the
  recursive entry, not the externally-invoked entry. Consistent.

## Verdict

**PASS** — after fixing Finding 1. All 20 functions MATCH (ExecSerializePlan's
node-build portion legitimately SEAMED to the unported nodes owner). Constants
verified against headers and c2rust. Seams thin and fully wired.

---

## Re-audit — family `shm-toc-address` (real-address contract change)

The inward `SerializeCursor` / `Fixed`/`Instrumentation`/`JitInstrumentation`
handle contract this crate consumes (via `backend-access-transam-parallel-seams`)
changed meaning from a `(slot<<32)|offset` side-table token to a **real
in-segment chunk address**; the typed `repr(C)` payloads are now written in
place at that address by the parallel owner.

Reviewed every consumer call site (`store_fixed_state`, `fixed_state_from_chunk`,
`set_fixed_param_exec`/`fixed_param_exec`/`fixed_eflags`/`fixed_jit_flags`/
`fixed_tuples_needed`, `store_cstring`/`cursor_cstring`,
`store_instrumentation_header`/`instrumentation_from_chunk`/`sei_*`/
`set_sei_plan_node_id`, `store_jit_instrumentation_header`/
`jit_instrumentation_from_chunk`/`shared_jit_num_workers`, and the
`SerializeCursor`-typed `buffer_usage`/`wal_usage`/`tqueuespace`/param paths).
All thread the handle/cursor opaquely from one seam to the next exactly as the C
threads the pointer, so they are correct under the address reinterpretation
without logic change. The one source edit is dropping the now-removed
`plan_node_id: Vec::new()` field from the `SharedExecutorInstrumentation` header
literal in `ExecInitParallelPlan` (the header is now the four-`int` `repr(C)`
struct; `plan_node_id` is the flexible array the owner writes via
`set_sei_plan_node_id`, matching C). The `instrument_offset` /
`OFFSET_OF_PLAN_NODE_ID` / `OFFSET_OF_JIT_INSTR` arithmetic is unchanged and now
addresses real in-segment offsets.

**PASS** — no logic regressions; `cargo check`/`cargo test --workspace` clean.

---

## Re-audit — family `tqueue-substrate-check` (tuple queues over real DSM)

Integration check that the executor's *tuple queues* resolve to the real DSM
substrate end-to-end, with NO emulation left.

- `ExecParallelSetupTupleQueues` (lib.rs:396) — the leader allocates the
  `mul_size(PARALLEL_TUPLE_QUEUE_SIZE, nworkers)` region with
  `shm_toc_allocate` (a **real in-segment chunk address**, family
  `shm-toc-address`), then per worker:
  `shm_mq_create_at(tqueuespace, i, SIZE)` carves the real `chunk + i*SIZE`
  base and builds the real `shm_mq` ring in place; `shm_mq_set_receiver_to_myproc`;
  `shm_mq_attach(mq, seg)` where `seg = pcxt_seg(pcxt)` is the leader's **real
  `DsmSegmentHandle`** (`Some(DsmSegmentId)`). The seam impl
  (`backend-storage-ipc-shm-mq::shm_mq_attach`) allocates the backend-private
  handle in `top_memory_context()` (`Mcx<'static>`), resolves the real
  `DsmSegmentId` (`seg_id_of`), and threads the ambient `MyLatch`
  (`backend_storage_ipc_latch_seams::my_latch()`) into `real_attach`.
- `ExecParallelGetReceiver` (lib.rs:1022, worker side) — `shm_toc_lookup`
  resolves the same real tuple-queue region; `shm_mq_at(mqspace,
  ParallelWorkerNumber, SIZE)` *casts* the leader-created queue (no re-create,
  which would wipe the leader's `mq_set_receiver`); `shm_mq_set_sender_to_myproc`;
  `shm_mq_attach(mq, Some(seg))` over the worker's real `seg`.

There is no `Vec<u8>` DSM buffer, no `(slot<<32)|offset` side-table token, no
`dsm_create_null_if_maxsegments`, and no worker-side runtime emulation seam in
this path — the tuple queues are real `shm_mq` rings over real in-segment chunk
addresses, exactly as the error queues (parallel.c audit §"shm-mq" families).

**Runtime test (real DSM).** `tuple_queue_roundtrip_over_real_dsm` (in
`backend-access-transam-parallel`, feature `test-bringup`) drives the *exact*
`ExecParallelSetupTupleQueues` loop body (multi-worker
`shm_toc_allocate`/`shm_mq_create_at`/`set_receiver`/`shm_mq_attach(Some(seg))`)
and the `ExecParallelGetReceiver` cast (`shm_mq_at`/`set_sender`/
`shm_mq_attach(Some(seg))`) over a real `dsm-core` segment, then round-trips a
tuple frame worker→leader through the real ring, proving real bytes flow over the
real DSM tuple chunk with the real `DsmSegmentId`/`Mcx<'static>`/`MyLatch`
threaded through.

**PASS** — tuple queues confirmed real; no new DESIGN_DEBT;
`cargo check`/`cargo test --workspace` clean.
