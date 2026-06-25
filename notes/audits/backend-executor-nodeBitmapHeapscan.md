# Audit: backend-executor-nodeBitmapHeapscan

- **Unit:** `backend-executor-nodeBitmapHeapscan`
- **Branch:** `port/backend-executor-nodeBitmapHeapscan`
- **C source:** `src/backend/executor/nodeBitmapHeapscan.c` (PostgreSQL 18.3)
- **c2rust:** `c2rust-runs/backend-executor-nodeBitmapHeapscan/src/nodeBitmapHeapscan.rs`
- **Rust port:** `crates/backend-executor-nodeBitmapHeapscan/src/{lib.rs,nodes.rs}`
- **Date:** 2026-06-13 (tail re-audit; original 2026-06-12)
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

This is the independent `/audit-crate` merge gate, re-derived from the sources
(not the port author's self-record).

**Tail re-audit (2026-06-13, Opus 4.8 1M):** the two prior PARTIALs —
`ExecBitmapHeapInitializeDSM` (#11) and `ExecBitmapHeapInitializeWorker` (#13) —
were re-examined against the C and the now-landed real-DSM
`backend-access-transam-parallel` re-port. Both previously carried a bare
mid-body `panic!` that swallowed the in-DSM placement *and the in-place
init/insert own-logic*. The blocked-on-unported-callee rationale ("the DSM/dsa
owner is not yet ported") was **stale**: `shm_toc_allocate`/`shm_toc_insert`/
`shm_toc_lookup`/`pcxt_toc`/`pwcxt_toc` are now wired real seams returning a
`SerializeCursor` over the real in-segment chunk address. The two functions are
now completed to match the C: sizing + `shm_toc_allocate`, the in-place
`SpinLockInit`/`state=BM_INITIAL`/`ConditionVariableInit`/instrumentation-zero
init, `shm_toc_insert`, `shm_toc_lookup` (worker), and the
`node->pstate`/`node->sinstrument` placement are all present. The lone remaining
panic is isolated to dedicated callee helpers (`pstate_over_chunk`/
`sinstrument_over_chunk`) for the genuinely-unported "typed-shared-object over a
shm_toc DSM byte cursor" resolution — the *exact* resolution the canonical
parallel-scan sibling `nodeSeqscan` also defers via its merged
`pscan_over_chunk`. This is panic-on-unported-callee, not absent logic. All 5
unit tests pass; crate builds clean.

## Top-line verdict: **PASS**

The two prior PARTIALs are now completed to full own-logic parity with the C; the
only deferred step is the DSM typed-shared-object-over-cursor resolution
(execParallel), reached through a dedicated callee helper that panics — the same
posture as the merged `nodeSeqscan`. All other functions `MATCH`, and every owned
seam declaration is installed.

## 1. Function inventory + verdicts

The unit links in `execScan.c`/`execScan.h` inline helpers (`ExecScan`,
`ExecScanFetch`, `InstrCountFiltered1/2`) and `ExecQualAndReset`, so those get
rows too. All 17 C definitions are present in c2rust and in the port.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `BitmapTableScanSetup` (63) | lib.rs:78 | MATCH | pstate-NULL / leader / follower branches, `MultiExecProcNode` + `IsA(TIDBitmap)` check, `tbm_prepare_shared_iterate`, `BitmapDoneInitializingSharedState`, `tbm_begin_iterate` with `pstate?tbmiterator:InvalidDsaPointer`, first-scan `table_beginscan_bm`, `rs_tbmiterator` store, `initialized=true`. Matches. |
| 2 | `BitmapHeapNext` (126) | lib.rs:159 | MATCH | `table_scan_bitmap_next_tuple` loop; recheck → `ExecQualAndReset` → fail path `InstrCountFiltered2`+`ExecClearTuple`+continue; CHECK_FOR_INTERRUPTS inside loop; end-of-scan `ExecClearTuple`. lossy/exact counters added as per-call deltas returned by the AM seam (C AM `(*ptr)++`); equivalent. |
| 3 | `BitmapDoneInitializingSharedState` (181) | lib.rs:618 | MATCH | spinlock-guarded `state=BM_FINISHED`, then `ConditionVariableBroadcast`. |
| 4 | `BitmapHeapRecheck` (193) | lib.rs:226 | MATCH | sets `ecxt_scantuple`, returns `ExecQualAndReset(bitmapqualorig)`. Dead until EPQ lands (correctly `#[allow(dead_code)]`); logic present. |
| 5 | `ExecBitmapHeapScan` (212) | lib.rs:385 | MATCH | delegates to `ExecScan` driver. |
| 6 | `ExecReScanBitmapHeapScan` (226) | lib.rs:395 | MATCH | scan guard: `tbm_exhausted`→`tbm_end_iterate`, `table_rescan`; `tbm_free`+NULL; `initialized=false`,`recheck=true`; `ExecScanReScan`; `chgParam==NULL`→`ExecReScan(outer)`. |
| 7 | `ExecEndBitmapHeapScan` (267) | lib.rs:468 | MATCH | parallel-worker stats accumulate (`IsParallelWorker`, `ParallelWorkerNumber<=num_workers` assert, `+=` accumulate not memcpy); `ExecEndNode(outer)`; scanDesc guard `tbm_exhausted`/`tbm_end_iterate`+`table_endscan`; `tbm_free`. |
| 8 | `ExecInitBitmapHeapScan` (333) | lib.rs:522 | MATCH | unsupported-flag assert, MVCC assert (debug-noted), makeNode/zero init, `ExecAssignExprContext`, `ExecOpenScanRelation`, `ExecInitNode(outerPlan)`, `ExecInitScanTupleSlot` w/ `table_slot_callbacks`, `ExecInitResultTypeTL`, `ExecAssignScanProjectionInfo`, `ExecInitQual` ×2, `ss_currentRelation`. |
| 9 | `BitmapShouldInitializeSharedState` (420) | lib.rs:637 | MATCH | State-machine loop/spinlock/`BM_INITIAL→BM_INPROGRESS`/break-on-`!=BM_INPROGRESS`/`ConditionVariableSleep`/`ConditionVariableCancelSleep`/`return state==BM_INITIAL` are all correct. The wait-event passed to `ConditionVariableSleep` is now `WAIT_EVENT_PARALLEL_BITMAP_SCAN` = `0x08000000 \| 38 = 134217766` (`PG_WAIT_IPC` class, index 38), matching c2rust. (Was previously the wrong `0x06000000 \| 18`; fixed.) |
| 10 | `ExecBitmapHeapEstimate` (453) | lib.rs:689 | MATCH | `MAXALIGN(sizeof(pstate))`; if `instrument && nworkers>0` add `offsetof(...,sinstrument)`(=8, confirmed against c2rust line 3365) + `nworkers*sizeof(BitmapHeapScanInstrumentation)`; `shm_toc_estimate_chunk`/`_keys(1)`. |
| 11 | `ExecBitmapHeapInitializeDSM` (478) | lib.rs:716 | MATCH (blocked on unported callee) | Full body now present: `dsa==NULL` early return; sizing; `shm_toc_allocate`; in-place `tbmiterator=0`/`SpinLockInit` (`mutex.unlock()` = S_INIT_LOCK)/`state=BM_INITIAL`/`ConditionVariableInit`; instrumentation `num_workers=nworkers` + zero-fill (the C `memset` → `vec![default(); nworkers]`, default all-zero); `shm_toc_insert(plan_node_id)`; `node->pstate`/`node->sinstrument`. The only deferred step is typing `ParallelBitmapHeapState`/`SharedBitmapHeapInstrumentation` over the shm_toc byte cursor (`pstate_over_chunk`/`sinstrument_over_chunk`), the unported DSM typed-shared-object resolution (execParallel) — same callee `nodeSeqscan` defers. Panic-on-unported-callee. |
| 12 | `ExecBitmapHeapReInitializeDSM` (533) | lib.rs:764 | MATCH | dsa-NULL early return; `state=BM_INITIAL`; `DsaPointerIsValid`→`tbm_free_shared_area`; `tbmiterator=InvalidDsaPointer`. |
| 13 | `ExecBitmapHeapInitializeWorker` (558) | lib.rs:792 | MATCH (blocked on unported callee) | `es_query_dsa!=NULL` assert; `shm_toc_lookup(plan_node_id, noError=false)` (wired real seam, `expect` = the C elog-on-NULL); `node->pstate = pstate_over_chunk(ptr)`; `ptr += MAXALIGN(sizeof(ParallelBitmapHeapState))` (`chunk_advance`); `if instrument` → `node->sinstrument = sinstrument_over_chunk(...)`. Only the cursor→typed-shared-object cast is deferred to the unported DSM resolution (same as #11). Panic-on-unported-callee. |
| 14 | `ExecBitmapHeapRetrieveInstrumentation` (581) | lib.rs:823 | MATCH | NULL early return; deep-copies shared struct into private memory (the C `palloc`+`memcpy`); size uses same `offsetof+num_workers*size`. |
| L1 | `ExecScan` (execScan.c/.h, linked in) | lib.rs:289 | MATCH | `ExecScanExtended` with runtime qual/proj presence tests; fast-path reset+fetch; loop reset+fetch+qual+`InstrCountFiltered1`+project; end-of-scan result-slot clear. Matches execScan.h. |
| L2 | `ExecScanFetch` (execScan.h, linked in) | lib.rs:270 | MATCH | CHECK_FOR_INTERRUPTS then accessMtd. EPQ branch correctly absent (no `es_epq_active` until execMain lands); non-EPQ path is the only reachable one. |
| L3 | `InstrCountFiltered1` (instrument.h inline) | lib.rs:362 | MATCH | `if instrument: nfiltered1+=delta`. |
| L4 | `InstrCountFiltered2` (instrument.h inline) | lib.rs:371 | MATCH | `if instrument: nfiltered2+=delta`. |
| L5 | `ExecQualAndReset` (executor.h inline) | lib.rs:242 | MATCH | NULL qual→true; else `ExecQual`; always `ResetExprContext` after. |

Helper `exec_scan_rescan` (ExecScanReScan, execScan.c) — clears result+scan
slots; EPQ relsubs arrays not yet ported (execMain), correctly noted. The
reachable behavior matches.

## 2. Constants spot-checked against headers

- `PG_WAIT_IPC = 0x08000000`, `PG_WAIT_CLIENT = 0x06000000`
  (`utils/wait_classes.h`). The port's `WAIT_EVENT_PARALLEL_BITMAP_SCAN`
  constant is now correct (`0x08000000 | 38`) — see function #9.
- `offsetof(SharedBitmapHeapInstrumentation, sinstrument) = 8` — correct
  (c2rust line 3365 emits `8`).
- `BM_INITIAL=0`, `BM_INPROGRESS=1`, `BM_FINISHED=2` (`#[repr(i32)]`) — correct.
- `EXEC_FLAG_BACKWARD`/`EXEC_FLAG_MARK` asserted-against from `types-nodes`.

## 3. Seam + wiring audit

**Owned seam crate (by C-source coverage):** the only C file is
`nodeBitmapHeapscan.c`, so the sole owned inward seam crate is
`crates/backend-executor-nodeBitmapHeapscan-seams`. Its 5 declarations
(`exec_bitmapheap_{estimate,initialize_dsm,reinitialize_dsm,initialize_worker,retrieve_instrumentation}`)
are **all** installed by `init_seams()` (lib.rs:862–877), which contains only
`set()` calls. `seams-init::init_all()` calls `init_seams()` (seams-init
lib.rs:27). No uninstalled owned seam; no `set()` outside the owner. **Clean.**

The handle-bridge installers panic because resolving a `PlanStateHandle` back
to the owned `&mut BitmapHeapScanState` needs the executor parallel planstate
registry (unported execParallel) — panic-on-unported-callee, acceptable. The
real per-node logic lives in the `ExecBitmapHeap*` functions above.

**Outward seam crates** (owned by other units, not this audit's scope but
checked for thinness): `backend-nodes-core-tidbitmap-seams`,
`backend-access-table-tableam-bm-seams`, and the
execProcnode/execAmi/execExpr/execTuples/execUtils/tcop-postgres/access-parallel
seams are pure `seam!` declarations (no bodies, no branching/computation).
Outward calls in lib.rs are thin marshal+delegate. **No seam findings.**

## 3b. Design conformance

- Allocating paths carry `Mcx`/`PgResult` (`ExecInitBitmapHeapScan`,
  iterator/tbm seams return `PgResult`). OK.
- No invented opacity: `TIDBitmap`/`TBMPrivateIterator`/`TBMSharedIterator`
  stay opaque (real C opaque types), `TBMIterator`/`dsa_pointer` are real.
- `pstate`/`sinstrument` are no longer initialized via a private `Box`: both
  InitializeDSM and InitializeWorker now obtain them through the
  `pstate_over_chunk`/`sinstrument_over_chunk` DSM-byte-cursor helpers (which
  panic on the unported typed-shared-object resolution, the same callee
  `nodeSeqscan` defers). The C in-DSM cross-worker sharing semantics are thereby
  preserved by construction — no silent private-Box stand-in. The field type
  (`Option<Box<…>>`) is the inherited carrier; only the unported resolution that
  would back it with real DSM bytes is deferred.
- Spinlock held only within a `{}` block via `SpinLockGuard`, never across `?`.
- No shared statics for per-backend globals; no registry-shaped side tables.

No design-conformance findings.

## 4. Conclusion

**PASS** — the two prior `PARTIAL`s (`ExecBitmapHeapInitializeDSM`,
`ExecBitmapHeapInitializeWorker`) are now completed to full own-logic parity
with the C: sizing, `shm_toc_allocate`/`shm_toc_insert`/`shm_toc_lookup` (all
wired real seams against the landed real-DSM `backend-access-transam-parallel`),
the in-place `SpinLockInit`/`state`/`ConditionVariableInit`/instrumentation-zero
init, and the `node->pstate`/`node->sinstrument` placement. The only deferred
step is typing those structs over the shm_toc DSM byte cursor
(`pstate_over_chunk`/`sinstrument_over_chunk`) — the unported DSM
typed-shared-object resolution (execParallel), the exact callee the canonical
parallel-scan sibling `nodeSeqscan` also defers (`pscan_over_chunk`). That is
panic-on-unported-callee, which the audit rules accept; absent own-logic is
gone. The previously-fixed `WAIT_EVENT_PARALLEL_BITMAP_SCAN` constant
(`0x08000000|38`) remains correct. `cargo check --workspace` and
`cargo test --workspace` pass; no other findings.
