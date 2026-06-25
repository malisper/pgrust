# Audit: backend-executor-nodeSort

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `port/backend-executor-nodeSort`
- **Unit c_sources:** `src/backend/executor/nodeSort.c`
- **Verdict: PASS**

Independent re-derivation (the port's own comments/self-review were not trusted).
Sources compared:
- C: `../pgrust/postgres-18.3/src/backend/executor/nodeSort.c`
- c2rust: `../pgrust/c2rust-runs/backend-executor-nodeSort/src/nodeSort.rs`
- Port: `crates/backend-executor-nodeSort/src/lib.rs`; state/plan vocabulary in
  `crates/types-nodes/src/nodesort.rs`

## 1. Function inventory

All 10 file-scope functions in `nodeSort.c` (cross-checked against the c2rust run,
which kept all 10). `SO1_printf`, `CHECK_FOR_INTERRUPTS`, `TupIsNull`,
`outerPlanState`, `castNode`, `TupleDescAttr` are header macros, not functions; no
statics/inline helpers exist beyond the 10.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecSort` (50) | `lib.rs:87` (+ `exec_sort_node` cb `lib.rs:283`) | MATCH | detail below |
| 2 | `ExecInitSort` (221) | `lib.rs:307` | MATCH | makeNode→`alloc_in`; castNode panic; flag/datumSort logic identical |
| 3 | `ExecEndSort` (301) | `lib.rs:395` | MATCH | tuplesort_end iff present then NULL; ExecEndNode(outer) |
| 4 | `ExecSortMarkPos` (329) | `lib.rs:423` | MATCH | `!sort_Done` early return; tuplesort_markpos |
| 5 | `ExecSortRestrPos` (347) | `lib.rs:438` | MATCH | `!sort_Done` early return; tuplesort_restorepos |
| 6 | `ExecReScanSort` (362) | `lib.rs:459` | MATCH | re-sort condition matches C lines 384-387 exactly |
| 7 | `ExecSortEstimate` (416) | `lib.rs:544` | MATCH (opaque reads/writes SEAMED) | guards + sizing in-crate |
| 8 | `ExecSortInitializeDSM` (437) | `lib.rs:569` | MATCH (opaque reads/writes SEAMED) | guards + sizing in-crate; shm ops on opaque shm_toc via support seam |
| 9 | `ExecSortInitializeWorker` (462) | `lib.rs:595` | MATCH (opaque reads/writes SEAMED) | shm_toc_lookup + am_worker=true via support seam |
| 10 | `ExecSortRetrieveInstrumentation` (476) | `lib.rs:610` | MATCH (opaque reads/writes SEAMED) | NULL-guard + sizing in-crate; palloc/memcpy on opaque shm via support seam |

## 2. Per-function detail (spot re-derivations)

### ExecSort — re-derived in full
- `CHECK_FOR_INTERRUPTS()` → `tcop_postgres::check_for_interrupts::call()?`.
- `dir = estate->es_direction`; first-call branch forces
  `es_direction = ForwardScanDirection`, restores `dir` after performsort.
- `tuplesortopts`: base `TUPLESORT_NONE`; `|= RANDOMACCESS` iff `randomAccess`;
  `|= ALLOWBOUNDED` iff `bounded`. Constants `0 / 1<<0 / 1<<1` verified vs
  `utils/tuplesort.h:94,97,100`.
- datum vs heap begin: `tuplesort_begin_datum(TupleDescAttr(tupDesc,0)->atttypid,
  sortOperators[0], collations[0], nullsFirst[0], work_mem, NULL, opts)` /
  `tuplesort_begin_heap(tupDesc, numCols, sortColIdx, sortOperators, collations,
  nullsFirst, work_mem, NULL, opts)`. `work_mem` via `globals::work_mem::call()`.
- `if (bounded) tuplesort_set_bound(state, bound)`.
- Feed loops mirror C: datum `slot_getsomeattrs(slot,1)`+`putdatum(values[0],
  isnull[0])`; tuple `puttupleslot(slot)`. Loop exit on `TupIsNull` ↔
  `next_outer_slot` returning `None` when the slot is missing **or** `is_empty()`,
  matching c2rust's `slot.is_null() || (tts_flags & TTS_FLAG_EMPTY)`.
- `performsort`; restore `es_direction`; `sort_Done=true`,
  `bounded_Done=bounded`, `bound_Done=bound`.
- worker-stats: `if (shared_info && am_worker)` → `tuplesort_get_stats` into
  `sinstrument[ParallelWorkerNumber]`; the two C `Assert`s become
  `debug_assert!`s; `ParallelWorkerNumber` via the parallel seam.
- retrieval: datum path `ExecClearTuple` then `if getdatum(...) StoreVirtual` —
  the port's `exec_store_first_datum` seam contract is itself
  `ExecClearTuple; write[0]; ExecStoreVirtualTuple`, and the not-found arm calls
  `exec_clear_tuple`, so the slot ends stored-virtual on hit / cleared on miss,
  identical to C. tuple path `(void) tuplesort_gettupleslot(state, forward, false,
  slot, NULL)`. `forward = ScanDirectionIsForward(dir)` in both.

Benign ordering observation (not a finding): C sets
`node->tuplesortstate = tuplesortstate` *before* the feed loop (C:125); the port
stashes it *after* performsort (`lib.rs:218`). Nothing reads `node.tuplesortstate`
between those points (the loop uses the local binding; `ExecProcNode` on the outer
node cannot reach this field), so behaviour is identical on every input.

### Sizing constant
`offsetof(SharedSortInfo, sinstrument)` = 8: C struct is `int num_workers` (4 B)
followed by `TuplesortInstrumentation sinstrument[]`; `TuplesortInstrumentation`
contains an `int64` ⇒ 8-byte alignment ⇒ the int pads to offset 8. Verified vs
`nodes/execnodes.h:2380` and `utils/tuplesort.h:111`. Port's
`SHARED_SORT_INFO_HEADER = 8` matches the c2rust literal `8`
(c2rust lines 2071/2092/2131).

Benign observation (not a finding): C wraps `ExecSortEstimate`/`DSM`/`Retrieve`
sizing in `mul_size`/`add_size`, which `ereport` only on `size_t` overflow; the
port uses `*`/`+`. For `nworkers*24 + 8` overflow is unreachable on 64-bit for any
realistic `nworkers`, so no input diverges.

### Constants verified against headers
- `TUPLESORT_NONE/RANDOMACCESS/ALLOWBOUNDED` = `0 / 1<<0 / 1<<1`
  (`utils/tuplesort.h:94,97,100`).
- `EXEC_FLAG_REWIND/BACKWARD/MARK` = `0x0004 / 0x0008 / 0x0010`
  (`executor/executor.h:67-69`).
- `T_SortState` NodeTag; `TuplesortMethod` (0,1<<0..1<<3) and `TuplesortSpaceType`
  (DISK=0, MEMORY=1) match `utils/tuplesort.h`.

## 3. Seam audit

**Owned seam crates (by C-source coverage):** the unit's only `c_source` is
`nodeSort.c`, so the sole owned seam crate is `crates/backend-executor-nodeSort-seams`.

- It declares exactly four inward hooks: `exec_sort_estimate`,
  `exec_sort_initialize_dsm`, `exec_sort_initialize_worker`,
  `exec_sort_retrieve_instrumentation`. **All four are installed** by this crate's
  `init_seams()` (`lib.rs:69-76`), which contains nothing but `set()` calls.
  `seams-init` calls `backend_executor_nodeSort::init_seams()`
  (`crates/seams-init/src/lib.rs:29`). No uninstalled owned seam; no `set()`
  outside the owner. Clean.

**Outward seam calls** — each backed by a genuinely unported owner, each a thin
declaration (no `::set()` installer exists anywhere for them yet — confirmed by
grep — so all panic until their owner lands; no branching/construction/computation
in any seam path):
- tuplesort.c engine ops → `backend-utils-sort-tuplesort-seams`.
- execProcnode.c (`ExecInitNode/ProcNode/EndNode`) → `backend-executor-execProcnode-seams`.
- execTuples.c (`ExecGetResultType`, `slot_getsomeattrs`, `ExecClearTuple`,
  first-datum store, `ExecInitResultTupleSlotTL`) → `backend-executor-execTuples-seams`.
- execUtils.c (`ExecCreateScanSlotFromOuterPlan`) → `backend-executor-execUtils-seams`.
- execAmi.c (`ExecReScan`) → `backend-executor-execAmi-seams`.
- tcop/postgres.c (`CHECK_FOR_INTERRUPTS`) → `backend-tcop-postgres-seams`.
- globals.c (`work_mem`) → `backend-utils-init-small-seams`.
- access/parallel.c (`ParallelWorkerNumber`) → `backend-access-transam-parallel-seams`.
- the four parallel hooks address the live `SortState`, opaque `ParallelContext`/
  `shm_toc`, and instrumentation fields via `backend-executor-execParallel-support-seams`.

The four parallel functions keep all C control flow in this crate (the
`instrument`/`nworkers` guards, the shm chunk sizing, the `am_worker` flag, the
`shared_info == NULL` guard and copy size). Only handle-addressed reads/writes of
genuinely unported types cross the seam. `sort_initialize_dsm_shared_info` /
`sort_retrieve_shared_info` bundle the primitive shm operations
(`shm_toc_allocate`+`memset`+set-`num_workers`+`shm_toc_insert`; `palloc`+`memcpy`)
on the opaque `shm_toc`/`SharedSortInfo` — these are inherited-opacity operations
on types nodeSort cannot name, with the decision and size computed in-crate, not
in-crate logic relocated elsewhere. No function body was replaced by a seam call to
"somewhere else"; all sort control flow lives here.

## 3b. Design conformance

- **Invented opacity (types.md 6-7):** none. `Tuplesortstate` is `void *` in C too;
  the type-erased carrier in `types-nodes::nodesort` is inherited opacity for the
  future tuplesort owner. `Sort`, `SortState`, `SharedSortInfo`,
  `TuplesortInstrumentation` are real structs mirroring the C headers
  field-for-field. Parallel `*Handle` types are the established opaque-handle
  vocabulary for the unported parallel subsystem.
- **Allocating fns/seams carry `Mcx` + `PgResult`:** `ExecInitSort` and the begin
  path derive `Mcx` from `estate.es_query_cxt` and are fallible; `tuplesort_begin_*`
  and the slot/datum seams are fallible.
- **No shared statics for per-backend globals** (`work_mem`,
  `ParallelWorkerNumber` read through owners' seams), **no ambient-global seams,
  no locks across `?`, no registry side tables, no unledgered divergence markers.**

## 4. Verdict

All 10 functions `MATCH` (the four parallel hooks MATCH with their opaque
handle reads/writes properly SEAMED). The single owned seam crate has every
declaration installed by `init_seams()`, wired into `seams-init`. Zero seam
findings, zero design-conformance findings. Crate builds clean
(`cargo build -p backend-executor-nodeSort`).

**PASS.** No `MISSING` / `PARTIAL` / `DIVERGES`. Audit-only lane — no fixes performed.
