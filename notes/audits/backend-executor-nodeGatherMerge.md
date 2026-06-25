# Audit: backend-executor-nodeGatherMerge

Independent function-by-function audit of `port/backend-executor-nodeGatherMerge`
(commit `0443eb1c`) against `src/backend/executor/nodeGatherMerge.c` (PG 18.3),
the c2rust rendering (`c2rust-runs/backend-executor-nodeGatherMerge`), and the
relevant headers. Re-derived from sources; the port's comments/self-review were
not trusted.

## Scope / inventory

`c_sources` for this unit is the single file `nodeGatherMerge.c`. The c2rust run
defines the file's 14 own functions; every other symbol it references
(`binaryheap_*`, `ExecInitParallelPlan`, `TupleQueueReaderNext`, …) is an
`extern` declaration — i.e. owned by another translation unit, not this file.
The `lib/binaryheap.c` slot-index heap is reimplemented in-crate as a leaf
algorithm (no dependency cycle), exactly mirroring `nodeMergeAppend`.

## Per-function table

| # | C function (nodeGatherMerge.c) | Port location (src/lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecInitGatherMerge` | `ExecInitGatherMerge` | MATCH | makeNode, ExecAssignExprContext (direct), ExecInitNode (seam), ExecGetResultType (direct), ExecInitResultTypeTL (seam) + ExecConditionalAssignProjectionInfo (direct) w/ OUTER_VAR=-2, ps_ProjInfo==NULL → resultops* set, sort-key loop, gather_merge_setup. The `outeropsset/outeropsfixed=true/false` stores have no field in the trimmed PlanStateData (documented behavior-preserving no-op). Asserts → debug_assert. |
| 2 | `ExecGatherMerge` | `ExecGatherMerge` (+`exec_gather_merge_node` dispatch shim) | MATCH | CHECK_FOR_INTERRUPTS; `!initialized` launch path with `num_workers>0 && es_use_parallel_mode`, pei init vs reinit, LaunchParallelWorkers, nworkers bookkeeping (`es_parallel_workers_to_launch/launched`), reader-array memcpy vs no-workers branch; `parallel_leader_participation || nreaders==0` → need_to_scan_locally; ResetExprContext; gather_merge_getnext → TupIsNull → NULL; ps_ProjInfo==NULL early return else ecxt_outertuple + ExecProject. initParam deep-copied to release the immutable plan borrow (behavior-preserving). |
| 3 | `ExecEndGatherMerge` | `ExecEndGatherMerge` | MATCH | ExecEndNode(outer) then ExecShutdownGatherMerge. |
| 4 | `ExecShutdownGatherMerge` | `ExecShutdownGatherMerge` | MATCH | ExecShutdownGatherMergeWorkers; if pei → ExecParallelCleanup, pei=NULL. |
| 5 | `ExecShutdownGatherMergeWorkers` | `exec_shutdown_gather_merge_workers` | MATCH | if pei → ExecParallelFinish; pfree(reader) → PgVec::clear; reader=NULL → empty vec. |
| 6 | `ExecReScanGatherMerge` | `ExecReScanGatherMerge` | MATCH | shutdown workers, clear tuples, initialized/gm_initialized=false; rescan_param>=0 → bms_add_member on outer chgParam; chgParam==NULL → ExecReScan(outer). |
| 7 | `gather_merge_setup` | `gather_merge_setup` | MATCH | gm_slots = (nreaders+1) NULLs; per-worker tuple_buffers (tuple array len MAX_TUPLE_STORE of NULLs) + ExecInitExtraTupleSlot(MinimalTuple); gm_heap = binaryheap_allocate(nreaders+1). |
| 8 | `gather_merge_init` | `gather_merge_init` | MATCH | gm_slots[0]=NULL; per-worker reset (nTuples/readCounter=0, done=false, ExecClearTuple); binaryheap_reset; the `reread:` goto modeled as a labeled loop — first pass over `0..=nreaders` with nowait, add_unordered live readers / load_tuple_array; recheck `1..=nreaders` → set nowait=false + `goto reread`; binaryheap_build; gm_initialized=true. Skip predicate `(i==0)?need_to_scan_locally:!done` matches. |
| 9 | `gather_merge_clear_tuples` | `gather_merge_clear_tuples` | MATCH | per-reader: pfree buffered tuples (readCounter<nTuples) → drop (set None) + advance; ExecClearTuple(gm_slots[i+1]). |
| 10 | `gather_merge_getnext` | `gather_merge_getnext` | MATCH | !gm_initialized → init; else binaryheap_first → readnext → replace_first or remove_first; binaryheap_empty → clear_tuples + NULL else return first slot index. |
| 11 | `load_tuple_array` | `load_tuple_array` | MATCH | reader==0 return; reset counters when empty; fill `nTuples..MAX_TUPLE_STORE` via gm_readnext_tuple(nowait=true), break on NULL. |
| 12 | `gather_merge_readnext` | `gather_merge_readnext` | MATCH | leader (reader==0): install es_query_dsa = pei?area:NULL, ExecProcNode, restore NULL, store slot[0]/return true or need_to_scan_locally=false → false. Worker: buffered tuple (nTuples>readCounter, readCounter++), else done→false, else gm_readnext_tuple+load_tuple_array; Assert(tup); ExecStoreMinimalTuple(slot[reader], pfree=true). |
| 13 | `gm_readnext_tuple` | `gm_readnext_tuple` | MATCH | CHECK_FOR_INTERRUPTS; reader[nreader-1]; `TupleQueueReaderNext(reader, nowait, done)` rewired to the landed tqueue seam `(reader,nowait)->(Option<Vec<u8>>,bool)`: `*done` set from the returned bool (C: false at entry, true only on SHM_MQ_DETACHED — owner returns (None,true) on Detached, (None,false) on WouldBlock/empty, (Some,false) on Success); `tup ? heap_copy_minimal_tuple(tup,0) : NULL` realized as `None→Ok(None)` / `Some(image)→from_minimal_parts(mcx, t_len=image[0..4], body=image[4..])` (the buffer-lived copy the C makes; image == owner's to_minimal_bytes). Re-verified vs nodeGatherMerge.c:714 + tqueue.c TupleQueueReaderNext. |
| 14 | `heap_compare_slots` | `heap_compare_slots` | MATCH | DatumGetInt32 slot indices; per sort key slot_getattr + ApplySortComparator; on nonzero INVERT_COMPARE_RESULT + return; else 0. |

### In-crate `lib/binaryheap.c` (leaf, no cycle — mirrors nodeMergeAppend)

| C (common/binaryheap.c) | Port | Verdict | Notes |
|---|---|---|---|
| `binaryheap_reset` | `binaryheap_reset` | MATCH | size=0, has_heap_property=true. |
| `binaryheap_empty` | `binaryheap_empty` | MATCH | size==0. |
| `binaryheap_add_unordered` | `binaryheap_add_unordered` | MATCH | `bh_size>=bh_space` → `elog(ERROR,"out of binary heap slots")`; has_heap_property=false; push; size++. |
| `binaryheap_first` | `binaryheap_first` | MATCH | return nodes[0] (caller ensures non-empty). |
| `binaryheap_build` | `binaryheap_build_node` | MATCH | `for i=parent_offset(size-1)..0 sift_down`; has_heap_property=true. |
| `binaryheap_remove_first` | `binaryheap_remove_first_node` | MATCH | size==1 short-circuit; else nodes[0]=last, size--, sift_down(0). |
| `binaryheap_replace_first` | `binaryheap_replace_first_node` | MATCH | nodes[0]=d; size>1 → sift_down(0). |
| `sift_down` | `sift_down` | MATCH | right-child-larger check; `left_off>=size || compare(node,swap)>=0` break (split into two short-circuit-equivalent checks so swap_off is never indexed when no left child); hole-fill. |
| `parent/left/right_offset` | same | MATCH | (i-1)/2, 2i+1, 2i+2. |

## Constants verified against headers (not memory)

- `OUTER_VAR = -2` — primnodes.h:243. ✓
- `MAX_TUPLE_STORE = 10` — nodeGatherMerge.c:31. ✓
- `T_GatherMerge = 369`, `T_GatherMergeState = 433` — generated nodetags.h. ✓
- `INVERT_COMPARE_RESULT(var) = (var<0)?1:-(var)` — c.h:1081. Port: `if var<0 {1} else {var.wrapping_neg()}` — equivalent, and wrapping_neg covers the `-INT_MIN` corner. ✓
- `ApplySortComparator` NULL/reverse logic — sortsupport.h:200. Port helper matches branch-for-branch (uses the regular `comparator`, as the C does in `heap_compare_slots`). ✓

## Seam / wiring audit

- This unit owns **no** `*-seams` crate (no C file here is the owner of any seam
  registry); it is reached through executor dispatch (execProcnode / execAmi),
  which depend on this crate directly without a cycle. Therefore `init_seams()`
  is correctly **empty**, and it is wired into `seams-init::init_all()`
  (seams-init/src/lib.rs:69). The recurrence_guard's inverse check
  (`every_declared_seam_is_installed_by_its_owner`) passes — no owned-but-unset
  seam.
- Every outward seam call is a thin marshal+delegate into the real owner of a
  genuine cycle: execProcnode (ExecInitNode/ExecProcNode/ExecEndNode), execAmi
  (ExecReScan), execTuples (slot setup/clear/store/getattr), execUtils-seams
  (ResetExprContext/ExecInitResultTypeTL), execExpr-seams (ExecProject),
  sortsupport (PrepareSortSupportFromOrderingOp/comparator), tcop-postgres
  (CHECK_FOR_INTERRUPTS), nodes-core (bms_add_member), execParallel-seams and
  transam-parallel-seams (the parallel launch path), tqueue-seams
  (TupleQueueReaderNext, landed `(reader,nowait)->(Option<Vec<u8>>,bool)` shape;
  the consumer rebuilds the MinimalTuple from the wire bytes — the copy is in
  the crate, not the seam path). ExecAssignExprContext/ExecGetResultType/
  ExecConditionalAssignProjectionInfo and the `parallel_leader_participation`
  GUC are reached **directly** (no cycle). No branching/node-construction/
  computation lives in any seam path; the `_owned` execParallel variants are
  faithful bridges over the owned plan-state tree (documented), not logic.

## Design conformance (3b)

- No invented opacity: handles (`ParallelExecutorInfo`, `TupleQueueReaderHandle`,
  `SlotId`/`EcxtId`) are owner-defined real types, not stand-ins introduced here.
- Allocating functions/seams carry `Mcx` + return `PgResult` (gather_merge_setup,
  the `_owned` parallel seams, clone_in helpers); `gm_readnext_tuple` allocates
  the rebuilt MinimalTuple in `estate.es_query_cxt` (PgResult-fallible). ✓
- No shared statics for per-backend state; no ambient-global seams; no locks
  across `?`; no registry side tables.
- No `todo!()`/`unimplemented!()`/own-logic stubs. Unported callees are
  seam-and-panic (mirror PG and panic), not restructured around.

## Gates

- `cargo check --workspace` — clean (warnings only, pre-existing, unrelated crates).
- `cargo test -p backend-executor-nodeGatherMerge` — 5 passed.
- `cargo test -p seams-init` — recurrence_guard 2 passed
  (wired-into-init_all + every-declared-seam-installed).

## Verdict: PASS

Every function MATCH (or thin SEAMED delegate per the rules); zero seam findings;
no MISSING/PARTIAL/DIVERGES; gates green.
