# Audit: backend-executor-nodeHashjoin

**Verdict: PASS**

Date: 2026-06-13
Model: claude-opus-4-8 (Opus 4.8, 1M context)

Crate: `crates/backend-executor-nodeHashjoin/src/lib.rs`
C source: `postgres-18.3/src/backend/executor/nodeHashjoin.c`
Headers: `executor/hashjoin.h`, `nodes/execnodes.h`, `nodes/plannodes.h`, `nodes/nodetags.h`

This is an INDEPENDENT, re-derived-from-C audit (re-audit; see the Re-audit
section at the end). `cargo check -p backend-executor-nodeHashjoin` is clean.
All 18 functions are MATCH or acceptable-panic, all design points hold, the
owned `ExecHashJoinSaveTuple` seam is installed and wired, and the canonical
NodeTag constants are correct. The two prior FAIL findings (S1 wiring, C1
NodeTag corruption) are resolved.

---

## Per-function inventory (nodeHashjoin.c)

Every function definition in nodeHashjoin.c, including statics.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecHashJoinImpl` (220) | `ExecHashJoinImpl` (167) | MATCH | All six HJ_* states, fall-through HJ_BUILD_HASHTABLE→HJ_NEED_NEW_OUTER via `handle_need_new_outer`, the empty-outer prefetch heuristic, the parallel build-barrier dance, the antijoin/right-semi/right-anti/single_match special cases, `InstrCountFiltered1/2`, and the default `elog(ERROR, "unrecognized hashjoin state")` all reproduced. The C `break` (exit switch, loop for(;;)) maps to `continue` on the outer loop. Spot-checked in detail (see below). |
| 2 | `ExecHashJoin` (683) | `exec_hash_join_node` (558) | MATCH | `ExecHashJoinImpl(pstate, false)`; castNode→enum match. |
| 3 | `ExecParallelHashJoin` (699) | `exec_parallel_hash_join_node` (570) | MATCH | `ExecHashJoinImpl(pstate, true)`. |
| 4 | `ExecInitHashJoin` (715) | `ExecInitHashJoin` (586) | MATCH | makeNode, ExecProcNode=ExecHashJoin, jointype, child init, result slot/projection, hj_OuterTupleSlot, single_match, null-tuple-slot switch (all 8 jointypes + default elog), hj_HashTupleSlot "voodoo", build_hash_exprs, qual/joinqual/hashclauses init, and the hash-state field resets. The skew-table gate reads the real inner `Hash.skewTable`. |
| 5 | `ExecEndHashJoin` (947) | `ExecEndHashJoin` (856) | MATCH | Destroy hash table if present (seam clears the field), then end outer + inner subtrees. |
| 6 | `ExecHashJoinOuterGetTuple` (978) | `ExecHashJoinOuterGetTuple` (885) | MATCH | curbatch==0 first-pass path (first-outer-tuple stash, hash eval, isnull skip, OuterNotEmpty), curbatch<nbatch batch-file path (file==NULL→NULL), end-of-batch NULL. |
| 7 | `ExecParallelHashJoinOuterGetTuple` (1057) | `ExecParallelHashJoinOuterGetTuple` (953) | MATCH | Single-batch direct-outer path vs. `sts_parallel_scan_next` batch-file path, ExecForceStoreMinimalTuple / ExecClearTuple, and the `batches[curbatch].outer_eof = true` at end. |
| 8 | `ExecHashJoinNewBatch` (1129) | `ExecHashJoinNewBatch` (1016) | MATCH (benign omission noted) | curbatch>0 closes prior outer file; first-batch skew reset; the batch-skip while-loop with all four break rules (1,1,2,3) and the temp-file releases; ExecHashTableReset; inner reload (rewind→GetSavedTuple→Insert→close); outer rewind. **Benign omission**: C also sets `skewBucket=NULL; skewBucketNums=NULL` in the first-batch branch; the port omits these two (it sets `skewEnabled=false; nSkewBuckets=0; spaceUsedSkew=0`). Behaviorally inert — skew lookups are gated entirely on `skewEnabled` (verified in nodeHash `ExecHashGetSkewBucket`, skew.rs:204), and the C NULLs only to drop dangling pointers into the about-to-be-reset context, which the owned `PgVec` model does not need. Provably identical on every input → MATCH. |
| 9 | `ExecParallelHashJoinNewBatch` (1270) | `ExecParallelHashJoinNewBatch` (1130) | MATCH | curbatch-detach, distributor-seeded round-robin search, the BarrierAttach phase switch with PHJ_BATCH_ELECT/ALLOCATE/LOAD fall-through chain (factored into `parallel_batch_allocate_load_probe` / `parallel_batch_load_probe`), PHJ_BATCH_PROBE return-attached, PHJ_BATCH_SCAN/FREE detach-and-loop, default `elog(ERROR,"unexpected batch phase")`, and the `(batchno+1)%nbatch`/start_batchno loop bound. |
| 10 | `ExecHashJoinSaveTuple` (1413) | seam `exec_hash_join_save_tuple_seam` (137) + in-crate `ExecHashJoinSaveTuple` (1350) | MATCH | Byte format `[u32 hashvalue][MinimalTuple]`, lazy BufFile create in spillCxt on first write. Two renderings: the seam entry (called by nodeHash) and the in-crate helper (used by `save_outer_tuple_to_batch`); both write hashvalue then tuple bytes. |
| 11 | `ExecHashJoinGetSavedTuple` (1454) | `ExecHashJoinGetSavedTuple` (1386) | MATCH | CHECK_FOR_INTERRUPTS, two-word header read (hashvalue, t_len), EOF→ExecClearTuple+NULL, palloc(t_len)/t_len assignment, read `t_len - sizeof(uint32)` body, ExecForceStoreMinimalTuple(...,true). Re-derived in detail (see below). |
| 12 | `ExecReScanHashJoin` (1493) | `ExecReScanHashJoin` (1467) | MATCH | Single-batch + inner-chgParam-NULL reuse path (match-flag reset for right/right-anti/right-semi/full, OuterNotEmpty reset, skip to HJ_NEED_NEW_OUTER) vs. destroy-and-rebuild (instrumentation accumulate, destroy, HJ_BUILD_HASHTABLE, conditional inner ExecReScan); always-reset intra-tuple state; conditional outer ExecReScan. The instrumentation-accumulate + `hashNode->hashtable=NULL` is delegated to nodeHash's `exec_hash_accum_instrumentation` (writes HashState — correct owner). |
| 13 | `ExecShutdownHashJoin` (1582) | `ExecShutdownHashJoin` (1553) | MATCH | DetachBatch + Detach when hj_HashTable present. |
| 14 | `ExecParallelHashJoinPartitionOuter` (1597) | `ExecParallelHashJoinPartitionOuter` (1565) | MATCH | Assert FirstOuterTupleSlot==NULL; outer-plan loop, hash eval + isnull skip, ExecFetchSlotMinimalTuple, ExecHashGetBucketAndBatch, sts_puttuple, CHECK_FOR_INTERRUPTS; final `sts_end_write` over all nbatch partitions. |
| 15 | `ExecHashJoinEstimate` (1648) | `ExecHashJoinEstimate` (1631) | MATCH (acceptable panic) | Body is `shm_toc_estimate_*` on the ParallelContext — entirely execParallel-owned (unported). Loud panic. No in-crate logic precedes it in C, so nothing is MISSING. Acceptable per the brief. |
| 16 | `ExecHashJoinInitializeDSM` (1655) | `ExecHashJoinInitializeDSM` (1646) | MATCH (acceptable panic) | In-crate part is the `ExecSetExecProcNode(&ps, ExecParallelHashJoin)` swap, which IS present (line 1648) before the panic; the rest (shm_toc_allocate, pstate init, LWLockInitialize, BarrierInit, SharedFileSetInit) is execParallel/nodeHash-owned. Note: C does `if (pcxt->seg == NULL) return;` before the swap; the port does the swap unconditionally then panics — since the function panics regardless, the seg==NULL early-return is not observable. Acceptable. |
| 17 | `ExecHashJoinReInitializeDSM` (1713) | `ExecHashJoinReInitializeDSM` (1663) | MATCH (acceptable panic) | In-crate part — DetachBatch + Detach when hj_HashTable!=NULL — runs first (lines 1667-1672), then panics on the shm_toc_lookup/SharedFileSetDeleteAll/BarrierInit tail. Acceptable. |
| 18 | `ExecHashJoinInitializeWorker` (1751) | `ExecHashJoinInitializeWorker` (1682) | MATCH (acceptable panic) | In-crate tail is the ExecProcNode swap (present, line 1689) before the panic; SharedFileSetAttach + shared-state lookup are execParallel-owned. Acceptable. |

No C function is MISSING, PARTIAL, or DIVERGES. The four parallel DSM hooks
panic on unported execParallel callees, with their in-crate parts present
before the panic — acceptable per the brief and the skill.

---

## Spot-checks (re-derived from C in detail)

**`ExecHashJoinGetSavedTuple` (#11).** C reads `uint32 header[2]` via
`BufFileReadMaybeEOF(file, header, 8, eofOK=true)`; `nread==0`→clear+NULL;
`*hashvalue=header[0]`; `tuple=palloc(header[1])`, `tuple->t_len=header[1]`;
`BufFileReadExact(file, (char*)tuple + sizeof(uint32), header[1]-sizeof(uint32))`;
`ExecForceStoreMinimalTuple(tuple, slot, shouldFree=true)`. Port: reads 8-byte
`header`, EOF→clear+`Ok(None)`, `hashvalue=ne_bytes[0..4]`, `t_len=ne_bytes[4..8]`,
`body_len = t_len - 4` (`saturating_sub` — equivalent for any well-formed
record where t_len>=4), reads `body_len` bytes, reassembles via
`from_minimal_parts(mcx, t_len, body)`, `exec_force_store_minimal_tuple(..., true)`.
Byte-for-byte identical, native endianness preserved. MATCH confirmed.

**`HJ_SCAN_BUCKET` match logic (#1).** Re-derived the jointype special cases:
right-semi early `continue` on already-matched inner (HeapTupleHeaderHasMatch);
set-match-if-unset; JOIN_ANTI→NEED_NEW_OUTER+continue (never return matched);
single_match→NEED_NEW_OUTER; JOIN_RIGHT_ANTI→continue (never return, keep
scanning); otherqual gate → ExecProject return vs. InstrCountFiltered2; joinqual
fail → InstrCountFiltered1. All branch order and predicates match lines 538-596.

**`ExecHashJoinNewBatch` skip rules (#8).** All four break conditions verified
against C lines 1190-1201: rule 1 outer (HJ_FILL_OUTER), rule 1 inner
(HJ_FILL_INNER), rule 2 (`nbatch != nbatch_original`), rule 3 (`nbatch !=
nbatch_outstart`); the `has_outer`/`has_inner` snapshots taken before the
branch match the C re-reads. Loop guard `curbatch < nbatch && (outer==NULL ||
inner==NULL)` matches.

---

## Design-conformance verification (brief points 1/2/3)

**(1) Canonical types / no introduced opacity — CONFIRMED.**
`types_nodes::nodehashjoin` re-exports `BufFile, HashJoinState,
HashJoinTableData, HashTupleIdx, ParallelHashJoinBatch,
ParallelHashJoinBatchAccessor, ParallelHashJoinState, SharedTuplestoreAccessor,
INVALID_SKEW_BUCKET_NO, PHJ_*` from `types_nodes::nodehash` (nodehashjoin.rs:172-178).
The old opacity stand-ins are deleted (documented at nodehashjoin.rs:165-171).
The crate navigates the full `HashJoinTableData`; `parallel_state:
Option<DsaPointer>` (nodehash.rs:357), `innerBatchFile`/`outerBatchFile:
PgVec<Option<PgBox<BufFile>>>` (nodehash.rs:336-338), batch accessors
`inner_tuples`/`outer_tuples: Option<PgBox<SharedTuplestoreAccessor>>`
(nodehash.rs:248-250). `BufFile`/`SharedTuplestoreAccessor` are opaque newtypes
over `Opaque` (nodehash.rs:37,43) — INHERITED opacity (buffile.c /
sharedtuplestore.c unported), which is OK. No invented opacity in this crate.

**(2) Hash-table-library seams — CONFIRMED correctly SEAMED.**
All hash-table operations go through `backend_executor_nodeHash_seams`:
`exec_hash_table_create`, `multi_exec_hash`, `exec_scan_hash_bucket`,
`exec_parallel_scan_hash_bucket`, `exec_hash_get_bucket_and_batch`,
`exec_hash_get_skew_bucket`, `exec_hash_table_insert`, `exec_hash_table_reset`,
`exec_hash_table_destroy`, `exec_hash_table_detach[_batch]`,
`exec_parallel_hash_table_*`, `exec_build_hash32_expr`,
`setup_skew_hashfunction`, the build-barrier/parallel-batch barrier helpers,
`exec_prep_hash_table_for_unmatched`, `cur_tuple_has_match/set_match`, etc.
nodeHash genuinely owns this logic and there is a real dependency cycle
(nodeHash calls back into `ExecHashJoinSaveTuple`). These are SEAMED, not
MISSING. Seam paths are thin marshal+delegate.

**(3) State-machine logic in-crate — CONFIRMED.**
ExecHashJoinImpl, all HJ_* states, OuterGetTuple (both variants), NewBatch
(both variants), SaveTuple/GetSavedTuple byte format, batch-skip rules,
jointype special cases, ExecInitHashJoin, build_hash_exprs, ExecReScanHashJoin,
ExecShutdownHashJoin, ExecParallelHashJoinPartitionOuter, and the
ExecSetExecProcNode swaps in the DSM hooks are all in `lib.rs`.

---

## Seam install audit

Owned seam crate (by C-source coverage of `executor/nodeHashjoin.c`):
`crates/backend-executor-nodeHashjoin-seams`. It declares exactly one seam,
`ExecHashJoinSaveTuple` (called by nodeHash's `ExecHashIncreaseNumBatches` /
`ExecHashRemoveNextSkewBucket`; confirmed live callers in nodeHash
`hash_table.rs:730,1040` and `skew.rs:386`).

- `init_seams()` (lib.rs:128-130) contains exactly one `set()` call:
  `ExecHashJoinSaveTuple::set(exec_hash_join_save_tuple_seam)`. Nothing but a
  set() call. **GOOD** — every owned-seam declaration is installed by the owner.

- **FINDING S1 — RESOLVED.** `crates/seams-init/src/lib.rs::init_all()` now
  calls `backend_executor_nodeHashjoin::init_seams();` (line 28, kept sorted
  next to the nodeHash line). The Cargo dep exists
  (`seams-init/Cargo.toml:32`). The owner's `init_seams()` (lib.rs:128-130)
  remains exactly one `set()` call and nothing else
  (`ExecHashJoinSaveTuple::set(exec_hash_join_save_tuple_seam)`). The owned seam
  is now installed. No finding.

---

## Other findings (canonical-types correctness — this re-port's surface)

- **FINDING C1 — RESOLVED.** `types_nodes::nodehashjoin.rs` now declares
  `T_HashJoin = NodeTag(359)` (line 18) and `T_Hash = NodeTag(370)` (line 101),
  and `T_HashJoinState` is no longer a duplicate constant: it is
  `pub use crate::execstate_tags::T_HashJoinState;` (line 22), the single
  canonical `NodeTag(423)`. All three re-derived against
  `postgres-18.3/src/backend/nodes/nodetags.h` this re-audit:
  `T_HashJoin = 359` (line 376), `T_Hash = 370` (line 387),
  `T_HashJoinState = 423` (line 440). Correct.

  The load-bearing consumers now resolve correctly:
  - `planstate.rs:17` imports `T_HashJoinState` from `nodehashjoin` (which now
    forwards to execstate_tags' 423); `PlanStateNode::tag()` (planstate.rs:58)
    returns 423 for `PlanStateNode::HashJoin(_)`.
  - `nodes.rs:129/130` return the corrected `nodehashjoin::T_HashJoin` (359)
    and `nodehashjoin::T_Hash` (370).
  - `backend-executor-execParallel` dispatches `tag == T_HashJoinState` against
    `execstate_tags::T_HashJoinState` (423) — now the single shared definition,
    so the node's `tag()` (423) matches and the parallel
    Estimate/InitializeDSM/ReInitializeDSM/InitializeWorker entry points are
    reachable for a hash join.
  - The prior 437 collision with `T_LimitState` (nodelimit.rs:18, still the
    correct `NodeTag(437)`) is gone — HashJoinState no longer aliases Limit.

  The recommended consolidation (consume the single canonical
  `execstate_tags::T_HashJoinState` rather than a duplicate) was applied. No
  finding.

(Note: the wait-event constants WAIT_EVENT_HASH_BATCH_* / HASH_BUILD_HASH_OUTER
at lib.rs:68-71 are auto-generated values that could not be cross-checked
against a source header in this tree; they are only passed to barrier-wait
seams for wait-event reporting and do not affect join correctness, and those
parallel paths panic pre-execParallel regardless. Low risk; flagged for the
owner to confirm against the generated wait_event enum, not counted as the
basis for FAIL.)

---

## Verdict

**PASS.** The crate's function-by-function logic is faithful (every C function
MATCH or acceptable-panic SEAMED-equivalent), brief points (1), (2), (3) hold,
the owned seam is installed and wired, and the canonical NodeTag constants are
correct. Both prior merge-blocking findings are resolved (see Re-audit). The
CATALOG row may be set to `audited`.

---

## Re-audit (2026-06-13, claude-opus-4-8)

Re-derived the two prior FAIL findings from the C sources and headers; did not
trust the green build or port comments.

- **S1 (uninstalled owned seam) — RESOLVED.** `seams-init/src/lib.rs:28` now
  calls `backend_executor_nodeHashjoin::init_seams();`; the Cargo dep is present
  (`seams-init/Cargo.toml:32`); the owner's `init_seams()` (lib.rs:128-130)
  is still set()-only, installing the sole owned seam
  `ExecHashJoinSaveTuple`. Owned-seam coverage is complete.

- **C1 (NodeTag corruption) — RESOLVED.** Verified against
  `nodes/nodetags.h`: `T_HashJoin = 359` (line 376), `T_Hash = 370` (387),
  `T_HashJoinState = 423` (440), `T_LimitState = 437` (454). The canonical
  types now match: `nodehashjoin.rs:18` `T_HashJoin = NodeTag(359)`,
  `:101` `T_Hash = NodeTag(370)`, and `:22` `T_HashJoinState` is a re-export
  of the single canonical `execstate_tags::T_HashJoinState` (`NodeTag(423)`)
  rather than a duplicate. `PlanStateNode::tag()` returns 423 and
  `Node::tag()` returns 359/370; execParallel's `tag == T_HashJoinState`
  dispatch (also 423, shared definition) matches a live HashJoinState. The
  prior 437 collision with `T_LimitState` is gone.

- **Regression check.** Re-confirmed the rest of the crate is unchanged and
  still all-MATCH/acceptable-panic (the per-function table above stands);
  `cargo check -p backend-executor-nodeHashjoin` is clean.

(The wait-event constants note from the original audit remains a low-risk
owner follow-up, not a basis for any finding.)
