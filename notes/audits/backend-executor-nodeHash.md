# Audit: backend-executor-nodeHash

**Verdict: PASS**

Date: 2026-06-13
Model: claude-opus-4-8 (Opus 4.8, 1M context)

Independent function-by-function RE-audit of the JOINT re-port of
`executor/nodeHash.c` (PostgreSQL 18.3), re-derived from the C source and the
hashjoin.h / execnodes.h / plannodes.h headers. The prior-round FAIL Findings A
and B (parallel-aware insert/scan leaf ops fabricating header-only tuples and a
hardcoded `Ok(false)` qual) have been fixed; this round re-derives the four
affected functions (#19, #20, #23, #27) from scratch and confirms they now match
the C. Audit only; no source edited. `cargo check -p backend-executor-nodeHash`
builds clean.

All prior PASS items still hold (canonical structs, full 36+4 seam install, the
serial bodies). The four previously-PARTIAL parallel leaf ops now route through
the same real, installed execTuples / execExpr / execUtils seams the serial
paths use, with a byte round-trip (`to_minimal_bytes` ⇄ `mintuple_from_dsa`)
that is mutually consistent field-for-field. No DIVERGES / PARTIAL / MISSING
remains.

---

## Per-function table

C location = nodeHash.c line. Verdicts: MATCH / SEAMED / PARTIAL / MISSING /
DIVERGES. "panic (unported callee)" is acceptable and scored MATCH per skill §4.

| # | C function | C line | Port location | Verdict | Notes |
|---|------------|--------|---------------|---------|-------|
| 1 | ExecHash | 91 | exec_hash.rs:81 | MATCH | elog(ERROR) → `Err(PgError::error(...))`. |
| 2 | MultiExecHash | 105 | exec_hash.rs:92 | MATCH | instrument paths panic on unported InstrStart/StopNode (acceptable); dispatch serial/parallel exact. |
| 3 | MultiExecPrivateHash | 138 | exec_hash.rs:134 | MATCH | loop, skew vs normal insert, IncreaseNumBuckets, spaceUsed/peak/partialTuples all faithful. |
| 4 | MultiExecParallelHash | 219 | exec_hash.rs:253 | MATCH | barrier phase switch + fallthrough modeled; sts_end_write wired per batch; counters/dimensions exact. |
| 5 | ExecInitHash | 370 | exec_hash.rs:457 | MATCH | makeNode, ExecAssignExprContext, child init, result-slot TL, qual==NIL assert, hash_expr=NULL. |
| 6 | ExecEndHash | 427 | exec_hash.rs:527 | MATCH | recurse ExecEndNode(outerPlan). |
| 7 | ExecHashTableCreate | 446 | hash_table.rs:156 | MATCH | sizes via Hash plan rows_total/plan_rows/plan_width through real Plan; serial multi-batch path panics on unported PrepareTempTablespaces (acceptable); parallel path delegates to ExecParallelHashJoinSetUpBatches/Alloc. |
| 8 | ExecChooseHashTableSize | 658 | hash_table.rs:316 | MATCH | tupsize, skew_mcvs, max_pointers, dbuckets, multi-batch sbuckets/dbatch, U-curve walk-back loop — all transcribed exactly incl. overflow guards. |
| 9 | ExecHashTableDestroy | 956 | hash_table.rs:503 | MATCH | close batch files 1..nbatch via buffile seam, drop arenas. |
| 10 | ExecHashIncreaseBatchSize | 998 | hash_table.rs:541 | MATCH | batchSpace compare, double spaceAllowed. |
| 11 | ExecHashIncreaseNumBatches | 1030 | hash_table.rs:563 | MATCH | growEnabled/overflow guards, file-array grow (panics on PrepareTempTablespaces, acceptable), nbuckets resize, chunk rescan via tuples arena, dump via ExecHashJoinSaveTuple seam, nfreed disable. Arena-walk equivalence sound. |
| 12 | ExecParallelHashIncreaseNumBatches | 1198 | parallel.rs:292 | MATCH | grow_batches phase machine ELECT→REALLOCATE→REPARTITION→DECIDE→FINISH with fallthrough; elect/decide bodies faithful (DSA/barrier/sts seams). |
| 13 | ExecParallelHashRepartitionFirst | 1430 | parallel.rs:651 | MATCH | pop chunk queue, per-tuple bucket/batch, copy to batch-0 chunk or sts_puttuple, counters. |
| 14 | ExecParallelHashRepartitionRest | 1497 | parallel.rs:713 | MATCH | attach old inner tuplestores, parallel scan, re-store, counters. |
| 15 | ExecParallelHashMergeCounters | 1557 | parallel.rs:772 | MATCH | LWLock-guarded fold into shared totals, reset locals. |
| 16 | ExecHashIncreaseNumBuckets | 1587 | hash_table.rs:762 | MATCH | nbuckets/log2 set, re-zero buckets, rebuild chains from tuples arena. |
| 17 | ExecParallelHashIncreaseNumBuckets | 1650 | parallel.rs:542 | MATCH | grow_buckets ELECT→REALLOCATE→REINSERT, double nbuckets, reinsert from chunk queue. |
| 18 | ExecHashTableInsert | 1749 | hash_table.rs:941 (+ exec_hash_table_insert_tuple 965) | MATCH | fetch via execTuples seam then full insert: dense_alloc, ClearMatch, bucket push, nbuckets_optimal bump, spaceUsed/peak, IncreaseNumBatches; else-branch ExecHashJoinSaveTuple seam. |
| 19 | ExecParallelHashTableInsert | 1839 | parallel.rs:810 | MATCH | Fetches the real tuple via `exec_fetch_slot_minimal_tuple_copy`, stages its `to_minimal_bytes` flat image, and `memcpy`s `t_len` real bytes into the DSA HashJoinTuple (batch 0) or `sts_puttuple`s it (spill). Retry/batch0/prealloc control flow faithful; clear-match + ntuples++ exact. Finding A RESOLVED. |
| 20 | ExecParallelHashTableInsertCurrentBatch | 1905 | parallel.rs:873 | MATCH | Same real-fetch + DSA copy; non-retryable `ExecParallelHashTupleAlloc`, hashvalue/clear-match/push exact. Finding A RESOLVED. |
| 21 | ExecHashGetBucketAndBatch | 1960 | hash_table.rs:1055 | MATCH | nbatch>1 ROR(hashvalue, log2_nbuckets)&(nbatch-1); else batchno=0. Exact. |
| 22 | ExecScanHashBucket | 1992 | hash_table.rs:1083 | MATCH | cur/skew/regular head selection, hashvalue compare, store minimal via execTuples seam, ExecQualAndReset via execExpr seam, hj_CurTuple. Spot-checked (below). |
| 23 | ExecParallelScanHashBucket | 2053 | parallel.rs:910 | MATCH | First/Next chain walk + hashvalue compare faithful; reconstructs the DSA MinimalTuple via `mintuple_from_dsa` and force-stores it into hj_HashTupleSlot via `exec_force_store_minimal_tuple`, sets ecxt_innertuple, then calls the REAL `exec_qual_and_reset(hashclauses, econtext)`; on pass sets hj_CurTuple and returns true. Findings A+B RESOLVED. |
| 24 | ExecPrepHashTableForUnmatched | 2104 | hash_table.rs:1194 | MATCH | zero the three cursor fields. |
| 25 | ExecParallelPrepHashTableForUnmatched | 2125 | parallel.rs:945 | MATCH | wait-free election, done/skip_unmatched, sts_end_parallel_scan, spacePeak, delegate to serial Prep. |
| 26 | ExecScanHashTableForUnmatched | 2190 | hash_table.rs:1209 | MATCH | bucket/skew bucket walk, HasMatch test, store + ResetExprContext via seams, cursor advance. |
| 27 | ExecParallelScanHashTableForUnmatched | 2264 | parallel.rs:1067 | MATCH | for(;;) Next/First-per-bucket walk + `!HasMatch` test faithful; reconstructs the DSA MinimalTuple via `mintuple_from_dsa`, force-stores into hj_HashTupleSlot, sets ecxt_innertuple, then calls `reset_per_tuple_expr_context` (execUtils, the C `ResetExprContext`), sets hj_CurTuple, returns true. Finding A RESOLVED. |
| 28 | ExecHashTableReset | 2327 | hash_table.rs:1316 | MATCH | clear arena, re-alloc empty buckets, spaceUsed=0, forget chunks. |
| 29 | ExecHashTableResetMatchFlags | 2355 | hash_table.rs:1349 | MATCH | clear match flag over main + skew chains. |
| 30 | ExecReScanHash | 2381 | exec_hash.rs:542 | MATCH | if chgParam==NULL ExecReScan(outerPlan) via execAmi seam. |
| 31 | ExecHashBuildSkewHash | 2403 | skew.rs:32 | MATCH | skewTable/mcvsToUse guards, get_attstatsslot_mcv seam, frac<MIN gate, nbuckets<<2, open-addressing insert with FunctionCall1Coll seam, space accounting. |
| 32 | ExecHashGetSkewBucket | 2555 | skew.rs:201 | MATCH | skewEnabled gate, AND-mask probe loop, found/INVALID. |
| 33 | ExecHashSkewTableInsert | 2601 | skew.rs:234 | MATCH | fetch via seam, ClearMatch, push to skew chain, space accounting, RemoveNext loop, IncreaseNumBatches. |
| 34 | ExecHashRemoveNextSkewBucket | 2647 | skew.rs:304 | MATCH | last bucket, bucket/batch, per-tuple move to dense or temp file, space adjust, free bucket, give-up-on-empty. |
| 35 | ExecHashEstimate | 2761 | instrument.rs:35 | SEAMED/MATCH | thin marshal to execParallel-support seam; gate + size formula correct. |
| 36 | ExecHashInitializeDSM | 2780 | instrument.rs:59 | SEAMED/MATCH | gate + size + shared-info init delegated to support seam. |
| 37 | ExecHashInitializeWorker | 2805 | instrument.rs:89 | SEAMED/MATCH | gate + plan_node_id lookup delegated. |
| 38 | ExecShutdownHash | 2831 | exec_hash.rs:558 | MATCH | alloc hinstrument if EXPLAIN & none, accumulate over final table. |
| 39 | ExecHashRetrieveInstrumentation | 2846 | instrument.rs:109 | SEAMED/MATCH | present gate + size + copy delegated. |
| 40 | ExecHashAccumInstrumentation | 2877 | instrument.rs:130 | MATCH | five Max() folds, exact. |
| 41 | dense_alloc | 2896 | hash_table.rs:834 | MATCH | MAXALIGN, oversized-chunk insert-after-head, current-chunk fit / fresh chunk, used/ntuples accounting. Owned-arena slot reservation is the faithful analog of the char* return. |
| 42 | ExecParallelHashTupleAlloc | 2976 | parallel.rs:1076/1091 | MATCH | fast path, LWLock slow path, growth-help retry (Ok(None)), oversized chunk, space/load-factor checks, chunk push, current_chunk. Retryable form models C return-NULL. |
| 43 | ExecParallelHashJoinSetUpBatches | 3124 | parallel.rs:1215 | MATCH | dsa_allocate0 (zeroed), accessor array, per-batch BarrierInit/attach-batch0 spin, sts_initialize inner/outer with i%dof%d names. |
| 44 | ExecParallelHashCloseBatchAccessors | 3204 | parallel.rs:1296 | MATCH | sts_end_write + sts_end_parallel_scan per batch, clear accessors. |
| 45 | ExecParallelHashEnsureBatchAccessors | 3225 | parallel.rs:1327 | MATCH | early-return-if-current, close-if-stale, DsaPointerIsValid assert, re-attach. |
| 46 | ExecParallelHashTableAlloc | 3289 | parallel.rs:1380 | MATCH | dsa_allocate bucket array, init each atomic to InvalidDsaPointer. |
| 47 | ExecHashTableDetachBatch | 3309 | parallel.rs:1404 | MATCH | parallel_state&&curbatch>=0, close files, PROBE/SCAN assert, skip_unmatched, ArriveAndDetachExceptLast/ArriveAndDetach, free chunks+buckets, spacePeak, curbatch=-1. |
| 48 | ExecHashTableDetach | 3401 | parallel.rs:1484 | MATCH | build_barrier>=RUN assert, close files, last-out free batches, parallel_state=NULL. |
| 49 | ExecParallelHashFirstTuple | 3451 | parallel.rs:1542 | MATCH | atomic read head, resolve, None on invalid. |
| 50 | ExecParallelHashNextTuple | 3467 | parallel.rs:1558 | MATCH | resolve tuple->next.shared. |
| 51 | ExecParallelHashPushTuple | 3481 | parallel.rs:1575 | MATCH | CAS loop on bucket head atomic. |
| 52 | ExecParallelHashTableSetCurrentBatch | 3499 | parallel.rs:1604 | MATCH | curbatch, nbuckets=pstate->nbuckets, log2, reset current_chunk, at_least_one_chunk=false. |
| 53 | ExecParallelHashPopChunkQueue | 3520 | parallel.rs:1630 | MATCH | LWLock, DsaPointerIsValid queue head, advance, return chunk+dp. |
| 54 | ExecParallelHashTuplePrealloc | 3561 | parallel.rs:1660 | MATCH | want, asserts, LWLock, growth-help, space check, set preallocated/estimated_size. |
| 55 | get_hash_memory_limit | 3622 | hash_table.rs:1387 | MATCH | work_mem*hash_mem_multiplier*1024 clamped to SIZE_MAX; GUCs threaded via parallel::hash_mem_gucs (thread_local per AGENTS.md). |

Inline static helpers (my_log2, pg_nextpower2/prevpower2, pg_rotate_right32) all
re-derived and correct.

---

## Re-audit (2026-06-13) — Findings A and B RESOLVED

The four affected functions were re-derived from scratch against nodeHash.c and
confirmed MATCH. The fabricating local stubs from the prior round
(`exec_fetch_slot_minimal_tuple`, `exec_store_minimal_tuple_into_inner`, the
local `exec_qual_and_reset`) are gone; a grep for their names and for the
"header-only" / "no qual match" comments returns nothing.

### Finding A (RESOLVED) — real tuple now moves through the parallel path
- Insert side (`ExecParallelHashTableInsert` parallel.rs:810,
  `ExecParallelHashTableInsertCurrentBatch` :873): fetch the real tuple via
  `backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple_copy::call`,
  stage `tuple.to_minimal_bytes()`, and `copy_nonoverlapping` `t_len` real bytes
  into the DSA `HJTUPLE_MINTUPLE(hashTuple)` (batch 0) or `sts_puttuple_raw` it
  to the inner tuplestore (spill). The header-only scratch buffer is gone.
- Scan side (`ExecParallelScanHashBucket` :910,
  `ExecParallelScanHashTableForUnmatched` :1067): reconstruct the DSA-resident
  MinimalTuple from its flat image via `mintuple_from_dsa(mcx, mintuple_addr)`
  then `backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call`
  into `hj_HashTupleSlot`, exactly mirroring C's
  `ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), hj_HashTupleSlot, false)`.

  Byte round-trip consistency VERIFIED field-for-field: `to_minimal_bytes`
  writes `t_len(4) | mt_padding[6] | t_infomask2(2) | t_infomask(2) | t_hoff(1)
  | bits_len(4) | t_bits[bits_len]`; `mintuple_from_dsa` reads `t_len` at off 0,
  `bits_len` at off 15, body at off 4 of length `19+bits_len`, and
  `MinimalTupleData::from_minimal_parts` parses the body in the identical field
  order. The write helper and read helper are mutually consistent and match the
  spill-file (`ExecHashJoinSaveTuple`) format. The canonical `MinimalTupleData`
  carries header + length-prefixed `t_bits` (no separate payload field) — this
  is the whole-crate model already accepted for the serial paths (#18/#22/#26),
  and the parallel byte round-trip preserves exactly those carried fields; the
  serial path stores `MinimalTupleData` by value and never byte-round-trips, so
  parity is on the carried fields, which both sides preserve.

### Finding B (RESOLVED) — parallel qual now real
`ExecParallelScanHashBucket` (parallel.rs:910) compiles `hashclauses` from
`hjstate.hashclauses` and, after storing the inner tuple and setting
`ecxt_innertuple`, calls the REAL
`backend_executor_execExpr_seams::exec_qual_and_reset::call(hashclauses,
econtext_id, estate)`; on pass it sets `hj_CurTuple` and returns `true`. No
hardcoded constant remains, so a parallel hash join can now report matches.
`ExecParallelScanHashTableForUnmatched` (parallel.rs:1067) calls
`backend_executor_execUtils_seams::reset_per_tuple_expr_context::call` after the
store, mirroring C's `ResetExprContext(econtext)`.

All four seams used are declared in their correct owner crates (execTuples /
execExpr / execUtils — not nodeHash) and are real, justified outward seam calls
(thin marshal + delegate). `cargo check -p backend-executor-nodeHash` clean.

No DIVERGES / PARTIAL / MISSING remains anywhere in the crate.

---

## Confirmation of the three JOINT-re-port objectives

1. **Canonical C-faithful structs / no introduced opacity — CONFIRMED.**
   `types_nodes::nodehash::HashJoinTableData` carries the full 32+ C fields
   (nbuckets/log2 family, buckets union, skew arrays, nbatch family, space
   accounting, chunks/current_chunk, area/parallel_state, batches, plus the
   owned-arena `tuples`/`chunk_arena` modeling the dense byte buffers).
   `ParallelHashJoinState` is `#[repr(C)]` with real `Barrier`×3, `LWLock`,
   `pg_atomic_uint32 distributor`, `SharedFileSet`. `ParallelHashJoinBatch` is
   `#[repr(C)]` with the genuine shared fields; `ParallelHashJoinBatchAccessor`
   is complete. The `Hash` plan node (nodehashjoin.rs, re-exported) carries
   skewTable/skewColumn/skewInherit/rows_total + a real `Plan`. `BufFile` and
   `SharedTuplestoreAccessor` are `Opaque` newtypes — INHERITED opacity only
   (buffile.c / sharedtuplestore.c unported), permitted by types.md. No invented
   handles/stand-ins for ported types.

2. **All 36 nodeHash-seams + 4 pq-seams installed — CONFIRMED.**
   `backend-executor-nodeHash-seams` declares exactly 36 `seam!`s; `init_seams`
   installs all 36 (`s::*::set`), exact name match (empty set-diff
   declared↔installed). `backend-executor-nodeHash-pq-seams` declares 4;
   `init_seams` installs all 4. `init_seams` is `set()`-only and is invoked from
   `seams-init::init_all()`. No empty installer, no uninstalled seam, no stray
   `set()`.

3. **Previously missing/partial bodies now real — CONFIRMED for the named ones.**
   - `ExecHashTableCreate` sizes from the inner Hash plan node's
     rows_total/plan_rows/plan_width via the real Plan fields and
     `PlanStateNode::Hash` resolution (#7 MATCH).
   - `ExecHashTableInsert` fetches via the execTuples seam then runs the full
     insert in `exec_hash_table_insert_tuple` (#18 MATCH).
   - `ExecScanHashBucket` / `ExecScanHashTableForUnmatched` store the minimal
     tuple via the execTuples seam and call ExecQualAndReset / ResetExprContext
     via the execExpr/execUtils seams (#22, #26 MATCH).
   - `MultiExecParallelHash` wires `sts_end_write` per batch (#4 MATCH).
   - `get_hash_memory_limit` reads work_mem/hash_mem_multiplier via
     `parallel::hash_mem_gucs` (#55 MATCH).

   Note: the analogous **parallel** leaf ops (Findings A/B) are the NEW PARTIAL
   regressions — the serial fixes were not mirrored on the parallel side.

---

## Seam-path conformance

Outward seam calls (execProcnode, execExpr, execTuples, execUtils, execAmi,
barrier, lwlock, dsa, sts, buffile, nodeHashjoin SaveTuple, lsyscache, fmgr,
execParallel-support) are thin marshal+delegate; no node construction or
branching smuggled into a seam path. The pq-seam adapters and the barrier/batch
one-liner adapters in `lib.rs` are pure resolve-and-delegate. The
`hash_mem_gucs` thread_local is the AGENTS.md-sanctioned backend-global stand-in
for the unported GUC owner (documented) — not a registry side-table. lwlock
guards are explicitly `.release()`d (no lock held across `?`). No invented
opacity.

## Design conformance

No violations found beyond Findings A/B (which are logic, not architecture). All
allocating functions take `Mcx` and return `PgResult`. Inherited opacity only.

---

## Verdict

PASS. Every function is MATCH or SEAMED-per-rules; zero seam findings; Findings
A and B from the prior round are resolved with faithful logic (not new stubs).
The crate may merge. Update the `CATALOG.tsv` row to `audited`.
