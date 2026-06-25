# Audit: backend-executor-nodeSeqscan

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Claude Fable 5
- **Unit:** backend-executor-nodeSeqscan
- **Branch:** fix/epqstate-canonical
- **C source:** `src/backend/executor/nodeSeqscan.c` + inlined `execScan.h`
  driver (`src/include/executor/execScan.h`) and the non-inlined `ExecScan`
  from `execScan.c` (PostgreSQL 18.3)

## Method

Independent re-derivation per `.claude/skills/audit-crate/SKILL.md`. Every
function definition in `nodeSeqscan.c` was enumerated and cross-checked against
the c2rust rendering
(`c2rust-runs/backend-executor-nodeSeqscan/src/nodeSeqscan.rs`) and the Rust
port (`crates/backend-executor-nodeSeqscan/src/lib.rs`). The `execScan.h`
inline helpers (`ExecScanFetch`, `ExecScanExtended`), which are compiled into
`nodeSeqscan.o` in C, were re-derived directly from the header. Constants and
structure layouts (`T_SeqScan = 339`, `scanrelid : Index`, `EPQState` field
set) were verified against the C headers, not from memory.

Special attention was paid to the EvalPlanQual path. The crate reads the
canonical owned `EPQState` held in
`EStateData::es_epq_active: Option<PgBox<EPQState>>`
(`crates/types-nodes/src/execnodes.rs:90,604`) directly; every
`relsubs_done` / `relsubs_slot` / `relsubs_rowmark` / `epqParam` read in
`ExecScanFetch` was checked against `execScan.h` lines 31-135 exactly. There is
**no** `EPQStateHandle` opacity stand-in anywhere in the crate or in
`types-nodes` (grep clean).

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `SeqNext` (static) | nodeSeqscan.c:50 | lib.rs:141 | MATCH | reads `es_direction`, `ss_ScanTupleSlot`; lazy `table_beginscan(rel, es_snapshot, 0, NULL)` when `ss_currentScanDesc == NULL`; `table_scan_getnextslot` → `Some(slot)` / `None`. Snapshot is the query snapshot, matching C. |
| `SeqRecheck` (static) | :89 | lib.rs:192 | MATCH | unconditional `true`; SeqScan uses no keys. |
| `ExecSeqScan` (static) | :109 | lib.rs:576 | MATCH | three `Assert`→`debug_assert` (no EPQ/qual/proj); `ExecScanExtended(.., false, false)`. |
| `ExecSeqScanWithQual` (static) | :129 | lib.rs:589 | MATCH | asserts qual present, no proj; `ExecScanExtended(.., true, false)`. |
| `ExecSeqScanWithProject` (static) | :149 | lib.rs:600 | MATCH | asserts proj present, no qual; `ExecScanExtended(.., false, true)`. |
| `ExecSeqScanWithQualProject` (static) | :170 | lib.rs:611 | MATCH | asserts qual+proj; `ExecScanExtended(.., true, true)`. |
| `ExecSeqScanEPQ` (static) | :192 | lib.rs:622 | MATCH | `ExecScan(&ss, SeqNext, SeqRecheck)` — no asserts, mirrors C. |
| `ExecInitSeqScan` | :206 | lib.rs:664 | MATCH | `outerPlan/innerPlan == NULL` asserts → `lefttree/righttree.is_none()`; makeNode in `es_query_cxt`; plan/state links; `ExecAssignExprContext`; `ExecOpenScanRelation`; `ExecInitScanTupleSlot(RelationGetDescr, table_slot_callbacks)`; `ExecInitResultTypeTL`; `ExecAssignScanProjectionInfo`; `ExecInitQual`. ExecProcNode dispatch: see note below. |
| `ExecEndSeqScan` | :288 | lib.rs:732 | MATCH | `if scanDesc != NULL table_endscan`. Clears the carrier before the seam call; behaviorally identical. |
| `ExecReScanSeqScan` | :316 | lib.rs:744 | MATCH | `if scan != NULL table_rescan(scan, NULL)`; then `ExecScanReScan`. |
| `ExecSeqScanEstimate` | :342 | lib.rs:775 | MATCH | `pscan_len = table_parallelscan_estimate(rel, es_snapshot)`; `shm_toc_estimate_chunk`; `shm_toc_estimate_keys(.., 1)`. |
| `ExecSeqScanInitializeDSM` | :360 | lib.rs:799 | MATCH | `shm_toc_allocate`; `table_parallelscan_initialize`; `shm_toc_insert(plan_node_id, pscan)`; `table_beginscan_parallel`. Orchestration complete; only the DSM byte-cursor→typed-descriptor primitive panics (unported execParallel callee). |
| `ExecSeqScanReInitializeDSM` | :382 | lib.rs:856 | MATCH | `pscan = ss_currentScanDesc->rs_parallel`; `table_parallelscan_reinitialize`. Same DSM primitive panics. |
| `ExecSeqScanInitializeWorker` | :398 | lib.rs:884 | MATCH | `shm_toc_lookup(plan_node_id, false)`; `table_beginscan_parallel`. Same DSM primitive panics. |

### Inlined `execScan.h` / `execScan.c` driver (linked into `nodeSeqscan.o`)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecScanFetch` | execScan.h:31 | lib.rs:241 | MATCH | `CHECK_FOR_INTERRUPTS`; `epqstate != NULL` branch reproduced exactly — see EPQ verification below. |
| `ExecScanExtended` | execScan.h:159 | lib.rs:347 | MATCH | no-qual/no-proj fast path (reset econtext → fetch); else reset, loop: fetch → TupIsNull (clear proj result slot if projecting, else return slot) → set `ecxt_scantuple` → qual (skip call when no qual) → project/return → `InstrCountFiltered1(node,1)` on fail → reset and retry. |
| `ExecScan` | execScan.c | lib.rs:454 | MATCH | thin wrapper: `has_qual = qual.is_some()`, `has_proj = ps_ProjInfo.is_some()`, delegate to `ExecScanExtended`. Equivalent to C `ExecScan(node, a, r)` = `ExecScanExtended(node, a, r, es_epq_active, qual, projInfo)`. |

### In-crate helpers

| Helper | Source | Verdict | Notes |
|---|---|---|---|
| `InstrCountFiltered1` | executor.h | MATCH | bumps `instrument.nfiltered1` by delta when instrument present. |
| `TupIsNull` (`tup_is_null` / `scan_tuple_is_null`) | tuptable.h | MATCH | absent slot or empty flag → true. |
| `scan_scanrelid` | (Scan*)plan->scanrelid | MATCH | reads `scanrelid : Index` (u32) off the SeqScan plan node. |
| `bms_is_member` (`epq_param_is_member_of_ext_param`) | bitmapset.c | MATCH | `bms_is_member(epqParam, plan->extParam)`; NULL set → false (bitmapset seam). |

## EvalPlanQual verification (execScan.h:31-135 vs lib.rs:241-336)

Re-derived check-by-check; the canonical owned `EPQState` is read directly with
no handle indirection:

1. `CHECK_FOR_INTERRUPTS()` → `tcop::check_for_interrupts::call()?` first. MATCH.
2. `if (epqstate != NULL)` → `if estate.es_epq_active.is_some()`. MATCH.
3. `scanrelid = ((Scan*)node->ps.plan)->scanrelid` → `scan_scanrelid(node)`. MATCH.
4. **`scanrelid == 0` pushdown:** `if bms_is_member(epqParam, plan->extParam)` →
   run recheck on `ss_ScanTupleSlot`, clear on fail, `return slot`; else fall
   through to accessMtd. Port lines 259-276 reproduce this exactly, including the
   fall-through when the bms test is false. MATCH.
5. **`relsubs_done[scanrelid-1]` short-circuit:** clear `ss_ScanTupleSlot` and
   return it. Port lines 277-283. MATCH.
6. **`relsubs_slot[scanrelid-1] != NULL` replacement:** set `relsubs_done = true`;
   `if TupIsNull(slot) return NULL`; recheck → on fail `ExecClearTuple(slot)` and
   return that (replacement) slot; else return the replacement slot. Port lines
   284-305 — crucially returns the **distinct** `repl` slot, not
   `ss_ScanTupleSlot`, matching C. The C `Assert(relsubs_rowmark[i]==NULL)` is a
   debug assert with no runtime effect; its omission is behavior-preserving. MATCH.
7. **`relsubs_rowmark[scanrelid-1] != NULL` rowmark fetch:** set
   `relsubs_done = true`; `EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)`
   (execMain seam) → false ⇒ NULL; `TupIsNull` ⇒ NULL; recheck → on fail clear and
   return `ss_ScanTupleSlot`; else return it. Port lines 306-330. MATCH.
8. Else `return (*accessMtd)(node)` → `access_mtd(node, estate)`. Port line 335.
   MATCH.

The branch ordering (`if scanrelid==0 … else if done … else if slot … else if
rowmark`) is preserved exactly. `epqParam` is read off the canonical
`EPQState.epqParam: i32`. The `relsubs_*` readers map the canonical
`Option<PgVec<..>>` (outer `None` = C NULL array; per-entry value = C entry); a
missing/None array reads as `false`/"no substitute", which is the same observable
result the C arrays produce once `EvalPlanQualBegin` has populated them — and
`ExecScanFetch` is only reached during an active recheck, where C has those
arrays allocated. No `relsubs_blocked` read appears in `ExecScanFetch`, matching
C (that field is only touched in execMain). No `EPQStateHandle` stand-in present.

## ExecProcNode dispatch (design note, not a divergence)

C `ExecInitSeqScan` installs one of six specialized `ExecProcNode` function
pointers selected by `(es_epq_active, qual, ps_ProjInfo)`. The port installs a
single `exec_seq_scan_node` callback (lib.rs:632) that **re-derives the same
selection** at call time from the identical predicates and delegates to the same
six entry functions. The selection logic is byte-for-byte the same boolean
lattice as the C `if/else` chain (lib.rs:720-725 install; lib.rs:646-657
dispatch). Observable behavior is identical on every input; the C
function-pointer specialization is purely a per-call-overhead optimization. The
six `ExecSeqScan*` entry points all exist with full logic. MATCH.

## Seam audit

**Owned seam crate (by C-source coverage):**
`crates/backend-executor-nodeSeqscan-seams` (maps to `nodeSeqscan.c`). It
declares four *inward* seams — `exec_seqscan_estimate`,
`exec_seqscan_initialize_dsm`, `exec_seqscan_reinitialize_dsm`,
`exec_seqscan_initialize_worker` — the parallel-executor methods that
execParallel dispatches to over opaque `PlanStateHandle`. All four are installed
by this crate's `init_seams()` (lib.rs:79-88), which contains nothing but
`set()` calls, and `init_seams()` is wired into `seams-init::init_all()`
(`crates/seams-init/src/lib.rs:32`). No uninstalled declaration; installer is
not empty. The installed adapters panic at the `PlanStateHandle →
SeqScanState` bridge, which is owned by the unported execParallel PlanState
resolution — mirror-PG-and-panic on an unported callee, with the full
`nodeSeqscan.c` logic living in the concrete `ExecSeqScan*` functions. PASS.

**Outward seams** (tableam, execExpr, execMain, execScan, execTuples, execUtils,
tcop, parallel, bitmapset) are each a thin marshal+delegate into a subsystem
below/beside the executor knot; none carries branching, node construction, or
computation on the seam path. The `EvalPlanQualFetchRowMark` call is correctly
routed through the execMain seam (it is execMain-owned, not seqscan logic). No
finding.

## Design conformance

- **No introduced opacity.** The EPQ state is the real owned
  `EPQState`/`EStateData::es_epq_active`, read directly; no `EPQStateHandle` or
  any invented newtype (types.md rules 6-7). The `ss_currentScanDesc` carrier and
  `PlanStateHandle`/`ParallelContextHandle` are inherited opacity (C
  forward-declares `TableScanDesc`/uses opaque `PlanState*` across the parallel
  framework), not introduced.
- **Allocating paths use `Mcx` + `PgResult`.** `ExecInitSeqScan` allocates the
  state tree in `estate.es_query_cxt` and is fallible; all seam/AM calls that
  can `ereport(ERROR+)` return `PgResult`.
- **No per-backend shared statics, no ambient-global seams, no locks held across
  `?`, no registry-shaped side tables, no unledgered divergence markers.**
- DSM parallel-scan primitives panic with a clear "execParallel not yet landed"
  message — a not-yet-resolved callee, not absent in-crate logic; the
  orchestration around them is fully ported.

## Conclusion

All 14 C functions are MATCH; the inlined `execScan.h` driver
(`ExecScanFetch`/`ExecScanExtended`) and `ExecScan` are MATCH; the EvalPlanQual
path reconciles against the canonical owned `EPQState` exactly (scanrelid==0
pushdown, relsubs_done short-circuit, replacement-slot return on the distinct
`relsubs_slot`, rowmark fetch) with no leftover `EPQStateHandle` opacity; the
seam audit is clean (four inward seams all installed by a non-empty
`init_seams()` wired into `init_all`); zero design-conformance findings.
**PASS.**
