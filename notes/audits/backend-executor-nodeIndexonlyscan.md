# Audit: backend-executor-nodeIndexonlyscan

- **Unit:** `backend-executor-nodeIndexonlyscan`
- **C source:** `src/backend/executor/nodeIndexonlyscan.c` (PostgreSQL 18.3)
- **Port crate:** `crates/backend-executor-nodeIndexonlyscan`
- **Branch:** `port/backend-executor-nodeIndexonlyscan`
- **Date:** 2026-06-13 (independent re-audit; prior entries 2026-06-12)
- **Model:** Claude Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Top-line verdict: **PASS** (independent re-audit 2026-06-13)

This is an independent from-scratch audit. The previously-failing findings
(F1–F4, D1) were re-derived from the C/c2rust/headers and confirmed **resolved**
(see per-finding Resolution blocks). One new divergence was found this round
(**F5 — `ExecIndexOnlyScan` bypassed the generic `ExecReScan` dispatcher**),
fixed on the branch, and re-audited clean. Final verdict PASS.

## Function inventory and verdicts

The translation unit defines 14 functions. `ExecScan`/`ExecScanExtended`/
`ExecScanFetch` are **not** part of this unit — they are extern symbols from
`execScan.c` (separate object `execScan.o` in the Makefile; c2rust renders
`ExecScan` as an `extern` declaration at line 199, only the *call* at line 3430
is in this TU). They are listed separately under design findings.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `IndexOnlyNext` | nodeIndexonlyscan.c:60 | lib.rs:87 | MATCH | VM-visible skip, heap-fetch fallback + `continue`, non-MVCC HOT-continue error, hitup/itup/none branch, lossy recheck + `InstrCountFiltered2` + `continue`, ORDER-BY recheck feature error (SQLSTATE `FEATURE_NOT_SUPPORTED`), page predicate lock when `!tuple_from_heap`, end-of-scan `ExecClearTuple`. `VM_ALL_VISIBLE` = `visibilitymap_get_status & VISIBILITYMAP_ALL_VISIBLE(0x01)` matches header. Leaf ops correctly seamed. |
| 2 | `StoreIndexTuple` | nodeIndexonlyscan.c:268 | lib.rs:301 | MATCH | ClearTuple → `index_deform_tuple` (AM itupdesc, not slot desc) → name-cstring fix-up (`unlikely` branch via non-empty `ioss_NameCStringAttNums`) → `ExecStoreVirtualTuple`. NAMEDATALEN(64) padding decision owned here; per-attr read/write seamed to execTuples. |
| 3 | `IndexOnlyRecheck` | nodeIndexonlyscan.c:325 | lib.rs:344 | MATCH | Always `elog(ERROR, "EvalPlanQual recheck is not supported in index-only scans")`. |
| 4 | `ExecIndexOnlyScan` | nodeIndexonlyscan.c:336 | lib.rs:351 | MATCH (after F5 fix) | Runtime-key guard now routes through the generic `ExecReScan` dispatcher (`execAmi::exec_re_scan`), matching C:345 `ExecReScan((PlanState *) node)`; then `ExecScan` via `exec_scan_indexonly` seam. See F5. |
| 5 | `ExecReScanIndexOnlyScan` | nodeIndexonlyscan.c:363 | lib.rs:536 | MATCH | Runtime-key reset+eval (seamed to nodeIndexscan helper), `RuntimeKeysReady=true`, conditional `index_rescan`, `ExecScanReScan`. |
| 6 | `ExecEndIndexOnlyScan` | nodeIndexonlyscan.c:398 | lib.rs:563 | MATCH | VM-buffer release, parallel-worker stat accumulation (`is_parallel_worker` + `accumulate_shared_index_searches`, additive not memcpy — matches C comment), `index_endscan`, `index_close(NoLock)` via handle drop (documented NoLock closer). |
| 7 | `ExecIndexOnlyMarkPos` | nodeIndexonlyscan.c:454 | lib.rs:606 | MATCH | EPQ guard (slot-or-rowmark present → assert relsubs_done → return), else `index_markpos`. `Assert(scanrelid>0)` → `debug_assert`. |
| 8 | `ExecIndexOnlyRestrPos` | nodeIndexonlyscan.c:491 | lib.rs:635 | MATCH | Same EPQ guard, else `index_restrpos`. (C reads `estate->es_epq_active` in the `if` then dereferences `epqstate`; port reads `epqstate` from the `if let` — equivalent.) |
| 9 | `ExecInitIndexOnlyScan` | nodeIndexonlyscan.c:527 | lib.rs:663 | **DIVERGES + PARTIAL** | See F1 (INDEX_VAR constant wrong) and F2 (`node_as_node` panics mid-body). |
| 10 | `ExecIndexOnlyScanEstimate` | nodeIndexonlyscan.c:727 | lib.rs:836 | MATCH | instrument/parallel_aware gate, `index_parallelscan_estimate`, chunk+key estimate. |
| 11 | `ExecIndexOnlyScanInitializeDSM` | nodeIndexonlyscan.c:757 | lib.rs:872 | **PARTIAL** | Body logic mirrors C, but `plan_node_id()` (lib.rs:1107) unconditionally returns `Err` for every non-trivial path, so the TOC insert + scandesc setup can never run (F3). |
| 12 | `ExecIndexOnlyScanReInitializeDSM` | nodeIndexonlyscan.c:812 | lib.rs:950 | MATCH | Assert parallel_aware, `index_parallelrescan`. |
| 13 | `ExecIndexOnlyScanInitializeWorker` | nodeIndexonlyscan.c:826 | lib.rs:964 | **MISSING** | The `if (instrument)` branch (C:842-844) sets `node->ioss_SharedInfo = OffsetToPointer(piscan, piscan->ps_offset_ins)`. Port does `let _ = &piscan;` — a no-op (F4). Also gated by `plan_node_id` Err (F3). |
| 14 | `ExecIndexOnlyScanRetrieveInstrumentation` | nodeIndexonlyscan.c:877 | lib.rs:1029 | MATCH | NULL guard, palloc+memcpy modeled as `offsetof(winstrument)+num_workers*sizeof` copy into mcx. |

## Findings

### F1 — DIVERGES: `INDEX_VAR` constant is wrong (silent corruption)
`lib.rs:59` defines `const INDEX_VAR: i32 = 65000;`. In PostgreSQL 18.3
`INDEX_VAR` is **`-3`** (`src/include/nodes/primnodes.h:244`); c2rust confirms
`pub const INDEX_VAR: c_int = -3` (nodeIndexonlyscan.rs:3103). `65000` is the
pre-12 value. This value is passed to
`exec_assign_scan_projection_info_with_varno(..., INDEX_VAR)` at lib.rs:711, so
projection Var-varno matching for the index-only scan targetlist uses the wrong
varno — a behavioral divergence on every index-only scan. This is exactly the
transcribed-table corruption the skill warns against.

### F2 — PARTIAL/DIVERGES: `ExecInitIndexOnlyScan` panics mid-body on its own logic
C:542 does `indexstate->ss.ps.plan = (Plan *) node;` — a trivial pointer
assignment that is this function's own logic. The port routes this through
`node_as_node` (lib.rs:1164) which is an unconditional `panic!`. Because the
`plan` link is never set, every reader of `node.ss.ps.plan`
(`IndexOnlyNext` direction, `scan_scanrelid`, `plan_parallel_aware`,
`plan_node_id`) operates on state the function itself failed to populate. Per
mirror-PG-and-panic, panicking on an unported *callee* is acceptable; this is
absent logic *inside* the function being audited, not a callee. The
`Node::IndexOnlyScan` enum variant already exists in `types-nodes`, so the
modeling gap is in how the init API receives the plan link, not a missing type.

### F3 — PARTIAL: parallel DSM/worker setup permanently disabled
`plan_node_id` (lib.rs:1107) returns `Err("…plan_node_id not modeled…")` for
every call. It is invoked by `ExecIndexOnlyScanInitializeDSM` (lib.rs:885) and
`ExecIndexOnlyScanInitializeWorker` (lib.rs:977) before any TOC work, so the
parallel scan-descriptor construction (`shm_toc_insert`, `index_beginscan_parallel`,
`index_rescan`) is unreachable. `plan_node_id` is a `Plan` field; absent it the
node logic cannot be completed. This is approximated/absent logic, not a callee
panic.

### F4 — MISSING: worker SharedInfo offset read dropped
`ExecIndexOnlyScanInitializeWorker` C:842-844 sets `ioss_SharedInfo` from
`OffsetToPointer(piscan, piscan->ps_offset_ins)` when instrumented. The port
(lib.rs:982-989) replaces this with `let _ = &piscan;` and a comment deferring
to "the DSM owner." The assignment is the node's own logic and is absent.

### F5 — DIVERGES: `ExecIndexOnlyScan` bypassed the generic `ExecReScan` dispatcher (found 2026-06-13)
C:345 does `ExecReScan((PlanState *) node)` in the runtime-key setup path —
the **generic** `ExecReScan` (execAmi.c:77), which before dispatching to the
node-specific `ExecReScanIndexOnlyScan` also runs `InstrEndLoop(instrument)`,
the full `chgParam` propagation to `initPlan`/`subPlan`/child trees
(`UpdateChangedParamSet` / `ExecReScanSetParamPlan`), and
`ReScanExprContext(ps_ExprContext)`. The port called
`ExecReScanIndexOnlyScan(node, estate)` **directly** (former lib.rs:357),
skipping all of that preamble — an observable divergence on any index-only scan
with instrumentation or changed parameters (the identical pattern exists at
nodeIndexscan.c:529, so it is not incidental). The crate even kept a dead
`execAmi::exec_re_scan::is_installed` reference to mark the import "live"
without ever calling it, which is the tell that the dispatch was dropped.

**Resolution (2026-06-13):** `ExecIndexOnlyScan` now takes the enclosing
`&mut PlanStateNode` (C: `PlanState *pstate`) and the runtime-key guard calls
`execAmi::exec_re_scan::call(pstate, estate)` — the generic dispatcher, owned
by execAmi, which calls back into this crate's `ExecReScanIndexOnlyScan` through
the node switch. The dead `is_installed` marker is removed. The
`exec_proc_node_trampoline` passes `pstate` straight through.
`cargo check --workspace` and `cargo test -p backend-executor-nodeIndexonlyscan`
pass.

## Seam audit

Owned seam crate (by C-source coverage): `backend-executor-nodeIndexonlyscan-seams`.
- Declares 5 parallel entry-point seams (estimate / initialize_dsm /
  reinitialize_dsm / initialize_worker / retrieve_instrumentation), all over
  execParallel's opaque `PlanStateHandle`/`ParallelContextHandle`.
- All 5 are installed by `init_seams()` (lib.rs:1210), which contains only
  `set()` calls, and `seams-init::init_all()` calls `init_seams()`
  (seams-init/src/lib.rs:27). No uninstalled or orphan declarations.
- The installed bridges (`bridge_estimate` etc.) `panic!` at the
  PlanStateHandle→IndexOnlyScanState resolution frontier. That resolution is
  genuinely owned by execParallel (a real unported callee), so the panic itself
  is acceptable under mirror-PG-and-panic — **no seam finding here.**

Outward seam calls (indexam, visibilitymap, bufmgr, predicate, execExpr,
execTuples, execUtils, parallel, indextuple, nodeIndexscan helpers) are thin
marshal+delegate and justified by real dependency direction. No branching/node
construction observed inside seam paths.

## Design-conformance findings (step 3b)

### D1 — Foreign execScan.c logic reproduced inside this crate
`ExecScan`, `ExecScanExtended`, and `ExecScanFetch` (lib.rs:359-514) reproduce
the full `execScan.c` driver — including the entire EvalPlanQual
replacement-tuple decision tree (scanrelid==0 ext-param path, relsubs_done,
relsubs_slot, relsubs_rowmark branches). These functions are **not** part of
this translation unit: `execScan.o` is a separate object in the executor
Makefile, and c2rust renders `ExecScan` as an extern declaration (only the call
site is in this TU). The skill's rule "a function whose body was replaced by a
seam call to somewhere else is MISSING — the logic must live in this crate"
applies in reverse: this crate must *not* own another unit's logic. A
`backend-executor-execScan-seams` crate already exists; the `ExecScan` call
should delegate to execScan's owner rather than carry a private copy of its EPQ
branching. This is substantive cross-unit logic (a branching decision tree),
not a thin shim.

## Resolution (re-port 2026-06-12)

- **F1 fixed:** `INDEX_VAR` is now `-3` (lib.rs), matching primnodes.h and
  c2rust (`pub const INDEX_VAR = -3`).
- **F2 fixed:** `ExecInitIndexOnlyScan` now takes `node: &Node` and `castNode`s
  it to `IndexOnlyScan` (mirroring nodeTableFuncscan/nodeSort), then sets
  `indexstate.ss.ps.plan = Some(node)` — the real plan link. The `node_as_node`
  panic helper is deleted; no mid-body panic.
- **F3 fixed:** `plan_node_id: i32` added to the trimmed `Plan`
  (types-nodes::nodeindexscan), threaded through `Plan::clone_in`. The port's
  `plan_node_id` helper now reads `plan_head().plan_node_id`, so the parallel
  DSM/worker TOC paths execute.
- **F4 fixed:** `ExecIndexOnlyScanInitializeWorker` sets
  `node.ioss_SharedInfo = Some(alloc_in(mcx, indexam::index_scan_resolve_shared_info(&piscan)?))`
  — the `OffsetToPointer(piscan, piscan->ps_offset_ins)` resolution is delegated
  to a new indexam seam (the DSM blob layout is the parallel owner's), and the
  assignment is in-crate.
- **D1 fixed:** the reproduced `ExecScan`/`ExecScanExtended`/`ExecScanFetch`
  driver is removed. `ExecIndexOnlyScan` now delegates to the execScan unit via
  the new `exec_scan_indexonly` seam (passing `IndexOnlyNext`/`IndexOnlyRecheck`),
  and `ExecReScanIndexOnlyScan` calls `exec_scan_rescan_ss`. The EPQ
  replacement-tuple decision tree now lives in its owning unit. The node's own
  EPQ guards in MarkPos/RestrPos (genuinely this node's logic) are retained.

Gate: `cargo check --workspace` and `cargo test --workspace` both pass.

## Independent re-audit (2026-06-13)

Re-derived all 14 functions from `nodeIndexonlyscan.c`, the c2rust rendering,
and the PG 18.3 headers (not from the prior audit's prose). Constants
re-verified against headers: `INDEX_VAR = -3` (primnodes.h:244, c2rust:3103),
`VISIBILITYMAP_ALL_VISIBLE = 0x01` (visibilitymapdefs.h:20),
`EXEC_FLAG_EXPLAIN_ONLY = 0x0001` (executor.h:65), `NAMEDATALEN = 64`
(pg_config_manual.h:29). F1–F4 + D1 confirmed resolved at the root (INDEX_VAR
constant, `&Node`+castNode plan link, real `plan_node_id` field threaded through
`clone_in`, in-crate `ioss_SharedInfo` assignment, execScan-seam delegation with
no reproduced `ExecScan` body). Seam audit re-confirmed: the one owned seam
crate (`backend-executor-nodeIndexonlyscan-seams`, 5 parallel entry points) has
every declaration installed by `init_seams()` (only `set()` calls), wired from
`seams-init::init_all()`; the bridges panic only at the execParallel
handle-resolution frontier (acceptable). New finding **F5** (above) fixed and
re-audited clean. All 14 functions now `MATCH`/`SEAMED`; zero outstanding seam
or design findings.
