# Audit: backend-executor-nodeForeignscan

- **Verdict:** **PASS**
- **Unit:** `backend-executor-nodeForeignscan`
- **C source:** `src/backend/executor/nodeForeignscan.c` (PostgreSQL 18.3) + the
  inlined `execScan.h` driver (`ExecScan`/`ExecScanExtended`/`ExecScanFetch`)
  and `execScan.c` (`ExecScanReScan`, called via the execScan seam).
- **Port crate:** `crates/backend-executor-nodeForeignscan`
- **Owned seam crate:** `crates/backend-executor-nodeForeignscan-seams`
- **Branch:** `fix/epqstate-canonical`
- **Date:** 2026-06-13
- **Model:** Claude Fable 5

## Method

Independent re-derivation from the C source and the PG 18.3 headers
(`primnodes.h`, `executor.h`, `plannodes.h`, `execnodes.h`, `execScan.h`),
cross-checked against `c2rust-runs/backend-executor-nodeForeignscan/src/
nodeForeignscan.rs`. Every C function (statics, public entry points, and the
inline `execScan.h` driver linked into `nodeForeignscan.o`) gets a row. The
port comments and the green build were not trusted. Constants and tags were
verified against the headers, not memory. Special attention was paid to the
canonical owned `EPQState` reconciliation in `ExecScanFetch`.

## Function inventory and verdicts

`nodeForeignscan.c` defines 15 functions (2 static + 13 external). The
`execScan.h` inline driver contributes 3 more functions that the C compiler
inlines into `nodeForeignscan.o`; they are reproduced in-crate (matching the
nodeIndexonlyscan / nodeTidrangescan precedent). `ExecScanReScan` belongs to the
separate `backend-executor-execScan` catalog unit (CATALOG line 243, `todo`);
it is correctly delegated via the execScan seam, not reproduced.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `ForeignNext` (static) | nodeForeignscan.c:41 | lib.rs:108 | MATCH | per-tuple context switch folded into FDW seam (documented); `operation != CMD_SELECT` → `debug_assert(es_epq_active.is_none())` + `IterateDirectModify`, else `IterateForeignScan`; `fsSystemCol && !TupIsNull` → tableoid stamp; returns scan slot when tuple present else None. `!TupIsNull(slot)` is the seam's bool return. |
| 2 | `ForeignRecheck` (static) | nodeForeignscan.c:78 | lib.rs:161 | MATCH | sets `ecxt_scantuple = slot`, `ResetExprContext`, `if (RecheckForeignScan && !RecheckForeignScan(...)) return false`, then `ExecQual(fdw_recheck_quals)`. `ExecQual(NULL)` → true mirrored exactly (C `ExecQual` returns true on NULL state). |
| 3 | `ExecForeignScan` (static) | nodeForeignscan.c:118 | lib.rs:222 | MATCH | EPQ-active && non-SELECT → return NULL; else `ExecScan(&ss, ForeignNext, ForeignRecheck)`. `castNode` reproduced in `exec_foreign_scan_node` (lib.rs:208). |
| 4 | `ExecInitForeignScan` | nodeForeignscan.c:142 | lib.rs:249 | MATCH | flag assert; makeNode; ExecProcNode set; ExecAssignExprContext; scanrelid>0 → open rel + GetFdwRoutineForRelation, else GetFdwRoutineByServerId; tlist/varno branch (INDEX_VAR vs scanrelid); scanopsfixed=false/scanopsset=true; result type + projection w/ varno; qual + fdw_recheck_quals init; async_capable computed (field trimmed, see D1); fdwroutine/fdw_state set; resultRelation>0 && !epq → result-rel lookup + `elog(ERROR,"result relation not initialized")`; outerPlan init; non-SELECT && !epq → BeginDirectModify else BeginForeignScan. All EPQ-active guards present. |
| 5 | `ExecEndForeignScan` | nodeForeignscan.c:297 | lib.rs:433 | MATCH | non-SELECT && !epq → EndDirectModify else EndForeignScan; outerPlan ExecEndNode. |
| 6 | `ExecReScanForeignScan` | nodeForeignscan.c:323 | lib.rs:471 | MATCH | EPQ-active && non-SELECT → return; ReScanForeignScan; `outerPlan != NULL && chgParam == NULL` → ExecReScan; ExecScanReScan via execScan seam. |
| 7 | `ExecForeignScanEstimate` | nodeForeignscan.c:356 | lib.rs:522 | MATCH | `if (EstimateDSMForeignScan)` → set pscan_len, shm_toc_estimate_chunk, shm_toc_estimate_keys(1). |
| 8 | `ExecForeignScanInitializeDSM` | nodeForeignscan.c:375 | lib.rs:542 | MATCH | `if (InitializeDSMForeignScan)` → allocate/init/insert folded into FDW seam (documented marshalling). |
| 9 | `ExecForeignScanReInitializeDSM` | nodeForeignscan.c:397 | lib.rs:565 | MATCH | `if (ReInitializeDSMForeignScan)` → toc lookup folded into seam. |
| 10 | `ExecForeignScanInitializeWorker` | nodeForeignscan.c:418 | lib.rs:584 | MATCH | `if (InitializeWorkerForeignScan)` → toc lookup folded into seam. |
| 11 | `ExecShutdownForeignScan` | nodeForeignscan.c:441 | lib.rs:600 | MATCH | `if (ShutdownForeignScan)` → call. |
| 12 | `ExecAsyncForeignScanRequest` | nodeForeignscan.c:456 | lib.rs:619 | MATCH | requestee node + fdwroutine resolved in seam (areq->requestee opaque); `Assert(ForeignAsyncRequest != NULL)` folded into seam (acceptable; requestee opaque). |
| 13 | `ExecAsyncForeignScanConfigureWait` | nodeForeignscan.c:472 | lib.rs:633 | MATCH | as above. |
| 14 | `ExecAsyncForeignScanNotify` | nodeForeignscan.c:488 | lib.rs:641 | MATCH | as above. |
| 15 | `ExecScan` (execScan.c, inline driver) | execScan.c:47 / execScan.h | lib.rs:842 | MATCH | reads epq_active/qual/projInfo presence, delegates to ExecScanExtended. |
| 16 | `ExecScanExtended` (execScan.h inline) | execScan.h:160 | lib.rs:757 | MATCH | no-qual/no-proj fast path; reset; loop: fetch → NULL-slot (proj→clear resultslot, else NULL); set ecxt_scantuple; qual check; proj→ExecProject else slot; reset+retry. `InstrCountFiltered1` is instrumentation-only (no control-flow effect), documented as deferred. |
| 17 | `ExecScanFetch` (execScan.h inline) | execScan.h:32 | lib.rs:656 | MATCH | CHECK_FOR_INTERRUPTS; EPQ decision tree — see EPQ section below. |
| — | `ExecScanReScan` (execScan.c) | execScan.c:108 | seam delegate (lib.rs:513) | SEAMED | Belongs to separate `backend-executor-execScan` unit; delegated via `execScan::exec_scan_rescan_ss`. The `relsubs_done = relsubs_blocked` / `fs_base_relids` reset logic is execScan.c's, not this unit's. |
| — | `ExecAssignScanProjectionInfoWithVarno` (execScan.c) | execScan.c:94 | seam delegate (lib.rs:355) | SEAMED | execScan.c-owned; delegated via execUtils seam. |

## EvalPlanQual / canonical `EPQState` audit (ExecScanFetch)

Verified read-by-read against `execScan.h:36-135` and the canonical
`EPQState` in `EStateData::es_epq_active: Option<PgBox<EPQState>>`
(`crates/types-nodes/src/execnodes.rs:90`). **No `EPQStateHandle` opacity
stand-in remains** — the former handle-based seams were removed (execMain-seams
lib.rs:9-16 documents the removal); all EPQ reads are direct field accesses on
the owned struct.

- `CHECK_FOR_INTERRUPTS()` → `tcop_postgres::check_for_interrupts::call()?` — MATCH.
- `epqstate != NULL` → `epq_active` bool from `es_epq_active.is_some()` — MATCH.
- `scanrelid = ((Scan *) node->ps.plan)->scanrelid` → `scan_scanrelid(node)` reads the owned `ForeignScan.scan.scanrelid` — MATCH.
- **scanrelid == 0 (pushed-down join):** `bms_is_member(epqstate->epqParam, node->ps.plan->extParam)` → `epq_param(estate)` (reads `EPQState.epqParam`) + `bms_is_member::call(epq_param, plan_head().extParam)` on the owned plan's `extParam` Bitmapset — MATCH. On member: slot = `ss_ScanTupleSlot`; `!recheckMtd → ExecClearTuple`; `return slot` — MATCH (note C returns the slot even after clearing; port returns `Ok(Some(slot))`, faithful).
- **relsubs_done[scanrelid-1]** → `epq_relsubs_done(estate, scanrelid-1)` reads `EPQState.relsubs_done` (`Option<PgVec<bool>>`); on true → ExecClearTuple + return None (C `return ExecClearTuple(slot)`, an empty slot ≡ None) — MATCH. Index `scanrelid-1` safe: this is an `else if` reached only when scanrelid≥1.
- **relsubs_slot[scanrelid-1] != NULL** → `epq_relsubs_slot` reads `relsubs_slot: Option<PgVec<Option<SlotId>>>` (`Some(slot)` = non-NULL C entry); `Assert(relsubs_rowmark[...] == NULL)` → `debug_assert(!epq_relsubs_rowmark_present)`; `relsubs_done[...] = true` → `epq_set_relsubs_done(...,true)`; `TupIsNull → return NULL`; `!recheckMtd → ExecClearTuple + return NULL`; else return slot — MATCH.
- **relsubs_rowmark[scanrelid-1] != NULL** → `epq_relsubs_rowmark_present` reads `relsubs_rowmark: Option<PgVec<bool>>` (`true` = non-NULL entry); slot = `ss_ScanTupleSlot`; `relsubs_done[...] = true`; `EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)` → execMain seam (owner is execMain.c; correct outward delegation); `TupIsNull → NULL`; `!recheckMtd → ExecClearTuple + NULL`; else slot — MATCH.
- Fall-through to `(*accessMtd)(node)` → `access_mtd(node, estate)` — MATCH.

The C uses `epqstate->relsubs_slot[scanrelid-1]` directly; the owned reads pass
`scanrelid - 1` (u32) into helpers that index the `PgVec`. The `else if` chain
ordering (done → slot → rowmark) matches the C exactly, including the scanrelid==0
short-circuit that *falls through* to accessMtd when not an EPQ descendant.

## Constants / tags / flags (verified against headers)

- `INDEX_VAR = -3` (lib.rs:872) — matches `primnodes.h:244` (`INNER_VAR -1`, `OUTER_VAR -2`, `INDEX_VAR -3`, `ROWID_VAR -4`). ✓
- `EXEC_FLAG_BACKWARD = 0x0008`, `EXEC_FLAG_MARK = 0x0010` — matches `executor.h:68-69`; used in the unsupported-flag `debug_assert`. ✓
- `CMD_SELECT` discriminant — `CmdType::CMD_SELECT` compared by enum equality (not numeric); the non-SELECT direct-modify branches match C `!= CMD_SELECT`. ✓
- `T_ForeignScan = 354`, `T_ForeignScanState = 418` — node dispatch via the typed `Node`/`PlanStateNode` enums (no transcribed numeric table). ✓
- `tableoid` is the only stamped system column (`ForeignNext`), matching C. ✓

## Seam and wiring audit

**Owned seam crate:** `backend-executor-nodeForeignscan-seams` declares 4
parallel-DSM inward seams (`exec_foreignscan_estimate`,
`exec_foreignscan_initialize_dsm`, `exec_foreignscan_reinitialize_dsm`,
`exec_foreignscan_initialize_worker`). **All 4 are installed** by this crate's
`init_seams()` (lib.rs:78-96), each a mirror-PG-and-panic because the parallel
DSM owner (execParallel) cannot yet hand over the owned `ForeignScanState` (the
opaque `PlanStateHandle` is not resolvable here). No declaration is left
uninstalled; the installer contains nothing but `set()` calls. `seams-init::
init_all()` calls `backend_executor_nodeForeignscan::init_seams()`
(crates/seams-init/src/lib.rs:31). ✓

The unit does **not** own `backend-foreign-foreign-seams` (owner: `foreign/
foreign.c`), `backend-executor-execScan-seams` (owner: `execScan.c`), nor the
execExpr/execUtils/execTuples/execMain/execProcnode/execAmi/shm_toc/
tcop-postgres seam crates — these are outward seams to other units' C files and
are correctly **not** installed here. Their `::set` installers belong to the
respective owners (none installed yet → calls panic; acceptable deferred
callees, not absent logic).

**Outward seams are thin marshal + delegate.** Each FDW-callback seam call is
argument conversion → one call → result conversion. The FDW callbacks are a
genuine external-provider boundary (extension-installed function pointers); the
presence checks (`if (fdwroutine->X)`) are read in-crate from the owned
`FdwRoutine` presence-flag struct, and only the *invocation* crosses the seam —
faithful to the C. No branching, node construction, or computation lives in any
seam path. `stamp_scan_slot_tableoid` (the `slot->tts_tableOid =
RelationGetRelid(...)` write) is reached through the foreign seam because both
the relcache relid read and the slot-payload write are FDW/relcache/slot-owned
and the slot payload model has not yet landed — acceptable.

## Design-conformance (docs/types.md)

- **No invented opacity (rules 6-7).** `EPQStateHandle` stand-in is fully gone;
  the EPQ state is the real owned `EPQState`. The remaining `Opaque` fields
  (`fdw_state`, `ParallelContext.toc`, `AsyncRequest.requestee`) are genuinely
  C-opaque (extension/DSM-owned `void *`), inherited not introduced. ✓
- **Mcx + PgResult on allocation/ereport.** `ExecInitForeignScan` takes `mcx =
  estate.es_query_cxt`, all allocating seam calls thread `mcx`/`estate` and
  return `PgResult`; the `elog(ERROR, "result relation not initialized")` maps
  to `PgError::error(...)` returned as `Err`. ✓
- **No shared statics for per-backend globals; no ambient-global seams.** The
  node state is threaded as `&mut ForeignScanState` + `&mut EStateData`; no
  thread_local or `static mut`. ✓
- **No locks held across `?`, no registry-shaped side tables.** None present. ✓

### D1 — `async_capable` computed-but-not-stored (deferred, not a divergence)

`ExecInitForeignScan` (lib.rs:381) computes `plan.scan.plan.async_capable &&
es_epq_active.is_none()` into `_async_capable` and discards it because
`PlanStateData` does not yet carry `async_capable` (trimmed; verified absent in
execnodes.rs). The C predicate is reproduced exactly. Per types.md rule 3
(field lands with its first reader) and given there is no in-tree consumer of
`ps.async_capable` yet, this is an acceptable deferral — the computed expression
is faithful and ledgered inline. **Not a FAIL.**

## Final verdict

**PASS.** All 17 in-unit functions (2 statics, 13 entry points, 3 inline
execScan driver functions) verify **MATCH**; the 2 execScan.c-owned helpers are
correctly **SEAMED** to their owning unit. The canonical `EPQState`
reconciliation in `ExecScanFetch` is faithful read-by-read, and no
`EPQStateHandle` opacity stand-in remains. The owned seam crate's 4 declarations
are all installed by `init_seams()`, which `init_all()` calls. Constants
(`INDEX_VAR=-3`, `EXEC_FLAG_*`) and node tags verify against the headers. No
design-conformance violations; the single `async_capable` deferral is ledgered
and behavior-preserving. Zero `MISSING`/`PARTIAL`/`DIVERGES`.
