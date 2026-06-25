# Audit: backend-executor-nodeWorktablescan

Unit: `backend-executor-nodeWorktablescan`
Branch: `port/backend-executor-nodeWorktablescan`
C source (`c_sources`): `src/backend/executor/nodeWorktablescan.c` (201 lines)
c2rust: `c2rust-runs/backend-executor-nodeWorktablescan/src/nodeWorktablescan.rs`
Port: `crates/backend-executor-nodeWorktablescan/src/lib.rs`
Seam crate: `crates/backend-executor-nodeWorktablescan-seams/src/lib.rs`

Verdict: **PASS**

## Method

Re-derived the function inventory independently from the C source and
cross-checked against the c2rust rendering. `nodeWorktablescan.c` defines
exactly five functions; the C TU is also linked against `execScan.c`'s
`ExecScan` and the inline `execScan.h` `ExecScanExtended`/`ExecScanFetch`
helpers (the access/recheck-method driver). `execScan.c` is a *separate*
unit (CATALOG status `todo`, unported). The port reproduces the inline
header driver (`ExecScan`/`ExecScanExtended`/`ExecScanFetch` +
`InstrCountFiltered1`) in-crate as private functions and reaches the
non-inline / unported leaf operations through this crate's seam crate
(panicking until the owners — execScan/execUtils/execTuples/tuplestore/
nodeRecursiveunion — land). This is the established mirror-PG-and-seam model
(cf. nodeTidrangescan).

## Function inventory (nodeWorktablescan.c)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `WorkTableScanNext` (static) | nodeWorktablescan.c:30 | lib.rs:54 | MATCH / SEAMED | Fetches next work-table tuple into scan slot. The whole body is `tuplestorestate = node->rustate->working_table; slot = node->ss.ss_ScanTupleSlot; tuplestore_gettupleslot(ts, true, false, slot); return slot;` — delegated to `seam::tuplestore_gettupleslot` (owns the RecursiveUnion working_table / tuplestore.c, unported). Forward+no-copy semantics captured in the seam doc; `Ok(true)`=tuple loaded, `Ok(false)`=exhausted. The forward-only `Assert(ScanDirectionIsForward(...))` is a release-compiled-out assert; correctly elided. |
| `WorkTableScanRecheck` (static) | nodeWorktablescan.c:66 | lib.rs:73 | MATCH | Always `Ok(true)` ("nothing to check"). |
| `ExecWorkTableScan` (static) | nodeWorktablescan.c:81 | lib.rs:280 | MATCH / SEAMED | First-call lazy resolution: `if node->rustate==NULL` → resolve rustate from `es_param_exec_vals[plan->wtParam]` (`seam::resolve_rustate`, whose doc carries the `Assert(param->execPlan==NULL)`/`Assert(!param->isnull)`/`Assert(node->rustate)`), `ExecAssignScanType(&ss, ExecGetResultType(&rustate->ps))` (`seam::exec_assign_scan_type_from_rustate`), `ExecAssignScanProjectionInfo` (`seam::exec_assign_scan_projection_info`); then `ExecScan(&ss, WorkTableScanNext, WorkTableScanRecheck)`. Branch predicate (`rustate.is_none()`) and ordering match exactly. |
| `ExecInitWorkTableScan` | nodeWorktablescan.c:130 | lib.rs:321 | MATCH | `makeNode` → owned `WorkTableScanStateData::default()`. Plan/EState links + `ExecProcNode` install + `rustate=NULL` via `seam::init_plan_state_links` (factory owns those refs). Then `ExecAssignExprContext`, `ExecInitResultTypeTL`, `resultopsset=true`/`resultopsfixed=false` (set inline — MATCH), `ExecInitScanTupleSlot(estate,&ss,NULL,&TTSOpsMinimalTuple)`, `ExecInitQual(node->scan.plan.qual, ...)`. Projection deferred (comment preserved). Order is identical to C. Unsupported-flag and no-children asserts mapped to `debug_assert!`. |
| `ExecReScanWorkTableScan` | nodeWorktablescan.c:191 | lib.rs:375 | MATCH | `if ps_ResultTupleSlot: ExecClearTuple(it)` (`seam::exec_clear_result_tuple_slot`), `ExecScanReScan(&ss)` (`seam::exec_scan_rescan` — execScan.c, unported), `if rustate: tuplestore_rescan(rustate->working_table)` (`seam::tuplestore_rescan`). Predicates (`is_some()`) and order match. |

## execScan driver reproduced in-crate (execScan.h / execScan.c)

These are not nodeWorktablescan.c functions; they belong to the unported
`execScan` unit but are inline in `execScan.h` (or linked) so the C TU
contains them. The port reproduces the inlinable logic faithfully:

- `ExecScan` (execScan.c:46 → lib.rs:240): reads `es_epq_active`
  (`seam::es_epq_active_present`), `qual` (`ps.qual.is_some()`),
  `ps_ProjInfo` (`ps.ps_ProjInfo.is_some()`), delegates to
  `ExecScanExtended`. MATCH.
- `ExecScanExtended` (execScan.h:170 → lib.rs:175): the `!qual && !projInfo`
  fast path (ResetExprContext + ExecScanFetch), the reset, and the
  `for(;;)` loop with the TupIsNull→(projInfo?ClearTuple(resultslot):slot)
  return, `ecxt_scantuple=slot`, `qual==NULL||ExecQual`,
  `projInfo?ExecProject:slot`, else `InstrCountFiltered1(node,1)` +
  ResetExprContext. Branch-for-branch identical. MATCH.
- `ExecScanFetch` (execScan.h:31 → lib.rs:102): CHECK_FOR_INTERRUPTS, then
  the EPQ ladder: `scanrelid==0` → bms_is_member(epqParam, extParam)
  recheck-or-clear; `relsubs_done` → ClearTuple+empty; `relsubs_slot!=NULL`
  → load slot, set done, TupIsNull→empty, recheck; `relsubs_rowmark!=NULL`
  → set done, EvalPlanQualFetchRowMark, TupIsNull→empty, recheck; fallthrough
  → accessMtd. Every branch, early return, and the fallthrough (scanrelid==0
  non-member → access method) match. The C `Assert(relsubs_rowmark==NULL)` in
  the relsubs_slot branch is a compiled-out assert; correctly elided. MATCH.
- `InstrCountFiltered1` (execnodes.h:1259 → lib.rs:263): `if instrument:
  instrument->nfiltered1 += delta`. MATCH.

`TupIsNull` is modeled by `scan_tuple_is_null` (lib.rs:94): slot absent OR
`tts_flags & TTS_FLAG_EMPTY`. Matches `TTS_EMPTY` semantics.

## Constants verified against headers

- `EXEC_FLAG_BACKWARD = 0x0008`, `EXEC_FLAG_MARK = 0x0010` (executor.h:68-69) ok
- `TTS_FLAG_EMPTY = (1 << 1)` (tuptable.h:95) ok (types-nodes/executor.rs:28)
- `TTSOpsMinimalTuple` used for the scan slot ok (matches C)
- `InstrCountFiltered1` macro guards on `instrument` non-NULL ok

## Seam audit

All 24 declarations in `backend-executor-nodeWorktablescan-seams` are
**outward** calls into subsystems below the executor-node layer
(execUtils/execScan/execTuples init helpers, the execScan.c driver leaf ops
+ EvalPlanQual machinery, the RecursiveUnion work-table tuplestore, and the
rustate resolution). Each is a thin declaration that panics until its owner
installs it; none contains branching/computation/node-construction (they are
pure marshal+delegate at the call sites). Each maps to a genuine unported
dependency (execScan/execUtils/execTuples/tuplestore/nodeRecursiveunion).

This unit owns **no inward-facing seam** — nothing else calls *into*
nodeWorktablescan via a seam. Therefore `init_seams()` is correctly empty
and is not wired into `seams-init::init_all()` (the recurrence_guard skips
zero-install crates; both guard tests pass). This matches the established
nodeTidrangescan / functioncmds pattern (the `*-seams` crate holds only
outward declarations installed by their real owners when they land).

No own-logic stubs: `grep` for `todo!`/`unimplemented!`/`unreachable!()` over
both crates returns nothing. No deferred-error escape hatches.

## Design conformance

- Owned-tree model: C `PlanState.state` back-pointer replaced by threading
  `&mut EStateData` explicitly; `ExecProcNode` "return slot" convention →
  `PgResult<bool>`. Consistent across all entry points. ok
- No invented opacity: `RecursiveUnionStateData`/`Tuplestorestate` are real
  mirror structs (owned by the not-yet-ported nodeRecursiveunion); the scan
  only reads `working_table`. No stand-in handles. ok
- Seams return `PgResult` (mirror the C failure surface — ExecQual/ExecProject/
  tuplestore can ereport). ok
- No allocating own-logic without Mcx; allocation/slot work lives behind seams
  in the owner subsystems. ok

## Gates

- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-access-common-printtup).
- `cargo test -p backend-executor-nodeWorktablescan`: 8 passed, 0 failed.
- `cargo test -p seams-init`: 2 passed (both recurrence_guard checks —
  init_all wiring + every-declared-seam-installed-by-owner).

## Conclusion

All five `nodeWorktablescan.c` functions are present with exact control flow,
error-surface, and constant parity (logic delegated only across seams to
genuinely-unported owners). The in-crate execScan driver reproduction matches
`execScan.h`/`execScan.c` branch-for-branch. No MISSING/PARTIAL/DIVERGES, no
own-logic stubs, no design-rule violations. **PASS.**
