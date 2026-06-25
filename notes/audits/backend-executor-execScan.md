# Audit: backend-executor-execScan

C source: `src/backend/executor/execScan.c` + the inline driver header
`src/include/executor/execScan.h`.
Port crate: `crates/backend-executor-execScan` (+ owned seam crate
`crates/backend-executor-execScan-seams`).

Independent re-derivation from the C and the c2rust rendering
(`c2rust-runs/backend-executor-execScan/src/execScan.rs`).

## Function inventory

| # | C function | C location | Kind | Port location | Verdict |
|---|-----------|-----------|------|---------------|---------|
| 1 | `ExecScan` | execScan.c | public | `exec_scan_core` (+ `exec_scan_tablefunc`/`exec_scan_indexonly` seam entries) | MATCH |
| 2 | `ExecAssignScanProjectionInfo` | execScan.c | public | `exec_assign_scan_projection_info` | MATCH |
| 3 | `ExecAssignScanProjectionInfoWithVarno` | execScan.c | public | delegate body homed at `execUtils-seams::exec_assign_scan_projection_info_with_varno` | MATCH (delegate) |
| 4 | `ExecScanReScan` | execScan.c | public | `exec_scan_rescan_ss` (+ `exec_scan_rescan_tablefunc`) | MATCH |
| 5 | `ExecScanFetch` | execScan.h | inline | `exec_scan_fetch` | MATCH |
| 6 | `ExecScanExtended` | execScan.h | inline | `exec_scan_extended` | MATCH |
| 7 | `TupIsNull` | tuptable.h macro (used here) | macro | `tup_is_null` | MATCH |
| 8 | `InstrCountFiltered1` | executor.h macro | macro | `instr_count_filtered1` | MATCH |
| 9 | `ResetExprContext` | executor.h macro | macro | `reset_expr_context` | MATCH |

## Per-function notes

1. **ExecScan / ExecScanExtended / ExecScanFetch.** The generic driver core is
   generic over a `ScanNode` trait (an `ss()` accessor onto the node's embedded
   `ScanStateData` head), so the per-node access/recheck callbacks see the full
   concrete node while the qual/projection loop runs over the shared head. EPQ
   state is read from `estate.es_epq_active` (the C `node->ps.state->es_epq_active`)
   — same model as the merged nodeForeignscan / nodeSeqscan reproductions.

   - `ExecScanExtended`: the `!qual && !projInfo` fast path resets the per-tuple
     context and returns the raw `ExecScanFetch` result; the main loop resets,
     fetches, and on `TupIsNull(slot)` returns the cleared projection result
     slot (projecting) or the slot (not projecting). Verified the `TupIsNull`
     test is applied to the fetched slot in `exec_scan_extended` (a fetched
     `Some(empty-slot)` — the relsubs_done / failed-recheck branches return the
     cleared scan slot — is correctly treated as end-of-scan, not fed to qual).
     `econtext->ecxt_scantuple = slot` set before qual; `ExecQual` only called
     when `qual != NULL`; `InstrCountFiltered1(node,1)` on qual failure;
     `ResetExprContext` after a failed tuple. All branches present.
   - `ExecScanFetch`: `CHECK_FOR_INTERRUPTS()` first; the EPQ tree mirrors the
     header exactly — `scanrelid==0` → `bms_is_member(epqParam, plan->extParam)`
     → recheck-into-scan-slot; `relsubs_done` → `ExecClearTuple(scanslot)`
     (returned as the cleared slot, TupIsNull-caught); `relsubs_slot != NULL`
     → `Assert(relsubs_rowmark==NULL)`, mark done, `TupIsNull`→None, recheck the
     PASSED replacement slot, clear on failed recheck; `relsubs_rowmark != NULL`
     → mark done, `EvalPlanQualFetchRowMark`, `TupIsNull`→None, recheck. The
     default path runs the access method. Slot identities verified against the
     header (scan slot vs replacement slot per branch).

2. **ExecAssignScanProjectionInfo.** C: `ExecConditionalAssignProjectionInfo(
   &node->ps, ss_ScanTupleSlot->tts_tupleDescriptor, scan->scanrelid)`. Port
   reads `scanrelid` off the owned plan node and calls the execUtils seam
   `exec_assign_scan_projection_info_with_varno` (which itself reads the scan
   slot tupdesc and runs `ExecConditionalAssignProjectionInfo`). SEAMED to
   execUtils.c — `ExecConditionalAssignProjectionInfo` lives there (genuine
   other-TU dependency); thin marshal + delegate.

3. **ExecAssignScanProjectionInfoWithVarno.** C body is the single
   `ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, varno)` call. That
   one-line delegate is the `execUtils-seams::exec_assign_scan_projection_info_with_varno`
   declaration consumed at the call sites; there is no consumer of a standalone
   execScan WithVarno entry, so no dead pub wrapper is added. The logic (the
   conditional-assign call) is faithfully present.

4. **ExecScanReScan.** `ExecClearTuple(ss_ScanTupleSlot)`; if `es_epq_active`,
   `scanrelid>0` → `relsubs_done[scanrelid-1] = relsubs_blocked[scanrelid-1]`.
   The `scanrelid==0` join-pushdown branch (FDW/CustomScan, iterating
   `fs_base_relids`/`custom_relids` via `bms_next_member`) is **mirror-PG-and-panic**:
   those plan-node fields and the bms loop are not modeled in `types-nodes` yet,
   and the C `elog(ERROR, "unexpected scan node")` default is preserved as a
   panic. This is an unported-callee/unmodeled-type boundary, not absent
   own-logic.

5-9. Macros: `TupIsNull` = empty-slot test over the EState slot pool;
   `InstrCountFiltered1` bumps `instrument->nfiltered1` when present (f64, matches
   the C `double`); `ResetExprContext` resets the per-node ExprContext's
   `ecxt_per_tuple_memory` (no-op when the context is unset, matching the
   owned-value model used across the merged sibling scan nodes). MATCH.

## Seam audit

Owned inward seam crate: `backend-executor-execScan-seams` (covers execScan.c).
All five declarations are installed by `init_seams()` and nowhere else:

- `exec_scan` → `exec_scan_tablefunc`
- `exec_scan_rescan` → `exec_scan_rescan_tablefunc`
- `exec_scan_indexonly` → `exec_scan_indexonly`
- `exec_assign_scan_projection_info` → `exec_assign_scan_projection_info`
- `exec_scan_rescan_ss` → `exec_scan_rescan_ss`

`init_seams()` contains only `set()` calls. `seams-init::init_all()` calls
`backend_executor_execScan::init_seams()`. Both recurrence guards pass
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`).

Three of the inward decls (`exec_scan`, `exec_scan_rescan`, `exec_scan_indexonly`)
had elided independent node/estate lifetimes; tightened to a shared `'mcx`
(a refinement satisfied by every existing same-lifetime caller; full workspace
check confirms nodeTableFuncscan / nodeIndexonlyscan / nodeSeqscan /
nodeForeignscan still build).

Outward seams (all genuine cross-TU dependencies, thin marshal + delegate, panic
until the owner lands): `execExpr` (`exec_qual`, `exec_project`), `execTuples`
(`exec_clear_tuple`), `execUtils` (`exec_assign_scan_projection_info_with_varno`),
`execMain` (`eval_plan_qual_fetch_row_mark`), `nodes-core` (`bms_is_member`),
`tcop-postgres` (`check_for_interrupts`). No branching/computation in any seam
path beyond argument selection inherent to the C call site.

## Design conformance

- No invented opacity / stand-in type aliases (`type _ = u32` grep clean).
- No infallible allocation on a pallocing path (`format!`/`clone`/`vec!`/`String::new`
  grep clean — the driver allocates nothing).
- No shared statics / atomics / OnceCell (grep clean); EPQ state threaded via the
  EState value, not an ambient global.
- No zero-arg foreign-global getter seams.
- No locks held across `?`.
- No `todo!`/`unimplemented!`/`unreachable!`. Panics are: (a) invariant guards on
  malformed plan/node state (the C would NULL-deref), and (b) the documented
  mirror-PG-and-panic for the unmodeled FDW/CustomScan join-pushdown EPQ-rescan
  branch — not own-logic stubs.
- No unledgered divergence markers.

## Verdict: PASS

Every function MATCH (or the WithVarno delegate / cross-TU SEAMED per step 3).
Zero seam findings. One mirror-PG-and-panic boundary (ExecScanReScan
scanrelid==0 join-pushdown), which is an unmodeled-type frontier, not missing
own-logic.
