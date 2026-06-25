# Audit: backend-executor-nodeSubqueryscan

Unit: `backend-executor-nodeSubqueryscan` (+ `types-nodes` additions:
`SubqueryScan`/`SubqueryScanStatus` plan node, `T_SubqueryScan`,
`SubqueryScanState` exec state, `Node::SubqueryScan` /
`PlanStateNode::SubqueryScan` dispatch variants; + `exec_scan_subquery` seam in
`execScan-seams`)
C source: `src/backend/executor/nodeSubqueryscan.c`
c2rust: `c2rust-runs/backend-executor-nodeSubqueryscan/src/nodeSubqueryscan.rs`
Port: `crates/backend-executor-nodeSubqueryscan/src/lib.rs`
Date: 2026-06-13
Model: Claude Opus 4.8 (1M context)
Verdict: PASS

## Function inventory and verdicts

nodeSubqueryscan.c defines exactly 6 functions (confirmed against the C and the
c2rust rendering, whose extra `ExecProcNode`/`newNode` entries are inlined
header helpers, not file functions):

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `SubqueryNext` | L45 | `lib.rs` `SubqueryNext` | MATCH | `slot = ExecProcNode(node->subplan); return slot`. Dispatch via `execProcnode::exec_proc_node` seam (real dep cycle: dispatch crate calls back into nodes). Returns subplan's own slot id directly, no `ExecCopySlot` (faithful). |
| 2 | `SubqueryRecheck` | L67 | `lib.rs` `SubqueryRecheck` | MATCH | `return true` unconditionally. |
| 3 | `ExecSubqueryScan` | L82 | `lib.rs` `ExecSubqueryScan` | MATCH | `castNode` is the enum match in `exec_subquery_scan_node`; body is `ExecScan(&ss, SubqueryNext, SubqueryRecheck)` via `execScan::exec_scan_subquery` seam (driver owned by execScan.c, cycle). |
| 4 | `ExecInitSubqueryScan` | L96 | `lib.rs` `ExecInitSubqueryScan` | MATCH | See detailed check below. |
| 5 | `ExecEndSubqueryScan` | L167 | `lib.rs` `ExecEndSubqueryScan` | MATCH | `ExecEndNode(node->subplan)` via `execProcnode::exec_end_node`. |
| 6 | `ExecReScanSubqueryScan` | L182 | `lib.rs` `ExecReScanSubqueryScan` | MATCH | See detailed check below. |

## Detailed re-derivation (spot-checks)

### ExecInitSubqueryScan (L96–166)

C sequence vs port, line for line:
- `Assert(!(eflags & EXEC_FLAG_MARK))` → `debug_assert!(eflags & EXEC_FLAG_MARK == 0)`. `EXEC_FLAG_MARK = 0x0010` verified against `executor/executor.h` (0x0010). MATCH.
- `Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL)` → `debug_assert!(node.scan.plan.lefttree.is_none()); ...righttree.is_none()`. `outerPlan`/`innerPlan` = `plan.lefttree`/`plan.righttree`. MATCH.
- `makeNode(SubqueryScanState)` + `ss.ps.plan = (Plan*)node` + `ss.ps.state = estate` + `ss.ps.ExecProcNode = ExecSubqueryScan` → `alloc_in(...default())` (zeroes), `ps.plan = Some(plan_node)`, threaded `estate`, `ps.ExecProcNode = Some(exec_subquery_scan_node)`. NodeTag is the `PlanStateNode::SubqueryScan` enum variant (= `T_SubqueryScanState=411`, verified vs `nodetags.h`). MATCH.
- `ExecAssignExprContext(estate, &ss.ps)` → `execUtils::exec_assign_expr_context`. SEAMED/MATCH.
- `subplan = ExecInitNode(node->subplan, estate, eflags)` → `execProcnode::exec_init_node(mcx, node.subplan.as_deref(), estate, eflags)`. MATCH.
- `ExecInitScanTupleSlot(estate, &ss, ExecGetResultType(subplan), ExecGetResultSlotOps(subplan, NULL))` → `exec_get_result_type` (borrow, cloned into mcx — owned model needs an owned desc), `exec_get_result_slot_ops_isfixed` (ignoring isfixed = C's NULL out-arg), then `exec_init_scan_tuple_slot`. MATCH (the two ExecGetResultSlotOps calls — one here, one below — are both preserved, matching the C's two calls).
- `ss.ps.scanopsset = true; scanops = ExecGetResultSlotOps(subplan, &scanopsfixed); resultopsset = true; resultops = scanops; resultopsfixed = scanopsfixed` → identical field assignments; second `exec_get_result_slot_ops_isfixed` call captures `(scanops, scanopsfixed)`. MATCH.
- `ExecInitResultTypeTL(&ss.ps); ExecAssignScanProjectionInfo(&ss)` → `execTuples::exec_init_result_type_tl`, `execScan::exec_assign_scan_projection_info`. MATCH.
- `ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState*)subquerystate)` → `execExpr::exec_init_qual(node.scan.plan.qual.as_deref(), &mut ss.ps, estate)`. The empty-qual→None (C NULL ExprState) is handled inside the execExpr seam (signature documents it), so no in-crate short-circuit is needed. MATCH.

### ExecReScanSubqueryScan (L182–204)

- `ExecScanReScan(&node->ss)` → `execScan::exec_scan_rescan_ss(&mut node.ss, estate)`. MATCH.
- `if (node->ss.ps.chgParam != NULL) UpdateChangedParamSet(node->subplan, node->ss.ps.chgParam)` → `chgParam.take()`, if Some → `execUtils::update_changed_param_set(mcx, subplan.ps_head_mut(), chg_param)`, then restored (C never clears it). The take/restore is a borrow-checker accommodation, behavior-identical. MATCH.
- `if (node->subplan->chgParam == NULL) ExecReScan(node->subplan)` → `if subplan.ps_head().chgParam.is_none() { execAmi::exec_re_scan(subplan, estate) }`. `node->subplan->chgParam` is the child PlanState head's chgParam = `ps_head().chgParam`. MATCH.

## Seam audit

Owned seam crates: enumerating `crates/X-seams` where X covers nodeSubqueryscan.c
→ none. There is no `backend-executor-nodeSubqueryscan-seams` crate. The crate
owns no inward seam (no other crate calls into it across a cycle: the executor
dispatch crate depends on it directly, and nodeSubqueryscan.c exposes no
parallel-scan entry points — unlike nodeSeqscan). `init_seams()` is therefore
empty, and is wired into `seams-init::init_all()` (recurrence_guard both tests
pass). This matches the nodeResult precedent (empty installer, no owned -seams).

Outward seam calls (each a real dependency cycle — the node layer is below the
dispatch/driver/setup subsystems that call back into per-node crates):
- `execProcnode`: `exec_proc_node`, `exec_init_node`, `exec_end_node`
- `execScan`: `exec_scan_subquery` (added, subquery-specialized like the
  TableFunc/IndexOnly entries), `exec_scan_rescan_ss`, `exec_assign_scan_projection_info`
- `execAmi`: `exec_re_scan`
- `execUtils`: `exec_assign_expr_context`, `exec_get_result_slot_ops_isfixed`, `update_changed_param_set`
- `execTuples`: `exec_get_result_type`, `exec_init_scan_tuple_slot`, `exec_init_result_type_tl`
- `execExpr`: `exec_init_qual`

All seam calls are thin marshal+delegate: no branching/construction/computation
lives on a seam path. The only in-crate computation is the C field assignments
and the `exec_subquery_scan_node` `castNode` dispatch (which is this node's own
ExecProcNode callback, correctly in-crate).

`exec_scan_subquery` is declared in `execScan-seams` (owner = execScan.c), so it
is the execScan owner's to install, not this crate's — correctly NOT in this
crate's init_seams.

## Design conformance

- Mcx + PgResult: every allocating path (alloc_in for the state node, clone_in
  for the result descriptor, all seam calls) is fallible PgResult; OOM
  surfaces. PASS.
- No invented opacity: SubqueryScan / SubqueryScanState are real structs
  mirroring the C field-for-field (scan, subplan, scanstatus / ss, subplan); the
  child link is the real `PlanStateNode` whole-node, not a handle. PASS.
- No shared statics / ambient-global seams / zero-arg getter seams. PASS.
- No locks held across `?`. PASS.
- No todo!/unimplemented!/registry side-table/unledgered divergence marker.
  The `expect`/`panic!` sites are invariant assertions mirroring C's
  unconditional `node->subplan` dereference and `castNode` assertion (a NULL
  subplan crashes in C too) — faithful, not error-path stand-ins. PASS.

## Constants verified vs headers

- `EXEC_FLAG_MARK = 0x0010` (executor/executor.h). OK.
- `T_SubqueryScan = 347`, `T_SubqueryScanState = 411` (backend/nodes/nodetags.h). OK.
- `SubqueryScanStatus`: UNKNOWN=0, TRIVIAL=1, NONTRIVIAL=2 (plannodes.h). OK.

## Verdict

PASS — all 6 functions MATCH, zero seam findings, zero design-conformance
findings. `cargo check --workspace` green; `seams-init` recurrence_guard tests
pass.

## Independent re-audit (2026-06-13)

Re-derived from scratch against C (`nodeSubqueryscan.c`), c2rust
(`c2rust-runs/backend-executor-nodeSubqueryscan/src/nodeSubqueryscan.rs`
lines 1608-1709), and the port (`crates/.../src/lib.rs`):

- SubqueryNext / SubqueryRecheck / ExecSubqueryScan (+ the
  `exec_subquery_scan_node` castNode callback) / ExecInitSubqueryScan /
  ExecEndSubqueryScan / ExecReScanSubqueryScan — all present, control flow and
  field assignments match the C line-for-line.
- ExecInitSubqueryScan: the two `ExecGetResultSlotOps` calls (NULL isfixed for
  ExecInitScanTupleSlot, then `&scanopsfixed`) are both reproduced;
  scanops/resultops/resultopsfixed assignments and ordering preserved.
- ExecReScanSubqueryScan: chgParam take/restore is borrow-only (C reads it
  const, never clears), and the `subplan->chgParam == NULL` guard before
  ExecReScan matches.
- This unit owns NO `*-seams` crate (only the `backend-executor-nodeSubqueryscan`
  crate; no `-seams` sibling), so the empty `init_seams()` is correct; it is
  wired into `seams-init::init_all()`.
- `exec_scan_subquery` is declared in `backend-executor-execScan-seams` (owner =
  execScan.c, NOT this unit) and is uninstalled only because execScan is not yet
  CATALOG-complete; the recurrence_guard correctly exempts it (clause b). Not a
  nodeSubqueryscan finding.

Gates re-run on the branch tip: `cargo check --workspace` green (only
pre-existing printtup warnings), `cargo test -p backend-executor-nodeSubqueryscan`
ok, `cargo test -p seams-init` ok (both recurrence_guard tests pass).
Verdict confirmed PASS.
