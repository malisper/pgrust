# Audit: backend-executor-nodeCtescan

Unit: `backend-executor-nodeCtescan` (+ `types-nodes` additions: `nodectescan`
module, `Node::CteScan` / `PlanStateNode::CteScan` variants;
`backend-executor-execMain-seams` `cte_*` family; `backend-executor-execScan-seams`
CteScan-specialized `exec_scan_cte` / `exec_scan_rescan_cte` /
`exec_assign_scan_projection_info_cte`)
C source: `src/backend/executor/nodeCtescan.c`
c2rust: `c2rust-runs/backend-executor-nodeCtescan/src/nodeCtescan.rs`
Port: `crates/backend-executor-nodeCtescan/src/lib.rs`
Date: 2026-06-13
Model: Claude Opus 4.8 (1M context)
Verdict: PASS

## Function inventory and verdicts

nodeCtescan.c defines exactly 6 functions (confirmed against the C and the
c2rust rendering; the c2rust file's other `extern` entries — tuplestore_*,
ExecScan, ExecProcNode, makeNode, etc. — are inlined/external callees, not
nodeCtescan functions).

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `CteScanNext` (static) | nodeCtescan.c:32 | lib.rs `CteScanNext` | MATCH | see below |
| 2 | `CteScanRecheck` (static) | nodeCtescan.c:150 | lib.rs `CteScanRecheck` | MATCH | returns `Ok(true)` |
| 3 | `ExecCteScan` (static) | nodeCtescan.c:163 | lib.rs `ExecCteScan` | MATCH | `ExecScan(ss, CteScanNext, CteScanRecheck)` via execScan-seams CteScan family |
| 4 | `ExecInitCteScan` | nodeCtescan.c:177 | lib.rs `ExecInitCteScan` | MATCH | see below |
| 5 | `ExecEndCteScan` | nodeCtescan.c:285 | lib.rs `ExecEndCteScan` | MATCH | leader-only `tuplestore_end` + `cte_table=None` |
| 6 | `ExecReScanCteScan` | nodeCtescan.c:303 | lib.rs `ExecReScanCteScan` | MATCH | see below |

### 1. CteScanNext — MATCH

Control flow mapped branch-for-branch:
- `forward = ScanDirectionIsForward(estate->es_direction)` — uses the real
  `EState.es_direction` + `types_nodes::ScanDirectionIsForward` (not a seam).
- `tuplestore_select_read_pointer(leader->cte_table, readptr)` → seam.
- `eof_tuplestore = tuplestore_ateof(...)` → seam.
- backward-at-EOF arm: `if (!forward && eof_tuplestore)` → if `!leader->eof_cte`,
  `tuplestore_advance(forward)`; `return NULL` (here `Ok(false)`) when advance
  fails (empty store); then `eof_tuplestore = false`. Exact.
- `if (!eof_tuplestore)`: `tuplestore_gettupleslot(forward, copy=true, scanslot)`
  → on hit `return slot` (`Ok(true)`); `if (forward) eof_tuplestore = true`.
  The `copy=true` is documented in the seam decl.
- `if (eof_tuplestore && !leader->eof_cte)`: `ExecProcNode(cteplanstate)`; on
  `TupIsNull` set `leader->eof_cte=true` and `return NULL` (`Ok(false)`); else
  reselect read pointer, `tuplestore_puttupleslot(cteslot)`, `ExecCopySlot(slot,
  cteslot)` → `return slot` (`Ok(true)`).
- fall-through `return ExecClearTuple(slot)` → `clear_scan_tuple_slot` +
  `Ok(false)`.

The C returns `TupleTableSlot *`/`NULL`; the port stages into the node's own
scan slot and reports a `bool`, matching the in-repo `ExecScan` access-method
convention (nodeMaterial/nodeSeqscan/nodeIndexonlyscan precedent). No logic
omitted; every leader-aliased dereference is a seam, every node-machine branch
is in-crate.

### 4. ExecInitCteScan — MATCH

- `Assert(!(eflags & EXEC_FLAG_MARK))` → `debug_assert!`. `EXEC_FLAG_MARK=0x0010`,
  `EXEC_FLAG_REWIND=0x0004` verified vs executor.h.
- `eflags |= EXEC_FLAG_REWIND` — present.
- `Assert(outerPlan==NULL); Assert(innerPlan==NULL)` → `debug_assert!` on
  `plan.scan.plan.lefttree/righttree`.
- `makeNode(CteScanState)` + `ss.ps.plan=(Plan*)node` + `ss.ps.ExecProcNode=
  ExecCteScan` → `alloc_in` + `ps.plan=Some(node)` + `ExecProcNode=
  Some(exec_cte_scan_node)` (the `castNode(CteScanState, pstate)` dispatch
  wrapper). `eflags`, `cte_table=None`, `eof_cte=false` set.
- `cteplanstate = list_nth(es_subplanstates, ctePlanId-1)` → `cte_link_plan_state`
  seam (es_subplanstates is executor-owned).
- Param-slot leader handshake (`prmdata=&es_param_exec_vals[cteParam]`,
  `Assert(execPlan==NULL)`, `Assert(!isnull)`, `leader=DatumGetPointer(value)`,
  leader-publish-or-record) → `cte_resolve_leader` seam returning `is_leader`.
- leader branch: `tuplestore_begin_heap(true,false,work_mem)` +
  `tuplestore_set_eflags(eflags)` + `readptr=0` → `cte_tuplestore_begin_heap_leader`
  seam + `readptr=0` in-crate. follower branch: `tuplestore_alloc_read_pointer` +
  `select` + `rescan` → `cte_tuplestore_alloc_read_pointer_follower` seam.
- `ExecAssignExprContext` → execUtils seam.
- `ExecInitScanTupleSlot(ExecGetResultType(cteplanstate), &TTSOpsMinimalTuple)` →
  `init_scan_tuple_slot_from_cte`: reads `ExecGetResultType` (execTuples seam)
  off the linked cteplanstate's `ps_head`, then `ExecInitScanTupleSlot` with
  `TupleSlotKind::MinimalTuple`. The `exec_get_result_type` read + descriptor
  clone is thin marshalling, not divergent logic.
- `ExecInitResultTypeTL` → execTuples seam.
- `ExecAssignScanProjectionInfo` → `exec_assign_scan_projection_info_cte`
  (execScan owner) seam.
- `ExecInitQual(scan.plan.qual, ...)` → execExpr seam, result assigned to
  `ss.ps.qual`.

### 6. ExecReScanCteScan — MATCH

- `if (ps_ResultTupleSlot) ExecClearTuple(it)` → `if let Some(slot) =
  ps_ResultTupleSlot { exec_clear_tuple(...) }`.
- `ExecScanReScan(&ss)` → `exec_scan_rescan_cte` seam.
- `if (leader->cteplanstate->chgParam != NULL)` → `cte_leader_cteplanstate_chgparam_set`
  seam; true → `tuplestore_clear` + `leader->eof_cte=false`; else →
  `tuplestore_select_read_pointer` + `tuplestore_rescan` (own pointer).

## Constants / tags

- `T_CteScan = 351`, `T_CteScanState = 415` — verified against
  `postgres-18.3/src/backend/nodes/nodetags.h` (T_CteScan=351, T_CteScanState=415).
- `EXEC_FLAG_MARK = 0x0010`, `EXEC_FLAG_REWIND = 0x0004` — verified against
  executor.h (types-nodes/executor.rs).

## Seam audit

Owned seam crates by C-source coverage: `backend-executor-nodeCtescan` maps to
`nodeCtescan.c`; there is **no** `crates/backend-executor-nodeCtescan-seams`
(this unit declares no inward seam crate — nothing else calls into it across a
cycle). Its `init_seams()` is empty, which is correct for a crate with no owned
seam crate (nodeResult/nodeMaterial/nodeSeqscan precedent). It is wired into
`seams-init::init_all()`. The `every_seam_installing_crate_is_wired_into_init_all`
and `every_declared_seam_is_installed_by_its_owner` guards both pass.

Outward seams, each justified:
- `execMain-seams::cte_*` (17 seams) — the aliased `leader` self-/cross-reference
  (shared `cte_table`/`eof_cte`, CTE subplan dispatch via leader's `cteplanstate`,
  and the `es_param_exec_vals` Param-slot leader handshake) cannot be modeled by
  an owned mutable alias in safe Rust; the resolution requires the live executor
  node graph + Param slot, owned by execMain (status `todo`). Homed in
  execMain-seams (NOT a nodeCtescan-owned crate) so the declared-seam guard
  treats them as legitimate seam-and-panic until execMain lands
  (mirror-pg-and-panic). Each is a 1:1 wrapper of one C operation; no branching
  or computation lives in the seam path.
- `execScan-seams::exec_scan_cte` / `exec_scan_rescan_cte` /
  `exec_assign_scan_projection_info_cte` — the generic `ExecScan` driver +
  `ExecScanReScan` + `ExecAssignScanProjectionInfo` are owned by execScan.c
  (status `todo`); CteScan-specialized entry points mirror the existing
  TableFunc/IndexOnly families in the same crate.
- `execTuples-seams::exec_clear_tuple` / `exec_get_result_type` /
  `exec_init_scan_tuple_slot` / `exec_init_result_type_tl`,
  `execUtils-seams::exec_assign_expr_context`, `execExpr-seams::exec_init_qual`
  — all execTuples/execUtils/execExpr-owned leaf operations, reused as-is.

No logic was replaced by a seam call: every node-machine branch (direction
classification, eof bookkeeping, leader-vs-follower init, chgParam rescan
decision, slot clearing) lives in `crates/backend-executor-nodeCtescan/src/lib.rs`.

## Design conformance

- No invented opacity: `cteplanstate` is the real `PlanStateNode` link
  (`Option<PgBox<PlanStateNode>>`), `cte_table` the real
  `Option<PgBox<Tuplestorestate>>`. The `leader` raw `CteScanState *` alias is
  not represented as an invented handle — it is dissolved into the execMain
  `cte_*` seams (the node-graph owner resolves it), per the "aliased
  externally-owned data" rule.
- Allocating paths (`alloc_in`, descriptor `clone_in`) are fallible (`PgResult`)
  with `Mcx` (`estate.es_query_cxt`); no infallible allocation on a C-palloc path.
- No shared statics / ambient-global seams / zero-arg getters: `work_mem`,
  `es_direction`, Param slot, subplan list are all reached as parameters or via
  the explicit estate.
- No locks held across `?`. No registry side tables.
- No `todo!()`/`unimplemented!()`; the only panics are the `castNode` failure
  arms (C `castNode` aborts) and the `ExecGetResultType` invariant `expect`
  (the C dereferences it unconditionally at that point).

## Verdict: PASS

All 6 functions MATCH; all seams justified and thin; design-conformance clean.
15 unit tests green; `cargo check --workspace` and both recurrence guards green.

## Independent re-audit (2026-06-13, Claude Fable 5)

Re-derived the inventory and every verdict from the C
(`nodeCtescan.c`), the c2rust rendering, and `lib.rs` without trusting the
above. Confirms:
- Exactly 6 C functions; all 6 present, control flow / error paths / fall-through
  mapped branch-for-branch (CteScanNext backward-EOF extra-advance, gettupleslot
  forward arm, subplan pull→reselect→puttupleslot→ExecCopySlot path, terminal
  ExecClearTuple; ExecInitCteScan REWIND force + child asserts + leader/follower
  store setup + qual; ExecReScanCteScan chgParam branch). No own-logic stubs, no
  `todo!()`/`unimplemented!()`, no deferred-error escape.
- Node tags re-verified against `postgres-18.3/src/backend/nodes/nodetags.h`:
  `T_CteScan = 351` (line 368), `T_CteScanState = 415` (line 432).
- Leader-aliased `cte_*` family is declared in `execMain-seams` (the genuine
  owner of the node graph + `es_param_exec_vals` + `es_subplanstates`), unset
  pending execMain — legitimate mirror-pg-and-panic, not absent logic; the
  node-machine control flow stays in-crate. `init_seams()` empty (no owned
  inward seam crate) and wired into `init_all()`.
- Gates re-run on `port/backend-executor-nodeCtescan`:
  `cargo test -p backend-executor-nodeCtescan` 15/15 green;
  `cargo test -p seams-init` both recurrence guards green;
  `cargo check --workspace` clean (only pre-existing unrelated warnings).

Verdict unchanged: PASS.
