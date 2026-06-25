# Audit: backend-executor-nodeResult

Unit: `backend-executor-nodeResult` (+ `types-nodes` additions: `noderesult`
module, `Node::Result` / `PlanStateNode::Result` variants)
C source: `src/backend/executor/nodeResult.c`
c2rust: `c2rust-runs/backend-executor-nodeResult/src/nodeResult.rs`
Port: `crates/backend-executor-nodeResult/src/lib.rs`
Date: 2026-06-13
Model: Claude Opus 4.8 (1M context)
Verdict: PASS

## Function inventory and verdicts

nodeResult.c defines exactly 6 functions (confirmed against the C and the
c2rust rendering, whose other `extern "C" fn` entries — `ExecQual`,
`ExecProject`, `ExecProcNode`, `ExecClearTuple`, `ExecEvalExpr*`,
`MemoryContextSwitchTo`, `DatumGetBool`, `newNode` — are inlined/imported
helpers owned by other units, reached here via seams).

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecResult` (static) | 64 | `ExecResult` / `exec_result_node` | MATCH | CHECK_FOR_INTERRUPTS; econtext = ps_ExprContext; rs_checkqual gate runs ExecQual(resconstantqual) once, clears rs_checkqual, on false sets rs_done + returns NULL; ResetExprContext (per-tuple ctx reset); `if !rs_done` { outer present → ExecProcNode, TupIsNull→NULL, set ecxt_outertuple; else rs_done=true } then ExecProject + return; trailing return NULL. ExecQual NULL-state short-circuit preserved (always-true). |
| `ExecResultMarkPos` | 142 | `ExecResultMarkPos` | MATCH | outerPlan present → ExecMarkPos; else `elog(DEBUG2, "Result nodes do not support mark/restore")` (severity DEBUG2, identical text). |
| `ExecResultRestrPos` | 158 | `ExecResultRestrPos` | MATCH | outerPlan present → ExecRestrPos; else `elog(ERROR, "Result nodes do not support mark/restore")` (severity ERROR, identical text → returns Err). |
| `ExecInitResult` | 180 | `ExecInitResult` | MATCH | castNode(Result); Assert(!(eflags & (MARK\|BACKWARD)) \|\| outerPlan != NULL); makeNode(ResultState) with plan/ExecProcNode back-links; rs_done=false; rs_checkqual = (resconstantqual != NULL); ExecAssignExprContext; ExecInitNode(outerPlan); Assert(innerPlan == NULL); ExecInitResultTupleSlotTL(TTSOpsVirtual) + ExecAssignProjectionInfo(NULL); qual = ExecInitQual(plan.qual); resconstantqual = ExecInitQual((List*)resconstantqual). All in order. |
| `ExecEndResult` | 233 | `ExecEndResult` | MATCH | ExecEndNode(outerPlanState). C calls unconditionally; ExecEndNode(NULL) is a no-op, so the `if let Some` guard is behavior-identical (the owned model has no NULL PlanState to pass). |
| `ExecReScanResult` | 243 | `ExecReScanResult` | MATCH | rs_done=false; rs_checkqual = (resconstantqual != NULL); if outerPlan && chgParam==NULL → ExecReScan. |

## Constants verified (against C headers, not memory)

- `T_Result = 331` (nodes/nodetags.h:348) — already present in `types-nodes::nodes`.
- `T_ResultState = 394` (nodes/nodetags.h:411) — added in `noderesult::T_ResultState`.
- `EXEC_FLAG_MARK = 0x0010`, `EXEC_FLAG_BACKWARD = 0x0008` (executor.h) — used in the Assert; values match `types-nodes::executor`.
- `DEBUG2` severity (elog.h level 13) and `ERROR` (level 21) — match `types-error`.
- Result struct shape (plannodes.h:248): `Plan plan; Node *resconstantqual;` — mirrored field-for-field (`resconstantqual` is the planner's qual `List`, modeled as `Option<PgVec<Expr>>`, consumed by ExecInitQual as a list exactly as C casts `(List *)`).
- ResultState struct shape (execnodes.h:1356): `PlanState ps; ExprState *resconstantqual; bool rs_done; bool rs_checkqual;` — mirrored field-for-field.

## Seam audit

This unit owns **no** `-seams` crate (no C file in its `c_sources` maps to an
`X-seams` crate; `execProcnode-seams` etc. are owned by their respective
units). Its `init_seams()` is therefore correctly an empty no-op, and it is
wired into `seams-init::init_all()` (and listed as a dep in
`seams-init/Cargo.toml`) for parity with the sibling nodeMaterial/nodeMergejoin
model.

Outward seam calls (all thin marshal+delegate, all into genuinely unported
owners, each panics until the owner lands — mirror-pg-and-panic):

- `backend-tcop-postgres-seams::check_for_interrupts` — CHECK_FOR_INTERRUPTS.
- `backend-executor-execProcnode-seams::{exec_init_node, exec_proc_node, exec_end_node}` — ExecInitNode/ExecProcNode/ExecEndNode. (execProcnode.c is `todo`.)
- `backend-executor-execAmi-seams::{exec_mark_pos, exec_restr_pos, exec_re_scan}` — ExecMarkPos/ExecRestrPos/ExecReScan.
- `backend-executor-execUtils-seams::{exec_assign_expr_context, exec_assign_projection_info}` — ExecAssignExprContext/ExecAssignProjectionInfo.
- `backend-executor-execTuples-seams::exec_init_result_tuple_slot_tl` — ExecInitResultTupleSlotTL.
- `backend-executor-execExpr-seams::{exec_init_qual, exec_qual, exec_project}` — ExecInitQual/ExecQual/ExecProject.
- `backend-utils-error::elog` — direct dependency (ported), used for the DEBUG2/ERROR mark/restore diagnostics; not a seam.

No branching/computation/node-construction occurs on any seam path; each call
site is argument-convert → delegate → result-convert. The `ResetExprContext`
(per-tuple memory reset) is done in-crate via `estate.ecxt_mut(id).
ecxt_per_tuple_memory.reset()`, matching the nodeMergejoin owned-tree model
(not a seam — it is a context op on owned EState state).

## Design conformance

- Opacity: no invented handles; `Result`/`ResultState` are real structs
  mirroring the C; `resconstantqual` resolves to the real qual list type.
- `Mcx` + `PgResult`: `ExecInitResult` (allocates the state tree) takes the
  per-query `Mcx` via `estate.es_query_cxt` and returns `PgResult`; every
  fallible/ereport-capable path returns `PgResult`. `ExecResult` returns
  `PgResult<Option<SlotId>>` (the C `TupleTableSlot *` / NULL).
- No shared statics, no ambient-global seams, no locks across `?`.
- The `ExecProcNode` dispatch callback (`exec_result_node`) mirrors the C
  `resstate->ps.ExecProcNode = ExecResult` install and the `castNode` check.

## Verdict

PASS — all 6 functions MATCH, constants verified against headers, seams thin
and into genuinely unported owners, design rules satisfied. 12 in-crate logic
tests cover the constant-target one-shot, the rs_checkqual one-time-filter gate
(true/false), outer-plan passthrough/exhaustion, rescan reset + child rescan,
and mark/restore delegation + the DEBUG2/ERROR no-outer-plan paths.
