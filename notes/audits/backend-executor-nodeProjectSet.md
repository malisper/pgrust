# Audit: backend-executor-nodeProjectSet

Independent function-by-function audit of `port/backend-executor-nodeProjectSet`
against `src/backend/executor/nodeProjectSet.c` (PG 18.3) and the c2rust run
`../pgrust/c2rust-runs/backend-executor-nodeProjectSet/`.

Verdict: **PASS**

## Function inventory

C source defines 5 functions (c2rust top-level non-helper renders confirm the
same set; the additional c2rust items `MemoryContextSwitchTo`, `newNode`,
`list_length`, `ExecClearTuple`, `ExecProcNode`, `ExecEvalExpr` are header-inlined
helpers, not nodeProjectSet.c functions, and are seamed in the port).

| C function | C loc | Port loc (src/lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| `ExecProjectSet` (static) | nodeProjectSet.c:41 | `ExecProjectSet` :67 / `exec_project_set_node` :305 | MATCH | CHECK_FOR_INTERRUPTS via tcop seam; ResetExprContext == per_tuple_memory.reset(); pending_srf_tuples continuation branch; outer-fetch loop with argcontext reset, ExecProcNode seam, TupIsNull check (None or empty slot), ecxt_outertuple set, ExecProjectSRF(false), post-loop econtext reset. Control flow identical. |
| `ExecProjectSRF` (static) | nodeProjectSet.c:138 | `ExecProjectSRF` :158 | MATCH | ExecClearTuple seam; pending_srf_tuples=false; per-column loop over nelems; `continuing && isdone==ExprEndResult` -> NULL fill (isdone untouched, matching C); `IsA(SetExprState)` -> exec_make_function_result_set seam, hasresult/pending bookkeeping; else plain expr -> exec_eval_expr_switch_context seam, isdone=ExprSingleResult; Assert(hassrf)->debug_assert; hasresult -> store_virtual_values + return slot, else None. tts_values/tts_isnull writes accumulated to local buffers and committed via execTuples seam (slot payload owned there) — behavior-identical. |
| `ExecInitProjectSet` | nodeProjectSet.c:226 | `ExecInitProjectSet` :324 | MATCH | castNode panic; EXEC_FLAG_MARK|BACKWARD debug_assert; makeNode/plan/ExecProcNode set; pending=false; ExecAssignExprContext seam; ExecInitNode(outerPlan) seam; innerPlan==NULL debug_assert; ExecInitResultTupleSlotTL(Virtual) seam; nelems=list_length(targetlist); elems/elemdone workspace; funcretset/opretset SRF detection -> ExecInitFunctionResultSet vs ExecInitExpr seam; qual==NIL debug_assert; argcontext child "tSRF function arguments". |
| `ExecEndProjectSet` | nodeProjectSet.c:327 | `ExecEndProjectSet` :457 | MATCH | ExecEndNode(outerPlanState) seam; C ExecEndNode(NULL) no-op modeled by `if let Some`. |
| `ExecReScanProjectSet` | nodeProjectSet.c:336 | `ExecReScanProjectSet` :472 | MATCH | pending_srf_tuples=false; `if outerPlan->chgParam == NULL` -> ExecReScan seam. |

Helper `expr_returns_set` :499 faithfully inlines the C init-walker discriminant
`(IsA(FuncExpr)&&funcretset)||(IsA(OpExpr)&&opretset)`.

## Constants

- `"tSRF function arguments"` argcontext name: matches C string literal.
- `EXEC_FLAG_MARK`/`EXEC_FLAG_BACKWARD` flag bits sourced from
  `types_nodes::executor` (not transcribed).
- `ExprDoneCond` variants (ExprSingleResult/ExprMultipleResult/ExprEndResult)
  from `types_nodes::execexpr`.

## Seam audit

nodeProjectSet owns no `*-seams` crate (its only C file is nodeProjectSet.c, and
no `nodeProjectSet-seams` crate exists), so `init_seams()` is correctly empty and
is wired into `seams-init::init_all()` (crates/seams-init/src/lib.rs:77). The
recurrence_guard tests (`every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner`) both pass.

All outward seam calls are thin marshal+delegate into named unported owners and
carry no own-logic:
- execProcnode: exec_init_node / exec_proc_node / exec_end_node
- execAmi: exec_re_scan
- execUtils: exec_assign_expr_context
- execTuples: exec_init_result_tuple_slot_tl / exec_clear_tuple / store_virtual_values
- execExpr: exec_init_expr / exec_eval_expr_switch_context
- execSRF: exec_init_function_result_set / exec_make_function_result_set
- tcop/postgres: check_for_interrupts

No own-logic stubs, no todo!()/unimplemented!(), no deferred/SEAMED-equivalent
escape of in-crate logic. Seam return types are PgResult mirroring the C
ereport(ERROR) failure surface.

## Gates

- `cargo check --workspace`: pass (only pre-existing unrelated warnings).
- `cargo test -p backend-executor-nodeProjectSet`: 9 passed.
- `cargo test -p seams-init`: 2 passed (recurrence_guard).
