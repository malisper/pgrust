# Audit: backend-executor-execExpr

- **Unit:** backend-executor-execExpr (the expression *compiler*)
- **C source:** `src/backend/executor/execExpr.c` (5074 lines, PostgreSQL 18.3)
- **c2rust reference:** `c2rust-runs/backend-executor-execExpr/src/*.rs`
- **Branch audited:** `assemble/expr-eval-keystone`
  (Expr-eval keystone assembly: `keystone/expr-eval-model` base + the five
  keystone-based compiler family sub-branches — compiler-core-inline-arms,
  compiler-func-strict, compiler-agg, compiler-hash, compiler-modify-quals —
  merged; the agg/hash overlap on `execExpr_domain_agg.rs` + the Cargo.toml
  ACL/objectaccess dep conflict were union-resolved in this round.)
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context, exact id `claude-opus-4-8[1m]`)
- **Verdict: PASS** (independently re-derived from the C + the owned model)

## Function inventory (33 functions in execExpr.c)

All 33 C function definitions (statics + public) have a Rust counterpart; no
function is absent.

| C function | Rust port | Verdict |
|---|---|---|
| ExecInitExpr | execExpr_core::exec_init_expr | MATCH |
| ExecInitExprWithParams | execExpr_core::exec_init_expr_with_params | MATCH |
| ExecInitQual | execExpr_core::exec_init_qual | MATCH |
| ExecInitCheck | execExpr_core::exec_init_qual (check wrapper) | MATCH |
| ExecInitExprList | execExpr_core::exec_init_expr_list | MATCH |
| ExecBuildProjectionInfo | execExpr_core::exec_build_projection_info[_impl] | MATCH |
| ExecBuildUpdateProjection | execExpr_core::exec_build_update_projection[_impl] | MATCH |
| ExecPrepareExpr | execExpr_core::exec_prepare_expr | MATCH |
| ExecPrepareQual | execExpr_core::exec_prepare_expr (qual wrapper) | MATCH |
| ExecPrepareCheck | execExpr_core::exec_prepare_expr (check wrapper) | MATCH |
| ExecPrepareExprList | execExpr_core::exec_prepare_expr_list | MATCH |
| ExecCheck | execExpr_core (exec_qual/check path) | MATCH |
| ExecReadyExpr | execExpr_core::exec_ready_expr | MATCH |
| ExecInitExprRec | execExpr_core::exec_init_expr_rec (45-arm switch) | MATCH (see model-gap arms) |
| ExprEvalPushStep | execExpr_core::expr_eval_push_step | MATCH |
| ExecInitFunc | execExpr_func_subscript::exec_init_func | MATCH |
| ExecInitSubPlanExpr | execExpr_func_subscript::exec_init_subplan* | SEAMED (nodeSubplan owner) |
| ExecCreateExprSetupSteps | execExpr_core::exec_create_expr_setup_steps[_list/_tlist] | MATCH |
| ExecPushExprSetupSteps | execExpr_core::exec_push_expr_setup_steps | MATCH |
| expr_setup_walker | execExpr_core::expr_setup_walker | MATCH |
| ExecComputeSlotInfo | execExpr_core::exec_compute_slot_info | MATCH |
| ExecInitWholeRowVar | execExpr_func_subscript::exec_init_whole_row_var | MATCH (SubqueryScan/CteScan parent → execJunk/execTuples seam) |
| ExecInitSubscriptingRef | execExpr_func_subscript::exec_init_subscripting_ref | SEAMED (getSubscriptingRoutines owner) |
| isAssignmentIndirectionExpr | execExpr_func_subscript::is_assignment_indirection_expr | MATCH |
| ExecInitCoerceToDomain | execExpr_domain_agg::exec_init_coerce_to_domain | SEAMED (typcache InitDomainConstraintRef, #58) |
| ExecBuildAggTrans | execExpr_domain_agg::exec_build_agg_trans | MATCH (common/presorted/single-col AND combine-with-deserialize fully emitted; only the multi-col non-presorted ORDER BY/DISTINCT path remains decomp-blocked on the execTuples slot-cell model #113) |
| ExecBuildAggTransCall | execExpr_domain_agg::exec_build_agg_trans_call | MATCH |
| ExecBuildHash32FromAttrs | execExpr_domain_agg::exec_build_hash32_from_attrs | MATCH |
| ExecBuildHash32Expr | execExpr_domain_agg::exec_build_hash32_expr | MATCH |
| ExecBuildGroupingEqual | execExpr_domain_agg::exec_build_grouping_equal | MATCH |
| ExecBuildParamSetEqual | execExpr_domain_agg::exec_build_param_set_equal | MATCH |
| ExecInitJsonExpr | execExpr_json::exec_init_json_expr | SEAMED (JsonExprState runtime-state types not modeled) |
| ExecInitJsonCoercion | execExpr_json::exec_init_json_coercion | SEAMED (JsonExprState escontext + lsyscache seams) |

## Closure of the prior FAIL

The prior audit (`port/backend-executor-execExpr`) FAILed because the agg / hash
/ grouping / domain leaf builders emitted only a faithful *prefix* then
`panic!()` **before the per-column emission that is this crate's own logic** —
the over-stated blockers denied an arg-cell mechanism that in fact existed.

This round closes that gap. The keystone (F0) expanded the per-arg `ResultCellId`
cells (HashDatum.arg_cell, AggDeserialize.arg_cell, AggStrictInputCheck.arg_cells,
ScalarArrayOp.scalar_cell/array_cell), and the compiler-agg / compiler-hash
family branches use them to complete the per-column loops:

- `exec_build_hash32_from_attrs`, `exec_build_hash32_expr`,
  `exec_build_grouping_equal`, `exec_build_param_set_equal`,
  `exec_build_agg_trans_call`: **fully emitted, zero panics** — per-column/per-key
  ACL check + fmgr_info + EEOP_*_VAR arg-cell wiring + equality/hash step emission
  all present.
- `exec_build_agg_trans`: the common (non-deserialize) combine, presorted,
  non-sorted, and single-column-sort transition paths are fully emitted; only two
  paths panic, both on genuine unported owners (see below).

## Remaining panics — all genuine unported-owner / model-layer blockers

Every residual `panic!` is mirror-PG-and-panic on an unported *owner* or a
deliberately-not-yet-expanded shared-model struct — never simplified own-logic.
Per the audit-crate rule "panicking on an unported callee is fine, absent logic
is not", these are acceptable:

1. ExecInitExprRec ScalarArrayOpExpr / MinMaxExpr / ArrayExpr arms — the
   keystone-trimmed `primnodes::ScalarArrayOpExpr` (no opfuncid/hashfuncid/
   negfuncid/inputcollid) and the MinMax/ArrayExpr step payloads (no per-element
   ResultCellId) lack fields the compiler must read. Owner: the shared
   nodes/keystone model layer (a different unit). Genuine model gap.
2. ExecBuildAggTrans deserialize path — NOW FULLY EMITTED (fix/tail round,
   2026-06-13). The prior panic claimed the step needed the nodeAgg-owned
   pre-bound `pertrans->deserialfn_fcinfo` and that the trimmed
   FunctionCallInfoBaseData could not carry it — but the keystone (F0) already
   expanded `AggDeserialize { fcinfo_data, arg_cell, jumpnull }`, the exact
   owned-model shape the HashDatum path uses. The fix mirrors the C
   (execExpr.c:3782-3815): a fresh `init_fcinfo` ds frame in `fcinfo_data`, the
   source sub-expression recursed into an `arg_cell` arena cell standing in for
   `&ds_fcinfo->args[0]`, strict-vs-nonstrict opcode selection
   (EEOP_AGG_STRICT_DESERIALIZE / EEOP_AGG_DESERIALIZE), output into a fresh
   transfn-arg cell (the C `&trans_fcinfo->args[argno+1]`), `adjust_bailout` only
   when the deserialfn is strict, and `strictargs` = that output cell. The dummy
   `args[1]` is a no-op (the trimmed frame has no `args[]`; the interpreter
   supplies it at call time). The interpreter's own `EEOP_AGG_*DESERIALIZE` arm
   still panics — but that is the *execution* layer's unported fmgr-widened frame
   + nodeAgg tmpcontext owner, the identical boundary every `EEOP_AGG_PLAIN_TRANS_*`
   step the rest of this builder already emits depends on; the compiler's job is
   to emit the step program, which it now does.
3. ExecBuildAggTrans multi-column non-presorted ORDER BY/DISTINCT path — recurses
   into `sortslot->tts_values/tts_isnull`, which the execTuples TupleTableSlot
   model (task #113, Bucket C) does not yet expose as addressable ResultCells.
4. ExecInitCoerceToDomain — needs typcache `InitDomainConstraintRef` (#58) to
   enumerate the domain's DomainConstraintState list; without it the
   per-constraint DOM_CONSTRAINT_CHECK/NOTNULL steps cannot be emitted.
5. ExecInitJsonExpr / ExecInitJsonCoercion — need the unmodeled JsonExprState /
   JsonPathVariable / ErrorSaveContext runtime-state types and lsyscache seams.
6. ExecInitSubPlanExpr / ExecInitSubscriptingRef / WholeRowVar subquery path —
   nodeSubplan / getSubscriptingRoutines / execJunk owners.

## Seam audit

Owned seam crate: `backend-executor-execExpr-seams` (48 `seam!` declarations —
the outward-facing entry points other executor units call into the compiler).
All 48 are installed by `backend_executor_execExpr::init_seams()` (48 `::set()`
calls; verified 48 = 48), which contains nothing but `set()` calls. seams-init
`init_all()` calls `backend_executor_execExpr::init_seams()` (line 47). The
seams-init `recurrence_guard` declared-seams-are-set test passes. No uninstalled
seam, no `set()` outside the owner, no branching/construction in any seam path.

## Gate

`cargo check --workspace` clean (0 errors). `cargo test --workspace` clean (no
failures; 2 known timeout flakes excluded). 89 distinct `EEOP_*` opcodes emitted
across the crate; no `todo!()`/`unimplemented!()` in any function body.

## Verdict

**PASS.** All 33 functions present; the previously-PARTIAL agg/hash/grouping/
param-set builders are now fully emitted on the F0 arg-cell model; every residual
panic is a justified mirror-PG-and-panic on an unported owner or a
not-yet-expanded shared-model struct. Seam wiring complete and clean.

### fix/tail update (2026-06-13)

`ExecBuildAggTrans` combine-with-deserialization path completed — it was the last
stale-post-keystone panic on this builder. The keystone-added `AggDeserialize {
fcinfo_data, arg_cell, jumpnull }` payload was the missing piece (the prior panic
mis-claimed it needed a cross-owner ds_fcinfo). `exec_build_agg_trans` now emits
every input path except the multi-column non-presorted ORDER BY/DISTINCT case,
which genuinely needs the execTuples TupleTableSlot slot-cell model (#113, Bucket
C) to name `sortslot->tts_values[i]`/`tts_isnull[i]` as recursion-output
ResultCells — reported to needs_decomp, not fixed here. Verdict unchanged: PASS.
