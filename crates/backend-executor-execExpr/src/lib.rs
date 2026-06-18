#![allow(non_snake_case)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// The family bodies are filled against the keystone F0 ExprEvalStep model (all
// 33 execExpr.c functions present; the agg/hash/grouping/param-set builders emit
// per-column logic on the F0 arg-cell cells). The remaining `panic!`s are
// mirror-PG-and-panic on unported owners / not-yet-expanded shared-model structs
// (ScalarArrayOp/MinMax/ArrayExpr primnodes; nodeAgg ds_fcinfo; execTuples slot
// model #113; typcache #58; JsonExprState; nodeSubplan/subscripting) — no
// placeholder stub in own logic. dead_code/unused_variables allowed for the threaded
// args those still-blocked arms hold.
#![allow(dead_code)]
#![allow(unused_variables)]

//! `backend-executor-execExpr` — port of `src/backend/executor/execExpr.c`.
//!
//! execExpr.c is the executor's expression *compiler*: it turns an `Expr` plan
//! tree into a flat array of [`ExprEvalStep`] instructions (an [`ExprState`])
//! that the interpreter (`execExprInterp.c`, the cycle partner) then walks. Its
//! public surface is the `ExecInit*` / `ExecBuild*Projection` / `ExecPrepare*`
//! family, the giant `ExecInitExprRec` opcode-emission switch, and the
//! step-pushing / slot-setup helpers (`ExprEvalPushStep`,
//! `ExecCreateExprSetupSteps`, `ExecComputeSlotInfo`).
//!
//! In the owned model the compiled program lives in the EState's per-query
//! context; the seams take `&mut EStateData` plus the relevant plan-state /
//! node by id and return owned `PgBox<'mcx, ExprState<'mcx>>` /
//! `ProjectionInfo<'mcx>`. Every cross-unit call (typcache, fmgr lookup, the
//! per-node-owner `ExecEval*` interpreter subroutines) routes through that
//! owner's seam crate (a loud panic until it lands).
//!
//! Decomposition (this unit is too large for one pass) — five family modules:
//!
//!  * [`execExpr_core`] — the dispatch + setup spine: `ExecInitExpr` /
//!    `…WithParams` / `ExecInitQual` / `ExecInitExprList` / `ExecCheck`,
//!    `ExecBuildProjectionInfo` / `ExecBuildUpdateProjection`,
//!    `ExecPrepareExpr` / `…Qual` / `…ExprList`, `ExecReadyExpr`, the giant
//!    `ExecInitExprRec` switch, `ExprEvalPushStep`,
//!    `ExecCreateExprSetupSteps` / `expr_setup_walker`, `ExecComputeSlotInfo`,
//!    and the plain `ExecQual` / `ExecProject` eval entry points. Owns +
//!    installs the bulk of the execExpr seams.
//!  * [`execExpr_func_subscript`] — `ExecInitFunc` (FUNCEXPR strict/fusage
//!    classification), `ExecInitSubPlanExpr`, `ExecInitWholeRowVar`,
//!    `ExecInitSubscriptingRef` + `isAssignmentIndirectionExpr`; the SubPlan
//!    init/eval/projection seams.
//!  * [`execExpr_domain_agg`] — `ExecInitCoerceToDomain`,
//!    `ExecBuildAggTrans[Call]`, `ExecBuildGroupingEqual`,
//!    `ExecBuildParamSetEqual`, `ExecBuildHash32FromAttrs` / `…Expr`; the
//!    hashed-subplan projection/equality build seams.
//!  * [`execExpr_json`] — `ExecInitJsonExpr` + `ExecInitJsonCoercion`.
//!  * [`execExpr_modify`] — the nodeModifyTable / execPartition leaf builders
//!    that are thin `ExecInitQual` / `ExecBuild*Projection` wrappers but live
//!    behind execExpr seams (MERGE / ON CONFLICT / RETURNING / WCO projections,
//!    the partition-init map-and-build seams).
//!
//! [`ExprEvalStep`]: types_nodes::execexpr::ExprEvalStep
//! [`ExprState`]: types_nodes::execexpr::ExprState
//! [`ProjectionInfo`]: types_nodes::execexpr::ProjectionInfo

pub mod execExpr_core;
pub mod execExpr_domain_agg;
pub mod execExpr_func_subscript;
pub mod execExpr_json;
pub mod execExpr_modify;

/// Install every seam this unit owns.
///
/// The unit owns one seam crate (by C-source coverage of `execExpr.c`):
/// `backend-executor-execExpr-seams`. Every declaration in it is installed
/// here, exactly once, routed to the family module that owns the C function.
pub fn init_seams() {
    use backend_executor_execExpr_seams as seams;

    // --- execExpr-core: ExecInit* / ExecBuild*Projection / ExecPrepare* /
    //     ExecQual / ExecProject / eval entry points ---
    seams::exec_init_expr::set(execExpr_core::exec_init_expr);
    seams::exec_init_expr_with_params::set(execExpr_core::exec_init_expr_with_params);
    seams::exec_init_qual::set(execExpr_core::exec_init_qual);
    seams::exec_prepare_qual::set(execExpr_core::exec_prepare_qual);
    seams::exec_init_expr_list::set(execExpr_core::exec_init_expr_list);
    seams::exec_init_expr_list_no_parent::set(execExpr_core::exec_init_expr_list_no_parent);
    seams::exec_prepare_expr::set(execExpr_core::exec_prepare_expr);
    seams::exec_prepare_expr_list::set(execExpr_core::exec_prepare_expr_list);
    seams::exec_build_projection_info::set(execExpr_core::exec_build_projection_info);
    seams::exec_build_update_projection::set(execExpr_core::exec_build_update_projection);
    seams::exec_eval_expr_switch_context::set(execExpr_core::exec_eval_expr_switch_context);
    seams::exec_eval_tid_expr_switch_context::set(execExpr_core::exec_eval_tid_expr_switch_context);
    seams::exec_eval_array_expr_switch_context::set(
        execExpr_core::exec_eval_array_expr_switch_context,
    );
    seams::exec_qual::set(execExpr_core::exec_qual);
    seams::exec_qual_and_reset::set(execExpr_core::exec_qual_and_reset);
    seams::exec_project::set(execExpr_core::exec_project);
    seams::exec_project_info::set(execExpr_core::exec_project_info);
    seams::create_executor_state::set(execExpr_core::create_executor_state);
    seams::free_executor_state::set(execExpr_core::free_executor_state);
    seams::eval_exec_param_into_list::set(execExpr_core::eval_exec_param_into_list);
    seams::exec_hashjoin_qual::set(execExpr_core::exec_hashjoin_qual);
    seams::exec_init_hashjoin_qual::set(execExpr_core::exec_init_hashjoin_qual);
    seams::exec_hashjoin_project::set(execExpr_core::exec_hashjoin_project);
    seams::eval_outer_hash::set(execExpr_core::eval_outer_hash);

    // --- execExpr-func-subscript: SubPlan init/eval/projection ---
    seams::sub_init_testexpr::set(execExpr_func_subscript::sub_init_testexpr);
    seams::sub_exec_project::set(execExpr_func_subscript::sub_exec_project);
    seams::sub_proj_result_slot_id::set(execExpr_func_subscript::sub_proj_result_slot_id);
    seams::sub_clear_proj_result_slot::set(execExpr_func_subscript::sub_clear_proj_result_slot);
    seams::proj_result_slot_natts::set(execExpr_func_subscript::proj_result_slot_natts);
    seams::proj_result_slot_attisnull::set(execExpr_func_subscript::proj_result_slot_attisnull);
    seams::proj_left_slot_getattr::set(execExpr_func_subscript::proj_left_slot_getattr);
    seams::eval_testexpr_switch_context::set(execExpr_func_subscript::eval_testexpr_switch_context);

    // --- execExpr-domain-agg: hashed-subplan equality/hash build ---
    seams::classify_testexpr::set(execExpr_domain_agg::classify_testexpr);
    seams::resolve_combining_op::set(execExpr_domain_agg::resolve_combining_op);
    seams::build_hash_projections_and_exprs::set(
        execExpr_domain_agg::build_hash_projections_and_exprs,
    );
    seams::exec_build_hash32_expr::set(execExpr_domain_agg::exec_build_hash32_expr);
    seams::exec_build_hash32_from_attrs::set(execExpr_domain_agg::exec_build_hash32_from_attrs);
    seams::exec_build_grouping_equal::set(execExpr_domain_agg::exec_build_grouping_equal);
    seams::exec_build_param_set_equal::set(execExpr_domain_agg::exec_build_param_set_equal);
    seams::exec_build_agg_trans::set(execExpr_domain_agg::seam_exec_build_agg_trans);

    // --- execExpr-modify: nodeModifyTable / execPartition leaf builders ---
    seams::exec_init_merge_when_qual::set(execExpr_modify::exec_init_merge_when_qual);
    seams::exec_build_merge_insert_projection::set(
        execExpr_modify::exec_build_merge_insert_projection,
    );
    seams::exec_build_merge_update_projection::set(
        execExpr_modify::exec_build_merge_update_projection,
    );
    seams::exec_init_merge_join_condition::set(execExpr_modify::exec_init_merge_join_condition);
    seams::exec_init_merge_inherited_root::set(execExpr_modify::exec_init_merge_inherited_root);
    seams::partition_init_with_check_options::set(
        execExpr_modify::partition_init_with_check_options,
    );
    seams::partition_init_returning::set(execExpr_modify::partition_init_returning);
    seams::partition_init_on_conflict_update::set(
        execExpr_modify::partition_init_on_conflict_update,
    );
    seams::partition_init_merge_actions::set(execExpr_modify::partition_init_merge_actions);
    seams::exec_project_new_tuple::set(execExpr_modify::exec_project_new_tuple);
    seams::exec_init_with_check_options::set(execExpr_modify::exec_init_with_check_options);
    seams::exec_build_returning_projection::set(execExpr_modify::exec_build_returning_projection);
    seams::exec_build_on_conflict_set_projection::set(
        execExpr_modify::exec_build_on_conflict_set_projection,
    );
    seams::exec_init_on_conflict_where::set(execExpr_modify::exec_init_on_conflict_where);
    seams::exec_project_returning::set(execExpr_modify::exec_project_returning);

    // OUTWARD seam owned elsewhere but BACKED by the executor: the const-test
    // evaluator that optimizer/util/predtest.c's operator_predicate_proof runs
    // (CreateExecutorState → make_opclause → fix_opfuncids → ExecInitExpr →
    // ExecEvalExprSwitchContext → FreeExecutorState). Its real owner is the
    // executor (execExpr), so install it here, onto the predtest-seams crate.
    backend_optimizer_util_inherit_predtest_seams::eval_const_test::set(
        execExpr_core::eval_const_test,
    );

    // OUTWARD seam owned by clauses.c (optimizer) but BACKED by the executor:
    // evaluate_expr's fallback evaluator runs the constant expression through a
    // throwaway ExecInitExpr/ExecEvalExpr (CreateExecutorState → fix_opfuncids →
    // ExecInitExpr → ExecEvalExprSwitchContext → makeConst → FreeExecutorState).
    // Its real owner is the executor (execExpr), so install it here.
    backend_optimizer_util_clauses_seams::evaluate_expr_fallback::set(
        execExpr_core::evaluate_expr_fallback,
    );
}
