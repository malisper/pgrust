// backend-executor-execExpr + backend-executor-execExprInterp (execExpr.c
// compile-to-steps + execExprInterp.c). Ported step families: DONE_RETURN/
// DONE_NO_RETURN, INNER/OUTER/SCAN_FETCHSOME, INNER/OUTER/SCAN_VAR,
// ASSIGN_*_VAR, ASSIGN_TMP[_MAKE_RO], CONST, FUNCEXPR[_STRICT[_1|_2]], QUAL,
// PARAM_EXTERN/PARAM_EXEC (compile-resolved; ParamBind), MINMAX,
// BOOL_AND/OR/NOT, NULLTEST_ISNULL/ISNOTNULL/ROWISNULL/ROWISNOTNULL.
// Deferred families (loud-panic at compile): WHOLEROW RECORD leg,
// PARAM_CALLBACK, JUMP_* +
// BOOLTEST + CASE/COALESCE (eval_const_expressions folds the all-Const
// forms; non-const forms wait for their vocabularies), FUSAGE,
// SQLVALUEFUNCTION, row/array/subscript/domain/hash/json/xml/agg/window/
// subplan sets.
#![allow(clippy::too_many_arguments)]

extern crate alloc;

mod arrayops;
mod compile;
pub mod domain;
mod interp;
pub mod jit;
mod hstoresubs;
mod jsonbsubs;
mod xmlops;
mod steps;
#[cfg(test)]
mod tests;

pub use compile::{
    economy_window, EconomyWindow,
    erase_fn_expr, exec_build_agg_projection_info, exec_build_agg_projection_info_subplans,
    exec_build_agg_qual, exec_build_agg_qual_subplans, exec_build_agg_trans,
    exec_build_window_projection_info, exec_build_window_projection_info_subplans,
    exec_build_agg_trans_gsets, exec_build_agg_trans_hashed,
    exec_build_agg_trans_hashed_masked, exec_build_agg_trans_hashed_subplans,
    exec_build_agg_trans_mixed, exec_build_agg_trans_plain_masked,
    exec_build_agg_trans_subplans,
    exec_build_grouping_equal,
    exec_build_hash32_from_attrs, exec_build_hash32_from_exprs, exec_build_projection_info,
    exec_build_merge_projection_info_subplans, exec_build_projection_info_subplans,
    exec_init_expr, exec_init_expr_subplans,
    exec_init_expr_subplans_agg, exec_init_expr_with_case_test,
    exec_init_qual, exec_init_qual_subplans, expr_type, lane_scan_qual, AggBind, AggOrderedSpec,
    AggTransSpec, LaneBoolTest, LaneClause, LaneCmpClause, LaneCmpRhs, LaneQualShape, LaneSuffix,
    SubplanCompileEnv, WinBind, INDEX_VAR, INNER_VAR, OUTER_VAR,
};
pub use interp::{
    agg_datum_copy, agg_datum_replace, exec_eval_expr, exec_eval_expr_outcome, exec_project, exec_project_prearmed, exec_project_outcome,
    exec_project_returning, exec_project_returning_outcome, exec_qual, exec_qual_outcome, EvalOutcome, EvalSlots, QualOutcome,
    Resume, RetSlot, RetSlots, Suspension,
};
pub use steps::{
    agg_count_star_advance, qual_bitmap_cmp_const, qual_bitmap_contains, AggPerGroup, CmpOp,
    ExprState, GroupedColsCell, Kernel, OutRef, ProjArithOp, ProjKeyCall, ScanCmpClauses,
    ScanContainsClause, ScanProjCol, ScanProjCols, ScanProjExprKey, SlotSrc, Step,
    PROJ_KEY_MAX_ARGS, PROJ_KEY_MAX_CALLS, SCAN_CMP_MAX_CLAUSES, SCAN_PROJ_MAX_COLS,
};
pub use types_portal::params::ParamBind;
pub use xmlops::map_sql_value_to_xml_value;
pub use arrayops::ResMcx;

/// evaluate_expr (clauses.c): run a const-foldable expression once, Const-wrap.
pub fn evaluate_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    expr: types_nodes::Node<'mcx>,
    result_type: types_core::Oid,
    result_typmod: i32,
    result_collation: types_core::Oid,
) -> types_error::PgResult<types_nodes::Node<'mcx>> {
    use types_nodes::primnodes::Const;

    let mut state =
        exec_init_expr(mcx, Some(expr), ParamBind::NONE)?.expect("expr is Some");
    state.arm_result_mcx(mcx);
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    let r = exec_eval_expr(&mut state, &mut slots)?;

    let (typlen, typbyval) = lsyscache::get_typlenbyval(result_type)?;
    let constvalue = if r.isnull || typbyval {
        r.value
    } else {
        let p = r.value.as_usize() as *const u8;
        // SAFETY: non-null by-ref result datum: typlen bytes readable, or a
        // live varlena/cstring image for -1/-2.
        let bytes = unsafe {
            match typlen {
                -1 => core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)),
                -2 => {
                    let mut n = 0usize;
                    while *p.add(n) != 0 {
                        n += 1;
                    }
                    core::slice::from_raw_parts(p, n + 1)
                }
                l => core::slice::from_raw_parts(p, l as usize),
            }
        };
        datum::Datum::from_usize(mcx::slice_borrow_in(mcx, bytes)?.as_ptr() as usize)
    };

    types_nodes::Node::mk(
        mcx,
        Const {
            consttype: result_type,
            consttypmod: result_typmod,
            constcollid: result_collation,
            constlen: typlen as i32,
            constvalue,
            constisnull: r.isnull,
            constbyval: typbyval,
            location: -1,
        },
    )
}

pub fn init_seams() {
    clauses_seams::evaluate_expr::set(evaluate_expr);
    typcache_seams::domain_check_input::set(domain::domain_check_input);
}
