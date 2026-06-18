//! The per-clause-node restriction-selectivity estimators of selfuncs.c that
//! `clausesel.c` dispatches to directly (not through the `oprrest` fmgr path):
//! `boolvarsel`, `booltestsel`, `nulltestsel`, and `rowcomparesel`.
//!
//! In C these are called from `clause_selectivity_ext()` for `Var` (boolean),
//! `BooleanTest`, `NullTest`, and `RowCompareExpr` clause nodes. The repo
//! reaches them through the clausesel-owned seams in
//! [`backend_optimizer_path_small_seams`] (`boolvarsel` / `booltestsel` /
//! `nulltestsel` / `nulltestsel_var` / `rowcomparesel`), which this crate
//! installs from [`crate::init_seams`].
//!
//! `boolvarsel` / `booltestsel` / `nulltestsel` operate over a
//! [`VariableStatData`] acquired by [`crate::examine::examine_variable`]; the
//! arithmetic is a 1:1 port. `booltestsel`'s stats-absent branch and
//! `rowcomparesel` recurse back into clausesel / plancat through the
//! `clause_selectivity_node` / `restriction_selectivity` / `join_selectivity` /
//! `num_relids` seams (the C mutual recursion between clausesel.c and
//! selfuncs.c), which are already installed by their owners.

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{JoinType, NodeId, PlannerInfo, SpecialJoinInfo};

use backend_optimizer_path_joinpath_seams as jp;
use backend_optimizer_path_small_seams as clausesel;
use backend_optimizer_util_relnode_seams as rel_seams;
use backend_utils_cache_lsyscache_seams as lsc;

use crate::examine::{examine_variable, release_variable_stats};
use crate::scalar::{stats_tuple_stanullfrac, var_eq_const};
use crate::{clamp_probability, STATISTIC_KIND_MCV};

use types_selfuncs::{ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES};

/* ---------------------------------------------------------------------------
 * Constants the node estimators test directly (selfuncs.h / catalog OIDs /
 * primnodes.h enums).
 * ------------------------------------------------------------------------- */

/// `DEFAULT_UNK_SEL` (selfuncs.h) — default `IS NULL` / `IS UNKNOWN`
/// selectivity, `0.005`.
const DEFAULT_UNK_SEL: f64 = 0.005;
/// `DEFAULT_NOT_UNK_SEL` (selfuncs.h) — `1.0 - DEFAULT_UNK_SEL`.
const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

/// `BooleanEqualOperator` (pg_operator.dat) — the `bool = bool` operator OID,
/// `91`. `boolvarsel` estimates a boolean Var as `V = 't'` through it.
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;

// `NullTestType` (primnodes.h): IS_NULL = 0, IS_NOT_NULL = 1.
const IS_NULL: i32 = 0;
const IS_NOT_NULL: i32 = 1;

// `BoolTestType` (primnodes.h): IS_TRUE = 0, IS_NOT_TRUE = 1, IS_FALSE = 2,
// IS_NOT_FALSE = 3, IS_UNKNOWN = 4, IS_NOT_UNKNOWN = 5.
const IS_TRUE: i32 = 0;
const IS_NOT_TRUE: i32 = 1;
const IS_FALSE: i32 = 2;
const IS_NOT_FALSE: i32 = 3;
const IS_UNKNOWN: i32 = 4;
const IS_NOT_UNKNOWN: i32 = 5;

/* ---------------------------------------------------------------------------
 * boolvarsel (selfuncs.c:1520)
 * ------------------------------------------------------------------------- */

/// `boolvarsel(root, arg, varRelid)` (selfuncs.c) — selectivity of a Boolean
/// variable. 1:1 with the C body. `arg` is the already-decoded boolean
/// expression node (interned into the planner arena for `examine_variable`).
pub(crate) fn boolvarsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    arg: &Expr,
    var_relid: i32,
) -> PgResult<f64> {
    let arg_id: NodeId = root.alloc_node(arg.clone());
    let vardata = examine_variable(mcx, run, root, arg_id, var_relid)?;

    let selec = if vardata.stats_tuple.is_some() {
        // A boolean variable V is equivalent to the clause V = 't', so we
        // compute the selectivity as if that is what we have.
        var_eq_const(
            mcx,
            root,
            &vardata,
            BOOLEAN_EQUAL_OPERATOR,
            InvalidOid,
            // V = 't': the by-value `true` const as the canonical `ByVal` Datum.
            &types_tuple::backend_access_common_heaptuple::Datum::from_bool(true),
            false, // constisnull
            true,  // varonleft
            false, // negate
        )?
    } else {
        // Otherwise, the default estimate is 0.5
        0.5
    };

    release_variable_stats(vardata);
    Ok(selec)
}

/* ---------------------------------------------------------------------------
 * booltestsel (selfuncs.c:1548)
 * ------------------------------------------------------------------------- */

/// `booltestsel(root, booltesttype, arg, varRelid, jointype, sjinfo)`
/// (selfuncs.c) — selectivity of a `BooleanTest` node. 1:1 with the C body.
pub(crate) fn booltestsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    booltesttype: i32,
    arg: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let arg_id: NodeId = root.alloc_node(arg.clone());
    let vardata = examine_variable(mcx, run, root, arg_id, var_relid)?;

    let mut selec: f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        let freq_null = stats_tuple_stanullfrac(stats_tuple) as f64;

        let slot = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;

        let have_mcv = slot
            .as_ref()
            .map(|s| !s.numbers.is_empty())
            .unwrap_or(false);

        if have_mcv {
            let slot = slot.as_ref().unwrap();

            // Get first MCV frequency and derive frequency for true.
            let freq_true = if slot.values[0].as_bool() {
                slot.numbers[0] as f64
            } else {
                1.0 - slot.numbers[0] as f64 - freq_null
            };

            // Next derive frequency for false. Then use these as appropriate to
            // derive frequency for each case.
            let freq_false = 1.0 - freq_true - freq_null;

            selec = match booltesttype {
                IS_UNKNOWN => freq_null,              // select only NULL values
                IS_NOT_UNKNOWN => 1.0 - freq_null,    // select non-NULL values
                IS_TRUE => freq_true,                 // select only TRUE values
                IS_NOT_TRUE => 1.0 - freq_true,       // select non-TRUE values
                IS_FALSE => freq_false,               // select only FALSE values
                IS_NOT_FALSE => 1.0 - freq_false,     // select non-FALSE values
                other => {
                    panic!("unrecognized booltesttype: {other}");
                }
            };
        } else {
            // No most-common-value info available. Still have null fraction
            // information, so use it for IS [NOT] UNKNOWN. Otherwise adjust for
            // null fraction and assume a 50-50 split of TRUE and FALSE.
            selec = match booltesttype {
                IS_UNKNOWN => freq_null,           // select only NULL values
                IS_NOT_UNKNOWN => 1.0 - freq_null, // select non-NULL values
                // Assume we select half of the non-NULL values
                IS_TRUE | IS_FALSE => (1.0 - freq_null) / 2.0,
                // Assume we select NULLs plus half of the non-NULLs
                // (equiv. to freq_null + (1.0 - freq_null) / 2.0)
                IS_NOT_TRUE | IS_NOT_FALSE => (freq_null + 1.0) / 2.0,
                other => {
                    panic!("unrecognized booltesttype: {other}");
                }
            };
        }
        // The slot's Drop is the C free_attstatsslot.
        drop(slot);
    } else {
        // If we can't get variable statistics for the argument, perhaps
        // clause_selectivity can do something with it. We ignore the
        // possibility of a NULL value when using clause_selectivity, and just
        // assume the value is either TRUE or FALSE.
        selec = match booltesttype {
            IS_UNKNOWN => DEFAULT_UNK_SEL,
            IS_NOT_UNKNOWN => DEFAULT_NOT_UNK_SEL,
            IS_TRUE | IS_NOT_FALSE => {
                clausesel::clause_selectivity_node::call(
                    run, root, arg, var_relid, jointype, sjinfo,
                )?
            }
            IS_FALSE | IS_NOT_TRUE => {
                1.0 - clausesel::clause_selectivity_node::call(
                    run, root, arg, var_relid, jointype, sjinfo,
                )?
            }
            other => {
                panic!("unrecognized booltesttype: {other}");
            }
        };
    }

    release_variable_stats(vardata);

    // result should be in range, but make sure...
    Ok(clamp_probability(selec))
}

/* ---------------------------------------------------------------------------
 * nulltestsel (selfuncs.c:1706)
 * ------------------------------------------------------------------------- */

/// `nulltestsel(root, nulltesttype, arg, varRelid, jointype, sjinfo)`
/// (selfuncs.c) — selectivity of a `NullTest` node. 1:1 with the C body.
/// (`jointype` / `sjinfo` are part of the C signature but not read; kept to
/// preserve it.)
#[allow(unused_variables)]
pub(crate) fn nulltestsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    nulltesttype: i32,
    arg: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let arg_id: NodeId = root.alloc_node(arg.clone());
    let vardata = examine_variable(mcx, run, root, arg_id, var_relid)?;

    let mut selec: f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        let freq_null = stats_tuple_stanullfrac(stats_tuple) as f64;

        selec = match nulltesttype {
            // Use freq_null directly.
            IS_NULL => freq_null,
            // Select not unknown (not null) values. Calculate from freq_null.
            IS_NOT_NULL => 1.0 - freq_null,
            other => {
                panic!("unrecognized nulltesttype: {other}");
            }
        };
    } else if root
        .node(vardata.var)
        .as_var()
        .map(|v| v.varattno < 0)
        .unwrap_or(false)
    {
        // There are no stats for system columns, but we know they are never
        // NULL.
        selec = if nulltesttype == IS_NULL { 0.0 } else { 1.0 };
    } else {
        // No ANALYZE stats available, so make a guess
        selec = match nulltesttype {
            IS_NULL => DEFAULT_UNK_SEL,
            IS_NOT_NULL => DEFAULT_NOT_UNK_SEL,
            other => {
                panic!("unrecognized nulltesttype: {other}");
            }
        };
    }

    release_variable_stats(vardata);

    // result should be in range, but make sure...
    Ok(clamp_probability(selec))
}

/* ---------------------------------------------------------------------------
 * rowcomparesel (selfuncs.c:2213)
 * ------------------------------------------------------------------------- */

/// `rowcomparesel(root, clause, varRelid, jointype, sjinfo)` (selfuncs.c) —
/// selectivity of a `RowCompareExpr` node. We estimate by considering just the
/// first (high order) columns, which makes it equivalent to an ordinary OpExpr.
/// 1:1 with the C body.
pub(crate) fn rowcomparesel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    clause: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let rc = match clause {
        Expr::RowCompareExpr(rc) => rc,
        other => panic!("rowcomparesel: expected RowCompareExpr, got {other:?}"),
    };

    let opno = rc.opnos[0]; // linitial_oid(clause->opnos)
    let inputcollid = rc.inputcollids[0]; // linitial_oid(clause->inputcollids)

    // Build equivalent arg list for single operator.
    let opargs: [Expr; 2] = [rc.largs[0].clone(), rc.rargs[0].clone()];

    // Decide if it's a join clause. This should match clausesel.c's
    // treat_as_join_clause(), except that we intentionally consider only the
    // leading columns and not the rest of the clause.
    let is_join_clause = if var_relid != 0 {
        // Caller is forcing restriction mode (eg, because we are examining an
        // inner indexscan qual).
        false
    } else if sjinfo.is_none() {
        // It must be a restriction clause, since it's being evaluated at a scan
        // node.
        false
    } else {
        // Otherwise, it's a join if there's more than one base relation used.
        // C: NumRelids(root, (Node *) opargs) > 1. NumRelids over the two-element
        // arg list = bms_num_members(pull_varnos over both leading args); there
        // is no List Expr, so we union pull_varnos of the two interned args.
        let larg_id: NodeId = root.alloc_node(opargs[0].clone());
        let rarg_id: NodeId = root.alloc_node(opargs[1].clone());
        let lvarnos = jp::pull_varnos::call(root, larg_id);
        let rvarnos = jp::pull_varnos::call(root, rarg_id);
        let varnos = rel_seams::relids_union::call(&lvarnos, &rvarnos);
        rel_seams::relids_num_members::call(&varnos) > 1
    };

    let s1 = if is_join_clause {
        // Estimate selectivity for a join clause.
        clausesel::join_selectivity::call(run, root, opno, &opargs, inputcollid, jointype, sjinfo)?
    } else {
        // Estimate selectivity for a restriction clause.
        clausesel::restriction_selectivity::call(run, root, opno, &opargs, inputcollid, var_relid)?
    };

    Ok(s1)
}

/* ---------------------------------------------------------------------------
 * Seam bodies (installed by crate::init_seams). The clausesel seams do not
 * carry an Mcx; the stats estimators run their detoast allocations in a per-call
 * planner-scoped memory context, matching C running them in the planner cxt.
 * ------------------------------------------------------------------------- */

/// Seam body for `boolvarsel`.
pub fn seam_boolvarsel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    arg: &Expr,
    var_relid: i32,
) -> PgResult<f64> {
    let cx = mcx::MemoryContext::new("selfuncs boolvarsel estimate");
    let mcx = cx.mcx();
    boolvarsel(mcx, run, root, arg, var_relid)
}

/// Seam body for `booltestsel`.
pub fn seam_booltestsel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    booltesttype: i32,
    arg: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let cx = mcx::MemoryContext::new("selfuncs booltestsel estimate");
    let mcx = cx.mcx();
    booltestsel(mcx, run, root, booltesttype, arg, var_relid, jointype, sjinfo)
}

/// Seam body for `nulltestsel` (both the `nulltestsel` and `nulltestsel_var`
/// seam forms — the C function is identical; the seam split only reflects the
/// two clausesel call shapes).
pub fn seam_nulltestsel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    nulltesttype: i32,
    arg: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let cx = mcx::MemoryContext::new("selfuncs nulltestsel estimate");
    let mcx = cx.mcx();
    nulltestsel(mcx, run, root, nulltesttype, arg, var_relid, jointype, sjinfo)
}

/// Seam body for `rowcomparesel`.
pub fn seam_rowcomparesel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    clause: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    rowcomparesel(run, root, clause, var_relid, jointype, sjinfo)
}
