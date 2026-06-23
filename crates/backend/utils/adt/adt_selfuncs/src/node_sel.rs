//! The per-clause-node restriction-selectivity estimators of selfuncs.c that
//! `clausesel.c` dispatches to directly (not through the `oprrest` fmgr path):
//! `boolvarsel`, `booltestsel`, `nulltestsel`, and `rowcomparesel`.
//!
//! In C these are called from `clause_selectivity_ext()` for `Var` (boolean),
//! `BooleanTest`, `NullTest`, and `RowCompareExpr` clause nodes. The repo
//! reaches them through the clausesel-owned seams in
//! [`path_small_seams`] (`boolvarsel` / `booltestsel` /
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

use ::mcx::Mcx;
use ::types_core::primitive::{InvalidOid, Oid};
use ::types_error::PgResult;
use ::nodes::primnodes::Expr;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{JoinType, NodeId, PlannerInfo, SpecialJoinInfo};

use joinpath_seams as jp;
use path_small_seams as clausesel;
use relnode_seams as rel_seams;
use arrayfuncs_seams as arr;
use lsyscache_seams as lsc;
use typcache_seams as tc;

use ::nodes::primnodes::CaseTestExpr;

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
    let arg_id: NodeId = root.alloc_node(arg.clone_in(mcx)?);
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
            &types_tuple::heaptuple::Datum::from_bool(true),
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
    let arg_id: NodeId = root.alloc_node(arg.clone_in(mcx)?);
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
    let arg_id: NodeId = root.alloc_node(arg.clone_in(mcx)?);
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
 * scalararraysel (selfuncs.c:1823)
 * ------------------------------------------------------------------------- */

/// `F_EQSEL` (fmgroids.h) — `eqsel`'s pg_proc OID.
const F_EQSEL: Oid = 101;
/// `F_NEQSEL` (fmgroids.h) — `neqsel`'s pg_proc OID.
const F_NEQSEL: Oid = 102;
/// `F_EQJOINSEL` (fmgroids.h) — `eqjoinsel`'s pg_proc OID.
const F_EQJOINSEL: Oid = 105;
/// `F_NEQJOINSEL` (fmgroids.h) — `neqjoinsel`'s pg_proc OID.
const F_NEQJOINSEL: Oid = 106;

/// `strip_array_coercion(node)` (selfuncs.c:1790) — peel binary-compatible
/// `ArrayCoerceExpr` / `RelabelType` wrappers off an array-valued expression.
pub(crate) fn strip_array_coercion<'a, 'mcx>(node: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    let mut node = node;
    loop {
        if let Some(acoerce) = node.as_arraycoerceexpr() {
            // If the per-element expression is just a RelabelType on top of
            // CaseTestExpr, then we know it's a binary-compatible relabeling.
            let is_binary_relabel = acoerce
                .elemexpr
                .as_deref()
                .and_then(|e| e.as_relabeltype())
                .and_then(|r| r.arg.as_deref())
                .map(|a| a.is_casetestexpr())
                .unwrap_or(false);
            if is_binary_relabel {
                if let Some(arg) = acoerce.arg.as_deref() {
                    node = arg;
                    continue;
                }
            }
            break;
        } else if let Some(relabel) = node.as_relabeltype() {
            // We don't really expect this case, but may as well cope.
            if let Some(arg) = relabel.arg.as_deref() {
                node = arg;
                continue;
            }
            break;
        } else {
            break;
        }
    }
    node
}

/// Apply the operator's selectivity estimator to a single `[leftop, elem]` arg
/// pair (C's per-element `FunctionCall4Coll`/`FunctionCall5Coll` over the
/// operator's `oprrest`/`oprjoin`). The installed `restriction_selectivity` /
/// `join_selectivity` seams perform the same `get_oprrest(operator)` +
/// `FunctionCallNColl` the C inner loop does.
#[allow(clippy::too_many_arguments)]
fn saop_element_selec<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    leftop: &Expr<'mcx>,
    elem: &Expr<'mcx>,
    inputcollid: Oid,
    is_join_clause: bool,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let args = [leftop.clone(), elem.clone()];
    if is_join_clause {
        clausesel::join_selectivity::call(run, root, operator, &args, inputcollid, jointype, sjinfo)
    } else {
        clausesel::restriction_selectivity::call(run, root, operator, &args, inputcollid, var_relid)
    }
}

/// `scalararraysel(root, clause, is_join_clause, varRelid, jointype, sjinfo)`
/// (selfuncs.c) — selectivity of a `ScalarArrayOpExpr` (`x = ANY(array)`,
/// `x <> ALL(array)`, etc.). 1:1 with the C body. The per-element operator
/// estimation runs through the installed `restriction_selectivity` /
/// `join_selectivity` seams, which look up the operator's `oprrest`/`oprjoin`
/// and invoke it exactly as the C inner loop's `FunctionCall4Coll` does.
pub(crate) fn scalararraysel<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    clause: &Expr<'a>,
    is_join_clause: bool,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let saop = match clause.as_scalararrayopexpr() {
        Some(s) => s,
        None => panic!("scalararraysel: expected ScalarArrayOpExpr, got {clause:?}"),
    };
    let operator = saop.opno;
    let use_or = saop.useOr;
    let inputcollid = saop.inputcollid;
    let mut is_equality = false;
    let mut is_inequality = false;

    // First, deconstruct the expression. Assert(list_length(clause->args) == 2).
    assert_eq!(saop.args.len(), 2, "scalararraysel: SAOP must have 2 args");
    let leftop = saop.args[0].clone();
    let rightop = saop.args[1].clone();

    // Aggressively reduce both sides to constants. The fold seam yields the
    // planner-arena `'static` form; bring each into this run's `mcx` (invariant
    // `Expr`) for the downstream `'mcx`-keyed selectivity helpers.
    let leftop: Expr<'mcx> = clausesel::estimate_expression_value::call(run, root, &leftop)?.clone_in(mcx)?;
    let rightop: Expr<'mcx> = clausesel::estimate_expression_value::call(run, root, &rightop)?.clone_in(mcx)?;

    // Get nominal (after relabeling) element type of rightop.
    let nominal_element_type =
        lsc::get_base_element_type::call(nodes_core::nodefuncs::expr_type(Some(&rightop))?)?;
    if nominal_element_type == InvalidOid {
        // probably shouldn't happen
        return Ok(0.5);
    }
    // Get nominal collation, too, for generating constants.
    let nominal_element_collation =
        nodes_core::nodefuncs::expr_collation(Some(&rightop))?;

    // Look through any binary-compatible relabeling of rightop.
    let rightop = strip_array_coercion(&rightop).clone();

    // Detect whether the operator is the default equality or inequality operator
    // of the array element type.
    let eq_opr = tc::lookup_type_cache_eq_opr::call(nominal_element_type)?;
    if eq_opr != InvalidOid {
        if operator == eq_opr {
            is_equality = true;
        } else if lsc::get_negator::call(operator)? == eq_opr {
            is_inequality = true;
        }
    }

    // If it is equality or inequality, we might be able to estimate this as a
    // form of array containment; for instance "const = ANY(column)" can be
    // treated as "ARRAY[const] <@ column". scalararraysel_containment tries that,
    // returning the selectivity estimate if successful, or -1 if not.
    if (is_equality || is_inequality) && !is_join_clause {
        let leftop_id: NodeId = root.alloc_node(leftop.clone());
        let rightop_id: NodeId = root.alloc_node(rightop.clone());
        let s1 = array_selfuncs::scalararraysel_containment(
            mcx,
            run,
            root,
            leftop_id,
            rightop_id,
            nominal_element_type,
            is_equality,
            use_or,
            var_relid,
        )?;
        if s1 >= 0.0 {
            return Ok(s1);
        }
    }

    // Look up the underlying operator's selectivity estimator. Punt if it hasn't
    // got one.
    let oprsel = if is_join_clause {
        lsc::get_oprjoin::call(operator)?
    } else {
        lsc::get_oprrest::call(operator)?
    };
    if oprsel == InvalidOid {
        return Ok(0.5);
    }

    // In the array-containment check above, we must only believe an operator is
    // equality/inequality if it is the default btree equality operator (or its
    // negator). But here we can be laxer, and also believe that any operator
    // using eqsel()/neqsel() as selectivity estimator acts like equality.
    if oprsel == F_EQSEL || oprsel == F_EQJOINSEL {
        is_equality = true;
    } else if oprsel == F_NEQSEL || oprsel == F_NEQJOINSEL {
        is_inequality = true;
    }

    let mut s1: f64;

    if let Some(c) = rightop.as_const() {
        // Case 1: rightop is an Array constant.
        if c.constisnull {
            // qual can't succeed if null array
            return Ok(0.0);
        }
        let arraydatum = c.constvalue.clone();

        // get_typlenbyvalalign(ARR_ELEMTYPE) — for a base-element Const array the
        // nominal element type equals the array header's element type.
        let s = lsc::get_typlenbyvalalign::call(nominal_element_type)?;
        let deconstructed = arr::deconstruct_array_v::call(
            mcx,
            arraydatum,
            nominal_element_type,
            s.typlen,
            s.typbyval,
            s.typalign as core::ffi::c_char,
        )?;

        // For generic operators we assume independence per element. But for
        // "= ANY" / "<> ALL", if the elements are distinct the probabilities are
        // disjoint, so we just sum them (with a sanity check on the result).
        let init = if use_or { 0.0 } else { 1.0 };
        s1 = init;
        let mut s1disjoint = init;

        for (elem_value, elem_isnull) in deconstructed.iter() {
            let elem = Expr::Const(nodes_core::makefuncs::make_const(
                mcx,
                nominal_element_type,
                -1,
                nominal_element_collation,
                s.typlen as i32,
                elem_value.clone(),
                *elem_isnull,
                s.typbyval,
            )?);
            let s2 = saop_element_selec(
                run, root, operator, &leftop, &elem, inputcollid, is_join_clause, var_relid,
                jointype, sjinfo,
            )?;

            if use_or {
                s1 = s1 + s2 - s1 * s2;
                if is_equality {
                    s1disjoint += s2;
                }
            } else {
                s1 *= s2;
                if is_inequality {
                    s1disjoint += s2 - 1.0;
                }
            }
        }

        // accept disjoint-probability estimate if in range
        if (if use_or { is_equality } else { is_inequality })
            && (0.0..=1.0).contains(&s1disjoint)
        {
            s1 = s1disjoint;
        }
    } else if let Some(ae) = rightop.as_arrayexpr().filter(|a| !a.multidims) {
        // Case 2: rightop is a non-multidim ARRAY[] construct.
        let (elmlen, _elmbyval) = lsc::get_typlenbyval::call(ae.element_typeid)?;
        let _ = elmlen;

        let init = if use_or { 0.0 } else { 1.0 };
        s1 = init;
        let mut s1disjoint = init;

        for elem in ae.elements.iter() {
            // Theoretically, if elem isn't of nominal_element_type we should
            // insert a RelabelType, but it seems unlikely any estimator cares.
            let s2 = saop_element_selec(
                run, root, operator, &leftop, elem, inputcollid, is_join_clause, var_relid,
                jointype, sjinfo,
            )?;

            if use_or {
                s1 = s1 + s2 - s1 * s2;
                if is_equality {
                    s1disjoint += s2;
                }
            } else {
                s1 *= s2;
                if is_inequality {
                    s1disjoint += s2 - 1.0;
                }
            }
        }

        if (if use_or { is_equality } else { is_inequality })
            && (0.0..=1.0).contains(&s1disjoint)
        {
            s1 = s1disjoint;
        }
    } else {
        // Case 3: otherwise, make a guess. We need a dummy rightop that doesn't
        // look like a constant; CaseTestExpr is a convenient choice.
        let dummyexpr = Expr::CaseTestExpr(CaseTestExpr {
            typeId: nominal_element_type,
            typeMod: -1,
            collation: inputcollid,
        });
        let s2 = saop_element_selec(
            run, root, operator, &leftop, &dummyexpr, inputcollid, is_join_clause, var_relid,
            jointype, sjinfo,
        )?;
        s1 = if use_or { 0.0 } else { 1.0 };

        // Arbitrarily assume 10 elements in the eventual array value (see also
        // estimate_array_length). We don't risk a disjoint-probability assumption.
        for _ in 0..10 {
            if use_or {
                s1 = s1 + s2 - s1 * s2;
            } else {
                s1 *= s2;
            }
        }
    }

    // result should be in range, but make sure...
    Ok(clamp_probability(s1))
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
    // OOM/detoast channel = the planner's own `run` arena (`run.mcx()`); with the
    // now-invariant `Node`/`Expr` carriers a shorter-lived local context no longer
    // unifies with `run`'s `'mcx`, and the planner cxt is the correct context.
    let mcx = run.mcx();
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
    let mcx = run.mcx();
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
    let mcx = run.mcx();
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

/// Seam body for `scalararraysel`.
pub fn seam_scalararraysel<'mcx, 'a>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    clause: &Expr<'a>,
    is_join_clause: bool,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let mcx = run.mcx();
    scalararraysel(
        mcx, run, root, clause, is_join_clause, var_relid, jointype, sjinfo,
    )
}
