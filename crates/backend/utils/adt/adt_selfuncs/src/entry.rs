//! The restriction-selectivity entry points of selfuncs.c — `eqsel` / `neqsel`
//! (via `eqsel_internal`) and `scalarltsel` / `scalarlesel` / `scalargtsel` /
//! `scalargesel` (via `scalarineqsel_wrapper`).
//!
//! In C these are `PGFunction`s dispatched through `get_oprrest` /
//! `OidFunctionCall*`; the repo reaches them through the `call_oprrest`
//! (plancat) seam. The decode of the raw `fcinfo` words into the typed planner
//! references (`root`, `operator`, `args`, `varRelid`, `collation`) happens at
//! that dispatch boundary, so these entry points take the already-decoded
//! arguments and orchestrate the ported estimation math.
//!
//! They call [`crate::examine::get_restriction_variable`] to recognize the
//! clause shape and acquire the variable's statistics; that examine layer is
//! keystone-blocked (see [`crate::examine`]), so these entry points reach the
//! same panic. The post-recognition arithmetic ([`crate::scalar`],
//! [`crate::ineq`]) is fully ported and is what runs once the keystone lands.

use ::mcx::Mcx;
use ::types_core::primitive::{Oid, OidIsValid};
use ::types_error::PgResult;
use ::nodes::primnodes::Expr;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{NodeId, PlannerInfo};

use lsyscache_seams as lsc;

use crate::examine::get_restriction_variable;
use crate::ineq::scalarineqsel;
use crate::scalar::{var_eq_const, var_eq_non_const};
use crate::{DEFAULT_EQ_SEL, DEFAULT_INEQ_SEL_FROM_TYPES as DEFAULT_INEQ_SEL};

/// `eqsel_internal(fcinfo, negate)` (selfuncs.c) — common code for `eqsel`
/// (`negate == false`) and `neqsel` (`negate == true`). 1:1 with the C body.
pub fn eqsel_internal<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
    negate: bool,
) -> PgResult<f64> {
    // When asked about <>, estimate via the corresponding = operator.
    if negate {
        operator = lsc::get_negator::call(operator)?;
        if !OidIsValid(operator) {
            // Use default selectivity.
            return Ok(1.0 - DEFAULT_EQ_SEL);
        }
    }

    // If expression is not var = something or something = var, punt.
    let (vardata, other, varonleft) =
        match get_restriction_variable(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => {
                return Ok(if negate {
                    1.0 - DEFAULT_EQ_SEL
                } else {
                    DEFAULT_EQ_SEL
                })
            }
        };

    // We can do a lot better if the something is a constant.
    let selec = match &other {
        Expr::Const(c) => var_eq_const(
            mcx,
            root,
            &vardata,
            operator,
            collation,
            // Thread the canonical `Const.constvalue` (by-value `ByVal` or
            // by-reference `ByRef`) — the bare-word extraction is deferred to the
            // MCV-comparison point in `var_eq_const`, so a by-reference literal on
            // the no-stats path (e.g. `WHERE relname = 'pg_type'`) no longer
            // panics at this call site.
            &c.constvalue,
            c.constisnull,
            varonleft,
            negate,
        )?,
        _ => var_eq_non_const(mcx, root, &vardata, operator, collation, varonleft, negate)?,
    };

    crate::examine::release_variable_stats(vardata);

    Ok(selec)
}

/// `eqsel(PG_FUNCTION_ARGS)` (selfuncs.c) — selectivity of `=`.
pub fn eqsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    eqsel_internal(mcx, run, root, operator, args, var_relid, collation, false)
}

/// `neqsel(PG_FUNCTION_ARGS)` (selfuncs.c) — selectivity of `<>`.
pub fn neqsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    eqsel_internal(mcx, run, root, operator, args, var_relid, collation, true)
}

/// `scalarineqsel_wrapper(fcinfo, isgt, iseq)` (selfuncs.c) — common code for
/// the four scalar inequality selectivity estimators. 1:1 with the C body.
pub fn scalarineqsel_wrapper<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
    mut isgt: bool,
    iseq: bool,
) -> PgResult<f64> {
    // If not var op something or something op var, punt.
    let (vardata, other, varonleft) =
        match get_restriction_variable(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(DEFAULT_INEQ_SEL),
        };

    // Can't do anything useful if the something is not a constant.
    let c = match &other {
        Expr::Const(c) => c,
        _ => {
            crate::examine::release_variable_stats(vardata);
            return Ok(DEFAULT_INEQ_SEL);
        }
    };

    // If the constant is NULL, assume operator is strict and return zero.
    if c.constisnull {
        crate::examine::release_variable_stats(vardata);
        return Ok(0.0);
    }
    // `scalarineqsel` compares the constant against the column's histogram / MCV
    // slot values through the operator's comparison proc. The constant crosses
    // that fmgr boundary as its canonical image (a by-value word OR a
    // by-reference referent) via the by-reference-capable `*_datum` lane, so a
    // by-reference inequality const (`text`/`name`/`bytea`/`timetz`) reaches the
    // estimator intact rather than being collapsed to a bare word. On a
    // stats-free relation the MCV/histogram loops never run and `scalarineqsel`
    // returns `DEFAULT_INEQ_SEL` without needing the const at all.
    let constval = &c.constvalue;
    let consttype = c.consttype;

    // Force the var to be on the left to simplify logic in scalarineqsel.
    if !varonleft {
        operator = lsc::get_commutator::call(operator)?;
        if !OidIsValid(operator) {
            crate::examine::release_variable_stats(vardata);
            return Ok(DEFAULT_INEQ_SEL);
        }
        isgt = !isgt;
    }

    // The rest of the work is done by scalarineqsel().
    let selec = scalarineqsel(
        mcx, run, root, operator, isgt, iseq, collation, &vardata, constval, consttype,
    )?;

    crate::examine::release_variable_stats(vardata);

    Ok(selec)
}

/// `scalarltsel` (selfuncs.c) — selectivity of `<`.
pub fn scalarltsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    scalarineqsel_wrapper(mcx, run, root, operator, args, var_relid, collation, false, false)
}

/// `scalarlesel` (selfuncs.c) — selectivity of `<=`.
pub fn scalarlesel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    scalarineqsel_wrapper(mcx, run, root, operator, args, var_relid, collation, false, true)
}

/// `scalargtsel` (selfuncs.c) — selectivity of `>`.
pub fn scalargtsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    scalarineqsel_wrapper(mcx, run, root, operator, args, var_relid, collation, true, false)
}

/// `scalargesel` (selfuncs.c) — selectivity of `>=`.
pub fn scalargesel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
) -> PgResult<f64> {
    scalarineqsel_wrapper(mcx, run, root, operator, args, var_relid, collation, true, true)
}
