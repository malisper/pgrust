//! The fmgr selectivity dispatch (`call_oprrest` / `call_oprjoin` /
//! `call_func_selectivity_support`) — the boundary plancat's
//! `restriction_selectivity` / `join_selectivity` / `function_selectivity` reach
//! through.
//!
//! In C these are `OidFunctionCall4Coll(oprrest, ...)` /
//! `OidFunctionCall5Coll(oprjoin, ...)` — a generic fmgr call on the operator's
//! registered `oprrest`/`oprjoin` `regproc`. This repo has no fmgr registry for
//! the built-in C selectivity estimators, so the dispatch resolves the
//! `oprrest`/`oprjoin` OID to the corresponding ported estimator by its
//! `pg_proc` OID (the established `F_*`-constant pattern). selfuncs owns these
//! estimators (`eqsel`/`scalarltsel`/...) and the cross-cycle siblings
//! (`rangesel`/`networksel`/`arraycontsel`/...), so it is the natural installer.
//!
//! An `oprrest`/`oprjoin` OID with no ported estimator is seam-and-panic
//! (mirror-PG-and-panic): the operator's selectivity function is unported.

use types_core::primitive::Oid;
use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo, SpecialJoinInfo};

use backend_optimizer_util_plancat_ext_seams as ext;

use crate::entry::{eqsel, neqsel, scalargesel, scalargtsel, scalarlesel, scalarltsel};
use crate::join::{eqjoinsel, neqjoinsel};

/* fmgroids.h OIDs (pg_proc.dat, PostgreSQL 18.3) — the restriction- and
 * join-selectivity estimators dispatched here. */
const F_EQSEL: Oid = 101;
const F_NEQSEL: Oid = 102;
const F_SCALARLTSEL: Oid = 103;
const F_SCALARGTSEL: Oid = 104;
const F_SCALARLESEL: Oid = 336;
const F_SCALARGESEL: Oid = 337;
const F_EQJOINSEL: Oid = 105;
const F_NEQJOINSEL: Oid = 106;
const F_SCALARLTJOINSEL: Oid = 107;
const F_SCALARGTJOINSEL: Oid = 108;
const F_SCALARLEJOINSEL: Oid = 386;
const F_SCALARGEJOINSEL: Oid = 398;
const F_MATCHINGSEL: Oid = 5040;
const F_MATCHINGJOINSEL: Oid = 5041;

/// `DEFAULT_MATCHING_SEL` (selfuncs.h) — default selectivity for "match"-style
/// operators (text search, jsonb containment) without a dedicated estimator.
const DEFAULT_MATCHING_SEL: f64 = 0.010;
const F_RANGESEL: Oid = 3169;
const F_MULTIRANGESEL: Oid = 4243;
const F_NETWORKSEL: Oid = 3560;
const F_NETWORKJOINSEL: Oid = 3561;
const F_ARRAYCONTSEL: Oid = 3817;
const F_AREASEL: Oid = 139;
const F_AREAJOINSEL: Oid = 140;
const F_POSITIONSEL: Oid = 1300;
const F_POSITIONJOINSEL: Oid = 1301;
const F_CONTSEL: Oid = 1302;
const F_CONTJOINSEL: Oid = 1303;

/// `OidFunctionCall4Coll(oprrest, inputcollid, root, operatorid, args,
/// varRelid)` (plancat.c `restriction_selectivity` body) — dispatch a
/// restriction-selectivity estimator by its `oprrest` OID.
pub fn call_oprrest<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    oprrest: Oid,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    var_relid: i32,
) -> PgResult<f64> {
    // The estimator's detoasted-stats allocations live in a per-call context
    // (the result is a scalar f64), matching C running these in the planner's
    // memory context.
    let cx = mcx::MemoryContext::new("selfuncs restriction estimate");
    let mcx = cx.mcx();
    match oprrest {
        F_EQSEL => eqsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_NEQSEL => neqsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARLTSEL => scalarltsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARLESEL => scalarlesel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARGTSEL => scalargtsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARGESEL => scalargesel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_RANGESEL => backend_utils_adt_range_selfuncs::range::rangesel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        F_MULTIRANGESEL => backend_utils_adt_range_selfuncs::multirange::multirangesel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        F_NETWORKSEL => backend_utils_adt_network_selfuncs::networksel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        F_ARRAYCONTSEL => backend_utils_adt_array_selfuncs::arraycontsel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        // geo_selfuncs.c — bogus constant geometric restriction estimators
        // (ignore all arguments, return a fixed selectivity).
        F_AREASEL => Ok(backend_utils_adt_geo_selfuncs::areasel()),
        F_POSITIONSEL => Ok(backend_utils_adt_geo_selfuncs::positionsel()),
        F_CONTSEL => Ok(backend_utils_adt_geo_selfuncs::contsel()),
        // matchingsel (selfuncs.c) — generic restriction selectivity logic with
        // the DEFAULT_MATCHING_SEL fallback (used by `@@`/text-search and the
        // jsonb containment operators that have no dedicated estimator).
        F_MATCHINGSEL => crate::misc::generic_restriction_selectivity(
            mcx,
            run,
            root,
            operatorid,
            inputcollid,
            args,
            var_relid,
            DEFAULT_MATCHING_SEL,
        ),
        other => panic!(
            "selfuncs: call_oprrest dispatch has no ported estimator for oprrest OID {other} \
             (operator {operatorid}) — the operator's restriction-selectivity PGFunction is \
             unported"
        ),
    }
}

/// `OidFunctionCall5Coll(oprjoin, inputcollid, root, operatorid, args, jointype,
/// sjinfo)` (plancat.c `join_selectivity` body) — dispatch a join-selectivity
/// estimator by its `oprjoin` OID.
pub fn call_oprjoin<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    oprjoin: Oid,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    jointype: i16,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let cx = mcx::MemoryContext::new("selfuncs join estimate");
    let mcx = cx.mcx();
    // C's join estimators dereference `sjinfo` unconditionally (the planner
    // always passes a real SpecialJoinInfo for an operator join clause).
    let sjinfo = sjinfo.expect("call_oprjoin: NULL sjinfo for a join-selectivity estimator");
    let jt = jointype_from_i16(jointype);
    match oprjoin {
        F_EQJOINSEL => eqjoinsel(mcx, run, root, operatorid, args, inputcollid, sjinfo),
        F_NEQJOINSEL => neqjoinsel(mcx, run, root, operatorid, args, jt, inputcollid, sjinfo),
        F_NETWORKJOINSEL => backend_utils_adt_network_selfuncs::networkjoinsel(
            mcx, run, root, operatorid, args, sjinfo,
        ),
        // geo_selfuncs.c — bogus constant geometric join estimators (ignore all
        // arguments, return a fixed selectivity).
        F_AREAJOINSEL => Ok(backend_utils_adt_geo_selfuncs::areajoinsel()),
        F_POSITIONJOINSEL => Ok(backend_utils_adt_geo_selfuncs::positionjoinsel()),
        F_CONTJOINSEL => Ok(backend_utils_adt_geo_selfuncs::contjoinsel()),
        // scalar{lt,gt,le,ge}joinsel (selfuncs.c) — all just punt to
        // DEFAULT_INEQ_SEL (no inequality join-selectivity estimation).
        F_SCALARLTJOINSEL | F_SCALARGTJOINSEL | F_SCALARLEJOINSEL | F_SCALARGEJOINSEL => {
            Ok(types_selfuncs::DEFAULT_INEQ_SEL)
        }
        // matchingjoinsel (selfuncs.c) — "just punt, for the moment" to
        // DEFAULT_MATCHING_SEL.
        F_MATCHINGJOINSEL => Ok(DEFAULT_MATCHING_SEL),
        other => panic!(
            "selfuncs: call_oprjoin dispatch has no ported estimator for oprjoin OID {other} \
             (operator {operatorid}) — the operator's join-selectivity PGFunction is unported"
        ),
    }
}

/// `function_selectivity`'s `SupportRequestSelectivity` dispatch over
/// `get_func_support(funcid)`. No support-function estimator is ported, so this
/// is seam-and-panic — but `function_selectivity` only reaches it when the
/// function actually has a `prosupport` (else it returns the 0.3333333
/// default before calling), so a function without support never panics.
pub fn call_func_selectivity_support<'mcx>(
    _run: &PlannerRun<'mcx>,
    _root: &mut PlannerInfo,
    funcid: Oid,
    _args: &[NodeId],
    _inputcollid: Oid,
    _is_join: bool,
    _var_relid: i32,
    _jointype: i16,
    _sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<Option<f64>> {
    panic!(
        "selfuncs: call_func_selectivity_support is unported — the SupportRequestSelectivity \
         support-function dispatch for funcid {funcid} (the prosupport selectivity estimators) \
         has no owner; function_selectivity reaches this only when get_func_support(funcid) is set"
    )
}

/// Map the wire `int16` jointype back to the planner [`JoinType`] enum.
fn jointype_from_i16(jt: i16) -> types_pathnodes::JoinType {
    use types_pathnodes::{
        JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_SEMI,
        JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
    };
    match jt as i32 {
        x if x == JOIN_INNER as i32 => JOIN_INNER,
        x if x == JOIN_LEFT as i32 => JOIN_LEFT,
        x if x == JOIN_FULL as i32 => JOIN_FULL,
        x if x == JOIN_RIGHT as i32 => JOIN_RIGHT,
        x if x == JOIN_SEMI as i32 => JOIN_SEMI,
        x if x == JOIN_ANTI as i32 => JOIN_ANTI,
        x if x == JOIN_RIGHT_ANTI as i32 => JOIN_RIGHT_ANTI,
        x if x == JOIN_UNIQUE_OUTER as i32 => JOIN_UNIQUE_OUTER,
        x if x == JOIN_UNIQUE_INNER as i32 => JOIN_UNIQUE_INNER,
        other => panic!("call_oprjoin: unrecognized join type {other}"),
    }
}

/// Install the plancat selectivity-dispatch seams this unit owns.
pub fn init_dispatch_seams() {
    ext::call_oprrest::set(call_oprrest);
    ext::call_oprjoin::set(call_oprjoin);
    ext::call_func_selectivity_support::set(call_func_selectivity_support);
}
