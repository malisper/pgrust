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
use pathnodes::planner_run::PlannerRun;
use pathnodes::{NodeId, PlannerInfo, SpecialJoinInfo};

use plancat_ext_seams as ext;

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
/// `tsmatchsel` (ts_selfuncs.c) — `oprrest` of `tsvector @@ tsquery` /
/// `tsquery @@ tsvector` (pg_proc.dat OID 3686).
const F_TSMATCHSEL: Oid = 3686;
const F_ARRAYCONTSEL: Oid = 3817;
const F_AREASEL: Oid = 139;
const F_AREAJOINSEL: Oid = 140;
const F_POSITIONSEL: Oid = 1300;
const F_POSITIONJOINSEL: Oid = 1301;
const F_CONTSEL: Oid = 1302;
const F_CONTJOINSEL: Oid = 1303;

/* like_support.c — the LIKE / regex / prefix restriction-selectivity estimators
 * (fmgroids.h OIDs). */
const F_LIKESEL: Oid = 1819;
const F_ICLIKESEL: Oid = 1814;
const F_REGEXEQSEL: Oid = 1818;
const F_ICREGEXEQSEL: Oid = 1820;
const F_NLIKESEL: Oid = 1822;
const F_ICNLIKESEL: Oid = 1815;
const F_REGEXNESEL: Oid = 1821;
const F_ICREGEXNESEL: Oid = 1823;
const F_PREFIXSEL: Oid = 3437;

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
    // C runs the estimators in the planner's memory context (the result is a
    // scalar f64, but the estimators also intern arena nodes — e.g.
    // `examine_variable` sets `vardata->var = root.alloc_node(...)` — that ESCAPE
    // into `PlannerInfo::node_arena`, which outlives this call). Allocating those
    // escaping nodes in a throwaway per-call context and dropping it here leaves
    // the arena holding dangling boxes that under-charge their (freed) context at
    // planner teardown (TD-STATIC-EROSION). Use the planner context so the
    // interned nodes live exactly as long as the arena, matching C.
    let mcx = run.mcx();
    match oprrest {
        F_EQSEL => eqsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_NEQSEL => neqsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARLTSEL => scalarltsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARLESEL => scalarlesel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARGTSEL => scalargtsel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_SCALARGESEL => scalargesel(mcx, run, root, operatorid, args, var_relid, inputcollid),
        F_RANGESEL => range_selfuncs::range::rangesel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        F_MULTIRANGESEL => range_selfuncs::multirange::multirangesel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        F_NETWORKSEL => network_selfuncs::networksel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        // tsmatchsel (ts_selfuncs.c) — restriction selectivity of `@@`
        // (tsvector @@ tsquery). Ignores the input collation (C's `#ifdef
        // NOT_USED` operator + no collation use), so `inputcollid` is unused.
        F_TSMATCHSEL => {
            crate::tsmatchsel::tsmatchsel(mcx, run, root, operatorid, args, var_relid)
        }
        F_ARRAYCONTSEL => array_selfuncs::arraycontsel(
            mcx, run, root, operatorid, args, var_relid,
        ),
        // geo_selfuncs.c — bogus constant geometric restriction estimators
        // (ignore all arguments, return a fixed selectivity).
        F_AREASEL => Ok(geo_selfuncs::areasel()),
        F_POSITIONSEL => Ok(geo_selfuncs::positionsel()),
        F_CONTSEL => Ok(geo_selfuncs::contsel()),
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
        // like_support.c — the LIKE / regex / ILIKE / prefix restriction
        // estimators (`oprrest` of the `~~`/`~`/`~~*`/`~*` and negated families
        // plus the `^@` prefix operator). The `inputcollid` is the operator's
        // collation `patternsel` works with.
        F_LIKESEL => crate::patternsel::likesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_ICLIKESEL => crate::patternsel::iclikesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_REGEXEQSEL => crate::patternsel::regexeqsel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_ICREGEXEQSEL => crate::patternsel::icregexeqsel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_NLIKESEL => crate::patternsel::nlikesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_ICNLIKESEL => crate::patternsel::icnlikesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_REGEXNESEL => crate::patternsel::regexnesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_ICREGEXNESEL => crate::patternsel::icregexnesel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
        ),
        F_PREFIXSEL => crate::patternsel::prefixsel(
            mcx, run, root, operatorid, args, var_relid, inputcollid,
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
    // Use the planner context (not a throwaway per-call context): the join
    // estimators intern arena nodes via `examine_variable` that escape into
    // `PlannerInfo::node_arena` and must outlive this call (TD-STATIC-EROSION).
    let mcx = run.mcx();
    // C's join estimators dereference `sjinfo` unconditionally (the planner
    // always passes a real SpecialJoinInfo for an operator join clause).
    let sjinfo = sjinfo.expect("call_oprjoin: NULL sjinfo for a join-selectivity estimator");
    let jt = jointype_from_i16(jointype);
    match oprjoin {
        F_EQJOINSEL => eqjoinsel(mcx, run, root, operatorid, args, inputcollid, sjinfo),
        F_NEQJOINSEL => neqjoinsel(mcx, run, root, operatorid, args, jt, inputcollid, sjinfo),
        F_NETWORKJOINSEL => network_selfuncs::networkjoinsel(
            mcx, run, root, operatorid, args, sjinfo,
        ),
        // geo_selfuncs.c — bogus constant geometric join estimators (ignore all
        // arguments, return a fixed selectivity).
        F_AREAJOINSEL => Ok(geo_selfuncs::areajoinsel()),
        F_POSITIONJOINSEL => Ok(geo_selfuncs::positionjoinsel()),
        F_CONTJOINSEL => Ok(geo_selfuncs::contjoinsel()),
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
/// `get_func_support(funcid)`. The like_support.c pattern support functions
/// (`textlike_support`/`texticlike_support`/`textregexeq_support`/
/// `texticregexeq_support`/`text_starts_with_support`) implement a
/// `SupportRequestSelectivity` branch that shares code with the operator
/// restriction estimators via `patternsel_common`; this delegates to it.
/// Returns `Some(selectivity)` when this unit owns the support function, or
/// `None` (the C "support function fails, use default 0.3333333" path) for any
/// other prosupport.
///
/// `function_selectivity` only reaches here when `get_func_support(funcid)` is
/// valid (else it returns the default before calling).
pub fn call_func_selectivity_support<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    is_join: bool,
    var_relid: i32,
    jointype: i16,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<Option<f64>> {
    crate::patternsel::func_selectivity_support(
        run.mcx(),
        run,
        root,
        funcid,
        args,
        var_relid,
        inputcollid,
        is_join,
        jointype,
        sjinfo,
    )
}

/// Map the wire `int16` jointype back to the planner [`JoinType`] enum.
pub(crate) fn jointype_from_i16(jt: i16) -> pathnodes::JoinType {
    use pathnodes::{
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
