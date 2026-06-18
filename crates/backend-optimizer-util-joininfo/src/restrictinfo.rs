//! `optimizer/util/restrictinfo.c` — RestrictInfo node manipulation.

use alloc::vec::Vec;

use backend_nodes_core::makefuncs::{make_andclause, make_orclause};
use types_error::PgResult;
use types_nodes::primnodes::{Expr, OpExpr};
use types_pathnodes::{
    NodeId, PlannerInfo, RelId, Relids, RestrictInfo, RinfoId, VOLATILITY_UNKNOWN,
};

use types_core::primitive::Index;
use crate::bms;
use crate::ext_seam;

use types_nodes::primnodes::{AND_EXPR, OR_EXPR};
const INVALID_OID: types_core::primitive::Oid = 0;

/// `is_orclause(node)` (clauses.h).
#[inline]
fn is_orclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == OR_EXPR)
}

/// `is_andclause(node)` (clauses.h).
#[inline]
fn is_andclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == AND_EXPR)
}

/// `is_opclause(node)` (clauses.h): `OpExpr`/`DistinctExpr`/`NullIfExpr`.
#[inline]
fn is_opclause(node: &Expr) -> bool {
    matches!(
        node,
        Expr::OpExpr(_) | Expr::DistinctExpr(_) | Expr::NullIfExpr(_)
    )
}

/// `((OpExpr *) clause)->args` for an opclause.
#[inline]
fn opclause_args(clause: &Expr) -> &Vec<Expr> {
    match clause {
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => &o.args,
        _ => unreachable!("opclause_args on non-OpExpr"),
    }
}

/// `make_restrictinfo`
///
/// Build a RestrictInfo node containing the given subexpression.
pub fn make_restrictinfo(
    root: &mut PlannerInfo,
    clause: Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> PgResult<RinfoId> {
    // If it's an OR clause, build a modified copy with RestrictInfos inserted
    // above each subclause of the top-level AND/OR structure.
    if is_orclause(&clause) {
        return make_sub_restrictinfos(
            root,
            clause,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        );
    }

    // Shouldn't be an AND clause, else AND/OR flattening messed up.
    debug_assert!(!is_andclause(&clause));

    make_plain_restrictinfo(
        root,
        clause,
        None,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        required_relids,
        incompatible_relids,
        outer_relids,
    )
}

/// `make_plain_restrictinfo`
///
/// Common code for the main entry points and the recursive cases.
pub fn make_plain_restrictinfo(
    root: &mut PlannerInfo,
    clause: Expr,
    orclause: Option<Expr>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> PgResult<RinfoId> {
    let mut left_relids: Relids = None;
    let mut right_relids: Relids = None;
    let clause_relids: Relids;
    let mut can_join = false;

    // If it's potentially delayable by lower-level security quals, figure out
    // whether it's leakproof.  We can skip testing this for level-zero quals.
    let leakproof = if security_level > 0 {
        !ext_seam::contain_leaked_vars::call(&clause)?
    } else {
        false /* really, "don't know" */
    };

    // If it's a binary opclause, set up left/right relids info.  In any case set
    // up the total clause relids info.
    if is_opclause(&clause) && opclause_args(&clause).len() == 2 {
        let args = opclause_args(&clause);
        left_relids = ext_seam::pull_varnos_expr::call(root, &args[0]);
        right_relids = ext_seam::pull_varnos_expr::call(root, &args[1]);

        clause_relids = bms::relids_union::call(&left_relids, &right_relids);

        // Does it look like a normal join clause, i.e., a binary operator
        // relating expressions from distinct relations?
        if !bms::relids_is_empty::call(&left_relids)
            && !bms::relids_is_empty::call(&right_relids)
            && !bms::relids_overlap::call(&left_relids, &right_relids)
        {
            can_join = true;
            // pseudoconstant should certainly not be true.
            debug_assert!(!pseudoconstant);
        }
    } else {
        // Not a binary opclause: mark left/right relid sets as empty and get the
        // total relid set the hard way.
        clause_relids = ext_seam::pull_varnos_expr::call(root, &clause);
    }

    // required_relids defaults to clause_relids.
    let required_relids = if required_relids.is_some() {
        required_relids
    } else {
        bms::relids_copy::call(&clause_relids)
    };

    // Count the number of base rels appearing in clause_relids: delete rels
    // mentioned in root->outer_join_rels and count the survivors.
    let baserels = bms::relids_difference::call(&clause_relids, &root.outer_join_rels);
    let num_base_rels = bms::relids_num_members::call(&baserels);

    // Label this RestrictInfo with a fresh serial number.
    root.last_rinfo_serial += 1;
    let rinfo_serial = root.last_rinfo_serial;

    // Intern the clause / orclause expression nodes.
    let clause_id: NodeId = root.alloc_node(clause);
    let orclause_id: Option<NodeId> = orclause.map(|e| root.alloc_node(e));

    let restrictinfo = RestrictInfo {
        clause: clause_id,
        orclause: orclause_id,
        is_pushed_down,
        pseudoconstant,
        has_clone,
        is_clone,
        can_join,
        security_level,
        incompatible_relids,
        outer_relids,
        leakproof,
        has_volatile: VOLATILITY_UNKNOWN,
        left_relids,
        right_relids,
        clause_relids,
        required_relids,
        num_base_rels,
        rinfo_serial,
        // Cacheable fields with "not yet set" markers.
        parent_ec: None,
        eval_cost: types_pathnodes::QualCost {
            startup: -1.0,
            per_tuple: 0.0,
        },
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: Vec::new(),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: Vec::new(),
        outer_is_left: false,
        hashjoinoperator: INVALID_OID,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: INVALID_OID,
        right_hasheqoperator: INVALID_OID,
    };

    Ok(root.alloc_rinfo(restrictinfo))
}

/// Recursively insert sub-RestrictInfo nodes into a boolean expression
/// (the C `make_sub_restrictinfos` entry called by `make_restrictinfo` for an OR
/// clause). We put RestrictInfos above simple (non-AND/OR) clauses and above
/// sub-OR clauses, but not above sub-AND clauses. The top-level OR returns a
/// `RinfoId` (the C cast of the `make_plain_restrictinfo` result to
/// `RestrictInfo *`); the recursive arms return `Expr` via
/// [`make_sub_restrictinfos_expr`].
fn make_sub_restrictinfos(
    root: &mut PlannerInfo,
    clause: Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> PgResult<RinfoId> {
    // Top level is an OR clause (checked by the caller). Build the orlist of
    // sub-results, then the plain RestrictInfo wrapping the original clause with
    // its make_orclause(orlist) attached.
    debug_assert!(is_orclause(&clause));
    let args = match &clause {
        Expr::BoolExpr(b) => b.args.clone(),
        _ => unreachable!(),
    };
    let mut orlist: Vec<Expr> = Vec::with_capacity(args.len());
    for arg in args {
        orlist.push(make_sub_restrictinfos_expr(
            root,
            arg,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            None, /* OR-clause constituents default to contained rels */
            incompatible_relids.clone(),
            outer_relids.clone(),
        )?);
    }
    let orclause = make_orclause(orlist);
    make_plain_restrictinfo(
        root,
        clause,
        Some(orclause),
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        required_relids,
        incompatible_relids,
        outer_relids,
    )
}

/// The `Expr`-returning recursion of `make_sub_restrictinfos` (mirrors the C
/// `static Expr *make_sub_restrictinfos`). Exactly as the C builds a
/// `RestrictInfo *` and casts it to `Expr *` to live inside the OR/AND arg list,
/// a non-AND clause builds a plain RestrictInfo (in the arena) and returns it
/// embedded as [`Expr::RestrictInfo`] (the [`RinfoRef`] handle); an AND clause
/// returns the rebuilt bare AND clause (no RestrictInfo above it).
///
/// [`RinfoRef`]: types_nodes::primnodes::RinfoRef
fn make_sub_restrictinfos_expr(
    root: &mut PlannerInfo,
    clause: Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> PgResult<Expr> {
    if is_orclause(&clause) {
        let args = match &clause {
            Expr::BoolExpr(b) => b.args.clone(),
            _ => unreachable!(),
        };
        let mut orlist: Vec<Expr> = Vec::with_capacity(args.len());
        for arg in args {
            orlist.push(make_sub_restrictinfos_expr(
                root,
                arg,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                None,
                incompatible_relids.clone(),
                outer_relids.clone(),
            )?);
        }
        let orclause = make_orclause(orlist);
        let rid = make_plain_restrictinfo(
            root,
            clause,
            Some(orclause),
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        )?;
        Ok(Expr::RestrictInfo(rid.as_expr_ref()))
    } else if is_andclause(&clause) {
        let args = match &clause {
            Expr::BoolExpr(b) => b.args.clone(),
            _ => unreachable!(),
        };
        let mut andlist: Vec<Expr> = Vec::with_capacity(args.len());
        for arg in args {
            andlist.push(make_sub_restrictinfos_expr(
                root,
                arg,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                required_relids.clone(),
                incompatible_relids.clone(),
                outer_relids.clone(),
            )?);
        }
        Ok(make_andclause(andlist))
    } else {
        let rid = make_plain_restrictinfo(
            root,
            clause,
            None,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        )?;
        Ok(Expr::RestrictInfo(rid.as_expr_ref()))
    }
}

/// `commute_restrictinfo`
///
/// Given a RestrictInfo containing a binary opclause, produce a RestrictInfo
/// representing the commutation of that clause.
pub fn commute_restrictinfo(
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    comm_op: types_core::primitive::Oid,
) -> RinfoId {
    let ri = root.rinfo(rinfo).clone();
    let clause_node = root.node(ri.clause).clone();
    let clause: OpExpr = match clause_node {
        Expr::OpExpr(o) => o,
        _ => panic!("commute_restrictinfo: clause is not an OpExpr"),
    };
    debug_assert!(clause.args.len() == 2);

    // flat-copy all the fields of clause ... and adjust those we need to change.
    let mut newclause = clause.clone();
    newclause.opno = comm_op;
    newclause.opfuncid = INVALID_OID;
    // list_make2(lsecond(args), linitial(args)) — swap the two args.
    newclause.args = alloc::vec![clause.args[1].clone(), clause.args[0].clone()];

    let new_clause_id = root.alloc_node(Expr::OpExpr(newclause));

    // flat-copy all the fields of rinfo, then adjust.
    let mut result = ri.clone();
    result.clause = new_clause_id;
    result.left_relids = ri.right_relids.clone();
    result.right_relids = ri.left_relids.clone();
    debug_assert!(ri.orclause.is_none());
    result.left_ec = ri.right_ec;
    result.right_ec = ri.left_ec;
    result.left_em = ri.right_em;
    result.right_em = ri.left_em;
    result.scansel_cache = Vec::new(); // not worth updating this
    if ri.hashjoinoperator == clause.opno {
        result.hashjoinoperator = comm_op;
    } else {
        result.hashjoinoperator = INVALID_OID;
    }
    result.left_bucketsize = ri.right_bucketsize;
    result.right_bucketsize = ri.left_bucketsize;
    result.left_mcvfreq = ri.right_mcvfreq;
    result.right_mcvfreq = ri.left_mcvfreq;
    result.left_hasheqoperator = INVALID_OID;
    result.right_hasheqoperator = INVALID_OID;

    root.alloc_rinfo(result)
}

/// `restriction_is_or_clause`: t iff the restrictinfo node contains an OR clause.
pub fn restriction_is_or_clause(root: &PlannerInfo, restrictinfo: RinfoId) -> bool {
    root.rinfo(restrictinfo).orclause.is_some()
}

/// `restriction_is_securely_promotable`: true if it's okay to evaluate this
/// clause "early", before other restriction clauses attached to the rel.
pub fn restriction_is_securely_promotable(
    root: &PlannerInfo,
    restrictinfo: RinfoId,
    rel: RelId,
) -> bool {
    let ri = root.rinfo(restrictinfo);
    // Okay if there are no baserestrictinfo clauses for the rel that would need
    // to go before this one, *or* if this one is leakproof.
    ri.security_level <= root.rel(rel).baserestrict_min_security || ri.leakproof
}

/// `rinfo_is_constant_true`: clause is constant TRUE (boolean type).
fn rinfo_is_constant_true(root: &PlannerInfo, rinfo: &RestrictInfo) -> bool {
    match root.node(rinfo.clause) {
        Expr::Const(c) => !c.constisnull && c.constvalue.as_bool(),
        _ => false,
    }
}

/// `get_actual_clauses`: bare clauses (as interned `NodeId`s) from the list.
///
/// Only valid where none of the RestrictInfos can be pseudoconstant.
pub fn get_actual_clauses(root: &PlannerInfo, restrictinfo_list: &[RinfoId]) -> Vec<NodeId> {
    let mut result = Vec::new();
    for &rid in restrictinfo_list {
        let rinfo = root.rinfo(rid);
        debug_assert!(!rinfo.pseudoconstant);
        debug_assert!(!rinfo_is_constant_true(root, rinfo));
        result.push(rinfo.clause);
    }
    result
}

/// `extract_actual_clauses`: bare clauses, either the regular ones or the
/// pseudoconstant ones per `pseudoconstant`; constant-TRUE clauses dropped.
pub fn extract_actual_clauses(
    root: &PlannerInfo,
    restrictinfo_list: &[RinfoId],
    pseudoconstant: bool,
) -> Vec<NodeId> {
    let mut result = Vec::new();
    for &rid in restrictinfo_list {
        let rinfo = root.rinfo(rid);
        if rinfo.pseudoconstant == pseudoconstant && !rinfo_is_constant_true(root, rinfo) {
            result.push(rinfo.clause);
        }
    }
    result
}

/// `extract_actual_join_clauses`: bare clauses, separating those that match the
/// join level from those pushed down. Pseudoconstant/constant-TRUE excluded.
/// Returns `(joinquals, otherquals)`.
pub fn extract_actual_join_clauses(
    root: &PlannerInfo,
    restrictinfo_list: &[RinfoId],
    joinrelids: &Relids,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut joinquals = Vec::new();
    let mut otherquals = Vec::new();

    for &rid in restrictinfo_list {
        let rinfo = root.rinfo(rid);
        if rinfo_is_pushed_down(rinfo, joinrelids) {
            if !rinfo.pseudoconstant && !rinfo_is_constant_true(root, rinfo) {
                otherquals.push(rinfo.clause);
            }
        } else {
            // joinquals shouldn't have been marked pseudoconstant.
            debug_assert!(!rinfo.pseudoconstant);
            if !rinfo_is_constant_true(root, rinfo) {
                joinquals.push(rinfo.clause);
            }
        }
    }
    (joinquals, otherquals)
}

/// `RINFO_IS_PUSHED_DOWN(rinfo, joinrelids)` (restrictinfo.h): the clause is
/// pushed-down if explicitly flagged, OR its required_relids isn't a subset of
/// the joinrelids (so it didn't originate at this join level).
#[inline]
fn rinfo_is_pushed_down(rinfo: &RestrictInfo, joinrelids: &Relids) -> bool {
    rinfo.is_pushed_down || !bms::relids_is_subset::call(&rinfo.required_relids, joinrelids)
}

/// `clause_sides_match_join(rinfo, outerrelids, innerrelids)` (restrictinfo.h
/// static inline) — the clause is a binary opclause referencing only the rels in
/// the current join; check whether it has the form
/// "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr" rather
/// than mixing outer and inner vars on either side. On a match, set the transient
/// `outer_is_left` flag to identify which side is which and return true.
pub fn clause_sides_match_join(
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    outerrelids: &Relids,
    innerrelids: &Relids,
) -> bool {
    let (left_relids, right_relids) = {
        let ri = root.rinfo(rinfo);
        (ri.left_relids.clone(), ri.right_relids.clone())
    };

    if bms::relids_is_subset::call(&left_relids, outerrelids)
        && bms::relids_is_subset::call(&right_relids, innerrelids)
    {
        // lefthand side is outer
        root.rinfo_mut(rinfo).outer_is_left = true;
        true
    } else if bms::relids_is_subset::call(&left_relids, innerrelids)
        && bms::relids_is_subset::call(&right_relids, outerrelids)
    {
        // righthand side is outer
        root.rinfo_mut(rinfo).outer_is_left = false;
        true
    } else {
        false // no good for these input relations
    }
}

/// `join_clause_is_movable_to`: whether a join clause is a safe candidate for
/// parameterization of a scan on the specified base relation.
pub fn join_clause_is_movable_to(root: &PlannerInfo, rinfo: RinfoId, baserel: RelId) -> bool {
    let ri = root.rinfo(rinfo);
    let rel = root.rel(baserel);

    // Clause must physically reference target rel.
    if !bms::relids_is_member::call(rel.relid as i32, &ri.clause_relids) {
        return false;
    }

    // Cannot move an outer-join clause into the join's outer side.
    if bms::relids_is_member::call(rel.relid as i32, &ri.outer_relids) {
        return false;
    }

    // Target rel's Vars must not be nulled by any outer join.
    if bms::relids_overlap::call(&ri.clause_relids, &rel.nulling_relids) {
        return false;
    }

    // Clause must not use any rels with LATERAL references to this rel.
    if bms::relids_overlap::call(&rel.lateral_referencers, &ri.clause_relids) {
        return false;
    }

    // Ignore clones, too.
    if ri.is_clone {
        return false;
    }

    true
}

/// `join_clause_is_movable_into`: whether a join clause is movable and can be
/// evaluated within the current join context.
///
/// `currentrelids`: the relids of the proposed evaluation location.
/// `current_and_outer`: the union of currentrelids and the required_outer relids.
pub fn join_clause_is_movable_into(
    root: &PlannerInfo,
    rinfo: RinfoId,
    currentrelids: &Relids,
    current_and_outer: &Relids,
) -> bool {
    let ri = root.rinfo(rinfo);

    // Clause must be evaluable given available context.
    if !bms::relids_is_subset::call(&ri.clause_relids, current_and_outer) {
        return false;
    }

    // Clause must physically reference at least one target rel.
    if !bms::relids_overlap::call(currentrelids, &ri.clause_relids) {
        return false;
    }

    // Cannot move an outer-join clause into the join's outer side.
    if bms::relids_overlap::call(currentrelids, &ri.outer_relids) {
        return false;
    }

    true
}
