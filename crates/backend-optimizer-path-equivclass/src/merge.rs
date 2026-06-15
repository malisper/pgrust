//! equivclass.c — the union-find merge core (`process_equivalence`), the
//! expression canonicaliser, the EM constructors, and `get_eclass_for_sort_expr`.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Index, Oid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    EcId, EmId, EquivalenceClass, EquivalenceMember, JoinDomain, PlannerInfo, Relids, RinfoId,
    RELOPT_BASEREL,
};

use backend_optimizer_path_equivclass_ext_seams as ec_seam;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as cat;

use crate::derives::{ec_add_derived_clauses, ec_clear_derived_clauses};
use crate::relevance::{live_ec_ids, new_iterator};

/// `RECORDOID` (pg_type_d.h).
const RECORDOID: Oid = 2249;
/// `COERCE_IMPLICIT_CAST` (primnodes.h CoercionForm).
const COERCE_IMPLICIT_CAST: i32 = 2;
/// `UINT_MAX` for `ec_min_security` initialisation.
const UINT_MAX_INDEX: Index = Index::MAX;

/// `IsPolymorphicType(typid)` (pseudotypes). The polymorphic OIDs.
fn is_polymorphic_type(typid: Oid) -> bool {
    /* pg_type_d.h: ANY* polymorphic pseudo-types */
    const ANYELEMENTOID: Oid = 2283;
    const ANYARRAYOID: Oid = 2277;
    const ANYNONARRAYOID: Oid = 2776;
    const ANYENUMOID: Oid = 3500;
    const ANYRANGEOID: Oid = 3831;
    const ANYMULTIRANGEOID: Oid = 4537;
    const ANYCOMPATIBLEOID: Oid = 5077;
    const ANYCOMPATIBLEARRAYOID: Oid = 5078;
    const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
    const ANYCOMPATIBLERANGEOID: Oid = 5080;
    const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/* ======================================================================
 * canonicalize_ec_expression (equivclass.c:544)
 * ==================================================================== */

/// `canonicalize_ec_expression(expr, req_type, req_collation)` — ensure `expr`
/// exposes the EC's expected type/collation, adding a `RelabelType` if needed.
pub fn canonicalize_ec_expression(expr: Expr, req_type: Oid, req_collation: Oid) -> Expr {
    let expr_type = ec_seam::expr_type::call(&expr);

    /* polymorphic / RECORD opclasses keep the same exposed type */
    let req_type = if is_polymorphic_type(req_type) || req_type == RECORDOID {
        expr_type
    } else {
        req_type
    };

    if expr_type != req_type || ec_seam::expr_collation::call(&expr) != req_collation {
        let req_typmod = if expr_type != req_type {
            -1
        } else {
            ec_seam::expr_typmod::call(&expr)
        };
        ec_seam::apply_relabel_type::call(
            expr,
            req_type,
            req_typmod,
            req_collation,
            COERCE_IMPLICIT_CAST,
            -1,
            false,
        )
    } else {
        expr
    }
}

/* ======================================================================
 * make_eq_member / add_eq_member / add_child_eq_member
 * (equivclass.c:591, 627, 660)
 * ==================================================================== */

/// `make_eq_member(ec, expr, relids, jdomain, parent, datatype)` — build an EM
/// (without adding it to an EC). If `parent` is `None` it's a parent member,
/// else a child member. May set `ec_has_const` via the caller's `ec` (returns a
/// flag instead, applied by the caller, to keep borrows simple).
fn make_eq_member(
    root: &mut PlannerInfo,
    expr: Expr,
    relids: Relids,
    jdomain: JoinDomain,
    parent: Option<EmId>,
    datatype: Oid,
) -> (EmId, bool) {
    let em_expr = root.alloc_node(expr);
    let mut em = EquivalenceMember {
        em_expr,
        em_relids: bms::relids_copy::call(&relids),
        em_is_const: false,
        em_is_child: parent.is_some(),
        em_datatype: datatype,
        em_jdomain: Some(alloc::boxed::Box::new(jdomain)),
        em_parent: parent,
    };

    let mut set_ec_has_const = false;
    if bms::relids_is_empty::call(&relids) {
        /* No Vars, assume pseudoconstant (correct for process_equivalence; the
         * sort path checks harder afterward). */
        debug_assert!(parent.is_none());
        em.em_is_const = true;
        set_ec_has_const = true;
    }
    let id = root.alloc_em(em);
    (id, set_ec_has_const)
}

/// `add_eq_member(ec, expr, relids, jdomain, datatype)` — build a non-child EM
/// and add it to `ec`.
fn add_eq_member(
    root: &mut PlannerInfo,
    ec: EcId,
    expr: Expr,
    relids: Relids,
    jdomain: JoinDomain,
    datatype: Oid,
) -> EmId {
    let (em, set_const) =
        make_eq_member(root, expr, bms::relids_copy::call(&relids), jdomain, None, datatype);

    let ecm = root.ec_mut(ec);
    ecm.ec_members.push(em);
    if set_const {
        ecm.ec_has_const = true;
    }
    /* record the relids for parent members */
    let cur = root.ec(ec).ec_relids.clone();
    let joined = bms::relids_add_members::call(cur, &relids);
    root.ec_mut(ec).ec_relids = joined;

    em
}

/// `add_child_eq_member(root, ec, ec_index, expr, relids, jdomain, parent_em,
/// datatype, child_relid)` (equivclass.c:660).
#[allow(clippy::too_many_arguments)]
pub(crate) fn add_child_eq_member(
    root: &mut PlannerInfo,
    ec: EcId,
    ec_index: i32,
    expr: Expr,
    relids: Relids,
    jdomain: JoinDomain,
    parent_em: EmId,
    datatype: Oid,
    child_relid: Index,
) -> EmId {
    /* allocate/extend the ec_childmembers array (indexed by relid) */
    let needed = root.simple_rel_array_size;
    {
        let ecm = root.ec_mut(ec);
        if ecm.ec_childmembers_size < needed {
            ecm.ec_childmembers
                .resize(needed as usize, Vec::new());
            ecm.ec_childmembers_size = needed;
        }
    }

    let (em, _set_const) =
        make_eq_member(root, expr, relids, jdomain, Some(parent_em), datatype);

    /* add member to the ec_childmembers list for child_relid */
    root.ec_mut(ec).ec_childmembers[child_relid as usize].push(em);

    /* record this EC index for the child rel */
    if ec_index >= 0 {
        if let Some(child_rel) = root.simple_rel_array[child_relid as usize] {
            let cur = root.rel(child_rel).eclass_indexes.clone();
            let added = bms::relids_add_member::call(cur, ec_index);
            root.rel_mut(child_rel).eclass_indexes = added;
        }
    }

    em
}

/* ======================================================================
 * process_equivalence (equivclass.c:179)
 * ==================================================================== */

/// `process_equivalence(root, &restrictinfo, jdomain)` — the union-find merge
/// core. Returns `(matched, restrictinfo)`; `restrictinfo` may differ from the
/// input (the X=X → X IS NOT NULL conversion).
pub fn process_equivalence(
    root: &mut PlannerInfo,
    restrictinfo: RinfoId,
    jdomain: Relids,
) -> PgResult<(bool, RinfoId)> {
    debug_assert!(root.rinfo(restrictinfo).left_ec.is_none());
    debug_assert!(root.rinfo(restrictinfo).right_ec.is_none());

    /* reject if potentially postponable by security considerations */
    if root.rinfo(restrictinfo).security_level > 0 && !root.rinfo(restrictinfo).leakproof {
        return Ok((false, restrictinfo));
    }

    /* extract info from the clause */
    let clause = root.node(root.rinfo(restrictinfo).clause).clone();
    let opexpr = clause
        .as_opexpr()
        .expect("process_equivalence: clause is not an OpExpr");
    let opno = opexpr.opno;
    let collation = opexpr.inputcollid;
    let item1_raw = opexpr.args[0].clone();
    let item2_raw = opexpr.args[1].clone();
    let item1_relids = root.rinfo(restrictinfo).left_relids.clone();
    let item2_relids = root.rinfo(restrictinfo).right_relids.clone();

    /* ensure both inputs expose the desired collation */
    let item1_type0 = ec_seam::expr_type::call(&item1_raw);
    let item1 = canonicalize_ec_expression(item1_raw, item1_type0, collation);
    let item2_type0 = ec_seam::expr_type::call(&item2_raw);
    let item2 = canonicalize_ec_expression(item2_raw, item2_type0, collation);

    /* X = X cannot become an EC */
    if ec_seam::equal::call(&item1, &item2) {
        /* if strict, treat as X IS NOT NULL */
        let opfuncid = cat::get_opcode::call(opno).expect("get_opcode");
        if cat::func_strict::call(opfuncid).expect("func_strict") {
            let ntest = ec_seam::make_is_not_null::call(item1);
            let ri = root.rinfo(restrictinfo).clone();
            let newri = ec_seam::make_restrictinfo::call(
                root,
                ntest,
                ri.is_pushed_down,
                ri.has_clone,
                ri.is_clone,
                ri.pseudoconstant,
                ri.security_level,
                None,
                ri.incompatible_relids.clone(),
                ri.outer_relids.clone(),
            )?;
            return Ok((false, newri));
        }
        return Ok((false, restrictinfo));
    }

    /* declared input types for opfamily lookup */
    let (item1_type, item2_type) = cat::op_input_types::call(opno).expect("op_input_types");

    let opfamilies = root.rinfo(restrictinfo).mergeopfamilies.clone();

    /* sweep ECs looking for matches to item1 and item2 */
    let mut ec1: Option<EcId> = None;
    let mut ec2: Option<EcId> = None;
    let mut em1: Option<EmId> = None;
    let mut em2: Option<EmId> = None;

    for cur_ec in live_ec_ids(root) {
        if root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        if collation != root.ec(cur_ec).ec_collation {
            continue;
        }
        if !opfamilies_equal(&opfamilies, &root.ec(cur_ec).ec_opfamilies) {
            continue;
        }
        debug_assert!(root.ec(cur_ec).ec_childmembers.is_empty());

        let members = root.ec(cur_ec).ec_members.clone();
        for cur_em in members {
            debug_assert!(!root.em(cur_em).em_is_child);

            /* match constants only within the same JoinDomain */
            if root.em(cur_em).em_is_const
                && !jdomain_eq(root, cur_em, &jdomain)
            {
                continue;
            }

            if ec1.is_none()
                && item1_type == root.em(cur_em).em_datatype
                && ec_seam::equal::call(&item1, &root.node(root.em(cur_em).em_expr).clone())
            {
                ec1 = Some(cur_ec);
                em1 = Some(cur_em);
                if ec2.is_some() {
                    break;
                }
            }
            if ec2.is_none()
                && item2_type == root.em(cur_em).em_datatype
                && ec_seam::equal::call(&item2, &root.node(root.em(cur_em).em_expr).clone())
            {
                ec2 = Some(cur_ec);
                em2 = Some(cur_em);
                if ec1.is_some() {
                    break;
                }
            }
        }
        if ec1.is_some() && ec2.is_some() {
            break;
        }
    }

    let security_level = root.rinfo(restrictinfo).security_level;

    match (ec1, ec2) {
        (Some(ec1), Some(ec2)) if ec1 == ec2 => {
            /* case 1: nothing to do but add to sources */
            ec_add_source(root, ec1, restrictinfo, security_level);
            mark_rinfo(root, restrictinfo, ec1, em1, em2);
            Ok((true, restrictinfo))
        }
        (Some(ec1), Some(ec2)) => {
            /* case 2: merge ec2 into ec1 */
            if root.ec_merging_done {
                panic!("too late to merge equivalence classes");
            }
            /* ec1->ec_members = list_concat(ec1, ec2) */
            let ec2_members = root.ec(ec2).ec_members.clone();
            root.ec_mut(ec1).ec_members.extend(ec2_members);
            let ec2_sources = root.ec(ec2).ec_sources.clone();
            root.ec_mut(ec1).ec_sources.extend(ec2_sources);
            /* append ec2's derived clauses into ec1 */
            let ec2_derives = root.ec(ec2).ec_derives_list.clone();
            ec_add_derived_clauses(root, ec1, &ec2_derives);
            /* ec1->ec_relids = bms_join(ec1, ec2) */
            let ec1r = root.ec(ec1).ec_relids.clone();
            let ec2r = root.ec(ec2).ec_relids.clone();
            root.ec_mut(ec1).ec_relids = bms::relids_join::call(ec1r, ec2r);
            let ec2_has_const = root.ec(ec2).ec_has_const;
            root.ec_mut(ec1).ec_has_const |= ec2_has_const;
            let (ec2_min, ec2_max) = (root.ec(ec2).ec_min_security, root.ec(ec2).ec_max_security);
            {
                let e = root.ec_mut(ec1);
                e.ec_min_security = e.ec_min_security.min(ec2_min);
                e.ec_max_security = e.ec_max_security.max(ec2_max);
            }
            root.ec_mut(ec2).ec_merged = Some(ec1);
            /* leave ec2 behind (arena handle stability) with cleared payload */
            root.ec_mut(ec2).ec_members = Vec::new();
            root.ec_mut(ec2).ec_sources = Vec::new();
            ec_clear_derived_clauses(root, ec2);
            root.ec_mut(ec2).ec_relids = None;

            ec_add_source(root, ec1, restrictinfo, security_level);
            mark_rinfo(root, restrictinfo, ec1, em1, em2);
            Ok((true, restrictinfo))
        }
        (Some(ec1), None) => {
            /* case 3: add item2 to ec1 */
            let jd = find_top_or_given_jdomain(&jdomain);
            let new_em2 = add_eq_member(root, ec1, item2, item2_relids, jd, item2_type);
            ec_add_source(root, ec1, restrictinfo, security_level);
            mark_rinfo(root, restrictinfo, ec1, em1, Some(new_em2));
            Ok((true, restrictinfo))
        }
        (None, Some(ec2)) => {
            /* case 3: add item1 to ec2 */
            let jd = find_top_or_given_jdomain(&jdomain);
            let new_em1 = add_eq_member(root, ec2, item1, item1_relids, jd, item1_type);
            ec_add_source(root, ec2, restrictinfo, security_level);
            mark_rinfo(root, restrictinfo, ec2, Some(new_em1), em2);
            Ok((true, restrictinfo))
        }
        (None, None) => {
            /* case 4: make a new two-entry EC */
            let ec = root.alloc_ec(EquivalenceClass {
                ec_opfamilies: opfamilies,
                ec_collation: collation,
                ec_childmembers_size: 0,
                ec_members: Vec::new(),
                ec_childmembers: Vec::new(),
                ec_sources: alloc::vec![restrictinfo],
                ec_derives_list: Vec::new(),
                ec_derives_hash: None,
                ec_relids: None,
                ec_has_const: false,
                ec_has_volatile: false,
                ec_broken: false,
                ec_sortref: 0,
                ec_min_security: security_level,
                ec_max_security: security_level,
                ec_merged: None,
            });
            let jd1 = find_top_or_given_jdomain(&jdomain);
            let new_em1 = add_eq_member(root, ec, item1, item1_relids, jd1, item1_type);
            let jd2 = find_top_or_given_jdomain(&jdomain);
            let new_em2 = add_eq_member(root, ec, item2, item2_relids, jd2, item2_type);
            mark_rinfo(root, restrictinfo, ec, Some(new_em1), Some(new_em2));
            Ok((true, restrictinfo))
        }
    }
}

fn ec_add_source(root: &mut PlannerInfo, ec: EcId, rinfo: RinfoId, security_level: Index) {
    let e = root.ec_mut(ec);
    e.ec_sources.push(rinfo);
    e.ec_min_security = e.ec_min_security.min(security_level);
    e.ec_max_security = e.ec_max_security.max(security_level);
}

fn mark_rinfo(
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    ec: EcId,
    em1: Option<EmId>,
    em2: Option<EmId>,
) {
    let r = root.rinfo_mut(rinfo);
    r.left_ec = Some(ec);
    r.right_ec = Some(ec);
    r.left_em = em1;
    r.right_em = em2;
}

/// The given `jdomain` (a `Relids` here) materialised into a [`JoinDomain`] for
/// EM construction. process_equivalence's C passes a `JoinDomain *`; the seam
/// boundary carries `jd_relids`, and a const EM stores it back for the
/// same-JoinDomain const-match test.
fn find_top_or_given_jdomain(jdomain: &Relids) -> JoinDomain {
    JoinDomain {
        jd_relids: jdomain.clone(),
    }
}

/// `cur_em->em_jdomain != jdomain` — compare a const EM's stored JoinDomain to
/// the active one (by `jd_relids`).
fn jdomain_eq(root: &PlannerInfo, em: EmId, jdomain: &Relids) -> bool {
    match &root.em(em).em_jdomain {
        Some(jd) => bms::relids_equal::call(&jd.jd_relids, jdomain),
        None => bms::relids_is_empty::call(jdomain),
    }
}

/// `equal(opfamilies_a, opfamilies_b)` for two OID lists (the C `equal()` over
/// `List *` of OIDs is element-wise equality of the same-length lists).
pub(crate) fn opfamilies_equal(a: &[Oid], b: &[Oid]) -> bool {
    a == b
}

/* ======================================================================
 * get_eclass_for_sort_expr (equivclass.c:736)
 * ==================================================================== */

/// `get_eclass_for_sort_expr(...)` — find or (optionally) build the EC for a
/// sort/group expression.
#[allow(clippy::too_many_arguments)]
pub fn get_eclass_for_sort_expr(
    root: &mut PlannerInfo,
    expr: Expr,
    opfamilies: Vec<Oid>,
    opcintype: Oid,
    collation: Oid,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> PgResult<Option<EcId>> {
    /* ensure the expression exposes the correct type and collation */
    let expr = canonicalize_ec_expression(expr, opcintype, collation);

    /* SortGroupClause expressions belong to the top JoinDomain */
    let jdomain = root
        .join_domains
        .first()
        .cloned()
        .expect("get_eclass_for_sort_expr: no join domains");

    /* scan existing ECs for a match */
    for cur_ec in live_ec_ids(root) {
        /* never match a volatile EC unless this is the same SortGroupClause */
        if root.ec(cur_ec).ec_has_volatile
            && (sortref == 0 || sortref != root.ec(cur_ec).ec_sortref)
        {
            continue;
        }
        if collation != root.ec(cur_ec).ec_collation {
            continue;
        }
        if !opfamilies_equal(&opfamilies, &root.ec(cur_ec).ec_opfamilies) {
            continue;
        }

        let mut it = new_iterator(root, cur_ec, &rel);
        while let Some(cur_em) = crate::relevance::eclass_member_iterator_next(root, &mut it) {
            /* ignore child members unless they match the request */
            if root.em(cur_em).em_is_child
                && !bms::relids_equal::call(&root.em(cur_em).em_relids, &rel)
            {
                continue;
            }
            /* match constants only within the same JoinDomain */
            if root.em(cur_em).em_is_const && !jdomain_eq(root, cur_em, &jdomain.jd_relids) {
                continue;
            }
            if opcintype == root.em(cur_em).em_datatype
                && ec_seam::equal::call(&expr, &root.node(root.em(cur_em).em_expr).clone())
            {
                return Ok(Some(cur_ec)); /* match! */
            }
        }
    }

    if !create_it {
        return Ok(None);
    }

    /* build a new single-member EC */
    let ec_has_volatile = ec_seam::contain_volatile_functions::call(&expr);
    if ec_has_volatile && sortref == 0 {
        panic!("volatile EquivalenceClass has no sortref");
    }
    let newec = root.alloc_ec(EquivalenceClass {
        ec_opfamilies: opfamilies,
        ec_collation: collation,
        ec_childmembers_size: 0,
        ec_members: Vec::new(),
        ec_childmembers: Vec::new(),
        ec_sources: Vec::new(),
        ec_derives_list: Vec::new(),
        ec_derives_hash: None,
        ec_relids: None,
        ec_has_const: false,
        ec_has_volatile,
        ec_broken: false,
        ec_sortref: sortref,
        ec_min_security: UINT_MAX_INDEX,
        ec_max_security: 0,
        ec_merged: None,
    });

    /* precise relids of the expression */
    let expr_relids = ec_seam::pull_varnos::call(root, &expr);
    let newem = add_eq_member(
        root,
        newec,
        expr.clone(),
        expr_relids,
        jdomain,
        opcintype,
    );

    /* re-check the const marking, which add_eq_member doesn't do for SRFs etc. */
    if root.ec(newec).ec_has_const
        && (root.ec(newec).ec_has_volatile
            || ec_seam::expression_returns_set::call(&expr)
            || ec_seam::contain_agg_clause::call(&expr)
            || ec_seam::contain_window_function::call(&expr))
    {
        root.ec_mut(newec).ec_has_const = false;
        root.em_mut(newem).em_is_const = false;
    }

    /* if EC merging is done, mop up eclass_indexes of mentioned rels */
    if root.ec_merging_done {
        let ec_index = newec.0 as i32;
        let ec_relids = root.ec(newec).ec_relids.clone();
        let mut i: i32 = -1;
        loop {
            i = bms::relids_next_member::call(&ec_relids, i);
            if i <= 0 {
                break;
            }
            if i == root.group_rtindex {
                continue;
            }
            match root.simple_rel_array[i as usize] {
                None => {
                    debug_assert!(bms::relids_is_member::call(i, &root.outer_join_rels));
                    continue;
                }
                Some(rel_id) => {
                    debug_assert!(root.rel(rel_id).reloptkind == RELOPT_BASEREL);
                    let cur = root.rel(rel_id).eclass_indexes.clone();
                    let added = bms::relids_add_member::call(cur, ec_index);
                    root.rel_mut(rel_id).eclass_indexes = added;
                }
            }
        }
    }

    Ok(Some(newec))
}

/// Resolve an [`EmId`]'s `em_expr` node to an owned [`Expr`].
pub(crate) fn em_expr(root: &PlannerInfo, em: EmId) -> Expr {
    root.node(root.em(em).em_expr).clone()
}

/// Strip outer `RelabelType`s from an [`Expr`], returning the inner expr.
pub(crate) fn strip_relabeltypes(mut node: &Expr) -> &Expr {
    while let Some(r) = node.as_relabeltype() {
        match r.arg.as_deref() {
            Some(arg) => node = arg,
            None => break,
        }
    }
    node
}

/// Resolve an EM and return a fresh [`EquivalenceMember`] value clone.
pub(crate) fn em_value(root: &PlannerInfo, em: EmId) -> EquivalenceMember {
    root.em(em).clone()
}
