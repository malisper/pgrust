//! equivclass.c — join implied-equality generation, `create_join_clause`, the
//! per-column generator, and the outer-join reconsideration rewrites.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_pathnodes::{
    EcId, EmId, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo,
};

use backend_optimizer_path_equivclass_ext_seams as ec_seam;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as cat;

use crate::derives::{ec_add_derived_clause, ec_search_clause_for_ems};
use crate::merge::{em_expr, process_equivalence};
use crate::relevance::{
    find_join_domain, is_other_rel, live_ec_ids, new_iterator, oid_is_valid,
    select_equality_operator,
};

/* ======================================================================
 * create_join_clause (equivclass.c:1983)
 * ==================================================================== */

/// `create_join_clause(root, ec, opno, leftem, rightem, parent_ec)`
/// (equivclass.c:1983).
pub fn create_join_clause(
    root: &mut PlannerInfo,
    ec: EcId,
    opno: Oid,
    leftem: EmId,
    rightem: EmId,
    parent_ec: Option<EcId>,
) -> PgResult<RinfoId> {
    if let Some(rinfo) = ec_search_clause_for_ems(root, ec, leftem, Some(rightem), parent_ec) {
        return Ok(rinfo);
    }

    /* if either EM is a child, recursively create the parent-to-parent clause
     * so we can duplicate its rinfo_serial */
    let mut parent_rinfo: Option<RinfoId> = None;
    if root.em(leftem).em_is_child || root.em(rightem).em_is_child {
        let leftp = root.em(leftem).em_parent.unwrap_or(leftem);
        let rightp = root.em(rightem).em_parent.unwrap_or(rightem);
        parent_rinfo = Some(create_join_clause(root, ec, opno, leftp, rightp, parent_ec)?);
    }

    let item1 = em_expr(root, leftem);
    let item2 = em_expr(root, rightem);
    let collation = root.ec(ec).ec_collation;
    let min_security = root.ec(ec).ec_min_security;
    let qualscope = bms::relids_union::call(
        &root.em(leftem).em_relids,
        &root.em(rightem).em_relids,
    );

    let rinfo = ec_seam::build_implied_join_equality::call(
        root,
        opno,
        collation,
        item1,
        item2,
        qualscope,
        min_security,
    )?;

    /* if either EM is a child, force the clause's clause_relids to include
     * the child rel's relid(s) */
    if root.em(leftem).em_is_child {
        let lr = root.em(leftem).em_relids.clone();
        let cur = root.rinfo(rinfo).clause_relids.clone();
        root.rinfo_mut(rinfo).clause_relids = bms::relids_add_members::call(cur, &lr);
    }
    if root.em(rightem).em_is_child {
        let rr = root.em(rightem).em_relids.clone();
        let cur = root.rinfo(rinfo).clause_relids.clone();
        root.rinfo_mut(rinfo).clause_relids = bms::relids_add_members::call(cur, &rr);
    }

    /* if a child clause, copy the parent's rinfo_serial */
    if let Some(parent_rinfo) = parent_rinfo {
        let serial = root.rinfo(parent_rinfo).rinfo_serial;
        root.rinfo_mut(rinfo).rinfo_serial = serial;
    }

    /* mark redundancy + left/right EC + EMs */
    {
        let r = root.rinfo_mut(rinfo);
        r.parent_ec = parent_ec;
        r.left_ec = Some(ec);
        r.right_ec = Some(ec);
        r.left_em = Some(leftem);
        r.right_em = Some(rightem);
    }
    /* save for possible re-use */
    ec_add_derived_clause(root, ec, rinfo);

    Ok(rinfo)
}

/* ======================================================================
 * generate_join_implied_equalities (equivclass.c:1550)
 * ==================================================================== */

/// `generate_join_implied_equalities(root, join_relids, outer_relids,
/// inner_rel, sjinfo)` (equivclass.c:1550).
pub fn generate_join_implied_equalities(
    root: &mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: RelId,
    sjinfo: Option<SpecialJoinInfo>,
) -> PgResult<Vec<RinfoId>> {
    let inner_relids = root.rel(inner_rel).relids.clone();

    let (nominal_inner_relids, nominal_join_relids) = if is_other_rel(root.rel(inner_rel)) {
        debug_assert!(!bms::relids_is_empty::call(&root.rel(inner_rel).top_parent_relids));
        let nominal_inner = root.rel(inner_rel).top_parent_relids.clone();
        let mut nominal_join = bms::relids_union::call(&outer_relids, &nominal_inner);
        nominal_join =
            ec_seam::add_outer_joins_to_relids::call(root, nominal_join, sjinfo.clone());
        (nominal_inner, nominal_join)
    } else {
        (inner_relids.clone(), join_relids.clone())
    };

    /* examine the potentially-relevant eclasses */
    let matching_ecs = match &sjinfo {
        Some(sj) if sj.ojrelid != 0 => {
            crate::relevance::get_eclass_indexes_for_relids(root, &nominal_join_relids)
        }
        _ => crate::relevance::get_common_eclass_indexes(root, &nominal_inner_relids, &outer_relids),
    };

    let mut result: Vec<RinfoId> = Vec::new();
    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&matching_ecs, i);
        if i < 0 {
            break;
        }
        let ec = EcId(i as u32);

        if root.ec(ec).ec_has_const {
            continue;
        }
        if root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        debug_assert!(bms::relids_overlap::call(
            &root.ec(ec).ec_relids,
            &nominal_join_relids
        ));

        let mut sublist = if !root.ec(ec).ec_broken {
            generate_join_implied_equalities_normal(
                root,
                ec,
                &join_relids,
                &outer_relids,
                &inner_relids,
            )?
        } else {
            Vec::new()
        };

        if root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                root,
                ec,
                &nominal_join_relids,
                &outer_relids,
                &nominal_inner_relids,
                inner_rel,
            )?;
        }
        result.extend(sublist);
    }
    Ok(result)
}

/* ======================================================================
 * generate_join_implied_equalities_for_ecs (equivclass.c:1650)
 * ==================================================================== */

/// `generate_join_implied_equalities_for_ecs(root, eclasses, join_relids,
/// outer_relids, inner_rel)` (equivclass.c:1650). Assumes sjinfo == NULL.
pub fn generate_join_implied_equalities_for_ecs(
    root: &mut PlannerInfo,
    eclasses: Vec<EcId>,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: RelId,
) -> PgResult<Vec<RinfoId>> {
    let inner_relids = root.rel(inner_rel).relids.clone();

    let (nominal_inner_relids, nominal_join_relids) = if is_other_rel(root.rel(inner_rel)) {
        debug_assert!(!bms::relids_is_empty::call(&root.rel(inner_rel).top_parent_relids));
        let nominal_inner = root.rel(inner_rel).top_parent_relids.clone();
        let nominal_join = bms::relids_union::call(&outer_relids, &nominal_inner);
        (nominal_inner, nominal_join)
    } else {
        (inner_relids.clone(), join_relids.clone())
    };

    let mut result: Vec<RinfoId> = Vec::new();
    for ec in eclasses {
        if root.ec(ec).ec_has_const {
            continue;
        }
        if root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        if !bms::relids_overlap::call(&root.ec(ec).ec_relids, &nominal_join_relids) {
            continue;
        }

        let mut sublist = if !root.ec(ec).ec_broken {
            generate_join_implied_equalities_normal(
                root,
                ec,
                &join_relids,
                &outer_relids,
                &inner_relids,
            )?
        } else {
            Vec::new()
        };
        if root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                root,
                ec,
                &nominal_join_relids,
                &outer_relids,
                &nominal_inner_relids,
                inner_rel,
            )?;
        }
        result.extend(sublist);
    }
    Ok(result)
}

/* ======================================================================
 * generate_join_implied_equalities_normal (equivclass.c:1721)
 * ==================================================================== */

fn em_expr_is_var_shaped(root: &PlannerInfo, em: EmId) -> bool {
    let e = em_expr(root, em);
    e.as_var().is_some()
        || e.as_relabeltype()
            .and_then(|r| r.arg.as_deref())
            .map(|a| a.as_var().is_some())
            .unwrap_or(false)
}

fn generate_join_implied_equalities_normal(
    root: &mut PlannerInfo,
    ec: EcId,
    join_relids: &Relids,
    outer_relids: &Relids,
    inner_relids: &Relids,
) -> PgResult<Vec<RinfoId>> {
    let mut result: Vec<RinfoId> = Vec::new();
    let mut new_members: Vec<EmId> = Vec::new();
    let mut outer_members: Vec<EmId> = Vec::new();
    let mut inner_members: Vec<EmId> = Vec::new();

    /* classify members by where they are computable */
    let mut it = new_iterator(root, ec, join_relids);
    while let Some(cur_em) = crate::relevance::eclass_member_iterator_next(root, &mut it) {
        let em_relids = root.em(cur_em).em_relids.clone();
        if !bms::relids_is_subset::call(&em_relids, join_relids) {
            continue; /* not computable yet, or wrong child */
        }
        if bms::relids_is_subset::call(&em_relids, outer_relids) {
            outer_members.push(cur_em);
        } else if bms::relids_is_subset::call(&em_relids, inner_relids) {
            inner_members.push(cur_em);
        } else {
            new_members.push(cur_em);
        }
    }

    /* select the joinclause if needed (best outer/inner pair) */
    if !outer_members.is_empty() && !inner_members.is_empty() {
        let mut best_outer_em: Option<EmId> = None;
        let mut best_inner_em: Option<EmId> = None;
        let mut best_eq_op: Oid = 0;
        let mut best_score: i32 = -1;

        'outer: for &outer_em in &outer_members {
            for &inner_em in &inner_members {
                let eq_op = select_equality_operator(
                    root.ec(ec),
                    root.em(outer_em).em_datatype,
                    root.em(inner_em).em_datatype,
                );
                if !oid_is_valid(eq_op) {
                    continue;
                }
                let mut score = 0;
                if em_expr_is_var_shaped(root, outer_em) {
                    score += 1;
                }
                if em_expr_is_var_shaped(root, inner_em) {
                    score += 1;
                }
                let outer_type = ec_seam::expr_type::call(&em_expr(root, outer_em));
                if cat::op_hashjoinable::call(eq_op, outer_type).expect("op_hashjoinable") {
                    score += 1;
                }
                if score > best_score {
                    best_outer_em = Some(outer_em);
                    best_inner_em = Some(inner_em);
                    best_eq_op = eq_op;
                    best_score = score;
                    if best_score == 3 {
                        break 'outer;
                    }
                }
            }
        }
        if best_score < 0 {
            root.ec_mut(ec).ec_broken = true;
            return Ok(Vec::new());
        }
        /* set parent_ec to mark redundant */
        let rinfo = create_join_clause(
            root,
            ec,
            best_eq_op,
            best_outer_em.expect("best outer em"),
            best_inner_em.expect("best inner em"),
            Some(ec),
        )?;
        result.push(rinfo);
    }

    /* build restrictions for expressions involving Vars from both sides */
    if !new_members.is_empty() {
        let mut old_members = outer_members;
        old_members.extend(inner_members);
        /* arbitrarily take the first old member */
        if let Some(&first_old) = old_members.first() {
            new_members.push(first_old);
        }

        let mut prev_em: Option<EmId> = None;
        for &cur_em in &new_members {
            if let Some(prev) = prev_em {
                let eq_op = select_equality_operator(
                    root.ec(ec),
                    root.em(prev).em_datatype,
                    root.em(cur_em).em_datatype,
                );
                if !oid_is_valid(eq_op) {
                    root.ec_mut(ec).ec_broken = true;
                    return Ok(Vec::new());
                }
                /* do NOT set parent_ec, not redundant */
                let rinfo = create_join_clause(root, ec, eq_op, prev, cur_em, None)?;
                result.push(rinfo);
            }
            prev_em = Some(cur_em);
        }
    }

    Ok(result)
}

/* ======================================================================
 * generate_join_implied_equalities_broken (equivclass.c:1899)
 * ==================================================================== */

fn generate_join_implied_equalities_broken(
    root: &mut PlannerInfo,
    ec: EcId,
    nominal_join_relids: &Relids,
    outer_relids: &Relids,
    nominal_inner_relids: &Relids,
    inner_rel: RelId,
) -> PgResult<Vec<RinfoId>> {
    let mut result: Vec<RinfoId> = Vec::new();
    let sources = root.ec(ec).ec_sources.clone();
    for restrictinfo in sources {
        let clause_relids = root.rinfo(restrictinfo).required_relids.clone();
        if bms::relids_is_subset::call(&clause_relids, nominal_join_relids)
            && !bms::relids_is_subset::call(&clause_relids, outer_relids)
            && !bms::relids_is_subset::call(&clause_relids, nominal_inner_relids)
        {
            result.push(restrictinfo);
        }
    }

    /* if inner is a child rel, translate parent→child Vars */
    if is_other_rel(root.rel(inner_rel)) && !result.is_empty() {
        panic!(
            "generate_join_implied_equalities_broken: child inner relation \
             (RELOPT_OTHER_*; adjust_appendrel_attrs_multilevel over the source \
             RestrictInfo list, equivclass.c:1929) is not ported — appendrel \
             child rels do not reach this path until the appendrel program lands"
        );
    }

    Ok(result)
}

/* ======================================================================
 * generate_implied_equalities_for_column (equivclass.c:3239)
 * ==================================================================== */

/// `ec_matches_callback_type` — the per-column match predicate.
pub type EcMatchesCallback<'a> = dyn FnMut(&PlannerInfo, RelId, EcId, EmId) -> bool + 'a;

/// `generate_implied_equalities_for_column(root, rel, callback, callback_arg,
/// prohibited_rels)` (equivclass.c:3239).
pub fn generate_implied_equalities_for_column(
    root: &mut PlannerInfo,
    rel: RelId,
    callback: &mut EcMatchesCallback<'_>,
    prohibited_rels: &Relids,
) -> PgResult<Vec<RinfoId>> {
    let mut result: Vec<RinfoId> = Vec::new();
    debug_assert!(root.ec_merging_done);

    let is_child_rel =
        root.rel(rel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL;
    let parent_relids: Relids = if is_child_rel {
        ec_seam::find_childrel_parents::call(root, rel)
    } else {
        None
    };

    let eclass_indexes = root.rel(rel).eclass_indexes.clone();
    let rel_relids = root.rel(rel).relids.clone();

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&eclass_indexes, i);
        if i < 0 {
            break;
        }
        let ec = EcId(i as u32);

        debug_assert!(
            is_child_rel
                || bms::relids_is_subset::call(&rel_relids, &root.ec(ec).ec_relids)
        );

        if root.ec(ec).ec_has_const || root.ec(ec).ec_members.len() <= 1 {
            continue;
        }

        /* scan members, looking for a match to the target column */
        let mut it = new_iterator(root, ec, &rel_relids);
        let mut cur_em: Option<EmId> = None;
        while let Some(em) = crate::relevance::eclass_member_iterator_next(root, &mut it) {
            if bms::relids_equal::call(&root.em(em).em_relids, &rel_relids)
                && callback(root, rel, ec, em)
            {
                cur_em = Some(em);
                break;
            }
        }
        let cur_em = match cur_em {
            None => continue,
            Some(em) => em,
        };

        /* scan the other EC members and attempt to generate joinclauses */
        let members = root.ec(ec).ec_members.clone();
        for other_em in members {
            debug_assert!(!root.em(other_em).em_is_child);

            if other_em == cur_em
                || bms::relids_overlap::call(&root.em(other_em).em_relids, &rel_relids)
            {
                continue;
            }
            if bms::relids_overlap::call(&root.em(other_em).em_relids, prohibited_rels) {
                continue;
            }
            /* if a child rel, avoid a useless join to its parent(s) */
            if is_child_rel
                && bms::relids_overlap::call(&parent_relids, &root.em(other_em).em_relids)
            {
                continue;
            }

            let eq_op = select_equality_operator(
                root.ec(ec),
                root.em(cur_em).em_datatype,
                root.em(other_em).em_datatype,
            );
            if !oid_is_valid(eq_op) {
                continue;
            }
            /* set parent_ec to mark redundant */
            let rinfo = create_join_clause(root, ec, eq_op, cur_em, other_em, Some(ec))?;
            result.push(rinfo);
        }

        if !result.is_empty() {
            break;
        }
    }

    Ok(result)
}

/* ======================================================================
 * reconsider_outer_join_clauses (equivclass.c:2135)
 * ==================================================================== */

/// `reconsider_outer_join_clauses(root)` (equivclass.c:2135).
pub fn reconsider_outer_join_clauses(root: &mut PlannerInfo) -> PgResult<()> {
    /* outer loop repeats until no more deductions */
    loop {
        let mut found = false;

        /* LEFT JOIN clauses */
        let mut idx = 0;
        while idx < root.left_join_clauses.len() {
            let ojcinfo = root.left_join_clauses[idx].clone();
            if reconsider_outer_join_clause(root, &ojcinfo, true)? {
                found = true;
                root.left_join_clauses.remove(idx);
                throw_back_dummy(root, ojcinfo.rinfo)?;
            } else {
                idx += 1;
            }
        }

        /* RIGHT JOIN clauses */
        let mut idx = 0;
        while idx < root.right_join_clauses.len() {
            let ojcinfo = root.right_join_clauses[idx].clone();
            if reconsider_outer_join_clause(root, &ojcinfo, false)? {
                found = true;
                root.right_join_clauses.remove(idx);
                throw_back_dummy(root, ojcinfo.rinfo)?;
            } else {
                idx += 1;
            }
        }

        /* FULL JOIN clauses */
        let mut idx = 0;
        while idx < root.full_join_clauses.len() {
            let ojcinfo = root.full_join_clauses[idx].clone();
            if reconsider_full_join_clause(root, &ojcinfo)? {
                found = true;
                root.full_join_clauses.remove(idx);
                throw_back_dummy(root, ojcinfo.rinfo)?;
            } else {
                idx += 1;
            }
        }

        if !found {
            break;
        }
    }

    /* any remaining clauses get thrown back */
    let remaining: Vec<RinfoId> = root
        .left_join_clauses
        .iter()
        .chain(root.right_join_clauses.iter())
        .chain(root.full_join_clauses.iter())
        .map(|o| o.rinfo)
        .collect();
    for rinfo in remaining {
        ec_seam::distribute_restrictinfo_to_rels::call(root, rinfo)?;
    }
    Ok(())
}

/// throw back a dummy constant-TRUE replacement clause (the "see notes above"
/// path in `reconsider_outer_join_clauses`).
fn throw_back_dummy(root: &mut PlannerInfo, rinfo: RinfoId) -> PgResult<()> {
    let ri = root.rinfo(rinfo).clone();
    let bool_true = ec_seam::make_bool_const::call(true, false);
    let dummy = ec_seam::make_restrictinfo::call(
        root,
        bool_true,
        ri.is_pushed_down,
        ri.has_clone,
        ri.is_clone,
        false, /* pseudoconstant */
        0,     /* security_level */
        ri.required_relids.clone(),
        ri.incompatible_relids.clone(),
        ri.outer_relids.clone(),
    )?;
    ec_seam::distribute_restrictinfo_to_rels::call(root, dummy)
}

/* ----- reconsider_outer_join_clause (equivclass.c:2257) -------------- */

fn reconsider_outer_join_clause(
    root: &mut PlannerInfo,
    ojcinfo: &types_pathnodes::OuterJoinClauseInfo,
    outer_on_left: bool,
) -> PgResult<bool> {
    let rinfo = ojcinfo.rinfo;
    let sjinfo = ojcinfo.sjinfo.clone();

    let clause = em_expr_of_clause(root, rinfo);
    let opexpr = clause
        .as_opexpr()
        .expect("reconsider_outer_join_clause: clause is not an OpExpr");
    let opno = opexpr.opno;
    let collation = opexpr.inputcollid;
    let leftop = opexpr.args[0].clone();
    let rightop = opexpr.args[1].clone();

    let (left_type, right_type) = cat::op_input_types::call(opno).expect("op_input_types");
    let (outervar, innervar, inner_datatype, inner_relids) = if outer_on_left {
        (leftop, rightop, right_type, root.rinfo(rinfo).right_relids.clone())
    } else {
        (rightop, leftop, left_type, root.rinfo(rinfo).left_relids.clone())
    };

    let mergeopfamilies = root.rinfo(rinfo).mergeopfamilies.clone();

    /* scan ECs for a match to outervar */
    for cur_ec in live_ec_ids(root) {
        debug_assert!(root.ec(cur_ec).ec_childmembers.is_empty());
        if !root.ec(cur_ec).ec_has_const {
            continue;
        }
        if root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        if collation != root.ec(cur_ec).ec_collation {
            continue;
        }
        if !crate::merge::opfamilies_equal(&mergeopfamilies, &root.ec(cur_ec).ec_opfamilies) {
            continue;
        }

        /* does it contain a match to outervar? */
        let mut matched = false;
        for &cur_em in &root.ec(cur_ec).ec_members.clone() {
            debug_assert!(!root.em(cur_em).em_is_child);
            if ec_seam::equal::call(&outervar, &em_expr(root, cur_em)) {
                matched = true;
                break;
            }
        }
        if !matched {
            continue;
        }

        /* try INNERVAR = CONSTANT for each const in the EC */
        let mut match_any = false;
        for cur_em in root.ec(cur_ec).ec_members.clone() {
            if !root.em(cur_em).em_is_const {
                continue;
            }
            let eq_op = select_equality_operator(
                root.ec(cur_ec),
                inner_datatype,
                root.em(cur_em).em_datatype,
            );
            if !oid_is_valid(eq_op) {
                continue;
            }
            let collation = root.ec(cur_ec).ec_collation;
            let min_security = root.ec(cur_ec).ec_min_security;
            let const_expr = em_expr(root, cur_em);
            let inner_relids_copy = bms::relids_copy::call(&inner_relids);
            let newrinfo = ec_seam::build_implied_join_equality::call(
                root,
                eq_op,
                collation,
                innervar.clone(),
                const_expr,
                inner_relids_copy,
                min_security,
            )?;
            /* holds within the OJ's child JoinDomain */
            let jdomain = find_join_domain(root, &sjinfo.syn_righthand).jd_relids;
            let (ok, _ri) = process_equivalence(root, newrinfo, jdomain)?;
            if ok {
                match_any = true;
            }
        }

        /* report success or stop (OUTERVAR is in at most one EC) */
        if match_any {
            return Ok(true);
        } else {
            break;
        }
    }

    Ok(false)
}

/* ----- reconsider_full_join_clause (equivclass.c:2384) -------------- */

fn reconsider_full_join_clause(
    root: &mut PlannerInfo,
    ojcinfo: &types_pathnodes::OuterJoinClauseInfo,
) -> PgResult<bool> {
    let rinfo = ojcinfo.rinfo;
    let sjinfo = ojcinfo.sjinfo.clone();
    let fjrelids = bms::relids_make_singleton::call(sjinfo.ojrelid as i32);

    let clause = em_expr_of_clause(root, rinfo);
    let opexpr = clause
        .as_opexpr()
        .expect("reconsider_full_join_clause: clause is not an OpExpr");
    let opno = opexpr.opno;
    let collation = opexpr.inputcollid;
    let (left_type, right_type) = cat::op_input_types::call(opno).expect("op_input_types");
    let leftvar = opexpr.args[0].clone();
    let rightvar = opexpr.args[1].clone();
    let left_relids = root.rinfo(rinfo).left_relids.clone();
    let right_relids = root.rinfo(rinfo).right_relids.clone();
    let mergeopfamilies = root.rinfo(rinfo).mergeopfamilies.clone();

    for cur_ec in live_ec_ids(root) {
        debug_assert!(root.ec(cur_ec).ec_childmembers.is_empty());
        if !root.ec(cur_ec).ec_has_const {
            continue;
        }
        if root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        if collation != root.ec(cur_ec).ec_collation {
            continue;
        }
        if !crate::merge::opfamilies_equal(&mergeopfamilies, &root.ec(cur_ec).ec_opfamilies) {
            continue;
        }

        /* does it contain a COALESCE(leftvar, rightvar)? */
        let mut coal_idx: Option<usize> = None;
        let members = root.ec(cur_ec).ec_members.clone();
        for (idx, &coal_em) in members.iter().enumerate() {
            debug_assert!(!root.em(coal_em).em_is_child);
            let emexpr = em_expr(root, coal_em);
            let cexpr = match emexpr.as_coalesceexpr() {
                Some(c) => c,
                None => continue,
            };
            if cexpr.args.len() != 2 {
                continue;
            }
            let cfirst = cexpr.args[0].clone();
            let csecond = cexpr.args[1].clone();
            /* strip the full join from the COALESCE args' nullingrels */
            let cfirst = ec_seam::remove_nulling_relids::call(
                cfirst,
                bms::relids_copy::call(&fjrelids),
                None,
            );
            let csecond = ec_seam::remove_nulling_relids::call(
                csecond,
                bms::relids_copy::call(&fjrelids),
                None,
            );
            if ec_seam::equal::call(&leftvar, &cfirst) && ec_seam::equal::call(&rightvar, &csecond)
            {
                coal_idx = Some(idx);
                break;
            }
        }
        let coal_idx = match coal_idx {
            None => continue,
            Some(c) => c,
        };

        /* try LEFTVAR = CONSTANT and RIGHTVAR = CONSTANT for each const */
        let mut matchleft = false;
        let mut matchright = false;
        for cur_em in root.ec(cur_ec).ec_members.clone() {
            if !root.em(cur_em).em_is_const {
                continue;
            }
            /* LEFT */
            let eq_op = select_equality_operator(root.ec(cur_ec), left_type, root.em(cur_em).em_datatype);
            if oid_is_valid(eq_op) {
                let collation = root.ec(cur_ec).ec_collation;
                let min_security = root.ec(cur_ec).ec_min_security;
                let const_expr = em_expr(root, cur_em);
                let lr = bms::relids_copy::call(&left_relids);
                let newrinfo = ec_seam::build_implied_join_equality::call(
                    root, eq_op, collation, leftvar.clone(), const_expr, lr, min_security,
                )?;
                let jdomain = find_join_domain(root, &sjinfo.syn_lefthand).jd_relids;
                let (ok, _ri) = process_equivalence(root, newrinfo, jdomain)?;
                if ok {
                    matchleft = true;
                }
            }
            /* RIGHT */
            let eq_op = select_equality_operator(root.ec(cur_ec), right_type, root.em(cur_em).em_datatype);
            if oid_is_valid(eq_op) {
                let collation = root.ec(cur_ec).ec_collation;
                let min_security = root.ec(cur_ec).ec_min_security;
                let const_expr = em_expr(root, cur_em);
                let rr = bms::relids_copy::call(&right_relids);
                let newrinfo = ec_seam::build_implied_join_equality::call(
                    root, eq_op, collation, rightvar.clone(), const_expr, rr, min_security,
                )?;
                let jdomain = find_join_domain(root, &sjinfo.syn_righthand).jd_relids;
                let (ok, _ri) = process_equivalence(root, newrinfo, jdomain)?;
                if ok {
                    matchright = true;
                }
            }
        }

        if matchleft && matchright {
            /* remove the COALESCE entry from the EC; both vars now constrained */
            root.ec_mut(cur_ec).ec_members.remove(coal_idx);
            return Ok(true);
        }

        /* COALESCE appears in at most one EC */
        break;
    }

    Ok(false)
}

/// Resolve a RestrictInfo's clause node to an owned [`Expr`].
fn em_expr_of_clause(root: &PlannerInfo, rinfo: RinfoId) -> types_nodes::primnodes::Expr {
    root.node(root.rinfo(rinfo).clause).clone()
}

#[allow(unused_imports)]
use types_pathnodes::{RELOPT_BASEREL as _RB, RELOPT_JOINREL as _RJ};
