//! equivclass.c: EquivalenceClass construction, merging, and derived-clause
//! generation. ECs live in the eq_classes arena; a merged EC keeps its slot
//! with ec_merged set (C deletes the list cell), so every full walk skips
//! merged entries and EcId indexes stay stable. eclass_indexes bitmaps hold
//! EcId values, not C's post-deletion list positions; producers and consumers
//! agree so the mapping is invisible.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    ECDerivesKey, EcId, EmId, EquivalenceClass, EquivalenceMember, IndexClause, RelId, Relids,
    RinfoId, SpecialJoinInfo, RELOPT_BASEREL,
};

use types_pathnodes::relids::{
    find_base_rel, find_childrel_parents, pgvec_clone_shallow, relids_add_member, relids_copy, relids_empty,
    relids_equal, relids_is_empty, relids_is_member, relids_is_subset, relids_members,
    relids_num_members, relids_overlap, relids_union,
};
use types_pathnodes::run::PlannerRun;

const EC_DERIVES_HASH_THRESHOLD: usize = 32;

fn live_ec(run: &PlannerRun<'_>, id: EcId) -> bool {
    run.root.ec(id).ec_merged.is_none()
}

// C's `Assert(ec->ec_childmembers == NULL)` sites: these phases all run
// before add_child_rel_equivalences can have added any child members.
fn assert_no_child_members(run: &PlannerRun<'_>, ec: EcId) {
    assert!(
        run.root.ec(ec).ec_childmembers.is_empty(),
        "child EC members exist before appendrel expansion (equivclass.c)"
    );
}

pub fn process_equivalence<'mcx>(
    run: &mut PlannerRun<'mcx>,
    p_rinfo: &mut RinfoId,
    jdomain: usize,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let rinfo = *p_rinfo;
    debug_assert!(run.root.rinfo(rinfo).left_ec.is_none());
    debug_assert!(run.root.rinfo(rinfo).right_ec.is_none());
    {
        let ri = run.root.rinfo(rinfo);
        if ri.security_level > 0 && !ri.leakproof {
            return Ok(false);
        }
    }

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("process_equivalence: not an opclause");
    let opno = op.opno;
    let collation = op.inputcollid;
    let item1_relids = relids_copy(mcx, &run.root.rinfo(rinfo).left_relids);
    let item2_relids = relids_copy(mcx, &run.root.rinfo(rinfo).right_relids);
    let item1 = canonicalize_ec_expression(
        mcx,
        op.args.nth(0),
        costsize::expr_type_typmod(op.args.nth(0)).0,
        collation,
    )?;
    let item2 = canonicalize_ec_expression(
        mcx,
        op.args.nth(1),
        costsize::expr_type_typmod(op.args.nth(1)).0,
        collation,
    )?;

    if types_nodes::equal(item1, item2) {
        // X=X with a strict operator reduces to X IS NOT NULL.
        let opfuncid = lsyscache::get_opcode(opno)?;
        if lsyscache::func_strict(opfuncid)? {
            let ntest = Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(item1),
                    nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                    argisrow: false,
                    location: -1,
                },
            )?;
            let (is_pushed_down, has_clone, is_clone, pseudoconstant, security, incompatible, outer) = {
                let ri = run.root.rinfo(rinfo);
                (
                    ri.is_pushed_down,
                    ri.has_clone,
                    ri.is_clone,
                    ri.pseudoconstant,
                    ri.security_level,
                    relids_copy(mcx, &ri.incompatible_relids),
                    relids_copy(mcx, &ri.outer_relids),
                )
            };
            *p_rinfo = planner_seams::make_restrictinfo::call(
                run,
                ntest,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security,
                types_pathnodes::relids::relids_empty(),
                incompatible,
                outer,
            )?;
        }
        return Ok(false);
    }

    let (item1_type, item2_type) = lsyscache::op_input_types(opno)?;
    let opfamilies = pgvec_clone_shallow(mcx, &run.root.rinfo(rinfo).mergeopfamilies);

    let mut ec1: Option<EcId> = None;
    let mut em1: Option<EmId> = None;
    let mut ec2: Option<EcId> = None;
    let mut em2: Option<EmId> = None;
    'sweep: for i in 0..run.root.eq_classes.len() {
        let cur = EcId(i as u32);
        {
            let ec = run.root.ec(cur);
            if ec.ec_merged.is_some() || ec.ec_has_volatile {
                continue;
            }
            if collation != ec.ec_collation {
                continue;
            }
            if ec.ec_opfamilies.as_slice() != opfamilies.as_slice() {
                continue;
            }
        }
        assert_no_child_members(run, cur);
        let n = run.root.ec(cur).ec_members.len();
        for m in 0..n {
            let em_id = run.root.ec(cur).ec_members[m];
            let (is_child, is_const, em_jd, em_type) = {
                let em = run.root.em(em_id);
                (em.em_is_child, em.em_is_const, em.em_jdomain, em.em_datatype)
            };
            debug_assert!(!is_child);
            if is_const && em_jd != jdomain {
                continue;
            }
            let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
            if ec1.is_none() && item1_type == em_type && types_nodes::equal(item1, em_expr) {
                ec1 = Some(cur);
                em1 = Some(em_id);
                if ec2.is_some() {
                    break;
                }
            }
            if ec2.is_none() && item2_type == em_type && types_nodes::equal(item2, em_expr) {
                ec2 = Some(cur);
                em2 = Some(em_id);
                if ec1.is_some() {
                    break;
                }
            }
        }
        if ec1.is_some() && ec2.is_some() {
            break 'sweep;
        }
    }

    let security_level = run.root.rinfo(rinfo).security_level;
    match (ec1, ec2) {
        (Some(e1), Some(e2)) if e1 == e2 => {
            let ec = run.root.ec_mut(e1);
            ec.ec_sources.push(rinfo);
            ec.ec_min_security = ec.ec_min_security.min(security_level);
            ec.ec_max_security = ec.ec_max_security.max(security_level);
        }
        (Some(e1), Some(e2)) => {
            assert!(!run.root.ec_merging_done, "too late to merge equivalence classes");
            let (members2, sources2, derives2, relids2, has_const2, min2, max2) = {
                let ec = run.root.ec_mut(e2);
                (
                    core::mem::replace(&mut ec.ec_members, PgVec::new_in(mcx)),
                    core::mem::replace(&mut ec.ec_sources, PgVec::new_in(mcx)),
                    core::mem::replace(&mut ec.ec_derives_list, PgVec::new_in(mcx)),
                    core::mem::take(&mut ec.ec_relids),
                    ec.ec_has_const,
                    ec.ec_min_security,
                    ec.ec_max_security,
                )
            };
            run.root.ec_mut(e2).ec_derives_hash = None;
            run.root.ec_mut(e2).ec_merged = Some(e1);
            {
                let ec = run.root.ec_mut(e1);
                ec.ec_members.extend(members2.iter().copied());
                ec.ec_sources.extend(sources2.iter().copied());
            }
            ec_add_derived_clauses(run, e1, &derives2);
            {
                let joined = relids_union(mcx, &run.root.ec(e1).ec_relids, &relids2);
                let ec = run.root.ec_mut(e1);
                ec.ec_relids = joined;
                ec.ec_has_const |= has_const2;
                ec.ec_min_security = ec.ec_min_security.min(min2);
                ec.ec_max_security = ec.ec_max_security.max(max2);
                ec.ec_sources.push(rinfo);
                ec.ec_min_security = ec.ec_min_security.min(security_level);
                ec.ec_max_security = ec.ec_max_security.max(security_level);
            }
        }
        (Some(e1), None) => {
            em2 = Some(add_eq_member(run, e1, item2, item2_relids, jdomain, item2_type));
            let ec = run.root.ec_mut(e1);
            ec.ec_sources.push(rinfo);
            ec.ec_min_security = ec.ec_min_security.min(security_level);
            ec.ec_max_security = ec.ec_max_security.max(security_level);
        }
        (None, Some(e2)) => {
            em1 = Some(add_eq_member(run, e2, item1, item1_relids, jdomain, item1_type));
            let ec = run.root.ec_mut(e2);
            ec.ec_sources.push(rinfo);
            ec.ec_min_security = ec.ec_min_security.min(security_level);
            ec.ec_max_security = ec.ec_max_security.max(security_level);
        }
        (None, None) => {
            let mut ec = EquivalenceClass::new(mcx);
            ec.ec_opfamilies = opfamilies;
            ec.ec_collation = collation;
            ec.ec_sources.push(rinfo);
            ec.ec_min_security = security_level;
            ec.ec_max_security = security_level;
            let id = run.root.alloc_ec(ec);
            em1 = Some(add_eq_member(run, id, item1, item1_relids, jdomain, item1_type));
            em2 = Some(add_eq_member(run, id, item2, item2_relids, jdomain, item2_type));
            ec1 = Some(id);
        }
    }

    let the_ec = ec1.or(ec2).expect("an EC was found or created");
    let r = run.root.rinfo_mut(rinfo);
    r.left_ec = Some(the_ec);
    r.right_ec = Some(the_ec);
    r.left_em = em1;
    r.right_em = em2;
    Ok(true)
}

pub fn canonicalize_ec_expression<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    expr: Node<'mcx>,
    req_type: u32,
    req_collation: u32,
) -> PgResult<Node<'mcx>> {
    use types_core::catalog::RECORDOID;
    use types_nodes::primnodes::CoercionForm;
    let (expr_type, expr_typmod) = costsize::expr_type_typmod(expr);
    let req_type = if clauses::fold::is_polymorphic_type(req_type) || req_type == RECORDOID {
        expr_type
    } else {
        req_type
    };
    if expr_type != req_type || planner_seams::expr_collation::call(expr) != req_collation {
        let req_typmod = if expr_type != req_type { -1 } else { expr_typmod };
        return clauses::fold::apply_relabel_type(
            mcx,
            expr,
            req_type,
            req_typmod,
            req_collation,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        );
    }
    Ok(expr)
}

fn make_eq_member<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    expr: Node<'mcx>,
    relids: Relids<'mcx>,
    jdomain: usize,
    datatype: u32,
) -> EmId {
    let em_expr = run.intern_expr(expr);
    let is_const = relids_is_empty(&relids);
    if is_const {
        run.root.ec_mut(ec).ec_has_const = true;
    }
    run.root.alloc_em(EquivalenceMember {
        em_expr,
        em_relids: relids,
        em_is_const: is_const,
        em_is_child: false,
        em_datatype: datatype,
        em_jdomain: jdomain,
        em_parent: None,
    })
}

fn add_eq_member<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    expr: Node<'mcx>,
    relids: Relids<'mcx>,
    jdomain: usize,
    datatype: u32,
) -> EmId {
    let mcx = run.mcx;
    let joined = relids_union(mcx, &run.root.ec(ec).ec_relids, &relids);
    let em = make_eq_member(run, ec, expr, relids, jdomain, datatype);
    let e = run.root.ec_mut(ec);
    e.ec_members.push(em);
    e.ec_relids = joined;
    em
}

// setup_eclass_member_iterator + eclass_member_iterator_next (equivclass.c),
// flattened: all parent members plus child members stored under relids' rels.
pub fn ec_members_for_relids<'mcx>(
    run: &PlannerRun<'mcx>,
    ec: EcId,
    relids: &Relids<'mcx>,
) -> PgVec<'mcx, EmId> {
    let mcx = run.mcx;
    let e = run.root.ec(ec);
    let mut out: PgVec<'mcx, EmId> = PgVec::new_in(mcx);
    out.extend(e.ec_members.iter().copied());
    if !e.ec_childmembers.is_empty() {
        for r in relids_members(relids) {
            if let Some(list) = e.ec_childmembers.get(r as usize) {
                out.extend(list.iter().copied());
            }
        }
    }
    out
}

// add_child_eq_member (equivclass.c): child members live in ec_childmembers
// keyed by child relid, never in ec_members, and never affect ec_relids.
#[allow(clippy::too_many_arguments)]
fn add_child_eq_member<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    ec_index: Option<EcId>,
    expr: Node<'mcx>,
    relids: Relids<'mcx>,
    jdomain: usize,
    parent_em: EmId,
    datatype: u32,
    child_relid: usize,
) -> EmId {
    let mcx = run.mcx;
    let em_expr = run.intern_expr(expr);
    let em = run.root.alloc_em(EquivalenceMember {
        em_expr,
        em_relids: relids,
        em_is_const: false,
        em_is_child: true,
        em_datatype: datatype,
        em_jdomain: jdomain,
        em_parent: Some(parent_em),
    });
    {
        let e = run.root.ec_mut(ec);
        while e.ec_childmembers.len() <= child_relid {
            e.ec_childmembers.push(PgVec::new_in(mcx));
        }
        e.ec_childmembers[child_relid].push(em);
        e.ec_childmembers_size = e.ec_childmembers.len() as i32;
    }
    if let Some(idx) = ec_index {
        let rel_id = run.root.simple_rel_array[child_relid].expect("child rel exists");
        let updated = relids_add_member(mcx, &run.root.rel(rel_id).eclass_indexes, idx.0);
        run.root.rel_mut(rel_id).eclass_indexes = updated;
    }
    em
}

// add_child_rel_equivalences (equivclass.c).
pub fn add_child_rel_equivalences<'mcx>(
    run: &mut PlannerRun<'mcx>,
    appinfo: &types_pathnodes::AppendRelInfo<'mcx>,
    parent_rel: RelId,
    child_rel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(run.root.ec_merging_done);
    debug_assert!(matches!(
        run.root.rel(parent_rel).reloptkind,
        RELOPT_BASEREL | types_pathnodes::RELOPT_OTHER_MEMBER_REL
    ));
    let top_parent_relids = relids_copy(mcx, &run.root.rel(child_rel).top_parent_relids);
    let child_relids = relids_copy(mcx, &run.root.rel(child_rel).relids);
    let child_relid = run.root.rel(child_rel).relid as usize;
    let ec_indexes = relids_copy(mcx, &run.root.rel(parent_rel).eclass_indexes);
    for i in relids_members(&ec_indexes) {
        let cur_ec = EcId(i as u32);
        // A volatile EC has only one EM; child copies would be dangerous.
        if run.root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        debug_assert!(relids_is_subset(&top_parent_relids, &run.root.ec(cur_ec).ec_relids));
        let n_members = run.root.ec(cur_ec).ec_members.len();
        for m in 0..n_members {
            let cur_em = run.root.ec(cur_ec).ec_members[m];
            let (is_const, is_child, em_relids, em_jd, em_type) = {
                let em = run.root.em(cur_em);
                (
                    em.em_is_const,
                    em.em_is_child,
                    relids_copy(mcx, &em.em_relids),
                    em.em_jdomain,
                    em.em_datatype,
                )
            };
            if is_const {
                continue;
            }
            debug_assert!(!is_child);
            // Members with nonempty varnullingrels don't translate to a
            // simple Var and are useless for child planning (C comment).
            if relids_is_subset(&em_relids, &top_parent_relids) && !relids_is_empty(&em_relids) {
                let expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
                let child_expr = if run.root.rel(parent_rel).reloptkind == RELOPT_BASEREL {
                    planner_seams::adjust_appendrel_attrs::call(run, expr, appinfo)?
                } else {
                    let top = run.root.rel(child_rel).top_parent.expect("child has top parent");
                    planner_seams::adjust_appendrel_attrs_multilevel::call(run, expr, child_rel, top)?
                };
                // Not pull_varnos(child_expr): a substituted constant must
                // not mark the child member const.
                let new_relids = relids_union(
                    mcx,
                    &types_pathnodes::relids::relids_difference(mcx, &em_relids, &top_parent_relids),
                    &child_relids,
                );
                add_child_eq_member(
                    run,
                    cur_ec,
                    Some(cur_ec),
                    child_expr,
                    new_relids,
                    em_jd,
                    cur_em,
                    em_type,
                    child_relid,
                );
            }
        }
    }
    Ok(())
}

// add_child_join_rel_equivalences (equivclass.c). Members for a child joinrel
// are stored under its first component relid only; iterator callers pass all
// component relids so the member is still found exactly once.
pub fn add_child_join_rel_equivalences<'mcx>(
    run: &mut PlannerRun<'mcx>,
    appinfos: &[types_pathnodes::AppendRelInfo<'mcx>],
    parent_joinrel: RelId,
    child_joinrel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(matches!(
        run.root.rel(parent_joinrel).reloptkind,
        types_pathnodes::RELOPT_JOINREL | types_pathnodes::RELOPT_OTHER_JOINREL
    ));
    let top_parent_relids = relids_copy(mcx, &run.root.rel(child_joinrel).top_parent_relids);
    let child_relids = relids_copy(mcx, &run.root.rel(child_joinrel).relids);
    let first_component = relids_members(&child_relids)
        .find(|&r| run.root.simple_rel_array.get(r as usize).copied().flatten().is_some())
        .expect("child joinrel has a baserel component") as usize;
    let matching_ecs = get_eclass_indexes_for_relids(run, &top_parent_relids);
    for i in relids_members(&matching_ecs) {
        let cur_ec = EcId(i as u32);
        if run.root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        debug_assert!(relids_overlap(&top_parent_relids, &run.root.ec(cur_ec).ec_relids));
        let n_members = run.root.ec(cur_ec).ec_members.len();
        for m in 0..n_members {
            let cur_em = run.root.ec(cur_ec).ec_members[m];
            let (is_const, is_child, em_relids, em_jd, em_type) = {
                let em = run.root.em(cur_em);
                (
                    em.em_is_const,
                    em.em_is_child,
                    relids_copy(mcx, &em.em_relids),
                    em.em_jdomain,
                    em.em_datatype,
                )
            };
            if is_const {
                continue;
            }
            debug_assert!(!is_child);
            // Single-baserel members were handled by add_child_rel_equivalences.
            if relids_num_members(&em_relids) <= 1 {
                continue;
            }
            if relids_overlap(&em_relids, &top_parent_relids) {
                let expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
                let child_expr = if run.root.rel(parent_joinrel).reloptkind
                    == types_pathnodes::RELOPT_JOINREL
                {
                    planner_seams::adjust_appendrel_attrs_multi::call(run, expr, appinfos)?
                } else {
                    let top =
                        run.root.rel(child_joinrel).top_parent.expect("child has top parent");
                    planner_seams::adjust_appendrel_attrs_multilevel::call(run, expr, child_joinrel, top)?
                };
                let new_relids = relids_union(
                    mcx,
                    &types_pathnodes::relids::relids_difference(mcx, &em_relids, &top_parent_relids),
                    &child_relids,
                );
                add_child_eq_member(
                    run,
                    cur_ec,
                    None,
                    child_expr,
                    new_relids,
                    em_jd,
                    cur_em,
                    em_type,
                    first_component,
                );
            }
        }
    }
    Ok(())
}

// add_setop_child_rel_equivalences (equivclass.c).
pub fn add_setop_child_rel_equivalences<'mcx>(
    run: &mut PlannerRun<'mcx>,
    child_rel: RelId,
    child_tlist: &types_nodes::list::NodeList<'mcx>,
    setop_pathkeys: &[types_pathnodes::PathKey],
) {
    let mcx = run.mcx;
    let mut pks = setop_pathkeys.iter();
    for tle_node in child_tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            continue;
        }
        let pk = pks.next().expect("too few pathkeys for set operation");
        let ec = pk.pk_eclass.expect("canonical pathkey has an eclass");
        // generate_union_paths adds the parent member first; its JoinDomain
        // covers the child member too.
        let parent_em = run.root.ec(ec).ec_members[0];
        let em_jdomain = run.root.em(parent_em).em_jdomain;
        let relids = relids_copy(mcx, &run.root.rel(child_rel).relids);
        let child_relid = run.root.rel(child_rel).relid as usize;
        let datatype = costsize::expr_type_typmod(tle.expr).0;
        add_child_eq_member(
            run,
            ec,
            None,
            tle.expr,
            relids,
            em_jdomain,
            parent_em,
            datatype,
            child_relid,
        );
    }
    // transformSetOperationStmt keeps the tlist resjunk-free, so every EC in
    // root gained a child member above.
    let mut idx = relids_copy(mcx, &run.root.rel(child_rel).eclass_indexes);
    for i in 0..run.root.eq_classes.len() {
        idx = relids_add_member(mcx, &idx, i as u32);
    }
    run.root.rel_mut(child_rel).eclass_indexes = idx;
}

// jdomain is always the top domain: sort/group expressions are top-level.
pub fn get_eclass_for_sort_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    opfamilies: &PgVec<'mcx, u32>,
    opcintype: u32,
    collation: u32,
    sortref: u32,
    rel: &Relids<'mcx>,
    create_it: bool,
) -> PgResult<Option<EcId>> {
    let mcx = run.mcx;
    let expr = canonicalize_ec_expression(mcx, expr, opcintype, collation)?;
    let jdomain = 0usize;

    for i in 0..run.root.eq_classes.len() {
        let id = EcId(i as u32);
        {
            let ec = run.root.ec(id);
            if ec.ec_merged.is_some() {
                continue;
            }
            if ec.ec_has_volatile && (sortref == 0 || sortref != ec.ec_sortref) {
                continue;
            }
            if collation != ec.ec_collation {
                continue;
            }
            if ec.ec_opfamilies.as_slice() != opfamilies.as_slice() {
                continue;
            }
        }
        let n_members = run.root.ec(id).ec_members.len();
        for m in 0..n_members {
            let em_id = run.root.ec(id).ec_members[m];
            let (is_child, is_const, em_jd, em_type) = {
                let em = run.root.em(em_id);
                (em.em_is_child, em.em_is_const, em.em_jdomain, em.em_datatype)
            };
            if is_child {
                continue;
            }
            if is_const && em_jd != jdomain {
                continue;
            }
            if opcintype == em_type
                && types_nodes::equal(*run.root.expr_node(run.root.em(em_id).em_expr), expr)
            {
                return Ok(Some(id));
            }
        }
        // Child members match only when their em_relids equal the request.
        for r in relids_members(rel) {
            let Some(list) = run.root.ec(id).ec_childmembers.get(r as usize) else { continue };
            let n = list.len();
            for m in 0..n {
                let em_id = run.root.ec(id).ec_childmembers[r as usize][m];
                let em_type = run.root.em(em_id).em_datatype;
                if !relids_equal(&run.root.em(em_id).em_relids, rel) {
                    continue;
                }
                if opcintype == em_type
                    && types_nodes::equal(*run.root.expr_node(run.root.em(em_id).em_expr), expr)
                {
                    return Ok(Some(id));
                }
            }
        }
    }

    if !create_it {
        return Ok(None);
    }

    let has_volatile = clauses::contain_volatile_functions(expr)?;
    assert!(!(has_volatile && sortref == 0), "volatile EquivalenceClass has no sortref");

    let mut ec = EquivalenceClass::new(mcx);
    ec.ec_opfamilies = pgvec_clone_shallow(mcx, opfamilies);
    ec.ec_collation = collation;
    ec.ec_has_volatile = has_volatile;
    ec.ec_sortref = sortref;
    ec.ec_min_security = u32::MAX;
    ec.ec_max_security = 0;
    let id = run.root.alloc_ec(ec);

    let expr_relids = planner_seams::pull_varnos_relids::call(run, expr)?;
    let em = add_eq_member(run, id, expr, expr_relids, jdomain, opcintype);

    // add_eq_member's const marking assumes a WHERE clause; sort exprs can
    // hide volatiles/SRFs/aggregates/window functions.
    if run.root.ec(id).ec_has_const
        && (has_volatile
            || expression_returns_set(expr)?
            || clauses::contain_agg_clause(expr)?
            || clauses::contain_window_function(expr)?)
    {
        run.root.ec_mut(id).ec_has_const = false;
        run.root.em_mut(em).em_is_const = false;
    }

    if run.root.ec_merging_done {
        let relids = relids_copy(mcx, &run.root.ec(id).ec_relids);
        for rti in relids_members(&relids) {
            if rti == run.root.group_rtindex {
                continue;
            }
            let Some(rel_id) = run.root.simple_rel_array[rti as usize] else {
                debug_assert!(relids_is_member(rti, &run.root.outer_join_rels));
                continue;
            };
            debug_assert_eq!(run.root.rel(rel_id).reloptkind, RELOPT_BASEREL);
            let updated = relids_add_member(mcx, &run.root.rel(rel_id).eclass_indexes, id.0);
            run.root.rel_mut(rel_id).eclass_indexes = updated;
        }
    }
    Ok(Some(id))
}

fn expression_returns_set(expr: Node<'_>) -> PgResult<bool> {
    struct W;
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_FuncExpr => {
                    if node.as_func_expr().unwrap().funcretset {
                        return Ok(true);
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                NodeTag::T_OpExpr => {
                    if node.as_op_expr().unwrap().opretset {
                        return Ok(true);
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                NodeTag::T_Aggref | NodeTag::T_GroupingFunc | NodeTag::T_WindowFunc => Ok(false),
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
    }
    nodes_core::NodeWalker::visit(&mut W, expr)
}

pub fn generate_base_implied_equalities(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    run.root.ec_merging_done = true;

    for i in 0..run.root.eq_classes.len() {
        let ec = EcId(i as u32);
        if !live_ec(run, ec) {
            continue;
        }
        debug_assert!(!run.root.ec(ec).ec_broken);

        let mut can_generate_joinclause = false;
        if run.root.ec(ec).ec_members.len() > 1 {
            if run.root.ec(ec).ec_has_const {
                generate_base_implied_equalities_const(run, ec)?;
            } else {
                generate_base_implied_equalities_no_const(run, ec)?;
            }
            if run.root.ec(ec).ec_broken {
                generate_base_implied_equalities_broken(run, ec)?;
            }
            can_generate_joinclause = relids_num_members(&run.root.ec(ec).ec_relids) > 1;
        }

        let relids = relids_copy(mcx, &run.root.ec(ec).ec_relids);
        for rti in relids_members(&relids) {
            if rti == run.root.group_rtindex {
                continue;
            }
            let Some(rel_id) = run.root.simple_rel_array[rti as usize] else {
                debug_assert!(relids_is_member(rti, &run.root.outer_join_rels));
                continue;
            };
            debug_assert_eq!(run.root.rel(rel_id).reloptkind, RELOPT_BASEREL);
            let rel = run.root.rel_mut(rel_id);
            types_pathnodes::relids::relids_add_member_mut(mcx, &mut rel.eclass_indexes, ec.0);
            if can_generate_joinclause {
                rel.has_eclass_joins = true;
            }
        }
    }
    Ok(())
}

fn generate_base_implied_equalities_const(run: &mut PlannerRun<'_>, ec: EcId) -> PgResult<()> {
    let mcx = run.mcx;
    // Trivial var = const: push the original clause back unchanged.
    if run.root.ec(ec).ec_members.len() == 2 && run.root.ec(ec).ec_sources.len() == 1 {
        let rinfo = run.root.ec(ec).ec_sources[0];
        return planner_seams::distribute_restrictinfo_to_rels::call(run, rinfo);
    }
    assert_no_child_members(run, ec);

    // Prefer an actual Const over other pseudoconstants (Params) for the
    // benefit of constraint exclusion.
    let mut const_em: Option<EmId> = None;
    for m in 0..run.root.ec(ec).ec_members.len() {
        let em_id = run.root.ec(ec).ec_members[m];
        if run.root.em(em_id).em_is_const {
            const_em = Some(em_id);
            if run.root.expr_node(run.root.em(em_id).em_expr).node_tag() == NodeTag::T_Const {
                break;
            }
        }
    }
    let const_em = const_em.expect("ec_has_const EC has a const member");

    for m in 0..run.root.ec(ec).ec_members.len() {
        let cur_em = run.root.ec(ec).ec_members[m];
        debug_assert!(!run.root.em(cur_em).em_is_child);
        if cur_em == const_em {
            continue;
        }
        let eq_op = select_equality_operator(
            run,
            ec,
            run.root.em(cur_em).em_datatype,
            run.root.em(const_em).em_datatype,
        )?;
        if eq_op == 0 {
            run.root.ec_mut(ec).ec_broken = true;
            break;
        }
        let (collation, min_security) = {
            let e = run.root.ec(ec);
            (e.ec_collation, e.ec_min_security)
        };
        let qualscope = {
            let jd = run.root.em(const_em).em_jdomain;
            relids_copy(mcx, &run.root.join_domains[jd].jd_relids)
        };
        let cur_expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
        let const_expr = *run.root.expr_node(run.root.em(const_em).em_expr);
        let both_const = run.root.em(cur_em).em_is_const;
        let rinfo = planner_seams::process_implied_equality::call(
            run, eq_op, collation, cur_expr, const_expr, qualscope, min_security, both_const,
        )?;
        if let Some(rid) = rinfo {
            if !run.root.rinfo(rid).mergeopfamilies.is_empty() {
                let r = run.root.rinfo_mut(rid);
                r.left_ec = Some(ec);
                r.right_ec = Some(ec);
                r.left_em = Some(cur_em);
                r.right_em = Some(const_em);
                ec_add_derived_clause(run, ec, rid);
            }
        }
    }
    Ok(())
}

fn generate_base_implied_equalities_no_const(run: &mut PlannerRun<'_>, ec: EcId) -> PgResult<()> {
    let mcx = run.mcx;
    let mut prev_ems: PgVec<'_, Option<EmId>> = PgVec::new_in(mcx);
    for _ in 0..run.root.simple_rel_array_size as usize {
        prev_ems.push(None);
    }
    assert_no_child_members(run, ec);

    for m in 0..run.root.ec(ec).ec_members.len() {
        let cur_em = run.root.ec(ec).ec_members[m];
        debug_assert!(!run.root.em(cur_em).em_is_child);
        let Some(relid) = types_pathnodes::relids::relids_singleton_member(&run.root.em(cur_em).em_relids)
        else {
            continue;
        };
        debug_assert!((relid as i32) < run.root.simple_rel_array_size);

        if let Some(prev_em) = prev_ems[relid as usize] {
            let eq_op = select_equality_operator(
                run,
                ec,
                run.root.em(prev_em).em_datatype,
                run.root.em(cur_em).em_datatype,
            )?;
            if eq_op == 0 {
                run.root.ec_mut(ec).ec_broken = true;
                break;
            }
            let (collation, min_security) = {
                let e = run.root.ec(ec);
                (e.ec_collation, e.ec_min_security)
            };
            let qualscope = relids_copy(mcx, &run.root.em(cur_em).em_relids);
            let prev_expr = *run.root.expr_node(run.root.em(prev_em).em_expr);
            let cur_expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
            let rinfo = planner_seams::process_implied_equality::call(
                run, eq_op, collation, prev_expr, cur_expr, qualscope, min_security, false,
            )?;
            // Not recorded as a derived clause: non-join clauses are never
            // re-found via ec_derives.
            if let Some(rid) = rinfo {
                if !run.root.rinfo(rid).mergeopfamilies.is_empty() {
                    let r = run.root.rinfo_mut(rid);
                    r.left_ec = Some(ec);
                    r.right_ec = Some(ec);
                    r.left_em = Some(prev_em);
                    r.right_em = Some(cur_em);
                }
            }
        }
        prev_ems[relid as usize] = Some(cur_em);
    }

    // All member Vars must be available at every join this EC could act at.
    let ec_relids = relids_copy(mcx, &run.root.ec(ec).ec_relids);
    for m in 0..run.root.ec(ec).ec_members.len() {
        let cur_em = run.root.ec(ec).ec_members[m];
        let expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
        let mut vars: PgVec<'_, Node<'_>> = PgVec::new_in(mcx);
        planner_seams::pull_var_nodes::call(expr, &mut vars);
        planner_seams::add_vars_to_targetlist::call(run, &vars, &ec_relids)?;
    }
    Ok(())
}

fn generate_base_implied_equalities_broken(run: &mut PlannerRun<'_>, ec: EcId) -> PgResult<()> {
    for s in 0..run.root.ec(ec).ec_sources.len() {
        let rinfo = run.root.ec(ec).ec_sources[s];
        let throw_back = run.root.ec(ec).ec_has_const
            || relids_num_members(&run.root.rinfo(rinfo).required_relids) <= 1;
        if throw_back {
            planner_seams::distribute_restrictinfo_to_rels::call(run, rinfo)?;
        }
    }
    Ok(())
}

pub fn generate_join_implied_equalities<'mcx>(
    run: &mut PlannerRun<'mcx>,
    join_relids: &Relids<'mcx>,
    outer_relids: &Relids<'mcx>,
    inner_rel: RelId,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let inner_relids = relids_copy(mcx, &run.root.rel(inner_rel).relids);
    // ECs are marked with parent relids, so a child inner rel matches through
    // its topmost parent (C's nominal relids).
    let is_child = !relids_is_empty(&run.root.rel(inner_rel).top_parent_relids);
    let (nominal_inner_relids, nominal_join_relids) = if is_child {
        let ninner = relids_copy(mcx, &run.root.rel(inner_rel).top_parent_relids);
        let mut njoin = relids_union(mcx, outer_relids, &ninner);
        if let Some(s) = sjinfo {
            if s.ojrelid != 0 {
                njoin = relids_add_member(mcx, &njoin, s.ojrelid);
            }
        }
        (ninner, njoin)
    } else {
        (relids_copy(mcx, &inner_relids), relids_copy(mcx, join_relids))
    };

    let matching_ecs = if sjinfo.is_some_and(|s| s.ojrelid != 0) {
        get_eclass_indexes_for_relids(run, &nominal_join_relids)
    } else {
        get_common_eclass_indexes(run, &nominal_inner_relids, outer_relids)
    };

    for i in relids_members(&matching_ecs) {
        let ec = EcId(i as u32);
        if run.root.ec(ec).ec_has_const {
            continue;
        }
        if run.root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        debug_assert!(relids_overlap(&run.root.ec(ec).ec_relids, &nominal_join_relids));

        let mut sublist: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        if !run.root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_normal(
                run,
                ec,
                join_relids,
                outer_relids,
                &inner_relids,
            )?;
        }
        if run.root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                run,
                ec,
                &nominal_join_relids,
                outer_relids,
                &nominal_inner_relids,
                inner_rel,
            )?;
        }
        result.extend(sublist.iter().copied());
    }
    Ok(result)
}

pub fn generate_join_implied_equalities_for_ecs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    eclasses: &[EcId],
    join_relids: &Relids<'mcx>,
    outer_relids: &Relids<'mcx>,
    inner_rel: RelId,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let inner_relids = relids_copy(mcx, &run.root.rel(inner_rel).relids);
    let is_child = !relids_is_empty(&run.root.rel(inner_rel).top_parent_relids);
    let (nominal_inner_relids, nominal_join_relids) = if is_child {
        let ninner = relids_copy(mcx, &run.root.rel(inner_rel).top_parent_relids);
        let njoin = relids_union(mcx, outer_relids, &ninner);
        (ninner, njoin)
    } else {
        (relids_copy(mcx, &inner_relids), relids_copy(mcx, join_relids))
    };

    for &ec in eclasses {
        if run.root.ec(ec).ec_has_const {
            continue;
        }
        if run.root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        if !relids_overlap(&run.root.ec(ec).ec_relids, &nominal_join_relids) {
            continue;
        }
        let mut sublist: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        if !run.root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_normal(
                run,
                ec,
                join_relids,
                outer_relids,
                &inner_relids,
            )?;
        }
        if run.root.ec(ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                run,
                ec,
                &nominal_join_relids,
                outer_relids,
                &nominal_inner_relids,
                inner_rel,
            )?;
        }
        result.extend(sublist.iter().copied());
    }
    Ok(result)
}

fn expr_is_var_shaped(node: Node<'_>) -> bool {
    match node.node_tag() {
        NodeTag::T_Var => true,
        NodeTag::T_RelabelType => {
            node.as_relabel_type().unwrap().arg.node_tag() == NodeTag::T_Var
        }
        _ => false,
    }
}

fn generate_join_implied_equalities_normal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    join_relids: &Relids<'mcx>,
    outer_relids: &Relids<'mcx>,
    inner_relids: &Relids<'mcx>,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let mut outer_members: PgVec<'mcx, EmId> = PgVec::new_in(mcx);
    let mut inner_members: PgVec<'mcx, EmId> = PgVec::new_in(mcx);
    let mut new_members: PgVec<'mcx, EmId> = PgVec::new_in(mcx);

    // Child members need no explicit check: a child EM subset of join_relids
    // is exactly one belonging to a child rel of this join (C comment).
    let candidates = ec_members_for_relids(run, ec, join_relids);
    for m in 0..candidates.len() {
        let cur_em = candidates[m];
        let em_relids = relids_copy(mcx, &run.root.em(cur_em).em_relids);
        if !relids_is_subset(&em_relids, join_relids) {
            continue;
        }
        if relids_is_subset(&em_relids, outer_relids) {
            outer_members.push(cur_em);
        } else if relids_is_subset(&em_relids, inner_relids) {
            inner_members.push(cur_em);
        } else {
            new_members.push(cur_em);
        }
    }

    if !outer_members.is_empty() && !inner_members.is_empty() {
        let mut best_outer: Option<EmId> = None;
        let mut best_inner: Option<EmId> = None;
        let mut best_eq_op: u32 = 0;
        let mut best_score: i32 = -1;
        'outer: for &outer_em in outer_members.iter() {
            for &inner_em in inner_members.iter() {
                let eq_op = select_equality_operator(
                    run,
                    ec,
                    run.root.em(outer_em).em_datatype,
                    run.root.em(inner_em).em_datatype,
                )?;
                if eq_op == 0 {
                    continue;
                }
                let outer_expr = *run.root.expr_node(run.root.em(outer_em).em_expr);
                let inner_expr = *run.root.expr_node(run.root.em(inner_em).em_expr);
                let mut score = 0;
                if expr_is_var_shaped(outer_expr) {
                    score += 1;
                }
                if expr_is_var_shaped(inner_expr) {
                    score += 1;
                }
                if lsyscache::op_hashjoinable(eq_op, costsize::expr_type_typmod(outer_expr).0)? {
                    score += 1;
                }
                if score > best_score {
                    best_outer = Some(outer_em);
                    best_inner = Some(inner_em);
                    best_eq_op = eq_op;
                    best_score = score;
                    if best_score == 3 {
                        break 'outer;
                    }
                }
            }
        }
        if best_score < 0 {
            run.root.ec_mut(ec).ec_broken = true;
            return Ok(PgVec::new_in(mcx));
        }
        let rinfo = create_join_clause(
            run,
            ec,
            best_eq_op,
            best_outer.unwrap(),
            best_inner.unwrap(),
            Some(ec),
        )?;
        result.push(rinfo);
    }

    if !new_members.is_empty() {
        let mut old_members = outer_members;
        old_members.extend(inner_members.iter().copied());
        if let Some(&first_old) = old_members.first() {
            new_members.push(first_old);
        }
        let mut prev_em: Option<EmId> = None;
        for &cur_em in new_members.iter() {
            if let Some(prev) = prev_em {
                let eq_op = select_equality_operator(
                    run,
                    ec,
                    run.root.em(prev).em_datatype,
                    run.root.em(cur_em).em_datatype,
                )?;
                if eq_op == 0 {
                    run.root.ec_mut(ec).ec_broken = true;
                    return Ok(PgVec::new_in(mcx));
                }
                // Not redundant with other joinclauses: parent_ec stays unset.
                let rinfo = create_join_clause(run, ec, eq_op, prev, cur_em, None)?;
                result.push(rinfo);
            }
            prev_em = Some(cur_em);
        }
    }

    Ok(result)
}

fn generate_join_implied_equalities_broken<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    nominal_join_relids: &Relids<'mcx>,
    outer_relids: &Relids<'mcx>,
    nominal_inner_relids: &Relids<'mcx>,
    inner_rel: RelId,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    for s in 0..run.root.ec(ec).ec_sources.len() {
        let rinfo = run.root.ec(ec).ec_sources[s];
        let clause_relids = relids_copy(mcx, &run.root.rinfo(rinfo).required_relids);
        if relids_is_subset(&clause_relids, nominal_join_relids)
            && !relids_is_subset(&clause_relids, outer_relids)
            && !relids_is_subset(&clause_relids, nominal_inner_relids)
        {
            result.push(rinfo);
        }
    }
    // ec_sources clauses are stated in parent Vars; brute-force translate for
    // a child inner rel, possibly through multiple appendrel levels. The
    // translated RestrictInfos are not registered in ec_derives (C comment:
    // narrow corner case, no duplication expected).
    if !relids_is_empty(&run.root.rel(inner_rel).top_parent_relids) && !result.is_empty() {
        let top = run.root.rel(inner_rel).top_parent.expect("other rel has a top_parent");
        for i in 0..result.len() {
            result[i] =
                planner_seams::adjust_child_rinfo_multilevel::call(run, result[i], inner_rel, top)?;
        }
    }
    Ok(result)
}

fn select_equality_operator(
    run: &PlannerRun<'_>,
    ec: EcId,
    lefttype: u32,
    righttype: u32,
) -> PgResult<u32> {
    use types_pathnodes::COMPARE_EQ;
    let n = run.root.ec(ec).ec_opfamilies.len();
    for i in 0..n {
        let opfamily = run.root.ec(ec).ec_opfamilies[i];
        let opno =
            lsyscache::get_opfamily_member_for_cmptype(opfamily, lefttype, righttype, COMPARE_EQ)?;
        if opno == 0 {
            continue;
        }
        if run.root.ec(ec).ec_max_security == 0 {
            return Ok(opno);
        }
        if lsyscache::get_func_leakproof(lsyscache::get_opcode(opno)?)? {
            return Ok(opno);
        }
    }
    Ok(0)
}

pub(crate) fn create_join_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ec: EcId,
    opno: u32,
    leftem: EmId,
    rightem: EmId,
    parent_ec: Option<EcId>,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    if let Some(rinfo) = ec_search_clause_for_ems(run, ec, leftem, rightem, parent_ec) {
        return Ok(rinfo);
    }
    // Child EMs: build the parent-to-parent clause first so the child clause
    // can duplicate its rinfo_serial.
    let parent_rinfo = if run.root.em(leftem).em_is_child || run.root.em(rightem).em_is_child {
        let leftp = run.root.em(leftem).em_parent.unwrap_or(leftem);
        let rightp = run.root.em(rightem).em_parent.unwrap_or(rightem);
        Some(create_join_clause(run, ec, opno, leftp, rightp, parent_ec)?)
    } else {
        None
    };
    let (collation, min_security) = {
        let e = run.root.ec(ec);
        (e.ec_collation, e.ec_min_security)
    };
    let left_expr = *run.root.expr_node(run.root.em(leftem).em_expr);
    let right_expr = *run.root.expr_node(run.root.em(rightem).em_expr);
    let qualscope = relids_union(
        mcx,
        &run.root.em(leftem).em_relids,
        &run.root.em(rightem).em_relids,
    );
    let rinfo = planner_seams::build_implied_join_equality::call(
        run, opno, collation, left_expr, right_expr, qualscope, min_security,
    )?;
    // A pseudoconstant-translated child EM (UNION ALL const output) may leave
    // its relids out of clause_relids; force them in so
    // join_clause_is_movable_into evaluates the clause at the right place.
    if run.root.em(leftem).em_is_child {
        let add = relids_copy(mcx, &run.root.em(leftem).em_relids);
        let r = run.root.rinfo_mut(rinfo);
        r.clause_relids = relids_union(mcx, &r.clause_relids.clone(), &add);
    }
    if run.root.em(rightem).em_is_child {
        let add = relids_copy(mcx, &run.root.em(rightem).em_relids);
        let r = run.root.rinfo_mut(rinfo);
        r.clause_relids = relids_union(mcx, &r.clause_relids.clone(), &add);
    }
    {
        let serial = parent_rinfo.map(|pr| run.root.rinfo(pr).rinfo_serial);
        let r = run.root.rinfo_mut(rinfo);
        if let Some(serial) = serial {
            r.rinfo_serial = serial;
        }
        r.parent_ec = parent_ec;
        r.left_ec = Some(ec);
        r.right_ec = Some(ec);
        r.left_em = Some(leftem);
        r.right_em = Some(rightem);
    }
    ec_add_derived_clause(run, ec, rinfo);
    Ok(rinfo)
}

pub fn reconsider_outer_join_clauses(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    loop {
        let mut found = false;

        let mut i = 0;
        while i < run.root.left_join_clauses.len() {
            let rinfo = run.root.left_join_clauses[i].rinfo;
            let sjinfo = run.root.left_join_clauses[i].sjinfo.clone();
            if reconsider_outer_join_clause(run, rinfo, &sjinfo, true)? {
                found = true;
                run.root.left_join_clauses.remove(i);
                throw_back_dummy_clause(run, rinfo)?;
            } else {
                i += 1;
            }
        }

        let mut i = 0;
        while i < run.root.right_join_clauses.len() {
            let rinfo = run.root.right_join_clauses[i].rinfo;
            let sjinfo = run.root.right_join_clauses[i].sjinfo.clone();
            if reconsider_outer_join_clause(run, rinfo, &sjinfo, false)? {
                found = true;
                run.root.right_join_clauses.remove(i);
                throw_back_dummy_clause(run, rinfo)?;
            } else {
                i += 1;
            }
        }

        let mut i = 0;
        while i < run.root.full_join_clauses.len() {
            let rinfo = run.root.full_join_clauses[i].rinfo;
            let sjinfo = run.root.full_join_clauses[i].sjinfo.clone();
            if reconsider_full_join_clause(run, rinfo, &sjinfo)? {
                found = true;
                run.root.full_join_clauses.remove(i);
                throw_back_dummy_clause(run, rinfo)?;
            } else {
                i += 1;
            }
        }

        if !found {
            break;
        }
    }

    // Any remaining set-aside clauses go back to regular processing.
    let mut leftovers: PgVec<'_, RinfoId> = PgVec::new_in(mcx);
    leftovers.extend(run.root.left_join_clauses.iter().map(|c| c.rinfo));
    leftovers.extend(run.root.right_join_clauses.iter().map(|c| c.rinfo));
    leftovers.extend(run.root.full_join_clauses.iter().map(|c| c.rinfo));
    for rinfo in leftovers {
        planner_seams::distribute_restrictinfo_to_rels::call(run, rinfo)?;
    }
    Ok(())
}

// The deduction made the OJ clause redundant, but the join must not look
// clauseless: throw back constant-TRUE with the same required_relids.
fn throw_back_dummy_clause(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    let mcx = run.mcx;
    let (is_pushed_down, has_clone, is_clone, required, incompatible, outer) = {
        let ri = run.root.rinfo(rinfo);
        (
            ri.is_pushed_down,
            ri.has_clone,
            ri.is_clone,
            relids_copy(mcx, &ri.required_relids),
            relids_copy(mcx, &ri.incompatible_relids),
            relids_copy(mcx, &ri.outer_relids),
        )
    };
    let clause = clauses::make_bool_const(mcx, true, false)?;
    let dummy = planner_seams::make_restrictinfo::call(
        run,
        clause,
        is_pushed_down,
        has_clone,
        is_clone,
        false,
        0,
        required,
        incompatible,
        outer,
    )?;
    planner_seams::distribute_restrictinfo_to_rels::call(run, dummy)
}

fn reconsider_outer_join_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    sjinfo: &SpecialJoinInfo<'mcx>,
    outer_on_left: bool,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("outer-join clause is an opclause");
    let opno = op.opno;
    let collation = op.inputcollid;
    let (left_type, right_type) = lsyscache::op_input_types(opno)?;
    let (outervar, innervar, inner_datatype, inner_relids) = if outer_on_left {
        (
            op.args.nth(0),
            op.args.nth(1),
            right_type,
            relids_copy(mcx, &run.root.rinfo(rinfo).right_relids),
        )
    } else {
        (
            op.args.nth(1),
            op.args.nth(0),
            left_type,
            relids_copy(mcx, &run.root.rinfo(rinfo).left_relids),
        )
    };
    let mergeopfamilies = pgvec_clone_shallow(mcx, &run.root.rinfo(rinfo).mergeopfamilies);

    for i in 0..run.root.eq_classes.len() {
        let cur_ec = EcId(i as u32);
        {
            let ec = run.root.ec(cur_ec);
            if ec.ec_merged.is_some() || !ec.ec_has_const || ec.ec_has_volatile {
                continue;
            }
            if collation != ec.ec_collation {
                continue;
            }
            if mergeopfamilies.as_slice() != ec.ec_opfamilies.as_slice() {
                continue;
            }
        }
        assert_no_child_members(run, cur_ec);

        let mut matched = false;
        for m in 0..run.root.ec(cur_ec).ec_members.len() {
            let em_id = run.root.ec(cur_ec).ec_members[m];
            debug_assert!(!run.root.em(em_id).em_is_child);
            if types_nodes::equal(outervar, *run.root.expr_node(run.root.em(em_id).em_expr)) {
                matched = true;
                break;
            }
        }
        if !matched {
            continue;
        }

        let mut derived = false;
        for m in 0..run.root.ec(cur_ec).ec_members.len() {
            let cur_em = run.root.ec(cur_ec).ec_members[m];
            if !run.root.em(cur_em).em_is_const {
                continue;
            }
            let eq_op =
                select_equality_operator(run, cur_ec, inner_datatype, run.root.em(cur_em).em_datatype)?;
            if eq_op == 0 {
                continue;
            }
            let (ec_collation, min_security) = {
                let e = run.root.ec(cur_ec);
                (e.ec_collation, e.ec_min_security)
            };
            let const_expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
            let mut newrinfo = planner_seams::build_implied_join_equality::call(
                run,
                eq_op,
                ec_collation,
                innervar,
                const_expr,
                relids_copy(mcx, &inner_relids),
                min_security,
            )?;
            let jdomain = find_join_domain(run, &sjinfo.syn_righthand);
            if process_equivalence(run, &mut newrinfo, jdomain)? {
                derived = true;
            }
        }
        // OUTERVAR appears in at most one EC.
        return Ok(derived);
    }
    Ok(false)
}

fn reconsider_full_join_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("full-join clause is an opclause");
    let opno = op.opno;
    let collation = op.inputcollid;
    let (left_type, right_type) = lsyscache::op_input_types(opno)?;
    let leftvar = op.args.nth(0);
    let rightvar = op.args.nth(1);
    let left_relids = relids_copy(mcx, &run.root.rinfo(rinfo).left_relids);
    let right_relids = relids_copy(mcx, &run.root.rinfo(rinfo).right_relids);
    let mergeopfamilies = pgvec_clone_shallow(mcx, &run.root.rinfo(rinfo).mergeopfamilies);

    for i in 0..run.root.eq_classes.len() {
        let cur_ec = EcId(i as u32);
        {
            let ec = run.root.ec(cur_ec);
            if ec.ec_merged.is_some() || !ec.ec_has_const || ec.ec_has_volatile {
                continue;
            }
            if collation != ec.ec_collation {
                continue;
            }
            if mergeopfamilies.as_slice() != ec.ec_opfamilies.as_slice() {
                continue;
            }
        }
        assert_no_child_members(run, cur_ec);

        // Look for COALESCE(leftvar, rightvar) among the members. The
        // COALESCE args carry the full join's nullingrel bit which must be
        // stripped before comparing (remove_nulling_relids in C).
        let mut coal_idx: Option<usize> = None;
        for m in 0..run.root.ec(cur_ec).ec_members.len() {
            let em_id = run.root.ec(cur_ec).ec_members[m];
            debug_assert!(!run.root.em(em_id).em_is_child);
            let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
            let Some(cexpr) = em_expr.as_coalesce_expr() else { continue };
            if cexpr.args.len() != 2 {
                continue;
            }
            let cfirst = strip_ojrelid_nulling(mcx, cexpr.args.nth(0), sjinfo.ojrelid)?;
            let csecond = strip_ojrelid_nulling(mcx, cexpr.args.nth(1), sjinfo.ojrelid)?;
            if types_nodes::equal(leftvar, cfirst) && types_nodes::equal(rightvar, csecond) {
                coal_idx = Some(m);
                break;
            }
        }
        let Some(coal_idx) = coal_idx else { continue };

        let mut matchleft = false;
        let mut matchright = false;
        for m in 0..run.root.ec(cur_ec).ec_members.len() {
            let cur_em = run.root.ec(cur_ec).ec_members[m];
            if !run.root.em(cur_em).em_is_const {
                continue;
            }
            let em_datatype = run.root.em(cur_em).em_datatype;
            let const_expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
            let (ec_collation, min_security) = {
                let e = run.root.ec(cur_ec);
                (e.ec_collation, e.ec_min_security)
            };
            let eq_op = select_equality_operator(run, cur_ec, left_type, em_datatype)?;
            if eq_op != 0 {
                let mut newrinfo = planner_seams::build_implied_join_equality::call(
                    run,
                    eq_op,
                    ec_collation,
                    leftvar,
                    const_expr,
                    relids_copy(mcx, &left_relids),
                    min_security,
                )?;
                let jdomain = find_join_domain(run, &sjinfo.syn_lefthand);
                if process_equivalence(run, &mut newrinfo, jdomain)? {
                    matchleft = true;
                }
            }
            let eq_op = select_equality_operator(run, cur_ec, right_type, em_datatype)?;
            if eq_op != 0 {
                let mut newrinfo = planner_seams::build_implied_join_equality::call(
                    run,
                    eq_op,
                    ec_collation,
                    rightvar,
                    const_expr,
                    relids_copy(mcx, &right_relids),
                    min_security,
                )?;
                let jdomain = find_join_domain(run, &sjinfo.syn_righthand);
                if process_equivalence(run, &mut newrinfo, jdomain)? {
                    matchright = true;
                }
            }
        }

        if matchleft && matchright {
            // The added restrictions pin the COALESCE's value; drop it.
            run.root.ec_mut(cur_ec).ec_members.remove(coal_idx);
            return Ok(true);
        }
        break;
    }
    Ok(false)
}

// remove_nulling_relids over the single ojrelid bit, expression-copy form.
fn strip_ojrelid_nulling<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    ojrelid: u32,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            if v.varlevelsup == 0 && v.varnullingrels.is_member(ojrelid as i32) {
                let mut nv = types_nodes::primnodes::Var {
                    varnullingrels: v.varnullingrels.clone_in(mcx)?,
                    ..*v
                };
                nv.varnullingrels.del_member(ojrelid as i32);
                return Node::mk(mcx, nv);
            }
            Ok(node)
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = strip_ojrelid_nulling(mcx, r.arg, ojrelid)?;
            if arg.ptr_eq(r.arg) {
                return Ok(node);
            }
            Node::mk(mcx, types_nodes::primnodes::RelabelType { arg, ..*r })
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            if phv.phlevelsup == 0
                && (phv.phnullingrels.is_member(ojrelid as i32)
                    || phv.phrels.is_member(ojrelid as i32))
            {
                let new_expr = strip_ojrelid_nulling(mcx, phv.phexpr, ojrelid)?;
                let mut phnullingrels = phv.phnullingrels.clone_in(mcx)?;
                let mut phrels = phv.phrels.clone_in(mcx)?;
                phnullingrels.del_member(ojrelid as i32);
                phrels.del_member(ojrelid as i32);
                debug_assert!(!phrels.is_empty());
                return Node::mk(
                    mcx,
                    types_nodes::primnodes::PlaceHolderVar {
                        phexpr: new_expr,
                        phrels,
                        phnullingrels,
                        phid: phv.phid,
                        phlevelsup: phv.phlevelsup,
                    },
                );
            }
            Ok(node)
        }
        _ => Ok(node),
    }
}

pub fn rebuild_eclass_attr_needed(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    for i in 0..run.root.eq_classes.len() {
        let ec = EcId(i as u32);
        if !live_ec(run, ec) {
            continue;
        }
        assert_no_child_members(run, ec);
        if run.root.ec(ec).ec_members.len() > 1 && !run.root.ec(ec).ec_has_const {
            let ec_relids = relids_copy(mcx, &run.root.ec(ec).ec_relids);
            for m in 0..run.root.ec(ec).ec_members.len() {
                let cur_em = run.root.ec(ec).ec_members[m];
                let expr = *run.root.expr_node(run.root.em(cur_em).em_expr);
                let mut vars: PgVec<'_, Node<'_>> = PgVec::new_in(mcx);
                planner_seams::pull_var_nodes::call(expr, &mut vars);
                planner_seams::add_vars_to_attr_needed::call(run, &vars, &ec_relids);
            }
        }
    }
    Ok(())
}

// remove_rel_from_eclass (analyzejoins.c), delete-member (subst < 0) form.
pub fn remove_rel_from_eclasses(run: &mut PlannerRun<'_>, relid: i32, ojrelid: i32) {
    let mcx = run.mcx;
    for i in 0..run.root.eq_classes.len() {
        let ec = EcId(i as u32);
        if !live_ec(run, ec) {
            continue;
        }
        if !relids_is_member(relid, &run.root.ec(ec).ec_relids)
            && !relids_is_member(ojrelid, &run.root.ec(ec).ec_relids)
        {
            continue;
        }
        assert_no_child_members(run, ec);
        {
            let stripped = types_pathnodes::relids::relids_del_member(
                mcx,
                &types_pathnodes::relids::relids_del_member(mcx, &run.root.ec(ec).ec_relids, relid),
                ojrelid,
            );
            run.root.ec_mut(ec).ec_relids = stripped;
        }
        let mut m = 0;
        while m < run.root.ec(ec).ec_members.len() {
            let em_id = run.root.ec(ec).ec_members[m];
            let touched = relids_is_member(relid, &run.root.em(em_id).em_relids)
                || relids_is_member(ojrelid, &run.root.em(em_id).em_relids);
            if touched {
                debug_assert!(!run.root.em(em_id).em_is_const);
                let stripped = types_pathnodes::relids::relids_del_member(
                    mcx,
                    &types_pathnodes::relids::relids_del_member(mcx, &run.root.em(em_id).em_relids, relid),
                    ojrelid,
                );
                run.root.em_mut(em_id).em_relids = stripped;
                if relids_is_empty(&run.root.em(em_id).em_relids) {
                    run.root.ec_mut(ec).ec_members.remove(m);
                    continue;
                }
            }
            m += 1;
        }
        for s in 0..run.root.ec(ec).ec_sources.len() {
            let rid = run.root.ec(ec).ec_sources[s];
            planner_seams::remove_rel_from_restrictinfo::call(run, rid, relid, ojrelid);
        }
        ec_clear_derived_clauses(run, ec);
    }
}

pub fn find_join_domain(run: &PlannerRun<'_>, relids: &Relids<'_>) -> usize {
    for (i, jd) in run.root.join_domains.iter().enumerate() {
        if relids_is_subset(&jd.jd_relids, relids) {
            return i;
        }
    }
    panic!("failed to find appropriate JoinDomain");
}

pub fn exprs_known_equal(
    run: &PlannerRun<'_>,
    item1: Node<'_>,
    item2: Node<'_>,
    opfamily: u32,
) -> bool {
    for i in 0..run.root.eq_classes.len() {
        let ec = EcId(i as u32);
        {
            let e = run.root.ec(ec);
            if e.ec_merged.is_some() || e.ec_has_volatile {
                continue;
            }
            // Broken ECs still prove equality of their members.
            if opfamily != 0 && !e.ec_opfamilies.contains(&opfamily) {
                continue;
            }
        }
        let mut item1member = false;
        let mut item2member = false;
        for m in 0..run.root.ec(ec).ec_members.len() {
            let em_id = run.root.ec(ec).ec_members[m];
            debug_assert!(!run.root.em(em_id).em_is_child);
            let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
            if types_nodes::equal(item1, em_expr) {
                item1member = true;
            } else if types_nodes::equal(item2, em_expr) {
                item2member = true;
            }
            if item1member && item2member {
                return true;
            }
        }
    }
    false
}

// match_eclasses_to_foreign_key_col (equivclass.c); on success also fills
// fkinfo.eclass[colno] and fkinfo.fk_eclass_member[colno].
pub fn match_eclasses_to_foreign_key_col(
    run: &mut PlannerRun<'_>,
    fkinfo_id: types_pathnodes::NodeId,
    colno: usize,
) -> PgResult<Option<EcId>> {
    let mcx = run.mcx;
    let (var1varno, var1attno, var2varno, var2attno, eqop) = {
        let fk = run.root.foreign_key(fkinfo_id);
        (fk.con_relid, fk.conkey[colno], fk.ref_relid, fk.confkey[colno], fk.conpfeqop[colno])
    };
    debug_assert!(run.root.ec_merging_done);
    let rel1 = find_base_rel(&run.root, var1varno as i32);
    let rel2 = find_base_rel(&run.root, var2varno as i32);
    let matching_ecs = types_pathnodes::relids::relids_intersect(
        mcx,
        &run.root.rel(rel1).eclass_indexes,
        &run.root.rel(rel2).eclass_indexes,
    );
    let mut opfamilies: Option<PgVec<'_, u32>> = None;
    for i in relids_members(&matching_ecs) {
        let ec = EcId(i as u32);
        if run.root.ec(ec).ec_has_volatile {
            continue;
        }
        // Broken ECs are okay to consider, per exprs_known_equal; child
        // members never appear in ec_members.
        let mut item1_em: Option<EmId> = None;
        let mut item2_em: Option<EmId> = None;
        for m in 0..run.root.ec(ec).ec_members.len() {
            let em_id = run.root.ec(ec).ec_members[m];
            debug_assert!(!run.root.em(em_id).em_is_child);
            let mut expr = *run.root.expr_node(run.root.em(em_id).em_expr);
            while expr.node_tag() == NodeTag::T_RelabelType {
                expr = expr.as_relabel_type().unwrap().arg;
            }
            let Some(var) = expr.as_var() else { continue };
            if var.varno == var1varno as i32 && var.varattno == var1attno {
                item1_em = Some(em_id);
            } else if var.varno == var2varno as i32 && var.varattno == var2attno {
                item2_em = Some(em_id);
            }
            if let (Some(_), Some(em2)) = (item1_em, item2_em) {
                if opfamilies.is_none() {
                    opfamilies = Some(lsyscache::get_mergejoin_opfamilies(mcx, eqop)?);
                }
                if opfamilies.as_ref().unwrap()[..] == run.root.ec(ec).ec_opfamilies[..] {
                    let fk = run.root.foreign_key_mut(fkinfo_id);
                    fk.eclass[colno] = Some(ec);
                    fk.fk_eclass_member[colno] = Some(em2);
                    return Ok(Some(ec));
                }
                break;
            }
        }
    }
    Ok(None)
}

pub fn find_derived_clause_for_ec_member(
    run: &mut PlannerRun<'_>,
    ec: EcId,
    em: EmId,
) -> Option<RinfoId> {
    debug_assert!(run.root.ec(ec).ec_has_const);
    debug_assert!(!run.root.em(em).em_is_const);
    ec_search_derived_clause_for_ems(run, ec, em, None, None)
}

pub fn generate_implied_equalities_for_column<'mcx, F>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    mut callback: F,
    prohibited_rels: &Relids<'mcx>,
) -> PgResult<PgVec<'mcx, RinfoId>>
where
    F: FnMut(&PlannerRun<'mcx>, RelId, EcId, EmId) -> bool,
{
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    debug_assert!(run.root.ec_merging_done);
    let is_child_rel =
        run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL;
    // Ancestor relids, to skip useless joins from a child to its own parents.
    let parent_relids =
        if is_child_rel { find_childrel_parents(&run.root, rel) } else { relids_empty() };

    let eclass_indexes = relids_copy(mcx, &run.root.rel(rel).eclass_indexes);
    for i in relids_members(&eclass_indexes) {
        let cur_ec = EcId(i as u32);
        debug_assert!(
            is_child_rel
                || relids_is_subset(&run.root.rel(rel).relids, &run.root.ec(cur_ec).ec_relids)
        );
        if run.root.ec(cur_ec).ec_has_const || run.root.ec(cur_ec).ec_members.len() <= 1 {
            continue;
        }

        let rel_relids = relids_copy(mcx, &run.root.rel(rel).relids);
        let mut cur_em: Option<EmId> = None;
        let candidates = ec_members_for_relids(run, cur_ec, &rel_relids);
        for m in 0..candidates.len() {
            let em_id = candidates[m];
            if relids_equal(&run.root.em(em_id).em_relids, &rel_relids)
                && callback(run, rel, cur_ec, em_id)
            {
                cur_em = Some(em_id);
                break;
            }
        }
        let Some(cur_em) = cur_em else { continue };

        // Only parent members can be join targets; children are never marked
        // useful for other rels.
        for m in 0..run.root.ec(cur_ec).ec_members.len() {
            let other_em = run.root.ec(cur_ec).ec_members[m];
            debug_assert!(!run.root.em(other_em).em_is_child);
            if other_em == cur_em
                || relids_overlap(&run.root.em(other_em).em_relids, &rel_relids)
            {
                continue;
            }
            if is_child_rel
                && relids_overlap(&parent_relids, &run.root.em(other_em).em_relids)
            {
                continue;
            }
            if relids_overlap(&run.root.em(other_em).em_relids, prohibited_rels) {
                continue;
            }
            let eq_op = select_equality_operator(
                run,
                cur_ec,
                run.root.em(cur_em).em_datatype,
                run.root.em(other_em).em_datatype,
            )?;
            if eq_op == 0 {
                continue;
            }
            let rinfo = create_join_clause(run, cur_ec, eq_op, cur_em, other_em, Some(cur_ec))?;
            result.push(rinfo);
        }

        if !result.is_empty() {
            break;
        }
    }
    Ok(result)
}

pub fn have_relevant_eclass_joinclause(run: &PlannerRun<'_>, rel1: RelId, rel2: RelId) -> bool {
    let mcx = run.mcx;
    let matching_ecs = get_common_eclass_indexes(
        run,
        &relids_copy(mcx, &run.root.rel(rel1).relids),
        &relids_copy(mcx, &run.root.rel(rel2).relids),
    );
    for i in relids_members(&matching_ecs) {
        let ec = EcId(i as u32);
        debug_assert!(relids_overlap(&run.root.rel(rel1).relids, &run.root.ec(ec).ec_relids));
        debug_assert!(relids_overlap(&run.root.rel(rel2).relids, &run.root.ec(ec).ec_relids));
        if run.root.ec(ec).ec_members.len() > 1 {
            return true;
        }
    }
    false
}

pub fn has_relevant_eclass_joinclause(run: &PlannerRun<'_>, rel1: RelId) -> bool {
    let mcx = run.mcx;
    let matched_ecs =
        get_eclass_indexes_for_relids(run, &relids_copy(mcx, &run.root.rel(rel1).relids));
    for i in relids_members(&matched_ecs) {
        let ec = EcId(i as u32);
        if run.root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        if !relids_is_subset(&run.root.ec(ec).ec_relids, &run.root.rel(rel1).relids) {
            return true;
        }
    }
    false
}

pub fn eclass_useful_for_merging(run: &PlannerRun<'_>, eclass: EcId, rel: RelId) -> bool {
    debug_assert!(run.root.ec(eclass).ec_merged.is_none());
    {
        let ec = run.root.ec(eclass);
        if ec.ec_has_const || ec.ec_members.len() <= 1 {
            return false;
        }
    }
    let rel_info = run.root.rel(rel);
    let relids = if relids_is_empty(&rel_info.top_parent_relids) {
        &rel_info.relids
    } else {
        &rel_info.top_parent_relids
    };
    if relids_is_subset(&run.root.ec(eclass).ec_relids, relids) {
        return false;
    }
    for m in 0..run.root.ec(eclass).ec_members.len() {
        let em_id = run.root.ec(eclass).ec_members[m];
        debug_assert!(!run.root.em(em_id).em_is_child);
        if !relids_overlap(&run.root.em(em_id).em_relids, relids) {
            return true;
        }
    }
    false
}

pub fn is_redundant_derived_clause(
    run: &PlannerRun<'_>,
    rinfo: RinfoId,
    clauselist: &[RinfoId],
) -> bool {
    let Some(parent_ec) = run.root.rinfo(rinfo).parent_ec else { return false };
    clauselist
        .iter()
        .any(|&other| run.root.rinfo(other).parent_ec == Some(parent_ec))
}

pub fn is_redundant_with_indexclauses(
    run: &PlannerRun<'_>,
    rinfo: RinfoId,
    indexclauses: &[IndexClause<'_>],
) -> bool {
    let parent_ec = run.root.rinfo(rinfo).parent_ec;
    for iclause in indexclauses {
        if iclause.lossy {
            continue;
        }
        let other = iclause.rinfo.expect("IndexClause rinfo");
        if rinfo == other {
            return true;
        }
        if parent_ec.is_some() && run.root.rinfo(other).parent_ec == parent_ec {
            return true;
        }
    }
    false
}

pub fn get_eclass_indexes_for_relids<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &Relids<'mcx>,
) -> Relids<'mcx> {
    let mcx = run.mcx;
    debug_assert!(run.root.ec_merging_done);
    let mut ec_indexes: Relids<'mcx> = relids_empty();
    for i in relids_members(relids) {
        if i == run.root.group_rtindex {
            continue;
        }
        let Some(rel) = run.root.simple_rel_array.get(i as usize).copied().flatten() else {
            debug_assert!(relids_is_member(i, &run.root.outer_join_rels));
            continue;
        };
        ec_indexes = relids_union(mcx, &ec_indexes, &run.root.rel(rel).eclass_indexes);
    }
    ec_indexes
}

pub fn get_common_eclass_indexes<'mcx>(
    run: &PlannerRun<'mcx>,
    relids1: &Relids<'mcx>,
    relids2: &Relids<'mcx>,
) -> Relids<'mcx> {
    let mcx = run.mcx;
    let rel1ecs = get_eclass_indexes_for_relids(run, relids1);
    let rel2ecs = if let Some(relid) = types_pathnodes::relids::relids_singleton_member(relids2) {
        relids_copy(
            mcx,
            &run.root.rel(find_base_rel(&run.root, relid)).eclass_indexes,
        )
    } else {
        get_eclass_indexes_for_relids(run, relids2)
    };
    types_pathnodes::relids::relids_intersect(mcx, &rel1ecs, &rel2ecs)
}

fn fill_ec_derives_key(
    leftem: EmId,
    rightem: Option<EmId>,
    parent_ec: Option<EcId>,
) -> ECDerivesKey {
    match rightem {
        None => ECDerivesKey { em1: None, em2: Some(leftem), parent_ec },
        Some(r) => {
            if leftem < r {
                ECDerivesKey { em1: Some(leftem), em2: Some(r), parent_ec }
            } else {
                ECDerivesKey { em1: Some(r), em2: Some(leftem), parent_ec }
            }
        }
    }
}

fn derives_hash_key(run: &PlannerRun<'_>, rid: RinfoId) -> ECDerivesKey {
    let ri = run.root.rinfo(rid);
    let left_em = ri.left_em.expect("derived clause has left_em");
    let right_em = ri.right_em.expect("derived clause has right_em");
    debug_assert!(!run.root.em(left_em).em_is_const);
    debug_assert!(ri.parent_ec.is_none() || !run.root.em(right_em).em_is_const);
    let right = if run.root.em(right_em).em_is_const { None } else { Some(right_em) };
    fill_ec_derives_key(left_em, right, ri.parent_ec)
}

fn ec_add_derived_clause(run: &mut PlannerRun<'_>, ec: EcId, rid: RinfoId) {
    let key = derives_hash_key(run, rid);
    let e = run.root.ec_mut(ec);
    e.ec_derives_list.push(rid);
    if let Some(hash) = e.ec_derives_hash.as_mut() {
        let prev = hash.insert(key, rid);
        debug_assert!(prev.is_none());
    }
}

fn ec_add_derived_clauses(run: &mut PlannerRun<'_>, ec: EcId, clauses: &[RinfoId]) {
    for &rid in clauses {
        if run.root.ec(ec).ec_derives_hash.is_some() {
            let key = derives_hash_key(run, rid);
            run.root
                .ec_mut(ec)
                .ec_derives_hash
                .as_mut()
                .unwrap()
                .insert(key, rid);
        }
        run.root.ec_mut(ec).ec_derives_list.push(rid);
    }
}

pub fn ec_clear_derived_clauses(run: &mut PlannerRun<'_>, ec: EcId) {
    let e = run.root.ec_mut(ec);
    e.ec_derives_list.clear();
    e.ec_derives_hash = None;
}

fn ec_build_derives_hash(run: &mut PlannerRun<'_>, ec: EcId) {
    debug_assert!(run.root.ec(ec).ec_derives_hash.is_none());
    let mcx = run.mcx;
    let mut hash = mcx::PgFxHashMap::with_capacity_and_hasher_in(
        run.root.ec(ec).ec_derives_list.len(),
        Default::default(),
        mcx,
    );
    for s in 0..run.root.ec(ec).ec_derives_list.len() {
        let rid = run.root.ec(ec).ec_derives_list[s];
        hash.insert(derives_hash_key(run, rid), rid);
    }
    run.root.ec_mut(ec).ec_derives_hash = Some(hash);
}

fn ec_search_clause_for_ems(
    run: &mut PlannerRun<'_>,
    ec: EcId,
    leftem: EmId,
    rightem: EmId,
    parent_ec: Option<EcId>,
) -> Option<RinfoId> {
    for s in 0..run.root.ec(ec).ec_sources.len() {
        let rid = run.root.ec(ec).ec_sources[s];
        let ri = run.root.rinfo(rid);
        if ri.parent_ec == parent_ec
            && ((ri.left_em == Some(leftem) && ri.right_em == Some(rightem))
                || (ri.left_em == Some(rightem) && ri.right_em == Some(leftem)))
        {
            return Some(rid);
        }
    }
    ec_search_derived_clause_for_ems(run, ec, leftem, Some(rightem), parent_ec)
}

fn ec_search_derived_clause_for_ems(
    run: &mut PlannerRun<'_>,
    ec: EcId,
    leftem: EmId,
    rightem: Option<EmId>,
    parent_ec: Option<EcId>,
) -> Option<RinfoId> {
    if run.root.ec(ec).ec_derives_hash.is_none()
        && run.root.ec(ec).ec_derives_list.len() >= EC_DERIVES_HASH_THRESHOLD
    {
        ec_build_derives_hash(run, ec);
    }
    if run.root.ec(ec).ec_derives_hash.is_some() {
        let key = fill_ec_derives_key(leftem, rightem, parent_ec);
        let found = run.root.ec(ec).ec_derives_hash.as_ref().unwrap().get(&key).copied();
        if let Some(rid) = found {
            debug_assert!(
                rightem.is_some()
                    || run
                        .root
                        .em(run.root.rinfo(rid).right_em.expect("derived clause right_em"))
                        .em_is_const
            );
            return Some(rid);
        }
        return None;
    }
    for s in 0..run.root.ec(ec).ec_derives_list.len() {
        let rid = run.root.ec(ec).ec_derives_list[s];
        let ri = run.root.rinfo(rid);
        match rightem {
            None => {
                if ri.left_em == Some(leftem) {
                    debug_assert!(run.root.em(ri.right_em.unwrap()).em_is_const);
                    return Some(rid);
                }
            }
            Some(r) => {
                if ri.parent_ec == parent_ec
                    && ((ri.left_em == Some(leftem) && ri.right_em == Some(r))
                        || (ri.left_em == Some(r) && ri.right_em == Some(leftem)))
                {
                    return Some(rid);
                }
            }
        }
    }
    None
}
