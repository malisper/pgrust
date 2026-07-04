use mcx::{box_new_in, vec_from_elem_in, Mcx, PgVec};
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::{
    Bitmapset, PathTarget, PlannerInfo, PtId, RangeTblEntryId, RelId, RelOptInfo, Relids,
    UpperRelationKind, RELOPT_BASEREL, RELOPT_UPPER_REL,
};

pub use types_pathnodes::relids::*;

pub fn setup_simple_rel_arrays<'mcx>(root: &mut PlannerInfo<'mcx>, nrtable: usize) {
    let size = nrtable + 1;
    root.simple_rel_array_size = size as i32;
    root.simple_rel_array.clear();
    root.simple_rte_array.clear();
    root.simple_rel_array.reserve(size);
    root.simple_rte_array.reserve(size);
    root.simple_rel_array.extend(core::iter::repeat(None).take(size));
    root.simple_rte_array.push(RangeTblEntryId::Invalid);
    for i in 0..nrtable {
        root.simple_rte_array
            .push(RangeTblEntryId::Parse { query: root.parse, index: i as u32 });
    }
    debug_assert!(root.append_rel_list.is_empty());
}

// build_simple_rel (relnode.c), parentless arm (inheritance children are the
// M2 partition lane).
pub fn build_simple_rel<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    relid: u32,
    rtekind: RTEKind,
) -> types_error::PgResult<RelId> {
    let eref_max_attr = match rtekind {
        RTEKind::RTE_FUNCTION | RTEKind::RTE_TABLEFUNC | RTEKind::RTE_VALUES
        | RTEKind::RTE_CTE | RTEKind::RTE_SUBQUERY => {
            run.rte(relid as usize).eref.expect("RTE has eref").colnames.len() as i16
        }
        _ => 0,
    };
    let root = &mut run.root;
    assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
    assert!(root.simple_rel_array[relid as usize].is_none(), "rel {relid} already exists");

    let mcx = root.mcx;
    let mut rel = RelOptInfo::new(mcx);
    rel.reloptkind = RELOPT_BASEREL;
    rel.relids = relids_singleton(mcx, relid);
    rel.consider_startup = root.tuple_fraction > 0.0;
    rel.relid = relid;
    rel.rtekind = rtekind as u32;
    rel.rel_parallel_workers = -1;
    rel.nparts = -1;
    rel.baserestrict_min_security = u32::MAX;
    rel.pathtarget_id = Some(empty_pathtarget_id(root));

    match rtekind {
        RTEKind::RTE_RELATION => {
            // rel.userid comes from the RTE's perminfo checkAsUser; RTEs on
            // this lane panicked earlier when perminfoindex != 0.
            rel.userid = 0;
        }
        RTEKind::RTE_RESULT => {
            // RTE_RESULT has no columns, nor could it have a whole-row Var.
            rel.min_attr = 0;
            rel.max_attr = -1;
        }
        RTEKind::RTE_FUNCTION | RTEKind::RTE_TABLEFUNC | RTEKind::RTE_VALUES
        | RTEKind::RTE_CTE | RTEKind::RTE_SUBQUERY => {
            rel.min_attr = 0;
            rel.max_attr = eref_max_attr;
            let span = (rel.max_attr - rel.min_attr + 1) as usize;
            rel.attr_widths = mcx::vec_from_elem_in(mcx, 0i32, span);
            rel.attr_needed = mcx::PgVec::new_in(mcx);
            for _ in 0..span {
                rel.attr_needed.push(None);
            }
        }
        other => panic!("build_simple_rel (relnode.c): rtekind {other:?}; M2 scan lane"),
    }

    let id = run.root.alloc_rel(rel);
    run.root.simple_rel_array[relid as usize] = Some(id);

    if rtekind == RTEKind::RTE_RELATION {
        let rte = run.rte(relid as usize);
        crate::plancat::get_relation_info(run, rte.relid, rte.inh, id)?;
    }

    Ok(id)
}

// build_simple_rel (relnode.c), inheritance-child arm: RELOPT_OTHER_MEMBER_REL
// plus parent back-links and apply_child_basequals.
pub fn build_simple_rel_child<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    relid: u32,
    parent: RelId,
) -> types_error::PgResult<RelId> {
    let rte = run.rte(relid as usize);
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    let root = &mut run.root;
    assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
    assert!(root.simple_rel_array[relid as usize].is_none(), "rel {relid} already exists");

    let mcx = root.mcx;
    let mut rel = RelOptInfo::new(mcx);
    rel.reloptkind = types_pathnodes::RELOPT_OTHER_MEMBER_REL;
    rel.relids = relids_singleton(mcx, relid);
    rel.consider_startup = root.tuple_fraction > 0.0;
    rel.relid = relid;
    rel.rtekind = RTEKind::RTE_RELATION as u32;
    rel.rel_parallel_workers = -1;
    rel.nparts = -1;
    rel.baserestrict_min_security = u32::MAX;
    rel.pathtarget_id = Some(empty_pathtarget_id(root));
    rel.userid = root.rel(parent).userid;
    rel.parent = Some(parent);
    let top = root.rel(parent).top_parent.unwrap_or(parent);
    rel.top_parent = Some(top);
    rel.top_parent_relids = relids_copy(mcx, &root.rel(top).relids);
    // A child rel is below the same outer joins as its parent, and gets the
    // parent's minimum lateral parameterization (any append path must use one
    // parameterization for every child anyway).
    rel.nulling_relids = relids_copy(mcx, &root.rel(parent).nulling_relids);
    rel.direct_lateral_relids = relids_copy(mcx, &root.rel(parent).direct_lateral_relids);
    rel.lateral_relids = relids_copy(mcx, &root.rel(parent).lateral_relids);
    rel.lateral_referencers = relids_copy(mcx, &root.rel(parent).lateral_referencers);

    let id = root.alloc_rel(rel);
    run.root.simple_rel_array[relid as usize] = Some(id);

    crate::plancat::get_relation_info(run, rte.relid, rte.inh, id)?;

    let appinfo = run.root.append_rel_array[relid as usize]
        .clone()
        .expect("child rel has an AppendRelInfo");
    if !crate::inherit::apply_child_basequals(run, parent, id, &appinfo)? {
        // mark_dummy_rel: constant-FALSE child qual, skip scanning.
        crate::allpaths::set_dummy_rel_pathlist(run, id)?;
    }
    Ok(id)
}

const PARTITION_MAX_KEYS: usize = 32;
const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;

fn part_schemes_match(a: &types_pathnodes::PartitionScheme<'_>, b: &types_pathnodes::PartitionScheme<'_>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => **x == **y,
        _ => false,
    }
}

pub fn rel_is_partitioned(root: &PlannerInfo<'_>, rel: RelId) -> bool {
    let r = root.rel(rel);
    r.part_scheme.is_some()
        && r.boundinfo.is_some()
        && r.nparts > 0
        && !r.part_rels.is_empty()
        && !crate::joinrels::is_dummy_rel(root, rel)
}

// build_joinrel_partition_info (relnode.c).
pub fn build_joinrel_partition_info<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &types_pathnodes::SpecialJoinInfo<'mcx>,
    restrictlist: &[types_pathnodes::RinfoId],
) -> types_error::PgResult<()> {
    if !crate::gucs::enable_partitionwise_join() {
        debug_assert!(!rel_is_partitioned(&run.root, joinrel));
        return Ok(());
    }
    if !part_schemes_match(&run.root.rel(outer_rel).part_scheme, &run.root.rel(inner_rel).part_scheme)
        || !run.root.rel(outer_rel).consider_partitionwise_join
        || !run.root.rel(inner_rel).consider_partitionwise_join
        || !have_partkey_equi_join(run, joinrel, outer_rel, inner_rel, sjinfo.jointype, restrictlist)?
    {
        debug_assert!(!rel_is_partitioned(&run.root, joinrel));
        return Ok(());
    }

    {
        let j = run.root.rel(joinrel);
        debug_assert!(
            j.part_scheme.is_none()
                && j.partexprs.is_empty()
                && j.nullable_partexprs.is_empty()
                && j.part_rels.is_empty()
                && j.boundinfo.is_none()
        );
    }
    // nparts/bounds/child rels are computed in try_partitionwise_join.
    let scheme_copy = match &run.root.rel(outer_rel).part_scheme {
        Some(s) => mcx::alloc_in(run.mcx, (**s).clone())?,
        None => unreachable!(),
    };
    run.root.rel_mut(joinrel).part_scheme = Some(scheme_copy);
    set_joinrel_partition_key_exprs(run, joinrel, outer_rel, inner_rel, sjinfo.jointype)?;
    run.root.rel_mut(joinrel).consider_partitionwise_join = true;
    Ok(())
}

// have_partkey_equi_join (relnode.c). The exprs_known_equal EC fallback is
// structurally dead here: eclass-lite ECs are single-expression and never
// merged, so no two distinct exprs are ever EC-proven equal.
fn have_partkey_equi_join<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    joinrel: RelId,
    rel1: RelId,
    rel2: RelId,
    jointype: types_pathnodes::JoinType,
    restrictlist: &[types_pathnodes::RinfoId],
) -> types_error::PgResult<bool> {
    let mcx = run.mcx;
    let partnatts = run.root.rel(rel1).part_scheme.as_ref().unwrap().partnatts as usize;
    let strategy = run.root.rel(rel1).part_scheme.as_ref().unwrap().strategy;
    let mut pk_known_equal = [false; PARTITION_MAX_KEYS];
    let mut num_equal_pks = 0usize;
    let joinrelids = relids_copy(mcx, &run.root.rel(joinrel).relids);
    let outer_join_rels = relids_copy(mcx, &run.root.outer_join_rels);

    for &rid in restrictlist {
        if types_pathnodes::is_outer_join(jointype)
            && crate::joinrels::rinfo_is_pushed_down(run, rid, &joinrelids)
        {
            continue;
        }
        {
            let ri = run.root.rinfo(rid);
            if !ri.can_join {
                continue;
            }
            if ri.mergeopfamilies.is_empty() && ri.hashjoinoperator == 0 {
                continue;
            }
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let opexpr = clause.as_op_expr().expect("equijoin clause is an OpExpr");
        let (mut expr1, mut expr2) = {
            let ri = run.root.rinfo(rid);
            if relids_is_subset(&ri.left_relids, &run.root.rel(rel1).relids)
                && relids_is_subset(&ri.right_relids, &run.root.rel(rel2).relids)
            {
                (opexpr.args.nth(0), opexpr.args.nth(1))
            } else if relids_is_subset(&ri.left_relids, &run.root.rel(rel2).relids)
                && relids_is_subset(&ri.right_relids, &run.root.rel(rel1).relids)
            {
                (opexpr.args.nth(1), opexpr.args.nth(0))
            } else {
                continue;
            }
        };
        let strict_op = lsyscache::op_strict(opexpr.opno)?;
        if strict_op {
            if relids_overlap(&run.root.rel(rel1).relids, &outer_join_rels) {
                expr1 = strip_nulling_relids(mcx, expr1, &outer_join_rels)?;
            }
            if relids_overlap(&run.root.rel(rel2).relids, &outer_join_rels) {
                expr2 = strip_nulling_relids(mcx, expr2, &outer_join_rels)?;
            }
        }
        let Some(ipk1) = match_expr_to_partition_keys(run, expr1, rel1, strict_op) else {
            continue;
        };
        let Some(ipk2) = match_expr_to_partition_keys(run, expr2, rel2, strict_op) else {
            continue;
        };
        if ipk1 != ipk2 {
            continue;
        }
        if pk_known_equal[ipk1] {
            continue;
        }
        let scheme = run.root.rel(rel1).part_scheme.as_ref().unwrap();
        if scheme.partcollation[ipk1] != opexpr.inputcollid {
            return Ok(false);
        }
        let partopfamily = scheme.partopfamily[ipk1];
        if strategy == PARTITION_STRATEGY_HASH {
            let hashop = run.root.rinfo(rid).hashjoinoperator;
            if hashop == 0 || !lsyscache::op_in_opfamily(hashop, partopfamily)? {
                continue;
            }
        } else if !run.root.rinfo(rid).mergeopfamilies.iter().any(|&f| f == partopfamily) {
            continue;
        }
        pk_known_equal[ipk1] = true;
        num_equal_pks += 1;
        if num_equal_pks == partnatts {
            return Ok(true);
        }
    }
    Ok(false)
}

// remove_nulling_relids (rewriteManip.c), copy-on-write expression form
// specialized to except_relids = NULL.
fn strip_nulling_relids<'mcx>(
    mcx: Mcx<'mcx>,
    node: types_nodes::Node<'mcx>,
    removable: &Relids<'mcx>,
) -> types_error::PgResult<types_nodes::Node<'mcx>> {
    use types_nodes::NodeTag;
    fn mutate<'mcx>(
        mcx: Mcx<'mcx>,
        node: types_nodes::Node<'mcx>,
        removable: &Relids<'mcx>,
    ) -> types_error::PgResult<Option<types_nodes::Node<'mcx>>> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().expect("Var");
                let mut nulling = v.varnullingrels.clone_in(mcx)?;
                if v.varlevelsup == 0 {
                    for m in relids_members(removable) {
                        nulling.del_member(m);
                    }
                }
                let newvar = types_nodes::primnodes::Var { varnullingrels: nulling, ..*v };
                Ok(Some(types_nodes::Node::mk(mcx, newvar)?))
            }
            NodeTag::T_PlaceHolderVar => {
                panic!("remove_nulling_relids_mutator (rewriteManip.c): PlaceHolderVar")
            }
            _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| mutate(mcx, n, removable)),
        }
    }
    Ok(mutate(mcx, node, removable)?.unwrap_or(node))
}

// match_expr_to_partition_keys (relnode.c).
fn match_expr_to_partition_keys(
    run: &crate::run::PlannerRun<'_>,
    expr: types_nodes::Node<'_>,
    rel: RelId,
    strict_op: bool,
) -> Option<usize> {
    let mut expr = expr;
    while let Some(r) = expr.as_relabel_type() {
        expr = r.arg;
    }
    let r = run.root.rel(rel);
    debug_assert!(r.part_scheme.is_some());
    for cnt in 0..r.part_scheme.as_ref().unwrap().partnatts as usize {
        for &id in r.partexprs[cnt].iter() {
            if types_nodes::equal::equal(*run.root.expr_node(id), expr) {
                return Some(cnt);
            }
        }
        if !strict_op {
            continue;
        }
        for &id in r.nullable_partexprs[cnt].iter() {
            if types_nodes::equal::equal(*run.root.expr_node(id), expr) {
                return Some(cnt);
            }
        }
    }
    None
}

// set_joinrel_partition_key_exprs (relnode.c).
fn set_joinrel_partition_key_exprs<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    jointype: types_pathnodes::JoinType,
) -> types_error::PgResult<()> {
    use types_pathnodes::{JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_SEMI};
    let mcx = run.mcx;
    let partnatts =
        run.root.rel(joinrel).part_scheme.as_ref().unwrap().partnatts as usize;
    let mut partexprs: PgVec<'mcx, PgVec<'mcx, types_pathnodes::NodeId>> = PgVec::new_in(mcx);
    let mut nullable_partexprs: PgVec<'mcx, PgVec<'mcx, types_pathnodes::NodeId>> =
        PgVec::new_in(mcx);
    for cnt in 0..partnatts {
        let outer_expr = pgvec_clone_shallow(mcx, &run.root.rel(outer_rel).partexprs[cnt]);
        let outer_null_expr =
            pgvec_clone_shallow(mcx, &run.root.rel(outer_rel).nullable_partexprs[cnt]);
        let inner_expr = pgvec_clone_shallow(mcx, &run.root.rel(inner_rel).partexprs[cnt]);
        let inner_null_expr =
            pgvec_clone_shallow(mcx, &run.root.rel(inner_rel).nullable_partexprs[cnt]);
        let mut partexpr: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
        let mut nullable_partexpr: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
        match jointype {
            JOIN_INNER => {
                partexpr.extend(outer_expr.iter().copied());
                partexpr.extend(inner_expr.iter().copied());
                nullable_partexpr.extend(outer_null_expr.iter().copied());
                nullable_partexpr.extend(inner_null_expr.iter().copied());
            }
            JOIN_SEMI | JOIN_ANTI => {
                partexpr.extend(outer_expr.iter().copied());
                nullable_partexpr.extend(outer_null_expr.iter().copied());
            }
            JOIN_LEFT => {
                partexpr.extend(outer_expr.iter().copied());
                nullable_partexpr.extend(inner_expr.iter().copied());
                nullable_partexpr.extend(outer_null_expr.iter().copied());
                nullable_partexpr.extend(inner_null_expr.iter().copied());
            }
            JOIN_FULL => {
                nullable_partexpr.extend(outer_expr.iter().copied());
                nullable_partexpr.extend(inner_expr.iter().copied());
                nullable_partexpr.extend(outer_null_expr.iter().copied());
                nullable_partexpr.extend(inner_null_expr.iter().copied());
                // COALESCE(l, r) per pair so JOIN USING equijoin exprs match.
                for &lid in outer_expr.iter().chain(outer_null_expr.iter()) {
                    for &rid in inner_expr.iter().chain(inner_null_expr.iter()) {
                        let larg = *run.root.expr_node(lid);
                        let rarg = *run.root.expr_node(rid);
                        let mut args = types_nodes::NodeList::nil();
                        args.lappend(mcx, larg)?;
                        args.lappend(mcx, rarg)?;
                        let c = types_nodes::primnodes::CoalesceExpr {
                            coalescetype: crate::costsize::expr_type_typmod(larg).0,
                            coalescecollid: crate::pathkeys::expr_collation(larg),
                            args,
                            location: -1,
                        };
                        let node = types_nodes::Node::mk(mcx, c)?;
                        let id = run.intern_expr(node);
                        nullable_partexpr.push(id);
                    }
                }
            }
            other => panic!("set_joinrel_partition_key_exprs (relnode.c): jointype {other}"),
        }
        partexprs.push(partexpr);
        nullable_partexprs.push(nullable_partexpr);
    }
    let j = run.root.rel_mut(joinrel);
    j.partexprs = partexprs;
    j.nullable_partexprs = nullable_partexprs;
    Ok(())
}

// build_child_join_rel (relnode.c); set_foreign_rel_properties and
// add_child_join_rel_equivalences are dead on this lane (no FDWs, eclass-lite
// never sets has_eclass_joins and child pathkeys come from child index paths).
pub fn build_child_join_rel<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
    parent_joinrel: RelId,
    restrictlist: &[types_pathnodes::RinfoId],
    sjinfo: &types_pathnodes::SpecialJoinInfo<'mcx>,
    appinfos: &[types_pathnodes::AppendRelInfo<'mcx>],
) -> types_error::PgResult<RelId> {
    let mcx = run.mcx;
    debug_assert!(matches!(
        run.root.rel(outer_rel).reloptkind,
        types_pathnodes::RELOPT_OTHER_MEMBER_REL | types_pathnodes::RELOPT_OTHER_JOINREL
    ));
    debug_assert!(run.root.rel(parent_joinrel).consider_partitionwise_join);

    let mut joinrel = RelOptInfo::new(mcx);
    joinrel.reloptkind = types_pathnodes::RELOPT_OTHER_JOINREL;
    joinrel.relids =
        crate::inherit::adjust_child_relids(mcx, &run.root.rel(parent_joinrel).relids, appinfos);
    joinrel.consider_startup = run.root.tuple_fraction > 0.0;
    joinrel.rtekind = types_nodes::parsenodes::RTEKind::RTE_JOIN as u32;
    joinrel.rel_parallel_workers = -1;
    joinrel.nparts = -1;
    joinrel.baserestrict_min_security = u32::MAX;
    joinrel.parent = Some(parent_joinrel);
    let top = run.root.rel(parent_joinrel).top_parent.unwrap_or(parent_joinrel);
    joinrel.top_parent = Some(top);
    joinrel.top_parent_relids = relids_copy(mcx, &run.root.rel(top).relids);
    joinrel.direct_lateral_relids =
        relids_copy(mcx, &run.root.rel(parent_joinrel).direct_lateral_relids);
    joinrel.lateral_relids = relids_copy(mcx, &run.root.rel(parent_joinrel).lateral_relids);
    joinrel.has_eclass_joins = run.root.rel(parent_joinrel).has_eclass_joins;
    joinrel.pathtarget_id =
        Some(run.root.alloc_pathtarget(types_pathnodes::PathTarget::new(mcx)));
    let joinrel = run.root.alloc_rel(joinrel);

    build_child_join_reltarget(run, parent_joinrel, joinrel, appinfos)?;

    {
        let parent_joininfo =
            pgvec_clone_shallow(mcx, &run.root.rel(parent_joinrel).joininfo);
        let mut joininfo: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(mcx);
        for &rid in parent_joininfo.iter() {
            joininfo.push(crate::inherit::adjust_child_rinfo(run, rid, appinfos)?);
        }
        run.root.rel_mut(joinrel).joininfo = joininfo;
    }

    build_joinrel_partition_info(run, joinrel, outer_rel, inner_rel, sjinfo, restrictlist)?;

    run.root.rel_mut(joinrel).consider_parallel =
        run.root.rel(parent_joinrel).consider_parallel;

    crate::costsize::set_joinrel_size_estimates(
        run, joinrel, outer_rel, inner_rel, sjinfo, restrictlist,
    )?;

    debug_assert!(crate::joinrels::find_join_rel(&run.root, &run.root.rel(joinrel).relids)
        .is_none());
    run.root.join_rel_list.push(joinrel);

    if run.root.rel(joinrel).has_eclass_joins
        || crate::pathkeys::has_useful_pathkeys(run, parent_joinrel)
    {
        crate::equivclass::add_child_join_rel_equivalences(run, appinfos, parent_joinrel, joinrel)?;
    }
    Ok(joinrel)
}

// build_child_join_reltarget (relnode.c).
fn build_child_join_reltarget<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parentrel: RelId,
    childrel: RelId,
    appinfos: &[types_pathnodes::AppendRelInfo<'mcx>],
) -> types_error::PgResult<()> {
    let mcx = run.mcx;
    let parent_exprs = pgvec_clone_shallow(mcx, &run.root.rel_reltarget(parentrel).exprs);
    let mut exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    for &eid in parent_exprs.iter() {
        let e = *run.root.expr_node(eid);
        let tr = crate::inherit::adjust_appendrel_attrs_multi(run, e, appinfos)?;
        exprs.push(run.intern_expr(tr));
    }
    let (cost, width) = {
        let p = run.root.rel_reltarget(parentrel);
        (p.cost, p.width)
    };
    let ct = run.rel_reltarget_id(childrel);
    let t = run.root.pathtarget_mut(ct);
    t.exprs = exprs;
    t.cost = cost;
    t.width = width;
    Ok(())
}
