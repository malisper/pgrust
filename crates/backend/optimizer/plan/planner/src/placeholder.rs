//! placeholder.c: PlaceHolderVar / PlaceHolderInfo manipulation.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::PlaceHolderVar;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{PhInfoId, PlaceHolderInfo, RelId, Relids};

use crate::relnode::{
    find_base_rel, relids_copy, relids_difference, relids_intersect, relids_is_empty,
    relids_is_member, relids_is_subset, relids_members, relids_singleton, relids_singleton_member,
    relids_union,
};
use crate::run::PlannerRun;

pub fn make_placeholder_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    phrels: types_nodes::Bitmapset<'mcx>,
) -> PgResult<Node<'mcx>> {
    run.glob.last_ph_id += 1;
    Node::mk(
        run.mcx,
        PlaceHolderVar {
            phexpr: expr,
            phrels,
            phnullingrels: types_nodes::Bitmapset::empty(),
            phid: run.glob.last_ph_id,
            phlevelsup: 0,
        },
    )
}

fn node_bms_to_relids<'mcx>(mcx: Mcx<'mcx>, bms: &types_nodes::Bitmapset<'mcx>) -> Relids<'mcx> {
    // Planner-arena set from the nodes-side words; value-identical to the
    // historical per-member union-of-singletons loop.
    crate::relnode::relids_from_words(mcx, bms.as_words())
}

fn relids_to_node_bms<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &Relids<'mcx>,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    let mut out = types_nodes::Bitmapset::empty();
    for m in relids_members(relids) {
        out.add_member(mcx, m)?;
    }
    Ok(out)
}

pub fn find_placeholder_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    phv: &PlaceHolderVar<'mcx>,
) -> PgResult<PhInfoId> {
    assert!(phv.phlevelsup == 0, "find_placeholder_info: phlevelsup > 0");
    let mcx = run.mcx;
    if let Some(Some(id)) = run.root.placeholder_array.get(phv.phid as usize) {
        debug_assert!(run.root.phinfo(*id).phid == phv.phid);
        return Ok(*id);
    }
    assert!(
        !run.root.placeholdersFrozen,
        "too late to create a new PlaceHolderInfo"
    );

    // DIVERGENCE from C's pull_varnos(root, phexpr): a nested PHV inside
    // phexpr contributes phrels here, not ph_eval_at. The C-shaped eval_at
    // narrows a nested PHV's evaluation to below joins our join-removal
    // residue keeps (join.sql RHS-removal family) and the placeholder
    // consumers then fail setrefs; restore with join-removal completion.
    let rels_used = {
        let bms = vars::pull_varnos(mcx, phv.phexpr)?;
        node_bms_to_relids(mcx, &bms)
    };
    let phrels = node_bms_to_relids(mcx, &phv.phrels);
    let ph_lateral = {
        let d = relids_difference(mcx, &rels_used, &phrels);
        if relids_is_empty(&d) {
            crate::relnode::relids_empty()
        } else {
            d
        }
    };
    let mut ph_eval_at = relids_intersect(mcx, &rels_used, &phrels);
    if relids_is_empty(&ph_eval_at) {
        ph_eval_at = relids_copy(mcx, &phrels);
        assert!(!relids_is_empty(&ph_eval_at));
    }
    let ph_width = lsyscache::get_typavgwidth(
        nodes_core::expr_type(phv.phexpr),
        nodes_core::expr_typmod(phv.phexpr),
    )?;
    let ph_var_phexpr = run.intern_expr(phv.phexpr);
    let id = run.root.alloc_phinfo(PlaceHolderInfo {
        phid: phv.phid,
        ph_var_phexpr,
        ph_var_phrels: phrels,
        ph_eval_at,
        ph_lateral,
        ph_needed: crate::relnode::relids_empty(),
        ph_width,
    });
    run.root.placeholder_list.push(id);
    if run.root.placeholder_array.len() <= phv.phid as usize {
        run.root
            .placeholder_array
            .resize(phv.phid as usize + 1, None);
        run.root.placeholder_array_size = run.root.placeholder_array.len() as i32;
    }
    run.root.placeholder_array[phv.phid as usize] = Some(id);

    let phexpr = *run.root.expr_node(ph_var_phexpr);
    find_placeholders_in_expr(run, phexpr)?;
    Ok(id)
}

pub fn find_placeholders_in_jointree<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    debug_assert!(!run.root.placeholdersFrozen);
    if run.glob.last_ph_id == 0 {
        return Ok(());
    }
    let jt = run.parse().jointree.expect("jointree is a FromExpr");
    for child in &jt.fromlist {
        find_placeholders_recurse(run, child)?;
    }
    match jt.quals {
        Some(q) => find_placeholders_in_expr(run, q),
        None => Ok(()),
    }
}

fn find_placeholders_recurse<'mcx>(run: &mut PlannerRun<'mcx>, jtnode: Node<'mcx>) -> PgResult<()> {
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => Ok(()),
        NodeTag::T_FromExpr => {
            let f = jtnode.as_from_expr().unwrap();
            for child in &f.fromlist {
                find_placeholders_recurse(run, child)?;
            }
            match f.quals {
                Some(q) => find_placeholders_in_expr(run, q),
                None => Ok(()),
            }
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().unwrap();
            find_placeholders_recurse(run, j.larg)?;
            find_placeholders_recurse(run, j.rarg)?;
            match j.quals {
                Some(q) => find_placeholders_in_expr(run, q),
                None => Ok(()),
            }
        }
        other => panic!("find_placeholders_recurse (placeholder.c): {other:?}"),
    }
}

fn find_placeholders_in_expr<'mcx>(run: &mut PlannerRun<'mcx>, expr: Node<'mcx>) -> PgResult<()> {
    let vars = vars::pull_var_clause(
        run.mcx,
        expr,
        vars::PVC_RECURSE_AGGREGATES
            | vars::PVC_RECURSE_WINDOWFUNCS
            | vars::PVC_INCLUDE_PLACEHOLDERS,
    )?;
    for node in &vars {
        if let Some(phv) = node.as_place_holder_var() {
            find_placeholder_info(run, phv)?;
        }
    }
    Ok(())
}

pub fn fix_placeholder_input_needed_levels<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let phexpr = *run.root.expr_node(run.root.phinfo(id).ph_var_phexpr);
        let eval_at = relids_copy(run.mcx, &run.root.phinfo(id).ph_eval_at);
        let vars = vars::pull_var_clause(
            run.mcx,
            phexpr,
            vars::PVC_RECURSE_AGGREGATES
                | vars::PVC_RECURSE_WINDOWFUNCS
                | vars::PVC_INCLUDE_PLACEHOLDERS,
        )?;
        add_vars_to_targetlist_incl_phvs(run, &vars, &eval_at)?;
    }
    Ok(())
}

pub fn rebuild_placeholder_attr_needed<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let phexpr = *run.root.expr_node(run.root.phinfo(id).ph_var_phexpr);
        let eval_at = relids_copy(run.mcx, &run.root.phinfo(id).ph_eval_at);
        let vars = vars::pull_var_clause(
            run.mcx,
            phexpr,
            vars::PVC_RECURSE_AGGREGATES
                | vars::PVC_RECURSE_WINDOWFUNCS
                | vars::PVC_INCLUDE_PLACEHOLDERS,
        )?;
        let mut v: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(run.mcx);
        v.extend(vars.iter());
        crate::initsplan::add_vars_to_attr_needed(run, &v, &eval_at);
    }
    Ok(())
}

// add_vars_to_targetlist (initsplan.c), PlaceHolderVar-bearing lists: Vars
// delegate to the initsplan leg, PHVs get ph_needed grown.
pub fn add_vars_to_targetlist_incl_phvs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    vars: &types_nodes::NodeList<'mcx>,
    where_needed: &Relids<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut plain: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for node in vars {
        if let Some(phv) = node.as_place_holder_var() {
            let id = find_placeholder_info(run, phv)?;
            let cur = crate::relnode::relids_take(&mut run.root.phinfo_mut(id).ph_needed);
            run.root.phinfo_mut(id).ph_needed = relids_union(mcx, &cur, where_needed);
        } else {
            plain.push(node);
        }
    }
    if !plain.is_empty() {
        crate::initsplan::add_vars_to_targetlist(run, &plain, where_needed)?;
    }
    Ok(())
}

pub fn add_placeholders_to_base_rels<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let (eval_at_singleton, needed_above) = {
            let phinfo = run.root.phinfo(id);
            (
                relids_singleton_member(&phinfo.ph_eval_at),
                !relids_is_subset(&phinfo.ph_needed, &phinfo.ph_eval_at),
            )
        };
        let Some(varno) = eval_at_singleton else {
            continue;
        };
        if !needed_above {
            continue;
        }
        let rel = find_base_rel(&run.root, varno);
        let phv_node = ph_var_node(run, id)?;
        let expr_id = run.intern_expr(phv_node);
        run.root.rel_reltarget_mut(rel).exprs.push(expr_id);
    }
    Ok(())
}

// The PlaceHolderInfo's ph_var (phnullingrels empty by convention),
// rematerialized from the decomposed fields.
pub fn ph_var_node<'mcx>(run: &mut PlannerRun<'mcx>, id: PhInfoId) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let phinfo = run.root.phinfo(id);
    let phexpr = *run.root.expr_node(phinfo.ph_var_phexpr);
    let phrels = relids_to_node_bms(mcx, &phinfo.ph_var_phrels)?;
    Node::mk(
        mcx,
        PlaceHolderVar {
            phexpr,
            phrels,
            phnullingrels: types_nodes::Bitmapset::empty(),
            phid: phinfo.phid,
            phlevelsup: 0,
        },
    )
}

pub fn add_placeholders_to_joinrel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    let relids = relids_copy(mcx, &run.root.rel(joinrel).relids);
    let mut tuple_width = run.root.rel_reltarget_mut(joinrel).width as i64;
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let (computable, needed_above, in_outer, in_inner, ph_width, ph_lateral) = {
            let phinfo = run.root.phinfo(id);
            (
                relids_is_subset(&phinfo.ph_eval_at, &relids),
                !relids_is_subset(&phinfo.ph_needed, &relids),
                relids_is_subset(&phinfo.ph_eval_at, &run.root.rel(outer_rel).relids),
                relids_is_subset(&phinfo.ph_eval_at, &run.root.rel(inner_rel).relids),
                phinfo.ph_width,
                relids_copy(mcx, &phinfo.ph_lateral),
            )
        };
        if !computable {
            continue;
        }
        if needed_above && !in_outer && !in_inner {
            let phv_node = ph_var_node(run, id)?;
            let phexpr = phv_node.as_place_holder_var().unwrap().phexpr;
            let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), phexpr)?;
            let expr_id = run.intern_expr(phv_node);
            let target = run.root.rel_reltarget_mut(joinrel);
            target.exprs.push(expr_id);
            target.cost.startup += cost.startup;
            target.cost.per_tuple += cost.per_tuple;
            tuple_width += ph_width as i64;
        }
        let cur = crate::relnode::relids_take(&mut run.root.rel_mut(joinrel).direct_lateral_relids);
        run.root.rel_mut(joinrel).direct_lateral_relids = relids_union(mcx, &cur, &ph_lateral);
    }
    run.root.rel_reltarget_mut(joinrel).width = crate::costsize::clamp_width_est(tuple_width);
    Ok(())
}

pub fn contain_placeholder_references_to<'mcx>(
    run: &PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
) -> PgResult<bool> {
    if run.glob.last_ph_id == 0 {
        return Ok(false);
    }
    struct W {
        relid: i32,
        sublevels_up: u32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_PlaceHolderVar => {
                    let phv = node.as_place_holder_var().unwrap();
                    if phv.phlevelsup == self.sublevels_up {
                        // phrels summarizes the contained expression; don't
                        // examine phnullingrels or recurse (C comment).
                        return Ok(phv.phrels.is_member(self.relid));
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                NodeTag::T_Query => {
                    self.sublevels_up += 1;
                    let r =
                        nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                    self.sublevels_up -= 1;
                    Ok(r)
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = nodes_core::query_tree_walker(q, self, 0)?;
            self.sublevels_up -= 1;
            Ok(r)
        }
    }
    use nodes_core::NodeWalker as _;
    W {
        relid,
        sublevels_up: 0,
    }
    .visit(clause)
}

pub fn get_placeholder_nulling_relids<'mcx>(run: &PlannerRun<'mcx>, id: PhInfoId) -> Relids<'mcx> {
    let mcx = run.mcx;
    let phinfo = run.root.phinfo(id);
    let mut result: Relids<'mcx> = crate::relnode::relids_empty();
    for relid in relids_members(&phinfo.ph_eval_at) {
        if relid == run.root.group_rtindex {
            continue;
        }
        let Some(rel) = run.root.simple_rel_array[relid as usize] else {
            debug_assert!(relids_is_member(relid, &run.root.outer_join_rels));
            continue;
        };
        result = relids_union(mcx, &result, &run.root.rel(rel).nulling_relids);
    }
    relids_difference(mcx, &result, &phinfo.ph_eval_at)
}

// substitute_phv_relids (prepjointree.c): retarget phrels from the pulled-up
// varno to its replacement relids. In-place per C; the pullup path owns the
// tree exclusively.
struct SubstPhvRelids<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    varno: i32,
    sublevels_up: u32,
    subrelids: &'a types_nodes::Bitmapset<'mcx>,
}
impl<'mcx> nodes_core::NodeWalker<'mcx> for SubstPhvRelids<'_, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().unwrap();
                if phv.phlevelsup == self.sublevels_up && phv.phrels.is_member(self.varno) {
                    let mcx = self.mcx;
                    let varno = self.varno;
                    let subrelids = self.subrelids;
                    // SAFETY: exclusive pullup-owned tree (module contract).
                    unsafe {
                        node.with_mut::<PlaceHolderVar, _>(|p| -> PgResult<()> {
                            p.phrels.add_members(mcx, subrelids)?;
                            p.phrels.del_member(varno);
                            Ok(())
                        })
                    }
                    .expect("PlaceHolderVar")?;
                    debug_assert!(!node.as_place_holder_var().unwrap().phrels.is_empty());
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r = nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                self.sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }
    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = nodes_core::query_tree_walker(q, self, 0)?;
        self.sublevels_up -= 1;
        Ok(r)
    }
}
pub fn substitute_phv_relids<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    subrelids: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    use nodes_core::NodeWalker as _;
    SubstPhvRelids {
        mcx,
        varno,
        sublevels_up: 0,
        subrelids,
    }
    .visit(node)?;
    Ok(())
}

pub fn substitute_phv_relids_query<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    varno: i32,
    subrelids: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    let mut w = SubstPhvRelids {
        mcx,
        varno,
        sublevels_up: 0,
        subrelids,
    };
    nodes_core::query_tree_walker(parse, &mut w, 0)?;
    Ok(())
}
