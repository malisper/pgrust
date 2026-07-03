//! split_pathtarget_at_srfs (tlist.c) + adjust_paths_for_srfs (planner.c),
//! depth-1 slice: [SRF-free input target, original target]; nested SRFs and
//! SRF-under-expression levels are loud.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{PathTarget, PtId, RelId};

use crate::run::PlannerRun;

pub fn is_srf_call(node: Node<'_>) -> bool {
    if let Some(fe) = node.as_func_expr() {
        return fe.funcretset;
    }
    if let Some(oe) = node.as_op_expr() {
        return oe.opretset;
    }
    false
}

pub fn split_pathtarget_at_srfs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    target: PtId,
) -> PgResult<(PgVec<'mcx, PtId>, PgVec<'mcx, bool>)> {
    let mcx = run.mcx;
    let n = run.root.pathtarget(target).exprs.len();
    let mut found_srf = false;
    // (expr, sortgroupref) items the SRF-free input level must emit; SRF-arg
    // vars precede top-level vars (C's level_input_vars ++ current_input_vars).
    let mut srf_items: PgVec<'mcx, (types_pathnodes::NodeId, u32)> = PgVec::new_in(mcx);
    let mut top_items: PgVec<'mcx, (types_pathnodes::NodeId, u32)> = PgVec::new_in(mcx);
    for i in 0..n {
        let t = run.root.pathtarget(target);
        let sgref = t.sortgrouprefs.get(i).copied().unwrap_or(0);
        let id = t.exprs[i];
        let node = *run.root.expr_node(id);
        if is_srf_call(node) {
            found_srf = true;
            let args = match node.as_func_expr() {
                Some(fe) => &fe.args,
                None => &node.as_op_expr().unwrap().args,
            };
            for arg in args {
                if coerce::expression_returns_set(arg) {
                    panic!(
                        "split_pathtarget_at_srfs (tlist.c): nested SRF — multi-level \
                         ProjectSet lane unported"
                    );
                }
                collect_input_items(run, arg, 0, &mut srf_items)?;
            }
        } else {
            if coerce::expression_returns_set(node) {
                panic!(
                    "split_pathtarget_at_srfs (tlist.c): SRF below the top level of a \
                     tlist expression — extra-Result projection lane unported"
                );
            }
            collect_input_items(run, node, sgref, &mut top_items)?;
        }
    }

    let mut targets: PgVec<'mcx, PtId> = PgVec::new_in(mcx);
    let mut contain_srfs: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    if !found_srf {
        targets.push(target);
        contain_srfs.push(false);
        return Ok((targets, contain_srfs));
    }

    let mut items = srf_items;
    for &(id, sgref) in top_items.iter() {
        merge_item(run, id, sgref, &mut items);
    }
    let mut input = PathTarget::new(mcx);
    let mut any_sgref = false;
    for &(id, sgref) in items.iter() {
        let node = *run.root.expr_node(id);
        if node.node_tag() != NodeTag::T_Var {
            let cost = crate::costsize::cost_qual_eval_node(node)?;
            input.cost.startup += cost.startup;
            input.cost.per_tuple += cost.per_tuple;
        }
        input.exprs.push(id);
        input.sortgrouprefs.push(sgref);
        any_sgref |= sgref != 0;
    }
    if !any_sgref {
        input.sortgrouprefs.clear();
    }
    let input_id = run.root.alloc_pathtarget(input);
    let mut tuple_width: i64 = 0;
    for i in 0..run.root.pathtarget(input_id).exprs.len() {
        let expr = run.root.pathtarget(input_id).exprs[i];
        tuple_width += crate::costsize::get_expr_width(run, expr)? as i64;
    }
    run.root.pathtarget_mut(input_id).width = crate::costsize::clamp_width_est(tuple_width);

    targets.push(input_id);
    targets.push(target);
    contain_srfs.push(false);
    contain_srfs.push(true);
    Ok((targets, contain_srfs))
}

// add_sp_item_to_pathtarget dedup: equal() merges, zero sortgroupref
// acquires a nonzero one, conflicting nonzero refs stay separate.
fn collect_input_items<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    sgref: u32,
    items: &mut PgVec<'mcx, (types_pathnodes::NodeId, u32)>,
) -> PgResult<()> {
    let vars = vars::pull_var_clause(run.mcx, node, 0)?;
    let whole_is_var = node.node_tag() == NodeTag::T_Var;
    for v in &vars {
        let item_sgref = if whole_is_var { sgref } else { 0 };
        let id = run.intern_expr(v);
        merge_item(run, id, item_sgref, items);
    }
    Ok(())
}

fn merge_item<'mcx>(
    run: &PlannerRun<'mcx>,
    id: types_pathnodes::NodeId,
    sgref: u32,
    items: &mut PgVec<'mcx, (types_pathnodes::NodeId, u32)>,
) {
    for existing in items.iter_mut() {
        if !types_nodes::equal(*run.root.expr_node(existing.0), *run.root.expr_node(id)) {
            continue;
        }
        if existing.1 == 0 {
            existing.1 = sgref;
            return;
        }
        if sgref == 0 || sgref == existing.1 {
            return;
        }
    }
    items.push((id, sgref));
}

// adjust_paths_for_srfs (planner.c); cheapest pointers refreshed by the
// caller's set_cheapest instead of C's in-place swap (same outcome).
pub fn adjust_paths_for_srfs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    targets: &PgVec<'mcx, PtId>,
    targets_contain_srfs: &PgVec<'mcx, bool>,
) -> PgResult<()> {
    debug_assert!(targets.len() == targets_contain_srfs.len());
    debug_assert!(!targets_contain_srfs[0]);
    if targets.len() == 1 {
        return Ok(());
    }
    debug_assert!(run.root.rel(rel_id).partial_pathlist.is_empty());
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel_id).pathlist);
    for (i, path_id) in paths.iter().enumerate() {
        debug_assert!(run.root.path(*path_id).base().param_info.is_none());
        let mut newpath = *path_id;
        for (lvl, &target) in targets.iter().enumerate().skip(1) {
            newpath = if targets_contain_srfs[lvl] {
                let p = crate::pathnode::create_set_projection_path(run, rel_id, newpath, target)?;
                run.root.alloc_path(p)
            } else {
                panic!(
                    "adjust_paths_for_srfs (planner.c): SRF-free upper level \
                     (apply_projection_to_path leg) unreachable on the depth-1 slice"
                );
            };
        }
        run.root.rel_mut(rel_id).pathlist[i] = newpath;
    }
    Ok(())
}
