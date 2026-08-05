//! split_pathtarget_at_srfs / split_pathtarget_at_srfs_grouping (tlist.c) +
//! adjust_paths_for_srfs (planner.c).

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{NodeId, PathTarget, PtId, RelId};

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

// split_pathtarget_item: (expr, sortgroupref).
type SpItem<'mcx> = (Node<'mcx>, u32);

struct SplitContext<'mcx> {
    mcx: Mcx<'mcx>,
    input_target_exprs: PgVec<'mcx, Node<'mcx>>,
    sanitize_group_rtindex: Option<i32>,
    level_srfs: PgVec<'mcx, PgVec<'mcx, SpItem<'mcx>>>,
    level_input_vars: PgVec<'mcx, PgVec<'mcx, SpItem<'mcx>>>,
    level_input_srfs: PgVec<'mcx, PgVec<'mcx, SpItem<'mcx>>>,
    current_input_vars: PgVec<'mcx, SpItem<'mcx>>,
    current_input_srfs: PgVec<'mcx, SpItem<'mcx>>,
    current_depth: usize,
    current_sgref: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for SplitContext<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        // An expression already computed in input_target acts like a Var
        // (setrefs replaces it with one); ignore its substructure. Matching
        // ignores the grouping nulling bit when crossing that boundary.
        let cmp_node = match self.sanitize_group_rtindex {
            Some(rt) => {
                crate::flatten_group::strip_group_nulling(self.mcx, node, rt)?.unwrap_or(node)
            }
            None => node,
        };
        if self.input_target_exprs.iter().any(|&e| types_nodes::equal(e, cmp_node)) {
            self.current_input_vars.push((node, self.current_sgref));
            return Ok(false);
        }
        match node.node_tag() {
            NodeTag::T_Var
            | NodeTag::T_PlaceHolderVar
            | NodeTag::T_Aggref
            | NodeTag::T_GroupingFunc
            | NodeTag::T_WindowFunc => {
                self.current_input_vars.push((node, self.current_sgref));
                return Ok(false);
            }
            _ => {}
        }
        if is_srf_call(node) {
            let item = (node, self.current_sgref);
            let save_vars =
                core::mem::replace(&mut self.current_input_vars, PgVec::new_in(self.mcx));
            let save_srfs =
                core::mem::replace(&mut self.current_input_srfs, PgVec::new_in(self.mcx));
            let save_depth = self.current_depth;
            self.current_depth = 0;
            self.current_sgref = 0;
            nodes_core::expression_tree_walker(node, self)?;
            let srf_depth = self.current_depth + 1;
            while srf_depth >= self.level_srfs.len() {
                self.level_srfs.push(PgVec::new_in(self.mcx));
                self.level_input_vars.push(PgVec::new_in(self.mcx));
                self.level_input_srfs.push(PgVec::new_in(self.mcx));
            }
            self.level_srfs[srf_depth].push(item);
            let civ = core::mem::replace(&mut self.current_input_vars, save_vars);
            let cis = core::mem::replace(&mut self.current_input_srfs, save_srfs);
            self.level_input_vars[srf_depth].extend(civ.iter().copied());
            self.level_input_srfs[srf_depth].extend(cis.iter().copied());
            self.current_input_srfs.push(item);
            self.current_depth = save_depth.max(srf_depth);
            return Ok(false);
        }
        self.current_sgref = 0;
        nodes_core::expression_tree_walker(node, self)
    }
}

pub fn split_pathtarget_at_srfs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    target: PtId,
    input_target: Option<PtId>,
) -> PgResult<(PgVec<'mcx, PtId>, PgVec<'mcx, bool>)> {
    split_pathtarget_at_srfs_extended(run, target, input_target, false)
}

// Variant for targets crossing the grouping boundary: C strips the grouping
// nulling bit before matching input_target, which only applies when
// parse.hasGroupRTE (loud upstream: flatten_group_exprs unported).
pub fn split_pathtarget_at_srfs_grouping<'mcx>(
    run: &mut PlannerRun<'mcx>,
    target: PtId,
    input_target: Option<PtId>,
) -> PgResult<(PgVec<'mcx, PtId>, PgVec<'mcx, bool>)> {
    split_pathtarget_at_srfs_extended(run, target, input_target, true)
}

fn split_pathtarget_at_srfs_extended<'mcx>(
    run: &mut PlannerRun<'mcx>,
    target: PtId,
    input_target: Option<PtId>,
    is_grouping_target: bool,
) -> PgResult<(PgVec<'mcx, PtId>, PgVec<'mcx, bool>)> {
    use nodes_core::NodeWalker;

    let mcx = run.mcx;
    if input_target == Some(target) {
        let mut targets = PgVec::new_in(mcx);
        targets.push(target);
        let mut contain_srfs = PgVec::new_in(mcx);
        contain_srfs.push(false);
        return Ok((targets, contain_srfs));
    }
    // Crossing the grouping boundary: strip the grouping RT index before
    // matching input_target, as set_upper_references does (tlist.c:1151-1165).
    let sanitize_group_rtindex = if is_grouping_target
        && run.parse().hasGroupRTE
        && !run.parse().groupingSets.is_nil()
    {
        Some(run.root.group_rtindex)
    } else {
        None
    };
    let mut input_target_exprs: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if let Some(it) = input_target {
        for i in 0..run.root.pathtarget(it).exprs.len() {
            let eid = run.root.pathtarget(it).exprs[i];
            input_target_exprs.push(*run.root.expr_node(eid));
        }
    }
    let mut ctx = SplitContext {
        mcx,
        input_target_exprs,
        sanitize_group_rtindex,
        level_srfs: PgVec::new_in(mcx),
        level_input_vars: PgVec::new_in(mcx),
        level_input_srfs: PgVec::new_in(mcx),
        current_input_vars: PgVec::new_in(mcx),
        current_input_srfs: PgVec::new_in(mcx),
        current_depth: 0,
        current_sgref: 0,
    };
    ctx.level_srfs.push(PgVec::new_in(mcx));
    ctx.level_input_vars.push(PgVec::new_in(mcx));
    ctx.level_input_srfs.push(PgVec::new_in(mcx));

    let mut max_depth = 0usize;
    let mut need_extra_projection = false;
    let n = run.root.pathtarget(target).exprs.len();
    for i in 0..n {
        let t = run.root.pathtarget(target);
        let sgref = t.sortgrouprefs.get(i).copied().unwrap_or(0);
        let node = *run.root.expr_node(t.exprs[i]);
        ctx.current_sgref = sgref;
        ctx.current_depth = 0;
        ctx.visit(node)?;
        if ctx.current_depth == 0 {
            continue;
        }
        if max_depth < ctx.current_depth {
            max_depth = ctx.current_depth;
            need_extra_projection = false;
        }
        // A maximum-depth SRF below the top of its expression forces an extra
        // Result level for the enclosing scalar expression.
        if max_depth == ctx.current_depth && !is_srf_call(node) {
            need_extra_projection = true;
        }
    }

    let mut targets: PgVec<'mcx, PtId> = PgVec::new_in(mcx);
    let mut contain_srfs: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    if max_depth == 0 {
        targets.push(target);
        contain_srfs.push(false);
        return Ok((targets, contain_srfs));
    }

    if need_extra_projection {
        ctx.level_srfs.push(PgVec::new_in(mcx));
        let civ = core::mem::replace(&mut ctx.current_input_vars, PgVec::new_in(mcx));
        let cis = core::mem::replace(&mut ctx.current_input_srfs, PgVec::new_in(mcx));
        ctx.level_input_vars.push(civ);
        ctx.level_input_srfs.push(cis);
    } else {
        let civ = core::mem::replace(&mut ctx.current_input_vars, PgVec::new_in(mcx));
        let cis = core::mem::replace(&mut ctx.current_input_srfs, PgVec::new_in(mcx));
        ctx.level_input_vars[max_depth].extend(civ.iter().copied());
        ctx.level_input_srfs[max_depth].extend(cis.iter().copied());
    }

    let nlevels = ctx.level_srfs.len();
    let mut prev_level_exprs: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
    for lvl in 0..nlevels {
        let has_srfs = !ctx.level_srfs[lvl].is_empty();
        let tid = if lvl == nlevels - 1 {
            target
        } else {
            let mut items: PgVec<'mcx, SpItem<'mcx>> = PgVec::new_in(mcx);
            for &it in ctx.level_srfs[lvl].iter() {
                add_sp_item(&mut items, it);
            }
            for j in (lvl + 1)..nlevels {
                for &it in ctx.level_input_vars[j].iter() {
                    add_sp_item(&mut items, it);
                }
            }
            // SRFs computed at earlier levels and needed later propagate only
            // if the previous level actually emitted them.
            for j in (lvl + 1)..nlevels {
                for &it in ctx.level_input_srfs[j].iter() {
                    let member = prev_level_exprs
                        .iter()
                        .any(|&id| types_nodes::equal(*run.root.expr_node(id), it.0));
                    if member {
                        add_sp_item(&mut items, it);
                    }
                }
            }
            build_level_pathtarget(run, &items)?
        };
        prev_level_exprs =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.pathtarget(tid).exprs);
        targets.push(tid);
        contain_srfs.push(has_srfs);
    }
    Ok((targets, contain_srfs))
}

// add_sp_item_to_pathtarget (tlist.c) dedup: equal() merges unless both
// sortgrouprefs are nonzero and differ; a merge acquires a nonzero ref.
fn add_sp_item<'mcx>(items: &mut PgVec<'mcx, SpItem<'mcx>>, item: SpItem<'mcx>) {
    for existing in items.iter_mut() {
        if (item.1 == existing.1 || item.1 == 0 || existing.1 == 0)
            && types_nodes::equal(existing.0, item.0)
        {
            if item.1 != 0 {
                existing.1 = item.1;
            }
            return;
        }
    }
    items.push(item);
}

// set_pathtarget_cost_width (costsize.c), as create_pathtarget.
fn build_level_pathtarget<'mcx>(
    run: &mut PlannerRun<'mcx>,
    items: &PgVec<'mcx, SpItem<'mcx>>,
) -> PgResult<PtId> {
    let mcx = run.mcx;
    let mut t = PathTarget::new(mcx);
    let mut any_sgref = false;
    for &(node, sgref) in items.iter() {
        if node.node_tag() != NodeTag::T_Var {
            let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
            t.cost.startup += cost.startup;
            t.cost.per_tuple += cost.per_tuple;
        }
        t.exprs.push(run.intern_expr(node));
        t.sortgrouprefs.push(sgref);
        any_sgref |= sgref != 0;
    }
    if !any_sgref {
        t.sortgrouprefs.clear();
    }
    let id = run.root.alloc_pathtarget(t);
    let mut tuple_width: i64 = 0;
    for i in 0..run.root.pathtarget(id).exprs.len() {
        let expr = run.root.pathtarget(id).exprs[i];
        tuple_width += crate::costsize::get_expr_width(run, expr)? as i64;
    }
    run.root.pathtarget_mut(id).width = crate::costsize::clamp_width_est(tuple_width);
    Ok(id)
}

// adjust_paths_for_srfs (planner.c); like C, no set_cheapest rerun — the
// cheapest-startup/total pointers are swapped to the rebuilt paths in place.
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
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel_id).pathlist);
    for (i, path_id) in paths.iter().enumerate() {
        debug_assert!(run.root.path(*path_id).base().param_info.is_none());
        let mut newpath = *path_id;
        for (lvl, &target) in targets.iter().enumerate() {
            newpath = if targets_contain_srfs[lvl] {
                let p = crate::pathnode::create_set_projection_path(run, rel_id, newpath, target)?;
                run.root.alloc_path(p)
            } else {
                crate::pathnode::apply_projection_to_path(run, rel_id, newpath, target)?
            };
        }
        let rel = run.root.rel_mut(rel_id);
        rel.pathlist[i] = newpath;
        if rel.cheapest_startup_path == Some(*path_id) {
            rel.cheapest_startup_path = Some(newpath);
        }
        if rel.cheapest_total_path == Some(*path_id) {
            rel.cheapest_total_path = Some(newpath);
        }
    }
    // Likewise for partial paths (C's second loop); the SRF-free levels avoid
    // apply_projection_to_path in case of multiple refs, as C.
    let partials =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel_id).partial_pathlist);
    for (i, path_id) in partials.iter().enumerate() {
        let mut newpath = *path_id;
        for (lvl, &target) in targets.iter().enumerate() {
            newpath = if targets_contain_srfs[lvl] {
                let p = crate::pathnode::create_set_projection_path(run, rel_id, newpath, target)?;
                run.root.alloc_path(p)
            } else {
                let safe = crate::is_parallel_safe_exprs(run, target)?;
                let p =
                    crate::pathnode::create_projection_path(run, rel_id, newpath, target, safe);
                run.root.alloc_path(p)
            };
        }
        run.root.rel_mut(rel_id).partial_pathlist[i] = newpath;
    }
    Ok(())
}
