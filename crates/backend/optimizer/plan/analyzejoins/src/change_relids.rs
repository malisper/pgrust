//! The planner relid-replacement walker — the analyzejoins.c keystone that makes
//! the planner `RestrictInfo` / `EquivalenceMember` (arena handles in this repo,
//! `RinfoId` / `EmId`) walkable for a change of RT index, mirroring C's
//! `ChangeVarNodesExtended((Node *) rinfo, …, replace_relid_callback)`.
//!
//! # Why this exists (the #295 keystone)
//!
//! In C, self-join elimination casts `RestrictInfo *` / `EquivalenceMember *` to
//! `Node *` and feeds it to `ChangeVarNodesExtended` with `replace_relid_callback`
//! (analyzejoins.c:1701); the callback's `IsA(node, RestrictInfo)` branch adjusts
//! the RestrictInfo's relid sets in place, recurses into `rinfo->clause` /
//! `rinfo->orclause` via `ChangeVarNodesWalkExpression`, and applies the
//! self-join `t1.a = t1.a → t1.a IS NOT NULL` rewrite.
//!
//! In this repo the planner `RestrictInfo` (a `RinfoId` arena struct) is NOT a
//! `Node` variant, and its `clause` is a `NodeId` arena handle. A `&mut Node`
//! callback cannot resolve that handle (it has no `&mut PlannerInfo`). So rather
//! than fight the generic `&mut Node` walker, this module reproduces the
//! callback's RestrictInfo branch as a direct, index-based recursion over the
//! arena, holding `&mut PlannerInfo` throughout (re-borrowing per step), and
//! drops into the standard expression walker
//! ([`rewrite_core::change::ChangeVarNodes`]) only for the leaf clause
//! `Expr`, which it detaches from the arena, walks standalone (plain
//! Var/PHV/RangeTblRef adjustment needs no `PlannerInfo`), and reattaches.
//!
//! The embedded-`RestrictInfo`-in-`orclause` case (`Expr::RestrictInfo(RinfoRef)`
//! children of a BoolExpr `orclause`, produced by `make_sub_restrictinfos`) is
//! handled by recursing through those handles in the arena, exactly as C's
//! callback recurses through `ChangeVarNodesWalkExpression((Node *) rinfo->orclause)`.

use alloc::vec::Vec;

use ::nodes::nodes::Node;
use ::nodes::primnodes::Expr;
use pathnodes::{EmId, NodeId, PlannerInfo, RinfoId};

use rewrite_core::change::ChangeVarNodes;

use crate::relids;

/// Mirrors C's `ChangeVarNodes_context` (analyzejoins reaches it with
/// `sublevels_up == 0` always, since planner clauses are at the current level).
#[derive(Clone, Copy)]
pub struct ReplaceRelidContext {
    /// `rt_index` — the relid being replaced.
    pub rt_index: i32,
    /// `new_index` — its replacement (negative ⇒ "delete only", as in the
    /// left-join removal `subst == -1` case).
    pub new_index: i32,
}

/// Walk + adjust the leaf clause [`Expr`] in place for the relid change. Mirrors
/// `ChangeVarNodesWalkExpression((Node *) rinfo->clause, context)`: it adjusts
/// `Var.varno` / `varnullingrels` / PHV rels / RangeTblRef etc. The arena `Expr`
/// is lifetime-free, so it is wrapped as a `Node::Expr` for the standalone walk.
/// `ChangeVarNodesExtended((Node *) phv->phexpr, relid, subst, …)` applied to a
/// `PlaceHolderInfo`'s *inline* `ph_var.phexpr` (the source-of-truth carried in
/// `PlaceHolderInfo.ph_var`). `change_relids_in_node` only re-points the separate
/// `ph_var_phexpr` NodeId mirror; the inline `ph_var.phexpr` is what
/// `rebuild_placeholder_attr_needed` (and other placeholder.c rebuilds) read via
/// `clone_phinfo_phexpr`, so it must be re-pointed too, or a removed self-join
/// relid survives there and `add_vars_to_attr_needed` -> `find_base_rel` panics
/// with `no relation entry`.
pub(crate) fn change_relids_in_phinfo_inline_phexpr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    phid: pathnodes::PhInfoId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    // Take the inline phexpr out (leaving None) — `Expr` deep-clones only via
    // `clone_in`, so we move the Box to avoid a derived `Expr::clone`.
    let taken = match root.phinfo_mut(phid).ph_var.phexpr.take() {
        Some(b) => b,
        None => return Ok(()),
    };
    let mut node = Node::mk_expr(mcx, (*taken).clone_in(mcx)?)?;
    ChangeVarNodes(&mut node, ctx.rt_index, ctx.new_index, 0, mcx);
    let walked = node
        .into_expr()
        .unwrap_or_else(|| unreachable!("ChangeVarNodes returned a non-Expr for an Expr input"));
    root.phinfo_mut(phid).ph_var.phexpr =
        Some(alloc::boxed::Box::new(walked.erase_lifetime()));
    Ok(())
}

pub(crate) fn change_relids_in_node<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    id: NodeId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    // Move the arena `Expr` out (leaving a cheap leaf placeholder) and wrap it as
    // a `Node` so the standalone walker owns a `&mut Node`, then store the walked
    // result back (mirroring the C in-place mutation through the
    // `(Node *) rinfo->clause` pointer). Taking the value by `mem::replace`
    // avoids a derived `Expr::clone`, which would panic on a context-allocated
    // child (Aggref/SubLink/SubPlan). The opaque `Node` is allocated in `mcx`.
    let taken: Expr<'static> =
        core::mem::replace(root.node_mut(id), Expr::Var(Default::default()));
    // Deep-clone the arena ('static) node into the walk arena `mcx`, mutate, then
    // re-intern into the planner node arena via `erase_lifetime` (the sanctioned
    // arena-intern boundary). A move would tie the `'static` arena slot to `'mcx`.
    let mut node = Node::mk_expr(mcx, taken.clone_in(mcx)?)?;
    ChangeVarNodes(&mut node, ctx.rt_index, ctx.new_index, 0, mcx);
    // ChangeVarNodes never changes the top-level node kind for an Expr input.
    let walked = node
        .into_expr()
        .unwrap_or_else(|| unreachable!("ChangeVarNodes returned a non-Expr for an Expr input"));
    *root.node_mut(id) = walked.erase_lifetime();
    Ok(())
}

/// `replace_relid_callback`'s RestrictInfo branch (analyzejoins.c:1708) for a
/// planner [`RinfoId`].
pub fn change_relids_in_rinfo<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    id: RinfoId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    let rt_index = ctx.rt_index;
    let new_index = ctx.new_index;

    // Snapshot the predicate inputs before any mutation.
    let (is_req_equal, clause_relids_is_multiple, in_clause, in_required) = {
        let rinfo = root.rinfo(id);
        let is_req_equal = relids::equal(&rinfo.required_relids, &rinfo.clause_relids);
        let clause_relids_is_multiple = relids::membership_is_multiple(&rinfo.clause_relids);
        let in_clause = relids::is_member(rt_index, &rinfo.clause_relids);
        let in_required = relids::is_member(rt_index, &rinfo.required_relids);
        (is_req_equal, clause_relids_is_multiple, in_clause, in_required)
    };

    // Recurse down into clauses if the target relation is present in
    // clause_relids or required_relids.
    if in_clause || in_required {
        // ChangeVarNodesWalkExpression((Node *) rinfo->clause, context);
        let clause_id = root.rinfo(id).clause;
        change_relids_in_node(mcx, root, clause_id, ctx)?;

        // ChangeVarNodesWalkExpression((Node *) rinfo->orclause, context);
        if let Some(orclause_id) = root.rinfo(id).orclause {
            change_relids_in_orclause(mcx, root, orclause_id, ctx)?;
        }

        let new_clause_relids = relids::adjust_relid_set(
            &root.rinfo(id).clause_relids,
            rt_index,
            new_index,
        );

        // Incrementally adjust num_base_rels based on the change of clause_relids.
        let old_n = relids::num_members(&root.rinfo(id).clause_relids);
        let new_n = relids::num_members(&new_clause_relids);
        {
            let rinfo = root.rinfo_mut(id);
            rinfo.num_base_rels -= old_n - new_n;
            rinfo.clause_relids = new_clause_relids;
            rinfo.left_relids =
                relids::adjust_relid_set(&rinfo.left_relids, rt_index, new_index);
            rinfo.right_relids =
                relids::adjust_relid_set(&rinfo.right_relids, rt_index, new_index);
        }
    }

    {
        let rinfo = root.rinfo_mut(id);
        if is_req_equal {
            rinfo.required_relids = relids::copy(&rinfo.clause_relids);
        } else {
            rinfo.required_relids =
                relids::adjust_relid_set(&rinfo.required_relids, rt_index, new_index);
        }
        rinfo.outer_relids =
            relids::adjust_relid_set(&rinfo.outer_relids, rt_index, new_index);
        rinfo.incompatible_relids =
            relids::adjust_relid_set(&rinfo.incompatible_relids, rt_index, new_index);
    }

    // Self-join `t1.a = t1.a → t1.a IS NOT NULL` rewrite, only when the clause
    // collapsed onto a single (the new) relid via a mergejoinable OpExpr.
    let do_selfjoin_check = {
        let rinfo = root.rinfo(id);
        !rinfo.mergeopfamilies.is_empty()
            && relids::get_singleton_member(&rinfo.clause_relids) == Some(new_index)
            && clause_relids_is_multiple
            && matches!(root.node(rinfo.clause), Expr::OpExpr(_))
    };
    if do_selfjoin_check {
        try_selfjoin_nulltest_rewrite(root, id);
    }
    Ok(())
}

/// Recurse through a BoolExpr `orclause`'s embedded `RestrictInfo` children,
/// mirroring `ChangeVarNodesWalkExpression((Node *) rinfo->orclause, context)`
/// reaching `replace_relid_callback` for each nested `IsA(node, RestrictInfo)`.
/// Non-RestrictInfo children are plain expressions adjusted via the standalone
/// walker (the orclause is `make_sub_restrictinfos` output: AND/OR of
/// `Expr::RestrictInfo` handles).
fn change_relids_in_orclause<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    id: NodeId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    // Collect nested RestrictInfo handles (depth-first), so we can recurse with
    // `&mut PlannerInfo` without aliasing the arena entry being read.
    let mut nested: Vec<RinfoId> = Vec::new();
    collect_nested_rinfos(root, id, &mut nested);
    if nested.is_empty() {
        // No embedded RestrictInfos: adjust the expression in place directly.
        change_relids_in_node(mcx, root, id, ctx)?;
        return Ok(());
    }
    for rid in nested {
        change_relids_in_rinfo(mcx, root, rid, ctx)?;
    }
    Ok(())
}

/// Depth-first collect every `Expr::RestrictInfo(RinfoRef)` handle reachable
/// from arena node `id` through inline BoolExpr args.
pub(crate) fn collect_nested_rinfos(root: &PlannerInfo, id: NodeId, out: &mut Vec<RinfoId>) {
    collect_in_expr(root.node(id), out);
}

fn collect_in_expr(e: &Expr, out: &mut Vec<RinfoId>) {
    match e {
        Expr::RestrictInfo(r) => out.push((*r).into()),
        Expr::BoolExpr(b) => {
            for arg in &b.args {
                collect_in_expr(arg, out);
            }
        }
        _ => {}
    }
}

/// `replace_relid_callback`'s OpExpr NullTest rewrite (analyzejoins.c:1764).
/// When `ChangeVarNodes` turned `t1.a = t2.a` into `t1.a = t1.a` (both operands
/// now equal per `equal()`), replace the clause with `t1.a IS NOT NULL`.
fn try_selfjoin_nulltest_rewrite(root: &mut PlannerInfo, id: RinfoId) {
    let clause_id = root.rinfo(id).clause;
    let (leftop, rightop) = match root.node(clause_id) {
        Expr::OpExpr(op) => {
            let l = op.args.first().cloned();
            let r = op.args.get(1).cloned();
            (l, r)
        }
        _ => return,
    };
    let (Some(leftop), Some(rightop)) = (leftop, rightop) else {
        return;
    };

    // equal(leftOp, rightOp) — the nodeFuncs equality predicate.
    if equivclass_ext_seams::equal::call(&leftop, &rightop) {
        // Build `leftOp IS NOT NULL`.
        let ntest = Expr::NullTest(::nodes::primnodes::NullTest {
            arg: Some(alloc::boxed::Box::new(leftop)),
            nulltesttype: ::nodes::primnodes::NullTestType::IS_NOT_NULL,
            argisrow: false,
            location: -1,
        });
        *root.node_mut(clause_id) = ntest;
        let rinfo = root.rinfo_mut(id);
        rinfo.mergeopfamilies.clear();
        rinfo.left_em = None;
        rinfo.right_em = None;
        // Assert(rinfo->orclause == NULL) — holds for a mergejoinable OpExpr.
    }
}

/// `ChangeVarNodesExtended((Node *) em->em_expr, from, to, 0,
/// replace_relid_callback)` for an [`EmId`]: the EM expr contains no embedded
/// RestrictInfos, so it is a plain expression adjustment.
pub fn change_relids_in_em<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    id: EmId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    let expr_id = root.em(id).em_expr;
    change_relids_in_node(mcx, root, expr_id, ctx)?;
    Ok(())
}

/// `ChangeVarNodesExtended((Node *) root->parse, from, to, 0,
/// replace_relid_callback)` (analyzejoins.c:1961, self-join elimination) — walk
/// the whole `Query` parse tree for the relid substitution.
///
/// Unlike the planner `RestrictInfo`/`EquivalenceMember` arena handles, the
/// `root->parse` `Query` is a real owned value (resolved off the [`QueryId`]
/// through the [`PlannerRun`] store) and a `Node` variant, so the standard
/// `Query`-aware walker in `rewrite_core` applies directly. The
/// `replace_relid_callback`'s only relevant branch for a *parse* tree is its
/// RangeTblRef skip (the parse tree carries no planner RestrictInfo nodes); SJE
/// must skip RangeTblRefs so the later `remove_rel_from_joinlist` can still find
/// them by their original relid.
pub fn change_relids_in_query(
    run: &mut pathnodes::planner_run::PlannerRun<'_>,
    parse: pathnodes::QueryId,
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    use rewrite_core::change::{
        ChangeVarNodesContext, ChangeVarNodesExtended,
    };
    use ::nodes::nodes::ntag;

    // Move the Query out into a Node::Query, walk it standalone (the walker
    // mutates in place, mirroring C's cast-to-Node + in-place mutation), and
    // write it back. The SJE callback skips RangeTblRef nodes. The placeholder
    // swapped in is immediately overwritten and never observed.
    let mcx = run.mcx();
    let query = core::mem::replace(
        run.resolve_mut(parse),
        ::nodes::copy_query::Query::new(mcx),
    );
    let mut node = Node::mk_query(mcx, query)?;

    let mut skip_rtr = |n: &mut Node<'_>, _c: &mut ChangeVarNodesContext| -> bool {
        matches!(n.node_tag(), t if t == ntag::T_RangeTblRef)
    };
    ChangeVarNodesExtended(
        &mut node,
        ctx.rt_index,
        ctx.new_index,
        0,
        Some(&mut skip_rtr),
        mcx,
    );

    let walked = node
        .into_query()
        .unwrap_or_else(|| unreachable!("ChangeVarNodes returned a non-Query for a Query input"));
    *run.resolve_mut(parse) = walked;
    Ok(())
}

/// Re-apply the self-join relid substitution to the planner's per-RT-index RTE
/// copies (`root->simple_rte_array`).
///
/// In C, `setup_simple_rel_arrays` records `simple_rte_array[rti] =
/// rt_fetch(rti, parse->rtable)`, i.e. the array entries are the *same*
/// `RangeTblEntry *` pointers as live in `parse->rtable`. So when
/// `remove_self_join_rel` runs `ChangeVarNodes((Node *) root->parse, ...)`, the
/// rewrite of a LATERAL subquery/function/values/tablefunc RTE inside the range
/// table is automatically visible through `simple_rte_array` too.
///
/// In this repo `simple_rte_array` carries `RangeTblEntryId` handles into a
/// *separate* run store — `setup_simple_rel_arrays` clones each `parse.rtable`
/// entry and interns a distinct copy. The `change_relids_in_query` walk above
/// therefore rewrites only the `parse.rtable` copy; the `simple_rte_array` copy
/// stays stale. `set_subquery_pathlist` later reads the LATERAL subquery out of
/// `simple_rte_array` (not `parse.rtable`) and derives its nestloop params from
/// it, so a stale varno there produces a Var referencing the *removed* relation
/// (→ `non-LATERAL parameter required by subquery`).
///
/// This restores C's aliasing semantics by running the identical relid
/// substitution over each interned RTE. We reuse `range_table_mutator`, which
/// already knows how to descend into each RTE-kind's lateral-bearing subfields
/// (subquery, functions, values_lists, tablefunc, RTE_RELATION tablesample),
/// wrapping each interned RTE in a one-element slice. `sublevels_up` starts at 0,
/// exactly as for the `parse` walk.
pub fn change_relids_in_simple_rte_array<'mcx>(
    run: &mut pathnodes::planner_run::PlannerRun<'mcx>,
    simple_rte_array: &[pathnodes::RangeTblEntryId],
    ctx: ReplaceRelidContext,
) {
    use nodes_core::node_walker::range_table_mutator;
    use rewrite_core::change::{ChangeVarNodes_walker, ChangeVarNodesContext};

    let mcx = run.mcx();
    // simple_rte_array[0] is the unused RT-index-0 slot (a sentinel handle); the
    // real per-RTE handles begin at index 1, mirroring rt_fetch's 1-based RT
    // indices. Skip the 0-slot by position.
    for &rte_id in simple_rte_array.iter().skip(1) {
        // Move the interned RTE out, walk it as a one-element range table (which
        // dispatches per rtekind to the lateral-bearing subfields), write it back.
        let mut rte = core::mem::replace(
            run.resolve_rte_mut(rte_id),
            ::nodes::parsenodes::RangeTblEntry::new_in(mcx),
        );
        let mut context = ChangeVarNodesContext {
            rt_index: ctx.rt_index,
            new_index: ctx.new_index,
            sublevels_up: 0,
            mcx,
        };
        let mut cb: Option<rewrite_core::change::ChangeVarNodesCallback> = None;
        range_table_mutator(
            core::slice::from_mut(&mut rte),
            &mut |n| ChangeVarNodes_walker(n, &mut context, &mut cb),
            0,
            mcx,
        );
        *run.resolve_rte_mut(rte_id) = rte;
    }
}

/// `ChangeVarNodesExtended((Node *) node, from, to, 0, replace_relid_callback)`
/// over a list of arena `Node` handles (`root->processed_tlist`,
/// `root->processed_groupClause`, `reltarget->exprs`). Dispatches on the arena
/// node kind: bare `Expr` handles (reltarget) are walked directly; `TargetEntry`
/// handles (processed_tlist) recurse into the entry's `expr` child handle (C's
/// expression walker descends through the TargetEntry); `SortGroupClause` handles
/// (processed_groupClause) carry no Vars, so they are no-ops.
pub fn change_relids_in_node_list<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    ids: &[pathnodes::NodeId],
    ctx: ReplaceRelidContext,
) -> types_error::PgResult<()> {
    use pathnodes::ArenaNode;
    for &id in ids {
        match &root.node_arena[id.index()] {
            ArenaNode::Expr(_) => change_relids_in_node(mcx, root, id, ctx)?,
            ArenaNode::TargetEntry(te) => {
                let expr_id = te.expr;
                change_relids_in_node(mcx, root, expr_id, ctx)?;
            }
            // SortGroupClause / other non-expression arena nodes hold no Vars.
            _ => {}
        }
    }
    Ok(())
}
