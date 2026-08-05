//! flatten_group_exprs, planner arm (var.c:951-1170): GROUP-RTE Vars in the
//! targetlist and HAVING become the underlying (already preprocessed)
//! grouping expressions, preserving varnullingrels; vars is the root == NULL
//! deparse arm's home.

use crate::run::PlannerRun;
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::Aggref;
use types_nodes::{Node, NodeList, NodeTag};

struct FgCtx<'a, 'mcx> {
    parse: &'a Query<'mcx>,
    sublevels_up: i32,
    possible_sublink: bool,
    inserted_sublink: bool,
}

pub(crate) fn flatten_group_exprs_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut ctx = FgCtx {
        parse,
        sublevels_up: 0,
        possible_sublink: parse.hasSubLinks,
        inserted_sublink: parse.hasSubLinks,
    };
    let mut out = NodeList::nil();
    for item in list.iter() {
        let new = fg_mutate(run, &mut ctx, item)?.unwrap_or(item);
        out.lappend(run.mcx, new)?;
    }
    Ok(out)
}

pub(crate) fn flatten_group_exprs_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut ctx = FgCtx {
        parse,
        sublevels_up: 0,
        possible_sublink: parse.hasSubLinks,
        inserted_sublink: parse.hasSubLinks,
    };
    Ok(fg_mutate(run, &mut ctx, node)?.unwrap_or(node))
}

// flatten_group_exprs_mutator (var.c:993-1101); None = unchanged.
fn fg_mutate<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut FgCtx<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup as i32 != ctx.sublevels_up {
                return Ok(None);
            }
            let rte = ctx
                .parse
                .rtable
                .nth(var.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            if rte.rtekind != RTEKind::RTE_GROUP {
                return Ok(None);
            }
            debug_assert!(var.varattno > 0);
            // copyObject: mark_nullable_by_grouping and the sublevel shift
            // mutate the replacement in place, so it must not alias the
            // shared groupexprs entry.
            let newnode = rewrite_manip::copy_node(
                mcx,
                rte.groupexprs.nth(var.varattno as usize - 1),
            )?;
            if ctx.sublevels_up != 0 {
                rewrite_manip::IncrementVarSublevelsUp(newnode, ctx.sublevels_up, 0)?;
            }
            if newnode.node_tag() == NodeTag::T_Var {
                let location = var.location;
                // SAFETY: newnode is the fresh copy above.
                unsafe {
                    newnode
                        .with_mut::<types_nodes::primnodes::Var, _>(|v| v.location = location)
                }
                .expect("Var");
            }
            if ctx.possible_sublink && !ctx.inserted_sublink {
                ctx.inserted_sublink = rewrite_manip::checkExprHasSubLink(newnode)?;
            }
            Ok(Some(mark_nullable_by_grouping(run, ctx.parse, newnode, var)?))
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == ctx.sublevels_up {
                // Same-level agg: only the direct arguments hold grouped Vars.
                let mut changed = false;
                let mut direct = NodeList::nil();
                for d in &agg.aggdirectargs {
                    match fg_mutate(run, ctx, d)? {
                        Some(new) => {
                            changed = true;
                            direct.lappend(mcx, new)?;
                        }
                        None => direct.lappend(mcx, d)?,
                    }
                }
                if changed {
                    // SAFETY: pre-seal planner-owned tree; no derived refs.
                    unsafe { node.with_mut::<Aggref, _>(|a| a.aggdirectargs = direct) }
                        .expect("Aggref");
                }
                return Ok(None);
            }
            if agglevelsup > ctx.sublevels_up {
                return Ok(None);
            }
            nodes_core::expression_tree_mutator(mcx, node, &mut |n| fg_mutate(run, ctx, n))
        }
        NodeTag::T_GroupingFunc => {
            if node.as_grouping_func().unwrap().agglevelsup as i32 >= ctx.sublevels_up {
                return Ok(None);
            }
            nodes_core::expression_tree_mutator(mcx, node, &mut |n| fg_mutate(run, ctx, n))
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            debug_assert!(sl.subselect.node_tag() == NodeTag::T_Query);
            fg_query_inplace(run, ctx, sl.subselect)?;
            nodes_core::expression_tree_mutator(mcx, node, &mut |n| fg_mutate(run, ctx, n))
        }
        NodeTag::T_Query => {
            fg_query_inplace(run, ctx, node)?;
            Ok(None)
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| fg_mutate(run, ctx, n)),
    }
}

// The sub-Query descent of flatten_group_exprs_mutator: query_tree_mutator
// with QTW_IGNORE_GROUPEXPRS, applied in place through the Query node handle;
// hasSubLinks tracks any grouping expression that carried a SubLink in.
fn fg_query_inplace<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut FgCtx<'_, 'mcx>,
    qnode: Node<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let q = qnode.as_query().expect("Query");
    ctx.sublevels_up += 1;
    let save_inserted = ctx.inserted_sublink;
    ctx.inserted_sublink = q.hasSubLinks;

    let new_target = fg_list_opt(run, ctx, &q.targetList)?;
    let new_returning = fg_list_opt(run, ctx, &q.returningList)?;
    let new_having = match q.havingQual {
        Some(h) => fg_mutate(run, ctx, h)?,
        None => None,
    };
    let new_limit_off = match q.limitOffset {
        Some(n) => fg_mutate(run, ctx, n)?,
        None => None,
    };
    let new_limit_cnt = match q.limitCount {
        Some(n) => fg_mutate(run, ctx, n)?,
        None => None,
    };
    let new_setops = match q.setOperations {
        Some(n) => fg_mutate(run, ctx, n)?,
        None => None,
    };
    let new_jointree = match q.jointree {
        None => None,
        Some(jt) => {
            let fl = fg_list_opt(run, ctx, &jt.fromlist)?;
            let quals = match jt.quals {
                Some(qu) => fg_mutate(run, ctx, qu)?,
                None => None,
            };
            if fl.is_some() || quals.is_some() {
                Some(mcx::alloc_leak_in(
                    mcx,
                    types_nodes::primnodes::FromExpr {
                        fromlist: match fl {
                            Some(l) => l,
                            None => jt.fromlist.clone_in(mcx)?,
                        },
                        quals: match quals {
                            Some(qu) => Some(qu),
                            None => jt.quals,
                        },
                    },
                )?)
            } else {
                None
            }
        }
    };
    for cte_node in &q.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        if let Some(cq) = cte.ctequery {
            debug_assert!(cq.node_tag() == NodeTag::T_Query);
            fg_query_inplace(run, ctx, cq)?;
        }
    }
    for rte_node in &q.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_SUBQUERY => {
                if rte.subquery.is_some() {
                    // A retained sub-Query holding an outer GROUP Var would
                    // need re-allocation (bare &Query); no such shape reaches
                    // the planner's flatten (pullup or SS_process consumed
                    // them), so only check.
                    debug_assert!(!grouped_outer_var_in_subquery(ctx, rte.subquery.unwrap())?);
                }
            }
            RTEKind::RTE_FUNCTION => {
                if let Some(l) = fg_list_opt(run, ctx, &rte.functions)? {
                    // SAFETY: pre-seal planner-owned tree; no derived refs.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.functions = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_TABLEFUNC => {
                if let Some(tf) = rte.tablefunc {
                    if let Some(new) = fg_mutate(run, ctx, tf)? {
                        // SAFETY: as above.
                        unsafe {
                            rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(
                                |r| r.tablefunc = Some(new),
                            )
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            RTEKind::RTE_VALUES => {
                if let Some(l) = fg_list_opt(run, ctx, &rte.values_lists)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.values_lists = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            // QTW_IGNORE_GROUPEXPRS: nested GROUP RTEs keep their exprs.
            _ => {}
        }
        if let Some(l) = fg_list_opt(run, ctx, &rte.securityQuals)? {
            // SAFETY: as above.
            unsafe {
                rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.securityQuals = l
                })
            }
            .expect("RangeTblEntry");
        }
    }

    let inserted = ctx.inserted_sublink;
    ctx.inserted_sublink = save_inserted;
    ctx.sublevels_up -= 1;

    if new_target.is_some()
        || new_returning.is_some()
        || new_having.is_some()
        || new_limit_off.is_some()
        || new_limit_cnt.is_some()
        || new_setops.is_some()
        || new_jointree.is_some()
        || inserted != q.hasSubLinks
    {
        // SAFETY: pre-seal planner-owned tree; no derived refs live.
        unsafe {
            qnode.with_mut::<Query, _>(|qm| {
                if let Some(t) = new_target {
                    qm.targetList = t;
                }
                if let Some(r) = new_returning {
                    qm.returningList = r;
                }
                if new_having.is_some() {
                    qm.havingQual = new_having;
                }
                if new_limit_off.is_some() {
                    qm.limitOffset = new_limit_off;
                }
                if new_limit_cnt.is_some() {
                    qm.limitCount = new_limit_cnt;
                }
                if new_setops.is_some() {
                    qm.setOperations = new_setops;
                }
                if let Some(jt) = new_jointree {
                    qm.jointree = Some(jt);
                }
                qm.hasSubLinks |= inserted;
            })
        }
        .expect("Query");
    }
    Ok(())
}

fn fg_list_opt<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut FgCtx<'_, 'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out: Vec<Node<'mcx>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        match fg_mutate(run, ctx, item)? {
            Some(new) => {
                changed = true;
                out.push(new);
            }
            None => out.push(item),
        }
    }
    if !changed {
        return Ok(None);
    }
    let mut l = NodeList::nil();
    for n in out {
        l.lappend(run.mcx, n)?;
    }
    Ok(Some(l))
}

fn grouped_outer_var_in_subquery<'mcx>(
    ctx: &FgCtx<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<bool> {
    struct W<'a, 'x> {
        parse: &'a Query<'x>,
        sublevels_up: i32,
        found: bool,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup as i32 == self.sublevels_up {
                    let rte = self
                        .parse
                        .rtable
                        .nth(v.varno as usize - 1)
                        .as_range_tbl_entry()
                        .expect("rtable entry");
                    if rte.rtekind == RTEKind::RTE_GROUP {
                        self.found = true;
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            if let Some(q) = node.as_query() {
                self.sublevels_up += 1;
                let r = nodes_core::query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                return r;
            }
            nodes_core::expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = nodes_core::query_tree_walker(q, self, 0);
            self.sublevels_up -= 1;
            r
        }
    }
    let mut w = W { parse: ctx.parse, sublevels_up: ctx.sublevels_up + 1, found: false };
    nodes_core::query_tree_walker(q, &mut w, 0)?;
    Ok(w.found)
}

// mark_nullable_by_grouping (var.c:1106-1170).
fn mark_nullable_by_grouping<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    newnode: Node<'mcx>,
    oldvar: &types_nodes::primnodes::Var<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    if oldvar.varnullingrels.is_empty() {
        return Ok(newnode);
    }
    debug_assert!(
        oldvar.varnullingrels.is_member(run.root.group_rtindex)
            && oldvar.varnullingrels.iter().count() == 1
    );
    let relids = vars::pull_varnos_of_level(mcx, newnode, oldvar.varlevelsup as i32)?;
    if !relids.is_empty() {
        // Marking the contained Vars/PHVs (not the whole expression) is
        // enough to distinguish the nullable form in ECs.
        crate::prepjointree::add_nulling_relids_expr(
            mcx,
            newnode,
            Some(&relids),
            &oldvar.varnullingrels,
        )?;
        return Ok(newnode);
    }
    if !clauses::contain_volatile_functions(newnode)? && !coerce::expression_returns_set(newnode)
    {
        let mut phrels = types_nodes::Bitmapset::empty();
        if let Some(jt) = parse.jointree {
            for child in &jt.fromlist {
                crate::prepjointree::get_relids_in_jointree_no_inner(mcx, child, &mut phrels)?;
            }
        }
        debug_assert!(!phrels.is_empty());
        let phv_node = crate::placeholder::make_placeholder_expr(run, newnode, phrels)?;
        let phlevelsup = oldvar.varlevelsup;
        let phnullingrels = oldvar.varnullingrels.clone_in(mcx)?;
        // SAFETY: phv_node is freshly built above.
        unsafe {
            phv_node.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|p| {
                p.phlevelsup = phlevelsup;
                p.phnullingrels = phnullingrels;
            })
        }
        .expect("PlaceHolderVar");
        return Ok(phv_node);
    }
    Ok(newnode)
}

// remove_nulling_relids over {group_rtindex} in copy-on-write form (C's
// rewriteManip copying mutator): shared subtrees stay shared, paths to
// stripped Vars/PHVs are rebuilt. None = no bit present.
pub(crate) fn strip_group_nulling<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    group_rtindex: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            if v.varlevelsup == 0 && v.varnullingrels.is_member(group_rtindex) {
                let mut nr = v.varnullingrels.clone_in(mcx)?;
                nr.del_member(group_rtindex);
                return Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::Var { varnullingrels: nr, ..*v },
                )?));
            }
            Ok(None)
        }
        NodeTag::T_PlaceHolderVar => {
            let p = node.as_place_holder_var().unwrap();
            let strip_own = p.phlevelsup == 0 && p.phnullingrels.is_member(group_rtindex);
            let new_expr = strip_group_nulling(mcx, p.phexpr, group_rtindex)?;
            if !strip_own && new_expr.is_none() {
                return Ok(None);
            }
            let mut nr = p.phnullingrels.clone_in(mcx)?;
            if strip_own {
                nr.del_member(group_rtindex);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::PlaceHolderVar {
                    phexpr: new_expr.unwrap_or(p.phexpr),
                    phrels: p.phrels.clone_in(mcx)?,
                    phnullingrels: nr,
                    phid: p.phid,
                    phlevelsup: p.phlevelsup,
                },
            )?))
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
            strip_group_nulling(mcx, n, group_rtindex)
        }),
    }
}

pub(crate) fn strip_group_nulling_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    group_rtindex: i32,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out: Vec<Node<'mcx>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        match strip_group_nulling(mcx, item, group_rtindex)? {
            Some(new) => {
                changed = true;
                out.push(new);
            }
            None => out.push(item),
        }
    }
    if !changed {
        return Ok(None);
    }
    let mut l = NodeList::nil();
    for n in out {
        l.lappend(mcx, n)?;
    }
    Ok(Some(l))
}
