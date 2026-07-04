//! prepjointree.c, simple-subquery slice: pull_up_subqueries over the full
//! jointree (FromExpr/JoinExpr) plus reduce_outer_joins with the LEFT->ANTI
//! reduction. Non-pullable subqueries stay as RTE_SUBQUERY for
//! set_subquery_pathlist (allpaths.rs); LATERAL is the remaining loud arm.

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, TargetEntry, Var};
use types_nodes::{Node, NodeTag};

// C recurses and mutates in place; here each pull-up rebuilds the jointree
// functionally (replace the RangeTblRef, substitute Vars in every qual), so
// the loop re-scans until no pullable subquery reference remains.
pub fn pull_up_subqueries<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let mut kept: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
    loop {
        let jt = parse.jointree.expect("jointree is a FromExpr");
        let mut target: Option<(i32, Option<Node<'mcx>>)> = None;
        for child in &jt.fromlist {
            find_pullable_subquery(parse, child, None, &mut target, &kept);
            if target.is_some() {
                break;
            }
        }
        let Some((rti, lowest_outer_join)) = target else { return Ok(()) };
        let rte_node = parse.rtable.nth(rti as usize - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if !is_simple_subquery(mcx, rte, lowest_outer_join)? {
            kept.push(rti);
            continue;
        }
        pull_up_simple_subquery(mcx, parse, rti, rte_node)?;
    }
}

fn find_pullable_subquery<'mcx>(
    parse: &Query<'mcx>,
    node: Node<'mcx>,
    lowest_outer_join: Option<Node<'mcx>>,
    target: &mut Option<(i32, Option<Node<'mcx>>)>,
    kept: &[i32],
) {
    if target.is_some() {
        return;
    }
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rti = node.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            let rte = parse.rtable.nth(rti as usize - 1).as_range_tbl_entry().expect("rtable cell");
            if rte.rtekind == RTEKind::RTE_SUBQUERY && !kept.contains(&rti) {
                *target = Some((rti, lowest_outer_join));
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            for child in &f.fromlist {
                find_pullable_subquery(parse, child, lowest_outer_join, target, kept);
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let loj = if j.jointype == types_nodes::JoinType::JOIN_INNER {
                lowest_outer_join
            } else {
                Some(node)
            };
            find_pullable_subquery(parse, j.larg, loj, target, kept);
            find_pullable_subquery(parse, j.rarg, loj, target, kept);
        }
        other => panic!(
            "pull_up_subqueries_recurse (prepjointree.c): {other:?} jointree arm; \
             M2 join lane"
        ),
    }
}

// get_relids_in_jointree (prepjointree.c), include_outer_joins=true,
// include_inner_joins=true.
fn get_relids_in_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    out: &mut types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            out.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            for child in &node.as_from_expr().unwrap().fromlist {
                get_relids_in_jointree(mcx, child, out)?;
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            get_relids_in_jointree(mcx, j.larg, out)?;
            get_relids_in_jointree(mcx, j.rarg, out)?;
            if j.rtindex != 0 {
                out.add_member(mcx, j.rtindex)?;
            }
        }
        other => panic!("get_relids_in_jointree (prepjointree.c): {other:?}"),
    }
    Ok(())
}

// jointree_contains_lateral_outer_refs (prepjointree.c).
fn jointree_contains_lateral_outer_refs<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    restricted: bool,
    safe_upper_varnos: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<bool> {
    let quals_unsafe = |quals: Option<Node<'mcx>>| -> PgResult<bool> {
        let Some(q) = quals else { return Ok(false) };
        Ok(!vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(safe_upper_varnos))
    };
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(false),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            for child in &f.fromlist {
                if jointree_contains_lateral_outer_refs(mcx, child, restricted, safe_upper_varnos)?
                {
                    return Ok(true);
                }
            }
            Ok(restricted && quals_unsafe(f.quals)?)
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let empty = types_nodes::Bitmapset::empty();
            let (restricted, safe) = if j.jointype != types_nodes::JoinType::JOIN_INNER {
                (true, &empty)
            } else {
                (restricted, safe_upper_varnos)
            };
            if jointree_contains_lateral_outer_refs(mcx, j.larg, restricted, safe)? {
                return Ok(true);
            }
            if jointree_contains_lateral_outer_refs(mcx, j.rarg, restricted, safe)? {
                return Ok(true);
            }
            let quals_unsafe = |quals: Option<Node<'mcx>>| -> PgResult<bool> {
                let Some(q) = quals else { return Ok(false) };
                Ok(!vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(safe))
            };
            Ok(restricted && quals_unsafe(j.quals)?)
        }
        other => panic!("jointree_contains_lateral_outer_refs (prepjointree.c): {other:?}"),
    }
}

// The CombineRangeTables perminfoindex fixup target: a struct-level copy of
// the RTE (C's copyObject; sub-nodes stay shared) so the sublink's stored
// sub-Query — shared with the plancache — is never scribbled on. A replan of
// a cached query would otherwise re-offset the same RTE.
pub(crate) fn rte_copy_with_perminfoindex<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    perminfoindex: u32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        RangeTblEntry {
            alias: rte.alias,
            eref: rte.eref,
            rtekind: rte.rtekind,
            relid: rte.relid,
            inh: rte.inh,
            relkind: rte.relkind,
            rellockmode: rte.rellockmode,
            perminfoindex,
            tablesample: rte.tablesample,
            subquery: rte.subquery,
            security_barrier: rte.security_barrier,
            jointype: rte.jointype,
            joinmergedcols: rte.joinmergedcols,
            joinaliasvars: rte.joinaliasvars.clone_in(mcx)?,
            joinleftcols: rte.joinleftcols.clone_in(mcx)?,
            joinrightcols: rte.joinrightcols.clone_in(mcx)?,
            join_using_alias: rte.join_using_alias,
            functions: rte.functions.clone_in(mcx)?,
            funcordinality: rte.funcordinality,
            tablefunc: rte.tablefunc,
            values_lists: rte.values_lists.clone_in(mcx)?,
            ctename: rte.ctename,
            ctelevelsup: rte.ctelevelsup,
            self_reference: rte.self_reference,
            coltypes: rte.coltypes.clone_in(mcx)?,
            coltypmods: rte.coltypmods.clone_in(mcx)?,
            colcollations: rte.colcollations.clone_in(mcx)?,
            enrname: rte.enrname,
            enrtuples: rte.enrtuples,
            groupexprs: rte.groupexprs.clone_in(mcx)?,
            lateral: rte.lateral,
            inFromCl: rte.inFromCl,
            securityQuals: rte.securityQuals.clone_in(mcx)?,
        },
    )
}

// is_simple_subquery (prepjointree.c): false keeps the RTE for the
// SubqueryScan path (set_subquery_pathlist).
fn is_simple_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    lowest_outer_join: Option<Node<'mcx>>,
) -> PgResult<bool> {
    let sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");
    let blocked = if sub.setOperations.is_some() {
        Some("setOperations")
    } else if sub.hasAggs {
        Some("hasAggs")
    } else if sub.hasWindowFuncs {
        Some("hasWindowFuncs")
    } else if sub.hasTargetSRFs {
        Some("hasTargetSRFs")
    } else if !sub.groupClause.is_nil() || !sub.groupingSets.is_nil() {
        Some("GROUP BY")
    } else if sub.havingQual.is_some() {
        Some("HAVING")
    } else if !sub.sortClause.is_nil() {
        Some("ORDER BY")
    } else if !sub.distinctClause.is_nil() {
        Some("DISTINCT")
    } else if sub.limitOffset.is_some() || sub.limitCount.is_some() {
        Some("LIMIT/OFFSET")
    } else if sub.hasForUpdate {
        Some("FOR UPDATE")
    } else if !sub.cteList.is_nil() {
        Some("WITH")
    } else if rte.security_barrier {
        Some("security_barrier")
    } else {
        None
    };
    if blocked.is_some() {
        return Ok(false);
    }
    if rte.lateral {
        let mut safe_upper_varnos = types_nodes::Bitmapset::empty();
        let restricted = match lowest_outer_join {
            Some(loj) => {
                get_relids_in_jointree(mcx, loj, &mut safe_upper_varnos)?;
                true
            }
            None => false,
        };
        let jt = sub.jointree.expect("jointree is a FromExpr");
        let mut contains = false;
        for child in &jt.fromlist {
            if jointree_contains_lateral_outer_refs(mcx, child, restricted, &safe_upper_varnos)? {
                contains = true;
                break;
            }
        }
        if !contains && restricted {
            if let Some(q) = jt.quals {
                contains = !vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(&safe_upper_varnos);
            }
        }
        if contains {
            return Ok(false);
        }
        if lowest_outer_join.is_some() {
            let mut lvarnos = types_nodes::Bitmapset::empty();
            for te in &sub.targetList {
                lvarnos.add_members(mcx, &vars::pull_varnos_of_level(mcx, te, 1)?)?;
            }
            if !lvarnos.is_subset(&safe_upper_varnos) {
                return Ok(false);
            }
        }
    }
    for te in &sub.targetList {
        if clauses::contain_volatile_functions(te)? {
            return Ok(false);
        }
    }
    Ok(true)
}

// pull_up_simple_subquery (prepjointree.c). C copyObject's rte->subquery and
// mutates the copy; here the offset pass rebuilds the pieces functionally, so
// the shared tree is never written. PlaceHolderVar wrapping is structurally
// unreachable (outer joins and grouping sets panic upstream).
fn pull_up_simple_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &mut Query<'mcx>,
    varno: i32,
    rte_node: Node<'mcx>,
) -> PgResult<()> {
    let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
    let shared_sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");

    if shared_sub.hasSubLinks {
        panic!("pull_up_sublinks (prepjointree.c): sublinks in pulled-up subquery; M2 lane");
    }
    assert!(
        !shared_sub.hasRowSecurity,
        "pull_up_simple_subquery (prepjointree.c): hasRowSecurity propagation unported"
    );
    assert!(
        shared_sub.rowMarks.is_nil(),
        "pull_up_simple_subquery (prepjointree.c): rowMarks concat unported"
    );
    // C recursively completes pull_up_subqueries for the child before
    // splicing it in; runs on a cells-copy (C copyObject), the shared tree
    // is never written.
    let sub: &Query<'mcx> = if shared_sub
        .rtable
        .iter()
        .any(|n| n.as_range_tbl_entry().expect("rtable cell").rtekind == RTEKind::RTE_SUBQUERY)
    {
        let mut sub_local = crate::subselect::query_cells_copy(mcx, shared_sub)?;
        // Fresh RTE nodes: the recursive pass ends with a with_mut fixup
        // (subquery = None) that must never write a shared node.
        let mut fresh_rtable = NodeList::nil();
        for srte_node in &sub_local.rtable {
            let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
            fresh_rtable
                .lappend(mcx, rte_copy_with_perminfoindex(mcx, srte, srte.perminfoindex)?)?;
        }
        sub_local.rtable = fresh_rtable;
        pull_up_subqueries(mcx, &mut sub_local)?;
        mcx::alloc_leak_in(mcx, sub_local)?
    } else {
        shared_sub
    };
    let sub_jt = sub.jointree.expect("jointree is a FromExpr");

    let rtoffset = parse.rtable.len() as i32;
    // replace_empty_jointree (prepjointree.c): an empty-FROM subquery gets a
    // dummy RTE_RESULT to supply its one row; it lands after the subquery's
    // own rtable entries in the combined range table.
    let result_rtr = if sub_jt.fromlist.is_nil() {
        Some(Node::mk_range_tbl_ref(mcx, rtoffset + sub.rtable.len() as i32 + 1)?)
    } else {
        None
    };

    let off_tlist = match clauses::walker::mutate_list(mcx, &sub.targetList, &mut |n| {
        offset_expr(mcx, n, rtoffset)
    })? {
        Some(l) => l,
        None => sub.targetList.clone_in(mcx)?,
    };
    let mut off_fromlist = NodeList::nil();
    if let Some(rtr) = result_rtr {
        off_fromlist.lappend(mcx, rtr)?;
    }
    for jnode in &sub_jt.fromlist {
        off_fromlist.lappend(mcx, offset_jointree(mcx, jnode, rtoffset)?)?;
    }
    let off_quals = offset_opt(mcx, sub_jt.quals, rtoffset)?;

    if let Some(l) = clauses::walker::mutate_list(mcx, &parse.targetList, &mut |n| {
        replace_var_expr(mcx, n, varno, &off_tlist)
    })? {
        parse.targetList = l;
    }
    parse.havingQual = replace_opt(mcx, parse.havingQual, varno, &off_tlist)?;
    if let Some(l) = clauses::walker::mutate_list(mcx, &parse.returningList, &mut |n| {
        replace_var_expr(mcx, n, varno, &off_tlist)
    })? {
        parse.returningList = l;
    }
    // perform_pullup_replace_vars: MERGE action targetlists/quals and the
    // join condition reference the source rel too.
    for action_node in &parse.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        let new_qual = replace_opt(mcx, action.qual, varno, &off_tlist)?;
        let new_tlist = match clauses::walker::mutate_list(mcx, &action.targetList, &mut |n| {
            replace_var_expr(mcx, n, varno, &off_tlist)
        })? {
            Some(l) => l,
            None => action.targetList.clone_in(mcx)?,
        };
        // SAFETY: pre-seal tree owned by this planner invocation.
        unsafe {
            action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                a.qual = new_qual;
                a.targetList = new_tlist;
            })
        }
        .expect("MergeAction");
    }
    parse.mergeJoinCondition =
        replace_opt(mcx, parse.mergeJoinCondition, varno, &off_tlist)?;

    // pullup_replace_vars over the jointree: substitute Vars in every qual
    // and splice the offset sub-jointree in place of the RangeTblRef.
    let replacement = if off_quals.is_none() && off_fromlist.len() == 1 {
        off_fromlist.nth(0)
    } else {
        Node::mk(mcx, FromExpr { fromlist: off_fromlist, quals: off_quals })?
    };
    let jt = parse.jointree.expect("jointree is a FromExpr");
    let mut new_fromlist = NodeList::nil();
    for child in &jt.fromlist {
        new_fromlist.lappend(
            mcx,
            splice_and_replace(mcx, &parse.rtable, child, varno, &off_tlist, replacement)?,
        )?;
    }
    let new_quals = replace_opt(mcx, jt.quals, varno, &off_tlist)?;
    parse.jointree = Some(mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist: new_fromlist, quals: new_quals },
    )?);

    // CombineRangeTables (rewriteManip.c): append rtable + rteperminfos,
    // renumbering the appended RTEs' perminfoindex. A LATERAL marker on the
    // pulled-up subquery propagates to child RTEs that can carry lateral
    // refs, and their expressions get the same offset/sublevel adjustment
    // the subquery body got.
    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &sub.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        let new_index = if srte.perminfoindex > 0 {
            srte.perminfoindex + perm_offset
        } else {
            srte.perminfoindex
        };
        // Copy, don't scribble: the subquery may be shared with a cached
        // parse tree (sublink pull-up) and a replan re-runs this offset.
        let copy = rte_copy_with_perminfoindex(mcx, srte, new_index)?;
        // range_table_walker's RTE legs of OffsetVarNodes: join alias vars,
        // function expressions and values lists carry Vars into the combined
        // rtable.
        let crte = copy.as_range_tbl_entry().expect("just built");
        match srte.rtekind {
            RTEKind::RTE_JOIN => {
                let off_aliasvars =
                    match clauses::walker::mutate_list(mcx, &crte.joinaliasvars, &mut |n| {
                        offset_expr(mcx, n, rtoffset)
                    })? {
                        Some(l) => l,
                        None => crte.joinaliasvars.clone_in(mcx)?,
                    };
                // SAFETY: exclusive pre-seal fixup of the fresh copy.
                unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.joinaliasvars = off_aliasvars) };
            }
            RTEKind::RTE_FUNCTION => {
                let off = match map_rtfunctions(mcx, &crte.functions, &mut |n| {
                    offset_expr(mcx, n, rtoffset)
                })? {
                    Some(l) => l,
                    None => crte.functions.clone_in(mcx)?,
                };
                // SAFETY: pre-seal copy owned by this planner invocation.
                unsafe {
                    copy.with_mut::<RangeTblEntry, _>(|r| {
                        r.functions = off;
                        if rte.lateral {
                            r.lateral = true;
                        }
                    })
                };
            }
            RTEKind::RTE_VALUES => {
                let off = match clauses::walker::mutate_list(mcx, &crte.values_lists, &mut |n| {
                    offset_expr(mcx, n, rtoffset)
                })? {
                    Some(l) => l,
                    None => crte.values_lists.clone_in(mcx)?,
                };
                // SAFETY: as above.
                unsafe {
                    copy.with_mut::<RangeTblEntry, _>(|r| {
                        r.values_lists = off;
                        if rte.lateral {
                            r.lateral = true;
                        }
                    })
                };
            }
            _ => {}
        }
        parse.rtable.lappend(mcx, copy)?;
    }
    if result_rtr.is_some() {
        let eref = Node::mk_mut(
            mcx,
            types_nodes::Alias { aliasname: Some("*RESULT*"), colnames: NodeList::nil() },
        )?
        .seal_ref();
        parse.rtable.lappend(
            mcx,
            Node::mk(
                mcx,
                RangeTblEntry {
                    rtekind: RTEKind::RTE_RESULT,
                    eref: Some(eref),
                    inFromCl: true,
                    ..Default::default()
                },
            )?,
        )?;
    }
    for p in &sub.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    // SAFETY: as above — exclusive pre-seal tree fixup.
    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = None) };
    Ok(())
}

// The functions list of an RTE holds RangeTblFunction wrappers, which the
// expression mutator does not know; map their funcexprs explicitly.
fn map_rtfunctions<'mcx>(
    mcx: Mcx<'mcx>,
    functions: &NodeList<'mcx>,
    f: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out = NodeList::nil();
    for f_node in functions {
        let rtf = f_node.as_range_tbl_function().expect("functions cell");
        let new_expr = match rtf.funcexpr {
            Some(e) => f(e)?,
            None => None,
        };
        match new_expr {
            Some(e) => {
                changed = true;
                out.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::parsenodes::RangeTblFunction {
                            funcexpr: Some(e),
                            funccolcount: rtf.funccolcount,
                            funccolnames: rtf.funccolnames.clone_in(mcx)?,
                            funccoltypes: rtf.funccoltypes.clone_in(mcx)?,
                            funccoltypmods: rtf.funccoltypmods.clone_in(mcx)?,
                            funccolcollations: rtf.funccolcollations.clone_in(mcx)?,
                            funcparams: rtf.funcparams.clone_in(mcx)?,
                        },
                    )?,
                )?;
            }
            None => out.lappend(mcx, f_node)?,
        }
    }
    Ok(if changed { Some(out) } else { None })
}

// The jointree leg of pullup_replace_vars (replace_vars_in_jointree): swap
// the pulled-up RangeTblRef for its replacement, rewrite the quals of every
// JoinExpr/FromExpr, and rewrite lateral sibling RTEs' expressions.
fn splice_and_replace<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    replacement: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtindex = node.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            if rtindex == varno {
                return Ok(replacement);
            }
            let other_node = rtable.nth(rtindex as usize - 1);
            let other = other_node.as_range_tbl_entry().expect("rtable cell");
            if other.lateral {
                match other.rtekind {
                    RTEKind::RTE_FUNCTION => {
                        if let Some(l) =
                            map_rtfunctions(mcx, &other.functions, &mut |n| {
                                replace_var_expr(mcx, n, varno, tlist)
                            })?
                        {
                            // SAFETY: pre-seal tree owned by this planner
                            // invocation; exclusive fixup.
                            unsafe {
                                other_node.with_mut::<RangeTblEntry, _>(|r| r.functions = l)
                            };
                        }
                    }
                    RTEKind::RTE_VALUES => {
                        if let Some(l) =
                            clauses::walker::mutate_list(mcx, &other.values_lists, &mut |n| {
                                replace_var_expr(mcx, n, varno, tlist)
                            })?
                        {
                            // SAFETY: as above.
                            unsafe {
                                other_node.with_mut::<RangeTblEntry, _>(|r| r.values_lists = l)
                            };
                        }
                    }
                    RTEKind::RTE_SUBQUERY => {
                        let subq = other.subquery.expect("RTE_SUBQUERY has a subquery");
                        if let Some(newq) =
                            replace_vars_in_lateral_subquery(mcx, subq, varno, tlist)?
                        {
                            // SAFETY: as above.
                            unsafe {
                                other_node
                                    .with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(newq))
                            };
                        }
                    }
                    _ => {}
                }
            }
            Ok(node)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(
                    mcx,
                    splice_and_replace(mcx, rtable, child, varno, tlist, replacement)?,
                )?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: replace_opt(mcx, f.quals, varno, tlist)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = splice_and_replace(mcx, rtable, j.larg, varno, tlist, replacement)?;
            let rarg = splice_and_replace(mcx, rtable, j.rarg, varno, tlist, replacement)?;
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: replace_opt(mcx, j.quals, varno, tlist)?,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("pullup_replace_vars (prepjointree.c): {other:?} jointree arm"),
    }
}

// OffsetVarNodes (rewriteManip.c), functional: changed nodes are rebuilt.
// OffsetVarNodes' jointree leg (rewriteManip.c): RangeTblRef rtindex and
// JoinExpr rtindex/quals shift by rtoffset; the tree is rebuilt, not scribbled.
fn offset_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let r = node.as_range_tbl_ref().expect("RangeTblRef");
            Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().expect("FromExpr");
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(mcx, offset_jointree(mcx, child, rtoffset)?)?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: offset_opt(mcx, f.quals, rtoffset)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("JoinExpr");
            let quals = offset_opt(mcx, j.quals, rtoffset)?;
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg: offset_jointree(mcx, j.larg, rtoffset)?,
                    rarg: offset_jointree(mcx, j.rarg, rtoffset)?,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals,
                    alias: j.alias,
                    // C: if (j->rtindex) j->rtindex += offset.
                    rtindex: if j.rtindex != 0 { j.rtindex + rtoffset } else { 0 },
                },
            )
        }
        other => panic!("OffsetVarNodes (rewriteManip.c): {other:?} jointree arm"),
    }
}

fn offset_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup > 0 {
                // IncrementVarSublevelsUp(-1, 1): lateral/outer refs are one
                // level closer to their rels after pull-up; varno untouched.
                let mut nv = offset_var(mcx, v, 0)?;
                nv.varlevelsup -= 1;
                return Ok(Some(Node::mk(mcx, nv)?));
            }
            Ok(Some(Node::mk(mcx, offset_var(mcx, v, rtoffset)?)?))
        }
        NodeTag::T_RangeTblRef => {
            let r = node.as_range_tbl_ref().expect("RangeTblRef");
            Ok(Some(Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)?))
        }
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            offset_expr(mcx, n, rtoffset)
        }),
    }
}

fn offset_var<'mcx>(mcx: Mcx<'mcx>, v: &Var<'mcx>, rtoffset: i32) -> PgResult<Var<'mcx>> {
    Ok(Var {
        varno: v.varno + rtoffset,
        varattno: v.varattno,
        vartype: v.vartype,
        vartypmod: v.vartypmod,
        varcollid: v.varcollid,
        varnullingrels: {
            // offset_relid_set: nulling relids (incl. ojrelids) shift too.
            let mut out = types_nodes::Bitmapset::default();
            for m in v.varnullingrels.iter() {
                out.add_member(mcx, m + rtoffset)?;
            }
            out
        },
        varlevelsup: v.varlevelsup,
        varreturningtype: v.varreturningtype,
        varnosyn: if v.varnosyn > 0 { v.varnosyn.wrapping_add(rtoffset as u32) } else { v.varnosyn },
        varattnosyn: v.varattnosyn,
        location: v.location,
    })
}

fn offset_opt<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    rtoffset: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => Ok(Some(offset_expr(mcx, n, rtoffset)?.unwrap_or(n))),
    }
}

// pullup_replace_vars → ReplaceVarFromTargetList (rewriteManip.c),
// REPLACE_WRAP_NONE / REPLACEVARS_REPORT_ERROR arm. sublevels_up matching is
// C's replace_rte_variables_mutator; non-matching upper vars pass through.
fn replace_var_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    replace_var_expr_su(mcx, node, varno, tlist, 0)
}

fn replace_var_expr_su<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    sublevels_up: u32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup != sublevels_up || v.varno != varno {
                return Ok(None);
            }
            if !v.varnullingrels.is_empty() {
                panic!(
                    "ReplaceVarFromTargetList (rewriteManip.c): nulled Var over a \
                     pulled-up subquery; outer-join pullup lane unported"
                );
            }
            if v.varattno == 0 {
                panic!(
                    "ReplaceVarFromTargetList (rewriteManip.c): whole-row Var expansion \
                     (expandRTE/RowExpr) not ported"
                );
            }
            let Some(tle) = get_tle_by_resno(tlist, v.varattno) else {
                return Err(missing_attribute(v.varattno));
            };
            debug_assert!(!tle.resjunk);
            Ok(Some(copy_expr(mcx, tle.expr, sublevels_up)?))
        }
        NodeTag::T_SubLink | NodeTag::T_Query if sublevels_up > 0 => panic!(
            "replace_rte_variables (rewriteManip.c): SubLink inside a lateral \
             sibling subquery during pull-up; sublevel-tracking arm unported"
        ),
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            replace_var_expr_su(mcx, n, varno, tlist, sublevels_up)
        }),
    }
}

// pullup_replace_vars_subquery (prepjointree.c): rewrite level-1 references
// to the pulled-up rel inside a lateral sibling subquery. Returns None when
// nothing changed.
fn replace_vars_in_lateral_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    q: &'mcx Query<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
) -> PgResult<Option<&'mcx Query<'mcx>>> {
    let mut changed = false;
    let mut newq = crate::subselect::query_cells_copy(mcx, q)?;

    let rep_list = |l: &NodeList<'mcx>, changed: &mut bool| -> PgResult<NodeList<'mcx>> {
        match clauses::walker::mutate_list(mcx, l, &mut |n| {
            replace_var_expr_su(mcx, n, varno, tlist, 1)
        })? {
            Some(nl) => {
                *changed = true;
                Ok(nl)
            }
            None => Ok(l.clone_in(mcx)?),
        }
    };
    newq.targetList = rep_list(&newq.targetList, &mut changed)?;
    newq.returningList = rep_list(&newq.returningList, &mut changed)?;
    let mut rep_opt = |n: Option<Node<'mcx>>, changed: &mut bool| -> PgResult<Option<Node<'mcx>>> {
        match n {
            None => Ok(None),
            Some(x) => match replace_var_expr_su(mcx, x, varno, tlist, 1)? {
                Some(nx) => {
                    *changed = true;
                    Ok(Some(nx))
                }
                None => Ok(Some(x)),
            },
        }
    };
    newq.havingQual = rep_opt(newq.havingQual, &mut changed)?;
    newq.limitOffset = rep_opt(newq.limitOffset, &mut changed)?;
    newq.limitCount = rep_opt(newq.limitCount, &mut changed)?;

    let jt = newq.jointree.expect("jointree is a FromExpr");
    let mut jt_changed = false;
    let mut fromlist = NodeList::nil();
    for child in &jt.fromlist {
        fromlist.lappend(
            mcx,
            replace_in_sibling_jointree(mcx, child, varno, tlist, &mut jt_changed)?,
        )?;
    }
    let quals = rep_opt(jt.quals, &mut jt_changed)?;
    if jt_changed {
        changed = true;
        newq.jointree = Some(mcx::alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    }

    for srte_node in &newq.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        if !srte.lateral {
            continue;
        }
        match srte.rtekind {
            RTEKind::RTE_FUNCTION => {
                // query_cells_copy shares RTE nodes with the original query;
                // an in-place functions rewrite would scribble a shared tree.
                if map_rtfunctions(mcx, &srte.functions, &mut |n| {
                    replace_var_expr_su(mcx, n, varno, tlist, 1)
                })?
                .is_some()
                {
                    panic!(
                        "pullup_replace_vars_subquery (prepjointree.c): lateral function \
                         RTE inside a lateral sibling subquery references the pulled-up \
                         rel; nested-lateral rewrite arm unported"
                    );
                }
            }
            RTEKind::RTE_SUBQUERY => {
                let inner = srte.subquery.expect("RTE_SUBQUERY has a subquery");
                if contains_level_ref(inner, varno, 2)? {
                    panic!(
                        "pullup_replace_vars_subquery (prepjointree.c): doubly nested \
                         lateral subquery references the pulled-up rel; unported"
                    );
                }
            }
            _ => {}
        }
    }

    if !changed {
        return Ok(None);
    }
    Ok(Some(mcx::leak_in(mcx::alloc_in(mcx, newq)?)))
}

fn replace_in_sibling_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    changed: &mut bool,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(node),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist
                    .lappend(mcx, replace_in_sibling_jointree(mcx, child, varno, tlist, changed)?)?;
            }
            let quals = match f.quals {
                None => None,
                Some(q) => match replace_var_expr_su(mcx, q, varno, tlist, 1)? {
                    Some(nq) => {
                        *changed = true;
                        Some(nq)
                    }
                    None => Some(q),
                },
            };
            Node::mk(mcx, FromExpr { fromlist, quals })
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = replace_in_sibling_jointree(mcx, j.larg, varno, tlist, changed)?;
            let rarg = replace_in_sibling_jointree(mcx, j.rarg, varno, tlist, changed)?;
            let quals = match j.quals {
                None => None,
                Some(q) => match replace_var_expr_su(mcx, q, varno, tlist, 1)? {
                    Some(nq) => {
                        *changed = true;
                        Some(nq)
                    }
                    None => Some(q),
                },
            };
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("replace_vars_in_jointree (prepjointree.c): {other:?} jointree arm"),
    }
}

// contain_vars_of_level-style probe for references to one specific varno.
fn contains_level_ref<'mcx>(q: &'mcx Query<'mcx>, varno: i32, level: u32) -> PgResult<bool> {
    use nodes_core::NodeWalker;
    struct W {
        varno: i32,
        level: u32,
        found: bool,
    }
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup == self.level && v.varno == self.varno {
                    self.found = true;
                    return Ok(true);
                }
                return Ok(false);
            }
            if matches!(node.node_tag(), NodeTag::T_Query | NodeTag::T_SubLink) {
                self.level += 1;
                let r = nodes_core::expression_tree_walker(node, self);
                self.level -= 1;
                return r;
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { varno, level, found: false };
    for te in &q.targetList {
        w.visit(te)?;
    }
    if let Some(jt) = q.jointree {
        for n in &jt.fromlist {
            w.visit(n)?;
        }
        if let Some(qq) = jt.quals {
            w.visit(qq)?;
        }
    }
    if let Some(h) = q.havingQual {
        w.visit(h)?;
    }
    Ok(w.found)
}

fn replace_opt<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    varno: i32,
    tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => Ok(Some(replace_var_expr(mcx, n, varno, tlist)?.unwrap_or(n))),
    }
}

fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("tlist cell"))
        .find(|te| te.resno == resno)
}

// copyObject of the substituted expression (C copies per replacement; a
// shared node here would be double-visited by setrefs' in-place fixups).
// levels_delta is C's IncrementVarSublevelsUp(newnode, sublevels_up, 0) after
// substitution into a deeper query level.
fn copy_expr<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>, levels_delta: u32) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes as pn;
    let copy_list = |mcx: Mcx<'mcx>, l: &NodeList<'mcx>| -> PgResult<NodeList<'mcx>> {
        let mut out = NodeList::nil();
        for n in l {
            out.lappend(mcx, copy_expr(mcx, n, levels_delta)?)?;
        }
        Ok(out)
    };
    let copy_opt = |mcx: Mcx<'mcx>, n: Option<Node<'mcx>>| -> PgResult<Option<Node<'mcx>>> {
        match n {
            Some(n) => Ok(Some(copy_expr(mcx, n, levels_delta)?)),
            None => Ok(None),
        }
    };
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            let mut nv = offset_var(mcx, v, 0)?;
            nv.varlevelsup += levels_delta;
            Node::mk(mcx, nv)
        }
        NodeTag::T_Const => Node::mk(mcx, *node.as_const().expect("Const")),
        NodeTag::T_Param => Node::mk(mcx, *node.as_param().expect("Param")),
        NodeTag::T_CaseTestExpr => {
            Node::mk(mcx, *node.as_case_test_expr().expect("CaseTestExpr"))
        }
        NodeTag::T_SetToDefault => Node::mk(mcx, *node.as_set_to_default().expect("SetToDefault")),
        NodeTag::T_SQLValueFunction => {
            let s = node.as_sql_value_function().expect("SQLValueFunction");
            Node::mk(
                mcx,
                pn::SQLValueFunction {
                    op: s.op,
                    r#type: s.r#type,
                    typmod: s.typmod,
                    location: s.location,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().expect("OpExpr");
            Node::mk(
                mcx,
                pn::OpExpr {
                    opno: o.opno,
                    opfuncid: o.opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args: copy_list(mcx, &o.args)?,
                    location: o.location,
                },
            )
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().expect("DistinctExpr");
            Node::mk(
                mcx,
                pn::DistinctExpr {
                    opno: d.opno,
                    opfuncid: d.opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args: copy_list(mcx, &d.args)?,
                    location: d.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().expect("FuncExpr");
            Node::mk(
                mcx,
                pn::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args: copy_list(mcx, &f.args)?,
                    location: f.location,
                },
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().expect("ScalarArrayOpExpr");
            Node::mk(
                mcx,
                pn::ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid: sa.opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args: copy_list(mcx, &sa.args)?,
                    location: sa.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().expect("BoolExpr");
            Node::mk(
                mcx,
                pn::BoolExpr {
                    boolop: b.boolop,
                    args: copy_list(mcx, &b.args)?,
                    location: b.location,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().expect("RelabelType");
            Node::mk(
                mcx,
                pn::RelabelType {
                    arg: copy_expr(mcx, r.arg, levels_delta)?,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().expect("CoerceViaIO");
            Node::mk(
                mcx,
                pn::CoerceViaIO {
                    arg: copy_expr(mcx, c.arg, levels_delta)?,
                    resulttype: c.resulttype,
                    resultcollid: c.resultcollid,
                    coerceformat: c.coerceformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().expect("NullTest");
            Node::mk(
                mcx,
                pn::NullTest {
                    arg: copy_opt(mcx, nt.arg)?,
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().expect("BooleanTest");
            Node::mk(
                mcx,
                pn::BooleanTest {
                    arg: copy_opt(mcx, bt.arg)?,
                    booltesttype: bt.booltesttype,
                    location: bt.location,
                },
            )
        }
        NodeTag::T_CaseExpr => {
            let ce = node.as_case_expr().expect("CaseExpr");
            Node::mk(
                mcx,
                pn::CaseExpr {
                    casetype: ce.casetype,
                    casecollid: ce.casecollid,
                    arg: copy_opt(mcx, ce.arg)?,
                    args: copy_list(mcx, &ce.args)?,
                    defresult: copy_opt(mcx, ce.defresult)?,
                    location: ce.location,
                },
            )
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().expect("CaseWhen");
            Node::mk(
                mcx,
                pn::CaseWhen {
                    expr: copy_opt(mcx, cw.expr)?,
                    result: copy_opt(mcx, cw.result)?,
                    location: cw.location,
                },
            )
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().expect("CoalesceExpr");
            Node::mk(
                mcx,
                pn::CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args: copy_list(mcx, &co.args)?,
                    location: co.location,
                },
            )
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().expect("MinMaxExpr");
            Node::mk(
                mcx,
                pn::MinMaxExpr {
                    minmaxtype: mm.minmaxtype,
                    minmaxcollid: mm.minmaxcollid,
                    inputcollid: mm.inputcollid,
                    op: mm.op,
                    args: copy_list(mcx, &mm.args)?,
                    location: mm.location,
                },
            )
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().expect("ArrayExpr");
            Node::mk(
                mcx,
                pn::ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements: copy_list(mcx, &a.elements)?,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().expect("RowExpr");
            Node::mk(
                mcx,
                pn::RowExpr {
                    args: copy_list(mcx, &r.args)?,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(mcx)?,
                    location: r.location,
                },
            )
        }
        other => panic!(
            "copyObject (pullup_replace_vars): {other:?} copy arm unported \
             (simple-view expression set)"
        ),
    }
}

// reduce_outer_joins (prepjointree.c), INNER/LEFT/RIGHT/SEMI/ANTI slice
// (FULL never parses), including the LEFT -> ANTI reduction.
struct RojPass1<'mcx> {
    relids: types_nodes::Bitmapset<'mcx>,
    contains_outer: bool,
    sub_states: mcx::PgVec<'mcx, RojPass1<'mcx>>,
}

pub fn reduce_outer_joins<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let f = parse.jointree.expect("jointree is a FromExpr");
    let mut state1 = RojPass1 {
        relids: types_nodes::Bitmapset::empty(),
        contains_outer: false,
        sub_states: mcx::PgVec::new_in(mcx),
    };
    for child in &f.fromlist {
        let sub = reduce_outer_joins_pass1(mcx, child)?;
        state1.relids.add_members(mcx, &sub.relids)?;
        state1.contains_outer |= sub.contains_outer;
        state1.sub_states.push(sub);
    }
    assert!(state1.contains_outer, "so where are the outer joins?");

    let mut inner_reduced = types_nodes::Bitmapset::empty();
    let pass_nonnullable = clauses::find_nonnullable_rels(mcx, f.quals)?;
    let pass_forced = clauses::find_forced_null_vars(mcx, f.quals)?;
    let mut fromlist = NodeList::nil();
    for (i, child) in f.fromlist.iter().enumerate() {
        let sub = &state1.sub_states[i];
        if sub.contains_outer {
            fromlist.lappend(
                mcx,
                reduce_outer_joins_pass2(
                    mcx,
                    parse,
                    child,
                    sub,
                    &mut inner_reduced,
                    &pass_nonnullable,
                    &pass_forced,
                )?,
            )?;
        } else {
            fromlist.lappend(mcx, child)?;
        }
    }
    parse.jointree = Some(mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist, quals: f.quals },
    )?);

    if !inner_reduced.is_empty() {
        remove_nulling_relids(parse, &inner_reduced)?;
    }
    Ok(())
}

fn reduce_outer_joins_pass1<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<RojPass1<'mcx>> {
    let mut result = RojPass1 {
        relids: types_nodes::Bitmapset::empty(),
        contains_outer: false,
        sub_states: mcx::PgVec::new_in(mcx),
    };
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            result.relids.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            for child in &f.fromlist {
                let sub = reduce_outer_joins_pass1(mcx, child)?;
                result.relids.add_members(mcx, &sub.relids)?;
                result.contains_outer |= sub.contains_outer;
                result.sub_states.push(sub);
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            if j.jointype.is_outer_join() {
                result.contains_outer = true;
            }
            for arg in [j.larg, j.rarg] {
                let sub = reduce_outer_joins_pass1(mcx, arg)?;
                result.relids.add_members(mcx, &sub.relids)?;
                result.contains_outer |= sub.contains_outer;
                result.sub_states.push(sub);
            }
        }
        other => panic!("reduce_outer_joins_pass1 (prepjointree.c): {other:?}"),
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn reduce_outer_joins_pass2<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    node: Node<'mcx>,
    state1: &RojPass1<'mcx>,
    inner_reduced: &mut types_nodes::Bitmapset<'mcx>,
    nonnullable_rels: &types_nodes::Bitmapset<'mcx>,
    forced_null_vars: &clauses::MultiBitmapset<'mcx>,
) -> PgResult<Node<'mcx>> {
    if let Some(f) = node.as_variant::<FromExpr>() {
        let mut pass_nonnullable = clauses::find_nonnullable_rels(mcx, f.quals)?;
        pass_nonnullable.add_members(mcx, nonnullable_rels)?;
        let mut pass_forced = clauses::find_forced_null_vars(mcx, f.quals)?;
        clauses::mbms_add_members(mcx, &mut pass_forced, forced_null_vars)?;
        debug_assert_eq!(f.fromlist.len(), state1.sub_states.len());
        let mut fromlist = NodeList::nil();
        for (child, sub) in f.fromlist.iter().zip(state1.sub_states.iter()) {
            if sub.contains_outer {
                fromlist.lappend(
                    mcx,
                    reduce_outer_joins_pass2(
                        mcx,
                        parse,
                        child,
                        sub,
                        inner_reduced,
                        &pass_nonnullable,
                        &pass_forced,
                    )?,
                )?;
            } else {
                fromlist.lappend(mcx, child)?;
            }
        }
        return Node::mk(mcx, FromExpr { fromlist, quals: f.quals });
    }
    let j = node
        .as_join_expr()
        .unwrap_or_else(|| panic!("reduce_outer_joins_pass2: reached {:?}", node.node_tag()));
    let rtindex = j.rtindex;
    let mut jointype = j.jointype;
    let (mut larg, mut rarg) = (j.larg, j.rarg);
    let mut left_ix = 0usize;
    let mut right_ix = 1usize;

    match jointype {
        types_nodes::JoinType::JOIN_INNER => {}
        types_nodes::JoinType::JOIN_LEFT => {
            if nonnullable_rels.overlap(&state1.sub_states[1].relids) {
                jointype = types_nodes::JoinType::JOIN_INNER;
            }
        }
        types_nodes::JoinType::JOIN_RIGHT => {
            if nonnullable_rels.overlap(&state1.sub_states[0].relids) {
                jointype = types_nodes::JoinType::JOIN_INNER;
            }
        }
        types_nodes::JoinType::JOIN_FULL => {
            let l = nonnullable_rels.overlap(&state1.sub_states[0].relids);
            let r = nonnullable_rels.overlap(&state1.sub_states[1].relids);
            if l && r {
                jointype = types_nodes::JoinType::JOIN_INNER;
            } else if l || r {
                panic!(
                    "reduce_outer_joins_pass2 (prepjointree.c): partial FULL reduction \
                     (report_reduced_full_join nulling-bit removal) unported"
                );
            }
        }
        types_nodes::JoinType::JOIN_SEMI | types_nodes::JoinType::JOIN_ANTI => {}
        other => panic!(
            "reduce_outer_joins_pass2 (prepjointree.c): unrecognized join type {other:?}"
        ),
    }

    // JOIN_RIGHT -> JOIN_LEFT by swapping inputs.
    if jointype == types_nodes::JoinType::JOIN_RIGHT {
        core::mem::swap(&mut larg, &mut rarg);
        jointype = types_nodes::JoinType::JOIN_LEFT;
        left_ix = 1;
        right_ix = 0;
    }

    if jointype == types_nodes::JoinType::JOIN_LEFT {
        let nonnullable_vars = clauses::find_nonnullable_vars(mcx, j.quals)?;
        let overlap = clauses::mbms_overlap_sets(mcx, &nonnullable_vars, forced_null_vars)?;
        if overlap.overlap(&state1.sub_states[right_ix].relids) {
            jointype = types_nodes::JoinType::JOIN_ANTI;
        }
    }

    if rtindex != 0 && jointype != j.jointype {
        let rte_node = parse.rtable.nth(rtindex as usize - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        debug_assert_eq!(rte.rtekind, RTEKind::RTE_JOIN);
        debug_assert_eq!(rte.jointype, j.jointype);
        // SAFETY: pre-seal tree owned by this planner invocation; the shared
        // borrow is not read past this write.
        unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.jointype = jointype) };
        if jointype == types_nodes::JoinType::JOIN_INNER {
            inner_reduced.add_member(mcx, rtindex)?;
        }
    }

    let left_state = &state1.sub_states[left_ix];
    let right_state = &state1.sub_states[right_ix];
    if left_state.contains_outer || right_state.contains_outer {
        // INNER passes local+upper constraints down; LEFT passes upper to the
        // outer side and local to the nullable side; FULL passes nothing
        // (C's comment block).
        let is_full = jointype == types_nodes::JoinType::JOIN_FULL;
        let mut local_nonnullable = if is_full {
            types_nodes::Bitmapset::empty()
        } else {
            clauses::find_nonnullable_rels(mcx, j.quals)?
        };
        let mut local_forced = if is_full {
            mcx::PgVec::new_in(mcx)
        } else {
            clauses::find_forced_null_vars(mcx, j.quals)?
        };
        let inner_or_semi = matches!(
            jointype,
            types_nodes::JoinType::JOIN_INNER | types_nodes::JoinType::JOIN_SEMI
        );
        if inner_or_semi {
            local_nonnullable.add_members(mcx, nonnullable_rels)?;
            clauses::mbms_add_members(mcx, &mut local_forced, forced_null_vars)?;
        }

        let empty_nn = types_nodes::Bitmapset::empty();
        let empty_fv = mcx::PgVec::new_in(mcx);
        if left_state.contains_outer {
            let (nn, fv) = if inner_or_semi {
                (&local_nonnullable, &local_forced)
            } else if !is_full {
                (nonnullable_rels, forced_null_vars)
            } else {
                (&empty_nn, &empty_fv)
            };
            larg = reduce_outer_joins_pass2(mcx, parse, larg, left_state, inner_reduced, nn, fv)?;
        }
        if right_state.contains_outer {
            let (nn, fv) = if !is_full {
                (&local_nonnullable, &local_forced)
            } else {
                (&empty_nn, &empty_fv)
            };
            rarg = reduce_outer_joins_pass2(mcx, parse, rarg, right_state, inner_reduced, nn, fv)?;
        }
    }

    Node::mk(
        mcx,
        types_nodes::JoinExpr {
            jointype,
            isNatural: j.isNatural,
            larg,
            rarg,
            usingClause: j.usingClause.clone_in(mcx)?,
            join_using_alias: j.join_using_alias,
            quals: j.quals,
            alias: j.alias,
            rtindex: j.rtindex,
        },
    )
}

// remove_nulling_relids (rewriteManip.c), level-0 in-place form: strips the
// reduced joins' bits from every Var reachable at this query level. Sublevel
// Vars referencing these joins imply correlation, which is loud downstream.
fn remove_nulling_relids<'mcx>(
    parse: &Query<'mcx>,
    removable: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    use nodes_core::NodeWalker;
    struct W<'a, 'x> {
        removable: &'a types_nodes::Bitmapset<'x>,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, '_> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    let v = node.as_var().unwrap();
                    if v.varlevelsup == 0 && v.varnullingrels.overlap(self.removable) {
                        // SAFETY: pre-seal tree owned by this planner
                        // invocation; the shared borrow ends before the write.
                        unsafe {
                            node.with_mut::<Var, _>(|v| {
                                v.varnullingrels.del_members(self.removable)
                            })
                        };
                    }
                    Ok(false)
                }
                NodeTag::T_SubLink | NodeTag::T_Query => Ok(false),
                NodeTag::T_PlaceHolderVar => {
                    panic!("remove_nulling_relids_mutator (rewriteManip.c): PlaceHolderVar")
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
    }
    fn walk_jt<'mcx>(node: Node<'mcx>, w: &mut impl nodes_core::NodeWalker<'mcx>) -> PgResult<()> {
        match node.node_tag() {
            NodeTag::T_RangeTblRef => {}
            NodeTag::T_JoinExpr => {
                let j = node.as_join_expr().unwrap();
                walk_jt(j.larg, w)?;
                walk_jt(j.rarg, w)?;
                if let Some(q) = j.quals {
                    w.visit(q)?;
                }
            }
            other => panic!("remove_nulling_relids: {other:?} jointree arm"),
        }
        Ok(())
    }
    let mut w = W { removable };
    // query_tree_walker needs &'mcx Query; the parse is still a pre-seal
    // local, so walk its expression-bearing fields directly.
    for te in &parse.targetList {
        w.visit(te)?;
    }
    for te in &parse.returningList {
        w.visit(te)?;
    }
    for a in &parse.mergeActionList {
        w.visit(a)?;
    }
    if let Some(jc) = parse.mergeJoinCondition {
        w.visit(jc)?;
    }
    if let Some(h) = parse.havingQual {
        w.visit(h)?;
    }
    if let Some(n) = parse.limitOffset {
        w.visit(n)?;
    }
    if let Some(n) = parse.limitCount {
        w.visit(n)?;
    }
    let f = parse.jointree.expect("jointree is a FromExpr");
    for item in &f.fromlist {
        walk_jt(item, &mut w)?;
    }
    if let Some(q) = f.quals {
        w.visit(q)?;
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn missing_attribute(attno: i16) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not find attribute {attno} in subquery targetlist"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cfg(test)]
mod tests {
    use mcx::{alloc_leak_in, Mcx, MemoryContext};
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::{Query, RTEKind, RTEPermissionInfo, RangeTblEntry};
    use types_nodes::primnodes::{FromExpr, OpExpr, Var};
    use types_nodes::{Node, NodeList};

    fn var<'mcx>(mcx: Mcx<'mcx>, varno: i32, attno: i16) -> Node<'mcx> {
        Node::mk(mcx, Var { varno, varattno: attno, vartype: 23, ..Default::default() }).unwrap()
    }

    fn tle<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>, resno: i16) -> Node<'mcx> {
        Node::mk_target_entry(mcx, expr, resno, None, false).unwrap()
    }

    fn perminfo<'mcx>(mcx: Mcx<'mcx>, relid: u32) -> Node<'mcx> {
        Node::mk(mcx, RTEPermissionInfo { relid, ..Default::default() }).unwrap()
    }

    fn from_expr<'mcx>(
        mcx: Mcx<'mcx>,
        rti: i32,
        quals: Option<Node<'mcx>>,
    ) -> &'mcx FromExpr<'mcx> {
        let rtr = Node::mk_range_tbl_ref(mcx, rti).unwrap();
        alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals },
        )
        .unwrap()
    }

    #[test]
    fn simple_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let sub_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: 77,
                relkind: b'r',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let sub = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, sub_rte).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 77)).unwrap(),
                targetList: NodeList::make2(
                    mcx,
                    tle(mcx, var(mcx, 1, 1), 1),
                    tle(mcx, var(mcx, 1, 2), 2),
                )
                .unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();

        let view_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                relid: 99,
                relkind: b'v',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let qual = Node::mk(
            mcx,
            OpExpr {
                opno: 521,
                opresulttype: 16,
                args: NodeList::make2(
                    mcx,
                    var(mcx, 1, 1),
                    Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(5), false, true)
                        .unwrap(),
                )
                .unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, Some(qual))),
            ..Default::default()
        };

        super::pull_up_subqueries(mcx, &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 2);
        let dangling = parse.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(dangling.rtekind, RTEKind::RTE_SUBQUERY);
        assert!(dangling.subquery.is_none());
        assert_eq!(dangling.perminfoindex, 1);
        let base = parse.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(base.relid, 77);
        assert_eq!(base.perminfoindex, 2);
        assert_eq!(parse.rteperminfos.len(), 2);
        assert_eq!(
            parse.rteperminfos.nth(1).as_rte_permission_info().unwrap().relid,
            77
        );

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 2);

        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (2, 1));

        let q = jt.quals.unwrap().as_op_expr().unwrap();
        let qual_var = q.args.nth(0).as_var().unwrap();
        assert_eq!((qual_var.varno, qual_var.varattno), (2, 1));
        assert!(q.args.nth(1).as_const().is_some());
    }


    fn rel_rte<'mcx>(mcx: Mcx<'mcx>, relid: u32, perm: u32) -> Node<'mcx> {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: b'r',
                perminfoindex: perm,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn view_rte<'mcx>(mcx: Mcx<'mcx>, relid: u32, sub: &'mcx Query<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                relid,
                relkind: b'v',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn eq_qual<'mcx>(mcx: Mcx<'mcx>, l: Node<'mcx>, r: Node<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            OpExpr {
                opno: 96,
                opresulttype: 16,
                args: NodeList::make2(mcx, l, r).unwrap(),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn join_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let join_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_JOIN,
                jointype: types_nodes::JoinType::JOIN_INNER,
                joinaliasvars: NodeList::make2(mcx, var(mcx, 1, 1), var(mcx, 2, 1)).unwrap(),
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let jexpr = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_INNER,
                isNatural: false,
                larg: Node::mk_range_tbl_ref(mcx, 1).unwrap(),
                rarg: Node::mk_range_tbl_ref(mcx, 2).unwrap(),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(eq_qual(mcx, var(mcx, 1, 1), var(mcx, 2, 1))),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        let sub = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make3(mcx, rel_rte(mcx, 77, 1), rel_rte(mcx, 78, 2), join_rte)
                    .unwrap(),
                rteperminfos: NodeList::make2(mcx, perminfo(mcx, 77), perminfo(mcx, 78)).unwrap(),
                targetList: NodeList::make2(
                    mcx,
                    tle(mcx, var(mcx, 1, 1), 1),
                    tle(mcx, var(mcx, 2, 1), 2),
                )
                .unwrap(),
                jointree: Some(
                    alloc_leak_in(
                        mcx,
                        FromExpr { fromlist: NodeList::make1(mcx, jexpr).unwrap(), quals: None },
                    )
                    .unwrap(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte(mcx, 99, sub)).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 2), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, None)),
            ..Default::default()
        };

        super::pull_up_subqueries(mcx, &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 4);
        assert_eq!(parse.rtable.nth(1).as_range_tbl_entry().unwrap().perminfoindex, 2);
        assert_eq!(parse.rtable.nth(2).as_range_tbl_entry().unwrap().perminfoindex, 3);
        let jrte = parse.rtable.nth(3).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.rtekind, RTEKind::RTE_JOIN);
        let av0 = jrte.joinaliasvars.nth(0).as_var().unwrap();
        let av1 = jrte.joinaliasvars.nth(1).as_var().unwrap();
        assert_eq!((av0.varno, av1.varno), (2, 3));

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        let j = jt.fromlist.nth(0).as_join_expr().unwrap();
        assert_eq!(j.rtindex, 4);
        assert_eq!(j.larg.as_range_tbl_ref().unwrap().rtindex, 2);
        assert_eq!(j.rarg.as_range_tbl_ref().unwrap().rtindex, 3);
        let jq = j.quals.unwrap().as_op_expr().unwrap();
        assert_eq!(jq.args.nth(0).as_var().unwrap().varno, 2);
        assert_eq!(jq.args.nth(1).as_var().unwrap().varno, 3);

        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (3, 1));
    }

    #[test]
    fn nested_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let v1 = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, rel_rte(mcx, 77, 1)).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 77)).unwrap(),
                targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();
        let v2 = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, view_rte(mcx, 88, v1)).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 88)).unwrap(),
                targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte(mcx, 99, v2)).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, None)),
            ..Default::default()
        };

        super::pull_up_subqueries(mcx, &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 3);
        let mid = parse.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(mid.rtekind, RTEKind::RTE_SUBQUERY);
        assert!(mid.subquery.is_none());
        let base = parse.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(base.relid, 77);
        assert_eq!(base.perminfoindex, 3);
        assert_eq!(parse.rteperminfos.len(), 3);
        assert_eq!(parse.rteperminfos.nth(2).as_rte_permission_info().unwrap().relid, 77);

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 3);
        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (3, 1));
    }
}

// transform_MERGE_to_join (prepjointree.c): replace the MERGE jointree (the
// bare source) with a join between the target and the source. WHEN NOT
// MATCHED BY SOURCE is the loud arm: it needs the outer-target join with
// source-var nulling marks and the executor's join-condition recheck.
pub fn transform_MERGE_to_join<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    use types_nodes::jointype::JoinType;
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::primnodes::{MergeMatchKind, NUM_MERGE_MATCH_KINDS};

    if parse.commandType != CmdType::CMD_MERGE {
        return Ok(());
    }

    let mut have_action = [false; NUM_MERGE_MATCH_KINDS];
    for action_node in &parse.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        if action.commandType != CmdType::CMD_NOTHING {
            have_action[action.matchKind as usize] = true;
        }
    }
    if have_action[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize] {
        panic!(
            "transform_MERGE_to_join (prepjointree.c): WHEN NOT MATCHED BY SOURCE \
             (outer-target join + add_nulling_relids + executor join-condition \
             recheck) unported — MERGE by-source lane"
        );
    }
    let jointype =
        if have_action[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize] {
            JoinType::JOIN_RIGHT
        } else {
            JoinType::JOIN_INNER
        };

    let eref = mcx::leak_in(mcx::alloc_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("*MERGE*"), colnames: NodeList::nil() },
    )?);
    let joinrte = RangeTblEntry {
        rtekind: RTEKind::RTE_JOIN,
        jointype,
        eref: Some(eref),
        inFromCl: true,
        ..Default::default()
    };
    parse.rtable.lappend(mcx, Node::mk(mcx, joinrte)?)?;
    let joinrti = parse.rtable.len() as i32;

    let jt = parse.jointree.expect("MERGE jointree is a FromExpr");
    let rtr = Node::mk(
        mcx,
        types_nodes::primnodes::RangeTblRef { rtindex: parse.mergeTargetRelation },
    )?;
    let target = Node::mk(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr)?, quals: jt.quals },
    )?;
    assert_eq!(jt.fromlist.len(), 1, "MERGE jointree carries exactly the source");
    let source = jt.fromlist.nth(0);
    debug_assert!(matches!(
        source.node_tag(),
        NodeTag::T_RangeTblRef | NodeTag::T_JoinExpr
    ));

    let joinexpr = Node::mk(
        mcx,
        types_nodes::primnodes::JoinExpr {
            jointype,
            isNatural: false,
            larg: target,
            rarg: source,
            usingClause: NodeList::nil(),
            join_using_alias: None,
            quals: parse.mergeJoinCondition,
            alias: None,
            rtindex: joinrti,
        },
    )?;
    parse.jointree = Some(
        Node::mk_mut(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, joinexpr)?, quals: None },
        )?
        .seal_ref(),
    );

    // A non-empty targetList here means a trigger-updatable view target
    // (add_nulling_relids over its wholerow Var) — the view lane is loud
    // upstream in the rewriter.
    debug_assert!(parse.targetList.is_nil());

    // Without BY SOURCE actions the executor never rechecks the join
    // condition; C drops it to save planning/execution cycles.
    parse.mergeJoinCondition = None;
    Ok(())
}
