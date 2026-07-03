//! prepjointree.c, simple-view slice: pull_up_subqueries over a one-level
//! FromExpr of RangeTblRefs. Every non-pullable subquery is a named panic —
//! the SubqueryScan fallback lane is unported, so a silent keep would plan
//! nothing.

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, TargetEntry, Var};
use types_nodes::{Node, NodeTag};

pub fn pull_up_subqueries<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let jt = parse.jointree.expect("jointree is a FromExpr");
    let mut fromlist = NodeList::nil();
    let mut quals = jt.quals;
    let mut changed = false;
    for child in &jt.fromlist {
        match child.node_tag() {
            NodeTag::T_RangeTblRef => {
                let rti = child.as_range_tbl_ref().expect("RangeTblRef").rtindex;
                let rte_node = parse.rtable.nth(rti as usize - 1);
                let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
                if rte.rtekind == RTEKind::RTE_SUBQUERY {
                    assert_simple_subquery(rte)?;
                    let (replacement, new_quals) =
                        pull_up_simple_subquery(mcx, parse, rti, rte_node, quals)?;
                    quals = new_quals;
                    fromlist.lappend(mcx, replacement)?;
                    changed = true;
                    continue;
                }
                fromlist.lappend(mcx, child)?;
            }
            other => panic!(
                "pull_up_subqueries_recurse (prepjointree.c): {other:?} jointree arm; \
                 M2 join lane"
            ),
        }
    }
    if changed {
        parse.jointree = Some(mcx::alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    }
    Ok(())
}

// is_simple_subquery (prepjointree.c) with every false-return a named panic:
// C keeps the RTE and plans a SubqueryScan; that lane is unported.
fn assert_simple_subquery(rte: &RangeTblEntry<'_>) -> PgResult<()> {
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
    } else if rte.lateral {
        Some("LATERAL")
    } else {
        None
    };
    if let Some(what) = blocked {
        panic!(
            "is_simple_subquery (prepjointree.c): {what} subquery is not pullable — \
             SubqueryScan planning lane unported"
        );
    }
    for te in &sub.targetList {
        if clauses::contain_volatile_functions(te)? {
            panic!(
                "is_simple_subquery (prepjointree.c): volatile targetlist — \
                 SubqueryScan planning lane unported"
            );
        }
    }
    Ok(())
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
    outer_quals: Option<Node<'mcx>>,
) -> PgResult<(Node<'mcx>, Option<Node<'mcx>>)> {
    let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
    let sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");

    if sub.hasSubLinks {
        panic!("pull_up_sublinks (prepjointree.c): sublinks in pulled-up subquery; M2 lane");
    }
    debug_assert!(!sub.hasRowSecurity);
    debug_assert!(sub.rowMarks.is_nil());
    for srte_node in &sub.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        if srte.rtekind == RTEKind::RTE_SUBQUERY {
            panic!(
                "pull_up_simple_subquery (prepjointree.c): recursive pull_up_subqueries \
                 (nested subquery / view-on-view) not ported"
            );
        }
    }
    let sub_jt = sub.jointree.expect("jointree is a FromExpr");
    if sub_jt.fromlist.is_nil() {
        panic!(
            "pull_up_simple_subquery (prepjointree.c): replace_empty_jointree of an \
             empty-FROM subquery not ported"
        );
    }

    let rtoffset = parse.rtable.len() as i32;

    let off_tlist = match clauses::walker::mutate_list(mcx, &sub.targetList, &mut |n| {
        offset_expr(mcx, n, rtoffset)
    })? {
        Some(l) => l,
        None => sub.targetList.clone_in(mcx)?,
    };
    let mut off_fromlist = NodeList::nil();
    for jnode in &sub_jt.fromlist {
        match jnode.node_tag() {
            NodeTag::T_RangeTblRef => {
                let r = jnode.as_range_tbl_ref().expect("RangeTblRef");
                off_fromlist
                    .lappend(mcx, Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)?)?;
            }
            other => panic!(
                "OffsetVarNodes (rewriteManip.c): {other:?} jointree arm; M2 join lane"
            ),
        }
    }
    let off_quals = offset_opt(mcx, sub_jt.quals, rtoffset)?;

    if let Some(l) = clauses::walker::mutate_list(mcx, &parse.targetList, &mut |n| {
        replace_var_expr(mcx, n, varno, &off_tlist)
    })? {
        parse.targetList = l;
    }
    let new_outer_quals = replace_opt(mcx, outer_quals, varno, &off_tlist)?;
    parse.havingQual = replace_opt(mcx, parse.havingQual, varno, &off_tlist)?;
    debug_assert!(parse.returningList.is_nil());

    // CombineRangeTables (rewriteManip.c): append rtable + rteperminfos,
    // renumbering the appended RTEs' perminfoindex.
    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &sub.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        if srte.perminfoindex > 0 && perm_offset > 0 {
            // SAFETY: pre-seal tree owned by this planner invocation; the
            // shared `srte` borrow is not read past this write.
            unsafe {
                srte_node.with_mut::<RangeTblEntry, _>(|r| r.perminfoindex += perm_offset)
            };
        }
        parse.rtable.lappend(mcx, srte_node)?;
    }
    for p in &sub.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    // SAFETY: as above — exclusive pre-seal tree fixup.
    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = None) };

    let replacement = if off_quals.is_none() && off_fromlist.len() == 1 {
        off_fromlist.nth(0)
    } else {
        Node::mk(mcx, FromExpr { fromlist: off_fromlist, quals: off_quals })?
    };
    Ok((replacement, new_outer_quals))
}

// OffsetVarNodes (rewriteManip.c), functional: changed nodes are rebuilt.
fn offset_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup > 0 {
                panic!(
                    "IncrementVarSublevelsUp (rewriteManip.c): upper-level Var in \
                     pulled-up subquery; M2 sublink lane"
                );
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
        varnullingrels: v.varnullingrels.clone_in(mcx)?,
        varlevelsup: v.varlevelsup,
        varreturningtype: v.varreturningtype,
        varnosyn: if v.varnosyn > 0 { v.varnosyn + rtoffset as u32 } else { v.varnosyn },
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
// REPLACE_WRAP_NONE / REPLACEVARS_REPORT_ERROR arm.
fn replace_var_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup > 0 {
                panic!(
                    "replace_rte_variables_mutator (rewriteManip.c): upper-level Var; \
                     M2 sublink lane"
                );
            }
            if v.varno != varno {
                return Ok(None);
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
            Ok(Some(copy_expr(mcx, tle.expr)?))
        }
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            replace_var_expr(mcx, n, varno, tlist)
        }),
    }
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
fn copy_expr<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            Node::mk(mcx, offset_var(mcx, v, 0)?)
        }
        NodeTag::T_Const => Node::mk(mcx, *node.as_const().expect("Const")),
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().expect("OpExpr");
            let mut args = NodeList::nil();
            for a in &o.args {
                args.lappend(mcx, copy_expr(mcx, a)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid: o.opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        other => panic!(
            "copyObject (pullup_replace_vars): {other:?} copy arm unported \
             (simple-view expression set)"
        ),
    }
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
}
