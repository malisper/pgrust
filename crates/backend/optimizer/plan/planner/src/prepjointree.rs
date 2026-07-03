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
        let mut target: Option<i32> = None;
        for child in &jt.fromlist {
            find_pullable_subquery(parse, child, &mut target, &kept);
            if target.is_some() {
                break;
            }
        }
        let Some(rti) = target else { return Ok(()) };
        let rte_node = parse.rtable.nth(rti as usize - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if !is_simple_subquery(rte)? {
            kept.push(rti);
            continue;
        }
        pull_up_simple_subquery(mcx, parse, rti, rte_node)?;
    }
}

fn find_pullable_subquery<'mcx>(
    parse: &Query<'mcx>,
    node: Node<'mcx>,
    target: &mut Option<i32>,
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
                *target = Some(rti);
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            for child in &f.fromlist {
                find_pullable_subquery(parse, child, target, kept);
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            find_pullable_subquery(parse, j.larg, target, kept);
            find_pullable_subquery(parse, j.rarg, target, kept);
        }
        other => panic!(
            "pull_up_subqueries_recurse (prepjointree.c): {other:?} jointree arm; \
             M2 join lane"
        ),
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
// SubqueryScan path (set_subquery_pathlist); LATERAL stays loud (parser too).
fn is_simple_subquery(rte: &RangeTblEntry<'_>) -> PgResult<bool> {
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
    assert!(!rte.lateral, "is_simple_subquery (prepjointree.c): LATERAL; M2 lateral lane");
    if blocked.is_some() {
        return Ok(false);
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
            splice_and_replace(mcx, child, varno, &off_tlist, replacement)?,
        )?;
    }
    let new_quals = replace_opt(mcx, jt.quals, varno, &off_tlist)?;
    parse.jointree = Some(mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist: new_fromlist, quals: new_quals },
    )?);

    // CombineRangeTables (rewriteManip.c): append rtable + rteperminfos,
    // renumbering the appended RTEs' perminfoindex.
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
        // range_table_walker's RTE legs of OffsetVarNodes: join alias vars and
        // function expressions carry Vars into the combined rtable.
        if srte.rtekind == RTEKind::RTE_JOIN {
            let off_aliasvars = match clauses::walker::mutate_list(
                mcx,
                &srte.joinaliasvars,
                &mut |n| offset_expr(mcx, n, rtoffset),
            )? {
                Some(l) => l,
                None => srte.joinaliasvars.clone_in(mcx)?,
            };
            // SAFETY: exclusive pre-seal fixup of the fresh copy.
            unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.joinaliasvars = off_aliasvars) };
        }
        if srte.rtekind == RTEKind::RTE_FUNCTION {
            if let Some(l) = clauses::walker::mutate_list(mcx, &srte.functions, &mut |n| {
                offset_expr(mcx, n, rtoffset)
            })? {
                // SAFETY: exclusive pre-seal fixup of the fresh copy.
                unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.functions = l) };
            }
        }
        parse.rtable.lappend(mcx, copy)?;
    }
    for p in &sub.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    // SAFETY: as above — exclusive pre-seal tree fixup.
    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = None) };
    Ok(())
}

// The jointree leg of pullup_replace_vars: swap the pulled-up RangeTblRef
// for its replacement and rewrite the quals of every JoinExpr/FromExpr.
fn splice_and_replace<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    replacement: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            if node.as_range_tbl_ref().expect("RangeTblRef").rtindex == varno {
                Ok(replacement)
            } else {
                Ok(node)
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(
                    mcx,
                    splice_and_replace(mcx, child, varno, tlist, replacement)?,
                )?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: replace_opt(mcx, f.quals, varno, tlist)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = splice_and_replace(mcx, j.larg, varno, tlist, replacement)?;
            let rarg = splice_and_replace(mcx, j.rarg, varno, tlist, replacement)?;
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
                    rtindex: j.rtindex + rtoffset,
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
}
