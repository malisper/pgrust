use mcx::{alloc_leak_in, Mcx};
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Alias, FromExpr};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

// Empty FROM becomes a dummy RTE_RESULT + RangeTblRef. C mutates the FromExpr
// in place; jointree is a shared ref here, so an equivalent one is rebuilt.
pub fn replace_empty_jointree<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let quals = match parse.jointree {
        Some(f) if f.fromlist.is_nil() => f.quals,
        Some(_) => return Ok(()),
        None => None,
    };
    if parse.setOperations.is_some() {
        return Ok(());
    }

    let eref = alloc_leak_in(
        mcx,
        Alias { aliasname: Some("*RESULT*"), colnames: NodeList::nil() },
    )?;
    let mut rte = Node::build::<RangeTblEntry>(mcx)?;
    rte.rtekind = RTEKind::RTE_RESULT;
    rte.eref = Some(eref);
    parse.rtable.lappend(mcx, rte.seal())?;
    let rti = parse.rtable.len() as i32;

    let rtr = Node::mk_range_tbl_ref(mcx, rti)?;
    let fromlist = NodeList::make1(mcx, rtr)?;
    parse.jointree = Some(alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    Ok(())
}

// remove_useless_result_rtes + remove_useless_results_recurse
// (prepjointree.c:3596). C mutates in place; here the recursion returns None
// for untouched subtrees so the unchanged case (every RESULT-bearing query
// pays this pass) allocates nothing.
pub fn remove_useless_result_rtes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let f = parse.jointree.expect("top jointree is a FromExpr");
    // All-RangeTblRef fast path (SELECT 1 and every no-join query pays this
    // pass): the recursion is a per-child no-op, so drop RESULT siblings
    // directly and rebuild only when something dropped. A RESULT that
    // computes PHVs needed by a sibling must stay (C's
    // find_dependent_phvs_in_jointree gate against the whole FromExpr).
    if f.fromlist.iter().all(|n| n.node_tag() == NodeTag::T_RangeTblRef) {
        let total = f.fromlist.len();
        // The dependent-PHV gate is C's lastPHId != 0 dynamic gate; its check
        // node is built lazily so SELECT 1 (and every no-join query) pays
        // nothing extra here.
        let mut f_node: Option<Node<'mcx>> = None;
        let mut dropped: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
        let mut fromlist = NodeList::nil();
        for n in &f.fromlist {
            if total - dropped.len() > 1 {
                let varno = get_result_relid(parse, n);
                if varno != 0 {
                    let dependent = if run.glob.last_ph_id == 0 {
                        false
                    } else {
                        let fnode = match f_node {
                            Some(x) => x,
                            None => {
                                let x = Node::mk(
                                    mcx,
                                    FromExpr {
                                        fromlist: f.fromlist.clone_in(mcx)?,
                                        quals: f.quals,
                                    },
                                )?;
                                f_node = Some(x);
                                x
                            }
                        };
                        crate::prepjointree::find_dependent_phvs_in_jointree(
                            run, parse, fnode, varno,
                        )?
                    };
                    if !dependent {
                        dropped.push(varno);
                        continue;
                    }
                }
            }
            fromlist.lappend(mcx, n)?;
        }
        if !dropped.is_empty() {
            if run.glob.last_ph_id != 0 {
                let new_f_node = Node::mk(
                    mcx,
                    FromExpr { fromlist: fromlist.clone_in(mcx)?, quals: f.quals },
                )?;
                for &varno in dropped.iter() {
                    crate::prepjointree::remove_result_refs(run, parse, varno, new_f_node)?;
                }
            }
            parse.jointree = Some(alloc_leak_in(mcx, FromExpr { fromlist, quals: f.quals })?);
        }
        // Unconditional in C: marks on SURVIVING RESULT RTEs drop too.
        if !run.root.rowMarks.is_empty() {
            drop_rowmarks_on_result(run, parse);
        }
        return Ok(());
    }
    let mut dropped_outer_joins = types_nodes::bitmapset::Bitmapset::empty();
    let mut slot = QualSlot { node: f.quals, changed: false };
    let mut children: mcx::PgVec<'mcx, Option<Node<'mcx>>> = mcx::PgVec::new_in(mcx);
    let mut any_child_changed = false;
    for child in &f.fromlist {
        match remove_useless_results_recurse(
            run,
            parse,
            child,
            Some(&mut slot),
            &mut dropped_outer_joins,
        )? {
            Some(n) => {
                any_child_changed = true;
                children.push(Some(n));
            }
            None => children.push(Some(child)),
        }
    }
    let (fromlist, ndropped) =
        drop_result_children(run, parse, &mut children, slot.node)?;
    if any_child_changed || slot.changed || ndropped > 0 {
        parse.jointree = Some(alloc_leak_in(mcx, FromExpr { fromlist, quals: slot.node })?);
    }

    if !dropped_outer_joins.is_empty() {
        crate::prepjointree::remove_nulling_relids(run, parse, &dropped_outer_joins, None)?;
    }
    if !run.root.rowMarks.is_empty() {
        drop_rowmarks_on_result(run, parse);
    }
    Ok(())
}

// C drops any PlanRowMark on a RESULT RTE (removed or surviving): the RTE
// produces no rows to mark. Only the id list shrinks; the store entry goes
// unreferenced (C frees the list cell, not the mark).
fn drop_rowmarks_on_result<'mcx>(run: &mut PlannerRun<'mcx>, parse: &Query<'mcx>) {
    let rowmarks = &run.rowmarks;
    run.root.rowMarks.retain(|&id| {
        let rc = &rowmarks[id.0 as usize];
        let rte = parse
            .rtable
            .nth(rc.rti as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        rte.rtekind != RTEKind::RTE_RESULT
    });
}

struct QualSlot<'mcx> {
    node: Option<Node<'mcx>>,
    changed: bool,
}

// The FromExpr-arm drop loop shared by the top level and the recursion:
// RESULT rels with at least one sibling drop unless a sibling (or the quals)
// references PHVs evaluated at them; dropped rels get remove_result_refs
// against the surviving fromlist.
fn drop_result_children<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    children: &mut [Option<Node<'mcx>>],
    quals: Option<Node<'mcx>>,
) -> PgResult<(NodeList<'mcx>, usize)> {
    let mcx = run.mcx;
    let mut remaining = children.len();
    let mut dropped: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
    for i in 0..children.len() {
        if remaining <= 1 {
            break;
        }
        let child = children[i].expect("live child");
        let varno = get_result_relid(parse, child);
        if varno == 0 {
            continue;
        }
        // C's dependent-PHV gate against f as it currently stands
        // (already-dropped RESULT siblings excluded; they carry no
        // expressions anyway), built only when PHVs exist (lastPHId != 0).
        if run.glob.last_ph_id != 0 {
            let mut cur = NodeList::nil();
            for c in children.iter().flatten() {
                cur.lappend(mcx, *c)?;
            }
            let f_node = Node::mk(mcx, FromExpr { fromlist: cur, quals })?;
            if crate::prepjointree::find_dependent_phvs_in_jointree(run, parse, f_node, varno)? {
                continue;
            }
        }
        remaining -= 1;
        dropped.push(varno);
        children[i] = None;
    }
    let mut fromlist = NodeList::nil();
    for c in children.iter().flatten() {
        fromlist.lappend(mcx, *c)?;
    }
    if !dropped.is_empty() && run.glob.last_ph_id != 0 {
        let new_f = Node::mk(mcx, FromExpr { fromlist: fromlist.clone_in(mcx)?, quals })?;
        for &varno in dropped.iter() {
            crate::prepjointree::remove_result_refs(run, parse, varno, new_f)?;
        }
    }
    Ok((fromlist, dropped.len()))
}

fn get_result_relid<'mcx>(parse: &Query<'mcx>, jtnode: Node<'mcx>) -> i32 {
    let Some(rtr) = jtnode.as_range_tbl_ref() else { return 0 };
    let rte = parse
        .rtable
        .nth(rtr.rtindex as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    if rte.rtekind == RTEKind::RTE_RESULT { rtr.rtindex } else { 0 }
}

// Pushed-up quals are implicit-AND T_List nodes; child quals go in front.
fn merge_quals<'mcx>(
    mcx: Mcx<'mcx>,
    child: Option<Node<'mcx>>,
    parent: &mut QualSlot<'mcx>,
) -> PgResult<()> {
    let Some(c) = child else { return Ok(()) };
    let mut merged = c
        .as_list()
        .expect("preprocessed quals are a list")
        .clone_in(mcx)?;
    if let Some(p) = parent.node {
        for q in p.as_list().expect("preprocessed quals are a list") {
            merged.lappend(mcx, q)?;
        }
    }
    parent.node = Some(Node::mk_list(mcx, merged)?);
    parent.changed = true;
    Ok(())
}

// Returns Some(replacement) when the subtree changed, None when untouched.
fn remove_useless_results_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    jtnode: Node<'mcx>,
    mut parent_quals: Option<&mut QualSlot<'mcx>>,
    dropped_outer_joins: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    use types_nodes::jointype::JoinType;
    let mcx = run.mcx;
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => Ok(None),
        NodeTag::T_FromExpr => {
            let f = jtnode.as_from_expr().expect("FromExpr");
            let mut slot = QualSlot { node: f.quals, changed: false };
            let mut children: mcx::PgVec<'mcx, Option<Node<'mcx>>> = mcx::PgVec::new_in(mcx);
            let mut any_child_changed = false;
            for child in &f.fromlist {
                match remove_useless_results_recurse(
                    run,
                    parse,
                    child,
                    Some(&mut slot),
                    dropped_outer_joins,
                )? {
                    Some(n) => {
                        any_child_changed = true;
                        children.push(Some(n));
                    }
                    None => children.push(Some(child)),
                }
            }
            let (fromlist, ndropped) =
                drop_result_children(run, parse, &mut children, slot.node)?;
            if fromlist.len() == 1 && (slot.node.is_none() || parent_quals.is_some()) {
                let kept = fromlist.nth(0);
                if let Some(p) = parent_quals {
                    merge_quals(mcx, slot.node, p)?;
                }
                return Ok(Some(kept));
            }
            if !any_child_changed && !slot.changed && ndropped == 0 {
                return Ok(None);
            }
            Ok(Some(Node::mk(mcx, FromExpr { fromlist, quals: slot.node })?))
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().expect("JoinExpr");
            let mut slot = QualSlot { node: j.quals, changed: false };
            let lres = match j.jointype {
                JoinType::JOIN_INNER => remove_useless_results_recurse(
                    run,
                    parse,
                    j.larg,
                    Some(&mut slot),
                    dropped_outer_joins,
                )?,
                JoinType::JOIN_LEFT => remove_useless_results_recurse(
                    run,
                    parse,
                    j.larg,
                    parent_quals.as_deref_mut(),
                    dropped_outer_joins,
                )?,
                _ => remove_useless_results_recurse(
                    run,
                    parse,
                    j.larg,
                    None,
                    dropped_outer_joins,
                )?,
            };
            let rres = match j.jointype {
                JoinType::JOIN_INNER | JoinType::JOIN_LEFT => remove_useless_results_recurse(
                    run,
                    parse,
                    j.rarg,
                    Some(&mut slot),
                    dropped_outer_joins,
                )?,
                _ => remove_useless_results_recurse(
                    run,
                    parse,
                    j.rarg,
                    None,
                    dropped_outer_joins,
                )?,
            };
            let larg = lres.unwrap_or(j.larg);
            let rarg = rres.unwrap_or(j.rarg);

            match j.jointype {
                JoinType::JOIN_INNER => {
                    let lrel = get_result_relid(parse, larg);
                    let rrel = get_result_relid(parse, rarg);
                    // C gates the larg drop on the rarg (the only side that
                    // may hold a lateral ref to it); the rarg drop needs no
                    // gate since nothing after it can reference it.
                    let keep = if lrel != 0
                        && !crate::prepjointree::find_dependent_phvs_in_jointree(
                            run, parse, rarg, lrel,
                        )? {
                        crate::prepjointree::remove_result_refs(run, parse, lrel, rarg)?;
                        Some(rarg)
                    } else if rrel != 0 {
                        crate::prepjointree::remove_result_refs(run, parse, rrel, larg)?;
                        Some(larg)
                    } else {
                        None
                    };
                    if let Some(keep) = keep {
                        if slot.node.is_some() && parent_quals.is_none() {
                            return Ok(Some(Node::mk(
                                mcx,
                                FromExpr {
                                    fromlist: NodeList::make1(mcx, keep)?,
                                    quals: slot.node,
                                },
                            )?));
                        }
                        if let Some(p) = parent_quals {
                            merge_quals(mcx, slot.node, p)?;
                        }
                        return Ok(Some(keep));
                    }
                }
                JoinType::JOIN_LEFT => {
                    // Strength-reduce only if no PHV depends on the RESULT
                    // rel when the qual could null-extend it (C: quals == NIL
                    // || !find_dependent_phvs).
                    let varno = get_result_relid(parse, rarg);
                    if varno != 0
                        && (slot.node.is_none()
                            || !crate::prepjointree::find_dependent_phvs(run, parse, varno)?)
                    {
                        crate::prepjointree::remove_result_refs(run, parse, varno, larg)?;
                        dropped_outer_joins.add_member(mcx, j.rtindex)?;
                        return Ok(Some(larg));
                    }
                }
                JoinType::JOIN_SEMI => {
                    if get_result_relid(parse, rarg) != 0 {
                        debug_assert_eq!(j.rtindex, 0);
                        crate::prepjointree::remove_result_refs(
                            run,
                            parse,
                            get_result_relid(parse, rarg),
                            larg,
                        )?;
                        if slot.node.is_some() && parent_quals.is_none() {
                            return Ok(Some(Node::mk(
                                mcx,
                                FromExpr {
                                    fromlist: NodeList::make1(mcx, larg)?,
                                    quals: slot.node,
                                },
                            )?));
                        }
                        if let Some(p) = parent_quals {
                            merge_quals(mcx, slot.node, p)?;
                        }
                        return Ok(Some(larg));
                    }
                }
                JoinType::JOIN_FULL | JoinType::JOIN_ANTI => {}
                other => panic!(
                    "remove_useless_results_recurse (prepjointree.c): join type {other:?}"
                ),
            }
            if lres.is_none() && rres.is_none() && !slot.changed {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: slot.node,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )?))
        }
        other => panic!("remove_useless_results_recurse (prepjointree.c): {other:?}"),
    }
}

// preprocess_rowmarks (planner.c); UPDATE/DELETE non-target marks stay loud.
pub fn preprocess_rowmarks<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
) -> PgResult<()> {
    use types_nodes::plannodes::PlanRowMark;

    if !parse.rowMarks.is_nil() {
        parser_analyze::CheckSelectLocking(
            parse,
            parse
                .rowMarks
                .nth(0)
                .as_row_mark_clause()
                .expect("rowMarks cell")
                .strength,
        )?;
    } else {
        match parse.commandType {
            CmdType::CMD_SELECT | CmdType::CMD_INSERT => return Ok(()),
            // C adds non-locking ROW_MARK_REFERENCE/COPY marks for every
            // non-target rel (junk ctid/wholerow columns via the preptlist
            // rowmark stanza). The marks flow into root.rowMarks and the
            // subplan tlist; DIVERGENCE: they stop at the planner — the EPQ
            // recheck rescans the source under the same snapshot instead of
            // re-fetching by mark; identical results unless several source
            // rows join the same rechecked target row.
            CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE => {}
            other => panic!("preprocess_rowmarks (planner.c): {other:?} rowmarks; M2 DML lane"),
        }
    }

    let mcx = run.mcx;
    let mut rels = types_nodes::bitmapset::Bitmapset::empty();
    collect_jointree_relids(mcx, parse.jointree.expect("jointree is a FromExpr"), &mut rels)?;
    rels.del_member(parse.resultRelation);

    for rc_node in &parse.rowMarks {
        let rc = rc_node.as_row_mark_clause().expect("rowMarks cell");
        let rte = parse
            .rtable
            .nth(rc.rti as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        debug_assert!(rc.rti != parse.resultRelation as u32);
        if rte.rtekind != RTEKind::RTE_RELATION {
            continue;
        }
        rels.del_member(rc.rti as i32);
        run.glob.last_row_mark_id += 1;
        let mark_type = select_rowmark_type(rte, rc.strength);
        let id = run.add_rowmark(PlanRowMark {
            rti: rc.rti,
            prti: rc.rti,
            rowmarkId: run.glob.last_row_mark_id,
            markType: mark_type,
            allMarkTypes: 1 << mark_type as i32,
            strength: rc.strength,
            waitPolicy: rc.waitPolicy,
            isParent: false,
        });
        run.root.rowMarks.push(id);
    }

    for (idx, rte_node) in parse.rtable.iter().enumerate() {
        let i = idx as u32 + 1;
        if !rels.is_member(i as i32) {
            continue;
        }
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        run.glob.last_row_mark_id += 1;
        let mark_type =
            select_rowmark_type(rte, types_nodes::LockClauseStrength::LCS_NONE);
        let id = run.add_rowmark(PlanRowMark {
            rti: i,
            prti: i,
            rowmarkId: run.glob.last_row_mark_id,
            markType: mark_type,
            allMarkTypes: 1 << mark_type as i32,
            strength: types_nodes::LockClauseStrength::LCS_NONE,
            waitPolicy: types_nodes::LockWaitPolicy::LockWaitBlock,
            isParent: false,
        });
        run.root.rowMarks.push(id);
    }
    Ok(())
}

// preptlist.c rowmark stanza: junk ctid (+ parent tableoid) columns.
fn add_rowmark_junk_columns<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    mut tlist: NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    use types_nodes::plannodes::RowMarkType;
    for &id in run.root.rowMarks.iter() {
        let rc = *run.rowmark(id);
        if rc.rti != rc.prti {
            continue;
        }
        if rc.allMarkTypes & !(1 << RowMarkType::ROW_MARK_COPY as i32) != 0 {
            let var = Node::mk_var(
                mcx,
                rc.rti as i32,
                types_tuple::htup::SelfItemPointerAttributeNumber as i16,
                types_core::catalog::TIDOID,
                -1,
                0,
                0,
            )?;
            let resname = arena_str(mcx, &format!("ctid{}", rc.rowmarkId))?;
            let tle =
                Node::mk_target_entry(mcx, var, tlist.len() as i16 + 1, Some(resname), true)?;
            tlist.lappend(mcx, tle)?;
        }
        if rc.allMarkTypes & (1 << RowMarkType::ROW_MARK_COPY as i32) != 0 {
            // makeWholeRowVar (makefuncs.c): named composite for relations
            // and view-expanded subqueries; for single-function RTEs (and
            // subqueries expanded from one) the function's composite result
            // type; RECORD otherwise. allowScalar is always false on this
            // planner path (C preptlist.c passes false).
            let rte = run
                .parse()
                .rtable
                .nth(rc.rti as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable cell");
            let func_rowtype = |functions: &NodeList<'mcx>| -> PgResult<types_core::Oid> {
                let fexpr = functions
                    .nth(0)
                    .as_range_tbl_function()
                    .expect("RangeTblFunction")
                    .funcexpr
                    .expect("RangeTblFunction.funcexpr");
                let toid = nodes_core::expr_type(fexpr);
                Ok(if lsyscache::typ::type_is_rowtype(toid)? {
                    toid
                } else {
                    types_core::catalog::RECORDOID
                })
            };
            let vartype = match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    let toid = lsyscache::get_rel_type_id(rte.relid)?;
                    assert!(toid != 0, "relation without a composite type");
                    toid
                }
                RTEKind::RTE_SUBQUERY if rte.relid != 0 => {
                    let toid = lsyscache::get_rel_type_id(rte.relid)?;
                    assert!(toid != 0, "relation without a composite type");
                    toid
                }
                // Subquery expanded from a single set-returning function.
                RTEKind::RTE_SUBQUERY if !rte.functions.is_nil() => func_rowtype(&rte.functions)?,
                RTEKind::RTE_FUNCTION => {
                    if rte.funcordinality || rte.functions.len() != 1 {
                        types_core::catalog::RECORDOID
                    } else {
                        func_rowtype(&rte.functions)?
                    }
                }
                _ => types_core::catalog::RECORDOID,
            };
            let var = Node::mk_var(mcx, rc.rti as i32, 0, vartype, -1, 0, 0)?;
            let resname = arena_str(mcx, &format!("wholerow{}", rc.rowmarkId))?;
            let tle =
                Node::mk_target_entry(mcx, var, tlist.len() as i16 + 1, Some(resname), true)?;
            tlist.lappend(mcx, tle)?;
        }
        if rc.isParent {
            let var = Node::mk_var(
                mcx,
                rc.rti as i32,
                types_tuple::htup::TableOidAttributeNumber as i16,
                types_core::catalog::OIDOID,
                -1,
                0,
                0,
            )?;
            let resname = arena_str(mcx, &format!("tableoid{}", rc.rowmarkId))?;
            let tle =
                Node::mk_target_entry(mcx, var, tlist.len() as i16 + 1, Some(resname), true)?;
            tlist.lappend(mcx, tle)?;
        }
    }
    Ok(tlist)
}

pub(crate) fn arena_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// get_relids_in_jointree (prepjointree.c), include_outer_joins=false shape.
fn collect_jointree_relids<'mcx>(
    mcx: Mcx<'mcx>,
    f: &types_nodes::primnodes::FromExpr<'mcx>,
    out: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    fn walk<'mcx>(
        mcx: Mcx<'mcx>,
        node: types_nodes::Node<'mcx>,
        out: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
    ) -> PgResult<()> {
        if let Some(rtr) = node.as_range_tbl_ref() {
            out.add_member(mcx, rtr.rtindex)?;
        } else if let Some(f) = node.as_from_expr() {
            // Nested FromExpr: transform_MERGE_to_join wraps the target in
            // one as the join's larg.
            for child in &f.fromlist {
                walk(mcx, child, out)?;
            }
        } else if let Some(j) = node.as_join_expr() {
            walk(mcx, j.larg, out)?;
            walk(mcx, j.rarg, out)?;
        } else {
            panic!(
                "get_relids_in_jointree (prepjointree.c): {:?} jointree node",
                node.node_tag()
            );
        }
        Ok(())
    }
    for child in &f.fromlist {
        walk(mcx, child, out)?;
    }
    Ok(())
}

// select_rowmark_type (planner.c).
pub fn select_rowmark_type(
    rte: &RangeTblEntry<'_>,
    strength: types_nodes::LockClauseStrength,
) -> types_nodes::plannodes::RowMarkType {
    use types_nodes::plannodes::RowMarkType::*;
    use types_nodes::LockClauseStrength::*;
    if rte.rtekind != RTEKind::RTE_RELATION {
        return ROW_MARK_COPY;
    }
    // C lets the FDW's GetForeignRowMarkType override; no in-tree FDW
    // installs one, so this is always C's ROW_MARK_COPY default.
    if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        return ROW_MARK_COPY;
    }
    match strength {
        LCS_NONE => ROW_MARK_REFERENCE,
        LCS_FORKEYSHARE => ROW_MARK_KEYSHARE,
        LCS_FORSHARE => ROW_MARK_SHARE,
        LCS_FORNOKEYUPDATE => ROW_MARK_NOKEYEXCLUSIVE,
        LCS_FORUPDATE => ROW_MARK_EXCLUSIVE,
    }
}

// SELECT arm shares the parse targetList (as C); the INSERT arm NULL-fills
// missing columns (expand_insert_targetlist). UPDATE/DELETE/MERGE row-identity
// lanes are loud.
pub fn preprocess_targetlist<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    if parse.resultRelation == 0 {
        debug_assert!(parse.commandType == CmdType::CMD_SELECT);
        if run.root.rowMarks.is_empty() {
            run.processed_tlist = Some(&parse.targetList);
            return Ok(());
        }
        let tlist = add_rowmark_junk_columns(mcx, run, parse.targetList.clone_in(mcx)?)?;
        run.processed_tlist = Some(mcx::leak_in(mcx::alloc_in(mcx, tlist)?));
        return Ok(());
    }
    let command_type = parse.commandType;
    let rte = parse
        .rtable
        .nth(parse.resultRelation as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
    let mut tlist = match command_type {
        CmdType::CMD_INSERT => expand_insert_targetlist(mcx, &parse.targetList, &rel)?,
        _ => {
            if command_type == CmdType::CMD_UPDATE {
                run.root.update_colnos =
                    extract_update_targetlist_colnos(mcx, &parse.targetList);
            }
            if rte.inh {
                // Inherited target: row identity is registered per leaf by
                // expand_single_inheritance_child, or by
                // distribute_row_identity_vars when every leaf is excluded.
                parse.targetList.clone_in(mcx)?
            } else {
                add_row_identity_columns(mcx, run, &parse.targetList, parse.resultRelation, &rel)?
            }
        }
    };
    if command_type == CmdType::CMD_MERGE {
        let result_relation = parse.resultRelation;
        for action_node in &parse.mergeActionList {
            let action = action_node.as_merge_action().expect("mergeActionList cell");
            match action.commandType {
                CmdType::CMD_INSERT => {
                    let expanded = expand_insert_targetlist(mcx, &action.targetList, &rel)?;
                    // SAFETY: parse tree is planner-owned; no derived refs live.
                    unsafe {
                        action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                            a.targetList = expanded;
                        })
                    }
                    .expect("MergeAction");
                }
                CmdType::CMD_UPDATE => {
                    let colnos = extract_update_targetlist_colnos(mcx, &action.targetList);
                    let mut il = types_nodes::IntList::nil();
                    for &c in colnos.iter() {
                        il.lappend(mcx, c as i32)?;
                    }
                    // SAFETY: as above.
                    unsafe {
                        action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                            a.updateColnos = il;
                        })
                    }
                    .expect("MergeAction");
                }
                _ => {}
            }
            let action = action_node.as_merge_action().expect("mergeActionList cell");
            if let Some(q) = action.qual {
                add_merge_junk_vars(mcx, &mut tlist, q, result_relation)?;
            }
            for tle in &action.targetList {
                add_merge_junk_vars(mcx, &mut tlist, tle, result_relation)?;
            }
        }
        if let Some(jc) = parse.mergeJoinCondition {
            add_merge_junk_vars(mcx, &mut tlist, jc, result_relation)?;
        }
    }
    table::table_close(rel, types_rel::NoLock)?;
    // Junk ctid/tableoid columns for rowmarked rels (non-target auto-marks;
    // C's rowMarks stanza runs before the RETURNING stanza).
    if !run.root.rowMarks.is_empty() {
        tlist = add_rowmark_junk_columns(mcx, run, tlist)?;
    }
    // Resjunk entries for RETURNING Vars of OTHER relations (the MERGE source
    // or, once join DML lands, UPDATE/DELETE FROM/USING rels).
    if !parse.returningList.is_nil() && parse.rtable.len() > 1 {
        let result_relation = parse.resultRelation;
        for tle_node in &parse.returningList {
            add_merge_junk_vars(mcx, &mut tlist, tle_node, result_relation)?;
        }
    }
    run.processed_tlist = Some(mcx::leak_in(mcx::alloc_in(mcx, tlist)?));
    Ok(())
}

// extract_update_targetlist_colnos (preptlist.c): collect the target column
// numbers, then renumber the shared TLEs consecutively (C mutates in place).
pub(crate) fn extract_update_targetlist_colnos<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'mcx>,
) -> mcx::PgVec<'mcx, i16> {
    let mut update_colnos: mcx::PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    let mut nextresno: i16 = 1;
    for tle_node in tlist {
        let resjunk = tle_node.as_target_entry().expect("tlist cell").resjunk;
        if !resjunk {
            update_colnos.push(tle_node.as_target_entry().unwrap().resno);
        }
        let resno = nextresno;
        nextresno += 1;
        // SAFETY: exclusive planner ownership of the preprocessed tlist.
        unsafe {
            tle_node.with_mut::<types_nodes::TargetEntry, _>(|t| t.resno = resno)
        }
        .expect("TargetEntry");
    }
    update_colnos
}

// add_row_identity_columns + add_row_identity_var (appendinfo.c), the
// non-inherited plain-table leg: append the junk ctid Var to the tlist.
// The MERGE junk-var stanza of preprocess_targetlist (preptlist.c): resjunk
// tlist entries for non-target Vars used in action quals/targetlists and the
// join condition.
fn add_merge_junk_vars<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &mut NodeList<'mcx>,
    node: Node<'mcx>,
    result_relation: i32,
) -> PgResult<()> {
    let vars = vars::pull_var_clause(mcx, node, vars::PVC_INCLUDE_PLACEHOLDERS)?;
    'next_var: for var_node in &vars {
        if var_node.as_var().is_some_and(|v| v.varno == result_relation) {
            continue;
        }
        for tle_node in tlist.iter() {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            if types_nodes::equal(tle.expr, var_node) {
                continue 'next_var;
            }
        }
        let tle =
            Node::mk_target_entry(mcx, var_node, tlist.len() as i16 + 1, None, true)?;
        tlist.lappend(mcx, tle)?;
    }
    Ok(())
}

fn add_row_identity_columns<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
    result_relation: i32,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    // Non-target auto rowmarks (preprocess_rowmarks) coexist with the row
    // identity; C's add_row_identity_columns has no rowMarks interaction.
    if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        // C's default wholerow arm (no in-tree FDW installs
        // AddForeignUpdateTargets); execution errors at CheckValidResultRel.
        let var =
            Node::mk_var(mcx, result_relation, 0, types_core::catalog::RECORDOID, -1, 0, 0)?;
        let mut new_tlist = tlist.clone_in(mcx)?;
        let tle =
            Node::mk_target_entry(mcx, var, new_tlist.len() as i16 + 1, Some("wholerow"), true)?;
        new_tlist.lappend(mcx, tle)?;
        return Ok(new_tlist);
    }
    if rel.rd_rel.relkind != types_rel::RELKIND_RELATION
        && rel.rd_rel.relkind != types_rel::RELKIND_MATVIEW
        && rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE
    {
        // Other relkinds (views, toast tables, sequences) fall through with
        // no row identity; C adds nothing and any error surfaces later
        // (e.g. the executor permission check for toast-table DML).
        return tlist.clone_in(mcx);
    }
    let var = Node::mk_var(
        mcx,
        result_relation,
        types_tuple::htup::SelfItemPointerAttributeNumber as i16,
        types_core::catalog::TIDOID,
        -1,
        0,
        0,
    )?;
    let mut new_tlist = tlist.clone_in(mcx)?;
    let tle = Node::mk_target_entry(
        mcx,
        var,
        new_tlist.len() as i16 + 1,
        Some("ctid"),
        true,
    )?;
    new_tlist.lappend(mcx, tle)?;
    Ok(new_tlist)
}

// expand_insert_targetlist (preptlist.c): produce one entry per attribute in
// attno order, NULL Consts for unassigned columns. Domain columns get
// coerce_null_to_domain's CoerceToDomain wrapper.
fn expand_insert_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut new_tlist = NodeList::nil();
    let mut tlist_iter = tlist.iter().peekable();
    let numattrs = rel.rd_att.natts;
    for attrno in 1..=numattrs {
        let att = rel.rd_att.attr(attrno as usize - 1);
        let mut new_tle = None;
        if let Some(&tle_node) = tlist_iter.peek() {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            if !tle.resjunk && tle.resno == attrno as i16 {
                new_tle = Some(tle_node);
                tlist_iter.next();
            }
        }
        let tle_node = match new_tle {
            Some(t) => t,
            None => {
                let new_expr = if att.attgenerated != 0 {
                    // preptlist.c:455-468: NULL of the domain's base type, no
                    // CoerceToDomain (the executor overwrites stored values).
                    let mut base_typmod = att.atttypmod;
                    let base_typid =
                        lsyscache::typ::getBaseTypeAndTypmod(att.atttypid, &mut base_typmod)?;
                    Node::mk_const(
                        mcx,
                        base_typid,
                        base_typmod,
                        att.attcollation,
                        att.attlen as i32,
                        datum::Datum::null(),
                        true,
                        att.attbyval,
                    )?
                } else if !att.attisdropped {
                    let e = coerce::coerce_null_to_domain(
                        mcx,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        att.attlen as i32,
                        att.attbyval,
                    )?;
                    if e.node_tag() == NodeTag::T_Const {
                        e
                    } else {
                        clauses::eval_const_expressions(mcx, e)?
                    }
                } else {
                    Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::null(), true, true)?
                };
                let name =
                    core::str::from_utf8(att.attname.name_str()).expect("attname is UTF-8");
                let name = mcx::slice_borrow_in(mcx, name.as_bytes())?;
                // SAFETY: byte-for-byte copy of a &str.
                let name = unsafe { core::str::from_utf8_unchecked(name) };
                Node::mk_target_entry(mcx, new_expr, attrno as i16, Some(name), false)?
            }
        };
        new_tlist.lappend(mcx, tle_node)?;
    }
    for tle_node in tlist_iter {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        assert!(tle.resjunk, "targetlist is not sorted correctly");
        panic!(
            "expand_insert_targetlist (preptlist.c): junk tlist entries; M4 lane"
        );
    }
    Ok(new_tlist)
}
