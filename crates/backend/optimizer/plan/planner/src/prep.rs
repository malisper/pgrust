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

// A lone RangeTblRef under the top FromExpr can never be elided or dropped.
pub fn remove_useless_result_rtes(run: &PlannerRun<'_>, parse: &Query<'_>) {
    let f = parse.jointree.expect("top jointree is a FromExpr");
    // A FromExpr of plain RangeTblRefs has no RTE_RESULT children to remove
    // (RTE_RESULT only enters via replace_empty_jointree's single-item form).
    if f.fromlist.iter().all(|n| n.node_tag() == NodeTag::T_RangeTblRef) {
        let _ = run;
        return;
    }
    panic!(
        "remove_useless_results_recurse (prepjointree.c): non-trivial jointree; \
         M2 join lane"
    );
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
            CmdType::CMD_UPDATE | CmdType::CMD_DELETE => {
                let f = parse.jointree.expect("jointree is a FromExpr");
                for child in &f.fromlist {
                    let rtr = child.as_range_tbl_ref().unwrap_or_else(|| {
                        panic!("preprocess_rowmarks: non-RTR jointree; M2 join lane")
                    });
                    if rtr.rtindex != parse.resultRelation {
                        panic!(
                            "preprocess_rowmarks (planner.c): non-target rel marks \
                             (ROW_MARK_REFERENCE); M2 join lane"
                        );
                    }
                }
                return Ok(());
            }
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
            panic!(
                "preprocess_targetlist (preptlist.c): ROW_MARK_COPY wholerow junk \
                 var (makeWholeRowVar); non-relation rowmark lane"
            );
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

fn arena_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
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

// select_rowmark_type (planner.c); the FDW arm is loud.
pub fn select_rowmark_type(
    rte: &RangeTblEntry<'_>,
    strength: types_nodes::LockClauseStrength,
) -> types_nodes::plannodes::RowMarkType {
    use types_nodes::plannodes::RowMarkType::*;
    use types_nodes::LockClauseStrength::*;
    if rte.rtekind != RTEKind::RTE_RELATION {
        return ROW_MARK_COPY;
    }
    if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        panic!("select_rowmark_type (planner.c): GetForeignRowMarkType; FDW lane");
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
    debug_assert!(run.root.rowMarks.is_empty());
    let command_type = parse.commandType;
    if command_type == CmdType::CMD_MERGE {
        panic!("preprocess_targetlist (preptlist.c): MERGE action lists; M4 MERGE lane");
    }
    let rte = parse
        .rtable
        .nth(parse.resultRelation as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
    let tlist = match command_type {
        CmdType::CMD_INSERT => expand_insert_targetlist(mcx, &parse.targetList, &rel)?,
        _ => {
            if command_type == CmdType::CMD_UPDATE {
                run.root.update_colnos =
                    extract_update_targetlist_colnos(mcx, &parse.targetList);
            }
            debug_assert!(!rte.inh);
            add_row_identity_columns(mcx, run, &parse.targetList, parse.resultRelation, &rel)?
        }
    };
    table::table_close(rel, types_rel::NoLock)?;
    // C adds resjunk entries for RETURNING Vars of OTHER relations; the join
    // DML lane is loud upstream, so only result-rel Vars can appear here.
    if !parse.returningList.is_nil() && parse.rtable.len() > 1 {
        let result_relation = parse.resultRelation;
        for tle_node in &parse.returningList {
            let vars = vars::pull_var_clause(
                mcx,
                tle_node,
                vars::PVC_RECURSE_AGGREGATES
                    | vars::PVC_RECURSE_WINDOWFUNCS
                    | vars::PVC_INCLUDE_PLACEHOLDERS,
            )?;
            for var_node in &vars {
                if var_node.as_var().unwrap().varno != result_relation {
                    panic!(
                        "preprocess_targetlist (preptlist.c): RETURNING Var of a \
                         non-result relation (resjunk tlist addition); M2 join lane"
                    );
                }
            }
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
fn add_row_identity_columns<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
    result_relation: i32,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    debug_assert!(run.root.rowMarks.is_empty());
    if rel.rd_rel.relkind != types_rel::RELKIND_RELATION {
        panic!(
            "add_row_identity_columns (appendinfo.c): relkind '{}' (wholerow/FDW \
             row identity); M4 lane",
            rel.rd_rel.relkind as char
        );
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
// attno order, NULL Consts for unassigned columns. Domain columns need
// coerce_null_to_domain's CoerceToDomain wrapper — loud.
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
                let new_expr = if !att.attisdropped {
                    debug_assert!(att.attgenerated == 0);
                    if lsyscache::typ::getBaseType(att.atttypid)? != att.atttypid {
                        panic!(
                            "expand_insert_targetlist (preptlist.c): \
                             coerce_null_to_domain (CoerceToDomain wrapper); M4 domain lane"
                        );
                    }
                    Node::mk_const(
                        mcx,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        att.attlen as i32,
                        datum::Datum::null(),
                        true,
                        att.attbyval,
                    )?
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
