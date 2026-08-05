// transformRuleStmt (parse_utilcmd.c): CREATE RULE parse analysis. OLD is
// varno 1, NEW is varno 2 (PRS2 invariant).
#![allow(non_snake_case)]

use mcx::Mcx;
use parser_small1::{make_parsestate, ParseExprKind};
use rewrite_manip::{PRS2_NEW_VARNO, PRS2_OLD_VARNO};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_OBJECT_DEFINITION,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RangeTblEntry};
use types_nodes::primnodes::{Alias, FromExpr, RangeTblRef};
use types_nodes::rawnodes::RuleStmt;
use types_nodes::{Node, NodeList, NodeTag};
use types_rel::{AccessExclusiveLock, AccessShareLock, NoLock, RELKIND_MATVIEW};

pub fn transformRuleStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RuleStmt<'mcx>,
    query_string: &str,
) -> PgResult<(NodeList<'mcx>, Option<Node<'mcx>>)> {
    let rvn = stmt.relation.expect("CREATE RULE has a relation");
    let rv = rel_vocab::RangeVar {
        catalogname: rvn.catalogname,
        schemaname: rvn.schemaname,
        relname: rvn.relname.expect("grammar always sets relname"),
        inh: rvn.inh,
        relpersistence: rvn.relpersistence,
        location: rvn.location,
    };
    let rel = table::table_openrv(mcx, &rv, AccessExclusiveLock)?;
    if rel.rd_rel.relkind == RELKIND_MATVIEW {
        return Err(feature_not_supported("rules on materialized views are not supported"));
    }

    let old_alias = mcx::alloc_leak_in(
        mcx,
        Alias { aliasname: Some("old"), colnames: NodeList::nil() },
    )?;
    let new_alias = mcx::alloc_leak_in(
        mcx,
        Alias { aliasname: Some("new"), colnames: NodeList::nil() },
    )?;

    let src = mcx::slice_borrow_in(mcx, query_string.as_bytes())?;
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(src);
    let oldnsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        &rel,
        AccessShareLock,
        Some(old_alias),
        false,
        false,
    )?;
    let newnsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        &rel,
        AccessShareLock,
        Some(new_alias),
        false,
        false,
    )?;
    let old_rtindex = oldnsitem.p_rtindex;
    debug_assert_eq!(old_rtindex, PRS2_OLD_VARNO);
    debug_assert_eq!(newnsitem.p_rtindex, PRS2_NEW_VARNO);

    match stmt.event {
        CmdType::CMD_SELECT => {
            parse_relation::addNSItemToQuery(mcx, &mut pstate, oldnsitem, false, true, true)?;
        }
        CmdType::CMD_UPDATE => {
            parse_relation::addNSItemToQuery(mcx, &mut pstate, oldnsitem, false, true, true)?;
            parse_relation::addNSItemToQuery(mcx, &mut pstate, newnsitem, false, true, true)?;
        }
        CmdType::CMD_INSERT => {
            parse_relation::addNSItemToQuery(mcx, &mut pstate, newnsitem, false, true, true)?;
        }
        CmdType::CMD_DELETE => {
            parse_relation::addNSItemToQuery(mcx, &mut pstate, oldnsitem, false, true, true)?;
        }
        other => panic!("unrecognized event type: {other:?}"),
    }

    let where_clause = parse_clause::transformWhereClause(
        mcx,
        &mut pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;
    if let Some(w) = where_clause {
        parse_collate::assign_expr_collations(mcx, &pstate, w)?;
    }

    if pstate.p_rtable.len() != 2 {
        return Err(Box::new(
            PgError::error("rule WHERE condition cannot contain references to other relations")
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    let mut actions = NodeList::nil();
    if stmt.actions.is_nil() {
        let nothing_qry = Query {
            commandType: CmdType::CMD_NOTHING,
            rtable: pstate.p_rtable.clone_in(mcx)?,
            rteperminfos: pstate.p_rteperminfos.clone_in(mcx)?,
            jointree: Some(mcx::alloc_leak_in(
                mcx,
                FromExpr { fromlist: NodeList::nil(), quals: None },
            )?),
            ..Default::default()
        };
        actions.lappend(mcx, Node::mk(mcx, nothing_qry)?)?;
    } else {
        for action in &stmt.actions {
            let mut sub_pstate = make_parsestate(mcx, None);
            sub_pstate.p_sourcetext = Some(src);

            let sub_old = parse_relation::addRangeTableEntryForRelation(
                mcx,
                &mut sub_pstate,
                &rel,
                AccessShareLock,
                Some(old_alias),
                false,
                false,
            )?;
            let sub_new = parse_relation::addRangeTableEntryForRelation(
                mcx,
                &mut sub_pstate,
                &rel,
                AccessShareLock,
                Some(new_alias),
                false,
                false,
            )?;
            let sub_old_rtindex = sub_old.p_rtindex;
            parse_relation::addNSItemToQuery(mcx, &mut sub_pstate, sub_old, false, true, false)?;
            parse_relation::addNSItemToQuery(mcx, &mut sub_pstate, sub_new, false, true, false)?;

            let top_subqry = crate::transformStmt(mcx, &mut sub_pstate, action)?;

            if top_subqry.commandType == CmdType::CMD_UTILITY && where_clause.is_some() {
                return Err(Box::new(
                    PgError::error(
                        "rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions",
                    )
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }

            let top_node = Node::mk(mcx, top_subqry)?;
            let top_q = top_node.as_query().expect("Query");
            let sub_qry = rewrite_manip::getInsertSelectQuery_ref(top_q)?;

            if sub_qry.setOperations.is_some() && where_clause.is_some() {
                return Err(feature_not_supported(
                    "conditional UNION/INTERSECT/EXCEPT statements are not implemented",
                ));
            }

            let has_old = rewrite_manip::rangeTableEntry_used_query(sub_qry, PRS2_OLD_VARNO, 0)?
                || rewrite_manip::rangeTableEntry_used_opt(where_clause, PRS2_OLD_VARNO, 0)?;
            let has_new = rewrite_manip::rangeTableEntry_used_query(sub_qry, PRS2_NEW_VARNO, 0)?
                || rewrite_manip::rangeTableEntry_used_opt(where_clause, PRS2_NEW_VARNO, 0)?;

            match stmt.event {
                CmdType::CMD_SELECT => {
                    if has_old {
                        return Err(invalid_object("ON SELECT rule cannot use OLD"));
                    }
                    if has_new {
                        return Err(invalid_object("ON SELECT rule cannot use NEW"));
                    }
                }
                CmdType::CMD_UPDATE => {}
                CmdType::CMD_INSERT => {
                    if has_old {
                        return Err(invalid_object("ON INSERT rule cannot use OLD"));
                    }
                }
                CmdType::CMD_DELETE => {
                    if has_new {
                        return Err(invalid_object("ON DELETE rule cannot use NEW"));
                    }
                }
                other => panic!("unrecognized event type: {other:?}"),
            }

            if rewrite_manip::rangeTableEntry_used_list(&top_q.cteList, PRS2_OLD_VARNO, 0)?
                || rewrite_manip::rangeTableEntry_used_list(&sub_qry.cteList, PRS2_OLD_VARNO, 0)?
            {
                return Err(feature_not_supported("cannot refer to OLD within WITH query"));
            }
            if rewrite_manip::rangeTableEntry_used_list(&top_q.cteList, PRS2_NEW_VARNO, 0)?
                || rewrite_manip::rangeTableEntry_used_list(&sub_qry.cteList, PRS2_NEW_VARNO, 0)?
            {
                return Err(feature_not_supported("cannot refer to NEW within WITH query"));
            }

            // OLD joins the action's FROM only when referenced; NEW rides the
            // targetlist (INSERT) or is another reference to OLD (UPDATE).
            if has_old || (has_new && stmt.event == CmdType::CMD_UPDATE) {
                if sub_qry.setOperations.is_some() {
                    return Err(feature_not_supported(
                        "conditional UNION/INTERSECT/EXCEPT statements are not implemented",
                    ));
                }
                let rtr = Node::mk(mcx, RangeTblRef { rtindex: sub_old_rtindex })?;
                append_to_jointree(mcx, top_node, sub_qry, rtr)?;
            }

            actions.lappend(mcx, top_node)?;
        }
    }

    table::table_close(rel, NoLock)?;
    Ok((actions, where_clause))
}

// sub_qry->jointree->fromlist = lappend(...). The effective sub-query is
// either top_node's own Query (mutable via the node) or an INSERT ... SELECT
// subquery reachable only as &Query — that arm rebuilds the subquery.
fn append_to_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    top_node: Node<'mcx>,
    sub_qry: &Query<'mcx>,
    rtr: Node<'mcx>,
) -> PgResult<()> {
    let jt = sub_qry.jointree.expect("analyzed query has a jointree");
    let mut fromlist = jt.fromlist.clone_in(mcx)?;
    fromlist.lappend(mcx, rtr)?;
    let new_jt = mcx::alloc_leak_in(mcx, FromExpr { fromlist, quals: jt.quals })?;

    let top_q = top_node.as_query().expect("Query");
    match rewrite_manip::getInsertSelectQuery_parts(top_q)? {
        None => {
            // SAFETY: statement-owned tree fresh out of parse analysis.
            unsafe { top_node.with_mut::<Query, _>(|q| q.jointree = Some(new_jt)) }
                .expect("Query");
        }
        Some((rte_node, sub)) => {
            let mut new_sub = rewrite_manip::flat_copy_query(mcx, sub)?;
            new_sub.jointree = Some(new_jt);
            let new_sub_ref = mcx::alloc_leak_in(mcx, new_sub)?;
            debug_assert!(rte_node.node_tag() == NodeTag::T_RangeTblEntry);
            // SAFETY: as above.
            unsafe {
                rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(new_sub_ref))
            }
            .expect("RangeTblEntry");
        }
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_object(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION))
}

// contain_vars_of_level's walker (var.c), shared with transformValuesClause's
// CREATE RULE lateral arm.
pub(crate) struct VarsOfLevel {
    pub(crate) sublevels_up: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for VarsOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                Ok(node.as_var().expect("Var").varlevelsup == self.sublevels_up)
            }
            NodeTag::T_CurrentOfExpr => Ok(self.sublevels_up == 0),
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r = nodes_core::query_tree_walker(
                    node.as_query().expect("Query"),
                    self,
                    0,
                )?;
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
