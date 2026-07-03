#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use relcache::rules::RewriteRuleMeta;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION};
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry};
use types_nodes::NodeTag;
use types_rel::{
    AccessShareLock, NoLock, Relation, RowShareLock, LOCKMODE, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};

#[cfg(test)]
mod tests;

pub fn init_seams() {
    rewrite_handler_seams::query_rewrite::set(QueryRewrite);
}

pub fn QueryRewrite<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    debug_assert_eq!(parsetree.querySource, QuerySource::QSRC_ORIGINAL);
    debug_assert!(parsetree.canSetTag);

    let input_query_id = parsetree.queryId;
    let orig_cmd_type = parsetree.commandType;

    let mut results = RewriteQuery(mcx, parsetree)?;

    for query in results.iter_mut() {
        let mut active_rirs: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        fireRIRrules(mcx, query, &mut active_rirs)?;
        query.queryId = input_query_id;
    }

    let mut found_original = false;
    let mut last_instead: Option<usize> = None;
    for (i, query) in results.iter().enumerate() {
        if query.querySource == QuerySource::QSRC_ORIGINAL {
            debug_assert!(query.canSetTag);
            debug_assert!(!found_original);
            found_original = true;
        } else {
            debug_assert!(!query.canSetTag);
            if query.commandType == orig_cmd_type
                && matches!(
                    query.querySource,
                    QuerySource::QSRC_INSTEAD_RULE | QuerySource::QSRC_QUAL_INSTEAD_RULE
                )
            {
                last_instead = Some(i);
            }
        }
    }
    if !found_original {
        if let Some(i) = last_instead {
            results[i].canSetTag = true;
        }
    }

    Ok(results)
}

fn RewriteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    let event = parsetree.commandType;

    if !parsetree.cteList.is_nil() {
        panic!(
            "RewriteQuery (rewriteHandler.c): WITH-clause rewrite needs CommonTableExpr \
             (types_nodes parsenodes unported)"
        );
    }

    match event {
        CmdType::CMD_SELECT | CmdType::CMD_UTILITY => {}
        CmdType::CMD_INSERT => rewrite_insert_query(mcx, &mut parsetree)?,
        CmdType::CMD_UPDATE | CmdType::CMD_DELETE => {
            rewrite_update_delete_query(mcx, &mut parsetree)?
        }
        other => panic!(
            "RewriteQuery (rewriteHandler.c): {other:?} rewrite needs the \
             mergeActionList arm (MERGE vocab unported)"
        ),
    }

    let mut rewritten = mcx::vec_with_capacity_in(mcx, 1)?;
    rewritten.push(parsetree);
    Ok(rewritten)
}

// The CMD_INSERT arm of RewriteQuery's DML block: adjust the targetlist, then
// fire INSERT rules. The trimmed relcache entry has no rd_rules, so a table
// carrying user CREATE RULE rules is undetectable until pg_rewrite lands
// (matchLocks = NIL in a stock initdb; same divergence as fireRIRrules).
fn rewrite_insert_query<'mcx>(mcx: Mcx<'mcx>, parsetree: &mut Query<'mcx>) -> PgResult<()> {
    let result_relation = parsetree.resultRelation;
    debug_assert!(result_relation != 0);
    let rt_entry = rte_of(parsetree.rtable.nth(result_relation as usize - 1));
    debug_assert!(rt_entry.rtekind == RTEKind::RTE_RELATION);

    let rel = table::table_open(mcx, rt_entry.relid, NoLock)?;
    if rel.rd_rel.relkind == RELKIND_VIEW {
        panic!(
            "RewriteQuery (rewriteHandler.c): auto-updatable view INSERT needs \
             rewriteTargetView (pg_rewrite vocab unported)"
        );
    }

    let mut values_rte = None;
    let jointree = parsetree.jointree.expect("INSERT jointree is a FromExpr");
    for rtr_node in &jointree.fromlist {
        if let Some(rtr) = rtr_node.as_range_tbl_ref() {
            let rte = rte_of(parsetree.rtable.nth(rtr.rtindex as usize - 1));
            if rte.rtekind == RTEKind::RTE_VALUES {
                debug_assert!(values_rte.is_none(), "more than one VALUES RTE found");
                values_rte = Some((rte, rtr.rtindex));
            }
        }
    }

    parsetree.targetList = rewriteTargetListIU(
        mcx,
        &parsetree.targetList,
        CmdType::CMD_INSERT,
        parsetree.r#override,
        &rel,
        values_rte,
    )?;

    if let Some((rte, _)) = values_rte {
        // rewriteValuesRTE only rewrites SetToDefault cells; their
        // construction is loud upstream (transformAssignedExpr).
        for row in &rte.values_lists {
            let row = row.as_list().expect("VALUES row is a List");
            debug_assert!(row.iter().all(|e| e.node_tag() != types_nodes::NodeTag::T_SetToDefault));
        }
    }

    debug_assert!(parsetree.onConflict.is_none());
    table::table_close(rel, NoLock)?;
    Ok(())
}

// The CMD_UPDATE/CMD_DELETE arm of RewriteQuery's DML block: same relation
// prologue as INSERT; UPDATE additionally reorders its targetlist. Same
// rd_rules divergence as rewrite_insert_query.
fn rewrite_update_delete_query<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &mut Query<'mcx>,
) -> PgResult<()> {
    let result_relation = parsetree.resultRelation;
    debug_assert!(result_relation != 0);
    let rt_entry = rte_of(parsetree.rtable.nth(result_relation as usize - 1));
    debug_assert!(rt_entry.rtekind == RTEKind::RTE_RELATION);

    let rel = table::table_open(mcx, rt_entry.relid, NoLock)?;
    if rel.rd_rel.relkind == RELKIND_VIEW {
        panic!(
            "RewriteQuery (rewriteHandler.c): auto-updatable view UPDATE/DELETE \
             needs rewriteTargetView (pg_rewrite vocab unported)"
        );
    }

    if parsetree.commandType == CmdType::CMD_UPDATE {
        debug_assert!(
            parsetree.r#override == types_nodes::OverridingKind::OVERRIDING_NOT_SET
        );
        parsetree.targetList = rewriteTargetListIU(
            mcx,
            &parsetree.targetList,
            CmdType::CMD_UPDATE,
            parsetree.r#override,
            &rel,
            None,
        )?;
    }

    table::table_close(rel, NoLock)?;
    Ok(())
}

// rewriteTargetListIU, INSERT/UPDATE arms: reorder non-junk TLEs into
// attribute order (junk entries keep their post-column resnos and trail the
// list) and apply defaults for unassigned INSERT columns (no stored default
// => the planner NULL-fills). Identity/generated columns, multiple assignment
// merges (process_matched_tle) and real pg_attrdef defaults
// (build_column_default) are loud.
fn rewriteTargetListIU<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &types_nodes::NodeList<'mcx>,
    command_type: CmdType,
    r#override: types_nodes::OverridingKind,
    target_relation: &types_rel::Relation<'mcx>,
    values_rte: Option<(&'mcx types_nodes::RangeTblEntry<'mcx>, i32)>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let _ = values_rte;
    let numattrs = target_relation.rd_att.natts as usize;
    let mut new_tles: PgVec<'mcx, Option<types_nodes::Node<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, numattrs)?;
    new_tles.extend((0..numattrs).map(|_| None));
    let mut junk_tlist = types_nodes::NodeList::nil();
    let mut next_junk_attrno = numattrs + 1;

    for tle_node in target_list {
        let tle = tle_node.as_target_entry().expect("targetlist cell");
        if tle.resjunk {
            // The parser already numbered junk entries past the column count
            // in tlist order; a mismatch would need flatCopyTargetEntry.
            assert_eq!(
                tle.resno as usize, next_junk_attrno,
                "rewriteTargetListIU (rewriteHandler.c): junk resno renumber \
                 (flatCopyTargetEntry) not ported"
            );
            junk_tlist.lappend(mcx, tle_node)?;
            next_junk_attrno += 1;
            continue;
        }
        let attrno = tle.resno as usize;
        assert!(attrno >= 1 && attrno <= numattrs, "bogus resno {attrno} in targetlist");
        if target_relation.rd_att.attr(attrno - 1).attisdropped {
            continue;
        }
        if new_tles[attrno - 1].is_some() {
            panic!(
                "rewriteTargetListIU (rewriteHandler.c): process_matched_tle \
                 (multiple assignment merge) not ported"
            );
        }
        new_tles[attrno - 1] = Some(tle_node);
    }

    let mut new_tlist = types_nodes::NodeList::nil();
    for attrno in 1..=numattrs {
        let att = target_relation.rd_att.attr(attrno - 1);
        if att.attisdropped {
            continue;
        }
        let new_tle = new_tles[attrno - 1];
        // SetToDefault construction is loud upstream (transformAssignedExpr),
        // so apply_default reduces to the INSERT missing-column case.
        let apply_default = new_tle.is_none() && command_type == CmdType::CMD_INSERT;
        if att.attidentity != 0 || att.attgenerated != 0 {
            panic!(
                "rewriteTargetListIU (rewriteHandler.c): identity/generated \
                 column arms not ported"
            );
        }
        debug_assert!(r#override == types_nodes::OverridingKind::OVERRIDING_NOT_SET);
        if apply_default && att.atthasdef {
            panic!(
                "rewriteTargetListIU (rewriteHandler.c): build_column_default \
                 (pg_attrdef adbin evaluation) not ported"
            );
        }
        // No stored default: C omits the entry; the planner inserts the NULL
        // (expand_insert_targetlist).
        if let Some(tle) = new_tle {
            new_tlist.lappend(mcx, tle)?;
        }
    }
    new_tlist.concat(mcx, &junk_tlist)?;
    Ok(new_tlist)
}

fn fireRIRrules<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    active_rirs: &mut PgVec<'mcx, Oid>,
) -> PgResult<()> {
    if !parsetree.cteList.is_nil() {
        panic!(
            "fireRIRrules (rewriteHandler.c): CTE descent + rewriteSearchAndCycle need \
             CommonTableExpr (types_nodes parsenodes unported)"
        );
    }
    debug_assert!(parsetree.onConflict.is_none());
    let orig_result_relation = parsetree.resultRelation;

    let mut rt_index = 0;
    while rt_index < parsetree.rtable.len() {
        let node = parsetree.rtable.nth(rt_index);
        rt_index += 1;
        let rte = rte_of(node);

        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            let sub = rte.subquery.expect("subquery RTE has a subquery");
            fireRIRrules(mcx, sub, active_rirs)?;
            debug_assert!(!sub.hasRowSecurity);
            continue;
        }
        if rte.rtekind != RTEKind::RTE_RELATION {
            continue;
        }
        if rte.relkind == RELKIND_MATVIEW {
            continue;
        }
        if rt_index as i32 != parsetree.resultRelation
            && !range_table_entry_used(parsetree, rt_index as i32)?
        {
            continue;
        }
        if rt_index as i32 == parsetree.resultRelation
            && rt_index as i32 != orig_result_relation
        {
            continue;
        }
        let rel = table::table_open(mcx, rte.relid, NoLock)?;
        // C divergence: the trimmed pg_class Form has no relhasrules, so the
        // rd_rules probe is keyed on relkind — a non-view relation carrying
        // user CREATE RULE rules is undetectable (none exist in a stock
        // initdb; CREATE RULE is unported).
        if rel.rd_rel.relkind == RELKIND_VIEW {
            if let Some(rules) = relcache::RelationGetRules(mcx, rte.relid)? {
                let is_select = |r: &&RewriteRuleMeta| r.event == CmdType::CMD_SELECT as i32;
                if rules.rules.iter().any(|r| is_select(&r)) {
                    if active_rirs.contains(&rte.relid) {
                        let err = infinite_recursion(rel.name());
                        table::table_close(rel, NoLock)?;
                        return Err(err);
                    }
                    active_rirs.push(rte.relid);
                    for rule in rules.rules.iter().filter(is_select) {
                        ApplyRetrieveRule(
                            mcx,
                            parsetree,
                            rule,
                            rt_index as i32,
                            node,
                            &rel,
                            active_rirs,
                        )?;
                    }
                    active_rirs.pop();
                }
            }
        }
        table::table_close(rel, NoLock)?;
    }

    if parsetree.hasSubLinks {
        panic!(
            "fireRIRrules (rewriteHandler.c): sublink descent needs the walker's \
             T_SubLink arm (SubLink vocabulary unported)"
        );
    }

    for node in parsetree.rtable.iter() {
        let rte = rte_of(node);
        if rte.rtekind != RTEKind::RTE_RELATION
            || (rte.relkind != RELKIND_RELATION && rte.relkind != RELKIND_PARTITIONED_TABLE)
        {
            continue;
        }
        let rel = table::table_open(mcx, rte.relid, NoLock)?;
        if rel.rd_rel.relrowsecurity {
            panic!(
                "fireRIRrules (rewriteHandler.c): row-level security needs \
                 get_row_security_policies (rowsecurity.c unported)"
            );
        }
        table::table_close(rel, NoLock)?;
    }

    Ok(())
}

// ApplyRetrieveRule (rewriteHandler.c), SELECT-only arm: the DML-on-view
// result-relation branch and FOR UPDATE/SHARE (markQueryForLocking) are loud.
// The restrict_nonsystem_relation_kind GUC is unported; its boot default (no
// restriction) is assumed.
#[allow(clippy::too_many_arguments)]
fn ApplyRetrieveRule<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rule: &RewriteRuleMeta,
    rt_index: i32,
    rte_node: Node<'mcx>,
    relation: &Relation<'mcx>,
    active_rirs: &mut PgVec<'mcx, Oid>,
) -> PgResult<()> {
    if rule.qual_src.is_some() {
        return Err(internal_error("cannot handle qualified ON SELECT rule"));
    }
    if rt_index == parsetree.resultRelation {
        panic!(
            "ApplyRetrieveRule (rewriteHandler.c): DML on view (result-relation \
             INSTEAD OF arms) not ported"
        );
    }
    if !parsetree.rowMarks.is_nil() {
        panic!(
            "ApplyRetrieveRule (rewriteHandler.c): FOR UPDATE/SHARE of view needs \
             get_parse_rowmark + markQueryForLocking (RowMarkClause unported)"
        );
    }

    // C copyObject's the rulescxt tree; the cache stores ev_action text, so
    // the per-use modifiable copy is a fresh read into the query context.
    let actions_node = readfuncs::stringToNode(mcx, rule.action_src.as_str())?;
    let actions = actions_node.as_list().expect("ev_action is a List");
    if actions.len() != 1 {
        return Err(internal_error("expected just one rule action"));
    }
    let action_node = actions.nth(0);
    let rule_action = action_node.as_query().expect("rule action is a Query");

    // setRuleCheckAsUser (rewriteDefine.c): C applies it once at rule load;
    // the text cache defers it to the freshly read tree — same net state.
    let view_opts = relation.rd_options.as_ref().and_then(|o| o.view());
    let check_as_user = if view_opts.is_some_and(|v| v.security_invoker) {
        InvalidOid
    } else {
        relation.rd_rel.relowner
    };
    set_rule_check_as_user(rule_action, check_as_user);

    AcquireRewriteLocks(mcx, rule_action, true, false)?;

    fireRIRrules(mcx, rule_action, active_rirs)?;
    // parsetree->hasRowSecurity propagation: the RLS arm below is loud, so a
    // true flag cannot reach here.
    debug_assert!(!rule_action.hasRowSecurity);

    let rte = rte_of(rte_node);
    let num_cols = rule_action
        .targetList
        .iter()
        .filter(|te| !te.as_target_entry().expect("tlist cell").resjunk)
        .count();
    if rte.eref.map_or(0, |e| e.colnames.len()) < num_cols {
        panic!(
            "ApplyRetrieveRule (rewriteHandler.c): eref colnames patch \
             (CREATE OR REPLACE VIEW added columns) not ported"
        );
    }

    let security_barrier = view_opts.is_some_and(|v| v.security_barrier);
    // C keeps relid/relkind/rellockmode/perminfoindex so the view is locked
    // and permission-checked at execution.
    // SAFETY: the rewriter owns the just-analyzed tree single-threaded; no
    // reference derived from `rte_node` is live across this write.
    unsafe {
        rte_node.with_mut::<RangeTblEntry, _>(|r| {
            r.rtekind = RTEKind::RTE_SUBQUERY;
            r.subquery = Some(rule_action);
            r.security_barrier = security_barrier;
            r.tablesample = None;
            r.inh = false;
        })
    };
    Ok(())
}

// setRuleCheckAsUser_Query (rewriteDefine.c).
fn set_rule_check_as_user(qry: &Query<'_>, userid: Oid) {
    for pnode in qry.rteperminfos.iter() {
        // SAFETY: the tree was just read by stringToNode; exclusively ours.
        unsafe { pnode.with_mut::<RTEPermissionInfo, _>(|p| p.checkAsUser = userid) }
            .expect("rteperminfos holds RTEPermissionInfo nodes");
    }
    for rnode in qry.rtable.iter() {
        let rte = rte_of(rnode);
        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            set_rule_check_as_user(rte.subquery.expect("subquery RTE"), userid);
        }
    }
    debug_assert!(qry.cteList.is_nil());
    if qry.hasSubLinks {
        panic!(
            "setRuleCheckAsUser (rewriteDefine.c): sublink descent needs the \
             walker's T_SubLink arm (SubLink vocabulary unported)"
        );
    }
}

struct RtiUsed {
    rt_index: i32,
    sublevels_up: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for RtiUsed {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().expect("Var");
                Ok(v.varno == self.rt_index && v.varlevelsup == self.sublevels_up)
            }
            NodeTag::T_RangeTblRef => Ok(self.sublevels_up == 0
                && node.as_range_tbl_ref().expect("RangeTblRef").rtindex == self.rt_index),
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let hit = nodes_core::query_tree_walker(q, self, 0)?;
        self.sublevels_up -= 1;
        Ok(hit)
    }
}

// rangeTableEntry_used (rewriteManip.c). The top Query is a stack value, so
// its fields are walked directly (query_tree_walker wants an arena &'mcx).
fn range_table_entry_used(parsetree: &Query<'_>, rt_index: i32) -> PgResult<bool> {
    let mut w = RtiUsed { rt_index, sublevels_up: 0 };
    if nodes_core::walk_list(&parsetree.targetList, &mut w)?
        || nodes_core::walk_list(&parsetree.returningList, &mut w)?
    {
        return Ok(true);
    }
    if let Some(jt) = parsetree.jointree {
        if nodes_core::walk_list(&jt.fromlist, &mut w)? || nodes_core::walk_opt(jt.quals, &mut w)?
        {
            return Ok(true);
        }
    }
    if nodes_core::walk_opt(parsetree.setOperations, &mut w)?
        || nodes_core::walk_opt(parsetree.havingQual, &mut w)?
        || nodes_core::walk_opt(parsetree.limitOffset, &mut w)?
        || nodes_core::walk_opt(parsetree.limitCount, &mut w)?
    {
        return Ok(true);
    }
    nodes_core::range_table_walker(&parsetree.rtable, &mut w, 0)
}

#[cold]
#[inline(never)]
fn infinite_recursion(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "infinite recursion detected in rules for relation \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn internal_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

pub fn AcquireRewriteLocks<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    forExecute: bool,
    forUpdatePushedDown: bool,
) -> PgResult<()> {
    for node in parsetree.rtable.iter() {
        let rtekind = rte_of(node).rtekind;
        match rtekind {
            RTEKind::RTE_RELATION => {
                let (relid, rellockmode) = {
                    let rte = rte_of(node);
                    (rte.relid, rte.rellockmode)
                };
                let lockmode: LOCKMODE = if !forExecute {
                    AccessShareLock
                } else if forUpdatePushedDown && rellockmode == AccessShareLock {
                    // SAFETY: the rewriter owns the just-analyzed tree
                    // single-threaded; no reference derived from `node` is
                    // live across this write.
                    unsafe { node.with_mut::<RangeTblEntry, _>(|r| r.rellockmode = RowShareLock) };
                    RowShareLock
                } else {
                    rellockmode
                };

                let rel = table::table_open(mcx, relid, lockmode)?;
                let relkind = rel.rd_rel.relkind;
                table::table_close(rel, NoLock)?;
                // SAFETY: as above — exclusive, single-threaded tree fixup.
                unsafe { node.with_mut::<RangeTblEntry, _>(|r| r.relkind = relkind) };
            }
            RTEKind::RTE_JOIN => {
                panic!(
                    "AcquireRewriteLocks (rewriteHandler.c): dropped-column fixup of \
                     joinaliasvars needs strip_implicit_coercions (nodeFuncs.c) + \
                     get_rte_attribute_is_dropped — both still missing from the landed \
                     nodes_core/parse_relation crates"
                );
            }
            RTEKind::RTE_SUBQUERY => {
                let pushed_down = forUpdatePushedDown || {
                    if parsetree.rowMarks.is_nil() {
                        false
                    } else {
                        panic!(
                            "AcquireRewriteLocks (rewriteHandler.c): FOR UPDATE/SHARE \
                             pushdown needs get_parse_rowmark/RowMarkClause — \
                             still missing from the landed parse_relation crate"
                        );
                    }
                };
                let sub = rte_of(node).subquery.expect("subquery RTE has a subquery");
                AcquireRewriteLocks(mcx, sub, forExecute, pushed_down)?;
            }
            _ => {}
        }
    }

    if !parsetree.cteList.is_nil() {
        panic!(
            "AcquireRewriteLocks (rewriteHandler.c): WITH descent needs CommonTableExpr \
             (types_nodes parsenodes unported)"
        );
    }

    if parsetree.hasSubLinks {
        panic!(
            "AcquireRewriteLocks (rewriteHandler.c): sublink descent needs the \
             walker's T_SubLink arm (SubLink vocabulary unported)"
        );
    }

    Ok(())
}

fn rte_of<'mcx>(node: Node<'mcx>) -> &'mcx RangeTblEntry<'mcx> {
    node.as_range_tbl_entry().expect("rtable holds RangeTblEntry nodes")
}
