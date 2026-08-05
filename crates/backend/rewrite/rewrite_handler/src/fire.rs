// Non-SELECT rule firing (rewriteHandler.c): matchLocks / fireRules /
// rewriteRuleAction / CopyAndAddInvertedQual. Rule quals and actions are
// fresh stringToNode reads per application (the relcache rule cache stores
// text — ApplyRetrieveRule precedent), which supplies the exclusive
// ownership C gets from copyObject.

use mcx::{Mcx, PgVec};
use relcache::rules::RewriteRuleMeta;
use rewrite_manip::{ReplaceVarsNoMatchOption, PRS2_NEW_VARNO, PRS2_OLD_VARNO};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource, RTEKind, RangeTblEntry};
use types_nodes::{Node, NodeList, NodeTag};

pub(crate) const RULE_FIRES_ON_ORIGIN: u8 = b'O';
pub(crate) const RULE_FIRES_ON_REPLICA: u8 = b'R';
pub(crate) const RULE_DISABLED: u8 = b'D';

// matchLocks (rewriteHandler.c). Returns indices into `rules`.
pub(crate) fn matchLocks(
    event: CmdType,
    rules: &[RewriteRuleMeta],
    varno: i32,
    rel_name: &str,
    parsetree: &Query<'_>,
    has_update: &mut bool,
) -> PgResult<Vec<usize>> {
    let mut matching = Vec::new();
    if parsetree.commandType != CmdType::CMD_SELECT && parsetree.resultRelation != varno {
        return Ok(matching);
    }
    debug_assert!(parsetree.commandType != CmdType::CMD_SELECT);
    for (i, rule) in rules.iter().enumerate() {
        if rule.event == CmdType::CMD_UPDATE as i32 {
            *has_update = true;
        }
        if rule.event != CmdType::CMD_SELECT as i32 {
            if guc_tables::vars::SessionReplicationRole.read()
                == guc_tables::consts::SESSION_REPLICATION_ROLE_REPLICA
            {
                if rule.enabled == RULE_FIRES_ON_ORIGIN || rule.enabled == RULE_DISABLED {
                    continue;
                }
            } else if rule.enabled == RULE_FIRES_ON_REPLICA || rule.enabled == RULE_DISABLED {
                continue;
            }
            if parsetree.commandType == CmdType::CMD_MERGE {
                return Err(Box::new(
                    PgError::error(format!("cannot execute MERGE on relation \"{rel_name}\""))
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .with_detail("MERGE is not supported for relations with rules."),
                ));
            }
        }
        if rule.event == event as i32 {
            matching.push(i);
        }
    }
    Ok(matching)
}

// fireRules (rewriteHandler.c). Product queries are returned as nodes; the
// caller flat-copies them into Query values before recursing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fireRules<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rt_index: i32,
    event: CmdType,
    rules: &[RewriteRuleMeta],
    locks: &[usize],
    instead_flag: &mut bool,
    returning_flag: &mut bool,
    qual_product: &mut Option<Node<'mcx>>,
    rel_owner: Oid,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let mut results: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for &li in locks {
        let rule = &rules[li];
        let qsrc = if rule.is_instead {
            if rule.qual_src.is_some() {
                QuerySource::QSRC_QUAL_INSTEAD_RULE
            } else {
                *instead_flag = true;
                QuerySource::QSRC_INSTEAD_RULE
            }
        } else {
            QuerySource::QSRC_NON_INSTEAD_RULE
        };

        if qsrc == QuerySource::QSRC_QUAL_INSTEAD_RULE && !*instead_flag {
            if qual_product.is_none() {
                *qual_product = Some(rewrite_manip::copy_query_node(mcx, parsetree)?);
            }
            CopyAndAddInvertedQual(
                mcx,
                qual_product.expect("just set"),
                rule.qual_src.as_ref().expect("qualified rule").as_str(),
                rt_index,
                event,
                rel_owner,
            )?;
        }

        let actions_node = readfuncs::stringToNode(mcx, rule.action_src.as_str())?;
        // setRuleCheckAsUser at RelationBuildRuleLock (relcache.c): the rule's
        // table references are checked as the relation owner, not the invoker.
        crate::set_rule_check_as_user_node(actions_node, rel_owner)?;
        let actions = actions_node.as_list().expect("ev_action is a List");
        for action_node in actions.iter() {
            let action_q = action_node.as_query().expect("rule action is a Query");
            if action_q.commandType == CmdType::CMD_NOTHING {
                continue;
            }
            let rule_qual = match &rule.qual_src {
                None => None,
                Some(s) => {
                    let q = readfuncs::stringToNode(mcx, s.as_str())?;
                    crate::set_rule_check_as_user_node(q, rel_owner)?;
                    Some(q)
                }
            };
            let rule_action = rewriteRuleAction(
                mcx,
                parsetree,
                action_node,
                rule_qual,
                rt_index,
                event,
                returning_flag,
            )?;
            // SAFETY: fresh-read tree, exclusively ours.
            unsafe {
                rule_action.with_mut::<Query, _>(|q| {
                    q.querySource = qsrc;
                    q.canSetTag = false;
                })
            }
            .expect("Query");
            results.push(rule_action);
        }
    }
    Ok(results)
}

// rewriteRuleAction (rewriteHandler.c). `rule_action_node` and `rule_qual`
// are fresh reads owned by this call; `parsetree` is read-only here and
// everything taken from it is deep-copied before insertion.
#[allow(clippy::too_many_arguments)]
fn rewriteRuleAction<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rule_action_node: Node<'mcx>,
    rule_qual: Option<Node<'mcx>>,
    rt_index: i32,
    event: CmdType,
    returning_flag: &mut bool,
) -> PgResult<Node<'mcx>> {
    crate::AcquireRewriteLocks(mcx, rule_action_node.as_query().expect("Query"), true, false)?;
    acquire_locks_on_sublinks(mcx, rule_qual)?;

    let current_varno = rt_index;
    let rt_length = parsetree.rtable.len() as i32;
    let new_varno = PRS2_NEW_VARNO + rt_length;

    let (sub_action_node, is_insert_select) =
        rewrite_manip::getInsertSelectQuery_node(mcx, rule_action_node)?;

    rewrite_manip::OffsetVarNodes(mcx, sub_action_node, rt_length, 0)?;
    if let Some(q) = rule_qual {
        rewrite_manip::OffsetVarNodes(mcx, q, rt_length, 0)?;
    }
    rewrite_manip::ChangeVarNodes(mcx, sub_action_node, PRS2_OLD_VARNO + rt_length, rt_index, 0)?;
    if let Some(q) = rule_qual {
        rewrite_manip::ChangeVarNodes(mcx, q, PRS2_OLD_VARNO + rt_length, rt_index, 0)?;
    }

    {
        let sub_action = sub_action_node.as_query().expect("Query");
        for rte_node in sub_action.rtable.iter() {
            let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
            if rte.rtekind == RTEKind::RTE_SUBQUERY && !rte.lateral {
                let sub = rte.subquery.expect("subquery RTE has a subquery");
                if contain_vars_of_level_query(sub, 1)? {
                    // SAFETY: fresh-read tree, exclusively ours.
                    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.lateral = true) }
                        .expect("RangeTblEntry");
                }
            }
        }
    }

    {
        let sub_action = sub_action_node.as_query().expect("Query");
        let rtable_tail = sub_action.rtable.clone_in(mcx)?;
        let perminfos_tail = sub_action.rteperminfos.clone_in(mcx)?;
        let mut new_rtable = rewrite_manip::copy_node_list(mcx, &parsetree.rtable)?;
        let mut new_perminfos = rewrite_manip::copy_node_list(mcx, &parsetree.rteperminfos)?;
        rewrite_manip::CombineRangeTables(
            mcx,
            &mut new_rtable,
            &mut new_perminfos,
            &rtable_tail,
            &perminfos_tail,
        )?;
        // SAFETY: as above.
        unsafe {
            sub_action_node.with_mut::<Query, _>(|q| {
                q.rtable = new_rtable;
                q.rteperminfos = new_perminfos;
            })
        }
        .expect("Query");
    }

    if parsetree.hasSubLinks && !sub_action_node.as_query().expect("Query").hasSubLinks {
        'outer: for rte_node in parsetree.rtable.iter() {
            let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
            let mut has = match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    rewrite_manip::checkExprHasSubLink_opt(rte.tablesample)?
                }
                RTEKind::RTE_FUNCTION => {
                    rewrite_manip::checkExprHasSubLink_list(&rte.functions)?
                }
                RTEKind::RTE_TABLEFUNC => {
                    rewrite_manip::checkExprHasSubLink_opt(rte.tablefunc)?
                }
                RTEKind::RTE_VALUES => {
                    rewrite_manip::checkExprHasSubLink_list(&rte.values_lists)?
                }
                _ => false,
            };
            has |= rewrite_manip::checkExprHasSubLink_list(&rte.securityQuals)?;
            if has {
                // SAFETY: as above.
                unsafe { sub_action_node.with_mut::<Query, _>(|q| q.hasSubLinks = true) }
                    .expect("Query");
                break 'outer;
            }
        }
    }

    if parsetree.hasRowSecurity {
        // SAFETY: as above.
        unsafe { sub_action_node.with_mut::<Query, _>(|q| q.hasRowSecurity = true) }
            .expect("Query");
    }

    if sub_action_node.as_query().expect("Query").commandType != CmdType::CMD_UTILITY {
        let sub_action = sub_action_node.as_query().expect("Query");
        debug_assert!(sub_action.jointree.is_some());
        let sub_jt = sub_action.jointree.expect("jointree");
        let sub_jt_node = Node::mk(
            mcx,
            types_nodes::primnodes::FromExpr {
                fromlist: sub_jt.fromlist.clone_in(mcx)?,
                quals: sub_jt.quals,
            },
        )?;
        let keeporig = !rewrite_manip::rangeTableEntry_used(sub_jt_node, rt_index, 0)?
            && (rewrite_manip::rangeTableEntry_used_opt(rule_qual, rt_index, 0)?
                || rewrite_manip::rangeTableEntry_used_opt(
                    parsetree.jointree.and_then(|jt| jt.quals),
                    rt_index,
                    0,
                )?);
        let newjointree = adjustJoinTreeList(mcx, parsetree, !keeporig, rt_index)?;
        if !newjointree.is_nil() {
            if sub_action.setOperations.is_some() {
                return Err(feature_not_supported(
                    "conditional UNION/INTERSECT/EXCEPT statements are not implemented",
                ));
            }
            let mark_sublinks = parsetree.hasSubLinks
                && !sub_action.hasSubLinks
                && rewrite_manip::checkExprHasSubLink_list(&newjointree)?;
            let mut merged = newjointree;
            merged.concat(mcx, &sub_jt.fromlist)?;
            let new_jt = mcx::alloc_leak_in(
                mcx,
                types_nodes::primnodes::FromExpr { fromlist: merged, quals: sub_jt.quals },
            )?;
            // SAFETY: as above.
            unsafe {
                sub_action_node.with_mut::<Query, _>(|q| {
                    q.jointree = Some(new_jt);
                    if mark_sublinks {
                        q.hasSubLinks = true;
                    }
                })
            }
            .expect("Query");
        }
    }

    if !parsetree.cteList.is_nil()
        && sub_action_node.as_query().expect("Query").commandType != CmdType::CMD_UTILITY
    {
        let sub_action = sub_action_node.as_query().expect("Query");
        for cte_node in &parsetree.cteList {
            let cte = cte_node.as_common_table_expr().expect("cteList cell");
            for cte2_node in &sub_action.cteList {
                let cte2 = cte2_node.as_common_table_expr().expect("cteList cell");
                if cte.ctename == cte2.ctename {
                    return Err(Box::new(
                        PgError::error(format!(
                            "WITH query name \"{}\" appears in both a rule action and the query being rewritten",
                            cte.ctename.unwrap_or("")
                        ))
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
            }
        }
        let mut merged = sub_action.cteList.clone_in(mcx)?;
        merged.concat(mcx, &rewrite_manip::copy_node_list(mcx, &parsetree.cteList)?)?;
        let has_recursive = parsetree.hasRecursive;
        let has_modifying = parsetree.hasModifyingCTE;
        // SAFETY: as above.
        unsafe {
            sub_action_node.with_mut::<Query, _>(|q| {
                q.cteList = merged;
                q.hasRecursive |= has_recursive;
                q.hasModifyingCTE |= has_modifying;
            })
        }
        .expect("Query");
        if sub_action_node.as_query().expect("Query").hasModifyingCTE && is_insert_select {
            return Err(feature_not_supported(
                "INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH",
            ));
        }
    }

    rewrite_manip::AddQual(mcx, sub_action_node, rule_qual)?;
    rewrite_manip::AddQual(mcx, sub_action_node, parsetree.jointree.and_then(|jt| jt.quals))?;

    if (event == CmdType::CMD_INSERT || event == CmdType::CMD_UPDATE)
        && sub_action_node.as_query().expect("Query").commandType != CmdType::CMD_UTILITY
    {
        let sub_action = sub_action_node.as_query().expect("Query");
        let target_rte = sub_action
            .rtable
            .nth(new_varno as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        let result_relation = sub_action.resultRelation;
        let nomatch = if event == CmdType::CMD_UPDATE {
            ReplaceVarsNoMatchOption::ChangeVarno(current_varno)
        } else {
            ReplaceVarsNoMatchOption::SubstituteNull
        };
        rewrite_manip::ReplaceVarsFromTargetList(
            mcx,
            sub_action_node,
            new_varno,
            0,
            target_rte,
            &parsetree.targetList,
            result_relation,
            nomatch,
            None,
        )?;
    }

    let rule_action = rule_action_node.as_query().expect("Query");
    if parsetree.returningList.is_nil() {
        // SAFETY: as above.
        unsafe {
            rule_action_node.with_mut::<Query, _>(|q| q.returningList = NodeList::nil())
        }
        .expect("Query");
    } else if !rule_action.returningList.is_nil() {
        if *returning_flag {
            return Err(feature_not_supported(
                "cannot have RETURNING lists in multiple rules",
            ));
        }
        *returning_flag = true;
        let target_rte = parsetree
            .rtable
            .nth(parsetree.resultRelation as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        let mut inserted_sublink = false;
        let returning_copy =
            rewrite_manip::copy_node_list(mcx, &parsetree.returningList)?;
        let new_returning = rewrite_manip::ReplaceVarsFromTargetList_list(
            mcx,
            &returning_copy,
            parsetree.resultRelation,
            0,
            target_rte,
            &rule_action.returningList,
            rule_action.resultRelation,
            ReplaceVarsNoMatchOption::ReportError,
            Some(&mut inserted_sublink),
        )?;
        let (old_alias, new_alias) = (parsetree.returningOldAlias, parsetree.returningNewAlias);
        let mark_sublinks = parsetree.hasSubLinks
            && !rule_action.hasSubLinks
            && rewrite_manip::checkExprHasSubLink_list(&new_returning)?;
        // SAFETY: as above.
        unsafe {
            rule_action_node.with_mut::<Query, _>(|q| {
                q.returningList = new_returning;
                q.returningOldAlias = old_alias;
                q.returningNewAlias = new_alias;
                if inserted_sublink || mark_sublinks {
                    q.hasSubLinks = true;
                }
            })
        }
        .expect("Query");
    }

    Ok(rule_action_node)
}

// adjustJoinTreeList (rewriteHandler.c): deep copy of the fromlist,
// optionally dropping the top-level RangeTblRef for rt_index.
fn adjustJoinTreeList<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    removert: bool,
    rt_index: i32,
) -> PgResult<NodeList<'mcx>> {
    let jt = parsetree.jointree.expect("jointree");
    let copied = rewrite_manip::copy_node_list(mcx, &jt.fromlist)?;
    if !removert {
        return Ok(copied);
    }
    let mut out = NodeList::nil();
    let mut removed = false;
    for n in &copied {
        if !removed {
            if let Some(rtr) = n.as_range_tbl_ref() {
                if rtr.rtindex == rt_index {
                    removed = true;
                    continue;
                }
            }
        }
        out.lappend(mcx, n)?;
    }
    Ok(out)
}

// CopyAndAddInvertedQual (rewriteHandler.c); `qual_product_node` is the
// already-copied original query.
fn CopyAndAddInvertedQual<'mcx>(
    mcx: Mcx<'mcx>,
    qual_product_node: Node<'mcx>,
    qual_src: &str,
    rt_index: i32,
    event: CmdType,
    rel_owner: Oid,
) -> PgResult<()> {
    let new_qual = readfuncs::stringToNode(mcx, qual_src)?;
    crate::set_rule_check_as_user_node(new_qual, rel_owner)?;
    acquire_locks_on_sublinks(mcx, Some(new_qual))?;
    rewrite_manip::ChangeVarNodes(mcx, new_qual, PRS2_OLD_VARNO, rt_index, 0)?;
    let new_qual = if event == CmdType::CMD_INSERT || event == CmdType::CMD_UPDATE {
        let qp = qual_product_node.as_query().expect("Query");
        let target_rte = qp
            .rtable
            .nth(rt_index as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        let nomatch = if event == CmdType::CMD_UPDATE {
            ReplaceVarsNoMatchOption::ChangeVarno(rt_index)
        } else {
            ReplaceVarsNoMatchOption::SubstituteNull
        };
        let mut inserted_sublink = false;
        let replaced = rewrite_manip::ReplaceVarsFromTargetList(
            mcx,
            new_qual,
            PRS2_NEW_VARNO,
            0,
            target_rte,
            &qp.targetList,
            qp.resultRelation,
            nomatch,
            Some(&mut inserted_sublink),
        )?;
        if inserted_sublink {
            // SAFETY: qual_product is our fresh copy.
            unsafe { qual_product_node.with_mut::<Query, _>(|q| q.hasSubLinks = true) }
                .expect("Query");
        }
        replaced
    } else {
        new_qual
    };
    rewrite_manip::AddInvertedQual(mcx, qual_product_node, Some(new_qual))
}

// acquireLocksOnSubLinks (rewriteHandler.c), for_execute arm: lock rels of
// every sublink subselect in a fresh-read qual.
fn acquire_locks_on_sublinks<'mcx>(mcx: Mcx<'mcx>, node: Option<Node<'mcx>>) -> PgResult<()> {
    struct W<'mcx> {
        mcx: Mcx<'mcx>,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(sl) = node.as_sub_link() {
                let sub = sl.subselect.as_query().expect("analyzed sublink sub-select");
                crate::AcquireRewriteLocks(self.mcx, sub, true, false)?;
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    if let Some(n) = node {
        use nodes_core::NodeWalker as _;
        W { mcx }.visit(n)?;
    }
    Ok(())
}

// contain_vars_of_level (var.c) over a subquery reachable only as &Query.
fn contain_vars_of_level_query<'mcx>(q: &'mcx Query<'mcx>, levelsup: u32) -> PgResult<bool> {
    struct W {
        sublevels_up: u32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    Ok(node.as_var().expect("Var").varlevelsup == self.sublevels_up)
                }
                NodeTag::T_CurrentOfExpr => Ok(self.sublevels_up == 0),
                NodeTag::T_PlaceHolderVar => panic!(
                    "contain_vars_of_level (var.c): PlaceHolderVar in parse tree"
                ),
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
    let mut w = W { sublevels_up: levelsup };
    nodes_core::query_tree_walker(q, &mut w, 0)
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}
