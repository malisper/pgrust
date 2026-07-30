#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use relcache::rules::RewriteRuleMeta;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION};
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::NodeList;
use types_nodes::parsenodes::{
    Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry, RowMarkClause, WCOKind,
    WithCheckOption, ACL_SELECT_FOR_UPDATE,
};
use types_nodes::{Bitmapset, LockClauseStrength, LockWaitPolicy, NodeTag};
use types_rel::{
    AccessShareLock, NoLock, Relation, RowExclusiveLock, RowShareLock, LOCKMODE,
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW, VIEW_OPTION_CHECK_OPTION_CASCADED,
    VIEW_OPTION_CHECK_OPTION_NOT_SET,
};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

mod fire;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    rewrite_handler_seams::query_rewrite::set(QueryRewrite);
    rewrite_handler_seams::acquire_rewrite_locks::set(AcquireRewriteLocks);
}

pub fn QueryRewrite<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    debug_assert_eq!(parsetree.querySource, QuerySource::QSRC_ORIGINAL);
    debug_assert!(parsetree.canSetTag);

    let input_query_id = parsetree.queryId;
    let orig_cmd_type = parsetree.commandType;

    let mut rewrite_events: PgVec<'mcx, (Oid, CmdType)> = PgVec::new_in(mcx);
    let mut results = RewriteQuery(mcx, parsetree, &mut rewrite_events, 0, 0)?;

    for query in results.iter_mut() {
        rewrite_dml_view_with_instead_trigger(mcx, query)?;
        let mut active_rirs: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        let rir = fireRIRrules(mcx, query, &mut active_rirs)?;
        query.hasRowSecurity |= rir.has_row_security;
        query.hasSubLinks |= rir.has_sub_links;
        if !rir.with_check_options.is_empty() {
            let mut wcos = NodeList::from_slice(mcx, &rir.with_check_options)?;
            wcos.concat(mcx, &query.withCheckOptions)?;
            query.withCheckOptions = wcos;
        }
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
    rewrite_events: &mut PgVec<'mcx, (Oid, CmdType)>,
    orig_rt_length: usize,
    num_ctes_processed: usize,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    let event = parsetree.commandType;
    let mut instead = false;
    let mut returning = false;
    let mut qual_product: Option<Node<'mcx>> = None;
    let mut rewritten: PgVec<'mcx, Query<'mcx>> = PgVec::new_in(mcx);

    // C's CTE loop only acts on data-modifying CTEs; SELECT CTEs `continue`,
    // and already-processed CTEs at the list tail are skipped on recursion.
    let cte_len = parsetree.cteList.len();
    for (i, cte_node) in parsetree.cteList.iter().enumerate() {
        if i >= cte_len - num_ctes_processed {
            break;
        }
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctequery = cte.ctequery.and_then(|n| n.as_query()).expect("analyzed CTE query");
        if ctequery.commandType == CmdType::CMD_SELECT {
            continue;
        }

        let ctq = rewrite_manip::flat_copy_query(mcx, ctequery)?;
        let newstuff = RewriteQuery(mcx, ctq, rewrite_events, 0, 0)?;

        // Only an unconditional single-statement DO INSTEAD rewrite fits back
        // into the CTE node.
        match newstuff.len() {
            1 => {
                let q = newstuff.into_iter().next().expect("len checked");
                if q.utilityStmt.is_some()
                    || !matches!(
                        q.commandType,
                        CmdType::CMD_SELECT
                            | CmdType::CMD_UPDATE
                            | CmdType::CMD_INSERT
                            | CmdType::CMD_DELETE
                            | CmdType::CMD_MERGE
                    )
                {
                    // Currently it could only be NOTIFY (C).
                    return Err(wcte_rule_unsupported("DO INSTEAD NOTIFY"));
                }
                debug_assert!(!q.canSetTag);
                let query_node = Node::mk(mcx, q)?;
                // SAFETY: rewriter-owned tree; no live derived refs.
                unsafe {
                    cte_node.with_mut::<types_nodes::parsenodes::CommonTableExpr, _>(|c| {
                        c.ctequery = Some(query_node)
                    })
                }
                .expect("cteList cell");
            }
            0 => return Err(wcte_rule_unsupported("DO INSTEAD NOTHING")),
            _ => {
                for q in newstuff.iter() {
                    if q.querySource == QuerySource::QSRC_QUAL_INSTEAD_RULE {
                        return Err(wcte_rule_unsupported("conditional DO INSTEAD"));
                    }
                    if q.querySource == QuerySource::QSRC_NON_INSTEAD_RULE {
                        return Err(wcte_rule_unsupported("DO ALSO"));
                    }
                }
                return Err(wcte_rule_unsupported("multi-statement DO INSTEAD"));
            }
        }
    }
    let num_ctes_processed = cte_len;

    let mut product_count = 0usize;
    let mut has_update = false;
    if event != CmdType::CMD_SELECT && event != CmdType::CMD_UTILITY {
        if !matches!(
            event,
            CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
        ) {
            panic!("unrecognized commandType: {event:?}");
        }
        let result_relation = parsetree.resultRelation;
        debug_assert!(result_relation != 0);
        let rt_entry = rte_of(parsetree.rtable.nth(result_relation as usize - 1));
        debug_assert!(rt_entry.rtekind == RTEKind::RTE_RELATION);

        let rel = table::table_open(mcx, rt_entry.relid, NoLock)?;

        let mut values_rte_index: i32 = 0;
        let mut defaults_remaining = false;

        match event {
            CmdType::CMD_INSERT => {
                let mut values_rte = None;
                let jointree = parsetree.jointree.expect("INSERT jointree is a FromExpr");
                for rtr_node in &jointree.fromlist {
                    if let Some(rtr) = rtr_node.as_range_tbl_ref() {
                        if rtr.rtindex as usize <= orig_rt_length {
                            // Product queries re-encounter the original query's
                            // already-processed VALUES RTEs; skip them (C).
                            continue;
                        }
                        let rte_node = parsetree.rtable.nth(rtr.rtindex as usize - 1);
                        let rte = rte_of(rte_node);
                        if rte.rtekind == RTEKind::RTE_VALUES {
                            debug_assert!(
                                values_rte.is_none(),
                                "more than one VALUES RTE found"
                            );
                            values_rte = Some((rte, rtr.rtindex, rte_node));
                        }
                    }
                }

                let mut unused_values_attrnos: PgVec<'_, i16> = PgVec::new_in(mcx);
                parsetree.targetList = rewriteTargetListIU(
                    mcx,
                    &parsetree.targetList,
                    CmdType::CMD_INSERT,
                    parsetree.r#override,
                    &rel,
                    values_rte.map(|(rte, rti, _)| (rte, rti)),
                    Some(&mut unused_values_attrnos),
                )?;

                if let Some((rte, rti, rte_node)) = values_rte {
                    if !rewriteValuesRTE(
                        mcx,
                        &parsetree,
                        rte,
                        rti,
                        rte_node,
                        &rel,
                        &unused_values_attrnos,
                    )? {
                        defaults_remaining = true;
                    }
                    values_rte_index = rti;
                }

                if let Some(oc_node) = parsetree.onConflict {
                    let oc =
                        oc_node.as_on_conflict_expr().expect("onConflict is an OnConflictExpr");
                    if oc.action == types_nodes::primnodes::OnConflictAction::ONCONFLICT_UPDATE {
                        let new_set = rewriteTargetListIU(
                            mcx,
                            &oc.onConflictSet,
                            CmdType::CMD_UPDATE,
                            parsetree.r#override,
                            &rel,
                            None,
                            None,
                        )?;
                        // SAFETY: exclusive Query-tree ownership during rewrite.
                        unsafe {
                            oc_node.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                                o.onConflictSet = new_set;
                            })
                        }
                        .expect("OnConflictExpr node");
                    }
                }
            }
            CmdType::CMD_UPDATE => {
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
                    None,
                )?;
            }
            CmdType::CMD_DELETE => {}
            CmdType::CMD_MERGE => {
                debug_assert!(
                    parsetree.r#override == types_nodes::OverridingKind::OVERRIDING_NOT_SET
                );
                for action_node in &parsetree.mergeActionList {
                    let action = action_node
                        .as_merge_action()
                        .expect("mergeActionList cell is a MergeAction");
                    match action.commandType {
                        CmdType::CMD_NOTHING | CmdType::CMD_DELETE => {}
                        CmdType::CMD_UPDATE | CmdType::CMD_INSERT => {
                            // MERGE actions do not permit multi-row INSERTs: no VALUES RTE.
                            let new_tlist = rewriteTargetListIU(
                                mcx,
                                &action.targetList,
                                action.commandType,
                                action.r#override,
                                &rel,
                                None,
                                None,
                            )?;
                            // SAFETY: exclusive Query-tree ownership during rewrite.
                            unsafe {
                                action_node.with_mut::<types_nodes::MergeAction, _>(|a| {
                                    a.targetList = new_tlist;
                                })
                            }
                            .expect("MergeAction node");
                        }
                        other => panic!("unrecognized commandType: {other:?}"),
                    }
                }
            }
            _ => unreachable!(),
        }

        let rules_rc = if rel.rd_hasrules {
            relcache::RelationGetRules(mcx, rt_entry.relid)?
        } else {
            None
        };
        let empty: [RewriteRuleMeta; 0] = [];
        let rules: &[RewriteRuleMeta] = match &rules_rc {
            Some(r) => &r.rules,
            None => &empty,
        };
        let locks =
            fire::matchLocks(event, rules, result_relation, rel.name(), &parsetree, &mut has_update)?;

        let product_orig_rt_length = parsetree.rtable.len();
        let product_queries = fire::fireRules(
            mcx,
            &parsetree,
            result_relation,
            event,
            rules,
            &locks,
            &mut instead,
            &mut returning,
            &mut qual_product,
            rel.rd_rel.relowner,
        )?;
        product_count = product_queries.len();

        if defaults_remaining && !product_queries.is_empty() {
            for pt_node in product_queries.iter() {
                let mut pt = pt_node.as_query().expect("Query");
                // An INSERT ... SELECT product carries the VALUES RTE in the
                // SELECT part at the same index.
                if pt.commandType == CmdType::CMD_INSERT {
                    if let Some(jt) = pt.jointree {
                        if jt.fromlist.len() == 1 {
                            if let Some(rtr) = jt.fromlist.nth(0).as_range_tbl_ref() {
                                let src_rte = rte_of(pt.rtable.nth(rtr.rtindex as usize - 1));
                                if src_rte.rtekind == RTEKind::RTE_SUBQUERY {
                                    if let Some(sub) = src_rte.subquery {
                                        if sub.commandType == CmdType::CMD_SELECT {
                                            pt = sub;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let values_rte_node = pt.rtable.nth(values_rte_index as usize - 1);
                if rte_of(values_rte_node).rtekind != RTEKind::RTE_VALUES {
                    table::table_close(rel, NoLock)?;
                    return Err(internal_error("failed to find VALUES RTE in product query"));
                }
                rewriteValuesRTEToNulls(mcx, values_rte_node)?;
            }
        }

        let mut updatableview = false;
        if !instead
            && rel.rd_rel.relkind == RELKIND_VIEW
            && !view_has_instead_trigger(&rel, event, &parsetree.mergeActionList)?
        {
            if qual_product.is_some() {
                let err = error_view_not_updatable(
                    &rel,
                    event,
                    &parsetree.mergeActionList,
                    Some("Views with conditional DO INSTEAD rules are not automatically updatable."),
                )
                .unwrap_or_else(|e| e);
                table::table_close(rel, NoLock)?;
                return Err(err);
            }
            parsetree = match rewriteTargetView(mcx, parsetree, &rel) {
                Ok(q) => q,
                Err(e) => {
                    table::table_close(rel, NoLock)?;
                    return Err(e);
                }
            };
            instead = true;
            returning = true;
            updatableview = true;
        }

        if !product_queries.is_empty() || updatableview {
            for ev in rewrite_events.iter() {
                if ev.0 == rt_entry.relid && ev.1 == event {
                    let err = infinite_recursion(rel.name());
                    table::table_close(rel, NoLock)?;
                    return Err(err);
                }
            }
            rewrite_events.push((rt_entry.relid, event));
            if updatableview && event == CmdType::CMD_INSERT {
                let vq = rewrite_manip::flat_copy_query(mcx, &parsetree)?;
                let newstuff =
                    RewriteQuery(mcx, vq, rewrite_events, orig_rt_length, num_ctes_processed)?;
                rewritten.extend(newstuff);
            }
            for pt_node in product_queries.iter() {
                let ptq = rewrite_manip::flat_copy_query(mcx, pt_node.as_query().expect("Query"))?;
                let newstuff = RewriteQuery(
                    mcx,
                    ptq,
                    rewrite_events,
                    product_orig_rt_length,
                    num_ctes_processed,
                )?;
                rewritten.extend(newstuff);
            }
            if updatableview && event != CmdType::CMD_INSERT {
                let vq = rewrite_manip::flat_copy_query(mcx, &parsetree)?;
                let newstuff =
                    RewriteQuery(mcx, vq, rewrite_events, orig_rt_length, num_ctes_processed)?;
                rewritten.extend(newstuff);
            }
            rewrite_events.pop();
        }

        if (instead || qual_product.is_some())
            && !parsetree.returningList.is_nil()
            && !returning
        {
            let err = returning_needs_instead_rule(event, rel.name());
            table::table_close(rel, NoLock)?;
            return Err(err);
        }

        if parsetree.onConflict.is_some() && (product_count > 0 || has_update) && !updatableview {
            table::table_close(rel, NoLock)?;
            return Err(Box::new(
                PgError::error(
                    "INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }

        table::table_close(rel, NoLock)?;
    }

    // INSERT products run after the original; UPDATE/DELETE products before.
    if !instead {
        let final_q = match qual_product {
            Some(n) => rewrite_manip::flat_copy_query(mcx, n.as_query().expect("Query"))?,
            None => parsetree,
        };
        if event == CmdType::CMD_INSERT {
            rewritten.insert(0, final_q);
        } else {
            rewritten.push(final_q);
        }
    }

    if cte_len > 0 {
        let qcount =
            rewritten.iter().filter(|q| q.commandType != CmdType::CMD_UTILITY).count();
        if qcount > 1 {
            return Err(Box::new(
                PgError::error(
                    "WITH cannot be used in a query that is rewritten by rules into multiple queries",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    }

    Ok(rewritten)
}

#[track_caller]
#[cold]
#[inline(never)]
fn wcte_rule_unsupported(kind: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "{kind} rules are not supported for data-modifying statements in WITH"
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn returning_needs_instead_rule(event: CmdType, relname: &str) -> Box<PgError> {
    let (verb, hint_evt) = match event {
        CmdType::CMD_INSERT => ("INSERT", "INSERT"),
        CmdType::CMD_UPDATE => ("UPDATE", "UPDATE"),
        _ => ("DELETE", "DELETE"),
    };
    Box::new(
        PgError::error(format!(
            "cannot perform {verb} RETURNING on relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint(format!(
            "You need an unconditional ON {hint_evt} DO INSTEAD rule with a RETURNING clause."
        )),
    )
}

// rewriteTargetListIU, INSERT/UPDATE arms: reorder non-junk TLEs into
// attribute order (junk entries keep their post-column resnos and trail the
// list) and apply stored pg_attrdef defaults for unassigned INSERT columns
// (no stored default => the planner NULL-fills).
fn rewriteTargetListIU<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &types_nodes::NodeList<'mcx>,
    command_type: CmdType,
    r#override: types_nodes::OverridingKind,
    target_relation: &types_rel::Relation<'mcx>,
    values_rte: Option<(&'mcx types_nodes::RangeTblEntry<'mcx>, i32)>,
    mut unused_values_attrnos: Option<&mut PgVec<'mcx, i16>>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
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
        let att = target_relation.rd_att.attr(attrno - 1);
        if att.attisdropped {
            continue;
        }
        new_tles[attrno - 1] = Some(process_matched_tle(
            mcx,
            tle_node,
            new_tles[attrno - 1],
            core::str::from_utf8(att.attname.name_str()).expect("attname"),
        )?);
    }

    use types_core::catalog::{ATTRIBUTE_IDENTITY_ALWAYS, ATTRIBUTE_IDENTITY_BY_DEFAULT};
    use types_nodes::OverridingKind;

    // findDefaultOnlyColumns (rewriteHandler.c), computed once on demand:
    // true per VALUES column iff every row's cell is SetToDefault.
    let mut default_only_cols: Option<PgVec<'mcx, bool>> = None;

    let mut new_tlist = types_nodes::NodeList::nil();
    for attrno in 1..=numattrs {
        let att = target_relation.rd_att.attr(attrno - 1);
        if att.attisdropped {
            continue;
        }
        let new_tle = new_tles[attrno - 1];
        let mut apply_default = (new_tle.is_none() && command_type == CmdType::CMD_INSERT)
            || new_tle.is_some_and(|t| {
                t.as_target_entry().expect("targetlist cell").expr.node_tag()
                    == types_nodes::NodeTag::T_SetToDefault
            });
        let values_attrno: i16 = match (values_rte, new_tle) {
            (Some((_, rti)), Some(t)) => t
                .as_target_entry()
                .expect("targetlist cell")
                .expr
                .as_var()
                .filter(|v| v.varno == rti)
                .map_or(0, |v| v.varattno),
            _ => 0,
        };
        let mut values_col_is_default_only =
            |default_only_cols: &mut Option<PgVec<'mcx, bool>>| -> PgResult<bool> {
                if values_attrno == 0 {
                    return Ok(false);
                }
                if default_only_cols.is_none() {
                    let rte = values_rte.expect("values_attrno nonzero").0;
                    let width = rte
                        .values_lists
                        .nth(0)
                        .as_list()
                        .expect("VALUES row is a List")
                        .len();
                    let mut cols: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, width)?;
                    cols.extend((0..width).map(|_| true));
                    for row in &rte.values_lists {
                        for (i, cell) in
                            row.as_list().expect("VALUES row is a List").iter().enumerate()
                        {
                            if cell.node_tag() != types_nodes::NodeTag::T_SetToDefault {
                                cols[i] = false;
                            }
                        }
                    }
                    *default_only_cols = Some(cols);
                }
                Ok(default_only_cols.as_ref().expect("just built")[values_attrno as usize - 1])
            };
        if command_type == CmdType::CMD_INSERT {
            if att.attidentity as u8 == ATTRIBUTE_IDENTITY_ALWAYS && !apply_default {
                if r#override == OverridingKind::OVERRIDING_USER_VALUE {
                    apply_default = true;
                } else if r#override != OverridingKind::OVERRIDING_SYSTEM_VALUE {
                    if values_col_is_default_only(&mut default_only_cols)? {
                        apply_default = true;
                    } else {
                        return Err(generated_always_insert_error(att, true));
                    }
                }
            }
            if att.attidentity as u8 == ATTRIBUTE_IDENTITY_BY_DEFAULT
                && r#override == OverridingKind::OVERRIDING_USER_VALUE
            {
                apply_default = true;
            }
            if att.attgenerated != 0 && !apply_default {
                if values_col_is_default_only(&mut default_only_cols)? {
                    apply_default = true;
                } else {
                    return Err(generated_always_insert_error(att, false));
                }
            }
            if values_attrno != 0 && apply_default {
                if let Some(unused) = unused_values_attrnos.as_deref_mut() {
                    unused.push(values_attrno);
                }
            }
        }
        if command_type == CmdType::CMD_UPDATE {
            if att.attidentity as u8 == ATTRIBUTE_IDENTITY_ALWAYS
                && new_tle.is_some()
                && !apply_default
            {
                return Err(generated_always_update_error(att, true));
            }
            if att.attgenerated != 0 && new_tle.is_some() && !apply_default {
                return Err(generated_always_update_error(att, false));
            }
        }
        let new_tle = if att.attgenerated != 0 {
            // Stored generated columns are computed in the executor.
            None
        } else if apply_default {
            let expr = match build_column_default(mcx, target_relation, attrno)? {
                Some(e) => Some(e),
                // No stored default: C omits the entry for INSERT (the
                // planner inserts the NULL); UPDATE SET col = DEFAULT sets an
                // explicit NULL, domain-wrapped.
                None if command_type == CmdType::CMD_INSERT => None,
                None => Some(coerce::coerce_null_to_domain(
                    mcx,
                    att.atttypid,
                    att.atttypmod,
                    att.attcollation,
                    att.attlen as i32,
                    att.attbyval,
                )?),
            };
            match expr {
                None => None,
                Some(expr) => {
                    let resname = core::str::from_utf8(att.attname.name_str()).expect("attname");
                    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, resname.len())?;
                    mcx::vec_append_bytes(&mut buf, resname.as_bytes())?;
                    Some(types_nodes::Node::mk(
                        mcx,
                        types_nodes::primnodes::TargetEntry {
                            expr,
                            resno: attrno as i16,
                            resname: Some(
                                core::str::from_utf8(buf.leak()).expect("was UTF-8"),
                            ),
                            ressortgroupref: 0,
                            resorigtbl: 0,
                            resorigcol: 0,
                            resjunk: false,
                        },
                    )?)
                }
            }
        } else {
            new_tle
        };
        if let Some(tle) = new_tle {
            new_tlist.lappend(mcx, tle)?;
        }
    }
    new_tlist.concat(mcx, &junk_tlist)?;
    Ok(new_tlist)
}

// process_matched_tle (rewriteHandler.c): merge multiple assignments to the
// same attribute; only FieldStore/SubscriptingRef assignment nodes combine
// (leftmost assignment nests innermost), with matching CoerceToDomain
// wrappers stripped and reapplied once over the combined node so domain
// checks run after all updates.
fn process_matched_tle<'mcx>(
    mcx: Mcx<'mcx>,
    src_tle_node: Node<'mcx>,
    prior_tle_node: Option<Node<'mcx>>,
    attr_name: &str,
) -> PgResult<Node<'mcx>> {
    let Some(prior_tle_node) = prior_tle_node else {
        return Ok(src_tle_node);
    };
    let src_tle = src_tle_node.as_target_entry().expect("TargetEntry");
    let prior_tle = prior_tle_node.as_target_entry().expect("TargetEntry");

    let mut src_expr = src_tle.expr;
    let mut prior_expr = prior_tle.expr;
    let mut coerce_expr: Option<&types_nodes::primnodes::CoerceToDomain<'mcx>> = None;
    if let (Some(src_cd), Some(prior_cd)) =
        (src_expr.as_coerce_to_domain(), prior_expr.as_coerce_to_domain())
    {
        if src_cd.resulttype == prior_cd.resulttype {
            // C assumes without checking that resulttypmod/resultcollid match.
            coerce_expr = Some(src_cd);
            src_expr = src_cd.arg;
            prior_expr = prior_cd.arg;
        }
    }

    let src_input = get_assignment_input(src_expr);
    let prior_input = get_assignment_input(prior_expr);
    if src_input.is_none()
        || prior_input.is_none()
        || nodes_core::node_funcs::expr_type(src_expr)
            != nodes_core::node_funcs::expr_type(prior_expr)
    {
        return Err(multiple_assignments_error(attr_name));
    }
    let src_input = src_input.expect("checked above");

    // The prior TLE may already be a nest of assignments; the original
    // column reference is at the bottom.
    let mut priorbottom = prior_input.expect("checked above");
    while let Some(newbottom) = get_assignment_input(priorbottom) {
        priorbottom = newbottom;
    }
    if !types_nodes::equal(priorbottom, src_input) {
        return Err(multiple_assignments_error(attr_name));
    }

    let newexpr = if let Some(src_fs) = src_expr.as_field_store() {
        if let Some(prior_fs) = prior_expr.as_field_store() {
            // Two FieldStores combine into one with multiple target fields.
            let mut newvals = prior_fs.newvals.clone_in(mcx)?;
            newvals.concat(mcx, &src_fs.newvals)?;
            let mut fieldnums = prior_fs.fieldnums.clone_in(mcx)?;
            fieldnums.concat(mcx, &src_fs.fieldnums)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldStore {
                    arg: prior_fs.arg,
                    newvals,
                    fieldnums,
                    resulttype: prior_fs.resulttype,
                },
            )?
        } else {
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldStore {
                    arg: prior_expr,
                    newvals: src_fs.newvals.clone_in(mcx)?,
                    fieldnums: src_fs.fieldnums.clone_in(mcx)?,
                    resulttype: src_fs.resulttype,
                },
            )?
        }
    } else if let Some(src_sr) = src_expr.as_subscripting_ref() {
        Node::mk(
            mcx,
            types_nodes::primnodes::SubscriptingRef {
                refcontainertype: src_sr.refcontainertype,
                refelemtype: src_sr.refelemtype,
                refrestype: src_sr.refrestype,
                reftypmod: src_sr.reftypmod,
                refcollid: src_sr.refcollid,
                refupperindexpr: src_sr.refupperindexpr.clone_in(mcx)?,
                reflowerindexpr: src_sr.reflowerindexpr.clone_in(mcx)?,
                refexpr: Some(prior_expr),
                refassgnexpr: src_sr.refassgnexpr,
            },
        )?
    } else {
        panic!("cannot happen");
    };

    let newexpr = match coerce_expr {
        Some(cd) => Node::mk(
            mcx,
            types_nodes::primnodes::CoerceToDomain {
                arg: newexpr,
                resulttype: cd.resulttype,
                resulttypmod: cd.resulttypmod,
                resultcollid: cd.resultcollid,
                coercionformat: cd.coercionformat,
                location: cd.location,
            },
        )?,
        None => newexpr,
    };

    Node::mk(
        mcx,
        types_nodes::primnodes::TargetEntry {
            expr: newexpr,
            resno: src_tle.resno,
            resname: src_tle.resname,
            ressortgroupref: src_tle.ressortgroupref,
            resorigtbl: src_tle.resorigtbl,
            resorigcol: src_tle.resorigcol,
            resjunk: src_tle.resjunk,
        },
    )
}

// get_assignment_input (rewriteHandler.c): an assignment node's input, or
// None if node is not an assignment node.
fn get_assignment_input<'mcx>(node: Node<'mcx>) -> Option<Node<'mcx>> {
    if let Some(fstore) = node.as_field_store() {
        return Some(fstore.arg);
    }
    if let Some(sbsref) = node.as_subscripting_ref() {
        if sbsref.refassgnexpr.is_none() {
            return None;
        }
        return sbsref.refexpr;
    }
    None
}

#[track_caller]
#[cold]
#[inline(never)]
fn multiple_assignments_error(attr_name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("multiple assignments to same column \"{attr_name}\""))
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
    )
}

// rewriteValuesRTE (rewriteHandler.c): replace SetToDefault cells with the
// column's stored default or an explicit NULL; unused_cols (targetlist entry
// replaced by a default expression) NULL-fill. On an auto-updatable view,
// default-less cells stay SetToDefault (returns false = !allReplaced) so the
// base relation's defaults apply after rewriteTargetView.
fn rewriteValuesRTE<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rte: &'mcx types_nodes::RangeTblEntry<'mcx>,
    rti: i32,
    rte_node: Node<'mcx>,
    target_relation: &types_rel::Relation<'mcx>,
    unused_cols: &[i16],
) -> PgResult<bool> {
    let mut has_default = false;
    'outer: for row in &rte.values_lists {
        for e in row.as_list().expect("VALUES row is a List").iter() {
            if e.node_tag() == types_nodes::NodeTag::T_SetToDefault {
                has_default = true;
                break 'outer;
            }
        }
    }
    if !has_default {
        return Ok(true);
    }

    let numattrs = rte.values_lists.nth(0).as_list().expect("VALUES row is a List").len();
    let mut attrnos: PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, numattrs)?;
    attrnos.extend((0..numattrs).map(|_| 0i16));
    for tle_node in &parsetree.targetList {
        let tle = tle_node.as_target_entry().expect("targetlist cell");
        if let Some(var) = tle.expr.as_var() {
            if var.varno == rti {
                let attrno = var.varattno as usize;
                debug_assert!(attrno >= 1 && attrno <= numattrs);
                attrnos[attrno - 1] = tle.resno;
            }
        }
    }

    let is_auto_updatable_view = if target_relation.rd_rel.relkind == RELKIND_VIEW
        && !view_has_instead_trigger(target_relation, CmdType::CMD_INSERT, &NodeList::nil())?
    {
        let rules_rc = if target_relation.rd_hasrules {
            relcache::RelationGetRules(mcx, target_relation.rd_id)?
        } else {
            None
        };
        let empty: [RewriteRuleMeta; 0] = [];
        let rules: &[RewriteRuleMeta] = match &rules_rc {
            Some(r) => &r.rules,
            None => &empty,
        };
        let mut has_update = false;
        let locks = fire::matchLocks(
            CmdType::CMD_INSERT,
            rules,
            parsetree.resultRelation,
            target_relation.name(),
            parsetree,
            &mut has_update,
        )?;
        // No unconditional DO INSTEAD rule: assume auto-updatable
        // (rewriteTargetView errors otherwise).
        !locks
            .iter()
            .any(|&i| rules[i].is_instead && rules[i].qual_src.is_none())
    } else {
        false
    };

    let mut all_replaced = true;
    let mut new_values = types_nodes::NodeList::nil();
    for row in &rte.values_lists {
        let mut new_list = types_nodes::NodeList::nil();
        for (i, col) in row.as_list().expect("VALUES row is a List").iter().enumerate() {
            if col.node_tag() != types_nodes::NodeTag::T_SetToDefault {
                new_list.lappend(mcx, col)?;
                continue;
            }
            if unused_cols.contains(&((i + 1) as i16)) {
                // The targetlist entry was replaced by a default expression;
                // C NULL-fills the now-unused cell (makeNullConst).
                let def = col
                    .as_variant::<types_nodes::primnodes::SetToDefault>()
                    .expect("SetToDefault");
                let (typlen, typbyval) = lsyscache::get_typlenbyval(def.typeId)?;
                new_list.lappend(
                    mcx,
                    types_nodes::Node::mk_const(
                        mcx,
                        def.typeId,
                        def.typeMod,
                        def.collation,
                        typlen as i32,
                        datum::Datum::null(),
                        true,
                        typbyval,
                    )?,
                )?;
                continue;
            }
            let attrno = attrnos[i] as usize;
            if attrno == 0 {
                return Err(Box::new(PgError::error(format!(
                    "cannot set value in column {} to DEFAULT",
                    i + 1
                ))));
            }
            debug_assert!(attrno <= target_relation.rd_att.natts as usize);
            let att = target_relation.rd_att.attr(attrno - 1);
            // Stored generated columns get the NULL placeholder (C leaves
            // new_expr NULL); the executor recomputes them.
            let default_expr = if !att.attisdropped && att.attgenerated == 0 {
                build_column_default(mcx, target_relation, attrno)?
            } else {
                None
            };
            let new_expr = match default_expr {
                Some(e) => e,
                None => {
                    if is_auto_updatable_view {
                        new_list.lappend(mcx, col)?;
                        all_replaced = false;
                        continue;
                    }
                    coerce::coerce_null_to_domain(
                        mcx,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        att.attlen as i32,
                        att.attbyval,
                    )?
                }
            };
            new_list.lappend(mcx, new_expr)?;
        }
        new_values.lappend(mcx, Node::mk_list(mcx, new_list)?)?;
    }
    // SAFETY: exclusive pre-plan Query fixup; no derived borrow of
    // values_lists is live across this write.
    unsafe {
        rte_node.with_mut::<types_nodes::RangeTblEntry, _>(|r| r.values_lists = new_values)
    }
    .expect("rtable holds RangeTblEntry");
    Ok(all_replaced)
}

// rewriteValuesRTEToNulls (rewriteHandler.c).
fn rewriteValuesRTEToNulls<'mcx>(mcx: Mcx<'mcx>, rte_node: Node<'mcx>) -> PgResult<()> {
    let rte = rte_of(rte_node);
    let mut new_values = types_nodes::NodeList::nil();
    for row in &rte.values_lists {
        let mut new_list = types_nodes::NodeList::nil();
        for col in row.as_list().expect("VALUES row is a List").iter() {
            if col.node_tag() != types_nodes::NodeTag::T_SetToDefault {
                new_list.lappend(mcx, col)?;
                continue;
            }
            let def = col
                .as_variant::<types_nodes::primnodes::SetToDefault>()
                .expect("SetToDefault");
            let (typlen, typbyval) = lsyscache::get_typlenbyval(def.typeId)?;
            new_list.lappend(
                mcx,
                types_nodes::Node::mk_const(
                    mcx,
                    def.typeId,
                    def.typeMod,
                    def.collation,
                    typlen as i32,
                    datum::Datum::null(),
                    true,
                    typbyval,
                )?,
            )?;
        }
        new_values.lappend(mcx, Node::mk_list(mcx, new_list)?)?;
    }
    // SAFETY: exclusive pre-plan Query fixup; no derived borrow of
    // values_lists is live across this write.
    unsafe {
        rte_node.with_mut::<types_nodes::RangeTblEntry, _>(|r| r.values_lists = new_values)
    }
    .expect("rtable holds RangeTblEntry");
    Ok(())
}

// build_column_default (rewriteHandler.c): the stored adbin, else the
// column type's own default (get_typdefault), coerced to the column type;
// None is C's NULL return (no default anywhere).
pub fn build_column_default<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attrno: usize,
) -> PgResult<Option<types_nodes::Node<'mcx>>> {
    let att = rel.rd_att.attr(attrno - 1);
    if att.attidentity != 0 {
        let seqid = pg_depend::getIdentitySequence(mcx, rel.rd_id, attrno as i32, false)?;
        return Ok(Some(types_nodes::Node::mk(
            mcx,
            types_nodes::primnodes::NextValueExpr { seqid, typeId: att.atttypid },
        )?));
    }
    let expr = if att.atthasdef {
        let constr = rel.rd_att.constr.as_deref();
        let adbin = constr
            .and_then(|c| c.defval.iter().find(|d| d.adnum == attrno as i16))
            .and_then(|d| d.adbin.as_ref());
        let adbin = match adbin {
            Some(s) => s,
            None => return Err(default_expression_not_found(attrno, rel)),
        };
        Some(readfuncs::stringToNode(mcx, adbin.as_str())?)
    } else {
        None
    };
    let expr = match expr {
        Some(e) => e,
        None => match lsyscache::get_typdefault(mcx, att.atttypid)? {
            Some(e) => e,
            None => return Ok(None),
        },
    };
    let exprtype = parse_expr::expr_type(expr);
    let pstate = parser_small1::make_parsestate(mcx, None);
    let coerced = coerce::coerce_to_target_type(
        mcx,
        &pstate,
        expr,
        exprtype,
        att.atttypid,
        att.atttypmod,
        coerce::CoercionContext::COERCION_ASSIGNMENT,
        types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    match coerced {
        Some(e) => Ok(Some(e)),
        None => Err(default_type_mismatch(att.attname.name_str(), att.atttypid, exprtype)),
    }
}

// build_generation_expression (rewriteHandler.c:4520).
pub fn build_generation_expression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attrno: usize,
) -> PgResult<Node<'mcx>> {
    let att = rel.rd_att.attr(attrno - 1);
    debug_assert!(att.attgenerated != 0);
    let defexpr = match build_column_default(mcx, rel, attrno)? {
        Some(e) => e,
        None => {
            let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
            return Err(Box::new(PgError::error(format!(
                "no generation expression found for column number {attrno} of table \"{relname}\""
            ))));
        }
    };
    let attcollid = att.attcollation;
    if attcollid != InvalidOid && attcollid != nodes_core::node_funcs::expr_collation(defexpr) {
        return Node::mk(
            mcx,
            types_nodes::primnodes::CollateExpr { arg: defexpr, collOid: attcollid, location: -1 },
        );
    }
    Ok(defexpr)
}

#[track_caller]
#[cold]
#[inline(never)]
fn generated_always_insert_error(
    att: &types_tuple::FormData_pg_attribute,
    identity: bool,
) -> Box<PgError> {
    let name = String::from_utf8_lossy(att.attname.name_str()).into_owned();
    let mut e = PgError::error(format!(
        "cannot insert a non-DEFAULT value into column \"{name}\""
    ))
    .with_sqlstate(types_error::ERRCODE_GENERATED_ALWAYS);
    if identity {
        e = e
            .with_detail(format!(
                "Column \"{name}\" is an identity column defined as GENERATED ALWAYS."
            ))
            .with_hint("Use OVERRIDING SYSTEM VALUE to override.");
    } else {
        e = e.with_detail(format!("Column \"{name}\" is a generated column."));
    }
    Box::new(e)
}

#[track_caller]
#[cold]
#[inline(never)]
fn generated_always_update_error(
    att: &types_tuple::FormData_pg_attribute,
    identity: bool,
) -> Box<PgError> {
    let name = String::from_utf8_lossy(att.attname.name_str()).into_owned();
    let mut e = PgError::error(format!(
        "column \"{name}\" can only be updated to DEFAULT"
    ))
    .with_sqlstate(types_error::ERRCODE_GENERATED_ALWAYS);
    if identity {
        e = e.with_detail(format!(
            "Column \"{name}\" is an identity column defined as GENERATED ALWAYS."
        ));
    } else {
        e = e.with_detail(format!("Column \"{name}\" is a generated column."));
    }
    Box::new(e)
}

#[track_caller]
#[cold]
#[inline(never)]
fn default_expression_not_found(attrno: usize, rel: &types_rel::Relation<'_>) -> Box<PgError> {
    let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    Box::new(PgError::error(format!(
        "default expression not found for attribute {attrno} of relation \"{relname}\""
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn default_type_mismatch(attname: &[u8], atttypid: Oid, exprtype: Oid) -> Box<PgError> {
    let attname = String::from_utf8_lossy(attname).into_owned();
    let want = format_type::format_type_be(atttypid).unwrap_or_else(|_| "???".into());
    let got = format_type::format_type_be(exprtype).unwrap_or_else(|_| "???".into());
    Box::new(
        PgError::error(format!(
            "column \"{attname}\" is of type {want} but default expression is of type {got}"
        ))
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
        .with_hint("You will need to rewrite or cast the expression."),
    )
}

// C sets hasRowSecurity/hasSubLinks/withCheckOptions on each Query in place;
// here each fireRIRrules level RETURNS them and the caller stamps the callee's
// Query through the Node it holds (nested Query headers are re-issued where no
// Node is held — see the RTE_SUBQUERY arm).
struct RirOut<'mcx> {
    has_row_security: bool,
    has_sub_links: bool,
    with_check_options: PgVec<'mcx, Node<'mcx>>,
}

fn stamp_query_flags<'mcx>(
    mcx: Mcx<'mcx>,
    qnode: Node<'mcx>,
    rir: &RirOut<'mcx>,
) -> PgResult<()> {
    if rir.has_row_security || rir.has_sub_links {
        // SAFETY: the rewriter owns the just-analyzed tree single-threaded; no
        // reference derived from `qnode` is live across this write.
        unsafe {
            qnode.with_mut::<Query, _>(|q| {
                q.hasRowSecurity |= rir.has_row_security;
                q.hasSubLinks |= rir.has_sub_links;
            })
        }
        .expect("Query node");
    }
    // RLS WCOs of a nested Query (DML in a CTE) attach to that Query itself,
    // new ones first (C fireRIRrules mutates parsetree->withCheckOptions in
    // place, rewriteHandler.c:2288).
    if !rir.with_check_options.is_empty() {
        let mut wcos = NodeList::from_slice(mcx, &rir.with_check_options)?;
        // SAFETY: same exclusive rewriter ownership as above.
        unsafe {
            qnode.with_mut::<Query, _>(|q| -> PgResult<()> {
                wcos.concat(mcx, &q.withCheckOptions)?;
                q.withCheckOptions = wcos;
                Ok(())
            })
        }
        .expect("Query node")?;
    }
    Ok(())
}

fn fireRIRrules<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    active_rirs: &mut PgVec<'mcx, Oid>,
) -> PgResult<RirOut<'mcx>> {
    let mut out = RirOut {
        has_row_security: false,
        has_sub_links: false,
        with_check_options: PgVec::new_in(mcx),
    };
    // SEARCH/CYCLE expansion precedes the RIR recursion into each ctequery
    // (C runs the expansion loop at the top of fireRIRrules); C copyObject's
    // the CTE and replaces the cell — the arena tree is mutated in place.
    for cte_node in &parsetree.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        if cte.search_clause.is_some() || cte.cycle_clause.is_some() {
            rewrite_search_cycle::rewriteSearchAndCycle(mcx, cte_node)?;
        }
    }
    // C reassigns cte->ctequery = fireRIRrules(...); fireRIRrules returns its
    // argument mutated in place, so the shared-ref recursion is equivalent.
    for cte_node in &parsetree.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let cte_query_node = cte.ctequery.expect("analyzed CTE query");
        let ctequery = cte_query_node.as_query().expect("analyzed CTE query");
        let rir = fireRIRrules(mcx, ctequery, active_rirs)?;
        stamp_query_flags(mcx, cte_query_node, &rir)?;
        out.has_row_security |= rir.has_row_security;
    }
    // The EXCLUDED pseudo-relation must stay RTE_RELATION; never expand it.
    let excl_rel_index = parsetree
        .onConflict
        .and_then(|n| n.as_on_conflict_expr())
        .map(|oc| oc.exclRelIndex)
        .unwrap_or(0);
    let orig_result_relation = parsetree.resultRelation;

    let mut rt_index = 0;
    while rt_index < parsetree.rtable.len() {
        let node = parsetree.rtable.nth(rt_index);
        rt_index += 1;
        let rte = rte_of(node);

        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            let sub = rte.subquery.expect("subquery RTE has a subquery");
            let rir = fireRIRrules(mcx, sub, active_rirs)?;
            if rir.has_row_security || rir.has_sub_links {
                assert!(
                    rir.with_check_options.is_empty(),
                    "fireRIRrules (rewriteHandler.c): RLS WithCheckOptions on a \
                     subquery RTE"
                );
                // No Node handle on rte.subquery: re-issue the Query header
                // with the flags set and re-point the RTE (the old header goes
                // unreferenced; its lists are moved, not aliased live).
                // SAFETY: Query is !Drop arena data; `sub` is bitwise-copied
                // once and the source reference is dead after the re-point.
                let mut q2: Query<'mcx> = unsafe { core::ptr::read(sub) };
                q2.hasRowSecurity |= rir.has_row_security;
                q2.hasSubLinks |= rir.has_sub_links;
                let q2node = Node::mk(mcx, q2)?;
                let q2ref = q2node.as_query().expect("Query node");
                // SAFETY: rewriter-owned tree; no live refs derived from `node`.
                unsafe {
                    node.with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(q2ref))
                };
                out.has_row_security |= rir.has_row_security;
            }
            continue;
        }
        if rte.rtekind != RTEKind::RTE_RELATION {
            continue;
        }
        if rte.relkind == RELKIND_MATVIEW {
            continue;
        }
        if excl_rel_index != 0 && rt_index as i32 == excl_rel_index {
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
        if rel.rd_hasrules {
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
                            &mut out.has_row_security,
                        )?;
                    }
                    active_rirs.pop();
                }
            }
        }
        table::table_close(rel, NoLock)?;
    }

    // fireRIRonSubLink (rewriteHandler.c): recurse into sublink sub-selects.
    // query_tree_walker needs &'mcx Query, so the expression-bearing fields
    // are walked directly (rtable/CTE subqueries were handled above, as C's
    // QTW_IGNORE_RC_SUBQUERIES arranges).
    if parsetree.hasSubLinks {
        struct W<'a, 'mcx> {
            mcx: Mcx<'mcx>,
            active_rirs: &'a mut PgVec<'mcx, Oid>,
            has_row_security: bool,
        }
        impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, 'mcx> {
            fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
                if let Some(sl) = node.as_sub_link() {
                    let sub = sl.subselect.as_query().expect("analyzed sublink sub-select");
                    let rir = fireRIRrules(self.mcx, sub, self.active_rirs)?;
                    stamp_query_flags(self.mcx, sl.subselect, &rir)?;
                    self.has_row_security |= rir.has_row_security;
                }
                nodes_core::expression_tree_walker(node, self)
            }
        }
        fn walk_jt<'mcx>(node: Node<'mcx>, w: &mut W<'_, 'mcx>) -> PgResult<()> {
            match node.node_tag() {
                NodeTag::T_RangeTblRef => {}
                NodeTag::T_FromExpr => {
                    let f = node.as_from_expr().expect("FromExpr");
                    for child in &f.fromlist {
                        walk_jt(child, w)?;
                    }
                    if let Some(q) = f.quals {
                        w.visit(q)?;
                    }
                }
                NodeTag::T_JoinExpr => {
                    let j = node.as_join_expr().expect("JoinExpr");
                    walk_jt(j.larg, w)?;
                    walk_jt(j.rarg, w)?;
                    if let Some(q) = j.quals {
                        w.visit(q)?;
                    }
                }
                other => panic!("fireRIRonSubLink (rewriteHandler.c): {other:?} jointree arm"),
            }
            Ok(())
        }
        use nodes_core::NodeWalker as _;
        let mut w = W { mcx, active_rirs, has_row_security: false };
        for te in &parsetree.targetList {
            w.visit(te)?;
        }
        for wco_node in &parsetree.withCheckOptions {
            let wco = wco_node.as_with_check_option().expect("withCheckOptions cell");
            if let Some(q) = wco.qual {
                w.visit(q)?;
            }
        }
        if let Some(oc_node) = parsetree.onConflict {
            let oc = oc_node.as_on_conflict_expr().expect("OnConflictExpr");
            for n in &oc.arbiterElems {
                w.visit(n)?;
            }
            if let Some(n) = oc.arbiterWhere {
                w.visit(n)?;
            }
            for n in &oc.onConflictSet {
                w.visit(n)?;
            }
            if let Some(n) = oc.onConflictWhere {
                w.visit(n)?;
            }
        }
        if let Some(n) = parsetree.mergeJoinCondition {
            w.visit(n)?;
        }
        for action_node in &parsetree.mergeActionList {
            let action =
                action_node.as_merge_action().expect("mergeActionList cell is a MergeAction");
            if let Some(q) = action.qual {
                w.visit(q)?;
            }
            for te in &action.targetList {
                w.visit(te)?;
            }
        }
        for te in &parsetree.returningList {
            w.visit(te)?;
        }
        if let Some(jt) = parsetree.jointree {
            for item in &jt.fromlist {
                walk_jt(item, &mut w)?;
            }
            if let Some(q) = jt.quals {
                w.visit(q)?;
            }
        }
        if let Some(h) = parsetree.havingQual {
            w.visit(h)?;
        }
        if let Some(n) = parsetree.limitOffset {
            w.visit(n)?;
        }
        if let Some(n) = parsetree.limitCount {
            w.visit(n)?;
        }
        out.has_row_security |= w.has_row_security;
    }

    // Apply row-level security policies last: the new quals need their own
    // recursion detection and must not be re-walked by the sublink pass above.
    let mut rt_index = 0i32;
    for node in parsetree.rtable.iter() {
        rt_index += 1;
        let rte = rte_of(node);
        if rte.rtekind != RTEKind::RTE_RELATION
            || (rte.relkind != RELKIND_RELATION && rte.relkind != RELKIND_PARTITIONED_TABLE)
        {
            continue;
        }
        let rel = table::table_open(mcx, rte.relid, NoLock)?;
        // relrowsecurity=false is check_enable_rls's RLS_NONE outcome
        // (rls.c:78); skipping here spares the perminfo/user probes per RTE.
        if !rel.rd_rel.relrowsecurity {
            table::table_close(rel, NoLock)?;
            continue;
        }

        let rls = rowsecurity::get_row_security_policies(mcx, parsetree, rte, rt_index)?;

        if !rls.security_quals.is_empty() || !rls.with_check_options.is_empty() {
            if rls.has_sub_links {
                if active_rirs.contains(&rte.relid) {
                    let err = infinite_recursion_policy(rel.name());
                    table::table_close(rel, NoLock)?;
                    return Err(err);
                }
                active_rirs.push(rte.relid);

                // securityQuals/withCheckOptions arrive post-parsing: lock any
                // relations their sublinks reference, then fire RIR rules on
                // them (acquireLocksOnSubLinks + fireRIRonSubLink in C).
                struct L<'mcx> {
                    mcx: Mcx<'mcx>,
                }
                impl<'mcx> nodes_core::NodeWalker<'mcx> for L<'mcx> {
                    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
                        if node.node_tag() == NodeTag::T_Query {
                            // AcquireRewriteLocks already covers nested levels.
                            return Ok(false);
                        }
                        if let Some(sl) = node.as_sub_link() {
                            let sub =
                                sl.subselect.as_query().expect("analyzed sublink sub-select");
                            AcquireRewriteLocks(self.mcx, sub, true, false)?;
                        }
                        nodes_core::expression_tree_walker(node, self)
                    }
                }
                struct F<'a, 'mcx> {
                    mcx: Mcx<'mcx>,
                    active_rirs: &'a mut PgVec<'mcx, Oid>,
                }
                impl<'mcx> nodes_core::NodeWalker<'mcx> for F<'_, 'mcx> {
                    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
                        if let Some(sl) = node.as_sub_link() {
                            let sub =
                                sl.subselect.as_query().expect("analyzed sublink sub-select");
                            let rir = fireRIRrules(self.mcx, sub, self.active_rirs)?;
                            // has_row_security discard: only reachable with the
                            // RTE's own has_row_security already true (C asserts).
                            stamp_query_flags(self.mcx, sl.subselect, &rir)?;
                        }
                        nodes_core::expression_tree_walker(node, self)
                    }
                }
                use nodes_core::NodeWalker as _;
                let mut l = L { mcx };
                for &q in rls.security_quals.iter() {
                    l.visit(q)?;
                }
                for &wnode in rls.with_check_options.iter() {
                    l.visit(wnode)?;
                }
                let mut f = F { mcx, active_rirs: &mut *active_rirs };
                for &q in rls.security_quals.iter() {
                    f.visit(q)?;
                }
                for &wnode in rls.with_check_options.iter() {
                    f.visit(wnode)?;
                }

                active_rirs.pop();
            }

            if !rls.security_quals.is_empty() {
                // New RLS quals go before existing (security-barrier view)
                // quals so they get applied first.
                let mut quals = NodeList::from_slice(mcx, &rls.security_quals)?;
                // SAFETY: rewriter-owned tree; no live refs derived from `node`.
                unsafe {
                    node.with_mut::<RangeTblEntry, _>(|r| -> PgResult<()> {
                        quals.concat(mcx, &r.securityQuals)?;
                        r.securityQuals = quals;
                        Ok(())
                    })
                }
                .expect("RangeTblEntry node")?;
            }
            for &wnode in rls.with_check_options.iter() {
                out.with_check_options.push(wnode);
            }
        }

        if rls.has_row_security {
            out.has_row_security = true;
        }
        if rls.has_sub_links {
            out.has_sub_links = true;
        }

        table::table_close(rel, NoLock)?;
    }

    Ok(out)
}

#[track_caller]
#[cold]
#[inline(never)]
fn infinite_recursion_policy(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "infinite recursion detected in policy for relation \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

// view_has_instead_trigger (rewriteHandler.c). For MERGE: true iff every
// data-modifying action has an INSTEAD OF trigger (no actions => true).
fn view_has_instead_trigger(
    rel: &Relation<'_>,
    event: CmdType,
    merge_action_list: &NodeList<'_>,
) -> PgResult<bool> {
    let (ins, upd, del) = instead_trigger_flags(rel)?;
    Ok(match event {
        CmdType::CMD_INSERT => ins,
        CmdType::CMD_UPDATE => upd,
        CmdType::CMD_DELETE => del,
        CmdType::CMD_MERGE => {
            for action_node in merge_action_list {
                let action = action_node
                    .as_merge_action()
                    .expect("mergeActionList cell is a MergeAction");
                let ok = match action.commandType {
                    CmdType::CMD_INSERT => ins,
                    CmdType::CMD_UPDATE => upd,
                    CmdType::CMD_DELETE => del,
                    CmdType::CMD_NOTHING => true,
                    other => panic!("unrecognized commandType: {other:?}"),
                };
                if !ok {
                    return Ok(false);
                }
            }
            true
        }
        other => panic!("unrecognized CmdType: {other:?}"),
    })
}

fn instead_trigger_flags(rel: &Relation<'_>) -> PgResult<(bool, bool, bool)> {
    if !rel.rd_hastriggers {
        return Ok((false, false, false));
    }
    Ok(match relcache::RelationGetTriggerDesc(rel.rd_id)? {
        Some(td) => (
            td.trig_insert_instead_row,
            td.trig_update_instead_row,
            td.trig_delete_instead_row,
        ),
        None => (false, false, false),
    })
}

// get_view_query (rewriteHandler.c). C returns a read-only relcache pointer;
// the text cache re-reads, so the result is a fresh tree the caller owns.
pub fn get_view_query<'mcx>(
    mcx: Mcx<'mcx>,
    view: &Relation<'mcx>,
) -> PgResult<&'mcx Query<'mcx>> {
    debug_assert_eq!(view.rd_rel.relkind, RELKIND_VIEW);
    if let Some(rules) = relcache::RelationGetRules(mcx, view.rd_id)? {
        for rule in rules.rules.iter() {
            if rule.event == CmdType::CMD_SELECT as i32 {
                let actions_node = readfuncs::stringToNode(mcx, rule.action_src.as_str())?;
                let actions = actions_node.as_list().expect("ev_action is a List");
                if actions.len() != 1 {
                    return Err(internal_error("invalid _RETURN rule action specification"));
                }
                return Ok(actions.nth(0).as_query().expect("rule action is a Query"));
            }
        }
    }
    Err(internal_error("failed to find _RETURN rule for view"))
}

fn view_col_is_auto_updatable(
    rtr_index: i32,
    tle: &types_nodes::primnodes::TargetEntry<'_>,
) -> Option<&'static str> {
    if tle.resjunk {
        return Some("Junk view columns are not updatable.");
    }
    let Some(var) = tle.expr.as_var() else {
        return Some(
            "View columns that are not columns of their base relation are not updatable.",
        );
    };
    if var.varno != rtr_index || var.varlevelsup != 0 {
        return Some(
            "View columns that are not columns of their base relation are not updatable.",
        );
    }
    if var.varattno < 0 {
        return Some("View columns that refer to system columns are not updatable.");
    }
    if var.varattno == 0 {
        return Some("View columns that return whole-row references are not updatable.");
    }
    None
}

pub fn view_query_is_auto_updatable(
    viewquery: &Query<'_>,
    check_cols: bool,
) -> Option<&'static str> {
    if !viewquery.distinctClause.is_nil() {
        return Some("Views containing DISTINCT are not automatically updatable.");
    }
    if !viewquery.groupClause.is_nil() || !viewquery.groupingSets.is_nil() {
        return Some("Views containing GROUP BY are not automatically updatable.");
    }
    if viewquery.havingQual.is_some() {
        return Some("Views containing HAVING are not automatically updatable.");
    }
    if viewquery.setOperations.is_some() {
        return Some(
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.",
        );
    }
    if !viewquery.cteList.is_nil() {
        return Some("Views containing WITH are not automatically updatable.");
    }
    if viewquery.limitOffset.is_some() || viewquery.limitCount.is_some() {
        return Some("Views containing LIMIT or OFFSET are not automatically updatable.");
    }
    if viewquery.hasAggs {
        return Some("Views that return aggregate functions are not automatically updatable.");
    }
    if viewquery.hasWindowFuncs {
        return Some("Views that return window functions are not automatically updatable.");
    }
    if viewquery.hasTargetSRFs {
        return Some(
            "Views that return set-returning functions are not automatically updatable.",
        );
    }

    const NOT_SINGLE_TABLE: &str =
        "Views that do not select from a single table or view are not automatically updatable.";
    let Some(jt) = viewquery.jointree else {
        return Some(NOT_SINGLE_TABLE);
    };
    if jt.fromlist.len() != 1 {
        return Some(NOT_SINGLE_TABLE);
    }
    let Some(rtr) = jt.fromlist.nth(0).as_range_tbl_ref() else {
        return Some(NOT_SINGLE_TABLE);
    };
    let base_rte = rte_of(viewquery.rtable.nth(rtr.rtindex as usize - 1));
    if base_rte.rtekind != RTEKind::RTE_RELATION
        || (base_rte.relkind != RELKIND_RELATION
            && base_rte.relkind != RELKIND_FOREIGN_TABLE
            && base_rte.relkind != RELKIND_VIEW
            && base_rte.relkind != RELKIND_PARTITIONED_TABLE)
    {
        return Some(NOT_SINGLE_TABLE);
    }
    if base_rte.tablesample.is_some() {
        return Some("Views containing TABLESAMPLE are not automatically updatable.");
    }

    if check_cols {
        let mut found = false;
        for tle_node in &viewquery.targetList {
            let tle = tle_node.as_target_entry().expect("targetlist cell");
            if view_col_is_auto_updatable(rtr.rtindex, tle).is_none() {
                found = true;
                break;
            }
        }
        if !found {
            return Some(
                "Views that have no updatable columns are not automatically updatable.",
            );
        }
    }

    None
}

fn view_cols_are_auto_updatable<'mcx>(
    mcx: Mcx<'mcx>,
    viewquery: &Query<'mcx>,
    required_cols: Option<&Bitmapset<'_>>,
    mut updatable_cols: Option<&mut Bitmapset<'mcx>>,
    non_updatable_col: &mut Option<std::string::String>,
) -> PgResult<Option<&'static str>> {
    let jt = viewquery.jointree.expect("auto-updatable view has a jointree");
    debug_assert_eq!(jt.fromlist.len(), 1);
    let rtr = jt.fromlist.nth(0).as_range_tbl_ref().expect("auto-updatable view fromlist");

    *non_updatable_col = None;

    let mut col = -FirstLowInvalidHeapAttributeNumber;
    for tle_node in &viewquery.targetList {
        let tle = tle_node.as_target_entry().expect("targetlist cell");
        col += 1;
        match view_col_is_auto_updatable(rtr.rtindex, tle) {
            None => {
                if let Some(set) = updatable_cols.as_deref_mut() {
                    set.add_member(mcx, col)?;
                }
            }
            Some(detail) => {
                if required_cols.is_some_and(|rc| rc.is_member(col)) {
                    *non_updatable_col = tle.resname.map(|s| s.to_string());
                    return Ok(Some(detail));
                }
            }
        }
    }

    Ok(None)
}

fn get_tle_by_resno<'mcx>(
    tlist: &NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx types_nodes::primnodes::TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("targetlist cell"))
        .find(|t| t.resno == resno)
}

// adjust_view_column_set (rewriteHandler.c): map view column numbers (offset
// by FirstLowInvalidHeapAttributeNumber) onto base-relation columns.
fn adjust_view_column_set<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &Bitmapset<'_>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut result = Bitmapset::empty();
    let mut col = -1;
    loop {
        col = cols.next_member(col);
        if col < 0 {
            break;
        }
        let attno = col + FirstLowInvalidHeapAttributeNumber;
        if attno == 0 {
            // Whole-row reference to the view: a reference to each column
            // available from the view (not the base relation).
            for tle_node in targetlist {
                let tle = tle_node.as_target_entry().expect("targetlist cell");
                if tle.resjunk {
                    continue;
                }
                let var = tle.expr.as_var().expect("view tlist entry is a Var");
                result.add_member(
                    mcx,
                    var.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                )?;
            }
        } else {
            match get_tle_by_resno(targetlist, attno as i16) {
                Some(tle) if !tle.resjunk && tle.expr.as_var().is_some() => {
                    let var = tle.expr.as_var().expect("just checked");
                    result.add_member(
                        mcx,
                        var.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                    )?;
                }
                _ => {
                    return Err(internal_error(&format!(
                        "attribute number {attno} not found in view targetlist"
                    )))
                }
            }
        }
    }
    Ok(result)
}

// error_view_not_updatable (rewriteHandler.c); builds (never raises) the error.
#[cold]
#[inline(never)]
fn error_view_not_updatable(
    view: &Relation<'_>,
    command: CmdType,
    merge_action_list: &NodeList<'_>,
    detail: Option<&str>,
) -> PgResult<Box<PgError>> {
    let name = view.name();
    let mk = |msg: std::string::String, hint: &str| -> Box<PgError> {
        let mut e = PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        if let Some(d) = detail {
            e = e.with_detail(d.to_string());
        }
        Box::new(e.with_hint(hint.to_string()))
    };
    Ok(match command {
        CmdType::CMD_INSERT => mk(
            format!("cannot insert into view \"{name}\""),
            "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.",
        ),
        CmdType::CMD_UPDATE => mk(
            format!("cannot update view \"{name}\""),
            "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule.",
        ),
        CmdType::CMD_DELETE => mk(
            format!("cannot delete from view \"{name}\""),
            "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule.",
        ),
        CmdType::CMD_MERGE => {
            let (ins, upd, del) = instead_trigger_flags(view)?;
            for action_node in merge_action_list {
                let action = action_node
                    .as_merge_action()
                    .expect("mergeActionList cell is a MergeAction");
                match action.commandType {
                    CmdType::CMD_INSERT if !ins => {
                        return Ok(mk(
                            format!("cannot insert into view \"{name}\""),
                            "To enable inserting into the view using MERGE, provide an INSTEAD OF INSERT trigger.",
                        ))
                    }
                    CmdType::CMD_UPDATE if !upd => {
                        return Ok(mk(
                            format!("cannot update view \"{name}\""),
                            "To enable updating the view using MERGE, provide an INSTEAD OF UPDATE trigger.",
                        ))
                    }
                    CmdType::CMD_DELETE if !del => {
                        return Ok(mk(
                            format!("cannot delete from view \"{name}\""),
                            "To enable deleting from the view using MERGE, provide an INSTEAD OF DELETE trigger.",
                        ))
                    }
                    CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE
                    | CmdType::CMD_NOTHING => {}
                    other => panic!("unrecognized commandType: {other:?}"),
                }
            }
            // view_has_instead_trigger guarantees an action lacked a trigger.
            internal_error(&format!("cannot merge into view \"{name}\""))
        }
        other => panic!("unrecognized CmdType: {other:?}"),
    })
}

// relation_is_updatable (rewriteHandler.c): bitmask of 1<<CMD_* events the
// relation supports. Exported for the pg_relation_is_updatable /
// pg_column_is_updatable fmgr builtins (not yet ported).
pub fn relation_is_updatable<'mcx>(
    mcx: Mcx<'mcx>,
    reloid: Oid,
    outer_reloids: &mut PgVec<'mcx, Oid>,
    include_triggers: bool,
    include_cols: Option<&Bitmapset<'_>>,
) -> PgResult<i32> {
    const ALL_EVENTS: i32 = (1 << CmdType::CMD_INSERT as i32)
        | (1 << CmdType::CMD_UPDATE as i32)
        | (1 << CmdType::CMD_DELETE as i32);
    let mut events = 0;

    let Some(rel) = relation::try_relation_open(mcx, reloid, AccessShareLock)? else {
        return Ok(0);
    };

    if outer_reloids.contains(&rel.rd_id) {
        rel.close(AccessShareLock)?;
        return Ok(0);
    }

    if rel.rd_rel.relkind == RELKIND_RELATION
        || rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
    {
        rel.close(AccessShareLock)?;
        return Ok(ALL_EVENTS);
    }

    if rel.rd_hasrules {
        if let Some(rules) = relcache::RelationGetRules(mcx, rel.rd_id)? {
            for rule in rules.rules.iter() {
                if rule.is_instead && rule.qual_src.is_none() {
                    events |= (1 << rule.event) & ALL_EVENTS;
                }
            }
            if events == ALL_EVENTS {
                rel.close(AccessShareLock)?;
                return Ok(events);
            }
        }
    }

    if include_triggers {
        let (ins, upd, del) = instead_trigger_flags(&rel)?;
        if ins {
            events |= 1 << CmdType::CMD_INSERT as i32;
        }
        if upd {
            events |= 1 << CmdType::CMD_UPDATE as i32;
        }
        if del {
            events |= 1 << CmdType::CMD_DELETE as i32;
        }
        if events == ALL_EVENTS {
            rel.close(AccessShareLock)?;
            return Ok(events);
        }
    }

    if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        // C derives events from GetFdwRoutineForRelation; no FDW handler is
        // invocable yet, so the live surface is the no-handler error
        // (plancat.rs precedent) and an installed handler stays loud.
        foreigncmds_seams::get_fdw_routine_by_rel_id::call(mcx, rel.rd_id)?;
        unreachable!("get_fdw_routine_by_rel_id returned");
    }

    if rel.rd_rel.relkind == RELKIND_VIEW {
        let viewquery = get_view_query(mcx, &rel)?;
        if view_query_is_auto_updatable(viewquery, false).is_none() {
            let mut updatable_cols = Bitmapset::empty();
            let mut non_updatable_col = None;
            view_cols_are_auto_updatable(
                mcx,
                viewquery,
                None,
                Some(&mut updatable_cols),
                &mut non_updatable_col,
            )?;
            if let Some(ic) = include_cols {
                updatable_cols.int_members(ic);
            }
            let mut auto_events = if updatable_cols.is_empty() {
                1 << CmdType::CMD_DELETE as i32
            } else {
                ALL_EVENTS
            };

            let jt = viewquery.jointree.expect("auto-updatable view has a jointree");
            let rtr = jt.fromlist.nth(0).as_range_tbl_ref().expect("auto-updatable view fromlist");
            let base_rte = rte_of(viewquery.rtable.nth(rtr.rtindex as usize - 1));
            debug_assert_eq!(base_rte.rtekind, RTEKind::RTE_RELATION);

            if base_rte.relkind != RELKIND_RELATION
                && base_rte.relkind != RELKIND_PARTITIONED_TABLE
            {
                let baseoid = base_rte.relid;
                outer_reloids.push(rel.rd_id);
                let inc = adjust_view_column_set(mcx, &updatable_cols, &viewquery.targetList)?;
                auto_events &= relation_is_updatable(
                    mcx,
                    baseoid,
                    outer_reloids,
                    include_triggers,
                    Some(&inc),
                )?;
                outer_reloids.pop();
            }
            events |= auto_events;
        }
    }

    rel.close(AccessShareLock)?;
    Ok(events)
}

// rewriteTargetView (rewriteHandler.c): rewrite DML on an auto-updatable view
// so the view's base relation becomes the target relation.
fn rewriteTargetView<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    view: &Relation<'mcx>,
) -> PgResult<Query<'mcx>> {
    use rewrite_manip::{ReplaceVarsFromTargetList, ReplaceVarsNoMatchOption};
    use types_nodes::primnodes::{OnConflictAction, OnConflictExpr, TargetEntry};

    let viewquery = get_view_query(mcx, view)?;

    let view_result_relation = parsetree.resultRelation;
    let view_rte_node = parsetree.rtable.nth(view_result_relation as usize - 1);
    let view_perminfo_node =
        parse_relation::getRTEPermissionInfo(&parsetree.rteperminfos, rte_of(view_rte_node))?;
    let view_perminfo =
        view_perminfo_node.as_rte_permission_info().expect("RTEPermissionInfo");

    let mut insert_or_update = parsetree.commandType == CmdType::CMD_INSERT
        || parsetree.commandType == CmdType::CMD_UPDATE;
    if parsetree.commandType == CmdType::CMD_MERGE {
        for action_node in &parsetree.mergeActionList {
            let action =
                action_node.as_merge_action().expect("mergeActionList cell is a MergeAction");
            if action.commandType == CmdType::CMD_INSERT
                || action.commandType == CmdType::CMD_UPDATE
            {
                insert_or_update = true;
                break;
            }
        }
    }

    check_view_expansion_restricted(view)?;

    if let Some(detail) = view_query_is_auto_updatable(viewquery, insert_or_update) {
        return Err(error_view_not_updatable(
            view,
            parsetree.commandType,
            &parsetree.mergeActionList,
            Some(detail),
        )?);
    }

    if insert_or_update {
        let mut modified_cols =
            view_perminfo.insertedCols.union(&view_perminfo.updatedCols, mcx)?;
        for tle_node in &parsetree.targetList {
            let tle = tle_node.as_target_entry().expect("targetlist cell");
            if !tle.resjunk {
                modified_cols
                    .add_member(mcx, tle.resno as i32 - FirstLowInvalidHeapAttributeNumber)?;
            }
        }
        if let Some(oc_node) = parsetree.onConflict {
            let oc = oc_node.as_on_conflict_expr().expect("OnConflictExpr");
            for tle_node in &oc.onConflictSet {
                let tle = tle_node.as_target_entry().expect("targetlist cell");
                if !tle.resjunk {
                    modified_cols
                        .add_member(mcx, tle.resno as i32 - FirstLowInvalidHeapAttributeNumber)?;
                }
            }
        }
        for action_node in &parsetree.mergeActionList {
            let action =
                action_node.as_merge_action().expect("mergeActionList cell is a MergeAction");
            if action.commandType == CmdType::CMD_INSERT
                || action.commandType == CmdType::CMD_UPDATE
            {
                for tle_node in &action.targetList {
                    let tle = tle_node.as_target_entry().expect("targetlist cell");
                    if !tle.resjunk {
                        modified_cols.add_member(
                            mcx,
                            tle.resno as i32 - FirstLowInvalidHeapAttributeNumber,
                        )?;
                    }
                }
            }
        }

        let mut non_updatable_col = None;
        if let Some(detail) = view_cols_are_auto_updatable(
            mcx,
            viewquery,
            Some(&modified_cols),
            None,
            &mut non_updatable_col,
        )? {
            let col = non_updatable_col.unwrap_or_default();
            let view_name = view.name();
            let msg = match parsetree.commandType {
                CmdType::CMD_INSERT => {
                    format!("cannot insert into column \"{col}\" of view \"{view_name}\"")
                }
                CmdType::CMD_UPDATE => {
                    format!("cannot update column \"{col}\" of view \"{view_name}\"")
                }
                CmdType::CMD_MERGE => {
                    format!("cannot merge into column \"{col}\" of view \"{view_name}\"")
                }
                other => panic!("unrecognized CmdType: {other:?}"),
            };
            return Err(Box::new(
                PgError::error(msg)
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail(detail.to_string()),
            ));
        }
    }

    // MERGE must not mix auto-update and trigger-update actions.
    if parsetree.commandType == CmdType::CMD_MERGE {
        for action_node in &parsetree.mergeActionList {
            let action =
                action_node.as_merge_action().expect("mergeActionList cell is a MergeAction");
            if action.commandType != CmdType::CMD_NOTHING
                && view_has_instead_trigger(view, action.commandType, &NodeList::nil())?
            {
                return Err(Box::new(
                    PgError::error(format!("cannot merge into view \"{}\"", view.name()))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .with_detail(
                            "MERGE is not supported for views with INSTEAD OF triggers for some actions but not all.",
                        )
                        .with_hint(
                            "To enable merging into the view, either provide a full set of INSTEAD OF triggers or drop the existing INSTEAD OF triggers.",
                        ),
                ));
            }
        }
    }

    let jt = viewquery.jointree.expect("auto-updatable view has a jointree");
    debug_assert_eq!(jt.fromlist.len(), 1);
    let rtr = jt.fromlist.nth(0).as_range_tbl_ref().expect("auto-updatable view fromlist");
    let base_rt_index = rtr.rtindex;
    let base_rte_node = viewquery.rtable.nth(base_rt_index as usize - 1);
    debug_assert_eq!(rte_of(base_rte_node).rtekind, RTEKind::RTE_RELATION);
    let base_perminfo_node =
        parse_relation::getRTEPermissionInfo(&viewquery.rteperminfos, rte_of(base_rte_node))?;
    let base_perminfo =
        base_perminfo_node.as_rte_permission_info().expect("RTEPermissionInfo");

    // The base relation becomes the query target: RowExclusiveLock, and the
    // subsequent recursive RewriteQuery relies on the lock being held.
    let base_rel = table::table_open(mcx, rte_of(base_rte_node).relid, RowExclusiveLock)?;

    let is_insert = parsetree.commandType == CmdType::CMD_INSERT;
    // SAFETY: viewquery is a fresh stringToNode tree owned by the rewriter.
    unsafe {
        base_rte_node.with_mut::<RangeTblEntry, _>(|r| {
            r.relkind = base_rel.rd_rel.relkind;
            r.rellockmode = RowExclusiveLock;
            if is_insert {
                r.inh = false;
            }
            r.perminfoindex = 0;
        })
    }
    .expect("RangeTblEntry");

    if viewquery.hasSubLinks {
        let mut w = LocksOnSubLinks { mcx, for_execute: true };
        nodes_core::query_tree_walker(viewquery, &mut w, nodes_core::QTW_IGNORE_RC_SUBQUERIES)?;
    }

    // The base RTE moves into the outer rangetable (C scribbles on the copy).
    parsetree.rtable.lappend(mcx, base_rte_node)?;
    let new_rt_index = parsetree.rtable.len() as i32;

    let view_targetlist = &viewquery.targetList;
    for tle_node in view_targetlist {
        rewrite_manip::ChangeVarNodes(mcx, tle_node, base_rt_index, new_rt_index, 0)?;
    }

    let new_perminfo_node = unsafe {
        base_rte_node.with_mut::<RangeTblEntry, _>(|r| {
            parse_relation::addRTEPermissionInfo(mcx, &mut parsetree.rteperminfos, r)
        })
    }
    .expect("RangeTblEntry")?;
    {
        let check_as_user = if view
            .rd_options
            .as_ref()
            .and_then(|o| o.view())
            .is_some_and(|v| v.security_invoker)
        {
            InvalidOid
        } else {
            view.rd_rel.relowner
        };
        let selected = base_perminfo.selectedCols.clone_in(mcx)?;
        let inserted =
            adjust_view_column_set(mcx, &view_perminfo.insertedCols, view_targetlist)?;
        let updated = adjust_view_column_set(mcx, &view_perminfo.updatedCols, view_targetlist)?;
        let required_perms = view_perminfo.requiredPerms;
        // SAFETY: perminfo node created just above; no derived refs live.
        unsafe {
            new_perminfo_node.with_mut::<RTEPermissionInfo, _>(|p| {
                debug_assert!(p.insertedCols.is_empty() && p.updatedCols.is_empty());
                p.checkAsUser = check_as_user;
                p.requiredPerms = required_perms;
                p.selectedCols = selected;
                p.insertedCols = inserted;
                p.updatedCols = updated;
            })
        }
        .expect("RTEPermissionInfo");
    }

    // Move any security-barrier quals from the view RTE onto the new target.
    {
        // SAFETY: rewriter-owned tree; no derived refs live across the writes.
        let quals = unsafe {
            view_rte_node
                .with_mut::<RangeTblEntry, _>(|r| core::mem::take(&mut r.securityQuals))
        }
        .expect("RangeTblEntry");
        unsafe { base_rte_node.with_mut::<RangeTblEntry, _>(|r| r.securityQuals = quals) }
            .expect("RangeTblEntry");
    }

    let pnode = Node::mk(mcx, parsetree)?;
    ReplaceVarsFromTargetList(
        mcx,
        pnode,
        view_result_relation,
        0,
        rte_of(view_rte_node),
        view_targetlist,
        new_rt_index,
        ReplaceVarsNoMatchOption::ReportError,
        None,
    )?;
    rewrite_manip::ChangeVarNodes(mcx, pnode, view_result_relation, new_rt_index, 0)?;
    debug_assert_eq!(pnode.as_query().expect("Query").resultRelation, new_rt_index);

    // Re-point INSERT/UPDATE (and MERGE action) targetlist resnos at base-rel
    // columns; the recursion's rewriteTargetListIU restores resno order.
    let remap_resnos = |tlist: &NodeList<'mcx>| -> PgResult<()> {
        for tle_node in tlist {
            let tle = tle_node.as_target_entry().expect("targetlist cell");
            if tle.resjunk {
                continue;
            }
            let attno = match get_tle_by_resno(view_targetlist, tle.resno) {
                Some(vt) if !vt.resjunk && vt.expr.as_var().is_some() => {
                    vt.expr.as_var().expect("just checked").varattno
                }
                _ => {
                    return Err(internal_error(&format!(
                        "attribute number {} not found in view targetlist",
                        tle.resno
                    )))
                }
            };
            // SAFETY: rewriter-owned tree.
            unsafe { tle_node.with_mut::<TargetEntry, _>(|t| t.resno = attno) }
                .expect("TargetEntry");
        }
        Ok(())
    };
    if pnode.as_query().expect("Query").commandType != CmdType::CMD_DELETE {
        let q = pnode.as_query().expect("Query");
        remap_resnos(&q.targetList)?;
        for action_node in &q.mergeActionList {
            let action =
                action_node.as_merge_action().expect("mergeActionList cell is a MergeAction");
            if action.commandType == CmdType::CMD_INSERT
                || action.commandType == CmdType::CMD_UPDATE
            {
                remap_resnos(&action.targetList)?;
            }
        }
    }

    // INSERT .. ON CONFLICT .. DO UPDATE: re-point the auxiliary UPDATE
    // targetlist and rebuild the EXCLUDED pseudo-relation over the base rel.
    let oc_is_update = pnode
        .as_query()
        .expect("Query")
        .onConflict
        .and_then(|n| n.as_on_conflict_expr())
        .is_some_and(|oc| oc.action == OnConflictAction::ONCONFLICT_UPDATE);
    if oc_is_update {
        let oc_node = pnode.as_query().expect("Query").onConflict.expect("onConflict");
        let oc = oc_node.as_on_conflict_expr().expect("OnConflictExpr");
        remap_resnos(&oc.onConflictSet)?;

        let old_excl_rel_index = oc.exclRelIndex;

        let excl_alias = mcx::alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias { aliasname: Some("excluded"), colnames: NodeList::nil() },
        )?;
        let mut excl_pstate = parser_small1::make_parsestate(mcx, None);
        parse_relation::addRangeTableEntryForRelation(
            mcx,
            &mut excl_pstate,
            &base_rel,
            RowExclusiveLock,
            Some(excl_alias),
            false,
            false,
        )?;
        let new_excl_rte_node = excl_pstate.p_rtable.nth(excl_pstate.p_rtable.len() - 1);
        // Composite relkind signals a pseudo-relation; drop the perminfo the
        // throwaway ParseState collected.
        // SAFETY: RTE node built just above.
        unsafe {
            new_excl_rte_node.with_mut::<RangeTblEntry, _>(|r| {
                r.relkind = RELKIND_COMPOSITE_TYPE;
                r.perminfoindex = 0;
            })
        }
        .expect("RangeTblEntry");
        // SAFETY: rewriter-owned tree.
        unsafe {
            pnode.with_mut::<Query, _>(|q| q.rtable.lappend(mcx, new_excl_rte_node))
        }
        .expect("Query")?;
        let new_excl_rel_index = pnode.as_query().expect("Query").rtable.len() as i32;

        let new_excl_tlist =
            parser_analyze::BuildOnConflictExcludedTargetlist(mcx, &base_rel, new_excl_rel_index)?;
        // SAFETY: rewriter-owned tree.
        unsafe {
            oc_node.with_mut::<OnConflictExpr, _>(|o| {
                o.exclRelIndex = new_excl_rel_index;
                o.exclRelTlist = new_excl_tlist;
            })
        }
        .expect("OnConflictExpr");

        let tmp_tlist = rewrite_manip::copy_node_list(mcx, view_targetlist)?;
        for tle_node in &tmp_tlist {
            rewrite_manip::ChangeVarNodes(mcx, tle_node, new_rt_index, new_excl_rel_index, 0)?;
        }

        let mut has_sublinks = pnode.as_query().expect("Query").hasSubLinks;
        let new_oc_node = ReplaceVarsFromTargetList(
            mcx,
            oc_node,
            old_excl_rel_index,
            0,
            rte_of(view_rte_node),
            &tmp_tlist,
            new_rt_index,
            ReplaceVarsNoMatchOption::ReportError,
            Some(&mut has_sublinks),
        )?;
        // SAFETY: rewriter-owned tree.
        unsafe {
            pnode.with_mut::<Query, _>(|q| {
                q.onConflict = Some(new_oc_node);
                q.hasSubLinks = has_sublinks;
            })
        }
        .expect("Query");
    }

    // For UPDATE/DELETE/MERGE pull up the view's WHERE quals (security-barrier
    // views attach them as security quals instead); INSERT ignores them.
    let view_quals = viewquery.jointree.and_then(|j| j.quals);
    let command_type = pnode.as_query().expect("Query").commandType;
    if command_type != CmdType::CMD_INSERT {
        if let Some(quals) = view_quals {
            let viewqual = rewrite_manip::copy_node(mcx, quals)?;
            rewrite_manip::ChangeVarNodes(mcx, viewqual, base_rt_index, new_rt_index, 0)?;

            let is_security_view = view
                .rd_options
                .as_ref()
                .and_then(|o| o.view())
                .is_some_and(|v| v.security_barrier);
            if is_security_view {
                // The view's quals go in front of existing barrier quals (an
                // outer security-barrier view's quals evaluate later).
                let new_rte_node =
                    pnode.as_query().expect("Query").rtable.nth(new_rt_index as usize - 1);
                let mut sq = NodeList::nil();
                sq.lappend(mcx, viewqual)?;
                // SAFETY: rewriter-owned tree.
                unsafe {
                    new_rte_node.with_mut::<RangeTblEntry, _>(|r| -> PgResult<()> {
                        sq.concat(mcx, &r.securityQuals)?;
                        r.securityQuals = sq;
                        Ok(())
                    })
                }
                .expect("RangeTblEntry")?;
                if !pnode.as_query().expect("Query").hasSubLinks
                    && rewrite_manip::checkExprHasSubLink(viewqual)?
                {
                    // SAFETY: rewriter-owned tree.
                    unsafe { pnode.with_mut::<Query, _>(|q| q.hasSubLinks = true) }
                        .expect("Query");
                }
            } else {
                rewrite_manip::AddQual(mcx, pnode, Some(viewqual))?;
            }
        }
    }

    // WITH CHECK OPTION: prepend a WCO_VIEW_CHECK (inner views check first).
    if insert_or_update {
        let view_opts = view.rd_options.as_ref().and_then(|o| o.view());
        let mut has_wco = view_opts
            .is_some_and(|v| v.check_option != VIEW_OPTION_CHECK_OPTION_NOT_SET);
        let mut cascaded =
            view_opts.is_some_and(|v| v.check_option == VIEW_OPTION_CHECK_OPTION_CASCADED);

        // A cascaded parent check makes this view cascaded too; new WCOs go at
        // the list head, so a cascaded parent is the first item.
        {
            let q = pnode.as_query().expect("Query");
            if !q.withCheckOptions.is_nil() {
                let parent = q
                    .withCheckOptions
                    .nth(0)
                    .as_with_check_option()
                    .expect("withCheckOptions cell");
                if parent.cascaded {
                    has_wco = true;
                    cascaded = true;
                }
            }
        }

        // A CASCADED check is added even without quals (child views may have
        // them); a LOCAL check without quals is omitted.
        if has_wco && (cascaded || view_quals.is_some()) {
            let mut qual = None;
            let mut added_sublink = false;
            if let Some(quals) = view_quals {
                rewrite_manip::ChangeVarNodes(mcx, quals, base_rt_index, new_rt_index, 0)?;
                // UPDATE/MERGE already added the same qual above and did the
                // sublink check there.
                if command_type == CmdType::CMD_INSERT {
                    added_sublink = rewrite_manip::checkExprHasSubLink(quals)?;
                }
                qual = Some(quals);
            }
            let wco_node = Node::mk(
                mcx,
                WithCheckOption {
                    kind: WCOKind::WCO_VIEW_CHECK,
                    relname: Some(mcx_str(mcx, view.name())?),
                    polname: None,
                    qual,
                    cascaded,
                },
            )?;
            let mut new_wcos = NodeList::nil();
            new_wcos.lappend(mcx, wco_node)?;
            // SAFETY: rewriter-owned tree.
            unsafe {
                pnode.with_mut::<Query, _>(|q| -> PgResult<()> {
                    new_wcos.concat(mcx, &q.withCheckOptions)?;
                    q.withCheckOptions = new_wcos;
                    if added_sublink {
                        q.hasSubLinks = true;
                    }
                    Ok(())
                })
            }
            .expect("Query")?;
        }
    }

    table::table_close(base_rel, NoLock)?;

    // SAFETY: Query is !Drop arena data; the node header goes unreferenced
    // once the value is moved out.
    Ok(unsafe { core::ptr::read(pnode.as_query().expect("Query")) })
}

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

// The fireRIRrules/ApplyRetrieveRule result-relation view arm
// (rewriteHandler.c:1737-1805), hoisted to run on the owned Query before
// fireRIRrules: UPDATE/DELETE/MERGE on a view append an unexpanded target
// copy of the view RTE (needed regardless of INSTEAD OF triggers, so the
// original RTE can be expanded for read access), re-point RETURNING at it,
// and add the resjunk wholerow Var over the original (soon-expanded) RTE.
// C copyObject-deep-copies RETURNING before ChangeVarNodes; the tree here is
// query-owned, mutate in place. mergeTargetRelation is left pointing at the
// original (now expanded) RTE, matching C.
fn rewrite_dml_view_with_instead_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    q: &mut Query<'mcx>,
) -> PgResult<()> {
    if q.resultRelation == 0
        || !matches!(
            q.commandType,
            CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
        )
    {
        return Ok(());
    }
    let old_rti = q.resultRelation;
    let rte_node = q.rtable.nth(old_rti as usize - 1);
    let rte = rte_of(rte_node);
    if rte.rtekind != RTEKind::RTE_RELATION || rte.relkind != RELKIND_VIEW {
        return Ok(());
    }
    // SAFETY: shallow bitwise copy; the copy stays unexpanded and its shared
    // subtrees are read-only from here on.
    let newrte: RangeTblEntry<'mcx> = unsafe { core::ptr::read(rte) };
    q.rtable.lappend(mcx, Node::mk(mcx, newrte)?)?;
    let new_rti = q.rtable.len() as i32;
    q.resultRelation = new_rti;

    for tle_node in &q.returningList {
        rewrite_manip::ChangeVarNodes(mcx, tle_node, old_rti, new_rti, 0)?;
    }

    let var = nodes_core::makefuncs::make_whole_row_var(mcx, rte, old_rti as u32, 0, false)?;
    let tle = types_nodes::primnodes::TargetEntry {
        expr: Node::mk(mcx, var)?,
        resno: (q.targetList.len() + 1) as i16,
        resname: Some("wholerow"),
        ressortgroupref: 0,
        resorigtbl: types_core::InvalidOid,
        resorigcol: 0,
        resjunk: true,
    };
    q.targetList.lappend(mcx, Node::mk(mcx, tle)?)?;
    Ok(())
}


// Restriction check shared by ApplyRetrieveRule/rewriteTargetView
// (rewriteHandler.c): expansion of non-system views can be disabled by the
// restrict_nonsystem_relation_kind GUC.
fn check_view_expansion_restricted(rel: &Relation<'_>) -> PgResult<()> {
    if guc_tables::backing::restrict_nonsystem_relation_kind()
        & guc_tables::consts::RESTRICT_RELKIND_VIEW
        != 0
        && rel.rd_id >= types_core::catalog::FirstNormalObjectId
    {
        let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
        return Err(Box::new(
            PgError::error(format!("access to non-system view \"{relname}\" is restricted"))
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    Ok(())
}

// ApplyRetrieveRule (rewriteHandler.c), SELECT-only arm: the DML-on-view
// result-relation branch and FOR UPDATE/SHARE (markQueryForLocking) are loud.
#[allow(clippy::too_many_arguments)]
fn ApplyRetrieveRule<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rule: &RewriteRuleMeta,
    rt_index: i32,
    rte_node: Node<'mcx>,
    relation: &Relation<'mcx>,
    active_rirs: &mut PgVec<'mcx, Oid>,
    caller_has_row_security: &mut bool,
) -> PgResult<()> {
    if rule.qual_src.is_some() {
        return Err(internal_error("cannot handle qualified ON SELECT rule"));
    }
    check_view_expansion_restricted(relation)?;
    if rt_index == parsetree.resultRelation {
        // The INSTEAD OF target stays an unexpanded RTE_RELATION: INSERT needs
        // no source data; UPDATE/DELETE were re-pointed at a target copy by
        // rewrite_dml_view_with_instead_trigger, so this is that copy.
        return Ok(());
    }
    let rc = parse_relation::get_parse_rowmark(parsetree, rt_index as u32).map(|n| {
        let rc = n.as_row_mark_clause().expect("rowMarks cell");
        (rc.strength, rc.waitPolicy)
    });

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
    set_rule_check_as_user(rule_action, check_as_user)?;

    AcquireRewriteLocks(mcx, rule_action, true, rc.is_some())?;

    if let Some((strength, wait_policy)) = rc {
        markQueryForLocking(mcx, action_node, strength, wait_policy, true)?;
    }

    let rir = fireRIRrules(mcx, rule_action, active_rirs)?;
    stamp_query_flags(mcx, action_node, &rir)?;
    *caller_has_row_security |= rir.has_row_security;
    let rule_action = action_node.as_query().expect("rule action is a Query");

    let rte = rte_of(rte_node);
    let num_cols = rule_action
        .targetList
        .iter()
        .filter(|te| !te.as_target_entry().expect("tlist cell").resjunk)
        .count();
    // CREATE OR REPLACE VIEW can have added columns since this RTE was made;
    // pad eref->colnames with "?column?" up to the clean tlist length (C).
    if rte.eref.map_or(0, |e| e.colnames.len()) < num_cols {
        let old = rte.eref;
        let mut colnames = NodeList::nil();
        if let Some(e) = old {
            for n in e.colnames.iter() {
                colnames.lappend(mcx, n)?;
            }
        }
        while colnames.len() < num_cols {
            colnames.lappend(mcx, Node::mk_string(mcx, "?column?")?)?;
        }
        let new_eref = Node::mk_mut(
            mcx,
            types_nodes::primnodes::Alias { aliasname: old.and_then(|e| e.aliasname), colnames },
        )?
        .seal_ref();
        // SAFETY: single-threaded rewrite; no live borrow of rte across this.
        unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.eref = Some(new_eref)) };
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

// markQueryForLocking (rewriteHandler.c): implicit FOR [KEY] UPDATE/SHARE on
// all base rels of a locked view.
fn markQueryForLocking<'mcx>(
    mcx: Mcx<'mcx>,
    qry_node: Node<'mcx>,
    strength: LockClauseStrength,
    wait_policy: LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    let q = qry_node.as_query().expect("Query");
    let Some(jt) = q.jointree else { return Ok(()) };
    for item in &jt.fromlist {
        mark_jointree_for_locking(mcx, qry_node, item, strength, wait_policy, pushed_down)?;
    }
    Ok(())
}

fn mark_jointree_for_locking<'mcx>(
    mcx: Mcx<'mcx>,
    qry_node: Node<'mcx>,
    jtnode: Node<'mcx>,
    strength: LockClauseStrength,
    wait_policy: LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rti = jtnode.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            let rte_node = qry_node.as_query().expect("Query").rtable.nth(rti as usize - 1);
            let rtekind = rte_of(rte_node).rtekind;
            if rtekind == RTEKind::RTE_RELATION {
                applyLockingClause(mcx, qry_node, rti as u32, strength, wait_policy, pushed_down)?;
                let q = qry_node.as_query().expect("Query");
                let perminfo =
                    parse_relation::getRTEPermissionInfo(&q.rteperminfos, rte_of(rte_node))?;
                // SAFETY: rewriter-owned tree; no derived refs live.
                unsafe {
                    perminfo.with_mut::<RTEPermissionInfo, _>(|p| {
                        p.requiredPerms |= ACL_SELECT_FOR_UPDATE
                    })
                }
                .expect("RTEPermissionInfo");
            } else if rtekind == RTEKind::RTE_SUBQUERY {
                applyLockingClause(mcx, qry_node, rti as u32, strength, wait_policy, pushed_down)?;
                // No Node handle on rte.subquery: re-issue the Query header and
                // re-point the RTE (see fireRIRrules' RTE_SUBQUERY arm).
                let sub = rte_of(rte_node).subquery.expect("subquery RTE has a subquery");
                // SAFETY: Query is !Drop arena data; the source reference is
                // dead after the re-point.
                let subq: Query<'mcx> = unsafe { core::ptr::read(sub) };
                let sub_node = Node::mk(mcx, subq)?;
                markQueryForLocking(mcx, sub_node, strength, wait_policy, true)?;
                let subref = sub_node.as_query().expect("Query");
                // SAFETY: rewriter-owned tree; no derived refs live.
                unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(subref)) };
            }
        }
        NodeTag::T_FromExpr => {
            let f = jtnode.as_from_expr().expect("FromExpr");
            for item in &f.fromlist {
                mark_jointree_for_locking(mcx, qry_node, item, strength, wait_policy, pushed_down)?;
            }
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().expect("JoinExpr");
            mark_jointree_for_locking(mcx, qry_node, j.larg, strength, wait_policy, pushed_down)?;
            mark_jointree_for_locking(mcx, qry_node, j.rarg, strength, wait_policy, pushed_down)?;
        }
        other => return Err(internal_error(&format!("unrecognized node type: {other:?}"))),
    }
    Ok(())
}

// applyLockingClause (analyze.c).
fn applyLockingClause<'mcx>(
    mcx: Mcx<'mcx>,
    qry_node: Node<'mcx>,
    rtindex: u32,
    strength: LockClauseStrength,
    wait_policy: LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    debug_assert!(strength != LockClauseStrength::LCS_NONE);
    if !pushed_down {
        // SAFETY: rewriter-owned tree; no derived refs live.
        unsafe { qry_node.with_mut::<Query, _>(|q| q.hasForUpdate = true) }.expect("Query");
    }
    let q = qry_node.as_query().expect("Query");
    if let Some(rc) = parse_relation::get_parse_rowmark(q, rtindex) {
        // Strongest strength wins; NOWAIT > SKIP LOCKED > block; pushedDown
        // clears once any clause is explicit (C's Max/&= merge).
        // SAFETY: rewriter-owned tree; no derived refs live.
        unsafe {
            rc.with_mut::<RowMarkClause, _>(|rc| {
                rc.strength = rc.strength.max(strength);
                rc.waitPolicy = rc.waitPolicy.max(wait_policy);
                rc.pushedDown &= pushed_down;
            })
        }
        .expect("RowMarkClause");
        return Ok(());
    }
    let rc = Node::mk(
        mcx,
        RowMarkClause { rti: rtindex, strength, waitPolicy: wait_policy, pushedDown: pushed_down },
    )?;
    // SAFETY: rewriter-owned tree; no derived refs live.
    unsafe { qry_node.with_mut::<Query, _>(|q| q.rowMarks.lappend(mcx, rc)) }.expect("Query")?;
    Ok(())
}

// setRuleCheckAsUser (rewriteDefine.c) over a bare node. C stamps the rule
// cache once in RelationBuildRuleLock (relcache.c); the text cache defers it
// to each fresh read.
pub(crate) fn set_rule_check_as_user_node<'mcx>(node: Node<'mcx>, userid: Oid) -> PgResult<()> {
    let mut w = RuleCheckAsUser { userid };
    nodes_core::NodeWalker::visit(&mut w, node)?;
    Ok(())
}

// setRuleCheckAsUser_Query (rewriteDefine.c).
fn set_rule_check_as_user<'mcx>(qry: &'mcx Query<'mcx>, userid: Oid) -> PgResult<()> {
    for pnode in qry.rteperminfos.iter() {
        // SAFETY: the tree was just read by stringToNode; exclusively ours.
        unsafe { pnode.with_mut::<RTEPermissionInfo, _>(|p| p.checkAsUser = userid) }
            .expect("rteperminfos holds RTEPermissionInfo nodes");
    }
    for rnode in qry.rtable.iter() {
        let rte = rte_of(rnode);
        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            set_rule_check_as_user(rte.subquery.expect("subquery RTE"), userid)?;
        }
    }
    for cnode in qry.cteList.iter() {
        let cte = cnode.as_common_table_expr().expect("cteList holds CommonTableExpr");
        let ctequery = cte
            .ctequery
            .and_then(|n| n.as_query())
            .expect("ctequery is a Query");
        set_rule_check_as_user(ctequery, userid)?;
    }
    if qry.hasSubLinks {
        let mut w = RuleCheckAsUser { userid };
        nodes_core::query_tree_walker(qry, &mut w, nodes_core::QTW_IGNORE_RC_SUBQUERIES)?;
    }
    Ok(())
}

// setRuleCheckAsUser_walker (rewriteDefine.c).
struct RuleCheckAsUser {
    userid: Oid,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for RuleCheckAsUser {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(q) = node.as_query() {
            set_rule_check_as_user(q, self.userid)?;
            return Ok(false);
        }
        nodes_core::expression_tree_walker(node, self)
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

#[track_caller]
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

#[track_caller]
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
    for (i, node) in parsetree.rtable.iter().enumerate() {
        let rt_index = i as i32 + 1;
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
                // C rebuilds joinaliasvars with dropped-column Vars replaced
                // by NULL cells; NodeList has no NULL cell, so the (initdb-
                // impossible for system views) dropped hit is a loud panic
                // and the no-drop path leaves the list shared, unrebuilt.
                let rte = rte_of(node);
                let mut curinputvarno: i32 = 0;
                let mut curinputrte: Option<&RangeTblEntry<'mcx>> = None;
                for aliasitem in &rte.joinaliasvars {
                    let aliasvar = nodes_core::strip_implicit_coercions(aliasitem);
                    let Some(v) = aliasvar.as_var() else { continue };
                    debug_assert_eq!(v.varlevelsup, 0);
                    if v.varno != curinputvarno {
                        curinputvarno = v.varno;
                        if curinputvarno >= rt_index {
                            return Err(internal_error(&format!(
                                "unexpected varno {curinputvarno} in JOIN RTE {rt_index}"
                            )));
                        }
                        curinputrte =
                            Some(rte_of(parsetree.rtable.nth(curinputvarno as usize - 1)));
                    }
                    if get_rte_attribute_is_dropped(
                        mcx,
                        curinputrte.expect("input RTE resolved"),
                        v.varattno,
                    )? {
                        panic!(
                            "AcquireRewriteLocks (rewriteHandler.c): joinaliasvars entry \
                             references a dropped column; the NULL-cell replacement has \
                             no NodeList representation"
                        );
                    }
                }
            }
            RTEKind::RTE_SUBQUERY => {
                let pushed_down = forUpdatePushedDown
                    || parse_relation::get_parse_rowmark(parsetree, rt_index as u32).is_some();
                let sub = rte_of(node).subquery.expect("subquery RTE has a subquery");
                AcquireRewriteLocks(mcx, sub, forExecute, pushed_down)?;
            }
            _ => {}
        }
    }

    for cte_node in &parsetree.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctequery = cte.ctequery.and_then(|n| n.as_query()).expect("analyzed CTE query");
        AcquireRewriteLocks(mcx, ctequery, forExecute, false)?;
    }

    // acquireLocksOnSubLinks (rewriteHandler.c); rtable/CTE subqueries were
    // recursed above (C passes QTW_IGNORE_RC_SUBQUERIES for the same reason).
    if parsetree.hasSubLinks {
        let mut w = LocksOnSubLinks { mcx, for_execute: forExecute };
        nodes_core::query_tree_walker(parsetree, &mut w, nodes_core::QTW_IGNORE_RC_SUBQUERIES)?;
    }

    Ok(())
}

// acquireLocksOnSubLinks (rewriteHandler.c).
struct LocksOnSubLinks<'mcx> {
    mcx: Mcx<'mcx>,
    for_execute: bool,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for LocksOnSubLinks<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(sl) = node.as_sub_link() {
            if let Some(q) = sl.subselect.as_query() {
                AcquireRewriteLocks(self.mcx, q, self.for_execute, false)?;
            }
        }
        nodes_core::expression_tree_walker(node, self)
    }
}

fn rte_of<'mcx>(node: Node<'mcx>) -> &'mcx RangeTblEntry<'mcx> {
    node.as_range_tbl_entry().expect("rtable holds RangeTblEntry nodes")
}

// get_rte_attribute_is_dropped (parse_relation.c); lives here until the
// parse_relation crate grows the expression-vocabulary deps it needs.
fn get_rte_attribute_is_dropped<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    attnum: i16,
) -> PgResult<bool> {
    const ANUM_PG_ATTRIBUTE_ATTISDROPPED: i32 = 17;
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            let Some(tp) = cache_syscache::SearchSysCache2(
                cache_syscache::ATTNUM,
                cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(rte.relid)),
                cache_syscache::SysCacheKey::Value(datum::Datum::from_i16(attnum)),
            )?
            else {
                return Err(internal_error(&format!(
                    "cache lookup failed for attribute {attnum} of relation {}",
                    rte.relid
                )));
            };
            let (d, _) = cache_syscache::SysCacheGetAttr(
                cache_syscache::ATTNUM,
                &tp,
                ANUM_PG_ATTRIBUTE_ATTISDROPPED,
            )?;
            let dropped = d.as_bool();
            cache_syscache::ReleaseSysCache(tp);
            Ok(dropped)
        }
        RTEKind::RTE_SUBQUERY
        | RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_CTE
        | RTEKind::RTE_GROUP => Ok(false),
        RTEKind::RTE_NAMEDTUPLESTORE => {
            if attnum <= 0 || attnum as usize > rte.coltypes.len() {
                return Err(internal_error(&format!("invalid varattno {attnum}")));
            }
            Ok(rte.coltypes.nth(attnum as usize - 1) == InvalidOid)
        }
        RTEKind::RTE_JOIN => {
            if attnum <= 0 || attnum as usize > rte.joinaliasvars.len() {
                return Err(internal_error(&format!("invalid varattno {attnum}")));
            }
            // C signals dropped via a NULL joinaliasvars cell; NodeList cells
            // are non-null, so nothing here can be dropped.
            Ok(false)
        }
        RTEKind::RTE_FUNCTION => {
            let mut atts_done: i16 = 0;
            for f in &rte.functions {
                let rtfunc = f.as_range_tbl_function().expect("functions holds RangeTblFunction");
                let colcount = rtfunc.funccolcount as i16;
                if attnum > atts_done && attnum <= atts_done + colcount {
                    if !rtfunc.funccolnames.is_nil() {
                        return Ok(false);
                    }
                    if let Some(tupdesc) =
                        funcapi::get_expr_result_tupdesc(mcx, rtfunc.funcexpr, true)?
                    {
                        debug_assert!((attnum - atts_done) as i32 <= tupdesc.natts);
                        return Ok(tupdesc.attr((attnum - atts_done - 1) as usize).attisdropped);
                    }
                    return Ok(false);
                }
                atts_done += colcount;
            }
            if rte.funcordinality && attnum == atts_done + 1 {
                return Ok(false);
            }
            Err(Box::new(
                PgError::error(format!(
                    "column {attnum} of relation \"{}\" does not exist",
                    rte.eref.and_then(|e| e.aliasname).unwrap_or("")
                ))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
            ))
        }
        RTEKind::RTE_RESULT => Err(Box::new(
            PgError::error(format!(
                "column {attnum} of relation \"{}\" does not exist",
                rte.eref.and_then(|e| e.aliasname).unwrap_or("")
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
        )),
    }
}

fn fc_pg_relation_is_updatable(
    _f: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let reloid = fcinfo.arg_oid(0);
    let include_triggers = fcinfo.arg_bool(1);
    let mcx = fcinfo.result_mcx();
    let mut outer_reloids = PgVec::new_in(mcx);
    let events = relation_is_updatable(mcx, reloid, &mut outer_reloids, include_triggers, None)?;
    Ok(datum::Datum::from_i32(events))
}

fn fc_pg_column_is_updatable(
    _f: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let reloid = fcinfo.arg_oid(0);
    let attnum = fcinfo.arg_i16(1);
    let include_triggers = fcinfo.arg_bool(2);
    if attnum <= 0 {
        return Ok(datum::Datum::from_bool(false));
    }
    let mcx = fcinfo.result_mcx();
    let col = attnum as i32 - FirstLowInvalidHeapAttributeNumber;
    let mut cols = Bitmapset::empty();
    cols.add_member(mcx, col)?;
    let mut outer_reloids = PgVec::new_in(mcx);
    let events =
        relation_is_updatable(mcx, reloid, &mut outer_reloids, include_triggers, Some(&cols))?;
    const REQ_EVENTS: i32 =
        (1 << CmdType::CMD_UPDATE as i32) | (1 << CmdType::CMD_DELETE as i32);
    Ok(datum::Datum::from_bool(events & REQ_EVENTS == REQ_EVENTS))
}

pub const REWRITE_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[
    types_fmgr::FmgrBuiltin {
        foid: 3842,
        name: "pg_relation_is_updatable",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_relation_is_updatable,
    },
    types_fmgr::FmgrBuiltin {
        foid: 3843,
        name: "pg_column_is_updatable",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_column_is_updatable,
    },
];
