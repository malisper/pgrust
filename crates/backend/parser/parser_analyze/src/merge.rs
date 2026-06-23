//! Port of `src/backend/parser/parse_merge.c` (PostgreSQL 18.3) — handling of
//! the `MERGE` statement in the parser.
//!
//! `parse_merge.c` defines exactly three functions, all ported here with their
//! original C name, branch order, switch arms, list-building order, error
//! message text, and SQLSTATE values preserved 1:1:
//!
//!   * [`transformMergeStmt`] — transform a `MERGE` statement into a `Query`.
//!   * `setNamespaceForMergeWhen` — set namespace visibility for one `WHEN`
//!     action before transforming its quals / targetlist.
//!   * `setNamespaceVisibilityForRTE` — flip the rel/cols visibility of the
//!     namespace item that refers to a given range-table entry.
//!
//! This file lives in `backend-parser-analyze` alongside `transformInsertStmt` /
//! `transformUpdateStmt` / `transformDeclareCursorStmt`: every transform helper
//! it calls (`transformFromClause`, `setTargetTable`, `transformWhereClause`,
//! `transformExpr`, `checkInsertTargets`, `transformExpressionList`,
//! `transformInsertRow`, `transformUpdateTargetList`, `transformReturningClause`,
//! `transformWithClause`, `assign_query_collations`) is already a merged sibling
//! owner reachable cycle-free from here (parse_merge.c is the only consumer the
//! analyze layer was missing).
//!
//! # Pointer-identity → index-identity
//!
//! C's `setNamespaceVisibilityForRTE` finds the namespace item by pointer
//! identity (`nsitem->p_rte == rte`, where `rte` came from `rt_fetch(rti,
//! p_rtable)`). In the owned tree there is no shared `RangeTblEntry *` to compare
//! by address, so the faithful identity is the range-table **index**: the nsitem
//! whose `p_rtindex` equals the target/source RTI. `setNamespaceForMergeWhen`
//! threads the RTIs straight through (it already has them), exactly selecting the
//! same namespace item C's pointer compare would.

use alloc::format;

use mcx::{Mcx, PgVec};
use types_acl::acl::{
    AclMode, ACL_DELETE, ACL_INSERT, ACL_NO_RIGHTS, ACL_SELECT, ACL_UPDATE,
};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_ALIAS, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR,
};
use nodes::copy_query::Query;
use nodes::modifytable::{MergeMatchKind, NUM_MERGE_MATCH_KINDS};
use nodes::nodes::{CmdType, Node, NodePtr};
use nodes::parsestmt::{ParseExprKind, ParseNamespaceItem, ParseState};
use nodes::rawnodes::{MergeAction, MergeStmt, MergeWhenClause};
use types_tuple::access::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW};

use utils_error::ereport;

use crate::elog_error;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h:27) — the offset used
/// to fold a real `AttrNumber` into the `RTEPermissionInfo` column bitmapset.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

// ===========================================================================
// transformMergeStmt (parse_merge.c:106)
// ===========================================================================

/// `transformMergeStmt(pstate, stmt)` (parse_merge.c:106) — transforms a MERGE
/// statement into a `Query`.
pub fn transformMergeStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &MergeStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    let mut target_perms: AclMode = ACL_NO_RIGHTS;
    let mut is_terminal: [bool; NUM_MERGE_MATCH_KINDS] = [false; NUM_MERGE_MATCH_KINDS];

    /* There can't be any outer WITH to worry about */
    debug_assert!(pstate.p_ctenamespace.is_empty());

    qry.commandType = CmdType::CMD_MERGE;
    qry.hasRecursive = false;

    /* process the WITH clause independently of all else */
    if let Some(with) = stmt.withClause.as_deref() {
        if with.recursive {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("WITH RECURSIVE is not supported for MERGE statement")
                .into_error());
        }

        let with_copy = with.clone_in(mcx)?;
        let ctes = cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = crate::cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    /*
     * Check WHEN clauses for permissions and sanity
     */
    is_terminal[MergeMatchKind::MERGE_WHEN_MATCHED as usize] = false;
    is_terminal[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize] = false;
    is_terminal[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize] = false;
    for l in stmt.mergeWhenClauses.iter() {
        let merge_when_clause = as_merge_when_clause(l.as_ref())?;

        /*
         * Collect permissions to check, according to action types. We require
         * SELECT privileges for DO NOTHING because it'd be irregular to have a
         * target relation with zero privileges checked, in case DO NOTHING is
         * the only action.  There's no damage from that: any meaningful MERGE
         * command requires at least some access to the table anyway.
         */
        match merge_when_clause.commandType {
            CmdType::CMD_INSERT => target_perms |= ACL_INSERT,
            CmdType::CMD_UPDATE => target_perms |= ACL_UPDATE,
            CmdType::CMD_DELETE => target_perms |= ACL_DELETE,
            CmdType::CMD_NOTHING => target_perms |= ACL_SELECT,
            _ => return Err(elog_error("unknown action in MERGE WHEN clause")),
        }

        /*
         * Check for unreachable WHEN clauses
         */
        if is_terminal[merge_when_clause.matchKind as usize] {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("unreachable WHEN clause specified after unconditional WHEN clause")
                .into_error());
        }
        if merge_when_clause.condition.is_none() {
            is_terminal[merge_when_clause.matchKind as usize] = true;
        }
    }

    /*
     * Set up the MERGE target table.  The target table is added to the
     * namespace below and to joinlist in transform_MERGE_to_join, so don't do
     * it here.
     *
     * Initially mergeTargetRelation is the same as resultRelation, so data is
     * read from the table being updated.  However, that might be changed by the
     * rewriter, if the target is a trigger-updatable view, to allow target data
     * to be read from the expanded view query while updating the original view
     * relation.
     */
    let relation = stmt
        .relation
        .as_deref()
        .ok_or_else(|| elog_error("MERGE statement has no target relation"))?;
    let relation_inh = relation.inh;
    qry.resultRelation = clause::setTargetTable(
        mcx,
        pstate,
        relation,
        relation_inh,
        false,
        target_perms,
    )?;
    qry.mergeTargetRelation = qry.resultRelation;

    /* The target relation must be a table or a view */
    let relkind = pstate
        .p_target_relation
        .as_ref()
        .ok_or_else(|| elog_error("MERGE has no target relation"))?
        .rd_rel
        .relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE && relkind != RELKIND_VIEW
    {
        let relname = pstate
            .p_target_relation
            .as_ref()
            .unwrap()
            .name()
            .to_string();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot execute MERGE on relation \"{relname}\""))
            .errdetail(pg_class::errdetail_relkind_not_supported(relkind)?)
            .into_error());
    }

    /* Now transform the source relation to produce the source RTE. */
    let source_relation = stmt
        .sourceRelation
        .as_ref()
        .ok_or_else(|| elog_error("MERGE statement has no source relation"))?;
    let source_copy = source_relation.as_ref().clone_in(mcx)?;
    let mut from_list: PgVec<'mcx, NodePtr<'mcx>> = mcx::vec_with_capacity_in(mcx, 1)?;
    from_list.push(mcx::alloc_in(mcx, source_copy)?);
    clause::transformFromClause(mcx, pstate, &from_list[..])?;
    let source_rti = pstate.p_rtable.len() as i32;

    /*
     * Check that the target table doesn't conflict with the source table. This
     * would typically be a checkNameSpaceConflicts call, but we want a more
     * specific error message.
     */
    let source_aliasname = {
        let nsitem =
            parser_relation::GetNSItemByRangeTablePosn(pstate, source_rti, 0)?;
        nsitem_aliasname(nsitem)
    };
    let target_aliasname = nsitem_aliasname(
        pstate
            .p_target_nsitem
            .as_deref()
            .ok_or_else(|| elog_error("MERGE target namespace item is missing"))?,
    );
    if target_aliasname == source_aliasname {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!(
                "name \"{target_aliasname}\" specified more than once"
            ))
            .errdetail("The name is used both as MERGE target table and data source.")
            .into_error());
    }

    /*
     * There's no need for a targetlist here; it'll be set up by
     * preprocess_targetlist later.
     */
    // There's no need for a targetlist here; it'll be set up by
    // preprocess_targetlist later.
    //
    // C sets `qry->rtable = pstate->p_rtable` / `qry->rteperminfos =
    // pstate->p_rteperminfos` here, but those are SHARED pointers: the
    // WHEN-action loop below still appends RTEs and edits rteperminfos through
    // `pstate`, and the final list is what the Query carries. In the owned
    // model there is no aliasing, so we keep the lists owned by `pstate`
    // through the loop and move them into `qry` once, at the very end (matching
    // insert.rs's "Done building the range table" transfer).
    qry.targetList = PgVec::new_in(mcx);

    /*
     * Transform the join condition.  This includes references to the target
     * side, so add that to the namespace.
     */
    let target_nsitem = pstate
        .p_target_nsitem
        .as_deref()
        .ok_or_else(|| elog_error("MERGE target namespace item is missing"))?
        .clone_in(mcx)?;
    parser_relation::addNSItemToQuery(mcx, pstate, target_nsitem, false, true, true)?;

    let join_condition = match stmt.joinCondition.as_ref() {
        Some(n) => Some(n.as_ref().clone_in(mcx)?),
        None => None,
    };
    let merge_join_cond = parse_expr::transformExpr(
        pstate,
        join_condition,
        ParseExprKind::EXPR_KIND_JOIN_ON,
    )?;
    qry.mergeJoinCondition = match merge_join_cond {
        // Parser-arena `'static` result re-cloned into the query's `mcx`.
        Some(e) => Some(mcx::alloc_in(mcx, e.clone_in(mcx)?)?),
        None => None,
    };

    /*
     * Create the temporary query's jointree using the joinlist we built using
     * just the source relation; the target relation is not included. The join
     * will be constructed fully by transform_MERGE_to_join.
     */
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, PgVec::new_in(mcx));
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: None,
        },
    )?);

    /* Transform the RETURNING list, if any */
    crate::update_delete::transformReturningClause(
        mcx,
        pstate,
        &mut qry,
        stmt.returningClause.as_deref(),
        ParseExprKind::EXPR_KIND_MERGE_RETURNING,
    )?;

    /*
     * We now have a good query shape, so now look at the WHEN conditions and
     * action targetlists.
     *
     * Overall, the MERGE Query's targetlist is NIL.
     *
     * Each individual action has its own targetlist that needs separate
     * transformation. These transforms don't do anything to the overall
     * targetlist, since that is only used for resjunk columns.
     *
     * We can reference any column in Target or Source, which is OK because both
     * of those already have RTEs. There is nothing like the EXCLUDED
     * pseudo-relation for INSERT ON CONFLICT.
     */
    let mut merge_action_list: PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, stmt.mergeWhenClauses.len())?;
    for l in stmt.mergeWhenClauses.iter() {
        // Re-read the fields we need up front so the clause borrow doesn't
        // conflict with the mutable pstate borrows in the helpers below.
        let (when_match_kind, when_command_type, when_override) = {
            let c = as_merge_when_clause(l.as_ref())?;
            (c.matchKind, c.commandType, c.r#override)
        };

        let mut action = MergeAction {
            matchKind: when_match_kind,
            commandType: when_command_type,
            r#override: nodes::modifytable::OverridingKind::OVERRIDING_NOT_SET,
            qual: None,
            targetList: PgVec::new_in(mcx),
            updateColnos: PgVec::new_in(mcx),
        };

        /*
         * Set namespace for the specific action. This must be done before
         * analyzing the WHEN quals and the action targetlist.
         */
        setNamespaceForMergeWhen(
            pstate,
            when_match_kind,
            when_command_type,
            qry.resultRelation,
            source_rti,
        );

        /*
         * Transform the WHEN condition.
         *
         * Note that these quals are NOT added to the join quals; instead they
         * are evaluated separately during execution to decide which of the WHEN
         * MATCHED or WHEN NOT MATCHED actions to execute.
         */
        let condition = {
            let c = as_merge_when_clause(l.as_ref())?;
            match c.condition.as_ref() {
                Some(n) => Some(n.as_ref().clone_in(mcx)?),
                None => None,
            }
        };
        let qual = clause::transformWhereClause(
            mcx,
            pstate,
            condition,
            ParseExprKind::EXPR_KIND_MERGE_WHEN,
            "WHEN",
        )?;
        action.qual = match qual {
            // Parser-arena `'static` qual re-cloned into `mcx` before wrapping.
            Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e.clone_in(mcx)?)?)?),
            None => None,
        };

        /*
         * Transform target lists for each INSERT and UPDATE action stmt
         */
        match when_command_type {
            CmdType::CMD_INSERT => {
                pstate.p_is_insert = true;

                /* checkInsertTargets needs a private copy of the targetList */
                let when_target_cols = {
                    let c = as_merge_when_clause(l.as_ref())?;
                    copy_restarget_list(mcx, &c.targetList)?
                };
                let (icolumns, attrnos) =
                    parse_target::checkInsertTargets(mcx, pstate, when_target_cols)?;
                debug_assert_eq!(icolumns.len(), attrnos.len());

                action.r#override = when_override;

                /*
                 * Handle INSERT much like in transformInsertStmt
                 */
                let when_values_empty = {
                    let c = as_merge_when_clause(l.as_ref())?;
                    c.values.is_empty()
                };
                let expr_list: PgVec<'mcx, nodes::primnodes::Expr> = if when_values_empty {
                    /*
                     * We have INSERT ... DEFAULT VALUES.  We can handle this
                     * case by emitting an empty targetlist --- all columns will
                     * be defaulted when the planner expands the targetlist.
                     */
                    PgVec::new_in(mcx)
                } else {
                    /*
                     * Process INSERT ... VALUES with a single VALUES sublist.
                     * We treat this case separately for efficiency.  The sublist
                     * is just computed directly as the Query's targetlist, with
                     * no VALUES RTE.  So it works just like a SELECT without any
                     * FROM.
                     *
                     * Do basic expression transformation (same as a ROW() expr,
                     * but allow SetToDefault at top level)
                     */
                    let values_copy = {
                        let c = as_merge_when_clause(l.as_ref())?;
                        let mut v: PgVec<'mcx, NodePtr<'mcx>> =
                            mcx::vec_with_capacity_in(mcx, c.values.len())?;
                        for item in c.values.iter() {
                            v.push(mcx::alloc_in(mcx, item.as_ref().clone_in(mcx)?)?);
                        }
                        v
                    };
                    let raw = parse_target::transformExpressionList(
                        mcx,
                        pstate,
                        values_copy,
                        ParseExprKind::EXPR_KIND_VALUES_SINGLE,
                        true,
                    )?;

                    /* Prepare row for assignment to target table */
                    let target_cols = {
                        let c = as_merge_when_clause(l.as_ref())?;
                        let mut v: PgVec<'mcx, NodePtr<'mcx>> =
                            mcx::vec_with_capacity_in(mcx, c.targetList.len())?;
                        for item in c.targetList.iter() {
                            v.push(mcx::alloc_in(mcx, item.as_ref().clone_in(mcx)?)?);
                        }
                        v
                    };
                    crate::insert::transformInsertRow(
                        mcx,
                        pstate,
                        raw,
                        &target_cols,
                        &icolumns,
                        &attrnos,
                        false,
                    )?
                };

                /*
                 * Generate action's target list using the computed list of
                 * expressions. Also, mark all the target columns as needing
                 * insert permissions.
                 *
                 * forthree(lc, exprList, icols, icolumns, attnos, attrnos)
                 */
                let perminfoindex = pstate
                    .p_target_nsitem
                    .as_deref()
                    .and_then(|ns| ns.p_rte.as_deref())
                    .map(|r| r.perminfoindex)
                    .unwrap_or(0);

                debug_assert!(expr_list.len() <= icolumns.len());
                for (idx, expr) in expr_list.into_iter().enumerate() {
                    let col = &icolumns[idx];
                    let attr_num = attrnos[idx];
                    let tle = nodes_core::makefuncs::make_target_entry(
                        mcx,
                        expr,
                        attr_num as i16,
                        col.name.as_ref().map(|s| s.as_str()),
                        false,
                    )?;
                    action
                        .targetList
                        .push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, tle)?)?);

                    // perminfo->insertedCols =
                    //   bms_add_member(perminfo->insertedCols,
                    //     attr_num - FirstLowInvalidHeapAttributeNumber);
                    if perminfoindex > 0 {
                        let pi = &mut pstate.p_rteperminfos[(perminfoindex - 1) as usize];
                        let inserted = pi.insertedCols.take();
                        pi.insertedCols = Some(nodes_core::bitmapset::bms_add_member(
                            mcx,
                            inserted,
                            attr_num - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
                        )?);
                    }
                }
            }
            CmdType::CMD_UPDATE => {
                pstate.p_is_insert = false;
                let target_list = {
                    let target = {
                        let c = as_merge_when_clause(l.as_ref())?;
                        let mut v: PgVec<'mcx, NodePtr<'mcx>> =
                            mcx::vec_with_capacity_in(mcx, c.targetList.len())?;
                        for item in c.targetList.iter() {
                            v.push(mcx::alloc_in(mcx, item.as_ref().clone_in(mcx)?)?);
                        }
                        v
                    };
                    crate::update_delete::transformUpdateTargetList(mcx, pstate, &target)?
                };
                action.targetList = mcx::vec_with_capacity_in(mcx, target_list.len())?;
                for tle in target_list.into_iter() {
                    action
                        .targetList
                        .push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, tle)?)?);
                }
            }
            CmdType::CMD_DELETE => {}

            CmdType::CMD_NOTHING => {
                action.targetList = PgVec::new_in(mcx);
            }
            _ => return Err(elog_error("unknown action in MERGE WHEN clause")),
        }

        merge_action_list.push(mcx::alloc_in(mcx, Node::mk_merge_action(mcx, action)?)?);
    }

    qry.mergeActionList = merge_action_list;

    qry.hasTargetSRFs = false;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    /* Done building the range table and jointree; hand them to the Query. */
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx));

    parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    crate::sync_cte_refcounts(pstate, &mut qry.cteList);
    Ok(qry)
}

// ===========================================================================
// setNamespaceForMergeWhen (parse_merge.c:51)
// ===========================================================================

/// Make appropriate changes to the namespace visibility while transforming an
/// individual action's quals and targetlist expressions. INSERT actions (NOT
/// MATCHED [BY TARGET]) see only the source relation; UPDATE/DELETE/NOTHING
/// (MATCHED) see both; NOT MATCHED BY SOURCE sees only the target.
///
/// Identity note: C fetches `targetRelRTE`/`sourceRelRTE` via `rt_fetch` and
/// passes the RTE pointers to `setNamespaceVisibilityForRTE`, which finds the
/// matching nsitem by pointer identity. In the owned tree the faithful identity
/// is the range-table index, so we pass `target_rti` / `source_rti` through.
fn setNamespaceForMergeWhen(
    pstate: &mut ParseState<'_>,
    match_kind: MergeMatchKind,
    command_type: CmdType,
    target_rti: i32,
    source_rti: i32,
) {
    if match_kind == MergeMatchKind::MERGE_WHEN_MATCHED {
        debug_assert!(
            command_type == CmdType::CMD_UPDATE
                || command_type == CmdType::CMD_DELETE
                || command_type == CmdType::CMD_NOTHING
        );

        /* MATCHED actions can see both target and source relations. */
        setNamespaceVisibilityForRTE(pstate, target_rti, true, true);
        setNamespaceVisibilityForRTE(pstate, source_rti, true, true);
    } else if match_kind == MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE {
        /*
         * NOT MATCHED BY SOURCE actions can see the target relation, but they
         * can't see the source relation.
         */
        debug_assert!(
            command_type == CmdType::CMD_UPDATE
                || command_type == CmdType::CMD_DELETE
                || command_type == CmdType::CMD_NOTHING
        );
        setNamespaceVisibilityForRTE(pstate, target_rti, true, true);
        setNamespaceVisibilityForRTE(pstate, source_rti, false, false);
    } else {
        /* MERGE_WHEN_NOT_MATCHED_BY_TARGET */
        /*
         * NOT MATCHED [BY TARGET] actions can't see target relation, but they
         * can see source relation.
         */
        debug_assert!(
            command_type == CmdType::CMD_INSERT || command_type == CmdType::CMD_NOTHING
        );
        setNamespaceVisibilityForRTE(pstate, target_rti, false, false);
        setNamespaceVisibilityForRTE(pstate, source_rti, true, true);
    }
}

// ===========================================================================
// setNamespaceVisibilityForRTE (parse_merge.c:414)
// ===========================================================================

/// Flip the rel/cols visibility of the namespace item that refers to the
/// range-table entry at index `rti`. In C this finds the item by pointer
/// identity (`nsitem->p_rte == rte`); the owned-tree faithful identity is the
/// range-table index (`nsitem.p_rtindex == rti`), with the same first-match +
/// early-`break` behavior.
fn setNamespaceVisibilityForRTE(
    pstate: &mut ParseState<'_>,
    rti: i32,
    rel_visible: bool,
    cols_visible: bool,
) {
    for nsitem in pstate.p_namespace.iter_mut() {
        if nsitem.p_rtindex == rti {
            nsitem.p_rel_visible = rel_visible;
            nsitem.p_cols_visible = cols_visible;
            break;
        }
    }
}

// ===========================================================================
// Local helpers
// ===========================================================================

/// `lfirst_node(MergeWhenClause, lc)` — fetch the `MergeWhenClause` payload of a
/// node-list element and check its tag (C `Assert`s `IsA(node,
/// MergeWhenClause)`); a mismatch surfaces as an `elog`-style internal error.
fn as_merge_when_clause<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a MergeWhenClause<'mcx>> {
    node.as_mergewhenclause().ok_or_else(|| {
        elog_error(format!(
            "expected MergeWhenClause in MERGE WHEN list, found node tag {:?}",
            node.tag()
        ))
    })
}

/// `nsitem->p_names->aliasname` — the namespace item's alias name (empty when the
/// `Alias` or its name is absent, matching a NULL `char *`'s use in `strcmp`).
fn nsitem_aliasname(nsitem: &ParseNamespaceItem<'_>) -> alloc::string::String {
    nsitem
        .p_names
        .as_deref()
        .and_then(|alias| alias.aliasname.as_deref())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Deep-copy a raw `targetList` (a `List` of `ResTarget` nodes) into `mcx` as a
/// `PgVec<ResTarget>` for `checkInsertTargets` (mirrors insert.rs `copy_cols`).
fn copy_restarget_list<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, nodes::rawnodes::ResTarget<'mcx>>> {
    let mut out: PgVec<'mcx, nodes::rawnodes::ResTarget<'mcx>> =
        mcx::vec_with_capacity_in(mcx, cols.len())?;
    for c in cols.iter() {
        match c.as_ref().as_restarget() {
            Some(rt) => out.push(rt.clone_in(mcx)?),
            None => return Err(elog_error("MERGE INSERT cols entry is not a ResTarget")),
        }
    }
    Ok(out)
}
