// parse_merge.c: transformMergeStmt + the per-action namespace visibility
// dance (setNamespaceForMergeWhen/setNamespaceVisibilityForRTE).
use core::mem;

use mcx::Mcx;
use parse_collate::assign_query_collations;
use parser_small1::{ParseExprKind, ParseNamespaceItem, ParseState};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{FromExpr, MergeMatchKind, NUM_MERGE_MATCH_KINDS};
use types_nodes::{Node, NodeList};

use crate::{transformInsertRow, transformReturningClause, transformUpdateTargetList};
use parse_clause::transformWhereClause;

pub(crate) fn transformMergeStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::MergeStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::{ACL_DELETE, ACL_INSERT, ACL_SELECT, ACL_UPDATE};

    let mut qry = Query::default();
    debug_assert!(pstate.p_ctenamespace.is_nil());
    qry.commandType = CmdType::CMD_MERGE;

    if let Some(with) = stmt.withClause {
        if with.as_with_clause().expect("withClause").recursive {
            return Err(merge_with_recursive());
        }
        qry.cteList = crate::parse_cte::transformWithClause(mcx, pstate, with)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    let mut target_perms: types_nodes::parsenodes::AclMode = 0;
    let mut is_terminal = [false; NUM_MERGE_MATCH_KINDS];
    for mwc_node in &stmt.mergeWhenClauses {
        let mwc = mwc_node.as_merge_when_clause().expect("mergeWhenClauses cell");
        target_perms |= match mwc.commandType {
            CmdType::CMD_INSERT => ACL_INSERT,
            CmdType::CMD_UPDATE => ACL_UPDATE,
            CmdType::CMD_DELETE => ACL_DELETE,
            CmdType::CMD_NOTHING => ACL_SELECT,
            other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
        };
        let kind = mwc.matchKind as usize;
        if is_terminal[kind] {
            return Err(unreachable_when_clause());
        }
        if mwc.condition.is_none() {
            is_terminal[kind] = true;
        }
    }

    let relation = stmt
        .relation
        .expect("grammar always sets MergeStmt.relation")
        .as_range_var()
        .expect("relation_expr_opt_alias is a RangeVar");
    qry.resultRelation = parse_clause::setTargetTable(
        mcx,
        pstate,
        relation,
        relation.inh,
        false,
        target_perms,
    )?;
    qry.mergeTargetRelation = qry.resultRelation;

    {
        let rel = pstate.p_target_relation.as_ref().expect("setTargetTable opened it");
        let relkind = rel.rd_rel.relkind;
        if relkind != types_rel::RELKIND_RELATION
            && relkind != types_rel::RELKIND_PARTITIONED_TABLE
            && relkind != types_rel::RELKIND_VIEW
        {
            return Err(merge_bad_relkind(rel));
        }
    }

    let source = stmt.sourceRelation.expect("grammar always sets sourceRelation");
    parse_clause::transformFromClause(mcx, pstate, &NodeList::make1(mcx, source)?)?;
    let source_rti = pstate.p_rtable.len() as i32;
    let source_nsitem = parse_relation::GetNSItemByRangeTablePosn(pstate, source_rti, 0);

    let target_alias = pstate
        .p_target_nsitem
        .expect("setTargetTable set p_target_nsitem")
        .p_names
        .aliasname
        .expect("target alias name");
    let source_alias = source_nsitem.p_names.aliasname.expect("source alias name");
    if target_alias == source_alias {
        return Err(duplicate_source_alias(target_alias));
    }

    let target_nsitem = crate::returning_target_nsitem(mcx, pstate)?;
    parse_relation::addNSItemToQuery(mcx, pstate, target_nsitem, false, true, true)?;
    qry.mergeJoinCondition = Some(parse_expr::transformExpr(
        mcx,
        pstate,
        stmt.joinCondition.expect("grammar always sets joinCondition"),
        ParseExprKind::EXPR_KIND_JOIN_ON,
    )?);

    transformReturningClause(
        mcx,
        pstate,
        &mut qry,
        stmt.returningClause,
        ParseExprKind::EXPR_KIND_MERGE_RETURNING,
    )?;

    let result_relation = qry.resultRelation;
    let mut merge_action_list = NodeList::nil();
    for mwc_node in &stmt.mergeWhenClauses {
        let mwc = mwc_node.as_merge_when_clause().expect("mergeWhenClauses cell");

        set_namespace_for_merge_when(mcx, pstate, mwc.matchKind, result_relation, source_rti)?;

        let qual = transformWhereClause(
            mcx,
            pstate,
            mwc.condition,
            ParseExprKind::EXPR_KIND_MERGE_WHEN,
            "WHEN",
        )?;

        let mut action_tlist = NodeList::nil();
        let mut action_override = mwc.r#override;
        match mwc.commandType {
            CmdType::CMD_INSERT => {
                pstate.p_is_insert = true;
                let (icolumns, attrnos) =
                    parse_target::checkInsertTargets(mcx, pstate, &mwc.targetList)?;
                debug_assert_eq!(icolumns.len(), attrnos.len());
                action_override = mwc.r#override;

                let expr_list = if mwc.values.is_nil() {
                    NodeList::nil()
                } else {
                    let exprs = parse_target::transformExpressionList(
                        mcx,
                        pstate,
                        &mwc.values,
                        ParseExprKind::EXPR_KIND_VALUES_SINGLE,
                        true,
                    )?;
                    transformInsertRow(
                        mcx,
                        pstate,
                        exprs,
                        &mwc.targetList,
                        &icolumns,
                        &attrnos,
                        false,
                    )?
                };

                let perminfo = pstate
                    .p_target_nsitem
                    .expect("target nsitem")
                    .p_perminfo
                    .expect("target nsitem has perminfo");
                debug_assert!(expr_list.len() <= icolumns.len());
                for (i, (expr, icol)) in expr_list.iter().zip(icolumns.iter()).enumerate() {
                    let col = icol.as_res_target().expect("icolumns are ResTargets");
                    let attr_num = attrnos[i] as i16;
                    let tle = Node::mk_target_entry(mcx, expr, attr_num, col.name, false)?;
                    action_tlist.lappend(mcx, tle)?;
                    // SAFETY: perminfo nodes are read only through transient
                    // as_* lookups; no derived reference is live here.
                    unsafe {
                        perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                            p.insertedCols.add_member(
                                mcx,
                                attr_num as i32
                                    - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
                            )
                        })
                    }
                    .expect("p_perminfo is RTEPermissionInfo")?;
                }
            }
            CmdType::CMD_UPDATE => {
                pstate.p_is_insert = false;
                action_tlist = transformUpdateTargetList(mcx, pstate, &mwc.targetList)?;
            }
            CmdType::CMD_DELETE | CmdType::CMD_NOTHING => {}
            other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
        }

        let action = Node::mk(
            mcx,
            types_nodes::MergeAction {
                matchKind: mwc.matchKind,
                commandType: mwc.commandType,
                r#override: action_override,
                qual,
                targetList: action_tlist,
                updateColnos: types_nodes::IntList::nil(),
            },
        )?;
        merge_action_list.lappend(mcx, action)?;
    }
    qry.mergeActionList = merge_action_list;

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    // The jointree holds only the source; transform_MERGE_to_join builds the
    // full join against the target.
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: None })?
            .seal_ref(),
    );

    qry.hasTargetSRFs = false;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;
    Ok(qry)
}

// setNamespaceForMergeWhen; visibility flips land as fresh arena items (the
// namespace holds shared refs, C mutates in place).
fn set_namespace_for_merge_when<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    match_kind: MergeMatchKind,
    target_rti: i32,
    source_rti: i32,
) -> PgResult<()> {
    let (target_vis, source_vis) = match match_kind {
        MergeMatchKind::MERGE_WHEN_MATCHED => (true, true),
        MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE => (true, false),
        MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET => (false, true),
    };
    set_namespace_visibility(mcx, pstate, target_rti, target_vis)?;
    set_namespace_visibility(mcx, pstate, source_rti, source_vis)
}

fn set_namespace_visibility<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    rtindex: i32,
    visible: bool,
) -> PgResult<()> {
    for slot in pstate.p_namespace.iter_mut() {
        if slot.p_rtindex != rtindex {
            continue;
        }
        if slot.p_rel_visible != visible || slot.p_cols_visible != visible {
            let item = ParseNamespaceItem {
                p_names: slot.p_names,
                p_rte: slot.p_rte,
                p_rtindex: slot.p_rtindex,
                p_perminfo: slot.p_perminfo,
                p_nscolumns: slot.p_nscolumns,
                p_rel_visible: visible,
                p_cols_visible: visible,
                p_lateral_only: core::cell::Cell::new(slot.p_lateral_only.get()),
                p_lateral_ok: core::cell::Cell::new(slot.p_lateral_ok.get()),
                p_returning_type: slot.p_returning_type,
            };
            *slot = mcx::leak_in(mcx::alloc_in(mcx, item)?);
        }
        break;
    }
    Ok(())
}

#[cold]
fn merge_with_recursive() -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("WITH RECURSIVE is not supported for MERGE statement".to_string())
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMergeStmt")),
    )
}

#[cold]
fn unreachable_when_clause() -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(
                "unreachable WHEN clause specified after unconditional WHEN clause"
                    .to_string(),
            )
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMergeStmt")),
    )
}

#[cold]
fn merge_bad_relkind(rel: &types_rel::Relation<'_>) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let relname = core::str::from_utf8(rel.rd_rel.relname.name_str())
        .unwrap_or_default()
        .to_owned();
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot execute MERGE on relation \"{relname}\""))
            .errdetail(
                pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)
                    .unwrap_or_default(),
            )
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMergeStmt")),
    )
}

#[cold]
fn duplicate_source_alias(name: &str) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DUPLICATE_ALIAS, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!("name \"{name}\" specified more than once"))
            .errdetail(
                "The name is used both as MERGE target table and data source.".to_string(),
            )
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMergeStmt")),
    )
}
