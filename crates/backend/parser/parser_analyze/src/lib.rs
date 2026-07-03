#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use core::mem;

use guc_tables::consts::{
    COMPUTE_QUERY_ID_OFF, COMPUTE_QUERY_ID_ON, COMPUTE_QUERY_ID_REGRESS,
};
use mcx::Mcx;
use parse_clause::{
    transformFromClause, transformGroupClause, transformLimitClause, transformSortClause,
    transformWhereClause, transformWindowDefinitions,
};
use parse_collate::assign_query_collations;
use parse_target::{markTargetListOrigins, resolveTargetListUnknowns, transformTargetList};
use parser_small1::{
    free_parsestate, make_parsestate, setup_parse_fixed_parameters, ParseExprKind, ParseState,
};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource};
use types_nodes::primnodes::FromExpr;
use types_nodes::rawnodes::{RawStmt, SelectStmt};
use types_nodes::{Node, NodeTag};
use types_portal::QueryEnvHandle;

pub fn init_seams() {
    analyze_seams::parse_analyze_fixedparams::set(parse_analyze_fixedparams);
    analyze_seams::parse_analyze_varparams::set(parse_analyze_varparams);
    analyze_seams::analyze_requires_snapshot::set(analyze_requires_snapshot);
}

pub fn parse_analyze_fixedparams<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &'a RawStmt<'mcx>,
    source_text: &'a str,
    param_types: &'a [Oid],
    query_env: QueryEnvHandle,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    if !param_types.is_empty() {
        setup_parse_fixed_parameters(&mut pstate, param_types);
    }

    if !query_env.is_null() {
        panic!(
            "parse_analyze_fixedparams (analyze.c): QueryEnvHandle resolution unported \
             (SPI/trigger transition tables) — unit backend-parser-analyze"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    if is_query_id_enabled() {
        panic!(
            "parse_analyze_fixedparams (analyze.c): JumbleQuery (queryjumble.c) unported \
             — compute_query_id is on"
        );
    }

    free_parsestate(pstate)?;

    backend_status::pgstat_report_query_id(query.queryId, false);

    Ok(query)
}

/// C parse_analyze_varparams: `paramTypes`/`numParams` are in-out there; here
/// the resolved types come back as the second tuple element.
pub fn parse_analyze_varparams<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &'a RawStmt<'mcx>,
    source_text: &'a str,
    param_types: &'a [Oid],
    query_env: QueryEnvHandle,
) -> PgResult<(Query<'mcx>, mcx::PgVec<'mcx, Oid>)> {
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    let parstate = parser_small1::VarParamState {
        param_types: std::rc::Rc::new(core::cell::RefCell::new(param_types.to_vec())),
    };
    parser_small1::setup_parse_variable_parameters(&mut pstate, parstate.clone());

    if !query_env.is_null() {
        panic!(
            "parse_analyze_varparams (analyze.c): QueryEnvHandle resolution unported \
             (SPI/trigger transition tables) — unit backend-parser-analyze"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    // check_variable_parameters walks &'mcx nodes; park the Query in the
    // arena for the walk, then move it back out of the (dead) slot.
    let slot: *mut Query<'mcx> = mcx::leak_in(mcx::alloc_in(mcx, query)?);
    // SAFETY: the walk's shared borrows end before the take; the arena slot
    // is reachable only through `slot`.
    parser_small1::check_variable_parameters(
        &pstate,
        unsafe { &*slot },
        mbutils::GetDatabaseEncoding(),
    )?;
    let query = mem::take(unsafe { &mut *slot });

    if is_query_id_enabled() {
        panic!(
            "parse_analyze_varparams (analyze.c): JumbleQuery (queryjumble.c) unported \
             — compute_query_id is on"
        );
    }

    free_parsestate(pstate)?;

    backend_status::pgstat_report_query_id(query.queryId, false);

    let resolved = parstate.param_types.borrow();
    let mut out: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    out.try_reserve_exact(resolved.len()).map_err(|_| mcx.oom(resolved.len()))?;
    for &t in resolved.iter() {
        out.push(t);
    }
    Ok((query, out))
}

// C `IsQueryIdEnabled` (queryjumble.h); the AUTO arm reads `query_id_enabled`,
// which only `EnableQueryId()` (unported jumble consumers) ever sets.
fn is_query_id_enabled() -> bool {
    match guc_tables::backing::compute_query_id() {
        COMPUTE_QUERY_ID_OFF => false,
        COMPUTE_QUERY_ID_ON | COMPUTE_QUERY_ID_REGRESS => true,
        _ => false,
    }
}

pub fn parse_sub_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: Node<'mcx>,
    parent_parse_state: &ParseState<'_, 'mcx>,
    parent_cte: Option<Node<'mcx>>,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, Some(parent_parse_state));
    pstate.p_parent_cte = parent_cte;
    pstate.p_locked_from_parent = locked_from_parent;
    pstate.p_resolve_unknowns = resolve_unknowns;

    let query = transformStmt(mcx, &mut pstate, parse_tree)?;

    free_parsestate(pstate)?;
    Ok(query)
}

pub fn transformTopLevelStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: &RawStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let stmt = parse_tree.stmt.expect("RawStmt.stmt is never NULL");
    let mut result = transformOptionalSelectInto(mcx, pstate, stmt)?;
    result.stmt_location = parse_tree.stmt_location;
    result.stmt_len = parse_tree.stmt_len;
    Ok(result)
}

fn transformOptionalSelectInto<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    if let Some(mut stmt) = parse_tree.as_select_stmt() {
        while stmt.op != types_nodes::parsenodes::SetOperation::SETOP_NONE {
            stmt = stmt.larg.expect("set-op tree always has a leftmost SelectStmt");
        }
        if stmt.intoClause.is_some() {
            panic!(
                "transformOptionalSelectInto (analyze.c): SELECT INTO -> CREATE TABLE AS \
                 rewrite unported (CreateTableAsStmt vocabulary + \
                 transformCreateTableAsStmt) — unit backend-parser-analyze"
            );
        }
    }
    transformStmt(mcx, pstate, parse_tree)
}

pub fn transformStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut result = match parse_tree.node_tag() {
        NodeTag::T_SelectStmt => {
            let n = parse_tree.as_select_stmt().unwrap();
            if !n.valuesLists.is_nil() {
                panic!(
                    "transformStmt (analyze.c): transformValuesClause unported — \
                     unit backend-parser-analyze"
                );
            } else if n.op == types_nodes::parsenodes::SetOperation::SETOP_NONE {
                transformSelectStmt(mcx, pstate, n)?
            } else {
                panic!(
                    "transformStmt (analyze.c): transformSetOperationStmt unported — \
                     unit backend-parser-analyze"
                );
            }
        }
        NodeTag::T_InsertStmt => {
            transformInsertStmt(mcx, pstate, parse_tree.as_insert_stmt().unwrap())?
        }
        NodeTag::T_DeleteStmt => {
            transformDeleteStmt(mcx, pstate, parse_tree.as_delete_stmt().unwrap())?
        }
        NodeTag::T_UpdateStmt => {
            transformUpdateStmt(mcx, pstate, parse_tree.as_update_stmt().unwrap())?
        }
        t @ (NodeTag::T_MergeStmt
        | NodeTag::T_ReturnStmt
        | NodeTag::T_PLAssignStmt
        | NodeTag::T_DeclareCursorStmt
        | NodeTag::T_ExplainStmt
        | NodeTag::T_CreateTableAsStmt
        | NodeTag::T_CallStmt) => panic!(
            "transformStmt (analyze.c): transform arm for {t:?} unported — \
             unit backend-parser-analyze"
        ),
        _ => {
            let mut result = Query::default();
            result.commandType = CmdType::CMD_UTILITY;
            result.utilityStmt = Some(parse_tree);
            result
        }
    };

    result.querySource = QuerySource::QSRC_ORIGINAL;
    result.canSetTag = true;
    Ok(result)
}

pub fn stmt_requires_parse_analysis(parse_tree: &RawStmt<'_>) -> bool {
    let stmt = parse_tree.stmt.expect("RawStmt.stmt is never NULL");
    matches!(
        stmt.node_tag(),
        NodeTag::T_InsertStmt
            | NodeTag::T_DeleteStmt
            | NodeTag::T_UpdateStmt
            | NodeTag::T_MergeStmt
            | NodeTag::T_SelectStmt
            | NodeTag::T_ReturnStmt
            | NodeTag::T_PLAssignStmt
            | NodeTag::T_DeclareCursorStmt
            | NodeTag::T_ExplainStmt
            | NodeTag::T_CreateTableAsStmt
            | NodeTag::T_CallStmt
    )
}

pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool {
    stmt_requires_parse_analysis(parse_tree)
}

fn transformSelectStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_SELECT;

    if stmt.withClause.is_some() {
        panic!(
            "transformSelectStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    if stmt.intoClause.is_some() {
        panic!(
            "transformSelectStmt (analyze.c): \"SELECT ... INTO is not allowed here\" \
             ereport needs IntoClause vocabulary — unit backend-parser-analyze"
        );
    }

    // C aliases the raw lists into pstate; header-clone until a shared-list
    // story exists (both are on loud-panic statement shapes today).
    if !stmt.lockingClause.is_nil() {
        pstate.p_locking_clause = stmt.lockingClause.clone_in(mcx)?;
    }
    if !stmt.windowClause.is_nil() {
        pstate.p_windowdefs = stmt.windowClause.clone_in(mcx)?;
    }

    transformFromClause(mcx, pstate, &stmt.fromClause)?;

    qry.targetList =
        transformTargetList(mcx, pstate, &stmt.targetList, ParseExprKind::EXPR_KIND_SELECT_TARGET)?;

    markTargetListOrigins(pstate, &qry.targetList)?;

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    qry.havingQual = transformWhereClause(
        mcx,
        pstate,
        stmt.havingClause,
        ParseExprKind::EXPR_KIND_HAVING,
        "HAVING",
    )?;

    qry.sortClause = transformSortClause(
        mcx,
        pstate,
        &stmt.sortClause,
        &mut qry.targetList,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    qry.groupClause = transformGroupClause(
        mcx,
        pstate,
        &stmt.groupClause,
        &mut qry.groupingSets,
        &mut qry.targetList,
        &qry.sortClause,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )?;
    qry.groupDistinct = stmt.groupDistinct;

    if !stmt.distinctClause.is_none() {
        panic!(
            "transformSelectStmt (analyze.c): transformDistinctClause/\
             transformDistinctOnClause unported (gram repr: DistinctClause::All = \
             C's one-NULL-cell list, DistinctClause::On = DISTINCT ON exprs) — \
             unit backend-parser-clause"
        );
    }

    qry.limitOffset = transformLimitClause(
        mcx,
        pstate,
        stmt.limitOffset,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    qry.limitCount = transformLimitClause(
        mcx,
        pstate,
        stmt.limitCount,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOption = stmt.limitOption;

    let windowdefs = mem::take(&mut pstate.p_windowdefs);
    qry.windowClause = transformWindowDefinitions(pstate, &windowdefs, &mut qry.targetList)?;

    if pstate.p_resolve_unknowns {
        resolveTargetListUnknowns(mcx, pstate, &qry.targetList)?;
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    if !pstate.p_locking_clause.is_nil() {
        panic!(
            "transformSelectStmt (analyze.c): transformLockingClause unported — \
             unit backend-parser-analyze"
        );
    }

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs
        || !qry.groupClause.is_nil()
        || !qry.groupingSets.is_nil()
        || qry.havingQual.is_some()
    {
        parse_agg::parseCheckAggregates(pstate, &qry)?;
    }

    Ok(qry)
}

fn transformInsertStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::InsertStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_INSERT;

    let mut qry = Query::default();
    debug_assert!(pstate.p_ctenamespace.is_nil());
    qry.commandType = CmdType::CMD_INSERT;
    pstate.p_is_insert = true;

    if stmt.withClause.is_some() {
        panic!(
            "transformInsertStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }
    qry.r#override = stmt.r#override;
    if stmt.onConflictClause.is_some() {
        panic!(
            "transformInsertStmt (analyze.c): transformOnConflictClause unported — \
             unit backend-parser-analyze"
        );
    }

    let select_stmt = stmt.selectStmt.map(|n| n.as_select_stmt().expect("grammar builds SelectStmt"));
    let is_general_select = select_stmt.is_some_and(|s| {
        s.valuesLists.is_nil()
            || !s.sortClause.is_nil()
            || s.limitOffset.is_some()
            || s.limitCount.is_some()
            || !s.lockingClause.is_nil()
            || s.withClause.is_some()
    });
    if is_general_select {
        panic!(
            "transformInsertStmt (analyze.c): INSERT ... SELECT arm \
             (sub-pstate + addRangeTableEntryForSubquery) unported — \
             unit backend-parser-analyze"
        );
    }
    // The CREATE RULE rtable pass-down only matters for isGeneralSelect.
    debug_assert!(pstate.p_rtable.is_nil());

    let relation = stmt
        .relation
        .expect("grammar always sets InsertStmt.relation")
        .as_range_var()
        .expect("insert_target is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, false, false, ACL_INSERT)?;

    let (icolumns, attrnos) = parse_target::checkInsertTargets(mcx, pstate, &stmt.cols)?;
    debug_assert_eq!(icolumns.len(), attrnos.len());

    let expr_list: types_nodes::NodeList<'mcx> = match select_stmt {
        None => types_nodes::NodeList::nil(),
        Some(sel) if sel.valuesLists.len() > 1 => {
            let mut exprs_lists = types_nodes::NodeList::nil();
            let mut coltypes = types_nodes::list::OidList::nil();
            let mut coltypmods = types_nodes::list::IntList::nil();
            let mut colcollations = types_nodes::list::OidList::nil();
            let mut sublist_length: i64 = -1;
            for sublist_node in &sel.valuesLists {
                let sublist = sublist_node.as_list().expect("VALUES row is a List");
                let sublist = parse_target::transformExpressionList(
                    mcx,
                    pstate,
                    sublist,
                    ParseExprKind::EXPR_KIND_VALUES,
                    true,
                )?;
                if sublist_length < 0 {
                    sublist_length = sublist.len() as i64;
                } else if sublist_length != sublist.len() as i64 {
                    return Err(values_length_mismatch(pstate, &sublist));
                }
                let sublist =
                    transformInsertRow(mcx, pstate, sublist, &stmt.cols, &icolumns, &attrnos, true)?;
                parse_collate::assign_list_collations(mcx, pstate, &sublist)?;
                exprs_lists.lappend(mcx, Node::mk_list(mcx, sublist)?)?;
            }

            for val in exprs_lists.nth(0).as_list().expect("row list").iter() {
                coltypes.lappend(mcx, parse_expr::expr_type(val))?;
                coltypmods.lappend(mcx, parse_expr::expr_typmod(val))?;
                colcollations.lappend(mcx, 0)?;
            }

            // contain_vars_of_level lateral marking only fires inside CREATE
            // RULE (NEW/OLD in the rtable); the target rel is the only RTE.
            debug_assert_eq!(pstate.p_rtable.len(), 1);
            let nsitem = parse_relation::addRangeTableEntryForValues(
                mcx,
                pstate,
                exprs_lists,
                coltypes,
                coltypmods,
                colcollations,
                None,
                false,
                true,
            )?;
            let (vars, _names) = parse_relation::expandNSItemVars(mcx, pstate, nsitem, 0, -1)?;
            parse_relation::addNSItemToQuery(mcx, pstate, nsitem, true, false, false)?;
            transformInsertRow(mcx, pstate, vars, &stmt.cols, &icolumns, &attrnos, false)?
        }
        Some(sel) => {
            debug_assert_eq!(sel.valuesLists.len(), 1);
            debug_assert!(sel.intoClause.is_none());
            let values = sel.valuesLists.nth(0).as_list().expect("VALUES row is a List");
            let expr_list = parse_target::transformExpressionList(
                mcx,
                pstate,
                values,
                ParseExprKind::EXPR_KIND_VALUES_SINGLE,
                true,
            )?;
            transformInsertRow(mcx, pstate, expr_list, &stmt.cols, &icolumns, &attrnos, false)?
        }
    };

    let perminfo = pstate
        .p_target_nsitem
        .expect("setTargetTable set p_target_nsitem")
        .p_perminfo
        .expect("target nsitem has perminfo");
    debug_assert!(expr_list.len() <= icolumns.len());
    let mut target_list = types_nodes::NodeList::nil();
    for (i, (expr, icol)) in expr_list.iter().zip(icolumns.iter()).enumerate() {
        let col = icol.as_res_target().expect("icolumns are ResTargets");
        let attr_num = attrnos[i] as i16;
        let tle = Node::mk_target_entry(mcx, expr, attr_num, col.name, false)?;
        target_list.lappend(mcx, tle)?;
        // SAFETY: perminfo nodes are read only through transient as_*
        // lookups; no derived reference is live across this write.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.insertedCols.add_member(
                    mcx,
                    attr_num as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
                )
            })
        }
        .expect("p_perminfo is RTEPermissionInfo")?;
    }
    qry.targetList = target_list;

    if stmt.returningClause.is_some() {
        panic!(
            "transformInsertStmt (analyze.c): transformReturningClause unported — \
             unit backend-parser-analyze"
        );
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: None })?
            .seal_ref(),
    );
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;
    Ok(qry)
}

fn transformDeleteStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::DeleteStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_DELETE;

    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_DELETE;

    if stmt.withClause.is_some() {
        panic!(
            "transformDeleteStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    let relation = stmt
        .relation
        .expect("grammar always sets DeleteStmt.relation")
        .as_range_var()
        .expect("relation_expr_opt_alias is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, relation.inh, true, ACL_DELETE)?;

    // C toggles p_lateral_only around the USING transform; the nsitem is
    // shared immutably here, so a USING list is loud until that lane lands.
    if !stmt.usingClause.is_nil() {
        panic!(
            "transformDeleteStmt (analyze.c): USING list (p_lateral_only toggle + \
             join planning) unported — M2 join lane"
        );
    }

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    if stmt.returningClause.is_some() {
        panic!(
            "transformDeleteStmt (analyze.c): transformReturningClause unported — \
             unit backend-parser-analyze"
        );
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs {
        parse_agg::parseCheckAggregates(pstate, &qry)?;
    }
    Ok(qry)
}

fn transformUpdateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::UpdateStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_UPDATE;

    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_UPDATE;
    pstate.p_is_insert = false;

    if stmt.withClause.is_some() {
        panic!(
            "transformUpdateStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    let relation = stmt
        .relation
        .expect("grammar always sets UpdateStmt.relation")
        .as_range_var()
        .expect("relation_expr_opt_alias is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, relation.inh, true, ACL_UPDATE)?;

    // C toggles p_lateral_only around the FROM transform; the nsitem is
    // shared immutably here, so a FROM list is loud until that lane lands.
    if !stmt.fromClause.is_nil() {
        panic!(
            "transformUpdateStmt (analyze.c): FROM list (p_lateral_only toggle + \
             join planning) unported — M2 join lane"
        );
    }

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    if stmt.returningClause.is_some() {
        panic!(
            "transformUpdateStmt (analyze.c): transformReturningClause unported — \
             unit backend-parser-analyze"
        );
    }

    qry.targetList = transformUpdateTargetList(mcx, pstate, &stmt.targetList)?;

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;
    Ok(qry)
}

// C transformUpdateTargetList (analyze.c): resnos become the target attribute
// numbers; resjunk entries renumber past the relation's column count.
fn transformUpdateTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    orig_tlist: &types_nodes::NodeList<'mcx>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let tlist = parse_target::transformTargetList(
        mcx,
        pstate,
        orig_tlist,
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )?;

    let numattrs = pstate
        .p_target_relation
        .as_ref()
        .expect("transformUpdateTargetList with no target relation")
        .rd_att
        .natts as i32;
    if pstate.p_next_resno <= numattrs {
        pstate.p_next_resno = numattrs + 1;
    }

    let perminfo = pstate
        .p_target_nsitem
        .expect("setTargetTable set p_target_nsitem")
        .p_perminfo
        .expect("target nsitem has perminfo");

    let mut orig_iter = orig_tlist.iter();
    for tle_node in &tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            let resno = pstate.p_next_resno as i16;
            pstate.p_next_resno += 1;
            // SAFETY: parser-owned tlist; the `tle` probe above is dead here.
            unsafe {
                tle_node.with_mut::<types_nodes::TargetEntry, _>(|t| {
                    t.resno = resno;
                    t.resname = None;
                })
            }
            .expect("TargetEntry");
            continue;
        }
        let orig = orig_iter
            .next()
            .unwrap_or_else(|| panic!("UPDATE target count mismatch --- internal error"));
        let orig_target = orig.as_res_target().expect("ResTarget");
        let colname = orig_target.name.expect("set_target always has a name");

        let attrno = {
            let rel = pstate.p_target_relation.as_ref().unwrap();
            parse_relation::attnameAttNum(rel, colname, true)
        };
        if attrno == 0 {
            return Err(undefined_update_column(pstate, colname, orig_target.location));
        }

        parse_target::updateTargetListEntry(
            mcx,
            pstate,
            tle_node,
            colname,
            attrno as i32,
            &orig_target.indirection,
            orig_target.location,
        )?;

        // SAFETY: perminfo nodes are read only through transient as_*
        // lookups; no derived reference is live across this write.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.updatedCols.add_member(
                    mcx,
                    attrno as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
                )
            })
        }
        .expect("p_perminfo is RTEPermissionInfo")?;
    }
    assert!(orig_iter.next().is_none(), "UPDATE target count mismatch --- internal error");
    Ok(tlist)
}

#[cold]
fn undefined_update_column(
    pstate: &ParseState<'_, '_>,
    colname: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERROR};
    let relname = pstate
        .p_target_relation
        .as_ref()
        .map(|r| std::string::String::from_utf8_lossy(r.rd_rel.relname.name_str()).into_owned())
        .unwrap_or_default();
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{colname}\" of relation \"{relname}\" does not exist"
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformUpdateTargetList")),
    )
}

// C transformInsertRow (analyze.c). strip_indirection is inert: FieldStore/
// SubscriptingRef construction panics upstream (transformAssignedExpr).
fn transformInsertRow<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    exprlist: types_nodes::NodeList<'mcx>,
    stmtcols: &types_nodes::NodeList<'mcx>,
    icolumns: &types_nodes::NodeList<'mcx>,
    attrnos: &[i32],
    strip_indirection: bool,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let _ = strip_indirection;
    if exprlist.len() > icolumns.len() {
        return Err(insert_row_length_error(
            pstate,
            "INSERT has more expressions than target columns",
            parse_expr::expr_location(exprlist.nth(icolumns.len())),
        ));
    }
    if !stmtcols.is_nil() && exprlist.len() < icolumns.len() {
        let col = icolumns.nth(exprlist.len()).as_res_target().expect("ResTarget");
        return Err(insert_row_length_error(
            pstate,
            "INSERT has more target columns than expressions",
            col.location,
        ));
    }

    let mut result = types_nodes::NodeList::nil();
    for (i, (expr, icol)) in exprlist.iter().zip(icolumns.iter()).enumerate() {
        let col = icol.as_res_target().expect("icolumns are ResTargets");
        let expr = parse_target::transformAssignedExpr(
            mcx,
            pstate,
            expr,
            ParseExprKind::EXPR_KIND_INSERT_TARGET,
            col.name,
            attrnos[i],
            &col.indirection,
            col.location,
        )?;
        result.lappend(mcx, expr)?;
    }
    Ok(result)
}

#[cold]
fn insert_row_length_error(
    pstate: &ParseState<'_, '_>,
    msg: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg.to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformInsertRow")),
    )
}

#[cold]
fn values_length_mismatch(
    pstate: &ParseState<'_, '_>,
    sublist: &types_nodes::NodeList<'_>,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    let location = sublist.iter().next().map(parse_expr::expr_location).unwrap_or(-1);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("VALUES lists must all be the same length".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformInsertStmt")),
    )
}
