use core::mem;

use mcx::{Mcx, PgVec};
use parse_clause::{transformLimitClause, transformSortClause};
use parse_collate::assign_query_collations;
use parse_expr::{expr_location, expr_type, expr_typmod};
use parser_small1::{
    free_parsestate, make_parsestate, parser_errposition, ParseExprKind, ParseNamespaceColumn,
    ParseState,
};
use types_core::catalog::{RECORDARRAYOID, RECORDOID, UNKNOWNOID};
use types_core::{AttrNumber, Index, Oid, ParseLoc};
use types_error::{PgError, PgResult, ERRCODE_QUERY_CANCELED};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource, SetOperation, SetOperationStmt, SortGroupClause};
use types_nodes::rawnodes::SelectStmt;
use types_nodes::{
    Alias, Bitmapset, FromExpr, JoinType, Node, NodeList, NodeTag, SetToDefault, TargetEntry, Var,
    VarReturningType,
};

pub(crate) fn transformSetOperationStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_SELECT;

    let mut leftmost = stmt;
    while leftmost.op != SetOperation::SETOP_NONE {
        leftmost = leftmost.larg.expect("set-op node has larg");
    }
    if let Some(into) = leftmost.intoClause {
        if !pstate.p_consumed_into.is_some_and(|c| c.ptr_eq(into)) {
            return Err(crate::select_into_error(
                pstate,
                into,
                "SELECT ... INTO is not allowed here",
            ));
        }
    }

    // C extracts ORDER BY/LIMIT/locking/WITH off the top node and NULLs them
    // before recursing; here the raw tree stays shared, so the tree walk
    // ignores them on the isTopLevel node instead.
    if !stmt.lockingClause.is_nil() {
        return Err(crate::locking_not_allowed_with(
            crate::first_locking_strength(&stmt.lockingClause),
            "UNION/INTERSECT/EXCEPT",
            "transformSetOperationStmt",
        ));
    }

    if let Some(with) = stmt.withClause {
        qry.hasRecursive = with.as_with_clause().expect("withClause").recursive;
        qry.cteList = crate::parse_cte::transformWithClause(mcx, pstate, with)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    let sostmt_node = transformSetOperationTree(mcx, pstate, stmt, true, None)?;
    let sostmt = sostmt_node.as_set_operation_stmt().expect("top set-op node");
    qry.setOperations = Some(sostmt_node);

    let mut node = sostmt.larg.expect("set-op node has larg");
    while let Some(op) = node.as_set_operation_stmt() {
        node = op.larg.expect("set-op node has larg");
    }
    let leftmostRTI = node.as_range_tbl_ref().expect("set-op leaf is a RangeTblRef").rtindex;
    let leftmostQuery = pstate
        .p_rtable
        .nth(leftmostRTI as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell")
        .subquery
        .expect("set-op leaf RTE has a subquery");

    let ncols = sostmt.colTypes.len();
    let mut targetvars = NodeList::nil();
    let mut targetnames = NodeList::nil();
    let mut sortnscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, ncols)?;

    for (i, lefttle_node) in leftmostQuery.targetList.iter().take(ncols).enumerate() {
        let lefttle = lefttle_node.as_target_entry().expect("tlist cell");
        debug_assert!(!lefttle.resjunk);
        let colType = sostmt.colTypes.nth(i);
        let colTypmod = sostmt.colTypmods.nth(i);
        let colCollation = sostmt.colCollations.nth(i);
        let colName = lefttle.resname.expect("non-junk tlist entry has resname");
        let var = Node::mk(
            mcx,
            Var {
                varno: leftmostRTI,
                varattno: lefttle.resno,
                vartype: colType,
                vartypmod: colTypmod,
                varcollid: colCollation,
                varnullingrels: Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
                varnosyn: leftmostRTI as Index,
                varattnosyn: lefttle.resno,
                location: expr_location(lefttle.expr),
            },
        )?;
        let resno = pstate.p_next_resno as AttrNumber;
        pstate.p_next_resno += 1;
        let tle = Node::mk_target_entry(mcx, var, resno, Some(colName), false)?;
        qry.targetList.lappend(mcx, tle)?;
        targetvars.lappend(mcx, var)?;
        targetnames.lappend(mcx, Node::mk_string(mcx, colName)?)?;
        sortnscolumns.push(ParseNamespaceColumn {
            p_varno: leftmostRTI as Index,
            p_varattno: lefttle.resno,
            p_vartype: colType,
            p_vartypmod: colTypmod,
            p_varcollid: colCollation,
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: leftmostRTI as Index,
            p_varattnosyn: lefttle.resno,
            p_dontexpand: false,
        });
    }

    let sv_rtable_length = pstate.p_rtable.len();

    let jnsitem = parse_relation::addRangeTableEntryForJoin(
        mcx,
        pstate,
        &targetnames,
        sortnscolumns,
        JoinType::JOIN_INNER,
        0,
        targetvars,
        types_nodes::list::IntList::nil(),
        types_nodes::list::IntList::nil(),
        None,
        None,
        false,
    )?;

    let sv_namespace = mem::replace(&mut pstate.p_namespace, PgVec::new_in(mcx));
    parse_relation::addNSItemToQuery(mcx, pstate, jnsitem, false, false, true)?;

    let tllen = qry.targetList.len();

    qry.sortClause = transformSortClause(
        mcx,
        pstate,
        &stmt.sortClause,
        &mut qry.targetList,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    pstate.p_namespace = sv_namespace;
    pstate.p_rtable.truncate(sv_rtable_length);

    if tllen != qry.targetList.len() {
        return Err(invalid_setop_order_by(pstate, qry.targetList.nth(tllen)));
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

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: None })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs.get();

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs.get()
        || !qry.groupClause.is_nil()
        || !qry.groupingSets.is_nil()
        || qry.havingQual.is_some()
    {
        parse_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    Ok(qry)
}

fn transformSetOperationTree<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
    isTopLevel: bool,
    targetlist: Option<&mut NodeList<'mcx>>,
) -> PgResult<Node<'mcx>> {
    stack_depth::check_stack_depth()?;

    if let Some(into) = stmt.intoClause {
        if !pstate.p_consumed_into.is_some_and(|c| c.ptr_eq(into)) {
            return Err(crate::select_into_error(
                pstate,
                into,
                "INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT",
            ));
        }
    }
    if !stmt.lockingClause.is_nil() {
        return Err(crate::locking_not_allowed_with(
            crate::first_locking_strength(&stmt.lockingClause),
            "UNION/INTERSECT/EXCEPT",
            "transformSetOperationTree",
        ));
    }

    // The isTopLevel caller consumed ORDER BY/LIMIT/locking/WITH (C NULLs them
    // on the node instead), so they must not force the leaf path here.
    let isLeaf = if stmt.op == SetOperation::SETOP_NONE {
        debug_assert!(stmt.larg.is_none() && stmt.rarg.is_none());
        true
    } else {
        debug_assert!(stmt.larg.is_some() && stmt.rarg.is_some());
        !isTopLevel
            && (!stmt.sortClause.is_nil()
                || stmt.limitOffset.is_some()
                || stmt.limitCount.is_some()
                || !stmt.lockingClause.is_nil()
                || stmt.withClause.is_some())
    };

    if isLeaf {
        let selectQuery = set_op_leaf_analyze(mcx, pstate, stmt)?;

        let query_node = Node::mk(mcx, selectQuery)?;

        // C (analyze.c:2124-2139): a non-empty namespace happens inside a
        // rule, where OLD/NEW are visible; the member statement only errors
        // if it actually references a same-level relation.
        if !pstate.p_namespace.is_empty() && vars::contain_vars_of_level(query_node, 1)? {
            return Err(setop_refers_to_same_level(
                pstate,
                vars::locate_var_of_level(query_node, 1)?,
            ));
        }

        let selectQuery = query_node.as_query().expect("just built");

        let mut columns: PgVec<'mcx, (Option<&'mcx str>, Oid, i32, Oid)> =
            mcx::vec_with_capacity_in(mcx, selectQuery.targetList.len())?;
        if let Some(tl) = targetlist {
            *tl = NodeList::nil();
            for tle_node in &selectQuery.targetList {
                if !tle_node.as_target_entry().expect("tlist cell").resjunk {
                    tl.lappend(mcx, tle_node)?;
                }
            }
        }
        for tle_node in &selectQuery.targetList {
            let te = tle_node.as_target_entry().expect("tlist cell");
            if te.resjunk {
                continue;
            }
            columns.push((
                te.resname,
                expr_type(te.expr),
                expr_typmod(te.expr),
                parse_expr::expr_collation(te.expr),
            ));
        }

        let selectName = str_in(mcx, &format!("*SELECT* {}", pstate.p_rtable.len() + 1))?;
        let alias = Node::mk_mut(
            mcx,
            Alias { aliasname: Some(selectName), colnames: NodeList::nil() },
        )?
        .seal_ref();
        let nsitem = parse_relation::addRangeTableEntryForSubquery(
            mcx,
            pstate,
            selectQuery,
            &columns,
            Some(alias),
            false,
            false,
        )?;

        Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)
    } else {
        let context: &'static str = match stmt.op {
            SetOperation::SETOP_UNION => "UNION",
            SetOperation::SETOP_INTERSECT => "INTERSECT",
            _ => "EXCEPT",
        };
        let recursive = pstate
            .p_parent_cte
            .is_some_and(|c| c.as_common_table_expr().expect("parent CTE").cterecursive);

        let mut ltargetlist = NodeList::nil();
        let larg = transformSetOperationTree(
            mcx,
            pstate,
            stmt.larg.expect("set-op node has larg"),
            false,
            Some(&mut ltargetlist),
        )?;

        // The non-recursive term's outputs become the containing CTE's result
        // columns; only at the topmost setop of the CTE.
        if isTopLevel && recursive {
            determineRecursiveColTypes(mcx, pstate, larg, &ltargetlist)?;
        }

        let mut rtargetlist = NodeList::nil();
        let rarg = transformSetOperationTree(
            mcx,
            pstate,
            stmt.rarg.expect("set-op node has rarg"),
            false,
            Some(&mut rtargetlist),
        )?;

        if ltargetlist.len() != rtargetlist.len() {
            return Err(setop_column_count_mismatch(pstate, context, &rtargetlist));
        }

        let mut targetlist = targetlist;
        if let Some(tl) = targetlist.as_deref_mut() {
            *tl = NodeList::nil();
        }
        let mut colTypes = types_nodes::list::OidList::nil();
        let mut colTypmods = types_nodes::list::IntList::nil();
        let mut colCollations = types_nodes::list::OidList::nil();
        let mut groupClauses = NodeList::nil();

        for (ltle_node, rtle_node) in ltargetlist.iter().zip(rtargetlist.iter()) {
            let mut lcolnode = ltle_node.as_target_entry().expect("tlist cell").expr;
            let mut rcolnode = rtle_node.as_target_entry().expect("tlist cell").expr;
            let lcoltype = expr_type(lcolnode);
            let rcoltype = expr_type(rcolnode);
            let lloc = expr_location(lcolnode);
            let rloc = expr_location(rcolnode);

            let (rescoltype, pick) = coerce::select_common_type_pick(
                pstate,
                &[(lcoltype, lloc), (rcoltype, rloc)],
                Some(context),
            )?;
            let bestlocation = if pick == 0 { lloc } else { rloc };

            // Non-UNKNOWN children: verify coercibility without modifying the
            // child query. UNKNOWN Const/Param leaf outputs: replace in place.
            if lcoltype != UNKNOWNOID {
                lcolnode = coerce::coerce_to_common_type(
                    mcx, pstate, lcolnode, lcoltype, lloc, rescoltype, context,
                )?;
            } else if matches!(lcolnode.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
                lcolnode = coerce::coerce_to_common_type(
                    mcx, pstate, lcolnode, lcoltype, lloc, rescoltype, context,
                )?;
                // SAFETY: parser-owned tlist; no reference derived from the
                // TargetEntry is live here.
                unsafe { ltle_node.with_mut::<TargetEntry, _>(|t| t.expr = lcolnode) }
                    .expect("tlist cell");
            }

            if rcoltype != UNKNOWNOID {
                rcolnode = coerce::coerce_to_common_type(
                    mcx, pstate, rcolnode, rcoltype, rloc, rescoltype, context,
                )?;
            } else if matches!(rcolnode.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
                rcolnode = coerce::coerce_to_common_type(
                    mcx, pstate, rcolnode, rcoltype, rloc, rescoltype, context,
                )?;
                // SAFETY: as above.
                unsafe { rtle_node.with_mut::<TargetEntry, _>(|t| t.expr = rcolnode) }
                    .expect("tlist cell");
            }

            let rescoltypmod = coerce::select_common_typmod(
                &[
                    (expr_type(lcolnode), expr_typmod(lcolnode)),
                    (expr_type(rcolnode), expr_typmod(rcolnode)),
                ],
                rescoltype,
            );

            let rescolcoll = parse_collate::select_common_collation(
                mcx,
                pstate,
                &NodeList::make2(mcx, lcolnode, rcolnode)?,
                stmt.op == SetOperation::SETOP_UNION && stmt.all,
            )?;

            colTypes.lappend(mcx, rescoltype)?;
            colTypmods.lappend(mcx, rescoltypmod)?;
            colCollations.lappend(mcx, rescolcoll)?;

            if stmt.op != SetOperation::SETOP_UNION || !stmt.all {
                let grpcl = makeSortGroupClauseForSetOp(mcx, rescoltype, recursive)
                    .map_err(|e| attach_parser_errposition(pstate, bestlocation, e))?;
                groupClauses.lappend(mcx, grpcl)?;
            }

            if let Some(tl) = targetlist.as_deref_mut() {
                let rescolnode = Node::mk(
                    mcx,
                    SetToDefault {
                        typeId: rescoltype,
                        typeMod: rescoltypmod,
                        collation: rescolcoll,
                        location: bestlocation,
                    },
                )?;
                tl.lappend(mcx, Node::mk_target_entry(mcx, rescolnode, 0, None, false)?)?;
            }
        }

        Node::mk(
            mcx,
            SetOperationStmt {
                op: stmt.op,
                all: stmt.all,
                larg: Some(larg),
                rarg: Some(rarg),
                colTypes,
                colTypmods,
                colCollations,
                groupClauses,
            },
        )
    }
}

fn determineRecursiveColTypes<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    larg: Node<'mcx>,
    nrtargetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    let mut node = larg;
    while let Some(op) = node.as_set_operation_stmt() {
        node = op.larg.expect("set-op node has larg");
    }
    let leftmostRTI = node.as_range_tbl_ref().expect("set-op leaf is a RangeTblRef").rtindex;
    let leftmostQuery = pstate
        .p_rtable
        .nth(leftmostRTI as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell")
        .subquery
        .expect("set-op leaf RTE has a subquery");

    let mut targetList = NodeList::nil();
    let mut next_resno: AttrNumber = 1;
    for (nrtle_node, lefttle_node) in nrtargetlist.iter().zip(leftmostQuery.targetList.iter()) {
        let nrtle = nrtle_node.as_target_entry().expect("tlist cell");
        let lefttle = lefttle_node.as_target_entry().expect("tlist cell");
        debug_assert!(!lefttle.resjunk);
        let colName = lefttle.resname.expect("non-junk tlist entry has resname");
        let tle = Node::mk_target_entry(mcx, nrtle.expr, next_resno, Some(colName), false)?;
        next_resno += 1;
        targetList.lappend(mcx, tle)?;
    }

    let parent_cte = pstate.p_parent_cte.expect("recursive set-op has a parent CTE");
    crate::parse_cte::analyzeCTETargetList(mcx, pstate, parent_cte, &targetList)
}

// C reaches leaves through parse_sub_analyze; SelectStmt children are borrows
// here, so this rebuilds its sub-pstate + transformStmt shape without a Node.
fn set_op_leaf_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    parent: &ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, Some(parent));
    pstate.p_parent_cte = None;
    pstate.p_locked_from_parent = false;
    pstate.p_resolve_unknowns = false;

    let mut query = if !stmt.valuesLists.is_nil() {
        crate::transformValuesClause(mcx, &mut pstate, stmt)?
    } else if stmt.op == SetOperation::SETOP_NONE {
        crate::transformSelectStmt(mcx, &mut pstate, stmt)?
    } else {
        transformSetOperationStmt(mcx, &mut pstate, stmt)?
    };
    query.querySource = QuerySource::QSRC_ORIGINAL;
    query.canSetTag = true;

    free_parsestate(pstate)?;
    Ok(query)
}

pub fn makeSortGroupClauseForSetOp<'mcx>(
    mcx: Mcx<'mcx>,
    rescoltype: Oid,
    require_hash: bool,
) -> PgResult<Node<'mcx>> {
    let ops = parse_oper::get_sort_group_operators(rescoltype, false, true, false, true)?;
    // C: the typcache never reports record as hashable, but a recursive union
    // needs hashing — assume it and let execution fail if a field can't hash.
    let hashable = ops.hashable
        || (require_hash && (rescoltype == RECORDOID || rescoltype == RECORDARRAYOID));
    Node::mk(
        mcx,
        SortGroupClause {
            tleSortGroupRef: 0,
            eqop: ops.eq_opr,
            sortop: ops.lt_opr,
            reverse_sort: false,
            nulls_first: false,
            hashable,
        },
    )
}

// C setup_parser_errposition_callback pattern: attach the position on Err.
fn attach_parser_errposition(
    pstate: &ParseState<'_, '_>,
    location: ParseLoc,
    e: Box<PgError>,
) -> Box<PgError> {
    if e.sqlstate() == ERRCODE_QUERY_CANCELED {
        return e;
    }
    let pos = parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    Box::new((*e).with_cursor_position(pos))
}

// C exprLocation over a List of TargetEntry nodes: first member with a
// location.
fn tlist_location(tlist: &NodeList<'_>) -> ParseLoc {
    for tle_node in tlist {
        let l = expr_location(tle_node.as_target_entry().expect("tlist cell").expr);
        if l >= 0 {
            return l;
        }
    }
    -1
}

#[track_caller]
#[cold]
fn setop_column_count_mismatch(
    pstate: &ParseState<'_, '_>,
    context: &str,
    rtargetlist: &NodeList<'_>,
) -> Box<PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("each {context} query must have the same number of columns"))
            .errposition(parser_errposition(
                pstate,
                tlist_location(rtargetlist),
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSetOperationTree")),
    )
}

#[track_caller]
#[cold]
fn setop_refers_to_same_level(pstate: &ParseState<'_, '_>, location: i32) -> Box<PgError> {
    use types_error::{ErrorLocation, ERRCODE_INVALID_COLUMN_REFERENCE, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(
                "UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of \
                 same query level"
                    .to_string(),
            )
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSetOperationTree")),
    )
}

#[track_caller]
#[cold]
fn invalid_setop_order_by(pstate: &ParseState<'_, '_>, extra_tle: Node<'_>) -> Box<PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let location = expr_location(extra_tle.as_target_entry().expect("tlist cell").expr);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("invalid UNION/INTERSECT/EXCEPT ORDER BY clause".to_string())
            .errdetail("Only result column names can be used, not expressions or functions.")
            .errhint(
                "Add the expression/function to every SELECT, or move the UNION into a \
                 FROM clause.",
            )
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSetOperationStmt")),
    )
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}
