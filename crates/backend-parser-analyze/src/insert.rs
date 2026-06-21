//! `transformInsertStmt` / `transformInsertRow` (parser/analyze.c) — transform a
//! raw `InsertStmt` into an analyzed `Query`.
//!
//! Milestone scope: the `INSERT ... DEFAULT VALUES` and single-row
//! `INSERT ... VALUES (...)` paths — exactly what the type-test suite emits
//! (`INSERT INTO t(f1) VALUES (x)`). The single-VALUES branch is handled like a
//! `SELECT` with no `FROM` (the sublist becomes the query targetlist directly,
//! with no VALUES RTE), so it does not depend on the still-blocked
//! `addRangeTableEntryForValues` (which needs the List-carrier RTE keystone).
//!
//! The multi-row VALUES, INSERT/SELECT, ON CONFLICT (DO NOTHING / DO UPDATE),
//! and RETURNING branches are all transformed here.

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::Expr;
use types_nodes::rawnodes::{InsertStmt, ResTarget, SelectStmt};

use backend_utils_error::ereport;

use crate::elog_error;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h:27) — the offset used
/// to fold a real `AttrNumber` into the `RTEPermissionInfo` column bitmapset.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

/// `UNKNOWNOID` (catalog/pg_type_d.h) — the unknown literal type.
const UNKNOWNOID: Oid = 705;

/// `RECORDOID` (catalog/pg_type_d.h) — the generic record type.
const RECORDOID: Oid = 2249;

/// `transformInsertStmt(pstate, stmt)` (analyze.c:625) — transform an
/// `InsertStmt` into an analyzed `Query`.
pub fn transformInsertStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &InsertStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);

    // There can't be any outer WITH to worry about.
    debug_assert!(pstate.p_ctenamespace.is_empty());

    qry.commandType = CmdType::CMD_INSERT;
    pstate.p_is_insert = true;

    // Process the WITH clause independently of all else.
    if let Some(with) = stmt.withClause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = crate::cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    qry.r#override = stmt.r#override;

    let is_on_conflict_update = stmt
        .onConflictClause
        .as_deref()
        .map(|c| c.action == types_nodes::nodes::OnConflictAction::ONCONFLICT_UPDATE)
        .unwrap_or(false);

    // The source can be: DEFAULT VALUES (selectStmt == None), a VALUES list, or
    // a general SELECT. We special-case VALUES, both for efficiency and so we
    // can handle DEFAULT specifications.
    let select_stmt: Option<&SelectStmt<'mcx>> = match stmt.selectStmt.as_deref() {
        Some(n) => match n.as_selectstmt() {
            Some(s) => Some(s),
            None => return Err(elog_error("INSERT selectStmt is not a SelectStmt")),
        },
        None => None,
    };

    // The grammar allows attaching ORDER BY / LIMIT / FOR UPDATE / WITH to a
    // VALUES clause; if any of those is present it must be treated as a general
    // SELECT (which is not yet ported).
    let is_general_select = select_stmt.map_or(false, |s| {
        s.valuesLists.is_empty()
            || !s.sortClause.is_empty()
            || s.limitOffset.is_some()
            || s.limitCount.is_some()
            || !s.lockingClause.is_empty()
            || s.withClause.is_some()
    });

    // If a non-nil rangetable/namespace was passed in, and we are doing
    // INSERT/SELECT, arrange to pass the rangetable/rteperminfos/namespace down
    // to the SELECT (only happens inside CREATE RULE, for OLD/NEW). We must do
    // this before adding the target table to the INSERT's rtable.
    let (sub_rtable, sub_rteperminfos, sub_namespace) = if is_general_select {
        (
            core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx)),
            core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx)),
            core::mem::replace(&mut pstate.p_namespace, PgVec::new_in(mcx)),
        )
    } else {
        (PgVec::new_in(mcx), PgVec::new_in(mcx), PgVec::new_in(mcx))
    };

    // Must get write lock on the INSERT target table before scanning SELECT.
    let mut target_perms = types_acl::acl::ACL_INSERT;
    if is_on_conflict_update {
        target_perms |= types_acl::acl::ACL_UPDATE;
    }
    qry.resultRelation = backend_parser_clause::setTargetTable(
        mcx,
        pstate,
        stmt.relation
            .as_deref()
            .ok_or_else(|| elog_error("INSERT has no target relation"))?,
        false,
        false,
        target_perms,
    )?;

    // Validate stmt->cols list, or build default list if no list given.
    let (icolumns, attrnos) =
        backend_parser_parse_target::checkInsertTargets(mcx, pstate, copy_cols(mcx, &stmt.cols)?)?;
    debug_assert_eq!(icolumns.len(), attrnos.len());

    // Determine which variant of INSERT we have.
    let expr_list: PgVec<'mcx, Expr> = if select_stmt.is_none() {
        // INSERT ... DEFAULT VALUES: emit an empty targetlist; all columns are
        // defaulted when the planner expands the targetlist.
        PgVec::new_in(mcx)
    } else if is_general_select {
        transformInsertSelect(
            mcx,
            pstate,
            stmt.selectStmt.as_deref().unwrap(),
            stmt,
            sub_rtable,
            sub_rteperminfos,
            sub_namespace,
            &icolumns,
            &attrnos,
        )?
    } else {
        let s = select_stmt.unwrap();
        if s.valuesLists.len() > 1 {
            // INSERT ... VALUES with multiple sublists: generate a VALUES RTE
            // holding the transformed expression lists, and build a targetlist
            // of Vars referencing it (analyze.c:825).
            transformInsertMultiRowValues(mcx, pstate, s, stmt, &icolumns, &attrnos)?
        } else {
            // INSERT ... VALUES with a single sublist: treat it like a SELECT
            // with no FROM — the sublist becomes the targetlist directly, no
            // VALUES RTE.
            debug_assert!(s.intoClause.is_none());
            let sublist = match s.valuesLists.first().map(|n| n.as_ref()).and_then(|n| n.as_list()) {
                Some(items) => items,
                None => return Err(elog_error("INSERT single VALUES sublist is not a List")),
            };
            let mut sublist_owned = mcx::vec_with_capacity_in(mcx, sublist.len())?;
            for item in sublist.iter() {
                sublist_owned.push(mcx::alloc_in(mcx, item.clone_in(mcx)?)?);
            }
            let raw = backend_parser_parse_target::transformExpressionList(
                mcx,
                pstate,
                sublist_owned,
                ParseExprKind::EXPR_KIND_VALUES_SINGLE,
                true,
            )?;
            // Prepare row for assignment to the target table.
            transformInsertRow(mcx, pstate, raw, &stmt.cols, &icolumns, &attrnos, false)?
        }
    };

    // Generate the query's target list from the computed expression list, and
    // mark all the target columns as needing insert permissions.
    let perminfoindex = pstate
        .p_target_nsitem
        .as_deref()
        .and_then(|ns| ns.p_rte.as_deref())
        .map(|r| r.perminfoindex)
        .unwrap_or(0);

    let mut target_list: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> =
        mcx::vec_with_capacity_in(mcx, expr_list.len())?;
    debug_assert!(expr_list.len() <= icolumns.len());

    for (idx, expr) in expr_list.into_iter().enumerate() {
        let col: &ResTarget<'mcx> = &icolumns[idx];
        let attr_num = attrnos[idx];
        let tle = backend_nodes_core::makefuncs::make_target_entry(
            mcx,
            expr,
            attr_num as i16,
            col.name.as_ref().map(|s| s.as_str()),
            false,
        )?;
        target_list.push(tle);

        if perminfoindex > 0 {
            let pi = &mut pstate.p_rteperminfos[(perminfoindex - 1) as usize];
            let inserted = pi.insertedCols.take();
            pi.insertedCols = Some(backend_nodes_core::bitmapset::bms_add_member(
                mcx,
                inserted,
                attr_num - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            )?);
        }
    }
    qry.targetList = target_list;

    // If we have any clauses yet to process, set the query namespace to contain
    // only the target relation, removing any entries added in a sub-SELECT or
    // VALUES list. (analyze.c:1010)
    if stmt.onConflictClause.is_some() || stmt.returningClause.is_some() {
        pstate.p_namespace = PgVec::new_in(mcx);
        let target = crate::update_delete::clone_target_nsitem(mcx, pstate)?;
        backend_parser_relation::addNSItemToQuery(mcx, pstate, target, false, true, true)?;
    }

    // Process ON CONFLICT, if any. (analyze.c:1019)
    if let Some(occ) = stmt.onConflictClause.as_deref() {
        qry.onConflict = Some(mcx::alloc_in(
            mcx,
            transformOnConflictClause(mcx, pstate, occ)?,
        )?);
    }

    // Process RETURNING, if any.
    crate::update_delete::transformReturningClause(
        mcx,
        pstate,
        &mut qry,
        stmt.returningClause.as_deref(),
        ParseExprKind::EXPR_KIND_RETURNING,
    )?;

    // Done building the range table and jointree.
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, PgVec::new_in(mcx));
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        types_nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: None,
        },
    )?);

    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    crate::sync_cte_refcounts(pstate, &mut qry.cteList);
    Ok(qry)
}

/// The general-SELECT source branch of `transformInsertStmt` (analyze.c:731).
/// Transforms `INSERT INTO t SELECT ...` (and `INSERT ... SELECT <const>`): the
/// source SELECT is transformed in a sub-pstate, wrapped as a `*SELECT*`
/// subquery RTE in the INSERT's range table, and an expression list of Vars
/// (or copied-up unknown literals) is built selecting the non-resjunk subquery
/// columns.
#[allow(clippy::too_many_arguments)]
fn transformInsertSelect<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    select_stmt_node: &Node<'mcx>,
    stmt: &InsertStmt<'mcx>,
    sub_rtable: PgVec<'mcx, types_nodes::parsenodes::RangeTblEntry<'mcx>>,
    sub_rteperminfos: PgVec<'mcx, types_nodes::parsenodes::RTEPermissionInfo<'mcx>>,
    sub_namespace: PgVec<'mcx, types_nodes::parsestmt::ParseNamespaceItem<'mcx>>,
    icolumns: &PgVec<'mcx, ResTarget<'mcx>>,
    attrnos: &PgVec<'mcx, i32>,
) -> PgResult<PgVec<'mcx, Expr>> {
    // We make the sub-pstate a child of the outer pstate so that it can see any
    // Param definitions supplied from above. Since the outer pstate's rtable and
    // namespace are presently empty, there are no side-effects of exposing names
    // the sub-SELECT shouldn't be able to see.
    let mut sub_pstate = backend_parser_small1::make_parsestate(mcx, Some(pstate))?;
    sub_pstate.p_rtable = sub_rtable;
    sub_pstate.p_rteperminfos = sub_rteperminfos;
    sub_pstate.p_joinexprs = PgVec::new_in(mcx); // sub_rtable has no joins
    sub_pstate.p_nullingrels = PgVec::new_in(mcx);
    sub_pstate.p_namespace = sub_namespace;
    // Prevent resolving unknown-type outputs as TEXT; the target column type is
    // applied below (analyze.c).
    sub_pstate.p_resolve_unknowns = false;

    let select_query = crate::transformStmt(mcx, &mut sub_pstate, select_stmt_node)?;

    // The owned model holds `parentParseState` by value (a deep copy of the
    // INSERT pstate's spine), so a SELECT-privilege mark or `cterefcount++` that
    // the sub-SELECT applies to an outer-level RTE / CTE (walking up
    // `parentParseState`) lands on the *clone*, not the live `pstate`. C bumps
    // the single shared node directly. Merge the clone's marks/refcounts back
    // into the live `pstate` before the child state is freed — the same step
    // `parse_sub_analyze` performs for a general subquery. Without the CTE merge,
    // a WITH attached to `INSERT ... SELECT` whose only reference is in the
    // SELECT keeps `cterefcount == 0`, and the planner drops the CTE plan ("no
    // plan was made for CTE").
    if let Some(cloned_parent) = sub_pstate.parentParseState.as_deref() {
        crate::merge_perminfo_marks(mcx, pstate, cloned_parent)?;
        crate::merge_cte_refcounts(pstate, cloned_parent);
    }

    backend_parser_small1::free_parsestate(sub_pstate)?;

    // The grammar should have produced a SELECT.
    if select_query.commandType != CmdType::CMD_SELECT {
        return Err(elog_error(
            "unexpected non-SELECT command in INSERT ... SELECT",
        ));
    }

    // Make the source be a subquery in the INSERT's rangetable, and add it to
    // the INSERT's joinlist (but not the namespace).
    let alias = backend_nodes_core::makefuncs::make_alias(mcx, "*SELECT*", PgVec::new_in(mcx))?;
    let nsitem = backend_parser_relation::addRangeTableEntryForSubquery(
        mcx,
        pstate,
        select_query,
        Some(alias),
        false,
        false,
    )?;
    let rtindex = nsitem.p_rtindex;
    // Read the subquery's targetlist (stored in the just-added RTE) before
    // addNSItemToQuery consumes the nsitem.
    let sub_target_list: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = {
        let rte = &pstate.p_rtable[(rtindex - 1) as usize];
        let subq = rte
            .subquery
            .as_deref()
            .ok_or_else(|| elog_error("INSERT ... SELECT subquery RTE has no subquery"))?;
        let mut tl = mcx::vec_with_capacity_in(mcx, subq.targetList.len())?;
        for te in subq.targetList.iter() {
            tl.push(te.clone_in(mcx)?);
        }
        tl
    };

    backend_parser_relation::addNSItemToQuery(mcx, pstate, nsitem, true, false, false)?;

    // Generate an expression list for the INSERT that selects all the
    // non-resjunk columns from the subquery.
    //
    // HACK: unknown-type constants and params in the SELECT's targetlist are
    // copied up as-is rather than referenced as subquery outputs, so they can
    // be coerced to the target column type (see coerce_type special cases).
    let mut expr_list: PgVec<'mcx, Expr> = PgVec::new_in(mcx);
    for tle in sub_target_list.iter() {
        if tle.resjunk {
            continue;
        }
        let copy_up = matches!(
            tle.expr.as_deref(),
            Some(Expr::Const(_)) | Some(Expr::Param(_))
        ) && backend_nodes_core::nodefuncs::expr_type(tle.expr.as_deref())? == UNKNOWNOID;

        let expr = if copy_up {
            tle.expr
                .as_deref()
                .ok_or_else(|| elog_error("INSERT ... SELECT TLE has no expr"))?
                .clone_in(mcx)?
        } else {
            let mut var =
                backend_nodes_core::makefuncs::make_var_from_target_entry(rtindex, tle)?;
            var.location = backend_nodes_core::nodefuncs::expr_location(tle.expr.as_deref())?;
            Expr::Var(var)
        };
        expr_list.push(expr);
    }

    // Prepare row for assignment to target table.
    transformInsertRow(mcx, pstate, expr_list, &stmt.cols, icolumns, attrnos, false)
}

/// The multi-row `INSERT ... VALUES (...), (...), ...` branch of
/// `transformInsertStmt` (analyze.c:825). Transforms each row, builds a VALUES
/// RTE carrying the coerced expression lists, adds it to the query, and returns
/// the targetlist expression list (Vars referencing the RTE).
fn transformInsertMultiRowValues<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    select_stmt: &SelectStmt<'mcx>,
    stmt: &InsertStmt<'mcx>,
    icolumns: &PgVec<'mcx, ResTarget<'mcx>>,
    attrnos: &PgVec<'mcx, i32>,
) -> PgResult<PgVec<'mcx, Expr>> {
    debug_assert!(select_stmt.intoClause.is_none());

    // exprsLists: a List (PgVec<NodePtr>) where each element is a Node::List of
    // Node::Expr — one row of coerced column expressions.
    let mut exprs_lists: PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, select_stmt.valuesLists.len())?;
    let mut sublist_length: i32 = -1;

    for row_node in select_stmt.valuesLists.iter() {
        let sublist = match row_node.as_ref().as_list() {
            Some(items) => items,
            None => return Err(elog_error("INSERT VALUES sublist is not a List")),
        };
        let mut sublist_owned = mcx::vec_with_capacity_in(mcx, sublist.len())?;
        for item in sublist.iter() {
            sublist_owned.push(mcx::alloc_in(mcx, item.clone_in(mcx)?)?);
        }

        // Basic expression transformation (same as a ROW() expr, but allow
        // SetToDefault at top level).
        let transformed = backend_parser_parse_target::transformExpressionList(
            mcx,
            pstate,
            sublist_owned,
            ParseExprKind::EXPR_KIND_VALUES,
            true,
        )?;

        // All sublists must be the same length after transformation.
        if sublist_length < 0 {
            sublist_length = transformed.len() as i32;
        } else if sublist_length != transformed.len() as i32 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("VALUES lists must all be the same length".to_string())
                .into_error());
        }

        // Prepare row for assignment to target table, stripping the resulting
        // field/array assignment nodes (strip_indirection = true).
        let mut row = transformInsertRow(
            mcx,
            pstate,
            transformed,
            &stmt.cols,
            icolumns,
            attrnos,
            true,
        )?;

        // Assign collations now (assign_query_collations doesn't process the
        // rangetable). Each row independently.
        backend_parser_parse_collate::assign_list_collations(Some(pstate), &mut row[..])?;

        // Wrap the row as a Node::List of Node::Expr for the VALUES RTE.
        let mut row_nodes: PgVec<'mcx, NodePtr<'mcx>> =
            mcx::vec_with_capacity_in(mcx, row.len())?;
        for expr in row.into_iter() {
            row_nodes.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, expr)?)?);
        }
        exprs_lists.push(mcx::alloc_in(mcx, Node::mk_list(mcx, row_nodes)?)?);
    }

    // Construct column type/typmod/collation lists from the first row.
    let mut coltypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut coltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut colcollations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    if let Some(first) = exprs_lists.first() {
        if let Some(items) = first.as_ref().as_list() {
            for val in items.iter() {
                let expr = val.as_ref().as_expr();
                coltypes.push(backend_nodes_core::nodefuncs::expr_type(expr)?);
                coltypmods.push(backend_nodes_core::nodefuncs::expr_typmod(expr)?);
                colcollations.push(InvalidOid);
            }
        }
    }

    // LATERAL only if there are current-level Vars (CREATE RULE NEW/OLD). The
    // namespace is otherwise empty.
    let lateral = if pstate.p_rtable.len() != 1 {
        let probe = Node::mk_list(mcx, {
            let mut v: PgVec<'mcx, NodePtr<'mcx>> =
                mcx::vec_with_capacity_in(mcx, exprs_lists.len())?;
            for e in exprs_lists.iter() {
                v.push(mcx::alloc_in(mcx, e.clone_in(mcx)?)?);
            }
            v
        })?;
        backend_optimizer_util_vars::contain_vars_of_level(&probe, 0)
    } else {
        false
    };

    // Generate the VALUES RTE.
    let nsitem = backend_parser_relation::addRangeTableEntryForValues(
        mcx,
        pstate,
        exprs_lists,
        coltypes,
        coltypmods,
        colcollations,
        None,
        lateral,
        true,
    )?;
    // Generate the list of Vars referencing the RTE. expandNSItemVars reads
    // only the nsitem's p_names/p_nscolumns (set at build time), so it is safe
    // to read before addNSItemToQuery (which only flips visibility flags and
    // pushes a RangeTblRef to the joinlist).
    let var_nodes = backend_parser_relation::expandNSItemVars(mcx, pstate, &nsitem, 0, -1, None)?;

    backend_parser_relation::addNSItemToQuery(mcx, pstate, nsitem, true, false, false)?;

    let mut var_exprs: PgVec<'mcx, Expr> = mcx::vec_with_capacity_in(mcx, var_nodes.len())?;
    for vn in var_nodes.into_iter() {
        match mcx::PgBox::into_inner(vn).into_expr() {
            Some(e) => var_exprs.push(e),
            None => return Err(elog_error("expandNSItemVars produced a non-Expr node")),
        }
    }

    // Re-apply any indirection on the target column specs to the Vars.
    transformInsertRow(mcx, pstate, var_exprs, &stmt.cols, icolumns, attrnos, false)
}

/// `transformInsertRow(pstate, exprlist, stmtcols, icolumns, attrnos,
/// strip_indirection)` (analyze.c:1043) — prepare an expression list for
/// assignment to the target table (length checks + `transformAssignedExpr`).
///
/// `strip_indirection` (used only by the multi-row VALUES branch) is not
/// reachable on the single-VALUES milestone path; it is honored as a no-op here
/// because that branch errors out before calling with `true`.
pub fn transformInsertRow<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    exprlist: PgVec<'mcx, Expr>,
    stmtcols: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    icolumns: &PgVec<'mcx, ResTarget<'mcx>>,
    attrnos: &PgVec<'mcx, i32>,
    strip_indirection: bool,
) -> PgResult<PgVec<'mcx, Expr>> {
    // Check length of the expr list: it must not have more expressions than
    // there are target columns. Fewer is allowed only if no explicit column
    // list was given (the remaining columns are implicitly defaulted).
    if exprlist.len() > icolumns.len() {
        // C: parser_errposition(pstate, exprLocation(list_nth(exprlist, list_length(icolumns))))
        let loc = backend_nodes_core::nodefuncs::expr_location(Some(&exprlist[icolumns.len()]))?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg_internal("INSERT has more expressions than target columns".to_string())
            .errposition(backend_parser_small1::parser_errposition(pstate, loc))
            .into_error());
    }
    if !stmtcols.is_empty() && exprlist.len() < icolumns.len() {
        // We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
        // where the user accidentally created a RowExpr instead of separate
        // columns. Add a suitable hint if that seems to be the problem.
        // C: parser_errposition(pstate, exprLocation(list_nth(icolumns,
        //     list_length(exprlist)))) — icolumns elements are ResTarget, whose
        // exprLocation is its `location` field.
        let loc = icolumns[exprlist.len()].location;
        let mut report = ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg_internal("INSERT has more target columns than expressions".to_string());
        if exprlist.len() == 1
            && count_rowexpr_columns(pstate, &exprlist[0]) == icolumns.len() as i32
        {
            report = report.errhint(
                "The insertion source is a row expression containing the same number of columns expected by the INSERT. Did you accidentally use extra parentheses?",
            );
        }
        return Err(report
            .errposition(backend_parser_small1::parser_errposition(pstate, loc))
            .into_error());
    }

    // Prepare columns for assignment to the target table.
    let mut result: PgVec<'mcx, Expr> = mcx::vec_with_capacity_in(mcx, exprlist.len())?;
    for (idx, expr) in exprlist.into_iter().enumerate() {
        let col: &ResTarget<'mcx> = &icolumns[idx];
        let attno = attrnos[idx];
        let mut transformed = backend_parser_parse_target::transformAssignedExpr(
            mcx,
            pstate,
            Some(expr),
            ParseExprKind::EXPR_KIND_INSERT_TARGET,
            col.name.as_ref().map(|s| s.as_str()).unwrap_or(""),
            attno,
            &col.indirection,
            col.location,
        )?;

        if strip_indirection {
            // Remove top-level FieldStores and SubscriptingRefs, as well as any
            // CoerceToDomain appearing above one of those (analyze.c:1117).
            loop {
                let mut subexpr = &transformed;
                while let Expr::CoerceToDomain(c) = subexpr {
                    match c.arg.as_deref() {
                        Some(a) => subexpr = a,
                        None => break,
                    }
                }
                match subexpr {
                    Expr::FieldStore(f) => {
                        let next = f
                            .newvals
                            .first()
                            .cloned()
                            .ok_or_else(|| elog_error("FieldStore has no newvals"))?;
                        transformed = next;
                    }
                    Expr::SubscriptingRef(s) => match s.refassgnexpr.as_deref() {
                        Some(a) => transformed = a.clone(),
                        None => break,
                    },
                    _ => break,
                }
            }
        }

        result.push(transformed);
    }
    Ok(result)
}

/// `count_rowexpr_columns(pstate, expr)` (analyze.c) — try to report the number
/// of columns contained in a RowExpr, so that the "more target columns than
/// expressions" error can offer the extra-parentheses hint. Returns -1 if we
/// can't determine the number of columns.
fn count_rowexpr_columns<'mcx>(pstate: &ParseState<'mcx>, expr: &Expr) -> i32 {
    match expr {
        Expr::RowExpr(r) => r.args.len() as i32,
        Expr::Var(var) => {
            let attnum = var.varattno;
            if attnum > 0 && var.vartype == RECORDOID {
                let rte = backend_parser_relation::GetRTEByRangeTablePosn(
                    pstate,
                    var.varno,
                    var.varlevelsup as i32,
                );
                if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY {
                    // Subselect-in-FROM: examine sub-select's output expr.
                    if let Some(subquery) = rte.subquery.as_deref() {
                        if let Some(ste) = backend_parser_relation::get_tle_by_resno(
                            &subquery.targetList,
                            attnum,
                        ) {
                            if !ste.resjunk {
                                if let Some(sexpr) = ste.expr.as_deref() {
                                    if let Expr::RowExpr(r) = sexpr {
                                        return r.args.len() as i32;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            -1
        }
        _ => -1,
    }
}

/// Deep-copy the raw `cols` list (a `List` of `ResTarget` nodes) into `mcx` as a
/// `PgVec<ResTarget>` for `checkInsertTargets`.
fn copy_cols<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, ResTarget<'mcx>>> {
    let mut out: PgVec<'mcx, ResTarget<'mcx>> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    for c in cols.iter() {
        match c.as_ref().as_restarget() {
            Some(rt) => out.push(rt.clone_in(mcx)?),
            None => return Err(elog_error("INSERT cols entry is not a ResTarget")),
        }
    }
    Ok(out)
}

/// `transformOnConflictClause(pstate, onConflictClause)` (analyze.c:1162) —
/// transform an `OnConflictClause` into an `OnConflictExpr`.
fn transformOnConflictClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    on_conflict_clause: &types_nodes::rawnodes::OnConflictClause<'mcx>,
) -> PgResult<types_nodes::rawnodes::OnConflictExpr<'mcx>> {
    use types_nodes::nodes::OnConflictAction;

    let mut excl_rel_index: i32 = 0;
    let mut excl_rel_tlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let mut on_conflict_set: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let mut on_conflict_where: Option<NodePtr<'mcx>> = None;
    // The EXCLUDED nsitem, created during the UPDATE prelude but only added to
    // the namespace (and later popped) once the arbiter clause is processed.
    let mut stashed_excl_nsitem: Option<types_nodes::parsestmt::ParseNamespaceItem<'mcx>> = None;

    // If this is ON CONFLICT ... UPDATE, first create the range table entry for
    // the EXCLUDED pseudo relation, so that that will be present while
    // processing arbiter expressions. (analyze.c:1177)
    if on_conflict_clause.action == OnConflictAction::ONCONFLICT_UPDATE {
        let alias = backend_nodes_core::makefuncs::make_alias(mcx, "excluded", PgVec::new_in(mcx))?;

        // Clone the cheap Rc-backed relcache handle so the borrow checker lets
        // us mutate pstate inside addRangeTableEntryForRelation.
        let targetrel = pstate
            .p_target_relation
            .as_ref()
            .ok_or_else(|| elog_error("transformOnConflictClause: no target relation"))?
            .alias();

        let mut excl_nsitem = backend_parser_relation::addRangeTableEntryForRelation(
            mcx,
            pstate,
            &targetrel,
            types_storage::lock::RowExclusiveLock,
            Some(alias),
            false,
            false,
        )?;
        excl_rel_index = excl_nsitem.p_rtindex;

        // relkind is set to composite to signal that we're not dealing with an
        // actual relation, and no permission checks are required on it. (We'll
        // check the actual target relation, instead.) (analyze.c:1192)
        let composite = types_tuple::access::RELKIND_COMPOSITE_TYPE as i8;
        pstate.p_rtable[(excl_rel_index - 1) as usize].relkind = composite;
        if let Some(rte) = excl_nsitem.p_rte.as_deref_mut() {
            rte.relkind = composite;
        }

        // Create EXCLUDED rel's targetlist for use by EXPLAIN. (analyze.c:1195)
        excl_rel_tlist = build_on_conflict_excluded_targetlist(mcx, &targetrel, excl_rel_index)?;

        // Hold the EXCLUDED nsitem; it is added to the namespace only in the DO
        // UPDATE block below, AFTER the arbiter clause is processed (analyze.c).
        stashed_excl_nsitem = Some(excl_nsitem);
    }

    // Process the arbiter clause, ON CONFLICT ON (...). (analyze.c:1202)
    let (arbiter_exprs, arbiter_where_expr, arbiter_constraint) =
        backend_parser_clause::transformOnConflictArbiter(
            mcx,
            pstate,
            on_conflict_clause,
        )?;

    // Convert the arbiter element Exprs into the node-pointer list the
    // OnConflictExpr carries.
    let mut arbiter_elems: PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, arbiter_exprs.len())?;
    for e in arbiter_exprs.into_iter() {
        arbiter_elems.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?);
    }
    let arbiter_where: Option<NodePtr<'mcx>> = match arbiter_where_expr {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };

    // Process DO UPDATE. (analyze.c:1205)
    if on_conflict_clause.action == OnConflictAction::ONCONFLICT_UPDATE {
        // Expressions in the UPDATE targetlist need to be handled like UPDATE
        // not INSERT. We don't need to save/restore this because all INSERT
        // expressions have been parsed already. (analyze.c:1212)
        pstate.p_is_insert = false;

        // Add the EXCLUDED pseudo relation to the query namespace, making it
        // available in the UPDATE subexpressions. (analyze.c:1216)
        let excl_nsitem = stashed_excl_nsitem
            .take()
            .ok_or_else(|| elog_error("transformOnConflictClause: missing EXCLUDED nsitem"))?;
        backend_parser_relation::addNSItemToQuery(mcx, pstate, excl_nsitem, false, true, true)?;

        // Now transform the UPDATE subexpressions. (analyze.c:1221)
        let set_tles = crate::update_delete::transformUpdateTargetList(
            mcx,
            pstate,
            &on_conflict_clause.targetList,
        )?;
        on_conflict_set = mcx::vec_with_capacity_in(mcx, set_tles.len())?;
        for tle in set_tles.into_iter() {
            on_conflict_set.push(mcx::alloc_in(
                mcx,
                Node::mk_target_entry(mcx, tle)?,
            )?);
        }

        // WHERE clause. (analyze.c:1224)
        let where_clause: Option<Node<'mcx>> = match &on_conflict_clause.whereClause {
            Some(n) => Some(n.as_ref().clone_in(mcx)?),
            None => None,
        };
        let where_expr = backend_parser_clause::transformWhereClause(
            mcx,
            pstate,
            where_clause,
            ParseExprKind::EXPR_KIND_WHERE,
            "WHERE",
        )?;
        on_conflict_where = match where_expr {
            Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
            None => None,
        };

        // Remove the EXCLUDED pseudo relation from the query namespace, since
        // it's not supposed to be available in RETURNING. (analyze.c:1231)
        debug_assert_eq!(
            pstate.p_namespace.last().map(|n| n.p_rtindex),
            Some(excl_rel_index)
        );
        pstate.p_namespace.pop();
    }

    Ok(types_nodes::rawnodes::OnConflictExpr {
        action: on_conflict_clause.action,
        arbiterElems: arbiter_elems,
        arbiterWhere: arbiter_where,
        constraint: arbiter_constraint,
        onConflictSet: on_conflict_set,
        onConflictWhere: on_conflict_where,
        exclRelIndex: excl_rel_index,
        exclRelTlist: excl_rel_tlist,
    })
}

/// `BuildOnConflictExcludedTargetlist(targetrel, exclRelIndex)` (analyze.c:1265)
/// — build the EXCLUDED pseudo-relation targetlist, one `TargetEntry` per
/// attribute (including dropped columns), plus a trailing whole-row Var.
pub fn build_on_conflict_excluded_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    targetrel: &types_rel::RelationData<'mcx>,
    excl_rel_index: i32,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let natts = targetrel.rd_att.attrs.len();
    let mut result: PgVec<'mcx, NodePtr<'mcx>> = mcx::vec_with_capacity_in(mcx, natts + 1)?;

    // Note that resnos of the tlist must correspond to attnos of the underlying
    // relation, hence we need entries for dropped columns too.
    for attno in 0..natts {
        let attr = targetrel.rd_att.attr(attno);
        let attname = core::str::from_utf8(attr.attname.name_str())
            .map_err(|_| elog_error("EXCLUDED tlist: attname is not valid UTF-8"))?;
        let (var, name): (Expr, Option<&str>) = if attr.attisdropped {
            // can't use atttypid here, but it doesn't really matter what type
            // the Const claims to be. (analyze.c:1289)
            let c = backend_nodes_core::makefuncs::make_null_const(
                mcx,
                types_tuple::heaptuple::INT4OID,
                -1,
                InvalidOid,
            )?;
            (Expr::Const(c), None)
        } else {
            let v = backend_nodes_core::makefuncs::make_var(
                excl_rel_index,
                (attno + 1) as i16,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                0,
            );
            (Expr::Var(v), Some(attname))
        };

        let te = backend_nodes_core::makefuncs::make_target_entry(
            mcx,
            var,
            (attno + 1) as i16,
            name,
            false,
        )?;
        result.push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, te)?)?);
    }

    // Add a whole-row-Var entry to support references to "EXCLUDED.*". Its resno
    // must match the Var's varattno (InvalidAttrNumber). (analyze.c:1316)
    let whole_row = backend_nodes_core::makefuncs::make_var(
        excl_rel_index,
        types_core::primitive::InvalidAttrNumber,
        targetrel.rd_rel.reltype,
        -1,
        InvalidOid,
        0,
    );
    let te = backend_nodes_core::makefuncs::make_target_entry(
        mcx,
        Expr::Var(whole_row),
        types_core::primitive::InvalidAttrNumber,
        None,
        true,
    )?;
    result.push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, te)?)?);

    Ok(result)
}
