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
//! The multi-row VALUES branch, INSERT/SELECT (general select), ON CONFLICT, and
//! RETURNING are follow-on work and panic loudly until their substrate lands.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::Expr;
use types_nodes::rawnodes::{InsertStmt, ResTarget, SelectStmt};

use crate::elog_error;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h:27) — the offset used
/// to fold a real `AttrNumber` into the `RTEPermissionInfo` column bitmapset.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

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
        Some(Node::SelectStmt(s)) => Some(s),
        Some(_) => return Err(elog_error("INSERT selectStmt is not a SelectStmt")),
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
        return Err(elog_error(
            "INSERT ... SELECT (general select source) is not yet ported (analyze.c:731)",
        ));
    } else {
        let s = select_stmt.unwrap();
        if s.valuesLists.len() > 1 {
            // The multi-row VALUES branch builds a VALUES RTE, which is blocked
            // on the List-carrier RTE keystone (addRangeTableEntryForValues).
            return Err(elog_error(
                "INSERT ... VALUES with multiple rows builds a VALUES RTE, which is \
                 blocked on the List-carrier RTE keystone (analyze.c:830, \
                 addRangeTableEntryForValues)",
            ));
        }

        // INSERT ... VALUES with a single sublist: treat it like a SELECT with
        // no FROM — the sublist becomes the targetlist directly, no VALUES RTE.
        debug_assert!(s.intoClause.is_none());
        let sublist = match s.valuesLists.first().map(|n| n.as_ref()) {
            Some(Node::List(items)) => items,
            _ => return Err(elog_error("INSERT single VALUES sublist is not a List")),
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

    // ON CONFLICT / RETURNING are follow-on work.
    if stmt.onConflictClause.is_some() {
        return Err(elog_error(
            "INSERT ... ON CONFLICT is not yet ported (analyze.c:1010)",
        ));
    }
    if stmt.returningClause.is_some() {
        return Err(elog_error(
            "INSERT ... RETURNING is not yet ported (analyze.c:1015)",
        ));
    }

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

    Ok(qry)
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
    _strip_indirection: bool,
) -> PgResult<PgVec<'mcx, Expr>> {
    // Check length of the expr list: it must not have more expressions than
    // there are target columns. Fewer is allowed only if no explicit column
    // list was given (the remaining columns are implicitly defaulted).
    if exprlist.len() > icolumns.len() {
        return Err(elog_error("INSERT has more expressions than target columns"));
    }
    if !stmtcols.is_empty() && exprlist.len() < icolumns.len() {
        return Err(elog_error("INSERT has more target columns than expressions"));
    }

    // Prepare columns for assignment to the target table.
    let mut result: PgVec<'mcx, Expr> = mcx::vec_with_capacity_in(mcx, exprlist.len())?;
    for (idx, expr) in exprlist.into_iter().enumerate() {
        let col: &ResTarget<'mcx> = &icolumns[idx];
        let attno = attrnos[idx];
        let transformed = backend_parser_parse_target::transformAssignedExpr(
            mcx,
            pstate,
            Some(expr),
            ParseExprKind::EXPR_KIND_INSERT_TARGET,
            col.name.as_ref().map(|s| s.as_str()).unwrap_or(""),
            attno,
            &col.indirection,
            col.location,
        )?;
        // `strip_indirection` (multi-row VALUES) is unreachable on this path.
        result.push(transformed);
    }
    Ok(result)
}

/// Deep-copy the raw `cols` list (a `List` of `ResTarget` nodes) into `mcx` as a
/// `PgVec<ResTarget>` for `checkInsertTargets`.
fn copy_cols<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, ResTarget<'mcx>>> {
    let mut out: PgVec<'mcx, ResTarget<'mcx>> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    for c in cols.iter() {
        match c.as_ref() {
            Node::ResTarget(rt) => out.push(rt.clone_in(mcx)?),
            _ => return Err(elog_error("INSERT cols entry is not a ResTarget")),
        }
    }
    Ok(out)
}
