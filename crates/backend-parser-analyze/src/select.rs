//! The SELECT path of `parser/analyze.c`: `transformSelectStmt` and
//! `transformValuesClause`.

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::rawnodes::{SelectStmt, SortBy};

use crate::{
    cte_vec_to_nodes, elog_error, node_vec_to_pgvec, opt_expr_to_box, opt_expr_to_node,
    sgc_vec_to_nodes,
};

/// Downcast a `List *` of raw `Node`s (`stmt->sortClause`) to the
/// `Vec<SortBy>` the clause owner's `transformSortClause` consumes. The grammar
/// guarantees every element is a `SortBy`.
pub(crate) fn sortby_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &[NodePtr<'mcx>],
) -> PgResult<Vec<SortBy<'mcx>>> {
    let mut out = Vec::new();
    out.try_reserve(list.len()).map_err(|_| mcx.oom(list.len()))?;
    for n in list {
        match n.as_ref().as_sortby() {
            Some(s) => out.push(s.clone_in(mcx)?),
            None => return Err(elog_error("transformSortClause: ORDER BY item is not a SortBy")),
        }
    }
    Ok(out)
}

/// `transformSelectStmt(pstate, stmt)` — transform a single (non-set-op,
/// non-VALUES) SELECT into a `Query`.
pub fn transformSelectStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_SELECT;

    /* process the WITH clause independently of all else */
    if let Some(with) = stmt.withClause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    /* Complain if we get called from someplace where INTO is not allowed */
    if stmt.intoClause.is_some() {
        return Err(elog_error("SELECT ... INTO is not allowed here"));
    }

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    pstate.p_locking_clause = {
        let mut v = mcx::vec_with_capacity_in(mcx, stmt.lockingClause.len())?;
        for n in stmt.lockingClause.iter() {
            v.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
        v
    };

    /* make WINDOW info available for window functions, too */
    pstate.p_windowdefs = {
        let mut v = mcx::vec_with_capacity_in(mcx, stmt.windowClause.len())?;
        for n in stmt.windowClause.iter() {
            match n.as_ref().as_windowdef() {
                Some(w) => v.push(w.clone_in(mcx)?),
                None => return Err(elog_error("WINDOW clause item is not a WindowDef")),
            }
        }
        v
    };

    /* process the FROM clause */
    backend_parser_clause::transformFromClause(mcx, pstate, &stmt.fromClause)?;

    /* transform targetlist */
    let target_list = {
        let mut tl = mcx::vec_with_capacity_in(mcx, stmt.targetList.len())?;
        for n in stmt.targetList.iter() {
            match n.as_ref().as_restarget() {
                Some(rt) => tl.push(rt.clone_in(mcx)?),
                None => return Err(elog_error("SELECT target list item is not a ResTarget")),
            }
        }
        tl
    };
    // The clause owners thread the target list as a `&mut Vec<TargetEntry>`, so
    // carry it as a std `Vec` from here and re-home into the Query's `PgVec`
    // once the clause passes are done.
    let mut tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>> =
        backend_parser_parse_target::transformTargetList(
            mcx,
            pstate,
            target_list,
            ParseExprKind::EXPR_KIND_SELECT_TARGET,
        )?
        .into_iter()
        .collect();

    /* mark column origins */
    backend_parser_parse_target::markTargetListOrigins(mcx, pstate, &mut tlist)?;

    /* transform WHERE */
    let qual = backend_parser_clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.whereClause)?,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    /* initial processing of HAVING clause is much like WHERE clause */
    let having = backend_parser_clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.havingClause)?,
        ParseExprKind::EXPR_KIND_HAVING,
        "HAVING",
    )?;
    qry.havingQual = opt_expr_to_box(mcx, having)?;

    /*
     * Transform sorting/grouping stuff. Do ORDER BY first because both
     * transformGroupClause and transformDistinctClause need the results.
     * These can also change the targetList, so it's passed by reference.
     */
    let sort_input = sortby_list(mcx, &stmt.sortClause)?;
    let sort_clause = backend_parser_clause::transformSortClause(
        mcx,
        pstate,
        &sort_input,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    let (group_clause, grouping_sets) = backend_parser_clause::transformGroupClause(
        mcx,
        pstate,
        &stmt.groupClause,
        &mut tlist,
        &sort_clause,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )?;
    qry.groupDistinct = stmt.groupDistinct;

    // C three-way branch on stmt->distinctClause:
    //   NIL                -> no DISTINCT
    //   linitial == NULL   -> SELECT DISTINCT (all columns)
    //   otherwise          -> SELECT DISTINCT ON (exprs)
    // The plain-DISTINCT case is the grammar's `list_make1(NIL)` — a one-element
    // list whose single element is a NULL pointer. The raw->owned converter
    // does not yet carry a NULL list cell (it requires every cell), so that
    // marker reaches analyze as an empty list cell only once the converter
    // NULL-cell follow-on lands; we detect it via `distinct_all_marker`.
    let distinct_clause;
    if stmt.distinctClause.is_empty() {
        distinct_clause = Vec::new();
        qry.hasDistinctOn = false;
    } else if distinct_all_marker(&stmt.distinctClause) {
        /* SELECT DISTINCT */
        distinct_clause = backend_parser_clause::transformDistinctClause(
            mcx,
            pstate,
            &mut tlist,
            &sort_clause,
            false,
        )?;
        qry.hasDistinctOn = false;
    } else {
        /* SELECT DISTINCT ON */
        distinct_clause = backend_parser_clause::transformDistinctOnClause(
            mcx,
            pstate,
            &stmt.distinctClause,
            &mut tlist,
            &sort_clause,
        )?;
        qry.hasDistinctOn = true;
    }

    /* transform LIMIT */
    let limit_offset = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.limitOffset)?,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    let limit_count = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.limitCount)?,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOffset = opt_expr_to_box(mcx, limit_offset)?;
    qry.limitCount = opt_expr_to_box(mcx, limit_count)?;
    qry.limitOption = stmt.limitOption;

    /* transform window clauses after we have seen all window functions */
    let windowdefs = {
        let mut v = mcx::vec_with_capacity_in(mcx, pstate.p_windowdefs.len())?;
        for w in pstate.p_windowdefs.iter() {
            v.push(w.clone_in(mcx)?);
        }
        v
    };
    let window_clause = backend_parser_clause::transformWindowDefinitions(
        mcx,
        pstate,
        &windowdefs,
        &mut tlist,
    )?;
    qry.windowClause = {
        let mut v = mcx::vec_with_capacity_in(mcx, window_clause.len())?;
        for wc in window_clause {
            v.push(mcx::alloc_in(mcx, Node::mk_window_clause(mcx, wc)?)?);
        }
        v
    };

    /* resolve any still-unresolved output columns as being type text */
    if pstate.p_resolve_unknowns {
        backend_parser_parse_target::resolveTargetListUnknowns(mcx, pstate, &mut tlist)?;
    }

    /* Put the (possibly clause-modified) target list back into the Query. */
    qry.targetList = {
        let mut v = mcx::vec_with_capacity_in(mcx, tlist.len())?;
        for te in tlist {
            v.push(te);
        }
        v
    };
    qry.sortClause = sgc_vec_to_nodes(mcx, sort_clause)?;
    qry.groupClause = sgc_vec_to_nodes(mcx, group_clause)?;
    qry.groupingSets = node_vec_to_pgvec(mcx, grouping_sets)?;
    qry.distinctClause = sgc_vec_to_nodes(mcx, distinct_clause)?;

    /* move the range table and join tree out of the ParseState into the Query */
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, mcx::PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, mcx::PgVec::new_in(mcx));
    let qual_node = opt_expr_to_node(mcx, qual)?;
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        types_nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: qual_node,
        },
    )?);

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    /* FOR UPDATE/SHARE */
    let locking = core::mem::replace(&mut pstate.p_locking_clause, mcx::PgVec::new_in(mcx));
    for lc_node in locking.iter() {
        match lc_node.as_ref().as_lockingclause() {
            Some(lc) => {
                crate::locking::transformLockingClause(mcx, pstate, &mut qry, lc, false)?;
            }
            None => return Err(elog_error("locking clause item is not a LockingClause")),
        }
    }

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    /* must be done after collations, for reliable comparison of exprs */
    if pstate.p_hasAggs
        || !qry.groupClause.is_empty()
        || !qry.groupingSets.is_empty()
        || qry.havingQual.is_some()
    {
        backend_parser_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    crate::sync_cte_refcounts(pstate, &mut qry.cteList);
    Ok(qry)
}

/// `transformReturnStmt(pstate, stmt)` (analyze.c) — transform a `RETURN expr`
/// statement (the body of a new-style SQL function defined with `RETURN`) into a
/// `CMD_SELECT` `Query` with `isReturn = true` and a single-column target list
/// holding the transformed return expression.
pub fn transformReturnStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &types_nodes::ddlnodes::ReturnStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_SELECT;
    qry.isReturn = true;

    /*
     * qry->targetList = list_make1(makeTargetEntry(
     *     transformExpr(pstate, stmt->returnval, EXPR_KIND_SELECT_TARGET),
     *     1, NULL, false));
     */
    let returnval = match stmt.returnval.as_deref() {
        Some(n) => Some(n.clone_in(mcx)?),
        None => None,
    };
    let expr = backend_parser_parse_expr::transformExpr(
        pstate,
        returnval,
        ParseExprKind::EXPR_KIND_SELECT_TARGET,
    )?;
    let expr = expr.ok_or_else(|| elog_error("RETURN has no return value"))?;
    let tle = backend_nodes_core::makefuncs::make_target_entry(mcx, expr, 1, None, false)?;
    qry.targetList = {
        let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
        v.push(tle);
        v
    };

    /* if (pstate->p_resolve_unknowns) resolveTargetListUnknowns(...) */
    if pstate.p_resolve_unknowns {
        let mut tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>> =
            core::mem::replace(&mut qry.targetList, mcx::PgVec::new_in(mcx))
                .into_iter()
                .collect();
        backend_parser_parse_target::resolveTargetListUnknowns(mcx, pstate, &mut tlist)?;
        qry.targetList = {
            let mut v = mcx::vec_with_capacity_in(mcx, tlist.len())?;
            for te in tlist {
                v.push(te);
            }
            v
        };
    }

    /* move the range table and join tree out of the ParseState into the Query */
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, mcx::PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, mcx::PgVec::new_in(mcx));
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        types_nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: None,
        },
    )?);

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    Ok(qry)
}

/// `transformValuesClause(pstate, stmt)` — transform a standalone VALUES into a
/// `Query` with a VALUES RTE. Reaches `addRangeTableEntryForValues`, which is a
/// seam-and-panic in the parse_relation owner (the central Node enum has no
/// List-of-columns carrier for `RTE.values_lists` yet); VALUES analyze
/// therefore panics at that boundary until the parse_relation VALUES-RTE
/// follow-on lands. The remaining VALUES logic is ported faithfully.
pub fn transformValuesClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_SELECT;

    /* process the WITH clause independently of all else */
    if let Some(with) = stmt.withClause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    /*
     * For each row of VALUES, transform the raw expressions, building a
     * column-organized intermediate representation.
     */
    let mut colexprs: Vec<Vec<types_nodes::primnodes::Expr>> = Vec::new();
    let mut sublist_length: i32 = -1;

    for lc in stmt.valuesLists.iter() {
        let sublist = match lc.as_ref().as_list() {
            Some(items) => items,
            None => return Err(elog_error("VALUES sublist is not a List")),
        };
        let mut sublist_owned = mcx::vec_with_capacity_in(mcx, sublist.len())?;
        for item in sublist.iter() {
            sublist_owned.push(mcx::alloc_in(mcx, item.clone_in(mcx)?)?);
        }
        let transformed = backend_parser_parse_target::transformExpressionList(
            mcx,
            pstate,
            sublist_owned,
            ParseExprKind::EXPR_KIND_VALUES,
            false,
        )?;

        if sublist_length < 0 {
            sublist_length = transformed.len() as i32;
            colexprs.try_reserve(sublist_length as usize).map_err(|_| mcx.oom(0))?;
            for _ in 0..sublist_length {
                colexprs.push(Vec::new());
            }
        } else if sublist_length != transformed.len() as i32 {
            return Err(elog_error("VALUES lists must all be the same length"));
        }

        for (i, expr) in transformed.into_iter().enumerate() {
            colexprs[i].push(expr);
        }
    }

    /*
     * Resolve common types/typmods/collations per column and coerce.
     */
    let mut coltypes = mcx::vec_with_capacity_in(mcx, sublist_length.max(0) as usize)?;
    let mut coltypmods = mcx::vec_with_capacity_in(mcx, sublist_length.max(0) as usize)?;
    let mut colcollations = mcx::vec_with_capacity_in(mcx, sublist_length.max(0) as usize)?;

    for i in 0..(sublist_length.max(0) as usize) {
        let coltype = backend_parser_coerce::select_common_type(
            Some(&*pstate),
            &colexprs[i],
            Some("VALUES"),
        )?;
        let col_take = core::mem::take(&mut colexprs[i]);
        let mut coerced = Vec::new();
        coerced.try_reserve(col_take.len()).map_err(|_| mcx.oom(0))?;
        for col in col_take {
            let c = backend_parser_coerce::coerce_to_common_type(
                mcx,
                Some(&mut *pstate),
                col,
                coltype,
                "VALUES",
            )?;
            coerced.push(c);
        }
        let coltypmod = backend_parser_coerce::select_common_typmod(&coerced, coltype)?;
        let mut coerced_mut = coerced;
        let colcoll = backend_parser_parse_collate::select_common_collation(
            Some(&*pstate),
            &mut coerced_mut,
            true,
        )?;
        let coerced = coerced_mut;
        colexprs[i] = coerced;
        coltypes.push(coltype);
        coltypmods.push(coltypmod);
        colcollations.push(colcoll);
    }

    /* rearrange the coerced expressions into row-organized lists */
    let nrows = stmt.valuesLists.len();
    let mut exprs_lists: mcx::PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, nrows)?;
    for r in 0..nrows {
        let mut row: mcx::PgVec<'mcx, NodePtr<'mcx>> =
            mcx::vec_with_capacity_in(mcx, sublist_length.max(0) as usize)?;
        for i in 0..(sublist_length.max(0) as usize) {
            let e = colexprs[i][r].clone();
            row.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?);
        }
        exprs_lists.push(mcx::alloc_in(mcx, Node::mk_list(mcx, row)?)?);
    }

    /*
     * Mark the VALUES RTE as LATERAL if (inside CREATE RULE) it references
     * NEW/OLD vars of the current level.
     */
    let mut lateral = false;
    if !pstate.p_rtable.is_empty() {
        let probe = Node::mk_list(mcx, {
            let mut v = mcx::vec_with_capacity_in(mcx, exprs_lists.len())?;
            for e in exprs_lists.iter() {
                v.push(mcx::alloc_in(mcx, e.clone_in(mcx)?)?);
            }
            v
        })?;
        if backend_optimizer_util_vars::var::contain_vars_of_level(&probe, 0) {
            lateral = true;
        }
    }

    /* Generate the VALUES RTE (parse_relation owner — panics until VALUES-RTE
     * carrier lands). */
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
    let ns_index = pstate.p_namespace.len();
    backend_parser_relation::addNSItemToQuery(mcx, pstate, nsitem, true, true, true)?;

    /* Generate a targetlist as though expanding "*" */
    let mut tlist_vec: Vec<_> =
        backend_parser_relation::expandNSItemAttrs(mcx, pstate, ns_index, 0, true, -1)?
            .into_iter()
            .collect();

    /* ORDER BY / LIMIT */
    let sort_input = sortby_list(mcx, &stmt.sortClause)?;
    let sort_clause = backend_parser_clause::transformSortClause(
        mcx,
        pstate,
        &sort_input,
        &mut tlist_vec,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;
    qry.sortClause = sgc_vec_to_nodes(mcx, sort_clause)?;
    qry.targetList = {
        let mut v = mcx::vec_with_capacity_in(mcx, tlist_vec.len())?;
        for te in tlist_vec {
            v.push(te);
        }
        v
    };

    let limit_offset = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.limitOffset)?,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    let limit_count = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.limitCount)?,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOffset = opt_expr_to_box(mcx, limit_offset)?;
    qry.limitCount = opt_expr_to_box(mcx, limit_count)?;
    qry.limitOption = stmt.limitOption;

    if !stmt.lockingClause.is_empty() {
        let strength = match stmt.lockingClause[0].as_ref().as_lockingclause() {
            Some(lc) => lc.strength,
            None => return Err(elog_error("locking clause item is not a LockingClause")),
        };
        return Err(elog_error(alloc::format!(
            "{} cannot be applied to VALUES",
            crate::locking::LCS_asString(strength)
        )));
    }

    qry.rtable = core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, mcx::PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, mcx::PgVec::new_in(mcx));
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        types_nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: None,
        },
    )?);

    qry.hasSubLinks = pstate.p_hasSubLinks;

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    Ok(qry)
}

/// Detect the C `linitial(stmt->distinctClause) == NULL` "SELECT DISTINCT (all
/// columns)" marker. The grammar's `list_make1(NIL)` carries a single NULL list
/// cell; the raw->owned converter encodes a NULL list cell as an empty
/// `Node::List`. A real DISTINCT ON list never starts with a NULL/empty marker
/// (its elements are column expressions).
pub(crate) fn distinct_all_marker(distinct: &[NodePtr<'_>]) -> bool {
    distinct.len() == 1 && distinct[0].as_ref().as_list().is_some_and(|l| l.is_empty())
}

/// Convert an `Option<NodePtr>` (a raw `Node *` clause input) into the owned
/// `Option<Node>` that the clause owners' `transform*Clause` entry points take.
pub(crate) fn opt_node_to_owned<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Option<NodePtr<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match n {
        Some(node) => Ok(Some(node.clone_in(mcx)?)),
        None => Ok(None),
    }
}
