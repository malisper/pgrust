//! The PL/pgSQL assignment path of `parser/analyze.c`: `transformPLAssignStmt`.
//!
//! A `PLAssignStmt` (`x := <expr>`, with optional indirection `x[i] := ...` /
//! `x.f := ...`) is produced only by the `RAW_PARSE_PLPGSQL_ASSIGN{1,2,3}` raw-
//! parse modes; PL/pgSQL's `exec_assign_value` path compiles the assignment into
//! this node and feeds it through parse analysis. The transform builds a
//! `CMD_SELECT` `Query` whose single target-list item computes the new value for
//! the target variable: with no indirection it is just the (coerced) source
//! expression; with indirection it incorporates `FieldStore` / assignment
//! `SubscriptingRef` nodes that read the target variable as the container source
//! (the same machinery `transformUpdateTargetList` uses for `UPDATE ... SET`).
//!
//! Ported 1:1 from `transformPLAssignStmt` (analyze.c) — branch order, list
//! peeling, the `transformAssignmentIndirection` vs `coerce_to_target_type` vs
//! composite-passthrough three-way, the clause passes, and the final-Query
//! assembly all match the C, reusing the same already-merged clause owners as
//! `transformSelectStmt`.

use alloc::format;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{PgResult, ERRCODE_SYNTAX_ERROR, ERROR};
use nodes::copy_query::Query;
use nodes::ddlnodes::{CoercionContext, PLAssignStmt};
use nodes::nodes::{CmdType, Node, NodePtr};
use nodes::parsestmt::{ParseExprKind, ParseState};
use nodes::primnodes::CoercionForm;
use nodes::rawnodes::{ColumnRef, SelectStmt};
use nodes::value::StringNode;

use nodes_core::nodefuncs::{expr_collation, expr_location, expr_type, expr_typmod};
use utils_error::ereport;

use crate::select::{distinct_all_marker, sortby_list};
use crate::{
    elog_error, node_vec_to_pgvec, opt_expr_to_box, opt_expr_to_node, sgc_vec_to_nodes,
};

/// `RECORDOID` (`pg_type.h`).
const RECORDOID: Oid = 2249;
/// `TYPTYPE_COMPOSITE` (`pg_type.h`).
const TYPTYPE_COMPOSITE: u8 = b'c';

/// `ISCOMPLEX(typeid)` (analyze.c macro): `typeid == RECORDOID ||
/// get_typtype(typeid) == TYPTYPE_COMPOSITE`.
fn iscomplex(typeid: Oid) -> PgResult<bool> {
    if typeid == RECORDOID {
        return Ok(true);
    }
    Ok(lsyscache_seams::get_typtype::call(typeid)? == TYPTYPE_COMPOSITE)
}

/// `transformPLAssignStmt(pstate, stmt)` (analyze.c) — transform a PL/pgSQL
/// assignment statement.
pub fn transformPLAssignStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &PLAssignStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);

    // List *indirection = stmt->indirection;
    // int   nnames = stmt->nnames;
    // SelectStmt *sstmt = stmt->val;
    //
    // The owned model carries the indirection list by value; C copies it (via
    // list_copy) only when peeling extra names below, so we mirror that by only
    // cloning when nnames > 1.
    let mut nnames = stmt.nnames;
    let sstmt: &SelectStmt<'mcx> = stmt
        .val
        .as_deref()
        .and_then(|n| n.as_selectstmt())
        .ok_or_else(|| elog_error("transformPLAssignStmt: PLAssignStmt val is not a SelectStmt"))?;

    let name: &str = stmt
        .name
        .as_ref()
        .map(|s| s.as_str())
        .ok_or_else(|| elog_error("transformPLAssignStmt: PLAssignStmt has no target name"))?;

    /*
     * First, construct a ColumnRef for the target variable.  If the target
     * has more than one dotted name, we have to pull the extra names out of
     * the indirection list.
     */
    // cref->fields = list_make1(makeString(stmt->name));
    let mut cref_fields: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    cref_fields.try_reserve(1).map_err(|_| mcx.oom(1))?;
    cref_fields.push(mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, StringNode { sval: mcx::PgString::from_str_in(name, mcx)? })?,
    )?);

    // The indirection list we work with: the raw list, or (when peeling names)
    // an owned copy we mutate. C: `indirection = list_copy(indirection)` then
    // `list_delete_first`. We model the peel as a running start-index `ind_start`
    // into a cloned Vec so the raw parsetree is not munged.
    // C `list_copy(indirection)` only when nnames > 1; otherwise it walks the
    // raw list directly. We always materialize an owned copy of the cells (a
    // `NodePtr` is `Box<Node>`, not `Clone`, so deep-copy via `clone_in`) — this
    // never munges the raw parsetree and matches C's behavior either way.
    let mut indirection: Vec<NodePtr<'mcx>> = Vec::new();
    indirection
        .try_reserve(stmt.indirection.len())
        .map_err(|_| mcx.oom(stmt.indirection.len()))?;
    for n in stmt.indirection.iter() {
        indirection.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    let mut ind_start: usize = 0;

    if nnames > 1 {
        // while (--nnames > 0 && indirection != NIL)
        loop {
            nnames -= 1;
            if nnames <= 0 || ind_start >= indirection.len() {
                break;
            }
            let ind = &indirection[ind_start];
            // if (!IsA(ind, String)) elog(ERROR, "invalid name count in PLAssignStmt");
            if !ind.is_string() {
                return Err(elog_error("invalid name count in PLAssignStmt"));
            }
            // cref->fields = lappend(cref->fields, ind);
            cref_fields.try_reserve(1).map_err(|_| mcx.oom(1))?;
            cref_fields.push(mcx::alloc_in(mcx, ind.clone_in(mcx)?)?);
            // indirection = list_delete_first(indirection);
            ind_start += 1;
        }
    }

    let cref = Node::mk_column_ref(
        mcx,
        ColumnRef {
            fields: cref_fields,
            location: stmt.location,
        },
    )?;

    /*
     * Transform the target reference.  Typically we will get back a Param
     * node, but there's no reason to be too picky about its type.
     */
    // target = transformExpr(pstate, (Node *) cref, EXPR_KIND_UPDATE_TARGET);
    // transformExpr yields an `Expr`; C keeps it as a `Node *` (`target`). We
    // read its type/typmod/collation/location here, and (only on the indirection
    // path) re-wrap it as a `Node` for `transformAssignmentIndirection`'s
    // basenode argument.
    let target: nodes::primnodes::Expr<'static> = parse_expr::transformExpr(
        pstate,
        Some(cref),
        ParseExprKind::EXPR_KIND_UPDATE_TARGET,
    )?
    .ok_or_else(|| elog_error("transformPLAssignStmt: target reference transformed to NULL"))?;
    // targettype = exprType(target);
    // targettypmod = exprTypmod(target);
    // targetcollation = exprCollation(target);
    let targettype = expr_type(Some(&target))?;
    let targettypmod = expr_typmod(Some(&target))?;
    let targetcollation = expr_collation(Some(&target))?;
    let target_location = expr_location(Some(&target))?;

    /*
     * The rest mostly matches transformSelectStmt, except that we needn't
     * consider WITH or INTO, and we build a targetlist our own way.
     */
    qry.commandType = CmdType::CMD_SELECT;
    pstate.p_is_insert = false;

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    pstate.p_locking_clause = {
        let mut v = mcx::vec_with_capacity_in(mcx, sstmt.lockingClause.len())?;
        for n in sstmt.lockingClause.iter() {
            v.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
        v
    };

    /* make WINDOW info available for window functions, too */
    pstate.p_windowdefs = {
        let mut v = mcx::vec_with_capacity_in(mcx, sstmt.windowClause.len())?;
        for n in sstmt.windowClause.iter() {
            match n.as_ref().as_windowdef() {
                Some(w) => v.push(w.clone_in(mcx)?),
                None => return Err(elog_error("WINDOW clause item is not a WindowDef")),
            }
        }
        v
    };

    /* process the FROM clause */
    clause::transformFromClause(mcx, pstate, &sstmt.fromClause)?;

    /* initially transform the targetlist as if in SELECT */
    let target_list = {
        let mut tl = mcx::vec_with_capacity_in(mcx, sstmt.targetList.len())?;
        for n in sstmt.targetList.iter() {
            match n.as_ref().as_restarget() {
                Some(rt) => tl.push(rt.clone_in(mcx)?),
                None => return Err(elog_error("SELECT target list item is not a ResTarget")),
            }
        }
        tl
    };
    let mut tlist: Vec<nodes::primnodes::TargetEntry<'mcx>> =
        parse_target::transformTargetList(
            mcx,
            pstate,
            target_list,
            ParseExprKind::EXPR_KIND_SELECT_TARGET,
        )?
        .into_iter()
        .collect();

    /* we should have exactly one targetlist item */
    if tlist.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("assignment source returned {} column{}", tlist.len(),
                if tlist.len() == 1 { "" } else { "s" }))
            .into_error());
    }

    // tle = linitial_node(TargetEntry, tlist);
    let mut tle = tlist.pop().expect("exactly one tlist item");

    /*
     * This next bit is similar to transformAssignedExpr; the key difference
     * is we use COERCION_PLPGSQL not COERCION_ASSIGNMENT.
     */
    // type_id = exprType((Node *) tle->expr);
    let type_id = expr_type(tle.expr.as_deref())?;

    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_UPDATE_TARGET;

    // The remaining indirection (after peeling names for nnames>1) drives the
    // three-way branch.
    let has_indirection = ind_start < indirection.len();

    if has_indirection {
        // tle->expr = transformAssignmentIndirection(pstate, target, stmt->name,
        //     false, targettype, targettypmod, targetcollation, indirection,
        //     list_head(indirection), (Node *) tle->expr, COERCION_PLPGSQL,
        //     exprLocation(target));
        //
        // The owner's transformAssignmentIndirection takes the indirection list
        // as a `PgVec` plus a start-cell index. We build the peeled tail into a
        // fresh PgVec (C passed the post-peel `indirection` with `list_head` as
        // the starting cell).
        let mut ind_vec: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
        let tail = &indirection[ind_start..];
        ind_vec.try_reserve(tail.len()).map_err(|_| mcx.oom(tail.len()))?;
        for n in tail {
            ind_vec.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }

        // (Node *) tle->expr — wrap the TargetEntry's Expr as a Node for the rhs.
        let rhs_expr = tle
            .expr
            .take()
            .ok_or_else(|| elog_error("transformPLAssignStmt: NULL source expr for indirection"))?;
        let rhs_node = Node::mk_expr(mcx, mcx::PgBox::into_inner(rhs_expr))?;

        // C passes `target` (a Node *) as the basenode; re-wrap our Expr value.
        let target_node = Node::mk_expr(mcx, target.clone_in(mcx)?)?;

        let assigned = parse_target::transformAssignmentIndirection(
            mcx,
            pstate,
            Some(target_node),
            name,
            false,
            targettype,
            targettypmod,
            targetcollation,
            &ind_vec,
            0,
            rhs_node,
            CoercionContext::COERCION_PLPGSQL,
            target_location,
        )?;
        // tle->expr = (Expr *) <assigned node>
        tle.expr = Some(mcx::PgBox::new_in(
            assigned
                .into_expr()
                .ok_or_else(|| elog_error("transformAssignmentIndirection did not return an Expr"))?,
            mcx,
        ));
    } else if targettype != type_id
        && (targettype == RECORDOID || iscomplex(targettype)?)
        && (type_id == RECORDOID || iscomplex(type_id)?)
    {
        /*
         * Hack: do not let coerce_to_target_type() deal with inconsistent
         * composite types.  Just pass the expression result through as-is,
         * and let the PL/pgSQL executor do the conversion its way.  This is
         * rather bogus, but it's needed for backwards compatibility.
         */
        // (no change to tle->expr)
    } else {
        /*
         * For normal non-qualified target column, do type checking and
         * coercion.
         */
        // Node *orig_expr = (Node *) tle->expr;
        let orig_expr = tle
            .expr
            .take()
            .ok_or_else(|| elog_error("transformPLAssignStmt: NULL source expr"))?;
        let orig_location = expr_location(Some(orig_expr.as_ref()))?;
        let orig = mcx::PgBox::into_inner(orig_expr);

        // tle->expr = coerce_to_target_type(pstate, orig_expr, type_id,
        //     targettype, targettypmod, COERCION_PLPGSQL, COERCE_IMPLICIT_CAST, -1);
        let coerced = coerce::coerce_to_target_type(
            mcx,
            Some(pstate),
            orig.erase_lifetime(),
            type_id,
            targettype,
            targettypmod,
            CoercionContext::COERCION_PLPGSQL,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;

        match coerced {
            // Parser-arena `'static` result re-cloned into the TLE's `mcx`.
            Some(e) => tle.expr = Some(mcx::PgBox::new_in(e.clone_in(mcx)?, mcx)),
            None => {
                // With COERCION_PLPGSQL, this error is probably unreachable.
                let targettype_name =
                    format_type_seams::format_type_be::call(mcx, targettype)?;
                let type_id_name =
                    format_type_seams::format_type_be::call(mcx, type_id)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "variable \"{}\" is of type {} but expression is of type {}",
                        name,
                        targettype_name.as_str(),
                        type_id_name.as_str()
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .errposition(small1::parser_errposition(pstate, orig_location))
                    .into_error());
            }
        }
    }

    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_NONE;

    // qry->targetList = list_make1(tle);
    let mut tlist: Vec<nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    tlist.push(tle);

    /* transform WHERE */
    let qual = clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_clone(mcx, &sstmt.whereClause)?,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    /* initial processing of HAVING clause is much like WHERE clause */
    let having = clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_clone(mcx, &sstmt.havingClause)?,
        ParseExprKind::EXPR_KIND_HAVING,
        "HAVING",
    )?;
    qry.havingQual = opt_expr_to_box(mcx, having)?;

    /*
     * Transform sorting/grouping stuff.  Do ORDER BY first because both
     * transformGroupClause and transformDistinctClause need the results.
     */
    let sort_input = sortby_list(mcx, &sstmt.sortClause)?;
    let sort_clause = clause::transformSortClause(
        mcx,
        pstate,
        &sort_input,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    let (group_clause, grouping_sets) = clause::transformGroupClause(
        mcx,
        pstate,
        &sstmt.groupClause,
        &mut tlist,
        &sort_clause,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )?;
    qry.groupDistinct = sstmt.groupDistinct;

    let distinct_clause;
    if sstmt.distinctClause.is_empty() {
        distinct_clause = Vec::new();
        qry.hasDistinctOn = false;
    } else if distinct_all_marker(&sstmt.distinctClause) {
        /* We had SELECT DISTINCT */
        distinct_clause = clause::transformDistinctClause(
            mcx,
            pstate,
            &mut tlist,
            &sort_clause,
            false,
        )?;
        qry.hasDistinctOn = false;
    } else {
        /* We had SELECT DISTINCT ON */
        distinct_clause = clause::transformDistinctOnClause(
            mcx,
            pstate,
            &sstmt.distinctClause,
            &mut tlist,
            &sort_clause,
        )?;
        qry.hasDistinctOn = true;
    }

    /* transform LIMIT */
    let limit_offset = clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_clone(mcx, &sstmt.limitOffset)?,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        sstmt.limitOption,
    )?;
    let limit_count = clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_clone(mcx, &sstmt.limitCount)?,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        sstmt.limitOption,
    )?;
    qry.limitOffset = opt_expr_to_box(mcx, limit_offset)?;
    qry.limitCount = opt_expr_to_box(mcx, limit_count)?;
    qry.limitOption = sstmt.limitOption;

    /* transform window clauses after we have seen all window functions */
    let windowdefs = {
        let mut v = mcx::vec_with_capacity_in(mcx, pstate.p_windowdefs.len())?;
        for w in pstate.p_windowdefs.iter() {
            v.push(w.clone_in(mcx)?);
        }
        v
    };
    let window_clause = clause::transformWindowDefinitions(
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

    /* qry->rtable / rteperminfos / jointree */
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, mcx::PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, mcx::PgVec::new_in(mcx));
    let qual_node = opt_expr_to_node(mcx, qual)?;
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: qual_node,
        },
    )?);

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    /* foreach lockingClause: transformLockingClause(pstate, qry, lc, false) */
    let locking = core::mem::replace(&mut pstate.p_locking_clause, mcx::PgVec::new_in(mcx));
    for lc_node in locking.iter() {
        match lc_node.as_ref().as_lockingclause() {
            Some(lc) => {
                crate::locking::transformLockingClause(mcx, pstate, &mut qry, lc, false)?;
            }
            None => return Err(elog_error("locking clause item is not a LockingClause")),
        }
    }

    parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    /* this must be done after collations, for reliable comparison of exprs */
    if pstate.p_hasAggs
        || !qry.groupClause.is_empty()
        || !qry.groupingSets.is_empty()
        || qry.havingQual.is_some()
    {
        agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    Ok(qry)
}

/// `Option<NodePtr>` raw clause input -> owned `Option<Node>` for a clause owner.
fn opt_node_clone<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Option<NodePtr<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match n {
        Some(node) => Ok(Some(node.clone_in(mcx)?)),
        None => Ok(None),
    }
}