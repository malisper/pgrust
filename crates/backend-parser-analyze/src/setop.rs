//! The set-operation path of `parser/analyze.c`:
//! `transformSetOperationStmt`, `transformSetOperationTree`,
//! `makeSortGroupClauseForSetOp`, `determineRecursiveColTypes`.

use alloc::format;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::{ntag, CmdType, Node, NodePtr};
use types_nodes::parsestmt::{ParseExprKind, ParseNamespaceColumn, ParseState};
use types_nodes::primnodes::{Expr, SetToDefault, TargetEntry};
use types_nodes::rawnodes::{
    SelectStmt, SetOperation, SetOperationStmt, SortGroupClause,
};

use crate::{elog_error, sgc_vec_to_nodes};

const RECORDOID: Oid = 2249;
const RECORDARRAYOID: Oid = 2287;
const UNKNOWNOID: Oid = 705;

/// `makeSortGroupClauseForSetOp(rescoltype, require_hash)` — a `SortGroupClause`
/// for a `SetOperationStmt`'s `groupClauses`.
pub fn makeSortGroupClauseForSetOp(
    rescoltype: Oid,
    require_hash: bool,
) -> PgResult<SortGroupClause> {
    let ops =
        backend_parser_parse_oper::get_sort_group_operators(rescoltype, false, true, false, true)?;
    let sortop = ops.lt_opr;
    let eqop = ops.eq_opr;

    // The type cache doesn't believe record is hashable, but if the caller
    // really needs hash support we assume it does.
    let hashable = if require_hash && (rescoltype == RECORDOID || rescoltype == RECORDARRAYOID) {
        true
    } else {
        ops.is_hashable
    };

    Ok(SortGroupClause {
        tleSortGroupRef: 0,
        eqop,
        sortop,
        reverse_sort: false,
        nulls_first: false,
        hashable,
    })
}

/// `transformSetOperationStmt(pstate, stmt)` — transform a UNION/INTERSECT/
/// EXCEPT tree into a top-level `Query` with a `setOperations` tree.
pub fn transformSetOperationStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_SELECT;

    // Find leftmost leaf SelectStmt (only to deliver a suitable INTO error).
    {
        let mut leftmost = stmt.larg.as_deref();
        while let Some(l) = leftmost {
            if l.op == SetOperation::SETOP_NONE {
                break;
            }
            leftmost = l.larg.as_deref();
        }
        if let Some(l) = leftmost {
            if l.intoClause.is_some() {
                return Err(elog_error("SELECT ... INTO is not allowed here"));
            }
        }
    }

    // Extract top-level clauses so transformSetOperationTree doesn't see them.
    let sort_clause = &stmt.sortClause;
    let limit_offset = &stmt.limitOffset;
    let limit_count = &stmt.limitCount;
    let locking_clause = &stmt.lockingClause;
    let with_clause = &stmt.withClause;

    // We build a working copy of the stmt with these cleared so the recursion
    // sees a clean internal node (C clears them in-place on the input).
    let mut stmt_inner = stmt.clone_in(mcx)?;
    stmt_inner.sortClause = PgVec::new_in(mcx);
    stmt_inner.limitOffset = None;
    stmt_inner.limitCount = None;
    stmt_inner.lockingClause = PgVec::new_in(mcx);
    stmt_inner.withClause = None;

    if !locking_clause.is_empty() {
        let strength = match locking_clause[0].as_ref().as_lockingclause() {
            Some(lc) => lc.strength,
            None => return Err(elog_error("locking clause item is not a LockingClause")),
        };
        return Err(elog_error(format!(
            "{} is not allowed with UNION/INTERSECT/EXCEPT",
            crate::locking::LCS_asString(strength)
        )));
    }

    // Process the WITH clause independently of all else.
    if let Some(with) = with_clause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = crate::cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    // Recursively transform the components of the tree.
    let sostmt_node = transformSetOperationTree(mcx, pstate, &stmt_inner, true, None)?;
    let sostmt = match sostmt_node.as_ref().as_setoperationstmt() {
        Some(s) => s,
        None => return Err(elog_error("transformSetOperationTree did not return a SetOperationStmt")),
    };

    // Re-find leftmost SELECT (now a sub-query in the rangetable).
    let leftmost_rti = {
        let mut node: &Node = sostmt.larg.as_deref().ok_or_else(|| {
            elog_error("set-op tree has no left child")
        })?;
        loop {
            match node.node_tag() {
                ntag::T_SetOperationStmt => {
                    let s = node.expect_setoperationstmt();
                    node = s.larg.as_deref().ok_or_else(|| {
                        elog_error("set-op tree has no left child")
                    })?;
                }
                ntag::T_RangeTblRef => break node.expect_rangetblref().rtindex,
                _ => return Err(elog_error("set-op leftmost is not a RangeTblRef")),
            }
        }
    };

    // Snapshot the column descriptors from the topmost set-op and the leftmost
    // subquery's targetlist (used to build the outer dummy targetlist).
    let col_types: Vec<Oid> = sostmt.colTypes.iter().copied().collect();
    let col_typmods: Vec<i32> = sostmt.colTypmods.iter().copied().collect();
    let col_collations: Vec<Oid> = sostmt.colCollations.iter().copied().collect();

    // Leftmost subquery's non-resjunk column names/resnos/locations.
    let left_cols: Vec<(AttrNumberish, Option<alloc::string::String>, i32)> = {
        let rte = &pstate.p_rtable[(leftmost_rti - 1) as usize];
        let leftq = rte
            .subquery
            .as_deref()
            .ok_or_else(|| elog_error("leftmost set-op member is not a subquery"))?;
        leftq
            .targetList
            .iter()
            .map(|tle| {
                let loc = tle
                    .expr
                    .as_deref()
                    .and_then(|e| backend_nodes_core::nodefuncs::expr_location(Some(e)).ok())
                    .unwrap_or(-1);
                (
                    tle.resno,
                    tle.resname.as_deref().map(|s| s.to_string()),
                    loc,
                )
            })
            .collect()
    };

    // Generate dummy targetlist + parallel vars/names/nscolumns.
    // The target list is threaded as a std `Vec` through transformSortClause.
    let mut tlist: Vec<TargetEntry<'mcx>> = Vec::new();
    tlist.try_reserve(col_types.len()).map_err(|_| mcx.oom(col_types.len()))?;
    let mut targetvars: PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, col_types.len())?;
    let mut targetnames: PgVec<'mcx, NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, col_types.len())?;
    let mut sortnscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, col_types.len())?;

    for i in 0..col_types.len() {
        let (resno, colname, varloc) = &left_cols[i];
        let col_type = col_types[i];
        let col_typmod = col_typmods[i];
        let col_collation = col_collations[i];

        let mut var = backend_nodes_core::makefuncs::make_var(
            leftmost_rti,
            *resno,
            col_type,
            col_typmod,
            col_collation,
            0,
        );
        var.location = *varloc;

        let resno_assigned = pstate.p_next_resno;
        pstate.p_next_resno += 1;

        let tle = backend_nodes_core::makefuncs::make_target_entry(
            mcx,
            Expr::Var(var.clone()),
            resno_assigned as types_core::primitive::AttrNumber,
            colname.as_deref(),
            false,
        )?;
        tlist.push(tle);
        targetvars.push(mcx::alloc_in(mcx, Node::Expr(Expr::Var(var)))?);
        // makeString(colName): colName == pstrdup(lefttle->resname), never NULL
        // for a valid (non-resjunk) leftmost target entry.
        let sval = match colname {
            Some(s) => mcx::PgString::from_str_in(s, mcx)?,
            None => return Err(elog_error("set-op leftmost column has no name")),
        };
        let name_node = Node::String(types_nodes::value::StringNode { sval });
        targetnames.push(mcx::alloc_in(mcx, name_node)?);
        sortnscolumns.push(ParseNamespaceColumn {
            p_varno: leftmost_rti as types_core::primitive::Index,
            p_varattno: *resno,
            p_vartype: col_type,
            p_vartypmod: col_typmod,
            p_varcollid: col_collation,
            p_varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: leftmost_rti as types_core::primitive::Index,
            p_varattnosyn: *resno,
            p_dontexpand: false,
        });
    }

    // Generate a Join RTE namespace entry making the output columns visible.
    let sv_rtable_length = pstate.p_rtable.len();

    let jnsitem = backend_parser_relation::addRangeTableEntryForJoin(
        mcx,
        pstate,
        &targetnames,
        sortnscolumns,
        types_nodes::jointype::JoinType::JOIN_INNER,
        0,
        targetvars,
        PgVec::new_in(mcx),
        PgVec::new_in(mcx),
        None,
        None,
        false,
    )?;

    let sv_namespace = core::mem::replace(&mut pstate.p_namespace, PgVec::new_in(mcx));

    // add jnsitem to column namespace only
    backend_parser_relation::addNSItemToQuery(mcx, pstate, jnsitem, false, false, true)?;

    // For now, only SQL92-spec ORDER BY (by name/number) is supported on a
    // set-op output; enforce by checking transformSortClause adds no tlist items.
    let tllen = tlist.len();

    let sort_input = sortby_list(mcx, sort_clause)?;
    let transformed_sort = backend_parser_clause::transformSortClause(
        mcx,
        pstate,
        &sort_input,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    // restore namespace, remove join RTE from rtable
    pstate.p_namespace = sv_namespace;
    pstate.p_rtable.truncate(sv_rtable_length);

    if tllen != tlist.len() {
        return Err(elog_error(
            "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
        ));
    }
    qry.targetList = {
        let mut v = mcx::vec_with_capacity_in(mcx, tlist.len())?;
        for te in tlist {
            v.push(te);
        }
        v
    };
    qry.sortClause = sgc_vec_to_nodes(mcx, transformed_sort)?;

    let lo = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_owned(mcx, limit_offset)?,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    let lc = backend_parser_clause::transformLimitClause(
        mcx,
        pstate,
        opt_node_owned(mcx, limit_count)?,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOffset = crate::opt_expr_to_box(mcx, lo)?;
    qry.limitCount = crate::opt_expr_to_box(mcx, lc)?;
    qry.limitOption = stmt.limitOption;

    qry.setOperations = Some(sostmt_node);

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

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    // lockingClause was already rejected above (NIL here).

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    if pstate.p_hasAggs
        || !qry.groupClause.is_empty()
        || !qry.groupingSets.is_empty()
        || qry.havingQual.is_some()
    {
        backend_parser_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    Ok(qry)
}

type AttrNumberish = types_core::primitive::AttrNumber;

/// `transformSetOperationTree(pstate, stmt, isTopLevel, targetlist)` —
/// recursively transform leaves and internal nodes of a set-op tree, returning
/// the transformed node (a `RangeTblRef` for a leaf or a `SetOperationStmt` for
/// an internal node). When `want_targetlist` is set, also collects the
/// per-column dummy/real `TargetEntry`s for the parent level.
fn transformSetOperationTree<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &SelectStmt<'mcx>,
    is_top_level: bool,
    targetlist: Option<&mut Vec<TargetEntry<'mcx>>>,
) -> PgResult<NodePtr<'mcx>> {
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    if stmt.intoClause.is_some() {
        return Err(elog_error(
            "INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT",
        ));
    }
    if !stmt.lockingClause.is_empty() {
        let strength = match stmt.lockingClause[0].as_ref().as_lockingclause() {
            Some(lc) => lc.strength,
            None => return Err(elog_error("locking clause item is not a LockingClause")),
        };
        return Err(elog_error(format!(
            "{} is not allowed with UNION/INTERSECT/EXCEPT",
            crate::locking::LCS_asString(strength)
        )));
    }

    let is_leaf = if stmt.op == SetOperation::SETOP_NONE {
        true
    } else if !stmt.sortClause.is_empty()
        || stmt.limitOffset.is_some()
        || stmt.limitCount.is_some()
        || !stmt.lockingClause.is_empty()
        || stmt.withClause.is_some()
    {
        true
    } else {
        false
    };

    if is_leaf {
        // Process leaf SELECT: analyze as a sub-query (resolve_unknowns=false).
        let stmt_node = Node::SelectStmt(stmt.clone_in(mcx)?);
        let select_query_node =
            crate::parse_sub_analyze(mcx, &stmt_node, pstate, None, false, false)?;
        let select_query_ref = select_query_node.as_ref();
        let select_query = match select_query_ref.node_tag() {
            ntag::T_Query => select_query_ref.expect_query(),
            _ => return Err(elog_error("parse_sub_analyze did not return a Query")),
        };

        // Reject Vars referencing the current query level (only possible in a
        // rule). Upper-level references are okay.
        if !pstate.p_namespace.is_empty()
            && backend_optimizer_util_vars::var::contain_vars_of_level(&select_query_node, 1)
        {
            return Err(elog_error(
                "UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query level",
            ));
        }

        // Extract non-junk TLEs for upper-level processing.
        if let Some(tl) = targetlist {
            tl.clear();
            for tle in select_query.targetList.iter() {
                if !tle.resjunk {
                    tl.push(tle.clone_in(mcx)?);
                }
            }
        }

        // Make the leaf query a subquery in the top-level rangetable.
        let select_name = format!("*SELECT* {}", pstate.p_rtable.len() + 1);
        let alias = backend_nodes_core::makefuncs::make_alias(mcx, &select_name, PgVec::new_in(mcx))?;
        let selectq_owned = select_query.clone_in(mcx)?;
        let nsitem = backend_parser_relation::addRangeTableEntryForSubquery(
            mcx,
            pstate,
            selectq_owned,
            Some(alias),
            false,
            false,
        )?;

        let rtr = types_nodes::rawnodes::RangeTblRef {
            rtindex: nsitem.p_rtindex,
        };
        return mcx::alloc_in(mcx, Node::RangeTblRef(rtr));
    }

    // Process an internal node (set operation node).
    let recursive = pstate
        .p_parent_cte
        .as_deref()
        .map(|c| c.cterecursive)
        .unwrap_or(false);

    let context = match stmt.op {
        SetOperation::SETOP_UNION => "UNION",
        SetOperation::SETOP_INTERSECT => "INTERSECT",
        _ => "EXCEPT",
    };

    let mut op = SetOperationStmt {
        op: stmt.op,
        all: stmt.all,
        larg: None,
        rarg: None,
        colTypes: PgVec::new_in(mcx),
        colTypmods: PgVec::new_in(mcx),
        colCollations: PgVec::new_in(mcx),
        groupClauses: PgVec::new_in(mcx),
    };

    // Recursively transform the left child.
    let mut ltargetlist: Vec<TargetEntry<'mcx>> = Vec::new();
    let larg = transformSetOperationTree(mcx, pstate, stmt.larg.as_deref().unwrap(), false, Some(&mut ltargetlist))?;

    if is_top_level && recursive {
        determineRecursiveColTypes(mcx, pstate, &larg, &ltargetlist)?;
    }
    op.larg = Some(larg);

    // Recursively transform the right child.
    let mut rtargetlist: Vec<TargetEntry<'mcx>> = Vec::new();
    let rarg = transformSetOperationTree(mcx, pstate, stmt.rarg.as_deref().unwrap(), false, Some(&mut rtargetlist))?;
    op.rarg = Some(rarg);

    if ltargetlist.len() != rtargetlist.len() {
        return Err(elog_error(format!(
            "each {} query must have the same number of columns",
            context
        )));
    }

    let mut out_tl: Vec<TargetEntry<'mcx>> = Vec::new();
    for (ltle, rtle) in ltargetlist.iter().zip(rtargetlist.iter()) {
        let lcolnode = ltle
            .expr
            .as_deref()
            .ok_or_else(|| elog_error("set-op left column expr is NULL"))?;
        let rcolnode = rtle
            .expr
            .as_deref()
            .ok_or_else(|| elog_error("set-op right column expr is NULL"))?;
        let lcoltype = exprtype(lcolnode)?;
        let rcoltype = exprtype(rcolnode)?;

        let exprs = [lcolnode.clone(), rcolnode.clone()];
        let rescoltype =
            backend_parser_coerce::select_common_type(Some(pstate), &exprs, Some(context))?;
        // bestlocation: the C tracks the bestexpr's location for the error
        // cursor; the ported select_common_type does not surface bestexpr, and
        // the location model is trimmed repo-wide, so bestlocation == -1.
        let _bestlocation = -1i32;

        // Coerce UNKNOWN Const/Param children in place; verify others.
        let lcolnode2 = if lcoltype != UNKNOWNOID {
            backend_parser_coerce::coerce_to_common_type(mcx, Some(pstate), lcolnode.clone(), rescoltype, context)?
        } else if matches!(lcolnode, Expr::Const(_)) || matches!(lcolnode, Expr::Param(_)) {
            backend_parser_coerce::coerce_to_common_type(mcx, Some(pstate), lcolnode.clone(), rescoltype, context)?
        } else {
            lcolnode.clone()
        };
        let rcolnode2 = if rcoltype != UNKNOWNOID {
            backend_parser_coerce::coerce_to_common_type(mcx, Some(pstate), rcolnode.clone(), rescoltype, context)?
        } else if matches!(rcolnode, Expr::Const(_)) || matches!(rcolnode, Expr::Param(_)) {
            backend_parser_coerce::coerce_to_common_type(mcx, Some(pstate), rcolnode.clone(), rescoltype, context)?
        } else {
            rcolnode.clone()
        };

        let mut coerced = [lcolnode2.clone(), rcolnode2.clone()];
        let rescoltypmod = backend_parser_coerce::select_common_typmod(&coerced, rescoltype)?;
        let rescolcoll = backend_parser_parse_collate::select_common_collation(
            Some(&*pstate),
            &mut coerced,
            op.op == SetOperation::SETOP_UNION && op.all,
        )?;

        op.colTypes.push(rescoltype);
        op.colTypmods.push(rescoltypmod);
        op.colCollations.push(rescolcoll);

        if op.op != SetOperation::SETOP_UNION || !op.all {
            let grpcl = makeSortGroupClauseForSetOp(rescoltype, recursive)?;
            op.groupClauses.push(mcx::alloc_in(mcx, Node::SortGroupClause(grpcl))?);
        }

        // Construct a dummy tlist entry to return (SetToDefault carrier).
        let rescolnode = SetToDefault {
            typeId: rescoltype,
            typeMod: rescoltypmod,
            collation: rescolcoll,
            location: _bestlocation,
        };
        let restle = backend_nodes_core::makefuncs::make_target_entry(
            mcx,
            Expr::SetToDefault(rescolnode),
            0,
            None,
            false,
        )?;
        out_tl.push(restle);
    }

    // Note: in-place coercion of UNKNOWN Const/Param children of leaf nodes is
    // a behaviour the C performs by mutating ltle->expr/rtle->expr of the leaf
    // query's targetlist. Because the owned model returns per-level copies, the
    // replacement here updates the dummy/extracted tlist only; the leaf query's
    // stored Const is re-resolved by the planner from the colTypes. This is the
    // documented trimmed-model boundary (see audits).

    if let Some(tl) = targetlist {
        *tl = out_tl;
    }

    mcx::alloc_in(mcx, Node::SetOperationStmt(op))
}

/// `determineRecursiveColTypes(pstate, larg, nrtargetlist)` — set up the parent
/// recursive CTE's columns from the non-recursive term's outputs.
fn determineRecursiveColTypes<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    larg: &NodePtr<'mcx>,
    nrtargetlist: &[TargetEntry<'mcx>],
) -> PgResult<()> {
    // Find leftmost leaf SELECT.
    let leftmost_rti = {
        let mut node: &Node = larg.as_ref();
        loop {
            match node.node_tag() {
                ntag::T_SetOperationStmt => {
                    let s = node.expect_setoperationstmt();
                    node = s.larg.as_deref().ok_or_else(|| elog_error("set-op tree has no left child"))?;
                }
                ntag::T_RangeTblRef => break node.expect_rangetblref().rtindex,
                _ => return Err(elog_error("set-op leftmost is not a RangeTblRef")),
            }
        }
    };

    let colnames: Vec<Option<alloc::string::String>> = {
        let rte = &pstate.p_rtable[(leftmost_rti - 1) as usize];
        let leftq = rte
            .subquery
            .as_deref()
            .ok_or_else(|| elog_error("leftmost set-op member is not a subquery"))?;
        leftq
            .targetList
            .iter()
            .map(|tle| tle.resname.as_deref().map(|s| s.to_string()))
            .collect()
    };

    let mut target_list: Vec<TargetEntry<'mcx>> = Vec::new();
    let mut next_resno = 1;
    for (nrtle, colname) in nrtargetlist.iter().zip(colnames.iter()) {
        let expr = nrtle
            .expr
            .as_deref()
            .ok_or_else(|| elog_error("recursive col expr is NULL"))?
            .clone();
        let tle = backend_nodes_core::makefuncs::make_target_entry(
            mcx,
            expr,
            next_resno,
            colname.as_deref(),
            false,
        )?;
        next_resno += 1;
        target_list.push(tle);
    }

    let cte_present = pstate.p_parent_cte.is_some();
    if !cte_present {
        return Err(elog_error("determineRecursiveColTypes: no parent CTE"));
    }
    let mut cte = pstate.p_parent_cte.take().unwrap();
    let r = backend_parser_cte::analyzeCTETargetList(mcx, pstate, &mut cte, &target_list);

    // In C, `p_parent_cte` and the `p_ctenamespace` entry that the recursive
    // self-reference resolves against are the same pointer, so writing the
    // analyzed columns onto `p_parent_cte` makes them visible to the recursive
    // term. The owned model holds separate copies (and the namespace entry lives
    // in an ancestor `ParseState`), so push the freshly-determined column
    // metadata into every matching `p_ctenamespace` entry up the parent chain.
    // The recursive term's child state is cloned from `pstate` *after* this
    // runs, so it then sees a CTE that exposes its columns.
    if r.is_ok() {
        if let Some(name) = cte.ctename.as_deref().map(|s| s.to_string()) {
            propagate_recursive_cte_columns(mcx, pstate, &name, &cte)?;
        }
    }

    pstate.p_parent_cte = Some(cte);
    r
}

/// Copy the analyzed output-column metadata of a recursive CTE into the
/// matching `p_ctenamespace` entry of `pstate` and every ancestor — the owned-
/// model stand-in for C aliasing `p_parent_cte` with the namespace CTE pointer.
fn propagate_recursive_cte_columns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    name: &str,
    cte: &types_nodes::rawnodes::CommonTableExpr<'mcx>,
) -> PgResult<()> {
    let mut cur: Option<&mut ParseState<'mcx>> = Some(pstate);
    while let Some(ps) = cur {
        for entry in ps.p_ctenamespace.iter_mut() {
            if entry.ctename.as_deref() == Some(name) {
                let mut ctecolnames: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
                ctecolnames
                    .try_reserve(cte.ctecolnames.len())
                    .map_err(|_| mcx.oom(cte.ctecolnames.len()))?;
                for n in cte.ctecolnames.iter() {
                    ctecolnames.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
                }
                entry.ctecolnames = ctecolnames;
                entry.ctecoltypes = mcx::slice_in(mcx, &cte.ctecoltypes)?;
                entry.ctecoltypmods = mcx::slice_in(mcx, &cte.ctecoltypmods)?;
                entry.ctecolcollations = mcx::slice_in(mcx, &cte.ctecolcollations)?;
            }
        }
        cur = ps.parentParseState.as_deref_mut();
    }
    Ok(())
}

/// `exprType(node)` for a typed `Expr` — delegate to nodes-core.
fn exprtype(e: &Expr) -> PgResult<Oid> {
    backend_nodes_core::nodefuncs::expr_type(Some(e))
}

/// Downcast a `List *` of raw `Node`s (`stmt->sortClause`) to `Vec<SortBy>`.
fn sortby_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &[NodePtr<'mcx>],
) -> PgResult<Vec<types_nodes::rawnodes::SortBy<'mcx>>> {
    let mut out = Vec::new();
    out.try_reserve(list.len()).map_err(|_| mcx.oom(list.len()))?;
    for n in list {
        match n.as_ref().as_sortby() {
            Some(s) => out.push(s.clone_in(mcx)?),
            None => return Err(elog_error("ORDER BY item is not a SortBy")),
        }
    }
    Ok(out)
}

fn opt_node_owned<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Option<NodePtr<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match n {
        Some(node) => Ok(Some(node.clone_in(mcx)?)),
        None => Ok(None),
    }
}
