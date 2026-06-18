//! The UPDATE/DELETE path of `parser/analyze.c`:
//! `transformUpdateStmt`, `transformDeleteStmt`, `transformUpdateTargetList`,
//! and `transformReturningClause`.
//!
//! Simple (non-inheritance, non-partition, no-trigger) UPDATE/DELETE are
//! transformed end-to-end. `transformReturningClause`/`addNSItemForReturning`
//! handle RETURNING for INSERT/UPDATE/DELETE; the OLD/NEW `WITH (...)` alias
//! options (which need a typed `ReturningOption` node the grammar conversion
//! does not yet produce) are rejected loudly.

use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_acl::acl::{ACL_DELETE, ACL_UPDATE};
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::VarReturningType;
use types_nodes::rawnodes::{DeleteStmt, ResTarget, UpdateStmt};

use crate::select::opt_node_to_owned;
use crate::{cte_vec_to_nodes, elog_error, opt_expr_to_node};

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h:27) — the offset used
/// to fold a real `AttrNumber` into the `RTEPermissionInfo` column bitmapset.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

/// `transformDeleteStmt(pstate, stmt)` (analyze.c:478) — transform a DELETE.
pub fn transformDeleteStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &DeleteStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_DELETE;

    /* process the WITH clause independently of all else */
    if let Some(with) = stmt.withClause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    /* set up range table with just the result rel */
    let relation = stmt
        .relation
        .as_deref()
        .ok_or_else(|| elog_error("DELETE: missing target relation"))?;
    qry.resultRelation = backend_parser_clause::setTargetTable(
        mcx,
        pstate,
        relation,
        relation.inh,
        true,
        ACL_DELETE,
    )?;

    /* there's no DISTINCT in DELETE */
    qry.distinctClause = PgVec::new_in(mcx);

    /* subqueries in USING cannot access the result relation */
    set_target_lateral(pstate, true, false);

    /*
     * The USING clause is non-standard SQL syntax, and is equivalent in
     * functionality to the FROM list that can be specified for UPDATE.
     */
    backend_parser_clause::transformFromClause(mcx, pstate, &stmt.usingClause)?;

    /* remaining clauses can reference the result relation normally */
    set_target_lateral(pstate, false, true);

    let qual = backend_parser_clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.whereClause)?,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    transformReturningClause(mcx, pstate, &mut qry, stmt.returningClause.as_deref(), ParseExprKind::EXPR_KIND_RETURNING)?;

    /* done building the range table and jointree */
    qry.rtable = core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, PgVec::new_in(mcx));
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

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    /* this must be done after collations, for reliable comparison of exprs */
    if pstate.p_hasAggs {
        backend_parser_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    Ok(qry)
}

/// `transformUpdateStmt(pstate, stmt)` (analyze.c:2389) — transform an UPDATE.
pub fn transformUpdateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &UpdateStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::new(mcx);
    qry.commandType = CmdType::CMD_UPDATE;
    pstate.p_is_insert = false;

    /* process the WITH clause independently of all else */
    if let Some(with) = stmt.withClause.as_deref() {
        qry.hasRecursive = with.recursive;
        let with_copy = with.clone_in(mcx)?;
        let ctes = backend_parser_cte::transformWithClause(mcx, pstate, with_copy)?;
        qry.cteList = cte_vec_to_nodes(mcx, ctes)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    let relation = stmt
        .relation
        .as_deref()
        .ok_or_else(|| elog_error("UPDATE: missing target relation"))?;
    qry.resultRelation = backend_parser_clause::setTargetTable(
        mcx,
        pstate,
        relation,
        relation.inh,
        true,
        ACL_UPDATE,
    )?;

    /* subqueries in FROM cannot access the result relation */
    set_target_lateral(pstate, true, false);

    /*
     * the FROM clause is non-standard SQL syntax. We used to be able to do
     * this with REPLACE in POSTQUEL so we keep the feature.
     */
    backend_parser_clause::transformFromClause(mcx, pstate, &stmt.fromClause)?;

    /* remaining clauses can reference the result relation normally */
    set_target_lateral(pstate, false, true);

    let qual = backend_parser_clause::transformWhereClause(
        mcx,
        pstate,
        opt_node_to_owned(mcx, &stmt.whereClause)?,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    transformReturningClause(mcx, pstate, &mut qry, stmt.returningClause.as_deref(), ParseExprKind::EXPR_KIND_RETURNING)?;

    /*
     * Now we are done with SELECT-like processing, and can get on with
     * transforming the target list to match the UPDATE target columns.
     */
    qry.targetList = transformUpdateTargetList(mcx, pstate, &stmt.targetList)?;

    qry.rtable = core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx));
    qry.rteperminfos = core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx));
    let joinlist = core::mem::replace(&mut pstate.p_joinlist, PgVec::new_in(mcx));
    let qual_node = opt_expr_to_node(mcx, qual)?;
    qry.jointree = Some(mcx::alloc_in(
        mcx,
        types_nodes::rawnodes::FromExpr {
            fromlist: joinlist,
            quals: qual_node,
        },
    )?);

    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    backend_parser_parse_collate::assign_query_collations(Some(pstate), &mut qry)?;

    Ok(qry)
}

/// `transformUpdateTargetList(pstate, origTlist)` (analyze.c:2469) — transform
/// the SET-list of an UPDATE into a `Vec<TargetEntry>`.
pub(crate) fn transformUpdateTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    orig_tlist: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    /* the clause owner threads the target list as an owned Vec<ResTarget> */
    let mut orig_targets: Vec<ResTarget<'mcx>> = Vec::new();
    orig_targets
        .try_reserve(orig_tlist.len())
        .map_err(|_| mcx.oom(orig_tlist.len()))?;
    for n in orig_tlist.iter() {
        match n.as_ref().as_restarget() {
            Some(rt) => orig_targets.push(rt.clone_in(mcx)?),
            None => return Err(elog_error("UPDATE SET item is not a ResTarget")),
        }
    }

    let mut tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>> =
        backend_parser_parse_target::transformTargetList(
            mcx,
            pstate,
            {
                let mut v = mcx::vec_with_capacity_in(mcx, orig_targets.len())?;
                for rt in orig_targets.iter() {
                    v.push(rt.clone_in(mcx)?);
                }
                v
            },
            ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
        )?
        .into_iter()
        .collect();

    /* Prepare to assign non-conflicting resnos to resjunk attributes */
    let target_natts = {
        let rel = pstate
            .p_target_relation
            .as_deref()
            .ok_or_else(|| elog_error("UPDATE: no target relation"))?;
        rel.rd_att.attrs.len() as i32
    };
    if pstate.p_next_resno <= target_natts {
        pstate.p_next_resno = target_natts + 1;
    }

    /* Prepare non-junk columns for assignment to target table */
    let perminfoindex = pstate
        .p_target_nsitem
        .as_deref()
        .and_then(|ns| ns.p_rte.as_deref())
        .map(|r| r.perminfoindex)
        .unwrap_or(0);

    let mut orig_idx = 0usize;
    for tle in tlist.iter_mut() {
        if tle.resjunk {
            /*
             * Resjunk nodes need no additional processing, but be sure they
             * have resnos that do not match any target columns; else rewriter
             * or planner might get confused. They don't need a resname either.
             */
            tle.resno = pstate.p_next_resno as i16;
            pstate.p_next_resno += 1;
            tle.resname = None;
            continue;
        }
        if orig_idx >= orig_targets.len() {
            return Err(elog_error(
                "UPDATE target count mismatch --- internal error",
            ));
        }
        let orig_target = &orig_targets[orig_idx];

        let colname = orig_target
            .name
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string();

        let attrno = {
            let rel = pstate
                .p_target_relation
                .as_deref()
                .ok_or_else(|| elog_error("UPDATE: no target relation"))?;
            backend_parser_relation::attnameAttNum(rel, &colname, true)?
        };
        if attrno == types_core::primitive::InvalidAttrNumber as i32 {
            let relname = pstate
                .p_target_relation
                .as_deref()
                .map(|r| r.rd_rel.relname.as_str())
                .unwrap_or("")
                .to_string();
            return Err(elog_error(alloc::format!(
                "column \"{}\" of relation \"{}\" does not exist",
                colname,
                relname
            )));
        }

        backend_parser_parse_target::updateTargetListEntry(
            mcx,
            pstate,
            tle,
            colname,
            attrno,
            &orig_target.indirection,
            orig_target.location,
        )?;

        /* Mark the target column as requiring update permissions */
        if perminfoindex > 0 {
            let pi = &mut pstate.p_rteperminfos[(perminfoindex - 1) as usize];
            let updated = pi.updatedCols.take();
            pi.updatedCols = Some(backend_nodes_core::bitmapset::bms_add_member(
                mcx,
                updated,
                attrno - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            )?);
        }

        orig_idx += 1;
    }
    if orig_idx != orig_targets.len() {
        return Err(elog_error(
            "UPDATE target count mismatch --- internal error",
        ));
    }

    let mut out = mcx::vec_with_capacity_in(mcx, tlist.len())?;
    for te in tlist {
        out.push(te);
    }
    Ok(out)
}

/// Deep-copy `pstate->p_target_nsitem` into a fresh owned `ParseNamespaceItem`.
/// The C code re-adds the same target nsitem pointer to the namespace; here we
/// clone it (the boxed RTE/perminfo/names and the per-column data).
pub(crate) fn clone_target_nsitem<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
) -> PgResult<types_nodes::parsestmt::ParseNamespaceItem<'mcx>> {
    let target = pstate
        .p_target_nsitem
        .as_deref()
        .ok_or_else(|| elog_error("clone_target_nsitem: p_target_nsitem must be set"))?;

    let p_names = match target.p_names.as_deref() {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    let p_rte = match target.p_rte.as_deref() {
        Some(rte) => Some(mcx::alloc_in(mcx, rte.clone_in(mcx)?)?),
        None => None,
    };
    let p_perminfo = match target.p_perminfo.as_deref() {
        Some(pi) => Some(mcx::alloc_in(mcx, pi.clone_in(mcx)?)?),
        None => None,
    };
    let mut p_nscolumns = mcx::vec_with_capacity_in(mcx, target.p_nscolumns.len())?;
    for c in target.p_nscolumns.iter() {
        p_nscolumns.push(*c);
    }

    Ok(types_nodes::parsestmt::ParseNamespaceItem {
        p_names,
        p_rte,
        p_rtindex: target.p_rtindex,
        p_perminfo,
        p_nscolumns,
        p_rel_visible: target.p_rel_visible,
        p_cols_visible: target.p_cols_visible,
        p_lateral_only: target.p_lateral_only,
        p_lateral_ok: target.p_lateral_ok,
        p_returning_type: target.p_returning_type,
    })
}

/// `addNSItemForReturning(pstate, aliasname, returning_type)` (analyze.c:2605)
/// — add a `ParseNamespaceItem` for the OLD or NEW alias in RETURNING, copying
/// most fields from the target relation's nsitem and marking every column with
/// the given `VarReturningType`.
fn addNSItemForReturning<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    aliasname: &str,
    returning_type: VarReturningType,
) -> PgResult<()> {
    // Snapshot the data we need out of the target nsitem (copy so we can hold
    // pstate mutably for addNSItemToQuery below).
    let (colnames, mut nscolumns, rte_box, rtindex, perminfo_box) = {
        let target = pstate
            .p_target_nsitem
            .as_deref()
            .ok_or_else(|| elog_error("addNSItemForReturning: p_target_nsitem must be set"))?;

        // copy per-column data from the target relation: colnames come from the
        // target RTE's eref.
        let target_rte = target
            .p_rte
            .as_deref()
            .ok_or_else(|| elog_error("addNSItemForReturning: target nsitem has no RTE"))?;
        let eref = target_rte
            .eref
            .as_deref()
            .ok_or_else(|| elog_error("addNSItemForReturning: target RTE has no eref Alias"))?
            .clone_in(mcx)?;
        let colnames = eref.colnames;

        let mut nscolumns = mcx::vec_with_capacity_in(mcx, target.p_nscolumns.len())?;
        for c in target.p_nscolumns.iter() {
            nscolumns.push(*c);
        }

        let rte_box = match target.p_rte.as_deref() {
            Some(rte) => Some(mcx::alloc_in(mcx, rte.clone_in(mcx)?)?),
            None => None,
        };
        let perminfo_box = match target.p_perminfo.as_deref() {
            Some(pi) => Some(mcx::alloc_in(mcx, pi.clone_in(mcx)?)?),
            None => None,
        };

        (colnames, nscolumns, rte_box, target.p_rtindex, perminfo_box)
    };

    // mark all columns as returning OLD/NEW
    for nscol in nscolumns.iter_mut() {
        nscol.p_varreturningtype = returning_type;
    }

    // build the nsitem, copying most fields from the target relation
    let names = backend_nodes_core::makefuncs::make_alias(mcx, aliasname, colnames)?;
    let nsitem = types_nodes::parsestmt::ParseNamespaceItem {
        p_names: Some(mcx::alloc_in(mcx, names)?),
        p_rte: rte_box,
        p_rtindex: rtindex,
        p_perminfo: perminfo_box,
        p_nscolumns: nscolumns,
        p_returning_type: returning_type,
        // palloc0 default; overwritten by addNSItemToQuery per the add_to_* args.
        p_rel_visible: false,
        p_cols_visible: false,
        p_lateral_only: false,
        p_lateral_ok: false,
    };

    // add it to the query namespace as a table-only item
    backend_parser_relation::addNSItemToQuery(mcx, pstate, nsitem, false, true, false)
}

/// `transformReturningClause(pstate, qry, returningClause, exprKind)`
/// (analyze.c:2645) — handle a RETURNING clause in INSERT/UPDATE/DELETE/MERGE.
pub(crate) fn transformReturningClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    qry: &mut Query<'mcx>,
    returning_clause: Option<&types_nodes::rawnodes::ReturningClause<'mcx>>,
    expr_kind: ParseExprKind,
) -> PgResult<()> {
    let Some(rc) = returning_clause else {
        return Ok(()); /* nothing to do */
    };

    let save_nslen = pstate.p_namespace.len();

    // Scan RETURNING WITH(...) options for OLD/NEW alias names. A present
    // ReturningOption requires a typed node that the grammar conversion does
    // not yet produce; loudly reject rather than silently ignore.
    if !rc.options.is_empty() {
        return Err(elog_error(
            "RETURNING WITH (OLD/NEW AS ...) is not yet ported (analyze.c:2659)",
        ));
    }

    // If OLD/NEW alias names weren't explicitly specified, use "old"/"new"
    // unless masked by existing relations.
    if qry.returningOldAlias.is_none()
        && backend_parser_relation::refnameNamespaceItem(pstate, None, "old", -1, false)?.is_none()
    {
        qry.returningOldAlias = Some(mcx::PgString::from_str_in("old", mcx)?);
        addNSItemForReturning(mcx, pstate, "old", VarReturningType::VAR_RETURNING_OLD)?;
    }
    if qry.returningNewAlias.is_none()
        && backend_parser_relation::refnameNamespaceItem(pstate, None, "new", -1, false)?.is_none()
    {
        qry.returningNewAlias = Some(mcx::PgString::from_str_in("new", mcx)?);
        addNSItemForReturning(mcx, pstate, "new", VarReturningType::VAR_RETURNING_NEW)?;
    }

    // We need to assign resnos starting at one in the RETURNING list. Save and
    // restore the main tlist's value of p_next_resno.
    let save_next_resno = pstate.p_next_resno;
    pstate.p_next_resno = 1;

    // transform RETURNING expressions identically to a SELECT targetlist. The
    // grammar makes returningClause->exprs a target_list (list of ResTarget).
    let mut res_targets = mcx::vec_with_capacity_in(mcx, rc.exprs.len())?;
    for n in rc.exprs.iter() {
        match n.as_ref().as_restarget() {
            Some(rt) => res_targets.push(rt.clone_in(mcx)?),
            None => return Err(elog_error("RETURNING list item is not a ResTarget")),
        }
    }
    let mut returning_list =
        backend_parser_parse_target::transformTargetList(mcx, pstate, res_targets, expr_kind)?;

    // Complain if the nonempty tlist expanded to nothing (possible for a
    // star-expansion of a zero-column table).
    if returning_list.is_empty() {
        return Err(elog_error("RETURNING must have at least one column"));
    }

    // mark column origins
    backend_parser_parse_target::markTargetListOrigins(mcx, pstate, &mut returning_list)?;

    // resolve any still-unresolved output columns as being type text
    if pstate.p_resolve_unknowns {
        backend_parser_parse_target::resolveTargetListUnknowns(mcx, pstate, &mut returning_list)?;
    }

    qry.returningList = returning_list;

    // restore state
    pstate.p_namespace.truncate(save_nslen);
    pstate.p_next_resno = save_next_resno;

    Ok(())
}

/// Set the target NSItem's `p_lateral_only`/`p_lateral_ok` flags (the C code
/// mutates these through the `nsitem = pstate->p_target_nsitem` alias).
fn set_target_lateral(pstate: &mut ParseState<'_>, lateral_only: bool, lateral_ok: bool) {
    if let Some(ns) = pstate.p_target_nsitem.as_mut() {
        ns.p_lateral_only = lateral_only;
        ns.p_lateral_ok = lateral_ok;
    }
}
