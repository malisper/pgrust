//! F3a of `parse_clause.c`: WINDOW definitions and ON CONFLICT clause
//! transformation, ported 1:1 over the split raw-[`Node`]/typed-[`Expr`] model.
//!
//!   * [`transformWindowDefinitions`] / `findWindowClause` / `transformFrameOffset`.
//!   * [`transformOnConflictArbiter`] / `resolve_unique_index_expr`.

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgString};

use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WINDOWING_ERROR, ERROR,
};
use backend_utils_error::ereport;

use types_acl::acl::ACL_SELECT;
use types_tuple::heaptuple::{INT8OID, UNKNOWNOID};

use types_nodes::nodes::{ntag, Node, NodePtr, ONCONFLICT_UPDATE};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{Expr, InferenceElem, TargetEntry};
use types_nodes::ddlnodes::IndexElem;
use types_nodes::rawnodes::{
    ColumnRef, InferClause, OnConflictClause, SortByDir, SortByNulls, SortGroupClause,
    WindowClause, WindowDef,
};
use types_nodes::value::StringNode;
use types_nodes::nodewindowagg::{
    FRAMEOPTION_DEFAULTS, FRAMEOPTION_END_OFFSET, FRAMEOPTION_GROUPS, FRAMEOPTION_RANGE,
    FRAMEOPTION_ROWS, FRAMEOPTION_START_OFFSET,
};

use ParseExprKind::{
    EXPR_KIND_INDEX_EXPRESSION, EXPR_KIND_INDEX_PREDICATE, EXPR_KIND_WINDOW_FRAME_GROUPS,
    EXPR_KIND_WINDOW_FRAME_RANGE, EXPR_KIND_WINDOW_FRAME_ROWS, EXPR_KIND_WINDOW_ORDER,
    EXPR_KIND_WINDOW_PARTITION,
};

use types_parsenodes::CoercionContext::COERCION_IMPLICIT;

use backend_nodes_core::nodefuncs::{expr_collation, expr_location, expr_type};

use backend_optimizer_util_vars::tlist::get_sortgroupclause_expr;
use backend_parser_parse_expr::transformExpr;
use backend_parser_coerce::{can_coerce_type, coerce_to_specific_type};

use backend_utils_adt_format_type as format_type;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;

use crate::{errpos, str_val, transformGroupClause, transformSortClause};

// BTREE_AM_OID (catalog/pg_am_d.h).
const BTREE_AM_OID: Oid = 403;
// BTINRANGE_PROC (access/nbtree.h) — support function 3, the in_range function.
const BTINRANGE_PROC: i16 = 3;

// ===========================================================================
// transformWindowDefinitions — parse_clause.c:2764
// ===========================================================================

/// Transform window definitions to use winref references.
///
/// Returns a list of [`WindowClause`] nodes, growing `targetlist` as needed.
pub fn transformWindowDefinitions<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    windowdefs: &[WindowDef<'mcx>],
    targetlist: &mut Vec<TargetEntry<'mcx>>,
) -> PgResult<Vec<WindowClause<'mcx>>> {
    let mut result: Vec<WindowClause<'mcx>> = Vec::new();
    let mut winref: u32 = 0;

    for windef in windowdefs.iter() {
        let mut rangeopfamily = InvalidOid;
        let mut rangeopcintype = InvalidOid;

        winref += 1;

        /*
         * Check for duplicate window names.
         */
        if let Some(name) = windef.name.as_ref() {
            if findWindowClause(&result, name.as_str()).is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WINDOWING_ERROR)
                    .errmsg(alloc::format!("window \"{}\" is already defined", name.as_str()))
                    .errposition(errpos(pstate, windef.location))
                    .into_error());
            }
        }

        /*
         * If it references a previous window, look that up.
         */
        let refwc_idx: Option<usize> = if let Some(refname) = windef.refname.as_ref() {
            match findWindowClause(&result, refname.as_str()) {
                Some(idx) => Some(idx),
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(alloc::format!(
                            "window \"{}\" does not exist",
                            refname.as_str()
                        ))
                        .errposition(errpos(pstate, windef.location))
                        .into_error());
                }
            }
        } else {
            None
        };

        /*
         * Transform PARTITION and ORDER specs, if any.  These are treated
         * almost exactly like top-level GROUP BY and ORDER BY clauses,
         * including the special handling of nondefault operator semantics.
         */
        let order_sortby = node_vec_as_sortby(mcx, &windef.orderClause)?;
        let orderClause = transformSortClause(
            mcx,
            pstate,
            &order_sortby,
            targetlist,
            EXPR_KIND_WINDOW_ORDER,
            true, /* force SQL99 rules */
        )?;
        /* transformGroupClause is called with groupingSets == NULL here. */
        let (partitionClause, _gsets) = transformGroupClause(
            mcx,
            pstate,
            &windef.partitionClause,
            targetlist,
            &orderClause,
            EXPR_KIND_WINDOW_PARTITION,
            true, /* force SQL99 rules */
        )?;

        /*
         * And prepare the new WindowClause.
         */
        let mut wc = WindowClause {
            name: copy_opt_pgstr(&windef.name, mcx)?,
            refname: copy_opt_pgstr(&windef.refname, mcx)?,
            partitionClause: empty_node_vec(mcx)?,
            orderClause: empty_node_vec(mcx)?,
            frameOptions: 0,
            startOffset: None,
            endOffset: None,
            startInRangeFunc: InvalidOid,
            endInRangeFunc: InvalidOid,
            inRangeColl: InvalidOid,
            inRangeAsc: false,
            inRangeNullsFirst: false,
            winref: 0,
            copiedOrder: false,
        };

        /*
         * Per spec, a windowdef that references a previous one copies the
         * previous partition clause (and mustn't specify its own).  It can
         * specify its own ordering clause, but only if the previous one had
         * none.  It always specifies its own frame clause, and the previous
         * one must not have a frame clause.  See SQL:2008 7.11 syntax rule 10
         * and general rule 1.
         */
        if let Some(refidx) = refwc_idx {
            if !partitionClause.is_empty() {
                let refname = windef.refname.as_ref().map(|s| s.as_str()).unwrap_or("");
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WINDOWING_ERROR)
                    .errmsg(alloc::format!(
                        "cannot override PARTITION BY clause of window \"{}\"",
                        refname
                    ))
                    .errposition(errpos(pstate, windef.location))
                    .into_error());
            }
            wc.partitionClause = copy_node_ptr_vec(mcx, &result[refidx].partitionClause)?;
        } else {
            wc.partitionClause = sortgroupclauses_to_node_vec(mcx, &partitionClause)?;
        }

        if let Some(refidx) = refwc_idx {
            if !orderClause.is_empty() && !result[refidx].orderClause.is_empty() {
                let refname = windef.refname.as_ref().map(|s| s.as_str()).unwrap_or("");
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WINDOWING_ERROR)
                    .errmsg(alloc::format!(
                        "cannot override ORDER BY clause of window \"{}\"",
                        refname
                    ))
                    .errposition(errpos(pstate, windef.location))
                    .into_error());
            }
            if !orderClause.is_empty() {
                wc.orderClause = sortgroupclauses_to_node_vec(mcx, &orderClause)?;
                wc.copiedOrder = false;
            } else {
                wc.orderClause = copy_node_ptr_vec(mcx, &result[refidx].orderClause)?;
                wc.copiedOrder = true;
            }
        } else {
            wc.orderClause = sortgroupclauses_to_node_vec(mcx, &orderClause)?;
            wc.copiedOrder = false;
        }

        if let Some(refidx) = refwc_idx {
            if result[refidx].frameOptions != FRAMEOPTION_DEFAULTS {
                /*
                 * Use this message if this is a WINDOW clause, or if it's an
                 * OVER clause that includes ORDER BY or framing clauses.  (We
                 * already rejected PARTITION BY above.)
                 */
                let refname = windef.refname.as_ref().map(|s| s.as_str()).unwrap_or("");
                if windef.name.is_some()
                    || !orderClause.is_empty()
                    || windef.frameOptions != FRAMEOPTION_DEFAULTS
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_WINDOWING_ERROR)
                        .errmsg(alloc::format!(
                            "cannot copy window \"{}\" because it has a frame clause",
                            refname
                        ))
                        .errposition(errpos(pstate, windef.location))
                        .into_error());
                }
                /* Else this clause is just OVER (foo), so say this: */
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WINDOWING_ERROR)
                    .errmsg(alloc::format!(
                        "cannot copy window \"{}\" because it has a frame clause",
                        refname
                    ))
                    .errhint("Omit the parentheses in this OVER clause.")
                    .errposition(errpos(pstate, windef.location))
                    .into_error());
            }
        }
        wc.frameOptions = windef.frameOptions;

        /*
         * RANGE offset PRECEDING/FOLLOWING requires exactly one ORDER BY
         * column; check that and get its sort opfamily info.
         */
        if (wc.frameOptions & FRAMEOPTION_RANGE) != 0
            && (wc.frameOptions & (FRAMEOPTION_START_OFFSET | FRAMEOPTION_END_OFFSET)) != 0
        {
            if orderClause.len() != 1 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WINDOWING_ERROR)
                    .errmsg(
                        "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
                    )
                    .errposition(errpos(pstate, windef.location))
                    .into_error());
            }
            let sortcl: &SortGroupClause = &orderClause[0];
            let sortkey = get_sortgroupclause_expr(sortcl, targetlist)?;
            /* Find the sort operator in pg_amop */
            match lsyscache::get_ordering_op_properties::call(sortcl.sortop)? {
                Some((opfamily, opcintype, _cmptype)) => {
                    rangeopfamily = opfamily;
                    rangeopcintype = opcintype;
                }
                None => {
                    return Err(crate::elog_error(alloc::format!(
                        "operator {} is not a valid ordering operator",
                        sortcl.sortop
                    )));
                }
            }
            /* Record properties of sort ordering */
            wc.inRangeColl = expr_collation(sortkey.as_ref())?;
            wc.inRangeAsc = !sortcl.reverse_sort;
            wc.inRangeNullsFirst = sortcl.nulls_first;
        }

        /* Per spec, GROUPS mode requires an ORDER BY clause */
        if (wc.frameOptions & FRAMEOPTION_GROUPS) != 0 && orderClause.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WINDOWING_ERROR)
                .errmsg("GROUPS mode requires an ORDER BY clause")
                .errposition(errpos(pstate, windef.location))
                .into_error());
        }

        /* Process frame offset expressions */
        let frame_options = wc.frameOptions;
        let (start_off, start_in_range) = transformFrameOffset(
            mcx,
            pstate,
            frame_options,
            rangeopfamily,
            rangeopcintype,
            windef.startOffset.as_deref(),
        )?;
        wc.startOffset = match start_off {
            Some(n) => Some(alloc_in(mcx, n)?),
            None => None,
        };
        wc.startInRangeFunc = start_in_range;

        let (end_off, end_in_range) = transformFrameOffset(
            mcx,
            pstate,
            frame_options,
            rangeopfamily,
            rangeopcintype,
            windef.endOffset.as_deref(),
        )?;
        wc.endOffset = match end_off {
            Some(n) => Some(alloc_in(mcx, n)?),
            None => None,
        };
        wc.endInRangeFunc = end_in_range;

        wc.winref = winref;

        result.push(wc);
    }

    Ok(result)
}

// ===========================================================================
// findWindowClause — parse_clause.c:3661
// ===========================================================================

/// Find the named WindowClause in the list, or return its index.
fn findWindowClause(wclist: &[WindowClause<'_>], name: &str) -> Option<usize> {
    for (idx, wc) in wclist.iter().enumerate() {
        if let Some(wcname) = wc.name.as_ref() {
            if wcname.as_str() == name {
                return Some(idx);
            }
        }
    }
    None
}

// ===========================================================================
// transformFrameOffset — parse_clause.c:3688
// ===========================================================================

/// Process a window frame offset expression.
///
/// Returns `(offset_node, inRangeFunc)`.  In RANGE mode, `rangeopfamily` is the
/// sort opfamily for the input ORDER BY column, and `rangeopcintype` is the
/// input data type the sort operator is registered with.
fn transformFrameOffset<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    frameOptions: i32,
    rangeopfamily: Oid,
    rangeopcintype: Oid,
    clause: Option<&Node<'mcx>>,
) -> PgResult<(Option<Node<'mcx>>, Oid)> {
    let constructName: &str;
    let node: Expr;
    let mut inRangeFunc = InvalidOid; /* default result */

    /* Quick exit if no offset expression */
    let Some(clause) = clause else {
        return Ok((None, inRangeFunc));
    };

    if (frameOptions & FRAMEOPTION_ROWS) != 0 {
        /* Transform the raw expression tree */
        let n = transformExpr(pstate, Some(clause.clone_in(mcx)?), EXPR_KIND_WINDOW_FRAME_ROWS)?
            .ok_or_else(|| crate::elog_error("transformFrameOffset: transformExpr returned NULL"))?;
        /* Like LIMIT clause, simply coerce to int8 */
        constructName = "ROWS";
        node = coerce_to_specific_type(mcx, Some(pstate), n, INT8OID, constructName)?;
    } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
        /*
         * We must look up the in_range support function that's to be used,
         * possibly choosing one of several, and coerce the "offset" value to
         * the appropriate input type.
         */
        let mut nmatches = 0;
        let mut nfuncs = 0;
        let mut selectedType = InvalidOid;
        let mut selectedFunc = InvalidOid;

        /* Transform the raw expression tree */
        let n = transformExpr(pstate, Some(clause.clone_in(mcx)?), EXPR_KIND_WINDOW_FRAME_RANGE)?
            .ok_or_else(|| crate::elog_error("transformFrameOffset: transformExpr returned NULL"))?;
        let nodeType = expr_type(Some(&n))?;

        /*
         * If there are multiple candidates, we'll prefer the one that exactly
         * matches nodeType; or if nodeType is as yet unknown, prefer the one
         * that exactly matches the sort column type.
         */
        let preferredType = if nodeType != UNKNOWNOID {
            nodeType
        } else {
            rangeopcintype
        };

        /* Find the in_range support functions applicable to this case */
        let proclist = syscache::search_amproc_list2::call(mcx, rangeopfamily, rangeopcintype)?;
        for procform in proclist.iter() {
            /* The search will find all support proc types; ignore others */
            if procform.amprocnum != BTINRANGE_PROC {
                continue;
            }
            nfuncs += 1;

            /* Ignore function if given value can't be coerced to that type */
            if !can_coerce_type(1, &[nodeType], &[procform.amprocrighttype], COERCION_IMPLICIT)? {
                continue;
            }
            nmatches += 1;

            /* Remember preferred match, or any match if didn't find that */
            if selectedType != preferredType {
                selectedType = procform.amprocrighttype;
                selectedFunc = procform.amproc;
            }
        }

        /*
         * Throw error if needed.  Distinguish "no support at all" from "you
         * didn't match any available offset type".
         */
        if nfuncs == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(alloc::format!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
                    format_type::format_type_be_str(rangeopcintype)?
                ))
                .errposition(errpos(pstate, expr_location(Some(&n))?))
                .into_error());
        }
        if nmatches == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(alloc::format!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {} and offset type {}",
                    format_type::format_type_be_str(rangeopcintype)?,
                    format_type::format_type_be_str(nodeType)?
                ))
                .errhint("Cast the offset value to an appropriate type.")
                .errposition(errpos(pstate, expr_location(Some(&n))?))
                .into_error());
        }
        if nmatches != 1 && selectedType != preferredType {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(alloc::format!(
                    "RANGE with offset PRECEDING/FOLLOWING has multiple interpretations for column type {} and offset type {}",
                    format_type::format_type_be_str(rangeopcintype)?,
                    format_type::format_type_be_str(nodeType)?
                ))
                .errhint("Cast the offset value to the exact intended type.")
                .errposition(errpos(pstate, expr_location(Some(&n))?))
                .into_error());
        }

        /* OK, coerce the offset to the right type */
        constructName = "RANGE";
        node = coerce_to_specific_type(mcx, Some(pstate), n, selectedType, constructName)?;
        inRangeFunc = selectedFunc;
    } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
        /* Transform the raw expression tree */
        let n = transformExpr(pstate, Some(clause.clone_in(mcx)?), EXPR_KIND_WINDOW_FRAME_GROUPS)?
            .ok_or_else(|| crate::elog_error("transformFrameOffset: transformExpr returned NULL"))?;
        /* Like LIMIT clause, simply coerce to int8 */
        constructName = "GROUPS";
        node = coerce_to_specific_type(mcx, Some(pstate), n, INT8OID, constructName)?;
    } else {
        /* C: Assert(false); node = NULL; */
        return Err(crate::elog_error(
            "transformFrameOffset: frame offset with no ROWS/RANGE/GROUPS option",
        ));
    }

    /* Disallow variables in frame offsets */
    crate::checkExprIsVarFree(pstate, &node, constructName)?;

    Ok((Some(Node::mk_expr(mcx, node)?), inRangeFunc))
}

// ===========================================================================
// resolve_unique_index_expr — parse_clause.c:3200
// ===========================================================================

/// Infer a unique index from a list of indexElems, for ON CONFLICT clause.
///
/// Builds a list of [`InferenceElem`] nodes (returned as a `Vec<Expr>`).
fn resolve_unique_index_expr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    infer: &InferClause<'mcx>,
) -> PgResult<Vec<Expr>> {
    let mut result: Vec<Expr> = Vec::new();

    for ielem_node in infer.indexElems.iter() {
        let ielem: &IndexElem = match ielem_node.node_tag() {
            ntag::T_IndexElem => ielem_node.expect_indexelem(),
            _ => {
                return Err(crate::elog_error(
                    "resolve_unique_index_expr: indexElems member is not an IndexElem",
                ))
            }
        };

        /*
         * Make no attempt to match ASC or DESC ordering or NULLS FIRST/NULLS
         * LAST ordering, since those are not significant for inference
         * purposes.  Actively reject this as wrong-headed.
         */
        if ielem.ordering != SortByDir::SORTBY_DEFAULT {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg("ASC/DESC is not allowed in ON CONFLICT clause")
                .errposition(errpos(pstate, infer.location))
                .into_error());
        }
        if ielem.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg("NULLS FIRST/LAST is not allowed in ON CONFLICT clause")
                .errposition(errpos(pstate, infer.location))
                .into_error());
        }

        /*
         * If the grammar didn't build a raw expression (plain column
         * reference), create one directly and transform it.  Otherwise parse
         * the supplied raw expression.
         */
        let parse: Node<'mcx> = match ielem.expr.as_deref() {
            None => {
                /* Simple index attribute */
                let name = ielem.name.as_ref().map(|s| s.as_str()).unwrap_or("");
                let mut fields = empty_node_vec(mcx)?;
                let field = alloc_in(
                    mcx,
                    Node::mk_string(mcx, StringNode {
                        sval: PgString::from_str_in(name, mcx)?,
                    })?,
                )?;
                fields.try_reserve(1).map_err(|_| mcx.oom(0))?;
                fields.push(field);
                Node::mk_column_ref(mcx, ColumnRef {
                    fields,
                    location: infer.location,
                })?
            }
            Some(expr) => expr.clone_in(mcx)?,
        };

        /*
         * transformExpr() will reject subqueries, aggregates, window
         * functions, and SRFs, based on EXPR_KIND_INDEX_EXPRESSION.
         */
        let pinfer_expr = transformExpr(pstate, Some(parse), EXPR_KIND_INDEX_EXPRESSION)?;

        /* Perform lookup of collation and operator class as required */
        let infercollid = if ielem.collation.is_empty() {
            InvalidOid
        } else {
            let loc = expr_location(pinfer_expr.as_ref())?;
            let collnames = node_vec_to_collnames(&ielem.collation)?;
            backend_parser_parse_type::LookupCollation(mcx, Some(pstate), &collnames, loc)?
        };

        let inferopclass = if ielem.opclass.is_empty() {
            InvalidOid
        } else {
            let opclassname = node_vec_to_opclass_names(&ielem.opclass);
            backend_commands_opclasscmds::get_opclass_oid(mcx, BTREE_AM_OID, &opclassname, false)?
        };

        result.push(Expr::InferenceElem(InferenceElem {
            expr: pinfer_expr.map(alloc::boxed::Box::new),
            infercollid,
            inferopclass,
        }));
    }

    Ok(result)
}

// ===========================================================================
// transformOnConflictArbiter — parse_clause.c:3296
// ===========================================================================

/// Transform arbiter expressions in an ON CONFLICT clause.
///
/// Returns `(arbiterExpr, arbiterWhere, constraint)`.
pub fn transformOnConflictArbiter<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    onConflictClause: &OnConflictClause<'mcx>,
) -> PgResult<(Vec<Expr>, Option<Expr>, Oid)> {
    let infer = onConflictClause.infer.as_deref();

    let mut arbiterExpr: Vec<Expr> = Vec::new();
    let mut arbiterWhere: Option<Expr> = None;
    let mut constraint: Oid = InvalidOid;

    if onConflictClause.action == ONCONFLICT_UPDATE && infer.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("ON CONFLICT DO UPDATE requires inference specification or constraint name")
            .errhint("For example, ON CONFLICT (column_name).")
            .errposition(errpos(pstate, onConflictClause.location))
            .into_error());
    }

    let target_rel = pstate
        .p_target_relation
        .as_ref()
        .ok_or_else(|| crate::elog_error("transformOnConflictArbiter: no target relation"))?;

    /*
     * To simplify certain aspects of its design, speculative insertion into
     * system catalogs is disallowed.
     */
    if backend_catalog_catalog::IsCatalogRelation(target_rel) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("ON CONFLICT is not supported with system catalog tables")
            .errposition(errpos(pstate, onConflictClause.location))
            .into_error());
    }

    /* Same applies to table used by logical decoding as catalog table */
    if RelationIsUsedAsCatalogTable(target_rel) {
        let relname = String::from(target_rel.name());
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "ON CONFLICT is not supported on table \"{}\" used as a catalog table",
                relname
            ))
            .errposition(errpos(pstate, onConflictClause.location))
            .into_error());
    }

    /* ON CONFLICT DO NOTHING does not require an inference clause */
    if let Some(infer) = infer {
        if !infer.indexElems.is_empty() {
            arbiterExpr = resolve_unique_index_expr(mcx, pstate, infer)?;
        }

        /*
         * Handling inference WHERE clause (for partial unique index
         * inference).
         */
        if let Some(where_clause) = infer.whereClause.as_deref() {
            arbiterWhere =
                transformExpr(pstate, Some(where_clause.clone_in(mcx)?), EXPR_KIND_INDEX_PREDICATE)?;
        }

        /*
         * If the arbiter is specified by constraint name, get the constraint
         * OID and mark the constrained columns as requiring SELECT privilege.
         */
        if let Some(conname) = infer.conname.as_ref() {
            let relid = pstate
                .p_target_relation
                .as_ref()
                .map(|r| r.rd_id)
                .unwrap_or(InvalidOid);

            let (constraint_oid, conattnos) =
                backend_catalog_pg_constraint::get_relation_constraint_attnos(
                    mcx,
                    relid,
                    conname.as_str(),
                    false,
                )?;
            constraint = constraint_oid;

            let perminfo = pstate
                .p_target_nsitem
                .as_deref_mut()
                .and_then(|nsi| nsi.p_perminfo.as_deref_mut())
                .ok_or_else(|| {
                    crate::elog_error("transformOnConflictArbiter: target nsitem has no perminfo")
                })?;

            /* Make sure the rel as a whole is marked for SELECT access */
            perminfo.requiredPerms |= ACL_SELECT;
            /* Mark the constrained columns as requiring SELECT access */
            let merged = backend_nodes_core::bitmapset::bms_add_members(
                mcx,
                perminfo.selectedCols.take(),
                conattnos.as_deref(),
            )?;
            perminfo.selectedCols = merged;
        }
    }

    Ok((arbiterExpr, arbiterWhere, constraint))
}

/// `RelationIsUsedAsCatalogTable(relation)` (utils/rel.h):
/// `(relation)->rd_options && ((StdRdOptions *) (relation)->rd_options)->user_catalog_table`.
fn RelationIsUsedAsCatalogTable(relation: &types_rel::RelationData<'_>) -> bool {
    match relation.rd_options.as_ref().and_then(|o| o.std()) {
        Some(opts) => opts.user_catalog_table,
        None => false,
    }
}

// ===========================================================================
// Internal helpers
// ===========================================================================

/// Deep-copy an `Option<PgString>` into `mcx`.
fn copy_opt_pgstr<'mcx>(
    s: &Option<PgString<'_>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(s.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// An empty `PgVec<NodePtr>` (the C `NIL`).
fn empty_node_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 0)
}

/// `copyObject` over a `List *` of nodes (deep-copy each cell into `mcx`).
fn copy_node_ptr_vec<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mcx::PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, list.len())?;
    for n in list.iter() {
        let cell = alloc_in(mcx, n.clone_in(mcx)?)?;
        v.try_reserve(1).map_err(|_| mcx.oom(0))?;
        v.push(cell);
    }
    Ok(v)
}

/// Wrap a list of transformed [`SortGroupClause`]s as a `List *` of
/// `T_SortGroupClause` nodes (the representation `WindowClause` stores).
fn sortgroupclauses_to_node_vec<'mcx>(
    mcx: Mcx<'mcx>,
    clauses: &[SortGroupClause],
) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, clauses.len())?;
    for cl in clauses.iter() {
        let cell = alloc_in(mcx, Node::mk_sort_group_clause(mcx, *cl)?)?;
        v.try_reserve(1).map_err(|_| mcx.oom(0))?;
        v.push(cell);
    }
    Ok(v)
}

/// Deep-copy a `PgVec<NodePtr>` of `T_SortBy` cells into a `Vec<SortBy>` for
/// `transformSortClause` (the grammar produces a `List *` of `SortBy` nodes).
fn node_vec_as_sortby<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mcx::PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<Vec<types_nodes::rawnodes::SortBy<'mcx>>> {
    let mut out = Vec::with_capacity(list.len());
    for n in list.iter() {
        match n.node_tag() {
            ntag::T_SortBy => out.push(n.expect_sortby().clone_in(mcx)?),
            _ => {
                return Err(crate::elog_error(
                    "transformWindowDefinitions: orderClause member is not a SortBy",
                ))
            }
        }
    }
    Ok(out)
}

/// Bridge a collation-name `List *` of `String` value nodes from the
/// raw-grammar [`Node`] vocabulary to the parser's own `types_parsenodes::Node`
/// vocabulary that `LookupCollation` consumes (a collation name list only ever
/// contains `String` value nodes).
fn node_vec_to_collnames(
    list: &mcx::PgVec<'_, NodePtr<'_>>,
) -> PgResult<Vec<types_parsenodes::Node>> {
    let mut out = Vec::with_capacity(list.len());
    for n in list.iter() {
        match n.node_tag() {
            ntag::T_String => out.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(String::from(n.expect_string().sval.as_str())),
                },
            )),
            _ => {
                return Err(crate::elog_error(
                    "resolve_unique_index_expr: collation name element is not a String value node",
                ))
            }
        }
    }
    Ok(out)
}

/// Convert an opclass name `List *` of `String` value nodes into the
/// `Vec<types_opclass::StringNode>` `get_opclass_oid` expects.
fn node_vec_to_opclass_names(
    list: &mcx::PgVec<'_, NodePtr<'_>>,
) -> Vec<types_opclass::StringNode> {
    let mut out = Vec::with_capacity(list.len());
    for n in list.iter() {
        if let Some(s) = str_val(n) {
            out.push(types_opclass::StringNode {
                sval: Some(String::from(s)),
            });
        }
    }
    out
}

