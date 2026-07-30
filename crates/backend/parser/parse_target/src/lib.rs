#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use nodes_core::node_funcs::{expr_collation, expr_typmod};
use parse_expr::{expr_location, expr_type, transformExpr};
use parser_small1::{ParseExprKind, ParseNamespaceItem, ParseState};
use types_core::catalog::{RECORDOID, TEXTOID, UNKNOWNOID};
use types_core::AttrNumber;
use types_tuple::tupdesc::TupleDescData;
use types_error::PgResult;
use types_nodes::rawnodes::{A_Expr_Kind, ColumnRef};
use types_nodes::{CoercionForm, Node, NodeList, NodeTag, RTEKind, TargetEntry};

pub fn init_seams() {
    parse_func_seams::expandRecordVariable::set(expandRecordVariable);
    parse_func_seams::transformExpressionList::set(transformExpressionList);
}

pub fn transformTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
    exprKind: ParseExprKind,
) -> PgResult<NodeList<'mcx>> {
    let mut p_target = NodeList::nil();
    debug_assert!(pstate.p_multiassign_exprs.is_nil());
    let expand_star = exprKind != ParseExprKind::EXPR_KIND_UPDATE_SOURCE;

    for o_target in targetlist {
        let res = o_target
            .as_res_target()
            .unwrap_or_else(|| panic!("targetlist element is not a ResTarget: {o_target:?}"));
        let val = res.val.expect("ResTarget.val is never NULL in a raw targetlist");

        if expand_star {
            if let Some(cref) = val.as_column_ref() {
                if cref.fields.last().is_some_and(|f| f.node_tag() == NodeTag::T_A_Star) {
                    p_target.concat(mcx, &ExpandColumnRefStar(mcx, pstate, cref, true)?)?;
                    continue;
                }
            } else if let Some(ind) = val.as_a_indirection() {
                // C expands only when the LAST indirection item is A_Star.
                if ind.indirection.last().is_some_and(|n| n.node_tag() == NodeTag::T_A_Star) {
                    p_target.concat(
                        mcx,
                        &ExpandIndirectionStar(mcx, pstate, ind, true, exprKind)?,
                    )?;
                    continue;
                }
            }
        }

        let te = transformTargetEntry(mcx, pstate, val, None, exprKind, res.name, false)?;
        p_target.lappend(mcx, te)?;
    }

    if !pstate.p_multiassign_exprs.is_nil() {
        debug_assert!(exprKind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE);
        p_target.concat(mcx, &pstate.p_multiassign_exprs)?;
        pstate.p_multiassign_exprs = NodeList::nil();
    }

    Ok(p_target)
}

pub fn transformTargetEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    expr: Option<Node<'mcx>>,
    exprKind: ParseExprKind,
    colname: Option<&'mcx str>,
    resjunk: bool,
) -> PgResult<Node<'mcx>> {
    let expr = match expr {
        Some(e) => e,
        None => {
            if exprKind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE
                && node.node_tag() == NodeTag::T_SetToDefault
            {
                node
            } else {
                transformExpr(mcx, pstate, node, exprKind)?
            }
        }
    };

    let colname = match colname {
        // C's transformSubLink scribbles the transformed Query into the raw
        // SubLink in place; the transformed expr carries that state here.
        None if !resjunk => Some(FigureColname(if node.node_tag() == NodeTag::T_SubLink {
            expr
        } else {
            node
        })),
        other => other,
    };

    let resno = pstate.p_next_resno as AttrNumber;
    pstate.p_next_resno += 1;
    Node::mk_target_entry(mcx, expr, resno, colname, resjunk)
}

/// C `transformExpressionList` (parse_target.c).
pub fn transformExpressionList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    exprlist: &NodeList<'mcx>,
    exprKind: ParseExprKind,
    allowDefault: bool,
) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    for e in exprlist {
        if let Some(cref) = e.as_column_ref() {
            if cref.fields.last().is_some_and(|f| f.node_tag() == NodeTag::T_A_Star) {
                result.concat(mcx, &ExpandColumnRefStar(mcx, pstate, cref, false)?)?;
                continue;
            }
        } else if let Some(ind) = e.as_a_indirection() {
            if ind.indirection.last().is_some_and(|n| n.node_tag() == NodeTag::T_A_Star) {
                result.concat(mcx, &ExpandIndirectionStar(mcx, pstate, ind, false, exprKind)?)?;
                continue;
            }
        }
        let e = if allowDefault && e.node_tag() == NodeTag::T_SetToDefault {
            e
        } else {
            transformExpr(mcx, pstate, e, exprKind)?
        };
        result.lappend(mcx, e)?;
    }
    debug_assert!(pstate.p_multiassign_exprs.is_nil());
    Ok(result)
}

/// C `transformAssignedExpr` (parse_target.c); the whole-row Var and
/// indirection arms are loud.
#[allow(clippy::too_many_arguments)]
pub fn transformAssignedExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    exprKind: ParseExprKind,
    colname: Option<&str>,
    attrno: i32,
    indirection: &NodeList<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    debug_assert!(exprKind != ParseExprKind::EXPR_KIND_NONE);
    // C sets p_expr_kind for the whole call (the subscript transforms below
    // read it outside any transformExpr scope).
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = exprKind;
    let r = transformAssignedExprInternal(
        mcx, pstate, expr, exprKind, colname, attrno, indirection, location,
    );
    pstate.p_expr_kind = sv_expr_kind;
    r
}

#[allow(clippy::too_many_arguments)]
fn transformAssignedExprInternal<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    exprKind: ParseExprKind,
    colname: Option<&str>,
    attrno: i32,
    indirection: &NodeList<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    let att = {
        let rel = pstate
            .p_target_relation
            .as_ref()
            .expect("transformAssignedExpr with no target relation");
        if attrno <= 0 {
            return Err(cannot_assign_to_system_column(pstate, colname, location));
        }
        rel.rd_att.attr((attrno - 1) as usize)
    };
    let (attrtype, attrtypmod, attrcollation) = (att.atttypid, att.atttypmod, att.attcollation);

    // Stamp the placeholder with the column's type so exprType is usable;
    // the rewriter substitutes the real default (rewriteTargetListIU).
    let expr = if let Some(d) = expr.as_set_to_default() {
        // No default exists for a portion of a column.
        if !indirection.is_nil() {
            return Err(default_with_indirection(pstate, indirection, location));
        }
        Node::mk(
            mcx,
            types_nodes::primnodes::SetToDefault {
                typeId: attrtype,
                typeMod: attrtypmod,
                collation: att.attcollation,
                location: d.location,
            },
        )?
    } else {
        expr
    };
    if !indirection.is_nil() {
        let col_var = if pstate.p_is_insert {
            Node::mk_const(mcx, attrtype, attrtypmod, attrcollation, -2, ::datum::Datum::null(), true, false)?
        } else {
            let rtindex = pstate
                .p_target_nsitem
                .expect("UPDATE with no target nsitem")
                .p_rtindex;
            Node::mk_var(mcx, rtindex, attrno as i16, attrtype, attrtypmod, attrcollation, 0)?
        };
        return transformAssignmentIndirection(
            mcx,
            pstate,
            Some(col_var),
            colname.unwrap_or("?"),
            false,
            attrtype,
            attrtypmod,
            attrcollation,
            indirection,
            0,
            expr,
            coerce::COERCION_ASSIGNMENT,
            location,
        );
    }

    let type_id = expr_type(expr);
    let coerced = coerce::coerce_to_target_type(
        mcx,
        pstate,
        expr,
        type_id,
        attrtype,
        attrtypmod,
        coerce::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    match coerced {
        Some(e) => Ok(e),
        None => Err(column_type_mismatch(
            pstate,
            colname.unwrap_or("?"),
            attrtype,
            type_id,
            expr_location(expr),
        )),
    }
}

/// C `updateTargetListEntry` (parse_target.c).
pub fn updateTargetListEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    tle_node: Node<'mcx>,
    colname: &'mcx str,
    attrno: i32,
    indirection: &NodeList<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<()> {
    let expr = tle_node.as_target_entry().expect("TargetEntry").expr;
    let new_expr = transformAssignedExpr(
        mcx,
        pstate,
        expr,
        ParseExprKind::EXPR_KIND_UPDATE_TARGET,
        Some(colname),
        attrno,
        indirection,
        location,
    )?;
    // SAFETY: parser-owned tlist; the `expr` probe above is dead here.
    unsafe {
        tle_node.with_mut::<TargetEntry, _>(|t| {
            t.expr = new_expr;
            t.resno = attrno as AttrNumber;
            t.resname = Some(colname);
        })
    }
    .expect("TargetEntry");
    Ok(())
}

/// C `checkInsertTargets` (parse_target.c); returns (icolumns, attrnos).
pub fn checkInsertTargets<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    cols: &NodeList<'mcx>,
) -> PgResult<(NodeList<'mcx>, mcx::PgVec<'mcx, i32>)> {
    let rel = pstate
        .p_target_relation
        .as_ref()
        .expect("checkInsertTargets with no target relation");
    let mut attrnos: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);

    if cols.is_nil() {
        let mut out = NodeList::nil();
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            if att.attisdropped {
                continue;
            }
            let name = core::str::from_utf8(att.attname.name_str()).expect("attname is UTF-8");
            let col =
                Node::mk_res_target(mcx, Some(str_in(mcx, name)?), NodeList::nil(), None, -1)?;
            out.lappend(mcx, col)?;
            attrnos.push(i as i32 + 1);
        }
        Ok((out, attrnos))
    } else {
        let mut wholecols = types_nodes::Bitmapset::empty();
        let mut partialcols = types_nodes::Bitmapset::empty();
        for col_node in cols {
            let col = col_node.as_res_target().expect("insert cols are ResTargets");
            let name = col.name.expect("insert_column_item always has a name");
            let attrno = parse_relation::attnameAttNum(rel, name, false);
            if attrno == 0 {
                let relname =
                    core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname UTF-8");
                return Err(undefined_insert_column(pstate, name, relname, col.location));
            }
            let attrno = attrno as i32;
            if col.indirection.is_nil() {
                if wholecols.is_member(attrno) || partialcols.is_member(attrno) {
                    return Err(duplicate_insert_column(pstate, name, col.location));
                }
                wholecols.add_member(mcx, attrno)?;
            } else {
                if wholecols.is_member(attrno) {
                    return Err(duplicate_insert_column(pstate, name, col.location));
                }
                partialcols.add_member(mcx, attrno)?;
            }
            attrnos.push(attrno);
        }
        Ok((cols.clone_in(mcx)?, attrnos))
    }
}

#[cold]
fn default_with_indirection(
    pstate: &ParseState<'_, '_>,
    indirection: &NodeList<'_>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let msg = if indirection.nth(0).as_a_indices().is_some() {
        "cannot set an array element to DEFAULT"
    } else {
        "cannot set a subfield to DEFAULT"
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(msg.to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformAssignedExpr")),
    )
}

#[cold]
fn cannot_assign_to_system_column(
    pstate: &ParseState<'_, '_>,
    colname: Option<&str>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("cannot assign to system column \"{}\"", colname.unwrap_or("?")))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformAssignedExpr")),
    )
}

#[cold]
fn column_type_mismatch(
    pstate: &ParseState<'_, '_>,
    colname: &str,
    attrtype: types_core::Oid,
    exprtype: types_core::Oid,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let (want, got) = match (
        format_type::format_type_be(attrtype),
        format_type::format_type_be(exprtype),
    ) {
        (Ok(w), Ok(g)) => (w, g),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "column \"{colname}\" is of type {want} but expression is of type {got}",
            ))
            .errhint("You will need to rewrite or cast the expression.")
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformAssignedExpr")),
    )
}

#[cold]
fn undefined_insert_column(
    pstate: &ParseState<'_, '_>,
    name: &str,
    relname: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("column \"{name}\" of relation \"{relname}\" does not exist"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "checkInsertTargets")),
    )
}

#[cold]
fn duplicate_insert_column(
    pstate: &ParseState<'_, '_>,
    name: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DUPLICATE_COLUMN, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_COLUMN)
            .errmsg(format!("column \"{name}\" specified more than once"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "checkInsertTargets")),
    )
}

pub fn markTargetListOrigins<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        markTargetListOrigin(pstate, tle_node, tle.expr, 0)?;
    }
    Ok(())
}

fn markTargetListOrigin<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    tle_node: Node<'mcx>,
    expr: Node<'mcx>,
    levelsup: i32,
) -> PgResult<()> {
    let Some(var) = expr.as_var() else {
        return Ok(());
    };
    let netlevelsup = var.varlevelsup as i32 + levelsup;
    let rte = parse_relation::GetRTEByRangeTablePosn(pstate, var.varno, netlevelsup);
    let attnum = var.varattno;

    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // SAFETY: parse analysis holds exclusive access to the targetlist
            // it just built; the `var` borrow is from expr, not tle_node.
            unsafe {
                tle_node
                    .with_mut::<TargetEntry, _>(|t| {
                        t.resorigtbl = rte.relid;
                        t.resorigcol = attnum;
                    })
                    .unwrap();
            }
        }
        RTEKind::RTE_SUBQUERY => {
            if attnum != 0 {
                let ste = rte
                    .subquery
                    .expect("RTE_SUBQUERY has subquery")
                    .targetList
                    .iter()
                    .map(|n| n.as_target_entry().expect("tlist cell"))
                    .find(|t| t.resno == attnum);
                let ste = match ste {
                    Some(t) if !t.resjunk => t,
                    _ => {
                        return Err(Box::new(types_error::PgError::error(format!(
                            "subquery {} does not have attribute {attnum}",
                            rte.eref.and_then(|e| e.aliasname).unwrap_or("")
                        ))))
                    }
                };
                let (resorigtbl, resorigcol) = (ste.resorigtbl, ste.resorigcol);
                // SAFETY: as the RTE_RELATION arm — the `ste` borrow is from
                // the subquery's tlist, not tle_node.
                unsafe {
                    tle_node
                        .with_mut::<TargetEntry, _>(|t| {
                            t.resorigtbl = resorigtbl;
                            t.resorigcol = resorigcol;
                        })
                        .unwrap();
                }
            }
        }
        RTEKind::RTE_CTE => {
            // A self-reference has no analyzed subquery to copy up from.
            if attnum != 0 && !rte.self_reference {
                let cte_node = parse_relation::GetCTEForRTE(pstate, rte, netlevelsup);
                let cte = cte_node.as_common_table_expr().expect("ctenamespace cell");
                let tl = cte.cte_target_list();
                // The RTE carries the search/cycle columns but the subquery
                // does not yet; skip origin lookups for those.
                let mut extra_cols: i16 = 0;
                if cte.search_clause.is_some() {
                    extra_cols += 1;
                }
                if cte.cycle_clause.is_some() {
                    extra_cols += 2;
                }
                if extra_cols > 0
                    && attnum > tl.len() as i16
                    && attnum <= tl.len() as i16 + extra_cols
                {
                    return Ok(());
                }
                let ste = tl
                    .iter()
                    .map(|n| n.as_target_entry().expect("tlist cell"))
                    .find(|t| t.resno == attnum);
                let ste = match ste {
                    Some(t) if !t.resjunk => t,
                    _ => {
                        return Err(Box::new(types_error::PgError::error(format!(
                            "CTE {} does not have attribute {attnum}",
                            rte.eref.and_then(|e| e.aliasname).unwrap_or("")
                        ))))
                    }
                };
                let (resorigtbl, resorigcol) = (ste.resorigtbl, ste.resorigcol);
                // SAFETY: as the RTE_RELATION arm — the `ste` borrow is from
                // the CTE query's tlist, not tle_node.
                unsafe {
                    tle_node
                        .with_mut::<TargetEntry, _>(|t| {
                            t.resorigtbl = resorigtbl;
                            t.resorigcol = resorigcol;
                        })
                        .unwrap();
                }
            }
        }
        // C: RTE_GROUP is unreachable here (the group RTE is not yet added
        // when targetlist origins are marked); same no-op arm.
        RTEKind::RTE_FUNCTION
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_NAMEDTUPLESTORE
        | RTEKind::RTE_JOIN
        | RTEKind::RTE_RESULT
        | RTEKind::RTE_GROUP => {}
    }
    Ok(())
}

pub fn resolveTargetListUnknowns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        let restype = expr_type(tle.expr);
        if restype == UNKNOWNOID {
            let coerced = coerce::coerce_type(
                mcx,
                pstate,
                tle.expr,
                restype,
                TEXTOID,
                -1,
                coerce::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            // SAFETY: parse analysis holds exclusive access to the targetlist
            // it just built; the `tle` borrow is not used past this point.
            unsafe {
                tle_node.with_mut::<TargetEntry, _>(|t| t.expr = coerced).unwrap();
            }
        }
    }
    Ok(())
}

// ExpandIndirectionStar (parse_target.c): strip the trailing A_Star,
// transform the remaining rowtype expression, expand it into fields.
fn ExpandIndirectionStar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    ind: &types_nodes::rawnodes::A_Indirection<'mcx>,
    make_target_entry: bool,
    exprKind: ParseExprKind,
) -> PgResult<NodeList<'mcx>> {
    let mut trimmed = NodeList::nil();
    for n in ind.indirection.as_slice()[..ind.indirection.len() - 1].iter() {
        trimmed.lappend(mcx, *n)?;
    }
    let arg = ind.arg.expect("A_Indirection.arg");
    let expr = if trimmed.is_nil() {
        transformExpr(mcx, pstate, arg, exprKind)?
    } else {
        let stripped = Node::mk(
            mcx,
            types_nodes::rawnodes::A_Indirection { arg: Some(arg), indirection: trimmed },
        )?;
        transformExpr(mcx, pstate, stripped, exprKind)?
    };
    ExpandRowReference(mcx, pstate, expr, make_target_entry)
}

// ExpandRowReference (parse_target.c). C copyObjects the rowtype expression
// per field; the sealed node is shared instead (immutable subtree).
fn ExpandRowReference<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    make_target_entry: bool,
) -> PgResult<NodeList<'mcx>> {
    if let Some(var) = expr.as_var() {
        if var.varattno == 0 {
            let nsitem = parse_relation::GetNSItemByRangeTablePosn(
                pstate,
                var.varno as i32,
                var.varlevelsup as i32,
            );
            return ExpandSingleTable(
                mcx,
                pstate,
                nsitem,
                var.varlevelsup as i32,
                var.location,
                make_target_entry,
            );
        }
    }

    let tupdesc = match expr.as_var() {
        Some(var) if var.vartype == RECORDOID => expandRecordVariable(mcx, pstate, expr, 0)?,
        // C falls back to lookup_rowtype_tupdesc_copy(exprType, exprTypmod):
        // registered RECORD-typmod expressions (plpgsql rec whole-row Params).
        _ => match funcapi::get_expr_result_tupdesc(mcx, Some(expr), true)? {
            Some(td) => td,
            None => typcache_seams::lookup_rowtype_tupdesc_copy::call(
                mcx,
                expr_type(expr),
                nodes_core::node_funcs::expr_typmod(expr),
            )?,
        },
    };
    let mut result = NodeList::nil();
    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        let fselect = Node::mk(
            mcx,
            types_nodes::primnodes::FieldSelect {
                arg: expr,
                fieldnum: (i + 1) as AttrNumber,
                resulttype: att.atttypid,
                resulttypmod: att.atttypmod,
                resultcollid: att.attcollation,
            },
        )?;
        if make_target_entry {
            let name = core::str::from_utf8(att.attname.name_str()).expect("attname is UTF-8");
            let resno = pstate.p_next_resno as AttrNumber;
            pstate.p_next_resno += 1;
            let te =
                Node::mk_target_entry(mcx, fselect, resno, Some(str_in(mcx, name)?), false)?;
            result.lappend(mcx, te)?;
        } else {
            result.lappend(mcx, fselect)?;
        }
    }
    Ok(result)
}

// expandRecordVariable (parse_target.c): tuple descriptor for a Var of type
// RECORD, drilling through subquery/join/CTE RTEs to the defining expression.
pub fn expandRecordVariable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    var_node: Node<'mcx>,
    levelsup: i32,
) -> PgResult<TupleDescData<'mcx>> {
    let var = var_node.as_var().expect("expandRecordVariable takes a Var");
    debug_assert_eq!(var.vartype, RECORDOID);
    let netlevelsup = var.varlevelsup as i32 + levelsup;
    let rte = parse_relation::GetRTEByRangeTablePosn(pstate, var.varno, netlevelsup);
    let attnum = var.varattno;

    if attnum == 0 {
        // Whole-row reference to an RTE: expand the known fields.
        let (names, vars) = parse_relation::expandRTE(
            mcx,
            rte,
            var.varno,
            0,
            var.varreturningtype,
            var.location,
            false,
        )?;
        let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, vars.len() as i32)?;
        for (i, (name, v)) in names.iter().zip(vars.iter()).enumerate() {
            let label = name.as_string().expect("colnames are String nodes").sval;
            let attno = (i + 1) as AttrNumber;
            tupdesc::TupleDescInitEntry(
                &mut desc,
                attno,
                Some(label),
                expr_type(v),
                expr_typmod(v),
                0,
            )?;
            tupdesc::TupleDescInitEntryCollation(&mut desc, attno, expr_collation(v));
        }
        return Ok(desc);
    }

    // Default if we can't drill down: inspect the Var itself at the bottom.
    let mut expr = var_node;
    match rte.rtekind {
        // These RTE kinds cannot yield a RECORD-typed column; fall through
        // and (most likely) fail at the bottom (C same).
        RTEKind::RTE_RELATION
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_NAMEDTUPLESTORE
        | RTEKind::RTE_RESULT
        | RTEKind::RTE_FUNCTION
        | RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_GROUP => {}
        RTEKind::RTE_SUBQUERY => {
            let subquery = rte.subquery.expect("RTE_SUBQUERY has subquery");
            let ste = get_tle_by_resno(&subquery.targetList, attnum);
            let ste = match ste {
                Some(t) if !t.resjunk => t,
                _ => return Err(no_such_attribute("subquery", rte, attnum)),
            };
            expr = ste.expr;
            if expr.node_tag() == NodeTag::T_Var {
                // Recurse with a fake ParseState one level down; the subquery
                // RTE might be from an outer level, so the fake state's parent
                // is the pstate that owns the RTE (C same).
                let mut p = pstate;
                for _ in 0..netlevelsup {
                    p = p.parentParseState.expect("GetRTEByRangeTablePosn validated depth");
                }
                let mut mypstate = parser_small1::make_parsestate(mcx, Some(p));
                // C aliases the subquery's rtable pointer; the 16-byte list
                // carrier is re-materialized (cells copied, nodes shared).
                mypstate.p_rtable = subquery.rtable.clone_in(mcx)?;
                return expandRecordVariable(mcx, &mypstate, expr, 0);
            }
        }
        RTEKind::RTE_JOIN => {
            debug_assert!(attnum > 0 && attnum as usize <= rte.joinaliasvars.len());
            // C intentionally doesn't strip implicit coercions here.
            expr = rte.joinaliasvars.nth(attnum as usize - 1);
            if expr.node_tag() == NodeTag::T_Var {
                return expandRecordVariable(mcx, pstate, expr, netlevelsup);
            }
        }
        RTEKind::RTE_CTE => {
            if !rte.self_reference {
                let cte_node = parse_relation::GetCTEForRTE(pstate, rte, netlevelsup);
                let cte = cte_node.as_common_table_expr().expect("ctenamespace cell");
                let ctequery = cte
                    .ctequery
                    .expect("analyzed CTE")
                    .as_query()
                    .expect("analyzed CTE is a Query");
                let ste = get_tle_by_resno(&ctequery.targetList, attnum);
                let ste = match ste {
                    Some(t) if !t.resjunk => t,
                    _ => return Err(no_such_attribute("CTE", rte, attnum)),
                };
                expr = ste.expr;
                if expr.node_tag() == NodeTag::T_Var {
                    let mut p = pstate;
                    for _ in 0..(rte.ctelevelsup as i32 + netlevelsup) {
                        p = p.parentParseState.expect("GetCTEForRTE validated depth");
                    }
                    let mut mypstate = parser_small1::make_parsestate(mcx, Some(p));
                    mypstate.p_rtable = ctequery.rtable.clone_in(mcx)?;
                    return expandRecordVariable(mcx, &mypstate, expr, 0);
                }
            }
        }
    }

    Ok(funcapi::get_expr_result_tupdesc(mcx, Some(expr), false)?
        .expect("no_error=false returns a descriptor"))
}

fn get_tle_by_resno<'mcx>(
    tlist: &NodeList<'mcx>,
    resno: AttrNumber,
) -> Option<&'mcx TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("tlist cell"))
        .find(|t| t.resno == resno)
}

#[cold]
fn no_such_attribute(
    what: &str,
    rte: &types_nodes::RangeTblEntry<'_>,
    attnum: AttrNumber,
) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "{what} {} does not have attribute {attnum}",
        rte.eref.and_then(|e| e.aliasname).unwrap_or("")
    )))
}

fn ExpandColumnRefStar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cref: &ColumnRef<'mcx>,
    make_target_entry: bool,
) -> PgResult<NodeList<'mcx>> {
    let fields = cref.fields.as_slice();
    if fields.len() == 1 {
        // Grammar accepts bare '*' only at SELECT top level (C same assert).
        debug_assert!(make_target_entry);
        return ExpandAllTables(mcx, pstate, cref.location);
    }

    let field_str = |n: Node<'mcx>| {
        n.as_string().map(|s| s.sval).expect("ColumnRef qualifier is a String")
    };
    let (nspname, relname) = match fields.len() {
        2 => (None, field_str(fields[0])),
        3 => (Some(field_str(fields[0])), field_str(fields[1])),
        4 => {
            let catname = field_str(fields[0]);
            let dbname = dbcommands_seams::get_database_name::call(
                init_small::globals::MyDatabaseId(),
            )?;
            if dbname.as_deref() != Some(catname) {
                return Err(cross_database_reference(pstate, &cref.fields, cref.location));
            }
            (Some(field_str(fields[1])), field_str(fields[2]))
        }
        _ => return Err(too_many_dotted_names(pstate, &cref.fields, cref.location)),
    };

    let mut levels_up = 0;
    let nsitem = parse_relation::refnameNamespaceItem(
        pstate,
        nspname,
        relname,
        cref.location,
        Some(&mut levels_up),
    )?;

    // The columnref-hook legs of C's ExpandColumnRefStar: a plpgsql rec.*
    // resolves to a whole-row Param, expanded per field.
    let plpgsql_hooks = pstate.p_ref_hook_state.as_plpgsql_params().copied();
    if let Some(st) = &plpgsql_hooks {
        let skip = match st.resolve_option {
            parser_small1::PlpgsqlResolveOption::Variable => false,
            parser_small1::PlpgsqlResolveOption::Column => nsitem.is_some(),
            _ => false,
        };
        // C: variable-precedence runs as the PRE hook (before the RTE
        // lookup); the shared resolver is order-insensitive here because a
        // hook hit under Variable precedence returns before ambiguity checks.
        if !skip {
            if let Some(node) = plpgsql_star_ref(mcx, pstate, st, fields, cref.location)? {
                if nsitem.is_some()
                    && st.resolve_option != parser_small1::PlpgsqlResolveOption::Variable
                {
                    use types_error::{ErrorLocation, ERRCODE_AMBIGUOUS_COLUMN, ERROR};
                    return Err(Box::new(
                        elog::ereport(ERROR)
                            .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                            .errmsg(format!(
                                "column reference \"{}\" is ambiguous",
                                name_list_to_string(&cref.fields)
                            ))
                            .errposition(parser_small1::parser_errposition(
                                pstate,
                                cref.location,
                                mbutils::GetDatabaseEncoding(),
                            ))
                            .into_error()
                            .with_error_location(ErrorLocation::new(
                                "parse_target.c",
                                0,
                                "ExpandColumnRefStar",
                            )),
                    ));
                }
                return ExpandRowReference(mcx, pstate, node, make_target_entry);
            }
        }
    }

    let Some(nsitem) = nsitem else {
        // The post_columnref leg of C's ExpandColumnRefStar:
        // sql_fn_post_column_ref never overrides a found table (returns NULL
        // when var != NULL), so it only runs on the nsitem-miss path.
        if let Some(node) =
            parse_expr::sql_fn_post_column_ref(mcx, pstate, fields, cref.location)?
        {
            return ExpandRowReference(mcx, pstate, node, make_target_entry);
        }
        let rv = Node::mk_mut(
            mcx,
            types_nodes::RangeVar {
                schemaname: nspname.map(|s| str_in(mcx, s)).transpose()?,
                relname: Some(str_in(mcx, relname)?),
                location: cref.location,
                ..Default::default()
            },
        )?
        .seal_ref();
        return Err(parse_relation::errorMissingRTE(mcx, pstate, rv));
    };

    ExpandSingleTable(mcx, pstate, nsitem, levels_up, cref.location, make_target_entry)
}

// resolve_column_ref's whole-row arms (pl_comp.c): the trailing A_Star maps
// to "*" (blocks scalar matches, keeps the valueless-rec 55000 arm), then a
// rec-gated prefix lookup returns the whole-row Param.
fn plpgsql_star_ref<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    st: &parser_small1::PlpgsqlHookState<'_>,
    fields: &[Node<'mcx>],
    location: types_core::ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let n = fields.len();
    if !(2..=3).contains(&n) || fields[n - 1].node_tag() != NodeTag::T_A_Star {
        return Ok(None);
    }
    let mut names: [&str; 3] = [""; 3];
    for (i, f) in fields[..n - 1].iter().enumerate() {
        match f.as_string() {
            Some(s) => names[i] = s.sval,
            None => return Ok(None),
        }
    }
    names[n - 1] = "*";
    if let Some(node) = parser_small1::plpgsql_resolve_column_ref(
        mcx,
        pstate,
        st,
        &names[..n],
        location,
        false,
        mbutils::GetDatabaseEncoding(),
    )? {
        return Ok(Some(node));
    }
    let prefix = names[..n - 1].join(".").to_ascii_lowercase();
    if !st.recs.iter().any(|r| *r == prefix) {
        return Ok(None);
    }
    parser_small1::plpgsql_resolve_column_ref(
        mcx,
        pstate,
        st,
        &names[..n - 1],
        location,
        false,
        mbutils::GetDatabaseEncoding(),
    )
}

// C NameListToString (namespace.c): dotted join, A_Star renders as "*".
fn name_list_to_string(fields: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        match f.as_string() {
            Some(s) => out.push_str(s.sval),
            None => out.push('*'),
        }
    }
    out
}

#[cold]
fn cross_database_reference(
    pstate: &ParseState<'_, '_>,
    fields: &NodeList<'_>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cross-database references are not implemented: {}",
                name_list_to_string(fields)
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ExpandColumnRefStar")),
    )
}

#[cold]
fn too_many_dotted_names(
    pstate: &ParseState<'_, '_>,
    fields: &NodeList<'_>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "improper qualified name (too many dotted names): {}",
                name_list_to_string(fields)
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ExpandColumnRefStar")),
    )
}

fn ExpandAllTables<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    location: i32,
) -> PgResult<NodeList<'mcx>> {
    let mut target = NodeList::nil();
    let mut found_table = false;

    // p_namespace is iterated by index: expandNSItemAttrs needs &mut pstate
    // (p_next_resno) while the vec's items are 'mcx-borrowed.
    for i in 0..pstate.p_namespace.len() {
        let nsitem = pstate.p_namespace[i];
        if !nsitem.p_cols_visible {
            continue;
        }
        debug_assert!(!nsitem.p_lateral_only.get());
        found_table = true;
        target.concat(
            mcx,
            &parse_relation::expandNSItemAttrs(mcx, pstate, nsitem, 0, true, location)?,
        )?;
    }

    if !found_table {
        return Err(star_with_no_tables(pstate, location));
    }
    Ok(target)
}

fn ExpandSingleTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: i32,
    make_target_entry: bool,
) -> PgResult<NodeList<'mcx>> {
    if make_target_entry {
        return parse_relation::expandNSItemAttrs(
            mcx,
            pstate,
            nsitem,
            sublevels_up,
            true,
            location,
        );
    }
    let rte = nsitem.rte();
    let (vars, _) = parse_relation::expandNSItemVars(mcx, pstate, nsitem, sublevels_up, location)?;
    if rte.rtekind == RTEKind::RTE_RELATION {
        let perminfo = nsitem.p_perminfo.expect("relation nsitem has perminfo");
        // SAFETY: perminfo nodes are read only through transient as_* lookups;
        // no derived reference is live across this call.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.requiredPerms |= types_nodes::parsenodes::ACL_SELECT
            })
        }
        .expect("p_perminfo is RTEPermissionInfo");
    }
    for var_node in &vars {
        let var = var_node.as_var().expect("expandNSItemVars yields Vars");
        parse_relation::markVarForSelectPriv(mcx, pstate, var)?;
    }
    Ok(vars)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[cold]
fn star_with_no_tables(
    pstate: &ParseState<'_, '_>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("SELECT * with no tables specified is not valid")
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ExpandAllTables")),
    )
}

pub fn FigureColname<'mcx>(node: Node<'mcx>) -> &'mcx str {
    let mut name = None;
    FigureColnameInternal(node, &mut name);
    name.unwrap_or("?column?")
}

// FigureIndexColname (parse_utilcmd.c): None when nothing suggests a name.
pub fn FigureIndexColname<'mcx>(node: Node<'mcx>) -> Option<&'mcx str> {
    let mut name = None;
    FigureColnameInternal(node, &mut name);
    name
}

// C's strength contract: 0 = no name, 1 = weak (type-cast name), 2 = good.
fn FigureColnameInternal<'mcx>(node: Node<'mcx>, name: &mut Option<&'mcx str>) -> i32 {
    match node.node_tag() {
        NodeTag::T_ColumnRef => {
            let mut fname = None;
            for f in &node.as_column_ref().unwrap().fields {
                if let Some(s) = f.as_string() {
                    fname = Some(s.sval);
                }
            }
            if let Some(fname) = fname {
                *name = Some(fname);
                return 2;
            }
            0
        }
        NodeTag::T_A_Expr => {
            if node.as_a_expr().unwrap().kind == A_Expr_Kind::AEXPR_NULLIF {
                *name = Some("nullif");
                return 2;
            }
            0
        }
        NodeTag::T_TypeCast => {
            let tc = node.as_type_cast().unwrap();
            let strength = tc.arg.map_or(0, |arg| FigureColnameInternal(arg, name));
            if strength <= 1 {
                if let Some(tn) =
                    tc.typeName.and_then(|n| n.as_variant::<types_nodes::TypeName>())
                {
                    if let Some(last) = tn.names.last().and_then(|n| n.as_string()) {
                        *name = Some(last.sval);
                        return 1;
                    }
                }
            }
            strength
        }
        NodeTag::T_CaseExpr => {
            let strength = node
                .as_case_expr()
                .unwrap()
                .defresult
                .map_or(0, |d| FigureColnameInternal(d, name));
            if strength <= 1 {
                *name = Some("case");
                return 1;
            }
            strength
        }
        NodeTag::T_CoalesceExpr => {
            *name = Some("coalesce");
            2
        }
        // C: ARRAY[] columns are named "array"; indirection names from the
        // last field selection, else the base expression.
        NodeTag::T_A_ArrayExpr => {
            *name = Some("array");
            2
        }
        NodeTag::T_A_Indirection => {
            let ind = node.as_a_indirection().unwrap();
            for n in ind.indirection.iter().rev() {
                if let Some(s) = n.as_string() {
                    *name = Some(s.sval);
                    return 2;
                }
            }
            ind.arg.map_or(0, |arg| FigureColnameInternal(arg, name))
        }
        NodeTag::T_MinMaxExpr => {
            *name = Some(match node.as_min_max_expr().unwrap().op {
                types_nodes::primnodes::MinMaxOp::IS_GREATEST => "greatest",
                types_nodes::primnodes::MinMaxOp::IS_LEAST => "least",
            });
            2
        }
        NodeTag::T_SQLValueFunction => {
            use types_nodes::primnodes::SQLValueFunctionOp as Op;
            *name = Some(match node.as_sql_value_function().unwrap().op {
                Op::SVFOP_CURRENT_DATE => "current_date",
                Op::SVFOP_CURRENT_TIME | Op::SVFOP_CURRENT_TIME_N => "current_time",
                Op::SVFOP_CURRENT_TIMESTAMP | Op::SVFOP_CURRENT_TIMESTAMP_N => {
                    "current_timestamp"
                }
                Op::SVFOP_LOCALTIME | Op::SVFOP_LOCALTIME_N => "localtime",
                Op::SVFOP_LOCALTIMESTAMP | Op::SVFOP_LOCALTIMESTAMP_N => "localtimestamp",
                Op::SVFOP_CURRENT_ROLE => "current_role",
                Op::SVFOP_CURRENT_USER => "current_user",
                Op::SVFOP_USER => "user",
                Op::SVFOP_SESSION_USER => "session_user",
                Op::SVFOP_CURRENT_CATALOG => "current_catalog",
                Op::SVFOP_CURRENT_SCHEMA => "current_schema",
            });
            2
        }
        // C: make GROUPING() act like a regular function.
        NodeTag::T_GroupingFunc => {
            *name = Some("grouping");
            2
        }
        // C: make MERGE_ACTION() act like a regular function.
        NodeTag::T_MergeSupportFunc => {
            *name = Some("merge_action");
            2
        }
        NodeTag::T_FuncCall => {
            let fc = node.as_func_call().unwrap();
            match fc.funcname.last().and_then(|n| n.as_string()) {
                Some(s) => {
                    *name = Some(s.sval);
                    2
                }
                None => 0,
            }
        }
        NodeTag::T_SubLink => {
            use types_nodes::SubLinkType;
            let sl = node.as_sub_link().unwrap();
            match sl.subLinkType {
                SubLinkType::EXISTS_SUBLINK => {
                    *name = Some("exists");
                    2
                }
                SubLinkType::ARRAY_SUBLINK => {
                    *name = Some("array");
                    2
                }
                SubLinkType::EXPR_SUBLINK => {
                    // C sees the transformed Query here because
                    // transformSubLink scribbles it into the raw SubLink in
                    // place; our transform builds a fresh node, so a SubLink
                    // nested under indirection/casts is still raw and the
                    // raw leg derives the resname transformTargetEntry would
                    // have assigned.
                    if let Some(q) = sl.subselect.as_query() {
                        if let Some(te) =
                            q.targetList.first().and_then(|n| n.as_target_entry())
                        {
                            if let Some(resname) = te.resname {
                                *name = Some(resname);
                                return 2;
                            }
                        }
                    } else if let Some(ss) = sl.subselect.as_select_stmt() {
                        if let Some(rt) =
                            ss.targetList.first().and_then(|n| n.as_res_target())
                        {
                            if let Some(n) = rt.name {
                                *name = Some(n);
                                return 2;
                            }
                            if let Some(val) = rt.val {
                                let mut inner = None;
                                FigureColnameInternal(val, &mut inner);
                                *name = Some(inner.unwrap_or("?column?"));
                                return 2;
                            }
                        }
                    }
                    0
                }
                _ => 0,
            }
        }
        NodeTag::T_CollateClause => match node.as_collate_clause().unwrap().arg {
            Some(arg) => FigureColnameInternal(arg, name),
            None => 0,
        },
        NodeTag::T_RowExpr => {
            *name = Some("row");
            2
        }
        NodeTag::T_XmlExpr => {
            use types_nodes::XmlExprOp::*;
            let n = match node.as_xml_expr().unwrap().op {
                IS_XMLCONCAT => Some("xmlconcat"),
                IS_XMLELEMENT => Some("xmlelement"),
                IS_XMLFOREST => Some("xmlforest"),
                IS_XMLPARSE => Some("xmlparse"),
                IS_XMLPI => Some("xmlpi"),
                IS_XMLROOT => Some("xmlroot"),
                IS_XMLSERIALIZE => Some("xmlserialize"),
                IS_DOCUMENT => None,
            };
            match n {
                Some(v) => {
                    *name = Some(v);
                    1
                }
                None => 0,
            }
        }
        NodeTag::T_XmlSerialize => {
            *name = Some("xmlserialize");
            1
        }
        NodeTag::T_SQLValueFunction => {
            use types_nodes::SQLValueFunctionOp::*;
            *name = Some(match node.as_sql_value_function().unwrap().op {
                SVFOP_CURRENT_DATE => "current_date",
                SVFOP_CURRENT_TIME | SVFOP_CURRENT_TIME_N => "current_time",
                SVFOP_CURRENT_TIMESTAMP | SVFOP_CURRENT_TIMESTAMP_N => "current_timestamp",
                SVFOP_LOCALTIME | SVFOP_LOCALTIME_N => "localtime",
                SVFOP_LOCALTIMESTAMP | SVFOP_LOCALTIMESTAMP_N => "localtimestamp",
                SVFOP_CURRENT_ROLE => "current_role",
                SVFOP_CURRENT_USER => "current_user",
                SVFOP_USER => "user",
                SVFOP_SESSION_USER => "session_user",
                SVFOP_CURRENT_CATALOG => "current_catalog",
                SVFOP_CURRENT_SCHEMA => "current_schema",
            });
            2
        }
        NodeTag::T_JsonParseExpr => {
            *name = Some("json");
            2
        }
        NodeTag::T_JsonScalarExpr => {
            *name = Some("json_scalar");
            2
        }
        NodeTag::T_JsonSerializeExpr => {
            *name = Some("json_serialize");
            2
        }
        NodeTag::T_JsonObjectConstructor => {
            *name = Some("json_object");
            2
        }
        NodeTag::T_JsonArrayConstructor | NodeTag::T_JsonArrayQueryConstructor => {
            *name = Some("json_array");
            2
        }
        NodeTag::T_JsonObjectAgg => {
            *name = Some("json_objectagg");
            2
        }
        NodeTag::T_JsonArrayAgg => {
            *name = Some("json_arrayagg");
            2
        }
        NodeTag::T_JsonFuncExpr => {
            use types_nodes::primnodes::JsonExprOp::*;
            *name = Some(match node.as_json_func_expr().unwrap().op {
                JSON_EXISTS_OP => "json_exists",
                JSON_QUERY_OP => "json_query",
                JSON_VALUE_OP => "json_value",
                JSON_TABLE_OP => panic!("JSON_TABLE_OP cannot happen here"),
            });
            2
        }
        // NullTest/BooleanTest/JsonIsPredicate take C's default arm: no name.
        NodeTag::T_JsonIsPredicate
        | NodeTag::T_A_Const
        | NodeTag::T_ParamRef
        | NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest => 0,
        // C's default arm: no name suggestion ("?column?"), never an error —
        // return 0 for any other raw node rather than panicking.
        _ => 0,
    }
}

// transformAssignmentIndirection (parse_target.c). `start` is C's
// indirection_cell; basenode None with cells remaining builds the
// CaseTestExpr substitute (only FieldStore/SubscriptingRef sit above it).
#[allow(clippy::too_many_arguments)]
pub fn transformAssignmentIndirection<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    basenode: Option<Node<'mcx>>,
    target_name: &str,
    target_is_subscripting: bool,
    target_type_id: types_core::Oid,
    target_typmod: i32,
    target_collation: types_core::Oid,
    indirection: &NodeList<'mcx>,
    start: usize,
    rhs: Node<'mcx>,
    ccontext: coerce::CoercionContext,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    let basenode = match basenode {
        None if start < indirection.len() => Some(Node::mk(
            mcx,
            types_nodes::primnodes::CaseTestExpr {
                typeId: target_type_id,
                typeMod: target_typmod,
                collation: target_collation,
            },
        )?),
        other => other,
    };
    let mut subscripts: NodeList<'mcx> = NodeList::nil();
    for (off, n) in indirection.as_slice()[start..].iter().enumerate() {
        let i = start + off;
        match n.node_tag() {
            NodeTag::T_A_Indices => subscripts.lappend(mcx, *n)?,
            NodeTag::T_A_Star => {
                return Err(star_expansion_not_supported(pstate, location));
            }
            _ => {
                let fieldname = n.as_string().expect("field indirection is a String").sval;
                if !subscripts.is_nil() {
                    return transformAssignmentSubscripts(
                        mcx,
                        pstate,
                        basenode,
                        target_name,
                        target_type_id,
                        target_typmod,
                        target_collation,
                        &subscripts,
                        indirection,
                        i,
                        rhs,
                        ccontext,
                        location,
                    );
                }

                let mut base_typmod = target_typmod;
                let base_type_id =
                    ::lsyscache::getBaseTypeAndTypmod(target_type_id, &mut base_typmod)?;
                let typrelid = ::lsyscache::get_typ_typrelid(base_type_id)?;
                if !types_core::OidIsValid(typrelid) {
                    return Err(field_of_noncomposite(
                        pstate,
                        fieldname,
                        target_name,
                        target_type_id,
                        location,
                    ));
                }
                let attnum = ::lsyscache::get_attnum(typrelid, fieldname)?;
                if attnum == types_core::InvalidAttrNumber {
                    return Err(no_such_field(
                        pstate,
                        fieldname,
                        target_name,
                        target_type_id,
                        location,
                    ));
                }
                if attnum < 0 {
                    return Err(assign_system_column_field(pstate, fieldname, location));
                }
                let (field_type_id, field_typmod, field_collation) =
                    ::lsyscache::get_atttypetypmodcoll(typrelid, attnum)?;

                let rhs = transformAssignmentIndirection(
                    mcx,
                    pstate,
                    None,
                    fieldname,
                    false,
                    field_type_id,
                    field_typmod,
                    field_collation,
                    indirection,
                    i + 1,
                    rhs,
                    ccontext,
                    location,
                )?;

                let fstore = Node::mk(
                    mcx,
                    types_nodes::FieldStore {
                        arg: basenode.expect("basenode set above (cells remain)"),
                        newvals: NodeList::make1(mcx, rhs)?,
                        fieldnums: types_nodes::list::IntList::make1(mcx, attnum as i32)?,
                        resulttype: base_type_id,
                    },
                )?;

                // Domain constraints are checked once per column after the
                // rewriter merges same-column subfield assignments.
                if base_type_id != target_type_id {
                    return coerce::coerce_to_domain(
                        mcx,
                        fstore,
                        base_type_id,
                        base_typmod,
                        target_type_id,
                        coerce::COERCION_IMPLICIT,
                        CoercionForm::COERCE_IMPLICIT_CAST,
                        location,
                        false,
                    );
                }
                return Ok(fstore);
            }
        }
    }
    if !subscripts.is_nil() {
        return transformAssignmentSubscripts(
            mcx,
            pstate,
            basenode,
            target_name,
            target_type_id,
            target_typmod,
            target_collation,
            &subscripts,
            indirection,
            indirection.len(),
            rhs,
            ccontext,
            location,
        );
    }

    let rhs_type = expr_type(rhs);
    let result = coerce::coerce_to_target_type(
        mcx,
        pstate,
        rhs,
        rhs_type,
        target_type_id,
        target_typmod,
        ccontext,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    match result {
        Some(r) => Ok(r),
        None if target_is_subscripting => Err(subscript_assign_type_mismatch(
            pstate,
            target_name,
            target_type_id,
            rhs_type,
            location,
        )),
        None => Err(subfield_type_mismatch(
            pstate,
            target_name,
            target_type_id,
            rhs_type,
            location,
        )),
    }
}

// transformAssignmentSubscripts (parse_target.c).
#[allow(clippy::too_many_arguments)]
fn transformAssignmentSubscripts<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    basenode: Option<Node<'mcx>>,
    target_name: &str,
    target_type_id: types_core::Oid,
    target_typmod: i32,
    target_collation: types_core::Oid,
    subscripts: &NodeList<'mcx>,
    indirection: &NodeList<'mcx>,
    next_indirection: usize,
    rhs: Node<'mcx>,
    ccontext: coerce::CoercionContext,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    debug_assert!(!subscripts.is_nil());
    let mut container_type = target_type_id;
    let mut container_typmod = target_typmod;
    parser_small1::transformContainerType(&mut container_type, &mut container_typmod)?;

    let sbsref_node = parse_expr::transformContainerSubscripts(
        mcx,
        pstate,
        basenode.expect("transformAssignmentIndirection always supplies a base"),
        container_type,
        container_typmod,
        subscripts,
        true,
    )?;
    let (type_needed, typmod_needed) = {
        let sr = sbsref_node.as_subscripting_ref().unwrap();
        (sr.refrestype, sr.reftypmod)
    };
    // A domain over a container is subscripted with the base type's
    // collation (labels a possible CaseTestExpr).
    let collation_needed = if container_type == target_type_id {
        target_collation
    } else {
        ::lsyscache::get_typcollation(container_type)?
    };

    let rhs = transformAssignmentIndirection(
        mcx,
        pstate,
        None,
        target_name,
        true,
        type_needed,
        typmod_needed,
        collation_needed,
        indirection,
        next_indirection,
        rhs,
        ccontext,
        location,
    )?;

    // SAFETY: parse analysis exclusively owns the just-built node.
    unsafe {
        Node::with_mut::<types_nodes::SubscriptingRef, ()>(sbsref_node, |sr| {
            sr.refassgnexpr = Some(rhs);
            sr.refrestype = container_type;
            sr.reftypmod = container_typmod;
        });
    }

    if container_type != target_type_id {
        // Premature if multiple elements are assigned; the rewriter fixes it.
        let resulttype = expr_type(sbsref_node);
        return coerce::coerce_to_target_type(
            mcx,
            pstate,
            sbsref_node,
            resulttype,
            target_type_id,
            target_typmod,
            ccontext,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| cannot_cast_up(pstate, resulttype, target_type_id, location));
    }
    Ok(sbsref_node)
}

#[cold]
fn star_expansion_not_supported(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("row expansion via \"*\" is not supported here")
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}

#[cold]
fn field_of_noncomposite(
    pstate: &ParseState<'_, '_>,
    fieldname: &str,
    target_name: &str,
    target_type: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let t = format_type::format_type_be(target_type).unwrap_or_else(|_| target_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "cannot assign to field \"{fieldname}\" of column \"{target_name}\" \
                 because its type {t} is not a composite type"
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}

#[cold]
fn no_such_field(
    pstate: &ParseState<'_, '_>,
    fieldname: &str,
    target_name: &str,
    target_type: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERROR};
    let t = format_type::format_type_be(target_type).unwrap_or_else(|_| target_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "cannot assign to field \"{fieldname}\" of column \"{target_name}\" \
                 because there is no such column in data type {t}"
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}

#[cold]
fn assign_system_column_field(
    pstate: &ParseState<'_, '_>,
    fieldname: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("cannot assign to system column \"{fieldname}\""))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}

#[cold]
fn subfield_type_mismatch(
    pstate: &ParseState<'_, '_>,
    target_name: &str,
    target_type: types_core::Oid,
    rhs_type: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let t = format_type::format_type_be(target_type).unwrap_or_else(|_| target_type.to_string());
    let r = format_type::format_type_be(rhs_type).unwrap_or_else(|_| rhs_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "subfield \"{target_name}\" is of type {t} but expression is of type {r}"
            ))
            .errhint("You will need to rewrite or cast the expression.".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}

#[cold]
fn cannot_cast_up(
    pstate: &ParseState<'_, '_>,
    from: types_core::Oid,
    to: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_CANNOT_COERCE, ERROR};
    let f = format_type::format_type_be(from).unwrap_or_else(|_| from.to_string());
    let t = format_type::format_type_be(to).unwrap_or_else(|_| to.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(format!("cannot cast type {f} to {t}"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentSubscripts",
            )),
    )
}

#[cold]
fn subscript_assign_type_mismatch(
    pstate: &ParseState<'_, '_>,
    target_name: &str,
    target_type: types_core::Oid,
    rhs_type: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let t = format_type::format_type_be(target_type).unwrap_or_else(|_| target_type.to_string());
    let r = format_type::format_type_be(rhs_type).unwrap_or_else(|_| rhs_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "subscripted assignment to \"{target_name}\" requires type {t} \
                 but expression is of type {r}"
            ))
            .errhint("You will need to rewrite or cast the expression.".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_target.c",
                0,
                "transformAssignmentIndirection",
            )),
    )
}
