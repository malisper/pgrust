//! transformContainerSubscripts (parse_node.c) + the array typsubscript
//! transform (arraysubs.c). The SubscriptRoutines set is closed — dispatch is
//! on the pg_type.typsubscript proc OID, not a function-pointer table.

use mcx::Mcx;
use parser_small1::{parser_errposition, transformContainerType, ParseExprKind, ParseState};
use types_core::{InvalidOid, Oid, ParseLoc, INT4OID, JSONBOID, TEXTOID, UNKNOWNOID};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERROR,
};
use types_nodes::{Node, NodeList, OptNodeList, SubscriptingRef};

use crate::{expr_location, expr_type};

const MAXDIM: usize = 6;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
const F_RAW_ARRAY_SUBSCRIPT_HANDLER: Oid = 6180;
const F_JSONB_SUBSCRIPT_HANDLER: Oid = 6098;

// getSubscriptingRoutines' closed handler set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubscriptHandler {
    Array,
    Jsonb,
    Hstore,
}

pub fn subscript_handler_for(container_type: Oid) -> PgResult<Option<(SubscriptHandler, Oid)>> {
    let (typsubscript, typelem) = lsyscache::typ::get_typsubscript(container_type)?;
    match typsubscript as Oid {
        0 => Ok(None),
        F_ARRAY_SUBSCRIPT_HANDLER | F_RAW_ARRAY_SUBSCRIPT_HANDLER => {
            Ok(Some((SubscriptHandler::Array, typelem)))
        }
        F_JSONB_SUBSCRIPT_HANDLER => Ok(Some((SubscriptHandler::Jsonb, typelem))),
        // Extension handlers carry dynamic oids; match by proname (the GIN
        // TrgmOps precedent).
        other => {
            let cx = ::mcx::MemoryContext::new("subscript handler probe");
            let name = lsyscache::get_func_name(cx.mcx(), other)?
                .map(|n| n.as_str().to_string());
            match name.as_deref() {
                Some("hstore_subscript_handler") => Ok(Some((SubscriptHandler::Hstore, typelem))),
                _ => panic!("getSubscriptingRoutines: typsubscript handler {other} unported"),
            }
        }
    }
}

#[track_caller]
#[cold]
fn cannot_subscript(
    pstate: &ParseState<'_, '_>,
    container_type: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let t = format_type::format_type_be(container_type)
        .unwrap_or_else(|_| container_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "cannot subscript type {t} because it does not support subscripting"
            ))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_node.c",
                0,
                "transformContainerSubscripts",
            )),
    )
}

#[track_caller]
#[cold]
fn subscript_not_integer(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("array subscript must have type integer".to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "arraysubs.c",
                0,
                "array_subscript_transform",
            )),
    )
}

pub fn transformContainerSubscripts<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    container_base: Node<'mcx>,
    mut container_type: Oid,
    mut container_typmod: i32,
    indirection: &NodeList<'mcx>,
    is_assignment: bool,
) -> PgResult<Node<'mcx>> {
    debug_assert!(!indirection.is_nil());
    if !is_assignment {
        transformContainerType(&mut container_type, &mut container_typmod)?;
    }

    let Some((handler, element_type)) = subscript_handler_for(container_type)? else {
        return Err(cannot_subscript(pstate, container_type, expr_location(container_base)));
    };

    let mut is_slice = false;
    for el in indirection.iter() {
        let ai = el.as_a_indices().expect("subscript list element is A_Indices");
        if ai.is_slice {
            is_slice = true;
            break;
        }
    }

    let mut sbsref = Node::build::<SubscriptingRef>(mcx)?;
    sbsref.refcontainertype = container_type;
    sbsref.refelemtype = element_type;
    sbsref.reftypmod = container_typmod;
    sbsref.refexpr = Some(container_base);

    match handler {
        SubscriptHandler::Array => {
            array_subscript_transform(mcx, pstate, &mut sbsref, indirection, is_slice)?;
        }
        SubscriptHandler::Jsonb => {
            jsonb_subscript_transform(mcx, pstate, &mut sbsref, indirection, is_slice)?;
        }
        SubscriptHandler::Hstore => {
            hstore_subscript_transform(mcx, pstate, &mut sbsref, indirection, is_slice)?;
        }
    }

    if sbsref.refrestype == InvalidOid {
        return Err(cannot_subscript(pstate, container_type, expr_location(container_base)));
    }
    Ok(sbsref.seal())
}

// arraysubs.c array_subscript_transform: coerce subscripts to int4, split
// upper/lower lists, set refrestype.
fn array_subscript_transform<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    sbsref: &mut SubscriptingRef<'mcx>,
    indirection: &NodeList<'mcx>,
    is_slice: bool,
) -> PgResult<()> {
    let mut upper: OptNodeList<'mcx> = OptNodeList::nil();
    let mut lower: OptNodeList<'mcx> = OptNodeList::nil();
    let expr_kind = pstate.p_expr_kind;

    for el in indirection.iter() {
        let ai = el.as_a_indices().unwrap();

        if is_slice {
            if let Some(lidx) = ai.lidx {
                let sub = coerce_subscript(mcx, pstate, lidx, expr_kind)?;
                lower.lappend(mcx, Some(sub))?;
            } else if !ai.is_slice {
                let one = Node::mk_const(
                    mcx,
                    INT4OID,
                    -1,
                    InvalidOid,
                    4,
                    ::datum::Datum::from_i32(1),
                    false,
                    true,
                )?;
                lower.lappend(mcx, Some(one))?;
            } else {
                lower.lappend(mcx, None)?;
            }
        }

        if let Some(uidx) = ai.uidx {
            let sub = coerce_subscript(mcx, pstate, uidx, expr_kind)?;
            upper.lappend(mcx, Some(sub))?;
        } else {
            debug_assert!(is_slice && ai.is_slice);
            upper.lappend(mcx, None)?;
        }
    }

    if upper.len() > MAXDIM {
        return Err(Box::new(
            PgError::error(format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({MAXDIM})",
                upper.len()
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }

    sbsref.refupperindexpr = upper;
    sbsref.reflowerindexpr = lower;
    sbsref.refrestype =
        if is_slice { sbsref.refcontainertype } else { sbsref.refelemtype };
    Ok(())
}

#[track_caller]
#[cold]
fn jsonb_no_slices(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("jsonb subscript does not support slices".to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "jsonbsubs.c",
                0,
                "jsonb_subscript_transform",
            )),
    )
}

#[track_caller]
#[cold]
fn jsonb_bad_subscript_type(
    pstate: &ParseState<'_, '_>,
    subexpr_type: Oid,
    hint: &'static str,
    location: ParseLoc,
) -> Box<PgError> {
    let t = format_type::format_type_be(subexpr_type)
        .unwrap_or_else(|_| subexpr_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("subscript type {t} is not supported"))
            .errhint(hint.to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "jsonbsubs.c",
                0,
                "jsonb_subscript_transform",
            )),
    )
}

// jsonbsubs.c jsonb_subscript_transform: coerce each subscript to exactly one
// of int4/text (implicit context), no slices, result type always jsonb.
fn jsonb_subscript_transform<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    sbsref: &mut SubscriptingRef<'mcx>,
    indirection: &NodeList<'mcx>,
    is_slice: bool,
) -> PgResult<()> {
    let mut upper: OptNodeList<'mcx> = OptNodeList::nil();
    let expr_kind = pstate.p_expr_kind;

    for el in indirection.iter() {
        let ai = el.as_a_indices().unwrap();

        if is_slice {
            let expr = ai.uidx.or(ai.lidx);
            return Err(jsonb_no_slices(
                pstate,
                expr.map_or(-1, expr_location),
            ));
        }
        let Some(uidx) = ai.uidx else {
            debug_assert!(is_slice && ai.is_slice);
            return Err(jsonb_no_slices(pstate, -1));
        };

        let sub = crate::transformExpr(mcx, pstate, uidx, expr_kind)?;
        let sub_type = expr_type(sub);
        let target_type = if sub_type != UNKNOWNOID {
            let mut target = UNKNOWNOID;
            for cand in [INT4OID, TEXTOID] {
                if coerce::can_coerce_type(&[sub_type], &[cand], coerce::COERCION_IMPLICIT)? {
                    if target != UNKNOWNOID {
                        return Err(jsonb_bad_subscript_type(
                            pstate,
                            sub_type,
                            "jsonb subscript must be coercible to only one type, integer or text.",
                            expr_location(sub),
                        ));
                    }
                    target = cand;
                }
            }
            if target == UNKNOWNOID {
                return Err(jsonb_bad_subscript_type(
                    pstate,
                    sub_type,
                    "jsonb subscript must be coercible to either integer or text.",
                    expr_location(sub),
                ));
            }
            target
        } else {
            TEXTOID
        };

        let coerced = coerce::coerce_type(
            mcx,
            pstate,
            sub,
            sub_type,
            target_type,
            -1,
            coerce::COERCION_IMPLICIT,
            types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        upper.lappend(mcx, Some(coerced))?;
    }

    sbsref.refupperindexpr = upper;
    sbsref.reflowerindexpr = OptNodeList::nil();
    sbsref.refrestype = JSONBOID;
    sbsref.reftypmod = -1;
    Ok(())
}

#[track_caller]
#[cold]
fn hstore_one_subscript(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("hstore allows only one subscript".to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "hstore_subs.c",
                0,
                "hstore_subscript_transform",
            )),
    )
}

#[track_caller]
#[cold]
fn hstore_subscript_not_text(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("hstore subscript must have type text".to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "hstore_subs.c",
                0,
                "hstore_subscript_transform",
            )),
    )
}

// hstore_subscript_transform (hstore_subs.c): single non-slice subscript
// coerced to text (assignment coercion); result type always text.
fn hstore_subscript_transform<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    sbsref: &mut SubscriptingRef<'mcx>,
    indirection: &NodeList<'mcx>,
    is_slice: bool,
) -> PgResult<()> {
    let expr_kind = pstate.p_expr_kind;
    if is_slice || indirection.len() != 1 {
        // C: parser_errposition(exprLocation((Node *) indirection)) — List of
        // A_Indices, and A_Indices has no exprLocation arm => -1, no position.
        return Err(hstore_one_subscript(pstate, -1));
    }
    let ai = indirection.iter().next().unwrap().as_a_indices().unwrap();
    let uidx = ai.uidx.expect("non-slice A_Indices has uidx");

    let sub = crate::transformExpr(mcx, pstate, uidx, expr_kind)?;
    let coerced = coerce::coerce_to_target_type(
        mcx,
        pstate,
        sub,
        expr_type(sub),
        TEXTOID,
        -1,
        coerce::COERCION_ASSIGNMENT,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?
    .ok_or_else(|| hstore_subscript_not_text(pstate, expr_location(uidx)))?;

    let mut upper: OptNodeList<'mcx> = OptNodeList::nil();
    upper.lappend(mcx, Some(coerced))?;
    sbsref.refupperindexpr = upper;
    sbsref.reflowerindexpr = OptNodeList::nil();
    sbsref.refrestype = TEXTOID;
    sbsref.reftypmod = -1;
    Ok(())
}

fn coerce_subscript<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    idx: Node<'mcx>,
    expr_kind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    let sub = crate::transformExpr(mcx, pstate, idx, expr_kind)?;
    let coerced = coerce::coerce_to_target_type(
        mcx,
        pstate,
        sub,
        expr_type(sub),
        INT4OID,
        -1,
        coerce::COERCION_ASSIGNMENT,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    coerced.ok_or_else(|| subscript_not_integer(pstate, expr_location(idx)))
}
