//! Family: **nodefuncs** — `nodes/nodeFuncs.c` (4838 lines).
//!
//! General-purpose manipulations of the expression-`Node` tree, keyed on the
//! split layered `types_nodes::Expr` vocabulary (the F0-expanded Expr enum that
//! landed with `assemble/expr-eval-keystone`). This family owns
//! `backend-nodes-nodeFuncs-seams` and installs the expression-inspection seams
//! from [`init_seams`].
//!
//! # What is ported (faithful, field-for-field vs nodeFuncs.c)
//!
//! * type/typmod/collation accessors — [`expr_type`], [`expr_typmod`],
//!   [`expr_collation`], [`expr_input_collation`], [`expr_set_collation`],
//!   [`expr_set_input_collation`], [`expr_location`];
//! * coercion helpers — [`expr_is_length_coercion`], [`apply_relabel_type`],
//!   [`relabel_to_typmod`], [`strip_implicit_coercions`],
//!   [`expression_returns_set`];
//! * opfuncid fixups — [`set_opfuncid`], [`set_sa_opfuncid`],
//!   [`fix_opfuncids`], and [`check_functions_in_node`];
//! * the generic recursion — [`expression_tree_walker`] /
//!   [`expression_tree_mutator`].
//!
//! Each accessor follows the C switch arms exactly: the same field is read for
//! the same tag, recursion descends into the same children in the same order.
//! The `default` arm matches C — `expr_type`/`expr_collation`/
//! `expr_set_collation` raise the internal `unrecognized node type` error (the
//! C `elog(ERROR)` longjmps out of an `Oid`-returning function, surfaced here
//! as `PgResult::Err`); `expr_typmod`/`expr_input_collation`/`expr_location`
//! return the documented `-1`/`InvalidOid` fallback.
//!
//! # Split-model coverage and the trimmed surface
//!
//! `types_nodes::Expr` carries the ~48 execution-time expression variants. The
//! C switches also have arms for node types the layered model does not yet
//! carry as `Expr` variants (`PlaceHolderVar`, `JsonBehavior`) or that belong
//! to the not-yet-ported parser/planner node universes (`A_Expr`, `ColumnRef`,
//! `FromExpr`, `JoinExpr`, `Query`, `RangeTblEntry`, the raw-grammar and
//! `PlanState` trees). Those arms are simply absent here: the trimmed model
//! cannot construct those nodes, so they are unreachable rather than stubbed.
//! Likewise the per-node `location` field is trimmed model-wide (docs/types.md
//! rule 3), so [`expr_location`] runs the real `leftmostLoc` recursion over the
//! modeled compound variants but bottoms out at the documented `-1` ("location
//! can't be determined") at the leaves.
//!
//! # Callback shape
//!
//! C's `tree_walker_callback` is `bool (*)(Node *, void *context)` and
//! `tree_mutator_callback` is `Node *(*)(Node *, void *context)`; the `void
//! *context` is folded into the closure environment. A walker is
//! `&mut dyn FnMut(&Expr) -> bool` (return `true` to abort); a mutator is
//! `&mut dyn FnMut(Expr) -> Expr` (consume a child, return its replacement).
//!
//! # Not modeled here (genuine unported owners)
//!
//! `query_tree_walker`/`query_tree_mutator`/`range_table_walker`/
//! `range_table_entry_walker`/`raw_expression_tree_walker`/
//! `planstate_tree_walker` traverse the `Query`/`RangeTblEntry`/raw-grammar/
//! `PlanState` node universes, which the parser/planner/executor units that own
//! them have not yet ported into the layered tree (the layered `Query`/
//! `RangeTblEntry` carriers are trimmed and do not expose their expression
//! subtrees as walkable `Expr` trees). They stay stub-free by being absent
//! — there is no faithful body to write against the current model.

#![allow(non_snake_case)]

use types_core::{Oid, InvalidOid};
use types_nodes::primnodes::{
    self, etag, ArrayExpr, CaseExpr, CaseWhen, Const, Expr, JsonConstructorExpr, JsonExpr, OpExpr,
    RelabelType, ScalarArrayOpExpr, SubLink,
};
use types_nodes::primnodes::{CoercionForm, SubLinkType, XmlExprOp};
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_adt_format_type_seams as format_type;
use types_error::{PgResult, ERRCODE_UNDEFINED_OBJECT};

// Well-known pg_type / pg_collation OIDs (pg_type.dat / pg_collation.dat).
// types-core exports BOOLOID / INT4OID / C_COLLATION_OID; the rest are spelled
// here from the catalog headers (real OIDs, inherited — not invented).
use types_core::catalog::{BOOLOID, C_COLLATION_OID, INT4OID};
/// `RECORDOID` (pg_type.dat).
const RECORDOID: Oid = 2249;
/// `TEXTOID` (pg_type.dat).
const TEXTOID: Oid = 25;
/// `XMLOID` (pg_type.dat).
const XMLOID: Oid = 142;
/// `NAMEOID` (pg_type.dat).
const NAMEOID: Oid = 19;
/// `DEFAULT_COLLATION_OID` (pg_collation.dat).
const DEFAULT_COLLATION_OID: Oid = 100;

/// `OidIsValid(oid)` (c.h) — nonzero.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

// ===========================================================================
// exprType — Oid of the type of the expression's result. (nodeFuncs.c:41)
// ===========================================================================

/// `exprType(expr)` (nodeFuncs.c) — the OID of the type of the expression's
/// result. A NULL expression (`None`) maps to the C `if (!expr) return
/// InvalidOid;`. `Err` carries the C `elog(ERROR, "unrecognized node type")`
/// (here only reachable for an `ARRAY_SUBLINK` whose element type has no array
/// type — the `get_promoted_array_type` lookup failure).
pub fn expr_type(expr: Option<&Expr>) -> PgResult<Oid> {
    let Some(expr) = expr else {
        return Ok(InvalidOid);
    };
    let type_ = match expr.expr_tag() {
        etag::T_Var => expr.as_var().unwrap().vartype,
        etag::T_Const => expr.as_const().unwrap().consttype,
        etag::T_Param => expr.as_param().unwrap().paramtype,
        etag::T_Aggref => expr.as_aggref().unwrap().aggtype,
        etag::T_GroupingFunc => INT4OID,
        etag::T_WindowFunc => expr.as_windowfunc().unwrap().wintype,
        etag::T_MergeSupportFunc => expr.as_mergesupportfunc().unwrap().msftype,
        etag::T_SubscriptingRef => expr.as_subscriptingref().unwrap().refrestype,
        etag::T_FuncExpr => expr.as_funcexpr().unwrap().funcresulttype,
        etag::T_NamedArgExpr => expr_type(expr.as_namedargexpr().unwrap().arg.as_deref())?,
        etag::T_OpExpr => expr.as_opexpr().unwrap().opresulttype,
        etag::T_DistinctExpr => expr.as_distinctexpr().unwrap().opresulttype,
        etag::T_NullIfExpr => expr.as_nullifexpr().unwrap().opresulttype,
        etag::T_ScalarArrayOpExpr => BOOLOID,
        etag::T_BoolExpr => BOOLOID,
        etag::T_SubLink => sublink_result_type(expr.as_sublink().unwrap())?,
        etag::T_SubPlan => {
            let subplan = &expr.as_subplan().unwrap().0;
            if subplan.subLinkType == SubLinkType::Expr
                || subplan.subLinkType == SubLinkType::Array
            {
                let mut t = subplan.firstColType;
                if subplan.subLinkType == SubLinkType::Array {
                    t = lsyscache::get_promoted_array_type::call(t)?;
                    if !oid_is_valid(t) {
                        return Err(no_array_type_error(subplan.firstColType)?);
                    }
                }
                t
            } else if subplan.subLinkType == SubLinkType::MultiExpr {
                RECORDOID
            } else {
                BOOLOID
            }
        }
        etag::T_AlternativeSubPlan => {
            // subplans should all return the same thing
            expr.as_alternativesubplan().unwrap().0.subplans[0].firstColType_via_sublink()?
        }
        etag::T_FieldSelect => expr.as_fieldselect().unwrap().resulttype,
        etag::T_FieldStore => expr.as_fieldstore().unwrap().resulttype,
        etag::T_RelabelType => expr.as_relabeltype().unwrap().resulttype,
        etag::T_CoerceViaIO => expr.as_coerceviaio().unwrap().resulttype,
        etag::T_ArrayCoerceExpr => expr.as_arraycoerceexpr().unwrap().resulttype,
        etag::T_ConvertRowtypeExpr => expr.as_convertrowtypeexpr().unwrap().resulttype,
        etag::T_CollateExpr => expr_type(expr.as_collateexpr().unwrap().arg.as_deref())?,
        etag::T_CaseExpr => expr.as_caseexpr().unwrap().casetype,
        etag::T_CaseTestExpr => expr.as_casetestexpr().unwrap().typeId,
        etag::T_ArrayExpr => expr.as_arrayexpr().unwrap().array_typeid,
        etag::T_RowExpr => expr.as_rowexpr().unwrap().row_typeid,
        etag::T_RowCompareExpr => BOOLOID,
        etag::T_CoalesceExpr => expr.as_coalesceexpr().unwrap().coalescetype,
        etag::T_MinMaxExpr => expr.as_minmaxexpr().unwrap().minmaxtype,
        etag::T_SQLValueFunction => expr.as_sqlvaluefunction().unwrap().r#type,
        etag::T_XmlExpr => {
            let x = expr.as_xmlexpr().unwrap();
            if x.op == XmlExprOp::IS_DOCUMENT {
                BOOLOID
            } else if x.op == XmlExprOp::IS_XMLSERIALIZE {
                TEXTOID
            } else {
                XMLOID
            }
        }
        etag::T_JsonValueExpr => {
            expr_type(expr.as_jsonvalueexpr().unwrap().formatted_expr.as_deref())?
        }
        etag::T_JsonConstructorExpr => {
            json_returning_typid(expr.as_jsonconstructorexpr().unwrap())
        }
        etag::T_JsonIsPredicate => BOOLOID,
        etag::T_JsonExpr => json_expr_returning_typid(expr.as_jsonexpr().unwrap()),
        etag::T_NullTest => BOOLOID,
        etag::T_BooleanTest => BOOLOID,
        etag::T_CoerceToDomain => expr.as_coercetodomain().unwrap().resulttype,
        etag::T_CoerceToDomainValue => expr.as_coercetodomainvalue().unwrap().typeId,
        etag::T_SetToDefault => expr.as_settodefault().unwrap().typeId,
        etag::T_CurrentOfExpr => BOOLOID,
        etag::T_NextValueExpr => expr.as_nextvalueexpr().unwrap().typeId,
        etag::T_InferenceElem => expr_type(expr.as_inferenceelem().unwrap().expr.as_deref())?,
        etag::T_ReturningExpr => {
            expr_type(expr.as_returningexpr().unwrap().retexpr.as_deref())?
        }
        etag::T_PlaceHolderVar => {
            // C: exprType((Node *) ((const PlaceHolderVar *) expr)->phexpr)
            expr_type(expr.as_placeholdervar().unwrap().phexpr.as_deref())?
        }
        // `Expr` is #[non_exhaustive]; an unmodeled future variant is the C
        // default: elog(ERROR, "unrecognized node type").
        _ => return Err(unrecognized_node_type_error(expr_variant_name(expr))?),
    };
    Ok(type_)
}

/// `linitial_node(TargetEntry, qtree->targetList)` over the embedded sub-Query:
/// the first target entry's expr. C asserts the embedded subselect IsA Query
/// and the first target entry is not resjunk; we surface the missing-Query case
/// as the C "untransformed sublink" elog.
fn sublink_first_target_expr<'a, 'mcx>(
    sublink: &'a SubLink<'mcx>,
    what: &str,
) -> PgResult<&'a Expr<'mcx>> {
    let qtree = sublink
        .subselect
        .as_ref()
        .ok_or_else(|| untransformed_sublink_error_t(what))?;
    let tent = qtree
        .targetList
        .first()
        .ok_or_else(|| untransformed_sublink_error_t(what))?;
    debug_assert!(!tent.resjunk);
    tent.expr
        .as_deref()
        .ok_or_else(|| untransformed_sublink_error_t(what))
}

/// The `EXPR_SUBLINK`/`ARRAY_SUBLINK`/`MULTIEXPR_SUBLINK`/boolean dispatch of
/// `exprType` over a `SubLink`. For EXPR/ARRAY the result type is that of the
/// subselect's first target column (ARRAY promotes it to its array type);
/// MULTIEXPR is RECORD; everything else is BOOLEAN.
fn sublink_result_type(sublink: &SubLink) -> PgResult<Oid> {
    if sublink.subLinkType == SubLinkType::Expr
        || sublink.subLinkType == SubLinkType::Array
    {
        let tent_expr = sublink_first_target_expr(sublink, "type")?;
        let mut type_ = expr_type(Some(tent_expr))?;
        if sublink.subLinkType == SubLinkType::Array {
            type_ = lsyscache::get_promoted_array_type::call(type_)?;
            if !oid_is_valid(type_) {
                return Err(no_array_type_error(expr_type(Some(tent_expr))?)?);
            }
        }
        Ok(type_)
    } else if sublink.subLinkType == SubLinkType::MultiExpr {
        Ok(RECORDOID)
    } else {
        Ok(BOOLOID)
    }
}

fn json_returning_typid(ctor: &JsonConstructorExpr) -> Oid {
    match &ctor.returning {
        Some(r) => r.typid,
        None => InvalidOid,
    }
}

fn json_expr_returning_typid(jexpr: &JsonExpr) -> Oid {
    match &jexpr.returning {
        Some(r) => r.typid,
        None => InvalidOid,
    }
}

// ===========================================================================
// exprTypmod (nodeFuncs.c:300)
// ===========================================================================

/// `exprTypmod(expr)` (nodeFuncs.c) — the type-specific modifier of the
/// expression's result type, or `-1` if it can't be determined.
pub fn expr_typmod(expr: Option<&Expr>) -> PgResult<i32> {
    let Some(expr) = expr else {
        return Ok(-1);
    };
    let typmod = match expr.expr_tag() {
        etag::T_Var => expr.as_var().unwrap().vartypmod,
        etag::T_Const => expr.as_const().unwrap().consttypmod,
        etag::T_Param => expr.as_param().unwrap().paramtypmod,
        etag::T_SubscriptingRef => expr.as_subscriptingref().unwrap().reftypmod,
        etag::T_FuncExpr => {
            // Be smart about length-coercion functions...
            let (is_len, coerced) = expr_is_length_coercion(Some(expr))?;
            if is_len {
                coerced
            } else {
                -1
            }
        }
        etag::T_NamedArgExpr => expr_typmod(expr.as_namedargexpr().unwrap().arg.as_deref())?,
        etag::T_NullIfExpr => {
            // result is first argument or NULL → report first arg's typmod
            expr_typmod(expr.as_nullifexpr().unwrap().args.first())?
        }
        etag::T_SubLink => {
            let sublink = expr.as_sublink().unwrap();
            if sublink.subLinkType == SubLinkType::Expr
                || sublink.subLinkType == SubLinkType::Array
            {
                // typmod of the subselect's first target column (we don't need
                // to care whether it's an array).
                let tent_expr = sublink_first_target_expr(sublink, "type")?;
                return expr_typmod(Some(tent_expr));
            }
            -1
        }
        etag::T_SubPlan => {
            let subplan = &expr.as_subplan().unwrap().0;
            if subplan.subLinkType == SubLinkType::Expr
                || subplan.subLinkType == SubLinkType::Array
            {
                subplan.firstColTypmod
            } else {
                -1
            }
        }
        etag::T_AlternativeSubPlan => {
            let sp = &expr.as_alternativesubplan().unwrap().0.subplans[0];
            if sp.subLinkType == SubLinkType::Expr
                || sp.subLinkType == SubLinkType::Array
            {
                sp.firstColTypmod
            } else {
                -1
            }
        }
        etag::T_FieldSelect => expr.as_fieldselect().unwrap().resulttypmod,
        etag::T_RelabelType => expr.as_relabeltype().unwrap().resulttypmod,
        etag::T_ArrayCoerceExpr => expr.as_arraycoerceexpr().unwrap().resulttypmod,
        etag::T_CollateExpr => expr_typmod(expr.as_collateexpr().unwrap().arg.as_deref())?,
        etag::T_CaseExpr => case_expr_typmod(expr.as_caseexpr().unwrap())?,
        etag::T_CaseTestExpr => expr.as_casetestexpr().unwrap().typeMod,
        etag::T_ArrayExpr => array_expr_typmod(expr.as_arrayexpr().unwrap())?,
        etag::T_CoalesceExpr => {
            let cexpr = expr.as_coalesceexpr().unwrap();
            agree_typmod(&cexpr.args, cexpr.coalescetype)?
        }
        etag::T_MinMaxExpr => {
            let mexpr = expr.as_minmaxexpr().unwrap();
            agree_typmod(&mexpr.args, mexpr.minmaxtype)?
        }
        etag::T_SQLValueFunction => expr.as_sqlvaluefunction().unwrap().typmod,
        etag::T_JsonValueExpr => {
            expr_typmod(expr.as_jsonvalueexpr().unwrap().formatted_expr.as_deref())?
        }
        etag::T_JsonConstructorExpr => match &expr.as_jsonconstructorexpr().unwrap().returning {
            Some(r) => r.typmod,
            None => -1,
        },
        etag::T_JsonExpr => match &expr.as_jsonexpr().unwrap().returning {
            Some(r) => r.typmod,
            None => -1,
        },
        etag::T_CoerceToDomain => expr.as_coercetodomain().unwrap().resulttypmod,
        etag::T_CoerceToDomainValue => expr.as_coercetodomainvalue().unwrap().typeMod,
        etag::T_SetToDefault => expr.as_settodefault().unwrap().typeMod,
        etag::T_ReturningExpr => {
            expr_typmod(expr.as_returningexpr().unwrap().retexpr.as_deref())?
        }
        etag::T_PlaceHolderVar => {
            // C: exprTypmod((Node *) ((const PlaceHolderVar *) expr)->phexpr)
            expr_typmod(expr.as_placeholdervar().unwrap().phexpr.as_deref())?
        }
        _ => -1,
    };
    Ok(typmod)
}

/// `T_CaseExpr` arm of `exprTypmod`: if all alternatives agree on type/typmod,
/// return that typmod, else `-1`.
fn case_expr_typmod(cexpr: &CaseExpr) -> PgResult<i32> {
    let Some(defresult) = cexpr.defresult.as_deref() else {
        return Ok(-1);
    };
    let casetype = cexpr.casetype;
    if expr_type(Some(defresult))? != casetype {
        return Ok(-1);
    }
    let typmod = expr_typmod(Some(defresult))?;
    if typmod < 0 {
        return Ok(-1);
    }
    for w in &cexpr.args {
        let result = w.result.as_deref();
        if expr_type(result)? != casetype {
            return Ok(-1);
        }
        if expr_typmod(result)? != typmod {
            return Ok(-1);
        }
    }
    Ok(typmod)
}

/// `T_ArrayExpr` arm of `exprTypmod`.
fn array_expr_typmod(arrayexpr: &ArrayExpr) -> PgResult<i32> {
    if arrayexpr.elements.is_empty() {
        return Ok(-1);
    }
    let typmod = expr_typmod(arrayexpr.elements.first())?;
    if typmod < 0 {
        return Ok(-1);
    }
    let commontype = if arrayexpr.multidims {
        arrayexpr.array_typeid
    } else {
        arrayexpr.element_typeid
    };
    for e in &arrayexpr.elements {
        if expr_type(Some(e))? != commontype {
            return Ok(-1);
        }
        if expr_typmod(Some(e))? != typmod {
            return Ok(-1);
        }
    }
    Ok(typmod)
}

/// Shared `CoalesceExpr`/`MinMaxExpr` "all args agree on type/typmod" logic.
fn agree_typmod(args: &[Expr], commontype: Oid) -> PgResult<i32> {
    let Some(first) = args.first() else {
        return Ok(-1);
    };
    if expr_type(Some(first))? != commontype {
        return Ok(-1);
    }
    let typmod = expr_typmod(Some(first))?;
    if typmod < 0 {
        return Ok(-1);
    }
    for e in args.iter().skip(1) {
        if expr_type(Some(e))? != commontype {
            return Ok(-1);
        }
        if expr_typmod(Some(e))? != typmod {
            return Ok(-1);
        }
    }
    Ok(typmod)
}

// ===========================================================================
// exprIsLengthCoercion (nodeFuncs.c:556)
// ===========================================================================

/// `exprIsLengthCoercion(expr, &coercedTypmod)` (nodeFuncs.c) — detect whether
/// an expression is an application of a datatype's typmod-coercion function.
/// Returns `(is_length_coercion, coerced_typmod)`; the typmod is `-1` when the
/// expression is not a length coercion.
pub fn expr_is_length_coercion(expr: Option<&Expr>) -> PgResult<(bool, i32)> {
    // Scalar-type length coercions are FuncExprs; array-type ones are
    // ArrayCoerceExprs.
    match expr {
        Some(Expr::FuncExpr(func)) => {
            // If it didn't come from a coercion context, reject.
            if func.funcformat != CoercionForm::COERCE_EXPLICIT_CAST
                && func.funcformat != CoercionForm::COERCE_IMPLICIT_CAST
            {
                return Ok((false, -1));
            }
            // Must be a two- or three-argument function whose second argument
            // is a non-null int4 Const.
            let nargs = func.args.len();
            if nargs < 2 || nargs > 3 {
                return Ok((false, -1));
            }
            let Expr::Const(second_arg) = &func.args[1] else {
                return Ok((false, -1));
            };
            if second_arg.consttype != INT4OID || second_arg.constisnull {
                return Ok((false, -1));
            }
            // OK, it is indeed a length-coercion function.
            // DatumGetInt32(second_arg->constvalue).
            let coerced = second_arg.constvalue.as_i32();
            Ok((true, coerced))
        }
        Some(Expr::ArrayCoerceExpr(acoerce)) => {
            // Not a length coercion unless there's a nondefault typmod.
            if acoerce.resulttypmod < 0 {
                Ok((false, -1))
            } else {
                Ok((true, acoerce.resulttypmod))
            }
        }
        _ => Ok((false, -1)),
    }
}

// ===========================================================================
// applyRelabelType / relabel_to_typmod (nodeFuncs.c:635)
// ===========================================================================

/// `applyRelabelType(arg, rtype, rtypmod, rcollid, rformat, rlocation,
/// overwrite_ok)` (nodeFuncs.c) — add a `RelabelType` node if needed to make
/// the expression expose the specified type/typmod/collation. Maintains the
/// post-`eval_const_expressions` invariants (no adjacent RelabelTypes; no
/// RelabelType atop a Const).
///
/// The C `rlocation` parameter is not yet threaded by this crate's callers
/// (it would ripple the `ec_seam::apply_relabel_type` seam contract and the
/// concurrently-edited clauses/equivclass consumers); the new `RelabelType` is
/// built with `location = -1`, matching the prior behavior. The Const path
/// keeps the Const's original `location` (it is cloned), per the C comment.
pub fn apply_relabel_type(
    mut arg: Expr,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
    rlocation: i32,
    _overwrite_ok: bool,
) -> PgResult<Expr> {
    // Discard stacked RelabelTypes (eg foo::int::oid).
    while let Expr::RelabelType(r) = &arg {
        match &r.arg {
            Some(inner) => arg = (**inner).clone(),
            None => break,
        }
    }

    if let Expr::Const(con) = &arg {
        // Modify the Const to preserve const-flatness. In the owned model the
        // Const is value-typed, so we always produce the updated copy
        // (overwrite-vs-copy is an in-place-pointer optimization in C with no
        // observable difference here).
        let mut con: Const = con.clone();
        con.consttype = rtype;
        con.consttypmod = rtypmod;
        con.constcollid = rcollid;
        return Ok(Expr::Const(con));
    }

    if expr_type(Some(&arg))? == rtype
        && expr_typmod(Some(&arg))? == rtypmod
        && expr_collation(Some(&arg))? == rcollid
    {
        // A nest of relabels that nets out to nothing.
        return Ok(arg);
    }

    // Nope, gotta have a RelabelType.
    Ok(Expr::RelabelType(RelabelType {
        arg: Some(Box::new(arg)),
        resulttype: rtype,
        resulttypmod: rtypmod,
        resultcollid: rcollid,
        relabelformat: rformat,
        location: rlocation,
    }))
}

/// `relabel_to_typmod(expr, typmod)` (nodeFuncs.c) — add a RelabelType that
/// changes just the typmod of the expression.
pub fn relabel_to_typmod(expr: Expr, typmod: i32) -> PgResult<Expr> {
    let rtype = expr_type(Some(&expr))?;
    let rcollid = expr_collation(Some(&expr))?;
    apply_relabel_type(
        expr,
        rtype,
        typmod,
        rcollid,
        CoercionForm::COERCE_EXPLICIT_CAST,
        -1,
        false,
    )
}

// ===========================================================================
// strip_implicit_coercions (nodeFuncs.c:704)
// ===========================================================================

/// `strip_implicit_coercions(node)` (nodeFuncs.c) — remove implicit coercions
/// at the top level of the tree, returning a borrow into a suitable place
/// within it. (A RowExpr is returned unchanged even if implicit.)
pub fn strip_implicit_coercions<'a, 'mcx>(node: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    match node {
        Expr::FuncExpr(f) if f.funcformat == CoercionForm::COERCE_IMPLICIT_CAST => {
            match f.args.first() {
                Some(first) => strip_implicit_coercions(first),
                None => node,
            }
        }
        Expr::RelabelType(r) if r.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST => {
            match &r.arg {
                Some(arg) => strip_implicit_coercions(arg),
                None => node,
            }
        }
        Expr::CoerceViaIO(c) if c.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST => {
            match &c.arg {
                Some(arg) => strip_implicit_coercions(arg),
                None => node,
            }
        }
        Expr::ArrayCoerceExpr(c) if c.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST => {
            match &c.arg {
                Some(arg) => strip_implicit_coercions(arg),
                None => node,
            }
        }
        Expr::ConvertRowtypeExpr(c)
            if c.convertformat == CoercionForm::COERCE_IMPLICIT_CAST =>
        {
            match &c.arg {
                Some(arg) => strip_implicit_coercions(arg),
                None => node,
            }
        }
        Expr::CoerceToDomain(c) if c.coercionformat == CoercionForm::COERCE_IMPLICIT_CAST => {
            match &c.arg {
                Some(arg) => strip_implicit_coercions(arg),
                None => node,
            }
        }
        _ => node,
    }
}

// ===========================================================================
// expression_returns_set (nodeFuncs.c:762)
// ===========================================================================

/// `expression_returns_set(clause)` (nodeFuncs.c) — test whether an expression
/// (or whole targetlist) returns a set result.
pub fn expression_returns_set(clause: Option<&Expr>) -> bool {
    expression_returns_set_walker(clause)
}

fn expression_returns_set_walker(node: Option<&Expr>) -> bool {
    let Some(node) = node else {
        return false;
    };
    match node {
        Expr::FuncExpr(expr) if expr.funcretset => return true,
        Expr::OpExpr(expr) if expr.opretset => return true,
        // Parser guarantees these never return a set; avoid recursing.
        Expr::Aggref(_) | Expr::GroupingFunc(_) | Expr::WindowFunc(_) => return false,
        _ => {}
    }
    let mut found = false;
    let mut walker = |child: &Expr| -> bool {
        if expression_returns_set_walker(Some(child)) {
            found = true;
            return true;
        }
        false
    };
    expression_tree_walker(Some(node), &mut walker);
    found
}

// ===========================================================================
// exprCollation (nodeFuncs.c:820)
// ===========================================================================

/// `exprCollation(expr)` (nodeFuncs.c) — the OID of the collation of the
/// expression's result. `Err` carries the C `elog(ERROR, "unrecognized node
/// type")` (only reachable on the untransformed-SubLink path).
pub fn expr_collation(expr: Option<&Expr>) -> PgResult<Oid> {
    let Some(expr) = expr else {
        return Ok(InvalidOid);
    };
    let coll = match expr.expr_tag() {
        etag::T_Var => expr.as_var().unwrap().varcollid,
        etag::T_Const => expr.as_const().unwrap().constcollid,
        etag::T_Param => expr.as_param().unwrap().paramcollid,
        etag::T_Aggref => expr.as_aggref().unwrap().aggcollid,
        etag::T_GroupingFunc => InvalidOid,
        etag::T_WindowFunc => expr.as_windowfunc().unwrap().wincollid,
        etag::T_MergeSupportFunc => expr.as_mergesupportfunc().unwrap().msfcollid,
        etag::T_SubscriptingRef => expr.as_subscriptingref().unwrap().refcollid,
        etag::T_FuncExpr => expr.as_funcexpr().unwrap().funccollid,
        etag::T_NamedArgExpr => expr_collation(expr.as_namedargexpr().unwrap().arg.as_deref())?,
        etag::T_OpExpr => expr.as_opexpr().unwrap().opcollid,
        etag::T_DistinctExpr => expr.as_distinctexpr().unwrap().opcollid,
        etag::T_NullIfExpr => expr.as_nullifexpr().unwrap().opcollid,
        etag::T_ScalarArrayOpExpr => InvalidOid,
        etag::T_BoolExpr => InvalidOid,
        etag::T_SubLink => {
            let sublink = expr.as_sublink().unwrap();
            if sublink.subLinkType == SubLinkType::Expr
                || sublink.subLinkType == SubLinkType::Array
            {
                // collation of the subselect's first target column (unchanged
                // by array conversion).
                let tent_expr = sublink_first_target_expr(sublink, "collation")?;
                return expr_collation(Some(tent_expr));
            }
            InvalidOid
        }
        etag::T_SubPlan => {
            let subplan = &expr.as_subplan().unwrap().0;
            if subplan.subLinkType == SubLinkType::Expr
                || subplan.subLinkType == SubLinkType::Array
            {
                subplan.firstColCollation
            } else {
                InvalidOid
            }
        }
        etag::T_AlternativeSubPlan => {
            expr.as_alternativesubplan().unwrap().0.subplans[0].firstColCollation
        }
        etag::T_FieldSelect => expr.as_fieldselect().unwrap().resultcollid,
        etag::T_FieldStore => InvalidOid,
        etag::T_RelabelType => expr.as_relabeltype().unwrap().resultcollid,
        etag::T_CoerceViaIO => expr.as_coerceviaio().unwrap().resultcollid,
        etag::T_ArrayCoerceExpr => expr.as_arraycoerceexpr().unwrap().resultcollid,
        etag::T_ConvertRowtypeExpr => InvalidOid,
        etag::T_CollateExpr => expr.as_collateexpr().unwrap().collOid,
        etag::T_CaseExpr => expr.as_caseexpr().unwrap().casecollid,
        etag::T_CaseTestExpr => expr.as_casetestexpr().unwrap().collation,
        etag::T_ArrayExpr => expr.as_arrayexpr().unwrap().array_collid,
        etag::T_RowExpr => InvalidOid,
        etag::T_RowCompareExpr => InvalidOid,
        etag::T_CoalesceExpr => expr.as_coalesceexpr().unwrap().coalescecollid,
        etag::T_MinMaxExpr => expr.as_minmaxexpr().unwrap().minmaxcollid,
        etag::T_SQLValueFunction => {
            if expr.as_sqlvaluefunction().unwrap().r#type == NAMEOID {
                C_COLLATION_OID
            } else {
                InvalidOid
            }
        }
        etag::T_XmlExpr => {
            if expr.as_xmlexpr().unwrap().op == XmlExprOp::IS_XMLSERIALIZE {
                DEFAULT_COLLATION_OID
            } else {
                InvalidOid
            }
        }
        etag::T_JsonValueExpr => {
            expr_collation(expr.as_jsonvalueexpr().unwrap().formatted_expr.as_deref())?
        }
        etag::T_JsonConstructorExpr => {
            match expr.as_jsonconstructorexpr().unwrap().coercion.as_deref() {
                Some(c) => expr_collation(Some(c))?,
                None => InvalidOid,
            }
        }
        etag::T_JsonIsPredicate => InvalidOid,
        etag::T_JsonExpr => expr.as_jsonexpr().unwrap().collation,
        etag::T_NullTest => InvalidOid,
        etag::T_BooleanTest => InvalidOid,
        etag::T_CoerceToDomain => expr.as_coercetodomain().unwrap().resultcollid,
        etag::T_CoerceToDomainValue => expr.as_coercetodomainvalue().unwrap().collation,
        etag::T_SetToDefault => expr.as_settodefault().unwrap().collation,
        etag::T_CurrentOfExpr => InvalidOid,
        etag::T_NextValueExpr => InvalidOid,
        etag::T_InferenceElem => {
            expr_collation(expr.as_inferenceelem().unwrap().expr.as_deref())?
        }
        etag::T_ReturningExpr => {
            expr_collation(expr.as_returningexpr().unwrap().retexpr.as_deref())?
        }
        etag::T_PlaceHolderVar => {
            // C: exprCollation((Node *) ((const PlaceHolderVar *) expr)->phexpr)
            expr_collation(expr.as_placeholdervar().unwrap().phexpr.as_deref())?
        }
        // #[non_exhaustive]: C default elog(ERROR, "unrecognized node type").
        _ => return Err(unrecognized_node_type_error(expr_variant_name(expr))?),
    };
    Ok(coll)
}

// ===========================================================================
// exprInputCollation (nodeFuncs.c:1075)
// ===========================================================================

/// `exprInputCollation(expr)` (nodeFuncs.c) — the collation a function should
/// use, or `InvalidOid` if the node type doesn't store it.
pub fn expr_input_collation(expr: Option<&Expr>) -> Oid {
    let Some(expr) = expr else {
        return InvalidOid;
    };
    match expr.expr_tag() {
        etag::T_Aggref => expr.as_aggref().unwrap().inputcollid,
        etag::T_WindowFunc => expr.as_windowfunc().unwrap().inputcollid,
        etag::T_FuncExpr => expr.as_funcexpr().unwrap().inputcollid,
        etag::T_OpExpr => expr.as_opexpr().unwrap().inputcollid,
        etag::T_DistinctExpr => expr.as_distinctexpr().unwrap().inputcollid,
        etag::T_NullIfExpr => expr.as_nullifexpr().unwrap().inputcollid,
        etag::T_ScalarArrayOpExpr => expr.as_scalararrayopexpr().unwrap().inputcollid,
        etag::T_MinMaxExpr => expr.as_minmaxexpr().unwrap().inputcollid,
        _ => InvalidOid,
    }
}

// ===========================================================================
// exprSetCollation (nodeFuncs.c:1123)
// ===========================================================================

/// `exprSetCollation(expr, collation)` (nodeFuncs.c) — assign collation
/// information to an expression-tree node. Mutates in place. The C
/// assert-only arms (where the result is non-collatable and the function only
/// checks `collation == InvalidOid`) are no-ops here, matching a non-asserting
/// build. `Err` carries the unrecognized-node-type error.
pub fn expr_set_collation(expr: &mut Expr, collation: Oid) -> PgResult<()> {
    match expr.expr_tag() {
        etag::T_Var => expr.as_var_mut().unwrap().varcollid = collation,
        etag::T_Const => expr.as_const_mut().unwrap().constcollid = collation,
        etag::T_Param => expr.as_param_mut().unwrap().paramcollid = collation,
        etag::T_Aggref => expr.as_aggref_mut().unwrap().aggcollid = collation,
        etag::T_GroupingFunc => {}
        etag::T_WindowFunc => expr.as_windowfunc_mut().unwrap().wincollid = collation,
        etag::T_MergeSupportFunc => expr.as_mergesupportfunc_mut().unwrap().msfcollid = collation,
        etag::T_SubscriptingRef => expr.as_subscriptingref_mut().unwrap().refcollid = collation,
        etag::T_FuncExpr => expr.as_funcexpr_mut().unwrap().funccollid = collation,
        etag::T_NamedArgExpr => {} // Assert(collation == exprCollation(arg))
        etag::T_OpExpr => expr.as_opexpr_mut().unwrap().opcollid = collation,
        etag::T_DistinctExpr => expr.as_distinctexpr_mut().unwrap().opcollid = collation,
        etag::T_NullIfExpr => expr.as_nullifexpr_mut().unwrap().opcollid = collation,
        etag::T_ScalarArrayOpExpr => {}
        etag::T_BoolExpr => {}
        etag::T_SubLink => {} // assert-only in C
        etag::T_FieldSelect => expr.as_fieldselect_mut().unwrap().resultcollid = collation,
        etag::T_FieldStore => {}
        etag::T_RelabelType => expr.as_relabeltype_mut().unwrap().resultcollid = collation,
        etag::T_CoerceViaIO => expr.as_coerceviaio_mut().unwrap().resultcollid = collation,
        etag::T_ArrayCoerceExpr => expr.as_arraycoerceexpr_mut().unwrap().resultcollid = collation,
        etag::T_ConvertRowtypeExpr => {}
        etag::T_CaseExpr => expr.as_caseexpr_mut().unwrap().casecollid = collation,
        etag::T_ArrayExpr => expr.as_arrayexpr_mut().unwrap().array_collid = collation,
        etag::T_RowExpr => {}
        etag::T_RowCompareExpr => {}
        etag::T_CoalesceExpr => expr.as_coalesceexpr_mut().unwrap().coalescecollid = collation,
        etag::T_MinMaxExpr => expr.as_minmaxexpr_mut().unwrap().minmaxcollid = collation,
        etag::T_SQLValueFunction => {} // assert-only
        etag::T_XmlExpr => {}          // assert-only
        etag::T_JsonValueExpr => {
            if let Some(fe) = expr.as_jsonvalueexpr_mut().unwrap().formatted_expr.as_deref_mut() {
                expr_set_collation(fe, collation)?;
            }
        }
        etag::T_JsonConstructorExpr => {
            if let Some(c) = expr.as_jsonconstructorexpr_mut().unwrap().coercion.as_deref_mut() {
                expr_set_collation(c, collation)?;
            }
        }
        etag::T_JsonIsPredicate => {}
        etag::T_JsonExpr => expr.as_jsonexpr_mut().unwrap().collation = collation,
        etag::T_NullTest => {}
        etag::T_BooleanTest => {}
        etag::T_CoerceToDomain => expr.as_coercetodomain_mut().unwrap().resultcollid = collation,
        etag::T_CoerceToDomainValue => {
            expr.as_coercetodomainvalue_mut().unwrap().collation = collation
        }
        etag::T_SetToDefault => expr.as_settodefault_mut().unwrap().collation = collation,
        etag::T_CurrentOfExpr => {}
        etag::T_NextValueExpr => {}
        // Per the C comment, exprSetCollation needn't worry about subplans,
        // PlaceHolderVars, or ReturningExprs (parse-analysis only).
        _ => {
            return Err(unrecognized_node_type_error(expr_variant_name(expr))?);
        }
    }
    Ok(())
}

// ===========================================================================
// exprSetInputCollation (nodeFuncs.c:1319)
// ===========================================================================

/// `exprSetInputCollation(expr, inputcollation)` (nodeFuncs.c) — assign
/// input-collation information; a no-op for node types that don't store it.
pub fn expr_set_input_collation(expr: &mut Expr, inputcollation: Oid) {
    match expr.expr_tag() {
        etag::T_Aggref => expr.as_aggref_mut().unwrap().inputcollid = inputcollation,
        etag::T_WindowFunc => expr.as_windowfunc_mut().unwrap().inputcollid = inputcollation,
        etag::T_FuncExpr => expr.as_funcexpr_mut().unwrap().inputcollid = inputcollation,
        etag::T_OpExpr => expr.as_opexpr_mut().unwrap().inputcollid = inputcollation,
        etag::T_DistinctExpr => expr.as_distinctexpr_mut().unwrap().inputcollid = inputcollation,
        etag::T_NullIfExpr => expr.as_nullifexpr_mut().unwrap().inputcollid = inputcollation,
        etag::T_ScalarArrayOpExpr => {
            expr.as_scalararrayopexpr_mut().unwrap().inputcollid = inputcollation
        }
        etag::T_MinMaxExpr => expr.as_minmaxexpr_mut().unwrap().inputcollid = inputcollation,
        _ => {}
    }
}

// ===========================================================================
// exprLocation / leftmostLoc (nodeFuncs.c:1383)
// ===========================================================================

/// `leftmostLoc(loc1, loc2)` (nodeFuncs.c) — minimum of two locations, ignoring
/// unknowns (`-1`).
fn leftmost_loc(loc1: i32, loc2: i32) -> i32 {
    if loc1 < 0 {
        loc2
    } else if loc2 < 0 {
        loc1
    } else {
        loc1.min(loc2)
    }
}

/// `exprLocation(expr)` (nodeFuncs.c) — the parse location (leftmost token) of
/// an expression tree, for error reports; `-1` if it can't be determined.
///
/// Modeled `Expr` variants that retain their parser `location` field report it
/// faithfully (matching the corresponding C `case`), and the `leftmostLoc`
/// recursion over compound variants combines each node's own `location` with
/// its children. The raw-grammar node types this C switch also covers
/// (RangeVar/TypeName/ColumnDef/Constraint/... and CaseWhen/JsonFormat, which
/// are not standalone `Expr` variants here) are not modeled and resolve to
/// `-1` — the documented "location can't be determined" fallback.
pub fn expr_location(expr: Option<&Expr>) -> PgResult<i32> {
    let Some(expr) = expr else {
        return Ok(-1);
    };
    let loc = match expr.expr_tag() {
        // Leaf nodes whose own `location` token is the answer (nodeFuncs.c).
        etag::T_Var => expr.as_var().unwrap().location,
        etag::T_Const => expr.as_const().unwrap().location,
        etag::T_Param => expr.as_param().unwrap().location,
        etag::T_WindowFunc => expr.as_windowfunc().unwrap().location,
        etag::T_MergeSupportFunc => expr.as_mergesupportfunc().unwrap().location,
        etag::T_CaseExpr => expr.as_caseexpr().unwrap().location,
        // ArrayExpr/RowExpr: location points at ARRAY/[ or ROW/( — leftmost.
        etag::T_ArrayExpr => expr.as_arrayexpr().unwrap().location,
        etag::T_RowExpr => expr.as_rowexpr().unwrap().location,
        etag::T_CoalesceExpr => expr.as_coalesceexpr().unwrap().location,
        etag::T_MinMaxExpr => expr.as_minmaxexpr().unwrap().location,
        etag::T_SQLValueFunction => expr.as_sqlvaluefunction().unwrap().location,
        etag::T_JsonConstructorExpr => expr.as_jsonconstructorexpr().unwrap().location,
        etag::T_SetToDefault => expr.as_settodefault().unwrap().location,
        etag::T_SubscriptingRef => {
            expr_location(expr.as_subscriptingref().unwrap().refexpr.as_deref())?
        }
        etag::T_FuncExpr => {
            let f = expr.as_funcexpr().unwrap();
            leftmost_loc(f.location, expr_location_list(&f.args)?)
        }
        etag::T_NamedArgExpr => {
            let na = expr.as_namedargexpr().unwrap();
            leftmost_loc(na.location, expr_location(na.arg.as_deref())?)
        }
        etag::T_OpExpr => {
            let o = expr.as_opexpr().unwrap();
            leftmost_loc(o.location, expr_location_list(&o.args)?)
        }
        etag::T_DistinctExpr => {
            let o = expr.as_distinctexpr().unwrap();
            leftmost_loc(o.location, expr_location_list(&o.args)?)
        }
        etag::T_NullIfExpr => {
            let o = expr.as_nullifexpr().unwrap();
            leftmost_loc(o.location, expr_location_list(&o.args)?)
        }
        etag::T_ScalarArrayOpExpr => {
            let s = expr.as_scalararrayopexpr().unwrap();
            leftmost_loc(s.location, expr_location_list(&s.args)?)
        }
        etag::T_BoolExpr => {
            let b = expr.as_boolexpr().unwrap();
            leftmost_loc(b.location, expr_location_list(&b.args)?)
        }
        etag::T_SubLink => {
            let s = expr.as_sublink().unwrap();
            leftmost_loc(expr_location(s.testexpr.as_deref())?, s.location)
        }
        etag::T_FieldSelect => expr_location(expr.as_fieldselect().unwrap().arg.as_deref())?,
        etag::T_FieldStore => expr_location(expr.as_fieldstore().unwrap().arg.as_deref())?,
        etag::T_RelabelType => {
            let r = expr.as_relabeltype().unwrap();
            leftmost_loc(r.location, expr_location(r.arg.as_deref())?)
        }
        etag::T_CoerceViaIO => {
            let c = expr.as_coerceviaio().unwrap();
            leftmost_loc(c.location, expr_location(c.arg.as_deref())?)
        }
        etag::T_ArrayCoerceExpr => {
            let c = expr.as_arraycoerceexpr().unwrap();
            leftmost_loc(c.location, expr_location(c.arg.as_deref())?)
        }
        etag::T_ConvertRowtypeExpr => {
            let c = expr.as_convertrowtypeexpr().unwrap();
            leftmost_loc(c.location, expr_location(c.arg.as_deref())?)
        }
        etag::T_CollateExpr => expr_location(expr.as_collateexpr().unwrap().arg.as_deref())?,
        etag::T_RowCompareExpr => expr_location_list(&expr.as_rowcompareexpr().unwrap().largs)?,
        etag::T_XmlExpr => {
            let x = expr.as_xmlexpr().unwrap();
            leftmost_loc(x.location, expr_location_list(&x.args)?)
        }
        etag::T_JsonValueExpr => {
            expr_location(expr.as_jsonvalueexpr().unwrap().raw_expr.as_deref())?
        }
        etag::T_JsonExpr => {
            let j = expr.as_jsonexpr().unwrap();
            leftmost_loc(j.location, expr_location(j.formatted_expr.as_deref())?)
        }
        etag::T_NullTest => {
            let n = expr.as_nulltest().unwrap();
            leftmost_loc(n.location, expr_location(n.arg.as_deref())?)
        }
        etag::T_BooleanTest => {
            let b = expr.as_booleantest().unwrap();
            leftmost_loc(b.location, expr_location(b.arg.as_deref())?)
        }
        etag::T_CoerceToDomain => {
            let c = expr.as_coercetodomain().unwrap();
            leftmost_loc(c.location, expr_location(c.arg.as_deref())?)
        }
        etag::T_ReturningExpr => {
            expr_location(expr.as_returningexpr().unwrap().retexpr.as_deref())?
        }
        etag::T_InferenceElem => {
            expr_location(expr.as_inferenceelem().unwrap().expr.as_deref())?
        }
        etag::T_GroupingFunc => {
            // C: loc = ((const GroupingFunc *) expr)->location; (token location)
            expr.as_groupingfunc().unwrap().location
        }
        etag::T_PlaceHolderVar => {
            // C: exprLocation((Node *) ((const PlaceHolderVar *) expr)->phexpr)
            expr_location(expr.as_placeholdervar().unwrap().phexpr.as_deref())?
        }
        // All other modeled variants carry only trimmed-away `location`
        // fields — unknown.
        _ => -1,
    };
    Ok(loc)
}

/// The `T_List` arm of `exprLocation` over an owned `Vec<Expr>`: report the
/// location of the first list member that has one.
fn expr_location_list(list: &[Expr]) -> PgResult<i32> {
    for e in list {
        let loc = expr_location(Some(e))?;
        if loc >= 0 {
            return Ok(loc);
        }
    }
    Ok(-1)
}

// ===========================================================================
// fix_opfuncids / set_opfuncid / set_sa_opfuncid (nodeFuncs.c:1837)
// ===========================================================================

/// `fix_opfuncids(node)` (nodeFuncs.c) — set the `opfuncid` from `opno` for
/// every OpExpr-family node in the tree (in place).
pub fn fix_opfuncids(node: &mut Expr) -> PgResult<()> {
    fix_opfuncids_walker(node)
}

fn fix_opfuncids_walker(node: &mut Expr) -> PgResult<()> {
    match node {
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => set_opfuncid(o)?,
        Expr::ScalarArrayOpExpr(s) => set_sa_opfuncid(s)?,
        _ => {}
    }
    // Recurse into children (the in-place mutable analogue of
    // expression_tree_walker(node, fix_opfuncids_walker)).
    let mut err: Option<types_error::PgError> = None;
    for_each_child_mut(node, &mut |child| {
        if err.is_none() {
            if let Err(e) = fix_opfuncids_walker(child) {
                err = Some(e);
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `set_opfuncid(opexpr)` (nodeFuncs.c) — set the `opfuncid` of an OpExpr (or
/// the struct-equivalent DistinctExpr/NullIfExpr) if not already set.
pub fn set_opfuncid(opexpr: &mut OpExpr) -> PgResult<()> {
    if opexpr.opfuncid == InvalidOid {
        opexpr.opfuncid = lsyscache::get_opcode::call(opexpr.opno)?;
    }
    Ok(())
}

/// The `opfuncid` `set_opfuncid` would compute, *without* scribbling on (or
/// cloning) the node. C's `set_opfuncid` writes the field in place; a
/// read-only walker that can't (or won't) mutate the node — e.g. the
/// dependency-extraction VALUE walk, whose `OpExpr.args` may carry an `Aggref`
/// that isn't deep-cloneable — resolves the same OID this way. Reads only the
/// scalar `opno`/`opfuncid`, never the argument subtree.
pub fn resolved_opfuncid(opno: Oid, opfuncid: Oid) -> PgResult<Oid> {
    if opfuncid == InvalidOid {
        lsyscache::get_opcode::call(opno)
    } else {
        Ok(opfuncid)
    }
}

/// `set_sa_opfuncid(opexpr)` (nodeFuncs.c) — as `set_opfuncid`, for
/// ScalarArrayOpExpr.
pub fn set_sa_opfuncid(opexpr: &mut ScalarArrayOpExpr) -> PgResult<()> {
    if opexpr.opfuncid == InvalidOid {
        opexpr.opfuncid = lsyscache::get_opcode::call(opexpr.opno)?;
    }
    Ok(())
}

// ===========================================================================
// check_functions_in_node (nodeFuncs.c:1905)
// ===========================================================================

/// `check_functions_in_node(node, checker, context)` (nodeFuncs.c) — apply
/// `checker` to each function OID directly contained in the node (no recursion
/// into sub-expressions). Returns true if `checker` does for any of them.
///
/// The C `T_CoerceViaIO` arm calls `getTypeInputInfo`/`getTypeOutputInfo` to
/// recover the I/O function OIDs; those catalog lookups belong to the lsyscache
/// owner and are not in scope for this pure-node-inspection family, so that arm
/// is conservatively treated as "no SQL-visible function found" — the safe C
/// fall-through (`return false`) that callers already handle for the ignored
/// MinMax/SQLValueFunction/XmlExpr/CoerceToDomain/NextValue cases. (Most callers
/// keep control of recursion themselves; the CoerceViaIO I/O-function check is
/// only used by a couple of dependency walkers.)
pub fn check_functions_in_node<F>(node: &mut Expr, checker: &mut F) -> PgResult<bool>
where
    F: FnMut(Oid) -> bool,
{
    let found = match node {
        Expr::Aggref(a) => checker(a.aggfnoid),
        Expr::WindowFunc(w) => checker(w.winfnoid),
        Expr::FuncExpr(f) => checker(f.funcid),
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => {
            set_opfuncid(o)?;
            checker(o.opfuncid)
        }
        Expr::ScalarArrayOpExpr(s) => {
            set_sa_opfuncid(s)?;
            checker(s.opfuncid)
        }
        Expr::RowCompareExpr(rc) => {
            let mut hit = false;
            for &opno in &rc.opnos {
                let opfuncid = lsyscache::get_opcode::call(opno)?;
                if checker(opfuncid) {
                    hit = true;
                    break;
                }
            }
            hit
        }
        _ => false,
    };
    Ok(found)
}

/// Read-only variant of [`check_functions_in_node`] over a `&Expr`.
///
/// The C `check_functions_in_node` takes a mutable `Node*` only so the
/// `OpExpr`/`ScalarArrayOpExpr` arms can `set_opfuncid` (lazily fill `opfuncid`
/// from `opno`) in place. Read-only callers (e.g. `contain_volatile_functions`,
/// which the C invokes on the live tree) do not need that node mutated: the
/// `opfuncid` derived from `opno` is the same OID either way. So this variant
/// resolves it by lookup (`get_opcode` when `opfuncid == InvalidOid`) without
/// touching the node, avoiding a deep node clone (which would panic on an
/// `Aggref`, whose args are a context-allocated `TargetEntry` list).
pub fn check_functions_in_node_ref<F>(node: &Expr, checker: &mut F) -> PgResult<bool>
where
    F: FnMut(Oid) -> bool,
{
    // set_opfuncid(o) lazily fills opfuncid from opno; read it without mutating.
    fn opfuncid_of(opfuncid: Oid, opno: Oid) -> PgResult<Oid> {
        if opfuncid == InvalidOid {
            lsyscache::get_opcode::call(opno)
        } else {
            Ok(opfuncid)
        }
    }
    let found = match node {
        Expr::Aggref(a) => checker(a.aggfnoid),
        Expr::WindowFunc(w) => checker(w.winfnoid),
        Expr::FuncExpr(f) => checker(f.funcid),
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => {
            checker(opfuncid_of(o.opfuncid, o.opno)?)
        }
        Expr::ScalarArrayOpExpr(s) => checker(opfuncid_of(s.opfuncid, s.opno)?),
        Expr::RowCompareExpr(rc) => {
            let mut hit = false;
            for &opno in &rc.opnos {
                let opfuncid = lsyscache::get_opcode::call(opno)?;
                if checker(opfuncid) {
                    hit = true;
                    break;
                }
            }
            hit
        }
        _ => false,
    };
    Ok(found)
}

// ===========================================================================
// expression_tree_walker (nodeFuncs.c:2088)
// ===========================================================================

/// `expression_tree_walker(node, walker, context)` (nodeFuncs.c) — recurse into
/// the sub-nodes of an already-visited node, invoking `walker` on each immediate
/// child. Returns `true` as soon as `walker` returns `true` (abort), else
/// `false`.
///
/// The walker has already visited `node`; we recurse into its children only.
/// The C arms over node types the layered model does not carry (List/Query/
/// FromExpr/JoinExpr/parser-and-planner nodes) are absent — the model cannot
/// construct those nodes.
pub fn expression_tree_walker<'mcx, F>(node: Option<&Expr<'mcx>>, walker: &mut F) -> bool
where
    F: FnMut(&Expr<'mcx>) -> bool,
{
    let Some(node) = node else {
        return false;
    };

    // WALK(opt): invoke walker on an Option<&Expr> child (NULL → false).
    macro_rules! walk_opt {
        ($child:expr) => {
            match $child {
                Some(c) => walker(c),
                None => false,
            }
        };
    }
    // LIST_WALK: recurse directly into a list without calling walker on the
    // list itself (C recurses to self for List nodes).
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list {
                if walker(e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }
    macro_rules! list_walk_opt {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list {
                if walk_opt!(e.as_ref()) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    match node {
        // primitive node types with no expression subnodes
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Param(_)
        | Expr::CaseTestExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::CoerceToDomainValue(_)
        | Expr::SetToDefault(_)
        | Expr::CurrentOfExpr(_)
        | Expr::NextValueExpr(_)
        | Expr::MergeSupportFunc(_) => false,
        Expr::Aggref(expr) => {
            list_walk!(&expr.aggdirectargs)
                || {
                    // args is a TargetEntry list; recurse on each te.expr
                    let mut aborted = false;
                    for te in &expr.args {
                        if walk_opt!(te.expr.as_ref().map(|b| boxed_mcx_expr_as(b))) {
                            aborted = true;
                            break;
                        }
                    }
                    aborted
                }
                || walk_opt!(expr.aggfilter.as_deref())
        }
        Expr::GroupingFunc(grouping) => list_walk!(&grouping.args),
        Expr::WindowFunc(expr) => {
            list_walk!(&expr.args)
                || walk_opt!(expr.aggfilter.as_deref())
                || list_walk!(&expr.runCondition)
        }
        Expr::SubscriptingRef(sbsref) => {
            list_walk_opt!(&sbsref.refupperindexpr)
                || list_walk_opt!(&sbsref.reflowerindexpr)
                || walk_opt!(sbsref.refexpr.as_deref())
                || walk_opt!(sbsref.refassgnexpr.as_deref())
        }
        Expr::FuncExpr(expr) => list_walk!(&expr.args),
        Expr::NamedArgExpr(n) => walk_opt!(n.arg.as_deref()),
        Expr::OpExpr(expr) | Expr::DistinctExpr(expr) | Expr::NullIfExpr(expr) => {
            list_walk!(&expr.args)
        }
        Expr::ScalarArrayOpExpr(expr) => list_walk!(&expr.args),
        Expr::BoolExpr(expr) => list_walk!(&expr.args),
        Expr::SubLink(sublink) => {
            // testexpr, then the sub-Query (opaque address here, not walked).
            walk_opt!(sublink.testexpr.as_deref())
        }
        Expr::SubPlan(sp) => {
            // testexpr, then args; not into the Plan. The SubPlan carries
            // context-allocated PgBox/PgVec children; recurse into each.
            let subplan = &sp.0;
            walk_pgbox_opt(&subplan.testexpr, walker)
                || walk_pgvec_box(&subplan.args, walker)
        }
        Expr::AlternativeSubPlan(asp) => {
            let mut aborted = false;
            for sp in &asp.0.subplans {
                // each subplan recursed as in T_SubPlan; AlternativeSubPlan is
                // a List walk over its SubPlans
                if walk_pgbox_opt(&sp.testexpr, walker) || walk_pgvec_box(&sp.args, walker) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }
        Expr::FieldSelect(f) => walk_opt!(f.arg.as_deref()),
        Expr::FieldStore(fstore) => {
            walk_opt!(fstore.arg.as_deref()) || list_walk!(&fstore.newvals)
        }
        Expr::RelabelType(r) => walk_opt!(r.arg.as_deref()),
        Expr::CoerceViaIO(c) => walk_opt!(c.arg.as_deref()),
        Expr::ArrayCoerceExpr(acoerce) => {
            walk_opt!(acoerce.arg.as_deref()) || walk_opt!(acoerce.elemexpr.as_deref())
        }
        Expr::ConvertRowtypeExpr(c) => walk_opt!(c.arg.as_deref()),
        Expr::CollateExpr(c) => walk_opt!(c.arg.as_deref()),
        Expr::CaseExpr(caseexpr) => {
            if walk_opt!(caseexpr.arg.as_deref()) {
                return true;
            }
            for when in &caseexpr.args {
                if walk_opt!(when.expr.as_deref()) || walk_opt!(when.result.as_deref()) {
                    return true;
                }
            }
            walk_opt!(caseexpr.defresult.as_deref())
        }
        Expr::ArrayExpr(a) => list_walk!(&a.elements),
        Expr::RowExpr(r) => list_walk!(&r.args),
        Expr::RowCompareExpr(rcexpr) => {
            list_walk!(&rcexpr.largs) || list_walk!(&rcexpr.rargs)
        }
        Expr::CoalesceExpr(c) => list_walk!(&c.args),
        Expr::MinMaxExpr(m) => list_walk!(&m.args),
        Expr::XmlExpr(xexpr) => {
            list_walk!(&xexpr.named_args) || list_walk!(&xexpr.args)
        }
        Expr::JsonValueExpr(jve) => {
            walk_opt!(jve.raw_expr.as_deref()) || walk_opt!(jve.formatted_expr.as_deref())
        }
        Expr::JsonConstructorExpr(ctor) => {
            list_walk!(&ctor.args)
                || walk_opt!(ctor.func.as_deref())
                || walk_opt!(ctor.coercion.as_deref())
        }
        Expr::JsonIsPredicate(j) => walk_opt!(j.expr.as_deref()),
        Expr::JsonExpr(jexpr) => {
            walk_opt!(jexpr.formatted_expr.as_deref())
                || walk_opt!(jexpr.path_spec.as_deref())
                || list_walk!(&jexpr.passing_values)
                || walk_json_behavior(jexpr.on_empty.as_deref(), walker)
                || walk_json_behavior(jexpr.on_error.as_deref(), walker)
        }
        Expr::NullTest(n) => walk_opt!(n.arg.as_deref()),
        Expr::BooleanTest(b) => walk_opt!(b.arg.as_deref()),
        Expr::CoerceToDomain(c) => walk_opt!(c.arg.as_deref()),
        Expr::InferenceElem(n) => walk_opt!(n.expr.as_deref()),
        Expr::ReturningExpr(r) => walk_opt!(r.retexpr.as_deref()),
        Expr::PlaceHolderVar(phv) => {
            // C (expression_tree_walker, T_PlaceHolderVar):
            //   return WALK(((PlaceHolderVar *) node)->phexpr);
            // The relids bitmapsets (phrels/phnullingrels) carry no Exprs.
            walk_opt!(phv.phexpr.as_deref())
        }
        // #[non_exhaustive]: an unmodeled future variant has no walkable
        // children to descend (the C default elog is unreachable for trees the
        // model can construct).
        _ => false,
    }
}

/// `T_JsonBehavior` recursion inside the JsonExpr walker arm.
fn walk_json_behavior<'mcx, F>(behavior: Option<&primnodes::JsonBehavior<'mcx>>, walker: &mut F) -> bool
where
    F: FnMut(&Expr<'mcx>) -> bool,
{
    match behavior.and_then(|b| b.expr.as_deref()) {
        Some(e) => walker(e),
        None => false,
    }
}

/// Walk an `Option<PgBox<Expr>>` SubPlan child.
fn walk_pgbox_opt<'mcx, F>(child: &Option<mcx::PgBox<'mcx, Expr<'mcx>>>, walker: &mut F) -> bool
where
    F: FnMut(&Expr<'mcx>) -> bool,
{
    match child {
        Some(b) => walker(&**b),
        None => false,
    }
}

/// Walk a `PgVec<PgBox<Expr>>` SubPlan args list.
fn walk_pgvec_box<'mcx, F>(list: &mcx::PgVec<'mcx, mcx::PgBox<'mcx, Expr<'mcx>>>, walker: &mut F) -> bool
where
    F: FnMut(&Expr<'mcx>) -> bool,
{
    for b in list.iter() {
        if walker(&**b) {
            return true;
        }
    }
    false
}

/// Reborrow a `PgBox<'static, Expr>` (TargetEntry.expr) as `&Expr`.
fn boxed_mcx_expr_as<'a, 'mcx>(b: &'a mcx::PgBox<'mcx, Expr<'mcx>>) -> &'a Expr<'mcx> {
    &**b
}

// ===========================================================================
// expression_tree_mutator (nodeFuncs.c:2945)
// ===========================================================================

/// `expression_tree_mutator(node, mutator, context)` (nodeFuncs.c) — make a
/// modified copy of an expression node, invoking `mutator` on each immediate
/// child sub-node to produce its replacement. Consumes and returns the owned
/// node.
///
/// Mirrors the C "copy this node, mutate sub-nodes" structure for the modeled
/// `Expr` variants. For SubPlan/AlternativeSubPlan, C mutates the `testexpr`
/// and `args` (the correlation/param-setting expressions) while copying the
/// inner `Plan` link as-is; the context-allocated (`PgBox`/`PgVec`) children are
/// mutated in place here (the arena allocation is reused), matching that C
/// behavior. Failing to mutate `args` leaves a correlation Var with its
/// base-relation varno, which execExpr later miscompiles as an `EEOP_SCAN_VAR`.
pub fn expression_tree_mutator<'mcx, F>(mut node: Expr<'mcx>, mutator: &mut F) -> Expr<'mcx>
where
    F: FnMut(Expr<'mcx>) -> Expr<'mcx>,
{
    macro_rules! mut_box {
        ($child:expr) => {
            if let Some(b) = $child.take() {
                $child = Some(Box::new(mutator(*b)));
            }
        };
    }
    macro_rules! mut_vec {
        ($list:expr) => {{
            let old = core::mem::take(&mut $list);
            $list = old.into_iter().map(|e| mutator(e)).collect();
        }};
    }
    macro_rules! mut_vec_opt {
        ($list:expr) => {{
            let old = core::mem::take(&mut $list);
            $list = old
                .into_iter()
                .map(|e| e.map(|inner| mutator(inner)))
                .collect();
        }};
    }
    // Mutate an `Option<PgBox<Expr>>` (SubPlan.testexpr) in place: the arena
    // allocation is reused; only the inner `Expr` is swapped out, mutated, and
    // written back.
    macro_rules! mut_pgbox_opt {
        ($child:expr) => {{
            if let Some(b) = $child.as_mut() {
                let old = core::mem::replace(&mut **b, Expr::Const(Const::default()));
                **b = mutator(old);
            }
        }};
    }
    // Mutate each element of a `PgVec<PgBox<Expr>>` (SubPlan.args) in place.
    macro_rules! mut_pgvec_box {
        ($list:expr) => {{
            for b in $list.iter_mut() {
                let old = core::mem::replace(&mut **b, Expr::Const(Const::default()));
                **b = mutator(old);
            }
        }};
    }

    match &mut node {
        // primitive node types: copied verbatim (no sub-nodes)
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Param(_)
        | Expr::CaseTestExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::CoerceToDomainValue(_)
        | Expr::SetToDefault(_)
        | Expr::CurrentOfExpr(_)
        | Expr::NextValueExpr(_)
        | Expr::MergeSupportFunc(_) => {}
        Expr::GroupingFunc(g) => {
            // C (expression_tree_mutator, T_GroupingFunc): MUTATE(newnode->args, ...).
            // args is a plain Expr list (GROUP BY column references); cols/refs
            // are index lists, copied verbatim.
            mut_vec!(g.args);
        }
        Expr::WindowFunc(w) => {
            mut_vec!(w.args);
            mut_box!(w.aggfilter);
            mut_vec!(w.runCondition);
        }
        Expr::WindowFuncRunCondition(w) => {
            mut_box!(w.arg);
        }
        Expr::Aggref(a) => {
            // C (expression_tree_mutator, T_Aggref):
            //   MUTATE(newnode->aggdirectargs, ...);   // List of Expr
            //   MUTATE(newnode->args, ...);            // List of TargetEntry
            //   MUTATE(newnode->aggorder, ...);        // List of SortGroupClause
            //   MUTATE(newnode->aggdistinct, ...);     // List of SortGroupClause
            //   MUTATE(newnode->aggfilter, ...);       // Expr
            // The args/aggdirectargs/aggfilter carry expressions that may contain
            // SubLinks (e.g. agg((SELECT ...))); process_sublinks_mutator must
            // descend so they become SubPlans. aggorder/aggdistinct are
            // SortGroupClause lists (index refs, no embedded Exprs) — mutating
            // them is a no-op, matching C, so we leave them verbatim.
            mut_vec!(a.aggdirectargs);
            // args is a TargetEntry list; the context-allocated `te.expr`
            // (Option<PgBox<Expr>>) is mutated in place, same shape as
            // mut_pgbox_opt! over SubPlan.testexpr.
            for te in a.args.iter_mut() {
                if let Some(b) = te.expr.as_mut() {
                    let old = core::mem::replace(&mut **b, Expr::Const(Const::default()));
                    **b = mutator(old);
                }
            }
            mut_box!(a.aggfilter);
        }
        Expr::SubscriptingRef(s) => {
            mut_vec_opt!(s.refupperindexpr);
            mut_vec_opt!(s.reflowerindexpr);
            mut_box!(s.refexpr);
            mut_box!(s.refassgnexpr);
        }
        Expr::FuncExpr(f) => mut_vec!(f.args),
        Expr::NamedArgExpr(n) => mut_box!(n.arg),
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => mut_vec!(o.args),
        Expr::ScalarArrayOpExpr(s) => mut_vec!(s.args),
        Expr::BoolExpr(b) => mut_vec!(b.args),
        Expr::SubLink(s) => mut_box!(s.testexpr),
        Expr::SubPlan(sp) => {
            // C (expression_tree_mutator, T_SubPlan):
            //   MUTATE(newnode->testexpr, subplan->testexpr, Node *);
            //   MUTATE(newnode->args, subplan->args, List *);
            //   /* but not the sub-Plan itself, which is referenced as-is */
            // The testexpr / args carry correlation Vars (e.g. setrefs.c
            // fix_join_expr rewriting a parParam-setting Var into an
            // OUTER_VAR/INNER_VAR); they MUST be mutated, or the args Var keeps
            // its base-relation varno and execExpr later compiles it as an
            // EEOP_SCAN_VAR whose econtext has no scantuple under a join.
            //
            // The children are context-allocated (`PgBox`/`PgVec`), so rather
            // than re-boxing into a (here-unavailable) arena we mutate each in
            // place: pull the inner `Expr` out of its existing allocation, run
            // the mutator, and write the replacement back into the same slot.
            mut_pgbox_opt!(sp.0.testexpr);
            mut_pgvec_box!(sp.0.args);
        }
        Expr::AlternativeSubPlan(asp) => {
            // C treats AlternativeSubPlan like SubPlan: each contained SubPlan's
            // testexpr/args are mutated, the sub-Plan link copied as-is.
            for sub in asp.0.subplans.iter_mut() {
                mut_pgbox_opt!(sub.testexpr);
                mut_pgvec_box!(sub.args);
            }
        }
        Expr::FieldSelect(f) => mut_box!(f.arg),
        Expr::FieldStore(f) => {
            mut_box!(f.arg);
            mut_vec!(f.newvals);
        }
        Expr::RelabelType(r) => mut_box!(r.arg),
        Expr::CoerceViaIO(c) => mut_box!(c.arg),
        Expr::ArrayCoerceExpr(a) => {
            mut_box!(a.arg);
            mut_box!(a.elemexpr);
        }
        Expr::ConvertRowtypeExpr(c) => mut_box!(c.arg),
        Expr::CollateExpr(c) => mut_box!(c.arg),
        Expr::CaseExpr(c) => {
            mut_box!(c.arg);
            let old = core::mem::take(&mut c.args);
            c.args = old
                .into_iter()
                .map(|mut when: CaseWhen| {
                    mut_box!(when.expr);
                    mut_box!(when.result);
                    when
                })
                .collect();
            mut_box!(c.defresult);
        }
        Expr::ArrayExpr(a) => mut_vec!(a.elements),
        Expr::RowExpr(r) => mut_vec!(r.args),
        Expr::RowCompareExpr(rc) => {
            mut_vec!(rc.largs);
            mut_vec!(rc.rargs);
        }
        Expr::CoalesceExpr(c) => mut_vec!(c.args),
        Expr::MinMaxExpr(m) => mut_vec!(m.args),
        Expr::XmlExpr(x) => {
            mut_vec!(x.named_args);
            mut_vec!(x.args);
        }
        Expr::JsonValueExpr(jve) => {
            mut_box!(jve.raw_expr);
            mut_box!(jve.formatted_expr);
        }
        Expr::JsonConstructorExpr(ctor) => {
            mut_vec!(ctor.args);
            mut_box!(ctor.func);
            mut_box!(ctor.coercion);
        }
        Expr::JsonIsPredicate(j) => mut_box!(j.expr),
        Expr::JsonExpr(jexpr) => {
            // C (expression_tree_mutator, T_JsonExpr):
            //   MUTATE(newnode->formatted_expr, ...);
            //   MUTATE(newnode->path_spec, ...);
            //   MUTATE(newnode->passing_values, ...);
            //   MUTATE(newnode->on_empty, jexpr->on_empty, JsonBehavior *);
            //   MUTATE(newnode->on_error, jexpr->on_error, JsonBehavior *);
            // C T_JsonBehavior FLATCOPYs the behavior and MUTATEs its `expr`;
            // here the JsonBehavior box is reused and only its `expr` mutated.
            mut_box!(jexpr.formatted_expr);
            mut_box!(jexpr.path_spec);
            mut_vec!(jexpr.passing_values);
            if let Some(b) = jexpr.on_empty.as_mut() {
                mut_box!(b.expr);
            }
            if let Some(b) = jexpr.on_error.as_mut() {
                mut_box!(b.expr);
            }
        }
        Expr::NullTest(n) => mut_box!(n.arg),
        Expr::BooleanTest(b) => mut_box!(b.arg),
        Expr::CoerceToDomain(c) => mut_box!(c.arg),
        Expr::InferenceElem(n) => mut_box!(n.expr),
        Expr::ReturningExpr(r) => mut_box!(r.retexpr),
        Expr::PlaceHolderVar(phv) => {
            // C (expression_tree_mutator, T_PlaceHolderVar):
            //   FLATCOPY(newnode, phv, PlaceHolderVar);
            //   MUTATE(newnode->phexpr, phv->phexpr, Expr *);
            //   /* Assume we need not copy the relids bitmapsets */
            // The FLATCOPY preserves phrels/phnullingrels/phid/phlevelsup
            // verbatim (handled by mutating in place); only phexpr is mutated.
            mut_box!(phv.phexpr);
        }
        // #[non_exhaustive]: an unmodeled future variant is copied verbatim.
        _ => {}
    }
    node
}

/// `pub(crate)` entry to [`for_each_child_mut`] for the `Node`-level in-place
/// walker (`node_walker::expression_tree_walker_mut`), which drives the mutating
/// recursion over an embedded `Expr`'s immediate children.
pub(crate) fn for_each_expr_child_mut<'mcx, F>(node: &mut Expr<'mcx>, f: &mut F)
where
    F: FnMut(&mut Expr<'mcx>),
{
    for_each_child_mut(node, f)
}

/// Drive `mutator` over the immediate `Box<Expr>`/`Vec<Expr>` children of a
/// node in place (the in-place analogue used by `fix_opfuncids_walker`, where
/// the recursion reads-and-writes the same tree rather than rebuilding it).
fn for_each_child_mut<'mcx, F>(node: &mut Expr<'mcx>, f: &mut F)
where
    F: FnMut(&mut Expr<'mcx>),
{
    macro_rules! on_box {
        ($child:expr) => {
            if let Some(b) = $child.as_deref_mut() {
                f(b);
            }
        };
    }
    macro_rules! on_vec {
        ($list:expr) => {
            for e in $list.iter_mut() {
                f(e);
            }
        };
    }
    macro_rules! on_vec_opt {
        ($list:expr) => {
            for e in $list.iter_mut() {
                if let Some(inner) = e.as_mut() {
                    f(inner);
                }
            }
        };
    }
    match node {
        Expr::Aggref(a) => {
            // C `expression_tree_walker` T_Aggref: aggdirectargs (plain exprs),
            // args (TargetEntry list — recurse into each te.expr), aggfilter.
            on_vec!(a.aggdirectargs);
            for te in a.args.iter_mut() {
                if let Some(b) = te.expr.as_deref_mut() {
                    f(b);
                }
            }
            on_box!(a.aggfilter);
        }
        Expr::WindowFunc(w) => {
            on_vec!(w.args);
            on_box!(w.aggfilter);
            on_vec!(w.runCondition);
        }
        Expr::WindowFuncRunCondition(w) => on_box!(w.arg),
        Expr::GroupingFunc(g) => on_vec!(g.args),
        Expr::SubscriptingRef(s) => {
            on_vec_opt!(s.refupperindexpr);
            on_vec_opt!(s.reflowerindexpr);
            on_box!(s.refexpr);
            on_box!(s.refassgnexpr);
        }
        Expr::FuncExpr(fx) => on_vec!(fx.args),
        Expr::NamedArgExpr(n) => on_box!(n.arg),
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => on_vec!(o.args),
        Expr::ScalarArrayOpExpr(s) => on_vec!(s.args),
        Expr::BoolExpr(b) => on_vec!(b.args),
        Expr::SubLink(s) => on_box!(s.testexpr),
        Expr::FieldSelect(fs) => on_box!(fs.arg),
        Expr::FieldStore(fs) => {
            on_box!(fs.arg);
            on_vec!(fs.newvals);
        }
        Expr::RelabelType(r) => on_box!(r.arg),
        Expr::CoerceViaIO(c) => on_box!(c.arg),
        Expr::ArrayCoerceExpr(a) => {
            on_box!(a.arg);
            on_box!(a.elemexpr);
        }
        Expr::ConvertRowtypeExpr(c) => on_box!(c.arg),
        Expr::CollateExpr(c) => on_box!(c.arg),
        Expr::CaseExpr(c) => {
            on_box!(c.arg);
            for when in c.args.iter_mut() {
                on_box!(when.expr);
                on_box!(when.result);
            }
            on_box!(c.defresult);
        }
        Expr::ArrayExpr(a) => on_vec!(a.elements),
        Expr::RowExpr(r) => on_vec!(r.args),
        Expr::RowCompareExpr(rc) => {
            on_vec!(rc.largs);
            on_vec!(rc.rargs);
        }
        Expr::CoalesceExpr(c) => on_vec!(c.args),
        Expr::MinMaxExpr(m) => on_vec!(m.args),
        Expr::XmlExpr(x) => {
            on_vec!(x.named_args);
            on_vec!(x.args);
        }
        Expr::JsonValueExpr(jve) => {
            on_box!(jve.raw_expr);
            on_box!(jve.formatted_expr);
        }
        Expr::JsonConstructorExpr(ctor) => {
            on_vec!(ctor.args);
            on_box!(ctor.func);
            on_box!(ctor.coercion);
        }
        Expr::JsonIsPredicate(j) => on_box!(j.expr),
        Expr::JsonExpr(jexpr) => {
            on_box!(jexpr.formatted_expr);
            on_box!(jexpr.path_spec);
            on_vec!(jexpr.passing_values);
            if let Some(b) = jexpr.on_empty.as_mut() {
                on_box!(b.expr);
            }
            if let Some(b) = jexpr.on_error.as_mut() {
                on_box!(b.expr);
            }
        }
        Expr::NullTest(n) => on_box!(n.arg),
        Expr::BooleanTest(b) => on_box!(b.arg),
        Expr::CoerceToDomain(c) => on_box!(c.arg),
        Expr::InferenceElem(n) => on_box!(n.expr),
        Expr::ReturningExpr(r) => on_box!(r.retexpr),
        Expr::PlaceHolderVar(phv) => on_box!(phv.phexpr),
        // C `expression_tree_mutator` T_SubPlan: mutate `testexpr` then `args`
        // (the correlation Var/Param exprs); the planned sub-`Plan` is not a
        // child here. The immutable `expression_tree_walker` walks the same set
        // (`walk_pgbox_opt(testexpr)` / `walk_pgvec_box(args)`); the in-place
        // walker must match so callers like `map_variable_attnos` can rewrite
        // the correlation Vars of a MULTIEXPR / correlated ON CONFLICT SET
        // sub-SELECT when adjusting for a partition's differing rowtype.
        Expr::SubPlan(sp) => {
            let subplan = &mut sp.0;
            if let Some(b) = subplan.testexpr.as_deref_mut() {
                f(b);
            }
            for a in subplan.args.iter_mut() {
                f(&mut **a);
            }
        }
        Expr::AlternativeSubPlan(asp) => {
            for subplan in asp.0.subplans.iter_mut() {
                if let Some(b) = subplan.testexpr.as_deref_mut() {
                    f(b);
                }
                for a in subplan.args.iter_mut() {
                    f(&mut **a);
                }
            }
        }
        // primitive / context-allocated / TargetEntry-bearing: no in-tree
        // Box/Vec<Expr> children to descend
        _ => {}
    }
}

// ===========================================================================
// Error constructors (the C elog/ereport surface)
// ===========================================================================

fn untransformed_sublink_error_t(what: &str) -> types_error::PgError {
    // C: elog(ERROR, "cannot get %s for untransformed sublink")
    types_error::PgError::error(format!(
        "cannot get {what} for untransformed sublink"
    ))
}

fn unrecognized_node_type_error(name: &str) -> PgResult<types_error::PgError> {
    // C: elog(ERROR, "unrecognized node type: %d", nodeTag(node))
    Ok(types_error::PgError::error(format!(
        "unrecognized node type: {name}"
    )))
}

fn no_array_type_error(elem_type: Oid) -> PgResult<types_error::PgError> {
    // C: ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
    //     errmsg("could not find array type for data type %s", format_type_be(...)))
    let tyname = format_type::format_type_be_str::call(elem_type)?;
    Ok(types_error::PgError::error(format!(
        "could not find array type for data type {tyname}"
    ))
    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT))
}

/// Diagnostic name of an `Expr` variant for the unrecognized-node error.
fn expr_variant_name(expr: &Expr) -> &'static str {
    match expr {
        Expr::SubPlan(_) => "SubPlan",
        Expr::AlternativeSubPlan(_) => "AlternativeSubPlan",
        Expr::InferenceElem(_) => "InferenceElem",
        Expr::ReturningExpr(_) => "ReturningExpr",
        _ => "expression",
    }
}

/// relnode.c `set_joinrel_partition_key_exprs` builds, for each full-join
/// output column, `makeNode(CoalesceExpr)` with `coalescetype = exprType(larg)`,
/// `coalescecollid = exprCollation(larg)`, `args = list_make2(larg, rarg)`,
/// `location = -1`. The node build is trivial but needs `exprType`/
/// `exprCollation` (nodeFuncs.c, this crate), so relnode reaches it through the
/// relnode-ext seam; this crate owns it.
pub fn make_coalesce_expr<'mcx>(larg: &Expr<'mcx>, rarg: &Expr<'mcx>) -> Expr<'mcx> {
    Expr::CoalesceExpr(types_nodes::primnodes::CoalesceExpr {
        coalescetype: expr_type(Some(larg)).expect("exprType"),
        coalescecollid: expr_collation(Some(larg)).expect("exprCollation"),
        args: vec![larg.clone(), rarg.clone()],
        location: -1,
    })
}

// ===========================================================================
// Seam wiring (the inward seams this family owns)
// ===========================================================================

/// Install the `backend-nodes-nodeFuncs-seams` this family owns. Called from
/// the crate `init_seams()`.
pub fn init_seams() {
    use backend_nodes_nodeFuncs_seams as seams;

    seams::expr_type_info::set(seam_expr_type_info);
    seams::expr_is_length_coercion::set(seam_expr_is_length_coercion);
    seams::expr_type::set(seam_expr_type);
    seams::call_expr_argtype::set(seam_call_expr_argtype);
    seams::get_call_expr_argtype_expr::set(seam_get_call_expr_argtype_expr);
    seams::call_expr_arg_stable::set(seam_call_expr_arg_stable);
    seams::expr_variadic::set(seam_expr_variadic);
    seams::expr_variadic_expr::set(seam_expr_variadic_expr);
    seams::call_expr_arg_stable_expr::set(seam_call_expr_arg_stable_expr);
    seams::get_call_expr_argtype_node::set(seam_get_call_expr_argtype_node);
    seams::expr_input_collation_node::set(seam_expr_input_collation_node);
    seams::expr_input_collation_expr::set(seam_expr_input_collation_expr);
    seams::targetentry_info::set(seam_targetentry_info);
    seams::sortgroupclause_info::set(seam_sortgroupclause_info);
    seams::get_sortgroupref_tle::set(seam_get_sortgroupref_tle);
    seams::get_sortgroupclause_expr::set(seam_get_sortgroupclause_expr);
    seams::get_sortgroupref_clause_noerr::set(seam_get_sortgroupref_clause_noerr);
    // The pure `&Expr -> Oid/i32` reads (exprCollation / exprLocation) the
    // pathkeys / equivclass leaves and several commands reach. The underlying
    // impls are infallible for the node kinds reached; a propagated error is a
    // loud panic (mirrors C's elog(ERROR) on an unrecognized node tag).
    seams::exprCollation::set(|expr| expr_collation(Some(expr)).expect("exprCollation"));
    seams::exprLocation::set(|expr| expr_location(Some(expr)).expect("exprLocation"));

    // `copyObject(expr)` (nodes/copyfuncs) over the in-arena `Expr` value model.
    // The mcx-less `&Expr -> Expr` shape (used by pathkeys.c's
    // `find_var_for_subquery_tle`, which copies a `Var` for safety) is a
    // structural deep-copy: the derived `Expr::clone` is a faithful copy for
    // every node kind whose children are not context-allocated (Var/Const/
    // OpExpr/…). For an `Aggref`/`SubPlan` (context-allocated arg lists) the
    // derived clone is the intentional guard-panic — those never reach this
    // seam's call sites (the pathkeys leaf only ever copies a `Var`).
    seams::copyObject::set(|expr| expr.clone());

    // `is_notclause(clause)` / `get_notclausearg(notclause)` (nodeFuncs.h static
    // inlines) — `IsA(clause, BoolExpr) && boolop == NOT_EXPR`, and the sole
    // argument of such a NOT clause. The SubLink-processing path
    // (prepjointree.c) reaches these; install them over the owned `Expr` model.
    seams::is_notclause::set(|clause| match clause.as_boolexpr() {
        Some(b) => b.boolop == primnodes::BoolExprType::NOT_EXPR,
        None => false,
    });
    seams::get_notclausearg::set(|notclause| {
        notclause
            .as_boolexpr()
            .expect("get_notclausearg: not a BoolExpr")
            .args
            .first()
            .expect("NOT clause must have one argument")
    });

    // costsize.c reaches `exprType((Node *) expr)` / `exprTypmod(...)` over
    // arena-resolved planner nodes (set_rel_width, get_expr_width). The
    // `(root, NodeId)` costsize-seams resolve the node here and dispatch to this
    // unit's nodeFuncs.c port. The underlying impls are infallible for the
    // node kinds reached; a propagated error is a loud panic (mirrors C's
    // elog(ERROR) on an unrecognized node tag).
    {
        use backend_optimizer_path_costsize_seams as cz;
        cz::expr_type::set(seam_costsize_expr_type);
        cz::expr_typmod::set(seam_costsize_expr_typmod);
    }
    // (`get_expr_result_type_node` is RETIRED: all arms of funcapi's
    // get_expr_result_type — incl. the RECORD-type-Const arm reached by EXPLAIN
    // of SEARCH/CYCLE recursive CTEs — are ported in place inside
    // backend-utils-fmgr-funcapi over the composite Datum's HeapTupleHeader, so
    // there is no cross-unit seam to install here.)

    // The equivclass-ext cycle-break seams initsplan.c / equivclass.c call into
    // nodeFuncs.c (`exprType`/`exprTypmod`/`exprCollation`) over an owned
    // rootless `&Expr`. var.c installs its pull_var_clause* legs of this same
    // ext-seam crate; nodeFuncs.c owns these accessor legs. The impls are
    // infallible for valid trees; a propagated error is a loud panic (mirrors
    // C's elog(ERROR) on an unrecognized node tag).
    {
        use backend_optimizer_path_equivclass_ext_seams as eqext;
        eqext::expr_type::set(|expr| expr_type(Some(expr)).expect("exprType"));
        eqext::expr_typmod::set(|expr| expr_typmod(Some(expr)).expect("exprTypmod"));
        eqext::expr_collation::set(|expr| expr_collation(Some(expr)).expect("exprCollation"));
        // `add_setop_child_rel_equivalences` (equivclass.c) inspects each setop
        // child `TargetEntry` by `NodeId` in the planner arena: read its
        // `resjunk` flag and clone its `expr` node. These are plain arena field
        // reads (mirror C `tle->resjunk` / `tle->expr`); nodeFuncs.c owns the
        // TargetEntry-inspection leg of this ext-seam crate.
        eqext::target_entry_resjunk::set(|root, tle| root.targetentry(tle).resjunk);
        eqext::target_entry_expr::set(|root, tle| {
            let expr_id = root.targetentry(tle).expr;
            root.node(expr_id).clone()
        });
        // `add_child_eq_member` (equivclass.c) calls `expression_returns_set`
        // (nodeFuncs.c) over the new member's expression; nodeFuncs.c owns it.
        eqext::expression_returns_set::set(|expr| expression_returns_set(Some(expr)));
        // `canonicalize_ec_expression` (equivclass.c) wraps the member expr in a
        // RelabelType via `applyRelabelType` (nodeFuncs.c), which this unit owns.
        eqext::apply_relabel_type::set(apply_relabel_type);
        // `process_equivalence` (equivclass.c) builds a `makeBoolConst` and an
        // `IS NOT NULL` `NullTest` over equivalence members; these are makefuncs.c
        // node builders, which this unit owns.
        eqext::make_bool_const::set(|value, isnull| {
            Expr::Const(crate::makefuncs::make_bool_const(value, isnull))
        });
        eqext::make_is_not_null::set(crate::makefuncs::make_is_not_null);
    }

    // joininfo.c / restrictinfo.c reach the same nodeFuncs.c accessors
    // (`exprType`/`exprTypmod`) over an owned rootless `&Expr` through the
    // joininfo-ext consumer-side seam crate (no owner directory). nodeFuncs.c
    // owns these accessor legs; same shape as the equivclass-ext legs above.
    {
        use backend_optimizer_util_joininfo_ext_seams as jiext;
        jiext::expr_type::set(|expr| expr_type(Some(expr)).expect("exprType"));
        jiext::expr_typmod::set(|expr| expr_typmod(Some(expr)).expect("exprTypmod"));
    }

    // relnode.c `set_joinrel_partition_key_exprs` builds a CoalesceExpr whose
    // type/collation come from this unit's `exprType`/`exprCollation`; it reaches
    // the build through the relnode-ext seam (no owner directory). This crate
    // (nodeFuncs.c + the makefuncs.c node builders) owns it.
    backend_optimizer_util_relnode_ext_seams::make_coalesce_expr::set(make_coalesce_expr);
}

/// `exprType((Node *) expr)` (nodeFuncs.c) over an arena-resolved planner node.
/// costsize.c's `expr_type` seam: resolve the `NodeId` to its `Expr` in the
/// planner node arena, then call this unit's `expr_type`.
fn seam_costsize_expr_type(
    root: &types_pathnodes::PlannerInfo,
    node: types_pathnodes::NodeId,
) -> Oid {
    expr_type(Some(root.node(node))).expect("exprType")
}

/// `exprTypmod((Node *) expr)` (nodeFuncs.c) over an arena-resolved planner node.
fn seam_costsize_expr_typmod(
    root: &types_pathnodes::PlannerInfo,
    node: types_pathnodes::NodeId,
) -> i32 {
    expr_typmod(Some(root.node(node))).expect("exprTypmod")
}

/// `sortgroupclause_info(root, sortcl)` seam — read the `SortGroupClause` fields
/// pathkeys needs off a `NodeId` resolving to a `SortGroupClause` in the planner
/// node arena. Infallible: a plain arena resolve (mirrors C field reads off a
/// `SortGroupClause *`).
fn seam_sortgroupclause_info(
    root: &types_pathnodes::PlannerInfo,
    sortcl: types_pathnodes::NodeId,
) -> backend_nodes_nodeFuncs_seams::SortGroupClauseInfo {
    let sgc = root.sortgroupclause(sortcl);
    backend_nodes_nodeFuncs_seams::SortGroupClauseInfo {
        tle_sort_group_ref: sgc.tleSortGroupRef,
        sortop: sgc.sortop,
        reverse_sort: sgc.reverse_sort,
        nulls_first: sgc.nulls_first,
    }
}

/// `get_sortgroupref_tle(root, sortref, target_list)` seam (tlist.c) — the
/// `TargetEntry` `NodeId` in `target_list` whose `ressortgroupref == sortref`.
/// The C `elog(ERROR, ...)` is surfaced as a loud panic.
fn seam_get_sortgroupref_tle(
    root: &types_pathnodes::PlannerInfo,
    sortref: u32,
    target_list: &[types_pathnodes::NodeId],
) -> types_pathnodes::NodeId {
    for &tle in target_list {
        if root.targetentry(tle).ressortgroupref == sortref {
            return tle;
        }
    }
    panic!("get_sortgroupref_tle: ORDER/GROUP BY expression not found in targetlist");
}

/// `get_sortgroupclause_expr(root, sortcl, target_list)` seam (tlist.c) — the
/// `expr` `NodeId` of the `TargetEntry` referenced by the `SortGroupClause`'s
/// `tleSortGroupRef`.
fn seam_get_sortgroupclause_expr(
    root: &types_pathnodes::PlannerInfo,
    sortcl: types_pathnodes::NodeId,
    target_list: &[types_pathnodes::NodeId],
) -> types_pathnodes::NodeId {
    let sortref = root.sortgroupclause(sortcl).tleSortGroupRef;
    let tle = seam_get_sortgroupref_tle(root, sortref, target_list);
    root.targetentry(tle).expr
}

/// `get_sortgroupref_clause_noerr(root, sortref, clauses)` seam (tlist.c) — the
/// `SortGroupClause` `NodeId` in `clauses` whose `tleSortGroupRef == sortref`,
/// or `None` (the `_noerr` variant). `clauses` entries are `SortGroupClause`
/// `NodeId`s.
fn seam_get_sortgroupref_clause_noerr(
    root: &types_pathnodes::PlannerInfo,
    sortref: u32,
    clauses: &[types_pathnodes::NodeId],
) -> Option<types_pathnodes::NodeId> {
    clauses
        .iter()
        .copied()
        .find(|&cl| root.sortgroupclause(cl).tleSortGroupRef == sortref)
}

/// `targetentry_info(root, tle)` seam — read the `TargetEntry` fields pathkeys
/// needs off a `NodeId` resolving to a `TargetEntry` in the planner node arena.
/// Infallible: a plain arena resolve (mirrors C field reads off a `TargetEntry *`).
fn seam_targetentry_info(
    root: &types_pathnodes::PlannerInfo,
    tle: types_pathnodes::NodeId,
) -> backend_nodes_nodeFuncs_seams::TargetEntryInfo {
    let te = root.targetentry(tle);
    backend_nodes_nodeFuncs_seams::TargetEntryInfo {
        ressortgroupref: te.ressortgroupref,
        resno: te.resno,
        resjunk: te.resjunk,
        expr: te.expr,
    }
}

/// `expr_type_info(expr)` seam — the `(typid, typmod, collation)` triple read
/// together (nodeFuncs.c `exprType`/`exprTypmod`/`exprCollation`).
fn seam_expr_type_info(
    expr: &Expr,
) -> PgResult<backend_nodes_nodeFuncs_seams::ExprTypeInfo> {
    Ok(backend_nodes_nodeFuncs_seams::ExprTypeInfo {
        typid: expr_type(Some(expr))?,
        typmod: expr_typmod(Some(expr))?,
        collation: expr_collation(Some(expr))?,
    })
}

/// `exprIsLengthCoercion(expr, &coercedTypmod)` seam adapter.
fn seam_expr_is_length_coercion(expr: &Expr) -> PgResult<(bool, i32)> {
    expr_is_length_coercion(Some(expr))
}

/// `exprType(ExternalFnExpr)` seam (fmgr `get_fn_expr_rettype` path).
///
/// When the carrier holds the field-bearing call node (`node == Some`, the
/// erased `primnodes::Expr` that `fmgr_info_set_expr` / the funcapi
/// `call_expr` adapter stamped), read its result type through the real
/// `expr_type` — this is C's `exprType(flinfo->fn_expr)` reading
/// `FuncExpr.funcresulttype` / `OpExpr.opresulttype`, and is what
/// `internal_get_result_type` needs to resolve a polymorphic (`anyelement`)
/// return type from the concrete call. A tag-only carrier (`node == None`)
/// falls through to `InvalidOid`, exactly the C fall-through for a node kind
/// whose type cannot be read.
fn seam_expr_type(expr: types_fmgr::ExternalFnExpr) -> Oid {
    match expr
        .node
        .as_ref()
        .and_then(|n| n.downcast_ref::<Expr>())
    {
        Some(e) => expr_type(Some(e)).unwrap_or(InvalidOid),
        None => InvalidOid,
    }
}

/// `get_call_expr_argtype(expr, argnum)` (fmgr.c) over the tag-only carrier.
/// The per-argument expression list is not carried by the tag-only
/// `ExternalFnExpr`, so this returns `InvalidOid` (out-of-range / unhandled
/// kind), the documented C fall-through.
fn seam_call_expr_argtype(_expr: types_fmgr::ExternalFnExpr, _argnum: i32) -> Oid {
    InvalidOid
}

/// `get_call_expr_arg_stable(expr, argnum)` (fmgr.c) over the tag-only carrier.
/// The argument node kind is not carried, so this conservatively returns
/// `false` (the C fall-through: treat the argument as non-stable).
fn seam_call_expr_arg_stable(_expr: types_fmgr::ExternalFnExpr, _argnum: i32) -> bool {
    false
}

/// `get_call_expr_argtype(expr, argnum)` (fmgr.c:1929) over the *field-bearing*
/// owned `Expr` that `fmgr_info_set_expr` stamps onto `FmgrInfo.fn_expr`.
///
/// Verbatim port of the C:
/// ```c
/// if (expr == NULL) return InvalidOid;
/// if (IsA(expr, FuncExpr))                 args = ((FuncExpr*)expr)->args;
/// else if (IsA(expr, OpExpr))              args = ((OpExpr*)expr)->args;
/// else if (IsA(expr, DistinctExpr))        args = ((DistinctExpr*)expr)->args;
/// else if (IsA(expr, ScalarArrayOpExpr))   args = ((ScalarArrayOpExpr*)expr)->args;
/// else if (IsA(expr, NullIfExpr))          args = ((NullIfExpr*)expr)->args;
/// else if (IsA(expr, WindowFunc))          args = ((WindowFunc*)expr)->args;
/// else return InvalidOid;
/// if (argnum < 0 || argnum >= list_length(args)) return InvalidOid;
/// argtype = exprType((Node*) list_nth(args, argnum));
/// if (IsA(expr, ScalarArrayOpExpr) && argnum == 1)
/// {   /* scalar array op uses the array's element type for the 2nd arg */
///     argtype = get_base_element_type(argtype);
/// }
/// return argtype;
/// ```
fn seam_get_call_expr_argtype_expr(expr: &Expr, argnum: i32) -> PgResult<Oid> {
    // Select the argument list by node kind (the C `IsA` dispatch). `DistinctExpr`
    // / `NullIfExpr` are `OpExpr`-shaped here, so their `.args` is read the same.
    let (args, is_saop): (&[Expr], bool) = match expr {
        Expr::FuncExpr(f) => (&f.args, false),
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => (&o.args, false),
        Expr::ScalarArrayOpExpr(s) => (&s.args, true),
        Expr::WindowFunc(w) => (&w.args, false),
        // C: every other node kind falls through to InvalidOid.
        _ => return Ok(InvalidOid),
    };

    // if (argnum < 0 || argnum >= list_length(args)) return InvalidOid;
    if argnum < 0 || argnum as usize >= args.len() {
        return Ok(InvalidOid);
    }

    // argtype = exprType((Node*) list_nth(args, argnum));
    let mut argtype = expr_type(Some(&args[argnum as usize]))?;

    // ScalarArrayOpExpr uses the array element type for the second argument
    // (C: argtype = get_base_element_type(argtype), unconditionally returned).
    if is_saop && argnum == 1 {
        argtype = lsyscache::get_base_element_type::call(argtype)?;
    }

    Ok(argtype)
}

/// `get_fn_expr_variadic` body (fmgr.c): `IsA(expr, FuncExpr) ? funcvariadic :
/// false`. The `funcvariadic` flag lives in a `FuncExpr` field the tag-only
/// carrier does not carry, so this returns `false` (the C fall-through for a
/// non-FuncExpr or unknown node).
fn seam_expr_variadic(_expr: types_fmgr::ExternalFnExpr) -> bool {
    false
}

/// `get_fn_expr_variadic` (fmgr.c) over the field-bearing owned `Expr`:
/// `IsA(expr, FuncExpr) ? ((FuncExpr *) expr)->funcvariadic : false`.
fn seam_expr_variadic_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FuncExpr(f) => f.funcvariadic,
        _ => false,
    }
}

/// `get_call_expr_arg_stable(expr, argnum)` (fmgr.c) over the field-bearing owned
/// `Expr`: select the arg list by node kind (the C `IsA` dispatch), range-guard,
/// then true iff the argument is a `Const` or an external (`PARAM_EXTERN`)
/// `Param`.
fn seam_call_expr_arg_stable_expr(expr: &Expr, argnum: i32) -> bool {
    use types_nodes::primnodes::PARAM_EXTERN;
    // DistinctExpr / NullIfExpr are OpExpr-shaped here, so their `.args` is read
    // the same way (mirrors seam_get_call_expr_argtype_expr).
    let args: &[Expr] = match expr {
        Expr::FuncExpr(f) => &f.args,
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => &o.args,
        Expr::ScalarArrayOpExpr(s) => &s.args,
        Expr::WindowFunc(w) => &w.args,
        _ => return false,
    };
    if argnum < 0 || argnum as usize >= args.len() {
        return false;
    }
    match &args[argnum as usize] {
        Expr::Const(_) => true,
        Expr::Param(p) => p.paramkind == PARAM_EXTERN,
        _ => false,
    }
}

/// `get_call_expr_argtype(call_expr, argnum)` keyed by the plan-tree `Node`
/// (funcapi result-type cluster's `call_expr`). The argument-bearing expression
/// nodes (`FuncExpr`/`OpExpr`/…) are not modeled by the plan-tree `Node` enum
/// (it carries only plan nodes), so this always falls through to `InvalidOid`,
/// matching the C fall-through for an unhandled kind.
fn seam_get_call_expr_argtype_node(
    _call_expr: &types_nodes::nodes::Node<'_>,
    _argnum: i32,
) -> Oid {
    InvalidOid
}

/// `exprInputCollation(node)` keyed by the plan-tree `Node`. The expression
/// nodes that carry an input collation are not modeled by the plan-tree `Node`
/// enum, so this returns `InvalidOid` (the C fall-through for an unhandled node
/// kind).
fn seam_expr_input_collation_node(_node: &types_nodes::nodes::Node<'_>) -> Oid {
    InvalidOid
}

/// `exprInputCollation(node)` over a field-bearing owned `Expr` — the funcapi
/// polymorphic resolver's read off the erased `FmgrInfo.fn_expr`.
fn seam_expr_input_collation_expr(expr: &Expr) -> Oid {
    expr_input_collation(Some(expr))
}

// `firstColType` helper for AlternativeSubPlan: read the shared first-column
// type of the choice's SubPlans (they all return the same thing).
trait SubPlanFirstCol {
    fn firstColType_via_sublink(&self) -> PgResult<Oid>;
}
impl SubPlanFirstCol for mcx::PgBox<'_, primnodes::SubPlan<'_>> {
    fn firstColType_via_sublink(&self) -> PgResult<Oid> {
        if self.subLinkType == SubLinkType::Expr
            || self.subLinkType == SubLinkType::Array
        {
            let mut t = self.firstColType;
            if self.subLinkType == SubLinkType::Array {
                t = lsyscache::get_promoted_array_type::call(t)?;
                if !oid_is_valid(t) {
                    return Err(no_array_type_error(self.firstColType)?);
                }
            }
            Ok(t)
        } else if self.subLinkType == SubLinkType::MultiExpr {
            Ok(RECORDOID)
        } else {
            Ok(BOOLOID)
        }
    }
}
