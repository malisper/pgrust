//! Port of `src/backend/parser/parse_expr.c` (PostgreSQL 18.3) — analyze and
//! transform raw grammar expressions into fully-typed expression trees.
//!
//! # Owned, split Expr/Node model
//!
//! [`transformExprRecurse`] is the central per-node-kind dispatcher. The C
//! `Node *expr` input is a raw-grammar [`types_nodes::nodes::Node`] (the
//! `A_Expr`/`A_Const`/`ColumnRef`/… vocabulary plus pass-through
//! `Node::Expr(Expr)` leaves); the output is always an expression node, modeled
//! as [`types_nodes::primnodes::Expr`]. A `List *` of nodes is a `PgVec<NodePtr>`
//! on the raw side and a `Vec<Expr>` on the typed side; a `NULL` pointer is
//! `Option::None`. There is no `extern "C"` and no raw pointers.
//!
//! # Seams
//!
//! Sibling parser subsystems that are not yet ported (`parse_coerce.c`,
//! `parse_func.c`, `parse_relation.c`, `parse_agg.c`, `parse_target.c`,
//! `analyze.c`, the SQL/XML & SQL/JSON transforms) are reached through their own
//! `*-seams` crates; a call panics loudly until the owner lands (mirror-PG-and-
//! panic). The merged siblings (`parse_oper.c`, `parse_type.c`,
//! `parse_collate.c`) and the catalog caches (`lsyscache.c`) are called
//! directly / through their installed seams.
//!
//! # Transform arms ported in-crate (full logic)
//!
//! The dispatcher itself plus: `A_Const` (`make_const`), the operator family
//! (`transformAExprOp`/`OpAny`/`OpAll`/`Distinct`/`NullIf`/`In`), `transformBoolExpr`,
//! `transformMergeSupportFunc`, `transformCoalesceExpr`, `transformMinMaxExpr`,
//! `transformSQLValueFunction`, `transformBooleanTest`, `transformCurrentOfExpr`,
//! `transformCollateClause`, `transformCaseExpr`, `transformTypeCast`,
//! `transformArrayExpr`, the row-comparison / DISTINCT builders
//! (`make_row_comparison_op`/`make_row_distinct_op`/`make_distinct_op`/
//! `make_nulltest_from_distinct`), `exprIsNullConstant`, and `ParseExprKindName`.
//!
//! # Transform arms reached through a panic-until-landed seam (named rationale)
//!
//! `transformColumnRef`/`transformParamRef`/`transformIndirection` (parse_relation
//! namespace machinery + parser hooks), `transformFuncCall` (ParseFuncOrColumn,
//! parse_func), `transformSubLink`/`transformMultiAssignRef` (analyze /
//! parse_target), `transformGroupingFunc` (parse_agg), `transformRowExpr`
//! (parse_target FigureColnames), `transformAExprBetween` (parse_target row
//! transform), `transformXmlExpr`/`transformXmlSerialize` (utils/adt/xml.c),
//! and the SQL/JSON constructor family (owning parse-node structs absent).

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::needless_range_loop)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use mcx::MemoryContext;

use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDETERMINATE_DATATYPE, ERRCODE_INTERNAL_ERROR, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_sortsupport::COMPARE_EQ;

/// `RowCompareExpr.cmptype` is the [`types_nodes::primnodes::CompareType`] enum;
/// the intersection logic in [`make_row_comparison_op`] works with the bare
/// `i32` cmptype carried by `OpIndexInterpretation`, so convert at the boundary.
fn cmptype_to_enum(c: i32) -> types_nodes::primnodes::CompareType {
    use types_nodes::primnodes::CompareType::*;
    match c {
        1 => COMPARE_LT,
        2 => COMPARE_LE,
        3 => COMPARE_EQ,
        4 => COMPARE_GE,
        5 => COMPARE_GT,
        6 => COMPARE_NE,
        _ => COMPARE_INVALID,
    }
}
use types_tuple::heaptuple::{
    BOOLOID, DATEOID, INT2VECTOROID, NAMEOID, OIDVECTOROID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, TIMETZOID, UNKNOWNOID,
};

use types_nodes::nodes::{self, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{
    ArrayExpr, BoolTestType, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr,
    CoercionForm, CollateExpr, CurrentOfExpr, Expr, MergeSupportFunc, MinMaxExpr, MinMaxOp,
    NullTest, NullTestType, OpExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp,
    AND_EXPR, NOT_EXPR, OR_EXPR,
};
use types_nodes::rawnodes::{A_Const, A_Expr, A_Expr_Kind, A_ArrayExpr, CollateClause, TypeCast};
use types_parsenodes::CoercionContext;

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::{make_bool_const, make_bool_expr};
use backend_nodes_core::nodefuncs::{
    expr_collation, expr_location, expr_type, expr_typmod, expression_returns_set,
};

use backend_parser_coerce_seams as coerce;
use backend_utils_cache_lsyscache_seams as lsyscache;

// ===========================================================================
// COMPARE_NE (access/cmptype.h): the no-such-btree-strategy "not equal"
// comparison type. The repo's types-sortsupport exports COMPARE_EQ/GT but not
// NE; pin the C value here.
// ===========================================================================

/// `COMPARE_NE` (access/cmptype.h) — value 6, the "<>" comparison type.
const COMPARE_NE: i32 = 6;

/// `Transform_null_equals` (utils/misc/guc_tables.c): the legacy MS-SQL-compat
/// GUC that rewrites `foo = NULL` into `foo IS NULL`. Defaults **off**, and the
/// GUC subsystem is not wired in; the compiled-in default (`false`) is faithful
/// to a stock server — the rewrite branch in [`transformAExprOp`] is dead, as
/// it is in a server with the default GUC.
const Transform_null_equals: bool = false;

// ===========================================================================
// Small helpers (the C `strVal` / `IsA` idioms).
// ===========================================================================

/// `strVal(node)` — the string contents of a boxed `String` value node.
fn str_val(node: &nodes::NodePtr<'_>) -> Option<String> {
    match &**node {
        Node::String(s) => Some(String::from(s.sval.as_str())),
        _ => None,
    }
}

/// Convert a raw `List *opname` (a `PgVec` of boxed `String` value nodes) into a
/// `Vec<String>` — the form `make_op`/`LookupOperName` consume. Non-`String`
/// elements are skipped (operator name lists are always `String` value nodes).
fn opname_strings(name: &mcx::PgVec<'_, nodes::NodePtr<'_>>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(name.len());
    for n in name.iter() {
        if let Some(s) = str_val(n) {
            out.push(s);
        }
    }
    out
}

/// `parser_errposition(pstate, location)` (parse_node.c) — translate a token
/// location into the 1-based cursor position recorded on an error, delegating to
/// the parse_node.c owner's seam (`backend-parser-small1`). The owner converts
/// the byte offset to a character index via `pg_mbstrlen_with_len`; it is
/// infallible (the `PgResult` seam contract always returns `Ok`), so a `0`
/// fallback is never observed.
fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> i32 {
    backend_parser_small1_seams::parser_errposition::call(pstate, location).unwrap_or(0)
}

/// Move the inner raw `Node` out of a `Option<NodePtr>` child by value (the C
/// reads `a->lexpr`/`a->rexpr`, then the transform consumes it). The owned model
/// moves out of the `PgBox` (no clone), preserving the `'mcx` lifetime.
fn boxed_node<'mcx>(child: Option<nodes::NodePtr<'mcx>>) -> Option<Node<'mcx>> {
    child.map(|b| mcx::PgBox::into_inner(b))
}

// ===========================================================================
// transformExpr / transformExprRecurse (parse_expr.c lines 118-389).
// ===========================================================================

/// `transformExpr(pstate, expr, exprKind)` — analyze and transform an
/// expression. Saves/restores `pstate->p_expr_kind` around the recursion.
pub fn transformExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    expr: Option<Node<'mcx>>,
    expr_kind: ParseExprKind,
) -> PgResult<Option<Expr>> {
    // Assert(exprKind != EXPR_KIND_NONE);
    debug_assert!(expr_kind != ParseExprKind::EXPR_KIND_NONE);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = expr_kind;

    let result = transformExprRecurse(pstate, expr);

    pstate.p_expr_kind = sv_expr_kind;

    result
}

/// `transformExprRecurse(pstate, expr)` — the per-node-kind `switch`.
pub fn transformExprRecurse<'mcx>(
    pstate: &mut ParseState<'mcx>,
    expr: Option<Node<'mcx>>,
) -> PgResult<Option<Expr>> {
    let Some(expr) = expr else {
        return Ok(None);
    };

    // check_stack_depth() — the recursion guard is the host stack; no explicit
    // depth counter is modeled (matching the other parser ports).

    let result: Expr = match expr {
        Node::ColumnRef(_) => seam_transform_column_ref(pstate, expr)?,
        Node::ParamRef(_) => seam_transform_param_ref(pstate, expr)?,

        // T_A_Const → make_const(pstate, (A_Const *) expr).
        Node::A_Const(a) => transform_a_const(pstate, a)?,

        Node::A_Indirection(_) => seam_transform_indirection(pstate, expr)?,

        // transformArrayExpr(pstate, a, InvalidOid, InvalidOid, -1).
        Node::A_ArrayExpr(a) => transformArrayExpr(pstate, a, InvalidOid, InvalidOid, -1)?,

        Node::TypeCast(_) => transformTypeCast(pstate, expr)?,

        Node::CollateClause(c) => transformCollateClause(pstate, c)?,

        Node::A_Expr(a) => {
            // Nested switch on a->kind (parse_expr.c:175-216).
            match a.kind {
                A_Expr_Kind::AEXPR_OP => transformAExprOp(pstate, a)?,
                A_Expr_Kind::AEXPR_OP_ANY => transformAExprOpAny(pstate, a)?,
                A_Expr_Kind::AEXPR_OP_ALL => transformAExprOpAll(pstate, a)?,
                A_Expr_Kind::AEXPR_DISTINCT | A_Expr_Kind::AEXPR_NOT_DISTINCT => {
                    transformAExprDistinct(pstate, a)?
                }
                A_Expr_Kind::AEXPR_NULLIF => transformAExprNullIf(pstate, a)?,
                A_Expr_Kind::AEXPR_IN => transformAExprIn(pstate, a)?,
                // LIKE / ILIKE / SIMILAR transform exactly like AEXPR_OP.
                A_Expr_Kind::AEXPR_LIKE
                | A_Expr_Kind::AEXPR_ILIKE
                | A_Expr_Kind::AEXPR_SIMILAR => transformAExprOp(pstate, a)?,
                A_Expr_Kind::AEXPR_BETWEEN
                | A_Expr_Kind::AEXPR_NOT_BETWEEN
                | A_Expr_Kind::AEXPR_BETWEEN_SYM
                | A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
                    seam_transform_a_expr_between(pstate, Node::A_Expr(a))?
                }
            }
        }

        Node::FuncCall(_) => seam_transform_func_call(pstate, expr)?,
        Node::MultiAssignRef(_) => seam_transform_multi_assign_ref(pstate, expr)?,

        // Expr-carried nodes that reach the dispatcher untransformed-or-recursed.
        Node::Expr(e) => transform_expr_node(pstate, e)?,

        // DEFAULT must have been processed by the caller (handled in the
        // `Node::Expr(SetToDefault)` arm of `transform_expr_node`).

        _ => {
            // The C default raises elog(ERROR, "unrecognized node type: %d").
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!("unrecognized node type: {}", expr.node_tag().0))
                .into_error());
        }
    };

    Ok(Some(result))
}

/// Dispatch the `Node::Expr(Expr)`-carried arms of the C switch
/// (`T_BoolExpr`/`T_GroupingFunc`/`T_MergeSupportFunc`/`T_NamedArgExpr`/
/// `T_SubLink`/`T_CaseExpr`/`T_RowExpr`/`T_CoalesceExpr`/`T_MinMaxExpr`/
/// `T_SQLValueFunction`/`T_XmlExpr`/`T_XmlSerialize`/`T_NullTest`/
/// `T_BooleanTest`/`T_CurrentOfExpr`/`T_SetToDefault`/`T_CaseTestExpr`/`T_Var`/
/// the SQL/JSON family).
fn transform_expr_node<'mcx>(
    pstate: &mut ParseState<'mcx>,
    e: Expr,
) -> PgResult<Expr> {
    match e {
        Expr::BoolExpr(a) => transformBoolExpr(pstate, a),
        Expr::GroupingFunc(_) => seam_transform_grouping_func(pstate, Node::Expr(e)),
        Expr::MergeSupportFunc(f) => transformMergeSupportFunc(pstate, f),

        Expr::NamedArgExpr(mut na) => {
            // na->arg = transformExprRecurse(pstate, na->arg); result = expr.
            let arg = na.arg.take().map(|b| expr_to_node(*b));
            let new_arg = transformExprRecurse(pstate, arg)?;
            na.arg = new_arg.map(Box::new);
            Ok(Expr::NamedArgExpr(na))
        }

        Expr::SubLink(_) => seam_transform_sublink(pstate, Node::Expr(e)),
        Expr::CaseExpr(c) => transformCaseExpr(pstate, c),
        Expr::RowExpr(_) => seam_transform_row_expr(pstate, Node::Expr(e), false),
        Expr::CoalesceExpr(c) => transformCoalesceExpr(pstate, c),
        Expr::MinMaxExpr(m) => transformMinMaxExpr(pstate, m),
        Expr::SQLValueFunction(svf) => transformSQLValueFunction(pstate, svf),
        Expr::XmlExpr(_) => seam_transform_xml_expr(pstate, Node::Expr(e)),

        Expr::NullTest(mut n) => {
            // n->arg = transformExprRecurse(...); argisrow from arg's type.
            let arg = n.arg.take().map(|b| expr_to_node(*b));
            let new_arg = transformExprRecurse(pstate, arg)?;
            n.argisrow = lsyscache::type_is_rowtype::call(expr_type(new_arg.as_ref())?)?;
            n.arg = new_arg.map(Box::new);
            Ok(Expr::NullTest(n))
        }

        Expr::BooleanTest(b) => transformBooleanTest(pstate, b),
        Expr::CurrentOfExpr(c) => transformCurrentOfExpr(pstate, c),

        // DEFAULT must have been processed by the caller.
        Expr::SetToDefault(_) => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("DEFAULT is not allowed in this context")
            .into_error()),

        // CaseTestExpr / Var are passed through untransformed (parse_expr.c:303).
        Expr::CaseTestExpr(_) | Expr::Var(_) => Ok(e),

        // SQL/JSON predicate — owning parse-node vocabulary not yet in types.
        Expr::JsonIsPredicate(_) => seam_transform_json_expr(pstate, Node::Expr(e)),

        other => Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!(
                "unrecognized node type: {}",
                Node::Expr(other).node_tag().0
            ))
            .into_error()),
    }
}

/// Wrap a typed `Expr` back into a raw `Node` for re-entry into
/// [`transformExprRecurse`] (the C casts `(Node *) expr` freely).
fn expr_to_node(e: Expr) -> Node<'static> {
    Node::Expr(e)
}

// ===========================================================================
// make_const dispatch arm (T_A_Const → parse_node.c make_const).
// ===========================================================================

/// `make_const(pstate, (A_Const *) expr)` — build a typed `Const` from a grammar
/// `A_Const` literal value node.
///
/// `make_const` lives in `parse_node.c`, owned by the merged `backend-parser-
/// small1` unit (landed, cycle-free); called directly. The resulting `Const`
/// carries a `Datum<'static>` (by-value literals; the by-ref string/numeric/
/// bitstring arms panic inside the owner pending the canonical Datum carrier),
/// so a scratch context for the decode is faithful (the parse-collate /
/// parse-type precedent).
fn transform_a_const<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Const<'mcx>,
) -> PgResult<Expr> {
    let scratch = MemoryContext::new("make_const");
    let con = backend_parser_small1::make_const(scratch.mcx(), &*pstate, &a)?;
    Ok(Expr::Const(con))
}

// ===========================================================================
// exprIsNullConstant (parse_expr.c:908).
// ===========================================================================

/// `exprIsNullConstant(arg)` — true if `arg` is an undecorated NULL `A_Const`.
fn exprIsNullConstant(arg: Option<&Node<'_>>) -> bool {
    matches!(arg, Some(Node::A_Const(con)) if con.isnull)
}

// ===========================================================================
// A_Expr operator transforms (parse_expr.c:921-1091).
// ===========================================================================

/// `transformAExprOp(pstate, a)` — an ordinary, ANY/ALL-free binary operator.
fn transformAExprOp<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, location, ..
    } = a;
    let lexpr = boxed_node(lexpr);
    let rexpr = boxed_node(rexpr);

    // Special-case "foo = NULL" / "NULL = foo" only when Transform_null_equals
    // is on (default off). The whole branch is therefore dead in a stock
    // server; kept structurally for faithfulness.
    let is_eq_name = name.len() == 1 && name.iter().next().and_then(str_val).as_deref() == Some("=");
    let either_null = exprIsNullConstant(lexpr.as_ref()) || exprIsNullConstant(rexpr.as_ref());
    let neither_casetest = !is_casetestexpr(lexpr.as_ref()) && !is_casetestexpr(rexpr.as_ref());

    if Transform_null_equals && is_eq_name && either_null && neither_casetest {
        // Build a NullTest on the non-NULL side and recurse on its arg.
        let arg = if exprIsNullConstant(lexpr.as_ref()) {
            rexpr
        } else {
            lexpr
        };
        let new_arg = transformExprRecurse(pstate, arg)?;
        return Ok(Expr::NullTest(NullTest {
            arg: new_arg.map(Box::new),
            nulltesttype: NullTestType::IS_NULL,
            argisrow: false,
            // n->location = a->location;
            location,
        }));
    }

    // "row op subselect" → ROWCOMPARE sublink: rewrites the SubLink and
    // recurses; needs SubLink transform machinery — routed through the SubLink
    // seam (parse_expr.c:953-973).
    if is_rowexpr(lexpr.as_ref()) && is_expr_sublink(rexpr.as_ref()) {
        let sublink = build_rowcompare_sublink(lexpr.unwrap(), rexpr.unwrap())?;
        return seam_transform_sublink(pstate, sublink);
    }

    if is_rowexpr(lexpr.as_ref()) && is_rowexpr(rexpr.as_ref()) {
        // ROW() op ROW() — transform both rows then build the row comparison.
        let lexpr_t = transformExprRecurse(pstate, lexpr)?;
        let rexpr_t = transformExprRecurse(pstate, rexpr)?;
        let largs = row_args(lexpr_t);
        let rargs = row_args(rexpr_t);
        return make_row_comparison_op(pstate, &name, largs, rargs, location);
    }

    // Ordinary scalar operator.
    let lexpr_t = transformExprRecurse(pstate, lexpr)?;
    let rexpr_t = transformExprRecurse(pstate, rexpr)?;
    let last_srf = last_srf_expr(pstate);
    let opname = opname_strings(&name);
    let res = backend_parser_parse_oper::make_op(
        Some(&*pstate),
        &opname,
        lexpr_t,
        rexpr_t,
        last_srf.as_ref(),
        location,
    )?;
    Ok(res)
}

/// Build the rewritten `SubLink` node for the "row op subselect" case
/// (parse_expr.c:953-973): the SubLink is the original `rexpr`; set its
/// `subLinkType`/`testexpr`. The trimmed [`SubLink`] drops `operName`/`location`
/// (the row-comparison operator-name and token position the C carries), so only
/// the structurally-modeled fields are set; the full transform is the
/// (unported) SubLink seam's responsibility.
fn build_rowcompare_sublink<'mcx>(
    lexpr: Node<'mcx>,
    rexpr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let Node::Expr(Expr::SubLink(mut s)) = rexpr else {
        return Ok(rexpr);
    };
    s.subLinkType = types_nodes::primnodes::SubLinkType::RowCompare;
    s.testexpr = node_into_expr(lexpr).map(Box::new);
    Ok(Node::Expr(Expr::SubLink(s)))
}

/// Extract a `RowExpr`'s already-transformed `args` (`castNode(RowExpr,x)->args`).
fn row_args(node: Option<Expr>) -> Vec<Expr> {
    match node {
        Some(Expr::RowExpr(r)) => r.args,
        _ => Vec::new(),
    }
}

/// `transformAExprOpAny(pstate, a)` — `scalar op ANY (array)`.
fn transformAExprOpAny<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, location, ..
    } = a;
    let lexpr = transformExprRecurse(pstate, boxed_node(lexpr))?
        .ok_or_else(|| PgError::error("transformAExprOpAny: lefthand is NULL"))?;
    let rexpr = transformExprRecurse(pstate, boxed_node(rexpr))?
        .ok_or_else(|| PgError::error("transformAExprOpAny: righthand is NULL"))?;
    let opname = opname_strings(&name);
    backend_parser_parse_oper::make_scalar_array_op(
        Some(&*pstate),
        &opname,
        true,
        lexpr,
        rexpr,
        location,
    )
}

/// `transformAExprOpAll(pstate, a)` — `scalar op ALL (array)`.
fn transformAExprOpAll<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, location, ..
    } = a;
    let lexpr = transformExprRecurse(pstate, boxed_node(lexpr))?
        .ok_or_else(|| PgError::error("transformAExprOpAll: lefthand is NULL"))?;
    let rexpr = transformExprRecurse(pstate, boxed_node(rexpr))?
        .ok_or_else(|| PgError::error("transformAExprOpAll: righthand is NULL"))?;
    let opname = opname_strings(&name);
    backend_parser_parse_oper::make_scalar_array_op(
        Some(&*pstate),
        &opname,
        false,
        lexpr,
        rexpr,
        location,
    )
}

/// `transformAExprIn(pstate, a)` (parse_expr.c:1089) — the `[NOT] IN
/// (value-list)` transform.
///
/// SEAM-AND-PANIC (named rationale): the C body destructures the list-valued
/// `a->rexpr` (`(List *) a->rexpr`) — a `List *` of element nodes. In this
/// repo's owned model a `List`'s `ListCell` is a C union over a raw
/// `*mut c_void`/`int`/`Oid` (nodes/pg_list.h ABI), so a `List` of owned
/// expression *nodes* cannot be walked without dereferencing raw pointers, which
/// the no-raw-pointer model forbids. The grammar carries the IN value-list this
/// way, so faithful destructuring is blocked until the parse-node carrier for an
/// expression-list lands. Routed to a panic-until-landed seam.
fn transformAExprIn<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    seam_transform_a_expr_list(pstate, Node::A_Expr(a), "transformAExprIn")
}

/// `transformAExprDistinct(pstate, a)` — `IS [NOT] DISTINCT FROM`.
fn transformAExprDistinct<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, kind, location, ..
    } = a;
    let lexpr = boxed_node(lexpr);
    let rexpr = boxed_node(rexpr);

    // Undecorated NULL on either side → NullTest on the other side.
    if exprIsNullConstant(rexpr.as_ref()) {
        return make_nulltest_from_distinct(pstate, kind, lexpr, location);
    }
    if exprIsNullConstant(lexpr.as_ref()) {
        return make_nulltest_from_distinct(pstate, kind, rexpr, location);
    }

    let lexpr_t = transformExprRecurse(pstate, lexpr)?;
    let rexpr_t = transformExprRecurse(pstate, rexpr)?;

    let mut result = if is_rowexpr_expr_opt(lexpr_t.as_ref()) && is_rowexpr_expr_opt(rexpr_t.as_ref())
    {
        let lrow = match lexpr_t {
            Some(Expr::RowExpr(r)) => r,
            _ => unreachable!(),
        };
        let rrow = match rexpr_t {
            Some(Expr::RowExpr(r)) => r,
            _ => unreachable!(),
        };
        make_row_distinct_op(pstate, &name, lrow, rrow, location)?
    } else {
        make_distinct_op(pstate, &name, lexpr_t, rexpr_t, location)?
    };

    // NOT DISTINCT → wrap the DistinctExpr in a NOT.
    if kind == A_Expr_Kind::AEXPR_NOT_DISTINCT {
        result = make_bool_expr(NOT_EXPR, vec![result], location);
    }

    Ok(result)
}

/// `transformAExprNullIf(pstate, a)` — `NULLIF(a, b)`.
fn transformAExprNullIf<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, location, ..
    } = a;
    let lexpr = transformExprRecurse(pstate, boxed_node(lexpr))?;
    let rexpr = transformExprRecurse(pstate, boxed_node(rexpr))?;

    let opname = opname_strings(&name);
    let last_srf = last_srf_expr(pstate);
    let result = backend_parser_parse_oper::make_op(
        Some(&*pstate),
        &opname,
        lexpr,
        rexpr,
        last_srf.as_ref(),
        location,
    )?;

    let mut op = match result {
        Expr::OpExpr(op) => op,
        other => return Ok(other),
    };

    // The comparison operator must yield boolean and not a set.
    if op.opresulttype != BOOLOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("NULLIF requires = operator to yield boolean")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
    if op.opretset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("NULLIF must not return a set")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // The NullIfExpr yields the first operand's type.
    op.opresulttype = expr_type(op.args.first())?;
    // NodeSetTag(result, T_NullIfExpr): NullIfExpr is a `typedef OpExpr`; the
    // repo models it as `Expr::NullIfExpr(OpExpr)`.
    Ok(Expr::NullIfExpr(op))
}

// ===========================================================================
// transformBoolExpr (parse_expr.c:1412).
// ===========================================================================

fn transformBoolExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: types_nodes::primnodes::BoolExpr,
) -> PgResult<Expr> {
    let opname = match a.boolop {
        AND_EXPR => "AND",
        OR_EXPR => "OR",
        NOT_EXPR => "NOT",
    };

    let mut args: Vec<Expr> = Vec::with_capacity(a.args.len());
    for arg in a.args {
        let arg = transformExprRecurse(pstate, Some(expr_to_node(arg)))?
            .ok_or_else(|| PgError::error("transformBoolExpr: BoolExpr argument is NULL"))?;
        let arg = coerce::coerce_to_boolean::call(pstate, arg, opname)?;
        args.push(arg);
    }

    Ok(make_bool_expr(a.boolop, args, -1))
}

// ===========================================================================
// transformMergeSupportFunc (parse_expr.c:1388).
// ===========================================================================

fn transformMergeSupportFunc<'mcx>(
    pstate: &mut ParseState<'mcx>,
    f: MergeSupportFunc,
) -> PgResult<Expr> {
    // Must appear in the RETURNING list of a MERGE (this level or an ancestor).
    if pstate.p_expr_kind != ParseExprKind::EXPR_KIND_MERGE_RETURNING {
        let mut found = false;
        let mut parent = pstate.parentParseState.as_deref();
        while let Some(p) = parent {
            if p.p_expr_kind == ParseExprKind::EXPR_KIND_MERGE_RETURNING {
                found = true;
                break;
            }
            parent = p.parentParseState.as_deref();
        }
        if !found {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(
                    "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command",
                )
                .into_error());
        }
    }
    Ok(Expr::MergeSupportFunc(f))
}

// ===========================================================================
// transformCoalesceExpr / transformMinMaxExpr (parse_expr.c:2225-2313).
// ===========================================================================

fn transformCoalesceExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    c: CoalesceExpr,
) -> PgResult<Expr> {
    let last_srf = clone_last_srf(pstate);
    let location = c.location;

    let mut newargs: Vec<Expr> = Vec::with_capacity(c.args.len());
    for e in c.args {
        let newe = transformExprRecurse(pstate, Some(expr_to_node(e)))?
            .ok_or_else(|| PgError::error("transformCoalesceExpr: COALESCE argument is NULL"))?;
        newargs.push(newe);
    }

    let coalescetype = coerce::select_common_type::call(pstate, &newargs, Some("COALESCE"))?;

    let mut newcoercedargs: Vec<Expr> = Vec::with_capacity(newargs.len());
    for e in newargs {
        let newe = coerce::coerce_to_common_type::call(pstate, e, coalescetype, "COALESCE")?;
        newcoercedargs.push(newe);
    }

    srf_check(pstate, &last_srf, "COALESCE")?;

    Ok(Expr::CoalesceExpr(CoalesceExpr {
        coalescetype,
        coalescecollid: InvalidOid,
        args: newcoercedargs,
        // newc->location = c->location;
        location,
    }))
}

fn transformMinMaxExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    m: MinMaxExpr,
) -> PgResult<Expr> {
    let funcname = if m.op == MinMaxOp::IS_GREATEST {
        "GREATEST"
    } else {
        "LEAST"
    };
    let location = m.location;

    let mut newargs: Vec<Expr> = Vec::with_capacity(m.args.len());
    for e in m.args {
        let newe = transformExprRecurse(pstate, Some(expr_to_node(e)))?
            .ok_or_else(|| PgError::error("transformMinMaxExpr: GREATEST/LEAST argument is NULL"))?;
        newargs.push(newe);
    }

    let minmaxtype = coerce::select_common_type::call(pstate, &newargs, Some(funcname))?;

    let mut newcoercedargs: Vec<Expr> = Vec::with_capacity(newargs.len());
    for e in newargs {
        let newe = coerce::coerce_to_common_type::call(pstate, e, minmaxtype, funcname)?;
        newcoercedargs.push(newe);
    }

    Ok(Expr::MinMaxExpr(MinMaxExpr {
        minmaxtype,
        minmaxcollid: InvalidOid,
        inputcollid: InvalidOid,
        op: m.op,
        args: newcoercedargs,
        // newm->location = m->location;
        location,
    }))
}

/// The shared "set-returning functions are not allowed in %s" check used by
/// CASE / COALESCE (parse_expr.c). `last_srf` is `p_last_srf` captured before
/// transforming the sub-expressions; the C compares the raw pointer. The owned
/// model detects "a new SRF was recorded" by node-tag / location difference.
fn srf_check(
    pstate: &ParseState<'_>,
    last_srf: &Option<(types_nodes::nodes::NodeTag, i32)>,
    construct: &str,
) -> PgResult<()> {
    let now = pstate
        .p_last_srf
        .as_ref()
        .map(|b| (b.node_tag(), expr_location(node_as_expr(b)).unwrap_or(-1)));
    let changed = now != *last_srf;
    if changed {
        let loc = pstate
            .p_last_srf
            .as_ref()
            .and_then(|b| expr_location(node_as_expr(b)).ok())
            .unwrap_or(-1);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "set-returning functions are not allowed in {}",
                construct
            ))
            .errhint(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
            )
            .errposition(parser_errposition(pstate, loc))
            .into_error());
    }
    Ok(())
}

/// Snapshot `p_last_srf` as a (tag, location) identity for [`srf_check`].
fn clone_last_srf(pstate: &ParseState<'_>) -> Option<(types_nodes::nodes::NodeTag, i32)> {
    pstate
        .p_last_srf
        .as_ref()
        .map(|b| (b.node_tag(), expr_location(node_as_expr(b)).unwrap_or(-1)))
}

/// View the inner `Expr` of a boxed `p_last_srf` `Node`.
fn node_as_expr<'a>(b: &'a nodes::NodePtr<'_>) -> Option<&'a Expr> {
    match &**b {
        Node::Expr(e) => Some(e),
        _ => None,
    }
}

// ===========================================================================
// transformSQLValueFunction (parse_expr.c:2313).
// ===========================================================================

fn transformSQLValueFunction<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    mut svf: SQLValueFunction,
) -> PgResult<Expr> {
    use SQLValueFunctionOp::*;
    match svf.op {
        SVFOP_CURRENT_DATE => svf.r#type = DATEOID,
        SVFOP_CURRENT_TIME => svf.r#type = TIMETZOID,
        SVFOP_CURRENT_TIME_N => {
            svf.r#type = TIMETZOID;
            svf.typmod = lsyscache_anytime_typmod_check(true, svf.typmod)?;
        }
        SVFOP_CURRENT_TIMESTAMP => svf.r#type = TIMESTAMPTZOID,
        SVFOP_CURRENT_TIMESTAMP_N => {
            svf.r#type = TIMESTAMPTZOID;
            svf.typmod = lsyscache_anytimestamp_typmod_check(true, svf.typmod)?;
        }
        SVFOP_LOCALTIME => svf.r#type = TIMEOID,
        SVFOP_LOCALTIME_N => {
            svf.r#type = TIMEOID;
            svf.typmod = lsyscache_anytime_typmod_check(false, svf.typmod)?;
        }
        SVFOP_LOCALTIMESTAMP => svf.r#type = TIMESTAMPOID,
        SVFOP_LOCALTIMESTAMP_N => {
            svf.r#type = TIMESTAMPOID;
            svf.typmod = lsyscache_anytimestamp_typmod_check(false, svf.typmod)?;
        }
        SVFOP_CURRENT_ROLE | SVFOP_CURRENT_USER | SVFOP_USER | SVFOP_SESSION_USER
        | SVFOP_CURRENT_CATALOG | SVFOP_CURRENT_SCHEMA => svf.r#type = NAMEOID,
    }
    Ok(Expr::SQLValueFunction(svf))
}

// ===========================================================================
// transformBooleanTest (parse_expr.c:2539).
// ===========================================================================

fn transformBooleanTest<'mcx>(
    pstate: &mut ParseState<'mcx>,
    mut b: BooleanTest,
) -> PgResult<Expr> {
    let clausename = match b.booltesttype {
        BoolTestType::IS_TRUE => "IS TRUE",
        BoolTestType::IS_NOT_TRUE => "IS NOT TRUE",
        BoolTestType::IS_FALSE => "IS FALSE",
        BoolTestType::IS_NOT_FALSE => "IS NOT FALSE",
        BoolTestType::IS_UNKNOWN => "IS UNKNOWN",
        BoolTestType::IS_NOT_UNKNOWN => "IS NOT UNKNOWN",
    };

    let arg = b.arg.take().map(|x| expr_to_node(*x));
    let arg = transformExprRecurse(pstate, arg)?
        .ok_or_else(|| PgError::error("transformBooleanTest: BooleanTest argument is NULL"))?;
    let arg = coerce::coerce_to_boolean::call(pstate, arg, clausename)?;
    b.arg = Some(Box::new(arg));
    Ok(Expr::BooleanTest(b))
}

// ===========================================================================
// transformCurrentOfExpr (parse_expr.c:2580).
// ===========================================================================

fn transformCurrentOfExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    mut cexpr: CurrentOfExpr,
) -> PgResult<Expr> {
    // CURRENT OF can only appear at top level of UPDATE/DELETE.
    let nsitem = pstate.p_target_nsitem.as_ref().ok_or_else(|| {
        PgError::error("transformCurrentOfExpr: CURRENT OF requires a target nsitem")
    })?;
    cexpr.cvarno = nsitem.p_rtindex as types_core::Index;

    // The cursor-name → REFCURSOR-Param rewrite consults the columnref parser
    // hooks (opaque cross-ABI function pointers). With no hook installed the C
    // takes the "no translation" path and leaves the node unchanged; that is
    // exactly what happens here when the hooks are absent. The hook-installed
    // path needs the columnref-hook ABI — reached via the columnref seam.
    if cexpr.cursor_name.is_some()
        && (pstate.p_pre_columnref_hook.is_some() || pstate.p_post_columnref_hook.is_some())
    {
        return seam_transform_column_ref_hook_currentof(pstate, Expr::CurrentOfExpr(cexpr));
    }

    Ok(Expr::CurrentOfExpr(cexpr))
}

// ===========================================================================
// transformCollateClause (parse_expr.c:2797).
// ===========================================================================

fn transformCollateClause<'mcx>(
    pstate: &mut ParseState<'mcx>,
    c: CollateClause<'mcx>,
) -> PgResult<Expr> {
    let CollateClause {
        arg, collname, location,
    } = c;
    let new_arg = transformExprRecurse(pstate, boxed_node(arg))?;

    let argtype = expr_type(new_arg.as_ref())?;

    // The unknown type is not collatable but coerce_type() handles it; let it go.
    if !lsyscache::type_is_collatable::call(argtype)? && argtype != UNKNOWNOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(alloc::format!(
                "collations are not supported by type {}",
                format_type_be(argtype)?
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // LookupCollation(pstate, c->collname, c->location). The collname is a
    // `List *` of `String` value nodes. The merged parse_type owner consumes the
    // parser's own node vocabulary (`types_parsenodes::Node`), distinct from the
    // raw-grammar `types_nodes::Node` this dispatcher carries; bridge the
    // String-only collname list across the two vocabularies (the only node kind
    // a collation name list ever contains).
    let mut collname_pn: Vec<types_parsenodes::Node> = Vec::with_capacity(collname.len());
    for n in collname.into_iter() {
        match mcx::PgBox::into_inner(n) {
            Node::String(s) => collname_pn.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(String::from(s.sval.as_str())),
                },
            )),
            other => {
                return Err(PgError::error(alloc::format!(
                    "transformCollateClause: collname element is not a String value node (tag {})",
                    other.node_tag().0
                )))
            }
        }
    }
    let scratch = MemoryContext::new("transformCollateClause");
    let coll_oid = backend_parser_parse_type::LookupCollation(
        scratch.mcx(),
        Some(&*pstate),
        &collname_pn,
        location,
    )?;

    Ok(Expr::CollateExpr(CollateExpr {
        arg: new_arg.map(Box::new),
        collOid: coll_oid,
        // newc->location = c->location;
        location,
    }))
}

// ===========================================================================
// transformCaseExpr (parse_expr.c:1641).
// ===========================================================================

/// `transformCaseExpr(pstate, c)`.
fn transformCaseExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    c: CaseExpr,
) -> PgResult<Expr> {
    let last_srf = clone_last_srf(pstate);
    let case_location = c.location;

    // Transform the test expression, if any.
    let arg = c.arg.map(|b| expr_to_node(*b));
    let arg = transformExprRecurse(pstate, arg)?;

    // Generate the placeholder for the test expression.
    let (arg, placeholder): (Option<Expr>, Option<CaseTestExpr>) = match arg {
        Some(mut a) => {
            if expr_type(Some(&a))? == UNKNOWNOID {
                a = coerce::coerce_to_common_type::call(pstate, a, TEXTOID, "CASE")?;
            }
            // Run collation assignment on the test expression.
            assign_expr_collations(pstate, &mut a)?;
            let placeholder = CaseTestExpr {
                typeId: expr_type(Some(&a))?,
                typeMod: expr_typmod(Some(&a))?,
                collation: expr_collation(Some(&a))?,
            };
            (Some(a), Some(placeholder))
        }
        None => (None, None),
    };

    // Transform the WHEN/THEN list.
    let mut newargs: Vec<CaseWhen> = Vec::with_capacity(c.args.len());
    let mut resultexprs: Vec<Expr> = Vec::new();
    for w in c.args {
        let when_location = w.location;
        // Optional CASE shorthand (form 2): expand `placeholder = warg`.
        // The C builds `makeSimpleA_Expr(AEXPR_OP, "=", placeholder, warg)` then
        // recurses — which transforms `warg` (the `placeholder` `CaseTestExpr`
        // passes through unchanged) and builds the `=` `OpExpr`. The owned model
        // reproduces this directly: transform `warg`, then `make_op("=",
        // CaseTestExpr, warg_t)` — equivalent to recursing on the synthesized
        // A_Expr but without a raw-pointer-backed transient `List` opname.
        let cond = if let Some(ph) = &placeholder {
            let warg = w.expr.map(|b| expr_to_node(*b));
            let warg_t = transformExprRecurse(pstate, warg)?;
            let eqname = vec![String::from("=")];
            let last_srf = last_srf_expr(pstate);
            let res = backend_parser_parse_oper::make_op(
                Some(&*pstate),
                &eqname,
                Some(Expr::CaseTestExpr(ph.clone())),
                warg_t,
                last_srf.as_ref(),
                // makeSimpleA_Expr(..., w->location).
                when_location,
            )?;
            res
        } else {
            let cond = w.expr.map(|b| expr_to_node(*b));
            transformExprRecurse(pstate, cond)?
                .ok_or_else(|| PgError::error("transformCaseExpr: CASE/WHEN condition is NULL"))?
        };
        let cond = coerce::coerce_to_boolean::call(pstate, cond, "CASE/WHEN")?;

        let wresult = w.result.map(|b| expr_to_node(*b));
        let wresult = transformExprRecurse(pstate, wresult)?
            .ok_or_else(|| PgError::error("transformCaseExpr: CASE/THEN result is NULL"))?;

        resultexprs.push(wresult.clone());
        newargs.push(CaseWhen {
            expr: Some(Box::new(cond)),
            result: Some(Box::new(wresult)),
            // neww->location = w->location;
            location: when_location,
        });
    }

    // Transform the default clause; NULL → untyped NULL A_Const.
    let defresult_node: Node<'static> = match c.defresult {
        Some(d) => expr_to_node(*d),
        None => Node::A_Const(A_Const {
            val: None,
            isnull: true,
            location: -1,
        }),
    };
    let mut defresult = transformExprRecurse(pstate, Some(defresult_node))?
        .ok_or_else(|| PgError::error("transformCaseExpr: CASE default result is NULL"))?;

    // Common type: default result first (lcons), then WHEN results.
    let mut common_inputs: Vec<Expr> = Vec::with_capacity(resultexprs.len() + 1);
    common_inputs.push(defresult.clone());
    common_inputs.extend(resultexprs);

    let ptype = coerce::select_common_type::call(pstate, &common_inputs, Some("CASE"))?;
    let casetype = ptype;

    // Coerce the default result, then each WHEN result.
    defresult = coerce::coerce_to_common_type::call(pstate, defresult, ptype, "CASE/ELSE")?;

    let mut coerced_args: Vec<CaseWhen> = Vec::with_capacity(newargs.len());
    for mut w in newargs {
        let result = w
            .result
            .take()
            .map(|b| *b)
            .ok_or_else(|| PgError::error("transformCaseExpr: WHEN result is NULL"))?;
        let coerced = coerce::coerce_to_common_type::call(pstate, result, ptype, "CASE/WHEN")?;
        w.result = Some(Box::new(coerced));
        coerced_args.push(w);
    }

    // Complain about any SRF that appeared.
    srf_check(pstate, &last_srf, "CASE")?;

    Ok(Expr::CaseExpr(CaseExpr {
        casetype,
        casecollid: InvalidOid,
        arg: arg.map(Box::new),
        args: coerced_args,
        defresult: Some(Box::new(defresult)),
        // newc->location = c->location;
        location: case_location,
    }))
}

// ===========================================================================
// Row-comparison / DISTINCT builders (parse_expr.c:2838-3142).
// ===========================================================================

/// `make_row_comparison_op(pstate, opname, largs, rargs, location)`.
fn make_row_comparison_op<'mcx>(
    pstate: &mut ParseState<'mcx>,
    opname: &mcx::PgVec<'_, nodes::NodePtr<'_>>,
    largs: Vec<Expr>,
    rargs: Vec<Expr>,
    location: i32,
) -> PgResult<Expr> {
    let nopers = largs.len();
    if nopers != rargs.len() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("unequal number of entries in row expressions")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
    if nopers == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot compare rows of zero length")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    let opname_s = opname_strings(opname);

    // Identify the pairwise operators with make_op.
    let mut opexprs: Vec<OpExpr> = Vec::with_capacity(nopers);
    for (larg, rarg) in largs.into_iter().zip(rargs.into_iter()) {
        let last_srf = last_srf_expr(pstate);
        let cmp_node = backend_parser_parse_oper::make_op(
            Some(&*pstate),
            &opname_s,
            Some(larg),
            Some(rarg),
            last_srf.as_ref(),
            location,
        )?;
        let cmp = match cmp_node {
            Expr::OpExpr(op) => op,
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg("make_op did not return an OpExpr in row comparison")
                    .into_error())
            }
        };

        // The operator must yield boolean directly and not a set.
        if cmp.opresulttype != BOOLOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(alloc::format!(
                    "row comparison operator must yield type boolean, not type {}",
                    format_type_be(cmp.opresulttype)?
                ))
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        if expression_returns_set(Some(&Expr::OpExpr(cmp.clone()))) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("row comparison operator must not return a set")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        opexprs.push(cmp);
    }

    // Length-1 rows: return the single operator directly.
    if nopers == 1 {
        return Ok(Expr::OpExpr(opexprs.into_iter().next().unwrap()));
    }

    // Intersect the comparison types found in the opfamilies for each operator.
    let scratch = MemoryContext::new("make_row_comparison_op");
    let mut opinfo_lists: Vec<
        mcx::PgVec<'_, lsyscache::OpIndexInterpretation>,
    > = Vec::with_capacity(nopers);
    let mut common_cmptypes: Option<Vec<i32>> = None;
    for op in &opexprs {
        let interps = lsyscache::get_op_index_interpretation::call(scratch.mcx(), op.opno)?;
        let mut this: Vec<i32> = Vec::new();
        for interp in interps.iter() {
            let ct = interp.cmptype;
            if !this.contains(&ct) {
                this.push(ct);
            }
        }
        common_cmptypes = Some(match common_cmptypes {
            None => this,
            Some(prev) => prev.into_iter().filter(|c| this.contains(c)).collect(),
        });
        opinfo_lists.push(interps);
    }

    // Lowest comparison-type number is chosen (bms_next_member from -1).
    let mut common = common_cmptypes.unwrap_or_default();
    common.sort_unstable();
    let Some(&first) = common.first() else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "could not determine interpretation of row comparison operator {}",
                opname_s.last().cloned().unwrap_or_default()
            ))
            .errhint("Row comparison operators must be associated with btree operator families.")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    };
    let cmptype: i32 = first;

    // For = and <> just AND/OR the pairwise operators.
    if cmptype == COMPARE_EQ || cmptype == COMPARE_NE {
        let args: Vec<Expr> = opexprs.into_iter().map(Expr::OpExpr).collect();
        let boolop = if cmptype == COMPARE_EQ { AND_EXPR } else { OR_EXPR };
        return Ok(make_bool_expr(boolop, args, location));
    }

    // Choose exactly one opfamily per operator for the chosen comparison type.
    let mut opfamilies: Vec<Oid> = Vec::with_capacity(nopers);
    for i in 0..nopers {
        let mut opfamily = InvalidOid;
        for interp in opinfo_lists[i].iter() {
            if interp.cmptype == first {
                opfamily = interp.opfamily_id;
                break;
            }
        }
        if OidIsValid(opfamily) {
            opfamilies.push(opfamily);
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(alloc::format!(
                    "could not determine interpretation of row comparison operator {}",
                    opname_s.last().cloned().unwrap_or_default()
                ))
                .errdetail("There are multiple equally-plausible candidates.")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
    }

    // Deconstruct the OpExprs into a RowCompareExpr.
    let mut opnos: Vec<Oid> = Vec::with_capacity(nopers);
    let mut new_largs: Vec<Expr> = Vec::with_capacity(nopers);
    let mut new_rargs: Vec<Expr> = Vec::with_capacity(nopers);
    for mut cmp in opexprs {
        opnos.push(cmp.opno);
        let mut it = cmp.args.drain(..);
        if let Some(l) = it.next() {
            new_largs.push(l);
        }
        if let Some(r) = it.next() {
            new_rargs.push(r);
        }
    }

    Ok(Expr::RowCompareExpr(RowCompareExpr {
        cmptype: cmptype_to_enum(cmptype),
        opnos,
        opfamilies,
        inputcollids: Vec::new(), // assign_expr_collations fixes this.
        largs: new_largs,
        rargs: new_rargs,
    }))
}

/// `make_row_distinct_op(pstate, opname, lrow, rrow, location)` — inputs are
/// already-transformed `RowExpr`s.
fn make_row_distinct_op<'mcx>(
    pstate: &mut ParseState<'mcx>,
    opname: &mcx::PgVec<'_, nodes::NodePtr<'_>>,
    lrow: RowExpr,
    rrow: RowExpr,
    location: i32,
) -> PgResult<Expr> {
    let largs = lrow.args;
    let rargs = rrow.args;
    if largs.len() != rargs.len() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("unequal number of entries in row expressions")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    let mut result: Option<Expr> = None;
    for (larg, rarg) in largs.into_iter().zip(rargs.into_iter()) {
        let cmp = make_distinct_op(pstate, opname, Some(larg), Some(rarg), location)?;
        result = Some(match result {
            None => cmp,
            Some(prev) => make_bool_expr(OR_EXPR, vec![prev, cmp], location),
        });
    }

    match result {
        Some(r) => Ok(r),
        // Zero-length rows → constant FALSE.
        None => Ok(Expr::Const(make_bool_const(false, false))),
    }
}

/// `make_distinct_op(pstate, opname, ltree, rtree, location)` — build an
/// `IS DISTINCT FROM` (a re-tagged `OpExpr`).
fn make_distinct_op<'mcx>(
    pstate: &mut ParseState<'mcx>,
    opname: &mcx::PgVec<'_, nodes::NodePtr<'_>>,
    ltree: Option<Expr>,
    rtree: Option<Expr>,
    location: i32,
) -> PgResult<Expr> {
    let opname_s = opname_strings(opname);
    let last_srf = last_srf_expr(pstate);
    let result = backend_parser_parse_oper::make_op(
        Some(&*pstate),
        &opname_s,
        ltree,
        rtree,
        last_srf.as_ref(),
        location,
    )?;

    let op = match result {
        Expr::OpExpr(op) => op,
        other => return Ok(other),
    };
    if op.opresulttype != BOOLOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("IS DISTINCT FROM requires = operator to yield boolean")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
    if op.opretset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("IS DISTINCT FROM must not return a set")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
    // NodeSetTag(result, T_DistinctExpr): DistinctExpr is a `typedef OpExpr`.
    Ok(Expr::DistinctExpr(op))
}

/// `make_nulltest_from_distinct(pstate, distincta, arg)` — produce a `NullTest`
/// from `IS [NOT] DISTINCT FROM NULL`. `arg` is the untransformed other side;
/// `kind`/`location` come from the originating `A_Expr` (`distincta`).
fn make_nulltest_from_distinct<'mcx>(
    pstate: &mut ParseState<'mcx>,
    kind: A_Expr_Kind,
    arg: Option<Node<'mcx>>,
    location: i32,
) -> PgResult<Expr> {
    let new_arg = transformExprRecurse(pstate, arg)?;
    let nulltesttype = if kind == A_Expr_Kind::AEXPR_NOT_DISTINCT {
        NullTestType::IS_NULL
    } else {
        NullTestType::IS_NOT_NULL
    };
    Ok(Expr::NullTest(NullTest {
        arg: new_arg.map(Box::new),
        nulltesttype,
        // argisrow = false is correct whether or not arg is composite.
        argisrow: false,
        // nt->location = distincta->location;
        location,
    }))
}

// ===========================================================================
// transformTypeCast (parse_expr.c:2714).
// ===========================================================================

/// `transformTypeCast(pstate, tc)` — transform `CAST(x AS t)` / `x::t`.
fn transformTypeCast<'mcx>(
    pstate: &mut ParseState<'mcx>,
    tc: Node<'mcx>,
) -> PgResult<Expr> {
    let Node::TypeCast(tc) = tc else {
        return Err(PgError::error("transformTypeCast: expected TypeCast"));
    };
    let TypeCast {
        arg, typeName, location,
    } = tc;

    // typenameTypeIdAndMod(pstate, tc->typeName, &targetType, &targetTypmod).
    let type_name =
        typeName.ok_or_else(|| PgError::error("transformTypeCast: TypeCast without typeName"))?;
    let (target_type, target_typmod) = typename_type_id_and_mod(pstate, &type_name)?;

    let arg = boxed_node(arg)
        .ok_or_else(|| PgError::error("transformTypeCast: TypeCast without arg"))?;

    // If the subject is an ARRAY[] construct and the target is an array type,
    // invoke transformArrayExpr directly to pass down type information.
    let expr = if matches!(arg, Node::A_ArrayExpr(_)) {
        // getBaseTypeAndTypmod(targetType, &targetTypmod) — resolve a domain
        // over array to its base array type/typmod (identity for non-domains).
        let (target_base_type, target_base_typmod) =
            base_type_and_typmod(target_type, target_typmod)?;
        let element_type =
            lsyscache::get_element_type::call(target_base_type)?.unwrap_or(InvalidOid);
        if OidIsValid(element_type) {
            let Node::A_ArrayExpr(a) = arg else {
                unreachable!()
            };
            transformArrayExpr(pstate, a, target_base_type, element_type, target_base_typmod)?
        } else {
            transformExprRecurse(pstate, Some(arg))?
                .ok_or_else(|| PgError::error("transformTypeCast: argument transformed to NULL"))?
        }
    } else {
        transformExprRecurse(pstate, Some(arg))?
            .ok_or_else(|| PgError::error("transformTypeCast: argument transformed to NULL"))?
    };
    let input_type = expr_type(Some(&expr))?;

    // result = coerce_to_target_type(...); NULL => cannot-cast ereport.
    let coerced = coerce::coerce_to_target_type::call(
        pstate,
        expr,
        input_type,
        target_type,
        target_typmod,
        CoercionContext::COERCION_EXPLICIT,
        CoercionForm::COERCE_EXPLICIT_CAST,
        location,
    )?;
    coerced.ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(alloc::format!(
                "cannot cast type {} to {}",
                format_type_be(input_type).unwrap_or_else(|_| String::from("?")),
                format_type_be(target_type).unwrap_or_else(|_| String::from("?"))
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error()
    })
}

/// `typenameTypeIdAndMod(pstate, typeName)` — resolve a raw-grammar `TypeName`
/// to `(targetType, targetTypmod)` via the merged parse_type owner.
///
/// The merged parse_type owner consumes the parser's own node vocabulary
/// (`types_parsenodes::TypeName`/`Node`), distinct from the raw-grammar
/// `types_nodes` `TypeName`/`Node` this dispatcher carries. The `names`
/// (qualified type name, `String` nodes) and `arrayBounds` (`Integer` nodes,
/// whose values the lookup ignores — only `arrayBounds != NIL` matters) bridge
/// cleanly. The `typmods` decoration (`varchar(10)` etc.) is a `List *` of
/// A_Const/ColumnRef raw nodes that the parser-node `Node` vocabulary does not
/// carry; a decorated-type cast is therefore SEAM-AND-PANIC pending a TypeName
/// vocabulary-unification keystone.
fn typename_type_id_and_mod<'mcx>(
    pstate: &ParseState<'mcx>,
    tn: &types_nodes::rawnodes::TypeName<'mcx>,
) -> PgResult<(Oid, i32)> {
    if !tn.typmods.is_empty() {
        panic!(
            "transformTypeCast: a decorated-type cast (TypeName.typmods non-empty, \
             e.g. `x::varchar(10)`) needs the typmod-decoration `List *` of \
             A_Const/ColumnRef nodes bridged into the merged parse_type owner's \
             `types_parsenodes::Node` vocabulary; that vocabulary does not carry \
             the raw-grammar typmod nodes, so it is blocked pending a TypeName \
             vocabulary-unification keystone."
        );
    }
    let mut names: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.names.len());
    for n in tn.names.iter() {
        match &**n {
            Node::String(s) => names.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(String::from(s.sval.as_str())),
                },
            )),
            other => {
                return Err(PgError::error(alloc::format!(
                    "transformTypeCast: TypeName.names element is not a String node (tag {})",
                    other.node_tag().0
                )))
            }
        }
    }
    let mut array_bounds: Vec<types_parsenodes::Node> =
        Vec::with_capacity(tn.arrayBounds.len());
    for n in tn.arrayBounds.iter() {
        // typeNameTypeId only tests `arrayBounds != NIL` (the bound values are
        // ignored by the lookup); carry the Integer bound through.
        match &**n {
            Node::Integer(i) => array_bounds.push(types_parsenodes::Node::Integer(
                types_parsenodes::Integer { ival: i.ival },
            )),
            _ => array_bounds.push(types_parsenodes::Node::Integer(
                types_parsenodes::Integer { ival: -1 },
            )),
        }
    }
    let tn_pn = types_parsenodes::TypeName {
        names,
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typmods: Vec::new(),
        typemod: tn.typemod,
        arrayBounds: array_bounds,
        location: tn.location,
    };
    let scratch = MemoryContext::new("transformTypeCast");
    backend_parser_parse_type::typenameTypeIdAndMod(scratch.mcx(), Some(pstate), &tn_pn)
}

/// `getBaseTypeAndTypmod(typid, &typmod)` (lsyscache.c) — resolve a domain to
/// its base type/typmod (identity for non-domains). The lsyscache seam
/// `get_base_type_and_typmod` returns the base type/typmod of `typid`; when
/// `typid` is not a domain it returns `(typid, typmod-of-typid)`. parse_expr
/// passes the cast's typmod through unchanged for non-domains, so we thread the
/// supplied `typmod` when the base type equals the input type.
fn base_type_and_typmod(typid: Oid, typmod: i32) -> PgResult<(Oid, i32)> {
    let (base, base_typmod) = lsyscache::get_base_type_and_typmod::call(typid)?;
    if base == typid {
        // Non-domain: keep the cast's typmod (C's getBaseTypeAndTypmod leaves
        // *typmod unchanged when the type is not a domain).
        Ok((base, typmod))
    } else {
        Ok((base, base_typmod))
    }
}

// ===========================================================================
// transformArrayExpr (parse_expr.c:2018).
// ===========================================================================

/// `transformArrayExpr(pstate, a, array_type, element_type, typmod)` — transform
/// an `ARRAY[...]` constructor.
fn transformArrayExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_ArrayExpr<'mcx>,
    array_type: Oid,
    element_type: Oid,
    typmod: i32,
) -> PgResult<Expr> {
    let mut array_type = array_type;
    let mut element_type = element_type;

    // Transform the element expressions. One-dimensional unless we find an
    // array-type element.
    let mut multidims = false;
    let A_ArrayExpr { elements, location, .. } = a;
    let mut newelems: Vec<Expr> = Vec::with_capacity(elements.len());
    for e in elements.into_iter() {
        let e = mcx::PgBox::into_inner(e);
        let newe = if let Node::A_ArrayExpr(sub) = e {
            // Sub-array: recurse directly, passing down the target type.
            let newe = transformArrayExpr(pstate, sub, array_type, element_type, typmod)?;
            debug_assert!(!OidIsValid(array_type) || array_type == expr_type(Some(&newe))?);
            multidims = true;
            newe
        } else {
            let newe = transformExprRecurse(pstate, Some(e))?
                .ok_or_else(|| PgError::error("transformArrayExpr: element transformed to NULL"))?;
            // Check for sub-array expressions, excluding int2vector/oidvector
            // and domain-over-array.
            if !multidims {
                let newetype = expr_type(Some(&newe))?;
                if newetype != INT2VECTOROID
                    && newetype != OIDVECTOROID
                    && OidIsValid(lsyscache::get_element_type::call(newetype)?.unwrap_or(InvalidOid))
                {
                    multidims = true;
                }
            }
            newe
        };
        newelems.push(newe);
    }

    // Select a target type for the elements.
    let coerce_type: Oid;
    let coerce_hard: bool;
    if OidIsValid(array_type) {
        debug_assert!(OidIsValid(element_type));
        coerce_type = if multidims { array_type } else { element_type };
        coerce_hard = true;
    } else {
        if newelems.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INDETERMINATE_DATATYPE)
                .errmsg("cannot determine type of empty array")
                .errhint("Explicitly cast to the desired type, for example ARRAY[]::integer[].")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }

        coerce_type = coerce::select_common_type::call(pstate, &newelems, Some("ARRAY"))?;

        if multidims {
            array_type = coerce_type;
            element_type =
                lsyscache::get_element_type::call(array_type)?.unwrap_or(InvalidOid);
            if !OidIsValid(element_type) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(alloc::format!(
                        "could not find element type for data type {}",
                        format_type_be(array_type)?
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
        } else {
            element_type = coerce_type;
            array_type =
                lsyscache::get_array_type::call(element_type)?.unwrap_or(InvalidOid);
            if !OidIsValid(array_type) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(alloc::format!(
                        "could not find array type for data type {}",
                        format_type_be(element_type)?
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
        }
        coerce_hard = false;
    }

    // Coerce elements to the target type.
    let mut newcoercedelems: Vec<Expr> = Vec::with_capacity(newelems.len());
    for e in newelems {
        let etype = expr_type(Some(&e))?;
        let eloc = expr_location(Some(&e))?;
        let newe = if coerce_hard {
            coerce::coerce_to_target_type::call(
                pstate,
                e,
                etype,
                coerce_type,
                typmod,
                CoercionContext::COERCION_EXPLICIT,
                CoercionForm::COERCE_EXPLICIT_CAST,
                -1,
            )?
            .ok_or_else(|| {
                ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(alloc::format!(
                        "cannot cast type {} to {}",
                        format_type_be(etype).unwrap_or_else(|_| String::from("?")),
                        format_type_be(coerce_type).unwrap_or_else(|_| String::from("?"))
                    ))
                    .errposition(parser_errposition(pstate, eloc))
                    .into_error()
            })?
        } else {
            coerce::coerce_to_common_type::call(pstate, e, coerce_type, "ARRAY")?
        };
        newcoercedelems.push(newe);
    }

    Ok(Expr::ArrayExpr(ArrayExpr {
        array_typeid: array_type,
        array_collid: InvalidOid,
        element_typeid: element_type,
        elements: newcoercedelems,
        multidims,
        // newa->location = a->location;
        location,
    }))
}

// ===========================================================================
// ParseExprKindName (parse_expr.c:3142).
// ===========================================================================

/// `ParseExprKindName(exprKind)` — a human-readable name for a parse-expr kind.
pub fn ParseExprKindName(expr_kind: ParseExprKind) -> &'static str {
    use ParseExprKind::*;
    match expr_kind {
        EXPR_KIND_NONE => "invalid expression context",
        EXPR_KIND_OTHER => "extension expression",
        EXPR_KIND_JOIN_ON => "JOIN/ON",
        EXPR_KIND_JOIN_USING => "JOIN/USING",
        EXPR_KIND_FROM_SUBSELECT => "sub-SELECT in FROM",
        EXPR_KIND_FROM_FUNCTION => "function in FROM",
        EXPR_KIND_WHERE => "WHERE",
        EXPR_KIND_POLICY => "POLICY",
        EXPR_KIND_HAVING => "HAVING",
        EXPR_KIND_FILTER => "FILTER",
        EXPR_KIND_WINDOW_PARTITION => "window PARTITION BY",
        EXPR_KIND_WINDOW_ORDER => "window ORDER BY",
        EXPR_KIND_WINDOW_FRAME_RANGE => "window RANGE",
        EXPR_KIND_WINDOW_FRAME_ROWS => "window ROWS",
        EXPR_KIND_WINDOW_FRAME_GROUPS => "window GROUPS",
        EXPR_KIND_SELECT_TARGET => "SELECT",
        EXPR_KIND_INSERT_TARGET => "INSERT",
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => "UPDATE",
        EXPR_KIND_MERGE_WHEN => "MERGE WHEN",
        EXPR_KIND_GROUP_BY => "GROUP BY",
        EXPR_KIND_ORDER_BY => "ORDER BY",
        EXPR_KIND_DISTINCT_ON => "DISTINCT ON",
        EXPR_KIND_LIMIT => "LIMIT",
        EXPR_KIND_OFFSET => "OFFSET",
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => "RETURNING",
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => "VALUES",
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => "CHECK",
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => "DEFAULT",
        EXPR_KIND_INDEX_EXPRESSION => "index expression",
        EXPR_KIND_INDEX_PREDICATE => "index predicate",
        EXPR_KIND_STATS_EXPRESSION => "statistics expression",
        EXPR_KIND_ALTER_COL_TRANSFORM => "USING",
        EXPR_KIND_EXECUTE_PARAMETER => "EXECUTE",
        EXPR_KIND_TRIGGER_WHEN => "WHEN",
        EXPR_KIND_PARTITION_BOUND => "partition bound",
        EXPR_KIND_PARTITION_EXPRESSION => "PARTITION BY",
        EXPR_KIND_CALL_ARGUMENT => "CALL",
        EXPR_KIND_COPY_WHERE => "WHERE",
        EXPR_KIND_GENERATED_COLUMN => "GENERATED AS",
        EXPR_KIND_CYCLE_MARK => "CYCLE",
    }
}

// ===========================================================================
// p_last_srf bridge (parse_oper's make_op consumes/updates p_last_srf via the
// `last_srf` arg + the merged set_last_srf seam; here we thread it through the
// owned ParseState field directly).
// ===========================================================================

/// View `pstate->p_last_srf` as an `&Expr` for passing to `make_op`'s
/// `last_srf` argument (a `Node *` in C).
fn last_srf_expr(pstate: &ParseState<'_>) -> Option<Expr> {
    pstate.p_last_srf.as_ref().and_then(|b| match &**b {
        Node::Expr(e) => Some(e.clone()),
        _ => None,
    })
}

// NB: the `pstate->p_last_srf` *write* for a set-returning operator is performed
// inside `parse_oper::make_op` (it calls the `set_last_srf` seam when the result
// returns a set), exactly as C's `make_op` does — this crate only *reads*
// `p_last_srf` (via `last_srf_expr`) to pass into `make_op` and to bracket the
// CASE/COALESCE `srf_check`.

// ===========================================================================
// Small typed helpers for predicate matching over Option<Node>/Option<Expr>.
// ===========================================================================

fn is_casetestexpr(n: Option<&Node<'_>>) -> bool {
    matches!(n, Some(Node::Expr(Expr::CaseTestExpr(_))))
}
fn is_rowexpr(n: Option<&Node<'_>>) -> bool {
    matches!(n, Some(Node::Expr(Expr::RowExpr(_))))
}
/// `rexpr IsA SubLink && ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK`
/// (parse_expr.c:954-955): only a plain expression sublink may be rewritten
/// into a ROWCOMPARE sublink.
fn is_expr_sublink(n: Option<&Node<'_>>) -> bool {
    matches!(
        n,
        Some(Node::Expr(Expr::SubLink(s)))
            if s.subLinkType == types_nodes::primnodes::SubLinkType::Expr
    )
}
fn is_rowexpr_expr_opt(e: Option<&Expr>) -> bool {
    matches!(e, Some(Expr::RowExpr(_)))
}

/// `(Node *) e` for a typed `Expr` consumed by the SubLink-rewrite path.
fn node_into_expr(n: Node<'_>) -> Option<Expr> {
    match n {
        Node::Expr(e) => Some(e),
        _ => None,
    }
}

// ===========================================================================
// format_type_be (format_type.c) — error-message-only type display.
// ===========================================================================

/// `format_type_be(typid)` — the displayable type name for the various
/// datatype-mismatch error messages. Reached through the merged format-type
/// owner via a scratch context (error path only).
fn format_type_be(typid: Oid) -> PgResult<String> {
    // The format-type owner is reached via lsyscache; mirror the parse-collate
    // precedent (scratch ctx, error-message use only). The repo exposes the
    // displayable name through the lsyscache `format_type_be` seam.
    lsyscache_format_type_be(typid)
}

// ===========================================================================
// Seam-and-panic transform arms (unported sibling owners). Each routes through
// a panic-until-landed seam declared in the matching sibling `*-seams` crate.
// ===========================================================================

fn seam_transform_column_ref<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _cref: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformColumnRef needs parse_relation.c namespace machinery \
         (colNameToVar/scanRTEForColumn/transformWholeRowRef) + the columnref \
         parser hooks; parse_relation.c is not yet ported."
    )
}

fn seam_transform_column_ref_hook_currentof<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _cexpr: Expr,
) -> PgResult<Expr> {
    panic!(
        "transformCurrentOfExpr cursor-name -> REFCURSOR-Param rewrite needs the \
         opaque columnref parser-hook ABI; the hook-installed path is reached only \
         when a hook is present (absent in the stock server)."
    )
}

fn seam_transform_param_ref<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _pref: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformParamRef applies the opaque p_paramref_hook (cross-ABI function \
         pointer); the hook ABI is not yet modeled."
    )
}

fn seam_transform_indirection<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _ind: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformIndirection needs transformContainerSubscripts (parse_node.c) + \
         ParseFuncOrColumn field-selection (parse_func.c); both unported."
    )
}

fn seam_transform_func_call<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _fn_call: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformFuncCall delegates to ParseFuncOrColumn (parse_func.c); \
         parse_func.c is not yet ported."
    )
}

fn seam_transform_multi_assign_ref<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _maref: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformMultiAssignRef needs parse_target.c + the SubLink transform; both unported."
    )
}

fn seam_transform_sublink<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _sublink: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformSubLink runs parse_sub_analyze (analyze.c) on the subquery and \
         resolves comparison operators; analyze.c is not yet ported."
    )
}

fn seam_transform_grouping_func<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _gf: Node<'mcx>,
) -> PgResult<Expr> {
    panic!("transformGroupingFunc lives in parse_agg.c (unported).")
}

fn seam_transform_row_expr<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _r: Node<'mcx>,
    _allow_default: bool,
) -> PgResult<Expr> {
    panic!(
        "transformRowExpr runs transformExpressionList + FigureColnames \
         (parse_target.c, unported)."
    )
}

fn seam_transform_a_expr_between<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _a: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformAExprBetween reads the list-valued `a->rexpr` ((List *) of the \
         two bound expressions); the owned model cannot walk a `List` of \
         expression *nodes* (its `ListCell` is a raw-pointer C union), so the \
         destructure is blocked until an expression-list parse-node carrier lands."
    )
}

fn seam_transform_a_expr_list<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _a: Node<'mcx>,
    which: &str,
) -> PgResult<Expr> {
    panic!(
        "{which} reads the list-valued `a->rexpr` ((List *) of value-list items); \
         the owned model cannot walk a `List` of expression *nodes* (its \
         `ListCell` is a raw-pointer C union), so the destructure is blocked \
         until an expression-list parse-node carrier lands.",
        which = which
    )
}

fn seam_transform_xml_expr<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _x: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformXmlExpr/Serialize reach map_sql_identifier_to_xml_name \
         (utils/adt/xml.c, unported) + coercion machinery."
    )
}

fn seam_transform_json_expr<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _node: Node<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "the SQL/JSON transform family (transformJson*Constructor/Agg/IsPredicate/\
         ParseExpr/ScalarExpr/SerializeExpr/FuncExpr) needs its owning parse-node \
         structs + utils/adt/json*.c (unported)."
    )
}

// ===========================================================================
// Direct-call shims for merged-owner / seam callees not yet re-homed cleanly.
// ===========================================================================

/// `anytime_typmod_check(istz, typmod)` (utils/adt/date.c) — validate/clamp a
/// time/timetz typmod. Reached through the date adt seam (unported owner →
/// panic).
fn lsyscache_anytime_typmod_check(_istz: bool, _typmod: i32) -> PgResult<i32> {
    panic!(
        "anytime_typmod_check (utils/adt/date.c) is not yet ported; \
         CURRENT_TIME(n)/LOCALTIME(n) typmod validation reaches the unported adt."
    )
}

/// `anytimestamp_typmod_check(istz, typmod)` (utils/adt/timestamp.c).
fn lsyscache_anytimestamp_typmod_check(_istz: bool, _typmod: i32) -> PgResult<i32> {
    panic!(
        "anytimestamp_typmod_check (utils/adt/timestamp.c) is not yet ported; \
         CURRENT_TIMESTAMP(n)/LOCALTIMESTAMP(n) typmod validation reaches the unported adt."
    )
}

/// `format_type_be(typid)` (format_type.c) — through the merged format-type
/// owner (owned `String`, error-message use only; the parse-oper precedent).
fn lsyscache_format_type_be(typid: Oid) -> PgResult<String> {
    backend_utils_adt_format_type::format_type_be_owned(typid)
}

// ===========================================================================
// assign_expr_collations (parse_collate.c, merged owner) — direct call.
// ===========================================================================

fn assign_expr_collations<'mcx>(
    pstate: &mut ParseState<'mcx>,
    expr: &mut Expr,
) -> PgResult<()> {
    backend_parser_parse_collate::assign_expr_collations(Some(&*pstate), expr)
}

// ===========================================================================
// EXECUTE-parameter analysis seam (analyze_one_exec_param) — inward seam this
// crate owns, consumed by the prepare driver's EvaluateParams.
// ===========================================================================

use backend_parser_parse_expr_seams as me;

fn analyze_one_exec_param_impl<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    source_text: &str,
    raw_param: &Node<'mcx>,
    _param_index: i32,
    expected_type_id: Oid,
) -> PgResult<me::AnalyzedExecParam<'mcx>> {
    // The per-parameter body of EvaluateParams (prepare.c:311-341):
    //   expr = transformExpr(pstate, expr, EXPR_KIND_EXECUTE_PARAMETER);
    //   given_type_id = exprType(expr);
    //   expr = coerce_to_target_type(pstate, expr, given_type_id, expected_type_id,
    //              -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
    //   if (expr == NULL) ereport(...);  -- driver raises it (coercion_failed)
    //   assign_expr_collations(pstate, expr);
    //   lfirst(l) = expr;
    //
    // The C runs this on the EvaluateParams caller's pstate; an EXECUTE-parameter
    // expression cannot reference range-table columns, so a fresh parse state
    // carrying only p_sourcetext is faithful. C `copyObject(params)` first
    // (the parser scribbles on its input) — `clone_in` is copyObject here.
    let mut pstate_box = backend_parser_small1::make_parsestate(mcx, None)?;
    pstate_box.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);
    let pstate: &mut ParseState<'mcx> = &mut pstate_box;

    let raw = raw_param.clone_in(mcx)?;
    // exprLocation(lfirst(l)) — the original parser node's location, captured
    // before transform for the cannot-be-coerced error position.
    let expr_location = node_location(&raw);

    let expr = transformExpr(
        pstate,
        Some(raw),
        ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER,
    )?;
    let expr = expr.ok_or_else(|| {
        PgError::error("analyze_one_exec_param: EXECUTE parameter transformed to NULL")
    })?;

    let given_type_id = expr_type(Some(&expr))?;

    let coerced = coerce::coerce_to_target_type::call(
        pstate,
        expr,
        given_type_id,
        expected_type_id,
        -1,
        CoercionContext::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;

    let Some(mut coerced) = coerced else {
        // coerce_to_target_type returned NULL — the driver raises the
        // cannot-be-coerced ereport with C's exact branch order.
        return Ok(me::AnalyzedExecParam {
            expr: None,
            coercion_failed: true,
            given_type_id,
            expr_location,
        });
    };

    assign_expr_collations(pstate, &mut coerced)?;

    Ok(me::AnalyzedExecParam {
        expr: Some(mcx::alloc_in(mcx, coerced)?),
        coercion_failed: false,
        given_type_id,
        expr_location,
    })
}

/// `exprLocation(node)` for a raw-grammar [`Node`] — the parser location used in
/// the EXECUTE-parameter cannot-be-coerced error. The trimmed model drops most
/// raw-node locations; the A_Const literal carries one, and an already-typed
/// `Node::Expr` defers to `exprLocation`. Other raw nodes report -1 (cursor 0).
fn node_location(n: &Node<'_>) -> i32 {
    match n {
        Node::A_Const(a) => a.location,
        Node::A_Expr(a) => a.location,
        Node::Expr(e) => expr_location(Some(e)).unwrap_or(-1),
        _ => -1,
    }
}

fn parser_errposition_impl(source_text: &str, location: i32) -> PgResult<i32> {
    // parser_errposition translates a raw location into a 1-based cursor; with a
    // negative location it is 0. The source-text offset model collapses to the
    // verbatim location (matching the sibling parser ports).
    let _ = source_text;
    Ok(if location < 0 { 0 } else { location })
}

/// Install this crate's inward seams (owner of `backend-parser-parse-expr-seams`).
pub fn init_seams() {
    me::analyze_one_exec_param::set(analyze_one_exec_param_impl);
    me::parser_errposition::set(parser_errposition_impl);
    me::transformExpr::set(transformExpr);
}

#[cfg(test)]
mod tests;
