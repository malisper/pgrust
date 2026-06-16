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
    BOOLOID, DATEOID, INT2VECTOROID, NAMEOID, OIDVECTOROID, RECORDOID, TEXTOID, TIMEOID,
    TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UNKNOWNOID,
};
use types_tuple::heaptuple::MaxTupleAttributeNumber;

use backend_optimizer_util_vars::var::contain_vars_of_level;

use types_nodes::nodes::{self, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{
    ArrayExpr, BoolTestType, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr,
    CoercionForm, CollateExpr, CurrentOfExpr, Expr, MergeSupportFunc, MinMaxExpr, MinMaxOp,
    NullTest, NullTestType, OpExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp,
    AND_EXPR, NOT_EXPR, OR_EXPR,
};
use types_nodes::rawnodes::{
    A_Const, A_Expr, A_Expr_Kind, A_ArrayExpr, A_Indices, A_Indirection, ColumnRef, CollateClause,
    FuncCall, MultiAssignRef, TypeCast,
};
use types_parsenodes::CoercionContext;

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::{make_bool_const, make_bool_expr, make_target_entry};
use backend_nodes_core::nodefuncs::{
    expr_collation, expr_location, expr_type, expr_typmod, expression_returns_set,
};

use backend_parser_coerce_seams as coerce;
use backend_utils_cache_lsyscache_seams as lsyscache;

use backend_parser_relation::{
    colNameToVar, errorMissingColumn, errorMissingRTE, refnameNamespaceItem, scanNSItemForColumn,
};
use backend_commands_dbcommands_seams as dbcommands_seams;
use backend_utils_init_small_seams as globals_seams;

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
        Node::ColumnRef(c) => transformColumnRef(pstate, c)?,
        Node::ParamRef(pref) => transformParamRef(pstate, &pref)?,

        // T_A_Const → make_const(pstate, (A_Const *) expr).
        Node::A_Const(a) => transform_a_const(pstate, a)?,

        Node::A_Indirection(ind) => transformIndirection(pstate, ind)?,

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
                | A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => transformAExprBetween(pstate, a)?,
            }
        }

        Node::FuncCall(f) => transformFuncCall(pstate, f)?,
        Node::MultiAssignRef(m) => transformMultiAssignRef(pstate, m)?,

        // T_SubLink → transformSubLink(pstate, (SubLink *) expr).
        Node::SubLink(s) => transformSubLink(pstate, s)?,

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

        // A raw-grammar SubLink reaches the dispatcher as the `Node::SubLink`
        // arm above (the C `T_SubLink` case); an already-analyzed
        // `Expr::SubLink` re-entering transformExprRecurse would be a bug
        // (the C never re-transforms an analyzed SubLink).
        Expr::SubLink(_) => {
            return Err(PgError::error(
                "transformExprRecurse: unexpected already-analyzed SubLink",
            ))
        }
        Expr::CaseExpr(c) => transformCaseExpr(pstate, c),
        Expr::RowExpr(r) => transformRowExpr(pstate, r, false),
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
    let mcx = aexpr_clone_ctx(pstate);
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
    // recurses (parse_expr.c:953-973).
    if is_rowexpr(lexpr.as_ref()) && is_expr_sublink(rexpr.as_ref()) {
        let mut s = match rexpr.unwrap() {
            Node::SubLink(s) => s,
            _ => unreachable!("is_expr_sublink guard"),
        };
        // s->subLinkType = ROWCOMPARE_SUBLINK; s->testexpr = lexpr;
        // s->operName = a->name; s->location = a->location;
        s.sub_link_type = types_nodes::primnodes::SubLinkType::RowCompare;
        s.testexpr = lexpr.map(|l| mcx::alloc_in(mcx, l)).transpose()?;
        s.oper_name = name;
        s.location = location;
        // result = transformExprRecurse(pstate, (Node *) s);
        return transformSubLink(pstate, s);
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
        Some(pstate),
        &opname,
        lexpr_t,
        rexpr_t,
        last_srf.as_ref(),
        location,
    )?;
    Ok(res)
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
        Some(pstate),
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
        Some(pstate),
        &opname,
        false,
        lexpr,
        rexpr,
        location,
    )
}

/// `transformAExprIn(pstate, a)` (parse_expr.c:1124) — the `[NOT] IN
/// (value-list)` transform. The value-list `a->rexpr` is a `Node::List` of
/// element nodes (the node-walker keystone added the `List` carrier).
fn transformAExprIn<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let A_Expr {
        name, lexpr, rexpr, rexpr_list_start, rexpr_list_end, location, ..
    } = a;

    // If the operator is <>, combine with AND not OR.
    let useor = !(name.len() == 1
        && name.iter().next().and_then(str_val).as_deref() == Some("<>"));

    // First step: transform all the inputs, detecting whether any contain Vars.
    let lexpr_t = transformExprRecurse(pstate, boxed_node(lexpr))?;

    let rexpr_list = match boxed_node(rexpr) {
        Some(Node::List(items)) => items,
        // The grammar always wraps the IN value-list as a List node.
        other => {
            return Err(PgError::error(alloc::format!(
                "transformAExprIn: expected a List rexpr, got {:?}",
                other.as_ref().map(|n| n.node_tag().0)
            )))
        }
    };

    let mut rexprs: Vec<Expr> = Vec::with_capacity(rexpr_list.len());
    let mut rvars: Vec<Expr> = Vec::new();
    let mut rnonvars: Vec<Expr> = Vec::new();
    let mut has_rvars = false;
    for r in rexpr_list.into_iter() {
        let rexpr = transformExprRecurse(pstate, Some(mcx::PgBox::into_inner(r)))?
            .ok_or_else(|| PgError::error("transformAExprIn: IN item is NULL"))?;
        rexprs.push(rexpr.clone());
        // contain_vars_of_level((Node *) rexpr, 0).
        if contain_vars_of_level(&Node::Expr(rexpr.clone()), 0) {
            rvars.push(rexpr);
            has_rvars = true;
        } else {
            rnonvars.push(rexpr);
        }
    }

    let opname = opname_strings(&name);

    let mut result: Option<Expr> = None;

    // ScalarArrayOpExpr is only useful if there's more than one non-Var RHS item.
    if rnonvars.len() > 1 {
        // Select a common type for the array elements. The LHS' type is first
        // in the list, so it is preferred when there is doubt.
        let mut allexprs: Vec<Expr> = Vec::with_capacity(rnonvars.len() + 1);
        if let Some(le) = &lexpr_t {
            allexprs.push(le.clone());
        }
        allexprs.extend(rnonvars.iter().cloned());

        let mut scalar_type = coerce::select_common_type::call(pstate, &allexprs, None)?;

        // Verify the selected type actually works.
        if OidIsValid(scalar_type) && !coerce::verify_common_type::call(scalar_type, &allexprs)? {
            scalar_type = InvalidOid;
        }

        // Do we have an array type to use? We avoid ScalarArrayOpExpr when the
        // common type is RECORD, because the RowExpr logic below copes with
        // some cases of non-identical row types.
        let array_type = if OidIsValid(scalar_type) && scalar_type != RECORDOID {
            lsyscache::get_array_type::call(scalar_type)?.unwrap_or(InvalidOid)
        } else {
            InvalidOid
        };

        if OidIsValid(array_type) {
            // Coerce all the RHS non-Var inputs to the common type and build
            // an ArrayExpr for them.
            let mut aexprs: Vec<Expr> = Vec::with_capacity(rnonvars.len());
            for rexpr in rnonvars.iter().cloned() {
                let rexpr = coerce::coerce_to_common_type::call(pstate, rexpr, scalar_type, "IN")?;
                aexprs.push(rexpr);
            }
            let newa = ArrayExpr {
                array_typeid: array_type,
                // array_collid will be set by parse_collate.c.
                array_collid: InvalidOid,
                element_typeid: scalar_type,
                elements: aexprs,
                multidims: false,
                // newa->location = -1. The C also records list_start/list_end
                // (disabling query-jumbling squashing when has_rvars), but the
                // trimmed ArrayExpr drops those fields.
                location: -1,
            };
            // has_rvars + rexpr_list_start/end feed ArrayExpr.list_start/end in
            // C (query-jumbling squashing control); those fields are not on the
            // trimmed ArrayExpr, so they have no effect here.
            let _ = (has_rvars, rexpr_list_start, rexpr_list_end);

            let lexpr_for_saop = lexpr_t
                .clone()
                .ok_or_else(|| PgError::error("transformAExprIn: IN lefthand is NULL"))?;
            result = Some(backend_parser_parse_oper::make_scalar_array_op(
                Some(pstate),
                &opname,
                useor,
                lexpr_for_saop,
                Expr::ArrayExpr(newa),
                location,
            )?);

            // Consider only the Vars (if any) in the loop below.
            rexprs = rvars;
        }
    }

    // Must do it the hard way: a boolean expression tree.
    for rexpr in rexprs.into_iter() {
        let cmp = if is_rowexpr_expr_opt(lexpr_t.as_ref()) && matches!(rexpr, Expr::RowExpr(_)) {
            // ROW() op ROW() is handled specially.
            let largs = match &lexpr_t {
                Some(Expr::RowExpr(r)) => r.args.clone(),
                _ => Vec::new(),
            };
            let rargs = match rexpr {
                Expr::RowExpr(r) => r.args,
                _ => Vec::new(),
            };
            make_row_comparison_op(pstate, &name, largs, rargs, location)?
        } else {
            // Ordinary scalar operator (copyObject(lexpr) per iteration).
            let last_srf = last_srf_expr(pstate);
            backend_parser_parse_oper::make_op(
                Some(pstate),
                &opname,
                lexpr_t.clone(),
                Some(rexpr),
                last_srf.as_ref(),
                location,
            )?
        };

        let cmp = coerce::coerce_to_boolean::call(pstate, cmp, "IN")?;
        result = Some(match result {
            None => cmp,
            Some(prev) => make_bool_expr(
                if useor { OR_EXPR } else { AND_EXPR },
                vec![prev, cmp],
                location,
            ),
        });
    }

    result.ok_or_else(|| PgError::error("transformAExprIn: produced no result"))
}

/// `makeSimpleA_Expr(kind, "op", lexpr, rexpr, location)` (makefuncs.c), built
/// in `mcx` — a one-operator `A_Expr` wrapped as a `Node`, for the BETWEEN
/// expansion. Mirrors `list_make1(makeString(op))` for the operator name.
fn make_simple_a_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    kind: A_Expr_Kind,
    op: &str,
    lexpr: Node<'mcx>,
    rexpr: Node<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let mut name: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    let str_node = Node::String(types_nodes::value::StringNode {
        sval: mcx::PgString::from_str_in(op, mcx)?,
    });
    name.push(mcx::alloc_in(mcx, str_node)?);
    Ok(Node::A_Expr(A_Expr {
        kind,
        name,
        lexpr: Some(mcx::alloc_in(mcx, lexpr)?),
        rexpr: Some(mcx::alloc_in(mcx, rexpr)?),
        rexpr_list_start: -1,
        rexpr_list_end: -1,
        location,
    }))
}

/// `transformAExprBetween(pstate, a)` (parse_expr.c:1293) — `BETWEEN` and its
/// SYM / NOT variants, expanded into `>=`/`<=` (or `<`/`>`) A_Expr trees and
/// recursed (matching `transformExprRecurse(makeBoolExpr(...))`). `a->rexpr` is
/// a two-element `Node::List` of the bounds.
fn transformAExprBetween<'mcx>(
    pstate: &mut ParseState<'mcx>,
    a: A_Expr<'mcx>,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);
    let A_Expr {
        kind, lexpr, rexpr, location, ..
    } = a;

    // Deconstruct A_Expr into three subexprs.
    let aexpr = boxed_node(lexpr)
        .ok_or_else(|| PgError::error("transformAExprBetween: missing lefthand"))?;
    let mut args = match boxed_node(rexpr) {
        Some(Node::List(items)) => items,
        other => {
            return Err(PgError::error(alloc::format!(
                "transformAExprBetween: expected a 2-element List rexpr, got {:?}",
                other.as_ref().map(|n| n.node_tag().0)
            )))
        }
    };
    if args.len() != 2 {
        return Err(PgError::error("transformAExprBetween: BETWEEN needs two bounds"));
    }
    let cexpr = mcx::PgBox::into_inner(args.remove(1));
    let bexpr = mcx::PgBox::into_inner(args.remove(0));

    // copyObject of a multiply-referenced subexpression.
    let clone = |n: &Node<'mcx>| -> PgResult<Node<'mcx>> { n.clone_in(mcx) };

    // Transform a synthesized comparison A_Expr and coerce it to boolean (what
    // transformExprRecurse over the synthesized makeBoolExpr's child does).
    let cmp = |pstate: &mut ParseState<'mcx>, op: &str, l: Node<'mcx>, r: Node<'mcx>|
        -> PgResult<Expr> {
        let node = make_simple_a_expr(mcx, A_Expr_Kind::AEXPR_OP, op, l, r, location)?;
        transformExprRecurse(pstate, Some(node))?
            .ok_or_else(|| PgError::error("transformAExprBetween: comparison is NULL"))
    };

    // makeBoolExpr(boolop, [...], location) over already-transformed children.
    let result: Expr = match kind {
        A_Expr_Kind::AEXPR_BETWEEN => {
            let c1 = cmp(pstate, ">=", clone(&aexpr)?, bexpr)?;
            let c2 = cmp(pstate, "<=", aexpr, cexpr)?;
            make_bool_expr(AND_EXPR, vec![c1, c2], location)
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN => {
            let c1 = cmp(pstate, "<", clone(&aexpr)?, bexpr)?;
            let c2 = cmp(pstate, ">", aexpr, cexpr)?;
            make_bool_expr(OR_EXPR, vec![c1, c2], location)
        }
        A_Expr_Kind::AEXPR_BETWEEN_SYM => {
            let s1a = cmp(pstate, ">=", clone(&aexpr)?, clone(&bexpr)?)?;
            let s1b = cmp(pstate, "<=", clone(&aexpr)?, clone(&cexpr)?)?;
            let sub1 = make_bool_expr(AND_EXPR, vec![s1a, s1b], location);
            let s2a = cmp(pstate, ">=", clone(&aexpr)?, cexpr)?;
            let s2b = cmp(pstate, "<=", aexpr, bexpr)?;
            let sub2 = make_bool_expr(AND_EXPR, vec![s2a, s2b], location);
            make_bool_expr(OR_EXPR, vec![sub1, sub2], location)
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
            let s1a = cmp(pstate, "<", clone(&aexpr)?, clone(&bexpr)?)?;
            let s1b = cmp(pstate, ">", clone(&aexpr)?, clone(&cexpr)?)?;
            let sub1 = make_bool_expr(OR_EXPR, vec![s1a, s1b], location);
            let s2a = cmp(pstate, "<", clone(&aexpr)?, cexpr)?;
            let s2b = cmp(pstate, ">", aexpr, bexpr)?;
            let sub2 = make_bool_expr(OR_EXPR, vec![s2a, s2b], location);
            make_bool_expr(AND_EXPR, vec![sub1, sub2], location)
        }
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!("unrecognized A_Expr kind: {}", kind as i32))
                .into_error())
        }
    };

    Ok(result)
}

/// The `'mcx` context to allocate the synthesized BETWEEN A_Expr tree and clone
/// the bound subexpressions into (C's `copyObject`). The tree lives at the query
/// level; recover the query context from a pstate-allocated field.
fn aexpr_clone_ctx<'mcx>(pstate: &ParseState<'mcx>) -> mcx::Mcx<'mcx> {
    *pstate.p_rtable.allocator()
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
        Some(pstate),
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
                Some(pstate),
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
            Some(pstate),
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
        Some(pstate),
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
/// `A_Const`/`ColumnRef` raw nodes; the owner's `typenameTypeMod` only ever
/// consults each element as one of the value-node forms `Integer`/`Float`/
/// `String` (the C "simple numeric constants, string literals, and
/// identifiers"). We bridge each raw typmod into the parser-node value Node it
/// reduces to: an `A_Const` carries its literal in `.val`
/// (`Integer`/`Float`/`String`/`Boolean`/`BitString`); a single-field
/// `ColumnRef` is an identifier and reduces to its `String` field. The actual
/// constant→cstring conversion + `typmodin` dispatch (and the "type modifiers
/// must be simple constants or identifiers" error for anything else) stays in
/// the owner, mirroring C `typenameTypeMod`.
fn typename_type_id_and_mod<'mcx>(
    pstate: &ParseState<'mcx>,
    tn: &types_nodes::rawnodes::TypeName<'mcx>,
) -> PgResult<(Oid, i32)> {
    let mut typmods: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.typmods.len());
    for tm in tn.typmods.iter() {
        let bridged: types_parsenodes::Node = match &**tm {
            // `IsA(tm, A_Const)`: the literal rides in `A_Const.val`.
            Node::A_Const(ac) => match ac.val.as_deref() {
                Some(Node::Integer(i)) => types_parsenodes::Node::Integer(
                    types_parsenodes::Integer { ival: i.ival },
                ),
                Some(Node::Float(f)) => {
                    types_parsenodes::Node::Float(types_parsenodes::Float {
                        fval: Some(String::from(f.fval.as_str())),
                    })
                }
                Some(Node::String(s)) => types_parsenodes::Node::String(
                    types_parsenodes::StringNode {
                        sval: Some(String::from(s.sval.as_str())),
                    },
                ),
                Some(Node::Boolean(b)) => types_parsenodes::Node::Boolean(
                    types_parsenodes::Boolean { boolval: b.boolval },
                ),
                Some(Node::BitString(b)) => types_parsenodes::Node::BitString(
                    types_parsenodes::BitString {
                        bsval: Some(String::from(b.bsval.as_str())),
                    },
                ),
                // SQL NULL constant or any other val: not a simple constant; carry
                // an A_Star so the owner rejects it with the C error message.
                _ => types_parsenodes::Node::A_Star,
            },
            // `IsA(tm, ColumnRef)` with a single String field is an identifier
            // typmod (the trimmed parser-node model carries it as a bare String).
            Node::ColumnRef(cr) => {
                if cr.fields.len() == 1 {
                    if let Node::String(s) = &*cr.fields[0] {
                        types_parsenodes::Node::String(types_parsenodes::StringNode {
                            sval: Some(String::from(s.sval.as_str())),
                        })
                    } else {
                        types_parsenodes::Node::A_Star
                    }
                } else {
                    types_parsenodes::Node::A_Star
                }
            }
            // Anything else is not a simple constant or identifier; let the owner
            // raise the C "type modifiers must be simple constants or
            // identifiers" error.
            _ => types_parsenodes::Node::A_Star,
        };
        typmods.push(bridged);
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
        typmods,
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
/// `IsA(node, RowExpr)` over a *raw-grammar* node — the grammar emits a raw
/// ROW(...) as [`Node::RowExpr`]. The transformAExprOp arms inspect the
/// untransformed `a->lexpr`/`a->rexpr`.
fn is_rowexpr(n: Option<&Node<'_>>) -> bool {
    matches!(n, Some(Node::RowExpr(_)))
}
/// `rexpr IsA SubLink && ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK`
/// (parse_expr.c:954-955): only a plain expression sublink may be rewritten
/// into a ROWCOMPARE sublink. The raw-grammar SubLink is [`Node::SubLink`].
fn is_expr_sublink(n: Option<&Node<'_>>) -> bool {
    matches!(
        n,
        Some(Node::SubLink(s))
            if s.sub_link_type == types_nodes::primnodes::SubLinkType::Expr
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
// transformColumnRef / transformWholeRowRef (parse_expr.c:508 / :2632).
// ===========================================================================

/// The "no translation found" reason categories of `transformColumnRef`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CrErr {
    NoColumn,
    NoRte,
    WrongDb,
    TooMany,
}

/// `transformColumnRef(pstate, cref)` (parse_expr.c:508) — resolve a column
/// reference against the range table / namespace.
fn transformColumnRef<'mcx>(
    pstate: &mut ParseState<'mcx>,
    cref: ColumnRef<'mcx>,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);
    let location = cref.location;

    // Check the column reference is in a valid place within the query: allowed
    // everywhere except default expressions and partition bound expressions.
    let err: Option<&str> = match pstate.p_expr_kind {
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT => {
            Some("cannot use column reference in DEFAULT expression")
        }
        ParseExprKind::EXPR_KIND_PARTITION_BOUND => {
            Some("cannot use column reference in partition bound expression")
        }
        _ => None,
    };
    if let Some(err) = err {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(String::from(err))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // Give the PreParseColumnRefHook, if any, first shot. The hook ABI carries
    // opaque cross-ABI function pointers; route through the columnref seam when
    // a hook is installed (absent in the stock server).
    if pstate.p_pre_columnref_hook.is_some() {
        return seam_transform_column_ref_hook(pstate, cref);
    }

    // node: the resolved Node, if any.
    let mut node: Option<Node<'mcx>> = None;
    let mut crerr = CrErr::NoColumn;
    let mut nspname: Option<String> = None;
    let mut relname: Option<String> = None;
    let mut colname: Option<String> = None;

    let nfields = cref.fields.len();
    // The C `switch` uses `break` to fall through to the common post-hook /
    // error tail; the labeled block reproduces that (a `break 'sw` is a C
    // `break`).
    'sw: {
        match nfields {
            1 => {
                let cname = str_val(&cref.fields[0])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                colname = Some(cname.clone());

                // Try as an unqualified column.
                node = colNameToVar(mcx, pstate, &cname, false, location)?;

                if node.is_none() {
                    // PostQUEL-compat: try the name as a relation in the RT.
                    if let Some((levels_up, ns_idx)) =
                        refnameNamespaceItem(pstate, None, &cname, location, true)?
                    {
                        node = Some(transformWholeRowRef(pstate, ns_idx, levels_up, location)?);
                    }
                }
            }
            2 => {
                let rname = str_val(&cref.fields[0])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                relname = Some(rname.clone());

                let nsitem =
                    refnameNamespaceItem(pstate, nspname.as_deref(), &rname, location, true)?;
                let Some((levels_up, ns_idx)) = nsitem else {
                    crerr = CrErr::NoRte;
                    break 'sw;
                };

                // Whole-row reference?
                if matches!(&*cref.fields[1], Node::A_Star(_)) {
                    node = Some(transformWholeRowRef(pstate, ns_idx, levels_up, location)?);
                } else {
                    let cname = str_val(&cref.fields[1]).ok_or_else(|| {
                        PgError::error("transformColumnRef: field is not a String")
                    })?;
                    colname = Some(cname.clone());
                    node = scanNSItemForColumn(mcx, pstate, ns_idx, levels_up, &cname, location)?;
                    if node.is_none() {
                        // Try it as a function call on the whole row.
                        let whole = transformWholeRowRef(pstate, ns_idx, levels_up, location)?;
                        node = parse_func_on_whole_row(pstate, &cname, whole, location)?;
                    }
                }
            }
            3 => {
                let nname = str_val(&cref.fields[0])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                let rname = str_val(&cref.fields[1])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                nspname = Some(nname.clone());
                relname = Some(rname.clone());

                let nsitem = refnameNamespaceItem(pstate, Some(&nname), &rname, location, true)?;
                let Some((levels_up, ns_idx)) = nsitem else {
                    crerr = CrErr::NoRte;
                    break 'sw;
                };

                if matches!(&*cref.fields[2], Node::A_Star(_)) {
                    node = Some(transformWholeRowRef(pstate, ns_idx, levels_up, location)?);
                } else {
                    let cname = str_val(&cref.fields[2]).ok_or_else(|| {
                        PgError::error("transformColumnRef: field is not a String")
                    })?;
                    colname = Some(cname.clone());
                    node = scanNSItemForColumn(mcx, pstate, ns_idx, levels_up, &cname, location)?;
                    if node.is_none() {
                        let whole = transformWholeRowRef(pstate, ns_idx, levels_up, location)?;
                        node = parse_func_on_whole_row(pstate, &cname, whole, location)?;
                    }
                }
            }
            4 => {
                let catname = str_val(&cref.fields[0])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                let nname = str_val(&cref.fields[1])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                let rname = str_val(&cref.fields[2])
                    .ok_or_else(|| PgError::error("transformColumnRef: field is not a String"))?;
                nspname = Some(nname.clone());
                relname = Some(rname.clone());

                // We check the catalog name and then ignore it.
                if catalogname_differs_from_database(mcx, &catname)? {
                    crerr = CrErr::WrongDb;
                    break 'sw;
                }

                let nsitem = refnameNamespaceItem(pstate, Some(&nname), &rname, location, true)?;
                let Some((levels_up, ns_idx)) = nsitem else {
                    crerr = CrErr::NoRte;
                    break 'sw;
                };

                if matches!(&*cref.fields[3], Node::A_Star(_)) {
                    node = Some(transformWholeRowRef(pstate, ns_idx, levels_up, location)?);
                } else {
                    let cname = str_val(&cref.fields[3]).ok_or_else(|| {
                        PgError::error("transformColumnRef: field is not a String")
                    })?;
                    colname = Some(cname.clone());
                    node = scanNSItemForColumn(mcx, pstate, ns_idx, levels_up, &cname, location)?;
                    if node.is_none() {
                        let whole = transformWholeRowRef(pstate, ns_idx, levels_up, location)?;
                        node = parse_func_on_whole_row(pstate, &cname, whole, location)?;
                    }
                }
            }
            _ => {
                crerr = CrErr::TooMany; // too many dotted names
            }
        }
    }

    let node = resolve_columnref_finish(
        pstate, node, crerr, nspname, relname, colname, &cref, location, mcx,
    )?;
    node_into_expr(node).ok_or_else(|| {
        PgError::error("transformColumnRef: resolved node is not an expression")
    })
}

/// The shared tail of `transformColumnRef`: the PostParseColumnRefHook step and
/// the "no translation found" error switch. Returns the resolved `Node`.
#[allow(clippy::too_many_arguments)]
fn resolve_columnref_finish<'mcx>(
    pstate: &mut ParseState<'mcx>,
    node: Option<Node<'mcx>>,
    crerr: CrErr,
    nspname: Option<String>,
    relname: Option<String>,
    colname: Option<String>,
    cref: &ColumnRef<'mcx>,
    location: i32,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<Node<'mcx>> {
    // Give the PostParseColumnRefHook, if any, a chance.
    if pstate.p_post_columnref_hook.is_some() {
        return seam_transform_post_columnref_hook(pstate, cref.clone_in(mcx)?, node);
    }

    if let Some(node) = node {
        return Ok(node);
    }

    // Throw error if no translation found.
    match crerr {
        CrErr::NoColumn => {
            errorMissingColumn(
                mcx,
                pstate,
                relname.as_deref(),
                colname.as_deref().unwrap_or(""),
                location,
            )?;
            unreachable!()
        }
        CrErr::NoRte => {
            // makeRangeVar(nspname, relname, location).
            let rv = types_nodes::rawnodes::RangeVar {
                catalogname: None,
                schemaname: match &nspname {
                    Some(s) => Some(mcx::PgString::from_str_in(s, mcx)?),
                    None => None,
                },
                relname: match &relname {
                    Some(s) => Some(mcx::PgString::from_str_in(s, mcx)?),
                    None => None,
                },
                inh: true,
                relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT as i8,
                alias: None,
                location,
            };
            errorMissingRTE(mcx, pstate, &rv)?;
            unreachable!()
        }
        CrErr::WrongDb => Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "cross-database references are not implemented: {}",
                namelist_to_string(&cref.fields)
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error()),
        CrErr::TooMany => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(alloc::format!(
                "improper qualified name (too many dotted names): {}",
                namelist_to_string(&cref.fields)
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error()),
    }
}

/// `NameListToString(fields)` (namespace.c) — render a dotted name list for the
/// error messages. Only `String` and `A_Star` ('*') elements occur.
fn namelist_to_string(fields: &mcx::PgVec<'_, nodes::NodePtr<'_>>) -> String {
    let mut out = String::new();
    for (i, n) in fields.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        match &**n {
            Node::String(s) => out.push_str(s.sval.as_str()),
            Node::A_Star(_) => out.push('*'),
            _ => {}
        }
    }
    out
}

/// `transformWholeRowRef(pstate, nsitem, sublevels_up, location)`
/// (parse_expr.c:2632). `nsitem` is identified by its index into
/// `pstate->p_namespace`.
fn transformWholeRowRef<'mcx>(
    pstate: &mut ParseState<'mcx>,
    nsitem_index: usize,
    sublevels_up: i32,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = aexpr_clone_ctx(pstate);

    // Read the nsitem fields we need (cloning the RTE and names).
    let (rte, p_rtindex, p_returning_type, names_is_eref, colnames_len, colnames) = {
        let nsitem = &pstate.p_namespace[nsitem_index];
        let rte = nsitem
            .p_rte
            .as_deref()
            .ok_or_else(|| PgError::error("transformWholeRowRef: nsitem has no RTE"))?
            .clone_in(mcx)?;
        let p_names = nsitem
            .p_names
            .as_deref()
            .ok_or_else(|| PgError::error("transformWholeRowRef: nsitem has no p_names"))?;
        // `nsitem->p_names == nsitem->p_rte->eref` (whole-row Var) vs a JOIN
        // USING alias (RowExpr). The owned model carries p_names as a clone of
        // the RTE's eref for ordinary nsitems and as the using-alias for JOIN
        // USING; the structural equality of the two Aliases is the faithful
        // proxy for the C pointer identity.
        let names_is_eref = alias_eq(p_names, rte.eref.as_deref());
        let colnames_len = p_names.colnames.len();
        let colnames: Vec<String> = p_names
            .colnames
            .iter()
            .filter_map(|n| str_val(n))
            .collect();
        (
            rte,
            nsitem.p_rtindex,
            nsitem.p_returning_type,
            names_is_eref,
            colnames_len,
            colnames,
        )
    };

    if names_is_eref || p_returning_type != types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT
    {
        // Normal whole-row Var.
        let mut var = make_whole_row_var(&rte, p_rtindex, sublevels_up as types_core::Index)?;
        var.varreturningtype = p_returning_type;
        // location is not filled in by makeWholeRowVar.
        var.location = location;
        // Mark Var if it's nulled by any outer joins.
        backend_parser_relation::markNullableIfNeeded(pstate, &mut var)?;
        // Mark relation as requiring whole-row SELECT access.
        backend_parser_relation::markVarForSelectPriv(mcx, pstate, &var)?;
        Ok(Node::Expr(Expr::Var(var)))
    } else {
        // JOIN USING alias: expand into a RowExpr of the common columns.
        let mut colvars: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
        backend_parser_relation::expandRTE(
            mcx,
            &rte,
            p_rtindex,
            sublevels_up,
            p_returning_type,
            location,
            false,
            None,
            Some(&mut colvars),
        )?;
        // list_truncate(fields, list_length(p_names->colnames)).
        let mut args: Vec<Expr> = Vec::with_capacity(colnames_len);
        for (i, cv) in colvars.into_iter().enumerate() {
            if i >= colnames_len {
                break;
            }
            if let Some(e) = node_into_expr(mcx::PgBox::into_inner(cv)) {
                args.push(e);
            }
        }
        Ok(Node::Expr(Expr::RowExpr(RowExpr {
            args,
            row_typeid: RECORDOID,
            row_format: CoercionForm::COERCE_IMPLICIT_CAST,
            colnames,
            location,
        })))
    }
}

/// `nsitem->p_names == nsitem->p_rte->eref` proxy: the two `Alias`es are equal
/// in contents (aliasname + colnames). For ordinary nsitems p_names is a clone
/// of eref; for a JOIN USING alias it is the (distinct) using-alias.
fn alias_eq(a: &types_nodes::rawnodes::Alias<'_>, b: Option<&types_nodes::rawnodes::Alias<'_>>) -> bool {
    let Some(b) = b else { return false };
    if a.aliasname.as_deref() != b.aliasname.as_deref() {
        return false;
    }
    if a.colnames.len() != b.colnames.len() {
        return false;
    }
    for (x, y) in a.colnames.iter().zip(b.colnames.iter()) {
        if str_val(x) != str_val(y) {
            return false;
        }
    }
    true
}

/// `makeWholeRowVar(rte, varno, varlevelsup, allowScalar=true)` (makefuncs.c).
/// The RTE_FUNCTION / SRF-expanded-subquery branches need `exprType` over a
/// `RangeTblFunction.funcexpr` `Node` (the trimmed `expr_type` only covers
/// `Expr`); those are routed to the funcapi seam (mirror-PG-and-panic).
fn make_whole_row_var(
    rte: &types_nodes::RangeTblEntry<'_>,
    varno: i32,
    varlevelsup: types_core::Index,
) -> PgResult<types_nodes::primnodes::Var> {
    use types_nodes::parsenodes::RTEKind::*;
    let mk = |toid: Oid, varattno: i32, varcollid: Oid| {
        backend_nodes_core::makefuncs::make_var(
            varno,
            varattno as types_core::AttrNumber,
            toid,
            -1,
            varcollid,
            varlevelsup,
        )
    };
    match rte.rtekind {
        RTE_RELATION => {
            let toid = lsyscache::get_rel_type_id::call(rte.relid)?;
            if !OidIsValid(toid) {
                let scratch = MemoryContext::new("makeWholeRowVar");
                let relname = lsyscache::get_rel_name::call(scratch.mcx(), rte.relid)?
                    .map(|s| String::from(s.as_str()))
                    .unwrap_or_default();
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!(
                        "relation \"{}\" does not have a composite type",
                        relname
                    ))
                    .into_error());
            }
            Ok(mk(toid, 0, InvalidOid))
        }
        RTE_SUBQUERY => {
            if OidIsValid(rte.relid) {
                // Subquery expanded from a view.
                let toid = lsyscache::get_rel_type_id::call(rte.relid)?;
                if !OidIsValid(toid) {
                    let scratch = MemoryContext::new("makeWholeRowVar");
                    let relname = lsyscache::get_rel_name::call(scratch.mcx(), rte.relid)?
                        .map(|s| String::from(s.as_str()))
                        .unwrap_or_default();
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(alloc::format!(
                            "relation \"{}\" does not have a composite type",
                            relname
                        ))
                        .into_error());
                }
                Ok(mk(toid, 0, InvalidOid))
            } else if !rte.functions.is_empty() {
                // Subquery expanded from a set-returning function (planning-only).
                make_whole_row_var_func(rte)
            } else {
                // Normal subquery-in-FROM.
                Ok(mk(RECORDOID, 0, InvalidOid))
            }
        }
        RTE_FUNCTION => make_whole_row_var_func(rte),
        // Join, tablefunc, VALUES, CTE, etc. → a whole-row Var of RECORD type.
        _ => Ok(mk(RECORDOID, 0, InvalidOid)),
    }
}

/// The RTE_FUNCTION branch of `makeWholeRowVar` — needs `exprType` over the
/// first `RangeTblFunction.funcexpr` (a `Node`). The trimmed `expr_type` works
/// only over `Expr`; the funcexpr-as-Node typing is the funcapi seam's
/// responsibility (mirror-PG-and-panic).
fn make_whole_row_var_func(
    _rte: &types_nodes::RangeTblEntry<'_>,
) -> PgResult<types_nodes::primnodes::Var> {
    panic!(
        "makeWholeRowVar over an RTE_FUNCTION needs exprType over the \
         RangeTblFunction.funcexpr Node; the repo's expr_type covers only the \
         trimmed Expr, so funcexpr-as-Node typing is blocked pending the funcapi \
         Node-level exprType seam."
    )
}

/// `ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node),
/// pstate->p_last_srf, NULL, false, location)` — the "function call on the whole
/// row" fallback shared by the 2/3/4-field column-ref cases.
fn parse_func_on_whole_row<'mcx>(
    pstate: &mut ParseState<'mcx>,
    colname: &str,
    whole: Node<'mcx>,
    location: i32,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = aexpr_clone_ctx(pstate);
    let funcname = [mcx::PgString::from_str_in(colname, mcx)?];
    let arg = node_into_expr(whole)
        .ok_or_else(|| PgError::error("parse_func_on_whole_row: whole-row ref is not an expr"))?;
    let last_srf = last_srf_expr(pstate);
    let res = backend_parser_func::ParseFuncOrColumn(
        pstate,
        &funcname,
        vec![arg],
        last_srf.as_ref(),
        None,
        false,
        location,
    )?;
    Ok(res.map(Node::Expr))
}

// ===========================================================================
// transformIndirection (parse_expr.c:436).
// ===========================================================================

/// `transformIndirection(pstate, ind)` — split field selections from container
/// subscripting in an `a.b[1].c`-style chain.
fn transformIndirection<'mcx>(
    pstate: &mut ParseState<'mcx>,
    ind: A_Indirection<'mcx>,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);
    let last_srf = last_srf_expr(pstate);

    let A_Indirection { arg, indirection } = ind;
    let mut result = transformExprRecurse(pstate, boxed_node(arg))?
        .ok_or_else(|| PgError::error("transformIndirection: argument is NULL"))?;
    let location = expr_location(Some(&result))?;

    // Adjacent A_Indices nodes are a single multidimensional subscript op.
    let mut subscripts: Vec<A_Indices<'mcx>> = Vec::new();

    for n in indirection.into_iter() {
        let n = mcx::PgBox::into_inner(n);
        match n {
            Node::A_Indices(ai) => subscripts.push(ai),
            Node::A_Star(_) => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("row expansion via \"*\" is not supported here")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            Node::String(s) => {
                // Process subscripts before this field selection.
                if !subscripts.is_empty() {
                    let ctype = expr_type(Some(&result))?;
                    let ctypmod = expr_typmod(Some(&result))?;
                    let sref = backend_parser_small1::transformContainerSubscripts(
                        mcx,
                        pstate,
                        result,
                        ctype,
                        ctypmod,
                        &subscripts,
                        false,
                    )?;
                    result = Expr::SubscriptingRef(sref);
                    subscripts = Vec::new();
                }

                let colname = String::from(s.sval.as_str());
                let funcname = [mcx::PgString::from_str_in(&colname, mcx)?];
                let newresult = backend_parser_func::ParseFuncOrColumn(
                    pstate,
                    &funcname,
                    vec![result.clone()],
                    last_srf.as_ref(),
                    None,
                    false,
                    location,
                )?;
                match newresult {
                    Some(e) => result = e,
                    None => {
                        unknown_attribute(pstate, &result, &colname, location)?;
                        unreachable!()
                    }
                }
            }
            other => {
                return Err(PgError::error(alloc::format!(
                    "transformIndirection: unexpected indirection node (tag {})",
                    other.node_tag().0
                )))
            }
        }
    }

    // Process trailing subscripts, if any.
    if !subscripts.is_empty() {
        let ctype = expr_type(Some(&result))?;
        let ctypmod = expr_typmod(Some(&result))?;
        let sref = backend_parser_small1::transformContainerSubscripts(
            mcx, pstate, result, ctype, ctypmod, &subscripts, false,
        )?;
        result = Expr::SubscriptingRef(sref);
    }

    Ok(result)
}

/// `unknown_attribute(pstate, relref, attname, location)` (parse_expr.c:401) —
/// the "no such column / not composite" error for a failed field selection.
fn unknown_attribute<'mcx>(
    pstate: &ParseState<'mcx>,
    relref: &Expr,
    attname: &str,
    location: i32,
) -> PgResult<core::convert::Infallible> {
    let rel_type = expr_type(Some(relref))?;
    if OidIsValid(rel_type) && rel_type != RECORDOID {
        Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(alloc::format!(
                "column \"{}\" not found in data type {}",
                attname,
                format_type_be(rel_type)?
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error())
    } else if rel_type == RECORDOID {
        Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(alloc::format!(
                "could not identify column \"{}\" in record data type",
                attname
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error())
    } else {
        Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(alloc::format!(
                "column notation .{} applied to type {}, which is not a composite type",
                attname,
                format_type_be(rel_type)?
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error())
    }
}

// ===========================================================================
// transformFuncCall (parse_expr.c:1448).
// ===========================================================================

/// `transformFuncCall(pstate, fn)` — transform the argument list (plus WITHIN
/// GROUP ORDER BY exprs) and hand off to `ParseFuncOrColumn`.
fn transformFuncCall<'mcx>(
    pstate: &mut ParseState<'mcx>,
    fn_call: FuncCall<'mcx>,
) -> PgResult<Expr> {
    let last_srf = last_srf_expr(pstate);

    // Transform the argument list.
    let mut targs: Vec<Expr> = Vec::with_capacity(fn_call.args.len());
    for arg in fn_call.args.iter() {
        let a = transformExprRecurse(pstate, Some((**arg).clone_in(aexpr_clone_ctx(pstate))?))?
            .ok_or_else(|| PgError::error("transformFuncCall: argument is NULL"))?;
        targs.push(a);
    }

    // WITHIN GROUP: treat the ORDER BY expressions as additional arguments.
    if fn_call.agg_within_group {
        for sb in fn_call.agg_order.iter() {
            let node = match &**sb {
                Node::SortBy(s) => s.node.as_deref().map(|n| n.clone_in(aexpr_clone_ctx(pstate))),
                _ => None,
            };
            let node = match node {
                Some(r) => Some(r?),
                None => None,
            };
            let e = transformExpr(pstate, node, ParseExprKind::EXPR_KIND_ORDER_BY)?
                .ok_or_else(|| PgError::error("transformFuncCall: WITHIN GROUP expr is NULL"))?;
            targs.push(e);
        }
    }

    // Hand off to ParseFuncOrColumn.
    let location = fn_call.location;
    let funcname = clone_namelist_pgstrings(&fn_call.funcname, aexpr_clone_ctx(pstate))?;
    let res = backend_parser_func::ParseFuncOrColumn(
        pstate,
        &funcname,
        targs,
        last_srf.as_ref(),
        Some(&fn_call),
        false,
        location,
    )?;
    res.ok_or_else(|| PgError::error("transformFuncCall: ParseFuncOrColumn returned NULL"))
}

/// Convert a raw `List *funcname` (`String` value nodes) into the
/// `&[PgString]` form `ParseFuncOrColumn` consumes.
fn clone_namelist_pgstrings<'mcx>(
    name: &mcx::PgVec<'_, nodes::NodePtr<'_>>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<Vec<mcx::PgString<'mcx>>> {
    let mut out: Vec<mcx::PgString<'mcx>> = Vec::with_capacity(name.len());
    for n in name.iter() {
        if let Some(s) = str_val(n) {
            out.push(mcx::PgString::from_str_in(&s, mcx)?);
        }
    }
    Ok(out)
}

// ===========================================================================
// transformRowExpr (parse_expr.c:2187).
// ===========================================================================

/// `transformRowExpr(pstate, r, allowDefault)` — transform a `ROW(...)`
/// constructor. The raw `RowExpr.args` carry the (untransformed) field
/// expressions; wrap them as `Node`s for `transformExpressionList`.
fn transformRowExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    r: RowExpr,
    allow_default: bool,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);
    let location = r.location;

    // Transform the field expressions. transformExpressionList expands any
    // "something.*" entries; build the raw-node list from the carried args.
    let mut exprlist: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    for e in r.args {
        exprlist.push(mcx::alloc_in(mcx, expr_to_node(e))?);
    }
    let expr_kind = pstate.p_expr_kind;
    let newargs_vec = backend_parser_parse_target::transformExpressionList(
        mcx,
        pstate,
        exprlist,
        expr_kind,
        allow_default,
    )?;
    let newargs: Vec<Expr> = newargs_vec.into_iter().collect();

    // Disallow more columns than will fit in a tuple.
    if newargs.len() as i32 > MaxTupleAttributeNumber {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(alloc::format!(
                "ROW expressions can have at most {} entries",
                MaxTupleAttributeNumber
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // ROW() has anonymous columns; invent f1, f2, … field names.
    let mut colnames: Vec<String> = Vec::with_capacity(newargs.len());
    for fnum in 1..=newargs.len() {
        colnames.push(alloc::format!("f{}", fnum));
    }

    Ok(Expr::RowExpr(RowExpr {
        args: newargs,
        // Barring later casting, the type is RECORD.
        row_typeid: RECORDOID,
        row_format: CoercionForm::COERCE_IMPLICIT_CAST,
        colnames,
        location,
    }))
}

// ===========================================================================
// transformMultiAssignRef (parse_expr.c:2074).
// ===========================================================================

/// `transformMultiAssignRef(pstate, maref)` — first-stage processing of an
/// UPDATE multi-column assignment (`(a,b) = (...)`).
fn transformMultiAssignRef<'mcx>(
    pstate: &mut ParseState<'mcx>,
    maref: MultiAssignRef<'mcx>,
) -> PgResult<Expr> {
    use types_nodes::primnodes::{Param, ParamKind, SubLinkType};

    let mcx = aexpr_clone_ctx(pstate);

    // Should only appear in first-stage processing of UPDATE tlists.
    debug_assert!(pstate.p_expr_kind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE);

    let MultiAssignRef { source, colno, ncolumns } = maref;

    // Only transform the source for the first column.
    if colno == 1 {
        let src = boxed_node(source)
            .ok_or_else(|| PgError::error("transformMultiAssignRef: NULL source"))?;
        // We only allow EXPR SubLinks and RowExprs as the source of an UPDATE
        // multiassignment. The raw-grammar SubLink is `Node::SubLink`; the
        // RowExpr carrier follows the crate's `Expr::RowExpr` convention
        // (transformRowExpr consumes the primnodes RowExpr, whose `args` are
        // the raw field expressions).
        let is_expr_sublink = matches!(
            &src,
            Node::SubLink(s)
                if s.sub_link_type == SubLinkType::Expr
        );
        let is_rowexpr = matches!(&src, Node::Expr(Expr::RowExpr(_)));

        if is_expr_sublink {
            let mut sublink = match src {
                Node::SubLink(s) => s,
                _ => unreachable!("is_expr_sublink guard"),
            };
            // Relabel it as a MULTIEXPR_SUBLINK, and transform it.
            sublink.sub_link_type = SubLinkType::MultiExpr;
            let transformed = transformSubLink(pstate, sublink)?;
            let mut sublink = match transformed {
                Expr::SubLink(s) => s,
                _ => unreachable!("transformSubLink yields SubLink"),
            };

            // qtree = castNode(Query, sublink->subselect).
            let ncols = {
                let qtree = sublink
                    .subselect
                    .as_deref()
                    .ok_or_else(|| PgError::error("MULTIEXPR SubLink has no subselect"))?;
                count_nonjunk_tlist_entries(&qtree.targetList)
            };
            // Check subquery returns required number of columns.
            if ncols != ncolumns as usize {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("number of columns does not match number of values")
                    .errposition(parser_errposition(pstate, sublink.location))
                    .into_error());
            }

            // Assign a unique-within-this-targetlist ID to the MULTIEXPR
            // SubLink: its position in p_multiassign_exprs (post-append).
            sublink.subLinkId = (pstate.p_multiassign_exprs.len() + 1) as i32;

            // Build a resjunk tlist item containing the MULTIEXPR SubLink and
            // add it to p_multiassign_exprs.
            let tle = make_target_entry(mcx, Expr::SubLink(sublink), 0, None, true)?;
            pstate.p_multiassign_exprs.push(tle);
        } else if is_rowexpr {
            let Node::Expr(Expr::RowExpr(rexpr)) = src else {
                unreachable!()
            };
            // Transform the RowExpr, allowing SetToDefault items.
            let rexpr = transformRowExpr(pstate, rexpr, true)?;
            let nargs = match &rexpr {
                Expr::RowExpr(r) => r.args.len() as i32,
                _ => 0,
            };
            if nargs != ncolumns {
                let loc = expr_location(Some(&rexpr))?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("number of columns does not match number of values")
                    .errposition(parser_errposition(pstate, loc))
                    .into_error());
            }
            // Temporarily append to p_multiassign_exprs so later columns can
            // re-fetch it.
            let tle = make_target_entry(mcx, rexpr, 0, None, true)?;
            pstate.p_multiassign_exprs.push(tle);
        } else {
            // exprLocation(maref->source).
            let loc = node_location(&src);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression",
                )
                .errposition(parser_errposition(pstate, loc))
                .into_error());
        }
    }

    // Emit the appropriate output expression for the current column. Re-fetch
    // the transformed RowExpr (or SubLink) — the last entry of
    // p_multiassign_exprs.
    let tle = pstate
        .p_multiassign_exprs
        .last()
        .ok_or_else(|| PgError::error("transformMultiAssignRef: empty p_multiassign_exprs"))?;
    let tle_expr = tle
        .expr
        .as_deref()
        .ok_or_else(|| PgError::error("transformMultiAssignRef: tle has no expr"))?;

    match tle_expr {
        Expr::SubLink(sublink) => {
            // Build a Param representing the current subquery output column
            // (PARAM_MULTIEXPR). paramid = (subLinkId << 16) | colno.
            debug_assert!(sublink.subLinkType == SubLinkType::MultiExpr);
            let texpr: Option<&Expr> = Some(tle_expr);
            let param = Param {
                paramkind: ParamKind::PARAM_MULTIEXPR,
                paramid: (sublink.subLinkId << 16) | colno,
                paramtype: expr_type(texpr)?,
                paramtypmod: expr_typmod(texpr)?,
                paramcollid: expr_collation(texpr)?,
                location: expr_location(texpr)?,
            };
            Ok(Expr::Param(param))
        }
        Expr::RowExpr(r) => {
            // Extract and return the next element of the RowExpr.
            let idx = (colno - 1) as usize;
            let result = r
                .args
                .get(idx)
                .cloned()
                .ok_or_else(|| PgError::error("transformMultiAssignRef: colno out of range"))?;
            // At the last column, delete the RowExpr from p_multiassign_exprs.
            if colno == ncolumns {
                pstate.p_multiassign_exprs.pop();
            }
            Ok(result)
        }
        _ => Err(PgError::error("unexpected expr type in multiassign list")),
    }
}

/// The PreParseColumnRefHook leg of `transformColumnRef` — the hook ABI carries
/// opaque cross-ABI function pointers; reached only when a hook is installed
/// (absent in the stock server).
fn seam_transform_column_ref_hook<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _cref: ColumnRef<'mcx>,
) -> PgResult<Expr> {
    panic!(
        "transformColumnRef's PreParseColumnRefHook leg needs the opaque \
         columnref parser-hook ABI; the hook-installed path is reached only when \
         a hook is present (absent in the stock server)."
    )
}

/// The PostParseColumnRefHook leg of `transformColumnRef`.
fn seam_transform_post_columnref_hook<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _cref: ColumnRef<'mcx>,
    _node: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    panic!(
        "transformColumnRef's PostParseColumnRefHook leg needs the opaque \
         columnref parser-hook ABI; the hook-installed path is reached only when \
         a hook is present (absent in the stock server)."
    )
}

// ===========================================================================
// Seam-and-panic transform arms (unported sibling owners). Each routes through
// a panic-until-landed seam declared in the matching sibling `*-seams` crate.
// ===========================================================================

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

/// `transformParamRef(pstate, pref)` (parse_expr.c) — transform a `$n`
/// parameter reference into a `Param`.
///
/// The core parser knows nothing about Params; in C the work is done by the
/// installed `pstate->p_paramref_hook`. The owned model selects the hook from
/// the active `pstate.p_ref_hook_state` arm — the real artifact the C function
/// pointer selects (cf. `setup_parse_{fixed,variable}_parameters`, which set the
/// ref-hook state and the hook in lockstep). If no hook is installed (or it
/// returns NULL) we throw the generic "there is no parameter $n" error, exactly
/// as C does.
fn transformParamRef<'mcx>(
    pstate: &mut ParseState<'mcx>,
    pref: &types_nodes::rawnodes::ParamRef,
) -> PgResult<Expr> {
    use types_nodes::parsestmt::ParseRefHookState;

    // The small1 paramref hooks take a `types_nodes::params::ParamRef`; bridge
    // from the raw-node `ParamRef` (same fields).
    let hook_pref = types_nodes::params::ParamRef {
        number: pref.number,
        location: pref.location,
    };

    // if (pstate->p_paramref_hook != NULL) result = pstate->p_paramref_hook(...)
    // else result = NULL;
    let result: Option<types_nodes::primnodes::Param> = match &pstate.p_ref_hook_state {
        ParseRefHookState::FixedParams(parstate) => Some(
            backend_parser_small1::fixed_paramref_hook(pstate, parstate, &hook_pref)?,
        ),
        ParseRefHookState::VarParams(parstate) => Some(
            backend_parser_small1::variable_paramref_hook(pstate, parstate, &hook_pref)?,
        ),
        ParseRefHookState::None => None,
    };

    // if (result == NULL) ereport(ERROR, "there is no parameter $%d", ...)
    match result {
        Some(param) => Ok(Expr::Param(param)),
        None => Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(alloc::format!("there is no parameter ${}", pref.number))
            .errposition(parser_errposition(pstate, pref.location))
            .into_error()),
    }
}

/// `count_nonjunk_tlist_entries(targetlist)` (parse_node.c) — the number of
/// non-resjunk entries in a target list.
fn count_nonjunk_tlist_entries(
    targetlist: &mcx::PgVec<'_, types_nodes::primnodes::TargetEntry<'_>>,
) -> usize {
    targetlist.iter().filter(|tle| !tle.resjunk).count()
}

/// `transformSubLink(pstate, sublink)` (parse_expr.c:1782) — analyze a
/// sub-SELECT appearing in an expression. Runs `parse_sub_analyze` (analyze.c)
/// over the raw `subselect`, embeds the resulting `Query` in the analyzed
/// `SubLink`, and (for ALL/ANY/ROWCOMPARE) builds the row-comparison
/// `testexpr`. The output is `(Node *) sublink` — an analyzed
/// [`Expr::SubLink`].
fn transformSubLink<'mcx>(
    pstate: &mut ParseState<'mcx>,
    sublink: types_nodes::rawexprnodes::SubLink<'mcx>,
) -> PgResult<Expr> {
    use types_nodes::primnodes::{Param, ParamKind, SubLinkType};

    let mcx = aexpr_clone_ctx(pstate);

    // Check to see if the sublink is in an invalid place within the query. We
    // allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE/MERGE, but
    // generally not in utility statements.
    let err: Option<&str> = match pstate.p_expr_kind {
        ParseExprKind::EXPR_KIND_NONE => {
            // Assert(false) — can't happen.
            debug_assert!(false, "EXPR_KIND_NONE in transformSubLink");
            None
        }
        ParseExprKind::EXPR_KIND_OTHER
        | ParseExprKind::EXPR_KIND_JOIN_ON
        | ParseExprKind::EXPR_KIND_JOIN_USING
        | ParseExprKind::EXPR_KIND_FROM_SUBSELECT
        | ParseExprKind::EXPR_KIND_FROM_FUNCTION
        | ParseExprKind::EXPR_KIND_WHERE
        | ParseExprKind::EXPR_KIND_POLICY
        | ParseExprKind::EXPR_KIND_HAVING
        | ParseExprKind::EXPR_KIND_FILTER
        | ParseExprKind::EXPR_KIND_WINDOW_PARTITION
        | ParseExprKind::EXPR_KIND_WINDOW_ORDER
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_GROUPS
        | ParseExprKind::EXPR_KIND_SELECT_TARGET
        | ParseExprKind::EXPR_KIND_INSERT_TARGET
        | ParseExprKind::EXPR_KIND_UPDATE_SOURCE
        | ParseExprKind::EXPR_KIND_UPDATE_TARGET
        | ParseExprKind::EXPR_KIND_MERGE_WHEN
        | ParseExprKind::EXPR_KIND_GROUP_BY
        | ParseExprKind::EXPR_KIND_ORDER_BY
        | ParseExprKind::EXPR_KIND_DISTINCT_ON
        | ParseExprKind::EXPR_KIND_LIMIT
        | ParseExprKind::EXPR_KIND_OFFSET
        | ParseExprKind::EXPR_KIND_RETURNING
        | ParseExprKind::EXPR_KIND_MERGE_RETURNING
        | ParseExprKind::EXPR_KIND_VALUES
        | ParseExprKind::EXPR_KIND_VALUES_SINGLE
        | ParseExprKind::EXPR_KIND_CYCLE_MARK => None,
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT
        | ParseExprKind::EXPR_KIND_DOMAIN_CHECK => {
            Some("cannot use subquery in check constraint")
        }
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT
        | ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT => {
            Some("cannot use subquery in DEFAULT expression")
        }
        ParseExprKind::EXPR_KIND_INDEX_EXPRESSION => {
            Some("cannot use subquery in index expression")
        }
        ParseExprKind::EXPR_KIND_INDEX_PREDICATE => {
            Some("cannot use subquery in index predicate")
        }
        ParseExprKind::EXPR_KIND_STATS_EXPRESSION => {
            Some("cannot use subquery in statistics expression")
        }
        ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM => {
            Some("cannot use subquery in transform expression")
        }
        ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER => {
            Some("cannot use subquery in EXECUTE parameter")
        }
        ParseExprKind::EXPR_KIND_TRIGGER_WHEN => {
            Some("cannot use subquery in trigger WHEN condition")
        }
        ParseExprKind::EXPR_KIND_PARTITION_BOUND => {
            Some("cannot use subquery in partition bound")
        }
        ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION => {
            Some("cannot use subquery in partition key expression")
        }
        ParseExprKind::EXPR_KIND_CALL_ARGUMENT => {
            Some("cannot use subquery in CALL argument")
        }
        ParseExprKind::EXPR_KIND_COPY_WHERE => {
            Some("cannot use subquery in COPY FROM WHERE condition")
        }
        ParseExprKind::EXPR_KIND_GENERATED_COLUMN => {
            Some("cannot use subquery in column generation expression")
        }
    };
    if let Some(err) = err {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(err)
            .errposition(parser_errposition(pstate, sublink.location))
            .into_error());
    }

    pstate.p_hasSubLinks = true;

    // Destructure the raw SubLink.
    let types_nodes::rawexprnodes::SubLink {
        sub_link_type,
        sub_link_id,
        testexpr,
        oper_name,
        subselect,
        location,
    } = sublink;

    // OK, let's transform the sub-SELECT.
    let subselect = subselect
        .ok_or_else(|| PgError::error("transformSubLink: NULL subselect"))?;
    let qtree_node =
        backend_parser_analyze_seams::parse_sub_analyze::call(
            mcx,
            &subselect,
            pstate,
            None,
            false,
            true,
        )?;

    // Check that we got a SELECT. Anything else should be impossible given
    // restrictions of the grammar, but check anyway.
    let qtree = match qtree_node.as_ref() {
        Node::Query(q) if q.commandType == types_nodes::nodes::CmdType::CMD_SELECT => q,
        _ => {
            return Err(PgError::error(
                "unexpected non-SELECT command in SubLink",
            ))
        }
    };

    // Embed the analyzed Query as the SubLink's owned subselect, mirroring
    // RangeTblEntry.subquery; the Expr tree is lifetime-free, so the embedded
    // Query carries the 'static notional lifetime (cf. Aggref::args /
    // SubPlanExpr — see tlist_into_static / query_into_static).
    let analyzed_subselect = Some(query_into_static(mcx, qtree)?);

    let mut out = types_nodes::primnodes::SubLink {
        subLinkType: sub_link_type,
        subLinkId: sub_link_id,
        testexpr: None,
        subselect: analyzed_subselect,
        location,
    };

    if sub_link_type == SubLinkType::Exists {
        // EXISTS needs no test expression or combining operator.
        out.testexpr = None;
    } else if sub_link_type == SubLinkType::Expr || sub_link_type == SubLinkType::Array {
        // Make sure the subselect delivers a single column (ignoring resjunk).
        if count_nonjunk_tlist_entries(&qtree.targetList) != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("subquery must return only one column")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        // EXPR and ARRAY need no test expression or combining operator.
        out.testexpr = None;
    } else if sub_link_type == SubLinkType::MultiExpr {
        // Same as EXPR case, except no restriction on number of columns.
        out.testexpr = None;
    } else {
        // ALL, ANY, or ROWCOMPARE: generate row-comparing expression.

        // If the source was "x IN (select)", convert to "x = ANY (select)".
        let oper_name = if oper_name.is_empty() {
            let mut v: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
            let str_node = Node::String(types_nodes::value::StringNode {
                sval: mcx::PgString::from_str_in("=", mcx)?,
            });
            v.push(mcx::alloc_in(mcx, str_node)?);
            v
        } else {
            oper_name
        };

        // Transform lefthand expression, and convert to a list.
        let lefthand = transformExprRecurse(pstate, testexpr.map(mcx::PgBox::into_inner))?;
        let left_list: Vec<Expr> = match lefthand {
            Some(Expr::RowExpr(r)) => r.args,
            Some(other) => vec![other],
            None => Vec::new(),
        };

        // Build a list of PARAM_SUBLINK nodes representing the output columns
        // of the subquery.
        let mut right_list: Vec<Expr> = Vec::new();
        for tent in qtree.targetList.iter() {
            if tent.resjunk {
                continue;
            }
            let texpr = tent.expr.as_deref();
            let param = Param {
                paramkind: ParamKind::PARAM_SUBLINK,
                paramid: tent.resno as i32,
                paramtype: expr_type(texpr)?,
                paramtypmod: expr_typmod(texpr)?,
                paramcollid: expr_collation(texpr)?,
                location: -1,
            };
            right_list.push(Expr::Param(param));
        }

        // We could rely on make_row_comparison_op to complain if the list
        // lengths differ, but we prefer to generate a more specific error.
        if left_list.len() < right_list.len() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("subquery has too many columns")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        if left_list.len() > right_list.len() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("subquery has too few columns")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }

        // Identify the combining operator(s) and generate a suitable
        // row-comparison expression.
        let testexpr =
            make_row_comparison_op(pstate, &oper_name, left_list, right_list, location)?;
        out.testexpr = Some(Box::new(testexpr));
    }

    Ok(Expr::SubLink(out))
}

/// Reinterpret an mcx-allocated `Query<'mcx>` clone as the `'static`-notional
/// owned sub-`Query` carried inside the lifetime-free `Expr` tree
/// (`SubLink.subselect`, mirroring `Aggref::args`'s `tlist_into_static`). The
/// Query is deep-cloned into `mcx` (so it is fully owned), then the lifetime
/// parameter is erased to `'static`; the data is unchanged and the backing
/// arena outlives parse analysis in practice.
fn query_into_static<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    qtree: &types_nodes::copy_query::Query<'mcx>,
) -> PgResult<mcx::PgBox<'static, types_nodes::copy_query::Query<'static>>> {
    let owned: types_nodes::copy_query::Query<'mcx> = qtree.clone_in(mcx)?;
    let boxed: mcx::PgBox<'mcx, types_nodes::copy_query::Query<'mcx>> =
        mcx::alloc_in(mcx, owned)?;
    // SAFETY: the embedded sub-Query lives inside the lifetime-free Expr tree
    // (SubLink.subselect: Option<PgBox<'static, Query<'static>>>, mirroring
    // SubPlanExpr(Box<SubPlan<'static>>)). The clone above made it fully owned
    // in `mcx`; this erases the 'mcx lifetime to the 'static notional lifetime
    // of the Expr tree — a transmute of the lifetime parameter only, the data
    // is unchanged (same convention as tlist_into_static in backend-parser-agg).
    let boxed_static: mcx::PgBox<'static, types_nodes::copy_query::Query<'static>> =
        unsafe { core::mem::transmute(boxed) };
    Ok(boxed_static)
}

fn seam_transform_grouping_func<'mcx>(
    pstate: &mut ParseState<'mcx>,
    gf: Node<'mcx>,
) -> PgResult<Expr> {
    backend_parser_parse_agg_seams::transform_grouping_func::call(pstate, gf)
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

/// `strcmp(catalogname, get_database_name(MyDatabaseId)) != 0` — the four-part
/// column-ref catalog-name check (parse_expr.c:776). A NULL database name (no
/// such database — impossible for `MyDatabaseId`) compares unequal.
fn catalogname_differs_from_database(mcx: mcx::Mcx<'_>, catalogname: &str) -> PgResult<bool> {
    let dbname =
        dbcommands_seams::get_database_name::call(mcx, globals_seams::my_database_id::call())?;
    Ok(dbname.as_ref().map(|s| s.as_str()) != Some(catalogname))
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
    me::parse_expr_kind_name::set(ParseExprKindName);
    me::transformExpr::set(transformExpr);
}

#[cfg(test)]
mod tests;
