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
    ErrorLocation, PgError, PgResult, ERRCODE_CANNOT_COERCE, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INDETERMINATE_DATATYPE, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR, WARNING,
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
    BOOLOID, BYTEAOID, DATEOID, INT2VECTOROID, INT4OID, JSONBOID, NAMEOID, OIDVECTOROID, RECORDOID,
    TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UNKNOWNOID, XMLOID,
};

// SQL/JSON catalog OIDs (stable; nodes/parsenodes.h transforms reference these).
const JSONOID: Oid = 114;
/// `F_TO_JSON` (utils/fmgroids.h).
const F_TO_JSON: Oid = 3176;
/// `F_TO_JSONB` (utils/fmgroids.h).
const F_TO_JSONB: Oid = 3787;
/// `F_CONVERT_FROM` (utils/fmgroids.h).
const F_CONVERT_FROM: Oid = 1714;
/// `TYPCATEGORY_STRING` (catalog/pg_type.h).
const TYPCATEGORY_STRING: u8 = b'S';
/// `TYPTYPE_PSEUDO` (catalog/pg_type.h).
const TYPTYPE_PSEUDO: u8 = b'p';
use backend_parser_parse_target::FigureColname;
use backend_utils_adt_xml::map_sql_identifier_to_xml_name;
use types_tuple::heaptuple::MaxTupleAttributeNumber;

use backend_optimizer_util_vars::var::contain_vars_of_level;

use types_nodes::nodes::{self, ntag, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{
    Aggref, ArrayExpr, BoolTestType, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr,
    CoercionForm, CollateExpr, CurrentOfExpr, Expr, JsonConstructorType, JsonEncoding, JsonFormat,
    JsonFormatType, JsonReturning, JsonValueExpr as CookedJsonValueExpr, JsonValueType,
    MergeSupportFunc, MinMaxExpr, MinMaxOp,
    NamedArgExpr, NullTest, NullTestType, OpExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp,
    SubscriptingRef, WindowFunc, AND_EXPR, NOT_EXPR, OR_EXPR,
    XmlExpr as CookedXmlExpr, XmlExprOp,
};
use types_nodes::rawnodes::{
    A_Const, A_Expr, A_Expr_Kind, A_ArrayExpr, A_Indices, A_Indirection, ColumnRef, CollateClause,
    FuncCall, MultiAssignRef, TypeCast,
};
use types_nodes::rawexprnodes::RowExpr as RawRowExpr;
use types_parsenodes::CoercionContext;

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::{
    make_bool_const, make_bool_expr, make_const, make_func_expr, make_json_constructor_expr,
    make_json_format, make_json_is_predicate, make_target_entry,
};
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

/// `bool Transform_null_equals = false;` (parse_expr.c) — the legacy MS-SQL-compat
/// GUC that rewrites `foo = NULL` into `foo IS NULL`. In C this is a plain global
/// owned by parse_expr.c and pointed at by the `transform_null_equals` entry in
/// `guc_tables.c` (`&Transform_null_equals`); the GUC engine reads it via
/// `*conf->variable` and writes it on `SET`. We mirror that with process-local
/// backing storage here (boot value `false`) plus C-named accessors, and install
/// them into the GUC engine's variable-accessor table from [`init_seams`] so the
/// engine reads/writes this exact cell. [`transformAExprOp`] reads the live value
/// through the GUC slot.
pub mod transform_null_equals_storage {
    use core::sync::atomic::{AtomicBool, Ordering};

    /// `bool Transform_null_equals = false;` backing storage. C keeps this as a
    /// plain process-global `bool`; this crate is `#![no_std]`, so the cell is an
    /// `AtomicBool` (boot value `false`) rather than a `thread_local!`.
    static TRANSFORM_NULL_EQUALS: AtomicBool = AtomicBool::new(false);

    /// Read `*conf->variable` for the `transform_null_equals` GUC.
    #[inline]
    pub fn get() -> bool {
        TRANSFORM_NULL_EQUALS.load(Ordering::Relaxed)
    }

    /// Write `*conf->variable` for the `transform_null_equals` GUC (used by the
    /// GUC engine on `SET`).
    #[inline]
    pub fn set(value: bool) {
        TRANSFORM_NULL_EQUALS.store(value, Ordering::Relaxed);
    }
}

// ===========================================================================
// Small helpers (the C `strVal` / `IsA` idioms).
// ===========================================================================

/// `strVal(node)` — the string contents of a boxed `String` value node.
fn str_val(node: &nodes::NodePtr<'_>) -> Option<String> {
    match node.as_string() {
        Some(s) => Some(String::from(s.sval.as_str())),
        None => None,
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

    let result: Expr = match expr.node_tag() {
        ntag::T_ColumnRef => transformColumnRef(pstate, expr.into_columnref().unwrap())?,
        ntag::T_ParamRef => transformParamRef(pstate, &expr.into_paramref().unwrap())?,

        // T_A_Const → make_const(pstate, (A_Const *) expr).
        ntag::T_A_Const => transform_a_const(pstate, expr.into_a_const().unwrap())?,

        ntag::T_A_Indirection => transformIndirection(pstate, expr.into_a_indirection().unwrap())?,

        // transformArrayExpr(pstate, a, InvalidOid, InvalidOid, -1).
        ntag::T_A_ArrayExpr => transformArrayExpr(pstate, expr.into_a_arrayexpr().unwrap(), InvalidOid, InvalidOid, -1)?,

        ntag::T_TypeCast => transformTypeCast(pstate, expr)?,

        ntag::T_CollateClause => transformCollateClause(pstate, expr.into_collateclause().unwrap())?,

        ntag::T_A_Expr => {
            let a = expr.into_a_expr().unwrap();
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

        // T_BoolExpr → transformBoolExpr(pstate, (BoolExpr *) expr). The raw
        // grammar emits a `Node::BoolExpr` carrying untransformed `NodePtr`
        // children; we transform+coerce each and rebuild a cooked BoolExpr.
        ntag::T_BoolExpr => transformBoolExpr(pstate, expr.into_boolexpr().unwrap())?,

        // T_NullTest → transform n->arg, set argisrow, return the cooked node.
        // The raw grammar emits a `Node::NullTest` carrying an untransformed
        // `NodePtr` arg (distinct from an already-analyzed `Expr::NullTest`).
        ntag::T_NullTest => transformNullTestRaw(pstate, expr.into_nulltest().unwrap())?,

        // T_BooleanTest → transformBooleanTest(pstate, (BooleanTest *) expr).
        ntag::T_BooleanTest => transformBooleanTestRaw(pstate, expr.into_booleantest().unwrap())?,

        // T_XmlExpr → transformXmlExpr(pstate, (XmlExpr *) expr).
        ntag::T_XmlExpr => transformXmlExpr(pstate, expr.into_xmlexpr().unwrap())?,

        // T_XmlSerialize → transformXmlSerialize(pstate, (XmlSerialize *) expr).
        ntag::T_XmlSerialize => transformXmlSerialize(pstate, expr.into_xmlserialize().unwrap())?,

        ntag::T_FuncCall => transformFuncCall(pstate, expr.into_funccall().unwrap())?,
        ntag::T_MultiAssignRef => transformMultiAssignRef(pstate, expr.into_multiassignref().unwrap())?,

        // T_GroupingFunc → transformGroupingFunc(pstate, (GroupingFunc *) expr)
        // (parse_expr.c). The grammar emits GROUPING(...) as the *raw*
        // `Node::GroupingFunc` (rawexprnodes); route it to the parse_agg seam,
        // which transforms the raw arg list and returns the analyzed
        // `Expr::GroupingFunc`.
        ntag::T_GroupingFunc => seam_transform_grouping_func(pstate, expr)?,

        // T_SubLink → transformSubLink(pstate, (SubLink *) expr).
        ntag::T_SubLink => transformSubLink(pstate, expr.into_sublink().unwrap())?,

        // T_RowExpr → transformRowExpr(pstate, (RowExpr *) expr, false). The raw
        // grammar emits ROW(...) as `Node::RowExpr` carrying raw field nodes.
        ntag::T_RowExpr => transformRowExpr(pstate, expr.into_rowexpr().unwrap(), false)?,

        // T_CaseExpr / T_CoalesceExpr / T_MinMaxExpr — the raw-grammar nodes
        // (rawexprnodes, NodePtr children) the parser emits for CASE / COALESCE /
        // GREATEST / LEAST.
        ntag::T_CaseExpr => transformCaseExpr(pstate, expr.into_caseexpr().unwrap())?,
        ntag::T_CoalesceExpr => transformCoalesceExpr(pstate, expr.into_coalesceexpr().unwrap())?,
        ntag::T_MinMaxExpr => transformMinMaxExpr(pstate, expr.into_minmaxexpr().unwrap())?,

        // T_SQLValueFunction → transformSQLValueFunction(pstate, (SQLValueFunction *) expr).
        // The grammar emits CURRENT_DATE / CURRENT_USER / … as the raw
        // `Node::SQLValueFunction`; lift it into the primnodes form (its result
        // type/typmod are filled in by `transformSQLValueFunction`).
        ntag::T_SQLValueFunction => {
            let raw = expr.into_sqlvaluefunction().unwrap();
            let svf = SQLValueFunction {
                op: raw.op,
                r#type: raw.type_,
                typmod: raw.typmod,
                location: raw.location,
            };
            transformSQLValueFunction(pstate, svf)?
        }

        // T_NamedArgExpr → na->arg = transformExprRecurse(...); result = na.
        // The raw grammar emits `name => value` named function arguments as a
        // top-level `Node::NamedArgExpr` carrying an untransformed arg.
        ntag::T_NamedArgExpr => transformNamedArgExprRaw(pstate, expr.into_namedargexpr().unwrap())?,

        // SQL/JSON constructor / predicate raw-grammar nodes (parse_expr.c).
        ntag::T_JsonObjectConstructor => {
            transformJsonObjectConstructor(pstate, expr.into_jsonobjectconstructor().unwrap())?
        }
        ntag::T_JsonArrayConstructor => {
            transformJsonArrayConstructor(pstate, expr.into_jsonarrayconstructor().unwrap())?
        }
        ntag::T_JsonScalarExpr => {
            transformJsonScalarExpr(pstate, expr.into_jsonscalarexpr().unwrap())?
        }
        ntag::T_JsonSerializeExpr => {
            transformJsonSerializeExpr(pstate, expr.into_jsonserializeexpr().unwrap())?
        }
        ntag::T_JsonParseExpr => {
            transformJsonParseExpr(pstate, expr.into_jsonparseexpr().unwrap())?
        }
        ntag::T_JsonObjectAgg => {
            transformJsonObjectAgg(pstate, expr.into_jsonobjectagg().unwrap())?
        }
        ntag::T_JsonArrayAgg => {
            transformJsonArrayAgg(pstate, expr.into_jsonarrayagg().unwrap())?
        }
        ntag::T_JsonIsPredicate => {
            transformJsonIsPredicate(pstate, expr.into_jsonispredicate().unwrap())?
        }

        // Expr-carried nodes that reach the dispatcher untransformed-or-recursed.
        // DEFAULT must have been processed by the caller (handled in the
        // `Node::Expr(SetToDefault)` arm of `transform_expr_node`).
        other => {
            if expr.is_expr() {
                transform_expr_node(pstate, expr.into_expr().unwrap())?
            } else {
                // The C default raises elog(ERROR, "unrecognized node type: %d").
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg(alloc::format!("unrecognized node type: {}", other.0))
                    .into_error());
            }
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
        // A raw-grammar BoolExpr reaches the dispatcher as the `Node::BoolExpr`
        // arm above (the C `T_BoolExpr` case). An already-analyzed
        // `Expr::BoolExpr` re-entering transformExprRecurse would be a bug (the
        // C never re-transforms an analyzed BoolExpr).
        Expr::BoolExpr(_) => {
            return Err(PgError::error(
                "transformExprRecurse: unexpected already-analyzed BoolExpr",
            ))
        }
        Expr::GroupingFunc(_) => {
            seam_transform_grouping_func(pstate, Node::mk_expr(aexpr_clone_ctx(pstate), e))
        }
        Expr::MergeSupportFunc(f) => transformMergeSupportFunc(pstate, f),

        Expr::NamedArgExpr(mut na) => {
            // na->arg = transformExprRecurse(pstate, na->arg); result = expr.
            let arg = na
                .arg
                .take()
                .map(|b| expr_to_node(aexpr_clone_ctx(pstate), *b))
                .transpose()?;
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
        // A raw-grammar CaseExpr / CoalesceExpr / MinMaxExpr / RowExpr reaches
        // the dispatcher as the `Node::CaseExpr` / `Node::CoalesceExpr` /
        // `Node::MinMaxExpr` / `Node::RowExpr` arms above (the C `T_CaseExpr`
        // etc cases); an already-analyzed `Expr::*` re-entering
        // transformExprRecurse would be a bug (the C never re-transforms an
        // analyzed node).
        Expr::CaseExpr(_) => Err(PgError::error(
            "transformExprRecurse: unexpected already-analyzed CaseExpr",
        )),
        Expr::RowExpr(_) => Err(PgError::error(
            "transformExprRecurse: unexpected already-analyzed RowExpr",
        )),
        Expr::CoalesceExpr(_) => Err(PgError::error(
            "transformExprRecurse: unexpected already-analyzed CoalesceExpr",
        )),
        Expr::MinMaxExpr(_) => Err(PgError::error(
            "transformExprRecurse: unexpected already-analyzed MinMaxExpr",
        )),
        Expr::SQLValueFunction(svf) => transformSQLValueFunction(pstate, svf),
        // A raw-grammar XmlExpr reaches the dispatcher as the `Node::XmlExpr`
        // arm above (the C `T_XmlExpr` case); an already-analyzed `Expr::XmlExpr`
        // re-entering transformExprRecurse would be a bug.
        Expr::XmlExpr(_) => Err(PgError::error(
            "transformExprRecurse: unexpected already-analyzed XmlExpr",
        )),

        Expr::NullTest(mut n) => {
            // n->arg = transformExprRecurse(...); argisrow from arg's type.
            let arg = n
                .arg
                .take()
                .map(|b| expr_to_node(aexpr_clone_ctx(pstate), *b))
                .transpose()?;
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

        // An already-analyzed JsonIsPredicate must not be re-transformed (the raw
        // grammar form arrives as `Node::JsonIsPredicate` and is handled in
        // `transformExprRecurse`'s `T_JsonIsPredicate` arm).
        Expr::JsonIsPredicate(_) => Ok(e),

        other => Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!(
                "unrecognized node type: {}",
                Node::mk_expr(aexpr_clone_ctx(pstate), other).node_tag().0
            ))
            .into_error()),
    }
}

/// Wrap a typed `Expr` back into a raw `Node` for re-entry into
/// [`transformExprRecurse`] (the C casts `(Node *) expr` freely).
///
/// The `mcx` arg threads the allocation context through the constructor (routed
/// via `Node::mk_expr`) so this construction site is ready for the node-opaque
/// flip (§6 `value_no_mcx` sub-sweep); today `mk_expr` ignores `mcx` so this is
/// behavior-preserving.
fn expr_to_node<'mcx>(mcx: mcx::Mcx<'mcx>, e: Expr) -> PgResult<Node<'mcx>> {
    Ok(Node::mk_expr(mcx, e))
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
    arg.and_then(|n| n.as_a_const()).is_some_and(|con| con.isnull)
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
    // is on (default off). The live GUC value is read through the engine slot,
    // which the GUC engine writes on `SET transform_null_equals = on`.
    let is_eq_name = name.len() == 1 && name.iter().next().and_then(str_val).as_deref() == Some("=");
    let either_null = exprIsNullConstant(lexpr.as_ref()) || exprIsNullConstant(rexpr.as_ref());
    let neither_casetest = !is_casetestexpr(lexpr.as_ref()) && !is_casetestexpr(rexpr.as_ref());

    if backend_utils_misc_guc_tables::vars::Transform_null_equals.read()
        && is_eq_name
        && either_null
        && neither_casetest
    {
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
        let mut s = rexpr
            .unwrap()
            .into_sublink()
            .unwrap_or_else(|| unreachable!("is_expr_sublink guard"));
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

    let rexpr_list = {
        let other = boxed_node(rexpr);
        let other_tag = other.as_ref().map(|n| n.node_tag().0);
        match other.and_then(|n| n.into_list()) {
            Some(items) => items,
            // The grammar always wraps the IN value-list as a List node.
            None => {
                return Err(PgError::error(alloc::format!(
                    "transformAExprIn: expected a List rexpr, got {:?}",
                    other_tag
                )))
            }
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
        if contain_vars_of_level(&Node::mk_expr(aexpr_clone_ctx(pstate), rexpr.clone()), 0) {
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
    let str_node = Node::mk_string(mcx, types_nodes::value::StringNode {
        sval: mcx::PgString::from_str_in(op, mcx)?,
    });
    name.push(mcx::alloc_in(mcx, str_node)?);
    Ok(Node::mk_a_expr(mcx, A_Expr {
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
    let mut args = {
        let other = boxed_node(rexpr);
        let other_tag = other.as_ref().map(|n| n.node_tag().0);
        match other.and_then(|n| n.into_list()) {
            Some(items) => items,
            None => {
                return Err(PgError::error(alloc::format!(
                    "transformAExprBetween: expected a 2-element List rexpr, got {:?}",
                    other_tag
                )))
            }
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
    a: types_nodes::rawexprnodes::BoolExpr<'mcx>,
) -> PgResult<Expr> {
    let opname = match a.boolop {
        AND_EXPR => "AND",
        OR_EXPR => "OR",
        NOT_EXPR => "NOT",
    };

    let location = a.location;
    let mut args: Vec<Expr> = Vec::with_capacity(a.args.len());
    for arg in a.args {
        // `Node *arg = (Node *) lfirst(lc);` — the raw child node, moved out.
        let arg = transformExprRecurse(pstate, boxed_node(Some(arg)))?
            .ok_or_else(|| PgError::error("transformBoolExpr: BoolExpr argument is NULL"))?;
        let arg = coerce::coerce_to_boolean::call(pstate, arg, opname)?;
        args.push(arg);
    }

    Ok(make_bool_expr(a.boolop, args, location))
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
    c: types_nodes::rawexprnodes::CoalesceExpr<'mcx>,
) -> PgResult<Expr> {
    let last_srf = clone_last_srf(pstate);
    let location = c.location;

    let mut newargs: Vec<Expr> = Vec::with_capacity(c.args.len());
    for e in c.args {
        let newe = transformExprRecurse(pstate, boxed_node(Some(e)))?
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
    m: types_nodes::rawexprnodes::MinMaxExpr<'mcx>,
) -> PgResult<Expr> {
    let funcname = if m.op == MinMaxOp::IS_GREATEST {
        "GREATEST"
    } else {
        "LEAST"
    };
    let location = m.location;

    let mut newargs: Vec<Expr> = Vec::with_capacity(m.args.len());
    for e in m.args {
        let newe = transformExprRecurse(pstate, boxed_node(Some(e)))?
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
    b.as_expr()
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

    let arg = b
        .arg
        .take()
        .map(|x| expr_to_node(aexpr_clone_ctx(pstate), *x))
        .transpose()?;
    let arg = transformExprRecurse(pstate, arg)?
        .ok_or_else(|| PgError::error("transformBooleanTest: BooleanTest argument is NULL"))?;
    let arg = coerce::coerce_to_boolean::call(pstate, arg, clausename)?;
    b.arg = Some(Box::new(arg));
    Ok(Expr::BooleanTest(b))
}

/// `case T_BooleanTest:` in `transformExprRecurse` (parse_expr.c:297). The raw
/// grammar emits a `Node::BooleanTest` carrying an untransformed `NodePtr` arg
/// (`rawexprnodes::BooleanTest`); transform/coerce the arg and rebuild the
/// cooked `primnodes::BooleanTest`. Same body as [`transformBooleanTest`], over
/// the raw node shape.
fn transformBooleanTestRaw<'mcx>(
    pstate: &mut ParseState<'mcx>,
    b: types_nodes::rawexprnodes::BooleanTest<'mcx>,
) -> PgResult<Expr> {
    let clausename = match b.booltesttype {
        BoolTestType::IS_TRUE => "IS TRUE",
        BoolTestType::IS_NOT_TRUE => "IS NOT TRUE",
        BoolTestType::IS_FALSE => "IS FALSE",
        BoolTestType::IS_NOT_FALSE => "IS NOT FALSE",
        BoolTestType::IS_UNKNOWN => "IS UNKNOWN",
        BoolTestType::IS_NOT_UNKNOWN => "IS NOT UNKNOWN",
    };

    // b->arg = (Expr *) transformExprRecurse(pstate, (Node *) b->arg);
    let arg = transformExprRecurse(pstate, boxed_node(b.arg))?
        .ok_or_else(|| PgError::error("transformBooleanTest: BooleanTest argument is NULL"))?;
    // b->arg = (Expr *) coerce_to_boolean(pstate, (Node *) b->arg, clausename);
    let arg = coerce::coerce_to_boolean::call(pstate, arg, clausename)?;
    Ok(Expr::BooleanTest(BooleanTest {
        arg: Some(Box::new(arg)),
        booltesttype: b.booltesttype,
        location: b.location,
    }))
}

/// `case T_NullTest:` in `transformExprRecurse` (parse_expr.c:286). The raw
/// grammar emits a `Node::NullTest` carrying an untransformed `NodePtr` arg
/// (`rawexprnodes::NullTest`); transform the arg (no coercion — the argument can
/// be any type), set `argisrow` from the arg's type, and rebuild the cooked
/// `primnodes::NullTest`.
fn transformNullTestRaw<'mcx>(
    pstate: &mut ParseState<'mcx>,
    n: types_nodes::rawexprnodes::NullTest<'mcx>,
) -> PgResult<Expr> {
    // n->arg = (Expr *) transformExprRecurse(pstate, (Node *) n->arg);
    let arg = transformExprRecurse(pstate, boxed_node(n.arg))?;
    // n->argisrow = type_is_rowtype(exprType((Node *) n->arg));
    let argisrow = lsyscache::type_is_rowtype::call(expr_type(arg.as_ref())?)?;
    Ok(Expr::NullTest(NullTest {
        arg: arg.map(Box::new),
        nulltesttype: n.nulltesttype,
        argisrow,
        location: n.location,
    }))
}

/// C: `case T_NamedArgExpr` (parse_expr.c:244). Transform the inner argument
/// and return the (now cooked) NamedArgExpr unchanged otherwise.
fn transformNamedArgExprRaw<'mcx>(
    pstate: &mut ParseState<'mcx>,
    na: types_nodes::rawexprnodes::NamedArgExpr<'mcx>,
) -> PgResult<Expr> {
    // na->arg = (Expr *) transformExprRecurse(pstate, (Node *) na->arg);
    let arg = transformExprRecurse(pstate, boxed_node(na.arg))?;
    Ok(Expr::NamedArgExpr(NamedArgExpr {
        arg: arg.map(Box::new),
        name: na.name.as_ref().map(|s| String::from(s.as_str())),
        argnumber: na.argnumber,
        location: na.location,
    }))
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
        let other = mcx::PgBox::into_inner(n);
        let other_tag = other.node_tag().0;
        match other.into_string() {
            Some(s) => collname_pn.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(String::from(s.sval.as_str())),
                },
            )),
            None => {
                return Err(PgError::error(alloc::format!(
                    "transformCollateClause: collname element is not a String value node (tag {})",
                    other_tag
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
    c: types_nodes::rawexprnodes::CaseExpr<'mcx>,
) -> PgResult<Expr> {
    let last_srf = clone_last_srf(pstate);
    let case_location = c.location;

    // Transform the test expression, if any.
    let arg = boxed_node(c.arg);
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
    for w_ptr in c.args {
        // Each list element is a raw `Node::CaseWhen` (the grammar's
        // `makeNode(CaseWhen)`); pull out its raw `expr`/`result` children.
        let w_node = mcx::PgBox::into_inner(w_ptr);
        let w_tag = w_node.node_tag();
        let w = match w_node.into_casewhen() {
            Some(w) => w,
            None => {
                return Err(PgError::error(alloc::format!(
                    "transformCaseExpr: CASE arm is not a CaseWhen: {}",
                    w_tag.0
                )))
            }
        };
        let when_location = w.location;
        // Optional CASE shorthand (form 2): expand `placeholder = warg`.
        // The C builds `makeSimpleA_Expr(AEXPR_OP, "=", placeholder, warg)` then
        // recurses — which transforms `warg` (the `placeholder` `CaseTestExpr`
        // passes through unchanged) and builds the `=` `OpExpr`. The owned model
        // reproduces this directly: transform `warg`, then `make_op("=",
        // CaseTestExpr, warg_t)` — equivalent to recursing on the synthesized
        // A_Expr but without a raw-pointer-backed transient `List` opname.
        let cond = if let Some(ph) = &placeholder {
            let warg = boxed_node(w.expr);
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
            let cond = boxed_node(w.expr);
            transformExprRecurse(pstate, cond)?
                .ok_or_else(|| PgError::error("transformCaseExpr: CASE/WHEN condition is NULL"))?
        };
        let cond = coerce::coerce_to_boolean::call(pstate, cond, "CASE/WHEN")?;

        let wresult = boxed_node(w.result);
        let wresult = transformExprRecurse(pstate, wresult)?
            .ok_or_else(|| PgError::error("transformCaseExpr: CASE/THEN result is NULL"))?;

        // C keeps the same node pointer in both `resultexprs` (for common-type
        // selection) and `neww->result`; the owned model needs a separate value,
        // so deep-copy via `clone_in` (the derived `.clone()` panics on embedded
        // owned sub-trees such as a SubLink CASE result — `ARRAY(SELECT ...)`).
        resultexprs.push(wresult.clone_in(aexpr_clone_ctx(pstate))?);
        newargs.push(CaseWhen {
            expr: Some(Box::new(cond)),
            result: Some(Box::new(wresult)),
            // neww->location = w->location;
            location: when_location,
        });
    }

    // Transform the default clause; NULL → untyped NULL A_Const.
    let defresult_node: Node<'mcx> = match boxed_node(c.defresult) {
        Some(d) => d,
        None => Node::mk_a_const(
            aexpr_clone_ctx(pstate),
            A_Const {
                val: None,
                isnull: true,
                location: -1,
            },
        ),
    };
    let mut defresult = transformExprRecurse(pstate, Some(defresult_node))?
        .ok_or_else(|| PgError::error("transformCaseExpr: CASE default result is NULL"))?;

    // Common type: default result first (lcons), then WHEN results.
    let mut common_inputs: Vec<Expr> = Vec::with_capacity(resultexprs.len() + 1);
    // Deep-copy (not derived `.clone()`, which panics on a SubLink default
    // result) — C reuses the same pointer; the owned model needs its own value.
    common_inputs.push(defresult.clone_in(aexpr_clone_ctx(pstate))?);
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
    let Some(tc) = tc.into_typecast() else {
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
    let expr = if arg.is_a_arrayexpr() {
        // getBaseTypeAndTypmod(targetType, &targetTypmod) — resolve a domain
        // over array to its base array type/typmod (identity for non-domains).
        let (target_base_type, target_base_typmod) =
            base_type_and_typmod(target_type, target_typmod)?;
        let element_type =
            lsyscache::get_element_type::call(target_base_type)?.unwrap_or(InvalidOid);
        if OidIsValid(element_type) {
            let Some(a) = arg.into_a_arrayexpr() else {
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
        let bridged: types_parsenodes::Node = match tm.node_tag() {
            // `IsA(tm, A_Const)`: the literal rides in `A_Const.val`.
            nodes::ntag::T_A_Const => {
                let ac = tm.expect_a_const();
                if let Some(i) = ac.val.as_deref().and_then(|v| v.as_integer()) {
                    types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })
                } else if let Some(f) = ac.val.as_deref().and_then(|v| v.as_float()) {
                    types_parsenodes::Node::Float(types_parsenodes::Float {
                        fval: Some(String::from(f.fval.as_str())),
                    })
                } else if let Some(s) = ac.val.as_deref().and_then(|v| v.as_string()) {
                    types_parsenodes::Node::String(types_parsenodes::StringNode {
                        sval: Some(String::from(s.sval.as_str())),
                    })
                } else if let Some(b) = ac.val.as_deref().and_then(|v| v.as_boolean()) {
                    types_parsenodes::Node::Boolean(types_parsenodes::Boolean { boolval: b.boolval })
                } else if let Some(b) = ac.val.as_deref().and_then(|v| v.as_bitstring()) {
                    types_parsenodes::Node::BitString(types_parsenodes::BitString {
                        bsval: Some(String::from(b.bsval.as_str())),
                    })
                } else {
                    // SQL NULL constant or any other val: not a simple constant;
                    // carry an A_Star so the owner rejects it with the C error.
                    types_parsenodes::Node::A_Star
                }
            }
            // `IsA(tm, ColumnRef)` with a single String field is an identifier
            // typmod (the trimmed parser-node model carries it as a bare String).
            nodes::ntag::T_ColumnRef => {
                let cr = tm.expect_columnref();
                if cr.fields.len() == 1 {
                    if let Some(s) = cr.fields[0].as_string() {
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
        if let Some(s) = n.as_string() {
            names.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(String::from(s.sval.as_str())),
                },
            ));
        } else {
            return Err(PgError::error(alloc::format!(
                "transformTypeCast: TypeName.names element is not a String node (tag {})",
                n.node_tag().0
            )));
        }
    }
    let mut array_bounds: Vec<types_parsenodes::Node> =
        Vec::with_capacity(tn.arrayBounds.len());
    for n in tn.arrayBounds.iter() {
        // typeNameTypeId only tests `arrayBounds != NIL` (the bound values are
        // ignored by the lookup); carry the Integer bound through.
        if let Some(i) = n.as_integer() {
            array_bounds.push(types_parsenodes::Node::Integer(
                types_parsenodes::Integer { ival: i.ival },
            ));
        } else {
            array_bounds.push(types_parsenodes::Node::Integer(
                types_parsenodes::Integer { ival: -1 },
            ));
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
        let newe = if e.is_a_arrayexpr() {
            let sub = e.into_a_arrayexpr().unwrap();
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
/// Clone the inner `Expr` of `pstate->p_last_srf` (the most recent
/// set-returning function/operator). Public so `analyze.c`'s `transformCallStmt`
/// can pass it to `ParseFuncOrColumn` (C reads `pstate->p_last_srf` directly).
pub fn last_srf_expr(pstate: &ParseState<'_>) -> Option<Expr> {
    pstate
        .p_last_srf
        .as_ref()
        .and_then(|b| b.as_expr().map(|e| e.clone()))
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
    n.is_some_and(|n| n.is_casetestexpr())
}
/// `IsA(node, RowExpr)` over a *raw-grammar* node — the grammar emits a raw
/// ROW(...) as [`Node::RowExpr`]. The transformAExprOp arms inspect the
/// untransformed `a->lexpr`/`a->rexpr`.
fn is_rowexpr(n: Option<&Node<'_>>) -> bool {
    n.is_some_and(|n| n.is_rowexpr())
}
/// `rexpr IsA SubLink && ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK`
/// (parse_expr.c:954-955): only a plain expression sublink may be rewritten
/// into a ROWCOMPARE sublink. The raw-grammar SubLink is [`Node::SubLink`].
fn is_expr_sublink(n: Option<&Node<'_>>) -> bool {
    n.and_then(|n| n.as_sublink())
        .is_some_and(|s| s.sub_link_type == types_nodes::primnodes::SubLinkType::Expr)
}
fn is_rowexpr_expr_opt(e: Option<&Expr>) -> bool {
    matches!(e, Some(Expr::RowExpr(_)))
}

/// `(Node *) e` for a typed `Expr` consumed by the SubLink-rewrite path.
fn node_into_expr(n: Node<'_>) -> Option<Expr> {
    n.into_expr()
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

    // Give the PreParseColumnRefHook, if any, first shot.
    //
    //   if (pstate->p_pre_columnref_hook != NULL)
    //   {
    //       node = pstate->p_pre_columnref_hook(pstate, cref);
    //       if (node != NULL)
    //           return node;
    //   }
    //
    // The owned hook is a real `fn` pointer stored on the ParseState by its
    // installer (e.g. domainAddCheckConstraint's `replace_domain_constraint_value`,
    // which reads the prepared `CoerceToDomainValue` out of
    // `pstate.p_ref_hook_state`). A non-NULL result replaces the reference; a
    // `None` falls through to the standard resolution below.
    if let Some(hook) = pstate.p_pre_columnref_hook {
        if let Some(node) = hook(pstate, &cref)? {
            let node: Node<'mcx> = mcx::PgBox::into_inner(node);
            return node_into_expr(node).ok_or_else(|| {
                PgError::error("transformColumnRef: pre-columnref-hook node is not an expression")
            });
        }
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
                if cref.fields[1].is_a_star() {
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

                if cref.fields[2].is_a_star() {
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

                if cref.fields[3].is_a_star() {
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
        if let Some(s) = n.as_string() {
            out.push_str(s.sval.as_str());
        } else if n.is_a_star() {
            out.push('*');
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

    // Read the nsitem fields we need (cloning the RTE and names). The index is
    // relative to the ParseState `sublevels_up` levels up the parent chain (see
    // scanNSItemForColumn), so resolve the owning ParseState first.
    let (rte, p_rtindex, p_returning_type, names_is_eref, colnames_len, colnames) = {
        let owner = backend_parser_relation::nsitem_level(pstate, sublevels_up);
        let nsitem = &owner.p_namespace[nsitem_index];
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
        Ok(Node::mk_var(mcx, var))
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
        Ok(Node::mk_expr(mcx, Expr::RowExpr(RowExpr {
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
        let n_tag = n.node_tag();
        match n_tag {
            ntag::T_A_Indices => subscripts.push(n.into_a_indices().unwrap()),
            ntag::T_A_Star => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("row expansion via \"*\" is not supported here")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            ntag::T_String => {
                let s = n.expect_string();
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
            _ => {
                return Err(PgError::error(alloc::format!(
                    "transformIndirection: unexpected indirection node (tag {})",
                    n_tag.0
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
            let node = match sb.as_sortby() {
                Some(s) => s.node.as_deref().map(|n| n.clone_in(aexpr_clone_ctx(pstate))),
                None => None,
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
/// `&[PgString]` form `ParseFuncOrColumn` consumes. Public so `analyze.c`'s
/// `transformCallStmt` can build the procedure name list.
pub fn clone_namelist_pgstrings<'mcx>(
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
    r: RawRowExpr<'mcx>,
    allow_default: bool,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);
    let location = r.location;

    // Transform the field expressions. transformExpressionList expands any
    // "something.*" entries; the raw-grammar RowExpr carries its fields as a
    // raw node list (C: r->args), passed straight through.
    let expr_kind = pstate.p_expr_kind;
    let newargs_vec = backend_parser_parse_target::transformExpressionList(
        mcx,
        pstate,
        r.args,
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
        // raw-grammar RowExpr is `Node::RowExpr` (transformRowExpr consumes the
        // raw RowExpr, whose `args` are the raw field expressions).
        let is_expr_sublink = src
            .as_sublink()
            .is_some_and(|s| s.sub_link_type == SubLinkType::Expr);
        let is_rowexpr = src.is_rowexpr();

        if is_expr_sublink {
            let mut sublink = src
                .into_sublink()
                .unwrap_or_else(|| unreachable!("is_expr_sublink guard"));
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
            let rexpr = src.into_rowexpr().unwrap_or_else(|| unreachable!("is_rowexpr guard"));
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
            // qtree = castNode(Query, sublink->subselect);
            // tle = (TargetEntry *) list_nth(qtree->targetList, colno - 1);
            // The Param's type/typmod/collation/location come from the *colno-th*
            // output column of the sub-SELECT, not from the SubLink as a whole
            // (whose exprType is RECORDOID).
            let qtree = sublink
                .subselect
                .as_deref()
                .ok_or_else(|| PgError::error("MULTIEXPR SubLink has no subselect"))?;
            let coltle = qtree
                .targetList
                .get((colno - 1) as usize)
                .ok_or_else(|| PgError::error("transformMultiAssignRef: colno out of range"))?;
            let colexpr: Option<&Expr> = coltle.expr.as_deref();
            let param = Param {
                paramkind: ParamKind::PARAM_MULTIEXPR,
                paramid: (sublink.subLinkId << 16) | colno,
                paramtype: expr_type(colexpr)?,
                paramtypmod: expr_typmod(colexpr)?,
                paramcollid: expr_collation(colexpr)?,
                location: expr_location(colexpr)?,
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

/// The PostParseColumnRefHook leg of `transformColumnRef`. Selects the installed
/// hook from the active `pstate.p_ref_hook_state` arm — the real artifact the C
/// function pointer selects. The only post-columnref hook in the core backend is
/// `sql_fn_post_column_ref` (SQL-function-body parsing), installed by
/// `sql_fn_parser_setup` via the `SqlFunction` ref-hook arm. A non-`None` result
/// replaces the reference; otherwise the default `var` (and, when that is also
/// `None`, the "no translation found" error) stands.
fn seam_transform_post_columnref_hook<'mcx>(
    pstate: &mut ParseState<'mcx>,
    cref: ColumnRef<'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::parsestmt::ParseRefHookState;

    let result = match &pstate.p_ref_hook_state {
        ParseRefHookState::SqlFunction(pinfo) => {
            let pinfo = pinfo.clone();
            sql_fn_post_column_ref(pstate, &pinfo, &cref, node.as_ref())?
        }
        // No other post-columnref hook exists in the core backend; the active arm
        // installs no `p_post_columnref_hook` (C `== NULL`), so the default `var`
        // stands. (PL/pgSQL's variable resolution is done by the pre-columnref
        // hook above; in the default RESOLVE_ERROR variable_conflict mode the
        // post hook only raises an ambiguity error, which a no-range-table
        // expression never reaches.)
        ParseRefHookState::None
        | ParseRefHookState::FixedParams(_)
        | ParseRefHookState::VarParams(_)
        | ParseRefHookState::PlpgsqlExpr(_)
        | ParseRefHookState::DomainCheckValue(_) => None,
    };

    // The hook's result, if any, replaces the reference (C: `if (node != NULL)
    // return node;`). Otherwise the default resolution stands.
    if let Some(n) = result {
        return Ok(n);
    }
    if let Some(n) = node {
        return Ok(n);
    }

    // Throw error if no translation found. The hook didn't resolve it and there
    // is no default var: this is the same "no column" error the no-hook tail
    // raises. errorMissingColumn over the single-part name (the common SQL-fn
    // bareword case); multi-part refs that reach here likewise have no column.
    let mcx = aexpr_clone_ctx(pstate);
    let colname = cref
        .fields
        .last()
        .and_then(str_val)
        .unwrap_or_default();
    let relname = if cref.fields.len() >= 2 {
        cref.fields.first().and_then(str_val)
    } else {
        None
    };
    errorMissingColumn(mcx, pstate, relname.as_deref(), &colname, cref.location)?;
    unreachable!()
}

/// `sql_fn_post_column_ref` (executor/functions.c:353) — parser callback for
/// `ColumnRef`s when parsing a SQL-function body. Resolves a bareword (or
/// `fname.param`, `fname.param.field`, `param.field`, with optional trailing `.*`)
/// that names a function parameter to the corresponding `$n` `Param`. Never
/// overrides a real table-column reference (returns `None` when `var` is set).
fn sql_fn_post_column_ref<'mcx>(
    pstate: &mut ParseState<'mcx>,
    pinfo: &types_nodes::parsestmt::SqlFnParseInfo,
    cref: &ColumnRef<'mcx>,
    var: Option<&Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    // Never override a table-column reference.
    if var.is_some() {
        return Ok(None);
    }

    // nnames = list_length(cref->fields);  if (nnames > 3) return NULL;
    let mut nnames = cref.fields.len();
    if nnames > 3 {
        return Ok(None);
    }

    // if (IsA(llast(cref->fields), A_Star)) nnames--;
    if cref
        .fields
        .last()
        .map(|f| f.is_a_star())
        .unwrap_or(false)
    {
        nnames -= 1;
    }

    let name1 = str_val(&cref.fields[0])
        .ok_or_else(|| PgError::error("sql_fn_post_column_ref: field is not a String"))?;
    let name2 = if nnames > 1 {
        Some(
            str_val(&cref.fields[1])
                .ok_or_else(|| PgError::error("sql_fn_post_column_ref: field is not a String"))?,
        )
    } else {
        None
    };

    // The resolved Param (if any) and whether a trailing subfield remains.
    let (param, has_subfield): (Option<types_nodes::primnodes::Param>, bool) = if nnames == 3 {
        // Three-part name: first part must match the function name; second part
        // is the parameter, third is a field reference.
        if name1 != pinfo.fname {
            return Ok(None);
        }
        let p = sql_fn_resolve_param_name(pinfo, name2.as_deref().unwrap(), cref.location)?;
        (p, true)
    } else if nnames == 2 && name1 == pinfo.fname {
        // Two-part name with first part matching function name: try the second
        // part as a parameter name (no subfield), else the first part as a
        // parameter name with the second as a subfield.
        let p = sql_fn_resolve_param_name(pinfo, name2.as_deref().unwrap(), cref.location)?;
        if p.is_some() {
            (p, false)
        } else {
            (
                sql_fn_resolve_param_name(pinfo, &name1, cref.location)?,
                true,
            )
        }
    } else {
        // Single name, or parameter name followed by subfield.
        let p = sql_fn_resolve_param_name(pinfo, &name1, cref.location)?;
        (p, nnames > 1)
    };

    let Some(param) = param else {
        return Ok(None); // No match.
    };

    if has_subfield {
        // Reference to a field of a composite parameter. `subfield` is the
        // second field for the 2-name `param.field` case, or the third for the
        // 3-name `fname.param.field` case. ParseFuncOrColumn resolves the field
        // selection; if it can't, it returns NULL and we fail back at the caller.
        let subfield_idx = if nnames == 3 { 2 } else { 1 };
        let subfield = str_val(&cref.fields[subfield_idx])
            .ok_or_else(|| PgError::error("sql_fn_post_column_ref: subfield is not a String"))?;
        let mcx = aexpr_clone_ctx(pstate);
        let funcname = [mcx::PgString::from_str_in(&subfield, mcx)?];
        let last_srf = last_srf_expr(pstate);
        let res = backend_parser_func::ParseFuncOrColumn(
            pstate,
            &funcname,
            vec![Expr::Param(param)],
            last_srf.as_ref(),
            None,
            false,
            cref.location,
        )?;
        return Ok(res.map(Node::Expr));
    }

    Ok(Some(Node::mk_expr(
        aexpr_clone_ctx(pstate),
        Expr::Param(param),
    )))
}

/// `sql_fn_make_param` (executor/functions.c:485) — construct a `PARAM_EXTERN`
/// `Param` node for the given 1-based parameter number, using the function's
/// argument types and (optionally) its input collation.
fn sql_fn_make_param(
    pinfo: &types_nodes::parsestmt::SqlFnParseInfo,
    paramno: i32,
    location: i32,
) -> PgResult<types_nodes::primnodes::Param> {
    let paramtype = pinfo.argtypes[(paramno - 1) as usize];
    let mut paramcollid = lsyscache::get_typcollation::call(paramtype)?;

    // A valid function input collation overrides the type-derived collation.
    if OidIsValid(pinfo.collation) && OidIsValid(paramcollid) {
        paramcollid = pinfo.collation;
    }

    Ok(types_nodes::primnodes::Param {
        paramkind: types_nodes::primnodes::PARAM_EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid,
        location,
    })
}

/// `sql_fn_resolve_param_name` (executor/functions.c:515) — search the function's
/// argument names for `paramname`; on a match, build the corresponding `Param`.
fn sql_fn_resolve_param_name(
    pinfo: &types_nodes::parsestmt::SqlFnParseInfo,
    paramname: &str,
    location: i32,
) -> PgResult<Option<types_nodes::primnodes::Param>> {
    let Some(argnames) = pinfo.argnames.as_ref() else {
        return Ok(None);
    };
    for (i, name) in argnames.iter().enumerate() {
        if i >= pinfo.argtypes.len() {
            break;
        }
        if name.as_deref() == Some(paramname) {
            return Ok(Some(sql_fn_make_param(pinfo, (i + 1) as i32, location)?));
        }
    }
    Ok(None)
}

/// `plpgsql_pre_column_ref(pstate, cref)` (pl_comp.c:1135) — the PL/pgSQL
/// expression `p_pre_columnref_hook`. A 1- or 2-element column reference that
/// names a PL/pgSQL variable (`var` or `block.var`) is resolved — ahead of any
/// table-column resolution, since a PL/pgSQL expression has no range table — to
/// a `PARAM_EXTERN` `Param` whose paramid is the variable's `dno + 1`, via the
/// pre-resolved namespace map in the [`ParseRefHookState::PlpgsqlExpr`] arm. The
/// referenced datum number is recorded so `setup_param_list` knows to bind it.
/// A name that does not resolve to a variable returns `None` (falls through to
/// the standard column resolution, which then errors if truly undefined — C
/// `resolve_column_ref` returning NULL).
fn plpgsql_pre_column_ref<'mcx>(
    pstate: &mut ParseState<'mcx>,
    cref: &types_nodes::rawnodes::ColumnRef<'mcx>,
) -> PgResult<Option<types_nodes::nodes::NodePtr<'mcx>>> {
    use types_nodes::parsestmt::ParseRefHookState;

    let ParseRefHookState::PlpgsqlExpr(state) = &pstate.p_ref_hook_state else {
        return Ok(None);
    };
    let state = state.clone();

    // Build the candidate down-cased lookup name(s). The plpgsql scanner already
    // down-cased identifiers in the namespace; match the same way. For a single
    // field `var`; for two fields `block.var` (and also try the bare `var`,
    // matching plpgsql_ns_lookup's two-name resolution).
    let parts: Vec<String> = cref
        .fields
        .iter()
        .filter_map(str_val)
        .collect();
    if parts.is_empty() || parts.len() > 3 {
        return Ok(None);
    }

    // Lookup keys: progressively shorter trailing suffixes of the dotted name,
    // longest first. This mirrors C `resolve_column_ref` + `plpgsql_ns_lookup`,
    // which strips a leading enclosing-block LABEL from a qualified reference:
    //   * `var`            -> scalar / whole-record bareword.
    //   * `rec.field`      -> a RECFIELD (the param map keys fields this way), or
    //                         `block.var` -> the scalar `var` (label stripped).
    //   * `label.rec.field`-> after stripping the leading block label, the
    //                         RECFIELD key is `rec.field` (the trailing 2 names).
    // The param map keys scalars by their bare name and record fields by
    // `rec.field`, so trying each trailing suffix from longest to shortest hits
    // the most-specific binding first (matching plpgsql_ns_lookup's preference
    // for a qualified match before the unqualified fallback).
    let info = {
        let names = &state.names;
        let mut found = None;
        for start in 0..parts.len() {
            let key = parts[start..].join(".").to_ascii_lowercase();
            if let Some(i) = names.get(&key) {
                found = Some(i.clone());
                break;
            }
        }
        found
    };

    let Some(info) = info else {
        return Ok(None);
    };

    // make_datum_param: PARAM_EXTERN with paramid = dno + 1.
    let mut paramcollid = info.collation;
    if !OidIsValid(paramcollid) && OidIsValid(state.input_collation) {
        paramcollid = state.input_collation;
    }
    let param = types_nodes::primnodes::Param {
        paramkind: types_nodes::primnodes::PARAM_EXTERN,
        paramid: info.dno + 1,
        paramtype: info.typeid,
        paramtypmod: info.typmod,
        paramcollid,
        location: cref.location,
    };

    // Record the referenced datum number (expr->paramnos) for setup_param_list.
    state.record_paramno(info.dno);

    let mcx = aexpr_clone_ctx(pstate);
    let node = Node::mk_expr(mcx, Expr::Param(param));
    Ok(Some(mcx::alloc_in(mcx, node)?))
}

/// Install the PL/pgSQL expression parser hooks on `pstate` (the
/// `plpgsql_parser_setup` body): the pre-columnref hook that resolves variable
/// references to Params, and the `PlpgsqlExpr` ref-hook state carrying the
/// pre-resolved namespace map. (The post-columnref / coerce-param hooks handle
/// the variable_conflict and unknown-coercion cases; for a PL/pgSQL expression
/// with no range table the pre-hook resolves every variable reference, so they
/// are not needed for the default RESOLVE_ERROR conflict mode.)
pub fn setup_parse_plpgsql_expr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    state: types_nodes::parsestmt::PlpgsqlExprParseState,
) {
    pstate.p_pre_columnref_hook = Some(plpgsql_pre_column_ref);
    pstate.p_ref_hook_state = types_nodes::parsestmt::ParseRefHookState::PlpgsqlExpr(state);
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
        // sql_fn_param_ref (functions.c:469): a `$n` valid for the function's
        // argument count resolves to a Param; an out-of-range number returns
        // NULL (falls to the generic error below).
        ParseRefHookState::SqlFunction(pinfo) => {
            let paramno = pref.number;
            if paramno <= 0 || paramno as usize > pinfo.argtypes.len() {
                None
            } else {
                let pinfo = pinfo.clone();
                Some(sql_fn_make_param(&pinfo, paramno, pref.location)?)
            }
        }
        // plpgsql_param_ref (pl_comp.c:1056): a `$n` ParamRef resolves through the
        // plpgsql namespace under the synthesized name `"$n"`. PL/pgSQL registers
        // every function argument in the namespace under BOTH its declared name
        // and `$1`/`$2`/… (pl_comp.c `add_parameter_name`), so a user-written
        // `$1` in a plpgsql expression resolves to the matching argument datum's
        // Param exactly like the bareword does — via make_datum_param (paramid =
        // dno + 1). A name not in the namespace returns NULL (the generic error
        // below). The owned pre-resolved `names` map carries the `"$n"` keys, so
        // look the synthesized name up there.
        ParseRefHookState::PlpgsqlExpr(state) => {
            let pname = alloc::format!("${}", pref.number);
            match state.names.get(&pname) {
                Some(info) => {
                    // make_datum_param: PARAM_EXTERN with paramid = dno + 1.
                    let mut paramcollid = info.collation;
                    if !OidIsValid(paramcollid) && OidIsValid(state.input_collation) {
                        paramcollid = state.input_collation;
                    }
                    let param = types_nodes::primnodes::Param {
                        paramkind: types_nodes::primnodes::PARAM_EXTERN,
                        paramid: info.dno + 1,
                        paramtype: info.typeid,
                        paramtypmod: info.typmod,
                        paramcollid,
                        location: pref.location,
                    };
                    // Record the referenced datum number (expr->paramnos).
                    state.record_paramno(info.dno);
                    Some(param)
                }
                None => None,
            }
        }
        // A domain CHECK parse state installs no paramref hook (C
        // `p_paramref_hook == NULL`): a `$n` reference falls to the generic
        // "there is no parameter $n" error below.
        ParseRefHookState::None | ParseRefHookState::DomainCheckValue(_) => None,
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
    let qtree = match qtree_node.as_ref().as_query() {
        Some(q) if q.commandType == types_nodes::nodes::CmdType::CMD_SELECT => q,
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
        // operName defaults to NIL; only the ALL/ANY/ROWCOMPARE arm below sets it.
        operName: Vec::new(),
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
            let str_node = Node::mk_string(mcx, types_nodes::value::StringNode {
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

        // C: sublink->operName = operName; — retain the (possibly defaulted)
        // operator name on the analyzed node so `_outSubLink`/`_readSubLink`
        // round-trips it in stored `_RETURN` rules (the analyzed carrier models
        // the `List *` of `String` as the lifetime-free `Vec<String>`).
        out.operName = oper_name
            .iter()
            .filter_map(|n| match &**n {
                Node::String(s) => Some(String::from(s.sval.as_str())),
                _ => None,
            })
            .collect();

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

/// `transformXmlExpr(pstate, x)` (parse_expr.c:2366) — analyze a raw-grammar
/// `XmlExpr` (XMLELEMENT/XMLFOREST/XMLCONCAT/XMLPARSE/XMLPI/XMLROOT/IS DOCUMENT),
/// transforming each named/positional argument and applying the per-op coercions.
fn transformXmlExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    x: types_nodes::rawexprnodes::XmlExpr<'mcx>,
) -> PgResult<Expr> {
    let types_nodes::rawexprnodes::XmlExpr {
        op,
        name,
        named_args,
        args,
        xmloption,
        location,
        ..
    } = x;

    let new_name: Option<String> = match name {
        Some(n) => Some(map_sql_identifier_to_xml_name(n.as_bytes(), false, false)?),
        None => None,
    };

    let mut new_named_args: Vec<Expr> = Vec::new();
    let mut new_arg_names: Vec<String> = Vec::new();

    // gram.y built the named args as a list of ResTarget. Transform each, and
    // break the names out as a separate list.
    for r_node in named_args.into_iter() {
        let r_node = mcx::PgBox::into_inner(r_node);
        let Some(r) = r_node.into_restarget() else {
            return Err(PgError::error(
                "transformXmlExpr: named_args element is not a ResTarget",
            ));
        };
        let r_name = r.name;
        let r_val = r.val;
        let r_location = r.location;

        // Keep a reference to the raw val for the ColumnRef/FigureColname path
        // before it is moved into the recursion.
        let val_node: Option<Node<'mcx>> = boxed_node(r_val);
        let is_columnref = val_node.as_ref().is_some_and(|n| n.is_columnref());
        // FigureColname needs the raw node; compute the colname before recursing.
        let figured = if is_columnref {
            FigureColname(val_node.as_ref())
        } else {
            None
        };

        let expr = transformExprRecurse(pstate, val_node)?
            .ok_or_else(|| PgError::error("transformXmlExpr: argument transformed to NULL"))?;

        let argname: String = if let Some(rn) = r_name {
            map_sql_identifier_to_xml_name(rn.as_bytes(), false, false)?
        } else if is_columnref {
            let colname = figured.ok_or_else(|| {
                PgError::error("transformXmlExpr: FigureColname returned no name")
            })?;
            map_sql_identifier_to_xml_name(colname.as_bytes(), true, false)?
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(if op == XmlExprOp::IS_XMLELEMENT {
                    "unnamed XML attribute value must be a column reference"
                } else {
                    "unnamed XML element value must be a column reference"
                })
                .errposition(parser_errposition(pstate, r_location))
                .into_error());
        };

        // reject duplicate argnames in XMLELEMENT only
        if op == XmlExprOp::IS_XMLELEMENT {
            for prev in new_arg_names.iter() {
                if *prev == argname {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "XML attribute name \"{}\" appears more than once",
                            argname
                        ))
                        .errposition(parser_errposition(pstate, r_location))
                        .into_error());
                }
            }
        }

        new_named_args.push(expr);
        new_arg_names.push(argname);
    }

    // The other arguments are of varying types depending on the function.
    let mut new_args: Vec<Expr> = Vec::new();
    for (i, e) in args.into_iter().enumerate() {
        let e = mcx::PgBox::into_inner(e);
        let newe = transformExprRecurse(pstate, Some(e))?
            .ok_or_else(|| PgError::error("transformXmlExpr: argument transformed to NULL"))?;
        let newe = match op {
            XmlExprOp::IS_XMLCONCAT => {
                coerce::coerce_to_specific_type::call(pstate, newe, XMLOID, "XMLCONCAT")?
            }
            XmlExprOp::IS_XMLELEMENT => newe, // no coercion necessary
            XmlExprOp::IS_XMLFOREST => {
                coerce::coerce_to_specific_type::call(pstate, newe, XMLOID, "XMLFOREST")?
            }
            XmlExprOp::IS_XMLPARSE => {
                if i == 0 {
                    coerce::coerce_to_specific_type::call(pstate, newe, TEXTOID, "XMLPARSE")?
                } else {
                    coerce::coerce_to_boolean::call(pstate, newe, "XMLPARSE")?
                }
            }
            XmlExprOp::IS_XMLPI => {
                coerce::coerce_to_specific_type::call(pstate, newe, TEXTOID, "XMLPI")?
            }
            XmlExprOp::IS_XMLROOT => {
                if i == 0 {
                    coerce::coerce_to_specific_type::call(pstate, newe, XMLOID, "XMLROOT")?
                } else if i == 1 {
                    coerce::coerce_to_specific_type::call(pstate, newe, TEXTOID, "XMLROOT")?
                } else {
                    coerce::coerce_to_specific_type::call(pstate, newe, INT4OID, "XMLROOT")?
                }
            }
            XmlExprOp::IS_XMLSERIALIZE => {
                // not handled here (Assert(false) in C)
                return Err(PgError::error(
                    "transformXmlExpr: IS_XMLSERIALIZE not handled here",
                ));
            }
            XmlExprOp::IS_DOCUMENT => {
                coerce::coerce_to_specific_type::call(pstate, newe, XMLOID, "IS DOCUMENT")?
            }
        };
        new_args.push(newe);
    }

    Ok(Expr::XmlExpr(CookedXmlExpr {
        op,
        name: new_name,
        named_args: new_named_args,
        arg_names: new_arg_names,
        args: new_args,
        xmloption,
        indent: false,
        r#type: XMLOID, // this just marks the node as transformed
        typmod: -1,
        location,
    }))
}

/// `transformXmlSerialize(pstate, xs)` (parse_expr.c:2495) — analyze a raw
/// `XMLSERIALIZE(... AS type)`: build an `IS_XMLSERIALIZE` `XmlExpr` over the
/// XML-coerced source expression, then coerce that to the requested target type
/// (text-castable).
fn transformXmlSerialize<'mcx>(
    pstate: &mut ParseState<'mcx>,
    xs: types_nodes::rawexprnodes::XmlSerialize<'mcx>,
) -> PgResult<Expr> {
    let types_nodes::rawexprnodes::XmlSerialize {
        xmloption,
        expr,
        type_name,
        indent,
        location,
    } = xs;

    let inner = boxed_node(expr)
        .ok_or_else(|| PgError::error("transformXmlSerialize: XMLSERIALIZE without expr"))?;
    let inner = transformExprRecurse(pstate, Some(inner))?
        .ok_or_else(|| PgError::error("transformXmlSerialize: argument transformed to NULL"))?;
    let inner = coerce::coerce_to_specific_type::call(pstate, inner, XMLOID, "XMLSERIALIZE")?;

    let type_name =
        type_name.ok_or_else(|| PgError::error("transformXmlSerialize: missing typeName"))?;
    let (target_type, target_typmod) = typename_type_id_and_mod(pstate, &*type_name)?;

    // xexpr->type/typmod are only needed to be able to parse back the expression.
    let xexpr = CookedXmlExpr {
        op: XmlExprOp::IS_XMLSERIALIZE,
        name: None,
        named_args: Vec::new(),
        arg_names: Vec::new(),
        args: alloc::vec![inner],
        xmloption,
        indent,
        r#type: target_type,
        typmod: target_typmod,
        location,
    };

    // SQL allows char/varchar as targets; we allow anything implicitly castable
    // from text, so user-defined text-like types fit automatically.
    let result = coerce::coerce_to_target_type::call(
        pstate,
        Expr::XmlExpr(xexpr),
        TEXTOID,
        target_type,
        target_typmod,
        CoercionContext::COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    result.ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(alloc::format!(
                "cannot cast XMLSERIALIZE result to {}",
                format_type_be(target_type).unwrap_or_else(|_| String::from("?"))
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error()
    })
}

// ===========================================================================
// SQL/JSON constructor / predicate transforms (parse_expr.c).
//
// These turn the raw-grammar SQL/JSON nodes (`types_nodes::rawexprnodes`
// `Json*`) into cooked `primnodes::Expr` (`JsonConstructorExpr` /
// `JsonIsPredicate`). The constructor's underlying json[b]_build_* function call
// is NOT resolved here — `JsonConstructorExpr.func` stays `None` and the
// executor (`ExecEvalJsonConstructor`) builds it, exactly as in C.
// ===========================================================================

/// `makeJsonByteaToTextConversion(expr, format, location)` (parse_expr.c:3287) —
/// `convert_from(expr, <encoding>)`. The encoding `Const` is a by-ref `name`;
/// this narrow path (FORMAT JSON over a `bytea` input) is not yet ported.
fn make_json_bytea_to_text_conversion<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _expr: Expr,
    _format: &Option<JsonFormat>,
    _location: i32,
) -> PgResult<Expr> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg("FORMAT JSON ENCODING over bytea input is not yet supported")
        .into_error())
}

/// `checkJsonOutputFormat(pstate, format, targettype, allow_format)`
/// (parse_expr.c:3471).
fn check_json_output_format<'mcx>(
    pstate: &ParseState<'mcx>,
    format: &JsonFormat,
    targettype: Oid,
    allow_format_for_non_strings: bool,
) -> PgResult<()> {
    if !allow_format_for_non_strings
        && format.format_type != JsonFormatType::JS_FORMAT_DEFAULT
        && targettype != BYTEAOID
        && targettype != JSONOID
        && targettype != JSONBOID
    {
        let (typcategory, _typispreferred) =
            lsyscache::get_type_category_preferred::call(targettype)?;
        if typcategory != TYPCATEGORY_STRING {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot use JSON format with non-string output types")
                .errposition(parser_errposition(pstate, format.location))
                .into_error());
        }
    }

    if format.format_type == JsonFormatType::JS_FORMAT_JSON {
        let enc = if format.encoding != JsonEncoding::JS_ENC_DEFAULT {
            format.encoding
        } else {
            JsonEncoding::JS_ENC_UTF8
        };

        if targettype != BYTEAOID && format.encoding != JsonEncoding::JS_ENC_DEFAULT {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot set JSON encoding for non-bytea output types")
                .errposition(parser_errposition(pstate, format.location))
                .into_error());
        }

        if enc != JsonEncoding::JS_ENC_UTF8 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("unsupported JSON encoding")
                .errhint("Only UTF8 JSON encoding is supported.")
                .errposition(parser_errposition(pstate, format.location))
                .into_error());
        }
    }

    Ok(())
}

/// `transformJsonOutput(pstate, output, allow_format)` (parse_expr.c:3522).
/// Resolves the RETURNING type/typmod and the default-or-checked FORMAT.
fn transform_json_output<'mcx>(
    pstate: &mut ParseState<'mcx>,
    output: Option<&types_nodes::rawexprnodes::JsonOutput<'mcx>>,
    allow_format: bool,
) -> PgResult<JsonReturning> {
    let Some(output) = output else {
        // default clause value
        return Ok(JsonReturning {
            format: Some(make_json_format(
                JsonFormatType::JS_FORMAT_DEFAULT,
                JsonEncoding::JS_ENC_DEFAULT,
                -1,
            )),
            typid: InvalidOid,
            typmod: -1,
        });
    };

    let type_name = output
        .type_name
        .as_ref()
        .ok_or_else(|| PgError::error("transformJsonOutput: JsonOutput without typeName"))?;

    let (typid, typmod) = typename_type_id_and_mod(pstate, &**type_name)?;

    if type_name.setof {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("returning SETOF types is not supported in SQL/JSON functions")
            .into_error());
    }

    if lsyscache::get_typtype::call(typid)? == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("returning pseudo-types is not supported in SQL/JSON functions")
            .into_error());
    }

    // `output->returning` is analyze-filled (None in the raw form); start from a
    // default JSON format and fill it as C does on the copied returning.
    let mut format = output.returning.and_then(|r| r.format).unwrap_or_else(|| {
        make_json_format(
            JsonFormatType::JS_FORMAT_DEFAULT,
            JsonEncoding::JS_ENC_DEFAULT,
            -1,
        )
    });

    if format.format_type == JsonFormatType::JS_FORMAT_DEFAULT {
        format.format_type = if typid == JSONBOID {
            JsonFormatType::JS_FORMAT_JSONB
        } else {
            JsonFormatType::JS_FORMAT_JSON
        };
    } else {
        check_json_output_format(pstate, &format, typid, allow_format)?;
    }

    Ok(JsonReturning {
        format: Some(format),
        typid,
        typmod,
    })
}

/// `transformJsonConstructorOutput(pstate, output, args)` (parse_expr.c:3569).
fn transform_json_constructor_output<'mcx>(
    pstate: &mut ParseState<'mcx>,
    output: Option<&types_nodes::rawexprnodes::JsonOutput<'mcx>>,
    args: &[Expr],
) -> PgResult<JsonReturning> {
    let mut returning = transform_json_output(pstate, output, true)?;

    if !OidIsValid(returning.typid) {
        let mut have_jsonb = false;
        for expr in args {
            if expr_type(Some(expr))? == JSONBOID {
                have_jsonb = true;
                break;
            }
        }

        if have_jsonb {
            returning.typid = JSONBOID;
            if let Some(f) = returning.format.as_mut() {
                f.format_type = JsonFormatType::JS_FORMAT_JSONB;
            }
        } else {
            // XXX TEXT is default by the standard, but we return JSON.
            returning.typid = JSONOID;
            if let Some(f) = returning.format.as_mut() {
                f.format_type = JsonFormatType::JS_FORMAT_JSON;
            }
        }
        returning.typmod = -1;
    }

    Ok(returning)
}

/// `coerceJsonFuncExpr(pstate, expr, returning, report_error)`
/// (parse_expr.c:3611). Returns the (possibly coerced) expression.
fn coerce_json_func_expr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    expr: Expr,
    returning: &JsonReturning,
    report_error: bool,
) -> PgResult<Option<Expr>> {
    let exprtype = expr_type(Some(&expr))?;

    // if output type is not specified or equals to function type, return.
    if !OidIsValid(returning.typid) || returning.typid == exprtype {
        return Ok(Some(expr));
    }

    let mut location = expr_location(Some(&expr))?;
    if location < 0 {
        location = returning.format.map(|f| f.location).unwrap_or(-1);
    }

    // special case for RETURNING bytea FORMAT json
    if returning.format.map(|f| f.format_type) == Some(JsonFormatType::JS_FORMAT_JSON)
        && returning.typid == BYTEAOID
    {
        // encode json text into bytea using pg_convert_to() — the by-ref name
        // encoding `Const` path; not yet ported.
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("RETURNING bytea FORMAT JSON is not yet supported")
            .into_error());
    }

    let res = coerce::coerce_to_target_type::call(
        pstate,
        expr,
        exprtype,
        returning.typid,
        returning.typmod,
        CoercionContext::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        location,
    )?;

    if res.is_none() && report_error {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(alloc::format!(
                "cannot cast type {} to {}",
                format_type_be(exprtype).unwrap_or_else(|_| String::from("?")),
                format_type_be(returning.typid).unwrap_or_else(|_| String::from("?"))
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    Ok(res)
}

/// `makeJsonConstructorExpr(...)` (parse_expr.c:3675). Builds the cooked
/// `JsonConstructorExpr` and adds the RETURNING coercion when needed.
#[allow(clippy::too_many_arguments)]
fn build_json_constructor_expr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    r#type: JsonConstructorType,
    args: Vec<Expr>,
    fexpr: Option<Expr>,
    returning: JsonReturning,
    unique: bool,
    absent_on_null: bool,
    location: i32,
) -> PgResult<Expr> {
    // We abuse CaseTestExpr as the placeholder for the coercion input.
    let placeholder = if let Some(fexpr) = fexpr.as_ref() {
        CaseTestExpr {
            typeId: expr_type(Some(fexpr))?,
            typeMod: expr_typmod(Some(fexpr))?,
            collation: expr_collation(Some(fexpr))?,
        }
    } else {
        CaseTestExpr {
            typeId: if returning.format.map(|f| f.format_type)
                == Some(JsonFormatType::JS_FORMAT_JSONB)
            {
                JSONBOID
            } else {
                JSONOID
            },
            typeMod: -1,
            collation: InvalidOid,
        }
    };

    let placeholder_expr = Expr::CaseTestExpr(placeholder);
    let coerced = coerce_json_func_expr(pstate, placeholder_expr.clone(), &returning, true)?;

    // `coercion` is set only if coerceJsonFuncExpr produced a different node.
    let coercion = match coerced {
        Some(c) if !exprs_identical_placeholder(&c) => Some(c),
        _ => None,
    };

    Ok(Expr::JsonConstructorExpr(make_json_constructor_expr(
        r#type,
        args,
        fexpr,
        coercion,
        Some(returning),
        unique,
        absent_on_null,
        location,
    )))
}

/// Whether a coercion result is still the bare `CaseTestExpr` placeholder
/// (i.e. no coercion was added). Mirrors C's `coercion != placeholder` pointer
/// check: a bare `CaseTestExpr` means "unchanged".
fn exprs_identical_placeholder(e: &Expr) -> bool {
    matches!(e, Expr::CaseTestExpr(_))
}

/// `transformJsonValueExpr(pstate, constructName, ve, default_format,
/// targettype, isarg)` (parse_expr.c:3309). Returns either a coerced plain
/// expression or a cooked `JsonValueExpr` carrying a `formatted_expr`.
fn transform_json_value_expr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    construct_name: &str,
    ve: &types_nodes::rawexprnodes::JsonValueExpr<'mcx>,
    default_format: JsonFormatType,
    mut targettype: Oid,
    isarg: bool,
) -> PgResult<Expr> {
    let raw = boxed_node(
        ve.raw_expr
            .as_ref()
            .map(|p| p.clone_in(aexpr_clone_ctx(pstate)))
            .transpose()?
            .map(|n| mcx::alloc_in(aexpr_clone_ctx(pstate), n))
            .transpose()?,
    );
    let mut expr = transformExprRecurse(pstate, raw)?
        .ok_or_else(|| PgError::error("transformJsonValueExpr: NULL value expression"))?;

    if expr_type(Some(&expr))? == UNKNOWNOID {
        expr = coerce::coerce_to_specific_type::call(pstate, expr, TEXTOID, construct_name)?;
    }

    let rawexpr = expr.clone();
    let mut exprtype = expr_type(Some(&expr))?;
    let location = expr_location(Some(&expr))?;

    let (typcategory, _typispreferred) =
        lsyscache::get_type_category_preferred::call(exprtype)?;

    let ve_format = ve.format.unwrap_or_else(|| {
        make_json_format(
            JsonFormatType::JS_FORMAT_DEFAULT,
            JsonEncoding::JS_ENC_DEFAULT,
            -1,
        )
    });

    let format: JsonFormatType;
    if ve_format.format_type != JsonFormatType::JS_FORMAT_DEFAULT {
        if ve_format.encoding != JsonEncoding::JS_ENC_DEFAULT && exprtype != BYTEAOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("JSON ENCODING clause is only allowed for bytea input type")
                .errposition(parser_errposition(pstate, ve_format.location))
                .into_error());
        }
        format = if exprtype == JSONOID || exprtype == JSONBOID {
            JsonFormatType::JS_FORMAT_DEFAULT
        } else {
            ve_format.format_type
        };
    } else if isarg {
        // Special treatment for PASSING arguments: types supported directly by
        // GetJsonPathVar()/JsonItemFromDatum() pass through unconverted.
        match exprtype {
            x if x == BOOLOID
                || x == TEXTOID
                || x == INT4OID
                || x == DATEOID
                || x == TIMEOID
                || x == TIMETZOID
                || x == TIMESTAMPOID
                || x == TIMESTAMPTZOID =>
            {
                return Ok(expr)
            }
            _ => {
                if typcategory == TYPCATEGORY_STRING {
                    return Ok(expr);
                }
            }
        }
        format = default_format;
    } else if exprtype == JSONOID || exprtype == JSONBOID {
        format = JsonFormatType::JS_FORMAT_DEFAULT;
    } else {
        format = default_format;
    }

    if format != JsonFormatType::JS_FORMAT_DEFAULT
        || (OidIsValid(targettype) && exprtype != targettype)
    {
        let only_allow_cast = OidIsValid(targettype);

        if !isarg
            && !only_allow_cast
            && exprtype != BYTEAOID
            && typcategory != TYPCATEGORY_STRING
        {
            let msg = if ve_format.format_type == JsonFormatType::JS_FORMAT_DEFAULT {
                "cannot use non-string types with implicit FORMAT JSON clause"
            } else {
                "cannot use non-string types with explicit FORMAT JSON clause"
            };
            let loc = if ve_format.location >= 0 {
                ve_format.location
            } else {
                location
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(msg)
                .errposition(parser_errposition(pstate, loc))
                .into_error());
        }

        if format == JsonFormatType::JS_FORMAT_JSON && exprtype == BYTEAOID {
            expr = make_json_bytea_to_text_conversion(pstate, expr, &ve.format, location)?;
            exprtype = TEXTOID;
        }

        if !OidIsValid(targettype) {
            targettype = if format == JsonFormatType::JS_FORMAT_JSONB {
                JSONBOID
            } else {
                JSONOID
            };
        }

        let coerced = coerce::coerce_to_target_type::call(
            pstate,
            expr.clone(),
            exprtype,
            targettype,
            -1,
            CoercionContext::COERCION_EXPLICIT,
            CoercionForm::COERCE_EXPLICIT_CAST,
            location,
        )?;

        let coerced = match coerced {
            Some(c) => c,
            None => {
                if only_allow_cast {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_CANNOT_COERCE)
                        .errmsg(alloc::format!(
                            "cannot cast type {} to {}",
                            format_type_be(exprtype).unwrap_or_else(|_| String::from("?")),
                            format_type_be(targettype).unwrap_or_else(|_| String::from("?"))
                        ))
                        .errposition(parser_errposition(pstate, location))
                        .into_error());
                }
                let fnoid = if targettype == JSONOID { F_TO_JSON } else { F_TO_JSONB };
                make_func_expr(
                    fnoid,
                    targettype,
                    alloc::vec![expr.clone()],
                    InvalidOid,
                    InvalidOid,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                )
            }
        };

        if exprs_eq_ptr(&coerced, &expr) {
            expr = rawexpr;
        } else {
            expr = Expr::JsonValueExpr(CookedJsonValueExpr {
                raw_expr: Some(Box::new(rawexpr)),
                formatted_expr: Some(Box::new(coerced)),
                format: ve.format,
            });
        }
    }

    Ok(expr)
}

/// Coarse "is the coercion the identity?" check. The C compares pointers; in the
/// owned model the only way `coerce_to_target_type` returns the input unchanged
/// is the no-op coercion, which we cannot detect by identity — but the
/// subsequent `JsonValueExpr` wrapping is harmless when it happens, and the
/// common (json/jsonb passthrough) path takes `format == JS_FORMAT_DEFAULT` and
/// never reaches here. Treat distinct nodes as coerced.
fn exprs_eq_ptr(_a: &Expr, _b: &Expr) -> bool {
    false
}

/// `transformJsonObjectConstructor(pstate, ctor)` (parse_expr.c:3735).
fn transformJsonObjectConstructor<'mcx>(
    pstate: &mut ParseState<'mcx>,
    ctor: types_nodes::rawexprnodes::JsonObjectConstructor<'mcx>,
) -> PgResult<Expr> {
    let mut args: Vec<Expr> = Vec::new();

    for kv_ptr in ctor.exprs.iter() {
        let kv_node = kv_ptr.clone_in(aexpr_clone_ctx(pstate))?;
        let kv = kv_node
            .into_jsonkeyvalue()
            .ok_or_else(|| PgError::error("JSON_OBJECT(): expected JsonKeyValue"))?;

        let key_node = boxed_node(
            kv.key
                .as_ref()
                .map(|p| p.clone_in(aexpr_clone_ctx(pstate)))
                .transpose()?
                .map(|n| mcx::alloc_in(aexpr_clone_ctx(pstate), n))
                .transpose()?,
        );
        let key = transformExprRecurse(pstate, key_node)?
            .ok_or_else(|| PgError::error("JSON_OBJECT(): NULL key"))?;

        let val_ve = kv
            .value
            .as_ref()
            .ok_or_else(|| PgError::error("JSON_OBJECT(): missing value"))?;
        let val = transform_json_value_expr(
            pstate,
            "JSON_OBJECT()",
            val_ve,
            JsonFormatType::JS_FORMAT_DEFAULT,
            InvalidOid,
            false,
        )?;

        args.push(key);
        args.push(val);
    }

    let returning = transform_json_constructor_output(pstate, ctor.output.as_deref(), &args)?;

    build_json_constructor_expr(
        pstate,
        JsonConstructorType::JSCTOR_JSON_OBJECT,
        args,
        None,
        returning,
        ctor.unique,
        ctor.absent_on_null,
        ctor.location,
    )
}

/// `transformJsonArrayConstructor(pstate, ctor)` (parse_expr.c:4031).
fn transformJsonArrayConstructor<'mcx>(
    pstate: &mut ParseState<'mcx>,
    ctor: types_nodes::rawexprnodes::JsonArrayConstructor<'mcx>,
) -> PgResult<Expr> {
    let mut args: Vec<Expr> = Vec::new();

    for ve_ptr in ctor.exprs.iter() {
        let ve_node = ve_ptr.clone_in(aexpr_clone_ctx(pstate))?;
        let ve = ve_node
            .into_jsonvalueexpr()
            .ok_or_else(|| PgError::error("JSON_ARRAY(): expected JsonValueExpr"))?;
        let val = transform_json_value_expr(
            pstate,
            "JSON_ARRAY()",
            &ve,
            JsonFormatType::JS_FORMAT_DEFAULT,
            InvalidOid,
            false,
        )?;
        args.push(val);
    }

    let returning = transform_json_constructor_output(pstate, ctor.output.as_deref(), &args)?;

    build_json_constructor_expr(
        pstate,
        JsonConstructorType::JSCTOR_JSON_ARRAY,
        args,
        None,
        returning,
        false,
        ctor.absent_on_null,
        ctor.location,
    )
}

/// `transformJsonScalarExpr(pstate, jsexpr)` (parse_expr.c:4223).
fn transformJsonScalarExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    jsexpr: types_nodes::rawexprnodes::JsonScalarExpr<'mcx>,
) -> PgResult<Expr> {
    let arg_node = boxed_node(
        jsexpr
            .expr
            .as_ref()
            .map(|p| p.clone_in(aexpr_clone_ctx(pstate)))
            .transpose()?
            .map(|n| mcx::alloc_in(aexpr_clone_ctx(pstate), n))
            .transpose()?,
    );
    let mut arg = transformExprRecurse(pstate, arg_node)?
        .ok_or_else(|| PgError::error("JSON_SCALAR(): NULL argument"))?;

    let returning = transform_json_returning(pstate, jsexpr.output.as_deref(), "JSON_SCALAR()")?;

    if expr_type(Some(&arg))? == UNKNOWNOID {
        arg = coerce::coerce_to_specific_type::call(pstate, arg, TEXTOID, "JSON_SCALAR")?;
    }

    build_json_constructor_expr(
        pstate,
        JsonConstructorType::JSCTOR_JSON_SCALAR,
        Vec::new(),
        Some(arg),
        returning,
        false,
        false,
        jsexpr.location,
    )
}

/// `transformJsonReturning(pstate, output, fname)` (parse_expr.c:4134).
fn transform_json_returning<'mcx>(
    pstate: &mut ParseState<'mcx>,
    output: Option<&types_nodes::rawexprnodes::JsonOutput<'mcx>>,
    fname: &str,
) -> PgResult<JsonReturning> {
    let mut returning = transform_json_output(pstate, output, false)?;

    if OidIsValid(returning.typid) {
        if returning.typid != JSONOID && returning.typid != JSONBOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(alloc::format!(
                    "cannot use RETURNING type {} in {}",
                    format_type_be(returning.typid).unwrap_or_else(|_| String::from("?")),
                    fname
                ))
                .into_error());
        }
    } else {
        // default to JSON
        returning.typid = JSONOID;
        if let Some(f) = returning.format.as_mut() {
            f.format_type = JsonFormatType::JS_FORMAT_JSON;
        }
    }

    Ok(returning)
}

/// `transformJsonSerializeExpr(pstate, expr)` (parse_expr.c:4246).
fn transformJsonSerializeExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    jsexpr: types_nodes::rawexprnodes::JsonSerializeExpr<'mcx>,
) -> PgResult<Expr> {
    let arg_ve = jsexpr
        .expr
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_SERIALIZE(): missing argument"))?;
    let arg = transform_json_value_expr(
        pstate,
        "JSON_SERIALIZE()",
        arg_ve,
        JsonFormatType::JS_FORMAT_JSON,
        InvalidOid,
        false,
    )?;

    let returning = transform_json_output(pstate, jsexpr.output.as_deref(), true)?;

    if OidIsValid(returning.typid) {
        let (typcategory, _typispreferred) =
            lsyscache::get_type_category_preferred::call(returning.typid)?;
        if returning.typid != BYTEAOID && typcategory != TYPCATEGORY_STRING {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(alloc::format!(
                    "cannot use RETURNING type {} in {}",
                    format_type_be(returning.typid).unwrap_or_else(|_| String::from("?")),
                    "JSON_SERIALIZE()"
                ))
                .into_error());
        }
    } else {
        // RETURNING text by default
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("JSON_SERIALIZE() without explicit RETURNING is not yet supported")
            .into_error());
    }

    build_json_constructor_expr(
        pstate,
        JsonConstructorType::JSCTOR_JSON_SERIALIZE,
        Vec::new(),
        Some(arg),
        returning,
        false,
        false,
        jsexpr.location,
    )
}

/// `transformJsonParseExpr(pstate, jsexpr)` (parse_expr.c:4174).
fn transformJsonParseExpr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    jsexpr: types_nodes::rawexprnodes::JsonParseExpr<'mcx>,
) -> PgResult<Expr> {
    let returning = transform_json_returning(pstate, jsexpr.output.as_deref(), "JSON()")?;

    let arg_ve = jsexpr
        .expr
        .as_ref()
        .ok_or_else(|| PgError::error("JSON(): missing argument"))?;

    if jsexpr.unique_keys {
        // Coercing this slightly differently (with UNIQUE KEYS) needs the
        // transformJsonParseArg + json IS-unique check path which is not yet
        // ported; report the gap precisely.
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("JSON(... WITH UNIQUE KEYS) is not yet supported")
            .into_error());
    }

    let arg = transform_json_value_expr(
        pstate,
        "JSON()",
        arg_ve,
        JsonFormatType::JS_FORMAT_JSON,
        returning.typid,
        false,
    )?;

    build_json_constructor_expr(
        pstate,
        JsonConstructorType::JSCTOR_JSON_PARSE,
        Vec::new(),
        Some(arg),
        returning,
        jsexpr.unique_keys,
        false,
        jsexpr.location,
    )
}

// fmgroids of the underlying SQL/JSON aggregate functions (fmgroids.h).
const F_JSON_AGG: Oid = 3175;
const F_JSON_OBJECT_AGG: Oid = 3197;
const F_JSONB_AGG: Oid = 3267;
const F_JSONB_OBJECT_AGG: Oid = 3270;
const F_JSON_AGG_STRICT: Oid = 6276;
const F_JSON_OBJECT_AGG_STRICT: Oid = 6280;
const F_JSON_OBJECT_AGG_UNIQUE: Oid = 6281;
const F_JSON_OBJECT_AGG_UNIQUE_STRICT: Oid = 6282;
const F_JSONB_AGG_STRICT: Oid = 6284;
const F_JSONB_OBJECT_AGG_STRICT: Oid = 6288;
const F_JSONB_OBJECT_AGG_UNIQUE: Oid = 6289;
const F_JSONB_OBJECT_AGG_UNIQUE_STRICT: Oid = 6290;

/// `transformJsonAggConstructor(pstate, agg_ctor, returning, args, aggfnoid,
/// aggtype, ctor_type, unique, absent_on_null)` (parse_expr.c:3849). Builds an
/// `Aggref` (or `WindowFunc` for `OVER`) over the resolved underlying
/// `json[b]_*agg*` aggregate and wraps it in a `JsonConstructorExpr`.
#[allow(clippy::too_many_arguments)]
fn transformJsonAggConstructor<'mcx>(
    pstate: &mut ParseState<'mcx>,
    agg_ctor: &types_nodes::rawexprnodes::JsonAggConstructor<'mcx>,
    returning: JsonReturning,
    args: Vec<Expr>,
    aggfnoid: Oid,
    aggtype: Oid,
    ctor_type: JsonConstructorType,
    unique: bool,
    absent_on_null: bool,
) -> PgResult<Expr> {
    let mcx = aexpr_clone_ctx(pstate);

    // aggfilter = agg_ctor->agg_filter ? transformWhereClause(...) : NULL
    let aggfilter = match agg_ctor.agg_filter.as_ref() {
        Some(af) => {
            let clause = af.clone_in(mcx)?;
            backend_parser_clause_seams::transform_where_clause::call(
                mcx,
                pstate,
                Some(clause),
                ParseExprKind::EXPR_KIND_FILTER,
                "FILTER",
            )?
        }
        None => None,
    };

    let node: Expr = if let Some(over) = agg_ctor.over.as_ref() {
        // window function
        let wfunc = WindowFunc {
            winfnoid: aggfnoid,
            wintype: aggtype,
            // wincollid and inputcollid will be set by parse_collate.c
            wincollid: InvalidOid,
            inputcollid: InvalidOid,
            args: args.clone(),
            aggfilter: aggfilter.map(Box::new),
            runCondition: Vec::new(),
            // winref will be set by transformWindowFuncCall
            winref: 0,
            winstar: false,
            winagg: true,
            location: agg_ctor.location,
        };

        // ordered aggs not allowed in windows yet
        if !agg_ctor.agg_order.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("aggregate ORDER BY is not implemented for window functions")
                .errposition(parser_errposition(pstate, agg_ctor.location))
                .into_error());
        }

        let windef = over.clone_in(mcx)?;
        let finished =
            backend_parser_parse_agg_seams::transform_window_func_call::call(pstate, wfunc, windef)?;
        Expr::WindowFunc(finished)
    } else {
        let aggref = Aggref {
            aggfnoid,
            aggtype,
            // aggcollid and inputcollid will be set by parse_collate.c
            aggcollid: InvalidOid,
            inputcollid: InvalidOid,
            // aggtranstype will be set by planner
            aggtranstype: InvalidOid,
            // aggargtypes will be set by transformAggregateCall
            aggargtypes: Vec::new(),
            // aggdirectargs and args will be set by transformAggregateCall
            aggdirectargs: Vec::new(),
            args: Vec::new(),
            // aggorder and aggdistinct will be set by transformAggregateCall
            aggorder: Vec::new(),
            aggdistinct: Vec::new(),
            aggfilter: aggfilter.map(Box::new),
            aggstar: false,
            aggvariadic: false,
            aggkind: types_parsenodes::AGGKIND_NORMAL,
            aggpresorted: false,
            // agglevelsup will be set by transformAggregateCall
            agglevelsup: 0,
            aggsplit: types_nodes::nodeagg::AGGSPLIT_SIMPLE, // planner might change this
            aggno: -1, // planner will set aggno and aggtransno
            aggtransno: -1,
            location: agg_ctor.location,
        };

        // transformAggregateCall(pstate, aggref, args, agg_ctor->agg_order, false)
        let mut aggorder: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
        for n in agg_ctor.agg_order.iter() {
            aggorder.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
        let finished = backend_parser_parse_agg_seams::transform_aggregate_call::call(
            pstate, aggref, args, aggorder, false,
        )?;
        Expr::Aggref(finished)
    };

    build_json_constructor_expr(
        pstate,
        ctor_type,
        Vec::new(),
        Some(node),
        returning,
        unique,
        absent_on_null,
        agg_ctor.location,
    )
}

/// `transformJsonObjectAgg(pstate, agg)` (parse_expr.c:3929). Builds an
/// `Aggref` calling the underlying `json[b]_object_agg*` aggregate, picking the
/// variant by `RETURNING json/jsonb` + ABSENT ON NULL + WITH UNIQUE.
fn transformJsonObjectAgg<'mcx>(
    pstate: &mut ParseState<'mcx>,
    agg: types_nodes::rawexprnodes::JsonObjectAgg<'mcx>,
) -> PgResult<Expr> {
    let kv = agg
        .arg
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_OBJECTAGG(): missing key/value"))?;

    let key_node = boxed_node(
        kv.key
            .as_ref()
            .map(|p| p.clone_in(aexpr_clone_ctx(pstate)))
            .transpose()?
            .map(|n| mcx::alloc_in(aexpr_clone_ctx(pstate), n))
            .transpose()?,
    );
    let key = transformExprRecurse(pstate, key_node)?
        .ok_or_else(|| PgError::error("JSON_OBJECTAGG(): NULL key"))?;

    let val_ve = kv
        .value
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_OBJECTAGG(): missing value"))?;
    let val = transform_json_value_expr(
        pstate,
        "JSON_OBJECTAGG()",
        val_ve,
        JsonFormatType::JS_FORMAT_DEFAULT,
        InvalidOid,
        false,
    )?;

    let args = vec![key, val];

    let constructor = agg
        .constructor
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_OBJECTAGG(): missing constructor"))?;

    let returning = transform_json_constructor_output(pstate, constructor.output.as_deref(), &args)?;

    let (aggfnoid, aggtype) =
        if returning.format.map(|f| f.format_type) == Some(JsonFormatType::JS_FORMAT_JSONB) {
            let oid = if agg.absent_on_null {
                if agg.unique {
                    F_JSONB_OBJECT_AGG_UNIQUE_STRICT
                } else {
                    F_JSONB_OBJECT_AGG_STRICT
                }
            } else if agg.unique {
                F_JSONB_OBJECT_AGG_UNIQUE
            } else {
                F_JSONB_OBJECT_AGG
            };
            (oid, JSONBOID)
        } else {
            let oid = if agg.absent_on_null {
                if agg.unique {
                    F_JSON_OBJECT_AGG_UNIQUE_STRICT
                } else {
                    F_JSON_OBJECT_AGG_STRICT
                }
            } else if agg.unique {
                F_JSON_OBJECT_AGG_UNIQUE
            } else {
                F_JSON_OBJECT_AGG
            };
            (oid, JSONOID)
        };

    transformJsonAggConstructor(
        pstate,
        constructor,
        returning,
        args,
        aggfnoid,
        aggtype,
        JsonConstructorType::JSCTOR_JSON_OBJECTAGG,
        agg.unique,
        agg.absent_on_null,
    )
}

/// `transformJsonArrayAgg(pstate, agg)` (parse_expr.c:3993). Builds an `Aggref`
/// calling the underlying `json[b]_agg*` aggregate.
fn transformJsonArrayAgg<'mcx>(
    pstate: &mut ParseState<'mcx>,
    agg: types_nodes::rawexprnodes::JsonArrayAgg<'mcx>,
) -> PgResult<Expr> {
    let arg_ve = agg
        .arg
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_ARRAYAGG(): missing argument"))?;
    let arg = transform_json_value_expr(
        pstate,
        "JSON_ARRAYAGG()",
        arg_ve,
        JsonFormatType::JS_FORMAT_DEFAULT,
        InvalidOid,
        false,
    )?;

    let args = vec![arg];

    let constructor = agg
        .constructor
        .as_ref()
        .ok_or_else(|| PgError::error("JSON_ARRAYAGG(): missing constructor"))?;

    let returning = transform_json_constructor_output(pstate, constructor.output.as_deref(), &args)?;

    let (aggfnoid, aggtype) =
        if returning.format.map(|f| f.format_type) == Some(JsonFormatType::JS_FORMAT_JSONB) {
            (
                if agg.absent_on_null {
                    F_JSONB_AGG_STRICT
                } else {
                    F_JSONB_AGG
                },
                JSONBOID,
            )
        } else {
            (
                if agg.absent_on_null {
                    F_JSON_AGG_STRICT
                } else {
                    F_JSON_AGG
                },
                JSONOID,
            )
        };

    transformJsonAggConstructor(
        pstate,
        constructor,
        returning,
        args,
        aggfnoid,
        aggtype,
        JsonConstructorType::JSCTOR_JSON_ARRAYAGG,
        false,
        agg.absent_on_null,
    )
}

/// `transformJsonIsPredicate(pstate, pred)` (parse_expr.c:4111).
fn transformJsonIsPredicate<'mcx>(
    pstate: &mut ParseState<'mcx>,
    pred: types_nodes::rawexprnodes::JsonIsPredicate<'mcx>,
) -> PgResult<Expr> {
    // transformJsonParseArg: recurse + coerce the subject to text/json/jsonb.
    let arg_node = boxed_node(
        pred.expr
            .as_ref()
            .map(|p| p.clone_in(aexpr_clone_ctx(pstate)))
            .transpose()?
            .map(|n| mcx::alloc_in(aexpr_clone_ctx(pstate), n))
            .transpose()?,
    );
    let mut expr = transformExprRecurse(pstate, arg_node)?
        .ok_or_else(|| PgError::error("IS JSON: NULL argument"))?;

    let mut exprtype = expr_type(Some(&expr))?;

    // Coerce UNKNOWN / string-category inputs to text (transformJsonParseArg).
    if exprtype == UNKNOWNOID {
        expr = coerce::coerce_to_specific_type::call(pstate, expr, TEXTOID, "IS JSON")?;
        exprtype = TEXTOID;
    } else if exprtype != JSONOID && exprtype != JSONBOID && exprtype != BYTEAOID {
        let (typcategory, _typispreferred) =
            lsyscache::get_type_category_preferred::call(exprtype)?;
        if typcategory == TYPCATEGORY_STRING {
            let coerced = coerce::coerce_to_target_type::call(
                pstate,
                expr.clone(),
                exprtype,
                TEXTOID,
                -1,
                CoercionContext::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            if let Some(c) = coerced {
                expr = c;
                exprtype = TEXTOID;
            }
        }
    }

    if exprtype != TEXTOID && exprtype != JSONOID && exprtype != JSONBOID && exprtype != BYTEAOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(alloc::format!(
                "cannot use type {} in IS JSON predicate",
                format_type_be(exprtype).unwrap_or_else(|_| String::from("?"))
            ))
            .into_error());
    }

    Ok(make_json_is_predicate(
        Some(expr),
        None,
        pred.item_type,
        pred.unique_keys,
        pred.location,
    ))
}

#[allow(dead_code)]
fn seam_transform_json_expr<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _node: Node<'mcx>,
) -> PgResult<Expr> {
    // The SQL/JSON transform family (transformJson{Object,Array}Constructor /
    // transformJson{Object,Array}Agg / transformJsonIsPredicate /
    // transformJson{Parse,Scalar,Serialize,Func}Expr and their helpers) is
    // blocked on a node-model keystone OUTSIDE this unit: the 12 raw SQL/JSON
    // grammar parse-nodes (JsonObjectConstructor, JsonArrayConstructor,
    // JsonArrayQueryConstructor, JsonAggConstructor, JsonObjectAgg,
    // JsonArrayAgg, JsonFuncExpr, JsonParseExpr, JsonScalarExpr,
    // JsonSerializeExpr, JsonOutput, JsonKeyValue) are NOT variants of
    // `types_nodes::nodes::Node`/`Expr` (they exist only in the parallel
    // `backend-nodes-types-fgram` tree), and the idiomatic grammar
    // (`backend-parser-gram-core`) emits no JSON productions. Lifting those
    // structs into `types-nodes` + adding their grammar converters is a
    // separate cross-owner campaign (types-nodes + gram-core); the coercion /
    // makefuncs substrate the transforms need is already real. Until that
    // lands there is no input node to match on.
    panic!(
        "SQL/JSON transform family blocked on node-model keystone: 12 raw JSON \
         grammar parse-nodes are absent from types_nodes::Node/Expr and the \
         idiomatic grammar emits no JSON productions (owner: types-nodes + \
         backend-parser-gram-core)."
    )
}

// ===========================================================================
// Direct-call shims for merged-owner / seam callees not yet re-homed cleanly.
// ===========================================================================

/// `MAX_TIME_PRECISION` / `MAX_TIMESTAMP_PRECISION` (datetime.h) — both 6.
const MAX_TIME_PRECISION: i32 = 6;
const MAX_TIMESTAMP_PRECISION: i32 = 6;

fn typmod_check_errloc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/parser/parse_expr.c", 0, funcname)
}

/// `anytime_typmod_check(istz, typmod)` (utils/adt/date.c) — validate a
/// time/timetz typmod: negative is an ERROR, over-max clamps to
/// `MAX_TIME_PRECISION` with a WARNING.  Faithful to date.c.
fn lsyscache_anytime_typmod_check(istz: bool, mut typmod: i32) -> PgResult<i32> {
    if typmod < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIME({typmod}){} precision must not be negative",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .into_error());
    }
    if typmod > MAX_TIME_PRECISION {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIME({typmod}){} precision reduced to maximum allowed, {MAX_TIME_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .finish(typmod_check_errloc("anytime_typmod_check"))?;
        typmod = MAX_TIME_PRECISION;
    }
    Ok(typmod)
}

/// `anytimestamp_typmod_check(istz, typmod)` (utils/adt/timestamp.c) — same
/// shape over `MAX_TIMESTAMP_PRECISION`.
fn lsyscache_anytimestamp_typmod_check(istz: bool, mut typmod: i32) -> PgResult<i32> {
    if typmod < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIMESTAMP({typmod}){} precision must not be negative",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .into_error());
    }
    if typmod > MAX_TIMESTAMP_PRECISION {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIMESTAMP({typmod}){} precision reduced to maximum allowed, {MAX_TIMESTAMP_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .finish(typmod_check_errloc("anytimestamp_typmod_check"))?;
        typmod = MAX_TIMESTAMP_PRECISION;
    }
    Ok(typmod)
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
    match n.node_tag() {
        ntag::T_A_Const => n.expect_a_const().location,
        ntag::T_A_Expr => n.expect_a_expr().location,
        _ => match n.as_expr() {
            Some(e) => expr_location(Some(e)).unwrap_or(-1),
            None => -1,
        },
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
// ===========================================================================
// subscripting_transform (array_subscript_transform / jsonb_subscript_transform)
// ===========================================================================
//
// The transform method (`sbsroutines->transform`) is dispatched by
// `transformContainerSubscripts` (parse_node.c, small1). Its C bodies live in
// the `utils/adt` handler files (arraysubs.c `array_subscript_transform`,
// jsonbsubs.c `jsonb_subscript_transform`), but they call `transformExpr` +
// `coerce_to_target_type` / `coerce_type` / `can_coerce_type` — parser-layer
// entry points above `utils/adt`. The install therefore lives here (this crate
// owns `transformExpr` and reaches the coerce seams) and dispatches on the
// container type's `SubscriptHandler`, re-derived from `refcontainertype`
// exactly as `getSubscriptingRoutines` does for the executor side.

/// `MAXDIM` (`utils/array.h`) — the maximum number of array dimensions.
const MAXDIM: usize = 6;

/// `array_subscript_transform(sbsref, indirection, pstate, isSlice, isAssignment)`
/// (arraysubs.c): transform the subscript expressions, splitting upper/lower
/// bounds, coercing each to int4, and computing the result type.
fn array_subscript_transform<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    mut sbsref: SubscriptingRef,
    indirection: &[A_Indices<'mcx>],
    pstate: &mut ParseState<'mcx>,
    is_slice: bool,
) -> PgResult<SubscriptingRef> {
    let mut upper_indexpr: Vec<Option<Expr>> = Vec::new();
    let mut lower_indexpr: Vec<Option<Expr>> = Vec::new();

    for ai in indirection.iter() {
        if is_slice {
            let subexpr: Option<Expr> = if let Some(lidx) = ai.lidx.as_deref() {
                let se = transformExpr(pstate, Some(lidx.clone_in(mcx)?), pstate.p_expr_kind)?
                    .expect("array_subscript_transform: lidx transformed to NULL");
                let setype = expr_type(Some(&se))?;
                match coerce::coerce_to_target_type::call(
                    pstate,
                    se,
                    setype,
                    INT4OID,
                    -1,
                    CoercionContext::COERCION_ASSIGNMENT,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                    -1,
                )? {
                    Some(c) => Some(c),
                    None => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_DATATYPE_MISMATCH)
                            .errmsg("array subscript must have type integer")
                            .errposition(parser_errposition(pstate, ai.lidx.as_deref().map(node_location).unwrap_or(-1)))
                            .into_error());
                    }
                }
            } else if !ai.is_slice {
                // Make a constant 1.
                Some(Expr::Const(make_const(
                    mcx,
                    INT4OID,
                    -1,
                    InvalidOid,
                    4,
                    types_tuple::Datum::from_i32(1),
                    false,
                    true,
                )?))
            } else {
                // Slice with omitted lower bound: put NULL into the list.
                None
            };
            lower_indexpr.push(subexpr);
        } else {
            debug_assert!(ai.lidx.is_none() && !ai.is_slice);
        }

        let subexpr: Option<Expr> = if let Some(uidx) = ai.uidx.as_deref() {
            let se = transformExpr(pstate, Some(uidx.clone_in(mcx)?), pstate.p_expr_kind)?
                .expect("array_subscript_transform: uidx transformed to NULL");
            let setype = expr_type(Some(&se))?;
            match coerce::coerce_to_target_type::call(
                pstate,
                se,
                setype,
                INT4OID,
                -1,
                CoercionContext::COERCION_ASSIGNMENT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )? {
                Some(c) => Some(c),
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg("array subscript must have type integer")
                        .errposition(parser_errposition(pstate, ai.uidx.as_deref().map(node_location).unwrap_or(-1)))
                        .into_error());
                }
            }
        } else {
            // Slice with omitted upper bound: put NULL into the list.
            debug_assert!(is_slice && ai.is_slice);
            None
        };
        upper_indexpr.push(subexpr);
    }

    // Verify subscript list lengths are within the implementation limit.
    if upper_indexpr.len() > MAXDIM {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(alloc::format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                upper_indexpr.len(),
                MAXDIM
            ))
            .into_error());
    }

    // Result type is the array type if slicing, else the element type. The
    // typmod is unchanged in either case (reftypmod already set by the caller).
    sbsref.refrestype = if is_slice {
        sbsref.refcontainertype
    } else {
        sbsref.refelemtype
    };

    sbsref.refupperindexpr = upper_indexpr;
    sbsref.reflowerindexpr = lower_indexpr;
    Ok(sbsref)
}

/// `jsonb_subscript_transform(sbsref, indirection, pstate, isSlice, isAssignment)`
/// (jsonbsubs.c): transform the subscript expressions, coercing each to int4 or
/// text (exactly one must be reachable), with no slice support.
fn jsonb_subscript_transform<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    mut sbsref: SubscriptingRef,
    indirection: &[A_Indices<'mcx>],
    pstate: &mut ParseState<'mcx>,
    is_slice: bool,
) -> PgResult<SubscriptingRef> {
    let mut upper_indexpr: Vec<Option<Expr>> = Vec::new();

    for ai in indirection.iter() {
        if is_slice {
            let loc = if ai.uidx.is_some() {
                ai.uidx.as_deref().map(node_location).unwrap_or(-1)
            } else {
                ai.lidx.as_deref().map(node_location).unwrap_or(-1)
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("jsonb subscript does not support slices")
                .errposition(parser_errposition(pstate, loc))
                .into_error());
        }

        if let Some(uidx) = ai.uidx.as_deref() {
            let mut target_type = UNKNOWNOID;
            let sub_expr = transformExpr(pstate, Some(uidx.clone_in(mcx)?), pstate.p_expr_kind)?
                .expect("jsonb_subscript_transform: uidx transformed to NULL");
            let sub_expr_type = expr_type(Some(&sub_expr))?;

            if sub_expr_type != UNKNOWNOID {
                let targets = [INT4OID, TEXTOID];
                for t in targets.iter() {
                    if coerce::can_coerce_type::call(
                        1,
                        &[sub_expr_type],
                        &[*t],
                        CoercionContext::COERCION_IMPLICIT,
                    )? {
                        // Two coercion targets possible => ambiguous, failure.
                        if target_type != UNKNOWNOID {
                            return Err(ereport(ERROR)
                                .errcode(ERRCODE_DATATYPE_MISMATCH)
                                .errmsg(alloc::format!(
                                    "subscript type {} is not supported",
                                    format_type_be(sub_expr_type)?
                                ))
                                .errhint(
                                    "jsonb subscript must be coercible to only one type, integer or text.",
                                )
                                .errposition(parser_errposition(pstate, expr_location(Some(&sub_expr))?))
                                .into_error());
                        }
                        target_type = *t;
                    }
                }
                // No suitable types found, failure.
                if target_type == UNKNOWNOID {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg(alloc::format!(
                            "subscript type {} is not supported",
                            format_type_be(sub_expr_type)?
                        ))
                        .errhint(
                            "jsonb subscript must be coercible to either integer or text.",
                        )
                        .errposition(parser_errposition(pstate, expr_location(Some(&sub_expr))?))
                        .into_error());
                }
            } else {
                target_type = TEXTOID;
            }

            // can_coerce_type guarantees success; coerce_type performs the cast.
            let coerced = coerce::coerce_type::call(
                Some(pstate),
                sub_expr,
                sub_expr_type,
                target_type,
                -1,
                CoercionContext::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            upper_indexpr.push(Some(coerced));
        } else {
            // Slice with omitted upper bound: cannot happen (errored above).
            debug_assert!(is_slice && ai.is_slice);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("jsonb subscript does not support slices")
                .errposition(parser_errposition(pstate, ai.uidx.as_deref().map(node_location).unwrap_or(-1)))
                .into_error());
        }
    }

    sbsref.refupperindexpr = upper_indexpr;
    sbsref.reflowerindexpr = Vec::new();
    sbsref.refrestype = JSONBOID;
    sbsref.reftypmod = -1;
    Ok(sbsref)
}

/// Install of `subscripting_transform`: dispatch on the container type's
/// `SubscriptHandler` (re-derived from `refcontainertype`) to the matching
/// transform method body.
fn subscripting_transform_impl<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    sbsref: SubscriptingRef,
    indirection: &[A_Indices<'mcx>],
    pstate: &mut ParseState<'mcx>,
    is_slice: bool,
    _is_assignment: bool,
) -> PgResult<SubscriptingRef> {
    use types_nodes::execexpr::SubscriptHandler;
    let routines = lsyscache::get_subscripting_routines::call(sbsref.refcontainertype)?
        .expect("subscripting_transform: refcontainertype is not subscriptable");
    match routines.0.handler {
        SubscriptHandler::Array | SubscriptHandler::RawArray => {
            array_subscript_transform(mcx, sbsref, indirection, pstate, is_slice)
        }
        SubscriptHandler::Jsonb => {
            jsonb_subscript_transform(mcx, sbsref, indirection, pstate, is_slice)
        }
    }
}

pub fn init_seams() {
    me::analyze_one_exec_param::set(analyze_one_exec_param_impl);
    me::parser_errposition::set(parser_errposition_impl);
    me::parse_expr_kind_name::set(ParseExprKindName);
    me::transformExpr::set(transformExpr);
    backend_parser_small1_seams::subscripting_transform::set(subscripting_transform_impl);

    // Install the GUC engine's variable accessors for `transform_null_equals`
    // (guc_tables.c points its `&Transform_null_equals` slot at the C global
    // owned by parse_expr.c). The engine reads `*conf->variable` via `get` and
    // writes it on `SET` via `set`; both land on this crate's backing cell.
    backend_utils_misc_guc_tables::vars::Transform_null_equals.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: transform_null_equals_storage::get,
            set: transform_null_equals_storage::set,
        },
    );
}

#[cfg(test)]
mod tests;
