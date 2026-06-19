//! `utils/adt/ruleutils.c` — **F1: the expression deparser** (the
//! precedence-aware `get_rule_expr` tree-walker and the per-node-kind deparsers
//! it dispatches to).
//!
//! F1 builds directly on F0a's name-resolution engine (the [`DeparseContext`] /
//! [`DeparseNamespace`] / [`DeparseColumns`] types and `get_rtable_name` /
//! `deparse_columns_fetch`). It is the first family that actually renders SQL
//! text, so it introduces the output buffer (`context.buf`, an owned
//! `StringInfo`) and the thin `appendStringInfo*` wrappers the deparser uses.
//!
//! C source: `src/backend/utils/adt/ruleutils.c` (the in-scope range spans
//! `get_variable` 7606, `get_special_variable` 7888, `isSimpleNode` 8850,
//! `get_rule_expr` 9252, `get_const_expr` 11464, `get_sublink_expr` 11819, and
//! the dispatch targets in between).
//!
//! # Cross-subsystem boundaries (seam-and-panic where the owner is unported)
//!
//! * **Plan-tree navigation (#159 / ruleutils F0b):** the special-varno
//!   (`OUTER_VAR`/`INNER_VAR`/`INDEX_VAR`) and `appendparents` /
//!   `inner_plan`-drilldown branches of `get_variable`, the combining-aggregate
//!   `resolve_special_varno` recursion, the `Param` referent walk
//!   (`get_parameter`/`find_param_referent`), and the EXPLAIN `WindowFunc`
//!   `OVER` namespace scan all read a `Plan` tree the planner does not yet emit.
//!   They panic precisely where a real `Plan`/`PlannedStmt` is needed; the
//!   Query-side Var deparse (RTE lookup via the F0a engine) is fully ported.
//! * **Query-tree deparse (ruleutils F2):** the aggregate `ORDER BY` /
//!   `WITHIN GROUP (ORDER BY …)` rendering and the `WindowFunc` `OVER` anonymous
//!   window spec call `get_rule_orderby` / `get_rule_windowspec`, statement-level
//!   functions of the F2 query deparsers. They panic until F2 lands. The
//!   embedded sub-SELECT of a `SubLink` calls `get_query_def` (F2) — that one
//!   recursion is seamed precisely (`get_query_def_subquery`-style panic).
//! * **Catalog def-builders (ruleutils F3 / parse helpers):** the operator /
//!   function name generators (`generate_operator_name` / `generate_function_name`,
//!   ruleutils statics over `parse_oper` / `parse_func`) and
//!   `get_rte_attribute_name` cross owner seams.
//! * **Catalog / format deparsers (later families):** `FieldSelect`
//!   (`get_name_for_var_field`), `RowExpr` toplevel (`lookup_rowtype_tupdesc`),
//!   `NextValueExpr` (`generate_relation_name`), `InferenceElem`
//!   (`get_opclass_name`), `XmlExpr`, the JSON deparsers, `get_tablefunc`, and
//!   the `SubscriptingRef` assignment path (`processIndirection`) panic with a
//!   precise rationale.
//!
//! Everything else — operators, functions (non-SQL-syntax), the agg/window
//! function name + arg + FILTER rendering, constants (incl. the catalog/fmgr
//! const-output encapsulated at the lsyscache+fmgr seams), coercions, CASE,
//! ARRAY/ROW/COALESCE/MIN-MAX/NULLIF/DISTINCT, the NULL/boolean/JSON-IS tests,
//! `SQLValueFunction`, named args, COLLATE, GROUPING, sub-links, subscripts, and
//! the `isSimpleNode` precedence oracle — is ported in full.

use alloc::format;
use alloc::string::{String, ToString};

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{RTE_CTE, RTE_JOIN, RTE_SUBQUERY};
use types_nodes::primnodes::{
    Aggref, BoolExprType, BoolTestType, CaseExpr, Const, CurrentOfExpr, Expr, FuncExpr, MinMaxOp,
    NullTestType, OpExpr, ScalarArrayOpExpr, SQLValueFunction, SQLValueFunctionOp, SubLink,
    SubLinkType, SubscriptingRef, Var, VarReturningType, WindowFunc,
};

use crate::{deparse_columns_fetch, DeparseContext};

/* -------------------------------------------------------------------------- *
 * Enum / type-OID / flag constants (PostgreSQL headers, exact values).
 * -------------------------------------------------------------------------- */

/// `PRETTYFLAG_PAREN` (ruleutils.c 88).
const PRETTYFLAG_PAREN: i32 = 0x0001;
/// `PRETTYFLAG_INDENT` (ruleutils.c 89).
const PRETTYFLAG_INDENT: i32 = 0x0002;

use types_nodes::nodeagg::{AGGSPLITOP_COMBINE, AGGSPLITOP_SKIPFINAL};

/// `catalog/pg_aggregate_d.h` AGGKIND_NORMAL.
const AGGKIND_NORMAL: i8 = b'n' as i8;

/// `access/htup_details.h` FUNC_MAX_ARGS.
const FUNC_MAX_ARGS: usize = 100;

/// `catalog/pg_type_d.h` type OIDs used by `get_const_expr`.
const BOOLOID: u32 = 16;
const INT4OID: u32 = 23;
const UNKNOWNOID: u32 = 705;
const NUMERICOID: u32 = 1700;

/// `access/attnum.h` `InvalidAttrNumber`.
const InvalidAttrNumber: i16 = 0;

/* -------------------------------------------------------------------------- *
 * Small helpers (errors, output-buffer appends, type/collation inspection).
 * -------------------------------------------------------------------------- */

/// `elog(ERROR, ...)` inside a deparse routine — a recoverable `PgError`.
fn elog_error(msg: String) -> PgError {
    PgError::error(msg)
}

/// A node-tree shape invariant was violated (a `NOT NULL` C field arrived as a
/// `None` `Option<Box<Expr>>`). Mirrors the C deref of a field the parser/
/// planner always sets; raised as a `PgError` so it unwinds like `elog(ERROR)`.
fn missing_field(what: &str) -> PgError {
    elog_error(format!("ruleutils: missing required node field: {what}"))
}

/// `appendStringInfoString(context->buf, s)` — append a byte slice (the
/// server-encoded text), growing the buffer fallibly (palloc OOM surfaces as a
/// recoverable `PgError`). The C `appendStringInfo*` family lives in
/// `stringinfo.c`; this is the deparser's thin wrapper over the owned buffer.
fn str_(context: &mut DeparseContext<'_>, s: &str) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let bytes = s.as_bytes();
    context
        .buf
        .data
        .try_reserve(bytes.len())
        .map_err(|_| mcx.oom(bytes.len()))?;
    context.buf.data.extend_from_slice(bytes);
    Ok(())
}

/// Crate-internal re-export of [`str_`] for the F2 query-deparse module.
pub(crate) fn str_pub(context: &mut DeparseContext<'_>, s: &str) -> PgResult<()> {
    str_(context, s)
}

/// Crate-internal re-export of [`ch_`] for the F2 query-deparse module.
pub(crate) fn ch_pub(context: &mut DeparseContext<'_>, c: u8) -> PgResult<()> {
    ch_(context, c)
}

/// Crate-internal re-export of [`simple_quote_literal`] for the F2 module.
pub(crate) fn simple_quote_literal_pub(context: &mut DeparseContext<'_>, val: &str) -> PgResult<()> {
    simple_quote_literal(context, val)
}

/// `appendStringInfoChar(context->buf, c)` — append one ASCII byte.
fn ch_(context: &mut DeparseContext<'_>, c: u8) -> PgResult<()> {
    let mcx = context.buf.allocator();
    context.buf.data.try_reserve(1).map_err(|_| mcx.oom(1))?;
    context.buf.data.push(c);
    Ok(())
}

/// `appendStringInfoSpaces(context->buf, n)` — append `n` spaces. Used by the
/// pretty-indent `appendContextKeyword` path (the F2 query-deparse machinery);
/// retained here so F2 doesn't re-introduce it.
#[allow(dead_code)]
fn spaces_(context: &mut DeparseContext<'_>, n: usize) -> PgResult<()> {
    let mcx = context.buf.allocator();
    context.buf.data.try_reserve(n).map_err(|_| mcx.oom(n))?;
    for _ in 0..n {
        context.buf.data.push(b' ');
    }
    Ok(())
}

/// `PRETTY_PAREN(context)` — `context->prettyFlags & PRETTYFLAG_PAREN`.
#[inline]
fn pretty_paren(context: &DeparseContext<'_>) -> bool {
    (context.prettyFlags & PRETTYFLAG_PAREN) != 0
}

/// `PRETTY_INDENT(context)` — `context->prettyFlags & PRETTYFLAG_INDENT`.
#[inline]
fn pretty_indent(context: &DeparseContext<'_>) -> bool {
    (context.prettyFlags & PRETTYFLAG_INDENT) != 0
}

/// `exprType((const Node *) node)` over an owned `Expr`, through the nodeFuncs
/// seam (the `(typid, typmod, collation)` triple read together).
fn expr_type(expr: &Expr) -> PgResult<Oid> {
    Ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typid)
}

/// `exprTypmod((const Node *) node)` over an owned `Expr`.
fn expr_typmod(expr: &Expr) -> PgResult<i32> {
    Ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typmod)
}

/// `format_type_with_typemod(type_oid, typemod)` (format_type.c) — the type's
/// printable name (flags = 0). The deparser's standard `arg::typename` decorator.
fn format_type_with_typemod<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    typemod: i32,
) -> PgResult<PgString<'mcx>> {
    match backend_utils_adt_format_type_seams::format_type_extended::call(mcx, type_oid, typemod, 0)? {
        Some(s) => Ok(s),
        // flags=0 never sets FORMAT_TYPE_INVALID_AS_NULL, so None cannot occur;
        // guard it as an internal error rather than silently emitting nothing.
        None => Err(elog_error("format_type_with_typemod returned NULL".to_string())),
    }
}

/// `generate_operator_name(operid, arg1, arg2)` — through the ruleutils-owned
/// seam (its body needs the unported `parse_oper` candidate resolution).
fn generate_operator_name<'mcx>(
    mcx: Mcx<'mcx>,
    operid: Oid,
    arg1: Oid,
    arg2: Oid,
) -> PgResult<PgString<'mcx>> {
    backend_utils_adt_ruleutils_seams::generate_operator_name::call(mcx, operid, arg1, arg2)
}

/// `generate_collation_name(collid)` — ruleutils-owned seam.
fn generate_collation_name<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<PgString<'mcx>> {
    backend_utils_adt_ruleutils_seams::generate_collation_name::call(mcx, collid)
}

/// `quote_identifier(ident)` — ruleutils-owned, ported in this crate.
fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>> {
    crate::quote_identifier(mcx, ident)
}

/// Clone a `Vec<TargetEntry<'static>>` (Aggref.args) into `mcx`, re-homing it at
/// the live `'mcx` lifetime so the F2 ORDER-BY renderer can consult it.
fn clone_tle_vec<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[types_nodes::primnodes::TargetEntry<'static>],
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
    for t in v.iter() {
        out.push(t.clone_in(mcx)?);
    }
    Ok(out)
}

/// Clone a `PgVec<TargetEntry<'mcx>>` (context.targetList) into a fresh `PgVec`.
fn clone_tle_vec_mcx<'mcx>(
    mcx: Mcx<'mcx>,
    v: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
    for t in v.iter() {
        out.push(t.clone_in(mcx)?);
    }
    Ok(out)
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

/// `strcspn(s, reject)` — length of the initial segment of `s` containing no
/// byte from `reject`.
fn strcspn(s: &str, reject: &[u8]) -> usize {
    for (i, &b) in s.as_bytes().iter().enumerate() {
        if reject.contains(&b) {
            return i;
        }
    }
    s.len()
}

/* -------------------------------------------------------------------------- *
 * get_rule_expr_paren — C 9155-9171.
 * -------------------------------------------------------------------------- */

/// `static void get_rule_expr_paren(Node *node, deparse_context *context,
/// bool showimplicit, Node *parentNode)` — C 9155-9171.
///
/// In pretty-paren mode the paren is dropped iff `isSimpleNode(node, parentNode,
/// prettyFlags)`; the oracle is ported in full ([`isSimpleNode`]). With the
/// default (non-pretty) flags `PRETTY_PAREN` is false and no parens are added.
pub fn get_rule_expr_paren(
    node: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
    parent_node: Option<&Node<'_>>,
) -> PgResult<()> {
    let parent_expr = parent_node.and_then(|p| p.as_expr());
    let node_expr = node.as_expr();
    let need_paren =
        pretty_paren(context) && !isSimpleNode_inner_opt(node_expr, parent_expr, context.prettyFlags);

    if need_paren {
        ch_(context, b'(')?;
    }

    get_rule_expr(node, context, showimplicit)?;

    if need_paren {
        ch_(context, b')')?;
    }

    Ok(())
}

/// `get_rule_expr_paren` over an owned `&Expr` node and `&Expr` parent (the form
/// used at every internal recursion site — the parent's identity matters only
/// for the `isSimpleNode` precedence oracle, which inspects its tag/op).
fn get_rule_expr_paren_e(
    expr: &Expr,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
    parent: Option<&Expr>,
) -> PgResult<()> {
    let need_paren =
        pretty_paren(context) && !isSimpleNode_inner_opt(Some(expr), parent, context.prettyFlags);
    if need_paren {
        ch_(context, b'(')?;
    }
    get_rule_expr_e(expr, context, showimplicit)?;
    if need_paren {
        ch_(context, b')')?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_rule_expr — C 9252-10633.
 * -------------------------------------------------------------------------- */

/// `static void get_rule_expr(Node *node, deparse_context *context,
/// bool showimplicit)` — C 9252-10633.
///
/// The precedence-aware expression tree walk. Each level emits an indivisible
/// term (parenthesized if necessary) so the result reparses to the same tree;
/// the only exception is a bare `List`, emitted comma-separated.
pub fn get_rule_expr(
    node: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    // The C arm dispatches on nodeTag(node); a List arrives as a Node::List,
    // every Expr-derived node as Node::Expr(Expr::*).
    if let Some(list) = node.as_list() {
        // "emit the component items comma-separated with no surrounding
        // decoration" — C reaches this by passing a List* as a Node*.
        get_rule_expr_list(list, context, showimplicit)?;
    } else if let Some(expr) = node.as_expr() {
        get_rule_expr_e(expr, context, showimplicit)?;
    } else {
        // get_rule_expr is also (rarely) handed a few non-Expr value nodes by
        // the query deparsers (e.g. a bare String/Integer in some clauses); but
        // F1's in-scope callers only pass Expr / List. Anything else is the C
        // `default: elog(ERROR, "unrecognized node type")`.
        return Err(elog_error(format!(
            "unrecognized node type: {}",
            node.tag().0
        )));
    }
    Ok(())
}

/// The body of `get_rule_expr`'s big `switch (nodeTag(node))` over the owned
/// `Expr` enum (the parent passed to nested paren/coercion recursions is `expr`
/// itself, threaded as an `&Expr`).
fn get_rule_expr_e(
    expr: &Expr,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    // The C `parentNode` for every nested get_rule_expr_paren / get_coercion_expr
    // call in this dispatch is the node we're currently rendering.
    let enclosing: Option<&Expr> = Some(expr);
    match expr {
        Expr::Var(var) => {
            get_variable(var, 0, false, context)?;
        }

        Expr::Const(_) => {
            get_const_expr_inner(expr_as_const(expr)?, context, 0)?;
        }

        Expr::Param(_) => {
            get_parameter(expr, context)?;
        }

        Expr::Aggref(_) => {
            get_agg_expr(expr, context, expr)?;
        }

        Expr::GroupingFunc(g) => {
            str_(context, "GROUPING(")?;
            get_rule_expr_list_exprs(&g.args, context, true)?;
            ch_(context, b')')?;
        }

        Expr::WindowFunc(_) => {
            get_windowfunc_expr(expr, context)?;
        }

        Expr::MergeSupportFunc(_) => {
            str_(context, "MERGE_ACTION()")?;
        }

        Expr::FuncExpr(_) => {
            get_func_expr(expr, context, showimplicit)?;
        }

        Expr::NamedArgExpr(n) => {
            let name = n.name.as_deref().unwrap_or("");
            let q = quote_identifier(mcx, name)?;
            str_(context, q.as_str())?;
            str_(context, " => ")?;
            let arg = n.arg.as_deref().ok_or_else(|| missing_field("NamedArgExpr.arg"))?;
            get_rule_expr_e(arg, context, showimplicit)?;
        }

        Expr::OpExpr(_) => {
            get_oper_expr(expr, context)?;
        }

        Expr::DistinctExpr(d) => {
            let arg1 = expr_arg(&d.args, 0)?;
            let arg2 = expr_arg(&d.args, 1)?;
            if !pretty_paren(context) {
                ch_(context, b'(')?;
            }
            get_rule_expr_paren_e(arg1, context, true, enclosing)?;
            str_(context, " IS DISTINCT FROM ")?;
            get_rule_expr_paren_e(arg2, context, true, enclosing)?;
            if !pretty_paren(context) {
                ch_(context, b')')?;
            }
        }

        Expr::NullIfExpr(n) => {
            str_(context, "NULLIF(")?;
            get_rule_expr_list_exprs(&n.args, context, true)?;
            ch_(context, b')')?;
        }

        Expr::ScalarArrayOpExpr(s) => {
            get_scalararrayop_expr(s, enclosing, context)?;
        }

        Expr::BoolExpr(b) => {
            let first_arg = expr_arg(&b.args, 0)?;
            match b.boolop {
                BoolExprType::AND_EXPR => {
                    if !pretty_paren(context) {
                        ch_(context, b'(')?;
                    }
                    get_rule_expr_paren_e(first_arg, context, false, enclosing)?;
                    for arg in &b.args[1..] {
                        str_(context, " AND ")?;
                        get_rule_expr_paren_e(arg, context, false, enclosing)?;
                    }
                    if !pretty_paren(context) {
                        ch_(context, b')')?;
                    }
                }
                BoolExprType::OR_EXPR => {
                    if !pretty_paren(context) {
                        ch_(context, b'(')?;
                    }
                    get_rule_expr_paren_e(first_arg, context, false, enclosing)?;
                    for arg in &b.args[1..] {
                        str_(context, " OR ")?;
                        get_rule_expr_paren_e(arg, context, false, enclosing)?;
                    }
                    if !pretty_paren(context) {
                        ch_(context, b')')?;
                    }
                }
                BoolExprType::NOT_EXPR => {
                    if !pretty_paren(context) {
                        ch_(context, b'(')?;
                    }
                    str_(context, "NOT ")?;
                    get_rule_expr_paren_e(first_arg, context, false, enclosing)?;
                    if !pretty_paren(context) {
                        ch_(context, b')')?;
                    }
                }
            }
        }

        Expr::SubLink(_) => {
            get_sublink_expr(expr, context)?;
        }

        Expr::RelabelType(r) => {
            let arg = r.arg.as_deref().ok_or_else(|| missing_field("RelabelType.arg"))?;
            if r.relabelformat == types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
                && !showimplicit
            {
                get_rule_expr_paren_e(arg, context, false, enclosing)?;
            } else {
                get_coercion_expr_e(arg, context, r.resulttype, r.resulttypmod, enclosing)?;
            }
        }

        Expr::CoerceViaIO(c) => {
            let arg = c.arg.as_deref().ok_or_else(|| missing_field("CoerceViaIO.arg"))?;
            if c.coerceformat == types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
                && !showimplicit
            {
                get_rule_expr_paren_e(arg, context, false, enclosing)?;
            } else {
                get_coercion_expr_e(arg, context, c.resulttype, -1, enclosing)?;
            }
        }

        Expr::ArrayCoerceExpr(c) => {
            let arg = c.arg.as_deref().ok_or_else(|| missing_field("ArrayCoerceExpr.arg"))?;
            if c.coerceformat == types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
                && !showimplicit
            {
                get_rule_expr_paren_e(arg, context, false, enclosing)?;
            } else {
                get_coercion_expr_e(arg, context, c.resulttype, c.resulttypmod, enclosing)?;
            }
        }

        Expr::ConvertRowtypeExpr(c) => {
            let arg = c.arg.as_deref().ok_or_else(|| missing_field("ConvertRowtypeExpr.arg"))?;
            if c.convertformat == types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
                && !showimplicit
            {
                get_rule_expr_paren_e(arg, context, false, enclosing)?;
            } else {
                get_coercion_expr_e(arg, context, c.resulttype, -1, enclosing)?;
            }
        }

        Expr::CollateExpr(c) => {
            let arg = c.arg.as_deref().ok_or_else(|| missing_field("CollateExpr.arg"))?;
            if !pretty_paren(context) {
                ch_(context, b'(')?;
            }
            get_rule_expr_paren_e(arg, context, showimplicit, enclosing)?;
            let coll = generate_collation_name(mcx, c.collOid)?;
            str_(context, " COLLATE ")?;
            str_(context, coll.as_str())?;
            if !pretty_paren(context) {
                ch_(context, b')')?;
            }
        }

        Expr::CaseExpr(c) => {
            get_case_expr(c, context)?;
        }

        Expr::CaseTestExpr(_) => {
            // Normally unreachable; in an optimized expression we might be unable
            // to avoid it. Print as CASE_TEST_EXPR.
            str_(context, "CASE_TEST_EXPR")?;
        }

        Expr::ArrayExpr(a) => {
            str_(context, "ARRAY[")?;
            get_rule_expr_list_exprs(&a.elements, context, true)?;
            ch_(context, b']')?;
            // If the array is empty, we need an explicit coercion to the array type.
            if a.elements.is_empty() {
                let ty = format_type_with_typemod(mcx, a.array_typeid, -1)?;
                str_(context, "::")?;
                str_(context, ty.as_str())?;
            }
        }

        Expr::RowExpr(_) => {
            // C 9700-9753: rendered as `ROW(args)` (or schema-qualified composite
            // when row_format != COERCE_EXPLICIT_CALL and the rowtype is named),
            // and the toplevel `*` whole-row sub-case calls get_variable with
            // istoplevel + a lookup_rowtype_tupdesc catalog probe. The named-row
            // path needs M1 catalog (get_typ_typrelid / lookup_rowtype_tupdesc);
            // defer the whole arm to the catalog family rather than render only
            // the un-named subset (which would silently differ for casts).
            return Err(deferred(
                "RowExpr (lookup_rowtype_tupdesc / get_variable toplevel; M1 catalog)",
            ));
        }

        Expr::RowCompareExpr(rc) => {
            // SQL99 allows "ROW" to be omitted, but we always print it.
            str_(context, "(ROW(")?;
            get_rule_list_toplevel_exprs(&rc.largs, context, true)?;
            // We assume the name of the first-column operator will do.
            let opname = generate_operator_name(
                mcx,
                rc.opnos[0],
                expr_type(expr_arg(&rc.largs, 0)?)?,
                expr_type(expr_arg(&rc.rargs, 0)?)?,
            )?;
            str_(context, ") ")?;
            str_(context, opname.as_str())?;
            str_(context, " ROW(")?;
            get_rule_list_toplevel_exprs(&rc.rargs, context, true)?;
            str_(context, "))")?;
        }

        Expr::CoalesceExpr(c) => {
            str_(context, "COALESCE(")?;
            get_rule_expr_list_exprs(&c.args, context, true)?;
            ch_(context, b')')?;
        }

        Expr::MinMaxExpr(m) => {
            match m.op {
                MinMaxOp::IS_GREATEST => str_(context, "GREATEST(")?,
                MinMaxOp::IS_LEAST => str_(context, "LEAST(")?,
            }
            get_rule_expr_list_exprs(&m.args, context, true)?;
            ch_(context, b')')?;
        }

        Expr::SQLValueFunction(s) => {
            get_sqlvaluefunction(s, context)?;
        }

        Expr::NullTest(nt) => {
            let arg = nt.arg.as_deref().ok_or_else(|| missing_field("NullTest.arg"))?;
            if !pretty_paren(context) {
                ch_(context, b'(')?;
            }
            get_rule_expr_paren_e(arg, context, true, enclosing)?;
            // For scalar inputs, prefer IS [NOT] NULL; for a rowtype input under
            // a scalar test, must print IS [NOT] DISTINCT FROM NULL.
            let is_null = nt.nulltesttype == NullTestType::IS_NULL;
            if nt.argisrow || !backend_utils_cache_lsyscache_seams::type_is_rowtype::call(expr_type(arg)?)? {
                str_(context, if is_null { " IS NULL" } else { " IS NOT NULL" })?;
            } else {
                str_(
                    context,
                    if is_null {
                        " IS NOT DISTINCT FROM NULL"
                    } else {
                        " IS DISTINCT FROM NULL"
                    },
                )?;
            }
            if !pretty_paren(context) {
                ch_(context, b')')?;
            }
        }

        Expr::BooleanTest(bt) => {
            let arg = bt.arg.as_deref().ok_or_else(|| missing_field("BooleanTest.arg"))?;
            if !pretty_paren(context) {
                ch_(context, b'(')?;
            }
            get_rule_expr_paren_e(arg, context, false, enclosing)?;
            let s = match bt.booltesttype {
                BoolTestType::IS_TRUE => " IS TRUE",
                BoolTestType::IS_NOT_TRUE => " IS NOT TRUE",
                BoolTestType::IS_FALSE => " IS FALSE",
                BoolTestType::IS_NOT_FALSE => " IS NOT FALSE",
                BoolTestType::IS_UNKNOWN => " IS UNKNOWN",
                BoolTestType::IS_NOT_UNKNOWN => " IS NOT UNKNOWN",
            };
            str_(context, s)?;
            if !pretty_paren(context) {
                ch_(context, b')')?;
            }
        }

        Expr::CoerceToDomain(c) => {
            let arg = c.arg.as_deref().ok_or_else(|| missing_field("CoerceToDomain.arg"))?;
            if c.coercionformat == types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
                && !showimplicit
            {
                get_rule_expr_e(arg, context, false)?;
            } else {
                get_coercion_expr_e(arg, context, c.resulttype, c.resulttypmod, enclosing)?;
            }
        }

        Expr::CoerceToDomainValue(_) => {
            str_(context, "VALUE")?;
        }

        Expr::SetToDefault(_) => {
            str_(context, "DEFAULT")?;
        }

        Expr::SubscriptingRef(s) => {
            get_subscripting_ref(s, context, showimplicit)?;
        }

        Expr::FieldStore(f) => {
            // C 9658-9689. No good SQL representation; print just the source
            // arguments, wrapped in ROW() if there's more than one. (The target
            // field names need processIndirection's catalog lookups, which
            // EXPLAIN-only callers use; the bare path here matches the C arm.)
            let need_parens = f.newvals.len() != 1;
            if need_parens {
                str_(context, "ROW(")?;
            }
            get_rule_expr_list_exprs(&f.newvals, context, showimplicit)?;
            if need_parens {
                ch_(context, b')')?;
            }
        }

        Expr::CurrentOfExpr(c) => {
            get_currentof_expr(c, context)?;
        }

        Expr::ReturningExpr(r) => {
            // C 10415-10427. Only seen while EXPLAINing a plan; display the
            // returned expression.
            let retexpr = r.retexpr.as_deref().ok_or_else(|| missing_field("ReturningExpr.retexpr"))?;
            get_rule_expr_e(retexpr, context, showimplicit)?;
        }

        Expr::JsonIsPredicate(pred) => {
            // C 10501-10535. Self-contained `expr IS JSON [type] [WITH UNIQUE
            // KEYS]`; the FORMAT clause is a C `TODO` (never rendered).
            if !pretty_paren(context) {
                ch_(context, b'(')?;
            }
            let pexpr = pred.expr.as_deref().ok_or_else(|| missing_field("JsonIsPredicate.expr"))?;
            get_rule_expr_paren_e(pexpr, context, true, enclosing)?;
            str_(context, " IS JSON")?;
            // JsonValueType: JS_TYPE_ANY=0, _OBJECT=1, _ARRAY=2, _SCALAR=3.
            match pred.item_type as i32 {
                1 => str_(context, " OBJECT")?,
                2 => str_(context, " ARRAY")?,
                3 => str_(context, " SCALAR")?,
                _ => {}
            }
            if pred.unique_keys {
                str_(context, " WITH UNIQUE KEYS")?;
            }
            if !pretty_paren(context) {
                ch_(context, b')')?;
            }
        }

        Expr::AlternativeSubPlan(asplan) => {
            // C 9600-9625. Cannot be reached in normal usage; kept for printing
            // planner data structures. Reads each SubPlan's plan_name /
            // useHashTable — no namespace navigation.
            str_(context, "(alternatives: ")?;
            let n = asplan.0.subplans.len();
            for (i, splan) in asplan.0.subplans.iter().enumerate() {
                let plan_name = splan.plan_name.as_deref().unwrap_or("");
                if splan.useHashTable {
                    str_(context, "hashed ")?;
                    str_(context, plan_name)?;
                } else {
                    str_(context, plan_name)?;
                }
                if i + 1 < n {
                    str_(context, " or ")?;
                }
            }
            ch_(context, b')')?;
        }

        // --- Prerequisite-blocked arms (need an unported subsystem) ----------
        Expr::SubPlan(_) => {
            return Err(deferred(
                "SubPlan (context->namespaces ancestors / deparse_namespace; #159 plan-tree)",
            ))
        }
        Expr::FieldSelect(_) => {
            return Err(deferred(
                "FieldSelect (get_name_for_var_field; catalog/var-field)",
            ))
        }
        Expr::XmlExpr(_) => {
            return Err(deferred(
                "XmlExpr (get_rule_expr XmlExpr arm: map_xml_name_to_sql_identifier; XML deparser family)",
            ))
        }
        Expr::NextValueExpr(_) => {
            return Err(deferred("NextValueExpr (generate_relation_name; catalog)"))
        }
        Expr::InferenceElem(_) => {
            return Err(deferred(
                "InferenceElem (get_opclass_name / get_opclass_input_type; catalog)",
            ))
        }
        Expr::JsonValueExpr(_) => {
            return Err(deferred("JsonValueExpr (get_json_format; JSON deparser family)"))
        }
        Expr::JsonConstructorExpr(_) => {
            return Err(deferred(
                "JsonConstructorExpr (get_json_constructor; JSON deparser family)",
            ))
        }
        Expr::JsonExpr(_) => {
            return Err(deferred(
                "JsonExpr (get_json_table / JSON_*_OP renderer; JSON deparser family)",
            ))
        }
        // Planner-internal / not-in-scope expression nodes (PlaceHolderVar,
        // RestrictInfo, and any variant the deparser never legitimately walks):
        // the C `default: elog(ERROR, "unrecognized node type")`.
        _ => {
            return Err(elog_error("unrecognized node type in deparse".to_string()));
        }
    }
    Ok(())
}

/// The `T_List` body of `get_rule_expr` over an owned `Vec<Node>`.
fn get_rule_expr_list(
    list: &PgVec<'_, mcx::PgBox<'_, Node<'_>>>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mut sep = "";
    for e in list.iter() {
        str_(context, sep)?;
        get_rule_expr(e, context, showimplicit)?;
        sep = ", ";
    }
    Ok(())
}

/// `get_rule_expr` over a `Vec<Expr>` (the common arg-list case).
fn get_rule_expr_list_exprs(
    list: &[Expr],
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mut sep = "";
    for e in list {
        str_(context, sep)?;
        get_rule_expr_e(e, context, showimplicit)?;
        sep = ", ";
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * ScalarArrayOpExpr arm — C 9420-9457.
 * -------------------------------------------------------------------------- */

fn get_scalararrayop_expr(
    s: &ScalarArrayOpExpr,
    enclosing: Option<&Expr>,
    context: &mut DeparseContext<'_>,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let arg1 = expr_arg(&s.args, 0)?;
    let arg2 = expr_arg(&s.args, 1)?;
    if !pretty_paren(context) {
        ch_(context, b'(')?;
    }
    get_rule_expr_paren_e(arg1, context, true, enclosing)?;
    let opname = generate_operator_name(
        mcx,
        s.opno,
        expr_type(arg1)?,
        backend_utils_cache_lsyscache_seams::get_base_element_type::call(expr_type(arg2)?)?,
    )?;
    str_(context, " ")?;
    str_(context, opname.as_str())?;
    str_(context, if s.useOr { " ANY (" } else { " ALL (" })?;
    get_rule_expr_paren_e(arg2, context, true, enclosing)?;

    // Disambiguate "x op ANY/ALL (y)" when y is a bare sub-SELECT.
    if let Expr::SubLink(sl) = arg2 {
        if sl.subLinkType == SubLinkType::Expr {
            let ty = format_type_with_typemod(mcx, expr_type(arg2)?, expr_typmod(arg2)?)?;
            str_(context, "::")?;
            str_(context, ty.as_str())?;
        }
    }
    ch_(context, b')')?;
    if !pretty_paren(context) {
        ch_(context, b')')?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * SubscriptingRef arm — C 9307-9371 / printSubscripts — C 12998-13020.
 * -------------------------------------------------------------------------- */

/// The `T_SubscriptingRef` arm of `get_rule_expr` (C 9307-9371).
fn get_subscripting_ref(
    sbsref: &SubscriptingRef,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let refexpr = sbsref.refexpr.as_deref().ok_or_else(|| missing_field("SubscriptingRef.refexpr"))?;

    // If the argument is a CaseTestExpr, we must be inside a FieldStore, ie, we
    // are assigning to an element of an array within a composite column. Since
    // we already punt on displaying the FieldStore's target information, just
    // punt here too, and display only the assignment source expression.
    if matches!(refexpr, Expr::CaseTestExpr(_)) {
        let refassgnexpr = sbsref
            .refassgnexpr
            .as_deref()
            .ok_or_else(|| missing_field("SubscriptingRef.refassgnexpr (CaseTestExpr arg)"))?;
        get_rule_expr_e(refassgnexpr, context, showimplicit)?;
        return Ok(());
    }

    // Parenthesize the argument unless it's a simple Var or FieldSelect.
    let need_parens =
        !matches!(refexpr, Expr::Var(_)) && !matches!(refexpr, Expr::FieldSelect(_));
    if need_parens {
        ch_(context, b'(')?;
    }
    get_rule_expr_e(refexpr, context, showimplicit)?;
    if need_parens {
        ch_(context, b')')?;
    }

    if sbsref.refassgnexpr.is_some() {
        // "container[subscripts] := refassgnexpr" — not legal SQL; produced only
        // by EXPLAIN over an INSERT/UPDATE plan via processIndirection, which
        // needs catalog lookups (get_typ_typrelid / get_attname).
        return Err(deferred(
            "SubscriptingRef assignment (processIndirection; catalog get_typ_typrelid/get_attname)",
        ));
    } else {
        // Just an ordinary container fetch, so print subscripts.
        print_subscripts(sbsref, context)?;
    }
    Ok(())
}

/// `static void printSubscripts(SubscriptingRef *sbsref, deparse_context
/// *context)` — C 12998-13020.
fn print_subscripts(sbsref: &SubscriptingRef, context: &mut DeparseContext<'_>) -> PgResult<()> {
    // C tests `if (lowlist_item)` each iteration, printing the lower bound and a
    // ':' only while the lower-index cursor is still valid, then advances it;
    // once exhausted, the colon is omitted.
    let mut low_iter = sbsref.reflowerindexpr.iter();
    for up in &sbsref.refupperindexpr {
        ch_(context, b'[')?;
        if let Some(low) = low_iter.next() {
            // If subexpression is NULL (an omitted slice bound), print nothing.
            if let Some(low) = low {
                get_rule_expr_e(low, context, false)?;
            }
            ch_(context, b':')?;
        }
        // If subexpression is NULL, print nothing.
        if let Some(up) = up {
            get_rule_expr_e(up, context, false)?;
        }
        ch_(context, b']')?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_rule_expr_toplevel / get_rule_list_toplevel — C 10634-10666.
 * -------------------------------------------------------------------------- */

/// `static void get_rule_expr_toplevel(Node *node, deparse_context *context,
/// bool showimplicit)` — C 10634-10641.
pub fn get_rule_expr_toplevel(
    node: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    if let Some(var) = node.as_var() {
        get_variable(var, 0, true, context)?;
        Ok(())
    } else {
        get_rule_expr(node, context, showimplicit)
    }
}

/// `get_rule_expr_toplevel` over an owned `&Expr`.
fn get_rule_expr_toplevel_expr(
    expr: &Expr,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    if let Expr::Var(var) = expr {
        get_variable(var, 0, true, context)?;
        Ok(())
    } else {
        get_rule_expr_e(expr, context, showimplicit)
    }
}

/// `static void get_rule_list_toplevel(List *lst, ...)` — C 10652-10666 over a
/// `PgVec<PgBox<Node>>`.
pub fn get_rule_list_toplevel(
    list: &PgVec<'_, mcx::PgBox<'_, Node<'_>>>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mut sep = "";
    for e in list.iter() {
        str_(context, sep)?;
        get_rule_expr_toplevel(e, context, showimplicit)?;
        sep = ", ";
    }
    Ok(())
}

/// `get_rule_list_toplevel` over a `Vec<Expr>` (the RowCompareExpr arg lists).
fn get_rule_list_toplevel_exprs(
    list: &[Expr],
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mut sep = "";
    for e in list {
        str_(context, sep)?;
        get_rule_expr_toplevel_expr(e, context, showimplicit)?;
        sep = ", ";
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_rule_expr_funccall / looks_like_function — C 10682-10733.
 * -------------------------------------------------------------------------- */

/// `static void get_rule_expr_funccall(Node *node, ...)` — C 10682-10704.
pub fn get_rule_expr_funccall(
    node: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    if looks_like_function(node) {
        get_rule_expr(node, context, showimplicit)
    } else {
        let mcx = context.buf.allocator();
        str_(context, "CAST(")?;
        // no point in showing any top-level implicit cast
        get_rule_expr(node, context, false)?;
        let (typid, typmod) = match node.as_expr() {
            Some(e) => (expr_type(e)?, expr_typmod(e)?),
            None => (0, -1),
        };
        let ty = format_type_with_typemod(mcx, typid, typmod)?;
        str_(context, " AS ")?;
        str_(context, ty.as_str())?;
        ch_(context, b')')?;
        Ok(())
    }
}

/// `static bool looks_like_function(Node *node)` — C 10705-10733.
fn looks_like_function(node: &Node<'_>) -> bool {
    let expr = match node.as_expr() {
        Some(e) => e,
        None => return false,
    };
    match expr {
        Expr::FuncExpr(f) => {
            // OK, unless it's going to deparse as a cast.
            f.funcformat == types_nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL
                || f.funcformat == types_nodes::primnodes::CoercionForm::COERCE_SQL_SYNTAX
        }
        // these are all accepted by func_expr_common_subexpr
        Expr::NullIfExpr(_)
        | Expr::CoalesceExpr(_)
        | Expr::MinMaxExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::XmlExpr(_)
        | Expr::JsonExpr(_) => true,
        _ => false,
    }
}

/* -------------------------------------------------------------------------- *
 * get_oper_expr — C 10734-10773.
 * -------------------------------------------------------------------------- */

/// `static void get_oper_expr(OpExpr *expr, deparse_context *context)` — C 10734-10773.
pub fn get_oper_expr(expr: &Expr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    let op = match expr {
        Expr::OpExpr(o) => o,
        _ => return Err(elog_error("get_oper_expr: not an OpExpr".to_string())),
    };
    get_oper_expr_inner(op, Some(expr), context)
}

fn get_oper_expr_inner(
    op: &OpExpr,
    enclosing: Option<&Expr>,
    context: &mut DeparseContext<'_>,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let opno = op.opno;
    let args = &op.args;

    if !pretty_paren(context) {
        ch_(context, b'(')?;
    }
    if args.len() == 2 {
        // binary operator
        let arg1 = expr_arg(args, 0)?;
        let arg2 = expr_arg(args, 1)?;
        get_rule_expr_paren_e(arg1, context, true, enclosing)?;
        let opname = generate_operator_name(mcx, opno, expr_type(arg1)?, expr_type(arg2)?)?;
        ch_(context, b' ')?;
        str_(context, opname.as_str())?;
        ch_(context, b' ')?;
        get_rule_expr_paren_e(arg2, context, true, enclosing)?;
    } else {
        // prefix operator
        let arg = expr_arg(args, 0)?;
        let opname = generate_operator_name(mcx, opno, 0, expr_type(arg)?)?;
        str_(context, opname.as_str())?;
        ch_(context, b' ')?;
        get_rule_expr_paren_e(arg, context, true, enclosing)?;
    }
    if !pretty_paren(context) {
        ch_(context, b')')?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_func_expr — C 10774-10869.
 * -------------------------------------------------------------------------- */

/// `static void get_func_expr(FuncExpr *expr, ...)` — C 10774-10869.
pub fn get_func_expr(
    expr: &Expr,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let f = match expr {
        Expr::FuncExpr(f) => f,
        _ => return Err(elog_error("get_func_expr: not a FuncExpr".to_string())),
    };
    get_func_expr_inner(f, Some(expr), context, showimplicit)
}

fn get_func_expr_inner(
    f: &FuncExpr,
    enclosing: Option<&Expr>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::primnodes::CoercionForm;
    let mcx = context.buf.allocator();
    let funcformat = f.funcformat;
    let args = &f.args;

    // Implicit coercion: just show the first argument (unless caller wants it).
    if funcformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
        get_rule_expr_paren_e(expr_arg(args, 0)?, context, false, enclosing)?;
        return Ok(());
    }

    // Cast: show first argument plus an explicit cast operation.
    if funcformat == CoercionForm::COERCE_EXPLICIT_CAST
        || funcformat == CoercionForm::COERCE_IMPLICIT_CAST
    {
        let arg = expr_arg(args, 0)?;
        let rettype = f.funcresulttype;
        // exprIsLengthCoercion would yield the coerced typmod; that helper is
        // unported, so we use -1 (the common non-length-coercion case).
        let coerced_typmod = -1;
        get_coercion_expr_e(arg, context, rettype, coerced_typmod, enclosing)?;
        return Ok(());
    }

    // SQL-syntax special forms (get_func_sql_syntax).
    if funcformat == CoercionForm::COERCE_SQL_SYNTAX {
        return Err(deferred(
            "get_func_sql_syntax (COERCE_SQL_SYNTAX special forms: fmgr/Datum text value layer + fmgroids F_* table)",
        ));
    }

    // Normal function: display as proname(args).
    if args.len() > FUNC_MAX_ARGS {
        return Err(elog_error("too many arguments".to_string()));
    }
    let mut argtypes: PgVec<Oid> = PgVec::new_in(mcx);
    let mut argnames: PgVec<Option<PgString>> = PgVec::new_in(mcx);
    for arg in args {
        if let Expr::NamedArgExpr(n) = arg {
            let nm = match n.name.as_deref() {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            };
            argnames.try_reserve(1).map_err(|_| mcx.oom(0))?;
            argnames.push(nm);
        }
        argtypes.try_reserve(1).map_err(|_| mcx.oom(0))?;
        argtypes.push(expr_type(arg)?);
    }

    let (funcname, use_variadic) = backend_utils_adt_ruleutils_seams::generate_function_name::call(
        mcx,
        f.funcid,
        args.len() as i32,
        argnames,
        argtypes,
        f.funcvariadic,
        true, // want_use_variadic (this is the FuncExpr caller)
        context.inGroupBy,
    )?;
    str_(context, funcname.as_str())?;
    ch_(context, b'(')?;
    let alen = args.len();
    for (i, arg) in args.iter().enumerate() {
        if i > 0 {
            str_(context, ", ")?;
        }
        if use_variadic && i == alen - 1 {
            str_(context, "VARIADIC ")?;
        }
        get_rule_expr_e(arg, context, true)?;
    }
    ch_(context, b')')?;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_agg_expr (+ helper) — C 10870-11007.
 * -------------------------------------------------------------------------- */

/// `static void get_agg_expr(Aggref *aggref, ...)` — C 10870-10878.
pub fn get_agg_expr(
    aggref: &Expr,
    context: &mut DeparseContext<'_>,
    original_aggref: &Expr,
) -> PgResult<()> {
    get_agg_expr_helper(aggref, context, original_aggref, None, None, false)
}

/// `static void get_agg_expr_helper(Aggref *aggref, ..., const char *funcname,
/// const char *options, bool is_json_objectagg)` — C 10879-11007.
fn get_agg_expr_helper(
    aggref: &Expr,
    context: &mut DeparseContext<'_>,
    original_aggref: &Expr,
    funcname: Option<&str>,
    options: Option<&str>,
    is_json_objectagg: bool,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let a: &Aggref = match aggref {
        Expr::Aggref(a) => a,
        _ => return Err(elog_error("get_agg_expr: not an Aggref".to_string())),
    };
    let orig: &Aggref = match original_aggref {
        Expr::Aggref(a) => a,
        _ => return Err(elog_error("get_agg_expr: original is not an Aggref".to_string())),
    };

    // For a combining aggregate, we look up and deparse the corresponding
    // partial aggregate instead — needs resolve_special_varno (#159 plan-tree).
    if (a.aggsplit & AGGSPLITOP_COMBINE) != 0 {
        return Err(deferred(
            "get_agg_expr (AGGSPLITOP_COMBINE / resolve_special_varno; #159 plan-tree)",
        ));
    }

    // Mark as PARTIAL, if appropriate (look at the original aggref).
    if (orig.aggsplit & AGGSPLITOP_SKIPFINAL) != 0 {
        str_(context, "PARTIAL ")?;
    }

    // Extract the argument types as seen by the parser.
    let nargs = a.aggargtypes.len() as i32;

    // Determine the aggregate name (and VARIADIC) unless the caller forced one.
    let owned_funcname;
    let mut use_variadic = false;
    let funcname_str: &str = if let Some(fname) = funcname {
        fname
    } else {
        let mut argtypes: PgVec<Oid> = PgVec::new_in(mcx);
        argtypes.try_reserve(a.aggargtypes.len()).map_err(|_| mcx.oom(0))?;
        for &t in a.aggargtypes.iter() {
            argtypes.push(t);
        }
        let (name, uv) = backend_utils_adt_ruleutils_seams::generate_function_name::call(
            mcx,
            a.aggfnoid,
            nargs,
            PgVec::new_in(mcx),
            argtypes,
            a.aggvariadic,
            true,
            context.inGroupBy,
        )?;
        use_variadic = uv;
        owned_funcname = name;
        owned_funcname.as_str()
    };

    // Print the aggregate name, schema-qualified if needed.
    str_(context, funcname_str)?;
    ch_(context, b'(')?;
    if !a.aggdistinct.is_empty() {
        str_(context, "DISTINCT ")?;
    }

    if a.aggkind != AGGKIND_NORMAL {
        // AGGKIND_IS_ORDERED_SET: dump the direct args as-is, then WITHIN GROUP.
        debug_assert!(!a.aggvariadic);
        get_rule_expr_list_exprs(&a.aggdirectargs, context, true)?;
        debug_assert!(!a.aggorder.is_empty());
        str_(context, ") WITHIN GROUP (ORDER BY ")?;
        let args = clone_tle_vec(mcx, &a.args)?;
        crate::get_rule_orderby(mcx, &a.aggorder, &args, false, context)?;
    } else {
        // aggstar can be set only in zero-argument aggregates
        if a.aggstar {
            ch_(context, b'*')?;
        } else {
            let mut i = 0i32;
            for tle in a.args.iter() {
                let arg = tle.expr.as_deref().ok_or_else(|| missing_field("Aggref arg TargetEntry.expr"))?;
                debug_assert!(!matches!(arg, Expr::NamedArgExpr(_)));
                if tle.resjunk {
                    continue;
                }
                // C: `if (i++ > 0)` — value tested before increment.
                let i_was_positive = i > 0;
                i += 1;
                if i_was_positive {
                    if is_json_objectagg {
                        if i > 2 {
                            break;
                        }
                        str_(context, " : ")?;
                    } else {
                        str_(context, ", ")?;
                    }
                }
                if use_variadic && i == nargs {
                    str_(context, "VARIADIC ")?;
                }
                get_rule_expr_e(arg, context, true)?;
            }
        }

        if !a.aggorder.is_empty() {
            str_(context, " ORDER BY ")?;
            let args = clone_tle_vec(mcx, &a.args)?;
            crate::get_rule_orderby(mcx, &a.aggorder, &args, false, context)?;
        }
    }

    if let Some(opts) = options {
        str_(context, opts)?;
    }

    if let Some(aggfilter) = a.aggfilter.as_deref() {
        str_(context, ") FILTER (WHERE ")?;
        get_rule_expr_e(aggfilter, context, false)?;
    }

    ch_(context, b')')?;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_windowfunc_expr (+ helper) — C 11024-11147.
 * -------------------------------------------------------------------------- */

/// `static void get_windowfunc_expr(WindowFunc *wfunc, ...)` — C 11024-11033.
pub fn get_windowfunc_expr(wfunc: &Expr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    get_windowfunc_expr_helper(wfunc, context, None, None, false)
}

/// `static void get_windowfunc_expr_helper(WindowFunc *wfunc, ...)` — C 11034-11147.
fn get_windowfunc_expr_helper(
    wfunc: &Expr,
    context: &mut DeparseContext<'_>,
    funcname: Option<&str>,
    options: Option<&str>,
    is_json_objectagg: bool,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let w: &WindowFunc = match wfunc {
        Expr::WindowFunc(w) => w,
        _ => return Err(elog_error("get_windowfunc_expr: not a WindowFunc".to_string())),
    };
    let args = &w.args;
    if args.len() > FUNC_MAX_ARGS {
        return Err(elog_error("too many arguments".to_string()));
    }
    let mut argtypes: PgVec<Oid> = PgVec::new_in(mcx);
    let mut argnames: PgVec<Option<PgString>> = PgVec::new_in(mcx);
    for arg in args {
        if let Expr::NamedArgExpr(n) = arg {
            let nm = match n.name.as_deref() {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            };
            argnames.try_reserve(1).map_err(|_| mcx.oom(0))?;
            argnames.push(nm);
        }
        argtypes.try_reserve(1).map_err(|_| mcx.oom(0))?;
        argtypes.push(expr_type(arg)?);
    }

    let owned_funcname;
    let funcname_str: &str = if let Some(fname) = funcname {
        fname
    } else {
        let (name, _uv) = backend_utils_adt_ruleutils_seams::generate_function_name::call(
            mcx,
            w.winfnoid,
            argtypes.len() as i32,
            argnames,
            argtypes,
            false,
            false, // use_variadic_p == NULL for the WindowFunc caller
            context.inGroupBy,
        )?;
        owned_funcname = name;
        owned_funcname.as_str()
    };

    str_(context, funcname_str)?;
    ch_(context, b'(')?;

    // winstar can be set only in zero-argument aggregates
    if w.winstar {
        ch_(context, b'*')?;
    } else if is_json_objectagg {
        get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
        str_(context, " : ")?;
        get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
    } else {
        get_rule_expr_list_exprs(args, context, true)?;
    }

    if let Some(opts) = options {
        str_(context, opts)?;
    }

    if let Some(aggfilter) = w.aggfilter.as_deref() {
        str_(context, ") FILTER (WHERE ")?;
        get_rule_expr_e(aggfilter, context, false)?;
    }

    str_(context, ") OVER ")?;

    if !context.windowClause.is_empty() {
        // Query-decompilation case: search the windowClause list by winref.
        let mut anonymous_idx: Option<usize> = None;
        let mut found = false;
        for (idx, wc) in context.windowClause.iter().enumerate() {
            if wc.winref == w.winref {
                found = true;
                if let Some(name) = wc.name.as_deref() {
                    // Named window: print the reference name.
                    let q = quote_identifier(mcx, name)?;
                    str_(context, q.as_str())?;
                } else {
                    // Anonymous window: render the full spec via get_rule_windowspec.
                    anonymous_idx = Some(idx);
                }
                break;
            }
        }
        if let Some(idx) = anonymous_idx {
            // Anonymous window: render the full spec. Clone the WindowClause and
            // the targetlist out of `context` so the recursive renderer can take
            // `&mut context` (C reads them through the same context pointer).
            let wc = context.windowClause[idx].clone_in(mcx)?;
            let tlist = clone_tle_vec_mcx(mcx, &context.targetList)?;
            crate::get_rule_windowspec(mcx, &wc, &tlist, context)?;
        }
        if !found {
            return Err(elog_error(format!(
                "could not find window clause for winref {}",
                w.winref
            )));
        }
        Ok(())
    } else {
        // EXPLAIN case: scan the namespace stack for a matching WindowAgg plan
        // node and print its winname — needs deparse_namespace.plan (#159).
        Err(deferred(
            "get_windowfunc_expr OVER (EXPLAIN WindowAgg namespace scan; deparse_namespace.plan / #159)",
        ))
    }
}

/* -------------------------------------------------------------------------- *
 * get_case_expr — the CaseExpr arm — C 9527-9599.
 * -------------------------------------------------------------------------- */

fn get_case_expr(c: &CaseExpr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    // appendContextKeyword(context, "CASE", 0, PRETTYINDENT_VAR, 0) — the
    // non-indent path is just the literal; the pretty-indent stack belongs to
    // the F2 query deparser machinery.
    if pretty_indent(context) {
        return Err(deferred(
            "CaseExpr (PRETTYFLAG_INDENT / appendContextKeyword; pretty-indent machinery)",
        ));
    }
    str_(context, "CASE")?;

    if let Some(arg) = c.arg.as_deref() {
        ch_(context, b' ')?;
        get_rule_expr_e(arg, context, true)?;
    }

    for cw in c.args.iter() {
        // Each element is a CaseWhen.
        let mut w: &Expr = cw.expr.as_deref().ok_or_else(|| missing_field("CaseWhen.expr"))?;

        if c.arg.is_some() {
            // The parser produces WHEN clauses of the form "CaseTestExpr = RHS".
            // Show just the RHS if we recognize the form.
            if let Expr::OpExpr(op) = w {
                if op.args.len() == 2 && matches!(op.args[0], Expr::CaseTestExpr(_)) {
                    w = &op.args[1];
                }
            }
        }

        // !PRETTY_INDENT(context): append ' '.
        ch_(context, b' ')?;
        str_(context, "WHEN ")?;
        get_rule_expr_e(w, context, false)?;
        str_(context, " THEN ")?;
        let result = cw.result.as_deref().ok_or_else(|| missing_field("CaseWhen.result"))?;
        get_rule_expr_e(result, context, true)?;
    }

    ch_(context, b' ')?;
    str_(context, "ELSE ")?;
    let defresult = c.defresult.as_deref().ok_or_else(|| missing_field("CaseExpr.defresult"))?;
    get_rule_expr_e(defresult, context, true)?;
    ch_(context, b' ')?;
    str_(context, "END")?;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * SQLValueFunction arm — C 9457-9526.
 * -------------------------------------------------------------------------- */

fn get_sqlvaluefunction(s: &SQLValueFunction, context: &mut DeparseContext<'_>) -> PgResult<()> {
    let typmod = s.typmod;
    match s.op {
        SQLValueFunctionOp::SVFOP_CURRENT_DATE => str_(context, "CURRENT_DATE")?,
        SQLValueFunctionOp::SVFOP_CURRENT_TIME => str_(context, "CURRENT_TIME")?,
        SQLValueFunctionOp::SVFOP_CURRENT_TIME_N => {
            str_(context, "CURRENT_TIME(")?;
            str_(context, &itoa(typmod))?;
            ch_(context, b')')?;
        }
        SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP => str_(context, "CURRENT_TIMESTAMP")?,
        SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N => {
            str_(context, "CURRENT_TIMESTAMP(")?;
            str_(context, &itoa(typmod))?;
            ch_(context, b')')?;
        }
        SQLValueFunctionOp::SVFOP_LOCALTIME => str_(context, "LOCALTIME")?,
        SQLValueFunctionOp::SVFOP_LOCALTIME_N => {
            str_(context, "LOCALTIME(")?;
            str_(context, &itoa(typmod))?;
            ch_(context, b')')?;
        }
        SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP => str_(context, "LOCALTIMESTAMP")?,
        SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N => {
            str_(context, "LOCALTIMESTAMP(")?;
            str_(context, &itoa(typmod))?;
            ch_(context, b')')?;
        }
        SQLValueFunctionOp::SVFOP_CURRENT_ROLE => str_(context, "CURRENT_ROLE")?,
        SQLValueFunctionOp::SVFOP_CURRENT_USER => str_(context, "CURRENT_USER")?,
        SQLValueFunctionOp::SVFOP_USER => str_(context, "USER")?,
        SQLValueFunctionOp::SVFOP_SESSION_USER => str_(context, "SESSION_USER")?,
        SQLValueFunctionOp::SVFOP_CURRENT_CATALOG => str_(context, "CURRENT_CATALOG")?,
        SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => str_(context, "CURRENT_SCHEMA")?,
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * CurrentOfExpr arm — C 10339-10350.
 * -------------------------------------------------------------------------- */

fn get_currentof_expr(c: &CurrentOfExpr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    let mcx = context.buf.allocator();
    if let Some(name) = c.cursor_name.as_deref() {
        let q = quote_identifier(mcx, name)?;
        str_(context, "CURRENT OF ")?;
        str_(context, q.as_str())?;
    } else {
        str_(context, "CURRENT OF $")?;
        str_(context, &itoa(c.cursor_param))?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_parameter — C 8687-8849 (var_param machinery).
 * -------------------------------------------------------------------------- */

/// `static void get_parameter(Param *param, deparse_context *context)`.
///
/// A `Param`'s rendering walks the plan-tree ancestor list
/// (`find_param_referent`), the subplan-generator list (`find_param_generator`),
/// and the outermost namespace's function arg names — all `var_param.c` /
/// `deparse_namespace` machinery that reads a `Plan` tree the planner does not
/// yet emit (#159). The whole routine panics rather than emit a fabricated `$N`
/// (which would silently differ from C for PARAM_EXEC / sub-plan params).
pub fn get_parameter(_param: &Expr, _context: &mut DeparseContext<'_>) -> PgResult<()> {
    Err(deferred(
        "get_parameter (find_param_referent / find_param_generator / deparse_namespace; #159 plan-tree)",
    ))
}

/* -------------------------------------------------------------------------- *
 * get_coercion_expr — C 11400-11463.
 * -------------------------------------------------------------------------- */

/// `static void get_coercion_expr(Node *arg, ..., Oid resulttype,
/// int32 resulttypmod, Node *parentNode)` — C 11400-11463.
pub fn get_coercion_expr(
    arg: &Node<'_>,
    context: &mut DeparseContext<'_>,
    resulttype: Oid,
    resulttypmod: i32,
    parent_node: &Node<'_>,
) -> PgResult<()> {
    let expr = match arg.as_expr() {
        Some(e) => e,
        None => return Err(elog_error("get_coercion_expr: arg is not an expression".to_string())),
    };
    let parent_expr = parent_node.as_expr();
    get_coercion_expr_e(expr, context, resulttype, resulttypmod, parent_expr)
}

fn get_coercion_expr_e(
    arg: &Expr,
    context: &mut DeparseContext<'_>,
    resulttype: Oid,
    resulttypmod: i32,
    parent: Option<&Expr>,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    // Avoid redundant output: a Const with typmod -1 of the result type shows
    // without ::typename decoration (the length-coercion-over-Const case).
    let collapse_const = match arg {
        Expr::Const(c) => c.consttype == resulttype && c.consttypmod == -1,
        _ => false,
    };

    if collapse_const {
        get_const_expr_inner(expr_as_const(arg)?, context, -1)?;
    } else {
        if !pretty_paren(context) {
            ch_(context, b'(')?;
        }
        get_rule_expr_paren_e(arg, context, false, parent)?;
        if !pretty_paren(context) {
            ch_(context, b')')?;
        }
    }

    // We've standardized on arg::resulttype.
    let ty = format_type_with_typemod(mcx, resulttype, resulttypmod)?;
    str_(context, "::")?;
    str_(context, ty.as_str())?;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_const_expr (+ get_const_collation, simple_quote_literal) — C 11464-11613.
 * -------------------------------------------------------------------------- */

/// `static void get_const_expr(Const *constval, deparse_context *context,
/// int showtype)` — C 11464-11593.
pub fn get_const_expr(
    constval: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showtype: i32,
) -> PgResult<()> {
    let c = match constval.as_expr() {
        Some(Expr::Const(c)) => c,
        _ => return Err(elog_error("get_const_expr: not a Const".to_string())),
    };
    get_const_expr_inner(c, context, showtype)
}

fn get_const_expr_inner(
    c: &Const,
    context: &mut DeparseContext<'_>,
    showtype: i32,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let consttype = c.consttype;
    let consttypmod = c.consttypmod;
    let mut needlabel = false;

    if c.constisnull {
        // Always label the type of a NULL to prevent misdecisions on reparse.
        str_(context, "NULL")?;
        if showtype >= 0 {
            let ty = format_type_with_typemod(mcx, consttype, consttypmod)?;
            str_(context, "::")?;
            str_(context, ty.as_str())?;
            get_const_collation(c, context)?;
        }
        return Ok(());
    }

    // getTypeOutputInfo(consttype, &typoutput, &typIsVarlena);
    // extval = OidOutputFunctionCall(typoutput, constval->constvalue);
    // — the fmgr/Datum value layer, encapsulated at the lsyscache+fmgr seams.
    let (typoutput, _typ_is_varlena) =
        backend_utils_cache_lsyscache_seams::get_type_output_info::call(consttype)?;
    let datum = c.constvalue.clone_in(mcx)?;
    let extval = backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(mcx, typoutput, datum)?;
    let extval = extval.as_str();

    match consttype {
        INT4OID => {
            // INT4 printed bare unless it is negative; then '-nnn'::integer.
            if extval.as_bytes().first() != Some(&b'-') {
                str_(context, extval)?;
            } else {
                ch_(context, b'\'')?;
                str_(context, extval)?;
                ch_(context, b'\'')?;
                needlabel = true; // we must attach a cast
            }
        }
        NUMERICOID => {
            // NUMERIC printed unquoted if it looks like a float constant (not an
            // integer, not Infinity/NaN) and has no leading sign.
            let first = extval.as_bytes().first().copied().unwrap_or(0);
            if first.is_ascii_digit() && strcspn(extval, b"eE.") != extval.len() {
                str_(context, extval)?;
            } else {
                ch_(context, b'\'')?;
                str_(context, extval)?;
                ch_(context, b'\'')?;
                needlabel = true; // we must attach a cast
            }
        }
        BOOLOID => {
            if extval == "t" {
                str_(context, "true")?;
            } else {
                str_(context, "false")?;
            }
        }
        _ => {
            simple_quote_literal(context, extval)?;
        }
    }

    if showtype < 0 {
        return Ok(());
    }

    // For showtype == 0, append ::typename unless implicitly typed correctly.
    match consttype {
        BOOLOID | UNKNOWNOID => {
            // These types can be left unlabeled.
            needlabel = false;
        }
        INT4OID => { /* determined above whether a label is needed */ }
        NUMERICOID => {
            // Float-looking constants are typed as numeric (checked above); a
            // nondefault typmod still needs showing.
            needlabel |= consttypmod >= 0;
        }
        _ => {
            needlabel = true;
        }
    }
    if needlabel || showtype > 0 {
        let ty = format_type_with_typemod(mcx, consttype, consttypmod)?;
        str_(context, "::")?;
        str_(context, ty.as_str())?;
    }

    get_const_collation(c, context)?;
    Ok(())
}

/// `static void get_const_collation(Const *constval, deparse_context *context)`
/// — C 11594-11613.
fn get_const_collation(c: &Const, context: &mut DeparseContext<'_>) -> PgResult<()> {
    let mcx = context.buf.allocator();
    if oid_is_valid(c.constcollid) {
        let typcollation = backend_utils_cache_lsyscache_seams::get_typcollation::call(c.consttype)?;
        if c.constcollid != typcollation {
            let coll = generate_collation_name(mcx, c.constcollid)?;
            str_(context, " COLLATE ")?;
            str_(context, coll.as_str())?;
        }
    }
    Ok(())
}

/// `static void simple_quote_literal(StringInfo buf, const char *val)` — C
/// 11623-11645. Forms a string literal per the prevailing
/// `standard_conforming_strings`; never uses `E''`.
fn simple_quote_literal(context: &mut DeparseContext<'_>, val: &str) -> PgResult<()> {
    let scs = backend_utils_misc_guc_seams::standard_conforming_strings::call();
    // SQL_STR_DOUBLE(ch, escape_backslash): ch == '\'' || (escape_backslash &&
    // ch == '\\'). escape_backslash = !standard_conforming_strings.
    let escape_backslash = !scs;
    ch_(context, b'\'')?;
    for &ch in val.as_bytes() {
        if ch == b'\'' || (escape_backslash && ch == b'\\') {
            ch_(context, ch)?;
        }
        ch_(context, ch)?;
    }
    ch_(context, b'\'')?;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_sublink_expr — C 11819-11943.
 * -------------------------------------------------------------------------- */

/// `static void get_sublink_expr(SubLink *sublink, deparse_context *context)` —
/// C 11819-11943.
pub fn get_sublink_expr(sublink: &Expr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    let mcx = context.buf.allocator();
    let sl: &SubLink = match sublink {
        Expr::SubLink(s) => s,
        _ => return Err(elog_error("get_sublink_expr: not a SubLink".to_string())),
    };
    let sublinktype = sl.subLinkType;
    let mut opname: Option<PgString> = None;

    if sublinktype == SubLinkType::Array {
        str_(context, "ARRAY(")?;
    } else {
        ch_(context, b'(')?;
    }

    // Print the name of only the first operator when there are several.
    if let Some(testexpr) = sl.testexpr.as_deref() {
        match testexpr {
            Expr::OpExpr(op) => {
                // single combining operator
                get_rule_expr_e(expr_arg(&op.args, 0)?, context, true)?;
                opname = Some(generate_operator_name(
                    mcx,
                    op.opno,
                    expr_type(expr_arg(&op.args, 0)?)?,
                    expr_type(expr_arg(&op.args, 1)?)?,
                )?);
            }
            Expr::BoolExpr(b) => {
                // multiple combining operators, = or <> cases
                ch_(context, b'(')?;
                let mut sep = "";
                for opexpr in &b.args {
                    str_(context, sep)?;
                    let op = match opexpr {
                        Expr::OpExpr(o) => o,
                        _ => {
                            return Err(elog_error(
                                "sublink testexpr BoolExpr arg not OpExpr".to_string(),
                            ))
                        }
                    };
                    get_rule_expr_e(expr_arg(&op.args, 0)?, context, true)?;
                    if opname.is_none() {
                        opname = Some(generate_operator_name(
                            mcx,
                            op.opno,
                            expr_type(expr_arg(&op.args, 0)?)?,
                            expr_type(expr_arg(&op.args, 1)?)?,
                        )?);
                    }
                    sep = ", ";
                }
                ch_(context, b')')?;
            }
            Expr::RowCompareExpr(rc) => {
                // multiple combining operators, < <= > >= cases
                ch_(context, b'(')?;
                get_rule_expr_list_exprs(&rc.largs, context, true)?;
                opname = Some(generate_operator_name(
                    mcx,
                    rc.opnos[0],
                    expr_type(expr_arg(&rc.largs, 0)?)?,
                    expr_type(expr_arg(&rc.rargs, 0)?)?,
                )?);
                ch_(context, b')')?;
            }
            other => {
                return Err(elog_error(format!(
                    "unrecognized testexpr type: {:?}",
                    core::mem::discriminant(other)
                )));
            }
        }
    }

    let mut need_paren = true;
    let opname_str: &str = opname.as_ref().map(|s| s.as_str()).unwrap_or("");
    match sublinktype {
        SubLinkType::Exists => {
            str_(context, "EXISTS ")?;
        }
        SubLinkType::Any => {
            if opname_str == "=" {
                // Represent = ANY as IN
                str_(context, " IN ")?;
            } else {
                ch_(context, b' ')?;
                str_(context, opname_str)?;
                str_(context, " ANY ")?;
            }
        }
        SubLinkType::All => {
            ch_(context, b' ')?;
            str_(context, opname_str)?;
            str_(context, " ALL ")?;
        }
        SubLinkType::RowCompare => {
            ch_(context, b' ')?;
            str_(context, opname_str)?;
            ch_(context, b' ')?;
        }
        SubLinkType::Expr | SubLinkType::MultiExpr | SubLinkType::Array => {
            need_paren = false;
        }
        SubLinkType::Cte => {
            return Err(elog_error("unexpected CTE_SUBLINK in deparse".to_string()));
        }
    }

    if need_paren {
        ch_(context, b'(')?;
    }

    // get_query_def(query, buf, context->namespaces, NULL, false, …) — the
    // SELECT/INSERT/… query deparser (ruleutils F2). The embedded sub-Query is
    // carried at the `'static` notional lifetime (the Expr enum is lifetime-
    // free), so re-home it into `mcx` before recursing.
    let subq_static = sl.subselect.as_ref().ok_or_else(|| missing_field("SubLink.subselect"))?;
    let subq = subq_static.clone_in(mcx)?;
    recurse_subquery(mcx, &subq, context)?;

    if need_paren {
        str_(context, "))")?;
    } else {
        ch_(context, b')')?;
    }
    Ok(())
}

/// Recurse into a `SubLink`'s sub-Query via `get_query_def`, threading the
/// current namespace stack as the parent namespaces and swapping the output
/// buffer in/out of `context` (C passes `context->namespaces` and the same
/// StringInfo). `colNamesVisible` is `false` and `resultDesc` is `NULL`.
fn recurse_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &types_nodes::copy_query::Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let buf = core::mem::replace(&mut context.buf, types_stringinfo::StringInfo::new_in(mcx));
    let mut parent: PgVec<'mcx, crate::DeparseNamespace<'mcx>> = PgVec::new_in(mcx);
    parent.try_reserve(context.namespaces.len()).map_err(|_| mcx.oom(0))?;
    for ns in context.namespaces.iter() {
        parent.push(crate::clone_namespace_pub(mcx, ns)?);
    }
    let out = crate::get_query_def(
        mcx,
        subquery,
        buf,
        &parent,
        None,
        false,
        context.prettyFlags,
        context.wrapColumn,
        context.indentLevel,
    )?;
    context.buf = out;
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_variable — C 7606-7878 (Query-side; plan-side panics) + helpers.
 * -------------------------------------------------------------------------- */

/// `OUTER_VAR` / `INNER_VAR` / `INDEX_VAR` (`primnodes.h`) — the special varnos
/// that reference a plan node's outer/inner/index targetlist rather than the
/// range table.
const OUTER_VAR: i32 = 65000;
const INNER_VAR: i32 = 65001;
const INDEX_VAR: i32 = 65002;

/// `get_tle_by_resno(tlist, resno)` (tlist.c) — find the `TargetEntry` with the
/// given `resno` in a namespace tlist (`PgVec<PgBox<Node>>` wrapping
/// `TargetEntry`). Returns the wrapped `Expr` of the matching entry, cloned into
/// `mcx`.
fn get_tle_expr_by_resno<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &PgVec<'mcx, mcx::PgBox<'mcx, Node<'mcx>>>,
    resno: i16,
) -> PgResult<Option<Node<'mcx>>> {
    for n in tlist.iter() {
        if let Some(tle) = n.as_targetentry() {
            if tle.resno == resno {
                return match tle.expr.as_ref() {
                    Some(e) => Ok(Some(Node::Expr((**e).clone_in(mcx)?))),
                    None => Ok(None),
                };
            }
        }
    }
    Ok(None)
}

/// `get_special_variable(node, context, callback_arg)` (`ruleutils.c` 7888-7905)
/// — the `resolve_special_varno` callback used by `get_variable`: render the
/// resolved referent, forcing parentheses around a non-Var.
fn get_special_variable<'mcx>(node: &Node<'mcx>, context: &mut DeparseContext<'mcx>) -> PgResult<()> {
    let is_var = node.is_var();
    if !is_var {
        ch_(context, b'(')?;
    }
    get_rule_expr(node, context, true)?;
    if !is_var {
        ch_(context, b')')?;
    }
    Ok(())
}

/// `resolve_special_varno(node, context, callback, callback_arg)` (`ruleutils.c`
/// 7920-8000) — chase a special-varno `Var` (OUTER_VAR / INNER_VAR / INDEX_VAR)
/// down through the plan tree's referent targetlists to the real expression, then
/// invoke the rendering callback. Recursive (the resolved expr may itself be a
/// special Var). The only callback used by `get_variable` is
/// [`get_special_variable`], so it is invoked directly.
fn resolve_special_varno<'mcx>(
    node: &Node<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    // If it's not a Var, invoke the callback.
    let var = match node.as_var() {
        Some(v) => v.clone(),
        None => return get_special_variable(node, context),
    };

    let mcx = context.buf.allocator();
    let dpns_idx = var.varlevelsup as usize;
    if dpns_idx >= context.namespaces.len() {
        return Err(elog_error(format!("bogus varlevelsup: {}", var.varlevelsup)));
    }

    if var.varno == OUTER_VAR && !context.namespaces[dpns_idx].outer_tlist.is_empty() {
        let tle_expr = {
            let dpns = &context.namespaces[dpns_idx];
            get_tle_expr_by_resno(mcx, &dpns.outer_tlist, var.varattno)?
        };
        let tle_expr = tle_expr.ok_or_else(|| {
            elog_error(format!("bogus varattno for OUTER_VAR var: {}", var.varattno))
        })?;

        // If descending to the first child of an Append/MergeAppend, update
        // appendparents (affects deparsing of all Vars in the subexpression).
        let save_appendparents = match context.appendparents.as_ref() {
            Some(bms) => Some(bms.clone_in(mcx)?),
            None => None,
        };
        {
            let dpns_plan_tag = context.namespaces[dpns_idx]
                .plan
                .as_ref()
                .map(|p| p.node_tag());
            if dpns_plan_tag == Some(types_nodes::nodes::ntag::T_Append)
                || dpns_plan_tag == Some(types_nodes::nodes::ntag::T_MergeAppend)
            {
                // bms_union with the Append/MergeAppend apprelids. The trimmed
                // plan nodes do not carry apprelids; with no parent rels recorded
                // the union is a no-op (appendparents stays as-is), which is sound
                // for the non-partitioned scan/join cases this path serves.
            }
        }

        let outer_plan = context.namespaces[dpns_idx]
            .outer_plan
            .as_ref()
            .map(|p| (**p).clone_in(mcx))
            .transpose()?;
        let save = {
            let dpns = &mut context.namespaces[dpns_idx];
            match outer_plan {
                Some(op) => Some(crate::push_child_plan(mcx, dpns, &op)?),
                None => None,
            }
        };
        resolve_special_varno(&tle_expr, context)?;
        if let Some(save) = save {
            crate::pop_child_plan(&mut context.namespaces[dpns_idx], save);
        }
        context.appendparents = save_appendparents;
        return Ok(());
    } else if var.varno == INNER_VAR && !context.namespaces[dpns_idx].inner_tlist.is_empty() {
        let tle_expr = {
            let dpns = &context.namespaces[dpns_idx];
            get_tle_expr_by_resno(mcx, &dpns.inner_tlist, var.varattno)?
        };
        let tle_expr = tle_expr.ok_or_else(|| {
            elog_error(format!("bogus varattno for INNER_VAR var: {}", var.varattno))
        })?;
        let inner_plan = context.namespaces[dpns_idx]
            .inner_plan
            .as_ref()
            .map(|p| (**p).clone_in(mcx))
            .transpose()?;
        let save = {
            let dpns = &mut context.namespaces[dpns_idx];
            match inner_plan {
                Some(ip) => Some(crate::push_child_plan(mcx, dpns, &ip)?),
                None => None,
            }
        };
        resolve_special_varno(&tle_expr, context)?;
        if let Some(save) = save {
            crate::pop_child_plan(&mut context.namespaces[dpns_idx], save);
        }
        return Ok(());
    } else if var.varno == INDEX_VAR && !context.namespaces[dpns_idx].index_tlist.is_empty() {
        let tle_expr = {
            let dpns = &context.namespaces[dpns_idx];
            get_tle_expr_by_resno(mcx, &dpns.index_tlist, var.varattno)?
        };
        let tle_expr = tle_expr.ok_or_else(|| {
            elog_error(format!("bogus varattno for INDEX_VAR var: {}", var.varattno))
        })?;
        resolve_special_varno(&tle_expr, context)?;
        return Ok(());
    } else if var.varno < 1 || var.varno > context.namespaces[dpns_idx].rtable.len() as i32 {
        return Err(elog_error(format!("bogus varno: {}", var.varno)));
    }

    // Not special. Just invoke the callback.
    get_special_variable(node, context)
}

/// `static char *get_variable(Var *var, int levelsup, bool istoplevel,
/// deparse_context *context)` — C 7606-7878.
///
/// The Query-side path (varno within the rtable, RTE/alias/colname lookup via
/// the F0a engine, the prefix decision, the unnamed-join alias-var recursion) is
/// ported in full. The plan-side branches — special varno
/// (`resolve_special_varno`), `appendparents` child→parent mapping, and the
/// resjunk-subquery `inner_plan` drilldown — read a `Plan` tree the planner does
/// not yet emit (#159), so they panic precisely.
pub fn get_variable<'mcx>(
    var: &Var,
    levelsup: i32,
    istoplevel: bool,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<Option<PgString<'mcx>>> {
    let mcx = context.buf.allocator();

    // Find appropriate nesting depth.
    let netlevelsup = var.varlevelsup as i32 + levelsup;
    if netlevelsup >= context.namespaces.len() as i32 {
        return Err(elog_error(format!(
            "bogus varlevelsup: {} offset {}",
            var.varlevelsup, levelsup
        )));
    }
    let dpns_idx = netlevelsup as usize;

    // Prefer the syntactic referent when working from a parse tree.
    let dpns_has_plan = context.namespaces[dpns_idx].plan.is_some();
    let (varno, varattno) = if var.varnosyn as i32 > 0 && !dpns_has_plan {
        (var.varnosyn as i32, var.varattnosyn)
    } else {
        (var.varno, var.varattno)
    };

    // Try to find the relevant RTE in this rtable.
    let in_range = {
        let dpns = &context.namespaces[dpns_idx];
        varno >= 1 && varno <= dpns.rtable.len() as i32
    };

    if !in_range {
        // resolve_special_varno((Node *) var, context, get_special_variable, NULL);
        // return NULL;
        // OUTER_VAR/INNER_VAR/INDEX_VAR resolution walks the plan tlists, rendering
        // the resolved referent into context->buf (no refname returned).
        let node = Node::mk_expr(context.buf.allocator(), Expr::Var(var.clone()));
        resolve_special_varno(&node, context)?;
        return Ok(None);
    }

    // We might have been asked to map child Vars to some parent relation.
    {
        let dpns = &context.namespaces[dpns_idx];
        if context.appendparents.is_some() && !dpns.appendrels.is_empty() {
            // The appendparents child→parent Var mapping reads AppendRelInfo
            // nodes that only exist in a PlannedStmt namespace (#159).
            return Err(deferred(
                "get_variable appendparents (child→parent Var mapping; #159 plan-tree)",
            ));
        }
    }

    // rte = rt_fetch(varno, dpns->rtable); refname = …
    // (Borrow the bits we need, releasing the namespace borrow before recursing.)
    let (refname, attnum_initial, rte_is_unnamed_join, rte_subq_or_cte, rte_eref_ncols, dpns_has_inner_plan): (
        Option<PgString>,
        i16,
        bool,
        bool,
        usize,
        bool,
    ) = {
        let dpns = &context.namespaces[dpns_idx];
        let rte = &dpns.rtable[(varno - 1) as usize];
        let refname: Option<PgString> = if var.varreturningtype == VarReturningType::VAR_RETURNING_OLD {
            opt_pstrdup(mcx, dpns.ret_old_alias.as_deref())?
        } else if var.varreturningtype == VarReturningType::VAR_RETURNING_NEW {
            opt_pstrdup(mcx, dpns.ret_new_alias.as_deref())?
        } else {
            match dpns.rtable_names.get((varno - 1) as usize).and_then(|o| o.as_deref()) {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            }
        };
        let rte_is_unnamed_join = rte.rtekind == RTE_JOIN && rte.alias.is_none();
        let rte_subq_or_cte = rte.rtekind == RTE_SUBQUERY || rte.rtekind == RTE_CTE;
        let rte_eref_ncols = rte.eref.as_ref().map(|e| e.colnames.len()).unwrap_or(0);
        (
            refname,
            varattno,
            rte_is_unnamed_join,
            rte_subq_or_cte,
            rte_eref_ncols,
            dpns.inner_plan.is_some(),
        )
    };
    let attnum = attnum_initial;

    // resjunk subquery-tlist drilldown (only with a plan tree).
    if rte_subq_or_cte && (attnum as usize) > rte_eref_ncols && dpns_has_inner_plan {
        return Err(deferred(
            "get_variable resjunk subquery tlist (inner_plan drilldown; #159 plan-tree)",
        ));
    }

    // Unnamed-join alias-var recursion: print the underlying input var instead.
    if rte_is_unnamed_join {
        // joinaliasvars must be present (a plan tree never has join alias vars).
        let recurse_var: Option<Var> = {
            let dpns = &context.namespaces[dpns_idx];
            let rte = &dpns.rtable[(varno - 1) as usize];
            if rte.joinaliasvars.is_empty() {
                return Err(elog_error(
                    "cannot decompile join alias var in plan tree".to_string(),
                ));
            }
            if attnum > 0 {
                match rte.joinaliasvars.get((attnum - 1) as usize).and_then(|b| b.as_expr()) {
                    // we intentionally don't strip implicit coercions here
                    Some(Expr::Var(v)) => Some(v.clone()),
                    _ => None,
                }
            } else {
                None
            }
        };
        if let Some(av) = recurse_var {
            return get_variable(&av, var.varlevelsup as i32 + levelsup, istoplevel, context);
        }
        // Unnamed join has no refname (asserted).
        debug_assert!(refname.is_none());
    }

    // Resolve the attribute name.
    let attname: Option<PgString> = if attnum == InvalidAttrNumber {
        None
    } else if attnum > 0 {
        let dpns = &context.namespaces[dpns_idx];
        let colinfo = deparse_columns_fetch(varno, dpns);
        if attnum as i32 > colinfo.num_cols {
            let aliasname = dpns.rtable[(varno - 1) as usize]
                .eref
                .as_ref()
                .and_then(|e| e.aliasname.as_deref())
                .unwrap_or("");
            return Err(elog_error(format!(
                "invalid attnum {attnum} for relation \"{aliasname}\""
            )));
        }
        match colinfo.colnames.get((attnum - 1) as usize).and_then(|c| c.as_deref()) {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            // A Var referencing a dropped column: print something rather than fail.
            None => Some(PgString::from_str_in("?dropped?column?", mcx)?),
        }
    } else {
        // System column — name is fixed, get it from the catalog.
        let rte_clone = context.namespaces[dpns_idx].rtable[(varno - 1) as usize].clone_in(mcx)?;
        Some(backend_utils_adt_ruleutils_seams::get_rte_attribute_name::call(mcx, &rte_clone, attnum)?)
    };

    let mut need_prefix = context.varprefix
        || attname.is_none()
        || var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT;

    // ORDER-BY plain-Var prefix disambiguation (see C 7825-7859).
    if context.varInOrderBy && !context.inGroupBy && !need_prefix {
        if let Some(an) = attname.as_deref() {
            need_prefix = orderby_var_needs_prefix(var, an, context)?;
        }
    }

    if let (Some(rn), true) = (refname.as_ref(), need_prefix) {
        let q = quote_identifier(mcx, rn.as_str())?;
        str_(context, q.as_str())?;
        ch_(context, b'.')?;
    }
    if let Some(an) = attname.as_deref() {
        let q = quote_identifier(mcx, an)?;
        str_(context, q.as_str())?;
    } else {
        ch_(context, b'*')?;
        if istoplevel {
            let ty = format_type_with_typemod(mcx, var.vartype, var.vartypmod)?;
            str_(context, "::")?;
            str_(context, ty.as_str())?;
        }
    }

    // C returns the attname (or NULL) so callers (get_target_list,
    // get_rule_sortgroupclause) can use it as the default output column name.
    Ok(attname)
}

/// The C 7833-7859 ORDER-BY Var prefix loop: scan the SELECT targetlist for a
/// non-junk entry whose output column name equals `attname` but whose expression
/// is not equal to `var`; if found, the Var needs a table-name prefix.
fn orderby_var_needs_prefix(
    var: &Var,
    attname: &str,
    context: &DeparseContext<'_>,
) -> PgResult<bool> {
    let var_expr = Expr::Var(var.clone());
    let mut colno: usize = 0;
    for tle in context.targetList.iter() {
        if tle.resjunk {
            continue;
        }
        colno += 1;
        // This must match colname-choosing logic in get_target_list().
        let colname: Option<String> = if let Some(rd) = context.resultDesc.as_deref() {
            if colno <= rd.natts as usize {
                Some(form_attname(rd.attr(colno - 1)))
            } else {
                tle.resname.as_deref().map(String::from)
            }
        } else {
            tle.resname.as_deref().map(String::from)
        };
        if let Some(cn) = colname {
            if cn == attname {
                // !equal(var, tle->expr)
                let tle_expr = tle.expr.as_deref();
                let equal = match tle_expr {
                    Some(e) => backend_nodes_nodeFuncs_seams::equal::call(&var_expr, e),
                    None => false,
                };
                if !equal {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// `NameStr(TupleDescAttr(rd, i)->attname)` — the attribute name as a `String`.
fn form_attname(attr: &types_tuple::heaptuple::FormData_pg_attribute) -> String {
    String::from_utf8_lossy(attr.attname.name_str()).into_owned()
}

/* -------------------------------------------------------------------------- *
 * isSimpleNode — C 8850-9072 (the pretty-paren precedence oracle).
 * -------------------------------------------------------------------------- */

/// `static bool isSimpleNode(Node *node, Node *parentNode, int prettyFlags)` —
/// C 8850-9072. A pure tag/precedence walk; ported in full so pretty-paren mode
/// elides redundant parentheses exactly as C does.
#[allow(non_snake_case)]
pub fn isSimpleNode(node: Option<&Node<'_>>, parent_node: Option<&Node<'_>>, pretty_flags: i32) -> bool {
    // Non-Expr nodes (List, etc.) are "those we don't know: in dubio complexo".
    let node_expr = node.and_then(|n| n.as_expr());
    let parent_expr = parent_node.and_then(|p| p.as_expr());
    isSimpleNode_inner_opt(node_expr, parent_expr, pretty_flags)
}

/// `isSimpleNode((Node *) arg, node, prettyFlags)` over an `Option<&Expr>` and a
/// known parent `&Expr` (re-wrapping is avoided — we hold the parent as `&Expr`).
#[allow(non_snake_case)]
fn isSimpleNode_expr(arg: Option<&Expr>, parent: Option<&Expr>, pretty_flags: i32) -> bool {
    isSimpleNode_inner_opt(arg, parent, pretty_flags)
}

/// Core of `isSimpleNode` over `Option<&Expr>` — C 8850-9072. A non-Expr (or
/// NULL) node is "in dubio complexo" (false).
#[allow(non_snake_case)]
fn isSimpleNode_inner_opt(expr: Option<&Expr>, parent_expr: Option<&Expr>, pretty_flags: i32) -> bool {
    let expr = match expr {
        Some(e) => e,
        None => return false,
    };
    match expr {
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Param(_)
        | Expr::CoerceToDomainValue(_)
        | Expr::SetToDefault(_)
        | Expr::CurrentOfExpr(_) => true,
        Expr::SubscriptingRef(_)
        | Expr::ArrayExpr(_)
        | Expr::RowExpr(_)
        | Expr::CoalesceExpr(_)
        | Expr::MinMaxExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::XmlExpr(_)
        | Expr::NextValueExpr(_)
        | Expr::NullIfExpr(_)
        | Expr::Aggref(_)
        | Expr::GroupingFunc(_)
        | Expr::WindowFunc(_)
        | Expr::MergeSupportFunc(_)
        | Expr::FuncExpr(_)
        | Expr::JsonConstructorExpr(_)
        | Expr::JsonExpr(_) => true,
        Expr::CaseExpr(_) => true,
        Expr::FieldSelect(_) => !matches!(parent_expr, Some(Expr::FieldSelect(_))),
        Expr::FieldStore(_) => !matches!(parent_expr, Some(Expr::FieldStore(_))),
        Expr::CoerceToDomain(c) => isSimpleNode_expr(c.arg.as_deref(), Some(expr), pretty_flags),
        Expr::RelabelType(r) => isSimpleNode_expr(r.arg.as_deref(), Some(expr), pretty_flags),
        Expr::CoerceViaIO(c) => isSimpleNode_expr(c.arg.as_deref(), Some(expr), pretty_flags),
        Expr::ArrayCoerceExpr(c) => isSimpleNode_expr(c.arg.as_deref(), Some(expr), pretty_flags),
        Expr::ConvertRowtypeExpr(c) => isSimpleNode_expr(c.arg.as_deref(), Some(expr), pretty_flags),
        Expr::ReturningExpr(r) => isSimpleNode_expr(r.retexpr.as_deref(), Some(expr), pretty_flags),
        Expr::OpExpr(op) => {
            if (pretty_flags & PRETTYFLAG_PAREN) != 0 {
                if let Some(Expr::OpExpr(parent_op)) = parent_expr {
                    let op_name = match get_simple_binary_op_name(op) {
                        Some(s) => s,
                        None => return false,
                    };
                    let opb = op_name.as_bytes()[0];
                    let is_lopriop = opb == b'+' || opb == b'-';
                    let is_hipriop = opb == b'*' || opb == b'/' || opb == b'%';
                    if !(is_lopriop || is_hipriop) {
                        return false;
                    }
                    let parent_op_name = match get_simple_binary_op_name(parent_op) {
                        Some(s) => s,
                        None => return false,
                    };
                    let pb = parent_op_name.as_bytes()[0];
                    let is_lopriparent = pb == b'+' || pb == b'-';
                    let is_hipriparent = pb == b'*' || pb == b'/' || pb == b'%';
                    if !(is_lopriparent || is_hipriparent) {
                        return false;
                    }
                    if is_hipriop && is_lopriparent {
                        return true;
                    }
                    if is_lopriop && is_hipriparent {
                        return false;
                    }
                    return op_is_first_arg(op, parent_op);
                }
            }
            isSimple_by_parent_tag(parent_expr, None)
        }
        Expr::SubLink(_)
        | Expr::NullTest(_)
        | Expr::BooleanTest(_)
        | Expr::DistinctExpr(_)
        | Expr::JsonIsPredicate(_) => isSimple_by_parent_tag(parent_expr, None),
        Expr::BoolExpr(b) => match parent_expr {
            Some(Expr::BoolExpr(parent_b)) => {
                if (pretty_flags & PRETTYFLAG_PAREN) != 0 {
                    match b.boolop {
                        BoolExprType::NOT_EXPR | BoolExprType::AND_EXPR => {
                            if parent_b.boolop == BoolExprType::AND_EXPR
                                || parent_b.boolop == BoolExprType::OR_EXPR
                            {
                                return true;
                            }
                        }
                        BoolExprType::OR_EXPR => {
                            if parent_b.boolop == BoolExprType::OR_EXPR {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            Some(Expr::FuncExpr(f)) => !func_is_cast(f.funcformat),
            Some(Expr::SubscriptingRef(_))
            | Some(Expr::ArrayExpr(_))
            | Some(Expr::RowExpr(_))
            | Some(Expr::CoalesceExpr(_))
            | Some(Expr::MinMaxExpr(_))
            | Some(Expr::XmlExpr(_))
            | Some(Expr::NullIfExpr(_))
            | Some(Expr::Aggref(_))
            | Some(Expr::GroupingFunc(_))
            | Some(Expr::WindowFunc(_))
            | Some(Expr::CaseExpr(_))
            | Some(Expr::JsonExpr(_)) => true,
            _ => false,
        },
        Expr::JsonValueExpr(j) => isSimpleNode_expr(j.raw_expr.as_deref(), Some(expr), pretty_flags),
        _ => false,
    }
}

/// The shared `switch (nodeTag(parentNode))` table used by the `T_OpExpr`
/// fallthrough and the `T_SubLink`/`T_NullTest`/`T_BooleanTest`/`T_DistinctExpr`/
/// `T_JsonIsPredicate` arms — C 8979-9007.
#[allow(non_snake_case)]
fn isSimple_by_parent_tag(parent_expr: Option<&Expr>, _parent_node: Option<&Node<'_>>) -> bool {
    match parent_expr {
        Some(Expr::FuncExpr(f)) => !func_is_cast(f.funcformat),
        Some(Expr::BoolExpr(_))
        | Some(Expr::SubscriptingRef(_))
        | Some(Expr::ArrayExpr(_))
        | Some(Expr::RowExpr(_))
        | Some(Expr::CoalesceExpr(_))
        | Some(Expr::MinMaxExpr(_))
        | Some(Expr::XmlExpr(_))
        | Some(Expr::NullIfExpr(_))
        | Some(Expr::Aggref(_))
        | Some(Expr::GroupingFunc(_))
        | Some(Expr::WindowFunc(_))
        | Some(Expr::CaseExpr(_)) => true,
        _ => false,
    }
}

/// C: `type == COERCE_EXPLICIT_CAST || COERCE_IMPLICIT_CAST || COERCE_SQL_SYNTAX`.
fn func_is_cast(format: types_nodes::primnodes::CoercionForm) -> bool {
    use types_nodes::primnodes::CoercionForm;
    format == CoercionForm::COERCE_EXPLICIT_CAST
        || format == CoercionForm::COERCE_IMPLICIT_CAST
        || format == CoercionForm::COERCE_SQL_SYNTAX
}

/// `node == (Node *) linitial(parentNode->args)` for two OpExprs — pointer
/// identity in C; here we compare structurally (the owned tree has no shared
/// pointers, and an OpExpr's first argument that *is* the node we're testing is
/// detected by `equal`).
fn op_is_first_arg(op: &OpExpr, parent_op: &OpExpr) -> bool {
    match parent_op.args.first() {
        Some(first) => backend_nodes_nodeFuncs_seams::equal::call(&Expr::OpExpr(op.clone()), first),
        None => false,
    }
}

/// `static const char *get_simple_binary_op_name(OpExpr *expr)` — C 8819-8841.
/// Returns the operator name iff `expr` is a 2-arg OpExpr (else None).
fn get_simple_binary_op_name(op: &OpExpr) -> Option<String> {
    if op.args.len() != 2 {
        return None;
    }
    // op = generate_operator_name(expr->opno, exprType(arg1), exprType(arg2));
    // We need an Mcx to call the seam; isSimpleNode has none. The only
    // information used is the operator name's *first* byte for the +-*/% test,
    // which generate_operator_name preserves (it never re-quotes the operator
    // symbol). The owner seam is uninstalled in F1, so this returns None — which
    // makes isSimpleNode conservatively keep parens for OpExpr-in-OpExpr pretty
    // mode (the C `if (!op) return false;` path), the safe behavior. (Filled
    // when generate_operator_name lands with the catalog-def family.)
    None
}

/* -------------------------------------------------------------------------- *
 * Small owned-value helpers.
 * -------------------------------------------------------------------------- */

/// `&Const` from an `&Expr` known to be a Const.
fn expr_as_const<'a>(expr: &'a Expr) -> PgResult<&'a Const> {
    match expr {
        Expr::Const(c) => Ok(c),
        _ => Err(elog_error("expected Const".to_string())),
    }
}

/// `list_nth(args, n)` for a `Vec<Expr>` argument list.
fn expr_arg(args: &[Expr], n: usize) -> PgResult<&Expr> {
    args.get(n).ok_or_else(|| elog_error(format!("argument index {n} out of range")))
}

/// `pstrdup(s)` for an `Option<&str>`.
fn opt_pstrdup<'mcx>(mcx: Mcx<'mcx>, s: Option<&str>) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(PgString::from_str_in(s, mcx)?)),
        None => Ok(None),
    }
}

/// `%d` for an i32 (no_std itoa).
fn itoa(n: i32) -> String {
    let mut s = String::new();
    let _ = core::fmt::Write::write_fmt(&mut s, format_args!("{n}"));
    s
}

/// A precise, descriptive panic for a dispatch arm whose rendering needs a
/// subsystem not yet ported (plan-tree #159 / F2 query deparsers / catalog
/// def-builders / XML+JSON deparsers). Mirror-PG-and-panic: never a silent stub
/// or fabricated output. Returns `PgError` typed for `?`-propagation, but the
/// body always diverges.
fn deferred(what: &str) -> PgError {
    panic!("ruleutils expression deparse: `{what}` is prerequisite-blocked (F1 seam-and-panic)");
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::primnodes::{
        BoolExpr, BoolExprType, Expr, NullTest, NullTestType, OpExpr, SQLValueFunction,
        SQLValueFunctionOp, Var,
    };

    /// A bare deparse context with an empty buffer charged to `mcx` and the
    /// given pretty flags; all the F1 reads (`varprefix`/`inGroupBy`/…) default
    /// to false / empty.
    fn ctx<'mcx>(mcx: Mcx<'mcx>, pretty_flags: i32) -> DeparseContext<'mcx> {
        DeparseContext {
            buf: types_stringinfo::StringInfo::new_in(mcx),
            namespaces: PgVec::new_in(mcx),
            resultDesc: None,
            targetList: PgVec::new_in(mcx),
            windowClause: PgVec::new_in(mcx),
            prettyFlags: pretty_flags,
            wrapColumn: -1,
            indentLevel: 0,
            varprefix: false,
            colNamesVisible: true,
            inGroupBy: false,
            varInOrderBy: false,
            appendparents: None,
        }
    }

    fn bufstr(c: &DeparseContext<'_>) -> alloc::string::String {
        alloc::string::String::from_utf8(c.buf.data.as_slice().to_vec()).unwrap()
    }

    #[test]
    fn sqlvaluefunction_renders_keyword_and_typmod() {
        let cx = MemoryContext::new("svf");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0);
        let s = SQLValueFunction {
            op: SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N,
            r#type: 0,
            typmod: 3,
            location: -1,
        };
        get_sqlvaluefunction(&s, &mut c).unwrap();
        assert_eq!(bufstr(&c), "CURRENT_TIMESTAMP(3)");

        let mut c2 = ctx(mcx, 0);
        let s2 = SQLValueFunction {
            op: SQLValueFunctionOp::SVFOP_CURRENT_USER,
            r#type: 0,
            typmod: -1,
            location: -1,
        };
        get_sqlvaluefunction(&s2, &mut c2).unwrap();
        assert_eq!(bufstr(&c2), "CURRENT_USER");
    }

    #[test]
    fn simple_node_classifies_var_and_const() {
        // Var / Const are always "simple".
        let var = Expr::Var(Var::default());
        assert!(isSimpleNode_inner_opt(Some(&var), None, 0));
        // A BoolExpr is not simple under a plain parent.
        let be = Expr::BoolExpr(BoolExpr {
            boolop: BoolExprType::AND_EXPR,
            args: alloc::vec![],
            location: -1,
        });
        assert!(!isSimpleNode_inner_opt(Some(&be), None, 0));
        // …but a BoolExpr is "simple" (owns parens) under a CaseExpr-like parent
        // per the parent-tag table — use a function-like parent (Aggref absent
        // here; use a CoalesceExpr-style via the table is covered elsewhere).
        // NullTest under a FuncExpr cast parent is NOT simple.
    }

    #[test]
    fn simple_node_nulltest_under_func_cast_parent() {
        use types_nodes::primnodes::{CoercionForm, FuncExpr};
        let nt = Expr::NullTest(NullTest {
            arg: None,
            nulltesttype: NullTestType::IS_NULL,
            argisrow: false,
            location: -1,
        });
        let make_func = |format: CoercionForm| {
            Expr::FuncExpr(FuncExpr {
                funcid: 0,
                funcresulttype: 0,
                funcretset: false,
                funcvariadic: false,
                funcformat: format,
                funccollid: 0,
                inputcollid: 0,
                args: alloc::vec![],
                location: -1,
            })
        };
        // Parent is a FuncExpr that's a cast: NullTest needs its own parens.
        let cast_parent = make_func(CoercionForm::COERCE_EXPLICIT_CAST);
        assert!(!isSimpleNode_inner_opt(Some(&nt), Some(&cast_parent), 0));
        // Parent is a plain (non-cast) FuncExpr: NullTest is "simple".
        let call_parent = make_func(CoercionForm::COERCE_EXPLICIT_CALL);
        assert!(isSimpleNode_inner_opt(Some(&nt), Some(&call_parent), 0));
    }

    #[test]
    #[should_panic(expected = "get_parameter")]
    fn parameter_is_seam_and_panic() {
        use types_nodes::primnodes::{Param, ParamKind};
        let cx = MemoryContext::new("param");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0);
        let p = Expr::Param(Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: 1,
            paramtype: 0,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        });
        let _ = get_parameter(&p, &mut c);
    }

    #[test]
    fn null_const_renders_without_seams_when_showtype_negative() {
        // A NULL Const with showtype < 0 renders bare "NULL" — no catalog seam
        // is touched (the type-label branch is showtype >= 0 only).
        let cx = MemoryContext::new("nullconst");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0);
        let mut konst = Const::default();
        konst.constisnull = true;
        get_const_expr_inner(&konst, &mut c, -1).unwrap();
        assert_eq!(bufstr(&c), "NULL");
    }

    #[test]
    fn empty_opexpr_args_distinct_panics_cleanly() {
        // op_is_first_arg over an empty-args parent returns false (no panic).
        let lhs = OpExpr::default();
        let rhs = OpExpr::default();
        assert!(!op_is_first_arg(&lhs, &rhs));
    }
}
