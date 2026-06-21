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
use types_nodes::parsenodes::{
    RTE_CTE, RTE_FUNCTION, RTE_GROUP, RTE_JOIN, RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT,
    RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};
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
const TEXTOID: u32 = 25;
const UNKNOWNOID: u32 = 705;
const NUMERICOID: u32 = 1700;

/// `utils/fmgroids.h` builtin-function OIDs referenced by `get_func_sql_syntax`.
/// These are stable, generated constants (Gen_fmgrtab.pl) — values taken from
/// `src/include/utils/fmgroids.h` for PG 18.
#[allow(non_upper_case_globals, dead_code)]
mod fmgroids {
    pub const F_TIMEZONE_INTERVAL_TIMESTAMPTZ: u32 = 1026;
    pub const F_TIMEZONE_TEXT_TIMESTAMPTZ: u32 = 1159;
    pub const F_TIMEZONE_TEXT_TIMETZ: u32 = 2037;
    pub const F_TIMEZONE_INTERVAL_TIMETZ: u32 = 2038;
    pub const F_TIMEZONE_TEXT_TIMESTAMP: u32 = 2069;
    pub const F_TIMEZONE_INTERVAL_TIMESTAMP: u32 = 2070;
    pub const F_TIMEZONE_TIMESTAMPTZ: u32 = 6334;
    pub const F_TIMEZONE_TIMESTAMP: u32 = 6335;
    pub const F_TIMEZONE_TIMETZ: u32 = 6336;

    pub const F_OVERLAPS_TIMETZ_TIMETZ_TIMETZ_TIMETZ: u32 = 1271;
    pub const F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ: u32 = 1304;
    pub const F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_INTERVAL: u32 = 1305;
    pub const F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_INTERVAL: u32 = 1306;
    pub const F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_TIMESTAMPTZ: u32 = 1307;
    pub const F_OVERLAPS_TIME_TIME_TIME_TIME: u32 = 1308;
    pub const F_OVERLAPS_TIME_INTERVAL_TIME_INTERVAL: u32 = 1309;
    pub const F_OVERLAPS_TIME_TIME_TIME_INTERVAL: u32 = 1310;
    pub const F_OVERLAPS_TIME_INTERVAL_TIME_TIME: u32 = 1311;
    pub const F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_TIMESTAMP: u32 = 2041;
    pub const F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_INTERVAL: u32 = 2042;
    pub const F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_INTERVAL: u32 = 2043;
    pub const F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_TIMESTAMP: u32 = 2044;

    pub const F_EXTRACT_TEXT_DATE: u32 = 6199;
    pub const F_EXTRACT_TEXT_TIME: u32 = 6200;
    pub const F_EXTRACT_TEXT_TIMETZ: u32 = 6201;
    pub const F_EXTRACT_TEXT_TIMESTAMP: u32 = 6202;
    pub const F_EXTRACT_TEXT_TIMESTAMPTZ: u32 = 6203;
    pub const F_EXTRACT_TEXT_INTERVAL: u32 = 6204;

    pub const F_IS_NORMALIZED: u32 = 4351;
    pub const F_PG_COLLATION_FOR: u32 = 3162;
    pub const F_NORMALIZE: u32 = 4350;

    pub const F_OVERLAY_BYTEA_BYTEA_INT4_INT4: u32 = 749;
    pub const F_OVERLAY_BYTEA_BYTEA_INT4: u32 = 752;
    pub const F_OVERLAY_TEXT_TEXT_INT4_INT4: u32 = 1404;
    pub const F_OVERLAY_TEXT_TEXT_INT4: u32 = 1405;
    pub const F_OVERLAY_BIT_BIT_INT4_INT4: u32 = 3030;
    pub const F_OVERLAY_BIT_BIT_INT4: u32 = 3031;

    pub const F_POSITION_TEXT_TEXT: u32 = 849;
    pub const F_POSITION_BIT_BIT: u32 = 1698;
    pub const F_POSITION_BYTEA_BYTEA: u32 = 2014;

    pub const F_SUBSTRING_TEXT_INT4_INT4: u32 = 936;
    pub const F_SUBSTRING_TEXT_INT4: u32 = 937;
    pub const F_SUBSTRING_BIT_INT4_INT4: u32 = 1680;
    pub const F_SUBSTRING_BIT_INT4: u32 = 1699;
    pub const F_SUBSTRING_BYTEA_INT4_INT4: u32 = 2012;
    pub const F_SUBSTRING_BYTEA_INT4: u32 = 2013;
    pub const F_SUBSTRING_TEXT_TEXT_TEXT: u32 = 2074;

    pub const F_BTRIM_TEXT_TEXT: u32 = 884;
    pub const F_BTRIM_TEXT: u32 = 885;
    pub const F_BTRIM_BYTEA_BYTEA: u32 = 2015;

    pub const F_LTRIM_TEXT_TEXT: u32 = 875;
    pub const F_LTRIM_TEXT: u32 = 881;
    pub const F_LTRIM_BYTEA_BYTEA: u32 = 6195;

    pub const F_RTRIM_TEXT_TEXT: u32 = 876;
    pub const F_RTRIM_TEXT: u32 = 882;
    pub const F_RTRIM_BYTEA_BYTEA: u32 = 6196;

    pub const F_SYSTEM_USER: u32 = 6311;
    pub const F_XMLEXISTS: u32 = 2614;
}

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

/// `exprType((const Node *) node)` over a generic `Node` (the index key node in
/// `pg_get_indexdef_worker`). Non-`Expr` nodes have no type — C's `exprType`
/// would `elog` on an unrecognized node, but index expression keys are always
/// `Expr`-derived, so an `InvalidOid` is the faithful fall-through.
pub(crate) fn expr_type_of_node(node: &Node<'_>) -> PgResult<Oid> {
    match node.as_expr() {
        Some(e) => expr_type(e),
        None => Ok(Oid::default()),
    }
}

/// `exprCollation((const Node *) node)` over a generic `Node`.
pub(crate) fn expr_collation_of_node(node: &Node<'_>) -> PgResult<Oid> {
    match node.as_expr() {
        Some(e) => Ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?.collation),
        None => Ok(Oid::default()),
    }
}

/// `looks_like_function(node)` (ruleutils.c 10706) re-exported for the index
/// deparser (an expressional index column is parenthesized unless it is a bare
/// function call).
pub(crate) fn looks_like_function_pub(node: &Node<'_>) -> bool {
    looks_like_function(node)
}

/// `format_type_with_typemod(type_oid, typemod)` (format_type.c) — the type's
/// printable name (flags = 0). The deparser's standard `arg::typename` decorator.
fn format_type_with_typemod<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    typemod: i32,
) -> PgResult<PgString<'mcx>> {
    // C `format_type_with_typemod` calls `format_type_extended(type_oid, typemod,
    // FORMAT_TYPE_TYPEMOD_GIVEN)` (format_type.c). The flag is essential: with it,
    // built-in types whose typmod-(-1) form differs from their no-typmod form (e.g.
    // `bit`, reported as the quoted `"bit"` so the parser won't assign a bogus
    // BIT(1) typmod) fall through to the quoted generic name instead of the
    // unquoted special case.
    match backend_utils_adt_format_type_seams::format_type_extended::call(
        mcx,
        type_oid,
        typemod,
        backend_utils_adt_format_type_seams::FORMAT_TYPE_TYPEMOD_GIVEN,
    )? {
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
    } else if let Some(spec) = node.as_partitionboundspec() {
        // C `case T_PartitionBoundSpec:` (ruleutils.c 10429-10486). Reached via
        // pg_get_expr(relpartbound, ...) for a partition's `FOR VALUES …` clause.
        get_rule_partition_bound_spec(spec, context)?;
    } else if let Some(tf) = node.as_table_func() {
        // C `case T_TableFunc:` (ruleutils.c 10613) → get_tablefunc. Reached via
        // EXPLAIN's "Table Function Call:" (show_expression over rte->tablefunc).
        get_tablefunc(tf, context, showimplicit)?;
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

/// `get_tablefunc(tf, context, showimplicit)` (ruleutils.c:12251) — parse back
/// a table function. XMLTABLE and JSON_TABLE are the only implementations;
/// JSON_TABLE (`get_json_table`) is deferred until that subsystem lands.
pub(crate) fn get_tablefunc(
    tf: &types_nodes::primnodes::TableFunc<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::primnodes::{TFT_JSON_TABLE, TFT_XMLTABLE};
    // XMLTABLE and JSON_TABLE are the only existing implementations.
    if tf.functype == TFT_XMLTABLE {
        get_xmltable(tf, context, showimplicit)
    } else if tf.functype == TFT_JSON_TABLE {
        get_json_table(tf, context, showimplicit)
    } else {
        Ok(())
    }
}

/// `get_json_behavior(behavior, context, on)` (ruleutils.c:9173) — render an ON
/// ERROR / ON EMPTY behavior clause. 1:1 port.
fn get_json_behavior(
    behavior: &types_nodes::primnodes::JsonBehavior,
    context: &mut DeparseContext<'_>,
    on: &str,
) -> PgResult<()> {
    use types_nodes::primnodes::JsonBehaviorType::*;
    let mcx = context.buf.allocator();

    // The order of array elements must correspond to the order of
    // JsonBehaviorType members.
    let behavior_name = match behavior.btype {
        JSON_BEHAVIOR_NULL => " NULL",
        JSON_BEHAVIOR_ERROR => " ERROR",
        JSON_BEHAVIOR_EMPTY => " EMPTY",
        JSON_BEHAVIOR_TRUE => " TRUE",
        JSON_BEHAVIOR_FALSE => " FALSE",
        JSON_BEHAVIOR_UNKNOWN => " UNKNOWN",
        JSON_BEHAVIOR_EMPTY_ARRAY => " EMPTY ARRAY",
        JSON_BEHAVIOR_EMPTY_OBJECT => " EMPTY OBJECT",
        JSON_BEHAVIOR_DEFAULT => " DEFAULT ",
    };

    str_(context, behavior_name)?;

    if behavior.btype == JSON_BEHAVIOR_DEFAULT {
        if let Some(expr) = behavior.expr.as_deref() {
            let n = Node::mk_expr(mcx, expr.clone_in(mcx)?)?;
            get_rule_expr(&n, context, false)?;
        }
    }

    str_(context, " ON ")?;
    str_(context, on)?;
    Ok(())
}

/// `get_json_expr_options(jsexpr, context, default_behavior)` (ruleutils.c:9211)
/// — parse back common options for JSON_QUERY, JSON_VALUE, JSON_EXISTS and
/// JSON_TABLE columns. 1:1 port.
fn get_json_expr_options(
    jsexpr: &types_nodes::primnodes::JsonExpr,
    context: &mut DeparseContext<'_>,
    default_behavior: types_nodes::primnodes::JsonBehaviorType,
) -> PgResult<()> {
    use types_nodes::primnodes::JsonExprOp::JSON_QUERY_OP;
    use types_nodes::primnodes::JsonWrapper::*;

    if jsexpr.op == JSON_QUERY_OP {
        match jsexpr.wrapper {
            JSW_CONDITIONAL => str_(context, " WITH CONDITIONAL WRAPPER")?,
            JSW_UNCONDITIONAL => str_(context, " WITH UNCONDITIONAL WRAPPER")?,
            // The default
            JSW_NONE | JSW_UNSPEC => str_(context, " WITHOUT WRAPPER")?,
        }

        if jsexpr.omit_quotes {
            str_(context, " OMIT QUOTES")?;
        } else {
            // The default
            str_(context, " KEEP QUOTES")?;
        }
    }

    if let Some(on_empty) = jsexpr.on_empty.as_deref() {
        if on_empty.btype != default_behavior {
            get_json_behavior(on_empty, context, "EMPTY")?;
        }
    }
    if let Some(on_error) = jsexpr.on_error.as_deref() {
        if on_error.btype != default_behavior {
            get_json_behavior(on_error, context, "ERROR")?;
        }
    }
    Ok(())
}

/// `get_json_path_spec(path_spec, context, showimplicit)` (ruleutils.c:11615) —
/// parse back a JSON path spec. 1:1 port.
fn get_json_path_spec(
    path_spec: &Expr,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mcx = context.buf.allocator();
    if let Expr::Const(c) = path_spec {
        get_const_expr_inner(c, context, -1)
    } else {
        let n = Node::mk_expr(mcx, path_spec.clone_in(mcx)?)?;
        get_rule_expr(&n, context, showimplicit)
    }
}

/// `get_json_table_nested_columns(tf, plan, context, showimplicit, needcomma)`
/// (ruleutils.c:12043) — parse back nested JSON_TABLE columns. 1:1 port.
fn get_json_table_nested_columns(
    tf: &types_nodes::primnodes::TableFunc<'_>,
    plan: &Node<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
    needcomma: bool,
) -> PgResult<()> {
    use crate::query_deparse::append_context_keyword;
    if let Some(scan) = plan.as_jsontablepathscan() {
        if needcomma {
            ch_(context, b',')?;
        }

        ch_(context, b' ')?;
        append_context_keyword(context, "NESTED PATH ", 0, 0, 0)?;
        // scan->path->value — the collapsed `path` is the Const value node.
        get_const_expr(&scan.path, context, -1)?;
        str_(context, " AS ")?;
        let mcx = context.buf.allocator();
        let q = quote_identifier(
            mcx,
            scan.name.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
        str_(context, q.as_str())?;
        get_json_table_columns(tf, scan, context, showimplicit)?;
    } else if let Some(join) = plan.as_jsontablesiblingjoin() {
        get_json_table_nested_columns(tf, &join.lplan, context, showimplicit, needcomma)?;
        get_json_table_nested_columns(tf, &join.rplan, context, showimplicit, true)?;
    }
    Ok(())
}

/// `get_json_table_columns(tf, scan, context, showimplicit)` (ruleutils.c:12075)
/// — parse back JSON_TABLE columns. 1:1 port.
fn get_json_table_columns(
    tf: &types_nodes::primnodes::TableFunc<'_>,
    scan: &types_nodes::primnodes::JsonTablePathScan<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    use crate::query_deparse::{append_context_keyword, PRETTYINDENT_VAR};
    use types_nodes::primnodes::JsonBehaviorType::{JSON_BEHAVIOR_FALSE, JSON_BEHAVIOR_NULL};
    use types_nodes::primnodes::JsonExprOp::{JSON_EXISTS_OP, JSON_QUERY_OP};
    use types_nodes::primnodes::JsonFormatType::JS_FORMAT_JSONB;

    // TYPCATEGORY_STRING ('S').
    const TYPCATEGORY_STRING: u8 = b'S';

    ch_(context, b' ')?;
    append_context_keyword(context, "COLUMNS (", 0, 0, 0)?;

    if pretty_indent(context) {
        context.indentLevel += PRETTYINDENT_VAR;
    }

    let colnames = tf.colnames.as_ref();
    let coltypes = tf.coltypes.as_ref();
    let coltypmods = tf.coltypmods.as_ref();
    let colvalexprs = tf.colvalexprs.as_ref();

    let ncols = colvalexprs.map(|v| v.len()).unwrap_or(0);
    let mut colnum: i32 = 0;
    for i in 0..ncols {
        let colname = colnames
            .and_then(|v| v.get(i))
            .map(|s| s.as_str())
            .unwrap_or("");
        let typid = coltypes.and_then(|v| v.get(i)).copied().unwrap_or(0);
        let typmod = coltypmods.and_then(|v| v.get(i)).copied().unwrap_or(-1);
        // castNode(JsonExpr, lfirst(lc_colvalexpr)) — NULL for an ordinality col.
        let colexpr: Option<&types_nodes::primnodes::JsonExpr> = colvalexprs
            .and_then(|v| v.get(i))
            .and_then(|o| o.as_deref())
            .and_then(|e| match e {
                Expr::JsonExpr(j) => Some(j),
                _ => None,
            });

        // Skip columns that don't belong to this scan.
        if scan.colMin < 0 || colnum < scan.colMin {
            colnum += 1;
            continue;
        }
        if colnum > scan.colMax {
            break;
        }

        if colnum > scan.colMin {
            str_(context, ", ")?;
        }

        colnum += 1;

        let ordinality = colexpr.is_none();

        append_context_keyword(context, "", 0, 0, 0)?;

        let mcx = context.buf.allocator();
        let q = quote_identifier(mcx, colname)?;
        str_(context, q.as_str())?;
        ch_(context, b' ')?;
        if ordinality {
            str_(context, "FOR ORDINALITY")?;
        } else {
            let ty = format_type_with_typemod(mcx, typid, typmod)?;
            str_(context, ty.as_str())?;
        }
        if ordinality {
            continue;
        }

        let colexpr = colexpr.unwrap();

        // Set default_behavior to guide get_json_expr_options() on whether to
        // emit the ON ERROR / EMPTY clauses.
        let default_behavior;
        if colexpr.op == JSON_EXISTS_OP {
            str_(context, " EXISTS")?;
            default_behavior = JSON_BEHAVIOR_FALSE;
        } else {
            if colexpr.op == JSON_QUERY_OP {
                let (typcategory, _typispreferred) =
                    backend_utils_cache_lsyscache_seams::get_type_category_preferred::call(typid)?;

                if typcategory == TYPCATEGORY_STRING {
                    let is_jsonb = colexpr
                        .format
                        .map(|f| f.format_type == JS_FORMAT_JSONB)
                        .unwrap_or(false);
                    str_(
                        context,
                        if is_jsonb {
                            " FORMAT JSONB"
                        } else {
                            " FORMAT JSON"
                        },
                    )?;
                }
            }

            default_behavior = JSON_BEHAVIOR_NULL;
        }

        str_(context, " PATH ")?;

        if let Some(path_spec) = colexpr.path_spec.as_deref() {
            get_json_path_spec(path_spec, context, showimplicit)?;
        }

        get_json_expr_options(colexpr, context, default_behavior)?;
    }

    if let Some(child) = scan.child.as_deref() {
        get_json_table_nested_columns(tf, child, context, showimplicit, scan.colMin >= 0)?;
    }

    if pretty_indent(context) {
        context.indentLevel -= PRETTYINDENT_VAR;
    }

    append_context_keyword(context, ")", 0, 0, 0)?;
    Ok(())
}

/// `get_json_table(tf, context, showimplicit)` (ruleutils.c:12182) — parse back
/// a JSON_TABLE function. 1:1 port.
fn get_json_table(
    tf: &types_nodes::primnodes::TableFunc<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    use crate::query_deparse::{append_context_keyword, PRETTYINDENT_VAR};
    let mcx = context.buf.allocator();

    // jexpr = castNode(JsonExpr, tf->docexpr).
    let jexpr = match tf.docexpr.as_deref() {
        Some(Expr::JsonExpr(j)) => j,
        _ => return Err(elog_error("get_json_table: docexpr is not a JsonExpr".to_string())),
    };
    // root = castNode(JsonTablePathScan, tf->plan).
    let root = tf
        .plan
        .as_deref()
        .and_then(|n| n.as_jsontablepathscan())
        .ok_or_else(|| elog_error("get_json_table: plan is not a JsonTablePathScan".to_string()))?;

    str_(context, "JSON_TABLE(")?;

    if pretty_indent(context) {
        context.indentLevel += PRETTYINDENT_VAR;
    }

    append_context_keyword(context, "", 0, 0, 0)?;

    if let Some(formatted_expr) = jexpr.formatted_expr.as_deref() {
        let n = Node::mk_expr(mcx, formatted_expr.clone_in(mcx)?)?;
        get_rule_expr(&n, context, showimplicit)?;
    }

    str_(context, ", ")?;

    // root->path->value — the collapsed `path` is the Const value node.
    get_const_expr(&root.path, context, -1)?;

    str_(context, " AS ")?;
    let q = quote_identifier(mcx, root.name.as_ref().map(|s| s.as_str()).unwrap_or(""))?;
    str_(context, q.as_str())?;

    if !jexpr.passing_values.is_empty() {
        let mut needcomma = false;

        ch_(context, b' ')?;
        append_context_keyword(context, "PASSING ", 0, 0, 0)?;

        if pretty_indent(context) {
            context.indentLevel += PRETTYINDENT_VAR;
        }

        for (i, val) in jexpr.passing_values.iter().enumerate() {
            if needcomma {
                str_(context, ", ")?;
            }
            needcomma = true;

            append_context_keyword(context, "", 0, 0, 0)?;

            let n = Node::mk_expr(mcx, val.clone_in(mcx)?)?;
            get_rule_expr(&n, context, false)?;
            str_(context, " AS ")?;
            let name = jexpr.passing_names.get(i).map(|s| s.as_str()).unwrap_or("");
            let q = quote_identifier(mcx, name)?;
            str_(context, q.as_str())?;
        }

        if pretty_indent(context) {
            context.indentLevel -= PRETTYINDENT_VAR;
        }
    }

    get_json_table_columns(tf, root, context, showimplicit)?;

    if let Some(on_error) = jexpr.on_error.as_deref() {
        if on_error.btype != types_nodes::primnodes::JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY {
            get_json_behavior(on_error, context, "ERROR")?;
        }
    }

    if pretty_indent(context) {
        context.indentLevel -= PRETTYINDENT_VAR;
    }

    append_context_keyword(context, ")", 0, 0, 0)?;
    Ok(())
}

/// `get_xmltable(tf, context, showimplicit)` (ruleutils.c:11945) — parse back an
/// XMLTABLE table function. 1:1 port.
fn get_xmltable(
    tf: &types_nodes::primnodes::TableFunc<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    let mcx = context.buf.allocator();

    str_(context, "XMLTABLE(")?;

    // XMLNAMESPACES (...)
    if let Some(ns_uris) = tf.ns_uris.as_ref().filter(|v| !v.is_empty()) {
        str_(context, "XMLNAMESPACES (")?;
        let mut first = true;
        for (i, expr) in ns_uris.iter().enumerate() {
            if !first {
                str_(context, ", ")?;
            } else {
                first = false;
            }
            let name: Option<&str> = tf
                .ns_names
                .as_ref()
                .and_then(|v| v.get(i))
                .and_then(|o| o.as_ref().map(|s| s.as_str()));
            match name {
                Some(n) => {
                    let expr_node = types_nodes::nodes::Node::mk_expr(mcx, (**expr).clone_in(mcx)?)?;
                    get_rule_expr(&expr_node, context, showimplicit)?;
                    let q = quote_identifier(mcx, n)?;
                    str_(context, " AS ")?;
                    str_(context, q.as_str())?;
                }
                None => {
                    str_(context, "DEFAULT ")?;
                    let expr_node = types_nodes::nodes::Node::mk_expr(mcx, (**expr).clone_in(mcx)?)?;
                    get_rule_expr(&expr_node, context, showimplicit)?;
                }
            }
        }
        str_(context, "), ")?;
    }

    ch_(context, b'(')?;
    if let Some(rowexpr) = tf.rowexpr.as_deref() {
        let n = types_nodes::nodes::Node::mk_expr(mcx, rowexpr.clone_in(mcx)?)?;
        get_rule_expr(&n, context, showimplicit)?;
    }
    str_(context, ") PASSING (")?;
    if let Some(docexpr) = tf.docexpr.as_deref() {
        let n = types_nodes::nodes::Node::mk_expr(mcx, docexpr.clone_in(mcx)?)?;
        get_rule_expr(&n, context, showimplicit)?;
    }
    ch_(context, b')')?;

    if let Some(colexprs) = tf.colexprs.as_ref().filter(|v| !v.is_empty()) {
        let colnames = tf.colnames.as_ref();
        let coltypes = tf.coltypes.as_ref();
        let coltypmods = tf.coltypmods.as_ref();
        let coldefexprs = tf.coldefexprs.as_ref();

        str_(context, " COLUMNS ")?;
        for colnum in 0..colexprs.len() {
            let colname = colnames
                .and_then(|v| v.get(colnum))
                .map(|s| s.as_str())
                .unwrap_or("");
            let typid = coltypes.and_then(|v| v.get(colnum)).copied().unwrap_or(0);
            let typmod = coltypmods.and_then(|v| v.get(colnum)).copied().unwrap_or(-1);
            let colexpr = colexprs.get(colnum).and_then(|o| o.as_deref());
            let coldefexpr = coldefexprs.and_then(|v| v.get(colnum)).and_then(|o| o.as_deref());
            let ordinality = tf.ordinalitycol == colnum as i32;
            let notnull = backend_nodes_core_seams::bms_is_member::call(
                colnum as i32,
                tf.notnulls.as_deref(),
            );

            if colnum > 0 {
                str_(context, ", ")?;
            }

            let q = quote_identifier(mcx, colname)?;
            str_(context, q.as_str())?;
            ch_(context, b' ')?;
            if ordinality {
                str_(context, "FOR ORDINALITY")?;
            } else {
                let ty = format_type_with_typemod(mcx, typid, typmod)?;
                str_(context, ty.as_str())?;
            }
            if ordinality {
                continue;
            }

            if let Some(cde) = coldefexpr {
                str_(context, " DEFAULT (")?;
                let n = types_nodes::nodes::Node::mk_expr(mcx, cde.clone_in(mcx)?)?;
                get_rule_expr(&n, context, showimplicit)?;
                ch_(context, b')')?;
            }
            if let Some(ce) = colexpr {
                str_(context, " PATH (")?;
                let n = types_nodes::nodes::Node::mk_expr(mcx, ce.clone_in(mcx)?)?;
                get_rule_expr(&n, context, showimplicit)?;
                ch_(context, b')')?;
            }
            if notnull {
                str_(context, " NOT NULL")?;
            }
        }
    }

    ch_(context, b')')?;
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

        Expr::RowExpr(rowexpr) => {
            // C 9883-9942. If it's a named type and not RECORD, we may have to
            // skip dropped columns and/or claim there are NULLs for added
            // columns, so probe the rowtype's tuple descriptor.
            const RECORDOID: Oid = 2249;
            let tupdesc = if rowexpr.row_typeid != RECORDOID {
                let td = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
                    mcx,
                    rowexpr.row_typeid,
                    -1,
                )?;
                debug_assert!(rowexpr.args.len() as i32 <= td.natts);
                Some(td)
            } else {
                None
            };

            // SQL99 allows "ROW" to be omitted when there is more than one
            // column, but for simplicity we always print it.
            str_(context, "ROW(")?;
            let mut sep = "";
            let mut i: i32 = 0;
            for e in rowexpr.args.iter() {
                if tupdesc.as_ref().is_none_or(|td| !td.attr(i as usize).attisdropped) {
                    str_(context, sep)?;
                    // Whole-row Vars need special treatment here.
                    get_rule_expr_toplevel_expr(e, context, true)?;
                    sep = ", ";
                }
                i += 1;
            }
            if let Some(td) = tupdesc.as_ref() {
                while i < td.natts {
                    if !td.attr(i as usize).attisdropped {
                        str_(context, sep)?;
                        str_(context, "NULL")?;
                        sep = ", ";
                    }
                    i += 1;
                }
            }
            ch_(context, b')')?;
            if rowexpr.row_format == types_nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CAST {
                let ty = format_type_with_typemod(mcx, rowexpr.row_typeid, -1)?;
                str_(context, "::")?;
                str_(context, ty.as_str())?;
            }
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

        Expr::SubPlan(sp) => {
            get_subplan_expr(&sp.0, context, showimplicit)?;
        }

        Expr::FieldSelect(fselect) => {
            let arg = fselect
                .arg
                .as_deref()
                .ok_or_else(|| missing_field("FieldSelect.arg"))?;
            let fno = fselect.fieldnum as i32;

            // Parenthesize the argument unless it's a SubscriptingRef or another
            // FieldSelect. It would be WRONG to not parenthesize a Var argument;
            // having the right number of names is the issue, not simplicity.
            let need_parens =
                !matches!(arg, Expr::SubscriptingRef(_)) && !matches!(arg, Expr::FieldSelect(_));
            if need_parens {
                ch_(context, b'(')?;
            }
            get_rule_expr_e(arg, context, true)?;
            if need_parens {
                ch_(context, b')')?;
            }

            // Get and print the field name.
            let mcx = context.buf.allocator();
            let fieldname = get_name_for_var_field(arg, fno, 0, context)?;
            ch_(context, b'.')?;
            let q = quote_identifier(mcx, fieldname.as_str())?;
            str_(context, q.as_str())?;
        }

        // --- Prerequisite-blocked arms (need an unported subsystem) ----------
        Expr::XmlExpr(_) => {
            return Err(deferred(
                "XmlExpr (get_rule_expr XmlExpr arm: map_xml_name_to_sql_identifier; XML deparser family)",
            ))
        }
        Expr::NextValueExpr(_) => {
            return Err(deferred("NextValueExpr (generate_relation_name; catalog)"))
        }
        Expr::InferenceElem(iexpr) => {
            // InferenceElem can only refer to target relation, so a prefix is
            // not useful, and indeed would cause parse errors.
            let save_varprefix = context.varprefix;
            context.varprefix = false;

            // Parenthesize the element unless it's a simple Var or a bare
            // function call.  Follows pg_get_indexdef_worker().
            //   need_parens = !IsA(iexpr->expr, Var);
            //   if (IsA(iexpr->expr, FuncExpr) &&
            //       ((FuncExpr *) iexpr->expr)->funcformat == COERCE_EXPLICIT_CALL)
            //       need_parens = false;
            let inner = iexpr.expr.as_deref();
            let mut need_parens = !matches!(inner, Some(Expr::Var(_)));
            if let Some(Expr::FuncExpr(f)) = inner {
                if f.funcformat == types_nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL {
                    need_parens = false;
                }
            }

            if need_parens {
                ch_(context, b'(')?;
            }
            if let Some(arg) = inner {
                get_rule_expr_e(arg, context, false)?;
            }
            if need_parens {
                ch_(context, b')')?;
            }

            context.varprefix = save_varprefix;

            // if (iexpr->infercollid)
            //     appendStringInfo(buf, " COLLATE %s",
            //                      generate_collation_name(iexpr->infercollid));
            if oid_is_valid(iexpr.infercollid) {
                let mcx = context.buf.allocator();
                let coll = generate_collation_name(mcx, iexpr.infercollid)?;
                str_(context, " COLLATE ")?;
                str_(context, coll.as_str())?;
            }

            // Add the operator class name, if not default.
            // if (iexpr->inferopclass) {
            //     inferopcinputtype = get_opclass_input_type(iexpr->inferopclass);
            //     get_opclass_name(inferopclass, inferopcinputtype, buf);
            // }
            if oid_is_valid(iexpr.inferopclass) {
                let mcx = context.buf.allocator();
                let inferopcinputtype =
                    backend_utils_cache_lsyscache_seams::get_opclass_input_type::call(
                        iexpr.inferopclass,
                    )?;
                let mut opcbuf = alloc::string::String::new();
                crate::get_opclass_name(mcx, &mut opcbuf, iexpr.inferopclass, inferopcinputtype)?;
                str_(context, &opcbuf)?;
            }
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

/// Crate-visible wrapper over [`print_subscripts`] so `query_deparse`'s
/// `processIndirection` (the assignment-target path) can print subscripts.
pub(crate) fn print_subscripts_pub(
    sbsref: &SubscriptingRef,
    context: &mut DeparseContext<'_>,
) -> PgResult<()> {
    print_subscripts(sbsref, context)
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
        // exprIsLengthCoercion((Node *) expr, &coercedTypmod): a length-coercion
        // cast (e.g. numeric(16,4) / varchar(8)) carries its typmod in the
        // function's int4 second argument, which we must print on the target
        // type name. Non-length coercions yield -1.
        let coerced_typmod = match enclosing {
            Some(e) => {
                backend_nodes_nodeFuncs_seams::expr_is_length_coercion::call(e)?.1
            }
            None => {
                // The enclosing FuncExpr node is what C passes; reconstruct it
                // when the caller didn't thread it through.
                let f_expr = Expr::FuncExpr(f.clone());
                backend_nodes_nodeFuncs_seams::expr_is_length_coercion::call(&f_expr)?.1
            }
        };
        get_coercion_expr_e(arg, context, rettype, coerced_typmod, enclosing)?;
        return Ok(());
    }

    // If the function was called using one of the SQL spec's random special
    // syntaxes, try to reproduce that.  If we don't recognize the function,
    // fall through.
    if funcformat == CoercionForm::COERCE_SQL_SYNTAX && get_func_sql_syntax(f, enclosing, context)? {
        return Ok(());
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
 * get_func_sql_syntax — C 11143-11399.
 * -------------------------------------------------------------------------- */

/// `TextDatumGetCString(con->constvalue)` for a TEXTOID, non-null `Const`.
///
/// The C code reads the text value straight out of the Const datum; we obtain
/// the same string via the type-output function (textout) at the fmgr seam, the
/// same path `get_const_expr` uses. Mirrors the C `Assert(IsA(con, Const) &&
/// con->consttype == TEXTOID && !con->constisnull)`.
fn text_const_cstring<'mcx>(
    mcx: Mcx<'mcx>,
    con: &Const,
) -> PgResult<PgString<'mcx>> {
    debug_assert!(con.consttype == TEXTOID && !con.constisnull);
    let (typoutput, _typ_is_varlena) =
        backend_utils_cache_lsyscache_seams::get_type_output_info::call(con.consttype)?;
    let datum = con.constvalue.clone_in(mcx)?;
    backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(mcx, typoutput, datum)
}

/// `static bool get_func_sql_syntax(FuncExpr *expr, deparse_context *context)`
/// — C 11149-11399.
///
/// Parse back a SQL-syntax function call. Returns `true` if we successfully
/// deparsed, `false` if we did not recognize the function.
fn get_func_sql_syntax(
    expr: &FuncExpr,
    enclosing: Option<&Expr>,
    context: &mut DeparseContext<'_>,
) -> PgResult<bool> {
    use fmgroids::*;
    let mcx = context.buf.allocator();
    let funcoid = expr.funcid;
    let args = &expr.args;

    match funcoid {
        F_TIMEZONE_INTERVAL_TIMESTAMP
        | F_TIMEZONE_INTERVAL_TIMESTAMPTZ
        | F_TIMEZONE_INTERVAL_TIMETZ
        | F_TIMEZONE_TEXT_TIMESTAMP
        | F_TIMEZONE_TEXT_TIMESTAMPTZ
        | F_TIMEZONE_TEXT_TIMETZ => {
            // AT TIME ZONE ... note reversed argument order
            ch_(context, b'(')?;
            get_rule_expr_paren_e(expr_arg(args, 1)?, context, false, enclosing)?;
            str_(context, " AT TIME ZONE ")?;
            get_rule_expr_paren_e(expr_arg(args, 0)?, context, false, enclosing)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_TIMEZONE_TIMESTAMP | F_TIMEZONE_TIMESTAMPTZ | F_TIMEZONE_TIMETZ => {
            // AT LOCAL
            ch_(context, b'(')?;
            get_rule_expr_paren_e(expr_arg(args, 0)?, context, false, enclosing)?;
            str_(context, " AT LOCAL)")?;
            Ok(true)
        }

        F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_INTERVAL
        | F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_TIMESTAMPTZ
        | F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_INTERVAL
        | F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ
        | F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_INTERVAL
        | F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_TIMESTAMP
        | F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_INTERVAL
        | F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_TIMESTAMP
        | F_OVERLAPS_TIMETZ_TIMETZ_TIMETZ_TIMETZ
        | F_OVERLAPS_TIME_INTERVAL_TIME_INTERVAL
        | F_OVERLAPS_TIME_INTERVAL_TIME_TIME
        | F_OVERLAPS_TIME_TIME_TIME_INTERVAL
        | F_OVERLAPS_TIME_TIME_TIME_TIME => {
            // (x1, x2) OVERLAPS (y1, y2)
            str_(context, "((")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, ", ")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            str_(context, ") OVERLAPS (")?;
            get_rule_expr_e(expr_arg(args, 2)?, context, false)?;
            str_(context, ", ")?;
            get_rule_expr_e(expr_arg(args, 3)?, context, false)?;
            str_(context, "))")?;
            Ok(true)
        }

        F_EXTRACT_TEXT_DATE
        | F_EXTRACT_TEXT_TIME
        | F_EXTRACT_TEXT_TIMETZ
        | F_EXTRACT_TEXT_TIMESTAMP
        | F_EXTRACT_TEXT_TIMESTAMPTZ
        | F_EXTRACT_TEXT_INTERVAL => {
            // EXTRACT (x FROM y)
            str_(context, "EXTRACT(")?;
            let con = expr_as_const(expr_arg(args, 0)?)?;
            let field = text_const_cstring(mcx, con)?;
            str_(context, field.as_str())?;
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_IS_NORMALIZED => {
            // IS xxx NORMALIZED
            ch_(context, b'(')?;
            get_rule_expr_paren_e(expr_arg(args, 0)?, context, false, enclosing)?;
            str_(context, " IS")?;
            if args.len() == 2 {
                let con = expr_as_const(expr_arg(args, 1)?)?;
                let form = text_const_cstring(mcx, con)?;
                ch_(context, b' ')?;
                str_(context, form.as_str())?;
            }
            str_(context, " NORMALIZED)")?;
            Ok(true)
        }

        F_PG_COLLATION_FOR => {
            // COLLATION FOR
            str_(context, "COLLATION FOR (")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_NORMALIZE => {
            // NORMALIZE()
            str_(context, "NORMALIZE(")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            if args.len() == 2 {
                let con = expr_as_const(expr_arg(args, 1)?)?;
                let form = text_const_cstring(mcx, con)?;
                str_(context, ", ")?;
                str_(context, form.as_str())?;
            }
            ch_(context, b')')?;
            Ok(true)
        }

        F_OVERLAY_BIT_BIT_INT4
        | F_OVERLAY_BIT_BIT_INT4_INT4
        | F_OVERLAY_BYTEA_BYTEA_INT4
        | F_OVERLAY_BYTEA_BYTEA_INT4_INT4
        | F_OVERLAY_TEXT_TEXT_INT4
        | F_OVERLAY_TEXT_TEXT_INT4_INT4 => {
            // OVERLAY()
            str_(context, "OVERLAY(")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, " PLACING ")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 2)?, context, false)?;
            if args.len() == 4 {
                str_(context, " FOR ")?;
                get_rule_expr_e(expr_arg(args, 3)?, context, false)?;
            }
            ch_(context, b')')?;
            Ok(true)
        }

        F_POSITION_BIT_BIT | F_POSITION_BYTEA_BYTEA | F_POSITION_TEXT_TEXT => {
            // POSITION() ... extra parens since args are b_expr not a_expr
            str_(context, "POSITION((")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            str_(context, ") IN (")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, "))")?;
            Ok(true)
        }

        F_SUBSTRING_BIT_INT4
        | F_SUBSTRING_BIT_INT4_INT4
        | F_SUBSTRING_BYTEA_INT4
        | F_SUBSTRING_BYTEA_INT4_INT4
        | F_SUBSTRING_TEXT_INT4
        | F_SUBSTRING_TEXT_INT4_INT4 => {
            // SUBSTRING FROM/FOR (i.e., integer-position variants)
            str_(context, "SUBSTRING(")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            if args.len() == 3 {
                str_(context, " FOR ")?;
                get_rule_expr_e(expr_arg(args, 2)?, context, false)?;
            }
            ch_(context, b')')?;
            Ok(true)
        }

        F_SUBSTRING_TEXT_TEXT_TEXT => {
            // SUBSTRING SIMILAR/ESCAPE
            str_(context, "SUBSTRING(")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, " SIMILAR ")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            str_(context, " ESCAPE ")?;
            get_rule_expr_e(expr_arg(args, 2)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_BTRIM_BYTEA_BYTEA | F_BTRIM_TEXT | F_BTRIM_TEXT_TEXT => {
            // TRIM()
            str_(context, "TRIM(BOTH")?;
            if args.len() == 2 {
                ch_(context, b' ')?;
                get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            }
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_LTRIM_BYTEA_BYTEA | F_LTRIM_TEXT | F_LTRIM_TEXT_TEXT => {
            // TRIM()
            str_(context, "TRIM(LEADING")?;
            if args.len() == 2 {
                ch_(context, b' ')?;
                get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            }
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_RTRIM_BYTEA_BYTEA | F_RTRIM_TEXT | F_RTRIM_TEXT_TEXT => {
            // TRIM()
            str_(context, "TRIM(TRAILING")?;
            if args.len() == 2 {
                ch_(context, b' ')?;
                get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            }
            str_(context, " FROM ")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            ch_(context, b')')?;
            Ok(true)
        }

        F_SYSTEM_USER => {
            str_(context, "SYSTEM_USER")?;
            Ok(true)
        }

        F_XMLEXISTS => {
            // XMLEXISTS ... extra parens because args are c_expr
            str_(context, "XMLEXISTS((")?;
            get_rule_expr_e(expr_arg(args, 0)?, context, false)?;
            str_(context, ") PASSING (")?;
            get_rule_expr_e(expr_arg(args, 1)?, context, false)?;
            str_(context, "))")?;
            Ok(true)
        }

        _ => Ok(false),
    }
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
    // partial aggregate instead. This is necessary because our input argument
    // list has been replaced; the new argument list always has just one element,
    // which points to a partial Aggref that supplies the transition states to
    // combine (ruleutils.c:10899-10907).
    if (a.aggsplit & AGGSPLITOP_COMBINE) != 0 {
        debug_assert_eq!(a.args.len(), 1);
        let tle = a
            .args
            .first()
            .ok_or_else(|| missing_field("combining Aggref args"))?;
        let tle_expr = tle
            .expr
            .as_deref()
            .ok_or_else(|| missing_field("combining Aggref TargetEntry.expr"))?;
        let node = Node::mk_expr(mcx, tle_expr.clone_in(mcx)?)?;
        return resolve_special_varno(
            &node,
            context,
            &RsvCallback::AggCombineExpr(original_aggref),
        );
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
    use crate::query_deparse::{append_context_keyword, PRETTYINDENT_VAR};

    // appendContextKeyword(context, "CASE", 0, PRETTYINDENT_VAR, 0)
    append_context_keyword(context, "CASE", 0, PRETTYINDENT_VAR, 0)?;

    if let Some(arg) = c.arg.as_deref() {
        ch_(context, b' ')?;
        get_rule_expr_e(arg, context, true)?;
    }

    for cw in c.args.iter() {
        // Each element is a CaseWhen.
        let mut w: &Expr = cw.expr.as_deref().ok_or_else(|| missing_field("CaseWhen.expr"))?;

        if c.arg.is_some() {
            // The parser produces WHEN clauses of the form "CaseTestExpr = RHS",
            // possibly with an implicit coercion above the CaseTestExpr. Show
            // just the RHS if we recognize the form; otherwise punt and display
            // it as-is.
            if let Expr::OpExpr(op) = w {
                if op.args.len() == 2
                    && matches!(
                        backend_nodes_core::nodefuncs::strip_implicit_coercions(&op.args[0]),
                        Expr::CaseTestExpr(_)
                    )
                {
                    w = &op.args[1];
                }
            }
        }

        if !pretty_indent(context) {
            ch_(context, b' ')?;
        }
        append_context_keyword(context, "WHEN ", 0, 0, 0)?;
        get_rule_expr_e(w, context, false)?;
        str_(context, " THEN ")?;
        let result = cw.result.as_deref().ok_or_else(|| missing_field("CaseWhen.result"))?;
        get_rule_expr_e(result, context, true)?;
    }

    if !pretty_indent(context) {
        ch_(context, b' ')?;
    }
    append_context_keyword(context, "ELSE ", 0, 0, 0)?;
    let defresult = c.defresult.as_deref().ok_or_else(|| missing_field("CaseExpr.defresult"))?;
    get_rule_expr_e(defresult, context, true)?;
    if !pretty_indent(context) {
        ch_(context, b' ')?;
    }
    append_context_keyword(context, "END", -PRETTYINDENT_VAR, 0, 0)?;
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

/// `find_param_referent(param, context, &dpns, &ancestor_cell)` (`ruleutils.c`
/// 8455-8556). For a `PARAM_EXEC` Param, walk the head namespace's ancestor
/// list looking for a `NestLoop` that transmits the param to its inner child
/// (returning the matching `NestLoopParam.paramval`) or a `SubPlan` that
/// supplies it as a `parParam`/`args` pair (returning the `arg`, but pointing
/// the deparse attention at the next non-`SubPlan` ancestor). On success returns
/// the source expression (cloned into `mcx`) and `Some(ancestor_index)` — the
/// 0-based position in `dpns.ancestors` to which `push_ancestor_plan` should
/// transfer attention. Returns `Ok(None)` when no referent is found.
fn find_param_referent<'mcx>(
    mcx: Mcx<'mcx>,
    param: &types_nodes::primnodes::Param,
    context: &DeparseContext<'mcx>,
) -> PgResult<Option<(Expr, usize)>> {
    use types_nodes::nodes::ntag;
    use types_nodes::primnodes::PARAM_EXEC;

    if param.paramkind != PARAM_EXEC {
        return Ok(None);
    }

    // dpns = linitial(context->namespaces); child_plan = dpns->plan;
    let Some(dpns) = context.namespaces.first() else {
        return Ok(None);
    };
    // child_plan starts at dpns->plan (the current plan node).
    let mut child_plan: Option<&Node<'mcx>> = dpns.plan.as_deref();

    // foreach(lc, dpns->ancestors)
    for (idx, ancestor) in dpns.ancestors.iter().enumerate() {
        let atag = ancestor.node_tag();

        // NestLoops transmit params to their inner child only.
        if atag == ntag::T_NestLoop {
            if let Some(nl) = ancestor.as_nestloop() {
                // child_plan == innerPlan(ancestor)
                let inner = nl.join.plan.righttree.as_deref();
                let is_inner = match (child_plan, inner) {
                    (Some(c), Some(i)) => core::ptr::eq(c, i) || nodes_struct_eq(c, i),
                    _ => false,
                };
                if is_inner {
                    for nlp in nl.nestParams.iter() {
                        if nlp.paramno == param.paramid {
                            // return (Node *) nlp->paramval; attention stays on
                            // this ancestor (idx). paramval is an Expr (Var or,
                            // transiently, a PlaceHolderVar).
                            return Ok(Some((nlp.paramval.clone(), idx)));
                        }
                    }
                }
            }
        }

        // If ancestor is a SubPlan, check the arguments it provides.
        if atag == ntag::T_SubPlan {
            if let Some(sp) = ancestor.as_subplan() {
                let subplan = &sp.0;
                // forboth(lc3, subplan->parParam, lc4, subplan->args)
                for (paramid, arg) in subplan.parParam.iter().zip(subplan.args.iter()) {
                    if *paramid == param.paramid {
                        // Vars in the arg evaluate in the surrounding context, so
                        // point attention at the next ancestor that is *not* a
                        // SubPlan (C: for_each_cell(rest, ancestors, lnext(lc))).
                        for (rest_idx, ancestor2) in
                            dpns.ancestors.iter().enumerate().skip(idx + 1)
                        {
                            if ancestor2.node_tag() != ntag::T_SubPlan {
                                let cloned = arg.clone_in(mcx)?;
                                return Ok(Some((cloned, rest_idx)));
                            }
                        }
                        return Err(elog_error("SubPlan cannot be outermost ancestor".into()));
                    }
                }
                // SubPlan isn't a kind of Plan, so skip the rest.
                continue;
            }
        }

        // No luck, crawl up to next ancestor (child_plan = ancestor).
        child_plan = Some(ancestor);
    }

    Ok(None)
}

/// `find_param_generator(param, context, &column)` (`ruleutils.c` 8569-8657).
/// For a `PARAM_EXEC` Param, search the innermost plan node's initplans, then
/// its MULTIEXPR_SUBLINK targetlist SubPlans, then the ancestor SubPlans /
/// initplans, for a subplan/initplan that emits the param. On success returns
/// `Some((subplan_name, useHashTable, column))` (cloned name); the caller renders
/// `(<hashed >name).colN`. Returns `Ok(None)` when no generator is found.
fn find_param_generator<'mcx>(
    mcx: Mcx<'mcx>,
    param: &types_nodes::primnodes::Param,
    context: &DeparseContext<'mcx>,
) -> PgResult<Option<(PgString<'mcx>, bool, i32)>> {
    use types_nodes::nodes::ntag;
    use types_nodes::primnodes::PARAM_EXEC;

    if param.paramkind != PARAM_EXEC {
        return Ok(None);
    }

    let Some(dpns) = context.namespaces.first() else {
        return Ok(None);
    };

    // First check the innermost plan node's initplans.
    if let Some(plan) = dpns.plan.as_deref() {
        if let Some(res) = find_param_generator_initplan(mcx, param, plan)? {
            return Ok(Some(res));
        }

        // The plan's targetlist might contain MULTIEXPR_SUBLINK SubPlans.
        if let Some(tlist) = plan.plan_head().targetlist.as_ref() {
            for tle in tlist.iter() {
                if let Some(expr) = tle.expr.as_deref() {
                    if let Expr::SubPlan(sp) = expr {
                        let subplan = &sp.0;
                        if subplan.subLinkType == SubLinkType::MultiExpr {
                            for (col, paramid) in subplan.setParam.iter().enumerate() {
                                if *paramid == param.paramid {
                                    let name = subplan_name(mcx, subplan)?;
                                    return Ok(Some((name, subplan.useHashTable, col as i32)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // No luck, so check the ancestor nodes.
    for ancestor in dpns.ancestors.iter() {
        if ancestor.node_tag() == ntag::T_SubPlan {
            if let Some(sp) = ancestor.as_subplan() {
                let subplan = &sp.0;
                for (col, paramid) in subplan.paramIds.iter().enumerate() {
                    if *paramid == param.paramid {
                        let name = subplan_name(mcx, subplan)?;
                        return Ok(Some((name, subplan.useHashTable, col as i32)));
                    }
                }
                // SubPlan isn't a kind of Plan, so skip the rest.
                continue;
            }
        }

        // Otherwise it's some kind of Plan node; check its initplans.
        if let Some(res) = find_param_generator_initplan(mcx, param, ancestor)? {
            return Ok(Some(res));
        }
    }

    Ok(None)
}

/// `find_param_generator_initplan(param, plan, &column)` (`ruleutils.c`
/// 8666-8682) — search one Plan node's initplans for one that sets the param.
fn find_param_generator_initplan<'mcx>(
    mcx: Mcx<'mcx>,
    param: &types_nodes::primnodes::Param,
    plan: &Node<'mcx>,
) -> PgResult<Option<(PgString<'mcx>, bool, i32)>> {
    if let Some(initplans) = plan.plan_head().initPlan.as_ref() {
        for subplan in initplans.iter() {
            for (col, paramid) in subplan.setParam.iter().enumerate() {
                if *paramid == param.paramid {
                    let name = subplan_name(mcx, subplan)?;
                    return Ok(Some((name, subplan.useHashTable, col as i32)));
                }
            }
        }
    }
    Ok(None)
}

/// Clone a `SubPlan.plan_name` into `mcx` (empty string when the C field is
/// NULL — `appendStringInfo("%s")` of a NULL would print nothing).
fn subplan_name<'mcx>(
    mcx: Mcx<'mcx>,
    subplan: &types_nodes::primnodes::SubPlan<'_>,
) -> PgResult<PgString<'mcx>> {
    match subplan.plan_name.as_ref() {
        Some(s) => PgString::from_str_in(s.as_str(), mcx),
        None => PgString::from_str_in("", mcx),
    }
}

/// Structural equality of two plan `Node`s used to match `child_plan ==
/// innerPlan(ancestor)`. The owned model clones plan nodes into the deparse
/// arena, so the C pointer-identity test cannot be used directly; we compare the
/// `plan_node_id` (unique across the final plan tree — `setrefs.c` assigns it),
/// which is exactly the identity the pointer test stands in for.
fn nodes_struct_eq(a: &Node<'_>, b: &Node<'_>) -> bool {
    a.plan_head().plan_node_id == b.plan_head().plan_node_id
}

/// The `T_SubPlan` arm of `get_rule_expr` (`ruleutils.c` 9540-9598). An
/// already-planned `SubPlan` can only be seen while EXPLAINing a query plan
/// (never in rule deparse); we don't reconstruct the original SQL, just show the
/// subLinkType + testexpr (so the referencing Params reveal which subplan), and
/// note whether it is hashed. While deparsing the testexpr the SubPlan is pushed
/// onto the head namespace's ancestors so PARAM_EXEC references to its paramIds
/// resolve (`find_param_referent` / `find_param_generator`).
fn get_subplan_expr(
    subplan: &types_nodes::primnodes::SubPlan<'_>,
    context: &mut DeparseContext<'_>,
    showimplicit: bool,
) -> PgResult<()> {
    match subplan.subLinkType {
        SubLinkType::Exists => str_(context, "EXISTS(")?,
        SubLinkType::All => str_(context, "(ALL ")?,
        SubLinkType::Any => str_(context, "(ANY ")?,
        // ROWCOMPARE / EXPR: parenthesizing the testexpr is sufficient / no decoration.
        SubLinkType::RowCompare | SubLinkType::Expr => ch_(context, b'(')?,
        SubLinkType::MultiExpr => str_(context, "(rescan ")?,
        SubLinkType::Array => str_(context, "ARRAY(")?,
        SubLinkType::Cte => str_(context, "CTE(")?,
    }

    if let Some(testexpr) = subplan.testexpr.as_deref() {
        let mcx = context.buf.allocator();
        // Push SubPlan into ancestors list while deparsing testexpr, so we can
        // handle PARAM_EXEC references to the SubPlan's paramIds.
        // dpns = linitial(context->namespaces); dpns->ancestors = lcons(subplan, ...).
        let sub_node = mcx::alloc_in(
            mcx,
            Node::mk_expr(
                mcx,
                Expr::SubPlan(types_nodes::primnodes::SubPlanExpr::from_subplan(mcx, subplan)?),
            )?,
        )?;
        let dpns = &mut context.namespaces[0];
        let mut new_ancestors = PgVec::new_in(mcx);
        new_ancestors
            .try_reserve(dpns.ancestors.len() + 1)
            .map_err(|_| mcx.oom(0))?;
        new_ancestors.push(sub_node);
        for a in dpns.ancestors.iter() {
            new_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
        }
        // Save the prior ancestors list to restore (C: list_delete_first).
        let saved = core::mem::replace(&mut dpns.ancestors, new_ancestors);

        let testexpr_owned = testexpr.clone_in(mcx)?;
        let node = Node::mk_expr(mcx, testexpr_owned)?;
        let r = get_rule_expr(&node, context, showimplicit);
        // Restore ancestors before propagating any error.
        context.namespaces[0].ancestors = saved;
        r?;
        ch_(context, b')')?;
    } else {
        // No referencing Params, so show the SubPlan's name.
        let mcx = context.buf.allocator();
        let name = subplan_name(mcx, subplan)?;
        if subplan.useHashTable {
            str_(context, "hashed ")?;
        }
        str_(context, name.as_str())?;
        ch_(context, b')')?;
    }
    Ok(())
}

/// `static void get_parameter(Param *param, deparse_context *context)`
/// (`ruleutils.c` 8687-8849). Render a `Param`:
/// (a) `PARAM_EXEC` whose value is computed by an ancestor NestLoop/SubPlan —
///     deparse the source expression (via `find_param_referent`, switching
///     deparse attention to the ancestor plan with `push_ancestor_plan`);
/// (b) `PARAM_EXEC` that is a subplan output — render `(<hashed >name).colN`
///     (via `find_param_generator`);
/// (c) `PARAM_EXTERN` whose outermost namespace supplies a function arg name —
///     render the (optionally qualified) argument name;
/// (d) otherwise — `$N`.
pub fn get_parameter(expr: &Expr, context: &mut DeparseContext<'_>) -> PgResult<()> {
    use types_nodes::primnodes::PARAM_EXTERN;

    let param = match expr {
        Expr::Param(p) => p,
        _ => return Err(elog_error("get_parameter: node is not a Param".into())),
    };

    let mcx = context.buf.allocator();

    // (a) Try to locate the expression from which the parameter was computed.
    if let Some((referent, ancestor_index)) = find_param_referent(mcx, param, context)? {
        // Switch attention to the ancestor plan node. The target ancestor node
        // is dpns.ancestors[ancestor_index]; clone it out before mutating dpns.
        let target = {
            let dpns = &context.namespaces[0];
            mcx::alloc_in(mcx, dpns.ancestors[ancestor_index].clone_in(mcx)?)?
        };
        let save = crate::push_ancestor_plan(mcx, &mut context.namespaces[0], ancestor_index, &target)?;

        // Force prefixing of Vars (they belong to the ancestor, not the
        // current scan).
        let save_varprefix = context.varprefix;
        context.varprefix = true;

        // A Param's expansion is typically a Var/Aggref/GroupingFunc/Param,
        // which need no parens; otherwise parenthesize to look atomic.
        let need_paren = !matches!(
            referent,
            Expr::Var(_) | Expr::Aggref(_) | Expr::GroupingFunc(_) | Expr::Param(_)
        );
        if need_paren {
            ch_(context, b'(')?;
        }

        let node = Node::mk_expr(mcx, referent)?;
        get_rule_expr(&node, context, false)?;

        if need_paren {
            ch_(context, b')')?;
        }

        context.varprefix = save_varprefix;
        crate::pop_ancestor_plan(&mut context.namespaces[0], save);
        return Ok(());
    }

    // (b) Maybe it's a subplan output — print as a reference to the subplan.
    if let Some((name, use_hash, column)) = find_param_generator(mcx, param, context)? {
        // appendStringInfo("(%s%s).col%d", hashed?, plan_name, column+1)
        str_(context, "(")?;
        if use_hash {
            str_(context, "hashed ")?;
        }
        str_(context, name.as_str())?;
        str_(context, ").col")?;
        str_(context, &itoa(column + 1))?;
        return Ok(());
    }

    // (c) PARAM_EXTERN whose outermost namespace provides function arg names.
    if param.paramkind == PARAM_EXTERN && !context.namespaces.is_empty() {
        // dpns = llast(context->namespaces)
        let last_idx = context.namespaces.len() - 1;
        let argname = {
            let dpns = &context.namespaces[last_idx];
            if !dpns.argnames.is_empty()
                && param.paramid > 0
                && param.paramid <= dpns.numargs
            {
                dpns.argnames
                    .get((param.paramid - 1) as usize)
                    .and_then(|a| a.as_ref())
                    .map(|s| s.as_str().to_string())
            } else {
                None
            }
        };
        if let Some(argname) = argname {
            // Qualify the parameter name if any other namespace has a range
            // table.
            let mut should_qualify = false;
            for depns in context.namespaces.iter() {
                if !depns.rtable_names.is_empty() {
                    should_qualify = true;
                    break;
                }
            }
            if should_qualify {
                let funcname = context.namespaces[last_idx]
                    .funcname
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let q = quote_identifier(mcx, &funcname)?;
                str_(context, q.as_str())?;
                ch_(context, b'.')?;
            }
            let q = quote_identifier(mcx, &argname)?;
            str_(context, q.as_str())?;
            return Ok(());
        }
    }

    // (d) Not PARAM_EXEC, or couldn't find referent: just print $N.
    // (C asserts paramkind == PARAM_EXTERN, but prints $N either way.)
    debug_assert!(param.paramkind == PARAM_EXTERN);
    str_(context, "$")?;
    str_(context, &itoa(param.paramid))?;
    Ok(())
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

/// C `get_rule_expr`'s `case T_PartitionBoundSpec:` (ruleutils.c 10429-10486) —
/// render a partition's `FOR VALUES …` / `DEFAULT` bound clause for HASH, LIST
/// and RANGE strategies (and the DEFAULT partition).
fn get_rule_partition_bound_spec(
    spec: &types_nodes::ddlnodes::PartitionBoundSpec<'_>,
    context: &mut DeparseContext<'_>,
) -> PgResult<()> {
    // if (spec->is_default) { appendStringInfoString(buf, "DEFAULT"); break; }
    if spec.is_default {
        str_(context, "DEFAULT")?;
        return Ok(());
    }

    match spec.strategy {
        // PARTITION_STRATEGY_HASH = 'h'
        s if s == b'h' as i8 => {
            // appendStringInfoString(buf, "FOR VALUES");
            // appendStringInfo(buf, " WITH (modulus %d, remainder %d)", ...);
            str_(context, "FOR VALUES")?;
            str_(
                context,
                &alloc::format!(
                    " WITH (modulus {}, remainder {})",
                    spec.modulus,
                    spec.remainder
                ),
            )?;
        }

        // PARTITION_STRATEGY_LIST = 'l'
        s if s == b'l' as i8 => {
            // appendStringInfoString(buf, "FOR VALUES IN (");
            str_(context, "FOR VALUES IN (")?;
            let mut sep = "";
            for cell in spec.listdatums.iter() {
                // Const *val = lfirst_node(Const, cell); get_const_expr(val, context, -1);
                str_(context, sep)?;
                get_const_expr(cell, context, -1)?;
                sep = ", ";
            }
            ch_(context, b')')?;
        }

        // PARTITION_STRATEGY_RANGE = 'r'
        s if s == b'r' as i8 => {
            // appendStringInfo(buf, "FOR VALUES FROM %s TO %s",
            //     get_range_partbound_string(spec->lowerdatums),
            //     get_range_partbound_string(spec->upperdatums));
            let mcx = context.buf.allocator();
            let lower = get_range_partbound_string(mcx, spec.lowerdatums.as_slice())?;
            let upper = get_range_partbound_string(mcx, spec.upperdatums.as_slice())?;
            str_(context, "FOR VALUES FROM ")?;
            str_(context, &lower)?;
            str_(context, " TO ")?;
            str_(context, &upper)?;
        }

        other => {
            return Err(elog_error(alloc::format!(
                "unrecognized partition strategy: {}",
                other
            )));
        }
    }

    Ok(())
}

/// `get_range_partbound_string(bound_datums)` (ruleutils.c 13676) — a C-string
/// representation of one range partition bound, e.g. `(0)`, `(MINVALUE, 5)`.
/// Each element is a `PartitionRangeDatum` node: the MINVALUE/MAXVALUE sentinels
/// render as keywords, a VALUE renders its `Const` via `get_const_expr`.
pub fn get_range_partbound_string<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    bound_datums: &[types_nodes::nodes::NodePtr<'_>],
) -> PgResult<alloc::string::String> {
    use types_nodes::partition::PartitionRangeDatumKind;

    // memset(&context, 0, sizeof(deparse_context)); context.buf = makeStringInfo();
    let mut context = DeparseContext {
        buf: types_stringinfo::StringInfo::new_in(mcx),
        namespaces: mcx::PgVec::new_in(mcx),
        resultDesc: None,
        targetList: mcx::PgVec::new_in(mcx),
        windowClause: mcx::PgVec::new_in(mcx),
        prettyFlags: 0,
        wrapColumn: -1,
        indentLevel: 0,
        varprefix: false,
        colNamesVisible: false,
        inGroupBy: false,
        varInOrderBy: false,
        appendparents: None,
    };

    ch_(&mut context, b'(')?;
    let mut sep = "";
    for cell in bound_datums.iter() {
        let datum = cell.as_partitionrangedatum().ok_or_else(|| {
            elog_error("get_range_partbound_string: not a PartitionRangeDatum".to_string())
        })?;

        str_(&mut context, sep)?;
        match datum.kind {
            PartitionRangeDatumKind::MinValue => str_(&mut context, "MINVALUE")?,
            PartitionRangeDatumKind::MaxValue => str_(&mut context, "MAXVALUE")?,
            PartitionRangeDatumKind::Value => {
                let val = datum.value.as_deref().ok_or_else(|| {
                    elog_error("get_range_partbound_string: VALUE datum has no Const".to_string())
                })?;
                get_const_expr(val, &mut context, -1)?;
            }
        }
        sep = ", ";
    }
    ch_(&mut context, b')')?;

    alloc::string::String::from_utf8(context.buf.data.as_slice().to_vec())
        .map_err(|_| elog_error("get_range_partbound_string: invalid UTF-8".to_string()))
}

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
const INNER_VAR: i32 = -1;
const OUTER_VAR: i32 = -2;
const INDEX_VAR: i32 = -3;

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
                    Some(e) => Ok(Some(Node::mk_expr(mcx, (**e).clone_in(mcx)?)?)),
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

/// The two `rsv_callback`s used in `ruleutils.c` for `resolve_special_varno`:
/// `get_special_variable` (from `get_variable`, no callback_arg) and
/// `get_agg_combine_expr` (from `get_agg_expr_helper`, callback_arg is the
/// original Aggref whose combine split is being deparsed).
enum RsvCallback<'a> {
    SpecialVariable,
    AggCombineExpr(&'a Expr),
}

impl<'a> RsvCallback<'a> {
    fn invoke<'mcx>(&self, node: &Node<'mcx>, context: &mut DeparseContext<'mcx>) -> PgResult<()> {
        match self {
            RsvCallback::SpecialVariable => get_special_variable(node, context),
            RsvCallback::AggCombineExpr(original_aggref) => {
                get_agg_combine_expr(node, context, original_aggref)
            }
        }
    }
}

/// `resolve_special_varno(node, context, callback, callback_arg)` (`ruleutils.c`
/// 7920-8000) — chase a special-varno `Var` (OUTER_VAR / INNER_VAR / INDEX_VAR)
/// down through the plan tree's referent targetlists to the real expression, then
/// invoke the rendering callback. Recursive (the resolved expr may itself be a
/// special Var).
fn resolve_special_varno<'mcx>(
    node: &Node<'mcx>,
    context: &mut DeparseContext<'mcx>,
    callback: &RsvCallback<'_>,
) -> PgResult<()> {
    // If it's not a Var, invoke the callback.
    let var = match node.as_var() {
        Some(v) => v.clone(),
        None => return callback.invoke(node, context),
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
            // If we're descending to the first child of an Append/MergeAppend,
            // update appendparents (bms_union with the node's apprelids). This
            // affects deparsing of all child Vars in the resolved subexpression
            // (ruleutils.c:7948-7956).
            let apprelids: Option<types_nodes::bitmapset::Bitmapset<'mcx>> = {
                let plan = context.namespaces[dpns_idx].plan.as_ref();
                match plan {
                    Some(p) if p.node_tag() == types_nodes::nodes::ntag::T_Append => p
                        .as_append()
                        .and_then(|a| a.apprelids.as_deref())
                        .map(|b| b.clone_in(mcx))
                        .transpose()?,
                    Some(p) if p.node_tag() == types_nodes::nodes::ntag::T_MergeAppend => p
                        .as_mergeappend()
                        .and_then(|m| m.apprelids.as_deref())
                        .map(|b| b.clone_in(mcx))
                        .transpose()?,
                    _ => None,
                }
            };
            if let Some(ar) = apprelids {
                let unioned = backend_nodes_core_seams::bms_union::call(
                    mcx,
                    context.appendparents.as_ref(),
                    Some(&ar),
                )?;
                context.appendparents = match unioned {
                    Some(b) => Some((*b).clone_in(mcx)?),
                    None => None,
                };
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
        resolve_special_varno(&tle_expr, context, callback)?;
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
        resolve_special_varno(&tle_expr, context, callback)?;
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
        resolve_special_varno(&tle_expr, context, callback)?;
        return Ok(());
    } else if var.varno < 1 || var.varno > context.namespaces[dpns_idx].rtable.len() as i32 {
        return Err(elog_error(format!("bogus varno: {}", var.varno)));
    }

    // Not special. Just invoke the callback.
    callback.invoke(node, context)
}

/// `get_agg_combine_expr(node, context, callback_arg)` (`ruleutils.c` 11009-11020)
/// — the `resolve_special_varno` callback for a combining aggregate: the resolved
/// node must be the partial `Aggref`; deparse it via `get_agg_expr`, carrying the
/// original (combining) Aggref so PARTIAL/aggsplit decisions look at the original.
fn get_agg_combine_expr<'mcx>(
    node: &Node<'mcx>,
    context: &mut DeparseContext<'mcx>,
    original_aggref: &Expr,
) -> PgResult<()> {
    let aggref = match node.as_expr() {
        Some(e) if matches!(e, Expr::Aggref(_)) => e,
        _ => {
            return Err(elog_error(
                "combining Aggref does not point to an Aggref".to_string(),
            ))
        }
    };
    get_agg_expr(aggref, context, original_aggref)
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
        let node = Node::mk_expr(context.buf.allocator(), Expr::Var(var.clone()))?;
        resolve_special_varno(&node, context, &RsvCallback::SpecialVariable)?;
        return Ok(None);
    }

    // We might have been asked to map child Vars to some parent relation
    // (ruleutils.c:7654-7696). Walk up the AppendRelInfo chain to an inheritance
    // parent; if that ancestor is in appendparents, print its column instead.
    let (varno, varattno) = {
        let dpns = &context.namespaces[dpns_idx];
        if context.appendparents.is_some() && !dpns.appendrels.is_empty() {
            let mut pvarno = varno;
            let mut pvarattno = varattno;
            let mut found = false;
            // appinfo = dpns->appendrels[pvarno]
            loop {
                let appinfo = match dpns.appendrels.get(pvarno as usize).and_then(|o| o.as_ref()) {
                    Some(a) => a,
                    None => break,
                };
                // Only map up to inheritance parents, not UNION ALL appendrels:
                // rt_fetch(appinfo->parent_relid)->rtekind == RTE_RELATION.
                let parent_idx = appinfo.parent_relid as usize;
                let is_rel_parent = parent_idx >= 1
                    && parent_idx <= dpns.rtable.len()
                    && dpns.rtable[parent_idx - 1].rtekind == RTE_RELATION;
                if !is_rel_parent {
                    break;
                }
                found = false;
                if pvarattno > 0 {
                    // system columns stay as-is
                    if pvarattno as i32 > appinfo.num_child_cols {
                        break; // safety check
                    }
                    pvarattno = appinfo.parent_colnos[(pvarattno - 1) as usize];
                    if pvarattno == 0 {
                        break; // Var is local to child
                    }
                }
                pvarno = appinfo.parent_relid as i32;
                found = true;
                // If the parent is itself a child, continue up (loop re-reads
                // dpns.appendrels[pvarno]).
            }
            // If we found an ancestral rel in appendparents, use its column.
            let in_appendparents = found
                && backend_nodes_core_seams::bms_is_member::call(
                    pvarno,
                    context.appendparents.as_ref(),
                );
            if in_appendparents {
                (pvarno, pvarattno)
            } else {
                (varno, varattno)
            }
        } else {
            (varno, varattno)
        }
    };

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

/// `NameStr(TupleDescAttr(tupleDesc, fieldno - 1)->attname)` — extract a field
/// name from a result tuple descriptor, copied into `mcx`.
fn tupdesc_field_name<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &types_tuple::heaptuple::TupleDesc<'mcx>,
    fieldno: i32,
) -> PgResult<PgString<'mcx>> {
    let rd = tupdesc
        .as_ref()
        .ok_or_else(|| elog_error("get_expr_result_tupdesc returned no descriptor".to_string()))?;
    // Assert(fieldno >= 1 && fieldno <= tupleDesc->natts).
    if fieldno < 1 || fieldno > rd.natts {
        return Err(elog_error(format!(
            "bogus fieldno {fieldno} for tuple descriptor with {} attributes",
            rd.natts
        )));
    }
    let attr = rd
        .attrs
        .get((fieldno - 1) as usize)
        .ok_or_else(|| elog_error(format!("tupdesc attr {} out of range", fieldno - 1)))?;
    let name = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
    PgString::from_str_in(&name, mcx)
}

/// `list_copy_tail(context->namespaces, n)` — clone the namespace stack from
/// index `n` onward (the parent namespaces for a nested subquery/CTE recursion).
fn clone_namespaces_tail<'mcx>(
    mcx: Mcx<'mcx>,
    context: &DeparseContext<'mcx>,
    n: usize,
) -> PgResult<PgVec<'mcx, crate::DeparseNamespace<'mcx>>> {
    let mut pv: PgVec<'mcx, crate::DeparseNamespace<'mcx>> = PgVec::new_in(mcx);
    pv.try_reserve(context.namespaces.len().saturating_sub(n))
        .map_err(|_| mcx.oom(0))?;
    for ns in context.namespaces.iter().skip(n) {
        pv.push(crate::clone_namespace_pub(mcx, ns)?);
    }
    Ok(pv)
}

/// `lcons(&mydpns, parent_namespaces)` — prepend a namespace to a (cloned) tail.
fn lcons_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    head: crate::DeparseNamespace<'mcx>,
    parent: &[crate::DeparseNamespace<'mcx>],
) -> PgResult<PgVec<'mcx, crate::DeparseNamespace<'mcx>>> {
    let mut nsv: PgVec<'mcx, crate::DeparseNamespace<'mcx>> = PgVec::new_in(mcx);
    nsv.try_reserve(parent.len() + 1).map_err(|_| mcx.oom(0))?;
    nsv.push(head);
    for ns in parent.iter() {
        nsv.push(crate::clone_namespace_pub(mcx, ns)?);
    }
    Ok(nsv)
}

/// `get_name_for_var_field(Var *var, int fieldno, int levelsup,
/// deparse_context *context)` — C ruleutils.c 8017-8444.
///
/// Determine the field name to use for a FieldSelect of `var.fieldno`. The
/// query-decompilation paths (RowExpr whole-row colnames, non-RECORD Var via
/// `get_expr_result_tupdesc`, RTE-in-rtable whole-row/RTE_SUBQUERY/RTE_JOIN/
/// RTE_CTE recursion) are ported in full. The plan-tree-only branches
/// (OUTER_VAR/INNER_VAR/INDEX_VAR digging into subplan tlists, the plan-tree
/// SubqueryScan/CteScan/WorkTableScan childless-Result paths, and the Param
/// referent into an ancestor plan) require the #159 plan-tree namespace, which
/// is unported; they raise a precise deferred() the same way `get_variable`
/// gates its #159 paths.
fn get_name_for_var_field<'mcx>(
    var: &Expr,
    fieldno: i32,
    levelsup: i32,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<PgString<'mcx>> {
    use types_nodes::nodes::CmdType;
    /// `RECORDOID` (pg_type.dat) — the pseudo-type for anonymous record values.
    const RECORDOID: Oid = 2249;

    let mcx = context.buf.allocator();

    // If it's a RowExpr that was expanded from a whole-row Var, use the column
    // names attached to it.
    if let Expr::RowExpr(r) = var {
        if fieldno > 0 && fieldno <= r.colnames.len() as i32 {
            return PgString::from_str_in(r.colnames[(fieldno - 1) as usize].as_str(), mcx);
        }
    }

    // If it's a Param of type RECORD, try to find what the Param refers to.
    if let Expr::Param(param) = var {
        // find_param_referent only resolves with a plan tree (dpns->plan set);
        // for query decompilation it returns None and we fall through.
        if let Some((expr, _ancestor_idx)) = find_param_referent(mcx, param, context)? {
            // Found a match: recurse to decipher the field name. push_ancestor_plan
            // / pop_ancestor_plan is a plan-tree (#159) operation, unreachable for
            // query decompilation (find_param_referent only succeeds with a plan).
            let _ = expr;
            return Err(deferred(
                "get_name_for_var_field Param referent (push_ancestor_plan; #159 plan-tree)",
            ));
        }
    }

    // If it's a Var of type RECORD, find what it refers to; otherwise use
    // get_expr_result_tupdesc().
    let v = match var {
        Expr::Var(v) if v.vartype == RECORDOID => v,
        _ => {
            let node = Node::mk_expr(mcx, var.clone_in(mcx)?)?;
            let tupdesc =
                backend_utils_fmgr_funcapi_seams::get_expr_result_tupdesc::call(mcx, Some(&node), false)?;
            return tupdesc_field_name(mcx, &tupdesc, fieldno);
        }
    };

    // Find appropriate nesting depth.
    let netlevelsup = v.varlevelsup as i32 + levelsup;
    if netlevelsup >= context.namespaces.len() as i32 {
        return Err(elog_error(format!(
            "bogus varlevelsup: {} offset {}",
            v.varlevelsup, levelsup
        )));
    }
    let dpns_idx = netlevelsup as usize;

    // Prefer the syntactic referent when working from a parse tree.
    let dpns_has_plan = context.namespaces[dpns_idx].plan.is_some();
    let (varno, varattno) = if v.varnosyn as i32 > 0 && !dpns_has_plan {
        (v.varnosyn as i32, v.varattnosyn)
    } else {
        (v.varno, v.varattno)
    };

    // Try to find the relevant RTE in this rtable. In a plan tree it's likely
    // OUTER_VAR/INNER_VAR/INDEX_VAR — those need #159.
    let in_range = {
        let dpns = &context.namespaces[dpns_idx];
        varno >= 1 && varno <= dpns.rtable.len() as i32
    };
    if !in_range {
        return Err(deferred(
            "get_name_for_var_field special varno (OUTER_VAR/INNER_VAR/INDEX_VAR; #159 plan-tree)",
        ));
    }

    let attnum = varattno;

    // attnum == InvalidAttrNumber: whole-row reference — select the right field.
    if attnum == InvalidAttrNumber {
        let rte_clone = context.namespaces[dpns_idx].rtable[(varno - 1) as usize].clone_in(mcx)?;
        return backend_utils_adt_ruleutils_seams::get_rte_attribute_name::call(mcx, &rte_clone, fieldno as i16);
    }

    // Drill down by RTE kind. expr = (Node *) var is the default if we can't.
    let rtekind = context.namespaces[dpns_idx].rtable[(varno - 1) as usize].rtekind;
    let mut drill_expr: Expr = var.clone_in(mcx)?;

    match rtekind {
        RTE_RELATION | RTE_VALUES | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            // A column of a table/values/ENR shouldn't have type RECORD. Fall
            // through and fail (most likely) at the bottom.
        }
        RTE_SUBQUERY => {
            // Subselect-in-FROM: examine sub-select's output expr.
            let has_subquery = context.namespaces[dpns_idx].rtable[(varno - 1) as usize]
                .subquery
                .is_some();
            if has_subquery {
                let (ste_expr, aliasname): (Option<Expr>, String) = {
                    let rte = &context.namespaces[dpns_idx].rtable[(varno - 1) as usize];
                    let subquery = rte.subquery.as_ref().unwrap();
                    let aliasname = rte
                        .eref
                        .as_ref()
                        .and_then(|e| e.aliasname.as_deref())
                        .unwrap_or("")
                        .to_string();
                    let ste = subquery
                        .targetList
                        .iter()
                        .find(|tle| tle.resno == attnum && !tle.resjunk);
                    match ste {
                        Some(tle) => (tle.expr.as_ref().map(|e| (**e).clone_in(mcx)).transpose()?, aliasname),
                        None => (None, aliasname),
                    }
                };
                let expr = ste_expr.ok_or_else(|| {
                    elog_error(format!("subquery {aliasname} does not have attribute {attnum}"))
                })?;
                if let Expr::Var(_) = &expr {
                    // Recurse into the sub-select. Build an additional namespace
                    // level (parent_namespaces = tail from netlevelsup).
                    let subquery_clone = {
                        let rte = &context.namespaces[dpns_idx].rtable[(varno - 1) as usize];
                        rte.subquery.as_ref().unwrap().clone_in(mcx)?
                    };
                    let parent_namespaces = clone_namespaces_tail(mcx, context, netlevelsup as usize)?;
                    let mut mydpns = crate::DeparseNamespace::zeroed(mcx);
                    crate::set_deparse_for_query(mcx, &mut mydpns, &subquery_clone, &parent_namespaces)?;
                    let new_ns = lcons_namespace(mcx, mydpns, &parent_namespaces)?;
                    let saved = core::mem::replace(&mut context.namespaces, new_ns);
                    let result = get_name_for_var_field(&expr, fieldno, 0, context);
                    context.namespaces = saved;
                    return result;
                }
                drill_expr = expr;
            } else {
                // Plan-tree SubqueryScan / childless-Result path (#159).
                return Err(deferred(
                    "get_name_for_var_field RTE_SUBQUERY plan-tree (SubqueryScan inner_plan; #159)",
                ));
            }
        }
        RTE_JOIN => {
            // Join RTE: recursively inspect the alias variable.
            let aliasvar: Option<Expr> = {
                let rte = &context.namespaces[dpns_idx].rtable[(varno - 1) as usize];
                if rte.joinaliasvars.is_empty() {
                    return Err(elog_error(
                        "cannot decompile join alias var in plan tree".to_string(),
                    ));
                }
                rte.joinaliasvars
                    .get((attnum - 1) as usize)
                    .and_then(|n| n.as_expr())
                    .map(|e| e.clone_in(mcx))
                    .transpose()?
            };
            let expr = aliasvar
                .ok_or_else(|| elog_error("join alias var is NULL".to_string()))?;
            // we intentionally don't strip implicit coercions here
            if let Expr::Var(av) = &expr {
                return get_name_for_var_field(
                    &Expr::Var(av.clone()),
                    fieldno,
                    v.varlevelsup as i32 + levelsup,
                    context,
                );
            }
            drill_expr = expr;
        }
        RTE_FUNCTION | RTE_TABLEFUNC => {
            // A function declared with a RECORD result column is not allowed —
            // we can't get here. Fall through and fail at the bottom.
        }
        RTE_CTE => {
            // CTE reference: examine subquery's output expr.
            let (ctelevelsup, ctename) = {
                let rte = &context.namespaces[dpns_idx].rtable[(varno - 1) as usize];
                (
                    rte.ctelevelsup as i32 + netlevelsup,
                    rte.ctename.as_deref().unwrap_or("").to_string(),
                )
            };
            // Try to find the referenced CTE using the namespace stack.
            let found_cte: Option<Expr> = if ctelevelsup >= context.namespaces.len() as i32 {
                None
            } else {
                let ctedpns = &context.namespaces[ctelevelsup as usize];
                let mut found: Option<Expr> = None;
                for cte_node in ctedpns.ctes.iter() {
                    if let Some(cte) = cte_node.as_commontableexpr() {
                        if cte.ctename.as_deref() == Some(ctename.as_str()) {
                            // GetCTETargetList(cte): SELECT->targetList else returningList.
                            let ctequery = cte
                                .ctequery
                                .as_ref()
                                .and_then(|q| q.as_query())
                                .ok_or_else(|| elog_error("CTE ctequery is not a Query".to_string()))?;
                            let tlist = if ctequery.commandType == CmdType::CMD_SELECT {
                                &ctequery.targetList
                            } else {
                                &ctequery.returningList
                            };
                            let ste = tlist.iter().find(|tle| tle.resno == attnum && !tle.resjunk);
                            let ste = ste.ok_or_else(|| {
                                elog_error(format!("CTE {ctename} does not have attribute {attnum}"))
                            })?;
                            found = Some(
                                ste.expr
                                    .as_deref()
                                    .ok_or_else(|| missing_field("CTE TargetEntry.expr"))?
                                    .clone_in(mcx)?,
                            );
                            break;
                        }
                    }
                }
                found
            };
            match found_cte {
                Some(expr) => {
                    if let Expr::Var(_) = &expr {
                        // Recurse into the CTE; build an additional namespace level.
                        // ctequery for the matching CTE.
                        let ctequery_clone = {
                            let ctedpns = &context.namespaces[ctelevelsup as usize];
                            let mut q = None;
                            for cte_node in ctedpns.ctes.iter() {
                                if let Some(cte) = cte_node.as_commontableexpr() {
                                    if cte.ctename.as_deref() == Some(ctename.as_str()) {
                                        q = cte
                                            .ctequery
                                            .as_ref()
                                            .and_then(|n| n.as_query())
                                            .map(|qq| qq.clone_in(mcx))
                                            .transpose()?;
                                        break;
                                    }
                                }
                            }
                            q.ok_or_else(|| elog_error("CTE ctequery vanished".to_string()))?
                        };
                        let parent_namespaces =
                            clone_namespaces_tail(mcx, context, ctelevelsup as usize)?;
                        let mut mydpns = crate::DeparseNamespace::zeroed(mcx);
                        crate::set_deparse_for_query(mcx, &mut mydpns, &ctequery_clone, &parent_namespaces)?;
                        let new_ns = lcons_namespace(mcx, mydpns, &parent_namespaces)?;
                        let saved = core::mem::replace(&mut context.namespaces, new_ns);
                        let result = get_name_for_var_field(&expr, fieldno, 0, context);
                        context.namespaces = saved;
                        return result;
                    }
                    drill_expr = expr;
                }
                None => {
                    // Plan-tree CteScan / WorkTableScan path (#159).
                    return Err(deferred(
                        "get_name_for_var_field RTE_CTE plan-tree (CteScan inner_plan; #159)",
                    ));
                }
            }
        }
        RTE_GROUP => {
            // Vars referencing RTE_GROUP should have been replaced with the
            // underlying grouping expressions; we can't get here.
        }
        _ => {}
    }

    // We now have an expression we can't expand any more; let
    // get_expr_result_tupdesc() take a crack at it.
    let node = Node::mk_expr(mcx, drill_expr)?;
    let tupdesc =
        backend_utils_fmgr_funcapi_seams::get_expr_result_tupdesc::call(mcx, Some(&node), false)?;
    tupdesc_field_name(mcx, &tupdesc, fieldno)
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
                    let opb = match get_simple_binary_op_name(op) {
                        Some(b) => b,
                        None => return false,
                    };
                    let is_lopriop = opb == b'+' || opb == b'-';
                    let is_hipriop = opb == b'*' || opb == b'/' || opb == b'%';
                    if !(is_lopriop || is_hipriop) {
                        return false;
                    }
                    let pb = match get_simple_binary_op_name(parent_op) {
                        Some(b) => b,
                        None => return false,
                    };
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
/// Returns the operator's name byte iff `expr` is a 2-arg OpExpr whose name is a
/// single character (C: `generate_operator_name(...)` then `strlen(op) == 1`).
///
/// C calls `generate_operator_name(expr->opno, exprType(arg1), exprType(arg2))`
/// and keeps the result only when one byte long. `isSimpleNode` has no `Mcx`, so
/// instead of allocating the qualified name we read `pg_operator.oprname`
/// directly through the allocation-free `get_op_name_single_byte` seam: a
/// single-character operator name never requires schema qualification, so it
/// equals `generate_operator_name`'s output. The downstream precedence test only
/// inspects this single byte, so no information is lost.
fn get_simple_binary_op_name(op: &OpExpr) -> Option<u8> {
    if op.args.len() != 2 {
        return None;
    }
    match backend_utils_cache_lsyscache_seams::get_op_name_single_byte::call(op.opno) {
        Ok(b) => b,
        Err(_) => None,
    }
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
    fn parameter_extern_renders_dollar_n() {
        // A PARAM_EXTERN with no function-arg namespace renders as `$N`
        // (get_parameter's fall-through case (d), ruleutils.c 8842-8848).
        use types_nodes::primnodes::{Param, ParamKind};
        let cx = MemoryContext::new("param");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0);
        let p = Expr::Param(Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: 5,
            paramtype: 0,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        });
        get_parameter(&p, &mut c).unwrap();
        assert_eq!(bufstr(&c), "$5");
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
