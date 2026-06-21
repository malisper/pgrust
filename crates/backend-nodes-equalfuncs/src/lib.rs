//! Port of `src/backend/nodes/equalfuncs.c` — the generic structural
//! node-equality routine `equal()` and the per-node `_equalXxx` comparators
//! that `gen_node_support.pl` generates into `equalfuncs.funcs.c` /
//! `equalfuncs.switch.c`.
//!
//! `equal(a, b)` returns whether two node trees are structurally equal. Per the
//! `equalfuncs.c` header note, parse-location fields and `CoercionForm` fields
//! are intentionally NOT compared (so e.g. a Var "x" equals another reference to
//! "x"); the `COMPARE_LOCATION_FIELD` / `COMPARE_COERCIONFORM_FIELD` macros are
//! no-ops, mirrored here by simply not comparing `location` / `*format`.
//!
//! # Model mapping
//!
//! In C, `equal()` switches on `nodeTag()` and dispatches to a `_equalXxx`
//! comparator that compares fields with the `COMPARE_*_FIELD` macros. This
//! repo carries expression nodes as the owned [`types_nodes::primnodes::Expr`]
//! enum (the `Expr *` discriminated union) and the wider node universe as
//! [`types_nodes::nodes::Node`]; the `nodeTag` switch becomes a `match` over the
//! enum variant. `COMPARE_NODE_FIELD` over a child `Expr`/`Node` recurses;
//! `COMPARE_SCALAR_FIELD` is `==`; `COMPARE_BITMAPSET_FIELD` is `==` over the
//! relids word storage; `COMPARE_STRING_FIELD` is the NULL-aware string compare.
//!
//! This unit OWNS and INSTALLS the central `equal()` seams declared in
//! `backend-nodes-equalfuncs-seams` (consumed by the optimizer/parser):
//! `equal_expr`, `equal_node`, and the three list-equal forms
//! (`equal_expr_list`, `equal_targetentry_list`, `equal_sortgroupclause_list`).

#![allow(non_snake_case)]

use backend_nodes_node_support::PgNodeEqual;
use types_nodes::nodes::ntag;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    Aggref, AlternativeSubPlan, ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CaseExpr,
    CaseTestExpr, CaseWhen, CoalesceExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO,
    CollateExpr, Const, ConvertRowtypeExpr, CurrentOfExpr, Expr, FieldSelect, FieldStore, FuncExpr,
    GroupingFunc, InferenceElem, JsonConstructorExpr, JsonExpr, JsonIsPredicate, JsonValueExpr,
    MergeSupportFunc, MinMaxExpr, NamedArgExpr, NextValueExpr, NullTest, OpExpr, Param,
    PlaceHolderVar, RelabelType, ReturningExpr, RowCompareExpr, RowExpr, SQLValueFunction,
    ScalarArrayOpExpr, SetToDefault, SubLink, SubPlan, SubscriptingRef, TargetEntry, Var,
    WindowFunc, XmlExpr,
};
use types_nodes::rawnodes::SortGroupClause;

// ===========================================================================
// COMPARE_*_FIELD helpers
// ===========================================================================

/// `COMPARE_STRING_FIELD` / `equalstr(a, b)`: NULL-aware string compare —
/// `((a != NULL && b != NULL) ? strcmp(a, b) == 0 : a == b)`. Two present
/// strings compare by value; otherwise equal iff both absent.
#[inline]
fn equalstr(a: Option<&str>, b: Option<&str>) -> bool {
    (match (a, b) {
        (Some(x), Some(y)) => x == y,
        (None, None) => true,
        _ => false,
    })
}

/// `COMPARE_NODE_FIELD` over an optional child `Expr` (`Expr *`, NULL-able):
/// both NULL is equal; one NULL is unequal; else recurse.
#[inline]
fn equal_opt_expr(a: Option<&Expr>, b: Option<&Expr>) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_expr(x, y),
        _ => false,
    })
}

/// `COMPARE_NODE_FIELD` over a `List *` of `Expr *` (`_equalList`): equal length
/// then element-wise `equal()`.
#[inline]
fn equal_expr_list_impl(a: &[Expr], b: &[Expr]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| equal_expr(x, y))
}

/// `COMPARE_NODE_FIELD` over a `List *` of `Expr *` that may contain NULL
/// elements (`SubscriptingRef.refupperindexpr`/`reflowerindexpr` carry NULL for
/// omitted/single-subscript positions). Length then element-wise NULL-aware.
#[inline]
fn equal_opt_expr_list(a: &[Option<Expr>], b: &[Option<Expr>]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| equal_opt_expr(x.as_ref(), y.as_ref()))
}

/// `COMPARE_NODE_FIELD` over a `List *` of `CaseWhen *`.
#[inline]
fn equal_casewhen_list(a: &[CaseWhen], b: &[CaseWhen]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| equal_case_when(x, y))
}

/// `COMPARE_NODE_FIELD` over a `List *` of `TargetEntry *`.
#[inline]
fn equal_targetentry_list_impl(a: &[TargetEntry<'_>], b: &[TargetEntry<'_>]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| equal_target_entry(x, y))
}

/// `COMPARE_NODE_FIELD` over a `List *` of `SortGroupClause *`.
#[inline]
fn equal_sortgroupclause_list_impl(a: &[SortGroupClause], b: &[SortGroupClause]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| equal_sort_group_clause(x, y))
}

// ===========================================================================
// Per-node comparators (`_equalXxx`), mirroring equalfuncs.funcs.c
// ===========================================================================

fn equal_var(a: &Var, b: &Var) -> bool {
    a.varno == b.varno
        && a.varattno == b.varattno
        && a.vartype == b.vartype
        && a.vartypmod == b.vartypmod
        && a.varcollid == b.varcollid
        && a.varnullingrels == b.varnullingrels // COMPARE_BITMAPSET_FIELD
        && a.varlevelsup == b.varlevelsup
        && a.varreturningtype == b.varreturningtype
    // varnosyn / varattnosyn are not compared by equalfuncs.c (gen marks them
    // `equal_ignore`); location is COMPARE_LOCATION_FIELD (no-op).
}

fn equal_const(a: &Const, b: &Const) -> bool {
    if a.consttype != b.consttype
        || a.consttypmod != b.consttypmod
        || a.constcollid != b.constcollid
        || a.constisnull != b.constisnull
    {
        return false;
    }
    // C also COMPARE_SCALAR_FIELD(constlen)/(constbyval); those byval/len fields
    // were dropped from this repo's trimmed `Const`, but the canonical `Datum`
    // enum carries the by-value / by-reference distinction itself, so comparing
    // `constvalue` covers them faithfully.
    //
    // "We treat all NULL constants of the same type as equal": if isnull, equal.
    if a.constisnull {
        return true;
    }
    // datumIsEqual(constvalue, constvalue, ...) — the canonical Datum enum's
    // PartialEq is exactly the byte-model datumIsEqual (ByVal word / ByRef bytes).
    a.constvalue == b.constvalue
}

fn equal_param(a: &Param, b: &Param) -> bool {
    a.paramkind == b.paramkind
        && a.paramid == b.paramid
        && a.paramtype == b.paramtype
        && a.paramtypmod == b.paramtypmod
        && a.paramcollid == b.paramcollid
}

fn equal_aggref(a: &Aggref, b: &Aggref) -> bool {
    a.aggfnoid == b.aggfnoid
        && a.aggtype == b.aggtype
        && a.aggcollid == b.aggcollid
        && a.inputcollid == b.inputcollid
        // aggtranstype is set by the planner and is not compared by equalfuncs.c
        // (gen marks it `equal_ignore` via the `query_jumble_ignore`-adjacent
        // attribute set); the generated _equalAggref does NOT compare it.
        && a.aggargtypes == b.aggargtypes // List of Oid
        && equal_expr_list_impl(&a.aggdirectargs, &b.aggdirectargs)
        && equal_targetentry_list_impl(&a.args, &b.args)
        && equal_sortgroupclause_list_impl(&a.aggorder, &b.aggorder)
        && equal_sortgroupclause_list_impl(&a.aggdistinct, &b.aggdistinct)
        && equal_opt_expr(a.aggfilter.as_deref(), b.aggfilter.as_deref())
        && a.aggstar == b.aggstar
        && a.aggvariadic == b.aggvariadic
        && a.aggkind == b.aggkind
        && a.agglevelsup == b.agglevelsup
        && a.aggsplit == b.aggsplit
        && a.aggno == b.aggno
        && a.aggtransno == b.aggtransno
}

fn equal_grouping_func(a: &GroupingFunc, b: &GroupingFunc) -> bool {
    // _equalGroupingFunc compares only args + agglevelsup (refs/cols are
    // `equal_ignore`).
    equal_expr_list_impl(&a.args, &b.args) && a.agglevelsup == b.agglevelsup
}

fn equal_window_func(a: &WindowFunc, b: &WindowFunc) -> bool {
    a.winfnoid == b.winfnoid
        && a.wintype == b.wintype
        && a.wincollid == b.wincollid
        && a.inputcollid == b.inputcollid
        && equal_expr_list_impl(&a.args, &b.args)
        && equal_opt_expr(a.aggfilter.as_deref(), b.aggfilter.as_deref())
        && equal_expr_list_impl(&a.runCondition, &b.runCondition)
        && a.winref == b.winref
        && a.winstar == b.winstar
        && a.winagg == b.winagg
}

fn equal_merge_support_func(a: &MergeSupportFunc, b: &MergeSupportFunc) -> bool {
    a.msftype == b.msftype && a.msfcollid == b.msfcollid
}

fn equal_subscripting_ref(a: &SubscriptingRef, b: &SubscriptingRef) -> bool {
    a.refcontainertype == b.refcontainertype
        && a.refelemtype == b.refelemtype
        && a.refrestype == b.refrestype
        && a.reftypmod == b.reftypmod
        && a.refcollid == b.refcollid
        && equal_opt_expr_list(&a.refupperindexpr, &b.refupperindexpr)
        && equal_opt_expr_list(&a.reflowerindexpr, &b.reflowerindexpr)
        && equal_opt_expr(a.refexpr.as_deref(), b.refexpr.as_deref())
        && equal_opt_expr(a.refassgnexpr.as_deref(), b.refassgnexpr.as_deref())
}

fn equal_func_expr(a: &FuncExpr, b: &FuncExpr) -> bool {
    a.funcid == b.funcid
        && a.funcresulttype == b.funcresulttype
        && a.funcretset == b.funcretset
        && a.funcvariadic == b.funcvariadic
        // funcformat is COMPARE_COERCIONFORM_FIELD (no-op).
        && a.funccollid == b.funccollid
        && a.inputcollid == b.inputcollid
        && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_named_arg_expr(a: &NamedArgExpr, b: &NamedArgExpr) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.name == b.name
        && a.argnumber == b.argnumber
}

/// `_equalOpExpr` (also `_equalDistinctExpr` / `_equalNullIfExpr`; same payload).
/// Note the special `opfuncid` rule: not compared if either side is unset (0).
fn equal_op_expr(a: &OpExpr, b: &OpExpr) -> bool {
    if a.opno != b.opno {
        return false;
    }
    if a.opfuncid != b.opfuncid && a.opfuncid != 0 && b.opfuncid != 0 {
        return false;
    }
    a.opresulttype == b.opresulttype
        && a.opretset == b.opretset
        && a.opcollid == b.opcollid
        && a.inputcollid == b.inputcollid
        && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_scalar_array_op_expr(a: &ScalarArrayOpExpr, b: &ScalarArrayOpExpr) -> bool {
    if a.opno != b.opno {
        return false;
    }
    if a.opfuncid != b.opfuncid && a.opfuncid != 0 && b.opfuncid != 0 {
        return false;
    }
    if a.hashfuncid != b.hashfuncid && a.hashfuncid != 0 && b.hashfuncid != 0 {
        return false;
    }
    if a.negfuncid != b.negfuncid && a.negfuncid != 0 && b.negfuncid != 0 {
        return false;
    }
    a.useOr == b.useOr
        && a.inputcollid == b.inputcollid
        && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_bool_expr(a: &BoolExpr, b: &BoolExpr) -> bool {
    a.boolop == b.boolop && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_sub_link(a: &SubLink, b: &SubLink) -> bool {
    // _equalSubLink: subLinkType, subLinkId, testexpr, operName, subselect.
    // `operName` is a parse-only List<String> that this repo's analyzed SubLink
    // does not carry; `subselect` is the embedded owned sub-`Query`, compared
    // with `COMPARE_NODE_FIELD(subselect)`.
    a.subLinkType == b.subLinkType
        && a.subLinkId == b.subLinkId
        && equal_opt_expr(a.testexpr.as_deref(), b.testexpr.as_deref())
        && equal_opt_subselect(a.subselect.as_deref(), b.subselect.as_deref())
}

/// `COMPARE_NODE_FIELD(subselect)` over the embedded `Query`. Both `None` is
/// equal; both `Some` defers to the per-node `Query` comparator [`equal_query`]
/// (`_equalQuery`).
fn equal_opt_subselect(
    a: Option<&types_nodes::copy_query::Query<'_>>,
    b: Option<&types_nodes::copy_query::Query<'_>>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_query(x, y),
        _ => false,
    })
}

fn equal_field_select(a: &FieldSelect, b: &FieldSelect) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.fieldnum == b.fieldnum
        && a.resulttype == b.resulttype
        && a.resulttypmod == b.resulttypmod
        && a.resultcollid == b.resultcollid
}

fn equal_field_store(a: &FieldStore, b: &FieldStore) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && equal_expr_list_impl(&a.newvals, &b.newvals)
        && a.fieldnums == b.fieldnums // List of AttrNumber
        && a.resulttype == b.resulttype
}

fn equal_relabel_type(a: &RelabelType, b: &RelabelType) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.resulttype == b.resulttype
        && a.resulttypmod == b.resulttypmod
        && a.resultcollid == b.resultcollid
    // relabelformat is COMPARE_COERCIONFORM_FIELD (no-op).
}

fn equal_coerce_via_io(a: &CoerceViaIO, b: &CoerceViaIO) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.resulttype == b.resulttype
        && a.resultcollid == b.resultcollid
    // coerceformat is COMPARE_COERCIONFORM_FIELD (no-op).
}

fn equal_array_coerce_expr(a: &ArrayCoerceExpr, b: &ArrayCoerceExpr) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && equal_opt_expr(a.elemexpr.as_deref(), b.elemexpr.as_deref())
        && a.resulttype == b.resulttype
        && a.resulttypmod == b.resulttypmod
        && a.resultcollid == b.resultcollid
    // coerceformat is COMPARE_COERCIONFORM_FIELD (no-op).
}

fn equal_convert_rowtype_expr(a: &ConvertRowtypeExpr, b: &ConvertRowtypeExpr) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref()) && a.resulttype == b.resulttype
    // convertformat is COMPARE_COERCIONFORM_FIELD (no-op).
}

fn equal_collate_expr(a: &CollateExpr, b: &CollateExpr) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref()) && a.collOid == b.collOid
}

fn equal_case_expr(a: &CaseExpr, b: &CaseExpr) -> bool {
    a.casetype == b.casetype
        && a.casecollid == b.casecollid
        && equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && equal_casewhen_list(&a.args, &b.args)
        && equal_opt_expr(a.defresult.as_deref(), b.defresult.as_deref())
}

fn equal_case_when(a: &CaseWhen, b: &CaseWhen) -> bool {
    equal_opt_expr(a.expr.as_deref(), b.expr.as_deref())
        && equal_opt_expr(a.result.as_deref(), b.result.as_deref())
}

fn equal_case_test_expr(a: &CaseTestExpr, b: &CaseTestExpr) -> bool {
    a.typeId == b.typeId && a.typeMod == b.typeMod && a.collation == b.collation
}

fn equal_array_expr(a: &ArrayExpr, b: &ArrayExpr) -> bool {
    a.array_typeid == b.array_typeid
        && a.array_collid == b.array_collid
        && a.element_typeid == b.element_typeid
        && equal_expr_list_impl(&a.elements, &b.elements)
        && a.multidims == b.multidims
    // list_start / list_end / location are COMPARE_LOCATION_FIELD (no-op); the
    // repo's ArrayExpr trims list_start/list_end (location-only fields).
}

fn equal_row_expr(a: &RowExpr, b: &RowExpr) -> bool {
    equal_expr_list_impl(&a.args, &b.args)
        && a.row_typeid == b.row_typeid
        // row_format is COMPARE_COERCIONFORM_FIELD (no-op).
        && a.colnames == b.colnames // List of String
}

fn equal_row_compare_expr(a: &RowCompareExpr, b: &RowCompareExpr) -> bool {
    a.cmptype == b.cmptype
        && a.opnos == b.opnos // List of Oid
        && a.opfamilies == b.opfamilies // List of Oid
        && a.inputcollids == b.inputcollids // List of Oid
        && equal_expr_list_impl(&a.largs, &b.largs)
        && equal_expr_list_impl(&a.rargs, &b.rargs)
}

fn equal_coalesce_expr(a: &CoalesceExpr, b: &CoalesceExpr) -> bool {
    a.coalescetype == b.coalescetype
        && a.coalescecollid == b.coalescecollid
        && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_min_max_expr(a: &MinMaxExpr, b: &MinMaxExpr) -> bool {
    a.minmaxtype == b.minmaxtype
        && a.minmaxcollid == b.minmaxcollid
        && a.inputcollid == b.inputcollid
        && a.op == b.op
        && equal_expr_list_impl(&a.args, &b.args)
}

fn equal_sqlvalue_function(a: &SQLValueFunction, b: &SQLValueFunction) -> bool {
    a.op == b.op && a.r#type == b.r#type && a.typmod == b.typmod
}

fn equal_xml_expr(a: &XmlExpr, b: &XmlExpr) -> bool {
    a.op == b.op
        && a.name == b.name
        && equal_expr_list_impl(&a.named_args, &b.named_args)
        && a.arg_names == b.arg_names // List of String
        && equal_expr_list_impl(&a.args, &b.args)
        && a.xmloption == b.xmloption
        && a.indent == b.indent
        && a.r#type == b.r#type
        && a.typmod == b.typmod
}

/// `_equalJsonFormat`.
fn equal_json_format(a: &types_nodes::primnodes::JsonFormat, b: &types_nodes::primnodes::JsonFormat) -> bool {
    a.format_type == b.format_type && a.encoding == b.encoding
}

#[inline]
fn equal_opt_json_format(
    a: Option<&types_nodes::primnodes::JsonFormat>,
    b: Option<&types_nodes::primnodes::JsonFormat>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_format(x, y),
        _ => false,
    })
}

/// `_equalJsonReturning`.
fn equal_json_returning(
    a: &types_nodes::primnodes::JsonReturning,
    b: &types_nodes::primnodes::JsonReturning,
) -> bool {
    equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
        && a.typid == b.typid
        && a.typmod == b.typmod
}

#[inline]
fn equal_opt_json_returning(
    a: Option<&types_nodes::primnodes::JsonReturning>,
    b: Option<&types_nodes::primnodes::JsonReturning>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_returning(x, y),
        _ => false,
    })
}

/// `_equalJsonBehavior`.
fn equal_json_behavior(
    a: &types_nodes::primnodes::JsonBehavior,
    b: &types_nodes::primnodes::JsonBehavior,
) -> bool {
    a.btype == b.btype
        && equal_opt_expr(a.expr.as_deref(), b.expr.as_deref())
        && a.coerce == b.coerce
}

#[inline]
fn equal_opt_json_behavior(
    a: Option<&types_nodes::primnodes::JsonBehavior>,
    b: Option<&types_nodes::primnodes::JsonBehavior>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_behavior(x, y),
        _ => false,
    })
}

fn equal_json_value_expr(a: &JsonValueExpr, b: &JsonValueExpr) -> bool {
    equal_opt_expr(a.raw_expr.as_deref(), b.raw_expr.as_deref())
        && equal_opt_expr(a.formatted_expr.as_deref(), b.formatted_expr.as_deref())
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
}

/// `_equalJsonValueExpr` over the RAW-grammar `rawexprnodes::JsonValueExpr`
/// (its `raw_expr`/`formatted_expr` are `Node *` children, not `Expr *`).
fn equal_json_value_expr_raw(
    a: &types_nodes::rawexprnodes::JsonValueExpr<'_>,
    b: &types_nodes::rawexprnodes::JsonValueExpr<'_>,
) -> bool {
    equal_opt_node(a.raw_expr.as_ref(), b.raw_expr.as_ref())
        && equal_opt_node(a.formatted_expr.as_ref(), b.formatted_expr.as_ref())
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
}

fn equal_json_constructor_expr(a: &JsonConstructorExpr, b: &JsonConstructorExpr) -> bool {
    a.r#type == b.r#type
        && equal_expr_list_impl(&a.args, &b.args)
        && equal_opt_expr(a.func.as_deref(), b.func.as_deref())
        && equal_opt_expr(a.coercion.as_deref(), b.coercion.as_deref())
        && equal_opt_json_returning(a.returning.as_ref(), b.returning.as_ref())
        && a.absent_on_null == b.absent_on_null
        && a.unique == b.unique
}

fn equal_json_is_predicate(a: &JsonIsPredicate, b: &JsonIsPredicate) -> bool {
    equal_opt_expr(a.expr.as_deref(), b.expr.as_deref())
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
        && a.item_type == b.item_type
        && a.unique_keys == b.unique_keys
}

fn equal_json_expr(a: &JsonExpr, b: &JsonExpr) -> bool {
    a.op == b.op
        && a.column_name == b.column_name
        && equal_opt_expr(a.formatted_expr.as_deref(), b.formatted_expr.as_deref())
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
        && equal_opt_expr(a.path_spec.as_deref(), b.path_spec.as_deref())
        && equal_opt_json_returning(a.returning.as_ref(), b.returning.as_ref())
        && a.passing_names == b.passing_names // List of String
        && equal_expr_list_impl(&a.passing_values, &b.passing_values)
        && equal_opt_json_behavior(a.on_empty.as_deref(), b.on_empty.as_deref())
        && equal_opt_json_behavior(a.on_error.as_deref(), b.on_error.as_deref())
        && a.use_io_coercion == b.use_io_coercion
        && a.use_json_coercion == b.use_json_coercion
        && a.wrapper == b.wrapper
        && a.omit_quotes == b.omit_quotes
        && a.collation == b.collation
}

fn equal_null_test(a: &NullTest, b: &NullTest) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.nulltesttype == b.nulltesttype
        && a.argisrow == b.argisrow
}

fn equal_boolean_test(a: &BooleanTest, b: &BooleanTest) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref()) && a.booltesttype == b.booltesttype
}

fn equal_coerce_to_domain(a: &CoerceToDomain, b: &CoerceToDomain) -> bool {
    equal_opt_expr(a.arg.as_deref(), b.arg.as_deref())
        && a.resulttype == b.resulttype
        && a.resulttypmod == b.resulttypmod
        && a.resultcollid == b.resultcollid
    // coercionformat is COMPARE_COERCIONFORM_FIELD (no-op).
}

fn equal_coerce_to_domain_value(a: &CoerceToDomainValue, b: &CoerceToDomainValue) -> bool {
    a.typeId == b.typeId && a.typeMod == b.typeMod && a.collation == b.collation
}

fn equal_set_to_default(a: &SetToDefault, b: &SetToDefault) -> bool {
    a.typeId == b.typeId && a.typeMod == b.typeMod && a.collation == b.collation
}

fn equal_current_of_expr(a: &CurrentOfExpr, b: &CurrentOfExpr) -> bool {
    a.cvarno == b.cvarno && a.cursor_name == b.cursor_name && a.cursor_param == b.cursor_param
}

fn equal_next_value_expr(a: &NextValueExpr, b: &NextValueExpr) -> bool {
    a.seqid == b.seqid && a.typeId == b.typeId
}

fn equal_inference_elem(a: &InferenceElem, b: &InferenceElem) -> bool {
    equal_opt_expr(a.expr.as_deref(), b.expr.as_deref())
        && a.infercollid == b.infercollid
        && a.inferopclass == b.inferopclass
}

fn equal_returning_expr(a: &ReturningExpr, b: &ReturningExpr) -> bool {
    a.retlevelsup == b.retlevelsup
        && a.retold == b.retold
        && equal_opt_expr(a.retexpr.as_deref(), b.retexpr.as_deref())
}

/// `_equalTargetEntry` (equalfuncs.funcs.c). `resorigtbl`/`resorigcol`/`resname`
/// ARE compared by the generated comparator.
fn equal_target_entry(a: &TargetEntry<'_>, b: &TargetEntry<'_>) -> bool {
    equal_opt_expr(a.expr.as_deref(), b.expr.as_deref())
        && a.resno == b.resno
        && equalstr(a.resname.as_deref(), b.resname.as_deref())
        && a.ressortgroupref == b.ressortgroupref
        && a.resorigtbl == b.resorigtbl
        && a.resorigcol == b.resorigcol
        && a.resjunk == b.resjunk
}

/// `_equalSortGroupClause` (equalfuncs.funcs.c).
fn equal_sort_group_clause(a: &SortGroupClause, b: &SortGroupClause) -> bool {
    a.tleSortGroupRef == b.tleSortGroupRef
        && a.eqop == b.eqop
        && a.sortop == b.sortop
        && a.reverse_sort == b.reverse_sort
        && a.nulls_first == b.nulls_first
        && a.hashable == b.hashable
}

// ===========================================================================
// Query-tree comparators (_equalRangeTblEntry, _equalQuery, ...)
//
// These mirror the `gen_node_support.pl`-generated comparators in
// `equalfuncs.funcs.c` for the parse/analyze/rewrite node universe. They are
// reachable through `equal()` via `SubLink.subselect` (an analyzed SubLink
// embeds an owned `Query`) and via the `Node`-list fields of `Query`/its
// sub-nodes (compared element-wise by [`equal_node`]).
// ===========================================================================

use types_nodes::nodes::NodePtr;

/// `COMPARE_NODE_FIELD` over a child `Node *` carried as a [`NodePtr`]
/// (`PgBox<Node>`): both NULL is equal; one NULL is unequal; else recurse into
/// [`equal_node`].
#[inline]
fn equal_opt_node(a: Option<&NodePtr<'_>>, b: Option<&NodePtr<'_>>) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_node(x, y),
        _ => false,
    })
}

/// `COMPARE_NODE_FIELD` over a `List *` carried as a `PgVec<NodePtr>`
/// (`_equalList`): equal length, then element-wise [`equal_node`].
#[inline]
fn equal_node_list(a: &[NodePtr<'_>], b: &[NodePtr<'_>]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| equal_node(x, y))
}

/// `COMPARE_BITMAPSET_FIELD` — `bms_equal(a, b)`: both NULL is equal; one NULL
/// is unequal; else compare the (canonicalized) word storage. Mirrors
/// `backend-nodes-core::bitmapset::bms_equal`.
#[inline]
fn equal_bms(
    a: Option<&types_nodes::bitmapset::Bitmapset<'_>>,
    b: Option<&types_nodes::bitmapset::Bitmapset<'_>>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.words == y.words,
        _ => false,
    })
}

/// `_equalAlias` (equalfuncs.funcs.c).
fn equal_alias(a: &types_nodes::rawnodes::Alias<'_>, b: &types_nodes::rawnodes::Alias<'_>) -> bool {
    equalstr(a.aliasname.as_deref(), b.aliasname.as_deref())
        && equal_node_list(&a.colnames, &b.colnames)
}

/// `COMPARE_NODE_FIELD` over an optional `Alias *`.
#[inline]
fn equal_opt_alias(
    a: Option<&types_nodes::rawnodes::Alias<'_>>,
    b: Option<&types_nodes::rawnodes::Alias<'_>>,
) -> bool {
    (match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_alias(x, y),
        _ => false,
    })
}

/// `_equalRangeTblEntry` (equalfuncs.funcs.c). Note: the C `List *` fields
/// `joinleftcols`/`joinrightcols`/`coltypes`/`coltypmods`/`colcollations` carry
/// Integer/Oid value nodes; this repo holds them as scalar `PgVec`s, so the
/// node-list compare reduces to slice equality (same semantics).
fn equal_range_tbl_entry(
    a: &types_nodes::parsenodes::RangeTblEntry<'_>,
    b: &types_nodes::parsenodes::RangeTblEntry<'_>,
) -> bool {
    equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
        && equal_opt_alias(a.eref.as_deref(), b.eref.as_deref())
        && a.rtekind == b.rtekind
        && a.relid == b.relid
        && a.inh == b.inh
        && a.relkind == b.relkind
        && a.rellockmode == b.rellockmode
        && a.perminfoindex == b.perminfoindex
        && equal_opt_node(a.tablesample.as_ref(), b.tablesample.as_ref())
        && equal_opt_subselect(a.subquery.as_deref(), b.subquery.as_deref())
        && a.security_barrier == b.security_barrier
        && a.jointype == b.jointype
        && a.joinmergedcols == b.joinmergedcols
        && equal_node_list(&a.joinaliasvars, &b.joinaliasvars)
        && a.joinleftcols == b.joinleftcols
        && a.joinrightcols == b.joinrightcols
        && equal_opt_alias(a.join_using_alias.as_deref(), b.join_using_alias.as_deref())
        && equal_node_list(&a.functions, &b.functions)
        && a.funcordinality == b.funcordinality
        && equal_opt_node(a.tablefunc.as_ref(), b.tablefunc.as_ref())
        && equal_node_list(&a.values_lists, &b.values_lists)
        && equalstr(a.ctename.as_deref(), b.ctename.as_deref())
        && a.ctelevelsup == b.ctelevelsup
        && a.self_reference == b.self_reference
        && a.coltypes == b.coltypes
        && a.coltypmods == b.coltypmods
        && a.colcollations == b.colcollations
        && equalstr(a.enrname.as_deref(), b.enrname.as_deref())
        && a.enrtuples == b.enrtuples
        && equal_node_list(&a.groupexprs, &b.groupexprs)
        && a.lateral == b.lateral
        && a.inFromCl == b.inFromCl
        && equal_node_list(&a.securityQuals, &b.securityQuals)
}

/// `_equalRTEPermissionInfo` (equalfuncs.funcs.c).
fn equal_rte_permission_info(
    a: &types_nodes::parsenodes::RTEPermissionInfo<'_>,
    b: &types_nodes::parsenodes::RTEPermissionInfo<'_>,
) -> bool {
    a.relid == b.relid
        && a.inh == b.inh
        && a.requiredPerms == b.requiredPerms
        && a.checkAsUser == b.checkAsUser
        && equal_bms(a.selectedCols.as_deref(), b.selectedCols.as_deref())
        && equal_bms(a.insertedCols.as_deref(), b.insertedCols.as_deref())
        && equal_bms(a.updatedCols.as_deref(), b.updatedCols.as_deref())
}

/// `_equalRangeTblFunction` (equalfuncs.funcs.c).
fn equal_range_tbl_function(
    a: &types_nodes::rawnodes::RangeTblFunction<'_>,
    b: &types_nodes::rawnodes::RangeTblFunction<'_>,
) -> bool {
    equal_opt_node(a.funcexpr.as_ref(), b.funcexpr.as_ref())
        && a.funccolcount == b.funccolcount
        && equal_node_list(&a.funccolnames, &b.funccolnames)
        && a.funccoltypes == b.funccoltypes
        && a.funccoltypmods == b.funccoltypmods
        && a.funccolcollations == b.funccolcollations
        && equal_bms(a.funcparams.as_deref(), b.funcparams.as_deref())
}

/// `_equalFromExpr` (equalfuncs.funcs.c).
fn equal_from_expr(
    a: &types_nodes::rawnodes::FromExpr<'_>,
    b: &types_nodes::rawnodes::FromExpr<'_>,
) -> bool {
    equal_node_list(&a.fromlist, &b.fromlist) && equal_opt_node(a.quals.as_ref(), b.quals.as_ref())
}

/// `_equalJoinExpr` (equalfuncs.funcs.c).
fn equal_join_expr(
    a: &types_nodes::rawnodes::JoinExpr<'_>,
    b: &types_nodes::rawnodes::JoinExpr<'_>,
) -> bool {
    a.jointype == b.jointype
        && a.isNatural == b.isNatural
        && equal_opt_node(a.larg.as_ref(), b.larg.as_ref())
        && equal_opt_node(a.rarg.as_ref(), b.rarg.as_ref())
        && equal_node_list(&a.usingClause, &b.usingClause)
        && equal_opt_alias(a.join_using_alias.as_deref(), b.join_using_alias.as_deref())
        && equal_opt_node(a.quals.as_ref(), b.quals.as_ref())
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
        && a.rtindex == b.rtindex
}

/// `_equalRangeTblRef` (equalfuncs.funcs.c).
fn equal_range_tbl_ref(
    a: &types_nodes::rawnodes::RangeTblRef,
    b: &types_nodes::rawnodes::RangeTblRef,
) -> bool {
    a.rtindex == b.rtindex
}

/// `_equalOnConflictExpr` (equalfuncs.funcs.c).
fn equal_on_conflict_expr(
    a: &types_nodes::rawnodes::OnConflictExpr<'_>,
    b: &types_nodes::rawnodes::OnConflictExpr<'_>,
) -> bool {
    a.action == b.action
        && equal_node_list(&a.arbiterElems, &b.arbiterElems)
        && equal_opt_node(a.arbiterWhere.as_ref(), b.arbiterWhere.as_ref())
        && a.constraint == b.constraint
        && equal_node_list(&a.onConflictSet, &b.onConflictSet)
        && equal_opt_node(a.onConflictWhere.as_ref(), b.onConflictWhere.as_ref())
        && a.exclRelIndex == b.exclRelIndex
        && equal_node_list(&a.exclRelTlist, &b.exclRelTlist)
}

/// `_equalMergeAction` (equalfuncs.funcs.c) — the parse-tree `MergeAction`.
fn equal_merge_action(
    a: &types_nodes::rawnodes::MergeAction<'_>,
    b: &types_nodes::rawnodes::MergeAction<'_>,
) -> bool {
    a.matchKind == b.matchKind
        && a.commandType == b.commandType
        && a.r#override == b.r#override
        && equal_opt_node(a.qual.as_ref(), b.qual.as_ref())
        && equal_node_list(&a.targetList, &b.targetList)
        && a.updateColnos == b.updateColnos
}

/// `_equalWithCheckOption` (equalfuncs.funcs.c).
fn equal_with_check_option(
    a: &types_nodes::rawnodes::WithCheckOption<'_>,
    b: &types_nodes::rawnodes::WithCheckOption<'_>,
) -> bool {
    a.kind == b.kind
        && equalstr(a.relname.as_deref(), b.relname.as_deref())
        && equalstr(a.polname.as_deref(), b.polname.as_deref())
        && equal_opt_node(a.qual.as_ref(), b.qual.as_ref())
        && a.cascaded == b.cascaded
}

/// `_equalGroupingSet` (equalfuncs.funcs.c).
fn equal_grouping_set(
    a: &types_nodes::rawnodes::GroupingSet<'_>,
    b: &types_nodes::rawnodes::GroupingSet<'_>,
) -> bool {
    a.kind == b.kind && equal_node_list(&a.content, &b.content)
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalWindowClause` (equalfuncs.funcs.c).
fn equal_window_clause(
    a: &types_nodes::rawnodes::WindowClause<'_>,
    b: &types_nodes::rawnodes::WindowClause<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equalstr(a.refname.as_deref(), b.refname.as_deref())
        && equal_node_list(&a.partitionClause, &b.partitionClause)
        && equal_node_list(&a.orderClause, &b.orderClause)
        && a.frameOptions == b.frameOptions
        && equal_opt_node(a.startOffset.as_ref(), b.startOffset.as_ref())
        && equal_opt_node(a.endOffset.as_ref(), b.endOffset.as_ref())
        && a.startInRangeFunc == b.startInRangeFunc
        && a.endInRangeFunc == b.endInRangeFunc
        && a.inRangeColl == b.inRangeColl
        && a.inRangeAsc == b.inRangeAsc
        && a.inRangeNullsFirst == b.inRangeNullsFirst
        && a.winref == b.winref
        && a.copiedOrder == b.copiedOrder
}

/// `_equalRowMarkClause` (equalfuncs.funcs.c).
fn equal_row_mark_clause(
    a: &types_nodes::rawnodes::RowMarkClause,
    b: &types_nodes::rawnodes::RowMarkClause,
) -> bool {
    a.rti == b.rti
        && a.strength == b.strength
        && a.waitPolicy == b.waitPolicy
        && a.pushedDown == b.pushedDown
}

/// `_equalCTESearchClause` (equalfuncs.funcs.c).
fn equal_cte_search_clause(
    a: &types_nodes::rawnodes::CTESearchClause<'_>,
    b: &types_nodes::rawnodes::CTESearchClause<'_>,
) -> bool {
    equal_node_list(&a.search_col_list, &b.search_col_list)
        && a.search_breadth_first == b.search_breadth_first
        && equalstr(a.search_seq_column.as_deref(), b.search_seq_column.as_deref())
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalCTECycleClause` (equalfuncs.funcs.c).
fn equal_cte_cycle_clause(
    a: &types_nodes::rawnodes::CTECycleClause<'_>,
    b: &types_nodes::rawnodes::CTECycleClause<'_>,
) -> bool {
    equal_node_list(&a.cycle_col_list, &b.cycle_col_list)
        && equalstr(a.cycle_mark_column.as_deref(), b.cycle_mark_column.as_deref())
        && equal_opt_node(a.cycle_mark_value.as_ref(), b.cycle_mark_value.as_ref())
        && equal_opt_node(a.cycle_mark_default.as_ref(), b.cycle_mark_default.as_ref())
        && equalstr(a.cycle_path_column.as_deref(), b.cycle_path_column.as_deref())
        // location is COMPARE_LOCATION_FIELD (no-op).
        && a.cycle_mark_type == b.cycle_mark_type
        && a.cycle_mark_typmod == b.cycle_mark_typmod
        && a.cycle_mark_collation == b.cycle_mark_collation
        && a.cycle_mark_neop == b.cycle_mark_neop
}

/// `COMPARE_NODE_FIELD` over an optional `List *` of `Expr *` (no NULL elements).
#[inline]
fn equal_opt_list<T>(
    a: &Option<impl AsRef<[T]>>,
    b: &Option<impl AsRef<[T]>>,
    eq: impl Fn(&T, &T) -> bool,
) -> bool {
    (match (a.as_ref(), b.as_ref()) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            let (x, y) = (x.as_ref(), y.as_ref());
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| eq(p, q))
        }
        _ => false,
    })
}

/// `_equalTableFunc` (equalfuncs.funcs.c, gen_node_support). Compares every
/// `COMPARE_*` field; `location` is `COMPARE_LOCATION_FIELD` (no-op).
fn equal_table_func(
    a: &types_nodes::primnodes::TableFunc<'_>,
    b: &types_nodes::primnodes::TableFunc<'_>,
) -> bool {
    a.functype == b.functype
        && equal_opt_list(&a.ns_uris, &b.ns_uris, |p, q| equal_expr(p, q))
        && equal_opt_list(&a.ns_names, &b.ns_names, |p, q| {
            match (p.as_ref(), q.as_ref()) {
                (None, None) => true,
                (Some(s), Some(t)) => s.as_str() == t.as_str(),
                _ => false,
            }
        })
        && equal_opt_expr(a.docexpr.as_deref(), b.docexpr.as_deref())
        && equal_opt_expr(a.rowexpr.as_deref(), b.rowexpr.as_deref())
        && equal_opt_list(&a.colnames, &b.colnames, |p, q| p.as_str() == q.as_str())
        && equal_opt_list(&a.coltypes, &b.coltypes, |p, q| p == q)
        && equal_opt_list(&a.coltypmods, &b.coltypmods, |p, q| p == q)
        && equal_opt_list(&a.colcollations, &b.colcollations, |p, q| p == q)
        && equal_opt_list(&a.colexprs, &b.colexprs, |p, q| {
            equal_opt_expr(p.as_deref(), q.as_deref())
        })
        && equal_opt_list(&a.coldefexprs, &b.coldefexprs, |p, q| {
            equal_opt_expr(p.as_deref(), q.as_deref())
        })
        && equal_opt_list(&a.colvalexprs, &b.colvalexprs, |p, q| {
            equal_opt_expr(p.as_deref(), q.as_deref())
        })
        && equal_opt_list(&a.passingvalexprs, &b.passingvalexprs, |p, q| equal_expr(p, q))
        && equal_bms(a.notnulls.as_deref(), b.notnulls.as_deref())
        && equal_opt_node(a.plan.as_ref(), b.plan.as_ref())
        && a.ordinalitycol == b.ordinalitycol
    // `location` is `COMPARE_LOCATION_FIELD` (no-op).
}

/// `_equalCommonTableExpr` (equalfuncs.funcs.c).
fn equal_common_table_expr(
    a: &types_nodes::rawnodes::CommonTableExpr<'_>,
    b: &types_nodes::rawnodes::CommonTableExpr<'_>,
) -> bool {
    equalstr(a.ctename.as_deref(), b.ctename.as_deref())
        && equal_node_list(&a.aliascolnames, &b.aliascolnames)
        && a.ctematerialized == b.ctematerialized
        && equal_opt_node(a.ctequery.as_ref(), b.ctequery.as_ref())
        && match (a.search_clause.as_deref(), b.search_clause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_cte_search_clause(x, y),
            _ => false,
        }
        && equal_opt_node(a.cycle_clause.as_ref(), b.cycle_clause.as_ref())
        // location is COMPARE_LOCATION_FIELD (no-op).
        && a.cterecursive == b.cterecursive
        && a.cterefcount == b.cterefcount
        && equal_node_list(&a.ctecolnames, &b.ctecolnames)
        && a.ctecoltypes == b.ctecoltypes
        && a.ctecoltypmods == b.ctecoltypmods
        && a.ctecolcollations == b.ctecolcollations
}

/// `_equalSetOperationStmt` (equalfuncs.funcs.c).
fn equal_set_operation_stmt(
    a: &types_nodes::rawnodes::SetOperationStmt<'_>,
    b: &types_nodes::rawnodes::SetOperationStmt<'_>,
) -> bool {
    a.op == b.op
        && a.all == b.all
        && equal_opt_node(a.larg.as_ref(), b.larg.as_ref())
        && equal_opt_node(a.rarg.as_ref(), b.rarg.as_ref())
        && a.colTypes == b.colTypes
        && a.colTypmods == b.colTypmods
        && a.colCollations == b.colCollations
        && equal_node_list(&a.groupClauses, &b.groupClauses)
}

/// `_equalQuery` (equalfuncs.funcs.c) — the full analyzed-`Query` comparator.
/// `queryId`/`querySource` location-ish fields excluded? No: `_equalQuery`
/// compares neither `queryId`/`hasGroupRTE`-class derived flags beyond what the
/// generated comparator lists — it follows the field set verbatim below.
/// `COMPARE_LOCATION_FIELD(stmt_location)`/`(stmt_len)` are no-ops.
fn equal_query(
    a: &types_nodes::copy_query::Query<'_>,
    b: &types_nodes::copy_query::Query<'_>,
) -> bool {
    a.commandType == b.commandType
        && a.querySource == b.querySource
        && a.canSetTag == b.canSetTag
        && equal_opt_node(a.utilityStmt.as_ref(), b.utilityStmt.as_ref())
        && a.resultRelation == b.resultRelation
        && a.hasAggs == b.hasAggs
        && a.hasWindowFuncs == b.hasWindowFuncs
        && a.hasTargetSRFs == b.hasTargetSRFs
        && a.hasSubLinks == b.hasSubLinks
        && a.hasDistinctOn == b.hasDistinctOn
        && a.hasRecursive == b.hasRecursive
        && a.hasModifyingCTE == b.hasModifyingCTE
        && a.hasForUpdate == b.hasForUpdate
        && a.hasRowSecurity == b.hasRowSecurity
        && a.hasGroupRTE == b.hasGroupRTE
        && a.isReturn == b.isReturn
        && equal_node_list(&a.cteList, &b.cteList)
        && a.rtable.len() == b.rtable.len()
        && a.rtable
            .iter()
            .zip(b.rtable.iter())
            .all(|(x, y)| equal_range_tbl_entry(x, y))
        && a.rteperminfos.len() == b.rteperminfos.len()
        && a.rteperminfos
            .iter()
            .zip(b.rteperminfos.iter())
            .all(|(x, y)| equal_rte_permission_info(x, y))
        && match (a.jointree.as_deref(), b.jointree.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_from_expr(x, y),
            _ => false,
        }
        && equal_node_list(&a.mergeActionList, &b.mergeActionList)
        && a.mergeTargetRelation == b.mergeTargetRelation
        && equal_opt_expr(a.mergeJoinCondition.as_deref(), b.mergeJoinCondition.as_deref())
        && equal_targetentry_list_impl(&a.targetList, &b.targetList)
        && a.r#override == b.r#override
        && match (a.onConflict.as_deref(), b.onConflict.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_on_conflict_expr(x, y),
            _ => false,
        }
        && equalstr(a.returningOldAlias.as_deref(), b.returningOldAlias.as_deref())
        && equalstr(a.returningNewAlias.as_deref(), b.returningNewAlias.as_deref())
        && equal_targetentry_list_impl(&a.returningList, &b.returningList)
        && equal_node_list(&a.groupClause, &b.groupClause)
        && a.groupDistinct == b.groupDistinct
        && equal_node_list(&a.groupingSets, &b.groupingSets)
        && equal_opt_expr(a.havingQual.as_deref(), b.havingQual.as_deref())
        && equal_node_list(&a.windowClause, &b.windowClause)
        && equal_node_list(&a.distinctClause, &b.distinctClause)
        && equal_node_list(&a.sortClause, &b.sortClause)
        && equal_opt_expr(a.limitOffset.as_deref(), b.limitOffset.as_deref())
        && equal_opt_expr(a.limitCount.as_deref(), b.limitCount.as_deref())
        && a.limitOption == b.limitOption
        && equal_node_list(&a.rowMarks, &b.rowMarks)
        && equal_opt_node(a.setOperations.as_ref(), b.setOperations.as_ref())
        && a.constraintDeps == b.constraintDeps
        && equal_node_list(&a.withCheckOptions, &b.withCheckOptions)
    // stmt_location / stmt_len are COMPARE_LOCATION_FIELD (no-ops).
}

/// `_equalSubPlan` (equalfuncs.funcs.c). All scalar/string/node fields are
/// compared (subLinkType, testexpr, paramIds, plan_id, plan_name, firstCol*,
/// useHashTable, unknownEqFalse, parallel_safe, setParam, parParam, args,
/// startup_cost, per_call_cost). `paramIds`/`setParam`/`parParam` are `List *`
/// of Integer value nodes in C, carried here as scalar `PgVec<i32>` (slice
/// equality has the same semantics).
fn equal_sub_plan(a: &SubPlan<'_>, b: &SubPlan<'_>) -> bool {
    a.subLinkType == b.subLinkType
        && equal_opt_expr(a.testexpr.as_deref(), b.testexpr.as_deref())
        && &a.paramIds[..] == &b.paramIds[..]
        && a.plan_id == b.plan_id
        && equalstr(a.plan_name.as_deref(), b.plan_name.as_deref())
        && a.firstColType == b.firstColType
        && a.firstColTypmod == b.firstColTypmod
        && a.firstColCollation == b.firstColCollation
        && a.useHashTable == b.useHashTable
        && a.unknownEqFalse == b.unknownEqFalse
        && a.parallel_safe == b.parallel_safe
        && &a.setParam[..] == &b.setParam[..]
        && &a.parParam[..] == &b.parParam[..]
        && a.args.len() == b.args.len()
        && a.args
            .iter()
            .zip(b.args.iter())
            .all(|(x, y)| equal_expr(x, y))
        && a.startup_cost == b.startup_cost
        && a.per_call_cost == b.per_call_cost
}

/// `_equalAlternativeSubPlan` (equalfuncs.funcs.c): the single `List *subplans`
/// field of `SubPlan *` children, compared element-wise by `_equalSubPlan`.
fn equal_alternative_sub_plan(a: &AlternativeSubPlan<'_>, b: &AlternativeSubPlan<'_>) -> bool {
    a.subplans.len() == b.subplans.len()
        && a.subplans
            .iter()
            .zip(b.subplans.iter())
            .all(|(x, y)| equal_sub_plan(x, y))
}

/// `_equalPlaceHolderVar` (equalfuncs.funcs.c). Per the node definition,
/// `phexpr` and `phrels` are NOT compared (gen marks them `equal_ignore`); only
/// `phnullingrels` (COMPARE_BITMAPSET_FIELD), `phid` and `phlevelsup`.
fn equal_place_holder_var(a: &PlaceHolderVar, b: &PlaceHolderVar) -> bool {
    a.phnullingrels == b.phnullingrels // COMPARE_BITMAPSET_FIELD
        && a.phid == b.phid
        && a.phlevelsup == b.phlevelsup
}

// ===========================================================================
// equal() — the central tag-discriminated dispatch
// ===========================================================================

/// `equal(a, b)` over two `Expr *`: the `equalfuncs.c` switch restricted to the
/// `Expr`-derived node universe. Two different variants (`nodeTag` mismatch) are
/// never equal; same-variant nodes are compared by their `_equalXxx`.
pub fn equal_expr(a: &Expr, b: &Expr) -> bool {
    (match (a, b) {
        (Expr::Var(x), Expr::Var(y)) => equal_var(x, y),
        (Expr::Const(x), Expr::Const(y)) => equal_const(x, y),
        (Expr::Param(x), Expr::Param(y)) => equal_param(x, y),
        (Expr::Aggref(x), Expr::Aggref(y)) => equal_aggref(x, y),
        (Expr::GroupingFunc(x), Expr::GroupingFunc(y)) => equal_grouping_func(x, y),
        (Expr::WindowFunc(x), Expr::WindowFunc(y)) => equal_window_func(x, y),
        (Expr::MergeSupportFunc(x), Expr::MergeSupportFunc(y)) => equal_merge_support_func(x, y),
        (Expr::SubscriptingRef(x), Expr::SubscriptingRef(y)) => equal_subscripting_ref(x, y),
        (Expr::FuncExpr(x), Expr::FuncExpr(y)) => equal_func_expr(x, y),
        (Expr::NamedArgExpr(x), Expr::NamedArgExpr(y)) => equal_named_arg_expr(x, y),
        (Expr::OpExpr(x), Expr::OpExpr(y)) => equal_op_expr(x, y),
        (Expr::DistinctExpr(x), Expr::DistinctExpr(y)) => equal_op_expr(x, y),
        (Expr::NullIfExpr(x), Expr::NullIfExpr(y)) => equal_op_expr(x, y),
        (Expr::ScalarArrayOpExpr(x), Expr::ScalarArrayOpExpr(y)) => equal_scalar_array_op_expr(x, y),
        (Expr::BoolExpr(x), Expr::BoolExpr(y)) => equal_bool_expr(x, y),
        (Expr::SubLink(x), Expr::SubLink(y)) => equal_sub_link(x, y),
        (Expr::FieldSelect(x), Expr::FieldSelect(y)) => equal_field_select(x, y),
        (Expr::FieldStore(x), Expr::FieldStore(y)) => equal_field_store(x, y),
        (Expr::RelabelType(x), Expr::RelabelType(y)) => equal_relabel_type(x, y),
        (Expr::CoerceViaIO(x), Expr::CoerceViaIO(y)) => equal_coerce_via_io(x, y),
        (Expr::ArrayCoerceExpr(x), Expr::ArrayCoerceExpr(y)) => equal_array_coerce_expr(x, y),
        (Expr::ConvertRowtypeExpr(x), Expr::ConvertRowtypeExpr(y)) => {
            equal_convert_rowtype_expr(x, y)
        }
        (Expr::CollateExpr(x), Expr::CollateExpr(y)) => equal_collate_expr(x, y),
        (Expr::CaseExpr(x), Expr::CaseExpr(y)) => equal_case_expr(x, y),
        (Expr::CaseTestExpr(x), Expr::CaseTestExpr(y)) => equal_case_test_expr(x, y),
        (Expr::ArrayExpr(x), Expr::ArrayExpr(y)) => equal_array_expr(x, y),
        (Expr::RowExpr(x), Expr::RowExpr(y)) => equal_row_expr(x, y),
        (Expr::RowCompareExpr(x), Expr::RowCompareExpr(y)) => equal_row_compare_expr(x, y),
        (Expr::CoalesceExpr(x), Expr::CoalesceExpr(y)) => equal_coalesce_expr(x, y),
        (Expr::MinMaxExpr(x), Expr::MinMaxExpr(y)) => equal_min_max_expr(x, y),
        (Expr::SQLValueFunction(x), Expr::SQLValueFunction(y)) => equal_sqlvalue_function(x, y),
        (Expr::XmlExpr(x), Expr::XmlExpr(y)) => equal_xml_expr(x, y),
        (Expr::JsonValueExpr(x), Expr::JsonValueExpr(y)) => equal_json_value_expr(x, y),
        (Expr::JsonConstructorExpr(x), Expr::JsonConstructorExpr(y)) => {
            equal_json_constructor_expr(x, y)
        }
        (Expr::JsonIsPredicate(x), Expr::JsonIsPredicate(y)) => equal_json_is_predicate(x, y),
        (Expr::JsonExpr(x), Expr::JsonExpr(y)) => equal_json_expr(x, y),
        (Expr::NullTest(x), Expr::NullTest(y)) => equal_null_test(x, y),
        (Expr::BooleanTest(x), Expr::BooleanTest(y)) => equal_boolean_test(x, y),
        (Expr::CoerceToDomain(x), Expr::CoerceToDomain(y)) => equal_coerce_to_domain(x, y),
        (Expr::CoerceToDomainValue(x), Expr::CoerceToDomainValue(y)) => {
            equal_coerce_to_domain_value(x, y)
        }
        (Expr::SetToDefault(x), Expr::SetToDefault(y)) => equal_set_to_default(x, y),
        (Expr::CurrentOfExpr(x), Expr::CurrentOfExpr(y)) => equal_current_of_expr(x, y),
        (Expr::NextValueExpr(x), Expr::NextValueExpr(y)) => equal_next_value_expr(x, y),
        (Expr::InferenceElem(x), Expr::InferenceElem(y)) => equal_inference_elem(x, y),
        (Expr::ReturningExpr(x), Expr::ReturningExpr(y)) => equal_returning_expr(x, y),
        // RestrictInfo carried as the planner-arena handle [`RinfoRef`]: compared
        // as the scalar arena id (C compares RestrictInfo by pointer at this
        // layer — the orclause BoolExpr embeds the same live RestrictInfo).
        (Expr::RestrictInfo(x), Expr::RestrictInfo(y)) => x == y,
        (Expr::SubPlan(x), Expr::SubPlan(y)) => equal_sub_plan(&x.0, &y.0),
        (Expr::AlternativeSubPlan(x), Expr::AlternativeSubPlan(y)) => {
            equal_alternative_sub_plan(&x.0, &y.0)
        }
        (Expr::PlaceHolderVar(x), Expr::PlaceHolderVar(y)) => equal_place_holder_var(x, y),
        // Different tags are never equal (the `nodeTag(a) != nodeTag(b)` early
        // return in equal()).
        _ => false,
    })
}

// ===========================================================================
// Raw-grammar parse nodes (equalfuncs.funcs.c). Reached by `equal()` over the
// untransformed parse tree — e.g. transformWindowFuncCall's de-duplication of
// inline window specifications compares the WindowDef's partition/order clauses
// (lists of SortBy carrying ColumnRef/A_Const/... expressions).
// ===========================================================================

/// `_equalColumnRef` (equalfuncs.funcs.c).
fn equal_column_ref(
    a: &types_nodes::rawnodes::ColumnRef<'_>,
    b: &types_nodes::rawnodes::ColumnRef<'_>,
) -> bool {
    equal_node_list(&a.fields, &b.fields)
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalParamRef` (equalfuncs.funcs.c).
fn equal_param_ref(
    a: &types_nodes::rawnodes::ParamRef,
    b: &types_nodes::rawnodes::ParamRef,
) -> bool {
    a.number == b.number
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalA_Expr` (equalfuncs.funcs.c).
fn equal_a_expr(
    a: &types_nodes::rawnodes::A_Expr<'_>,
    b: &types_nodes::rawnodes::A_Expr<'_>,
) -> bool {
    a.kind == b.kind
        && equal_node_list(&a.name, &b.name)
        && equal_opt_node(a.lexpr.as_ref(), b.lexpr.as_ref())
        && equal_opt_node(a.rexpr.as_ref(), b.rexpr.as_ref())
    // rexpr_list_start/rexpr_list_end/location are COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalA_Const` (equalfuncs.c). `val` is the in-line value node, valid only
/// when `!isnull`.
fn equal_a_const(
    a: &types_nodes::rawnodes::A_Const<'_>,
    b: &types_nodes::rawnodes::A_Const<'_>,
) -> bool {
    if a.isnull != b.isnull {
        return false;
    }
    if !a.isnull && !equal_opt_node(a.val.as_ref(), b.val.as_ref()) {
        return false;
    }
    true
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalFuncCall` (equalfuncs.funcs.c).
fn equal_func_call(
    a: &types_nodes::rawnodes::FuncCall<'_>,
    b: &types_nodes::rawnodes::FuncCall<'_>,
) -> bool {
    equal_node_list(&a.funcname, &b.funcname)
        && equal_node_list(&a.args, &b.args)
        && equal_node_list(&a.agg_order, &b.agg_order)
        && equal_opt_node(a.agg_filter.as_ref(), b.agg_filter.as_ref())
        && match (a.over.as_ref(), b.over.as_ref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_window_def(x, y),
            _ => false,
        }
        && a.agg_within_group == b.agg_within_group
        && a.agg_star == b.agg_star
        && a.agg_distinct == b.agg_distinct
        && a.func_variadic == b.func_variadic
    // funcformat is COMPARE_COERCIONFORM_FIELD (no-op); location no-op.
}

/// `_equalA_Star` (equalfuncs.funcs.c) — no fields.
fn equal_a_star(
    _a: &types_nodes::rawnodes::A_Star,
    _b: &types_nodes::rawnodes::A_Star,
) -> bool {
    true
}

/// `_equalA_Indices` (equalfuncs.funcs.c).
fn equal_a_indices(
    a: &types_nodes::rawnodes::A_Indices<'_>,
    b: &types_nodes::rawnodes::A_Indices<'_>,
) -> bool {
    a.is_slice == b.is_slice
        && equal_opt_node(a.lidx.as_ref(), b.lidx.as_ref())
        && equal_opt_node(a.uidx.as_ref(), b.uidx.as_ref())
}

/// `_equalA_Indirection` (equalfuncs.funcs.c).
fn equal_a_indirection(
    a: &types_nodes::rawnodes::A_Indirection<'_>,
    b: &types_nodes::rawnodes::A_Indirection<'_>,
) -> bool {
    equal_opt_node(a.arg.as_ref(), b.arg.as_ref())
        && equal_node_list(&a.indirection, &b.indirection)
}

/// `_equalA_ArrayExpr` (equalfuncs.funcs.c).
fn equal_a_array_expr(
    a: &types_nodes::rawnodes::A_ArrayExpr<'_>,
    b: &types_nodes::rawnodes::A_ArrayExpr<'_>,
) -> bool {
    equal_node_list(&a.elements, &b.elements)
    // list_start/list_end/location are COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalTypeName` (equalfuncs.funcs.c).
fn equal_type_name(
    a: &types_nodes::rawnodes::TypeName<'_>,
    b: &types_nodes::rawnodes::TypeName<'_>,
) -> bool {
    equal_node_list(&a.names, &b.names)
        && a.typeOid == b.typeOid
        && a.setof == b.setof
        && a.pct_type == b.pct_type
        && equal_node_list(&a.typmods, &b.typmods)
        && a.typemod == b.typemod
        && equal_node_list(&a.arrayBounds, &b.arrayBounds)
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalTypeCast` (equalfuncs.funcs.c).
fn equal_type_cast(
    a: &types_nodes::rawnodes::TypeCast<'_>,
    b: &types_nodes::rawnodes::TypeCast<'_>,
) -> bool {
    equal_opt_node(a.arg.as_ref(), b.arg.as_ref())
        && match (a.typeName.as_ref(), b.typeName.as_ref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_type_name(x, y),
            _ => false,
        }
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalCollateClause` (equalfuncs.funcs.c).
fn equal_collate_clause(
    a: &types_nodes::rawnodes::CollateClause<'_>,
    b: &types_nodes::rawnodes::CollateClause<'_>,
) -> bool {
    equal_opt_node(a.arg.as_ref(), b.arg.as_ref())
        && equal_node_list(&a.collname, &b.collname)
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalResTarget` (equalfuncs.funcs.c).
fn equal_res_target(
    a: &types_nodes::rawnodes::ResTarget<'_>,
    b: &types_nodes::rawnodes::ResTarget<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.indirection, &b.indirection)
        && equal_opt_node(a.val.as_ref(), b.val.as_ref())
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalMultiAssignRef` (equalfuncs.funcs.c).
fn equal_multi_assign_ref(
    a: &types_nodes::rawnodes::MultiAssignRef<'_>,
    b: &types_nodes::rawnodes::MultiAssignRef<'_>,
) -> bool {
    equal_opt_node(a.source.as_ref(), b.source.as_ref())
        && a.colno == b.colno
        && a.ncolumns == b.ncolumns
}

/// `_equalIndexElem` (equalfuncs.funcs.c).
fn equal_index_elem(
    a: &types_nodes::ddlnodes::IndexElem<'_>,
    b: &types_nodes::ddlnodes::IndexElem<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_opt_node(a.expr.as_ref(), b.expr.as_ref())
        && equalstr(a.indexcolname.as_deref(), b.indexcolname.as_deref())
        && equal_node_list(&a.collation, &b.collation)
        && equal_node_list(&a.opclass, &b.opclass)
        && equal_node_list(&a.opclassopts, &b.opclassopts)
        && a.ordering == b.ordering
        && a.nulls_ordering == b.nulls_ordering
}

/// `_equalSortBy` (equalfuncs.funcs.c).
fn equal_sort_by(
    a: &types_nodes::rawnodes::SortBy<'_>,
    b: &types_nodes::rawnodes::SortBy<'_>,
) -> bool {
    equal_opt_node(a.node.as_ref(), b.node.as_ref())
        && a.sortby_dir == b.sortby_dir
        && a.sortby_nulls == b.sortby_nulls
        && equal_node_list(&a.useOp, &b.useOp)
    // location is COMPARE_LOCATION_FIELD (no-op).
}

/// `_equalWindowDef` (equalfuncs.funcs.c).
fn equal_window_def(
    a: &types_nodes::rawnodes::WindowDef<'_>,
    b: &types_nodes::rawnodes::WindowDef<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equalstr(a.refname.as_deref(), b.refname.as_deref())
        && equal_node_list(&a.partitionClause, &b.partitionClause)
        && equal_node_list(&a.orderClause, &b.orderClause)
        && a.frameOptions == b.frameOptions
        && equal_opt_node(a.startOffset.as_ref(), b.startOffset.as_ref())
        && equal_opt_node(a.endOffset.as_ref(), b.endOffset.as_ref())
    // location is COMPARE_LOCATION_FIELD (no-op).
}

// ===========================================================================
// Ported `_equalXxx` comparators for the DDL/utility/raw-parse/JSON node family
// (equalfuncs.funcs.c). Each is a faithful field-by-field translation; the
// `equal_node` switch below dispatches to them.
// ===========================================================================

/// `_equalAccessPriv` (equalfuncs.funcs.c).
fn equal_access_priv(a: &types_nodes::ddlnodes::AccessPriv<'_>, b: &types_nodes::ddlnodes::AccessPriv<'_>) -> bool {
    equalstr(a.priv_name.as_deref(), b.priv_name.as_deref())
        && equal_node_list(&a.cols, &b.cols)
}

/// `_equalAlterCollationStmt` (equalfuncs.funcs.c).
fn equal_alter_collation_stmt(a: &types_nodes::ddlnodes::AlterCollationStmt<'_>, b: &types_nodes::ddlnodes::AlterCollationStmt<'_>) -> bool {
    equal_node_list(&a.collname, &b.collname)
}

/// `_equalAlterDatabaseRefreshCollStmt` (equalfuncs.funcs.c).
fn equal_alter_database_refresh_coll_stmt(a: &types_nodes::ddlnodes::AlterDatabaseRefreshCollStmt<'_>, b: &types_nodes::ddlnodes::AlterDatabaseRefreshCollStmt<'_>) -> bool {
    equalstr(a.dbname.as_deref(), b.dbname.as_deref())
}

/// `_equalAlterDatabaseSetStmt` (equalfuncs.funcs.c).
fn equal_alter_database_set_stmt(a: &types_nodes::ddlnodes::AlterDatabaseSetStmt<'_>, b: &types_nodes::ddlnodes::AlterDatabaseSetStmt<'_>) -> bool {
    equalstr(a.dbname.as_deref(), b.dbname.as_deref())
        && equal_opt_node(a.setstmt.as_ref(), b.setstmt.as_ref())
}

/// `_equalAlterDatabaseStmt` (equalfuncs.funcs.c).
fn equal_alter_database_stmt(a: &types_nodes::ddlnodes::AlterDatabaseStmt<'_>, b: &types_nodes::ddlnodes::AlterDatabaseStmt<'_>) -> bool {
    equalstr(a.dbname.as_deref(), b.dbname.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterDefaultPrivilegesStmt` (equalfuncs.funcs.c).
fn equal_alter_default_privileges_stmt(a: &types_nodes::ddlnodes::AlterDefaultPrivilegesStmt<'_>, b: &types_nodes::ddlnodes::AlterDefaultPrivilegesStmt<'_>) -> bool {
    equal_node_list(&a.options, &b.options)
        && equal_opt_node(a.action.as_ref(), b.action.as_ref())
}

/// `_equalAlterDomainStmt` (equalfuncs.funcs.c).
fn equal_alter_domain_stmt(a: &types_nodes::ddlnodes::AlterDomainStmt<'_>, b: &types_nodes::ddlnodes::AlterDomainStmt<'_>) -> bool {
    a.subtype == b.subtype
        && equal_node_list(&a.typeName, &b.typeName)
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_opt_node(a.def.as_ref(), b.def.as_ref())
        && a.behavior == b.behavior
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterEnumStmt` (equalfuncs.funcs.c).
fn equal_alter_enum_stmt(a: &types_nodes::ddlnodes::AlterEnumStmt<'_>, b: &types_nodes::ddlnodes::AlterEnumStmt<'_>) -> bool {
    equal_node_list(&a.typeName, &b.typeName)
        && equalstr(a.oldVal.as_deref(), b.oldVal.as_deref())
        && equalstr(a.newVal.as_deref(), b.newVal.as_deref())
        && equalstr(a.newValNeighbor.as_deref(), b.newValNeighbor.as_deref())
        && a.newValIsAfter == b.newValIsAfter
        && a.skipIfNewValExists == b.skipIfNewValExists
}

/// `_equalAlterEventTrigStmt` (equalfuncs.funcs.c).
fn equal_alter_event_trig_stmt(a: &types_nodes::ddlnodes::AlterEventTrigStmt<'_>, b: &types_nodes::ddlnodes::AlterEventTrigStmt<'_>) -> bool {
    equalstr(a.trigname.as_deref(), b.trigname.as_deref())
        && a.tgenabled == b.tgenabled
}

/// `_equalAlterExtensionContentsStmt` (equalfuncs.funcs.c).
fn equal_alter_extension_contents_stmt(a: &types_nodes::ddlnodes::AlterExtensionContentsStmt<'_>, b: &types_nodes::ddlnodes::AlterExtensionContentsStmt<'_>) -> bool {
    equalstr(a.extname.as_deref(), b.extname.as_deref())
        && a.action == b.action
        && a.objtype == b.objtype
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
}

/// `_equalAlterExtensionStmt` (equalfuncs.funcs.c).
fn equal_alter_extension_stmt(a: &types_nodes::ddlnodes::AlterExtensionStmt<'_>, b: &types_nodes::ddlnodes::AlterExtensionStmt<'_>) -> bool {
    equalstr(a.extname.as_deref(), b.extname.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterFdwStmt` (equalfuncs.funcs.c).
fn equal_alter_fdw_stmt(a: &types_nodes::ddlnodes::AlterFdwStmt<'_>, b: &types_nodes::ddlnodes::AlterFdwStmt<'_>) -> bool {
    equalstr(a.fdwname.as_deref(), b.fdwname.as_deref())
        && equal_node_list(&a.func_options, &b.func_options)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterForeignServerStmt` (equalfuncs.funcs.c).
fn equal_alter_foreign_server_stmt(a: &types_nodes::ddlnodes::AlterForeignServerStmt<'_>, b: &types_nodes::ddlnodes::AlterForeignServerStmt<'_>) -> bool {
    equalstr(a.servername.as_deref(), b.servername.as_deref())
        && equalstr(a.version.as_deref(), b.version.as_deref())
        && equal_node_list(&a.options, &b.options)
        && a.has_version == b.has_version
}

/// `_equalAlterFunctionStmt` (equalfuncs.funcs.c).
fn equal_alter_function_stmt(a: &types_nodes::ddlnodes::AlterFunctionStmt<'_>, b: &types_nodes::ddlnodes::AlterFunctionStmt<'_>) -> bool {
    a.objtype == b.objtype
        && equal_opt_node(a.func.as_ref(), b.func.as_ref())
        && equal_node_list(&a.actions, &b.actions)
}

/// `_equalAlterObjectDependsStmt` (equalfuncs.funcs.c).
fn equal_alter_object_depends_stmt(a: &types_nodes::ddlnodes::AlterObjectDependsStmt<'_>, b: &types_nodes::ddlnodes::AlterObjectDependsStmt<'_>) -> bool {
    a.objectType == b.objectType
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equal_opt_node(a.extname.as_ref(), b.extname.as_ref())
        && a.remove == b.remove
}

/// `_equalAlterObjectSchemaStmt` (equalfuncs.funcs.c).
fn equal_alter_object_schema_stmt(a: &types_nodes::ddlnodes::AlterObjectSchemaStmt<'_>, b: &types_nodes::ddlnodes::AlterObjectSchemaStmt<'_>) -> bool {
    a.objectType == b.objectType
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equalstr(a.newschema.as_deref(), b.newschema.as_deref())
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterOperatorStmt` (equalfuncs.funcs.c).
fn equal_alter_operator_stmt(a: &types_nodes::ddlnodes::AlterOperatorStmt<'_>, b: &types_nodes::ddlnodes::AlterOperatorStmt<'_>) -> bool {
    equal_opt_node(a.opername.as_ref(), b.opername.as_ref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterOpFamilyStmt` (equalfuncs.funcs.c).
fn equal_alter_op_family_stmt(a: &types_nodes::ddlnodes::AlterOpFamilyStmt<'_>, b: &types_nodes::ddlnodes::AlterOpFamilyStmt<'_>) -> bool {
    equal_node_list(&a.opfamilyname, &b.opfamilyname)
        && equalstr(a.amname.as_deref(), b.amname.as_deref())
        && a.isDrop == b.isDrop
        && equal_node_list(&a.items, &b.items)
}

/// `_equalAlterOwnerStmt` (equalfuncs.funcs.c).
fn equal_alter_owner_stmt(a: &types_nodes::ddlnodes::AlterOwnerStmt<'_>, b: &types_nodes::ddlnodes::AlterOwnerStmt<'_>) -> bool {
    a.objectType == b.objectType
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equal_opt_node(a.newowner.as_ref(), b.newowner.as_ref())
}

/// `_equalAlterPolicyStmt` (equalfuncs.funcs.c).
fn equal_alter_policy_stmt(a: &types_nodes::ddlnodes::AlterPolicyStmt<'_>, b: &types_nodes::ddlnodes::AlterPolicyStmt<'_>) -> bool {
    equalstr(a.policy_name.as_deref(), b.policy_name.as_deref())
        && equal_opt_node(a.table.as_ref(), b.table.as_ref())
        && equal_node_list(&a.roles, &b.roles)
        && equal_opt_node(a.qual.as_ref(), b.qual.as_ref())
        && equal_opt_node(a.with_check.as_ref(), b.with_check.as_ref())
}

/// `_equalAlterPublicationStmt` (equalfuncs.funcs.c).
fn equal_alter_publication_stmt(a: &types_nodes::ddlnodes::AlterPublicationStmt<'_>, b: &types_nodes::ddlnodes::AlterPublicationStmt<'_>) -> bool {
    equalstr(a.pubname.as_deref(), b.pubname.as_deref())
        && equal_node_list(&a.options, &b.options)
        && equal_node_list(&a.pubobjects, &b.pubobjects)
        && a.for_all_tables == b.for_all_tables
        && a.action == b.action
}

/// `_equalAlterRoleSetStmt` (equalfuncs.funcs.c).
fn equal_alter_role_set_stmt(a: &types_nodes::ddlnodes::AlterRoleSetStmt<'_>, b: &types_nodes::ddlnodes::AlterRoleSetStmt<'_>) -> bool {
    equal_opt_node(a.role.as_ref(), b.role.as_ref())
        && equalstr(a.database.as_deref(), b.database.as_deref())
        && equal_opt_node(a.setstmt.as_ref(), b.setstmt.as_ref())
}

/// `_equalAlterRoleStmt` (equalfuncs.funcs.c).
fn equal_alter_role_stmt(a: &types_nodes::ddlnodes::AlterRoleStmt<'_>, b: &types_nodes::ddlnodes::AlterRoleStmt<'_>) -> bool {
    equal_opt_node(a.role.as_ref(), b.role.as_ref())
        && equal_node_list(&a.options, &b.options)
        && a.action == b.action
}

/// `_equalAlterSeqStmt` (equalfuncs.funcs.c).
fn equal_alter_seq_stmt(a: &types_nodes::ddlnodes::AlterSeqStmt<'_>, b: &types_nodes::ddlnodes::AlterSeqStmt<'_>) -> bool {
    equal_opt_node(a.sequence.as_ref(), b.sequence.as_ref())
        && equal_node_list(&a.options, &b.options)
        && a.for_identity == b.for_identity
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterStatsStmt` (equalfuncs.funcs.c).
fn equal_alter_stats_stmt(a: &types_nodes::ddlnodes::AlterStatsStmt<'_>, b: &types_nodes::ddlnodes::AlterStatsStmt<'_>) -> bool {
    equal_node_list(&a.defnames, &b.defnames)
        && equal_opt_node(a.stxstattarget.as_ref(), b.stxstattarget.as_ref())
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterSubscriptionStmt` (equalfuncs.funcs.c).
fn equal_alter_subscription_stmt(a: &types_nodes::ddlnodes::AlterSubscriptionStmt<'_>, b: &types_nodes::ddlnodes::AlterSubscriptionStmt<'_>) -> bool {
    a.kind == b.kind
        && equalstr(a.subname.as_deref(), b.subname.as_deref())
        && equalstr(a.conninfo.as_deref(), b.conninfo.as_deref())
        && equal_node_list(&a.publication, &b.publication)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterSystemStmt` (equalfuncs.funcs.c).
fn equal_alter_system_stmt(a: &types_nodes::ddlnodes::AlterSystemStmt<'_>, b: &types_nodes::ddlnodes::AlterSystemStmt<'_>) -> bool {
    equal_opt_node(a.setstmt.as_ref(), b.setstmt.as_ref())
}

/// `_equalAlterTableCmd` (equalfuncs.funcs.c).
fn equal_alter_table_cmd(a: &types_nodes::ddlnodes::AlterTableCmd<'_>, b: &types_nodes::ddlnodes::AlterTableCmd<'_>) -> bool {
    a.subtype == b.subtype
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && a.num == b.num
        && equal_opt_node(a.newowner.as_ref(), b.newowner.as_ref())
        && equal_opt_node(a.def.as_ref(), b.def.as_ref())
        && a.behavior == b.behavior
        && a.missing_ok == b.missing_ok
        && a.recurse == b.recurse
}

/// `_equalAlterTableMoveAllStmt` (equalfuncs.funcs.c).
fn equal_alter_table_move_all_stmt(a: &types_nodes::ddlnodes::AlterTableMoveAllStmt<'_>, b: &types_nodes::ddlnodes::AlterTableMoveAllStmt<'_>) -> bool {
    equalstr(a.orig_tablespacename.as_deref(), b.orig_tablespacename.as_deref())
        && a.objtype == b.objtype
        && equal_node_list(&a.roles, &b.roles)
        && equalstr(a.new_tablespacename.as_deref(), b.new_tablespacename.as_deref())
        && a.nowait == b.nowait
}

/// `_equalAlterTableSpaceOptionsStmt` (equalfuncs.funcs.c).
fn equal_alter_table_space_options_stmt(a: &types_nodes::ddlnodes::AlterTableSpaceOptionsStmt<'_>, b: &types_nodes::ddlnodes::AlterTableSpaceOptionsStmt<'_>) -> bool {
    equalstr(a.tablespacename.as_deref(), b.tablespacename.as_deref())
        && equal_node_list(&a.options, &b.options)
        && a.isReset == b.isReset
}

/// `_equalAlterTableStmt` (equalfuncs.funcs.c).
fn equal_alter_table_stmt(a: &types_nodes::ddlnodes::AlterTableStmt<'_>, b: &types_nodes::ddlnodes::AlterTableStmt<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_node_list(&a.cmds, &b.cmds)
        && a.objtype == b.objtype
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterTSConfigurationStmt` (equalfuncs.funcs.c).
fn equal_alter_ts_configuration_stmt(a: &types_nodes::ddlnodes::AlterTSConfigurationStmt<'_>, b: &types_nodes::ddlnodes::AlterTSConfigurationStmt<'_>) -> bool {
    a.kind == b.kind
        && equal_node_list(&a.cfgname, &b.cfgname)
        && equal_node_list(&a.tokentype, &b.tokentype)
        && equal_node_list(&a.dicts, &b.dicts)
        && a.override_ == b.override_
        && a.replace == b.replace
        && a.missing_ok == b.missing_ok
}

/// `_equalAlterTSDictionaryStmt` (equalfuncs.funcs.c).
fn equal_alter_ts_dictionary_stmt(a: &types_nodes::ddlnodes::AlterTSDictionaryStmt<'_>, b: &types_nodes::ddlnodes::AlterTSDictionaryStmt<'_>) -> bool {
    equal_node_list(&a.dictname, &b.dictname)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterTypeStmt` (equalfuncs.funcs.c).
fn equal_alter_type_stmt(a: &types_nodes::ddlnodes::AlterTypeStmt<'_>, b: &types_nodes::ddlnodes::AlterTypeStmt<'_>) -> bool {
    equal_node_list(&a.typeName, &b.typeName)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalAlterUserMappingStmt` (equalfuncs.funcs.c).
fn equal_alter_user_mapping_stmt(a: &types_nodes::ddlnodes::AlterUserMappingStmt<'_>, b: &types_nodes::ddlnodes::AlterUserMappingStmt<'_>) -> bool {
    equal_opt_node(a.user.as_ref(), b.user.as_ref())
        && equalstr(a.servername.as_deref(), b.servername.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalATAlterConstraint` (equalfuncs.funcs.c).
fn equal_at_alter_constraint(a: &types_nodes::ddlnodes::ATAlterConstraint<'_>, b: &types_nodes::ddlnodes::ATAlterConstraint<'_>) -> bool {
    equalstr(a.conname.as_deref(), b.conname.as_deref())
        && a.alterEnforceability == b.alterEnforceability
        && a.is_enforced == b.is_enforced
        && a.alterDeferrability == b.alterDeferrability
        && a.deferrable == b.deferrable
        && a.initdeferred == b.initdeferred
        && a.alterInheritability == b.alterInheritability
        && a.noinherit == b.noinherit
}

/// `_equalCompositeTypeStmt` (equalfuncs.funcs.c).
fn equal_composite_type_stmt(a: &types_nodes::ddlnodes::CompositeTypeStmt<'_>, b: &types_nodes::ddlnodes::CompositeTypeStmt<'_>) -> bool {
    equal_opt_node(a.typevar.as_ref(), b.typevar.as_ref())
        && equal_node_list(&a.coldeflist, &b.coldeflist)
}

/// `_equalCreateCastStmt` (equalfuncs.funcs.c).
fn equal_create_cast_stmt(a: &types_nodes::ddlnodes::CreateCastStmt<'_>, b: &types_nodes::ddlnodes::CreateCastStmt<'_>) -> bool {
    equal_opt_node(a.sourcetype.as_ref(), b.sourcetype.as_ref())
        && equal_opt_node(a.targettype.as_ref(), b.targettype.as_ref())
        && equal_opt_node(a.func.as_ref(), b.func.as_ref())
        && a.context == b.context
        && a.inout == b.inout
}

/// `_equalCreatedbStmt` (equalfuncs.funcs.c).
fn equal_createdb_stmt(a: &types_nodes::ddlnodes::CreatedbStmt<'_>, b: &types_nodes::ddlnodes::CreatedbStmt<'_>) -> bool {
    equalstr(a.dbname.as_deref(), b.dbname.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateDomainStmt` (equalfuncs.funcs.c).
fn equal_create_domain_stmt(a: &types_nodes::ddlnodes::CreateDomainStmt<'_>, b: &types_nodes::ddlnodes::CreateDomainStmt<'_>) -> bool {
    equal_node_list(&a.domainname, &b.domainname)
        && equal_opt_node(a.typeName.as_ref(), b.typeName.as_ref())
        && equal_opt_node(a.collClause.as_ref(), b.collClause.as_ref())
        && equal_node_list(&a.constraints, &b.constraints)
}

/// `_equalCreateEnumStmt` (equalfuncs.funcs.c).
fn equal_create_enum_stmt(a: &types_nodes::ddlnodes::CreateEnumStmt<'_>, b: &types_nodes::ddlnodes::CreateEnumStmt<'_>) -> bool {
    equal_node_list(&a.typeName, &b.typeName)
        && equal_node_list(&a.vals, &b.vals)
}

/// `_equalCreateEventTrigStmt` (equalfuncs.funcs.c).
fn equal_create_event_trig_stmt(a: &types_nodes::ddlnodes::CreateEventTrigStmt<'_>, b: &types_nodes::ddlnodes::CreateEventTrigStmt<'_>) -> bool {
    equalstr(a.trigname.as_deref(), b.trigname.as_deref())
        && equalstr(a.eventname.as_deref(), b.eventname.as_deref())
        && equal_node_list(&a.whenclause, &b.whenclause)
        && equal_node_list(&a.funcname, &b.funcname)
}

/// `_equalCreateExtensionStmt` (equalfuncs.funcs.c).
fn equal_create_extension_stmt(a: &types_nodes::ddlnodes::CreateExtensionStmt<'_>, b: &types_nodes::ddlnodes::CreateExtensionStmt<'_>) -> bool {
    equalstr(a.extname.as_deref(), b.extname.as_deref())
        && a.if_not_exists == b.if_not_exists
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateFdwStmt` (equalfuncs.funcs.c).
fn equal_create_fdw_stmt(a: &types_nodes::ddlnodes::CreateFdwStmt<'_>, b: &types_nodes::ddlnodes::CreateFdwStmt<'_>) -> bool {
    equalstr(a.fdwname.as_deref(), b.fdwname.as_deref())
        && equal_node_list(&a.func_options, &b.func_options)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateForeignServerStmt` (equalfuncs.funcs.c).
fn equal_create_foreign_server_stmt(a: &types_nodes::ddlnodes::CreateForeignServerStmt<'_>, b: &types_nodes::ddlnodes::CreateForeignServerStmt<'_>) -> bool {
    equalstr(a.servername.as_deref(), b.servername.as_deref())
        && equalstr(a.servertype.as_deref(), b.servertype.as_deref())
        && equalstr(a.version.as_deref(), b.version.as_deref())
        && equalstr(a.fdwname.as_deref(), b.fdwname.as_deref())
        && a.if_not_exists == b.if_not_exists
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateForeignTableStmt` (equalfuncs.funcs.c).
fn equal_create_foreign_table_stmt(a: &types_nodes::ddlnodes::CreateForeignTableStmt<'_>, b: &types_nodes::ddlnodes::CreateForeignTableStmt<'_>) -> bool {
    equal_opt_node(a.base.relation.as_ref(), b.base.relation.as_ref())
        && equal_node_list(&a.base.tableElts, &b.base.tableElts)
        && equal_node_list(&a.base.inhRelations, &b.base.inhRelations)
        && equal_opt_node(a.base.partbound.as_ref(), b.base.partbound.as_ref())
        && equal_opt_node(a.base.partspec.as_ref(), b.base.partspec.as_ref())
        && equal_opt_node(a.base.ofTypename.as_ref(), b.base.ofTypename.as_ref())
        && equal_node_list(&a.base.constraints, &b.base.constraints)
        && equal_node_list(&a.base.nnconstraints, &b.base.nnconstraints)
        && equal_node_list(&a.base.options, &b.base.options)
        && a.base.oncommit == b.base.oncommit
        && equalstr(a.base.tablespacename.as_deref(), b.base.tablespacename.as_deref())
        && equalstr(a.base.accessMethod.as_deref(), b.base.accessMethod.as_deref())
        && a.base.if_not_exists == b.base.if_not_exists
        && equalstr(a.servername.as_deref(), b.servername.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateFunctionStmt` (equalfuncs.funcs.c).
fn equal_create_function_stmt(a: &types_nodes::ddlnodes::CreateFunctionStmt<'_>, b: &types_nodes::ddlnodes::CreateFunctionStmt<'_>) -> bool {
    a.is_procedure == b.is_procedure
        && a.replace == b.replace
        && equal_node_list(&a.funcname, &b.funcname)
        && equal_node_list(&a.parameters, &b.parameters)
        && equal_opt_node(a.returnType.as_ref(), b.returnType.as_ref())
        && equal_node_list(&a.options, &b.options)
        && equal_opt_node(a.sql_body.as_ref(), b.sql_body.as_ref())
}

/// `_equalCreateOpClassItem` (equalfuncs.funcs.c).
fn equal_create_op_class_item(a: &types_nodes::ddlnodes::CreateOpClassItem<'_>, b: &types_nodes::ddlnodes::CreateOpClassItem<'_>) -> bool {
    a.itemtype == b.itemtype
        && equal_opt_node(a.name.as_ref(), b.name.as_ref())
        && a.number == b.number
        && equal_node_list(&a.order_family, &b.order_family)
        && equal_node_list(&a.class_args, &b.class_args)
        && equal_opt_node(a.storedtype.as_ref(), b.storedtype.as_ref())
}

/// `_equalCreateOpClassStmt` (equalfuncs.funcs.c).
fn equal_create_op_class_stmt(a: &types_nodes::ddlnodes::CreateOpClassStmt<'_>, b: &types_nodes::ddlnodes::CreateOpClassStmt<'_>) -> bool {
    equal_node_list(&a.opclassname, &b.opclassname)
        && equal_node_list(&a.opfamilyname, &b.opfamilyname)
        && equalstr(a.amname.as_deref(), b.amname.as_deref())
        && equal_opt_node(a.datatype.as_ref(), b.datatype.as_ref())
        && equal_node_list(&a.items, &b.items)
        && a.isDefault == b.isDefault
}

/// `_equalCreateOpFamilyStmt` (equalfuncs.funcs.c).
fn equal_create_op_family_stmt(a: &types_nodes::ddlnodes::CreateOpFamilyStmt<'_>, b: &types_nodes::ddlnodes::CreateOpFamilyStmt<'_>) -> bool {
    equal_node_list(&a.opfamilyname, &b.opfamilyname)
        && equalstr(a.amname.as_deref(), b.amname.as_deref())
}

/// `_equalCreatePLangStmt` (equalfuncs.funcs.c).
fn equal_create_plang_stmt(a: &types_nodes::ddlnodes::CreatePLangStmt<'_>, b: &types_nodes::ddlnodes::CreatePLangStmt<'_>) -> bool {
    a.replace == b.replace
        && equalstr(a.plname.as_deref(), b.plname.as_deref())
        && equal_node_list(&a.plhandler, &b.plhandler)
        && equal_node_list(&a.plinline, &b.plinline)
        && equal_node_list(&a.plvalidator, &b.plvalidator)
        && a.pltrusted == b.pltrusted
}

/// `_equalCreatePolicyStmt` (equalfuncs.funcs.c).
fn equal_create_policy_stmt(a: &types_nodes::ddlnodes::CreatePolicyStmt<'_>, b: &types_nodes::ddlnodes::CreatePolicyStmt<'_>) -> bool {
    equalstr(a.policy_name.as_deref(), b.policy_name.as_deref())
        && equal_opt_node(a.table.as_ref(), b.table.as_ref())
        && equalstr(a.cmd_name.as_deref(), b.cmd_name.as_deref())
        && a.permissive == b.permissive
        && equal_node_list(&a.roles, &b.roles)
        && equal_opt_node(a.qual.as_ref(), b.qual.as_ref())
        && equal_opt_node(a.with_check.as_ref(), b.with_check.as_ref())
}

/// `_equalCreatePublicationStmt` (equalfuncs.funcs.c).
fn equal_create_publication_stmt(a: &types_nodes::ddlnodes::CreatePublicationStmt<'_>, b: &types_nodes::ddlnodes::CreatePublicationStmt<'_>) -> bool {
    equalstr(a.pubname.as_deref(), b.pubname.as_deref())
        && equal_node_list(&a.options, &b.options)
        && equal_node_list(&a.pubobjects, &b.pubobjects)
        && a.for_all_tables == b.for_all_tables
}

/// `_equalCreateRangeStmt` (equalfuncs.funcs.c).
fn equal_create_range_stmt(a: &types_nodes::ddlnodes::CreateRangeStmt<'_>, b: &types_nodes::ddlnodes::CreateRangeStmt<'_>) -> bool {
    equal_node_list(&a.typeName, &b.typeName)
        && equal_node_list(&a.params, &b.params)
}

/// `_equalCreateRoleStmt` (equalfuncs.funcs.c).
fn equal_create_role_stmt(a: &types_nodes::ddlnodes::CreateRoleStmt<'_>, b: &types_nodes::ddlnodes::CreateRoleStmt<'_>) -> bool {
    a.stmt_type == b.stmt_type
        && equalstr(a.role.as_deref(), b.role.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateSchemaStmt` (equalfuncs.funcs.c).
fn equal_create_schema_stmt(a: &types_nodes::ddlnodes::CreateSchemaStmt<'_>, b: &types_nodes::ddlnodes::CreateSchemaStmt<'_>) -> bool {
    equalstr(a.schemaname.as_deref(), b.schemaname.as_deref())
        && equal_opt_node(a.authrole.as_ref(), b.authrole.as_ref())
        && equal_node_list(&a.schemaElts, &b.schemaElts)
        && a.if_not_exists == b.if_not_exists
}

/// `_equalCreateSeqStmt` (equalfuncs.funcs.c).
fn equal_create_seq_stmt(a: &types_nodes::ddlnodes::CreateSeqStmt<'_>, b: &types_nodes::ddlnodes::CreateSeqStmt<'_>) -> bool {
    equal_opt_node(a.sequence.as_ref(), b.sequence.as_ref())
        && equal_node_list(&a.options, &b.options)
        && a.ownerId == b.ownerId
        && a.for_identity == b.for_identity
        && a.if_not_exists == b.if_not_exists
}

/// `_equalCreateStatsStmt` (equalfuncs.funcs.c).
fn equal_create_stats_stmt(a: &types_nodes::ddlnodes::CreateStatsStmt<'_>, b: &types_nodes::ddlnodes::CreateStatsStmt<'_>) -> bool {
    equal_node_list(&a.defnames, &b.defnames)
        && equal_node_list(&a.stat_types, &b.stat_types)
        && equal_node_list(&a.exprs, &b.exprs)
        && equal_node_list(&a.relations, &b.relations)
        && equalstr(a.stxcomment.as_deref(), b.stxcomment.as_deref())
        && a.transformed == b.transformed
        && a.if_not_exists == b.if_not_exists
}

/// `_equalCreateStmt` (equalfuncs.funcs.c).
fn equal_create_stmt(a: &types_nodes::ddlnodes::CreateStmt<'_>, b: &types_nodes::ddlnodes::CreateStmt<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_node_list(&a.tableElts, &b.tableElts)
        && equal_node_list(&a.inhRelations, &b.inhRelations)
        && equal_opt_node(a.partbound.as_ref(), b.partbound.as_ref())
        && equal_opt_node(a.partspec.as_ref(), b.partspec.as_ref())
        && equal_opt_node(a.ofTypename.as_ref(), b.ofTypename.as_ref())
        && equal_node_list(&a.constraints, &b.constraints)
        && equal_node_list(&a.nnconstraints, &b.nnconstraints)
        && equal_node_list(&a.options, &b.options)
        && a.oncommit == b.oncommit
        && equalstr(a.tablespacename.as_deref(), b.tablespacename.as_deref())
        && equalstr(a.accessMethod.as_deref(), b.accessMethod.as_deref())
        && a.if_not_exists == b.if_not_exists
}

/// `_equalCreateSubscriptionStmt` (equalfuncs.funcs.c).
fn equal_create_subscription_stmt(a: &types_nodes::ddlnodes::CreateSubscriptionStmt<'_>, b: &types_nodes::ddlnodes::CreateSubscriptionStmt<'_>) -> bool {
    equalstr(a.subname.as_deref(), b.subname.as_deref())
        && equalstr(a.conninfo.as_deref(), b.conninfo.as_deref())
        && equal_node_list(&a.publication, &b.publication)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateTableAsStmt` (equalfuncs.funcs.c).
fn equal_create_table_as_stmt(a: &types_nodes::ddlnodes::CreateTableAsStmt<'_>, b: &types_nodes::ddlnodes::CreateTableAsStmt<'_>) -> bool {
    equal_opt_node(a.query.as_ref(), b.query.as_ref())
        && equal_opt_node(a.into.as_ref(), b.into.as_ref())
        && a.objtype == b.objtype
        && a.is_select_into == b.is_select_into
        && a.if_not_exists == b.if_not_exists
}

/// `_equalCreateTableSpaceStmt` (equalfuncs.funcs.c).
fn equal_create_table_space_stmt(a: &types_nodes::ddlnodes::CreateTableSpaceStmt<'_>, b: &types_nodes::ddlnodes::CreateTableSpaceStmt<'_>) -> bool {
    equalstr(a.tablespacename.as_deref(), b.tablespacename.as_deref())
        && equal_opt_node(a.owner.as_ref(), b.owner.as_ref())
        && equalstr(a.location.as_deref(), b.location.as_deref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCreateTransformStmt` (equalfuncs.funcs.c).
fn equal_create_transform_stmt(a: &types_nodes::ddlnodes::CreateTransformStmt<'_>, b: &types_nodes::ddlnodes::CreateTransformStmt<'_>) -> bool {
    a.replace == b.replace
        && equal_opt_node(a.type_name.as_ref(), b.type_name.as_ref())
        && equalstr(a.lang.as_deref(), b.lang.as_deref())
        && equal_opt_node(a.fromsql.as_ref(), b.fromsql.as_ref())
        && equal_opt_node(a.tosql.as_ref(), b.tosql.as_ref())
}

/// `_equalCreateTrigStmt` (equalfuncs.funcs.c).
fn equal_create_trig_stmt(a: &types_nodes::ddlnodes::CreateTrigStmt<'_>, b: &types_nodes::ddlnodes::CreateTrigStmt<'_>) -> bool {
    a.replace == b.replace
        && a.isconstraint == b.isconstraint
        && equalstr(a.trigname.as_deref(), b.trigname.as_deref())
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_node_list(&a.funcname, &b.funcname)
        && equal_node_list(&a.args, &b.args)
        && a.row == b.row
        && a.timing == b.timing
        && a.events == b.events
        && equal_node_list(&a.columns, &b.columns)
        && equal_opt_node(a.whenClause.as_ref(), b.whenClause.as_ref())
        && equal_node_list(&a.transitionRels, &b.transitionRels)
        && a.deferrable == b.deferrable
        && a.initdeferred == b.initdeferred
        && equal_opt_node(a.constrrel.as_ref(), b.constrrel.as_ref())
}

/// `_equalCreateUserMappingStmt` (equalfuncs.funcs.c).
fn equal_create_user_mapping_stmt(a: &types_nodes::ddlnodes::CreateUserMappingStmt<'_>, b: &types_nodes::ddlnodes::CreateUserMappingStmt<'_>) -> bool {
    equal_opt_node(a.user.as_ref(), b.user.as_ref())
        && equalstr(a.servername.as_deref(), b.servername.as_deref())
        && a.if_not_exists == b.if_not_exists
        && equal_node_list(&a.options, &b.options)
}

/// `_equalCallStmt` (equalfuncs.funcs.c).
fn equal_call_stmt(a: &types_nodes::ddlnodes::CallStmt<'_>, b: &types_nodes::ddlnodes::CallStmt<'_>) -> bool {
    equal_opt_node(a.funccall.as_ref(), b.funccall.as_ref())
        && equal_opt_node(a.funcexpr.as_ref(), b.funcexpr.as_ref())
        && equal_node_list(&a.outargs, &b.outargs)
}

/// `_equalCheckPointStmt` (equalfuncs.funcs.c).
fn equal_check_point_stmt(_a: &types_nodes::ddlnodes::CheckPointStmt, _b: &types_nodes::ddlnodes::CheckPointStmt) -> bool {
    true
}

/// `_equalClosePortalStmt` (equalfuncs.funcs.c).
fn equal_close_portal_stmt(a: &types_nodes::ddlnodes::ClosePortalStmt<'_>, b: &types_nodes::ddlnodes::ClosePortalStmt<'_>) -> bool {
    equalstr(a.portalname.as_deref(), b.portalname.as_deref())
}

/// `_equalClusterStmt` (equalfuncs.funcs.c).
fn equal_cluster_stmt(a: &types_nodes::ddlnodes::ClusterStmt<'_>, b: &types_nodes::ddlnodes::ClusterStmt<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equalstr(a.indexname.as_deref(), b.indexname.as_deref())
        && equal_node_list(&a.params, &b.params)
}

/// `_equalCommentStmt` (equalfuncs.funcs.c).
fn equal_comment_stmt(a: &types_nodes::ddlnodes::CommentStmt<'_>, b: &types_nodes::ddlnodes::CommentStmt<'_>) -> bool {
    a.objtype == b.objtype
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equalstr(a.comment.as_deref(), b.comment.as_deref())
}

/// `_equalConstraintsSetStmt` (equalfuncs.funcs.c).
fn equal_constraints_set_stmt(a: &types_nodes::ddlnodes::ConstraintsSetStmt<'_>, b: &types_nodes::ddlnodes::ConstraintsSetStmt<'_>) -> bool {
    equal_node_list(&a.constraints, &b.constraints)
        && a.deferred == b.deferred
}

/// `_equalCopyStmt` (equalfuncs.funcs.c).
fn equal_copy_stmt(a: &types_nodes::ddlnodes::CopyStmt<'_>, b: &types_nodes::ddlnodes::CopyStmt<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.query.as_ref(), b.query.as_ref())
        && equal_node_list(&a.attlist, &b.attlist)
        && a.is_from == b.is_from
        && a.is_program == b.is_program
        && equalstr(a.filename.as_deref(), b.filename.as_deref())
        && equal_node_list(&a.options, &b.options)
        && equal_opt_node(a.where_clause.as_ref(), b.where_clause.as_ref())
}

/// `_equalConstraint` (equalfuncs.funcs.c).
fn equal_constraint(a: &types_nodes::ddlnodes::Constraint<'_>, b: &types_nodes::ddlnodes::Constraint<'_>) -> bool {
    a.contype == b.contype
        && equalstr(a.conname.as_deref(), b.conname.as_deref())
        && a.deferrable == b.deferrable
        && a.initdeferred == b.initdeferred
        && a.is_enforced == b.is_enforced
        && a.skip_validation == b.skip_validation
        && a.initially_valid == b.initially_valid
        && a.is_no_inherit == b.is_no_inherit
        && equal_opt_node(a.raw_expr.as_ref(), b.raw_expr.as_ref())
        && equalstr(a.cooked_expr.as_deref(), b.cooked_expr.as_deref())
        && a.generated_when == b.generated_when
        && a.generated_kind == b.generated_kind
        && a.nulls_not_distinct == b.nulls_not_distinct
        && equal_node_list(&a.keys, &b.keys)
        && a.without_overlaps == b.without_overlaps
        && equal_node_list(&a.including, &b.including)
        && equal_node_list(&a.exclusions, &b.exclusions)
        && equal_node_list(&a.options, &b.options)
        && equalstr(a.indexname.as_deref(), b.indexname.as_deref())
        && equalstr(a.indexspace.as_deref(), b.indexspace.as_deref())
        && a.reset_default_tblspc == b.reset_default_tblspc
        && equalstr(a.access_method.as_deref(), b.access_method.as_deref())
        && equal_opt_node(a.where_clause.as_ref(), b.where_clause.as_ref())
        && equal_opt_node(a.pktable.as_ref(), b.pktable.as_ref())
        && equal_node_list(&a.fk_attrs, &b.fk_attrs)
        && equal_node_list(&a.pk_attrs, &b.pk_attrs)
        && a.fk_with_period == b.fk_with_period
        && a.pk_with_period == b.pk_with_period
        && a.fk_matchtype == b.fk_matchtype
        && a.fk_upd_action == b.fk_upd_action
        && a.fk_del_action == b.fk_del_action
        && equal_node_list(&a.fk_del_set_cols, &b.fk_del_set_cols)
        && equal_node_list(&a.old_conpfeqop, &b.old_conpfeqop)
        && a.old_pktable_oid == b.old_pktable_oid
    // location no-op
}

/// `_equalDeclareCursorStmt` (equalfuncs.funcs.c).
fn equal_declare_cursor_stmt(a: &types_nodes::ddlnodes::DeclareCursorStmt<'_>, b: &types_nodes::ddlnodes::DeclareCursorStmt<'_>) -> bool {
    equalstr(a.portalname.as_deref(), b.portalname.as_deref())
        && a.options == b.options
        && equal_opt_node(a.query.as_ref(), b.query.as_ref())
}

/// `_equalDefElem` (equalfuncs.funcs.c).
fn equal_def_elem(a: &types_nodes::ddlnodes::DefElem<'_>, b: &types_nodes::ddlnodes::DefElem<'_>) -> bool {
    equalstr(a.defnamespace.as_deref(), b.defnamespace.as_deref())
        && equalstr(a.defname.as_deref(), b.defname.as_deref())
        && equal_opt_node(a.arg.as_ref(), b.arg.as_ref())
        && a.defaction == b.defaction
    // location no-op
}

/// `_equalDefineStmt` (equalfuncs.funcs.c).
fn equal_define_stmt(a: &types_nodes::ddlnodes::DefineStmt<'_>, b: &types_nodes::ddlnodes::DefineStmt<'_>) -> bool {
    a.kind == b.kind
        && a.oldstyle == b.oldstyle
        && equal_node_list(&a.defnames, &b.defnames)
        && equal_node_list(&a.args, &b.args)
        && equal_node_list(&a.definition, &b.definition)
        && a.if_not_exists == b.if_not_exists
        && a.replace == b.replace
}

/// `_equalDiscardStmt` (equalfuncs.funcs.c).
fn equal_discard_stmt(a: &types_nodes::ddlnodes::DiscardStmt, b: &types_nodes::ddlnodes::DiscardStmt) -> bool {
    a.target == b.target
}

/// `_equalDoStmt` (equalfuncs.funcs.c).
fn equal_do_stmt(a: &types_nodes::ddlnodes::DoStmt<'_>, b: &types_nodes::ddlnodes::DoStmt<'_>) -> bool {
    equal_node_list(&a.args, &b.args)
}

/// `_equalDropdbStmt` (equalfuncs.funcs.c).
fn equal_dropdb_stmt(a: &types_nodes::ddlnodes::DropdbStmt<'_>, b: &types_nodes::ddlnodes::DropdbStmt<'_>) -> bool {
    equalstr(a.dbname.as_deref(), b.dbname.as_deref())
        && a.missing_ok == b.missing_ok
        && equal_node_list(&a.options, &b.options)
}

/// `_equalDropOwnedStmt` (equalfuncs.funcs.c).
fn equal_drop_owned_stmt(a: &types_nodes::ddlnodes::DropOwnedStmt<'_>, b: &types_nodes::ddlnodes::DropOwnedStmt<'_>) -> bool {
    equal_node_list(&a.roles, &b.roles)
        && a.behavior == b.behavior
}

/// `_equalDropRoleStmt` (equalfuncs.funcs.c).
fn equal_drop_role_stmt(a: &types_nodes::ddlnodes::DropRoleStmt<'_>, b: &types_nodes::ddlnodes::DropRoleStmt<'_>) -> bool {
    equal_node_list(&a.roles, &b.roles)
        && a.missing_ok == b.missing_ok
}

/// `_equalDropStmt` (equalfuncs.funcs.c).
fn equal_drop_stmt(a: &types_nodes::ddlnodes::DropStmt<'_>, b: &types_nodes::ddlnodes::DropStmt<'_>) -> bool {
    equal_node_list(&a.objects, &b.objects)
        && a.removeType == b.removeType
        && a.behavior == b.behavior
        && a.missing_ok == b.missing_ok
        && a.concurrent == b.concurrent
}

/// `_equalDropSubscriptionStmt` (equalfuncs.funcs.c).
fn equal_drop_subscription_stmt(a: &types_nodes::ddlnodes::DropSubscriptionStmt<'_>, b: &types_nodes::ddlnodes::DropSubscriptionStmt<'_>) -> bool {
    equalstr(a.subname.as_deref(), b.subname.as_deref())
        && a.missing_ok == b.missing_ok
        && a.behavior == b.behavior
}

/// `_equalDropTableSpaceStmt` (equalfuncs.funcs.c).
fn equal_drop_table_space_stmt(a: &types_nodes::ddlnodes::DropTableSpaceStmt<'_>, b: &types_nodes::ddlnodes::DropTableSpaceStmt<'_>) -> bool {
    equalstr(a.tablespacename.as_deref(), b.tablespacename.as_deref())
        && a.missing_ok == b.missing_ok
}

/// `_equalDropUserMappingStmt` (equalfuncs.funcs.c).
fn equal_drop_user_mapping_stmt(a: &types_nodes::ddlnodes::DropUserMappingStmt<'_>, b: &types_nodes::ddlnodes::DropUserMappingStmt<'_>) -> bool {
    equal_opt_node(a.user.as_ref(), b.user.as_ref())
        && equalstr(a.servername.as_deref(), b.servername.as_deref())
        && a.missing_ok == b.missing_ok
}

/// `_equalExecuteStmt` (equalfuncs.funcs.c).
fn equal_execute_stmt(a: &types_nodes::ddlnodes::ExecuteStmt<'_>, b: &types_nodes::ddlnodes::ExecuteStmt<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.params, &b.params)
}

/// `_equalExplainStmt` (equalfuncs.funcs.c).
fn equal_explain_stmt(a: &types_nodes::ddlnodes::ExplainStmt<'_>, b: &types_nodes::ddlnodes::ExplainStmt<'_>) -> bool {
    equal_opt_node(a.query.as_ref(), b.query.as_ref())
        && equal_node_list(&a.options, &b.options)
}

/// `_equalFunctionParameter` (equalfuncs.funcs.c).
fn equal_function_parameter(a: &types_nodes::ddlnodes::FunctionParameter<'_>, b: &types_nodes::ddlnodes::FunctionParameter<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_opt_node(a.argType.as_ref(), b.argType.as_ref())
        && a.mode == b.mode
        && equal_opt_node(a.defexpr.as_ref(), b.defexpr.as_ref())
    // location no-op
}

/// `_equalGrantRoleStmt` (equalfuncs.funcs.c).
fn equal_grant_role_stmt(a: &types_nodes::ddlnodes::GrantRoleStmt<'_>, b: &types_nodes::ddlnodes::GrantRoleStmt<'_>) -> bool {
    equal_node_list(&a.granted_roles, &b.granted_roles)
        && equal_node_list(&a.grantee_roles, &b.grantee_roles)
        && a.is_grant == b.is_grant
        && equal_node_list(&a.opt, &b.opt)
        && equal_opt_node(a.grantor.as_ref(), b.grantor.as_ref())
        && a.behavior == b.behavior
}

/// `_equalGrantStmt` (equalfuncs.funcs.c).
fn equal_grant_stmt(a: &types_nodes::ddlnodes::GrantStmt<'_>, b: &types_nodes::ddlnodes::GrantStmt<'_>) -> bool {
    a.is_grant == b.is_grant
        && a.targtype == b.targtype
        && a.objtype == b.objtype
        && equal_node_list(&a.objects, &b.objects)
        && equal_node_list(&a.privileges, &b.privileges)
        && equal_node_list(&a.grantees, &b.grantees)
        && a.grant_option == b.grant_option
        && equal_opt_node(a.grantor.as_ref(), b.grantor.as_ref())
        && a.behavior == b.behavior
}

/// `_equalImportForeignSchemaStmt` (equalfuncs.funcs.c).
fn equal_import_foreign_schema_stmt(a: &types_nodes::ddlnodes::ImportForeignSchemaStmt<'_>, b: &types_nodes::ddlnodes::ImportForeignSchemaStmt<'_>) -> bool {
    equalstr(a.server_name.as_deref(), b.server_name.as_deref())
        && equalstr(a.remote_schema.as_deref(), b.remote_schema.as_deref())
        && equalstr(a.local_schema.as_deref(), b.local_schema.as_deref())
        && a.list_type == b.list_type
        && equal_node_list(&a.table_list, &b.table_list)
        && equal_node_list(&a.options, &b.options)
}

/// `_equalIndexStmt` (equalfuncs.funcs.c).
fn equal_index_stmt(a: &types_nodes::ddlnodes::IndexStmt<'_>, b: &types_nodes::ddlnodes::IndexStmt<'_>) -> bool {
    equalstr(a.idxname.as_deref(), b.idxname.as_deref())
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equalstr(a.accessMethod.as_deref(), b.accessMethod.as_deref())
        && equalstr(a.tableSpace.as_deref(), b.tableSpace.as_deref())
        && equal_node_list(&a.indexParams, &b.indexParams)
        && equal_node_list(&a.indexIncludingParams, &b.indexIncludingParams)
        && equal_node_list(&a.options, &b.options)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
        && equal_node_list(&a.excludeOpNames, &b.excludeOpNames)
        && equalstr(a.idxcomment.as_deref(), b.idxcomment.as_deref())
        && a.indexOid == b.indexOid
        && a.oldNumber == b.oldNumber
        && a.oldCreateSubid == b.oldCreateSubid
        && a.oldFirstRelfilelocatorSubid == b.oldFirstRelfilelocatorSubid
        && a.unique == b.unique
        && a.nulls_not_distinct == b.nulls_not_distinct
        && a.primary == b.primary
        && a.isconstraint == b.isconstraint
        && a.iswithoutoverlaps == b.iswithoutoverlaps
        && a.deferrable == b.deferrable
        && a.initdeferred == b.initdeferred
        && a.transformed == b.transformed
        && a.concurrent == b.concurrent
        && a.if_not_exists == b.if_not_exists
        && a.reset_default_tblspc == b.reset_default_tblspc
}

/// `_equalIntoClause` (equalfuncs.funcs.c).
fn equal_into_clause(a: &types_nodes::ddlnodes::IntoClause<'_>, b: &types_nodes::ddlnodes::IntoClause<'_>) -> bool {
    equal_opt_node(a.rel.as_ref(), b.rel.as_ref())
        && equal_node_list(&a.colNames, &b.colNames)
        && equalstr(a.accessMethod.as_deref(), b.accessMethod.as_deref())
        && equal_node_list(&a.options, &b.options)
        && a.onCommit == b.onCommit
        && equalstr(a.tableSpaceName.as_deref(), b.tableSpaceName.as_deref())
        && equal_opt_node(a.viewQuery.as_ref(), b.viewQuery.as_ref())
        && a.skipData == b.skipData
}

/// `_equalListenStmt` (equalfuncs.funcs.c).
fn equal_listen_stmt(a: &types_nodes::ddlnodes::ListenStmt<'_>, b: &types_nodes::ddlnodes::ListenStmt<'_>) -> bool {
    equalstr(a.conditionname.as_deref(), b.conditionname.as_deref())
}

/// `_equalLoadStmt` (equalfuncs.funcs.c).
fn equal_load_stmt(a: &types_nodes::ddlnodes::LoadStmt<'_>, b: &types_nodes::ddlnodes::LoadStmt<'_>) -> bool {
    equalstr(a.filename.as_deref(), b.filename.as_deref())
}

/// `_equalLockStmt` (equalfuncs.funcs.c).
fn equal_lock_stmt(a: &types_nodes::ddlnodes::LockStmt<'_>, b: &types_nodes::ddlnodes::LockStmt<'_>) -> bool {
    equal_node_list(&a.relations, &b.relations)
        && a.mode == b.mode
        && a.nowait == b.nowait
}

/// `_equalNotifyStmt` (equalfuncs.funcs.c).
fn equal_notify_stmt(a: &types_nodes::ddlnodes::NotifyStmt<'_>, b: &types_nodes::ddlnodes::NotifyStmt<'_>) -> bool {
    equalstr(a.conditionname.as_deref(), b.conditionname.as_deref())
        && equalstr(a.payload.as_deref(), b.payload.as_deref())
}

/// `_equalObjectWithArgs` (equalfuncs.funcs.c).
fn equal_object_with_args(a: &types_nodes::ddlnodes::ObjectWithArgs<'_>, b: &types_nodes::ddlnodes::ObjectWithArgs<'_>) -> bool {
    equal_node_list(&a.objname, &b.objname)
        && equal_node_list(&a.objargs, &b.objargs)
        && equal_node_list(&a.objfuncargs, &b.objfuncargs)
        && a.args_unspecified == b.args_unspecified
}

/// `_equalPartitionBoundSpec` (equalfuncs.funcs.c).
fn equal_partition_bound_spec(a: &types_nodes::ddlnodes::PartitionBoundSpec<'_>, b: &types_nodes::ddlnodes::PartitionBoundSpec<'_>) -> bool {
    a.strategy == b.strategy
        && a.is_default == b.is_default
        && a.modulus == b.modulus
        && a.remainder == b.remainder
        && equal_node_list(&a.listdatums, &b.listdatums)
        && equal_node_list(&a.lowerdatums, &b.lowerdatums)
        && equal_node_list(&a.upperdatums, &b.upperdatums)
    // location no-op
}

/// `_equalPartitionCmd` (equalfuncs.funcs.c).
fn equal_partition_cmd(a: &types_nodes::ddlnodes::PartitionCmd<'_>, b: &types_nodes::ddlnodes::PartitionCmd<'_>) -> bool {
    equal_opt_node(a.name.as_ref(), b.name.as_ref())
        && equal_opt_node(a.bound.as_ref(), b.bound.as_ref())
        && a.concurrent == b.concurrent
}

/// `_equalPartitionElem` (equalfuncs.funcs.c).
fn equal_partition_elem(a: &types_nodes::ddlnodes::PartitionElem<'_>, b: &types_nodes::ddlnodes::PartitionElem<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_opt_node(a.expr.as_ref(), b.expr.as_ref())
        && equal_node_list(&a.collation, &b.collation)
        && equal_node_list(&a.opclass, &b.opclass)
    // location no-op
}

/// `_equalPartitionRangeDatum` (equalfuncs.funcs.c).
fn equal_partition_range_datum(a: &types_nodes::ddlnodes::PartitionRangeDatum<'_>, b: &types_nodes::ddlnodes::PartitionRangeDatum<'_>) -> bool {
    a.kind == b.kind
        && equal_opt_node(a.value.as_ref(), b.value.as_ref())
    // location no-op
}

/// `_equalPartitionSpec` (equalfuncs.funcs.c).
fn equal_partition_spec(a: &types_nodes::ddlnodes::PartitionSpec<'_>, b: &types_nodes::ddlnodes::PartitionSpec<'_>) -> bool {
    a.strategy == b.strategy
        && equal_node_list(&a.partParams, &b.partParams)
    // location no-op
}

/// `_equalPLAssignStmt` (equalfuncs.funcs.c).
fn equal_pl_assign_stmt(a: &types_nodes::ddlnodes::PLAssignStmt<'_>, b: &types_nodes::ddlnodes::PLAssignStmt<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.indirection, &b.indirection)
        && a.nnames == b.nnames
        && equal_opt_node(a.val.as_ref(), b.val.as_ref())
    // location no-op
}

/// `_equalPublicationTable` (equalfuncs.funcs.c).
fn equal_publication_table(a: &types_nodes::ddlnodes::PublicationTable<'_>, b: &types_nodes::ddlnodes::PublicationTable<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.where_clause.as_ref(), b.where_clause.as_ref())
        && equal_node_list(&a.columns, &b.columns)
}

/// `_equalPublicationObjSpec` (equalfuncs.funcs.c).
fn equal_publication_obj_spec(a: &types_nodes::ddlnodes::PublicationObjSpec<'_>, b: &types_nodes::ddlnodes::PublicationObjSpec<'_>) -> bool {
    a.pubobjtype == b.pubobjtype
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && match (a.pubtable.as_deref(), b.pubtable.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_publication_table(x, y),
            _ => false,
        }
    // location no-op
}

/// `_equalReassignOwnedStmt` (equalfuncs.funcs.c).
fn equal_reassign_owned_stmt(a: &types_nodes::ddlnodes::ReassignOwnedStmt<'_>, b: &types_nodes::ddlnodes::ReassignOwnedStmt<'_>) -> bool {
    equal_node_list(&a.roles, &b.roles)
        && equal_opt_node(a.newrole.as_ref(), b.newrole.as_ref())
}

/// `_equalRefreshMatViewStmt` (equalfuncs.funcs.c).
fn equal_refresh_mat_view_stmt(a: &types_nodes::ddlnodes::RefreshMatViewStmt<'_>, b: &types_nodes::ddlnodes::RefreshMatViewStmt<'_>) -> bool {
    a.concurrent == b.concurrent
        && a.skip_data == b.skip_data
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
}

/// `_equalReindexStmt` (equalfuncs.funcs.c).
fn equal_reindex_stmt(a: &types_nodes::ddlnodes::ReindexStmt<'_>, b: &types_nodes::ddlnodes::ReindexStmt<'_>) -> bool {
    a.kind == b.kind
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.params, &b.params)
}

/// `_equalRenameStmt` (equalfuncs.funcs.c).
fn equal_rename_stmt(a: &types_nodes::ddlnodes::RenameStmt<'_>, b: &types_nodes::ddlnodes::RenameStmt<'_>) -> bool {
    a.renameType == b.renameType
        && a.relationType == b.relationType
        && equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equalstr(a.subname.as_deref(), b.subname.as_deref())
        && equalstr(a.newname.as_deref(), b.newname.as_deref())
        && a.behavior == b.behavior
        && a.missing_ok == b.missing_ok
}

/// `_equalReplicaIdentityStmt` (equalfuncs.funcs.c).
fn equal_replica_identity_stmt(a: &types_nodes::ddlnodes::ReplicaIdentityStmt<'_>, b: &types_nodes::ddlnodes::ReplicaIdentityStmt<'_>) -> bool {
    a.identity_type == b.identity_type
        && equalstr(a.name.as_deref(), b.name.as_deref())
}

/// `_equalReturnStmt` (equalfuncs.funcs.c).
fn equal_return_stmt(a: &types_nodes::ddlnodes::ReturnStmt<'_>, b: &types_nodes::ddlnodes::ReturnStmt<'_>) -> bool {
    equal_opt_node(a.returnval.as_ref(), b.returnval.as_ref())
}

/// `_equalRuleStmt` (equalfuncs.funcs.c).
fn equal_rule_stmt(a: &types_nodes::ddlnodes::RuleStmt<'_>, b: &types_nodes::ddlnodes::RuleStmt<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equalstr(a.rulename.as_deref(), b.rulename.as_deref())
        && equal_opt_node(a.where_clause.as_ref(), b.where_clause.as_ref())
        && a.event == b.event
        && a.instead == b.instead
        && equal_node_list(&a.actions, &b.actions)
        && a.replace == b.replace
}

/// `_equalSecLabelStmt` (equalfuncs.funcs.c).
fn equal_sec_label_stmt(a: &types_nodes::ddlnodes::SecLabelStmt<'_>, b: &types_nodes::ddlnodes::SecLabelStmt<'_>) -> bool {
    a.objtype == b.objtype
        && equal_opt_node(a.object.as_ref(), b.object.as_ref())
        && equalstr(a.provider.as_deref(), b.provider.as_deref())
        && equalstr(a.label.as_deref(), b.label.as_deref())
}

/// `_equalStatsElem` (equalfuncs.funcs.c).
fn equal_stats_elem(a: &types_nodes::ddlnodes::StatsElem<'_>, b: &types_nodes::ddlnodes::StatsElem<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_opt_node(a.expr.as_ref(), b.expr.as_ref())
}

/// `_equalTableLikeClause` (equalfuncs.funcs.c).
fn equal_table_like_clause(a: &types_nodes::ddlnodes::TableLikeClause<'_>, b: &types_nodes::ddlnodes::TableLikeClause<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && a.options == b.options
        && a.relationOid == b.relationOid
}

/// `_equalTransactionStmt` (equalfuncs.funcs.c).
fn equal_transaction_stmt(a: &types_nodes::ddlnodes::TransactionStmt<'_>, b: &types_nodes::ddlnodes::TransactionStmt<'_>) -> bool {
    a.kind == b.kind
        && equal_node_list(&a.options, &b.options)
        && equalstr(a.savepoint_name.as_deref(), b.savepoint_name.as_deref())
        && equalstr(a.gid.as_deref(), b.gid.as_deref())
        && a.chain == b.chain
    // location no-op
}

/// `_equalTruncateStmt` (equalfuncs.funcs.c).
fn equal_truncate_stmt(a: &types_nodes::ddlnodes::TruncateStmt<'_>, b: &types_nodes::ddlnodes::TruncateStmt<'_>) -> bool {
    equal_node_list(&a.relations, &b.relations)
        && a.restart_seqs == b.restart_seqs
        && a.behavior == b.behavior
}

/// `_equalUnlistenStmt` (equalfuncs.funcs.c).
fn equal_unlisten_stmt(a: &types_nodes::ddlnodes::UnlistenStmt<'_>, b: &types_nodes::ddlnodes::UnlistenStmt<'_>) -> bool {
    equalstr(a.conditionname.as_deref(), b.conditionname.as_deref())
}

/// `_equalVacuumRelation` (equalfuncs.funcs.c).
fn equal_vacuum_relation(a: &types_nodes::ddlnodes::VacuumRelation<'_>, b: &types_nodes::ddlnodes::VacuumRelation<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && a.oid == b.oid
        && equal_node_list(&a.va_cols, &b.va_cols)
}

/// `_equalVacuumStmt` (equalfuncs.funcs.c).
fn equal_vacuum_stmt(a: &types_nodes::ddlnodes::VacuumStmt<'_>, b: &types_nodes::ddlnodes::VacuumStmt<'_>) -> bool {
    equal_node_list(&a.options, &b.options)
        && equal_node_list(&a.rels, &b.rels)
        && a.is_vacuumcmd == b.is_vacuumcmd
}

/// `_equalVariableSetStmt` (equalfuncs.funcs.c).
fn equal_variable_set_stmt(a: &types_nodes::ddlnodes::VariableSetStmt<'_>, b: &types_nodes::ddlnodes::VariableSetStmt<'_>) -> bool {
    a.kind == b.kind
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.args, &b.args)
        && a.jumble_args == b.jumble_args
        && a.is_local == b.is_local
    // location no-op
}

/// `_equalVariableShowStmt` (equalfuncs.funcs.c).
fn equal_variable_show_stmt(a: &types_nodes::ddlnodes::VariableShowStmt<'_>, b: &types_nodes::ddlnodes::VariableShowStmt<'_>) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
}

/// `_equalViewStmt` (equalfuncs.funcs.c).
fn equal_view_stmt(a: &types_nodes::ddlnodes::ViewStmt<'_>, b: &types_nodes::ddlnodes::ViewStmt<'_>) -> bool {
    equal_opt_node(a.view.as_ref(), b.view.as_ref())
        && equal_node_list(&a.aliases, &b.aliases)
        && equal_opt_node(a.query.as_ref(), b.query.as_ref())
        && a.replace == b.replace
        && equal_node_list(&a.options, &b.options)
        && a.withCheckOption == b.withCheckOption
}

/// `_equalRangeVar` (equalfuncs.funcs.c).
fn equal_range_var(a: &types_nodes::rawnodes::RangeVar<'_>, b: &types_nodes::rawnodes::RangeVar<'_>) -> bool {
    equalstr(a.catalogname.as_deref(), b.catalogname.as_deref())
        && equalstr(a.schemaname.as_deref(), b.schemaname.as_deref())
        && equalstr(a.relname.as_deref(), b.relname.as_deref())
        && a.inh == b.inh
        && a.relpersistence == b.relpersistence
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
    // location no-op
}

/// `_equalColumnDef` (equalfuncs.funcs.c).
fn equal_column_def(a: &types_nodes::rawnodes::ColumnDef<'_>, b: &types_nodes::rawnodes::ColumnDef<'_>) -> bool {
    equalstr(a.colname.as_deref(), b.colname.as_deref())
        && match (a.typeName.as_deref(), b.typeName.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_type_name(x, y),
            _ => false,
        }
        && equalstr(a.compression.as_deref(), b.compression.as_deref())
        && a.inhcount == b.inhcount
        && a.is_local == b.is_local
        && a.is_not_null == b.is_not_null
        && a.is_from_type == b.is_from_type
        && a.storage == b.storage
        && equalstr(a.storage_name.as_deref(), b.storage_name.as_deref())
        && equal_opt_node(a.raw_default.as_ref(), b.raw_default.as_ref())
        && equal_opt_node(a.cooked_default.as_ref(), b.cooked_default.as_ref())
        && a.identity == b.identity
        && match (a.identitySequence.as_deref(), b.identitySequence.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_range_var(x, y),
            _ => false,
        }
        && a.generated == b.generated
        && match (a.collClause.as_deref(), b.collClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_collate_clause(x, y),
            _ => false,
        }
        && a.collOid == b.collOid
        && equal_node_list(&a.constraints, &b.constraints)
        && equal_node_list(&a.fdwoptions, &b.fdwoptions)
    // location no-op
}

/// `_equalSelectStmt` (equalfuncs.funcs.c).
fn equal_select_stmt(a: &types_nodes::rawnodes::SelectStmt<'_>, b: &types_nodes::rawnodes::SelectStmt<'_>) -> bool {
    equal_node_list(&a.distinctClause, &b.distinctClause)
        && equal_opt_node(a.intoClause.as_ref(), b.intoClause.as_ref())
        && equal_node_list(&a.targetList, &b.targetList)
        && equal_node_list(&a.fromClause, &b.fromClause)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
        && equal_node_list(&a.groupClause, &b.groupClause)
        && a.groupDistinct == b.groupDistinct
        && equal_opt_node(a.havingClause.as_ref(), b.havingClause.as_ref())
        && equal_node_list(&a.windowClause, &b.windowClause)
        && equal_node_list(&a.valuesLists, &b.valuesLists)
        && equal_node_list(&a.sortClause, &b.sortClause)
        && equal_opt_node(a.limitOffset.as_ref(), b.limitOffset.as_ref())
        && equal_opt_node(a.limitCount.as_ref(), b.limitCount.as_ref())
        && a.limitOption == b.limitOption
        && equal_node_list(&a.lockingClause, &b.lockingClause)
        && match (a.withClause.as_deref(), b.withClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_with_clause(x, y),
            _ => false,
        }
        && a.op == b.op
        && a.all == b.all
        && match (a.larg.as_deref(), b.larg.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_select_stmt(x, y),
            _ => false,
        }
        && match (a.rarg.as_deref(), b.rarg.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_select_stmt(x, y),
            _ => false,
        }
}

/// `_equalInsertStmt` (equalfuncs.funcs.c).
fn equal_insert_stmt(a: &types_nodes::rawnodes::InsertStmt<'_>, b: &types_nodes::rawnodes::InsertStmt<'_>) -> bool {
    (match (a.relation.as_deref(), b.relation.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_range_var(x, y),
        _ => false,
    })
        && equal_node_list(&a.cols, &b.cols)
        && equal_opt_node(a.selectStmt.as_ref(), b.selectStmt.as_ref())
        && match (a.onConflictClause.as_deref(), b.onConflictClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_on_conflict_clause(x, y),
            _ => false,
        }
        && match (a.returningClause.as_deref(), b.returningClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_returning_clause(x, y),
            _ => false,
        }
        && match (a.withClause.as_deref(), b.withClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_with_clause(x, y),
            _ => false,
        }
        && a.r#override == b.r#override
}

/// `_equalUpdateStmt` (equalfuncs.funcs.c).
fn equal_update_stmt(a: &types_nodes::rawnodes::UpdateStmt<'_>, b: &types_nodes::rawnodes::UpdateStmt<'_>) -> bool {
    (match (a.relation.as_deref(), b.relation.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_range_var(x, y),
        _ => false,
    })
        && equal_node_list(&a.targetList, &b.targetList)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
        && equal_node_list(&a.fromClause, &b.fromClause)
        && match (a.returningClause.as_deref(), b.returningClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_returning_clause(x, y),
            _ => false,
        }
        && match (a.withClause.as_deref(), b.withClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_with_clause(x, y),
            _ => false,
        }
}

/// `_equalDeleteStmt` (equalfuncs.funcs.c).
fn equal_delete_stmt(a: &types_nodes::rawnodes::DeleteStmt<'_>, b: &types_nodes::rawnodes::DeleteStmt<'_>) -> bool {
    (match (a.relation.as_deref(), b.relation.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_range_var(x, y),
        _ => false,
    })
        && equal_node_list(&a.usingClause, &b.usingClause)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
        && match (a.returningClause.as_deref(), b.returningClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_returning_clause(x, y),
            _ => false,
        }
        && match (a.withClause.as_deref(), b.withClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_with_clause(x, y),
            _ => false,
        }
}

/// `_equalMergeStmt` (equalfuncs.funcs.c).
fn equal_merge_stmt(a: &types_nodes::rawnodes::MergeStmt<'_>, b: &types_nodes::rawnodes::MergeStmt<'_>) -> bool {
    (match (a.relation.as_deref(), b.relation.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_range_var(x, y),
        _ => false,
    })
        && equal_opt_node(a.sourceRelation.as_ref(), b.sourceRelation.as_ref())
        && equal_opt_node(a.joinCondition.as_ref(), b.joinCondition.as_ref())
        && equal_node_list(&a.mergeWhenClauses, &b.mergeWhenClauses)
        && match (a.returningClause.as_deref(), b.returningClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_returning_clause(x, y),
            _ => false,
        }
        && match (a.withClause.as_deref(), b.withClause.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_with_clause(x, y),
            _ => false,
        }
}

/// `_equalMergeWhenClause` (equalfuncs.funcs.c).
fn equal_merge_when_clause(a: &types_nodes::rawnodes::MergeWhenClause<'_>, b: &types_nodes::rawnodes::MergeWhenClause<'_>) -> bool {
    a.matchKind == b.matchKind
        && a.commandType == b.commandType
        && a.r#override == b.r#override
        && equal_opt_node(a.condition.as_ref(), b.condition.as_ref())
        && equal_node_list(&a.targetList, &b.targetList)
        && equal_node_list(&a.values, &b.values)
}

/// `_equalRangeFunction` (equalfuncs.funcs.c).
fn equal_range_function(a: &types_nodes::rawnodes::RangeFunction<'_>, b: &types_nodes::rawnodes::RangeFunction<'_>) -> bool {
    a.lateral == b.lateral
        && a.ordinality == b.ordinality
        && a.is_rowsfrom == b.is_rowsfrom
        && equal_node_list(&a.functions, &b.functions)
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
        && equal_node_list(&a.coldeflist, &b.coldeflist)
}

/// `_equalRangeSubselect` (equalfuncs.funcs.c).
fn equal_range_subselect(a: &types_nodes::rawnodes::RangeSubselect<'_>, b: &types_nodes::rawnodes::RangeSubselect<'_>) -> bool {
    a.lateral == b.lateral
        && equal_opt_node(a.subquery.as_ref(), b.subquery.as_ref())
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
}

/// `_equalRangeTableFunc` (equalfuncs.funcs.c).
fn equal_range_table_func(a: &types_nodes::rawnodes::RangeTableFunc<'_>, b: &types_nodes::rawnodes::RangeTableFunc<'_>) -> bool {
    a.lateral == b.lateral
        && equal_opt_node(a.docexpr.as_ref(), b.docexpr.as_ref())
        && equal_opt_node(a.rowexpr.as_ref(), b.rowexpr.as_ref())
        && equal_node_list(&a.namespaces, &b.namespaces)
        && equal_node_list(&a.columns, &b.columns)
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
    // location no-op
}

/// `_equalRangeTableFuncCol` (equalfuncs.funcs.c).
fn equal_range_table_func_col(a: &types_nodes::rawnodes::RangeTableFuncCol<'_>, b: &types_nodes::rawnodes::RangeTableFuncCol<'_>) -> bool {
    equalstr(a.colname.as_deref(), b.colname.as_deref())
        && match (a.typeName.as_deref(), b.typeName.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_type_name(x, y),
            _ => false,
        }
        && a.for_ordinality == b.for_ordinality
        && a.is_not_null == b.is_not_null
        && equal_opt_node(a.colexpr.as_ref(), b.colexpr.as_ref())
        && equal_opt_node(a.coldefexpr.as_ref(), b.coldefexpr.as_ref())
    // location no-op
}

/// `_equalRangeTableSample` (equalfuncs.funcs.c).
fn equal_range_table_sample(a: &types_nodes::rawnodes::RangeTableSample<'_>, b: &types_nodes::rawnodes::RangeTableSample<'_>) -> bool {
    equal_opt_node(a.relation.as_ref(), b.relation.as_ref())
        && equal_node_list(&a.method, &b.method)
        && equal_node_list(&a.args, &b.args)
        && equal_opt_node(a.repeatable.as_ref(), b.repeatable.as_ref())
    // location no-op
}

/// `_equalInferClause` (equalfuncs.funcs.c).
fn equal_infer_clause(a: &types_nodes::rawnodes::InferClause<'_>, b: &types_nodes::rawnodes::InferClause<'_>) -> bool {
    equal_node_list(&a.indexElems, &b.indexElems)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
        && equalstr(a.conname.as_deref(), b.conname.as_deref())
    // location no-op
}

/// `_equalOnConflictClause` (equalfuncs.funcs.c).
fn equal_on_conflict_clause(a: &types_nodes::rawnodes::OnConflictClause<'_>, b: &types_nodes::rawnodes::OnConflictClause<'_>) -> bool {
    a.action == b.action
        && match (a.infer.as_deref(), b.infer.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_infer_clause(x, y),
            _ => false,
        }
        && equal_node_list(&a.targetList, &b.targetList)
        && equal_opt_node(a.whereClause.as_ref(), b.whereClause.as_ref())
    // location no-op
}

/// `_equalLockingClause` (equalfuncs.funcs.c).
fn equal_locking_clause(a: &types_nodes::rawnodes::LockingClause<'_>, b: &types_nodes::rawnodes::LockingClause<'_>) -> bool {
    equal_node_list(&a.lockedRels, &b.lockedRels)
        && a.strength == b.strength
        && a.waitPolicy == b.waitPolicy
}

/// `_equalWithClause` (equalfuncs.funcs.c).
fn equal_with_clause(a: &types_nodes::rawnodes::WithClause<'_>, b: &types_nodes::rawnodes::WithClause<'_>) -> bool {
    equal_node_list(&a.ctes, &b.ctes)
        && a.recursive == b.recursive
    // location no-op
}

/// `_equalTableSampleClause` (equalfuncs.funcs.c).
fn equal_table_sample_clause(a: &types_nodes::nodesamplescan::TableSampleClause<'_>, b: &types_nodes::nodesamplescan::TableSampleClause<'_>) -> bool {
    a.tsmhandler == b.tsmhandler
        && match (a.args.as_ref(), b.args.as_ref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_expr_list_impl(x, y),
            _ => false,
        }
        && equal_opt_expr(a.repeatable.as_deref(), b.repeatable.as_deref())
}

/// `_equalReturningClause` (equalfuncs.funcs.c).
fn equal_returning_clause(a: &types_nodes::rawnodes::ReturningClause<'_>, b: &types_nodes::rawnodes::ReturningClause<'_>) -> bool {
    equal_node_list(&a.options, &b.options)
        && equal_node_list(&a.exprs, &b.exprs)
}

/// `_equalReturningOption` (equalfuncs.funcs.c).
fn equal_returning_option(a: &types_nodes::rawnodes::ReturningOption<'_>, b: &types_nodes::rawnodes::ReturningOption<'_>) -> bool {
    a.option == b.option
        && equalstr(a.value.as_deref(), b.value.as_deref())
    // location no-op
}

/// `_equalJsonBehavior` (equalfuncs.funcs.c) — RAW parse node (`rawexprnodes::JsonBehavior`).
fn equal_json_behavior_raw(a: &types_nodes::rawexprnodes::JsonBehavior<'_>, b: &types_nodes::rawexprnodes::JsonBehavior<'_>) -> bool {
    a.btype == b.btype
        && equal_opt_node(a.expr.as_ref(), b.expr.as_ref())
        && a.coerce == b.coerce
    // location no-op
}

/// `_equalJsonOutput` (equalfuncs.funcs.c).
fn equal_json_output(a: &types_nodes::rawexprnodes::JsonOutput<'_>, b: &types_nodes::rawexprnodes::JsonOutput<'_>) -> bool {
    (match (a.type_name.as_deref(), b.type_name.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_type_name(x, y),
        _ => false,
    })
        && equal_opt_json_returning(a.returning.as_ref(), b.returning.as_ref())
}

/// `_equalJsonArgument` (equalfuncs.funcs.c).
fn equal_json_argument(a: &types_nodes::rawexprnodes::JsonArgument<'_>, b: &types_nodes::rawexprnodes::JsonArgument<'_>) -> bool {
    (match (a.val.as_deref(), b.val.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
        _ => false,
    })
        && equalstr(a.name.as_deref(), b.name.as_deref())
}

/// `_equalJsonFuncExpr` (equalfuncs.funcs.c).
fn equal_json_func_expr(a: &types_nodes::rawexprnodes::JsonFuncExpr<'_>, b: &types_nodes::rawexprnodes::JsonFuncExpr<'_>) -> bool {
    a.op == b.op
        && equalstr(a.column_name.as_deref(), b.column_name.as_deref())
        && match (a.context_item.as_deref(), b.context_item.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
            _ => false,
        }
        && equal_opt_node(a.pathspec.as_ref(), b.pathspec.as_ref())
        && equal_node_list(&a.passing, &b.passing)
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
        && equal_opt_node(a.on_empty.as_ref(), b.on_empty.as_ref())
        && equal_opt_node(a.on_error.as_ref(), b.on_error.as_ref())
        && a.wrapper == b.wrapper
        && a.quotes == b.quotes
    // location no-op
}

/// `_equalJsonTablePathSpec` (equalfuncs.funcs.c).
fn equal_json_table_path_spec(a: &types_nodes::rawexprnodes::JsonTablePathSpec<'_>, b: &types_nodes::rawexprnodes::JsonTablePathSpec<'_>) -> bool {
    equal_opt_node(a.string.as_ref(), b.string.as_ref())
        && equalstr(a.name.as_deref(), b.name.as_deref())
    // name_location / location no-op
}

/// `_equalJsonTable` (equalfuncs.funcs.c).
fn equal_json_table(a: &types_nodes::rawexprnodes::JsonTable<'_>, b: &types_nodes::rawexprnodes::JsonTable<'_>) -> bool {
    (match (a.context_item.as_deref(), b.context_item.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
        _ => false,
    })
        && match (a.pathspec.as_deref(), b.pathspec.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_table_path_spec(x, y),
            _ => false,
        }
        && equal_node_list(&a.passing, &b.passing)
        && equal_node_list(&a.columns, &b.columns)
        && equal_opt_node(a.on_error.as_ref(), b.on_error.as_ref())
        && equal_opt_alias(a.alias.as_deref(), b.alias.as_deref())
        && a.lateral == b.lateral
    // location no-op
}

/// `_equalJsonTableColumn` (equalfuncs.funcs.c).
fn equal_json_table_column(a: &types_nodes::rawexprnodes::JsonTableColumn<'_>, b: &types_nodes::rawexprnodes::JsonTableColumn<'_>) -> bool {
    a.coltype == b.coltype
        && equalstr(a.name.as_deref(), b.name.as_deref())
        && match (a.type_name.as_deref(), b.type_name.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_type_name(x, y),
            _ => false,
        }
        && match (a.pathspec.as_deref(), b.pathspec.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_table_path_spec(x, y),
            _ => false,
        }
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
        && a.wrapper == b.wrapper
        && a.quotes == b.quotes
        && equal_node_list(&a.columns, &b.columns)
        && equal_opt_node(a.on_empty.as_ref(), b.on_empty.as_ref())
        && equal_opt_node(a.on_error.as_ref(), b.on_error.as_ref())
    // location no-op
}

/// `_equalJsonKeyValue` (equalfuncs.funcs.c).
fn equal_json_key_value(a: &types_nodes::rawexprnodes::JsonKeyValue<'_>, b: &types_nodes::rawexprnodes::JsonKeyValue<'_>) -> bool {
    equal_opt_node(a.key.as_ref(), b.key.as_ref())
        && match (a.value.as_deref(), b.value.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
            _ => false,
        }
}

/// `_equalJsonParseExpr` (equalfuncs.funcs.c).
fn equal_json_parse_expr(a: &types_nodes::rawexprnodes::JsonParseExpr<'_>, b: &types_nodes::rawexprnodes::JsonParseExpr<'_>) -> bool {
    (match (a.expr.as_deref(), b.expr.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
        _ => false,
    })
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
        && a.unique_keys == b.unique_keys
    // location no-op
}

/// `_equalJsonScalarExpr` (equalfuncs.funcs.c).
fn equal_json_scalar_expr(a: &types_nodes::rawexprnodes::JsonScalarExpr<'_>, b: &types_nodes::rawexprnodes::JsonScalarExpr<'_>) -> bool {
    equal_opt_node(a.expr.as_ref(), b.expr.as_ref())
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
    // location no-op
}

/// `_equalJsonSerializeExpr` (equalfuncs.funcs.c).
fn equal_json_serialize_expr(a: &types_nodes::rawexprnodes::JsonSerializeExpr<'_>, b: &types_nodes::rawexprnodes::JsonSerializeExpr<'_>) -> bool {
    (match (a.expr.as_deref(), b.expr.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
        _ => false,
    })
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
    // location no-op
}

/// `_equalJsonObjectConstructor` (equalfuncs.funcs.c).
fn equal_json_object_constructor(a: &types_nodes::rawexprnodes::JsonObjectConstructor<'_>, b: &types_nodes::rawexprnodes::JsonObjectConstructor<'_>) -> bool {
    equal_node_list(&a.exprs, &b.exprs)
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
        && a.absent_on_null == b.absent_on_null
        && a.unique == b.unique
    // location no-op
}

/// `_equalJsonArrayConstructor` (equalfuncs.funcs.c).
fn equal_json_array_constructor(a: &types_nodes::rawexprnodes::JsonArrayConstructor<'_>, b: &types_nodes::rawexprnodes::JsonArrayConstructor<'_>) -> bool {
    equal_node_list(&a.exprs, &b.exprs)
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
        && a.absent_on_null == b.absent_on_null
    // location no-op
}

/// `_equalJsonArrayQueryConstructor` (equalfuncs.funcs.c).
fn equal_json_array_query_constructor(a: &types_nodes::rawexprnodes::JsonArrayQueryConstructor<'_>, b: &types_nodes::rawexprnodes::JsonArrayQueryConstructor<'_>) -> bool {
    equal_opt_node(a.query.as_ref(), b.query.as_ref())
        && match (a.output.as_deref(), b.output.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_output(x, y),
            _ => false,
        }
        && equal_opt_json_format(a.format.as_ref(), b.format.as_ref())
        && a.absent_on_null == b.absent_on_null
    // location no-op
}

/// `_equalJsonAggConstructor` (equalfuncs.funcs.c).
fn equal_json_agg_constructor(a: &types_nodes::rawexprnodes::JsonAggConstructor<'_>, b: &types_nodes::rawexprnodes::JsonAggConstructor<'_>) -> bool {
    (match (a.output.as_deref(), b.output.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_output(x, y),
        _ => false,
    })
        && equal_opt_node(a.agg_filter.as_ref(), b.agg_filter.as_ref())
        && equal_node_list(&a.agg_order, &b.agg_order)
        && match (a.over.as_deref(), b.over.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_window_def(x, y),
            _ => false,
        }
    // location no-op
}

/// `_equalJsonObjectAgg` (equalfuncs.funcs.c).
fn equal_json_object_agg(a: &types_nodes::rawexprnodes::JsonObjectAgg<'_>, b: &types_nodes::rawexprnodes::JsonObjectAgg<'_>) -> bool {
    (match (a.constructor.as_deref(), b.constructor.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_agg_constructor(x, y),
        _ => false,
    })
        && match (a.arg.as_deref(), b.arg.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_key_value(x, y),
            _ => false,
        }
        && a.absent_on_null == b.absent_on_null
        && a.unique == b.unique
}

/// `_equalJsonArrayAgg` (equalfuncs.funcs.c).
fn equal_json_array_agg(a: &types_nodes::rawexprnodes::JsonArrayAgg<'_>, b: &types_nodes::rawexprnodes::JsonArrayAgg<'_>) -> bool {
    (match (a.constructor.as_deref(), b.constructor.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_agg_constructor(x, y),
        _ => false,
    })
        && match (a.arg.as_deref(), b.arg.as_deref()) {
            (None, None) => true,
            (Some(x), Some(y)) => equal_json_value_expr_raw(x, y),
            _ => false,
        }
        && a.absent_on_null == b.absent_on_null
}

/// `_equalCreateAmStmt` (equalfuncs.funcs.c).
fn equal_create_am_stmt(
    a: &types_nodes::ddlnodes::CreateAmStmt<'_>,
    b: &types_nodes::ddlnodes::CreateAmStmt<'_>,
) -> bool {
    equalstr(a.amname.as_deref(), b.amname.as_deref())
        && equal_node_list(&a.handler_name, &b.handler_name)
        && a.amtype == b.amtype
}

/// `_equalCreateConversionStmt` (equalfuncs.funcs.c).
fn equal_create_conversion_stmt(
    a: &types_nodes::ddlnodes::CreateConversionStmt<'_>,
    b: &types_nodes::ddlnodes::CreateConversionStmt<'_>,
) -> bool {
    equal_node_list(&a.conversion_name, &b.conversion_name)
        && equalstr(a.for_encoding_name.as_deref(), b.for_encoding_name.as_deref())
        && equalstr(a.to_encoding_name.as_deref(), b.to_encoding_name.as_deref())
        && equal_node_list(&a.func_name, &b.func_name)
        && a.def == b.def
}

/// `_equalDeallocateStmt` (equalfuncs.funcs.c).
fn equal_deallocate_stmt(
    a: &types_nodes::ddlnodes::DeallocateStmt<'_>,
    b: &types_nodes::ddlnodes::DeallocateStmt<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && a.isall == b.isall
    // location no-op
}

/// `_equalPrepareStmt` (equalfuncs.funcs.c).
fn equal_prepare_stmt(
    a: &types_nodes::ddlnodes::PrepareStmt<'_>,
    b: &types_nodes::ddlnodes::PrepareStmt<'_>,
) -> bool {
    equalstr(a.name.as_deref(), b.name.as_deref())
        && equal_node_list(&a.argtypes, &b.argtypes)
        && equal_opt_node(a.query.as_ref(), b.query.as_ref())
}

/// `_equalFetchStmt` (equalfuncs.funcs.c).
fn equal_fetch_stmt(
    a: &types_nodes::ddlnodes::FetchStmt<'_>,
    b: &types_nodes::ddlnodes::FetchStmt<'_>,
) -> bool {
    a.direction == b.direction
        && a.how_many == b.how_many
        && equalstr(a.portalname.as_deref(), b.portalname.as_deref())
        && a.ismove == b.ismove
}

/// `_equalRoleSpec` (equalfuncs.funcs.c).
fn equal_role_spec(
    a: &types_nodes::ddlnodes::RoleSpec<'_>,
    b: &types_nodes::ddlnodes::RoleSpec<'_>,
) -> bool {
    a.roletype == b.roletype
        && equalstr(a.rolename.as_deref(), b.rolename.as_deref())
    // location no-op
}


/// `equal(a, b)` over two general `Node *`: the full `equalfuncs.c` switch. The
/// `a == b` / one-NULL early returns are the caller's concern (Rust references
/// are always non-null); the `nodeTag(a) != nodeTag(b)` rule is the
/// different-variant `_ => false` arms.
pub fn equal_node(a: &Node<'_>, b: &Node<'_>) -> bool {
    // Expr-family nodes are dual-homed: a post-analysis `Node::Expr` shares its
    // NodeTag with a raw-grammar twin, so pure tag dispatch can't tell them
    // apart. Peel `Node::Expr` on both sides first (structural), then dispatch
    // the remaining single-tag arms through `node_tag()` + `expect_*`.
    if let (Some(x), Some(y)) = (a.as_expr(), b.as_expr()) {
        return equal_expr(x, y);
    }
    // If exactly one side is an `Expr` and the other a same-tagged raw twin, the
    // tags match but the variants differ — fall through so the tag-mismatch /
    // variant-specific arms below decide (raw `expect_*` will fire on the raw
    // side; a one-sided Expr is the not-yet-ported panic).
    match (a.node_tag(), b.node_tag()) {
        (ntag::T_TargetEntry, ntag::T_TargetEntry) => {
            equal_target_entry(a.expect_targetentry(), b.expect_targetentry())
        }
        (ntag::T_TableFunc, ntag::T_TableFunc) => {
            equal_table_func(a.expect_tablefunc(), b.expect_tablefunc())
        }
        (ntag::T_CTECycleClause, ntag::T_CTECycleClause) => {
            equal_cte_cycle_clause(a.expect_ctecycleclause(), b.expect_ctecycleclause())
        }
        (ntag::T_SortGroupClause, ntag::T_SortGroupClause) => {
            equal_sort_group_clause(a.expect_sortgroupclause(), b.expect_sortgroupclause())
        }
        // The Value leaf nodes (`_equalInteger`/`_equalFloat`/`_equalBoolean`/
        // `_equalString`/`_equalBitString`) compare by their single value field;
        // the `#[derive(PgNode)]`-generated `PgNodeEqual::equal_node` IS that
        // faithful per-struct comparator.
        (ntag::T_Integer, ntag::T_Integer) => a.expect_integer().equal_node(b.expect_integer()),
        (ntag::T_Float, ntag::T_Float) => a.expect_float().equal_node(b.expect_float()),
        (ntag::T_Boolean, ntag::T_Boolean) => a.expect_boolean().equal_node(b.expect_boolean()),
        (ntag::T_String, ntag::T_String) => a.expect_string().equal_node(b.expect_string()),
        (ntag::T_BitString, ntag::T_BitString) => {
            a.expect_bitstring().equal_node(b.expect_bitstring())
        }
        // `_equalList` (T_List): equal length then element-wise `equal()`.
        (ntag::T_List, ntag::T_List) => {
            let x = a.expect_list();
            let y = b.expect_list();
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| equal_node(p, q))
        }
        // Parse/analyze/rewrite query-tree node family (`_equalQuery` and the
        // sub-node comparators reachable through `Query`'s `Node`-list fields).
        (ntag::T_Query, ntag::T_Query) => equal_query(a.expect_query(), b.expect_query()),
        (ntag::T_RangeTblEntry, ntag::T_RangeTblEntry) => {
            equal_range_tbl_entry(a.expect_rangetblentry(), b.expect_rangetblentry())
        }
        (ntag::T_RTEPermissionInfo, ntag::T_RTEPermissionInfo) => {
            equal_rte_permission_info(a.expect_rtepermissioninfo(), b.expect_rtepermissioninfo())
        }
        (ntag::T_RangeTblFunction, ntag::T_RangeTblFunction) => {
            equal_range_tbl_function(a.expect_rangetblfunction(), b.expect_rangetblfunction())
        }
        (ntag::T_RangeTblRef, ntag::T_RangeTblRef) => {
            equal_range_tbl_ref(a.expect_rangetblref(), b.expect_rangetblref())
        }
        (ntag::T_FromExpr, ntag::T_FromExpr) => {
            equal_from_expr(a.expect_fromexpr(), b.expect_fromexpr())
        }
        (ntag::T_JoinExpr, ntag::T_JoinExpr) => {
            equal_join_expr(a.expect_joinexpr(), b.expect_joinexpr())
        }
        (ntag::T_OnConflictExpr, ntag::T_OnConflictExpr) => {
            equal_on_conflict_expr(a.expect_onconflictexpr(), b.expect_onconflictexpr())
        }
        (ntag::T_MergeAction, ntag::T_MergeAction) => {
            equal_merge_action(a.expect_mergeaction(), b.expect_mergeaction())
        }
        (ntag::T_GroupingSet, ntag::T_GroupingSet) => {
            equal_grouping_set(a.expect_groupingset(), b.expect_groupingset())
        }
        (ntag::T_WindowClause, ntag::T_WindowClause) => {
            equal_window_clause(a.expect_windowclause(), b.expect_windowclause())
        }
        (ntag::T_RowMarkClause, ntag::T_RowMarkClause) => {
            equal_row_mark_clause(a.expect_rowmarkclause(), b.expect_rowmarkclause())
        }
        (ntag::T_WithCheckOption, ntag::T_WithCheckOption) => {
            equal_with_check_option(a.expect_withcheckoption(), b.expect_withcheckoption())
        }
        (ntag::T_CommonTableExpr, ntag::T_CommonTableExpr) => {
            equal_common_table_expr(a.expect_commontableexpr(), b.expect_commontableexpr())
        }
        (ntag::T_SetOperationStmt, ntag::T_SetOperationStmt) => {
            equal_set_operation_stmt(a.expect_setoperationstmt(), b.expect_setoperationstmt())
        }
        (ntag::T_Alias, ntag::T_Alias) => equal_alias(a.expect_alias(), b.expect_alias()),
        // Raw-grammar parse nodes (equalfuncs.funcs.c). Reached by `equal()` over
        // an untransformed parse tree (e.g. transformWindowFuncCall window dedup).
        (ntag::T_ColumnRef, ntag::T_ColumnRef) => {
            equal_column_ref(a.expect_columnref(), b.expect_columnref())
        }
        (ntag::T_ParamRef, ntag::T_ParamRef) => {
            equal_param_ref(a.expect_paramref(), b.expect_paramref())
        }
        (ntag::T_A_Expr, ntag::T_A_Expr) => equal_a_expr(a.expect_a_expr(), b.expect_a_expr()),
        (ntag::T_A_Const, ntag::T_A_Const) => equal_a_const(a.expect_a_const(), b.expect_a_const()),
        (ntag::T_FuncCall, ntag::T_FuncCall) => {
            equal_func_call(a.expect_funccall(), b.expect_funccall())
        }
        (ntag::T_A_Star, ntag::T_A_Star) => equal_a_star(a.expect_a_star(), b.expect_a_star()),
        (ntag::T_A_Indices, ntag::T_A_Indices) => {
            equal_a_indices(a.expect_a_indices(), b.expect_a_indices())
        }
        (ntag::T_A_Indirection, ntag::T_A_Indirection) => {
            equal_a_indirection(a.expect_a_indirection(), b.expect_a_indirection())
        }
        (ntag::T_A_ArrayExpr, ntag::T_A_ArrayExpr) => {
            equal_a_array_expr(a.expect_a_arrayexpr(), b.expect_a_arrayexpr())
        }
        (ntag::T_TypeName, ntag::T_TypeName) => {
            equal_type_name(a.expect_typename(), b.expect_typename())
        }
        (ntag::T_TypeCast, ntag::T_TypeCast) => {
            equal_type_cast(a.expect_typecast(), b.expect_typecast())
        }
        (ntag::T_CollateClause, ntag::T_CollateClause) => {
            equal_collate_clause(a.expect_collateclause(), b.expect_collateclause())
        }
        (ntag::T_ResTarget, ntag::T_ResTarget) => {
            equal_res_target(a.expect_restarget(), b.expect_restarget())
        }
        (ntag::T_MultiAssignRef, ntag::T_MultiAssignRef) => {
            equal_multi_assign_ref(a.expect_multiassignref(), b.expect_multiassignref())
        }
        (ntag::T_IndexElem, ntag::T_IndexElem) => {
            equal_index_elem(a.expect_indexelem(), b.expect_indexelem())
        }
        (ntag::T_SortBy, ntag::T_SortBy) => equal_sort_by(a.expect_sortby(), b.expect_sortby()),
        (ntag::T_WindowDef, ntag::T_WindowDef) => {
            equal_window_def(a.expect_windowdef(), b.expect_windowdef())
        }
        (ntag::T_AccessPriv, ntag::T_AccessPriv) => equal_access_priv(a.expect_accesspriv(), b.expect_accesspriv()),
        (ntag::T_AlterCollationStmt, ntag::T_AlterCollationStmt) => equal_alter_collation_stmt(a.expect_altercollationstmt(), b.expect_altercollationstmt()),
        (ntag::T_AlterDatabaseRefreshCollStmt, ntag::T_AlterDatabaseRefreshCollStmt) => equal_alter_database_refresh_coll_stmt(a.expect_alterdatabaserefreshcollstmt(), b.expect_alterdatabaserefreshcollstmt()),
        (ntag::T_AlterDatabaseSetStmt, ntag::T_AlterDatabaseSetStmt) => equal_alter_database_set_stmt(a.expect_alterdatabasesetstmt(), b.expect_alterdatabasesetstmt()),
        (ntag::T_AlterDatabaseStmt, ntag::T_AlterDatabaseStmt) => equal_alter_database_stmt(a.expect_alterdatabasestmt(), b.expect_alterdatabasestmt()),
        (ntag::T_AlterDefaultPrivilegesStmt, ntag::T_AlterDefaultPrivilegesStmt) => equal_alter_default_privileges_stmt(a.expect_alterdefaultprivilegesstmt(), b.expect_alterdefaultprivilegesstmt()),
        (ntag::T_AlterDomainStmt, ntag::T_AlterDomainStmt) => equal_alter_domain_stmt(a.expect_alterdomainstmt(), b.expect_alterdomainstmt()),
        (ntag::T_AlterEnumStmt, ntag::T_AlterEnumStmt) => equal_alter_enum_stmt(a.expect_alterenumstmt(), b.expect_alterenumstmt()),
        (ntag::T_AlterEventTrigStmt, ntag::T_AlterEventTrigStmt) => equal_alter_event_trig_stmt(a.expect_altereventtrigstmt(), b.expect_altereventtrigstmt()),
        (ntag::T_AlterExtensionContentsStmt, ntag::T_AlterExtensionContentsStmt) => equal_alter_extension_contents_stmt(a.expect_alterextensioncontentsstmt(), b.expect_alterextensioncontentsstmt()),
        (ntag::T_AlterExtensionStmt, ntag::T_AlterExtensionStmt) => equal_alter_extension_stmt(a.expect_alterextensionstmt(), b.expect_alterextensionstmt()),
        (ntag::T_AlterFdwStmt, ntag::T_AlterFdwStmt) => equal_alter_fdw_stmt(a.expect_alterfdwstmt(), b.expect_alterfdwstmt()),
        (ntag::T_AlterForeignServerStmt, ntag::T_AlterForeignServerStmt) => equal_alter_foreign_server_stmt(a.expect_alterforeignserverstmt(), b.expect_alterforeignserverstmt()),
        (ntag::T_AlterFunctionStmt, ntag::T_AlterFunctionStmt) => equal_alter_function_stmt(a.expect_alterfunctionstmt(), b.expect_alterfunctionstmt()),
        (ntag::T_AlterObjectDependsStmt, ntag::T_AlterObjectDependsStmt) => equal_alter_object_depends_stmt(a.expect_alterobjectdependsstmt(), b.expect_alterobjectdependsstmt()),
        (ntag::T_AlterObjectSchemaStmt, ntag::T_AlterObjectSchemaStmt) => equal_alter_object_schema_stmt(a.expect_alterobjectschemastmt(), b.expect_alterobjectschemastmt()),
        (ntag::T_AlterOperatorStmt, ntag::T_AlterOperatorStmt) => equal_alter_operator_stmt(a.expect_alteroperatorstmt(), b.expect_alteroperatorstmt()),
        (ntag::T_AlterOpFamilyStmt, ntag::T_AlterOpFamilyStmt) => equal_alter_op_family_stmt(a.expect_alteropfamilystmt(), b.expect_alteropfamilystmt()),
        (ntag::T_AlterOwnerStmt, ntag::T_AlterOwnerStmt) => equal_alter_owner_stmt(a.expect_alterownerstmt(), b.expect_alterownerstmt()),
        (ntag::T_AlterPolicyStmt, ntag::T_AlterPolicyStmt) => equal_alter_policy_stmt(a.expect_alterpolicystmt(), b.expect_alterpolicystmt()),
        (ntag::T_AlterPublicationStmt, ntag::T_AlterPublicationStmt) => equal_alter_publication_stmt(a.expect_alterpublicationstmt(), b.expect_alterpublicationstmt()),
        (ntag::T_AlterRoleSetStmt, ntag::T_AlterRoleSetStmt) => equal_alter_role_set_stmt(a.expect_alterrolesetstmt(), b.expect_alterrolesetstmt()),
        (ntag::T_AlterRoleStmt, ntag::T_AlterRoleStmt) => equal_alter_role_stmt(a.expect_alterrolestmt(), b.expect_alterrolestmt()),
        (ntag::T_AlterSeqStmt, ntag::T_AlterSeqStmt) => equal_alter_seq_stmt(a.expect_alterseqstmt(), b.expect_alterseqstmt()),
        (ntag::T_AlterStatsStmt, ntag::T_AlterStatsStmt) => equal_alter_stats_stmt(a.expect_alterstatsstmt(), b.expect_alterstatsstmt()),
        (ntag::T_AlterSubscriptionStmt, ntag::T_AlterSubscriptionStmt) => equal_alter_subscription_stmt(a.expect_altersubscriptionstmt(), b.expect_altersubscriptionstmt()),
        (ntag::T_AlterSystemStmt, ntag::T_AlterSystemStmt) => equal_alter_system_stmt(a.expect_altersystemstmt(), b.expect_altersystemstmt()),
        (ntag::T_AlterTableCmd, ntag::T_AlterTableCmd) => equal_alter_table_cmd(a.expect_altertablecmd(), b.expect_altertablecmd()),
        (ntag::T_AlterTableMoveAllStmt, ntag::T_AlterTableMoveAllStmt) => equal_alter_table_move_all_stmt(a.expect_altertablemoveallstmt(), b.expect_altertablemoveallstmt()),
        (ntag::T_AlterTableSpaceOptionsStmt, ntag::T_AlterTableSpaceOptionsStmt) => equal_alter_table_space_options_stmt(a.expect_altertablespaceoptionsstmt(), b.expect_altertablespaceoptionsstmt()),
        (ntag::T_AlterTableStmt, ntag::T_AlterTableStmt) => equal_alter_table_stmt(a.expect_altertablestmt(), b.expect_altertablestmt()),
        (ntag::T_AlterTSConfigurationStmt, ntag::T_AlterTSConfigurationStmt) => equal_alter_ts_configuration_stmt(a.expect_altertsconfigurationstmt(), b.expect_altertsconfigurationstmt()),
        (ntag::T_AlterTSDictionaryStmt, ntag::T_AlterTSDictionaryStmt) => equal_alter_ts_dictionary_stmt(a.expect_altertsdictionarystmt(), b.expect_altertsdictionarystmt()),
        (ntag::T_AlterTypeStmt, ntag::T_AlterTypeStmt) => equal_alter_type_stmt(a.expect_altertypestmt(), b.expect_altertypestmt()),
        (ntag::T_AlterUserMappingStmt, ntag::T_AlterUserMappingStmt) => equal_alter_user_mapping_stmt(a.expect_alterusermappingstmt(), b.expect_alterusermappingstmt()),
        (ntag::T_ATAlterConstraint, ntag::T_ATAlterConstraint) => equal_at_alter_constraint(a.expect_atalterconstraint(), b.expect_atalterconstraint()),
        (ntag::T_CompositeTypeStmt, ntag::T_CompositeTypeStmt) => equal_composite_type_stmt(a.expect_compositetypestmt(), b.expect_compositetypestmt()),
        (ntag::T_CreateCastStmt, ntag::T_CreateCastStmt) => equal_create_cast_stmt(a.expect_createcaststmt(), b.expect_createcaststmt()),
        (ntag::T_CreatedbStmt, ntag::T_CreatedbStmt) => equal_createdb_stmt(a.expect_createdbstmt(), b.expect_createdbstmt()),
        (ntag::T_CreateDomainStmt, ntag::T_CreateDomainStmt) => equal_create_domain_stmt(a.expect_createdomainstmt(), b.expect_createdomainstmt()),
        (ntag::T_CreateEnumStmt, ntag::T_CreateEnumStmt) => equal_create_enum_stmt(a.expect_createenumstmt(), b.expect_createenumstmt()),
        (ntag::T_CreateEventTrigStmt, ntag::T_CreateEventTrigStmt) => equal_create_event_trig_stmt(a.expect_createeventtrigstmt(), b.expect_createeventtrigstmt()),
        (ntag::T_CreateExtensionStmt, ntag::T_CreateExtensionStmt) => equal_create_extension_stmt(a.expect_createextensionstmt(), b.expect_createextensionstmt()),
        (ntag::T_CreateFdwStmt, ntag::T_CreateFdwStmt) => equal_create_fdw_stmt(a.expect_createfdwstmt(), b.expect_createfdwstmt()),
        (ntag::T_CreateForeignServerStmt, ntag::T_CreateForeignServerStmt) => equal_create_foreign_server_stmt(a.expect_createforeignserverstmt(), b.expect_createforeignserverstmt()),
        (ntag::T_CreateForeignTableStmt, ntag::T_CreateForeignTableStmt) => equal_create_foreign_table_stmt(a.expect_createforeigntablestmt(), b.expect_createforeigntablestmt()),
        (ntag::T_CreateFunctionStmt, ntag::T_CreateFunctionStmt) => equal_create_function_stmt(a.expect_createfunctionstmt(), b.expect_createfunctionstmt()),
        (ntag::T_CreateOpClassItem, ntag::T_CreateOpClassItem) => equal_create_op_class_item(a.expect_createopclassitem(), b.expect_createopclassitem()),
        (ntag::T_CreateOpClassStmt, ntag::T_CreateOpClassStmt) => equal_create_op_class_stmt(a.expect_createopclassstmt(), b.expect_createopclassstmt()),
        (ntag::T_CreateOpFamilyStmt, ntag::T_CreateOpFamilyStmt) => equal_create_op_family_stmt(a.expect_createopfamilystmt(), b.expect_createopfamilystmt()),
        (ntag::T_CreatePLangStmt, ntag::T_CreatePLangStmt) => equal_create_plang_stmt(a.expect_createplangstmt(), b.expect_createplangstmt()),
        (ntag::T_CreatePolicyStmt, ntag::T_CreatePolicyStmt) => equal_create_policy_stmt(a.expect_createpolicystmt(), b.expect_createpolicystmt()),
        (ntag::T_CreatePublicationStmt, ntag::T_CreatePublicationStmt) => equal_create_publication_stmt(a.expect_createpublicationstmt(), b.expect_createpublicationstmt()),
        (ntag::T_CreateRangeStmt, ntag::T_CreateRangeStmt) => equal_create_range_stmt(a.expect_createrangestmt(), b.expect_createrangestmt()),
        (ntag::T_CreateRoleStmt, ntag::T_CreateRoleStmt) => equal_create_role_stmt(a.expect_createrolestmt(), b.expect_createrolestmt()),
        (ntag::T_CreateSchemaStmt, ntag::T_CreateSchemaStmt) => equal_create_schema_stmt(a.expect_createschemastmt(), b.expect_createschemastmt()),
        (ntag::T_CreateSeqStmt, ntag::T_CreateSeqStmt) => equal_create_seq_stmt(a.expect_createseqstmt(), b.expect_createseqstmt()),
        (ntag::T_CreateStatsStmt, ntag::T_CreateStatsStmt) => equal_create_stats_stmt(a.expect_createstatsstmt(), b.expect_createstatsstmt()),
        (ntag::T_CreateStmt, ntag::T_CreateStmt) => equal_create_stmt(a.expect_createstmt(), b.expect_createstmt()),
        (ntag::T_CreateSubscriptionStmt, ntag::T_CreateSubscriptionStmt) => equal_create_subscription_stmt(a.expect_createsubscriptionstmt(), b.expect_createsubscriptionstmt()),
        (ntag::T_CreateTableAsStmt, ntag::T_CreateTableAsStmt) => equal_create_table_as_stmt(a.expect_createtableasstmt(), b.expect_createtableasstmt()),
        (ntag::T_CreateTableSpaceStmt, ntag::T_CreateTableSpaceStmt) => equal_create_table_space_stmt(a.expect_createtablespacestmt(), b.expect_createtablespacestmt()),
        (ntag::T_CreateTransformStmt, ntag::T_CreateTransformStmt) => equal_create_transform_stmt(a.expect_createtransformstmt(), b.expect_createtransformstmt()),
        (ntag::T_CreateTrigStmt, ntag::T_CreateTrigStmt) => equal_create_trig_stmt(a.expect_createtrigstmt(), b.expect_createtrigstmt()),
        (ntag::T_CreateUserMappingStmt, ntag::T_CreateUserMappingStmt) => equal_create_user_mapping_stmt(a.expect_createusermappingstmt(), b.expect_createusermappingstmt()),
        (ntag::T_CallStmt, ntag::T_CallStmt) => equal_call_stmt(a.expect_callstmt(), b.expect_callstmt()),
        (ntag::T_CheckPointStmt, ntag::T_CheckPointStmt) => equal_check_point_stmt(a.expect_checkpointstmt(), b.expect_checkpointstmt()),
        (ntag::T_ClosePortalStmt, ntag::T_ClosePortalStmt) => equal_close_portal_stmt(a.expect_closeportalstmt(), b.expect_closeportalstmt()),
        (ntag::T_ClusterStmt, ntag::T_ClusterStmt) => equal_cluster_stmt(a.expect_clusterstmt(), b.expect_clusterstmt()),
        (ntag::T_CommentStmt, ntag::T_CommentStmt) => equal_comment_stmt(a.expect_commentstmt(), b.expect_commentstmt()),
        (ntag::T_ConstraintsSetStmt, ntag::T_ConstraintsSetStmt) => equal_constraints_set_stmt(a.expect_constraintssetstmt(), b.expect_constraintssetstmt()),
        (ntag::T_CopyStmt, ntag::T_CopyStmt) => equal_copy_stmt(a.expect_copystmt(), b.expect_copystmt()),
        (ntag::T_Constraint, ntag::T_Constraint) => equal_constraint(a.expect_constraint(), b.expect_constraint()),
        (ntag::T_DeclareCursorStmt, ntag::T_DeclareCursorStmt) => equal_declare_cursor_stmt(a.expect_declarecursorstmt(), b.expect_declarecursorstmt()),
        (ntag::T_DefElem, ntag::T_DefElem) => equal_def_elem(a.expect_defelem(), b.expect_defelem()),
        (ntag::T_DefineStmt, ntag::T_DefineStmt) => equal_define_stmt(a.expect_definestmt(), b.expect_definestmt()),
        (ntag::T_DiscardStmt, ntag::T_DiscardStmt) => equal_discard_stmt(a.expect_discardstmt(), b.expect_discardstmt()),
        (ntag::T_DoStmt, ntag::T_DoStmt) => equal_do_stmt(a.expect_dostmt(), b.expect_dostmt()),
        (ntag::T_DropdbStmt, ntag::T_DropdbStmt) => equal_dropdb_stmt(a.expect_dropdbstmt(), b.expect_dropdbstmt()),
        (ntag::T_DropOwnedStmt, ntag::T_DropOwnedStmt) => equal_drop_owned_stmt(a.expect_dropownedstmt(), b.expect_dropownedstmt()),
        (ntag::T_DropRoleStmt, ntag::T_DropRoleStmt) => equal_drop_role_stmt(a.expect_droprolestmt(), b.expect_droprolestmt()),
        (ntag::T_DropStmt, ntag::T_DropStmt) => equal_drop_stmt(a.expect_dropstmt(), b.expect_dropstmt()),
        (ntag::T_DropSubscriptionStmt, ntag::T_DropSubscriptionStmt) => equal_drop_subscription_stmt(a.expect_dropsubscriptionstmt(), b.expect_dropsubscriptionstmt()),
        (ntag::T_DropTableSpaceStmt, ntag::T_DropTableSpaceStmt) => equal_drop_table_space_stmt(a.expect_droptablespacestmt(), b.expect_droptablespacestmt()),
        (ntag::T_DropUserMappingStmt, ntag::T_DropUserMappingStmt) => equal_drop_user_mapping_stmt(a.expect_dropusermappingstmt(), b.expect_dropusermappingstmt()),
        (ntag::T_ExecuteStmt, ntag::T_ExecuteStmt) => equal_execute_stmt(a.expect_executestmt(), b.expect_executestmt()),
        (ntag::T_ExplainStmt, ntag::T_ExplainStmt) => equal_explain_stmt(a.expect_explainstmt(), b.expect_explainstmt()),
        (ntag::T_FunctionParameter, ntag::T_FunctionParameter) => equal_function_parameter(a.expect_functionparameter(), b.expect_functionparameter()),
        (ntag::T_GrantRoleStmt, ntag::T_GrantRoleStmt) => equal_grant_role_stmt(a.expect_grantrolestmt(), b.expect_grantrolestmt()),
        (ntag::T_GrantStmt, ntag::T_GrantStmt) => equal_grant_stmt(a.expect_grantstmt(), b.expect_grantstmt()),
        (ntag::T_ImportForeignSchemaStmt, ntag::T_ImportForeignSchemaStmt) => equal_import_foreign_schema_stmt(a.expect_importforeignschemastmt(), b.expect_importforeignschemastmt()),
        (ntag::T_IndexStmt, ntag::T_IndexStmt) => equal_index_stmt(a.expect_indexstmt(), b.expect_indexstmt()),
        (ntag::T_IntoClause, ntag::T_IntoClause) => equal_into_clause(a.expect_intoclause(), b.expect_intoclause()),
        (ntag::T_ListenStmt, ntag::T_ListenStmt) => equal_listen_stmt(a.expect_listenstmt(), b.expect_listenstmt()),
        (ntag::T_LoadStmt, ntag::T_LoadStmt) => equal_load_stmt(a.expect_loadstmt(), b.expect_loadstmt()),
        (ntag::T_LockStmt, ntag::T_LockStmt) => equal_lock_stmt(a.expect_lockstmt(), b.expect_lockstmt()),
        (ntag::T_NotifyStmt, ntag::T_NotifyStmt) => equal_notify_stmt(a.expect_notifystmt(), b.expect_notifystmt()),
        (ntag::T_ObjectWithArgs, ntag::T_ObjectWithArgs) => equal_object_with_args(a.expect_objectwithargs(), b.expect_objectwithargs()),
        (ntag::T_PartitionBoundSpec, ntag::T_PartitionBoundSpec) => equal_partition_bound_spec(a.expect_partitionboundspec(), b.expect_partitionboundspec()),
        (ntag::T_PartitionCmd, ntag::T_PartitionCmd) => equal_partition_cmd(a.expect_partitioncmd(), b.expect_partitioncmd()),
        (ntag::T_PartitionElem, ntag::T_PartitionElem) => equal_partition_elem(a.expect_partitionelem(), b.expect_partitionelem()),
        (ntag::T_PartitionRangeDatum, ntag::T_PartitionRangeDatum) => equal_partition_range_datum(a.expect_partitionrangedatum(), b.expect_partitionrangedatum()),
        (ntag::T_PartitionSpec, ntag::T_PartitionSpec) => equal_partition_spec(a.expect_partitionspec(), b.expect_partitionspec()),
        (ntag::T_PLAssignStmt, ntag::T_PLAssignStmt) => equal_pl_assign_stmt(a.expect_plassignstmt(), b.expect_plassignstmt()),
        (ntag::T_PublicationObjSpec, ntag::T_PublicationObjSpec) => equal_publication_obj_spec(a.expect_publicationobjspec(), b.expect_publicationobjspec()),
        (ntag::T_PublicationTable, ntag::T_PublicationTable) => equal_publication_table(a.expect_publicationtable(), b.expect_publicationtable()),
        (ntag::T_ReassignOwnedStmt, ntag::T_ReassignOwnedStmt) => equal_reassign_owned_stmt(a.expect_reassignownedstmt(), b.expect_reassignownedstmt()),
        (ntag::T_RefreshMatViewStmt, ntag::T_RefreshMatViewStmt) => equal_refresh_mat_view_stmt(a.expect_refreshmatviewstmt(), b.expect_refreshmatviewstmt()),
        (ntag::T_ReindexStmt, ntag::T_ReindexStmt) => equal_reindex_stmt(a.expect_reindexstmt(), b.expect_reindexstmt()),
        (ntag::T_RenameStmt, ntag::T_RenameStmt) => equal_rename_stmt(a.expect_renamestmt(), b.expect_renamestmt()),
        (ntag::T_ReplicaIdentityStmt, ntag::T_ReplicaIdentityStmt) => equal_replica_identity_stmt(a.expect_replicaidentitystmt(), b.expect_replicaidentitystmt()),
        (ntag::T_ReturnStmt, ntag::T_ReturnStmt) => equal_return_stmt(a.expect_returnstmt(), b.expect_returnstmt()),
        (ntag::T_RuleStmt, ntag::T_RuleStmt) => equal_rule_stmt(a.expect_rulestmt(), b.expect_rulestmt()),
        (ntag::T_SecLabelStmt, ntag::T_SecLabelStmt) => equal_sec_label_stmt(a.expect_seclabelstmt(), b.expect_seclabelstmt()),
        (ntag::T_StatsElem, ntag::T_StatsElem) => equal_stats_elem(a.expect_statselem(), b.expect_statselem()),
        (ntag::T_TableLikeClause, ntag::T_TableLikeClause) => equal_table_like_clause(a.expect_tablelikeclause(), b.expect_tablelikeclause()),
        (ntag::T_TransactionStmt, ntag::T_TransactionStmt) => equal_transaction_stmt(a.expect_transactionstmt(), b.expect_transactionstmt()),
        (ntag::T_TruncateStmt, ntag::T_TruncateStmt) => equal_truncate_stmt(a.expect_truncatestmt(), b.expect_truncatestmt()),
        (ntag::T_UnlistenStmt, ntag::T_UnlistenStmt) => equal_unlisten_stmt(a.expect_unlistenstmt(), b.expect_unlistenstmt()),
        (ntag::T_VacuumRelation, ntag::T_VacuumRelation) => equal_vacuum_relation(a.expect_vacuumrelation(), b.expect_vacuumrelation()),
        (ntag::T_VacuumStmt, ntag::T_VacuumStmt) => equal_vacuum_stmt(a.expect_vacuumstmt(), b.expect_vacuumstmt()),
        (ntag::T_VariableSetStmt, ntag::T_VariableSetStmt) => equal_variable_set_stmt(a.expect_variablesetstmt(), b.expect_variablesetstmt()),
        (ntag::T_VariableShowStmt, ntag::T_VariableShowStmt) => equal_variable_show_stmt(a.expect_variableshowstmt(), b.expect_variableshowstmt()),
        (ntag::T_ViewStmt, ntag::T_ViewStmt) => equal_view_stmt(a.expect_viewstmt(), b.expect_viewstmt()),
        (ntag::T_RangeVar, ntag::T_RangeVar) => equal_range_var(a.expect_rangevar(), b.expect_rangevar()),
        (ntag::T_ColumnDef, ntag::T_ColumnDef) => equal_column_def(a.expect_columndef(), b.expect_columndef()),
        (ntag::T_SelectStmt, ntag::T_SelectStmt) => equal_select_stmt(a.expect_selectstmt(), b.expect_selectstmt()),
        (ntag::T_InsertStmt, ntag::T_InsertStmt) => equal_insert_stmt(a.expect_insertstmt(), b.expect_insertstmt()),
        (ntag::T_UpdateStmt, ntag::T_UpdateStmt) => equal_update_stmt(a.expect_updatestmt(), b.expect_updatestmt()),
        (ntag::T_DeleteStmt, ntag::T_DeleteStmt) => equal_delete_stmt(a.expect_deletestmt(), b.expect_deletestmt()),
        (ntag::T_MergeStmt, ntag::T_MergeStmt) => equal_merge_stmt(a.expect_mergestmt(), b.expect_mergestmt()),
        (ntag::T_MergeWhenClause, ntag::T_MergeWhenClause) => equal_merge_when_clause(a.expect_mergewhenclause(), b.expect_mergewhenclause()),
        (ntag::T_RangeFunction, ntag::T_RangeFunction) => equal_range_function(a.expect_rangefunction(), b.expect_rangefunction()),
        (ntag::T_RangeSubselect, ntag::T_RangeSubselect) => equal_range_subselect(a.expect_rangesubselect(), b.expect_rangesubselect()),
        (ntag::T_RangeTableFunc, ntag::T_RangeTableFunc) => equal_range_table_func(a.expect_rangetablefunc(), b.expect_rangetablefunc()),
        (ntag::T_RangeTableFuncCol, ntag::T_RangeTableFuncCol) => equal_range_table_func_col(a.expect_rangetablefunccol(), b.expect_rangetablefunccol()),
        (ntag::T_RangeTableSample, ntag::T_RangeTableSample) => equal_range_table_sample(a.expect_rangetablesample(), b.expect_rangetablesample()),
        (ntag::T_InferClause, ntag::T_InferClause) => equal_infer_clause(a.expect_inferclause(), b.expect_inferclause()),
        (ntag::T_OnConflictClause, ntag::T_OnConflictClause) => equal_on_conflict_clause(a.expect_onconflictclause(), b.expect_onconflictclause()),
        (ntag::T_LockingClause, ntag::T_LockingClause) => equal_locking_clause(a.expect_lockingclause(), b.expect_lockingclause()),
        (ntag::T_WithClause, ntag::T_WithClause) => equal_with_clause(a.expect_withclause(), b.expect_withclause()),
        (ntag::T_TableSampleClause, ntag::T_TableSampleClause) => equal_table_sample_clause(a.expect_tablesampleclause(), b.expect_tablesampleclause()),
        (ntag::T_ReturningClause, ntag::T_ReturningClause) => equal_returning_clause(a.expect_returningclause(), b.expect_returningclause()),
        (ntag::T_ReturningOption, ntag::T_ReturningOption) => equal_returning_option(a.expect_returningoption(), b.expect_returningoption()),
        (ntag::T_JsonBehavior, ntag::T_JsonBehavior) => equal_json_behavior_raw(a.expect_jsonbehavior(), b.expect_jsonbehavior()),
        (ntag::T_JsonOutput, ntag::T_JsonOutput) => equal_json_output(a.expect_jsonoutput(), b.expect_jsonoutput()),
        (ntag::T_JsonArgument, ntag::T_JsonArgument) => equal_json_argument(a.expect_jsonargument(), b.expect_jsonargument()),
        (ntag::T_JsonFuncExpr, ntag::T_JsonFuncExpr) => equal_json_func_expr(a.expect_jsonfuncexpr(), b.expect_jsonfuncexpr()),
        (ntag::T_JsonTablePathSpec, ntag::T_JsonTablePathSpec) => equal_json_table_path_spec(a.expect_jsontablepathspec(), b.expect_jsontablepathspec()),
        (ntag::T_JsonTable, ntag::T_JsonTable) => equal_json_table(a.expect_jsontable(), b.expect_jsontable()),
        (ntag::T_JsonTableColumn, ntag::T_JsonTableColumn) => equal_json_table_column(a.expect_jsontablecolumn(), b.expect_jsontablecolumn()),
        (ntag::T_JsonKeyValue, ntag::T_JsonKeyValue) => equal_json_key_value(a.expect_jsonkeyvalue(), b.expect_jsonkeyvalue()),
        (ntag::T_JsonParseExpr, ntag::T_JsonParseExpr) => equal_json_parse_expr(a.expect_jsonparseexpr(), b.expect_jsonparseexpr()),
        (ntag::T_JsonScalarExpr, ntag::T_JsonScalarExpr) => equal_json_scalar_expr(a.expect_jsonscalarexpr(), b.expect_jsonscalarexpr()),
        (ntag::T_JsonSerializeExpr, ntag::T_JsonSerializeExpr) => equal_json_serialize_expr(a.expect_jsonserializeexpr(), b.expect_jsonserializeexpr()),
        (ntag::T_JsonObjectConstructor, ntag::T_JsonObjectConstructor) => equal_json_object_constructor(a.expect_jsonobjectconstructor(), b.expect_jsonobjectconstructor()),
        (ntag::T_JsonArrayConstructor, ntag::T_JsonArrayConstructor) => equal_json_array_constructor(a.expect_jsonarrayconstructor(), b.expect_jsonarrayconstructor()),
        (ntag::T_JsonArrayQueryConstructor, ntag::T_JsonArrayQueryConstructor) => equal_json_array_query_constructor(a.expect_jsonarrayqueryconstructor(), b.expect_jsonarrayqueryconstructor()),
        (ntag::T_JsonAggConstructor, ntag::T_JsonAggConstructor) => equal_json_agg_constructor(a.expect_jsonaggconstructor(), b.expect_jsonaggconstructor()),
        (ntag::T_JsonObjectAgg, ntag::T_JsonObjectAgg) => equal_json_object_agg(a.expect_jsonobjectagg(), b.expect_jsonobjectagg()),
        (ntag::T_JsonArrayAgg, ntag::T_JsonArrayAgg) => equal_json_array_agg(a.expect_jsonarrayagg(), b.expect_jsonarrayagg()),
        (ntag::T_CreateAmStmt, ntag::T_CreateAmStmt) => equal_create_am_stmt(a.expect_createamstmt(), b.expect_createamstmt()),
        (ntag::T_CreateConversionStmt, ntag::T_CreateConversionStmt) => equal_create_conversion_stmt(a.expect_createconversionstmt(), b.expect_createconversionstmt()),
        (ntag::T_DeallocateStmt, ntag::T_DeallocateStmt) => equal_deallocate_stmt(a.expect_deallocatestmt(), b.expect_deallocatestmt()),
        (ntag::T_PrepareStmt, ntag::T_PrepareStmt) => equal_prepare_stmt(a.expect_preparestmt(), b.expect_preparestmt()),
        (ntag::T_FetchStmt, ntag::T_FetchStmt) => equal_fetch_stmt(a.expect_fetchstmt(), b.expect_fetchstmt()),
        (ntag::T_RoleSpec, ntag::T_RoleSpec) => equal_role_spec(a.expect_rolespec(), b.expect_rolespec()),
        // Different tags are never equal.
        (ta, tb) if ta != tb => false,
        // Same-tag node family not yet reachable through equal() in the ported
        // (prep/parse) layer. Mirrors equalfuncs.c's behaviour of having a
        // comparator per node type: when a consumer first feeds one of these,
        // its `_equalXxx` gets ported here (seam-and-panic until then, never a
        // silent wrong answer).
        _ => panic!(
            "equalfuncs: equal() not yet ported for node type {:?}",
            a.node_tag()
        ),
    }
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install the central `equal()` seams owned by this unit. Called once at
/// single-threaded startup from `seams-init`.
/// Node-opaque flip seam (`types_nodes::opaque_node::node_equal_seam`): the
/// installable per-payload equality comparator that the *gated* generated
/// `NodePayload::equal_dyn` bodies route through (node-opaque P3 codegen, behind
/// the off-by-default `node_payload_codegen` feature). The generated `equal_dyn`
/// has already tag-checked both sides equal and handed us the shared
/// [`NodeTag`] plus the two `repr(transparent)` payload data pointers
/// (`__payload_ptr()`) — exactly the witnesses C's `equal()` dispatch uses.
///
/// This adapter reconstructs the typed payload refs from those pointers and
/// calls the *same* per-payload comparators `equal_node` already uses. For the
/// whole `Expr` family the payload is the nested [`Expr`] enum (the `Expr`
/// adapter wraps `Expr`, with `node_tag() == Expr::expr_tag()`), so any
/// Expr-leaf tag routes through `equal_expr`. The non-Expr raw/parse arms each
/// transmute to their typed payload struct and call their `_equalXxx` port.
///
/// SAFETY: the gated caller guarantees both pointers address a live payload of
/// the variant named by `tag` (the tag<->adapter bijection, codegen §1.3), and
/// `repr(transparent)` puts the payload at the adapter's address. The lifetime
/// is erased to `'_` here (the gated trait fixes a single shared `'mcx`), which
/// is sound because every comparator only *reads* through the refs.
///
/// In the normal (un-gated) build NOTHING calls `node_equal_seam`, so this is
/// dead but harmless: installing it is a verified no-op for the live `Node`
/// enum. It NEVER fabricates an answer — an un-ported same-tag family panics
/// exactly as `equal_node`'s tail arm does.
fn node_equal_seam_adapter(
    tag: types_nodes::nodes::NodeTag,
    a: *const (),
    b: *const (),
) -> bool {
    use types_nodes::rawnodes;
    // Helper: reborrow a payload pointer as `&T` (single shared erased lifetime).
    // SAFETY (per call site): the gated caller passes a `__payload_ptr()` to a
    // live `T`-shaped payload selected by `tag`.
    macro_rules! p {
        ($ptr:expr, $T:ty) => {
            unsafe { &*($ptr as *const $T) }
        };
    }
    match tag {
        // The entire `Expr *` discriminated union: payload is the `Expr` enum.
        // Any Expr-leaf tag (T_Var, T_OpExpr, ...) lands here.
        ntag::T_Var
        | ntag::T_Const
        | ntag::T_Param
        | ntag::T_Aggref
        | ntag::T_GroupingFunc
        | ntag::T_WindowFunc
        | ntag::T_SubscriptingRef
        | ntag::T_FuncExpr
        | ntag::T_NamedArgExpr
        | ntag::T_OpExpr
        | ntag::T_DistinctExpr
        | ntag::T_NullIfExpr
        | ntag::T_ScalarArrayOpExpr
        | ntag::T_BoolExpr
        | ntag::T_SubLink
        | ntag::T_SubPlan
        | ntag::T_AlternativeSubPlan
        | ntag::T_FieldSelect
        | ntag::T_FieldStore
        | ntag::T_RelabelType
        | ntag::T_CoerceViaIO
        | ntag::T_ArrayCoerceExpr
        | ntag::T_ConvertRowtypeExpr
        | ntag::T_CollateExpr
        | ntag::T_CaseExpr
        | ntag::T_CaseWhen
        | ntag::T_CaseTestExpr
        | ntag::T_ArrayExpr
        | ntag::T_RowExpr
        | ntag::T_RowCompareExpr
        | ntag::T_CoalesceExpr
        | ntag::T_MinMaxExpr
        | ntag::T_SQLValueFunction
        | ntag::T_XmlExpr
        | ntag::T_JsonValueExpr
        | ntag::T_JsonConstructorExpr
        | ntag::T_JsonIsPredicate
        | ntag::T_JsonExpr
        | ntag::T_NullTest
        | ntag::T_BooleanTest
        | ntag::T_MergeSupportFunc
        | ntag::T_CoerceToDomain
        | ntag::T_CoerceToDomainValue
        | ntag::T_SetToDefault
        | ntag::T_CurrentOfExpr
        | ntag::T_NextValueExpr
        | ntag::T_InferenceElem
        | ntag::T_PlaceHolderVar
        | ntag::T_ReturningExpr => equal_expr(p!(a, Expr), p!(b, Expr)),

        // Value leaves (`#[derive(PgNode)]` per-struct comparator).
        ntag::T_Integer => p!(a, types_nodes::value::Integer)
            .equal_node(p!(b, types_nodes::value::Integer)),
        ntag::T_Float => {
            p!(a, types_nodes::value::Float).equal_node(p!(b, types_nodes::value::Float))
        }
        ntag::T_Boolean => p!(a, types_nodes::value::Boolean)
            .equal_node(p!(b, types_nodes::value::Boolean)),
        ntag::T_String => p!(a, types_nodes::value::StringNode)
            .equal_node(p!(b, types_nodes::value::StringNode)),
        ntag::T_BitString => p!(a, types_nodes::value::BitString)
            .equal_node(p!(b, types_nodes::value::BitString)),

        // Query-tree / parse / rewrite node family — the exact comparators
        // `equal_node`'s switch calls, reached here by typed payload ref.
        ntag::T_TargetEntry => {
            equal_target_entry(p!(a, TargetEntry), p!(b, TargetEntry))
        }
        ntag::T_TableFunc => equal_table_func(
            p!(a, types_nodes::primnodes::TableFunc),
            p!(b, types_nodes::primnodes::TableFunc),
        ),
        ntag::T_CTECycleClause => equal_cte_cycle_clause(
            p!(a, rawnodes::CTECycleClause),
            p!(b, rawnodes::CTECycleClause),
        ),
        ntag::T_SortGroupClause => {
            equal_sort_group_clause(p!(a, SortGroupClause), p!(b, SortGroupClause))
        }
        ntag::T_Query => equal_query(
            p!(a, types_nodes::copy_query::Query),
            p!(b, types_nodes::copy_query::Query),
        ),
        ntag::T_RangeTblEntry => equal_range_tbl_entry(
            p!(a, types_nodes::parsenodes::RangeTblEntry),
            p!(b, types_nodes::parsenodes::RangeTblEntry),
        ),
        ntag::T_RTEPermissionInfo => equal_rte_permission_info(
            p!(a, types_nodes::parsenodes::RTEPermissionInfo),
            p!(b, types_nodes::parsenodes::RTEPermissionInfo),
        ),
        ntag::T_RangeTblFunction => equal_range_tbl_function(
            p!(a, rawnodes::RangeTblFunction),
            p!(b, rawnodes::RangeTblFunction),
        ),
        ntag::T_RangeTblRef => {
            equal_range_tbl_ref(p!(a, rawnodes::RangeTblRef), p!(b, rawnodes::RangeTblRef))
        }
        ntag::T_FromExpr => {
            equal_from_expr(p!(a, rawnodes::FromExpr), p!(b, rawnodes::FromExpr))
        }
        ntag::T_JoinExpr => {
            equal_join_expr(p!(a, rawnodes::JoinExpr), p!(b, rawnodes::JoinExpr))
        }
        ntag::T_OnConflictExpr => equal_on_conflict_expr(
            p!(a, rawnodes::OnConflictExpr),
            p!(b, rawnodes::OnConflictExpr),
        ),
        ntag::T_MergeAction => {
            equal_merge_action(p!(a, rawnodes::MergeAction), p!(b, rawnodes::MergeAction))
        }
        ntag::T_GroupingSet => {
            equal_grouping_set(p!(a, rawnodes::GroupingSet), p!(b, rawnodes::GroupingSet))
        }
        ntag::T_WindowClause => {
            equal_window_clause(p!(a, rawnodes::WindowClause), p!(b, rawnodes::WindowClause))
        }
        ntag::T_RowMarkClause => equal_row_mark_clause(
            p!(a, rawnodes::RowMarkClause),
            p!(b, rawnodes::RowMarkClause),
        ),
        ntag::T_WithCheckOption => equal_with_check_option(
            p!(a, rawnodes::WithCheckOption),
            p!(b, rawnodes::WithCheckOption),
        ),
        ntag::T_CommonTableExpr => equal_common_table_expr(
            p!(a, rawnodes::CommonTableExpr),
            p!(b, rawnodes::CommonTableExpr),
        ),
        ntag::T_SetOperationStmt => equal_set_operation_stmt(
            p!(a, rawnodes::SetOperationStmt),
            p!(b, rawnodes::SetOperationStmt),
        ),
        ntag::T_Alias => equal_alias(p!(a, rawnodes::Alias), p!(b, rawnodes::Alias)),
        ntag::T_ColumnRef => {
            equal_column_ref(p!(a, rawnodes::ColumnRef), p!(b, rawnodes::ColumnRef))
        }
        ntag::T_ParamRef => {
            equal_param_ref(p!(a, rawnodes::ParamRef), p!(b, rawnodes::ParamRef))
        }
        ntag::T_A_Expr => equal_a_expr(p!(a, rawnodes::A_Expr), p!(b, rawnodes::A_Expr)),
        ntag::T_A_Const => {
            equal_a_const(p!(a, rawnodes::A_Const), p!(b, rawnodes::A_Const))
        }
        ntag::T_FuncCall => {
            equal_func_call(p!(a, rawnodes::FuncCall), p!(b, rawnodes::FuncCall))
        }
        ntag::T_A_Star => equal_a_star(p!(a, rawnodes::A_Star), p!(b, rawnodes::A_Star)),
        ntag::T_A_Indices => {
            equal_a_indices(p!(a, rawnodes::A_Indices), p!(b, rawnodes::A_Indices))
        }
        ntag::T_A_Indirection => equal_a_indirection(
            p!(a, rawnodes::A_Indirection),
            p!(b, rawnodes::A_Indirection),
        ),
        ntag::T_A_ArrayExpr => {
            equal_a_array_expr(p!(a, rawnodes::A_ArrayExpr), p!(b, rawnodes::A_ArrayExpr))
        }
        ntag::T_TypeName => {
            equal_type_name(p!(a, rawnodes::TypeName), p!(b, rawnodes::TypeName))
        }
        ntag::T_TypeCast => {
            equal_type_cast(p!(a, rawnodes::TypeCast), p!(b, rawnodes::TypeCast))
        }
        ntag::T_CollateClause => equal_collate_clause(
            p!(a, rawnodes::CollateClause),
            p!(b, rawnodes::CollateClause),
        ),
        ntag::T_ResTarget => {
            equal_res_target(p!(a, rawnodes::ResTarget), p!(b, rawnodes::ResTarget))
        }
        ntag::T_MultiAssignRef => equal_multi_assign_ref(
            p!(a, rawnodes::MultiAssignRef),
            p!(b, rawnodes::MultiAssignRef),
        ),
        ntag::T_SortBy => equal_sort_by(p!(a, rawnodes::SortBy), p!(b, rawnodes::SortBy)),
        ntag::T_WindowDef => {
            equal_window_def(p!(a, rawnodes::WindowDef), p!(b, rawnodes::WindowDef))
        }
        // Same-tag node family not yet reachable through equal() in the ported
        // layer — mirror `equal_node`'s tail: seam-and-panic, never a silent
        // wrong answer.
        _ => panic!(
            "equalfuncs: node_equal_seam not yet ported for node type {:?}",
            tag
        ),
    }
}

pub fn init_seams() {
    backend_nodes_equalfuncs_seams::equal_expr::set(equal_expr);
    backend_nodes_equalfuncs_seams::equal_node::set(equal_node);

    // Node-opaque flip seam: the gated `NodePayload::equal_dyn` comparator entry
    // (consumed only under the off-by-default `node_payload_codegen` feature).
    // Installing it now is an additive, verified no-op for the live `Node` enum
    // (nothing in the normal build calls `node_equal_seam`).
    types_nodes::opaque_node::install_node_equal_seam(node_equal_seam_adapter);
    backend_nodes_equalfuncs_seams::equal_expr_list::set(equal_expr_list_impl);
    backend_nodes_equalfuncs_seams::equal_targetentry_list::set(equal_targetentry_list_impl);
    backend_nodes_equalfuncs_seams::equal_sortgroupclause_list::set(equal_sortgroupclause_list_impl);

    // equivclass-ext cycle-break leg owned by equalfuncs.c: `equal(a, b)` over
    // two owned `&Expr` (equivclass.c `process_equivalence` /
    // `get_eclass_for_sort_expr` member matching). Same impl as the
    // `equal_expr` seam.
    backend_optimizer_path_equivclass_ext_seams::equal::set(equal_expr);

    // `equal(a, b)` over two owned `&Expr` (nodeFuncs.h `equal()`) — the
    // pathkeys.c partition-pruning / `targetIsInAllPartitionLists` and
    // subquery-pathkey-conversion leaves reach it. Same `equal_expr` impl;
    // declared in nodeFuncs-seams, owned by equalfuncs.c.
    backend_nodes_nodeFuncs_seams::equal::set(equal_expr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_nodes::primnodes::{Const, OpExpr, Var};
    use types_tuple::backend_access_common_heaptuple::Datum;

    fn var(varno: i32, varattno: i16) -> Expr {
        Expr::Var(Var {
            varno,
            varattno,
            ..Var::default()
        })
    }

    fn op(opno: u32, args: Vec<Expr>) -> Expr {
        Expr::OpExpr(OpExpr {
            opno,
            args,
            ..OpExpr::default()
        })
    }

    #[test]
    fn different_variants_are_unequal() {
        let v = var(1, 1);
        let c = Expr::Const(Const::default());
        assert!(!equal_expr(&v, &c));
    }

    #[test]
    fn equal_vars_match_and_differ() {
        assert!(equal_expr(&var(1, 2), &var(1, 2)));
        assert!(!equal_expr(&var(1, 2), &var(1, 3)));
        assert!(!equal_expr(&var(2, 2), &var(1, 2)));
    }

    #[test]
    fn opexpr_recurses_into_args() {
        let a = op(96, vec![var(1, 1), var(1, 2)]);
        let b = op(96, vec![var(1, 1), var(1, 2)]);
        let c = op(96, vec![var(1, 1), var(1, 3)]); // child differs
        let d = op(97, vec![var(1, 1), var(1, 2)]); // opno differs
        assert!(equal_expr(&a, &b));
        assert!(!equal_expr(&a, &c));
        assert!(!equal_expr(&a, &d));
    }

    #[test]
    fn opexpr_opfuncid_zero_is_ignored() {
        // _equalOpExpr: opfuncid not compared if either side is unset (0).
        let a = Expr::OpExpr(OpExpr {
            opno: 96,
            opfuncid: 0,
            ..OpExpr::default()
        });
        let b = Expr::OpExpr(OpExpr {
            opno: 96,
            opfuncid: 1234,
            ..OpExpr::default()
        });
        assert!(equal_expr(&a, &b), "opfuncid==0 on one side must be ignored");

        // Both set but different => unequal.
        let c = Expr::OpExpr(OpExpr {
            opno: 96,
            opfuncid: 5678,
            ..OpExpr::default()
        });
        assert!(!equal_expr(&b, &c));
    }

    #[test]
    fn const_null_of_same_type_is_equal() {
        // "We treat all NULL constants of the same type as equal."
        let a = Expr::Const(Const {
            consttype: 23,
            constisnull: true,
            constvalue: Datum::ByVal(111),
            ..Const::default()
        });
        let b = Expr::Const(Const {
            consttype: 23,
            constisnull: true,
            constvalue: Datum::ByVal(222),
            ..Const::default()
        });
        assert!(equal_expr(&a, &b));

        // Different type => unequal even when null.
        let c = Expr::Const(Const {
            consttype: 25,
            constisnull: true,
            ..Const::default()
        });
        assert!(!equal_expr(&a, &c));
    }

    #[test]
    fn const_non_null_compares_value() {
        let a = Expr::Const(Const {
            consttype: 23,
            constisnull: false,
            constvalue: Datum::ByVal(42),
            ..Const::default()
        });
        let b = Expr::Const(Const {
            consttype: 23,
            constisnull: false,
            constvalue: Datum::ByVal(42),
            ..Const::default()
        });
        let c = Expr::Const(Const {
            consttype: 23,
            constisnull: false,
            constvalue: Datum::ByVal(43),
            ..Const::default()
        });
        assert!(equal_expr(&a, &b));
        assert!(!equal_expr(&a, &c));
    }

    #[test]
    fn place_holder_var_compares_id_levelsup_nullingrels() {
        use types_nodes::primnodes::PlaceHolderVar;
        let mk = |phid: u32, phlevelsup: u32| {
            Expr::PlaceHolderVar(PlaceHolderVar {
                phexpr: None,
                phrels: Default::default(),
                phnullingrels: Default::default(),
                phid,
                phlevelsup,
            })
        };
        assert!(equal_expr(&mk(3, 0), &mk(3, 0)));
        assert!(!equal_expr(&mk(3, 0), &mk(4, 0)));
        assert!(!equal_expr(&mk(3, 0), &mk(3, 1)));
    }

    #[test]
    fn place_holder_var_ignores_phexpr_and_phrels() {
        // phexpr and phrels are equal_ignore in equalfuncs.c; differing values
        // must not make two PHVs unequal.
        use types_nodes::primnodes::PlaceHolderVar;
        let a = Expr::PlaceHolderVar(PlaceHolderVar {
            phexpr: Some(Box::new(var(1, 1))),
            phrels: Default::default(),
            phnullingrels: Default::default(),
            phid: 7,
            phlevelsup: 0,
        });
        let b = Expr::PlaceHolderVar(PlaceHolderVar {
            phexpr: None,
            phrels: Default::default(),
            phnullingrels: Default::default(),
            phid: 7,
            phlevelsup: 0,
        });
        assert!(equal_expr(&a, &b));
    }

    #[test]
    fn list_seams_compare_elementwise() {
        let a = vec![var(1, 1), var(1, 2)];
        let b = vec![var(1, 1), var(1, 2)];
        let c = vec![var(1, 1)];
        assert!(equal_expr_list_impl(&a, &b));
        assert!(!equal_expr_list_impl(&a, &c)); // length differs
    }
}
