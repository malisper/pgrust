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
use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    Aggref, ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen,
    CoalesceExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO, CollateExpr, Const,
    ConvertRowtypeExpr, CurrentOfExpr, Expr, FieldSelect, FieldStore, FuncExpr, GroupingFunc,
    InferenceElem, JsonConstructorExpr, JsonExpr, JsonIsPredicate, JsonValueExpr, MergeSupportFunc,
    MinMaxExpr, NamedArgExpr, NextValueExpr, NullTest, OpExpr, Param, RelabelType, ReturningExpr,
    RowCompareExpr, RowExpr, SQLValueFunction, ScalarArrayOpExpr, SetToDefault, SubLink,
    SubscriptingRef, TargetEntry, Var, WindowFunc, XmlExpr,
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
    match (a, b) {
        (Some(x), Some(y)) => x == y,
        (None, None) => true,
        _ => false,
    }
}

/// `COMPARE_NODE_FIELD` over an optional child `Expr` (`Expr *`, NULL-able):
/// both NULL is equal; one NULL is unequal; else recurse.
#[inline]
fn equal_opt_expr(a: Option<&Expr>, b: Option<&Expr>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_expr(x, y),
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_query(x, y),
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_format(x, y),
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_returning(x, y),
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_json_behavior(x, y),
        _ => false,
    }
}

fn equal_json_value_expr(a: &JsonValueExpr, b: &JsonValueExpr) -> bool {
    equal_opt_expr(a.raw_expr.as_deref(), b.raw_expr.as_deref())
        && equal_opt_expr(a.formatted_expr.as_deref(), b.formatted_expr.as_deref())
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_node(x, y),
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.words == y.words,
        _ => false,
    }
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
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_alias(x, y),
        _ => false,
    }
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
    match (a.as_ref(), b.as_ref()) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            let (x, y) = (x.as_ref(), y.as_ref());
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| eq(p, q))
        }
        _ => false,
    }
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
        && a.ordinalitycol == b.ordinalitycol
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
        && equal_opt_node(a.mergeJoinCondition.as_ref(), b.mergeJoinCondition.as_ref())
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
        && equal_opt_node(a.havingQual.as_ref(), b.havingQual.as_ref())
        && equal_node_list(&a.windowClause, &b.windowClause)
        && equal_node_list(&a.distinctClause, &b.distinctClause)
        && equal_node_list(&a.sortClause, &b.sortClause)
        && equal_opt_node(a.limitOffset.as_ref(), b.limitOffset.as_ref())
        && equal_opt_node(a.limitCount.as_ref(), b.limitCount.as_ref())
        && a.limitOption == b.limitOption
        && equal_node_list(&a.rowMarks, &b.rowMarks)
        && equal_opt_node(a.setOperations.as_ref(), b.setOperations.as_ref())
        && a.constraintDeps == b.constraintDeps
        && equal_node_list(&a.withCheckOptions, &b.withCheckOptions)
    // stmt_location / stmt_len are COMPARE_LOCATION_FIELD (no-ops).
}

// ===========================================================================
// equal() — the central tag-discriminated dispatch
// ===========================================================================

/// `equal(a, b)` over two `Expr *`: the `equalfuncs.c` switch restricted to the
/// `Expr`-derived node universe. Two different variants (`nodeTag` mismatch) are
/// never equal; same-variant nodes are compared by their `_equalXxx`.
pub fn equal_expr(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
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
        // SubPlan / AlternativeSubPlan / PlaceHolderVar carry context-allocated
        // planner children (`SubPlanExpr`/`AlternativeSubPlanExpr` clone panics
        // by design) and are not de-duplicated by equal() in the prep/parse
        // layer; reaching equal() on them is a not-yet-ported boundary.
        (Expr::SubPlan(_), Expr::SubPlan(_))
        | (Expr::AlternativeSubPlan(_), Expr::AlternativeSubPlan(_))
        | (Expr::PlaceHolderVar(_), Expr::PlaceHolderVar(_)) => {
            panic!(
                "equalfuncs: equal() not yet ported for planner-internal Expr \
                 variant (SubPlan/AlternativeSubPlan/PlaceHolderVar)"
            )
        }
        // Different tags are never equal (the `nodeTag(a) != nodeTag(b)` early
        // return in equal()).
        _ => false,
    }
}

/// `equal(a, b)` over two general `Node *`: the full `equalfuncs.c` switch. The
/// `a == b` / one-NULL early returns are the caller's concern (Rust references
/// are always non-null); the `nodeTag(a) != nodeTag(b)` rule is the
/// different-variant `_ => false` arms.
pub fn equal_node(a: &Node<'_>, b: &Node<'_>) -> bool {
    match (a, b) {
        (Node::Expr(x), Node::Expr(y)) => equal_expr(x, y),
        (Node::TargetEntry(x), Node::TargetEntry(y)) => equal_target_entry(x, y),
        (Node::TableFunc(x), Node::TableFunc(y)) => equal_table_func(x, y),
        (Node::CTECycleClause(x), Node::CTECycleClause(y)) => equal_cte_cycle_clause(x, y),
        (Node::SortGroupClause(x), Node::SortGroupClause(y)) => equal_sort_group_clause(x, y),
        // The Value leaf nodes (`_equalInteger`/`_equalFloat`/`_equalBoolean`/
        // `_equalString`/`_equalBitString`) compare by their single value field;
        // the `#[derive(PgNode)]`-generated `PgNodeEqual::equal_node` IS that
        // faithful per-struct comparator.
        (Node::Integer(x), Node::Integer(y)) => x.equal_node(y),
        (Node::Float(x), Node::Float(y)) => x.equal_node(y),
        (Node::Boolean(x), Node::Boolean(y)) => x.equal_node(y),
        (Node::String(x), Node::String(y)) => x.equal_node(y),
        (Node::BitString(x), Node::BitString(y)) => x.equal_node(y),
        // `_equalList` (T_List): equal length then element-wise `equal()`.
        (Node::List(x), Node::List(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| equal_node(p, q))
        }
        // Parse/analyze/rewrite query-tree node family (`_equalQuery` and the
        // sub-node comparators reachable through `Query`'s `Node`-list fields).
        (Node::Query(x), Node::Query(y)) => equal_query(x, y),
        (Node::RangeTblEntry(x), Node::RangeTblEntry(y)) => equal_range_tbl_entry(x, y),
        (Node::RTEPermissionInfo(x), Node::RTEPermissionInfo(y)) => {
            equal_rte_permission_info(x, y)
        }
        (Node::RangeTblFunction(x), Node::RangeTblFunction(y)) => equal_range_tbl_function(x, y),
        (Node::RangeTblRef(x), Node::RangeTblRef(y)) => equal_range_tbl_ref(x, y),
        (Node::FromExpr(x), Node::FromExpr(y)) => equal_from_expr(x, y),
        (Node::JoinExpr(x), Node::JoinExpr(y)) => equal_join_expr(x, y),
        (Node::OnConflictExpr(x), Node::OnConflictExpr(y)) => equal_on_conflict_expr(x, y),
        (Node::MergeAction(x), Node::MergeAction(y)) => equal_merge_action(x, y),
        (Node::GroupingSet(x), Node::GroupingSet(y)) => equal_grouping_set(x, y),
        (Node::WindowClause(x), Node::WindowClause(y)) => equal_window_clause(x, y),
        (Node::RowMarkClause(x), Node::RowMarkClause(y)) => equal_row_mark_clause(x, y),
        (Node::WithCheckOption(x), Node::WithCheckOption(y)) => equal_with_check_option(x, y),
        (Node::CommonTableExpr(x), Node::CommonTableExpr(y)) => equal_common_table_expr(x, y),
        (Node::SetOperationStmt(x), Node::SetOperationStmt(y)) => equal_set_operation_stmt(x, y),
        (Node::Alias(x), Node::Alias(y)) => equal_alias(x, y),
        // Different tags are never equal.
        (a, b) if a.node_tag() != b.node_tag() => false,
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
pub fn init_seams() {
    backend_nodes_equalfuncs_seams::equal_expr::set(equal_expr);
    backend_nodes_equalfuncs_seams::equal_node::set(equal_node);
    backend_nodes_equalfuncs_seams::equal_expr_list::set(equal_expr_list_impl);
    backend_nodes_equalfuncs_seams::equal_targetentry_list::set(equal_targetentry_list_impl);
    backend_nodes_equalfuncs_seams::equal_sortgroupclause_list::set(equal_sortgroupclause_list_impl);
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
    fn list_seams_compare_elementwise() {
        let a = vec![var(1, 1), var(1, 2)];
        let b = vec![var(1, 1), var(1, 2)];
        let c = vec![var(1, 1)];
        assert!(equal_expr_list_impl(&a, &b));
        assert!(!equal_expr_list_impl(&a, &c)); // length differs
    }
}
