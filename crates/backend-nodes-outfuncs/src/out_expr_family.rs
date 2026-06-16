//! `_out<Type>` writers for the `primnodes.h` expression family — both the
//! post-analysis [`types_nodes::primnodes::Expr`] arms (carried by
//! `Node::Expr`) and the raw-grammar `Expr`-deriving nodes carried as their own
//! `Node` arms ([`types_nodes::rawexprnodes`]). Each writer mirrors its
//! `outfuncs.funcs.c` / `outfuncs.c` body field-for-field with EXACT C
//! token-name + field-order parity.
//!
//! `try_out` returns `true` iff it claimed and wrote `node`.
//!
//! ## Reachability note
//!
//! `lib.rs`'s `out_node_inner` routes `Node::Expr(e)` straight into `out_expr`
//! (which covers Var/Const/Param/Op/Func/Bool and panics otherwise); the
//! `Node::Expr(Expr::X)` arms below are therefore only reached once `out_expr`
//! is extended to chain into the family writers. The raw-grammar `Node::X` arms
//! ARE reached today (they fall through `out_node_inner`'s `other =>` to this
//! chain). The READ side (`read_expr_family`) reconstructs the post-analysis
//! `Node::Expr(Expr::X)` form for every shared LABEL.

use alloc::string::String;

use types_nodes::nodes::Node;
use types_nodes::primnodes::Expr;

use crate::{
    out_expr, out_node_inner, write_bool_field, write_enum_field, write_expr_list_field,
    write_int_field, write_location_field, write_node_list_field, write_oid_field,
    write_string_field, write_uint_field,
};

/// `WRITE_NODE_FIELD` over a single optional child `Expr` carried as an
/// `Option<Box<Expr>>` (C: `outNode`; NULL renders `<>`).
fn write_opt_box_expr(buf: &mut String, name: &str, child: &Option<alloc::boxed::Box<Expr>>, wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    match child {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, wl),
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `Expr` that may contain NULL elements
/// (the slice case of `SubscriptingRef.ref{upper,lower}indexpr`). C: `outNode`
/// of the list, each element either an `Expr` or `<>`.
fn write_opt_expr_list_field(buf: &mut String, name: &str, list: &[Option<Expr>], wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for e in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        match e {
            None => buf.push_str("<>"),
            Some(x) => out_expr(buf, x, wl),
        }
    }
    buf.push(')');
}

/// `WRITE_NODE_FIELD` over a `List *` of `String` value nodes carried as
/// `Vec<String>` (e.g. `RowExpr.colnames`, `XmlExpr.arg_names`,
/// `JsonExpr.passing_names`). C: `outNode` of the list → `("a" "b" ...)`; a NIL
/// list is `<>`, but the repo carries an (always-present, possibly empty) Vec —
/// an empty Vec serializes as the empty list `()` (matching C's `_outList` for
/// a non-NULL empty `List *`, which the grammar never produces but is the
/// faithful encoding of an empty Vec).
fn write_string_list_field(buf: &mut String, name: &str, list: &[String], _wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for s in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        // `_outString`: wrap in quotes; escape inner via outToken.
        buf.push('"');
        if !s.is_empty() {
            crate::out_token(buf, s);
        }
        buf.push('"');
    }
    buf.push(')');
}

// ---------------------------------------------------------------------------
// Post-analysis `Expr` writers (outfuncs.funcs.c / outfuncs.c).
// ---------------------------------------------------------------------------

/// `WRITE_NODE_FIELD` over a `List *` of framed `TargetEntry` (C: `outNode` of
/// the list → `({TARGETENTRY ...} {TARGETENTRY ...} ...)`). Each element is the
/// `lib.rs`-owned `_outTargetEntry`, framed by `{`/`}` (the bare-list `(...)`
/// opener/closer is `_outList`'s). An empty Vec serializes as `()`.
fn write_targetentry_list_field(
    buf: &mut String,
    name: &str,
    list: &[types_nodes::primnodes::TargetEntry<'_>],
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for te in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        crate::framed(buf, |b| crate::out_targetentry(b, te, wl));
    }
    buf.push(')');
}

/// `WRITE_NODE_FIELD` over a `List *` of framed `SortGroupClause` (C: `outNode`
/// of the list → `({SORTGROUPCLAUSE ...} ...)`). Each element is the
/// parse-family-owned `_outSortGroupClause`, framed by `{`/`}`. Empty Vec → `()`.
fn write_sortgroupclause_list_field(
    buf: &mut String,
    name: &str,
    list: &[types_nodes::rawnodes::SortGroupClause],
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for sgc in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        crate::framed(buf, |b| crate::out_parse_family::out_sort_group_clause(b, sgc, wl));
    }
    buf.push(')');
}

/// `_outAggref` (outfuncs.funcs.c). The `aggno`/`aggtransno`/`aggpresorted`
/// fields carry `pg_node_attr(read_write_ignore)` in C and are NOT serialized;
/// every other field is emitted in struct order. `args` is a List of
/// `TargetEntry`, `aggorder`/`aggdistinct` Lists of `SortGroupClause`, reached
/// through the framed list writers above (`lib.rs`'s `_outTargetEntry` and the
/// parse family's `_outSortGroupClause`).
///
/// READ asymmetry: `_readAggref` is carrier-blocked — `Aggref.args` is
/// `Vec<TargetEntry<'static>>`, so a reader cannot store the mcx-allocated
/// children it reads off the cursor (same `'static`-carrier blocker as SUBPLAN).
/// OUT therefore serializes faithfully (for plan-tree dump / debug) while READ
/// stays a precise seam-panic until the carrier carries `'mcx`.
fn out_aggref(buf: &mut String, n: &types_nodes::primnodes::Aggref, wl: bool) {
    buf.push_str("AGGREF");
    write_oid_field(buf, "aggfnoid", n.aggfnoid);
    write_oid_field(buf, "aggtype", n.aggtype);
    write_oid_field(buf, "aggcollid", n.aggcollid);
    write_oid_field(buf, "inputcollid", n.inputcollid);
    write_oid_field(buf, "aggtranstype", n.aggtranstype);
    crate::write_oid_list_field(buf, "aggargtypes", Some(&n.aggargtypes));
    write_expr_list_field(buf, "aggdirectargs", &n.aggdirectargs, wl);
    write_targetentry_list_field(buf, "args", &n.args, wl);
    write_sortgroupclause_list_field(buf, "aggorder", &n.aggorder, wl);
    write_sortgroupclause_list_field(buf, "aggdistinct", &n.aggdistinct, wl);
    write_opt_box_expr(buf, "aggfilter", &n.aggfilter, wl);
    write_bool_field(buf, "aggstar", n.aggstar);
    write_bool_field(buf, "aggvariadic", n.aggvariadic);
    crate::write_char_field(buf, "aggkind", n.aggkind as u8);
    write_uint_field(buf, "agglevelsup", n.agglevelsup);
    write_enum_field(buf, "aggsplit", n.aggsplit);
    write_location_field(buf, "location", n.location, wl);
}

fn out_grouping_func(buf: &mut String, n: &types_nodes::primnodes::GroupingFunc, wl: bool) {
    buf.push_str("GROUPINGFUNC");
    write_expr_list_field(buf, "args", &n.args, wl);
    crate::write_int_list_field(buf, "refs", Some(&n.refs));
    crate::write_int_list_field(buf, "cols", Some(&n.cols));
    write_uint_field(buf, "agglevelsup", n.agglevelsup);
    write_location_field(buf, "location", n.location, wl);
}

fn out_window_func(buf: &mut String, n: &types_nodes::primnodes::WindowFunc, wl: bool) {
    buf.push_str("WINDOWFUNC");
    write_oid_field(buf, "winfnoid", n.winfnoid);
    write_oid_field(buf, "wintype", n.wintype);
    write_oid_field(buf, "wincollid", n.wincollid);
    write_oid_field(buf, "inputcollid", n.inputcollid);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_opt_box_expr(buf, "aggfilter", &n.aggfilter, wl);
    write_expr_list_field(buf, "runCondition", &n.runCondition, wl);
    write_uint_field(buf, "winref", n.winref);
    write_bool_field(buf, "winstar", n.winstar);
    write_bool_field(buf, "winagg", n.winagg);
    write_location_field(buf, "location", n.location, wl);
}

fn out_merge_support_func(buf: &mut String, n: &types_nodes::primnodes::MergeSupportFunc, wl: bool) {
    buf.push_str("MERGESUPPORTFUNC");
    write_oid_field(buf, "msftype", n.msftype);
    write_oid_field(buf, "msfcollid", n.msfcollid);
    write_location_field(buf, "location", n.location, wl);
}

fn out_subscripting_ref(buf: &mut String, n: &types_nodes::primnodes::SubscriptingRef, wl: bool) {
    buf.push_str("SUBSCRIPTINGREF");
    write_oid_field(buf, "refcontainertype", n.refcontainertype);
    write_oid_field(buf, "refelemtype", n.refelemtype);
    write_oid_field(buf, "refrestype", n.refrestype);
    write_int_field(buf, "reftypmod", n.reftypmod);
    write_oid_field(buf, "refcollid", n.refcollid);
    write_opt_expr_list_field(buf, "refupperindexpr", &n.refupperindexpr, wl);
    write_opt_expr_list_field(buf, "reflowerindexpr", &n.reflowerindexpr, wl);
    write_opt_box_expr(buf, "refexpr", &n.refexpr, wl);
    write_opt_box_expr(buf, "refassgnexpr", &n.refassgnexpr, wl);
}

fn out_named_arg_expr(buf: &mut String, n: &types_nodes::primnodes::NamedArgExpr, wl: bool) {
    buf.push_str("NAMEDARGEXPR");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_string_field(buf, "name", n.name.as_deref());
    write_int_field(buf, "argnumber", n.argnumber);
    write_location_field(buf, "location", n.location, wl);
}

fn out_scalar_array_op_expr(
    buf: &mut String,
    n: &types_nodes::primnodes::ScalarArrayOpExpr,
    wl: bool,
) {
    buf.push_str("SCALARARRAYOPEXPR");
    write_oid_field(buf, "opno", n.opno);
    write_oid_field(buf, "opfuncid", n.opfuncid);
    write_oid_field(buf, "hashfuncid", n.hashfuncid);
    write_oid_field(buf, "negfuncid", n.negfuncid);
    write_bool_field(buf, "useOr", n.useOr);
    write_oid_field(buf, "inputcollid", n.inputcollid);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_sublink(buf: &mut String, n: &types_nodes::primnodes::SubLink, wl: bool) {
    buf.push_str("SUBLINK");
    write_enum_field(buf, "subLinkType", n.subLinkType as i32);
    write_int_field(buf, "subLinkId", n.subLinkId);
    write_opt_box_expr(buf, "testexpr", &n.testexpr, wl);
    // WRITE_NODE_FIELD(operName) — a `List *` of String. After analysis operName
    // is always NIL; the repo trims it from the post-analysis SubLink. C still
    // writes `:operName <>`.
    buf.push_str(" :operName <>");
    // WRITE_NODE_FIELD(subselect) — the embedded sub-Query. The carrier is a
    // `PgBox<Query>` (Query is a serializable node); borrow it and emit the
    // framed `{QUERY ...}` form through the parse family's Query writer. A NULL
    // subselect renders `<>`.
    buf.push_str(" :subselect ");
    match &n.subselect {
        None => buf.push_str("<>"),
        Some(q) => crate::framed(buf, |b| crate::out_parse_family::out_query(b, q, wl)),
    }
    write_location_field(buf, "location", n.location, wl);
}

fn out_field_select(buf: &mut String, n: &types_nodes::primnodes::FieldSelect, wl: bool) {
    buf.push_str("FIELDSELECT");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_int_field(buf, "fieldnum", n.fieldnum as i32);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_int_field(buf, "resulttypmod", n.resulttypmod);
    write_oid_field(buf, "resultcollid", n.resultcollid);
}

fn out_field_store(buf: &mut String, n: &types_nodes::primnodes::FieldStore, wl: bool) {
    buf.push_str("FIELDSTORE");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_expr_list_field(buf, "newvals", &n.newvals, wl);
    // WRITE_NODE_FIELD(fieldnums) — an integer `List *`.
    let nums: alloc::vec::Vec<i32> = n.fieldnums.iter().map(|a| *a as i32).collect();
    crate::write_int_list_field(buf, "fieldnums", Some(&nums));
    write_oid_field(buf, "resulttype", n.resulttype);
}

fn out_relabel_type(buf: &mut String, n: &types_nodes::primnodes::RelabelType, wl: bool) {
    buf.push_str("RELABELTYPE");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_int_field(buf, "resulttypmod", n.resulttypmod);
    write_oid_field(buf, "resultcollid", n.resultcollid);
    write_enum_field(buf, "relabelformat", n.relabelformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_coerce_via_io(buf: &mut String, n: &types_nodes::primnodes::CoerceViaIO, wl: bool) {
    buf.push_str("COERCEVIAIO");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_oid_field(buf, "resultcollid", n.resultcollid);
    write_enum_field(buf, "coerceformat", n.coerceformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_array_coerce_expr(buf: &mut String, n: &types_nodes::primnodes::ArrayCoerceExpr, wl: bool) {
    buf.push_str("ARRAYCOERCEEXPR");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_opt_box_expr(buf, "elemexpr", &n.elemexpr, wl);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_int_field(buf, "resulttypmod", n.resulttypmod);
    write_oid_field(buf, "resultcollid", n.resultcollid);
    write_enum_field(buf, "coerceformat", n.coerceformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_convert_rowtype_expr(
    buf: &mut String,
    n: &types_nodes::primnodes::ConvertRowtypeExpr,
    wl: bool,
) {
    buf.push_str("CONVERTROWTYPEEXPR");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_enum_field(buf, "convertformat", n.convertformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_collate_expr(buf: &mut String, n: &types_nodes::primnodes::CollateExpr, wl: bool) {
    buf.push_str("COLLATEEXPR");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "collOid", n.collOid);
    write_location_field(buf, "location", n.location, wl);
}

fn out_case_expr(buf: &mut String, n: &types_nodes::primnodes::CaseExpr, wl: bool) {
    buf.push_str("CASEEXPR");
    write_oid_field(buf, "casetype", n.casetype);
    write_oid_field(buf, "casecollid", n.casecollid);
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    // WRITE_NODE_FIELD(args) — a `List *` of CaseWhen.
    write_node_list_field(buf, "args", Some(&n.args), wl, |b, cw, w| {
        crate::framed(b, |b2| out_case_when(b2, cw, w))
    });
    write_opt_box_expr(buf, "defresult", &n.defresult, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_case_when(buf: &mut String, n: &types_nodes::primnodes::CaseWhen, wl: bool) {
    buf.push_str("CASEWHEN");
    write_opt_box_expr(buf, "expr", &n.expr, wl);
    write_opt_box_expr(buf, "result", &n.result, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_case_test_expr(buf: &mut String, n: &types_nodes::primnodes::CaseTestExpr, _wl: bool) {
    buf.push_str("CASETESTEXPR");
    write_oid_field(buf, "typeId", n.typeId);
    write_int_field(buf, "typeMod", n.typeMod);
    write_oid_field(buf, "collation", n.collation);
}

fn out_array_expr(buf: &mut String, n: &types_nodes::primnodes::ArrayExpr, wl: bool) {
    buf.push_str("ARRAYEXPR");
    write_oid_field(buf, "array_typeid", n.array_typeid);
    write_oid_field(buf, "array_collid", n.array_collid);
    write_oid_field(buf, "element_typeid", n.element_typeid);
    write_expr_list_field(buf, "elements", &n.elements, wl);
    write_bool_field(buf, "multidims", n.multidims);
    // NOTE: the repo's post-analysis `ArrayExpr` trims `list_start`/`list_end`
    // (query-jumble-only ParseLoc fields C writes). C `_outArrayExpr` writes
    // `:list_start :list_end :location`; we emit `list_start`/`list_end` as the
    // location value (-1 unless write_loc) so the token stream stays
    // field-for-field. They are not round-trippable into the trimmed struct, so
    // READ drops them (faithful: location fields read as -1).
    write_location_field(buf, "list_start", -1, wl);
    write_location_field(buf, "list_end", -1, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_row_expr(buf: &mut String, n: &types_nodes::primnodes::RowExpr, wl: bool) {
    buf.push_str("ROWEXPR");
    write_expr_list_field(buf, "args", &n.args, wl);
    write_oid_field(buf, "row_typeid", n.row_typeid);
    write_enum_field(buf, "row_format", n.row_format as i32);
    write_string_list_field(buf, "colnames", &n.colnames, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_row_compare_expr(buf: &mut String, n: &types_nodes::primnodes::RowCompareExpr, wl: bool) {
    buf.push_str("ROWCOMPAREEXPR");
    write_enum_field(buf, "cmptype", n.cmptype as i32);
    crate::write_oid_list_field(buf, "opnos", Some(&n.opnos));
    crate::write_oid_list_field(buf, "opfamilies", Some(&n.opfamilies));
    crate::write_oid_list_field(buf, "inputcollids", Some(&n.inputcollids));
    write_expr_list_field(buf, "largs", &n.largs, wl);
    write_expr_list_field(buf, "rargs", &n.rargs, wl);
}

fn out_coalesce_expr(buf: &mut String, n: &types_nodes::primnodes::CoalesceExpr, wl: bool) {
    buf.push_str("COALESCEEXPR");
    write_oid_field(buf, "coalescetype", n.coalescetype);
    write_oid_field(buf, "coalescecollid", n.coalescecollid);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_minmax_expr(buf: &mut String, n: &types_nodes::primnodes::MinMaxExpr, wl: bool) {
    buf.push_str("MINMAXEXPR");
    write_oid_field(buf, "minmaxtype", n.minmaxtype);
    write_oid_field(buf, "minmaxcollid", n.minmaxcollid);
    write_oid_field(buf, "inputcollid", n.inputcollid);
    write_enum_field(buf, "op", n.op as i32);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_sqlvalue_function(buf: &mut String, n: &types_nodes::primnodes::SQLValueFunction, wl: bool) {
    buf.push_str("SQLVALUEFUNCTION");
    write_enum_field(buf, "op", n.op as i32);
    write_oid_field(buf, "type", n.r#type);
    write_int_field(buf, "typmod", n.typmod);
    write_location_field(buf, "location", n.location, wl);
}

fn out_xml_expr(buf: &mut String, n: &types_nodes::primnodes::XmlExpr, wl: bool) {
    buf.push_str("XMLEXPR");
    write_enum_field(buf, "op", n.op as i32);
    write_string_field(buf, "name", n.name.as_deref());
    write_expr_list_field(buf, "named_args", &n.named_args, wl);
    write_string_list_field(buf, "arg_names", &n.arg_names, wl);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_enum_field(buf, "xmloption", n.xmloption as i32);
    write_bool_field(buf, "indent", n.indent);
    write_oid_field(buf, "type", n.r#type);
    write_int_field(buf, "typmod", n.typmod);
    write_location_field(buf, "location", n.location, wl);
}

// JSON support nodes (carried inline, not Expr-dispatched but referenced).

fn out_json_format(buf: &mut String, n: &types_nodes::primnodes::JsonFormat, wl: bool) {
    buf.push_str("JSONFORMAT");
    write_enum_field(buf, "format_type", n.format_type as i32);
    write_enum_field(buf, "encoding", n.encoding as i32);
    write_location_field(buf, "location", n.location, wl);
}

/// `WRITE_NODE_FIELD` over an optional `JsonFormat *` (a framed `{JSONFORMAT ...}`
/// child, or `<>`).
fn write_opt_json_format(
    buf: &mut String,
    name: &str,
    f: &Option<types_nodes::primnodes::JsonFormat>,
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    match f {
        None => buf.push_str("<>"),
        Some(fmt) => crate::framed(buf, |b| out_json_format(b, fmt, wl)),
    }
}

fn out_json_returning(buf: &mut String, n: &types_nodes::primnodes::JsonReturning, wl: bool) {
    buf.push_str("JSONRETURNING");
    write_opt_json_format(buf, "format", &n.format, wl);
    write_oid_field(buf, "typid", n.typid);
    write_int_field(buf, "typmod", n.typmod);
}

fn write_opt_json_returning(
    buf: &mut String,
    name: &str,
    r: &Option<types_nodes::primnodes::JsonReturning>,
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    match r {
        None => buf.push_str("<>"),
        Some(jr) => crate::framed(buf, |b| out_json_returning(b, jr, wl)),
    }
}

fn out_json_value_expr(buf: &mut String, n: &types_nodes::primnodes::JsonValueExpr, wl: bool) {
    buf.push_str("JSONVALUEEXPR");
    write_opt_box_expr(buf, "raw_expr", &n.raw_expr, wl);
    write_opt_box_expr(buf, "formatted_expr", &n.formatted_expr, wl);
    write_opt_json_format(buf, "format", &n.format, wl);
}

fn out_json_constructor_expr(
    buf: &mut String,
    n: &types_nodes::primnodes::JsonConstructorExpr,
    wl: bool,
) {
    buf.push_str("JSONCONSTRUCTOREXPR");
    write_enum_field(buf, "type", n.r#type as i32);
    write_expr_list_field(buf, "args", &n.args, wl);
    write_opt_box_expr(buf, "func", &n.func, wl);
    write_opt_box_expr(buf, "coercion", &n.coercion, wl);
    write_opt_json_returning(buf, "returning", &n.returning, wl);
    write_bool_field(buf, "absent_on_null", n.absent_on_null);
    write_bool_field(buf, "unique", n.unique);
    write_location_field(buf, "location", n.location, wl);
}

fn out_json_is_predicate(buf: &mut String, n: &types_nodes::primnodes::JsonIsPredicate, wl: bool) {
    buf.push_str("JSONISPREDICATE");
    write_opt_box_expr(buf, "expr", &n.expr, wl);
    write_opt_json_format(buf, "format", &n.format, wl);
    write_enum_field(buf, "item_type", n.item_type as i32);
    write_bool_field(buf, "unique_keys", n.unique_keys);
    write_location_field(buf, "location", n.location, wl);
}

fn out_json_behavior(buf: &mut String, n: &types_nodes::primnodes::JsonBehavior, wl: bool) {
    buf.push_str("JSONBEHAVIOR");
    write_enum_field(buf, "btype", n.btype as i32);
    write_opt_box_expr(buf, "expr", &n.expr, wl);
    write_bool_field(buf, "coerce", n.coerce);
    write_location_field(buf, "location", n.location, wl);
}

fn write_opt_json_behavior(
    buf: &mut String,
    name: &str,
    b: &Option<alloc::boxed::Box<types_nodes::primnodes::JsonBehavior>>,
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    match b {
        None => buf.push_str("<>"),
        Some(jb) => crate::framed(buf, |bb| out_json_behavior(bb, jb, wl)),
    }
}

fn out_json_expr(buf: &mut String, n: &types_nodes::primnodes::JsonExpr, wl: bool) {
    buf.push_str("JSONEXPR");
    write_enum_field(buf, "op", n.op as i32);
    write_string_field(buf, "column_name", n.column_name.as_deref());
    write_opt_box_expr(buf, "formatted_expr", &n.formatted_expr, wl);
    write_opt_json_format(buf, "format", &n.format, wl);
    write_opt_box_expr(buf, "path_spec", &n.path_spec, wl);
    write_opt_json_returning(buf, "returning", &n.returning, wl);
    write_string_list_field(buf, "passing_names", &n.passing_names, wl);
    write_expr_list_field(buf, "passing_values", &n.passing_values, wl);
    write_opt_json_behavior(buf, "on_empty", &n.on_empty, wl);
    write_opt_json_behavior(buf, "on_error", &n.on_error, wl);
    write_bool_field(buf, "use_io_coercion", n.use_io_coercion);
    write_bool_field(buf, "use_json_coercion", n.use_json_coercion);
    write_enum_field(buf, "wrapper", n.wrapper as i32);
    write_bool_field(buf, "omit_quotes", n.omit_quotes);
    write_oid_field(buf, "collation", n.collation);
    write_location_field(buf, "location", n.location, wl);
}

fn out_null_test(buf: &mut String, n: &types_nodes::primnodes::NullTest, wl: bool) {
    buf.push_str("NULLTEST");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_enum_field(buf, "nulltesttype", n.nulltesttype as i32);
    write_bool_field(buf, "argisrow", n.argisrow);
    write_location_field(buf, "location", n.location, wl);
}

fn out_boolean_test(buf: &mut String, n: &types_nodes::primnodes::BooleanTest, wl: bool) {
    buf.push_str("BOOLEANTEST");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_enum_field(buf, "booltesttype", n.booltesttype as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_coerce_to_domain(buf: &mut String, n: &types_nodes::primnodes::CoerceToDomain, wl: bool) {
    buf.push_str("COERCETODOMAIN");
    write_opt_box_expr(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "resulttype", n.resulttype);
    write_int_field(buf, "resulttypmod", n.resulttypmod);
    write_oid_field(buf, "resultcollid", n.resultcollid);
    write_enum_field(buf, "coercionformat", n.coercionformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_coerce_to_domain_value(
    buf: &mut String,
    n: &types_nodes::primnodes::CoerceToDomainValue,
    wl: bool,
) {
    buf.push_str("COERCETODOMAINVALUE");
    write_oid_field(buf, "typeId", n.typeId);
    write_int_field(buf, "typeMod", n.typeMod);
    write_oid_field(buf, "collation", n.collation);
    write_location_field(buf, "location", n.location, wl);
}

fn out_set_to_default(buf: &mut String, n: &types_nodes::primnodes::SetToDefault, wl: bool) {
    buf.push_str("SETTODEFAULT");
    write_oid_field(buf, "typeId", n.typeId);
    write_int_field(buf, "typeMod", n.typeMod);
    write_oid_field(buf, "collation", n.collation);
    write_location_field(buf, "location", n.location, wl);
}

fn out_current_of_expr(buf: &mut String, n: &types_nodes::primnodes::CurrentOfExpr, _wl: bool) {
    buf.push_str("CURRENTOFEXPR");
    write_uint_field(buf, "cvarno", n.cvarno);
    write_string_field(buf, "cursor_name", n.cursor_name.as_deref());
    write_int_field(buf, "cursor_param", n.cursor_param);
}

fn out_next_value_expr(buf: &mut String, n: &types_nodes::primnodes::NextValueExpr, _wl: bool) {
    buf.push_str("NEXTVALUEEXPR");
    write_oid_field(buf, "seqid", n.seqid);
    write_oid_field(buf, "typeId", n.typeId);
}

fn out_inference_elem(buf: &mut String, n: &types_nodes::primnodes::InferenceElem, wl: bool) {
    buf.push_str("INFERENCEELEM");
    write_opt_box_expr(buf, "expr", &n.expr, wl);
    write_oid_field(buf, "infercollid", n.infercollid);
    write_oid_field(buf, "inferopclass", n.inferopclass);
}

fn out_returning_expr(buf: &mut String, n: &types_nodes::primnodes::ReturningExpr, wl: bool) {
    buf.push_str("RETURNINGEXPR");
    write_int_field(buf, "retlevelsup", n.retlevelsup);
    write_bool_field(buf, "retold", n.retold);
    write_opt_box_expr(buf, "retexpr", &n.retexpr, wl);
}

/// A `List *` of `Expr` carried as `PgVec<PgBox<Expr>>` (e.g. `SubPlan.args`).
/// C: `outNode` of the list → `({...} {...})`; a NIL list is `<>`, but the repo
/// carries an always-present (possibly empty) Vec, which serializes as `()`
/// (the faithful encoding of an empty `List *`). `_readSubPlan` reads it via
/// `nodeRead`, so an empty `()` round-trips to an empty Vec.
fn write_pgbox_expr_list_field(
    buf: &mut String,
    name: &str,
    list: &[mcx::PgBox<'_, Expr>],
    wl: bool,
) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for e in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_expr(buf, e, wl);
    }
    buf.push(')');
}

/// `_outSubPlan` (outfuncs.funcs.c). `testexpr` is a single `Node *` child (an
/// `OpExpr`/`RowCompareExpr`); `paramIds`/`setParam`/`parParam` are `IntList`s;
/// `args` is a `List *` of `Expr`.
pub(crate) fn out_subplan(buf: &mut String, n: &types_nodes::primnodes::SubPlan<'_>, wl: bool) {
    buf.push_str("SUBPLAN");
    write_enum_field(buf, "subLinkType", n.subLinkType as i32);
    // testexpr: Option<PgBox<Expr>> — single Node child.
    {
        use core::fmt::Write as _;
        let _ = write!(buf, " :testexpr ");
        match n.testexpr.as_deref() {
            None => buf.push_str("<>"),
            Some(e) => out_expr(buf, e, wl),
        }
    }
    crate::write_int_list_field(buf, "paramIds", Some(&n.paramIds));
    write_int_field(buf, "plan_id", n.plan_id);
    write_string_field(buf, "plan_name", n.plan_name.as_ref().map(|s| s.as_str()));
    write_oid_field(buf, "firstColType", n.firstColType);
    write_int_field(buf, "firstColTypmod", n.firstColTypmod);
    write_oid_field(buf, "firstColCollation", n.firstColCollation);
    write_bool_field(buf, "useHashTable", n.useHashTable);
    write_bool_field(buf, "unknownEqFalse", n.unknownEqFalse);
    write_bool_field(buf, "parallel_safe", n.parallel_safe);
    crate::write_int_list_field(buf, "setParam", Some(&n.setParam));
    crate::write_int_list_field(buf, "parParam", Some(&n.parParam));
    write_pgbox_expr_list_field(buf, "args", &n.args, wl);
    crate::write_float_field(buf, "startup_cost", n.startup_cost);
    crate::write_float_field(buf, "per_call_cost", n.per_call_cost);
}

/// `_outAlternativeSubPlan` (outfuncs.funcs.c): a single `:subplans` node list
/// of `SubPlan`s.
fn out_alternative_subplan(
    buf: &mut String,
    n: &types_nodes::primnodes::AlternativeSubPlan<'_>,
    wl: bool,
) {
    buf.push_str("ALTERNATIVESUBPLAN");
    use core::fmt::Write as _;
    let _ = write!(buf, " :subplans ");
    buf.push('(');
    let mut first = true;
    for sp in &n.subplans {
        if !first {
            buf.push(' ');
        }
        first = false;
        crate::framed(buf, |b| out_subplan(b, sp, wl));
    }
    buf.push(')');
}

// ---------------------------------------------------------------------------
// Raw-grammar `Node::X` writers. These share C node tags/LABELs with the
// post-analysis Expr forms; their children are raw `Node *` (NodePtr), written
// via `out_node_inner`. The READ side reconstructs the post-analysis Expr form
// (which is what `readfuncs.c` builds), so a raw arm's READ is a precise
// seam-panic (raw parse trees are not round-tripped through readfuncs).
// ---------------------------------------------------------------------------

/// `WRITE_NODE_FIELD` over a raw `Option<NodePtr>` child (C `outNode`; `<>` for
/// NULL).
fn write_raw_node(buf: &mut String, name: &str, child: &Option<types_nodes::nodes::NodePtr<'_>>, wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    match child {
        None => buf.push_str("<>"),
        Some(np) => out_node_inner(buf, np, wl),
    }
}

/// `WRITE_NODE_FIELD` over a raw `List *` of `NodePtr` (the bare `(child ...)`
/// list form; empty Vec → `()`).
fn write_raw_node_list(buf: &mut String, name: &str, list: &[types_nodes::nodes::NodePtr<'_>], wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for np in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_node_inner(buf, np, wl);
    }
    buf.push(')');
}

fn out_raw_bool_expr(buf: &mut String, n: &types_nodes::rawexprnodes::BoolExpr<'_>, wl: bool) {
    // `_outBoolExpr` (outfuncs.c, custom): the do-it-yourself `:boolop` string.
    buf.push_str("BOOLEXPR");
    let opstr = match n.boolop {
        types_nodes::primnodes::BoolExprType::AND_EXPR => "and",
        types_nodes::primnodes::BoolExprType::OR_EXPR => "or",
        types_nodes::primnodes::BoolExprType::NOT_EXPR => "not",
    };
    buf.push_str(" :boolop ");
    crate::out_token(buf, opstr);
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_case_expr(buf: &mut String, n: &types_nodes::rawexprnodes::CaseExpr<'_>, wl: bool) {
    buf.push_str("CASEEXPR");
    write_oid_field(buf, "casetype", n.casetype);
    write_oid_field(buf, "casecollid", n.casecollid);
    write_raw_node(buf, "arg", &n.arg, wl);
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_raw_node(buf, "defresult", &n.defresult, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_case_when(buf: &mut String, n: &types_nodes::rawexprnodes::CaseWhen<'_>, wl: bool) {
    buf.push_str("CASEWHEN");
    write_raw_node(buf, "expr", &n.expr, wl);
    write_raw_node(buf, "result", &n.result, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_coalesce_expr(buf: &mut String, n: &types_nodes::rawexprnodes::CoalesceExpr<'_>, wl: bool) {
    buf.push_str("COALESCEEXPR");
    write_oid_field(buf, "coalescetype", n.coalescetype);
    write_oid_field(buf, "coalescecollid", n.coalescecollid);
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_minmax_expr(buf: &mut String, n: &types_nodes::rawexprnodes::MinMaxExpr<'_>, wl: bool) {
    buf.push_str("MINMAXEXPR");
    write_oid_field(buf, "minmaxtype", n.minmaxtype);
    write_oid_field(buf, "minmaxcollid", n.minmaxcollid);
    write_oid_field(buf, "inputcollid", n.inputcollid);
    write_enum_field(buf, "op", n.op as i32);
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_sublink(buf: &mut String, n: &types_nodes::rawexprnodes::SubLink<'_>, wl: bool) {
    buf.push_str("SUBLINK");
    write_enum_field(buf, "subLinkType", n.sub_link_type as i32);
    write_int_field(buf, "subLinkId", n.sub_link_id);
    write_raw_node(buf, "testexpr", &n.testexpr, wl);
    write_raw_node_list(buf, "operName", n.oper_name.as_slice(), wl);
    write_raw_node(buf, "subselect", &n.subselect, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_null_test(buf: &mut String, n: &types_nodes::rawexprnodes::NullTest<'_>, wl: bool) {
    buf.push_str("NULLTEST");
    write_raw_node(buf, "arg", &n.arg, wl);
    write_enum_field(buf, "nulltesttype", n.nulltesttype as i32);
    write_bool_field(buf, "argisrow", n.argisrow);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_boolean_test(buf: &mut String, n: &types_nodes::rawexprnodes::BooleanTest<'_>, wl: bool) {
    buf.push_str("BOOLEANTEST");
    write_raw_node(buf, "arg", &n.arg, wl);
    write_enum_field(buf, "booltesttype", n.booltesttype as i32);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_row_expr(buf: &mut String, n: &types_nodes::rawexprnodes::RowExpr<'_>, wl: bool) {
    buf.push_str("ROWEXPR");
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_oid_field(buf, "row_typeid", n.row_typeid);
    write_enum_field(buf, "row_format", n.row_format as i32);
    write_raw_node_list(buf, "colnames", n.colnames.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_grouping_func(buf: &mut String, n: &types_nodes::rawexprnodes::GroupingFunc<'_>, wl: bool) {
    buf.push_str("GROUPINGFUNC");
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    let refs: alloc::vec::Vec<i32> = n.refs.iter().copied().collect();
    crate::write_int_list_field(buf, "refs", Some(&refs));
    let cols: alloc::vec::Vec<i32> = n.cols.iter().copied().collect();
    crate::write_int_list_field(buf, "cols", Some(&cols));
    write_uint_field(buf, "agglevelsup", n.agglevelsup);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_collate_expr(buf: &mut String, n: &types_nodes::rawexprnodes::CollateExpr<'_>, wl: bool) {
    buf.push_str("COLLATEEXPR");
    write_raw_node(buf, "arg", &n.arg, wl);
    write_oid_field(buf, "collOid", n.coll_oid);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_set_to_default(buf: &mut String, n: &types_nodes::rawexprnodes::SetToDefault, wl: bool) {
    buf.push_str("SETTODEFAULT");
    write_oid_field(buf, "typeId", n.type_id);
    write_int_field(buf, "typeMod", n.type_mod);
    write_oid_field(buf, "collation", n.collation);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_current_of_expr(buf: &mut String, n: &types_nodes::rawexprnodes::CurrentOfExpr<'_>, _wl: bool) {
    buf.push_str("CURRENTOFEXPR");
    write_uint_field(buf, "cvarno", n.cvarno);
    write_string_field(buf, "cursor_name", n.cursor_name.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "cursor_param", n.cursor_param);
}

fn out_raw_named_arg_expr(buf: &mut String, n: &types_nodes::rawexprnodes::NamedArgExpr<'_>, wl: bool) {
    buf.push_str("NAMEDARGEXPR");
    write_raw_node(buf, "arg", &n.arg, wl);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "argnumber", n.argnumber);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_sqlvalue_function(buf: &mut String, n: &types_nodes::rawexprnodes::SQLValueFunction, wl: bool) {
    buf.push_str("SQLVALUEFUNCTION");
    write_enum_field(buf, "op", n.op as i32);
    write_oid_field(buf, "type", n.type_);
    write_int_field(buf, "typmod", n.typmod);
    write_location_field(buf, "location", n.location, wl);
}

fn out_raw_xml_expr(buf: &mut String, n: &types_nodes::rawexprnodes::XmlExpr<'_>, wl: bool) {
    buf.push_str("XMLEXPR");
    write_enum_field(buf, "op", n.op as i32);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_raw_node_list(buf, "named_args", n.named_args.as_slice(), wl);
    write_raw_node_list(buf, "arg_names", n.arg_names.as_slice(), wl);
    write_raw_node_list(buf, "args", n.args.as_slice(), wl);
    write_enum_field(buf, "xmloption", n.xmloption as i32);
    write_bool_field(buf, "indent", n.indent);
    write_oid_field(buf, "type", n.type_);
    write_int_field(buf, "typmod", n.typmod);
    write_location_field(buf, "location", n.location, wl);
}

/// Write the per-node `_out<Type>` BODY (label + `WRITE_*_FIELD`s, NO `{}`
/// framing) for a post-analysis [`Expr`] arm this family owns. The `{`/`}` is
/// supplied by `lib.rs`'s `out_expr` (which calls this on its fallback arm).
/// Returns `true` iff this family claimed `e`.
pub(crate) fn out_expr_body(buf: &mut String, e: &Expr, wl: bool) -> bool {
    match e {
        Expr::Aggref(n) => out_aggref(buf, n, wl),
        Expr::GroupingFunc(n) => out_grouping_func(buf, n, wl),
        Expr::WindowFunc(n) => out_window_func(buf, n, wl),
        Expr::MergeSupportFunc(n) => out_merge_support_func(buf, n, wl),
        Expr::SubscriptingRef(n) => out_subscripting_ref(buf, n, wl),
        Expr::NamedArgExpr(n) => out_named_arg_expr(buf, n, wl),
        Expr::ScalarArrayOpExpr(n) => out_scalar_array_op_expr(buf, n, wl),
        Expr::SubLink(n) => out_sublink(buf, n, wl),
        Expr::FieldSelect(n) => out_field_select(buf, n, wl),
        Expr::FieldStore(n) => out_field_store(buf, n, wl),
        Expr::RelabelType(n) => out_relabel_type(buf, n, wl),
        Expr::CoerceViaIO(n) => out_coerce_via_io(buf, n, wl),
        Expr::ArrayCoerceExpr(n) => out_array_coerce_expr(buf, n, wl),
        Expr::ConvertRowtypeExpr(n) => out_convert_rowtype_expr(buf, n, wl),
        Expr::CollateExpr(n) => out_collate_expr(buf, n, wl),
        Expr::CaseExpr(n) => out_case_expr(buf, n, wl),
        Expr::CaseTestExpr(n) => out_case_test_expr(buf, n, wl),
        Expr::ArrayExpr(n) => out_array_expr(buf, n, wl),
        Expr::RowExpr(n) => out_row_expr(buf, n, wl),
        Expr::RowCompareExpr(n) => out_row_compare_expr(buf, n, wl),
        Expr::CoalesceExpr(n) => out_coalesce_expr(buf, n, wl),
        Expr::MinMaxExpr(n) => out_minmax_expr(buf, n, wl),
        Expr::SQLValueFunction(n) => out_sqlvalue_function(buf, n, wl),
        Expr::XmlExpr(n) => out_xml_expr(buf, n, wl),
        Expr::JsonValueExpr(n) => out_json_value_expr(buf, n, wl),
        Expr::JsonConstructorExpr(n) => out_json_constructor_expr(buf, n, wl),
        Expr::JsonIsPredicate(n) => out_json_is_predicate(buf, n, wl),
        Expr::JsonExpr(n) => out_json_expr(buf, n, wl),
        Expr::NullTest(n) => out_null_test(buf, n, wl),
        Expr::BooleanTest(n) => out_boolean_test(buf, n, wl),
        Expr::CoerceToDomain(n) => out_coerce_to_domain(buf, n, wl),
        Expr::CoerceToDomainValue(n) => out_coerce_to_domain_value(buf, n, wl),
        Expr::SetToDefault(n) => out_set_to_default(buf, n, wl),
        Expr::CurrentOfExpr(n) => out_current_of_expr(buf, n, wl),
        Expr::NextValueExpr(n) => out_next_value_expr(buf, n, wl),
        Expr::InferenceElem(n) => out_inference_elem(buf, n, wl),
        Expr::ReturningExpr(n) => out_returning_expr(buf, n, wl),
        Expr::SubPlan(n) => out_subplan(buf, &n.0, wl),
        Expr::AlternativeSubPlan(n) => out_alternative_subplan(buf, &n.0, wl),
        _ => return false,
    }
    true
}

/// Dispatch the expression-family `Node` arms this module owns.
pub(crate) fn try_out(buf: &mut String, node: &Node<'_>, wl: bool) -> bool {
    match node {
        // Post-analysis `Expr` arms. (Reached once lib.rs `out_expr` chains here;
        // included for completeness + so the writers are tested directly.)
        Node::Expr(e) => match e {
            Expr::Aggref(n) => crate::framed(buf, |b| out_aggref(b, n, wl)),
            Expr::GroupingFunc(n) => crate::framed(buf, |b| out_grouping_func(b, n, wl)),
            Expr::WindowFunc(n) => crate::framed(buf, |b| out_window_func(b, n, wl)),
            Expr::MergeSupportFunc(n) => crate::framed(buf, |b| out_merge_support_func(b, n, wl)),
            Expr::SubscriptingRef(n) => crate::framed(buf, |b| out_subscripting_ref(b, n, wl)),
            Expr::NamedArgExpr(n) => crate::framed(buf, |b| out_named_arg_expr(b, n, wl)),
            Expr::ScalarArrayOpExpr(n) => crate::framed(buf, |b| out_scalar_array_op_expr(b, n, wl)),
            Expr::SubLink(n) => crate::framed(buf, |b| out_sublink(b, n, wl)),
            Expr::FieldSelect(n) => crate::framed(buf, |b| out_field_select(b, n, wl)),
            Expr::FieldStore(n) => crate::framed(buf, |b| out_field_store(b, n, wl)),
            Expr::RelabelType(n) => crate::framed(buf, |b| out_relabel_type(b, n, wl)),
            Expr::CoerceViaIO(n) => crate::framed(buf, |b| out_coerce_via_io(b, n, wl)),
            Expr::ArrayCoerceExpr(n) => crate::framed(buf, |b| out_array_coerce_expr(b, n, wl)),
            Expr::ConvertRowtypeExpr(n) => crate::framed(buf, |b| out_convert_rowtype_expr(b, n, wl)),
            Expr::CollateExpr(n) => crate::framed(buf, |b| out_collate_expr(b, n, wl)),
            Expr::CaseExpr(n) => crate::framed(buf, |b| out_case_expr(b, n, wl)),
            Expr::CaseTestExpr(n) => crate::framed(buf, |b| out_case_test_expr(b, n, wl)),
            Expr::ArrayExpr(n) => crate::framed(buf, |b| out_array_expr(b, n, wl)),
            Expr::RowExpr(n) => crate::framed(buf, |b| out_row_expr(b, n, wl)),
            Expr::RowCompareExpr(n) => crate::framed(buf, |b| out_row_compare_expr(b, n, wl)),
            Expr::CoalesceExpr(n) => crate::framed(buf, |b| out_coalesce_expr(b, n, wl)),
            Expr::MinMaxExpr(n) => crate::framed(buf, |b| out_minmax_expr(b, n, wl)),
            Expr::SQLValueFunction(n) => crate::framed(buf, |b| out_sqlvalue_function(b, n, wl)),
            Expr::XmlExpr(n) => crate::framed(buf, |b| out_xml_expr(b, n, wl)),
            Expr::JsonValueExpr(n) => crate::framed(buf, |b| out_json_value_expr(b, n, wl)),
            Expr::JsonConstructorExpr(n) => {
                crate::framed(buf, |b| out_json_constructor_expr(b, n, wl))
            }
            Expr::JsonIsPredicate(n) => crate::framed(buf, |b| out_json_is_predicate(b, n, wl)),
            Expr::JsonExpr(n) => crate::framed(buf, |b| out_json_expr(b, n, wl)),
            Expr::NullTest(n) => crate::framed(buf, |b| out_null_test(b, n, wl)),
            Expr::BooleanTest(n) => crate::framed(buf, |b| out_boolean_test(b, n, wl)),
            Expr::CoerceToDomain(n) => crate::framed(buf, |b| out_coerce_to_domain(b, n, wl)),
            Expr::CoerceToDomainValue(n) => {
                crate::framed(buf, |b| out_coerce_to_domain_value(b, n, wl))
            }
            Expr::SetToDefault(n) => crate::framed(buf, |b| out_set_to_default(b, n, wl)),
            Expr::CurrentOfExpr(n) => crate::framed(buf, |b| out_current_of_expr(b, n, wl)),
            Expr::NextValueExpr(n) => crate::framed(buf, |b| out_next_value_expr(b, n, wl)),
            Expr::InferenceElem(n) => crate::framed(buf, |b| out_inference_elem(b, n, wl)),
            Expr::ReturningExpr(n) => crate::framed(buf, |b| out_returning_expr(b, n, wl)),
            // SubPlan/AlternativeSubPlan/PlaceHolderVar/RestrictInfo: see the
            // notes in try_read; their writers are not in this family's scope
            // (SubPlan is a plan-layer node; PHV/RestrictInfo are pathnodes.h
            // planner-internal). Decline so the central dispatch panics loudly.
            _ => return false,
        },
        // Raw-grammar `Node::X` arms (reachable through `out_node_inner`'s
        // `other =>` fallthrough today).
        Node::BoolExpr(n) => crate::framed(buf, |b| out_raw_bool_expr(b, n, wl)),
        Node::CaseExpr(n) => crate::framed(buf, |b| out_raw_case_expr(b, n, wl)),
        Node::CaseWhen(n) => crate::framed(buf, |b| out_raw_case_when(b, n, wl)),
        Node::CoalesceExpr(n) => crate::framed(buf, |b| out_raw_coalesce_expr(b, n, wl)),
        Node::MinMaxExpr(n) => crate::framed(buf, |b| out_raw_minmax_expr(b, n, wl)),
        Node::SubLink(n) => crate::framed(buf, |b| out_raw_sublink(b, n, wl)),
        Node::NullTest(n) => crate::framed(buf, |b| out_raw_null_test(b, n, wl)),
        Node::BooleanTest(n) => crate::framed(buf, |b| out_raw_boolean_test(b, n, wl)),
        Node::RowExpr(n) => crate::framed(buf, |b| out_raw_row_expr(b, n, wl)),
        Node::GroupingFunc(n) => crate::framed(buf, |b| out_raw_grouping_func(b, n, wl)),
        Node::CollateExpr(n) => crate::framed(buf, |b| out_raw_collate_expr(b, n, wl)),
        Node::SetToDefault(n) => crate::framed(buf, |b| out_raw_set_to_default(b, n, wl)),
        Node::CurrentOfExpr(n) => crate::framed(buf, |b| out_raw_current_of_expr(b, n, wl)),
        Node::NamedArgExpr(n) => crate::framed(buf, |b| out_raw_named_arg_expr(b, n, wl)),
        Node::SQLValueFunction(n) => crate::framed(buf, |b| out_raw_sqlvalue_function(b, n, wl)),
        Node::XmlExpr(n) => crate::framed(buf, |b| out_raw_xml_expr(b, n, wl)),
        _ => return false,
    }
    true
}
