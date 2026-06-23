//! `_read<Type>` readers for the `primnodes.h` expression family. Each reader
//! reconstructs the post-analysis [`::nodes::primnodes::Expr`] form (carried
//! by `Node::Expr`) — that is what `readfuncs.c` builds for every framed `{LABEL
//! ...}` whose tag is a primitive-expression node, both for the executor and the
//! raw-grammar variants (they share the same LABEL). Fields are read in the
//! exact order the OUT side wrote them.
//!
//! The family-private field readers below mirror the `READ_*_FIELD` macros using
//! the public `::nodes_core::read` tokenizer (`pg_strtok` / `node_read` /
//! `debackslash`) and the crate-level helpers (`read_int_field`, `read_oid_field`,
//! …). They do not reuse `lib.rs`'s private `read_string_field` / Expr-list
//! readers because those are not exported; the local copies are byte-identical.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgString};
use ::types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::primnodes::{
    self as pn, ArrayCoerceExpr, ArrayExpr, BoolTestType, BooleanTest, CaseExpr, CaseTestExpr,
    CaseWhen, CoalesceExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO, CoercionForm,
    CollateExpr, CompareType, ConvertRowtypeExpr, CurrentOfExpr, Expr, FieldSelect, FieldStore,
    GroupingFunc, InferenceElem, JsonBehavior, JsonBehaviorType, JsonConstructorExpr,
    JsonConstructorType, JsonEncoding, JsonExpr, JsonExprOp, JsonFormat, JsonFormatType,
    JsonIsPredicate, JsonReturning, JsonValueExpr, JsonValueType, JsonWrapper, MergeSupportFunc,
    MinMaxExpr, MinMaxOp, NamedArgExpr, NextValueExpr, NullTest, NullTestType, RelabelType,
    ReturningExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp, ScalarArrayOpExpr,
    SetToDefault, SubscriptingRef, WindowFunc, XmlExprOp, XmlOptionType,
};

use ::nodes_core::read;

use crate::{
    atoi_i64, atoui_u64, elog_error, read_bool_field, read_enum_field, read_int_field,
    read_location_field, read_oid_field, read_uint_field, tok_str,
};

// ---------------------------------------------------------------------------
// Family-private READ_*_FIELD helpers (mirror readfuncs.c macros).
// ---------------------------------------------------------------------------

fn next_token<'a>() -> PgResult<read::Token<'a>> {
    read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))
}

/// Skip the `:fldname` label, then return the value token.
fn read_field_value<'a>() -> PgResult<read::Token<'a>> {
    let _label = next_token()?;
    next_token()
}

/// `READ_STRING_FIELD` (readfuncs.c `nullable_string`): `<>` → `None`; `""` →
/// empty; else `debackslash`.
fn read_string_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgString<'mcx>>> {
    let v = read_field_value()?;
    if v.bytes.is_empty() {
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(PgString::from_str_in("", mcx)?));
    }
    let s = read::debackslash(v.bytes);
    Ok(Some(PgString::from_str_in(&s, mcx)?))
}

/// `READ_STRING_FIELD` returning an owned `String` (for the lifetime-free `Expr`
/// structs whose `char *` fields are `Option<String>`).
fn read_owned_string_field() -> PgResult<Option<String>> {
    let v = read_field_value()?;
    if v.bytes.is_empty() {
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(String::new()));
    }
    Ok(Some(read::debackslash(v.bytes)))
}

/// Unwrap a `node_read` result to a `Box<Expr>` (the C `Node *` child is an
/// `Expr` here); `<>` is `None`.
fn node_to_opt_box_expr<'mcx>(
    read: Option<PgBox<'_, Node<'mcx>>>,
) -> PgResult<Option<Box<Expr<'mcx>>>> {
    match read {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => Ok(Some(Box::new(e))),
                None => Err(elog_error(alloc::format!(
                "expected Expr child, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` of a single optional child `Expr`: skip label, `node_read`.
fn read_opt_box_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<Box<Expr>>> {
    let _label = next_token()?;
    node_to_opt_box_expr(read::node_read(mcx, None)?)
}

/// `READ_NODE_FIELD` of a `List *` of `Expr`: skip label, `node_read`; a `<>`
/// (NIL) is the empty list. Each list element is an `Expr`.
fn read_expr_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Expr<'mcx>>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(Vec::new()),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elements) => {
                let mut out = Vec::with_capacity(elements.len());
                for cell in elements {
                    {
            let __n = PgBox::into_inner(cell);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(e),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected Expr in list, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Ok(out)
            },
                None => Err(elog_error(alloc::format!(
                "expected List, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` of a `List *` of `Expr` that may carry NULL elements (the
/// slice case of `SubscriptingRef.ref{upper,lower}indexpr`). A `<>` element is a
/// C NULL list element. `<>` for the whole field is the empty list.
fn read_opt_expr_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Option<Expr>>> {
    let _label = next_token()?;
    // The list opens with '(' (handled by node_read) — but node_read rejects
    // NULL list elements. Parse the `( ... )` form by hand: peek the first token.
    let first = next_token()?;
    if first.bytes != b"(" {
        if first.bytes.is_empty() {
            return Ok(Vec::new()); // `<>` NIL
        }
        return Err(elog_error("expected '(' for opt-expr list"));
    }
    let mut out: Vec<Option<Expr>> = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        if t.bytes.is_empty() {
            // `<>` — a NULL element.
            out.push(None);
            continue;
        }
        // A `{...}`-framed Expr element: feed the pre-read token to node_read.
        let child = read::node_read(mcx, Some(t))?;
        match child {
            None => out.push(None),
            Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(Some(e)),
                None => {
                    return Err(elog_error(alloc::format!(
                        "expected Expr in opt-expr list, got {:?}",
                        __tag
                    )))
                },
            }
        },
        }
    }
    Ok(out)
}

/// Read a `(i v1 v2 ...)` integer list (the `_outList` T_IntList form). Skips
/// the `:fldname` label first. `<>` (NIL) → empty.
fn read_int_list_field() -> PgResult<Vec<i32>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(Vec::new());
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for int list"));
    }
    let tag = next_token()?;
    if tag.bytes != b"i" {
        return Err(elog_error("expected 'i' for int list"));
    }
    let mut out = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        out.push(atoi_i64(&tok_str(&t)) as i32);
    }
    Ok(out)
}

/// Read a `(o v1 v2 ...)` OID list (the `_outList` T_OidList form). `<>` → empty.
fn read_oid_list_field() -> PgResult<Vec<u32>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(Vec::new());
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for oid list"));
    }
    let tag = next_token()?;
    if tag.bytes != b"o" {
        return Err(elog_error("expected 'o' for oid list"));
    }
    let mut out = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        out.push(atoui_u64(&tok_str(&t)) as u32);
    }
    Ok(out)
}

/// Read a `( "a" "b" ... )` String value-node list into `Vec<String>` (e.g.
/// `RowExpr.colnames`). `<>` → empty.
fn read_string_list_field() -> PgResult<Vec<String>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(Vec::new());
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for string list"));
    }
    let mut out = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        // Each element is a `_outString` token: `"..."` (already a single token
        // because pg_strtok keeps a quoted string whole) → debackslash inner.
        let s = read::debackslash(t.bytes);
        // Strip the surrounding quotes added by `_outString`.
        let trimmed = s.strip_prefix('"').and_then(|x| x.strip_suffix('"')).unwrap_or(&s);
        out.push(String::from(trimmed));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Enum decoders (the READ_ENUM_FIELD `(enumtype) atoi` casts).
// ---------------------------------------------------------------------------

fn coercion_form_from(c: i32) -> CoercionForm {
    match c {
        0 => CoercionForm::COERCE_EXPLICIT_CALL,
        1 => CoercionForm::COERCE_EXPLICIT_CAST,
        2 => CoercionForm::COERCE_IMPLICIT_CAST,
        _ => CoercionForm::COERCE_SQL_SYNTAX,
    }
}

pub(crate) fn sublink_type_from(c: i32) -> pn::SubLinkType {
    match c {
        0 => pn::SubLinkType::Exists,
        1 => pn::SubLinkType::All,
        2 => pn::SubLinkType::Any,
        3 => pn::SubLinkType::RowCompare,
        4 => pn::SubLinkType::Expr,
        5 => pn::SubLinkType::MultiExpr,
        6 => pn::SubLinkType::Array,
        _ => pn::SubLinkType::Cte,
    }
}

fn compare_type_from(c: i32) -> CompareType {
    match c {
        1 => CompareType::COMPARE_LT,
        2 => CompareType::COMPARE_LE,
        3 => CompareType::COMPARE_EQ,
        4 => CompareType::COMPARE_GE,
        5 => CompareType::COMPARE_GT,
        6 => CompareType::COMPARE_NE,
        _ => CompareType::COMPARE_INVALID,
    }
}

fn minmax_op_from(c: i32) -> MinMaxOp {
    match c {
        1 => MinMaxOp::IS_LEAST,
        _ => MinMaxOp::IS_GREATEST,
    }
}

fn null_test_type_from(c: i32) -> NullTestType {
    match c {
        1 => NullTestType::IS_NOT_NULL,
        _ => NullTestType::IS_NULL,
    }
}

fn bool_test_type_from(c: i32) -> BoolTestType {
    match c {
        0 => BoolTestType::IS_TRUE,
        1 => BoolTestType::IS_NOT_TRUE,
        2 => BoolTestType::IS_FALSE,
        3 => BoolTestType::IS_NOT_FALSE,
        4 => BoolTestType::IS_UNKNOWN,
        _ => BoolTestType::IS_NOT_UNKNOWN,
    }
}

fn sqlvalue_op_from(c: i32) -> SQLValueFunctionOp {
    use SQLValueFunctionOp::*;
    match c {
        0 => SVFOP_CURRENT_DATE,
        1 => SVFOP_CURRENT_TIME,
        2 => SVFOP_CURRENT_TIME_N,
        3 => SVFOP_CURRENT_TIMESTAMP,
        4 => SVFOP_CURRENT_TIMESTAMP_N,
        5 => SVFOP_LOCALTIME,
        6 => SVFOP_LOCALTIME_N,
        7 => SVFOP_LOCALTIMESTAMP,
        8 => SVFOP_LOCALTIMESTAMP_N,
        9 => SVFOP_CURRENT_ROLE,
        10 => SVFOP_CURRENT_USER,
        11 => SVFOP_USER,
        12 => SVFOP_SESSION_USER,
        13 => SVFOP_CURRENT_CATALOG,
        _ => SVFOP_CURRENT_SCHEMA,
    }
}

fn xml_expr_op_from(c: i32) -> XmlExprOp {
    use XmlExprOp::*;
    match c {
        0 => IS_XMLCONCAT,
        1 => IS_XMLELEMENT,
        2 => IS_XMLFOREST,
        3 => IS_XMLPARSE,
        4 => IS_XMLPI,
        5 => IS_XMLROOT,
        6 => IS_XMLSERIALIZE,
        _ => IS_DOCUMENT,
    }
}

fn xml_option_from(c: i32) -> XmlOptionType {
    match c {
        1 => XmlOptionType::XMLOPTION_CONTENT,
        _ => XmlOptionType::XMLOPTION_DOCUMENT,
    }
}

fn json_format_type_from(c: i32) -> JsonFormatType {
    match c {
        1 => JsonFormatType::JS_FORMAT_JSON,
        2 => JsonFormatType::JS_FORMAT_JSONB,
        _ => JsonFormatType::JS_FORMAT_DEFAULT,
    }
}

fn json_encoding_from(c: i32) -> JsonEncoding {
    match c {
        1 => JsonEncoding::JS_ENC_UTF8,
        2 => JsonEncoding::JS_ENC_UTF16,
        3 => JsonEncoding::JS_ENC_UTF32,
        _ => JsonEncoding::JS_ENC_DEFAULT,
    }
}

fn json_ctor_type_from(c: i32) -> JsonConstructorType {
    use JsonConstructorType::*;
    match c {
        1 => JSCTOR_JSON_OBJECT,
        2 => JSCTOR_JSON_ARRAY,
        3 => JSCTOR_JSON_OBJECTAGG,
        4 => JSCTOR_JSON_ARRAYAGG,
        5 => JSCTOR_JSON_PARSE,
        6 => JSCTOR_JSON_SCALAR,
        _ => JSCTOR_JSON_SERIALIZE,
    }
}

fn json_value_type_from(c: i32) -> JsonValueType {
    use JsonValueType::*;
    match c {
        1 => JS_TYPE_OBJECT,
        2 => JS_TYPE_ARRAY,
        3 => JS_TYPE_SCALAR,
        _ => JS_TYPE_ANY,
    }
}

fn json_expr_op_from(c: i32) -> JsonExprOp {
    use JsonExprOp::*;
    match c {
        1 => JSON_QUERY_OP,
        2 => JSON_VALUE_OP,
        3 => JSON_TABLE_OP,
        _ => JSON_EXISTS_OP,
    }
}

fn json_wrapper_from(c: i32) -> JsonWrapper {
    use JsonWrapper::*;
    match c {
        1 => JSW_NONE,
        2 => JSW_CONDITIONAL,
        3 => JSW_UNCONDITIONAL,
        _ => JSW_UNSPEC,
    }
}

fn json_behavior_type_from(c: i32) -> JsonBehaviorType {
    use JsonBehaviorType::*;
    match c {
        1 => JSON_BEHAVIOR_ERROR,
        2 => JSON_BEHAVIOR_EMPTY,
        3 => JSON_BEHAVIOR_TRUE,
        4 => JSON_BEHAVIOR_FALSE,
        5 => JSON_BEHAVIOR_UNKNOWN,
        6 => JSON_BEHAVIOR_EMPTY_ARRAY,
        7 => JSON_BEHAVIOR_EMPTY_OBJECT,
        8 => JSON_BEHAVIOR_DEFAULT,
        _ => JSON_BEHAVIOR_NULL,
    }
}

// ---------------------------------------------------------------------------
// JSON support-node readers (an optional framed `{JSONFORMAT ...}` etc.).
// ---------------------------------------------------------------------------

/// Read an optional framed child node and require it to be the given LABEL,
/// reconstructing it via this family's reader. `<>` → `None`. Used for the
/// JsonFormat / JsonReturning / JsonBehavior `WRITE_NODE_FIELD` children.
fn read_opt_json_format() -> PgResult<Option<JsonFormat>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None); // `<>`
    }
    if open.bytes != b"{" {
        return Err(elog_error("expected '{' for JsonFormat"));
    }
    let tag = next_token()?;
    if tag.bytes != b"JSONFORMAT" {
        return Err(elog_error("expected JSONFORMAT label"));
    }
    let f = read_json_format_body()?;
    let close = next_token()?;
    if close.bytes != b"}" {
        return Err(elog_error("expected '}' after JsonFormat"));
    }
    Ok(Some(f))
}

fn read_json_format_body() -> PgResult<JsonFormat> {
    let format_type = json_format_type_from(read_enum_field()?);
    let encoding = json_encoding_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(JsonFormat {
        format_type,
        encoding,
        location,
    })
}

fn read_opt_json_returning() -> PgResult<Option<JsonReturning>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None);
    }
    if open.bytes != b"{" {
        return Err(elog_error("expected '{' for JsonReturning"));
    }
    let tag = next_token()?;
    if tag.bytes != b"JSONRETURNING" {
        return Err(elog_error("expected JSONRETURNING label"));
    }
    // body: format (opt JsonFormat), typid, typmod.
    let format = read_opt_json_format()?;
    let typid = read_oid_field()?;
    let typmod = read_int_field()?;
    let close = next_token()?;
    if close.bytes != b"}" {
        return Err(elog_error("expected '}' after JsonReturning"));
    }
    Ok(Some(JsonReturning {
        format,
        typid,
        typmod,
    }))
}

fn read_opt_json_behavior<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<Box<JsonBehavior>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None);
    }
    if open.bytes != b"{" {
        return Err(elog_error("expected '{' for JsonBehavior"));
    }
    let tag = next_token()?;
    if tag.bytes != b"JSONBEHAVIOR" {
        return Err(elog_error("expected JSONBEHAVIOR label"));
    }
    let btype = json_behavior_type_from(read_enum_field()?);
    let expr = read_opt_box_expr(mcx)?;
    let coerce = read_bool_field()?;
    let location = read_location_field()?;
    let close = next_token()?;
    if close.bytes != b"}" {
        return Err(elog_error("expected '}' after JsonBehavior"));
    }
    Ok(Some(Box::new(JsonBehavior {
        btype,
        expr,
        coerce,
        location,
    })))
}

// ---------------------------------------------------------------------------
// Per-tag `_read<Type>` readers (build the post-analysis Expr form).
// ---------------------------------------------------------------------------

fn read_grouping_func<'mcx>(mcx: Mcx<'mcx>) -> PgResult<GroupingFunc> {
    let args = read_expr_list_field(mcx)?;
    let refs = read_int_list_field()?;
    let cols = read_int_list_field()?;
    let agglevelsup = read_uint_field()?;
    let location = read_location_field()?;
    Ok(GroupingFunc {
        args,
        refs,
        cols,
        agglevelsup,
        location,
    })
}

fn read_window_func<'mcx>(mcx: Mcx<'mcx>) -> PgResult<WindowFunc> {
    let winfnoid = read_oid_field()?;
    let wintype = read_oid_field()?;
    let wincollid = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let args = read_expr_list_field(mcx)?;
    let aggfilter = read_opt_box_expr(mcx)?;
    let run_condition = read_expr_list_field(mcx)?;
    let winref = read_uint_field()?;
    let winstar = read_bool_field()?;
    let winagg = read_bool_field()?;
    let location = read_location_field()?;
    Ok(WindowFunc {
        winfnoid,
        wintype,
        wincollid,
        inputcollid,
        args,
        aggfilter,
        runCondition: run_condition,
        winref,
        winstar,
        winagg,
        location,
    })
}

fn read_window_func_run_condition<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::primnodes::WindowFuncRunCondition<'mcx>> {
    let opno = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let wfunc_left = read_bool_field()?;
    let arg = read_opt_box_expr(mcx)?;
    Ok(::nodes::primnodes::WindowFuncRunCondition {
        opno,
        inputcollid,
        wfunc_left,
        arg,
    })
}

fn read_merge_support_func() -> PgResult<MergeSupportFunc> {
    let msftype = read_oid_field()?;
    let msfcollid = read_oid_field()?;
    let location = read_location_field()?;
    Ok(MergeSupportFunc {
        msftype,
        msfcollid,
        location,
    })
}

fn read_subscripting_ref<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SubscriptingRef> {
    let refcontainertype = read_oid_field()?;
    let refelemtype = read_oid_field()?;
    let refrestype = read_oid_field()?;
    let reftypmod = read_int_field()?;
    let refcollid = read_oid_field()?;
    let refupperindexpr = read_opt_expr_list_field(mcx)?;
    let reflowerindexpr = read_opt_expr_list_field(mcx)?;
    let refexpr = read_opt_box_expr(mcx)?;
    let refassgnexpr = read_opt_box_expr(mcx)?;
    Ok(SubscriptingRef {
        refcontainertype,
        refelemtype,
        refrestype,
        reftypmod,
        refcollid,
        refupperindexpr,
        reflowerindexpr,
        refexpr,
        refassgnexpr,
    })
}

fn read_named_arg_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<NamedArgExpr> {
    let arg = read_opt_box_expr(mcx)?;
    let name = read_owned_string_field()?;
    let argnumber = read_int_field()?;
    let location = read_location_field()?;
    Ok(NamedArgExpr {
        arg,
        name,
        argnumber,
        location,
    })
}

fn read_scalar_array_op_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ScalarArrayOpExpr> {
    let opno = read_oid_field()?;
    let opfuncid = read_oid_field()?;
    let hashfuncid = read_oid_field()?;
    let negfuncid = read_oid_field()?;
    let use_or = read_bool_field()?;
    let inputcollid = read_oid_field()?;
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(ScalarArrayOpExpr {
        opno,
        opfuncid,
        hashfuncid,
        negfuncid,
        useOr: use_or,
        inputcollid,
        args,
        location,
    })
}

fn read_field_select<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FieldSelect> {
    let arg = read_opt_box_expr(mcx)?;
    let fieldnum = read_int_field()? as i16;
    let resulttype = read_oid_field()?;
    let resulttypmod = read_int_field()?;
    let resultcollid = read_oid_field()?;
    Ok(FieldSelect {
        arg,
        fieldnum,
        resulttype,
        resulttypmod,
        resultcollid,
    })
}

fn read_field_store<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FieldStore> {
    let arg = read_opt_box_expr(mcx)?;
    let newvals = read_expr_list_field(mcx)?;
    let fieldnums = read_int_list_field()?.into_iter().map(|v| v as i16).collect();
    let resulttype = read_oid_field()?;
    Ok(FieldStore {
        arg,
        newvals,
        fieldnums,
        resulttype,
    })
}

fn read_relabel_type<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RelabelType> {
    let arg = read_opt_box_expr(mcx)?;
    let resulttype = read_oid_field()?;
    let resulttypmod = read_int_field()?;
    let resultcollid = read_oid_field()?;
    let relabelformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(RelabelType {
        arg,
        resulttype,
        resulttypmod,
        resultcollid,
        relabelformat,
        location,
    })
}

fn read_coerce_via_io<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CoerceViaIO> {
    let arg = read_opt_box_expr(mcx)?;
    let resulttype = read_oid_field()?;
    let resultcollid = read_oid_field()?;
    let coerceformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(CoerceViaIO {
        arg,
        resulttype,
        resultcollid,
        coerceformat,
        location,
    })
}

fn read_array_coerce_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ArrayCoerceExpr> {
    let arg = read_opt_box_expr(mcx)?;
    let elemexpr = read_opt_box_expr(mcx)?;
    let resulttype = read_oid_field()?;
    let resulttypmod = read_int_field()?;
    let resultcollid = read_oid_field()?;
    let coerceformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(ArrayCoerceExpr {
        arg,
        elemexpr,
        resulttype,
        resulttypmod,
        resultcollid,
        coerceformat,
        location,
    })
}

fn read_convert_rowtype_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ConvertRowtypeExpr> {
    let arg = read_opt_box_expr(mcx)?;
    let resulttype = read_oid_field()?;
    let convertformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(ConvertRowtypeExpr {
        arg,
        resulttype,
        convertformat,
        location,
    })
}

fn read_collate_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CollateExpr> {
    let arg = read_opt_box_expr(mcx)?;
    let coll_oid = read_oid_field()?;
    let location = read_location_field()?;
    Ok(CollateExpr {
        arg,
        collOid: coll_oid,
        location,
    })
}

fn read_case_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CaseExpr> {
    let casetype = read_oid_field()?;
    let casecollid = read_oid_field()?;
    let arg = read_opt_box_expr(mcx)?;
    // args is a `List *` of CaseWhen (framed `{CASEWHEN ...}` children).
    let args = read_case_when_list(mcx)?;
    let defresult = read_opt_box_expr(mcx)?;
    let location = read_location_field()?;
    Ok(CaseExpr {
        casetype,
        casecollid,
        arg,
        args,
        defresult,
        location,
    })
}

/// Read the `:args ( {CASEWHEN ...} ... )` field of a CaseExpr.
fn read_case_when_list<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<CaseWhen>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(Vec::new());
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for CaseExpr args"));
    }
    let mut out = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        if t.bytes != b"{" {
            return Err(elog_error("expected '{' for CaseWhen"));
        }
        let tag = next_token()?;
        if tag.bytes != b"CASEWHEN" {
            return Err(elog_error("expected CASEWHEN label"));
        }
        let expr = read_opt_box_expr(mcx)?;
        let result = read_opt_box_expr(mcx)?;
        let location = read_location_field()?;
        let close = next_token()?;
        if close.bytes != b"}" {
            return Err(elog_error("expected '}' after CaseWhen"));
        }
        out.push(CaseWhen {
            expr,
            result,
            location,
        });
    }
    Ok(out)
}

fn read_case_test_expr() -> PgResult<CaseTestExpr> {
    let type_id = read_oid_field()?;
    let type_mod = read_int_field()?;
    let collation = read_oid_field()?;
    Ok(CaseTestExpr {
        typeId: type_id,
        typeMod: type_mod,
        collation,
    })
}

fn read_array_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ArrayExpr> {
    let array_typeid = read_oid_field()?;
    let array_collid = read_oid_field()?;
    let element_typeid = read_oid_field()?;
    let elements = read_expr_list_field(mcx)?;
    let multidims = read_bool_field()?;
    // list_start / list_end are written by OUT but trimmed from the repo struct
    // (query-jumble-only). Consume them (read as locations → -1, discarded).
    let _list_start = read_location_field()?;
    let _list_end = read_location_field()?;
    let location = read_location_field()?;
    Ok(ArrayExpr {
        array_typeid,
        array_collid,
        element_typeid,
        elements,
        multidims,
        location,
    })
}

fn read_row_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RowExpr> {
    let args = read_expr_list_field(mcx)?;
    let row_typeid = read_oid_field()?;
    let row_format = coercion_form_from(read_enum_field()?);
    let colnames = read_string_list_field()?;
    let location = read_location_field()?;
    Ok(RowExpr {
        args,
        row_typeid,
        row_format,
        colnames,
        location,
    })
}

fn read_row_compare_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RowCompareExpr> {
    let cmptype = compare_type_from(read_enum_field()?);
    let opnos = read_oid_list_field()?;
    let opfamilies = read_oid_list_field()?;
    let inputcollids = read_oid_list_field()?;
    let largs = read_expr_list_field(mcx)?;
    let rargs = read_expr_list_field(mcx)?;
    Ok(RowCompareExpr {
        cmptype,
        opnos,
        opfamilies,
        inputcollids,
        largs,
        rargs,
    })
}

fn read_coalesce_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CoalesceExpr> {
    let coalescetype = read_oid_field()?;
    let coalescecollid = read_oid_field()?;
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(CoalesceExpr {
        coalescetype,
        coalescecollid,
        args,
        location,
    })
}

fn read_minmax_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<MinMaxExpr> {
    let minmaxtype = read_oid_field()?;
    let minmaxcollid = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let op = minmax_op_from(read_enum_field()?);
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(MinMaxExpr {
        minmaxtype,
        minmaxcollid,
        inputcollid,
        op,
        args,
        location,
    })
}

fn read_sqlvalue_function() -> PgResult<SQLValueFunction> {
    let op = sqlvalue_op_from(read_enum_field()?);
    let r#type = read_oid_field()?;
    let typmod = read_int_field()?;
    let location = read_location_field()?;
    Ok(SQLValueFunction {
        op,
        r#type,
        typmod,
        location,
    })
}

fn read_xml_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<pn::XmlExpr> {
    let op = xml_expr_op_from(read_enum_field()?);
    let name = read_owned_string_field()?;
    let named_args = read_expr_list_field(mcx)?;
    let arg_names = read_string_list_field()?;
    let args = read_expr_list_field(mcx)?;
    let xmloption = xml_option_from(read_enum_field()?);
    let indent = read_bool_field()?;
    let r#type = read_oid_field()?;
    let typmod = read_int_field()?;
    let location = read_location_field()?;
    Ok(pn::XmlExpr {
        op,
        name,
        named_args,
        arg_names,
        args,
        xmloption,
        indent,
        r#type,
        typmod,
        location,
    })
}

fn read_json_value_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<JsonValueExpr> {
    let raw_expr = read_opt_box_expr(mcx)?;
    let formatted_expr = read_opt_box_expr(mcx)?;
    let format = read_opt_json_format()?;
    Ok(JsonValueExpr {
        raw_expr,
        formatted_expr,
        format,
    })
}

fn read_json_constructor_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<JsonConstructorExpr> {
    let r#type = json_ctor_type_from(read_enum_field()?);
    let args = read_expr_list_field(mcx)?;
    let func = read_opt_box_expr(mcx)?;
    let coercion = read_opt_box_expr(mcx)?;
    let returning = read_opt_json_returning()?;
    let absent_on_null = read_bool_field()?;
    let unique = read_bool_field()?;
    let location = read_location_field()?;
    Ok(JsonConstructorExpr {
        r#type,
        args,
        func,
        coercion,
        returning,
        absent_on_null,
        unique,
        location,
    })
}

fn read_json_is_predicate<'mcx>(mcx: Mcx<'mcx>) -> PgResult<JsonIsPredicate> {
    let expr = read_opt_box_expr(mcx)?;
    let format = read_opt_json_format()?;
    let item_type = json_value_type_from(read_enum_field()?);
    let unique_keys = read_bool_field()?;
    let location = read_location_field()?;
    Ok(JsonIsPredicate {
        expr,
        format,
        item_type,
        unique_keys,
        location,
    })
}

fn read_json_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<JsonExpr> {
    let op = json_expr_op_from(read_enum_field()?);
    let column_name = read_owned_string_field()?;
    let formatted_expr = read_opt_box_expr(mcx)?;
    let format = read_opt_json_format()?;
    let path_spec = read_opt_box_expr(mcx)?;
    let returning = read_opt_json_returning()?;
    let passing_names = read_string_list_field()?;
    let passing_values = read_expr_list_field(mcx)?;
    let on_empty = read_opt_json_behavior(mcx)?;
    let on_error = read_opt_json_behavior(mcx)?;
    let use_io_coercion = read_bool_field()?;
    let use_json_coercion = read_bool_field()?;
    let wrapper = json_wrapper_from(read_enum_field()?);
    let omit_quotes = read_bool_field()?;
    let collation = read_oid_field()?;
    let location = read_location_field()?;
    Ok(JsonExpr {
        op,
        column_name,
        formatted_expr,
        format,
        path_spec,
        returning,
        passing_names,
        passing_values,
        on_empty,
        on_error,
        use_io_coercion,
        use_json_coercion,
        wrapper,
        omit_quotes,
        collation,
        location,
    })
}

fn read_null_test<'mcx>(mcx: Mcx<'mcx>) -> PgResult<NullTest> {
    let arg = read_opt_box_expr(mcx)?;
    let nulltesttype = null_test_type_from(read_enum_field()?);
    let argisrow = read_bool_field()?;
    let location = read_location_field()?;
    Ok(NullTest {
        arg,
        nulltesttype,
        argisrow,
        location,
    })
}

fn read_boolean_test<'mcx>(mcx: Mcx<'mcx>) -> PgResult<BooleanTest> {
    let arg = read_opt_box_expr(mcx)?;
    let booltesttype = bool_test_type_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(BooleanTest {
        arg,
        booltesttype,
        location,
    })
}

fn read_coerce_to_domain<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CoerceToDomain> {
    let arg = read_opt_box_expr(mcx)?;
    let resulttype = read_oid_field()?;
    let resulttypmod = read_int_field()?;
    let resultcollid = read_oid_field()?;
    let coercionformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(CoerceToDomain {
        arg,
        resulttype,
        resulttypmod,
        resultcollid,
        coercionformat,
        location,
    })
}

fn read_coerce_to_domain_value() -> PgResult<CoerceToDomainValue> {
    let type_id = read_oid_field()?;
    let type_mod = read_int_field()?;
    let collation = read_oid_field()?;
    let location = read_location_field()?;
    Ok(CoerceToDomainValue {
        typeId: type_id,
        typeMod: type_mod,
        collation,
        location,
    })
}

fn read_set_to_default() -> PgResult<SetToDefault> {
    let type_id = read_oid_field()?;
    let type_mod = read_int_field()?;
    let collation = read_oid_field()?;
    let location = read_location_field()?;
    Ok(SetToDefault {
        typeId: type_id,
        typeMod: type_mod,
        collation,
        location,
    })
}

fn read_current_of_expr() -> PgResult<CurrentOfExpr> {
    let cvarno = read_uint_field()?;
    let cursor_name = read_owned_string_field()?;
    let cursor_param = read_int_field()?;
    Ok(CurrentOfExpr {
        cvarno,
        cursor_name,
        cursor_param,
    })
}

fn read_next_value_expr() -> PgResult<NextValueExpr> {
    let seqid = read_oid_field()?;
    let type_id = read_oid_field()?;
    Ok(NextValueExpr {
        seqid,
        typeId: type_id,
    })
}

fn read_inference_elem<'mcx>(mcx: Mcx<'mcx>) -> PgResult<InferenceElem> {
    let expr = read_opt_box_expr(mcx)?;
    let infercollid = read_oid_field()?;
    let inferopclass = read_oid_field()?;
    Ok(InferenceElem {
        expr,
        infercollid,
        inferopclass,
    })
}

fn read_returning_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ReturningExpr> {
    let retlevelsup = read_int_field()?;
    let retold = read_bool_field()?;
    let retexpr = read_opt_box_expr(mcx)?;
    Ok(ReturningExpr {
        retlevelsup,
        retold,
        retexpr,
    })
}

fn read_sublink<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::primnodes::SubLink<'mcx>> {
    let sub_link_type = sublink_type_from(read_enum_field()?);
    let sub_link_id = read_int_field()?;
    let testexpr = read_opt_box_expr(mcx)?;
    // operName — a `List *` of String (the ALL/ANY/ROWCOMPARE operator name, e.g.
    // `("=")`). It SURVIVES parse-analysis, so stored `_RETURN` rules embed a
    // non-NIL list here; reading only one token (assuming `<>`) misaligns the
    // shared cursor on `("=")` and surfaces as "unexpected right parenthesis".
    // `read_string_list_field` consumes `<>` (→ empty Vec) or `(...)` (incl. its
    // closing `)`), mirroring C's `READ_NODE_FIELD(operName)`.
    let oper_name = read_string_list_field()?;
    // subselect — the embedded Query. The core `node_read` reconstructs the
    // framed `{QUERY ...}` child into an mcx-owned `Node::Query`; box it and
    // re-erase the lifetime parameter to the lifetime-free `Expr` tree's
    // 'static notional lifetime (the exact idiom SubLink::clone_in uses; cf.
    // clone_sublink in types-::nodes::primnodes). A `<>` (NULL) → None.
    let _label = next_token()?; // skip :subselect
    let sub = read::node_read(mcx, None)?;
    let subselect = match sub {
        None => None,
        Some(boxed) => match PgBox::into_inner(boxed).into_query() {
            Some(q) => {
                // The sub-`Query` is read into `mcx`; box it in `mcx` and keep the
                // `'mcx` lifetime to match the `SubLink<'mcx>.subselect` field (no
                // `'static` erasure — the node is arena-bound to `mcx`).
                Some(::mcx::alloc_in(mcx, q)?)
            }
            None => {
                return Err(elog_error(
                    "_readSubLink: expected a QUERY node for SubLink.subselect",
                ))
            }
        },
    };
    let location = read_location_field()?;
    Ok(::nodes::primnodes::SubLink {
        subLinkType: sub_link_type,
        subLinkId: sub_link_id,
        testexpr,
        operName: oper_name,
        subselect,
        location,
    })
}

/// Read a framed `( {TARGETENTRY ...} ... )` list field into the
/// `Vec<TargetEntry<'static>>` carrier of [`pn::Aggref::args`]. Each element is a
/// framed `TargetEntry` reconstructed via `node_read`; its mcx-allocated children
/// are fully owned in `mcx`, so the `'mcx` → `'static` erase is a
/// lifetime-parameter-only transmute (the exact idiom `clone_aggref` /
/// `tlist_into_static` use). `<>`/`()` → empty.
fn read_aggref_args_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<pn::TargetEntry<'mcx>>> {
    let _label = next_token()?;
    let elements = match read::node_read(mcx, None)? {
        None => return Ok(Vec::new()),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => elems,
                None => {
                    return Err(elog_error(alloc::format!(
                        "expected List for Aggref.args, got {:?}",
                        __tag
                    )))
                }
            }
        }
    };
    let mut out: Vec<pn::TargetEntry<'mcx>> = Vec::with_capacity(elements.len());
    for cell in elements {
        let __n = PgBox::into_inner(cell);
        let __tag = __n.node_tag();
        match __n.into_targetentry() {
            Some(te) => {
                // The TargetEntry's children (expr/resname) are fully owned in
                // `mcx`; keep the `'mcx` lifetime to match `Aggref<'mcx>.args`
                // (no `'static` erasure — the node is arena-bound to `mcx`).
                out.push(te);
            }
            None => {
                return Err(elog_error(alloc::format!(
                    "expected TargetEntry in Aggref.args, got {:?}",
                    __tag
                )))
            }
        }
    }
    Ok(out)
}

/// Read a framed `( {SORTGROUPCLAUSE ...} ... )` list field (Aggref's
/// `aggorder`/`aggdistinct`) into `Vec<SortGroupClause>`. Each element is a
/// framed `SortGroupClause` reconstructed via `node_read`. `<>`/`()` → empty.
fn read_sortgroupclause_list_field<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<::nodes::rawnodes::SortGroupClause>> {
    let _label = next_token()?;
    let elements = match read::node_read(mcx, None)? {
        None => return Ok(Vec::new()),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => elems,
                None => {
                    return Err(elog_error(alloc::format!(
                        "expected List for SortGroupClause field, got {:?}",
                        __tag
                    )))
                }
            }
        }
    };
    let mut out = Vec::with_capacity(elements.len());
    for cell in elements {
        let __n = PgBox::into_inner(cell);
        if __n.is_sortgroupclause() {
            out.push(__n.into_sortgroupclause().unwrap());
        } else {
            return Err(elog_error(alloc::format!(
                "expected SortGroupClause in list, got {:?}",
                __n.node_tag()
            )));
        }
    }
    Ok(out)
}

/// `_readAggref` (readfuncs.funcs.c). Reads every field in the exact order
/// [`crate::out_expr_family::out_aggref`] (i.e. `_outAggref`) wrote them. The
/// `aggno`/`aggtransno`/`aggpresorted` fields carry `read_write_ignore` in C and
/// are NOT serialized — they default (0/0/false) as `makeNode` would zero them.
fn read_aggref<'mcx>(mcx: Mcx<'mcx>) -> PgResult<pn::Aggref<'mcx>> {
    let aggfnoid = read_oid_field()?;
    let aggtype = read_oid_field()?;
    let aggcollid = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let aggtranstype = read_oid_field()?;
    let aggargtypes = read_oid_list_field()?;
    let aggdirectargs = read_expr_list_field(mcx)?;
    let args = read_aggref_args_field(mcx)?;
    let aggorder = read_sortgroupclause_list_field(mcx)?;
    let aggdistinct = read_sortgroupclause_list_field(mcx)?;
    let aggfilter = read_opt_box_expr(mcx)?;
    let aggstar = read_bool_field()?;
    let aggvariadic = read_bool_field()?;
    let aggkind = crate::read_char_field()? as i8;
    // PG18 `_outAggref` writes aggpresorted/aggno/aggtransno (they are NOT
    // read_write_ignore — outfuncs.funcs.c:154,157,158). Reading them in struct
    // order keeps the shared cursor aligned; omitting them left `:aggno -1
    // :aggtransno -1` unconsumed → "did not find '}'" on stored agg view rules.
    let aggpresorted = read_bool_field()?;
    let agglevelsup = read_uint_field()?;
    let aggsplit = read_enum_field()?;
    let aggno = read_int_field()?;
    let aggtransno = read_int_field()?;
    let location = read_location_field()?;
    Ok(pn::Aggref {
        aggfnoid,
        aggtype,
        aggcollid,
        inputcollid,
        aggtranstype,
        aggargtypes,
        aggdirectargs,
        args,
        aggorder,
        aggdistinct,
        aggfilter,
        aggstar,
        aggvariadic,
        aggkind,
        aggpresorted,
        agglevelsup,
        aggsplit,
        aggno,
        aggtransno,
        location,
    })
}

/// Dispatch the read_expr_family LABELs this module owns. Reconstructs the
/// post-analysis `Node::mk_expr(mcx, Expr::X)` form for each shared LABEL.
pub(crate) fn try_read<'mcx>(mcx: Mcx<'mcx>, label: &[u8]) -> Option<PgResult<Node<'mcx>>> {
    let res: PgResult<Node<'mcx>> = match label {
        b"AGGREF" => read_aggref(mcx).and_then(|n| Node::mk_expr(mcx, Expr::Aggref(n))),
        b"GROUPINGFUNC" => read_grouping_func(mcx).and_then(|n| Node::mk_expr(mcx, Expr::GroupingFunc(n))),
        b"WINDOWFUNC" => read_window_func(mcx).and_then(|n| Node::mk_expr(mcx, Expr::WindowFunc(n))),
        b"WINDOWFUNCRUNCONDITION" => read_window_func_run_condition(mcx)
            .and_then(|n| Node::mk_expr(mcx, Expr::WindowFuncRunCondition(n))),
        b"MERGESUPPORTFUNC" => {
            read_merge_support_func().and_then(|n| Node::mk_expr(mcx, Expr::MergeSupportFunc(n)))
        }
        b"SUBSCRIPTINGREF" => {
            read_subscripting_ref(mcx).and_then(|n| Node::mk_expr(mcx, Expr::SubscriptingRef(n)))
        }
        b"NAMEDARGEXPR" => read_named_arg_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::NamedArgExpr(n))),
        b"SCALARARRAYOPEXPR" => {
            read_scalar_array_op_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::ScalarArrayOpExpr(n)))
        }
        b"SUBLINK" => read_sublink(mcx).and_then(|n| Node::mk_expr(mcx, Expr::SubLink(n))),
        b"FIELDSELECT" => read_field_select(mcx).and_then(|n| Node::mk_expr(mcx, Expr::FieldSelect(n))),
        b"FIELDSTORE" => read_field_store(mcx).and_then(|n| Node::mk_expr(mcx, Expr::FieldStore(n))),
        b"RELABELTYPE" => read_relabel_type(mcx).and_then(|n| Node::mk_expr(mcx, Expr::RelabelType(n))),
        b"COERCEVIAIO" => read_coerce_via_io(mcx).and_then(|n| Node::mk_expr(mcx, Expr::CoerceViaIO(n))),
        b"ARRAYCOERCEEXPR" => {
            read_array_coerce_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::ArrayCoerceExpr(n)))
        }
        b"CONVERTROWTYPEEXPR" => {
            read_convert_rowtype_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::ConvertRowtypeExpr(n)))
        }
        b"COLLATEEXPR" => read_collate_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::CollateExpr(n))),
        b"CASEEXPR" => read_case_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::CaseExpr(n))),
        b"CASETESTEXPR" => read_case_test_expr().and_then(|n| Node::mk_expr(mcx, Expr::CaseTestExpr(n))),
        b"ARRAYEXPR" => read_array_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::ArrayExpr(n))),
        b"ROWEXPR" => read_row_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::RowExpr(n))),
        b"ROWCOMPAREEXPR" => read_row_compare_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::RowCompareExpr(n))),
        b"COALESCEEXPR" => read_coalesce_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::CoalesceExpr(n))),
        b"MINMAXEXPR" => read_minmax_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::MinMaxExpr(n))),
        b"SQLVALUEFUNCTION" => {
            read_sqlvalue_function().and_then(|n| Node::mk_expr(mcx, Expr::SQLValueFunction(n)))
        }
        b"XMLEXPR" => read_xml_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::XmlExpr(n))),
        b"JSONVALUEEXPR" => read_json_value_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::JsonValueExpr(n))),
        b"JSONCONSTRUCTOREXPR" => {
            read_json_constructor_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::JsonConstructorExpr(n)))
        }
        b"JSONISPREDICATE" => {
            read_json_is_predicate(mcx).and_then(|n| Node::mk_expr(mcx, Expr::JsonIsPredicate(n)))
        }
        b"JSONEXPR" => read_json_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::JsonExpr(n))),
        b"NULLTEST" => read_null_test(mcx).and_then(|n| Node::mk_expr(mcx, Expr::NullTest(n))),
        b"BOOLEANTEST" => read_boolean_test(mcx).and_then(|n| Node::mk_expr(mcx, Expr::BooleanTest(n))),
        b"COERCETODOMAIN" => read_coerce_to_domain(mcx).and_then(|n| Node::mk_expr(mcx, Expr::CoerceToDomain(n))),
        b"COERCETODOMAINVALUE" => {
            read_coerce_to_domain_value().and_then(|n| Node::mk_expr(mcx, Expr::CoerceToDomainValue(n)))
        }
        b"SETTODEFAULT" => read_set_to_default().and_then(|n| Node::mk_expr(mcx, Expr::SetToDefault(n))),
        b"CURRENTOFEXPR" => read_current_of_expr().and_then(|n| Node::mk_expr(mcx, Expr::CurrentOfExpr(n))),
        b"NEXTVALUEEXPR" => read_next_value_expr().and_then(|n| Node::mk_expr(mcx, Expr::NextValueExpr(n))),
        b"INFERENCEELEM" => read_inference_elem(mcx).and_then(|n| Node::mk_expr(mcx, Expr::InferenceElem(n))),
        b"RETURNINGEXPR" => read_returning_expr(mcx).and_then(|n| Node::mk_expr(mcx, Expr::ReturningExpr(n))),
        // `_readSubPlan` / `_readAlternativeSubPlan` (readfuncs.funcs.c). The
        // `SubPlanExpr<'mcx>(Box<SubPlan<'mcx>>)` / `AlternativeSubPlanExpr<'mcx>`
        // carriers are lifetime-carrying (the Expr-`'mcx` campaign made the Expr
        // tree honest), so the mcx-allocated `testexpr`/`args` children read off
        // the cursor by `read_plan_family::read_subplan` go straight into the
        // carrier. This unblocks worker plan-deserialization of any plan carrying
        // a hashed/correlated SubPlan (e.g. `(a,b) NOT IN (SELECT ...)` shipped to
        // a parallel worker).
        b"SUBPLAN" => crate::read_plan_family::read_subplan(mcx).and_then(|sp| {
            Node::mk_expr(
                mcx,
                Expr::SubPlan(::nodes::primnodes::SubPlanExpr(Box::new(sp))),
            )
        }),
        b"ALTERNATIVESUBPLAN" => {
            crate::read_plan_family::read_alternative_subplan(mcx).and_then(|asp| {
                Node::mk_expr(
                    mcx,
                    Expr::AlternativeSubPlan(::nodes::primnodes::AlternativeSubPlanExpr(
                        Box::new(asp),
                    )),
                )
            })
        }
        _ => return None,
    };
    Some(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::nodes_core::read::string_to_node;
    use ::outfuncs::out_node;
    use ::mcx::MemoryContext;
    use ::nodes::primnodes::{Expr, Var, VarReturningType};

    extern crate std;
    use std::string::ToString;

    use crate::ensure_seams_for_tests as ensure_seams;

    fn mk_var() -> Var {
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Default::default(),
            varlevelsup: 0,
            varnosyn: 1,
            varattnosyn: 2,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            location: 7,
        }
    }

    /// Parse a framed `{LABEL ...}` string into a `Node` (drives `node_read` →
    /// the installed `parse_node_string` → this family's `try_read`).
    fn parse(s: &str) -> Node<'static> {
        ensure_seams();
        // Leak a context so the returned Node carries 'static for the assertions.
        let ctx = std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new("read-expr-test")));
        let mcx = ctx.mcx();
        let n = string_to_node(mcx, s).expect("string_to_node");
        PgBox::into_inner(n)
    }

    /// `_outAggref`/`_readAggref`: an Aggref carrying an aggregated `args`
    /// TargetEntry (whose expr is a Var), one `aggdistinct` SortGroupClause, and
    /// the scalar fields round-trips through the framed TARGETENTRY /
    /// SORTGROUPCLAUSE list writers and the `'static`-erase READ bridge. Before
    /// this fix `_readAggref` was a seam-panic.
    #[test]
    fn aggref_with_args_round_trips() {
        ensure_seams();
        let ctx =
            std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new("read-aggref-test")));
        let mcx = ctx.mcx();

        let te = ::nodes::primnodes::TargetEntry {
            expr: Some(::mcx::alloc_in(mcx, Expr::Var(mk_var()?)).expect("alloc")),
            resno: 1,
            resname: None,
            ressortgroupref: 1,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        let sgc = ::nodes::rawnodes::SortGroupClause {
            tleSortGroupRef: 1,
            eqop: 96,
            sortop: 97,
            reverse_sort: false,
            nulls_first: false,
            hashable: true,
        };
        let aggref = ::nodes::primnodes::Aggref {
            aggfnoid: 2147,
            aggtype: 20,
            aggcollid: 0,
            inputcollid: 0,
            aggtranstype: 20,
            aggargtypes: std::vec![23],
            aggdirectargs: std::vec::Vec::new(),
            args: std::vec![::nodes::primnodes::targetentry_into_static(te)],
            aggorder: std::vec::Vec::new(),
            aggdistinct: std::vec![sgc],
            aggfilter: None,
            aggstar: false,
            aggvariadic: false,
            aggkind: b'n' as i8,
            aggpresorted: false,
            agglevelsup: 0,
            aggsplit: 0,
            aggno: -1,
            aggtransno: -1,
            location: -1,
        };
        let node = Node::mk_expr(mcx, Expr::Aggref(aggref))?;

        let text = ::outfuncs::nodeToString(mcx, &node).expect("out");
        let parsed = string_to_node(mcx, text.as_str()).expect("read");
        let text2 = ::outfuncs::nodeToString(mcx, &parsed).expect("re-out");
        assert_eq!(text.as_str(), text2.as_str(), "Aggref round-trip not stable");

        let parsed = PgBox::into_inner(parsed);
        let tag = parsed.node_tag();
        match parsed.into_expr() {
            Some(Expr::Aggref(a)) => {
                assert_eq!(a.aggfnoid, 2147);
                assert_eq!(a.aggargtypes, std::vec![23]);
                assert_eq!(a.args.len(), 1, "args TargetEntry lost in round-trip");
                assert_eq!(a.args[0].resno, 1);
                assert!(a.args[0].expr.is_some(), "args[0].expr lost");
                assert_eq!(a.aggdistinct.len(), 1, "aggdistinct lost in round-trip");
                assert_eq!(a.aggdistinct[0].eqop, 96);
            }
            _ => panic!("expected Aggref, got {tag:?}"),
        }
    }

    #[test]
    fn nulltest_reads() {
        // `_outNullTest` field order: arg, nulltesttype, argisrow, location.
        let node = parse("{NULLTEST :arg <> :nulltesttype 1 :argisrow false :location 9}");
        let tag = node.node_tag();
        match node.into_expr() {
            Some(Expr::NullTest(n)) => {
                assert!(n.arg.is_none());
                assert_eq!(n.nulltesttype, NullTestType::IS_NOT_NULL);
                assert!(!n.argisrow);
                assert_eq!(n.location, -1); // READ_LOCATION_FIELD → -1
            }
            _ => panic!("expected NullTest, got {tag:?}"),
        }
    }

    #[test]
    fn nulltest_with_var_arg_reads() {
        // arg is a framed VAR child (exercises the Expr<->Node bridge on READ).
        let s = "{NULLTEST :arg {VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 \
                 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 \
                 :varnosyn 1 :varattnosyn 2 :location -1} :nulltesttype 0 :argisrow true \
                 :location -1}";
        let node = parse(s);
        let tag = node.node_tag();
        match node.into_expr() {
            Some(Expr::NullTest(n)) => {
                assert!(n.argisrow);
                assert_eq!(n.nulltesttype, NullTestType::IS_NULL);
                match n.arg.as_deref() {
                    Some(Expr::Var(v)) => assert_eq!(v.varno, 1),
                    other => panic!("expected Var arg, got {other:?}"),
                }
            }
            _ => panic!("expected NullTest, got {tag:?}"),
        }
    }

    #[test]
    fn scalararrayopexpr_reads() {
        // `_outScalarArrayOpExpr`: opno, opfuncid, hashfuncid, negfuncid, useOr,
        // inputcollid, args, location.
        let s = "{SCALARARRAYOPEXPR :opno 96 :opfuncid 65 :hashfuncid 0 :negfuncid 0 \
                 :useOr true :inputcollid 0 :args () :location -1}";
        let node = parse(s);
        let tag = node.node_tag();
        match node.into_expr() {
            Some(Expr::ScalarArrayOpExpr(n)) => {
                assert_eq!(n.opno, 96);
                assert_eq!(n.opfuncid, 65);
                assert!(n.useOr);
                assert!(n.args.is_empty());
            }
            _ => panic!("expected ScalarArrayOpExpr, got {tag:?}"),
        }
    }

    /// `_outSubLink`/`_readSubLink`: the embedded `subselect` Query round-trips
    /// through the OUT framed-Query writer and the READ `'static`-erase bridge.
    #[test]
    fn sublink_with_subselect_round_trips() {
        ensure_seams();
        let ctx = std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new(
            "read-sublink-test",
        )));
        let mcx = ctx.mcx();

        let q = ::nodes::copy_query::Query::new(mcx);
        let subselect = Some(
            ::nodes::primnodes::query_box_into_static(q, mcx).expect("erase"),
        );
        let sublink = Expr::SubLink(::nodes::primnodes::SubLink {
            subLinkType: ::nodes::primnodes::SubLinkType::Any,
            subLinkId: 3,
            testexpr: Some(std::boxed::Box::new(Expr::Var(mk_var()?))),
            // ANY sublink carries an operName like `("=")`; it must survive the
            // out/read round-trip (the bug this exercises: the reader formerly
            // consumed only the `<>` token, misaligning on the real `("=")` list).
            operName: std::vec![std::string::String::from("=")],
            subselect,
            location: -1,
        });
        let node = Node::mk_expr(mcx, sublink)?;

        let text = ::outfuncs::nodeToString(mcx, &node).expect("out");
        let parsed = string_to_node(mcx, text.as_str()).expect("read");
        let text2 = ::outfuncs::nodeToString(mcx, &parsed).expect("re-out");
        assert_eq!(text.as_str(), text2.as_str(), "SubLink round-trip not stable");

        let parsed = PgBox::into_inner(parsed);
        let tag = parsed.node_tag();
        match parsed.into_expr() {
            Some(Expr::SubLink(s)) => {
                assert_eq!(s.subLinkId, 3);
                assert!(s.subselect.is_some(), "subselect lost in round-trip");
                assert_eq!(s.operName, std::vec![std::string::String::from("=")],
                    "operName lost/garbled in round-trip");
            }
            _ => panic!("expected SubLink, got {tag:?}"),
        }
    }

    #[test]
    fn booleantest_reads() {
        let node = parse("{BOOLEANTEST :arg <> :booltesttype 2 :location -1}");
        let tag = node.node_tag();
        match node.into_expr() {
            Some(Expr::BooleanTest(n)) => {
                assert_eq!(n.booltesttype, BoolTestType::IS_FALSE);
            }
            _ => panic!("expected BooleanTest, got {tag:?}"),
        }
    }

    #[test]
    fn relabeltype_reads() {
        let s = "{RELABELTYPE :arg <> :resulttype 25 :resulttypmod -1 :resultcollid 100 \
                 :relabelformat 1 :location -1}";
        let node = parse(s);
        let tag = node.node_tag();
        match node.into_expr() {
            Some(Expr::RelabelType(n)) => {
                assert_eq!(n.resulttype, 25);
                assert_eq!(n.resultcollid, 100);
                assert_eq!(n.relabelformat, CoercionForm::COERCE_EXPLICIT_CAST);
            }
            _ => panic!("expected RelabelType, got {tag:?}"),
        }
    }

    /// OUT a raw-grammar `Node::NullTest` (reached through `out_node_inner`'s
    /// family chain) and assert the exact token stream matches `_outNullTest`.
    #[test]
    fn raw_nulltest_out_text() {
        let ctx = MemoryContext::new("raw-nulltest");
        let mcx = ctx.mcx();
        let n = ::nodes::rawexprnodes::NullTest {
            arg: None,
            nulltesttype: NullTestType::IS_NOT_NULL,
            argisrow: false,
            location: 5,
        };
        let node = Node::mk_null_test(mcx, n)?;
        let mut buf = String::new();
        out_node(&mut buf, &node);
        // location renders -1 (non-debug WRITE_LOCATION_FIELD via out_node).
        assert_eq!(
            buf,
            "{NULLTEST :arg <> :nulltesttype 1 :argisrow false :location -1}".to_string()
        );
    }

    /// OUT a raw `Node::CurrentOfExpr` and assert the token stream.
    #[test]
    fn raw_currentof_out_text() {
        let ctx = MemoryContext::new("raw-cof");
        let mcx = ctx.mcx();
        let n = ::nodes::rawexprnodes::CurrentOfExpr {
            cvarno: 3,
            cursor_name: Some(PgString::from_str_in("c1", mcx).unwrap()),
            cursor_param: 0,
        };
        let node = Node::mk_current_of_expr(mcx, n)?;
        let mut buf = String::new();
        out_node(&mut buf, &node);
        assert_eq!(
            buf,
            "{CURRENTOFEXPR :cvarno 3 :cursor_name c1 :cursor_param 0}".to_string()
        );
        let _ = mk_var()?; // keep helper referenced
    }
}

