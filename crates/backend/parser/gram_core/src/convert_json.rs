// SQL/JSON raw-grammar node converters (included into convert.rs).
//
// The c2rust raw `*mut Node` SQL/JSON parse graph → owned
// `nodes::rawexprnodes` raw-grammar nodes. Same uniform mapping as the
// rest of the converter: `*mut Node` → `Option<NodePtr>`/`NodePtr`, `*mut List`
// → `PgVec<NodePtr>`, `*mut c_char` → `Option<PgString>`, typed `*mut Child` →
// `Option<PgBox<Child>>`, the `NodeTag` header dropped (the enum arm carries
// it). analyze's `transformJson*` family later rewrites these into the cooked
// `primnodes::Expr` (`JsonConstructorExpr`/`JsonExpr`/…). These mirror
// `parse_expr.c`/`gram.y`'s raw output exactly.

// --- small-enum discriminant mappers ----------------------------------------

fn json_format_type(v: cpr::JsonFormatType) -> tn_prim::JsonFormatType {
    use tn_prim::JsonFormatType::*;
    match v {
        cpr::JS_FORMAT_DEFAULT => JS_FORMAT_DEFAULT,
        cpr::JS_FORMAT_JSON => JS_FORMAT_JSON,
        cpr::JS_FORMAT_JSONB => JS_FORMAT_JSONB,
        other => panic!("gram converter: invalid JsonFormatType {other}"),
    }
}

fn json_encoding(v: cpr::JsonEncoding) -> tn_prim::JsonEncoding {
    use tn_prim::JsonEncoding::*;
    match v {
        cpr::JS_ENC_DEFAULT => JS_ENC_DEFAULT,
        cpr::JS_ENC_UTF8 => JS_ENC_UTF8,
        cpr::JS_ENC_UTF16 => JS_ENC_UTF16,
        cpr::JS_ENC_UTF32 => JS_ENC_UTF32,
        other => panic!("gram converter: invalid JsonEncoding {other}"),
    }
}

fn json_expr_op(v: cpr::JsonExprOp) -> tn_prim::JsonExprOp {
    use tn_prim::JsonExprOp::*;
    match v {
        cpr::JSON_EXISTS_OP => JSON_EXISTS_OP,
        cpr::JSON_QUERY_OP => JSON_QUERY_OP,
        cpr::JSON_VALUE_OP => JSON_VALUE_OP,
        cpr::JSON_TABLE_OP => JSON_TABLE_OP,
        other => panic!("gram converter: invalid JsonExprOp {other}"),
    }
}

fn json_wrapper(v: cpr::JsonWrapper) -> tn_prim::JsonWrapper {
    use tn_prim::JsonWrapper::*;
    match v {
        cpr::JSW_UNSPEC => JSW_UNSPEC,
        cpr::JSW_NONE => JSW_NONE,
        cpr::JSW_CONDITIONAL => JSW_CONDITIONAL,
        cpr::JSW_UNCONDITIONAL => JSW_UNCONDITIONAL,
        other => panic!("gram converter: invalid JsonWrapper {other}"),
    }
}

fn json_quotes(v: cp::JsonQuotes) -> tn_prim::JsonQuotes {
    use tn_prim::JsonQuotes::*;
    match v {
        cp::JS_QUOTES_UNSPEC => JS_QUOTES_UNSPEC,
        cp::JS_QUOTES_KEEP => JS_QUOTES_KEEP,
        cp::JS_QUOTES_OMIT => JS_QUOTES_OMIT,
        other => panic!("gram converter: invalid JsonQuotes {other}"),
    }
}

fn json_table_column_type(v: cp::JsonTableColumnType) -> tn_prim::JsonTableColumnType {
    use tn_prim::JsonTableColumnType::*;
    match v {
        cp::JTC_FOR_ORDINALITY => JTC_FOR_ORDINALITY,
        cp::JTC_REGULAR => JTC_REGULAR,
        cp::JTC_EXISTS => JTC_EXISTS,
        cp::JTC_FORMATTED => JTC_FORMATTED,
        cp::JTC_NESTED => JTC_NESTED,
        other => panic!("gram converter: invalid JsonTableColumnType {other}"),
    }
}

fn json_behavior_type(v: cpr::JsonBehaviorType) -> tn_prim::JsonBehaviorType {
    use tn_prim::JsonBehaviorType::*;
    match v {
        cpr::JSON_BEHAVIOR_NULL => JSON_BEHAVIOR_NULL,
        cpr::JSON_BEHAVIOR_ERROR => JSON_BEHAVIOR_ERROR,
        cpr::JSON_BEHAVIOR_EMPTY => JSON_BEHAVIOR_EMPTY,
        cpr::JSON_BEHAVIOR_TRUE => JSON_BEHAVIOR_TRUE,
        cpr::JSON_BEHAVIOR_FALSE => JSON_BEHAVIOR_FALSE,
        cpr::JSON_BEHAVIOR_UNKNOWN => JSON_BEHAVIOR_UNKNOWN,
        cpr::JSON_BEHAVIOR_EMPTY_ARRAY => JSON_BEHAVIOR_EMPTY_ARRAY,
        cpr::JSON_BEHAVIOR_EMPTY_OBJECT => JSON_BEHAVIOR_EMPTY_OBJECT,
        cpr::JSON_BEHAVIOR_DEFAULT => JSON_BEHAVIOR_DEFAULT,
        other => panic!("gram converter: invalid JsonBehaviorType {other}"),
    }
}

// --- JsonFormat (by-value, no children) -------------------------------------

/// `*mut JsonFormat` → `Option<JsonFormat>` (NULL → None).
fn json_format_opt(p: *mut cpr::JsonFormat) -> Option<tn_prim::JsonFormat> {
    if p.is_null() {
        return None;
    }
    let f = unsafe { &*p };
    Some(tn_prim::JsonFormat {
        format_type: json_format_type(f.format_type),
        encoding: json_encoding(f.encoding),
        location: f.location,
    })
}

// --- child converters -------------------------------------------------------

fn conv_json_value_expr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::JsonValueExpr,
) -> PgResult<tn_re::JsonValueExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonValueExpr {
        raw_expr: node_opt(mcx, e.raw_expr.cast())?,
        formatted_expr: node_opt(mcx, e.formatted_expr.cast())?,
        format: json_format_opt(e.format),
    })
}

fn json_value_type(v: cpr::JsonValueType) -> tn_prim::JsonValueType {
    use tn_prim::JsonValueType::*;
    match v {
        cpr::JS_TYPE_ANY => JS_TYPE_ANY,
        cpr::JS_TYPE_OBJECT => JS_TYPE_OBJECT,
        cpr::JS_TYPE_ARRAY => JS_TYPE_ARRAY,
        cpr::JS_TYPE_SCALAR => JS_TYPE_SCALAR,
        other => panic!("gram converter: invalid JsonValueType {other}"),
    }
}

fn conv_json_is_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::JsonIsPredicate,
) -> PgResult<tn_re::JsonIsPredicate<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonIsPredicate {
        expr: node_opt(mcx, e.expr)?,
        format: json_format_opt(e.format),
        item_type: json_value_type(e.item_type),
        unique_keys: e.unique_keys,
        location: e.location,
    })
}

fn conv_json_behavior<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::JsonBehavior,
) -> PgResult<tn_re::JsonBehavior<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonBehavior {
        btype: json_behavior_type(e.btype),
        expr: node_opt(mcx, e.expr)?,
        coerce: e.coerce,
        location: e.location,
    })
}

/// `*mut JsonReturning` → `Option<JsonReturning>` (NULL → None). The grammar
/// fills only `format` on the `RETURNING` clause; `typid`/`typmod` are
/// analyze-filled, so they start zeroed (Invalid/-1-equivalent) here.
fn json_returning_opt(p: *mut cpr::JsonReturning) -> Option<tn_prim::JsonReturning> {
    if p.is_null() {
        return None;
    }
    let r = unsafe { &*p };
    Some(tn_prim::JsonReturning {
        format: json_format_opt(r.format),
        typid: r.typid,
        typmod: r.typmod,
    })
}

fn conv_json_output<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonOutput,
) -> PgResult<tn_re::JsonOutput<'mcx>> {
    let e = unsafe { &*p };
    // The grammar fills `returning->format` from the FORMAT clause;
    // `typid`/`typmod` are analyze-filled later in transformJsonOutput.
    Ok(tn_re::JsonOutput {
        type_name: child_opt(mcx, e.type_name, conv_typename)?,
        returning: json_returning_opt(e.returning),
    })
}

fn conv_json_key_value<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonKeyValue,
) -> PgResult<tn_re::JsonKeyValue<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonKeyValue {
        key: node_opt(mcx, e.key.cast())?,
        value: child_opt(mcx, e.value, conv_json_value_expr)?,
    })
}

fn conv_json_argument<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonArgument,
) -> PgResult<tn_re::JsonArgument<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonArgument {
        val: child_opt(mcx, e.val, conv_json_value_expr)?,
        name: cstr_opt(mcx, e.name)?,
    })
}

fn conv_json_object_constructor<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonObjectConstructor,
) -> PgResult<tn_re::JsonObjectConstructor<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonObjectConstructor {
        exprs: node_list(mcx, e.exprs)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        absent_on_null: e.absent_on_null,
        unique: e.unique,
        location: e.location,
    })
}

fn conv_json_array_constructor<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonArrayConstructor,
) -> PgResult<tn_re::JsonArrayConstructor<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonArrayConstructor {
        exprs: node_list(mcx, e.exprs)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        absent_on_null: e.absent_on_null,
        location: e.location,
    })
}

fn conv_json_array_query_constructor<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonArrayQueryConstructor,
) -> PgResult<tn_re::JsonArrayQueryConstructor<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonArrayQueryConstructor {
        query: node_opt(mcx, e.query)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        format: json_format_opt(e.format),
        absent_on_null: e.absent_on_null,
        location: e.location,
    })
}

fn conv_json_agg_constructor<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonAggConstructor,
) -> PgResult<tn_re::JsonAggConstructor<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonAggConstructor {
        output: child_opt(mcx, e.output, conv_json_output)?,
        agg_filter: node_opt(mcx, e.agg_filter)?,
        agg_order: node_list(mcx, e.agg_order)?,
        over: child_opt(mcx, e.over, conv_windowdef)?,
        location: e.location,
    })
}

fn conv_json_object_agg<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonObjectAgg,
) -> PgResult<tn_re::JsonObjectAgg<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonObjectAgg {
        constructor: child_opt(mcx, e.constructor, conv_json_agg_constructor)?,
        arg: child_opt(mcx, e.arg, conv_json_key_value)?,
        absent_on_null: e.absent_on_null,
        unique: e.unique,
    })
}

fn conv_json_array_agg<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonArrayAgg,
) -> PgResult<tn_re::JsonArrayAgg<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonArrayAgg {
        constructor: child_opt(mcx, e.constructor, conv_json_agg_constructor)?,
        arg: child_opt(mcx, e.arg, conv_json_value_expr)?,
        absent_on_null: e.absent_on_null,
    })
}

fn conv_json_parse_expr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonParseExpr,
) -> PgResult<tn_re::JsonParseExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonParseExpr {
        expr: child_opt(mcx, e.expr, conv_json_value_expr)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        unique_keys: e.unique_keys,
        location: e.location,
    })
}

fn conv_json_scalar_expr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonScalarExpr,
) -> PgResult<tn_re::JsonScalarExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonScalarExpr {
        expr: node_opt(mcx, e.expr.cast())?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        location: e.location,
    })
}

fn conv_json_serialize_expr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonSerializeExpr,
) -> PgResult<tn_re::JsonSerializeExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonSerializeExpr {
        expr: child_opt(mcx, e.expr, conv_json_value_expr)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        location: e.location,
    })
}

fn conv_json_func_expr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonFuncExpr,
) -> PgResult<tn_re::JsonFuncExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonFuncExpr {
        op: json_expr_op(e.op),
        column_name: cstr_opt(mcx, e.column_name)?,
        context_item: child_opt(mcx, e.context_item, conv_json_value_expr)?,
        pathspec: node_opt(mcx, e.pathspec)?,
        passing: node_list(mcx, e.passing)?,
        output: child_opt(mcx, e.output, conv_json_output)?,
        on_empty: node_opt(mcx, e.on_empty.cast())?,
        on_error: node_opt(mcx, e.on_error.cast())?,
        wrapper: json_wrapper(e.wrapper),
        quotes: json_quotes(e.quotes),
        location: e.location,
    })
}

fn conv_json_table_path_spec<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonTablePathSpec,
) -> PgResult<tn_re::JsonTablePathSpec<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonTablePathSpec {
        string: node_opt(mcx, e.string)?,
        name: cstr_opt(mcx, e.name)?,
        name_location: e.name_location,
        location: e.location,
    })
}

fn conv_json_table<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonTable,
) -> PgResult<tn_re::JsonTable<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonTable {
        context_item: child_opt(mcx, e.context_item, conv_json_value_expr)?,
        pathspec: child_opt(mcx, e.pathspec, conv_json_table_path_spec)?,
        passing: node_list(mcx, e.passing)?,
        columns: node_list(mcx, e.columns)?,
        on_error: node_opt(mcx, e.on_error.cast())?,
        alias: child_opt(mcx, e.alias, conv_alias)?,
        lateral: e.lateral,
        location: e.location,
    })
}

fn conv_json_table_column<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::JsonTableColumn,
) -> PgResult<tn_re::JsonTableColumn<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::JsonTableColumn {
        coltype: json_table_column_type(e.coltype),
        name: cstr_opt(mcx, e.name)?,
        type_name: child_opt(mcx, e.type_name, conv_typename)?,
        pathspec: child_opt(mcx, e.pathspec, conv_json_table_path_spec)?,
        format: json_format_opt(e.format),
        wrapper: json_wrapper(e.wrapper),
        quotes: json_quotes(e.quotes),
        columns: node_list(mcx, e.columns)?,
        on_empty: node_opt(mcx, e.on_empty.cast())?,
        on_error: node_opt(mcx, e.on_error.cast())?,
        location: e.location,
    })
}
