// A_Const + grammar-produced `Expr` leaves + tag naming (included into
// convert.rs).

// ---------------------------------------------------------------------------
// A_Const — the inline ValUnion literal.
// ---------------------------------------------------------------------------

/// `A_Const` carries its value node *inline* in a `union ValUnion`. We discover
/// the active arm via `val.node.type_`, then produce an owned `A_Const` whose
/// `val` is the corresponding `Node` value (or `None` for a NULL constant).
fn conv_a_const<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::A_Const) -> PgResult<Node<'mcx>> {
    let c = unsafe { &*p };
    let val: Option<NodePtr<'mcx>> = if c.isnull {
        None
    } else {
        // The union's first member is a bare `Node` header carrying the tag.
        let inner_tag = unsafe { c.val.node.type_ };
        let node = match inner_tag {
            tags::T_Integer => Node::mk_integer(mcx, tn_val::Integer {
                ival: unsafe { c.val.ival.ival },
            })?,
            tags::T_Float => Node::mk_float(mcx, tn_val::Float {
                fval: cstr(mcx, unsafe { c.val.fval.fval })?,
            })?,
            tags::T_Boolean => Node::mk_boolean(mcx, tn_val::Boolean {
                boolval: unsafe { c.val.boolval.boolval },
            })?,
            tags::T_String => Node::mk_string(mcx, tn_val::StringNode {
                sval: cstr(mcx, unsafe { c.val.sval.sval })?,
            })?,
            tags::T_BitString => Node::mk_bit_string(mcx, tn_val::BitString {
                bsval: cstr(mcx, unsafe { c.val.bsval.bsval })?,
            })?,
            other => panic!("gram converter: invalid A_Const ValUnion tag {other}"),
        };
        Some(mcx::alloc_in(mcx, node)?)
    };
    Ok(Node::mk_a_const(mcx, tn::A_Const {
        val,
        isnull: c.isnull,
        location: c.location,
    })?)
}

// ---------------------------------------------------------------------------
// Grammar-produced raw `Expr`-deriving nodes (rawexprnodes).
//
// In C, these `Expr`-deriving nodes (`makeBoolExpr`, `CaseExpr`, `CoalesceExpr`,
// …) are the same struct in the raw grammar output and the post-analysis tree,
// but the grammar fills their `Node *`/`List *` children with *raw* parse-tree
// nodes (ColumnRef/A_Expr/A_Const/…). The owned post-analysis
// `primnodes::Expr` enum carries `Expr` children and so cannot hold those raw
// children; the *raw* counterparts live in `types_nodes::rawexprnodes` (the
// raw-expression node-model keystone) and ride as their own `Node` arms. These
// converters target those raw types directly; analyze.c's `transformExpr`
// later turns them into post-analysis `Expr`.
//
// The c2rust `*mut Expr` child fields are `Node`-headed; cast to `*mut RawNode`
// for the uniform `node_opt`/`node_list` helpers.
// ---------------------------------------------------------------------------

fn conv_boolexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::BoolExpr) -> PgResult<tn_re::BoolExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::BoolExpr {
        boolop: bool_expr_type(e.boolop),
        args: node_list(mcx, e.args)?,
        location: e.location,
    })
}

fn conv_caseexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::CaseExpr) -> PgResult<tn_re::CaseExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::CaseExpr {
        casetype: e.casetype,
        casecollid: e.casecollid,
        arg: node_opt(mcx, e.arg.cast())?,
        args: node_list(mcx, e.args)?,
        defresult: node_opt(mcx, e.defresult.cast())?,
        location: e.location,
    })
}

fn conv_casewhen<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::CaseWhen) -> PgResult<tn_re::CaseWhen<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::CaseWhen {
        expr: node_opt(mcx, e.expr.cast())?,
        result: node_opt(mcx, e.result.cast())?,
        location: e.location,
    })
}

fn conv_coalesceexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::CoalesceExpr,
) -> PgResult<tn_re::CoalesceExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::CoalesceExpr {
        coalescetype: e.coalescetype,
        coalescecollid: e.coalescecollid,
        args: node_list(mcx, e.args)?,
        location: e.location,
    })
}

fn conv_minmaxexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::MinMaxExpr,
) -> PgResult<tn_re::MinMaxExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::MinMaxExpr {
        minmaxtype: e.minmaxtype,
        minmaxcollid: e.minmaxcollid,
        inputcollid: e.inputcollid,
        op: min_max_op(e.op),
        args: node_list(mcx, e.args)?,
        location: e.location,
    })
}

fn conv_sublink<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::SubLink) -> PgResult<tn_re::SubLink<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::SubLink {
        sub_link_type: sub_link_type(e.sub_link_type),
        sub_link_id: e.sub_link_id,
        testexpr: node_opt(mcx, e.testexpr)?,
        oper_name: node_list(mcx, e.oper_name)?,
        subselect: node_opt(mcx, e.subselect)?,
        location: e.location,
    })
}

fn conv_nulltest<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::NullTest) -> PgResult<tn_re::NullTest<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::NullTest {
        arg: node_opt(mcx, e.arg.cast())?,
        nulltesttype: null_test_type(e.nulltesttype),
        argisrow: e.argisrow,
        location: e.location,
    })
}

fn conv_booleantest<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::BooleanTest,
) -> PgResult<tn_re::BooleanTest<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::BooleanTest {
        arg: node_opt(mcx, e.arg.cast())?,
        booltesttype: bool_test_type(e.booltesttype),
        location: e.location,
    })
}

fn conv_rowexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::RowExpr) -> PgResult<tn_re::RowExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::RowExpr {
        args: node_list(mcx, e.args)?,
        row_typeid: e.row_typeid,
        row_format: coercion_form(e.row_format),
        colnames: node_list(mcx, e.colnames)?,
        location: e.location,
    })
}

fn conv_groupingfunc<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::GroupingFunc,
) -> PgResult<tn_re::GroupingFunc<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::GroupingFunc {
        args: node_list(mcx, e.args)?,
        refs: int_list(mcx, e.refs)?,
        cols: int_list(mcx, e.cols)?,
        agglevelsup: e.agglevelsup,
        location: e.location,
    })
}

fn conv_collateexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::CollateExpr,
) -> PgResult<tn_re::CollateExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::CollateExpr {
        arg: node_opt(mcx, e.arg.cast())?,
        coll_oid: e.coll_oid,
        location: e.location,
    })
}

fn conv_settodefault(p: *mut cpr::SetToDefault) -> tn_re::SetToDefault {
    let e = unsafe { &*p };
    tn_re::SetToDefault {
        type_id: e.type_id,
        type_mod: e.type_mod,
        collation: e.collation,
        location: e.location,
    }
}

fn conv_currentofexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::CurrentOfExpr,
) -> PgResult<tn_re::CurrentOfExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::CurrentOfExpr {
        cvarno: e.cvarno,
        cursor_name: cstr_opt(mcx, e.cursor_name)?,
        cursor_param: e.cursor_param,
    })
}

fn conv_namedargexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::NamedArgExpr,
) -> PgResult<tn_re::NamedArgExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::NamedArgExpr {
        arg: node_opt(mcx, e.arg.cast())?,
        name: cstr_opt(mcx, e.name)?,
        argnumber: e.argnumber,
        location: e.location,
    })
}

fn conv_sqlvaluefunction(p: *mut cpr::SQLValueFunction) -> tn_re::SQLValueFunction {
    let e = unsafe { &*p };
    tn_re::SQLValueFunction {
        op: sql_value_function_op(e.op),
        type_: e.type_,
        typmod: e.typmod,
        location: e.location,
    }
}

fn conv_xmlexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::XmlExpr) -> PgResult<tn_re::XmlExpr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::XmlExpr {
        op: xml_expr_op(e.op),
        name: cstr_opt(mcx, e.name)?,
        named_args: node_list(mcx, e.named_args)?,
        arg_names: node_list(mcx, e.arg_names)?,
        args: node_list(mcx, e.args)?,
        xmloption: xml_option_type(e.xmloption),
        indent: e.indent,
        type_: e.type_,
        typmod: e.typmod,
        location: e.location,
    })
}

fn conv_xmlserialize<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::XmlSerialize,
) -> PgResult<tn_re::XmlSerialize<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn_re::XmlSerialize {
        xmloption: xml_option_type(e.xmloption),
        expr: node_opt(mcx, e.expr)?,
        type_name: child_opt(mcx, e.type_name, conv_typename)?,
        indent: e.indent,
        location: e.location,
    })
}

// --- small-enum discriminant mappers for the raw `Expr`-deriving nodes ---

fn bool_expr_type(v: cpr::BoolExprType) -> tn_prim::BoolExprType {
    use tn_prim::BoolExprType::*;
    match v {
        cpr::AND_EXPR => AND_EXPR,
        cpr::OR_EXPR => OR_EXPR,
        cpr::NOT_EXPR => NOT_EXPR,
        other => panic!("gram converter: invalid BoolExprType {other}"),
    }
}

fn sub_link_type(v: cpr::SubLinkType) -> tn_prim::SubLinkType {
    use tn_prim::SubLinkType::*;
    match v {
        cpr::EXISTS_SUBLINK => Exists,
        cpr::ALL_SUBLINK => All,
        cpr::ANY_SUBLINK => Any,
        cpr::ROWCOMPARE_SUBLINK => RowCompare,
        cpr::EXPR_SUBLINK => Expr,
        cpr::MULTIEXPR_SUBLINK => MultiExpr,
        cpr::ARRAY_SUBLINK => Array,
        cpr::CTE_SUBLINK => Cte,
        other => panic!("gram converter: invalid SubLinkType {other}"),
    }
}

fn min_max_op(v: cpr::MinMaxOp) -> tn_prim::MinMaxOp {
    use tn_prim::MinMaxOp::*;
    match v {
        cpr::IS_GREATEST => IS_GREATEST,
        cpr::IS_LEAST => IS_LEAST,
        other => panic!("gram converter: invalid MinMaxOp {other}"),
    }
}

fn null_test_type(v: cpr::NullTestType) -> tn_prim::NullTestType {
    use tn_prim::NullTestType::*;
    match v {
        cpr::IS_NULL => IS_NULL,
        cpr::IS_NOT_NULL => IS_NOT_NULL,
        other => panic!("gram converter: invalid NullTestType {other}"),
    }
}

fn bool_test_type(v: cpr::BoolTestType) -> tn_prim::BoolTestType {
    use tn_prim::BoolTestType::*;
    match v {
        cpr::IS_TRUE => IS_TRUE,
        cpr::IS_NOT_TRUE => IS_NOT_TRUE,
        cpr::IS_FALSE => IS_FALSE,
        cpr::IS_NOT_FALSE => IS_NOT_FALSE,
        cpr::IS_UNKNOWN => IS_UNKNOWN,
        cpr::IS_NOT_UNKNOWN => IS_NOT_UNKNOWN,
        other => panic!("gram converter: invalid BoolTestType {other}"),
    }
}

fn sql_value_function_op(v: cpr::SQLValueFunctionOp) -> tn_prim::SQLValueFunctionOp {
    use tn_prim::SQLValueFunctionOp::*;
    match v {
        cpr::SVFOP_CURRENT_DATE => SVFOP_CURRENT_DATE,
        cpr::SVFOP_CURRENT_TIME => SVFOP_CURRENT_TIME,
        cpr::SVFOP_CURRENT_TIME_N => SVFOP_CURRENT_TIME_N,
        cpr::SVFOP_CURRENT_TIMESTAMP => SVFOP_CURRENT_TIMESTAMP,
        cpr::SVFOP_CURRENT_TIMESTAMP_N => SVFOP_CURRENT_TIMESTAMP_N,
        cpr::SVFOP_LOCALTIME => SVFOP_LOCALTIME,
        cpr::SVFOP_LOCALTIME_N => SVFOP_LOCALTIME_N,
        cpr::SVFOP_LOCALTIMESTAMP => SVFOP_LOCALTIMESTAMP,
        cpr::SVFOP_LOCALTIMESTAMP_N => SVFOP_LOCALTIMESTAMP_N,
        cpr::SVFOP_CURRENT_ROLE => SVFOP_CURRENT_ROLE,
        cpr::SVFOP_CURRENT_USER => SVFOP_CURRENT_USER,
        cpr::SVFOP_USER => SVFOP_USER,
        cpr::SVFOP_SESSION_USER => SVFOP_SESSION_USER,
        cpr::SVFOP_CURRENT_CATALOG => SVFOP_CURRENT_CATALOG,
        cpr::SVFOP_CURRENT_SCHEMA => SVFOP_CURRENT_SCHEMA,
        other => panic!("gram converter: invalid SQLValueFunctionOp {other}"),
    }
}

fn xml_expr_op(v: cpr::XmlExprOp) -> tn_prim::XmlExprOp {
    use tn_prim::XmlExprOp::*;
    match v {
        cpr::IS_XMLCONCAT => IS_XMLCONCAT,
        cpr::IS_XMLELEMENT => IS_XMLELEMENT,
        cpr::IS_XMLFOREST => IS_XMLFOREST,
        cpr::IS_XMLPARSE => IS_XMLPARSE,
        cpr::IS_XMLPI => IS_XMLPI,
        cpr::IS_XMLROOT => IS_XMLROOT,
        cpr::IS_XMLSERIALIZE => IS_XMLSERIALIZE,
        cpr::IS_DOCUMENT => IS_DOCUMENT,
        other => panic!("gram converter: invalid XmlExprOp {other}"),
    }
}

fn xml_option_type(v: cpr::XmlOptionType) -> tn_prim::XmlOptionType {
    use tn_prim::XmlOptionType::*;
    match v {
        cpr::XMLOPTION_DOCUMENT => XMLOPTION_DOCUMENT,
        cpr::XMLOPTION_CONTENT => XMLOPTION_CONTENT,
        other => panic!("gram converter: invalid XmlOptionType {other}"),
    }
}

// ---------------------------------------------------------------------------
// Tag naming (for panic messages).
// ---------------------------------------------------------------------------

fn node_tag_name(tag: u32) -> &'static str {
    match tag {
        tags::T_CaseExpr => "CaseExpr",
        tags::T_CoalesceExpr => "CoalesceExpr",
        tags::T_MinMaxExpr => "MinMaxExpr",
        tags::T_SubLink => "SubLink",
        tags::T_BooleanTest => "BooleanTest",
        tags::T_NullTest => "NullTest",
        tags::T_XmlExpr => "XmlExpr",
        tags::T_RowExpr => "RowExpr",
        tags::T_GroupingFunc => "GroupingFunc",
        tags::T_CollateExpr => "CollateExpr",
        tags::T_SetToDefault => "SetToDefault",
        tags::T_CurrentOfExpr => "CurrentOfExpr",
        tags::T_NamedArgExpr => "NamedArgExpr",
        tags::T_BoolExpr => "BoolExpr",
        tags::T_SQLValueFunction => "SQLValueFunction",
        _ => "unknown/DDL-utility-node",
    }
}
