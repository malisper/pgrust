// SQL/JSON constructor + query-function transforms (parse_expr.c 3280-4980).
use mcx::Mcx;
use parser_small1::{ParseExprKind, ParseState};
use types_core::catalog::{
    BOOLOID, BYTEAOID, INT4OID, JSONBOID, JSONOID, TEXTOID, UNKNOWNOID,
};
use types_core::{Oid, ParseLoc};
use types_error::PgResult;
use types_nodes::primnodes::{
    Aggref, CaseTestExpr, FuncExpr, JsonBehavior, JsonBehaviorType, JsonConstructorExpr,
    JsonConstructorType, JsonEncoding, JsonExpr, JsonExprOp, JsonFormat, JsonFormatType,
    JsonIsPredicate, JsonReturning, JsonValueExpr, JsonWrapper, WindowFunc, AGGKIND_NORMAL,
};
use types_nodes::rawnodes::{JsonQuotes, RangeSubselect, ResTarget, TypeName};
use types_nodes::{
    CoercionForm, Node, NodeList, NodeTag, SubLink, SubLinkType,
};

use crate::{expr_location, expr_type, transformExprRecurse};

const F_TO_JSON: Oid = 3176;
const F_TO_JSONB: Oid = 3787;
const F_CONVERT_FROM: Oid = 1714;
const F_CONVERT_TO: Oid = 1717;
const F_JSON_AGG: Oid = 3175;
const F_JSON_AGG_STRICT: Oid = 6276;
const F_JSON_OBJECT_AGG: Oid = 3197;
const F_JSON_OBJECT_AGG_STRICT: Oid = 6280;
const F_JSON_OBJECT_AGG_UNIQUE: Oid = 6281;
const F_JSON_OBJECT_AGG_UNIQUE_STRICT: Oid = 6282;
const F_JSONB_AGG: Oid = 3267;
const F_JSONB_AGG_STRICT: Oid = 6284;
const F_JSONB_OBJECT_AGG: Oid = 3270;
const F_JSONB_OBJECT_AGG_STRICT: Oid = 6288;
const F_JSONB_OBJECT_AGG_UNIQUE: Oid = 6289;
const F_JSONB_OBJECT_AGG_UNIQUE_STRICT: Oid = 6290;
pub const JSONPATHOID: Oid = 4072;

const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

fn err(
    pstate: &ParseState<'_, '_>,
    code: types_error::SqlState,
    msg: String,
    location: ParseLoc,
) -> Box<types_error::PgError> {
    let mut e = types_error::PgError::error(msg).with_sqlstate(code);
    if location >= 0 {
        e = e.with_cursor_position(parser_small1::parser_errposition(
            pstate,
            location,
            mbutils::GetDatabaseEncoding(),
        ));
    }
    Box::new(e)
}

fn type_name(t: Oid) -> String {
    format_type::format_type_be(t).unwrap_or_else(|_| t.to_string())
}

fn default_format<'mcx>(mcx: Mcx<'mcx>) -> PgResult<&'mcx JsonFormat> {
    Ok(Node::mk_mut(mcx, JsonFormat::default())?.seal_ref())
}

fn mk_format<'mcx>(
    mcx: Mcx<'mcx>,
    format_type: JsonFormatType,
    encoding: JsonEncoding,
    location: ParseLoc,
) -> PgResult<&'mcx JsonFormat> {
    Ok(Node::mk_mut(mcx, JsonFormat { format_type, encoding, location })?.seal_ref())
}

fn jve<'mcx>(n: Node<'mcx>, what: &str) -> &'mcx JsonValueExpr<'mcx> {
    n.as_json_value_expr().unwrap_or_else(|| panic!("{what}: expected JsonValueExpr"))
}

// getJsonEncodingConst (parse_expr.c:3249): NAMEOID Const with the encoding
// name; UTF8 is the default.
fn get_json_encoding_const<'mcx>(mcx: Mcx<'mcx>, format: &JsonFormat) -> PgResult<Node<'mcx>> {
    let encoding = if format.format_type == JsonFormatType::JS_FORMAT_DEFAULT
        || format.encoding == JsonEncoding::JS_ENC_DEFAULT
    {
        JsonEncoding::JS_ENC_UTF8
    } else {
        format.encoding
    };
    let enc = match encoding {
        JsonEncoding::JS_ENC_UTF16 => "UTF16",
        JsonEncoding::JS_ENC_UTF32 => "UTF32",
        JsonEncoding::JS_ENC_UTF8 => "UTF8",
        other => panic!("invalid JSON encoding: {}", other as i32),
    };
    let mut block = [0u8; 64];
    block[..enc.len()].copy_from_slice(enc.as_bytes());
    let mut name = mcx::vec_with_capacity_in(mcx, 64)?;
    mcx::vec_append_bytes(&mut name, &block)?;
    let d = datum::Datum::from_usize(name.as_ptr() as usize);
    core::mem::forget(name);
    Node::mk_const(mcx, types_core::catalog::NAMEOID, -1, 0, 64, d, false, false)
}

// makeJsonByteaToTextConversion (parse_expr.c:3288): convert_from(expr, enc).
fn make_json_bytea_to_text_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Node<'mcx>,
    format: &JsonFormat,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let encoding = get_json_encoding_const(mcx, format)?;
    Node::mk(
        mcx,
        FuncExpr {
            funcid: F_CONVERT_FROM,
            funcresulttype: TEXTOID,
            funcretset: false,
            funcvariadic: false,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            funccollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, expr, encoding)?,
            location,
        },
    )
}

// transformJsonValueExpr: coerce a JSON value expression per FORMAT clause /
// target type; returns the coerced expr or a JsonValueExpr wrapper with
// formatted_expr set.
fn transformJsonValueExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    construct_name: &str,
    ve: &'mcx JsonValueExpr<'mcx>,
    default_fmt: JsonFormatType,
    targettype: Oid,
    isarg: bool,
) -> PgResult<Node<'mcx>> {
    let mut expr = transformExprRecurse(mcx, pstate, ve.raw_expr.expect("raw_expr"))?;
    let ve_format = ve.format.expect("format");

    if expr_type(expr) == UNKNOWNOID {
        expr = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            expr,
            UNKNOWNOID,
            expr_location(expr),
            TEXTOID,
            construct_name,
        )?;
    }

    let rawexpr = expr;
    let mut exprtype = expr_type(expr);
    let location = expr_location(expr);

    let (typcategory, _typispreferred) = lsyscache::get_type_category_preferred(exprtype)?;

    let format: JsonFormatType;
    if ve_format.format_type != JsonFormatType::JS_FORMAT_DEFAULT {
        if ve_format.encoding != JsonEncoding::JS_ENC_DEFAULT && exprtype != BYTEAOID {
            return Err(err(
                pstate,
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "JSON ENCODING clause is only allowed for bytea input type".into(),
                ve_format.location,
            ));
        }
        if exprtype == JSONOID || exprtype == JSONBOID {
            format = JsonFormatType::JS_FORMAT_DEFAULT;
        } else {
            format = ve_format.format_type;
        }
    } else if isarg {
        // PASSING args: types GetJsonPathVar()/JsonItemFromDatum() take
        // directly skip the json[b] conversion.
        match exprtype {
            types_core::catalog::BOOLOID
            | types_core::catalog::NUMERICOID
            | types_core::catalog::INT2OID
            | types_core::catalog::INT4OID
            | types_core::catalog::INT8OID
            | types_core::catalog::FLOAT4OID
            | types_core::catalog::FLOAT8OID
            | types_core::catalog::TEXTOID
            | types_core::catalog::VARCHAROID
            | types_core::catalog::DATEOID
            | types_core::catalog::TIMEOID
            | types_core::catalog::TIMETZOID
            | types_core::catalog::TIMESTAMPOID
            | types_core::catalog::TIMESTAMPTZOID => return Ok(expr),
            _ => {
                if typcategory == coerce::TYPCATEGORY_STRING {
                    return Ok(expr);
                }
            }
        }
        format = default_fmt;
    } else if exprtype == JSONOID || exprtype == JSONBOID {
        format = JsonFormatType::JS_FORMAT_DEFAULT;
    } else {
        format = default_fmt;
    }

    if format != JsonFormatType::JS_FORMAT_DEFAULT
        || (targettype != 0 && exprtype != targettype)
    {
        let only_allow_cast = targettype != 0;

        if !isarg
            && !only_allow_cast
            && exprtype != BYTEAOID
            && typcategory != coerce::TYPCATEGORY_STRING
        {
            let msg = if ve_format.format_type == JsonFormatType::JS_FORMAT_DEFAULT {
                "cannot use non-string types with implicit FORMAT JSON clause"
            } else {
                "cannot use non-string types with explicit FORMAT JSON clause"
            };
            return Err(err(
                pstate,
                types_error::ERRCODE_DATATYPE_MISMATCH,
                msg.into(),
                if ve_format.location >= 0 { ve_format.location } else { location },
            ));
        }

        if format == JsonFormatType::JS_FORMAT_JSON && exprtype == BYTEAOID {
            expr = make_json_bytea_to_text_conversion(mcx, expr, ve_format, location)?;
        }

        let targettype = if targettype != 0 {
            targettype
        } else if format == JsonFormatType::JS_FORMAT_JSONB {
            JSONBOID
        } else {
            JSONOID
        };
        exprtype = expr_type(expr);

        let coerced = coerce::coerce_to_target_type(
            mcx,
            pstate,
            expr,
            exprtype,
            targettype,
            -1,
            coerce::COERCION_EXPLICIT,
            CoercionForm::COERCE_EXPLICIT_CAST,
            location,
        )?;

        let coerced = match coerced {
            Some(c) => c,
            None => {
                if only_allow_cast {
                    return Err(err(
                        pstate,
                        types_error::ERRCODE_CANNOT_COERCE,
                        format!(
                            "cannot cast type {} to {}",
                            type_name(exprtype),
                            type_name(targettype)
                        ),
                        location,
                    ));
                }
                let fnoid = if targettype == JSONOID { F_TO_JSON } else { F_TO_JSONB };
                Node::mk(
                    mcx,
                    FuncExpr {
                        funcid: fnoid,
                        funcresulttype: targettype,
                        funcretset: false,
                        funcvariadic: false,
                        funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                        funccollid: 0,
                        inputcollid: 0,
                        args: NodeList::make1(mcx, expr)?,
                        location,
                    },
                )?
            }
        };

        if coerced.ptr_eq(expr) {
            expr = rawexpr;
        } else {
            expr = Node::mk(
                mcx,
                JsonValueExpr {
                    raw_expr: Some(rawexpr),
                    formatted_expr: Some(coerced),
                    format: ve.format,
                },
            )?;
        }
    }

    Ok(expr)
}

fn checkJsonOutputFormat(
    pstate: &ParseState<'_, '_>,
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
        let (typcategory, _) = lsyscache::get_type_category_preferred(targettype)?;
        if typcategory != coerce::TYPCATEGORY_STRING {
            return Err(err(
                pstate,
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot use JSON format with non-string output types".into(),
                format.location,
            ));
        }
    }

    if format.format_type == JsonFormatType::JS_FORMAT_JSON {
        let enc = if format.encoding != JsonEncoding::JS_ENC_DEFAULT {
            format.encoding
        } else {
            JsonEncoding::JS_ENC_UTF8
        };
        if targettype != BYTEAOID && format.encoding != JsonEncoding::JS_ENC_DEFAULT {
            return Err(err(
                pstate,
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot set JSON encoding for non-bytea output types".into(),
                format.location,
            ));
        }
        if enc != JsonEncoding::JS_ENC_UTF8 {
            return Err(Box::new(
                types_error::PgError::error("unsupported JSON encoding".to_string())
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Only UTF8 JSON encoding is supported.".to_string())
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        format.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
            ));
        }
    }
    Ok(())
}

fn transformJsonOutput<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    output: Option<Node<'mcx>>,
    allow_format: bool,
) -> PgResult<&'mcx JsonReturning<'mcx>> {
    let Some(output) = output else {
        return Ok(Node::mk_mut(
            mcx,
            JsonReturning { format: Some(default_format(mcx)?), typid: 0, typmod: -1 },
        )?
        .seal_ref());
    };
    let output = output.as_json_output().expect("JsonOutput");
    let base = output.returning.expect("returning");

    let tn_node = output.typeName.expect("typeName");
    let tn = tn_node.as_variant::<TypeName>().expect("TypeName");
    // C typenameTypeIdAndMod (parse_type.c) has no typtype gate; the
    // pseudo-type rejection below owns that error (parse_expr.c:3551).
    let (typid, typmod) =
        parse_utilcmd::typenameTypeIdAndModAllowComposite(mcx, Some(&*pstate), tn)?;

    if tn.setof {
        return Err(err(
            pstate,
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "returning SETOF types is not supported in SQL/JSON functions".into(),
            -1,
        ));
    }
    if lsyscache::get_typtype(typid)? == TYPTYPE_PSEUDO {
        return Err(err(
            pstate,
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "returning pseudo-types is not supported in SQL/JSON functions".into(),
            -1,
        ));
    }

    let base_format = base.format.expect("format");
    let format = if base_format.format_type == JsonFormatType::JS_FORMAT_DEFAULT {
        mk_format(
            mcx,
            if typid == JSONBOID {
                JsonFormatType::JS_FORMAT_JSONB
            } else {
                JsonFormatType::JS_FORMAT_JSON
            },
            base_format.encoding,
            base_format.location,
        )?
    } else {
        checkJsonOutputFormat(pstate, base_format, typid, allow_format)?;
        base_format
    };

    Ok(Node::mk_mut(mcx, JsonReturning { format: Some(format), typid, typmod })?.seal_ref())
}

fn transformJsonConstructorOutput<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    output: Option<Node<'mcx>>,
    args: &NodeList<'mcx>,
) -> PgResult<&'mcx JsonReturning<'mcx>> {
    let returning = transformJsonOutput(mcx, pstate, output, true)?;
    if returning.typid != 0 {
        return Ok(returning);
    }
    let have_jsonb = args.iter().any(|a| expr_type(a) == JSONBOID);
    let (typid, ftype) = if have_jsonb {
        (JSONBOID, JsonFormatType::JS_FORMAT_JSONB)
    } else {
        // C: TEXT is default by the standard, but we return JSON.
        (JSONOID, JsonFormatType::JS_FORMAT_JSON)
    };
    let old = returning.format.expect("format");
    let format = mk_format(mcx, ftype, old.encoding, old.location)?;
    Ok(Node::mk_mut(mcx, JsonReturning { format: Some(format), typid, typmod: -1 })?.seal_ref())
}

// coerceJsonFuncExpr: coerce a json[b]-valued expression to the output type.
// Returns None iff no cast exists and !report_error.
fn coerceJsonFuncExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    returning: &JsonReturning<'mcx>,
    report_error: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let exprtype = expr_type(expr);
    if returning.typid == 0 || returning.typid == exprtype {
        return Ok(Some(expr));
    }

    let format = returning.format.expect("format");
    let mut location = expr_location(expr);
    if location < 0 {
        location = format.location;
    }

    // RETURNING bytea FORMAT JSON: convert_to(text, enc) (parse_expr.c:3629).
    if format.format_type == JsonFormatType::JS_FORMAT_JSON && returning.typid == BYTEAOID {
        let texpr = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            expr,
            exprtype,
            expr_location(expr),
            TEXTOID,
            "JSON_FUNCTION",
        )?;
        let enc = get_json_encoding_const(mcx, format)?;
        return Ok(Some(Node::mk(
            mcx,
            FuncExpr {
                funcid: F_CONVERT_TO,
                funcresulttype: BYTEAOID,
                funcretset: false,
                funcvariadic: false,
                funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                funccollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, texpr, enc)?,
                location,
            },
        )?));
    }

    let res = coerce::coerce_to_target_type(
        mcx,
        pstate,
        expr,
        exprtype,
        returning.typid,
        returning.typmod,
        coerce::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        location,
    )?;

    if res.is_none() && report_error {
        return Err(err(
            pstate,
            types_error::ERRCODE_CANNOT_COERCE,
            format!(
                "cannot cast type {} to {}",
                type_name(exprtype),
                type_name(returning.typid)
            ),
            if location >= 0 { location } else { expr_location(expr) },
        ));
    }
    Ok(res)
}

fn makeJsonConstructorExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    ctor_type: JsonConstructorType,
    args: NodeList<'mcx>,
    fexpr: Option<Node<'mcx>>,
    returning: &'mcx JsonReturning<'mcx>,
    unique: bool,
    absent_on_null: bool,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    // CaseTestExpr placeholder carries the pre-coercion json[b] value.
    let placeholder = match fexpr {
        Some(f) => Node::mk(
            mcx,
            CaseTestExpr {
                typeId: expr_type(f),
                typeMod: nodes_core::node_funcs::expr_typmod(f),
                collation: crate::expr_collation(f),
            },
        )?,
        None => Node::mk(
            mcx,
            CaseTestExpr {
                typeId: if returning.format.expect("format").format_type
                    == JsonFormatType::JS_FORMAT_JSONB
                {
                    JSONBOID
                } else {
                    JSONOID
                },
                typeMod: -1,
                collation: 0,
            },
        )?,
    };

    let coercion = coerceJsonFuncExpr(mcx, pstate, placeholder, returning, true)?
        .expect("report_error=true");
    let coercion = if coercion.ptr_eq(placeholder) { None } else { Some(coercion) };

    Node::mk(
        mcx,
        JsonConstructorExpr {
            r#type: ctor_type,
            args,
            func: fexpr,
            coercion,
            returning: Some(returning),
            absent_on_null,
            unique,
            location,
        },
    )
}

pub(crate) fn transformJsonObjectConstructor<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let ctor = expr.as_json_object_constructor().unwrap();
    let mut args = NodeList::nil();
    for kv in &ctor.exprs {
        let kv = kv.as_json_key_value().expect("JsonKeyValue");
        let key = transformExprRecurse(mcx, pstate, kv.key.expect("key"))?;
        let val = transformJsonValueExpr(
            mcx,
            pstate,
            "JSON_OBJECT()",
            jve(kv.value.expect("value"), "JSON_OBJECT()"),
            JsonFormatType::JS_FORMAT_DEFAULT,
            0,
            false,
        )?;
        args.lappend(mcx, key)?;
        args.lappend(mcx, val)?;
    }
    let returning = transformJsonConstructorOutput(mcx, pstate, ctor.output, &args)?;
    makeJsonConstructorExpr(
        mcx,
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

pub(crate) fn transformJsonArrayConstructor<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let ctor = expr.as_json_array_constructor().unwrap();
    let mut args = NodeList::nil();
    for e in &ctor.exprs {
        let val = transformJsonValueExpr(
            mcx,
            pstate,
            "JSON_ARRAY()",
            jve(e, "JSON_ARRAY()"),
            JsonFormatType::JS_FORMAT_DEFAULT,
            0,
            false,
        )?;
        args.lappend(mcx, val)?;
    }
    let returning = transformJsonConstructorOutput(mcx, pstate, ctor.output, &args)?;
    makeJsonConstructorExpr(
        mcx,
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

// transformJsonArrayQueryConstructor: JSON_ARRAY(query ...) ->
// (SELECT JSON_ARRAYAGG(a ...) FROM (query) q(a)).
// Divergence: C pre-transforms a copyObject of the query only to report
// "subquery must return only one column" at ctor->location; here the EXPR
// sublink transform raises the same error at the sublink location.
pub(crate) fn transformJsonArrayQueryConstructor<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let ctor = expr.as_json_array_query_constructor().unwrap();

    // Transform a copy of the query only to count tlist entries
    // (parse_expr.c:3958-3967).
    let query_copy = copyfuncs::copy_object(mcx, ctor.query.expect("query"))?;
    let qtree = analyze_seams::parse_sub_analyze::call(mcx, query_copy, pstate, None, false, true)?;
    let nonjunk = qtree
        .targetList
        .iter()
        .filter(|te| !te.as_target_entry().expect("tlist entry").resjunk)
        .count();
    if nonjunk != 1 {
        return Err(err(
            pstate,
            types_error::ERRCODE_SYNTAX_ERROR,
            "subquery must return only one column".into(),
            ctor.location,
        ));
    }

    let mut fields = NodeList::make1(mcx, Node::mk_string(mcx, "q")?)?;
    fields.lappend(mcx, Node::mk_string(mcx, "a")?)?;
    let colref = Node::mk(
        mcx,
        types_nodes::rawnodes::ColumnRef { fields, location: ctor.location },
    )?;
    let agg_arg = Node::mk(
        mcx,
        JsonValueExpr {
            raw_expr: Some(colref),
            formatted_expr: Some(colref),
            format: ctor.format,
        },
    )?;
    let agg_ctor = Node::mk(
        mcx,
        types_nodes::rawnodes::JsonAggConstructor {
            output: ctor.output,
            agg_filter: None,
            agg_order: NodeList::nil(),
            over: None,
            location: ctor.location,
        },
    )?;
    let agg = Node::mk(
        mcx,
        types_nodes::rawnodes::JsonArrayAgg {
            constructor: Some(agg_ctor),
            arg: Some(agg_arg),
            absent_on_null: ctor.absent_on_null,
        },
    )?;

    let target = Node::mk(
        mcx,
        ResTarget {
            name: None,
            indirection: NodeList::nil(),
            val: Some(agg),
            location: ctor.location,
        },
    )?;

    let alias = Node::mk_mut(
        mcx,
        types_nodes::Alias {
            aliasname: Some("q"),
            colnames: NodeList::make1(mcx, Node::mk_string(mcx, "a")?)?,
        },
    )?
    .seal_ref();

    let range = Node::mk(
        mcx,
        RangeSubselect {
            lateral: false,
            subquery: ctor.query,
            alias: Some(alias),
        },
    )?;

    let mut select = Node::build::<types_nodes::SelectStmt>(mcx)?;
    select.targetList = NodeList::make1(mcx, target)?;
    select.fromClause = NodeList::make1(mcx, range)?;
    let select = select.seal();

    let sublink = Node::mk(
        mcx,
        SubLink {
            subLinkType: SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: select,
            location: ctor.location,
        },
    )?;

    transformExprRecurse(mcx, pstate, sublink)
}

fn transformJsonAggConstructor<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    agg_ctor_node: Node<'mcx>,
    returning: &'mcx JsonReturning<'mcx>,
    args: NodeList<'mcx>,
    aggfnoid: Oid,
    aggtype: Oid,
    ctor_type: JsonConstructorType,
    unique: bool,
    absent_on_null: bool,
) -> PgResult<Node<'mcx>> {
    let agg_ctor = agg_ctor_node.as_json_agg_constructor().expect("JsonAggConstructor");

    let aggfilter = match agg_ctor.agg_filter {
        None => None,
        Some(f) => {
            let qual = crate::transformExpr(mcx, pstate, f, ParseExprKind::EXPR_KIND_FILTER)?;
            Some(coerce::coerce_to_boolean(
                mcx,
                pstate,
                qual,
                expr_type(qual),
                expr_location(qual),
                "FILTER",
            )?)
        }
    };

    let node;
    if let Some(over) = agg_ctor.over {
        let mut wfunc = Node::build::<WindowFunc>(mcx)?;
        wfunc.winfnoid = aggfnoid;
        wfunc.wintype = aggtype;
        wfunc.args = args;
        wfunc.aggfilter = aggfilter;
        wfunc.winstar = false;
        wfunc.winagg = true;
        wfunc.location = agg_ctor.location;

        if !agg_ctor.agg_order.is_nil() {
            return Err(err(
                pstate,
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "aggregate ORDER BY is not implemented for window functions".into(),
                agg_ctor.location,
            ));
        }

        parse_agg::transformWindowFuncCall(mcx, pstate, &mut wfunc, over)?;
        node = wfunc.seal();
    } else {
        let mut arg_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, args.len())?;
        for a in &args {
            arg_types.push(expr_type(a));
        }
        let mut aggref = Node::build::<Aggref>(mcx)?;
        aggref.aggfnoid = aggfnoid;
        aggref.aggtype = aggtype;
        aggref.aggfilter = aggfilter;
        aggref.aggstar = false;
        aggref.aggvariadic = false;
        aggref.aggkind = AGGKIND_NORMAL;
        aggref.location = agg_ctor.location;

        parse_agg::transformAggregateCall(
            mcx,
            pstate,
            &mut aggref,
            &args,
            arg_types.as_slice(),
            &agg_ctor.agg_order,
            false,
        )?;
        node = aggref.seal();
    }

    makeJsonConstructorExpr(
        mcx,
        pstate,
        ctor_type,
        NodeList::nil(),
        Some(node),
        returning,
        unique,
        absent_on_null,
        agg_ctor.location,
    )
}

pub(crate) fn transformJsonObjectAgg<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let agg = expr.as_json_object_agg().unwrap();
    let arg = agg.arg.expect("arg").as_json_key_value().expect("JsonKeyValue");

    let key = transformExprRecurse(mcx, pstate, arg.key.expect("key"))?;
    let val = transformJsonValueExpr(
        mcx,
        pstate,
        "JSON_OBJECTAGG()",
        jve(arg.value.expect("value"), "JSON_OBJECTAGG()"),
        JsonFormatType::JS_FORMAT_DEFAULT,
        0,
        false,
    )?;
    let mut args = NodeList::make1(mcx, key)?;
    args.lappend(mcx, val)?;

    let ctor_node = agg.constructor.expect("constructor");
    let output = ctor_node.as_json_agg_constructor().expect("JsonAggConstructor").output;
    let returning = transformJsonConstructorOutput(mcx, pstate, output, &args)?;

    let is_jsonb =
        returning.format.expect("format").format_type == JsonFormatType::JS_FORMAT_JSONB;
    let (aggfnoid, aggtype) = if is_jsonb {
        (
            match (agg.absent_on_null, agg.unique) {
                (true, true) => F_JSONB_OBJECT_AGG_UNIQUE_STRICT,
                (true, false) => F_JSONB_OBJECT_AGG_STRICT,
                (false, true) => F_JSONB_OBJECT_AGG_UNIQUE,
                (false, false) => F_JSONB_OBJECT_AGG,
            },
            JSONBOID,
        )
    } else {
        (
            match (agg.absent_on_null, agg.unique) {
                (true, true) => F_JSON_OBJECT_AGG_UNIQUE_STRICT,
                (true, false) => F_JSON_OBJECT_AGG_STRICT,
                (false, true) => F_JSON_OBJECT_AGG_UNIQUE,
                (false, false) => F_JSON_OBJECT_AGG,
            },
            JSONOID,
        )
    };

    transformJsonAggConstructor(
        mcx,
        pstate,
        ctor_node,
        returning,
        args,
        aggfnoid,
        aggtype,
        JsonConstructorType::JSCTOR_JSON_OBJECTAGG,
        agg.unique,
        agg.absent_on_null,
    )
}

pub(crate) fn transformJsonArrayAgg<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let agg = expr.as_json_array_agg().unwrap();

    let arg = transformJsonValueExpr(
        mcx,
        pstate,
        "JSON_ARRAYAGG()",
        jve(agg.arg.expect("arg"), "JSON_ARRAYAGG()"),
        JsonFormatType::JS_FORMAT_DEFAULT,
        0,
        false,
    )?;
    let args = NodeList::make1(mcx, arg)?;

    let ctor_node = agg.constructor.expect("constructor");
    let output = ctor_node.as_json_agg_constructor().expect("JsonAggConstructor").output;
    let returning = transformJsonConstructorOutput(mcx, pstate, output, &args)?;

    let is_jsonb =
        returning.format.expect("format").format_type == JsonFormatType::JS_FORMAT_JSONB;
    let (aggfnoid, aggtype) = if is_jsonb {
        (if agg.absent_on_null { F_JSONB_AGG_STRICT } else { F_JSONB_AGG }, JSONBOID)
    } else {
        (if agg.absent_on_null { F_JSON_AGG_STRICT } else { F_JSON_AGG }, JSONOID)
    };

    transformJsonAggConstructor(
        mcx,
        pstate,
        ctor_node,
        returning,
        args,
        aggfnoid,
        aggtype,
        JsonConstructorType::JSCTOR_JSON_ARRAYAGG,
        false,
        agg.absent_on_null,
    )
}

// transformJsonParseArg: prepare the input document for JSON()/IS JSON.
fn transformJsonParseArg<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    jsexpr: Node<'mcx>,
    format: &'mcx JsonFormat,
    exprtype: &mut Oid,
) -> PgResult<Node<'mcx>> {
    let raw_expr = transformExprRecurse(mcx, pstate, jsexpr)?;
    let mut expr = raw_expr;
    *exprtype = expr_type(expr);

    if *exprtype == BYTEAOID {
        expr = make_json_bytea_to_text_conversion(mcx, expr, format, expr_location(expr))?;
        *exprtype = TEXTOID;
        expr = Node::mk(
            mcx,
            JsonValueExpr {
                raw_expr: Some(raw_expr),
                formatted_expr: Some(expr),
                format: Some(format),
            },
        )?;
    } else {
        let (typcategory, _) = lsyscache::get_type_category_preferred(*exprtype)?;
        if *exprtype == UNKNOWNOID || typcategory == coerce::TYPCATEGORY_STRING {
            expr = coerce::coerce_to_target_type(
                mcx,
                pstate,
                expr,
                *exprtype,
                TEXTOID,
                -1,
                coerce::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?
            .expect("string category coerces to text");
            *exprtype = TEXTOID;
        }
        if format.encoding != JsonEncoding::JS_ENC_DEFAULT {
            return Err(err(
                pstate,
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot use JSON FORMAT ENCODING clause for non-bytea input types".into(),
                format.location,
            ));
        }
    }
    Ok(expr)
}

pub(crate) fn transformJsonIsPredicate<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let pred = expr.as_json_is_predicate().unwrap();
    let mut exprtype = 0;
    let arg = transformJsonParseArg(
        mcx,
        pstate,
        pred.expr.expect("expr"),
        pred.format.expect("format"),
        &mut exprtype,
    )?;

    if exprtype != TEXTOID && exprtype != JSONOID && exprtype != JSONBOID {
        return Err(err(
            pstate,
            types_error::ERRCODE_DATATYPE_MISMATCH,
            format!("cannot use type {} in IS JSON predicate", type_name(exprtype)),
            -1,
        ));
    }

    // C intentionally(?) drops the format clause.
    Node::mk(
        mcx,
        JsonIsPredicate {
            expr: Some(arg),
            format: None,
            item_type: pred.item_type,
            unique_keys: pred.unique_keys,
            location: pred.location,
        },
    )
}

fn transformJsonReturning<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    output: Option<Node<'mcx>>,
    fname: &str,
) -> PgResult<&'mcx JsonReturning<'mcx>> {
    if let Some(output_node) = output {
        let returning = transformJsonOutput(mcx, pstate, Some(output_node), false)?;
        debug_assert!(returning.typid != 0);
        if returning.typid != JSONOID && returning.typid != JSONBOID {
            let tn_loc = output_node
                .as_json_output()
                .and_then(|o| o.typeName)
                .and_then(|t| t.as_variant::<TypeName>())
                .map_or(-1, |t| t.location);
            return Err(Box::new(
                types_error::PgError::error(format!(
                    "cannot use type {} in RETURNING clause of {}",
                    type_name(returning.typid),
                    fname
                ))
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                .with_hint("Try returning json or jsonb.".to_string())
                .with_cursor_position(parser_small1::parser_errposition(
                    pstate,
                    tn_loc,
                    mbutils::GetDatabaseEncoding(),
                )),
            ));
        }
        Ok(returning)
    } else {
        let format = mk_format(mcx, JsonFormatType::JS_FORMAT_JSON, JsonEncoding::JS_ENC_DEFAULT, -1)?;
        Ok(Node::mk_mut(
            mcx,
            JsonReturning { format: Some(format), typid: JSONOID, typmod: -1 },
        )?
        .seal_ref())
    }
}

pub(crate) fn transformJsonParseExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let jsexpr = expr.as_json_parse_expr().unwrap();
    let returning = transformJsonReturning(mcx, pstate, jsexpr.output, "JSON()")?;

    let arg = if jsexpr.unique_keys {
        let ve = jve(jsexpr.expr.expect("expr"), "JSON()");
        let mut arg_type = 0;
        let arg = transformJsonParseArg(
            mcx,
            pstate,
            ve.raw_expr.expect("raw_expr"),
            ve.format.expect("format"),
            &mut arg_type,
        )?;
        if arg_type != TEXTOID {
            return Err(err(
                pstate,
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "cannot use non-string types with WITH UNIQUE KEYS clause".into(),
                jsexpr.location,
            ));
        }
        arg
    } else {
        transformJsonValueExpr(
            mcx,
            pstate,
            "JSON()",
            jve(jsexpr.expr.expect("expr"), "JSON()"),
            JsonFormatType::JS_FORMAT_JSON,
            returning.typid,
            false,
        )?
    };

    makeJsonConstructorExpr(
        mcx,
        pstate,
        JsonConstructorType::JSCTOR_JSON_PARSE,
        NodeList::make1(mcx, arg)?,
        None,
        returning,
        jsexpr.unique_keys,
        false,
        jsexpr.location,
    )
}

pub(crate) fn transformJsonScalarExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let jsexpr = expr.as_json_scalar_expr().unwrap();
    let mut arg = transformExprRecurse(mcx, pstate, jsexpr.expr.expect("expr"))?;
    let returning = transformJsonReturning(mcx, pstate, jsexpr.output, "JSON_SCALAR()")?;

    if expr_type(arg) == UNKNOWNOID {
        arg = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            arg,
            UNKNOWNOID,
            expr_location(arg),
            TEXTOID,
            "JSON_SCALAR",
        )?;
    }

    makeJsonConstructorExpr(
        mcx,
        pstate,
        JsonConstructorType::JSCTOR_JSON_SCALAR,
        NodeList::make1(mcx, arg)?,
        None,
        returning,
        false,
        false,
        jsexpr.location,
    )
}

pub(crate) fn transformJsonSerializeExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let jsexpr = expr.as_json_serialize_expr().unwrap();
    let arg = transformJsonValueExpr(
        mcx,
        pstate,
        "JSON_SERIALIZE()",
        jve(jsexpr.expr.expect("expr"), "JSON_SERIALIZE()"),
        JsonFormatType::JS_FORMAT_JSON,
        0,
        false,
    )?;

    let returning = if let Some(output) = jsexpr.output {
        let returning = transformJsonOutput(mcx, pstate, Some(output), true)?;
        if returning.typid != BYTEAOID {
            let (typcategory, _) = lsyscache::get_type_category_preferred(returning.typid)?;
            if typcategory != coerce::TYPCATEGORY_STRING {
                return Err(Box::new(
                    types_error::PgError::error(format!(
                        "cannot use type {} in RETURNING clause of {}",
                        type_name(returning.typid),
                        "JSON_SERIALIZE()"
                    ))
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                    .with_hint("Try returning a string type or bytea.".to_string()),
                ));
            }
        }
        returning
    } else {
        // RETURNING TEXT FORMAT JSON by default.
        let format =
            mk_format(mcx, JsonFormatType::JS_FORMAT_JSON, JsonEncoding::JS_ENC_DEFAULT, -1)?;
        Node::mk_mut(mcx, JsonReturning { format: Some(format), typid: TEXTOID, typmod: -1 })?
            .seal_ref()
    };

    makeJsonConstructorExpr(
        mcx,
        pstate,
        JsonConstructorType::JSCTOR_JSON_SERIALIZE,
        NodeList::make1(mcx, arg)?,
        None,
        returning,
        false,
        false,
        jsexpr.location,
    )
}

fn behavior_of<'mcx>(n: Option<Node<'mcx>>) -> Option<&'mcx JsonBehavior<'mcx>> {
    n.map(|n| n.as_json_behavior().expect("JsonBehavior"))
}

#[cold]
fn invalid_behavior_err(
    pstate: &ParseState<'_, '_>,
    clause: &str,
    column_name: Option<&str>,
    detail_ctx: &str,
    detail_col: &str,
    location: ParseLoc,
) -> Box<types_error::PgError> {
    let (msg, detail) = match column_name {
        None => (format!("invalid {clause} behavior"), detail_ctx.to_string()),
        Some(col) => (
            format!("invalid {clause} behavior for column \"{col}\""),
            detail_col.to_string(),
        ),
    };
    Box::new(
        types_error::PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
            .with_detail(detail)
            .with_cursor_position(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            )),
    )
}

pub(crate) fn transformJsonFuncExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let func = expr.as_json_func_expr().unwrap();

    let (func_name, default_fmt) = match func.op {
        JsonExprOp::JSON_EXISTS_OP => ("JSON_EXISTS", JsonFormatType::JS_FORMAT_DEFAULT),
        JsonExprOp::JSON_QUERY_OP => ("JSON_QUERY", JsonFormatType::JS_FORMAT_JSONB),
        JsonExprOp::JSON_VALUE_OP => ("JSON_VALUE", JsonFormatType::JS_FORMAT_DEFAULT),
        JsonExprOp::JSON_TABLE_OP => ("JSON_TABLE", JsonFormatType::JS_FORMAT_JSONB),
    };

    if let Some(output) = func.output {
        if func.op != JsonExprOp::JSON_QUERY_OP {
            let format = output
                .as_json_output()
                .expect("JsonOutput")
                .returning
                .expect("returning")
                .format
                .expect("format");
            if format.format_type != JsonFormatType::JS_FORMAT_DEFAULT
                || format.encoding != JsonEncoding::JS_ENC_DEFAULT
            {
                return Err(err(
                    pstate,
                    types_error::ERRCODE_SYNTAX_ERROR,
                    format!("cannot specify FORMAT JSON in RETURNING clause of {func_name}()"),
                    format.location,
                ));
            }
        }
    }

    let on_empty_b = behavior_of(func.on_empty);
    let on_error_b = behavior_of(func.on_error);

    if func.op == JsonExprOp::JSON_QUERY_OP {
        if func.quotes == JsonQuotes::JS_QUOTES_OMIT
            && matches!(
                func.wrapper,
                JsonWrapper::JSW_CONDITIONAL | JsonWrapper::JSW_UNCONDITIONAL
            )
        {
            return Err(err(
                pstate,
                types_error::ERRCODE_SYNTAX_ERROR,
                "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used"
                    .into(),
                func.location,
            ));
        }
        for (b, clause) in [(on_empty_b, "ON EMPTY"), (on_error_b, "ON ERROR")] {
            if let Some(b) = b {
                if !matches!(
                    b.btype,
                    JsonBehaviorType::JSON_BEHAVIOR_ERROR
                        | JsonBehaviorType::JSON_BEHAVIOR_NULL
                        | JsonBehaviorType::JSON_BEHAVIOR_EMPTY
                        | JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY
                        | JsonBehaviorType::JSON_BEHAVIOR_EMPTY_OBJECT
                        | JsonBehaviorType::JSON_BEHAVIOR_DEFAULT
                ) {
                    return Err(invalid_behavior_err(
                        pstate,
                        clause,
                        func.column_name,
                        &format!("Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression is allowed in {clause} for {}.", "JSON_QUERY()"),
                        &format!("Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression is allowed in {clause} for formatted columns."),
                        b.location,
                    ));
                }
            }
        }
    }

    if func.op == JsonExprOp::JSON_EXISTS_OP {
        if let Some(b) = on_error_b {
            if !matches!(
                b.btype,
                JsonBehaviorType::JSON_BEHAVIOR_ERROR
                    | JsonBehaviorType::JSON_BEHAVIOR_TRUE
                    | JsonBehaviorType::JSON_BEHAVIOR_FALSE
                    | JsonBehaviorType::JSON_BEHAVIOR_UNKNOWN
            ) {
                return Err(invalid_behavior_err(
                    pstate,
                    "ON ERROR",
                    func.column_name,
                    &format!(
                        "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in {} for {}.",
                        "ON ERROR", "JSON_EXISTS()"
                    ),
                    "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in ON ERROR for EXISTS columns.",
                    b.location,
                ));
            }
        }
    }
    if func.op == JsonExprOp::JSON_VALUE_OP {
        for (b, clause) in [(on_empty_b, "ON EMPTY"), (on_error_b, "ON ERROR")] {
            if let Some(b) = b {
                if !matches!(
                    b.btype,
                    JsonBehaviorType::JSON_BEHAVIOR_ERROR
                        | JsonBehaviorType::JSON_BEHAVIOR_NULL
                        | JsonBehaviorType::JSON_BEHAVIOR_DEFAULT
                ) {
                    return Err(invalid_behavior_err(
                        pstate,
                        clause,
                        func.column_name,
                        &format!("Only ERROR, NULL, or DEFAULT expression is allowed in {clause} for {}.", "JSON_VALUE()"),
                        &format!("Only ERROR, NULL, or DEFAULT expression is allowed in {clause} for scalar columns."),
                        b.location,
                    ));
                }
            }
        }
    }

    let context_item = jve(func.context_item.expect("context_item"), func_name);

    // jsonpath machinery handles only jsonb; coerce the input.
    let formatted_expr = transformJsonValueExpr(
        mcx,
        pstate,
        func_name,
        context_item,
        default_fmt,
        JSONBOID,
        false,
    )?;

    let path_spec = transformExprRecurse(mcx, pstate, func.pathspec.expect("pathspec"))?;
    let pathspec_type = expr_type(path_spec);
    let pathspec_loc = expr_location(path_spec);
    let coerced_path_spec = coerce::coerce_to_target_type(
        mcx,
        pstate,
        path_spec,
        pathspec_type,
        JSONPATHOID,
        -1,
        coerce::COERCION_EXPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        pathspec_loc,
    )?;
    let Some(coerced_path_spec) = coerced_path_spec else {
        return Err(err(
            pstate,
            types_error::ERRCODE_DATATYPE_MISMATCH,
            format!(
                "JSON path expression must be of type {}, not of type {}",
                "jsonpath",
                type_name(pathspec_type)
            ),
            pathspec_loc,
        ));
    };

    // PASSING args, coerced to jsonb.
    let mut passing_values = NodeList::nil();
    let mut passing_names = NodeList::nil();
    for arg in &func.passing {
        let arg = arg.as_json_argument().expect("JsonArgument");
        let e = transformJsonValueExpr(
            mcx,
            pstate,
            func_name,
            jve(arg.val.expect("val"), func_name),
            JsonFormatType::JS_FORMAT_JSONB,
            0,
            true,
        )?;
        passing_values.lappend(mcx, e)?;
        passing_names.lappend(mcx, Node::mk_string(mcx, arg.name.expect("name"))?)?;
    }

    let returning = transformJsonOutput(mcx, pstate, func.output, false)?;

    let mut jsexpr = JsonExpr {
        op: func.op,
        column_name: func.column_name,
        formatted_expr: Some(formatted_expr),
        format: context_item.format,
        path_spec: Some(coerced_path_spec),
        returning: Some(returning),
        passing_names,
        passing_values,
        on_empty: None,
        on_error: None,
        use_io_coercion: false,
        use_json_coercion: false,
        wrapper: JsonWrapper::JSW_UNSPEC,
        omit_quotes: false,
        collation: 0,
        location: func.location,
    };

    match func.op {
        JsonExprOp::JSON_EXISTS_OP => {
            let returning = if returning.typid == 0 {
                Node::mk_mut(
                    mcx,
                    JsonReturning { format: returning.format, typid: BOOLOID, typmod: -1 },
                )?
                .seal_ref()
            } else {
                returning
            };
            jsexpr.returning = Some(returning);
            if returning.typid != BOOLOID {
                jsexpr.use_json_coercion = true;
            }
            jsexpr.on_error = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_error,
                JsonBehaviorType::JSON_BEHAVIOR_FALSE,
                returning,
            )?);
        }
        JsonExprOp::JSON_QUERY_OP => {
            let returning = if returning.typid == 0 {
                Node::mk_mut(
                    mcx,
                    JsonReturning { format: returning.format, typid: JSONBOID, typmod: -1 },
                )?
                .seal_ref()
            } else {
                returning
            };
            jsexpr.returning = Some(returning);
            jsexpr.collation = lsyscache::get_typcollation(returning.typid)?;
            jsexpr.omit_quotes = func.quotes == JsonQuotes::JS_QUOTES_OMIT;
            jsexpr.wrapper = func.wrapper;
            if returning.typid != JSONBOID || jsexpr.omit_quotes {
                jsexpr.use_json_coercion = true;
            }
            jsexpr.on_empty = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_empty,
                JsonBehaviorType::JSON_BEHAVIOR_NULL,
                returning,
            )?);
            jsexpr.on_error = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_error,
                JsonBehaviorType::JSON_BEHAVIOR_NULL,
                returning,
            )?);
        }
        JsonExprOp::JSON_VALUE_OP => {
            let returning = if returning.typid == 0 {
                Node::mk_mut(
                    mcx,
                    JsonReturning { format: returning.format, typid: TEXTOID, typmod: -1 },
                )?
                .seal_ref()
            } else {
                returning
            };
            // Override transformJsonOutput's jsonb-oriented format.
            let returning = Node::mk_mut(
                mcx,
                JsonReturning {
                    format: Some(mk_format(
                        mcx,
                        JsonFormatType::JS_FORMAT_DEFAULT,
                        JsonEncoding::JS_ENC_DEFAULT,
                        returning.format.expect("format").location,
                    )?),
                    typid: returning.typid,
                    typmod: returning.typmod,
                },
            )?
            .seal_ref();
            jsexpr.returning = Some(returning);
            jsexpr.collation = lsyscache::get_typcollation(returning.typid)?;
            jsexpr.omit_quotes = true;
            if returning.typid != TEXTOID {
                if lsyscache::get_typtype(returning.typid)? == TYPTYPE_DOMAIN
                    && typcache_seams::domain_has_constraints::call(returning.typid)?
                {
                    jsexpr.use_json_coercion = true;
                } else {
                    jsexpr.use_io_coercion = true;
                }
            }
            jsexpr.on_empty = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_empty,
                JsonBehaviorType::JSON_BEHAVIOR_NULL,
                returning,
            )?);
            jsexpr.on_error = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_error,
                JsonBehaviorType::JSON_BEHAVIOR_NULL,
                returning,
            )?);
        }
        JsonExprOp::JSON_TABLE_OP => {
            let returning = if returning.typid == 0 {
                Node::mk_mut(
                    mcx,
                    JsonReturning {
                        format: returning.format,
                        typid: expr_type(formatted_expr),
                        typmod: -1,
                    },
                )?
                .seal_ref()
            } else {
                returning
            };
            jsexpr.returning = Some(returning);
            jsexpr.collation = lsyscache::get_typcollation(returning.typid)?;
            // ON EMPTY is column-level only; the top level takes EMPTY ARRAY
            // ON ERROR by default.
            jsexpr.on_error = Some(transformJsonBehavior(
                mcx,
                pstate,
                &jsexpr,
                func.on_error,
                JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY,
                returning,
            )?);
        }
    }

    Node::mk(mcx, jsexpr)
}

// ValidJsonBehaviorDefaultExpr (parse_expr.c).
fn valid_json_behavior_default_expr(expr: Node<'_>) -> bool {
    match expr.node_tag() {
        NodeTag::T_Const | NodeTag::T_FuncExpr | NodeTag::T_OpExpr => true,
        NodeTag::T_CoerceViaIO => valid_json_behavior_default_expr(
            expr.as_coerce_via_io().unwrap().arg,
        ),
        NodeTag::T_ArrayCoerceExpr => {
            let a = expr.as_array_coerce_expr().unwrap();
            valid_json_behavior_default_expr(a.arg)
                || a.elemexpr.is_some_and(valid_json_behavior_default_expr)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            valid_json_behavior_default_expr(expr.as_convert_rowtype_expr().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => valid_json_behavior_default_expr(
            expr.as_coerce_to_domain().unwrap().arg,
        ),
        NodeTag::T_RelabelType => {
            valid_json_behavior_default_expr(expr.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_CollateExpr => {
            valid_json_behavior_default_expr(expr.as_collate_expr().unwrap().arg)
        }
        _ => false,
    }
}

fn jsonb_const<'mcx>(mcx: Mcx<'mcx>, json: &str, location: ParseLoc) -> PgResult<Node<'mcx>> {
    let image = adt_jsonb::io::jsonb_in(mcx, json.as_bytes(), None)?
        .expect("hard errsave without escontext returns Err");
    let d = datum::Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    let n = Node::mk_const(mcx, JSONBOID, -1, 0, -1, d, false, false)?;
    // SAFETY: parse-tree owned Const, no derived refs.
    unsafe {
        n.with_mut::<types_nodes::Const, _>(|c| c.location = location).expect("Const");
    }
    Ok(n)
}

fn get_json_behavior_const<'mcx>(
    mcx: Mcx<'mcx>,
    btype: JsonBehaviorType,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let n = match btype {
        JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY => return jsonb_const(mcx, "[]", location),
        JsonBehaviorType::JSON_BEHAVIOR_EMPTY_OBJECT => return jsonb_const(mcx, "{}", location),
        JsonBehaviorType::JSON_BEHAVIOR_TRUE => {
            Node::mk_const(mcx, BOOLOID, -1, 0, 1, datum::Datum::from_bool(true), false, true)?
        }
        JsonBehaviorType::JSON_BEHAVIOR_FALSE => {
            Node::mk_const(mcx, BOOLOID, -1, 0, 1, datum::Datum::from_bool(false), false, true)?
        }
        JsonBehaviorType::JSON_BEHAVIOR_NULL
        | JsonBehaviorType::JSON_BEHAVIOR_UNKNOWN
        | JsonBehaviorType::JSON_BEHAVIOR_EMPTY => {
            Node::mk_const(mcx, INT4OID, -1, 0, 4, datum::Datum::from_i32(0), true, true)?
        }
        JsonBehaviorType::JSON_BEHAVIOR_DEFAULT | JsonBehaviorType::JSON_BEHAVIOR_ERROR => {
            unreachable!("handled by caller")
        }
    };
    // SAFETY: parse-tree owned Const, no derived refs.
    unsafe {
        n.with_mut::<types_nodes::Const, _>(|c| c.location = location).expect("Const");
    }
    Ok(n)
}

fn transformJsonBehavior<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    jsexpr: &JsonExpr<'mcx>,
    behavior: Option<Node<'mcx>>,
    default_behavior: JsonBehaviorType,
    returning: &JsonReturning<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut btype = default_behavior;
    let mut expr: Option<Node<'mcx>> = None;
    let mut coerce_at_runtime = false;
    let mut location: ParseLoc = -1;

    if let Some(behavior_node) = behavior {
        let b = behavior_node.as_json_behavior().expect("JsonBehavior");
        btype = b.btype;
        location = b.location;
        if btype == JsonBehaviorType::JSON_BEHAVIOR_DEFAULT {
            let e = transformExprRecurse(mcx, pstate, b.expr.expect("DEFAULT expr"))?;
            if !valid_json_behavior_default_expr(e) {
                return Err(err(
                    pstate,
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "can only specify a constant, non-aggregate function, or operator expression for DEFAULT".into(),
                    expr_location(e),
                ));
            }
            if vars::contain_var_clause(e)? {
                return Err(err(
                    pstate,
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "DEFAULT expression must not contain column references".into(),
                    expr_location(e),
                ));
            }
            if coerce::expression_returns_set(e) {
                return Err(err(
                    pstate,
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "DEFAULT expression must not return a set".into(),
                    expr_location(e),
                ));
            }
            let mut exprcoll = crate::expr_collation(e);
            if exprcoll == 0 {
                exprcoll = lsyscache::get_typcollation(expr_type(e))?;
            }
            let targetcoll = jsexpr.collation;
            if targetcoll != 0 && exprcoll != 0 && targetcoll != exprcoll {
                return Err(Box::new(
                    types_error::PgError::error(
                        "collation of DEFAULT expression conflicts with RETURNING clause"
                            .to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_COLLATION_MISMATCH)
                    .with_detail(format!(
                        "\"{}\" versus \"{}\"",
                        lsyscache::get_collation_name(mcx, exprcoll)?
                            .map_or_else(|| exprcoll.to_string(), |s| s.as_str().to_string()),
                        lsyscache::get_collation_name(mcx, targetcoll)?
                            .map_or_else(|| targetcoll.to_string(), |s| s.as_str().to_string()),
                    ))
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        expr_location(e),
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            expr = Some(e);
        }
    }

    if expr.is_none() && btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR {
        expr = Some(get_json_behavior_const(mcx, btype, location)?);
    }

    if let Some(e) = expr {
        if expr_type(e) != returning.typid {
            let isnull = e.as_const().is_some_and(|c| c.constisnull);
            if isnull
                || expr_type(e) == JSONBOID
                || (expr_type(e) == BOOLOID && lsyscache::getBaseType(returning.typid)? != INT4OID)
            {
                coerce_at_runtime = true;
                if expr_type(e) == BOOLOID {
                    let val =
                        if btype == JsonBehaviorType::JSON_BEHAVIOR_TRUE { "true" } else { "false" };
                    expr = Some(jsonb_const(mcx, val, -1)?);
                }
            } else {
                let typcategory = lsyscache::get_type_category_preferred(returning.typid)?.0;
                // 'V' = TYPCATEGORY_BITSTRING.
                let ccontext = if typcategory == coerce::TYPCATEGORY_STRING
                    || typcategory == b'V' as i8
                {
                    coerce::COERCION_ASSIGNMENT
                } else {
                    coerce::COERCION_EXPLICIT
                };
                let coerced = coerce::coerce_to_target_type(
                    mcx,
                    pstate,
                    e,
                    expr_type(e),
                    returning.typid,
                    returning.typmod,
                    ccontext,
                    CoercionForm::COERCE_EXPLICIT_CAST,
                    behavior.map_or(-1, expr_location),
                )?;
                let Some(coerced) = coerced else {
                    let base = format!(
                        "cannot cast behavior expression of type {} to {}",
                        type_name(expr_type(e)),
                        type_name(returning.typid)
                    );
                    let mut pe = types_error::PgError::error(base)
                        .with_sqlstate(types_error::ERRCODE_CANNOT_COERCE)
                        .with_cursor_position(parser_small1::parser_errposition(
                            pstate,
                            expr_location(e),
                            mbutils::GetDatabaseEncoding(),
                        ));
                    if btype == JsonBehaviorType::JSON_BEHAVIOR_DEFAULT {
                        pe = pe.with_hint(format!(
                            "You will need to explicitly cast the expression to type {}.",
                            type_name(returning.typid)
                        ));
                    }
                    return Err(Box::new(pe));
                };
                expr = Some(coerced);
            }
        }
    }

    Node::mk(
        mcx,
        JsonBehavior { btype, expr, coerce: coerce_at_runtime, location },
    )
}
