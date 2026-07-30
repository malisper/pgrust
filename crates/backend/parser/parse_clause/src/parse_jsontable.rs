// parse_jsontable.c: JSON_TABLE() transform into a TableFunc RTE.
use mcx::Mcx;
use types_core::catalog::{INT4OID, JSONBOID, JSONOID, RECORDOID};
use types_core::{Oid, ParseLoc};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{
    CaseTestExpr, JsonBehaviorType, JsonFormat, JsonReturning, JsonTablePath, JsonTablePathScan,
    JsonTableSiblingJoin, JsonWrapper, TableFunc, TableFuncType,
};
use types_nodes::rawnodes::{
    JsonFuncExpr, JsonQuotes, JsonTable, JsonTableColumn, JsonTableColumnType, JsonTablePathSpec,
    ValUnion,
};
use types_nodes::{Node, NodeList};

use parser_small1::{parser_errposition, ParseExprKind, ParseNamespaceItem, ParseState};

const JSONPATHOID: Oid = 4072;
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

struct JsonTableParseContext<'mcx> {
    path_name_id: i32,
    path_names: mcx::PgVec<'mcx, &'mcx str>,
}

pub(crate) fn transformJsonTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    jt: &'mcx JsonTable<'mcx>,
) -> PgResult<&'mcx ParseNamespaceItem<'mcx>> {
    let root_pathspec_node = jt.pathspec.expect("grammar sets pathspec");

    if let Some(oe) = jt.on_error {
        let b = oe.as_json_behavior().expect("JsonBehavior");
        if b.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR
            && b.btype != JsonBehaviorType::JSON_BEHAVIOR_EMPTY
            && b.btype != JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY
        {
            return Err(jsontable_err(
                pstate,
                types_error::ERRCODE_SYNTAX_ERROR,
                "invalid ON ERROR behavior".into(),
                Some(
                    "Only EMPTY [ ARRAY ] or ERROR is allowed in the top-level ON ERROR clause."
                        .into(),
                ),
                b.location,
            ));
        }
    }

    let mut cxt =
        JsonTableParseContext { path_name_id: 0, path_names: mcx::PgVec::new_in(mcx) };
    let root_name = root_pathspec_node
        .as_json_table_path_spec()
        .expect("pathspec is JsonTablePathSpec")
        .name;
    match root_name {
        None => {
            let name = generateJsonTablePathName(mcx, &mut cxt)?;
            // SAFETY: parser-owned tree, no live derived refs.
            unsafe {
                root_pathspec_node
                    .with_mut::<JsonTablePathSpec, _>(|p| p.name = Some(name))
                    .expect("JsonTablePathSpec")
            };
        }
        Some(name) => {
            cxt.path_names.try_reserve(1).map_err(|_| mcx.oom(0))?;
            cxt.path_names.push(name);
        }
    }
    // Derived only after the name write above.
    let root_pathspec =
        root_pathspec_node.as_json_table_path_spec().expect("pathspec is JsonTablePathSpec");
    CheckDuplicateColumnOrPathNames(mcx, pstate, &mut cxt, &jt.columns)?;

    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;
    let result = (|| -> PgResult<Node<'mcx>> {
        let mut tf = Node::build::<TableFunc>(mcx)?;
        tf.functype = TableFuncType::TFT_JSON_TABLE;

        // Dummy JSON_TABLE_OP JsonFuncExpr carrying the top-level context
        // item and root path; transforms into the JsonExpr docexpr.
        let jfe = Node::mk(
            mcx,
            JsonFuncExpr {
                op: types_nodes::primnodes::JsonExprOp::JSON_TABLE_OP,
                column_name: None,
                context_item: jt.context_item,
                pathspec: root_pathspec.string,
                passing: jt.passing.clone_in(mcx)?,
                output: None,
                on_empty: None,
                on_error: jt.on_error,
                wrapper: JsonWrapper::JSW_UNSPEC,
                quotes: JsonQuotes::JS_QUOTES_UNSPEC,
                location: jt.location,
            },
        )?;
        tf.docexpr = Some(parse_expr::transformExpr(
            mcx,
            pstate,
            jfe,
            ParseExprKind::EXPR_KIND_FROM_FUNCTION,
        )?);

        let plan = transformJsonTableColumns(
            mcx,
            pstate,
            &mut cxt,
            jt,
            &mut *tf,
            &jt.columns,
            &jt.passing,
            root_pathspec_node,
        )?;
        tf.plan = Some(plan);

        // C copyObject's the transformed PASSING values into the TableFunc
        // (they are evaluated separately); the shallow list clone shares the
        // immutable expression nodes — setrefs' rtoffset-0 walker only
        // records dependencies, so aliasing is safe.
        let je = tf
            .docexpr
            .expect("set above")
            .as_json_expr()
            .expect("transformJsonFuncExpr yields JsonExpr");
        tf.passingvalexprs = je.passing_values.clone_in(mcx)?;

        tf.ordinalitycol = -1;
        tf.location = jt.location;
        Ok(tf.seal())
    })();
    pstate.p_lateral_active = false;
    let tf = result?;

    let is_lateral = jt.lateral || vars::contain_vars_of_level(tf, 0)?;

    parse_relation::addRangeTableEntryForTableFunc(mcx, pstate, tf, jt.alias, is_lateral, true)
        .map(|nsitem| &*nsitem)
}

fn CheckDuplicateColumnOrPathNames<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    cxt: &mut JsonTableParseContext<'mcx>,
    columns: &NodeList<'mcx>,
) -> PgResult<()> {
    for col in columns {
        let jtc = col.as_json_table_column().expect("JsonTableColumn");

        if jtc.coltype == JsonTableColumnType::JTC_NESTED {
            let pathspec = jtc
                .pathspec
                .expect("grammar sets NESTED pathspec")
                .as_json_table_path_spec()
                .expect("JsonTablePathSpec");
            if let Some(name) = pathspec.name {
                if LookupPathOrColumnName(cxt, name) {
                    return Err(duplicate_name_err(pstate, name, pathspec.name_location));
                }
                cxt.path_names.try_reserve(1).map_err(|_| mcx.oom(0))?;
                cxt.path_names.push(name);
            }
            CheckDuplicateColumnOrPathNames(mcx, pstate, cxt, &jtc.columns)?;
        } else {
            let name = jtc.name.expect("grammar sets column name");
            if LookupPathOrColumnName(cxt, name) {
                return Err(duplicate_name_err(pstate, name, jtc.location));
            }
            cxt.path_names.try_reserve(1).map_err(|_| mcx.oom(0))?;
            cxt.path_names.push(name);
        }
    }
    Ok(())
}

fn LookupPathOrColumnName(cxt: &JsonTableParseContext<'_>, name: &str) -> bool {
    cxt.path_names.iter().any(|n| *n == name)
}

fn generateJsonTablePathName<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &mut JsonTableParseContext<'mcx>,
) -> PgResult<&'mcx str> {
    let name = arena_str(mcx, &format!("json_table_path_{}", cxt.path_name_id))?;
    cxt.path_name_id += 1;
    cxt.path_names.try_reserve(1).map_err(|_| mcx.oom(0))?;
    cxt.path_names.push(name);
    Ok(name)
}

fn arena_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[allow(clippy::too_many_arguments)]
fn transformJsonTableColumns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cxt: &mut JsonTableParseContext<'mcx>,
    jt: &'mcx JsonTable<'mcx>,
    tf: &mut TableFunc<'mcx>,
    columns: &NodeList<'mcx>,
    passing_args: &NodeList<'mcx>,
    pathspec_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut ordinality_found = false;
    let error_on_error = jt.on_error.is_some_and(|oe| {
        oe.as_json_behavior().expect("JsonBehavior").btype
            == JsonBehaviorType::JSON_BEHAVIOR_ERROR
    });
    let context_item_typid = parse_expr::expr_type(tf.docexpr.expect("docexpr set"));

    let col_min = tf.colvalexprs.len() as i32;

    for col in columns {
        let jtc = col.as_json_table_column().expect("JsonTableColumn");

        if jtc.coltype != JsonTableColumnType::JTC_NESTED {
            let name = jtc.name.expect("grammar sets column name");
            tf.colnames.lappend(mcx, Node::mk_string(mcx, name)?)?;
        }

        let typid: Oid;
        let typmod: i32;
        let mut typcoll: Oid = 0;
        let colexpr: Option<Node<'mcx>>;

        match jtc.coltype {
            JsonTableColumnType::JTC_FOR_ORDINALITY => {
                if ordinality_found {
                    return Err(jsontable_err(
                        pstate,
                        types_error::ERRCODE_SYNTAX_ERROR,
                        "only one FOR ORDINALITY column is allowed".into(),
                        None,
                        jtc.location,
                    ));
                }
                ordinality_found = true;
                colexpr = None;
                typid = INT4OID;
                typmod = -1;
            }
            JsonTableColumnType::JTC_REGULAR
            | JsonTableColumnType::JTC_FORMATTED
            | JsonTableColumnType::JTC_EXISTS => {
                let mut eff_coltype = jtc.coltype;
                if jtc.coltype == JsonTableColumnType::JTC_REGULAR {
                    let tn = jtc
                        .typeName
                        .and_then(|n| n.as_type_name())
                        .expect("grammar sets column typeName");
                    let (id, _md) =
                        parse_utilcmd::typenameTypeIdAndMod(mcx, Some(&*pstate), tn)?;
                    // JTC_FORMATTED (JSON_QUERY) fits composite-ish types and
                    // explicit WRAPPER/QUOTES better than JSON_VALUE.
                    if isCompositeType(id)?
                        || jtc.quotes != JsonQuotes::JS_QUOTES_UNSPEC
                        || jtc.wrapper != JsonWrapper::JSW_UNSPEC
                    {
                        eff_coltype = JsonTableColumnType::JTC_FORMATTED;
                    }
                }

                let param = Node::mk(
                    mcx,
                    CaseTestExpr { typeId: context_item_typid, typeMod: -1, collation: 0 },
                )?;
                let jfe = transformJsonTableColumn(mcx, eff_coltype, jtc, param, passing_args)?;
                let ce = parse_expr::transformExpr(
                    mcx,
                    pstate,
                    jfe,
                    ParseExprKind::EXPR_KIND_FROM_FUNCTION,
                )?;
                parse_collate::assign_expr_collations(mcx, pstate, ce)?;

                typid = parse_expr::expr_type(ce);
                typmod = parse_expr::expr_typmod(ce);
                typcoll = parse_expr::expr_collation(ce);
                colexpr = Some(ce);
            }
            JsonTableColumnType::JTC_NESTED => continue,
        }

        tf.coltypes.lappend(mcx, typid)?;
        tf.coltypmods.lappend(mcx, typmod)?;
        tf.colcollations.lappend(mcx, typcoll)?;
        tf.colvalexprs.lappend(mcx, colexpr)?;
    }

    let (col_min, col_max) = if tf.colvalexprs.len() as i32 == col_min {
        // No columns in this scan beside the nested ones.
        (-1, -1)
    } else {
        (col_min, tf.colvalexprs.len() as i32 - 1)
    };

    let childplan =
        transformJsonTableNestedColumns(mcx, pstate, cxt, jt, tf, passing_args, columns)?;

    makeJsonTablePathScan(mcx, pathspec_node, error_on_error, col_min, col_max, childplan)
}

fn isCompositeType(typid: Oid) -> PgResult<bool> {
    let typtype = lsyscache::typ::get_typtype(typid)?;
    Ok(typid == JSONOID
        || typid == JSONBOID
        || typid == RECORDOID
        || lsyscache::typ::get_element_type(typid)? != 0
        || typtype == TYPTYPE_COMPOSITE
        || (typtype == TYPTYPE_DOMAIN && isCompositeType(lsyscache::typ::getBaseType(typid)?)?))
}

// Turns a regular column into JSON_VALUE(), a FORMAT JSON column into
// JSON_QUERY(), and an EXISTS column into JSON_EXISTS().
fn transformJsonTableColumn<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: JsonTableColumnType,
    jtc: &'mcx JsonTableColumn<'mcx>,
    context_item_expr: Node<'mcx>,
    passing_args: &NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::{JsonEncoding, JsonExprOp, JsonFormatType, JsonValueExpr};
    use types_nodes::rawnodes::JsonOutput;

    let op = match coltype {
        JsonTableColumnType::JTC_REGULAR => JsonExprOp::JSON_VALUE_OP,
        JsonTableColumnType::JTC_EXISTS => JsonExprOp::JSON_EXISTS_OP,
        _ => JsonExprOp::JSON_QUERY_OP,
    };

    let name = jtc.name.expect("grammar sets column name");

    let format = Node::mk_mut(
        mcx,
        JsonFormat {
            format_type: JsonFormatType::JS_FORMAT_DEFAULT,
            encoding: JsonEncoding::JS_ENC_DEFAULT,
            location: -1,
        },
    )?
    .seal_ref();
    let context_item = Node::mk(
        mcx,
        JsonValueExpr {
            raw_expr: Some(context_item_expr),
            formatted_expr: None,
            format: Some(format),
        },
    )?;

    let pathspec = match jtc.pathspec {
        Some(ps) => ps
            .as_json_table_path_spec()
            .expect("JsonTablePathSpec")
            .string
            .expect("pathspec carries a string"),
        None => {
            // Default path '$."column_name"'.
            let mut buf = stringinfo::StringInfo::new_in(mcx)?;
            buf.append_bytes(b"$.")?;
            adt_json::escape_json(&mut buf, name.as_bytes())?;
            let path = core::str::from_utf8(buf.into_vec().leak())
                .expect("escape_json output is UTF-8");
            Node::mk_a_const(
                mcx,
                Some(ValUnion::String(types_nodes::String { sval: path })),
                -1,
            )?
        }
    };

    let returning = Node::mk_mut(
        mcx,
        JsonReturning { format: jtc.format, typid: 0, typmod: 0 },
    )?
    .seal_ref();
    let output = Node::mk(
        mcx,
        JsonOutput { typeName: jtc.typeName, returning: Some(returning) },
    )?;

    Node::mk(
        mcx,
        JsonFuncExpr {
            op,
            // Column name lets runtime JsonExpr errors print it.
            column_name: Some(name),
            context_item: Some(context_item),
            pathspec: Some(pathspec),
            passing: passing_args.clone_in(mcx)?,
            output: Some(output),
            on_empty: jtc.on_empty,
            on_error: jtc.on_error,
            wrapper: jtc.wrapper,
            quotes: jtc.quotes,
            location: jtc.location,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn transformJsonTableNestedColumns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cxt: &mut JsonTableParseContext<'mcx>,
    jt: &'mcx JsonTable<'mcx>,
    tf: &mut TableFunc<'mcx>,
    passing_args: &NodeList<'mcx>,
    columns: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let mut plan: Option<Node<'mcx>> = None;

    // Multiple NESTED COLUMNS clauses combine via a sibling join, a UNION of
    // the row sets of each nested plan.
    for col in columns {
        let jtc = col.as_json_table_column().expect("JsonTableColumn");
        if jtc.coltype != JsonTableColumnType::JTC_NESTED {
            continue;
        }

        let pathspec_node = jtc.pathspec.expect("grammar sets NESTED pathspec");
        if pathspec_node
            .as_json_table_path_spec()
            .expect("JsonTablePathSpec")
            .name
            .is_none()
        {
            let name = generateJsonTablePathName(mcx, cxt)?;
            // SAFETY: parser-owned tree, no live derived refs.
            unsafe {
                pathspec_node
                    .with_mut::<JsonTablePathSpec, _>(|p| p.name = Some(name))
                    .expect("JsonTablePathSpec")
            };
        }

        let nested = transformJsonTableColumns(
            mcx,
            pstate,
            cxt,
            jt,
            tf,
            &jtc.columns,
            passing_args,
            pathspec_node,
        )?;

        plan = Some(match plan {
            Some(p) => Node::mk(
                mcx,
                JsonTableSiblingJoin { lplan: Some(p), rplan: Some(nested) },
            )?,
            None => nested,
        });
    }

    Ok(plan)
}

fn makeJsonTablePathScan<'mcx>(
    mcx: Mcx<'mcx>,
    pathspec_node: Node<'mcx>,
    error_on_error: bool,
    col_min: i32,
    col_max: i32,
    childplan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let pathspec =
        pathspec_node.as_json_table_path_spec().expect("JsonTablePathSpec");
    let pathstring = match pathspec.string.and_then(|s| s.as_a_const()).map(|c| c.val) {
        Some(Some(ValUnion::String(s))) => s.sval,
        _ => panic!("makeJsonTablePathScan: pathspec string is not an A_Const String"),
    };

    // jsonpath_in yields the flattened on-disk image (own varlena header);
    // stored verbatim as the by-ref Const value.
    let image = adt_jsonpath::path::jsonpath_in(mcx, pathstring.as_bytes(), None)?
        .expect("hard errsave without escontext returns Err");
    let d = datum::Datum::from_usize(image.leak().as_ptr() as usize);
    let value = Node::mk_const(mcx, JSONPATHOID, -1, 0, -1, d, false, false)?;

    let path = Node::mk(mcx, JsonTablePath { value: Some(value), name: pathspec.name })?;

    Node::mk(
        mcx,
        JsonTablePathScan {
            path: Some(path),
            errorOnError: error_on_error,
            child: childplan,
            colMin: col_min,
            colMax: col_max,
        },
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn jsontable_err(
    pstate: &ParseState<'_, '_>,
    code: types_error::SqlState,
    msg: std::string::String,
    detail: Option<std::string::String>,
    location: ParseLoc,
) -> Box<PgError> {
    let mut e = PgError::error(msg).with_sqlstate(code);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    if location >= 0 {
        e = e.with_cursor_position(parser_errposition(
            pstate,
            location,
            mbutils::GetDatabaseEncoding(),
        ));
    }
    Box::new(e)
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_name_err(
    pstate: &ParseState<'_, '_>,
    name: &str,
    location: ParseLoc,
) -> Box<PgError> {
    jsontable_err(
        pstate,
        types_error::ERRCODE_DUPLICATE_ALIAS,
        format!("duplicate JSON_TABLE column or path name: {name}"),
        None,
        location,
    )
}
