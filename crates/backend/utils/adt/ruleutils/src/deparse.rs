//! get_rule_expr slice: Const, Var, OpExpr, BoolExpr, RelabelType,
//! CoerceViaIO, FuncExpr, NullTest, Aggref, CaseExpr, CoalesceExpr,
//! MinMaxExpr, ScalarArrayOpExpr, ArrayExpr, SubLink, Param. Every other
//! node tag is a loud named panic.

use std::rc::Rc;

use datum::Datum;
use format_type::format_type_with_typemod;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, BOOLOID, INT4OID, NUMERICOID, UNKNOWNOID};
use types_error::PgResult;
use types_nodes::primnodes::{
    Aggref, ArrayExpr, BoolExpr, BoolExprType, CaseExpr, CoalesceExpr, CoercionForm, Const,
    FuncExpr, MinMaxExpr, MinMaxOp, NullTest, NullTestType, OpExpr, Param, ParamKind,
    ScalarArrayOpExpr, SubLink, SubLinkType, Var, VarReturningType,
};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum, PartitionRangeDatumKind};
use types_nodes::{BoolTestType, Node, NodeList, NodeTag, RTEKind, RangeTblEntry};

use crate::query::{self, DeparseNamespace};
use crate::{gap, generate_function_name, generate_operator_name, quote_identifier};

pub(crate) const PRETTYINDENT_STD: i32 = 8;
pub(crate) const PRETTYINDENT_JOIN: i32 = 4;
pub(crate) const PRETTYINDENT_VAR: i32 = 4;
pub(crate) const PRETTYINDENT_LIMIT: i32 = 40;

pub(crate) struct DeparseContext<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub buf: String,
    pub namespaces: Vec<Rc<DeparseNamespace<'mcx>>>,
    pub result_desc: Option<Rc<Vec<String>>>,
    pub target_list: Option<&'mcx NodeList<'mcx>>,
    pub window_clause: Option<&'mcx NodeList<'mcx>>,
    pub varprefix: bool,
    pub pretty_flags: i32,
    pub wrap_column: i32,
    pub indent_level: i32,
    pub colnames_visible: bool,
    pub in_group_by: bool,
    pub var_in_order_by: bool,
    pub appendparents: types_nodes::bitmapset::Bitmapset<'mcx>,
}

impl<'mcx> DeparseContext<'mcx> {
    pub(crate) fn new(mcx: Mcx<'mcx>, pretty_flags: i32) -> Self {
        DeparseContext {
            mcx,
            buf: String::new(),
            namespaces: Vec::new(),
            result_desc: None,
            target_list: None,
            window_clause: None,
            varprefix: false,
            pretty_flags,
            wrap_column: -1,
            indent_level: 0,
            colnames_visible: true,
            in_group_by: false,
            var_in_order_by: false,
            appendparents: types_nodes::bitmapset::Bitmapset::empty(),
        }
    }

    pub(crate) fn pretty_paren(&self) -> bool {
        self.pretty_flags & crate::PRETTYFLAG_PAREN != 0
    }

    pub(crate) fn pretty_indent(&self) -> bool {
        self.pretty_flags & crate::PRETTYFLAG_INDENT != 0
    }
}

pub(crate) fn append_context_keyword(
    ctx: &mut DeparseContext<'_>,
    s: &str,
    indent_before: i32,
    indent_after: i32,
    indent_plus: i32,
) {
    if ctx.pretty_indent() {
        ctx.indent_level += indent_before;
        remove_trailing_spaces(&mut ctx.buf);
        ctx.buf.push('\n');
        let amount = if ctx.indent_level < PRETTYINDENT_LIMIT {
            ctx.indent_level.max(0) + indent_plus
        } else {
            let mut a =
                PRETTYINDENT_LIMIT + (ctx.indent_level - PRETTYINDENT_LIMIT) / (PRETTYINDENT_STD / 2);
            a %= PRETTYINDENT_LIMIT;
            a + indent_plus
        };
        for _ in 0..amount {
            ctx.buf.push(' ');
        }
        ctx.buf.push_str(s);
        ctx.indent_level += indent_after;
        if ctx.indent_level < 0 {
            ctx.indent_level = 0;
        }
    } else {
        ctx.buf.push_str(s);
    }
}

pub(crate) fn remove_trailing_spaces(buf: &mut String) {
    let trimmed = buf.trim_end_matches(' ').len();
    buf.truncate(trimmed);
}

pub fn deparse_expression_pretty<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Node<'mcx>,
    relid: Oid,
    showimplicit: bool,
    pretty_flags: i32,
) -> PgResult<String> {
    let mut ctx = DeparseContext::new(mcx, pretty_flags);
    if relid != InvalidOid {
        let relname = lsyscache::get_rel_name(mcx, relid)?
            .expect("deparse_context_for: relation exists")
            .as_str()
            .to_owned();
        ctx.namespaces.push(Rc::new(query::deparse_context_for(mcx, &relname, relid)?));
    }
    get_rule_expr(expr, &mut ctx, showimplicit)?;
    Ok(ctx.buf)
}

pub(crate) fn get_rule_expr<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => get_variable(node, node.as_var().unwrap(), 0, false, ctx).map(|_| ()),
        NodeTag::T_Const => get_const_expr(node.as_const().unwrap(), ctx, 0),
        NodeTag::T_Param => get_parameter(node.as_param().unwrap(), ctx),
        NodeTag::T_Aggref => get_agg_expr(node.as_aggref().unwrap(), ctx),
        NodeTag::T_WindowFunc => get_windowfunc_expr(node.as_window_func().unwrap(), ctx),
        NodeTag::T_MergeSupportFunc => {
            ctx.buf.push_str("MERGE_ACTION()");
            Ok(())
        }
        NodeTag::T_CollateExpr => {
            let collate = node.as_collate_expr().unwrap();
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            get_rule_expr_paren(collate.arg, ctx, showimplicit, Some(node))?;
            let collname = crate::generate_collation_name(ctx.mcx, collate.collOid)?;
            ctx.buf.push_str(&format!(" COLLATE {collname}"));
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_OpExpr => get_oper_expr(node, node.as_op_expr().unwrap(), ctx),
        NodeTag::T_FuncExpr => get_func_expr(node, node.as_func_expr().unwrap(), ctx, showimplicit),
        NodeTag::T_NamedArgExpr => {
            let na = node.as_named_arg_expr().unwrap();
            ctx.buf.push_str(&format!(
                "{} => ",
                quote_identifier(na.name.expect("NamedArgExpr has a name"))
            ));
            get_rule_expr(na.arg.expect("NamedArgExpr has an arg"), ctx, showimplicit)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            get_saop_expr(node, node.as_scalar_array_op_expr().unwrap(), ctx)
        }
        NodeTag::T_BoolExpr => get_bool_expr(node, node.as_bool_expr().unwrap(), ctx),
        NodeTag::T_SubLink => get_sublink_expr(node.as_sub_link().unwrap(), ctx),
        NodeTag::T_RelabelType => {
            let relabel = node.as_relabel_type().unwrap();
            if relabel.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
                get_rule_expr_paren(relabel.arg, ctx, false, Some(node))
            } else {
                get_coercion_expr(relabel.arg, ctx, relabel.resulttype, relabel.resulttypmod, node)
            }
        }
        NodeTag::T_CoerceViaIO => {
            let ioc = node.as_coerce_via_io().unwrap();
            if ioc.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
                get_rule_expr_paren(ioc.arg, ctx, false, Some(node))
            } else {
                get_coercion_expr(ioc.arg, ctx, ioc.resulttype, -1, node)
            }
        }
        NodeTag::T_ArrayCoerceExpr => {
            let acoerce = node.as_array_coerce_expr().unwrap();
            if acoerce.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
                get_rule_expr_paren(acoerce.arg, ctx, false, Some(node))
            } else {
                get_coercion_expr(acoerce.arg, ctx, acoerce.resulttype, acoerce.resulttypmod, node)
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let convert = node.as_convert_rowtype_expr().unwrap();
            if convert.convertformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
                get_rule_expr_paren(convert.arg, ctx, false, Some(node))
            } else {
                get_coercion_expr(convert.arg, ctx, convert.resulttype, -1, node)
            }
        }
        NodeTag::T_CoerceToDomain => {
            let ctest = node.as_coerce_to_domain().unwrap();
            if ctest.coercionformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
                get_rule_expr(ctest.arg, ctx, false)
            } else {
                get_coercion_expr(ctest.arg, ctx, ctest.resulttype, ctest.resulttypmod, node)
            }
        }
        NodeTag::T_CoerceToDomainValue => {
            ctx.buf.push_str("VALUE");
            Ok(())
        }
        NodeTag::T_SetToDefault => {
            ctx.buf.push_str("DEFAULT");
            Ok(())
        }
        NodeTag::T_CurrentOfExpr => {
            let cexpr = node.as_current_of_expr().unwrap();
            match cexpr.cursor_name {
                Some(name) => ctx
                    .buf
                    .push_str(&format!("CURRENT OF {}", quote_identifier(name))),
                None => ctx.buf.push_str(&format!("CURRENT OF ${}", cexpr.cursor_param)),
            }
            Ok(())
        }
        NodeTag::T_CaseExpr => get_case_expr(node.as_case_expr().unwrap(), ctx),
        NodeTag::T_CaseTestExpr => {
            ctx.buf.push_str("CASE_TEST_EXPR");
            Ok(())
        }
        NodeTag::T_ArrayExpr => get_array_expr(node.as_array_expr().unwrap(), ctx),
        NodeTag::T_CoalesceExpr => get_coalesce_expr(node.as_coalesce_expr().unwrap(), ctx),
        NodeTag::T_MinMaxExpr => get_minmax_expr(node.as_min_max_expr().unwrap(), ctx),
        NodeTag::T_NullTest => get_null_test(node, node.as_null_test().unwrap(), ctx),
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            ctx.buf.push_str("GROUPING(");
            let mut first = true;
            for arg in g.args.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                get_rule_expr(arg, ctx, true)?;
            }
            ctx.buf.push(')');
            Ok(())
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            get_rule_expr_paren(d.args.nth(0), ctx, true, Some(node))?;
            ctx.buf.push_str(" IS DISTINCT FROM ");
            get_rule_expr_paren(d.args.nth(1), ctx, true, Some(node))?;
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_NullIfExpr => {
            let n = node.as_null_if_expr().unwrap();
            ctx.buf.push_str("NULLIF(");
            let mut first = true;
            for a in n.args.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                get_rule_expr(a, ctx, true)?;
            }
            ctx.buf.push(')');
            Ok(())
        }
        NodeTag::T_InferenceElem => {
            let ie = node.as_inference_elem().unwrap();
            let expr = ie.expr.expect("InferenceElem.expr");
            let save_varprefix = ctx.varprefix;
            ctx.varprefix = false;
            let mut need_parens = expr.node_tag() != NodeTag::T_Var;
            if let Some(f) = expr.as_func_expr() {
                if f.funcformat == CoercionForm::COERCE_EXPLICIT_CALL {
                    need_parens = false;
                }
            }
            if need_parens {
                ctx.buf.push('(');
            }
            get_rule_expr(expr, ctx, false)?;
            if need_parens {
                ctx.buf.push(')');
            }
            ctx.varprefix = save_varprefix;
            if ie.infercollid != InvalidOid {
                let collname = crate::generate_collation_name(ctx.mcx, ie.infercollid)?;
                ctx.buf.push_str(&format!(" COLLATE {collname}"));
            }
            if ie.inferopclass != InvalidOid {
                let input_type = lsyscache::get_opclass_input_type(ie.inferopclass)?;
                let mut opc = String::new();
                crate::get_opclass_name(ctx.mcx, ie.inferopclass, input_type, &mut opc)?;
                ctx.buf.push_str(&opc);
            }
            Ok(())
        }
        NodeTag::T_BooleanTest => {
            let btest = node.as_boolean_test().unwrap();
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            get_rule_expr_paren(btest.arg.expect("BooleanTest.arg"), ctx, false, Some(node))?;
            ctx.buf.push_str(match btest.booltesttype {
                BoolTestType::IS_TRUE => " IS TRUE",
                BoolTestType::IS_NOT_TRUE => " IS NOT TRUE",
                BoolTestType::IS_FALSE => " IS FALSE",
                BoolTestType::IS_NOT_FALSE => " IS NOT FALSE",
                BoolTestType::IS_UNKNOWN => " IS UNKNOWN",
                BoolTestType::IS_NOT_UNKNOWN => " IS NOT UNKNOWN",
            });
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_SQLValueFunction => {
            use types_nodes::primnodes::SQLValueFunctionOp as Op;
            let svf = node.as_sql_value_function().unwrap();
            match svf.op {
                Op::SVFOP_CURRENT_DATE => ctx.buf.push_str("CURRENT_DATE"),
                Op::SVFOP_CURRENT_TIME => ctx.buf.push_str("CURRENT_TIME"),
                Op::SVFOP_CURRENT_TIME_N => {
                    ctx.buf.push_str(&format!("CURRENT_TIME({})", svf.typmod));
                }
                Op::SVFOP_CURRENT_TIMESTAMP => ctx.buf.push_str("CURRENT_TIMESTAMP"),
                Op::SVFOP_CURRENT_TIMESTAMP_N => {
                    ctx.buf.push_str(&format!("CURRENT_TIMESTAMP({})", svf.typmod));
                }
                Op::SVFOP_LOCALTIME => ctx.buf.push_str("LOCALTIME"),
                Op::SVFOP_LOCALTIME_N => ctx.buf.push_str(&format!("LOCALTIME({})", svf.typmod)),
                Op::SVFOP_LOCALTIMESTAMP => ctx.buf.push_str("LOCALTIMESTAMP"),
                Op::SVFOP_LOCALTIMESTAMP_N => {
                    ctx.buf.push_str(&format!("LOCALTIMESTAMP({})", svf.typmod));
                }
                Op::SVFOP_CURRENT_ROLE => ctx.buf.push_str("CURRENT_ROLE"),
                Op::SVFOP_CURRENT_USER => ctx.buf.push_str("CURRENT_USER"),
                Op::SVFOP_USER => ctx.buf.push_str("USER"),
                Op::SVFOP_SESSION_USER => ctx.buf.push_str("SESSION_USER"),
                Op::SVFOP_CURRENT_CATALOG => ctx.buf.push_str("CURRENT_CATALOG"),
                Op::SVFOP_CURRENT_SCHEMA => ctx.buf.push_str("CURRENT_SCHEMA"),
            }
            Ok(())
        }
        NodeTag::T_SubscriptingRef => {
            let sbsref = node.as_subscripting_ref().unwrap();
            let refexpr = sbsref.refexpr.expect("SubscriptingRef.refexpr");
            if refexpr.node_tag() == NodeTag::T_CaseTestExpr {
                gap("get_rule_expr", "SubscriptingRef inside FieldStore");
            }
            let need_parens = !matches!(refexpr.node_tag(), NodeTag::T_Var | NodeTag::T_FieldSelect);
            if need_parens {
                ctx.buf.push('(');
            }
            get_rule_expr(refexpr, ctx, showimplicit)?;
            if need_parens {
                ctx.buf.push(')');
            }
            if sbsref.refassgnexpr.is_some() {
                // Not legal SQL (container[subscripts] := rhs); C reaches it
                // only when EXPLAIN prints an UPDATE plan targetlist.
                let refassgnexpr = query::process_indirection(node, ctx)?;
                ctx.buf.push_str(" := ");
                get_rule_expr(refassgnexpr, ctx, showimplicit)?;
            } else {
                print_subscripts(sbsref, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_FieldSelect => {
            let fselect = node.as_field_select().unwrap();
            let arg = fselect.arg;
            // C: a Var argument MUST be parenthesized — name-count correctness,
            // not simplicity.
            let need_parens =
                !matches!(arg.node_tag(), NodeTag::T_SubscriptingRef | NodeTag::T_FieldSelect);
            if need_parens {
                ctx.buf.push('(');
            }
            get_rule_expr(arg, ctx, true)?;
            if need_parens {
                ctx.buf.push(')');
            }
            let fieldname = get_name_for_var_field(arg, fselect.fieldnum as i32, 0, ctx)?;
            ctx.buf.push('.');
            ctx.buf.push_str(&quote_identifier(&fieldname));
            Ok(())
        }
        NodeTag::T_FieldStore => {
            // No SQL spelling exists; EXPLAIN-only, per C: print the source
            // args, ROW()-wrapped when the planner collapsed several stores.
            let fstore = node.as_field_store().unwrap();
            let need_parens = fstore.newvals.len() != 1;
            if need_parens {
                ctx.buf.push_str("ROW(");
            }
            let mut first = true;
            for v in fstore.newvals.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                get_rule_expr(v, ctx, showimplicit)?;
            }
            if need_parens {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_XmlExpr => get_xml_expr(node, ctx),
        NodeTag::T_TableFunc => {
            get_tablefunc(node.as_table_func().unwrap(), ctx, showimplicit)
        }
        NodeTag::T_SubPlan => {
            let subplan = node.as_sub_plan().unwrap();
            ctx.buf.push_str(match subplan.subLinkType {
                SubLinkType::EXISTS_SUBLINK => "EXISTS(",
                SubLinkType::ALL_SUBLINK => "(ALL ",
                SubLinkType::ANY_SUBLINK => "(ANY ",
                SubLinkType::ROWCOMPARE_SUBLINK | SubLinkType::EXPR_SUBLINK => "(",
                SubLinkType::MULTIEXPR_SUBLINK => "(rescan ",
                SubLinkType::ARRAY_SUBLINK => "ARRAY(",
                SubLinkType::CTE_SUBLINK => "CTE(",
            });
            if let Some(testexpr) = subplan.testexpr {
                let dpns = Rc::clone(&ctx.namespaces[0]);
                dpns.plan
                    .borrow_mut()
                    .ancestors
                    .insert(0, crate::plan::AncestorEntry::Sub(subplan));
                let r = get_rule_expr(testexpr, ctx, showimplicit);
                dpns.plan.borrow_mut().ancestors.remove(0);
                r?;
                ctx.buf.push(')');
            } else {
                if subplan.useHashTable {
                    ctx.buf.push_str("hashed ");
                }
                ctx.buf.push_str(subplan.plan_name.expect("planned SubPlan has a name"));
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_AlternativeSubPlan => {
            let asplan = node.as_alternative_sub_plan().unwrap();
            ctx.buf.push_str("(alternatives: ");
            let mut first = true;
            for sp_node in asplan.subplans.iter() {
                let splan = sp_node.as_sub_plan().expect("AlternativeSubPlan cell");
                if !first {
                    ctx.buf.push_str(" or ");
                }
                first = false;
                if splan.useHashTable {
                    ctx.buf.push_str("hashed ");
                }
                ctx.buf.push_str(splan.plan_name.expect("planned SubPlan has a name"));
            }
            ctx.buf.push(')');
            Ok(())
        }
        NodeTag::T_List => {
            let mut first = true;
            for item in node.as_list().unwrap().iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                get_rule_expr(item, ctx, showimplicit)?;
            }
            Ok(())
        }
        NodeTag::T_JsonValueExpr => {
            let jve = node.as_json_value_expr().unwrap();
            get_rule_expr(jve.raw_expr.expect("raw_expr"), ctx, false)?;
            get_json_format(jve.format, ctx);
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            get_json_constructor(node.as_json_constructor_expr().unwrap(), ctx, false)
        }
        NodeTag::T_JsonIsPredicate => {
            let pred = node.as_json_is_predicate().unwrap();
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            get_rule_expr_paren(pred.expr.expect("expr"), ctx, true, Some(node))?;
            ctx.buf.push_str(" IS JSON");
            match pred.item_type {
                types_nodes::JsonValueType::JS_TYPE_SCALAR => ctx.buf.push_str(" SCALAR"),
                types_nodes::JsonValueType::JS_TYPE_ARRAY => ctx.buf.push_str(" ARRAY"),
                types_nodes::JsonValueType::JS_TYPE_OBJECT => ctx.buf.push_str(" OBJECT"),
                types_nodes::JsonValueType::JS_TYPE_ANY => {}
            }
            if pred.unique_keys {
                ctx.buf.push_str(" WITH UNIQUE KEYS");
            }
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_JsonExpr => get_json_expr(node.as_json_expr().unwrap(), ctx, showimplicit),
        // C get_rule_list_toplevel: whole-row Vars inside ROW() print as x.*.
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            ctx.buf.push_str("(ROW(");
            get_rule_list_toplevel(&rc.largs, ctx, showimplicit)?;
            let opname = generate_operator_name(
                ctx.mcx,
                rc.opnos.nth(0),
                parse_expr::expr_type(rc.largs.nth(0)),
                parse_expr::expr_type(rc.rargs.nth(0)),
            )?;
            ctx.buf.push_str(&format!(") {opname} ROW("));
            get_rule_list_toplevel(&rc.rargs, ctx, showimplicit)?;
            ctx.buf.push_str("))");
            Ok(())
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let tupdesc = if r.row_typeid != types_core::catalog::RECORDOID {
                Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(
                    ctx.mcx,
                    r.row_typeid,
                    -1,
                )?)
            } else {
                None
            };
            ctx.buf.push_str("ROW(");
            let mut first = true;
            for (i, e) in r.args.iter().enumerate() {
                if tupdesc.as_ref().is_none_or(|d| !d.attr(i).attisdropped) {
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    get_rule_expr_toplevel(e, ctx, true)?;
                }
            }
            ctx.buf.push(')');
            if r.row_format == CoercionForm::COERCE_EXPLICIT_CAST {
                let tyname = format_type_with_typemod(r.row_typeid, -1)?;
                ctx.buf.push_str(&format!("::{tyname}"));
            }
            Ok(())
        }
        NodeTag::T_PartitionBoundSpec => {
            let spec = node.as_variant::<PartitionBoundSpec>().unwrap();
            if spec.is_default {
                ctx.buf.push_str("DEFAULT");
                return Ok(());
            }
            match spec.strategy {
                b'h' => {
                    assert!(spec.modulus > 0 && spec.remainder >= 0);
                    assert!(spec.modulus > spec.remainder);
                    ctx.buf.push_str(&format!(
                        "FOR VALUES WITH (modulus {}, remainder {})",
                        spec.modulus, spec.remainder
                    ));
                }
                b'l' => {
                    ctx.buf.push_str("FOR VALUES IN (");
                    let mut sep = "";
                    for cell in spec.listdatums.iter() {
                        let val = cell.as_const().expect("list partition datum is a Const");
                        ctx.buf.push_str(sep);
                        get_const_expr(val, ctx, -1)?;
                        sep = ", ";
                    }
                    ctx.buf.push(')');
                }
                b'r' => {
                    assert!(
                        spec.lowerdatums.len() > 0
                            && spec.lowerdatums.len() == spec.upperdatums.len()
                    );
                    ctx.buf.push_str("FOR VALUES FROM ");
                    get_range_partbound_string(&spec.lowerdatums, ctx)?;
                    ctx.buf.push_str(" TO ");
                    get_range_partbound_string(&spec.upperdatums, ctx)?;
                }
                other => panic!("unrecognized partition strategy: {other}"),
            }
            Ok(())
        }
        // Only seen while EXPLAINing a plan (rewriter-added, never in a rule);
        // display just the wrapped expression (an expanded view column).
        NodeTag::T_ReturningExpr => {
            get_rule_expr(node.as_returning_expr().unwrap().retexpr, ctx, showimplicit)
        }
        NodeTag::T_SetToDefault => {
            ctx.buf.push_str("DEFAULT");
            Ok(())
        }
        other => gap("get_rule_expr", &format!("{other:?} deparse arm")),
    }
}

// The datum leg of C get_range_partbound_string: get_const_expr with
// showtype -1 (no ::type suffix); seam entry for partbounds' errdetail.
pub fn deparse_partbound_const<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> PgResult<String> {
    let c = expr.as_const().expect("range partition datum value is a Const");
    let mut ctx = DeparseContext::new(mcx, 0);
    get_const_expr(c, &mut ctx, -1)?;
    Ok(ctx.buf)
}

// C get_range_partbound_string (ruleutils.c); appends into ctx.buf instead of
// returning a fresh string.
pub(crate) fn get_range_partbound_string<'mcx>(
    bound_datums: &NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ctx.buf.push('(');
    let mut sep = "";
    for cell in bound_datums.iter() {
        let datum = cell
            .as_variant::<PartitionRangeDatum>()
            .expect("range bound datum is a PartitionRangeDatum");
        ctx.buf.push_str(sep);
        match datum.kind {
            PartitionRangeDatumKind::Minvalue => ctx.buf.push_str("MINVALUE"),
            PartitionRangeDatumKind::Maxvalue => ctx.buf.push_str("MAXVALUE"),
            PartitionRangeDatumKind::Value => {
                let val = datum
                    .value
                    .and_then(|v| v.as_const())
                    .expect("range partition datum value is a Const");
                get_const_expr(val, ctx, -1)?;
            }
        }
        sep = ", ";
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_json_format(format: Option<&types_nodes::JsonFormat>, ctx: &mut DeparseContext<'_>) {
    use types_nodes::primnodes::{JsonEncoding, JsonFormatType};
    let Some(format) = format else { return };
    if format.format_type == JsonFormatType::JS_FORMAT_DEFAULT {
        return;
    }
    ctx.buf.push_str(if format.format_type == JsonFormatType::JS_FORMAT_JSONB {
        " FORMAT JSONB"
    } else {
        " FORMAT JSON"
    });
    if format.encoding != JsonEncoding::JS_ENC_DEFAULT {
        ctx.buf.push_str(" ENCODING ");
        ctx.buf.push_str(match format.encoding {
            JsonEncoding::JS_ENC_UTF16 => "UTF16",
            JsonEncoding::JS_ENC_UTF32 => "UTF32",
            _ => "UTF8",
        });
    }
}

fn get_json_returning(
    returning: &types_nodes::JsonReturning<'_>,
    ctx: &mut DeparseContext<'_>,
    json_format_by_default: bool,
) -> PgResult<()> {
    use types_nodes::primnodes::JsonFormatType;
    if returning.typid == 0 {
        return Ok(());
    }
    ctx.buf.push_str(" RETURNING ");
    ctx.buf.push_str(&format_type_with_typemod(returning.typid, returning.typmod)?);
    let expected = if returning.typid == types_core::catalog::JSONBOID {
        JsonFormatType::JS_FORMAT_JSONB
    } else {
        JsonFormatType::JS_FORMAT_JSON
    };
    if !json_format_by_default
        || returning.format.expect("format").format_type != expected
    {
        get_json_format(returning.format, ctx);
    }
    Ok(())
}

fn get_json_constructor_options(
    ctor: &types_nodes::JsonConstructorExpr<'_>,
    ctx: &mut DeparseContext<'_>,
) -> PgResult<()> {
    use types_nodes::JsonConstructorType as JC;
    if ctor.absent_on_null {
        if matches!(ctor.r#type, JC::JSCTOR_JSON_OBJECT | JC::JSCTOR_JSON_OBJECTAGG) {
            ctx.buf.push_str(" ABSENT ON NULL");
        }
    } else if matches!(ctor.r#type, JC::JSCTOR_JSON_ARRAY | JC::JSCTOR_JSON_ARRAYAGG) {
        ctx.buf.push_str(" NULL ON NULL");
    }
    if ctor.unique {
        ctx.buf.push_str(" WITH UNIQUE KEYS");
    }
    if !matches!(ctor.r#type, JC::JSCTOR_JSON_PARSE | JC::JSCTOR_JSON_SCALAR) {
        get_json_returning(ctor.returning.expect("returning"), ctx, true)?;
    }
    Ok(())
}

fn get_json_constructor<'mcx>(
    ctor: &'mcx types_nodes::JsonConstructorExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    _showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::JsonConstructorType as JC;
    match ctor.r#type {
        JC::JSCTOR_JSON_OBJECTAGG => {
            return get_json_agg_constructor(ctor, ctx, "JSON_OBJECTAGG", true)
        }
        JC::JSCTOR_JSON_ARRAYAGG => {
            return get_json_agg_constructor(ctor, ctx, "JSON_ARRAYAGG", false)
        }
        _ => {}
    }
    let funcname = match ctor.r#type {
        JC::JSCTOR_JSON_OBJECT => "JSON_OBJECT",
        JC::JSCTOR_JSON_ARRAY => "JSON_ARRAY",
        JC::JSCTOR_JSON_PARSE => "JSON",
        JC::JSCTOR_JSON_SCALAR => "JSON_SCALAR",
        JC::JSCTOR_JSON_SERIALIZE => "JSON_SERIALIZE",
        other => panic!("invalid JsonConstructorType {other:?}"),
    };
    ctx.buf.push_str(funcname);
    ctx.buf.push('(');
    let is_json_object = ctor.r#type == JC::JSCTOR_JSON_OBJECT;
    for (curridx, arg) in ctor.args.iter().enumerate() {
        if curridx > 0 {
            ctx.buf.push_str(if is_json_object && curridx % 2 != 0 { " : " } else { ", " });
        }
        get_rule_expr(arg, ctx, true)?;
    }
    get_json_constructor_options(ctor, ctx)?;
    ctx.buf.push(')');
    Ok(())
}

fn get_json_agg_constructor<'mcx>(
    ctor: &'mcx types_nodes::JsonConstructorExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    funcname: &str,
    is_json_objectagg: bool,
) -> PgResult<()> {
    let func = ctor.func.expect("func");
    let Some(aggref) = func.as_aggref() else {
        let Some(wfunc) = func.as_window_func() else {
            gap("get_json_agg_constructor", "non-Aggref/WindowFunc constructor func");
        };
        return get_windowfunc_expr_helper(wfunc, ctx, Some((ctor, funcname, is_json_objectagg)));
    };
    // get_agg_expr_helper: funcname override + options suffix.
    if aggref.aggsplit != types_nodes::primnodes::AGGSPLIT_SIMPLE {
        gap("get_json_agg_constructor", "partial/combining aggregate deparse");
    }
    ctx.buf.push_str(funcname);
    ctx.buf.push('(');
    let mut i = 0;
    for tle_node in aggref.args.iter() {
        let tle = tle_node.as_target_entry().expect("Aggref args are TargetEntries");
        if tle.resjunk {
            continue;
        }
        if i > 0 {
            ctx.buf.push_str(if is_json_objectagg { " : " } else { ", " });
        }
        i += 1;
        get_rule_expr(tle.expr, ctx, true)?;
    }
    if !aggref.aggorder.is_nil() {
        ctx.buf.push_str(" ORDER BY ");
        query::get_rule_orderby(&aggref.aggorder, &aggref.args, false, ctx)?;
    }
    get_json_constructor_options(ctor, ctx)?;
    if let Some(filter) = aggref.aggfilter {
        ctx.buf.push_str(") FILTER (WHERE ");
        get_rule_expr(filter, ctx, false)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_json_behavior<'mcx>(
    behavior: &types_nodes::JsonBehavior<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    on: &str,
) -> PgResult<()> {
    use types_nodes::JsonBehaviorType::*;
    ctx.buf.push_str(match behavior.btype {
        JSON_BEHAVIOR_NULL => " NULL",
        JSON_BEHAVIOR_ERROR => " ERROR",
        JSON_BEHAVIOR_EMPTY => " EMPTY",
        JSON_BEHAVIOR_TRUE => " TRUE",
        JSON_BEHAVIOR_FALSE => " FALSE",
        JSON_BEHAVIOR_UNKNOWN => " UNKNOWN",
        JSON_BEHAVIOR_EMPTY_ARRAY => " EMPTY ARRAY",
        JSON_BEHAVIOR_EMPTY_OBJECT => " EMPTY OBJECT",
        JSON_BEHAVIOR_DEFAULT => " DEFAULT ",
    });
    if behavior.btype == JSON_BEHAVIOR_DEFAULT {
        get_rule_expr(behavior.expr.expect("DEFAULT expr"), ctx, false)?;
    }
    ctx.buf.push_str(" ON ");
    ctx.buf.push_str(on);
    Ok(())
}

fn get_json_expr_options<'mcx>(
    jexpr: &types_nodes::JsonExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    default_behavior: types_nodes::JsonBehaviorType,
) -> PgResult<()> {
    use types_nodes::JsonExprOp;
    use types_nodes::JsonWrapper::*;
    if jexpr.op == JsonExprOp::JSON_QUERY_OP {
        match jexpr.wrapper {
            JSW_CONDITIONAL => ctx.buf.push_str(" WITH CONDITIONAL WRAPPER"),
            JSW_UNCONDITIONAL => ctx.buf.push_str(" WITH UNCONDITIONAL WRAPPER"),
            JSW_NONE | JSW_UNSPEC => ctx.buf.push_str(" WITHOUT WRAPPER"),
        }
        ctx.buf.push_str(if jexpr.omit_quotes { " OMIT QUOTES" } else { " KEEP QUOTES" });
    }
    if let Some(oe) = jexpr.on_empty {
        let b = oe.as_json_behavior().expect("JsonBehavior");
        if b.btype != default_behavior {
            get_json_behavior(b, ctx, "EMPTY")?;
        }
    }
    if let Some(oe) = jexpr.on_error {
        let b = oe.as_json_behavior().expect("JsonBehavior");
        if b.btype != default_behavior {
            get_json_behavior(b, ctx, "ERROR")?;
        }
    }
    Ok(())
}

fn get_json_expr<'mcx>(
    jexpr: &'mcx types_nodes::JsonExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::JsonBehaviorType;
    use types_nodes::JsonExprOp::*;
    ctx.buf.push_str(match jexpr.op {
        JSON_EXISTS_OP => "JSON_EXISTS(",
        JSON_QUERY_OP => "JSON_QUERY(",
        JSON_VALUE_OP => "JSON_VALUE(",
        JSON_TABLE_OP => panic!("unrecognized JsonExpr op"),
    });
    get_rule_expr(jexpr.formatted_expr.expect("formatted_expr"), ctx, showimplicit)?;
    ctx.buf.push_str(", ");
    let path_spec = jexpr.path_spec.expect("path_spec");
    if let Some(c) = path_spec.as_const() {
        get_const_expr(c, ctx, -1)?;
    } else {
        get_rule_expr(path_spec, ctx, showimplicit)?;
    }
    if !jexpr.passing_values.is_nil() {
        ctx.buf.push_str(" PASSING ");
        let mut needcomma = false;
        for (name, value) in jexpr.passing_names.iter().zip(jexpr.passing_values.iter()) {
            if needcomma {
                ctx.buf.push_str(", ");
            }
            needcomma = true;
            get_rule_expr(value, ctx, showimplicit)?;
            ctx.buf.push_str(" AS ");
            ctx.buf.push_str(&quote_identifier(
                name.as_string().expect("passing name").sval,
            ));
        }
    }
    let returning = jexpr.returning.expect("returning");
    if jexpr.op != JSON_EXISTS_OP || returning.typid != types_core::catalog::BOOLOID {
        get_json_returning(returning, ctx, jexpr.op == JSON_QUERY_OP)?;
    }
    get_json_expr_options(
        jexpr,
        ctx,
        if jexpr.op != JSON_EXISTS_OP {
            JsonBehaviorType::JSON_BEHAVIOR_NULL
        } else {
            JsonBehaviorType::JSON_BEHAVIOR_FALSE
        },
    )?;
    ctx.buf.push(')');
    Ok(())
}

// get_rule_expr_toplevel / get_rule_list_toplevel (ruleutils.c): a whole-row
// Var at list top level prints its .* form via get_variable(istoplevel).
fn get_rule_expr_toplevel<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    if let Some(v) = node.as_var() {
        if v.varattno == types_core::InvalidAttrNumber {
            return get_variable(node, v, 0, true, ctx).map(|_| ());
        }
    }
    get_rule_expr(node, ctx, showimplicit)
}

fn get_rule_list_toplevel<'mcx>(
    lst: &NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    let mut first = true;
    for item in lst.iter() {
        if !first {
            ctx.buf.push_str(", ");
        }
        first = false;
        get_rule_expr_toplevel(item, ctx, showimplicit)?;
    }
    Ok(())
}

pub(crate) fn get_rule_expr_paren<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
    parent: Option<Node<'mcx>>,
) -> PgResult<()> {
    let need_paren = ctx.pretty_paren() && !is_simple_node(node, parent, ctx.pretty_flags);
    if need_paren {
        ctx.buf.push('(');
    }
    get_rule_expr(node, ctx, showimplicit)?;
    if need_paren {
        ctx.buf.push(')');
    }
    Ok(())
}

fn get_simple_binary_op_name<'a>(mcx: Mcx<'a>, expr: &OpExpr<'_>) -> Option<mcx::PgString<'a>> {
    if expr.args.len() != 2 {
        return None;
    }
    lsyscache::get_opname(mcx, expr.opno).ok().flatten()
}

fn is_simple_node(node: Node<'_>, parent: Option<Node<'_>>, pretty_flags: i32) -> bool {
    let Some(parent) = parent else {
        return false;
    };
    match node.node_tag() {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr => true,

        NodeTag::T_SubscriptingRef
        | NodeTag::T_ArrayExpr
        | NodeTag::T_RowExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_MinMaxExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_XmlExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_NullIfExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_FuncExpr
        | NodeTag::T_JsonConstructorExpr
        | NodeTag::T_JsonExpr => true,

        NodeTag::T_CaseExpr => true,

        NodeTag::T_FieldSelect => parent.node_tag() != NodeTag::T_FieldSelect,
        NodeTag::T_FieldStore => parent.node_tag() != NodeTag::T_FieldStore,

        NodeTag::T_RelabelType => {
            is_simple_node(node.as_relabel_type().unwrap().arg, Some(node), pretty_flags)
        }
        NodeTag::T_CoerceViaIO => {
            is_simple_node(node.as_coerce_via_io().unwrap().arg, Some(node), pretty_flags)
        }
        NodeTag::T_ArrayCoerceExpr => {
            is_simple_node(node.as_array_coerce_expr().unwrap().arg, Some(node), pretty_flags)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            is_simple_node(node.as_convert_rowtype_expr().unwrap().arg, Some(node), pretty_flags)
        }
        NodeTag::T_ReturningExpr => {
            is_simple_node(node.as_returning_expr().unwrap().retexpr, Some(node), pretty_flags)
        }

        NodeTag::T_OpExpr => {
            if pretty_flags & crate::PRETTYFLAG_PAREN != 0
                && parent.node_tag() == NodeTag::T_OpExpr
            {
                let this = node.as_op_expr().unwrap();
                // Only the scratch names matter; a throwaway context is fine
                // in this cold path.
                let scratch = mcx::MemoryContext::new("isSimpleNode ops");
                let mcx = scratch.mcx();
                let Some(op) = get_simple_binary_op_name(mcx, this) else {
                    return false;
                };
                let c = op.as_bytes()[0];
                let is_lopriop = matches!(c, b'+' | b'-');
                let is_hipriop = matches!(c, b'*' | b'/' | b'%');
                if !(is_lopriop || is_hipriop) {
                    return false;
                }
                let parent_op = parent.as_op_expr().unwrap();
                let Some(pop) = get_simple_binary_op_name(mcx, parent_op) else {
                    return false;
                };
                let pc = pop.as_bytes()[0];
                let is_lopriparent = matches!(pc, b'+' | b'-');
                let is_hipriparent = matches!(pc, b'*' | b'/' | b'%');
                if !(is_lopriparent || is_hipriparent) {
                    return false;
                }
                if is_hipriop && is_lopriparent {
                    return true;
                }
                if is_lopriop && is_hipriparent {
                    return false;
                }
                return core::ptr::eq(node.as_op_expr().unwrap(), {
                    let first = parent_op.args.nth(0);
                    match first.as_op_expr() {
                        Some(f) => f,
                        None => return false,
                    }
                });
            }
            simple_under_parent(parent)
        }

        NodeTag::T_SubLink
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr
        | NodeTag::T_JsonIsPredicate => simple_under_parent(parent),

        NodeTag::T_BoolExpr => match parent.node_tag() {
            NodeTag::T_BoolExpr => {
                if pretty_flags & crate::PRETTYFLAG_PAREN != 0 {
                    let ty = node.as_bool_expr().unwrap().boolop;
                    let pty = parent.as_bool_expr().unwrap().boolop;
                    match ty {
                        BoolExprType::NOT_EXPR | BoolExprType::AND_EXPR => {
                            matches!(pty, BoolExprType::AND_EXPR | BoolExprType::OR_EXPR)
                        }
                        BoolExprType::OR_EXPR => pty == BoolExprType::OR_EXPR,
                    }
                } else {
                    false
                }
            }
            NodeTag::T_FuncExpr => {
                let ff = parent.as_func_expr().unwrap().funcformat;
                !matches!(
                    ff,
                    CoercionForm::COERCE_EXPLICIT_CAST
                        | CoercionForm::COERCE_IMPLICIT_CAST
                        | CoercionForm::COERCE_SQL_SYNTAX
                )
            }
            NodeTag::T_SubscriptingRef
            | NodeTag::T_ArrayExpr
            | NodeTag::T_RowExpr
            | NodeTag::T_CoalesceExpr
            | NodeTag::T_MinMaxExpr
            | NodeTag::T_XmlExpr
            | NodeTag::T_NullIfExpr
            | NodeTag::T_Aggref
            | NodeTag::T_GroupingFunc
            | NodeTag::T_WindowFunc
            | NodeTag::T_CaseExpr
            | NodeTag::T_JsonExpr => true,
            _ => false,
        },

        _ => false,
    }
}

// The shared parent switch for T_OpExpr-fallthrough / T_NullTest / T_SubLink.
fn simple_under_parent(parent: Node<'_>) -> bool {
    match parent.node_tag() {
        NodeTag::T_FuncExpr => {
            let ff = parent.as_func_expr().unwrap().funcformat;
            !matches!(
                ff,
                CoercionForm::COERCE_EXPLICIT_CAST
                    | CoercionForm::COERCE_IMPLICIT_CAST
                    | CoercionForm::COERCE_SQL_SYNTAX
            )
        }
        NodeTag::T_BoolExpr
        | NodeTag::T_SubscriptingRef
        | NodeTag::T_ArrayExpr
        | NodeTag::T_RowExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_MinMaxExpr
        | NodeTag::T_XmlExpr
        | NodeTag::T_NullIfExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_CaseExpr => true,
        _ => false,
    }
}

// get_variable (ruleutils.c): returns the attname, or None for a whole-row
// Var (used by get_target_list for the implicit AS label).
pub(crate) fn get_variable<'mcx>(
    node: Node<'mcx>,
    var: &Var<'mcx>,
    levelsup: u32,
    istoplevel: bool,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<Option<String>> {
    let netlevelsup = (var.varlevelsup + levelsup) as usize;
    if netlevelsup >= ctx.namespaces.len() {
        panic!("bogus varlevelsup: {} offset {levelsup}", var.varlevelsup);
    }
    let dpns = Rc::clone(&ctx.namespaces[netlevelsup]);
    let plan_active = dpns.plan.borrow().plan.is_some();

    let (varno, varattno) = if var.varnosyn > 0 && !plan_active {
        (var.varnosyn as usize, var.varattnosyn)
    } else {
        (var.varno as usize, var.varattno)
    };

    if varno < 1 || varno > dpns.rtable.len() {
        crate::plan::get_variable_special(node, ctx)?;
        return Ok(None);
    }
    let (mut varno, mut varattno) = (varno, varattno);
    if !ctx.appendparents.is_empty() {
        if let Some(appendrels) = &dpns.appendrels {
            let mut pvarno = varno;
            let mut pvarattno = varattno;
            let mut appinfo = appendrels[pvarno];
            let mut found = false;
            // Map up to inheritance parents only, not UNION ALL appendrels.
            while let Some(ai) = appinfo {
                if dpns.rtable[ai.parent_relid as usize - 1].rtekind != RTEKind::RTE_RELATION {
                    break;
                }
                found = false;
                if pvarattno > 0 {
                    if pvarattno as i32 > ai.num_child_cols {
                        break;
                    }
                    pvarattno = ai.parent_colnos[pvarattno as usize - 1];
                    if pvarattno == 0 {
                        break;
                    }
                }
                pvarno = ai.parent_relid as usize;
                found = true;
                appinfo = appendrels[pvarno];
            }
            if found && ctx.appendparents.is_member(pvarno as i32) {
                varno = pvarno;
                varattno = pvarattno;
            }
        }
    }
    let rte: &RangeTblEntry<'_> = dpns.rtable[varno - 1];
    let mut refname = match var.varreturningtype {
        VarReturningType::VAR_RETURNING_OLD => {
            dpns.plan.borrow().ret_old_alias.map(str::to_owned)
        }
        VarReturningType::VAR_RETURNING_NEW => {
            dpns.plan.borrow().ret_new_alias.map(str::to_owned)
        }
        VarReturningType::VAR_RETURNING_DEFAULT => dpns.rtable_names[varno - 1].clone(),
    };
    let colinfo = &dpns.rtable_columns[varno - 1];
    let attnum = varattno;

    if crate::plan::inner_plan_drilldown(var, rte, &dpns, ctx)? {
        debug_assert!(netlevelsup == 0);
        return Ok(None);
    }

    if rte.rtekind == RTEKind::RTE_JOIN && rte.alias.is_none() {
        if rte.joinaliasvars.is_nil() {
            panic!("cannot decompile join alias var in plan tree");
        }
        if attnum > 0 {
            let aliasvar = rte.joinaliasvars.nth(attnum as usize - 1);
            if let Some(av) = aliasvar.as_var() {
                return get_variable(aliasvar, av, var.varlevelsup + levelsup, istoplevel, ctx);
            }
        }
        refname = None;
    }

    let attname: Option<String> = if attnum == 0 {
        None
    } else if attnum > 0 {
        if attnum as usize > colinfo.colnames.len() {
            panic!("invalid attnum {attnum} for deparse column set");
        }
        Some(
            colinfo.colnames[attnum as usize - 1]
                .clone()
                .unwrap_or_else(|| "?dropped?column?".to_string()),
        )
    } else if rte.rtekind == RTEKind::RTE_RELATION {
        // C get_rte_attribute_name: system columns always use the pg_attribute name.
        Some(
            lsyscache::get_attname(ctx.mcx, rte.relid, attnum, false)?
                .expect("get_attname missing_ok=false")
                .as_str()
                .to_string(),
        )
    } else {
        // get_rte_attribute_name -> get_attname: fixed system-column names.
        Some(
            match attnum {
                -1 => "ctid",
                -2 => "xmin",
                -3 => "cmin",
                -4 => "xmax",
                -5 => "cmax",
                -6 => "tableoid",
                _ => gap("get_variable", "unknown system attnum"),
            }
            .to_string(),
        )
    };

    let mut need_prefix = ctx.varprefix
        || attname.is_none()
        || var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT;

    if ctx.var_in_order_by && !ctx.in_group_by && !need_prefix {
        if let (Some(tlist), Some(att)) = (ctx.target_list, attname.as_deref()) {
            let mut colno = 0usize;
            for tle_node in tlist.iter() {
                let tle = tle_node.as_target_entry().expect("tlist entry");
                if tle.resjunk {
                    continue;
                }
                colno += 1;
                let colname: Option<&str> = match &ctx.result_desc {
                    Some(rd) if colno <= rd.len() => Some(rd[colno - 1].as_str()),
                    _ => tle.resname,
                };
                if let Some(cn) = colname {
                    if cn == att && !same_var(tle.expr, var) {
                        need_prefix = true;
                        break;
                    }
                }
            }
        }
    }

    if let (Some(r), true) = (refname.as_deref(), need_prefix) {
        ctx.buf.push_str(&quote_identifier(r));
        ctx.buf.push('.');
    }
    match &attname {
        Some(a) => ctx.buf.push_str(&quote_identifier(a)),
        None => {
            ctx.buf.push('*');
            if istoplevel {
                ctx.buf.push_str("::");
                ctx.buf.push_str(&format_type_with_typemod(var.vartype, var.vartypmod)?);
            }
        }
    }
    Ok(attname)
}

// equal(var, tle->expr) reduced to the Var-vs-Var comparison this check needs.
fn same_var(expr: Node<'_>, var: &Var<'_>) -> bool {
    match expr.as_var() {
        Some(v) => {
            v.varno == var.varno
                && v.varattno == var.varattno
                && v.varlevelsup == var.varlevelsup
                && v.vartype == var.vartype
        }
        None => false,
    }
}

fn get_parameter<'mcx>(param: &Param, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    if let Some((expr, dpns, ancestor_idx)) = crate::plan::find_param_referent(param, ctx) {
        let save = crate::plan::push_ancestor_plan(&dpns, ancestor_idx);
        let save_varprefix = ctx.varprefix;
        ctx.varprefix = true;
        let need_paren = !matches!(
            expr.node_tag(),
            NodeTag::T_Var | NodeTag::T_Aggref | NodeTag::T_GroupingFunc | NodeTag::T_Param
        );
        if need_paren {
            ctx.buf.push('(');
        }
        let r = get_rule_expr(expr, ctx, false);
        if need_paren {
            ctx.buf.push(')');
        }
        ctx.varprefix = save_varprefix;
        crate::plan::pop_ancestor_plan(&dpns, save);
        return r;
    }
    if let Some((subplan, column)) = crate::plan::find_param_generator(param, ctx) {
        ctx.buf.push_str(&format!(
            "({}{}).col{}",
            if subplan.useHashTable { "hashed " } else { "" },
            subplan.plan_name.expect("planned SubPlan has a name"),
            column + 1
        ));
        return Ok(());
    }
    if param.paramkind == ParamKind::PARAM_EXTERN && !ctx.namespaces.is_empty() {
        let dpns = ctx.namespaces.last().unwrap().clone();
        if let Some(argnames) = &dpns.argnames {
            if param.paramid > 0 && param.paramid as usize <= argnames.len() {
                let argname = &argnames[param.paramid as usize - 1];
                let should_qualify = ctx.namespaces.iter().any(|d| !d.rtable_names.is_empty());
                if should_qualify {
                    let funcname = dpns.funcname.as_deref().expect("function dpns has a name");
                    ctx.buf.push_str(&quote_identifier(funcname));
                    ctx.buf.push('.');
                }
                ctx.buf.push_str(&quote_identifier(argname));
                return Ok(());
            }
        }
    }
    match param.paramkind {
        ParamKind::PARAM_EXTERN | ParamKind::PARAM_EXEC => {
            ctx.buf.push_str(&format!("${}", param.paramid));
            Ok(())
        }
        other => gap("get_parameter", &format!("{other:?} deparse")),
    }
}

pub(crate) fn print_subscripts<'mcx>(
    sbsref: &types_nodes::primnodes::SubscriptingRef<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut low = sbsref.reflowerindexpr.iter();
    let has_lower = !sbsref.reflowerindexpr.is_nil();
    for up in sbsref.refupperindexpr.iter() {
        ctx.buf.push('[');
        if has_lower {
            if let Some(Some(l)) = low.next() {
                get_rule_expr(l, ctx, false)?;
            }
            ctx.buf.push(':');
        }
        if let Some(u) = up {
            get_rule_expr(u, ctx, false)?;
        }
        ctx.buf.push(']');
    }
    Ok(())
}

fn oid_output_function_call(mcx: Mcx<'_>, typoutput: Oid, value: Datum) -> PgResult<String> {
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let d = types_fmgr::function_call1_coll_in(&mut finfo, InvalidOid, mcx, value)?;
    // SAFETY: out functions return a NUL-terminated cstring datum; copied out
    // before finfo (and its scratch) dies.
    let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(s.to_str().expect("non-UTF-8 output function result").to_owned())
}

pub(crate) fn get_const_expr(
    c: &Const,
    ctx: &mut DeparseContext<'_>,
    showtype: i32,
) -> PgResult<()> {
    if c.constisnull {
        ctx.buf.push_str("NULL");
        if showtype >= 0 {
            ctx.buf.push_str("::");
            ctx.buf.push_str(&format_type_with_typemod(c.consttype, c.consttypmod)?);
            get_const_collation(c, ctx)?;
        }
        return Ok(());
    }

    let (typoutput, _typisvarlena) = lsyscache::getTypeOutputInfo(c.consttype)?;
    let extval = oid_output_function_call(ctx.mcx, typoutput, c.constvalue)?;

    let mut needlabel = false;
    match c.consttype {
        INT4OID => {
            if !extval.starts_with('-') {
                ctx.buf.push_str(&extval);
            } else {
                ctx.buf.push_str(&format!("'{extval}'"));
                needlabel = true;
            }
        }
        NUMERICOID => {
            if extval.as_bytes().first().is_some_and(u8::is_ascii_digit)
                && extval.contains(['e', 'E', '.'])
            {
                ctx.buf.push_str(&extval);
            } else {
                ctx.buf.push_str(&format!("'{extval}'"));
                needlabel = true;
            }
        }
        BOOLOID => ctx.buf.push_str(if extval == "t" { "true" } else { "false" }),
        _ => simple_quote_literal(&mut ctx.buf, &extval),
    }

    if showtype < 0 {
        return Ok(());
    }

    match c.consttype {
        BOOLOID | UNKNOWNOID => needlabel = false,
        INT4OID => {}
        NUMERICOID => needlabel |= c.consttypmod >= 0,
        _ => needlabel = true,
    }
    if needlabel || showtype > 0 {
        ctx.buf.push_str("::");
        ctx.buf.push_str(&format_type_with_typemod(c.consttype, c.consttypmod)?);
    }
    get_const_collation(c, ctx)
}

fn get_const_collation(c: &Const, ctx: &mut DeparseContext<'_>) -> PgResult<()> {
    if c.constcollid != InvalidOid && c.constcollid != lsyscache::get_typcollation(c.consttype)? {
        let collname = crate::generate_collation_name(ctx.mcx, c.constcollid)?;
        ctx.buf.push_str(&format!(" COLLATE {collname}"));
    }
    Ok(())
}

pub(crate) fn simple_quote_literal(buf: &mut String, val: &str) {
    // standard_conforming_strings=on shape: only ' doubles, never E''.
    buf.push('\'');
    for ch in val.chars() {
        if ch == '\'' {
            buf.push(ch);
        }
        buf.push(ch);
    }
    buf.push('\'');
}

fn get_oper_expr<'mcx>(node: Node<'mcx>, expr: &OpExpr<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    if !ctx.pretty_paren() {
        ctx.buf.push('(');
    }
    if expr.args.len() == 2 {
        let arg1 = expr.args.nth(0);
        let arg2 = expr.args.nth(1);
        get_rule_expr_paren(arg1, ctx, true, Some(node))?;
        let opname = generate_operator_name(
            ctx.mcx,
            expr.opno,
            parse_expr::expr_type(arg1),
            parse_expr::expr_type(arg2),
        )?;
        ctx.buf.push_str(&format!(" {opname} "));
        get_rule_expr_paren(arg2, ctx, true, Some(node))?;
    } else {
        let arg = expr.args.nth(0);
        let opname =
            generate_operator_name(ctx.mcx, expr.opno, InvalidOid, parse_expr::expr_type(arg))?;
        ctx.buf.push_str(&format!("{opname} "));
        get_rule_expr_paren(arg, ctx, true, Some(node))?;
    }
    if !ctx.pretty_paren() {
        ctx.buf.push(')');
    }
    Ok(())
}

// exprIsLengthCoercion (nodeFuncs.c) reduced to the FuncExpr shape.
fn func_expr_length_coercion_typmod(expr: &FuncExpr<'_>) -> i32 {
    if expr.args.len() != 2 && expr.args.len() != 3 {
        return -1;
    }
    let second = expr.args.nth(1);
    match second.as_const() {
        Some(c) if c.consttype == INT4OID && !c.constisnull => c.constvalue.as_i32(),
        _ => -1,
    }
}

fn get_func_expr<'mcx>(
    node: Node<'mcx>,
    expr: &FuncExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    if expr.funcformat == CoercionForm::COERCE_IMPLICIT_CAST && !showimplicit {
        return get_rule_expr_paren(expr.args.nth(0), ctx, false, Some(node));
    }
    if expr.funcformat == CoercionForm::COERCE_EXPLICIT_CAST
        || expr.funcformat == CoercionForm::COERCE_IMPLICIT_CAST
    {
        let arg = expr.args.nth(0);
        let coerced_typmod = func_expr_length_coercion_typmod(expr);
        return get_coercion_expr(arg, ctx, expr.funcresulttype, coerced_typmod, node);
    }
    if expr.funcformat == CoercionForm::COERCE_SQL_SYNTAX && get_func_sql_syntax(node, expr, ctx)? {
        return Ok(());
    }

    let mut argtypes = Vec::with_capacity(expr.args.len());
    let mut argnames = Vec::new();
    for arg in expr.args.iter() {
        if let Some(na) = arg.as_named_arg_expr() {
            argnames.push(na.name.expect("NamedArgExpr has a name"));
        }
        argtypes.push(parse_expr::expr_type(arg));
    }
    let funcname =
        generate_function_name(ctx.mcx, expr.funcid, &argtypes, &argnames, expr.funcvariadic)?;
    ctx.buf.push_str(&funcname);
    ctx.buf.push('(');
    let nargs = expr.args.len();
    for (i, arg) in expr.args.iter().enumerate() {
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        if expr.funcvariadic && i == nargs - 1 {
            ctx.buf.push_str("VARIADIC ");
        }
        get_rule_expr(arg, ctx, true)?;
    }
    ctx.buf.push(')');
    Ok(())
}

const F_TIMEZONE_INTERVAL_TIMESTAMP: Oid = 2070;
const F_TIMEZONE_INTERVAL_TIMESTAMPTZ: Oid = 1026;
const F_TIMEZONE_INTERVAL_TIMETZ: Oid = 2038;
const F_TIMEZONE_TEXT_TIMESTAMP: Oid = 2069;
const F_TIMEZONE_TEXT_TIMESTAMPTZ: Oid = 1159;
const F_TIMEZONE_TEXT_TIMETZ: Oid = 2037;
const F_TIMEZONE_TIMESTAMP: Oid = 6335;
const F_TIMEZONE_TIMESTAMPTZ: Oid = 6334;
const F_TIMEZONE_TIMETZ: Oid = 6336;
const F_OVERLAPS_OIDS: [Oid; 13] =
    [1271, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 2041, 2042, 2043, 2044];
const F_EXTRACT_OIDS: [Oid; 6] = [6199, 6200, 6201, 6202, 6203, 6204];
const F_IS_NORMALIZED: Oid = 4351;
const F_PG_COLLATION_FOR: Oid = 3162;
const F_NORMALIZE: Oid = 4350;
const F_OVERLAY_OIDS: [Oid; 6] = [749, 752, 1404, 1405, 3030, 3031];
const F_POSITION_OIDS: [Oid; 3] = [849, 1698, 2014];
const F_SUBSTRING_FROM_OIDS: [Oid; 6] = [936, 937, 1680, 1699, 2012, 2013];
const F_SUBSTRING_TEXT_TEXT_TEXT: Oid = 2074;
const F_BTRIM_OIDS: [Oid; 3] = [884, 885, 2015];
const F_LTRIM_OIDS: [Oid; 3] = [875, 881, 6195];
const F_RTRIM_OIDS: [Oid; 3] = [876, 882, 6196];
const F_SYSTEM_USER: Oid = 6311;
const F_XMLEXISTS: Oid = 2614;

fn text_const_str(arg: Node<'_>) -> String {
    let c = arg.as_const().expect("SQL-syntax option arg is a Const");
    debug_assert!(c.consttype == types_core::TEXTOID && !c.constisnull);
    crate::text_at(c.constvalue)
}

fn get_func_sql_syntax<'mcx>(
    node: Node<'mcx>,
    expr: &FuncExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<bool> {
    let funcoid = expr.funcid;
    match funcoid {
        F_TIMEZONE_INTERVAL_TIMESTAMP
        | F_TIMEZONE_INTERVAL_TIMESTAMPTZ
        | F_TIMEZONE_INTERVAL_TIMETZ
        | F_TIMEZONE_TEXT_TIMESTAMP
        | F_TIMEZONE_TEXT_TIMESTAMPTZ
        | F_TIMEZONE_TEXT_TIMETZ => {
            ctx.buf.push('(');
            get_rule_expr_paren(expr.args.nth(1), ctx, false, Some(node))?;
            ctx.buf.push_str(" AT TIME ZONE ");
            get_rule_expr_paren(expr.args.nth(0), ctx, false, Some(node))?;
            ctx.buf.push(')');
            Ok(true)
        }
        F_TIMEZONE_TIMESTAMP | F_TIMEZONE_TIMESTAMPTZ | F_TIMEZONE_TIMETZ => {
            ctx.buf.push('(');
            get_rule_expr_paren(expr.args.nth(0), ctx, false, Some(node))?;
            ctx.buf.push_str(" AT LOCAL)");
            Ok(true)
        }
        _ if F_OVERLAPS_OIDS.contains(&funcoid) => {
            ctx.buf.push_str("((");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str(", ");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push_str(") OVERLAPS (");
            get_rule_expr(expr.args.nth(2), ctx, false)?;
            ctx.buf.push_str(", ");
            get_rule_expr(expr.args.nth(3), ctx, false)?;
            ctx.buf.push_str("))");
            Ok(true)
        }
        _ if F_EXTRACT_OIDS.contains(&funcoid) => {
            ctx.buf.push_str("EXTRACT(");
            ctx.buf.push_str(&text_const_str(expr.args.nth(0)));
            ctx.buf.push_str(" FROM ");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push(')');
            Ok(true)
        }
        F_IS_NORMALIZED => {
            ctx.buf.push('(');
            get_rule_expr_paren(expr.args.nth(0), ctx, false, Some(node))?;
            ctx.buf.push_str(" IS");
            if expr.args.len() == 2 {
                ctx.buf.push(' ');
                ctx.buf.push_str(&text_const_str(expr.args.nth(1)));
            }
            ctx.buf.push_str(" NORMALIZED)");
            Ok(true)
        }
        F_PG_COLLATION_FOR => {
            ctx.buf.push_str("COLLATION FOR (");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push(')');
            Ok(true)
        }
        F_NORMALIZE => {
            ctx.buf.push_str("NORMALIZE(");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            if expr.args.len() == 2 {
                ctx.buf.push_str(", ");
                ctx.buf.push_str(&text_const_str(expr.args.nth(1)));
            }
            ctx.buf.push(')');
            Ok(true)
        }
        _ if F_OVERLAY_OIDS.contains(&funcoid) => {
            ctx.buf.push_str("OVERLAY(");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str(" PLACING ");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push_str(" FROM ");
            get_rule_expr(expr.args.nth(2), ctx, false)?;
            if expr.args.len() == 4 {
                ctx.buf.push_str(" FOR ");
                get_rule_expr(expr.args.nth(3), ctx, false)?;
            }
            ctx.buf.push(')');
            Ok(true)
        }
        _ if F_POSITION_OIDS.contains(&funcoid) => {
            ctx.buf.push_str("POSITION((");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push_str(") IN (");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str("))");
            Ok(true)
        }
        _ if F_SUBSTRING_FROM_OIDS.contains(&funcoid) => {
            ctx.buf.push_str("SUBSTRING(");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str(" FROM ");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            if expr.args.len() == 3 {
                ctx.buf.push_str(" FOR ");
                get_rule_expr(expr.args.nth(2), ctx, false)?;
            }
            ctx.buf.push(')');
            Ok(true)
        }
        F_SUBSTRING_TEXT_TEXT_TEXT => {
            ctx.buf.push_str("SUBSTRING(");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str(" SIMILAR ");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push_str(" ESCAPE ");
            get_rule_expr(expr.args.nth(2), ctx, false)?;
            ctx.buf.push(')');
            Ok(true)
        }
        _ if F_BTRIM_OIDS.contains(&funcoid)
            || F_LTRIM_OIDS.contains(&funcoid)
            || F_RTRIM_OIDS.contains(&funcoid) =>
        {
            ctx.buf.push_str(if F_BTRIM_OIDS.contains(&funcoid) {
                "TRIM(BOTH"
            } else if F_LTRIM_OIDS.contains(&funcoid) {
                "TRIM(LEADING"
            } else {
                "TRIM(TRAILING"
            });
            if expr.args.len() == 2 {
                ctx.buf.push(' ');
                get_rule_expr(expr.args.nth(1), ctx, false)?;
            }
            ctx.buf.push_str(" FROM ");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push(')');
            Ok(true)
        }
        F_SYSTEM_USER => {
            ctx.buf.push_str("SYSTEM_USER");
            Ok(true)
        }
        F_XMLEXISTS => {
            // Extra parens: the args are c_exprs.
            ctx.buf.push_str("XMLEXISTS((");
            get_rule_expr(expr.args.nth(0), ctx, false)?;
            ctx.buf.push_str(") PASSING (");
            get_rule_expr(expr.args.nth(1), ctx, false)?;
            ctx.buf.push_str("))");
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn get_agg_expr<'mcx>(aggref: &'mcx Aggref<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    get_agg_expr_original(aggref, ctx, aggref)
}

// get_agg_expr_helper's combine/PARTIAL arms: a combining Aggref deparses the
// child partial Aggref it consumes; PARTIAL prefix keys off original_aggref.
fn get_agg_expr_original<'mcx>(
    aggref: &'mcx Aggref<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    original_aggref: &'mcx Aggref<'mcx>,
) -> PgResult<()> {
    if aggref.aggsplit & types_nodes::primnodes::AGGSPLITOP_COMBINE != 0 {
        assert!(aggref.args.len() == 1, "combining Aggref has one argument");
        let tle = aggref
            .args
            .iter()
            .next()
            .unwrap()
            .as_target_entry()
            .expect("Aggref args are TargetEntries");
        return crate::plan::resolve_special_varno(tle.expr, ctx, &mut |node, ctx| {
            let child = node
                .as_aggref()
                .expect("combining Aggref does not point to an Aggref");
            get_agg_expr_original(child, ctx, original_aggref)
        });
    }
    if original_aggref.aggsplit & types_nodes::primnodes::AGGSPLITOP_SKIPFINAL != 0 {
        ctx.buf.push_str("PARTIAL ");
    }
    let ordered_set = matches!(aggref.aggkind, AGGKIND_ORDERED_SET | AGGKIND_HYPOTHETICAL);
    if aggref.aggkind != types_nodes::primnodes::AGGKIND_NORMAL && !ordered_set {
        gap("get_agg_expr", "unrecognized aggkind");
    }
    let argtypes: Vec<Oid> = aggref.aggargtypes.iter().collect();
    let funcname =
        generate_function_name(ctx.mcx, aggref.aggfnoid, &argtypes, &[], aggref.aggvariadic)?;
    ctx.buf.push_str(&funcname);
    ctx.buf.push('(');
    if !aggref.aggdistinct.is_nil() {
        ctx.buf.push_str("DISTINCT ");
    }
    if ordered_set {
        debug_assert!(!aggref.aggvariadic);
        let mut first = true;
        for arg in aggref.aggdirectargs.iter() {
            if !first {
                ctx.buf.push_str(", ");
            }
            first = false;
            get_rule_expr(arg, ctx, true)?;
        }
        debug_assert!(!aggref.aggorder.is_nil());
        ctx.buf.push_str(") WITHIN GROUP (ORDER BY ");
        query::get_rule_orderby(&aggref.aggorder, &aggref.args, false, ctx)?;
    } else if aggref.aggstar {
        ctx.buf.push('*');
    } else {
        let nargs = aggref
            .args
            .iter()
            .filter(|n| !n.as_target_entry().expect("Aggref args are TargetEntries").resjunk)
            .count();
        let mut i = 0;
        for tle_node in aggref.args.iter() {
            let tle = tle_node.as_target_entry().expect("Aggref args are TargetEntries");
            if tle.resjunk {
                continue;
            }
            if i > 0 {
                ctx.buf.push_str(", ");
            }
            i += 1;
            if aggref.aggvariadic && i == nargs {
                ctx.buf.push_str("VARIADIC ");
            }
            get_rule_expr(tle.expr, ctx, true)?;
        }
    }
    if !ordered_set && !aggref.aggorder.is_nil() {
        ctx.buf.push_str(" ORDER BY ");
        query::get_rule_orderby(&aggref.aggorder, &aggref.args, false, ctx)?;
    }
    if let Some(filter) = aggref.aggfilter {
        ctx.buf.push_str(") FILTER (WHERE ");
        get_rule_expr(filter, ctx, false)?;
    }
    ctx.buf.push(')');
    Ok(())
}

const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

fn get_windowfunc_expr<'mcx>(
    wfunc: &'mcx types_nodes::primnodes::WindowFunc<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_windowfunc_expr_helper(wfunc, ctx, None)
}

// C get_windowfunc_expr_helper: `json` carries the SQL/JSON constructor
// funcname/options override (ruleutils.c:11133).
fn get_windowfunc_expr_helper<'mcx>(
    wfunc: &'mcx types_nodes::primnodes::WindowFunc<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    json: Option<(&'mcx types_nodes::JsonConstructorExpr<'mcx>, &str, bool)>,
) -> PgResult<()> {
    if let Some((_, funcname, _)) = json {
        ctx.buf.push_str(funcname);
    } else {
        let mut argtypes = Vec::with_capacity(wfunc.args.len());
        let mut argnames = Vec::new();
        for arg in wfunc.args.iter() {
            if let Some(na) = arg.as_named_arg_expr() {
                argnames.push(na.name.expect("NamedArgExpr has a name"));
            }
            argtypes.push(parse_expr::expr_type(arg));
        }
        let funcname = generate_function_name(ctx.mcx, wfunc.winfnoid, &argtypes, &argnames, false)?;
        ctx.buf.push_str(&funcname);
    }
    ctx.buf.push('(');
    if wfunc.winstar {
        ctx.buf.push('*');
    } else if matches!(json, Some((_, _, true))) {
        get_rule_expr(wfunc.args.nth(0), ctx, false)?;
        ctx.buf.push_str(" : ");
        get_rule_expr(wfunc.args.nth(1), ctx, false)?;
    } else {
        let mut first = true;
        for arg in wfunc.args.iter() {
            if !first {
                ctx.buf.push_str(", ");
            }
            first = false;
            get_rule_expr(arg, ctx, true)?;
        }
    }
    if let Some((ctor, _, _)) = json {
        get_json_constructor_options(ctor, ctx)?;
    }
    if let Some(filter) = wfunc.aggfilter {
        ctx.buf.push_str(") FILTER (WHERE ");
        get_rule_expr(filter, ctx, false)?;
    }
    ctx.buf.push_str(") OVER ");

    let Some(wclauses) = ctx.window_clause else {
        for dpns in &ctx.namespaces {
            let ps = dpns.plan.borrow();
            if let Some(wagg) = ps.plan.and_then(Node::as_window_agg) {
                if wagg.winref == wfunc.winref {
                    let name = wagg.winname.expect("planned WindowAgg has a winname");
                    ctx.buf.push_str(&quote_identifier(name));
                    return Ok(());
                }
            }
        }
        panic!("could not find window clause for winref {}", wfunc.winref);
    };
    let wc = wclauses
        .iter()
        .map(|n| n.as_window_clause().expect("windowClause entry"))
        .find(|wc| wc.winref == wfunc.winref)
        .unwrap_or_else(|| panic!("could not find window clause for winref {}", wfunc.winref));
    match wc.name {
        Some(name) => ctx.buf.push_str(&quote_identifier(name)),
        None => {
            let tlist = ctx.target_list.expect("query deparse has a targetList");
            query::get_rule_windowspec(wc, tlist, ctx)?;
        }
    }
    Ok(())
}

fn get_case_expr<'mcx>(caseexpr: &CaseExpr<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    append_context_keyword(ctx, "CASE", 0, PRETTYINDENT_VAR, 0);
    if let Some(arg) = caseexpr.arg {
        ctx.buf.push(' ');
        get_rule_expr(arg, ctx, true)?;
    }
    for when_node in caseexpr.args.iter() {
        let when = when_node.as_case_when().expect("CASE args are CaseWhen");
        let mut w = when.expr.expect("CaseWhen has a condition");
        if caseexpr.arg.is_some() {
            if let Some(op) = w.as_op_expr() {
                if op.args.len() == 2
                    && strip_implicit_coercions(op.args.nth(0)).node_tag()
                        == NodeTag::T_CaseTestExpr
                {
                    w = op.args.nth(1);
                }
            }
        }
        if !ctx.pretty_indent() {
            ctx.buf.push(' ');
        }
        append_context_keyword(ctx, "WHEN ", 0, 0, 0);
        get_rule_expr(w, ctx, false)?;
        ctx.buf.push_str(" THEN ");
        get_rule_expr(when.result.expect("CaseWhen has a result"), ctx, true)?;
    }
    if !ctx.pretty_indent() {
        ctx.buf.push(' ');
    }
    append_context_keyword(ctx, "ELSE ", 0, 0, 0);
    get_rule_expr(caseexpr.defresult.expect("transformed CASE has a defresult"), ctx, true)?;
    if !ctx.pretty_indent() {
        ctx.buf.push(' ');
    }
    append_context_keyword(ctx, "END", -PRETTYINDENT_VAR, 0, 0);
    Ok(())
}

pub(crate) fn strip_implicit_coercions(node: Node<'_>) -> Node<'_> {
    match node.node_tag() {
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(f.args.nth(0));
            }
            node
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            if r.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(r.arg);
            }
            node
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            if c.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(c.arg);
            }
            node
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            if a.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(a.arg);
            }
            node
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            if c.convertformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(c.arg);
            }
            node
        }
        _ => node,
    }
}

fn get_array_expr<'mcx>(arrayexpr: &ArrayExpr<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    ctx.buf.push_str("ARRAY[");
    let mut first = true;
    for e in arrayexpr.elements.iter() {
        if !first {
            ctx.buf.push_str(", ");
        }
        first = false;
        get_rule_expr(e, ctx, true)?;
    }
    ctx.buf.push(']');
    if arrayexpr.elements.is_nil() {
        ctx.buf.push_str("::");
        ctx.buf.push_str(&format_type_with_typemod(arrayexpr.array_typeid, -1)?);
    }
    Ok(())
}

fn get_coalesce_expr<'mcx>(c: &CoalesceExpr<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    ctx.buf.push_str("COALESCE(");
    let mut first = true;
    for a in c.args.iter() {
        if !first {
            ctx.buf.push_str(", ");
        }
        first = false;
        get_rule_expr(a, ctx, true)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_minmax_expr<'mcx>(m: &MinMaxExpr<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    ctx.buf.push_str(match m.op {
        MinMaxOp::IS_GREATEST => "GREATEST(",
        MinMaxOp::IS_LEAST => "LEAST(",
    });
    let mut first = true;
    for a in m.args.iter() {
        if !first {
            ctx.buf.push_str(", ");
        }
        first = false;
        get_rule_expr(a, ctx, true)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_saop_expr<'mcx>(
    node: Node<'mcx>,
    expr: &ScalarArrayOpExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let arg1 = expr.args.nth(0);
    let arg2 = expr.args.nth(1);
    if !ctx.pretty_paren() {
        ctx.buf.push('(');
    }
    get_rule_expr_paren(arg1, ctx, true, Some(node))?;
    let opname = generate_operator_name(
        ctx.mcx,
        expr.opno,
        parse_expr::expr_type(arg1),
        lsyscache::get_base_element_type(parse_expr::expr_type(arg2))?,
    )?;
    ctx.buf.push_str(&format!(" {opname} {} (", if expr.useOr { "ANY" } else { "ALL" }));
    get_rule_expr_paren(arg2, ctx, true, Some(node))?;
    if arg2.node_tag() == NodeTag::T_SubLink
        && arg2.as_sub_link().unwrap().subLinkType == SubLinkType::EXPR_SUBLINK
    {
        ctx.buf.push_str("::");
        ctx.buf.push_str(&format_type_with_typemod(
            parse_expr::expr_type(arg2),
            parse_expr::expr_typmod(arg2),
        )?);
    }
    ctx.buf.push(')');
    if !ctx.pretty_paren() {
        ctx.buf.push(')');
    }
    Ok(())
}

fn get_sublink_expr<'mcx>(sublink: &SubLink<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    let query = sublink
        .subselect
        .as_query()
        .unwrap_or_else(|| gap("get_sublink_expr", "untransformed subselect"));

    if sublink.subLinkType == SubLinkType::ARRAY_SUBLINK {
        ctx.buf.push_str("ARRAY(");
    } else {
        ctx.buf.push('(');
    }

    let mut opname: Option<String> = None;
    if let Some(testexpr) = sublink.testexpr {
        match testexpr.node_tag() {
            NodeTag::T_OpExpr => {
                let opexpr = testexpr.as_op_expr().unwrap();
                get_rule_expr(opexpr.args.nth(0), ctx, true)?;
                opname = Some(generate_operator_name(
                    ctx.mcx,
                    opexpr.opno,
                    parse_expr::expr_type(opexpr.args.nth(0)),
                    parse_expr::expr_type(opexpr.args.nth(1)),
                )?);
            }
            NodeTag::T_BoolExpr => {
                ctx.buf.push('(');
                let mut first = true;
                for l in testexpr.as_bool_expr().unwrap().args.iter() {
                    let opexpr =
                        l.as_op_expr().unwrap_or_else(|| gap("get_sublink_expr", "row testexpr"));
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    get_rule_expr(opexpr.args.nth(0), ctx, true)?;
                    if opname.is_none() {
                        opname = Some(generate_operator_name(
                            ctx.mcx,
                            opexpr.opno,
                            parse_expr::expr_type(opexpr.args.nth(0)),
                            parse_expr::expr_type(opexpr.args.nth(1)),
                        )?);
                    }
                }
                ctx.buf.push(')');
            }
            NodeTag::T_RowCompareExpr => {
                // multiple combining operators, < <= > >= cases
                let rcexpr = testexpr.as_row_compare_expr().unwrap();
                ctx.buf.push('(');
                let mut first = true;
                for a in rcexpr.largs.iter() {
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    get_rule_expr(a, ctx, true)?;
                }
                opname = Some(generate_operator_name(
                    ctx.mcx,
                    rcexpr.opnos.nth(0),
                    parse_expr::expr_type(rcexpr.largs.nth(0)),
                    parse_expr::expr_type(rcexpr.rargs.nth(0)),
                )?);
                ctx.buf.push(')');
            }
            other => gap("get_sublink_expr", &format!("{other:?} testexpr")),
        }
    }

    let mut need_paren = true;
    match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => ctx.buf.push_str("EXISTS "),
        SubLinkType::ANY_SUBLINK => {
            let op = opname.as_deref().expect("ANY sublink has a testexpr operator");
            if op == "=" {
                ctx.buf.push_str(" IN ");
            } else {
                ctx.buf.push_str(&format!(" {op} ANY "));
            }
        }
        SubLinkType::ALL_SUBLINK => {
            let op = opname.as_deref().expect("ALL sublink has a testexpr operator");
            ctx.buf.push_str(&format!(" {op} ALL "));
        }
        SubLinkType::ROWCOMPARE_SUBLINK => {
            let op = opname.as_deref().expect("ROWCOMPARE sublink has a testexpr operator");
            ctx.buf.push_str(&format!(" {op} "));
        }
        SubLinkType::EXPR_SUBLINK
        | SubLinkType::MULTIEXPR_SUBLINK
        | SubLinkType::ARRAY_SUBLINK => need_paren = false,
        other => gap("get_sublink_expr", &format!("{other:?} deparse")),
    }

    if need_paren {
        ctx.buf.push('(');
    }
    query::get_query_def(query, ctx, None, false)?;
    if need_paren {
        ctx.buf.push_str("))");
    } else {
        ctx.buf.push(')');
    }
    Ok(())
}

pub(crate) fn get_coercion_expr<'mcx>(
    arg: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    resulttype: Oid,
    resulttypmod: i32,
    parent: Node<'mcx>,
) -> PgResult<()> {
    match arg.as_const() {
        Some(c) if c.consttype == resulttype && c.consttypmod == -1 => {
            get_const_expr(c, ctx, -1)?;
        }
        _ => {
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            get_rule_expr_paren(arg, ctx, false, Some(parent))?;
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
        }
    }
    ctx.buf.push_str("::");
    ctx.buf.push_str(&format_type_with_typemod(resulttype, resulttypmod)?);
    Ok(())
}

fn get_bool_expr<'mcx>(
    node: Node<'mcx>,
    expr: &BoolExpr<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match expr.boolop {
        BoolExprType::AND_EXPR | BoolExprType::OR_EXPR => {
            let sep = if expr.boolop == BoolExprType::AND_EXPR { " AND " } else { " OR " };
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            for (i, arg) in expr.args.iter().enumerate() {
                if i > 0 {
                    ctx.buf.push_str(sep);
                }
                get_rule_expr_paren(arg, ctx, false, Some(node))?;
            }
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
        }
        BoolExprType::NOT_EXPR => {
            if !ctx.pretty_paren() {
                ctx.buf.push('(');
            }
            ctx.buf.push_str("NOT ");
            get_rule_expr_paren(expr.args.nth(0), ctx, false, Some(node))?;
            if !ctx.pretty_paren() {
                ctx.buf.push(')');
            }
        }
    }
    Ok(())
}

fn get_null_test<'mcx>(
    node: Node<'mcx>,
    ntest: &NullTest<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let arg = ntest.arg.expect("NullTest has an arg");
    if !ctx.pretty_paren() {
        ctx.buf.push('(');
    }
    get_rule_expr_paren(arg, ctx, true, Some(node))?;
    let scalar = ntest.argisrow || !lsyscache::type_is_rowtype(parse_expr::expr_type(arg))?;
    ctx.buf.push_str(match (scalar, ntest.nulltesttype) {
        (true, NullTestType::IS_NULL) => " IS NULL",
        (true, NullTestType::IS_NOT_NULL) => " IS NOT NULL",
        (false, NullTestType::IS_NULL) => " IS NOT DISTINCT FROM NULL",
        (false, NullTestType::IS_NOT_NULL) => " IS DISTINCT FROM NULL",
    });
    if !ctx.pretty_paren() {
        ctx.buf.push(')');
    }
    Ok(())
}

fn xml_name_to_sql(name: &str) -> PgResult<String> {
    let mapped = xml::map_xml_name_to_sql_identifier(name.as_bytes())?;
    Ok(String::from_utf8(mapped).expect("XML name is valid UTF-8"))
}

fn get_xml_expr<'mcx>(node: Node<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    use types_nodes::primnodes::XmlExprOp::*;
    use types_nodes::primnodes::XmlOptionType;
    let x = node.as_xml_expr().unwrap();
    let mut needcomma = false;
    match x.op {
        IS_XMLCONCAT => ctx.buf.push_str("XMLCONCAT("),
        IS_XMLELEMENT => ctx.buf.push_str("XMLELEMENT("),
        IS_XMLFOREST => ctx.buf.push_str("XMLFOREST("),
        IS_XMLPARSE => ctx.buf.push_str("XMLPARSE("),
        IS_XMLPI => ctx.buf.push_str("XMLPI("),
        IS_XMLROOT => ctx.buf.push_str("XMLROOT("),
        IS_XMLSERIALIZE => ctx.buf.push_str("XMLSERIALIZE("),
        IS_DOCUMENT => {}
    }
    if matches!(x.op, IS_XMLPARSE | IS_XMLSERIALIZE) {
        ctx.buf.push_str(if x.xmloption == XmlOptionType::XMLOPTION_DOCUMENT {
            "DOCUMENT "
        } else {
            "CONTENT "
        });
    }
    if let Some(name) = x.name {
        ctx.buf.push_str("NAME ");
        ctx.buf.push_str(&quote_identifier(&xml_name_to_sql(name)?));
        needcomma = true;
    }
    if !x.named_args.is_nil() {
        if x.op != IS_XMLFOREST {
            if needcomma {
                ctx.buf.push_str(", ");
            }
            ctx.buf.push_str("XMLATTRIBUTES(");
            needcomma = false;
        }
        for (e, narg) in x.named_args.iter().zip(x.arg_names.iter()) {
            if needcomma {
                ctx.buf.push_str(", ");
            }
            get_rule_expr(e, ctx, true)?;
            ctx.buf.push_str(" AS ");
            let argname = narg.as_string().expect("arg_names cell").sval;
            ctx.buf.push_str(&quote_identifier(&xml_name_to_sql(argname)?));
            needcomma = true;
        }
        if x.op != IS_XMLFOREST {
            ctx.buf.push(')');
        }
    }
    if !x.args.is_nil() {
        if needcomma {
            ctx.buf.push_str(", ");
        }
        match x.op {
            IS_XMLCONCAT | IS_XMLELEMENT | IS_XMLFOREST | IS_XMLPI | IS_XMLSERIALIZE => {
                let mut first = true;
                for a in x.args.iter() {
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    get_rule_expr(a, ctx, true)?;
                }
            }
            IS_XMLPARSE => {
                debug_assert!(x.args.len() == 2);
                get_rule_expr(x.args.nth(0), ctx, true)?;
                let con = x.args.nth(1).as_const().expect("XMLPARSE whitespace Const");
                debug_assert!(!con.constisnull);
                ctx.buf.push_str(if con.constvalue.as_bool() {
                    " PRESERVE WHITESPACE"
                } else {
                    " STRIP WHITESPACE"
                });
            }
            IS_XMLROOT => {
                debug_assert!(x.args.len() == 3);
                get_rule_expr(x.args.nth(0), ctx, true)?;
                ctx.buf.push_str(", VERSION ");
                let ver = x.args.nth(1);
                match ver.as_const() {
                    Some(c) if c.constisnull => ctx.buf.push_str("NO VALUE"),
                    _ => get_rule_expr(ver, ctx, false)?,
                }
                let con = x.args.nth(2).as_const().expect("XMLROOT standalone Const");
                if !con.constisnull {
                    // XmlStandaloneType (xml.h): YES / NO / NO_VALUE / OMITTED.
                    match con.constvalue.as_i32() {
                        0 => ctx.buf.push_str(", STANDALONE YES"),
                        1 => ctx.buf.push_str(", STANDALONE NO"),
                        2 => ctx.buf.push_str(", STANDALONE NO VALUE"),
                        _ => {}
                    }
                }
            }
            IS_DOCUMENT => {
                // C passes the args List to get_rule_expr_paren; a List is
                // never "simple", so parens appear exactly under PRETTY_PAREN.
                if ctx.pretty_paren() {
                    ctx.buf.push('(');
                }
                let mut first = true;
                for a in x.args.iter() {
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    get_rule_expr(a, ctx, false)?;
                }
                if ctx.pretty_paren() {
                    ctx.buf.push(')');
                }
            }
        }
    }
    if x.op == IS_XMLSERIALIZE {
        ctx.buf.push_str(" AS ");
        ctx.buf.push_str(&format_type_with_typemod(x.r#type, x.typmod)?);
        ctx.buf.push_str(if x.indent { " INDENT" } else { " NO INDENT" });
    }
    if x.op == IS_DOCUMENT {
        ctx.buf.push_str(" IS DOCUMENT");
    } else {
        ctx.buf.push(')');
    }
    Ok(())
}

pub(crate) fn get_tablefunc<'mcx>(
    tf: &'mcx types_nodes::primnodes::TableFunc<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    match tf.functype {
        types_nodes::TableFuncType::TFT_XMLTABLE => get_xmltable(tf, ctx, showimplicit),
        types_nodes::TableFuncType::TFT_JSON_TABLE => get_json_table(tf, ctx, showimplicit),
    }
}

fn get_json_table_nested_columns<'mcx>(
    tf: &'mcx types_nodes::primnodes::TableFunc<'mcx>,
    plan: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
    needcomma: bool,
) -> PgResult<()> {
    if let Some(scan) = plan.as_json_table_path_scan() {
        if needcomma {
            ctx.buf.push(',');
        }
        ctx.buf.push(' ');
        append_context_keyword(ctx, "NESTED PATH ", 0, 0, 0);
        let path = scan.path.expect("path").as_json_table_path().expect("JsonTablePath");
        get_const_expr(path.value.expect("path value").as_const().expect("Const"), ctx, -1)?;
        ctx.buf.push_str(" AS ");
        ctx.buf.push_str(&quote_identifier(path.name.expect("path name")));
        get_json_table_columns(tf, scan, ctx, showimplicit)
    } else {
        let join = plan.as_json_table_sibling_join().expect("JsonTableSiblingJoin");
        get_json_table_nested_columns(tf, join.lplan.expect("lplan"), ctx, showimplicit, needcomma)?;
        get_json_table_nested_columns(tf, join.rplan.expect("rplan"), ctx, showimplicit, true)
    }
}

fn get_json_table_columns<'mcx>(
    tf: &'mcx types_nodes::primnodes::TableFunc<'mcx>,
    scan: &'mcx types_nodes::primnodes::JsonTablePathScan<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::primnodes::JsonFormatType;
    use types_nodes::{JsonBehaviorType, JsonExprOp};
    const TYPCATEGORY_STRING: i8 = b'S' as i8;

    ctx.buf.push(' ');
    append_context_keyword(ctx, "COLUMNS (", 0, 0, 0);
    if ctx.pretty_indent() {
        ctx.indent_level += PRETTYINDENT_VAR;
    }
    let mut colnum = 0;
    for (((colname, typid), typmod), colvalexpr) in tf
        .colnames
        .iter()
        .zip(tf.coltypes.iter())
        .zip(tf.coltypmods.iter())
        .zip(tf.colvalexprs.iter())
    {
        if scan.colMin < 0 || colnum < scan.colMin {
            colnum += 1;
            continue;
        }
        if colnum > scan.colMax {
            break;
        }
        if colnum > scan.colMin {
            ctx.buf.push_str(", ");
        }
        colnum += 1;
        append_context_keyword(ctx, "", 0, 0, 0);
        ctx.buf
            .push_str(&quote_identifier(colname.as_string().expect("colnames cell").sval));
        ctx.buf.push(' ');
        let Some(colexpr) = colvalexpr else {
            ctx.buf.push_str("FOR ORDINALITY");
            continue;
        };
        let colexpr = colexpr.as_json_expr().expect("JsonExpr");
        ctx.buf.push_str(&format_type_with_typemod(typid, typmod)?);
        let default_behavior;
        if colexpr.op == JsonExprOp::JSON_EXISTS_OP {
            ctx.buf.push_str(" EXISTS");
            default_behavior = JsonBehaviorType::JSON_BEHAVIOR_FALSE;
        } else {
            if colexpr.op == JsonExprOp::JSON_QUERY_OP {
                let (typcategory, _typispreferred) =
                    lsyscache::get_type_category_preferred(typid)?;
                if typcategory == TYPCATEGORY_STRING {
                    ctx.buf.push_str(
                        if colexpr.format.expect("format").format_type
                            == JsonFormatType::JS_FORMAT_JSONB
                        {
                            " FORMAT JSONB"
                        } else {
                            " FORMAT JSON"
                        },
                    );
                }
            }
            default_behavior = JsonBehaviorType::JSON_BEHAVIOR_NULL;
        }
        ctx.buf.push_str(" PATH ");
        let path_spec = colexpr.path_spec.expect("path_spec");
        if let Some(c) = path_spec.as_const() {
            get_const_expr(c, ctx, -1)?;
        } else {
            get_rule_expr(path_spec, ctx, showimplicit)?;
        }
        get_json_expr_options(colexpr, ctx, default_behavior)?;
    }
    if let Some(child) = scan.child {
        get_json_table_nested_columns(tf, child, ctx, showimplicit, scan.colMin >= 0)?;
    }
    if ctx.pretty_indent() {
        ctx.indent_level -= PRETTYINDENT_VAR;
    }
    append_context_keyword(ctx, ")", 0, 0, 0);
    Ok(())
}

fn get_json_table<'mcx>(
    tf: &'mcx types_nodes::primnodes::TableFunc<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    use types_nodes::JsonBehaviorType;
    let jexpr = tf.docexpr.expect("docexpr").as_json_expr().expect("JsonExpr");
    let root = tf.plan.expect("plan").as_json_table_path_scan().expect("JsonTablePathScan");
    ctx.buf.push_str("JSON_TABLE(");
    if ctx.pretty_indent() {
        ctx.indent_level += PRETTYINDENT_VAR;
    }
    append_context_keyword(ctx, "", 0, 0, 0);
    get_rule_expr(jexpr.formatted_expr.expect("formatted_expr"), ctx, showimplicit)?;
    ctx.buf.push_str(", ");
    let path = root.path.expect("path").as_json_table_path().expect("JsonTablePath");
    get_const_expr(path.value.expect("path value").as_const().expect("Const"), ctx, -1)?;
    ctx.buf.push_str(" AS ");
    ctx.buf.push_str(&quote_identifier(path.name.expect("path name")));
    if !jexpr.passing_values.is_nil() {
        ctx.buf.push(' ');
        append_context_keyword(ctx, "PASSING ", 0, 0, 0);
        if ctx.pretty_indent() {
            ctx.indent_level += PRETTYINDENT_VAR;
        }
        let mut needcomma = false;
        for (name, value) in jexpr.passing_names.iter().zip(jexpr.passing_values.iter()) {
            if needcomma {
                ctx.buf.push_str(", ");
            }
            needcomma = true;
            append_context_keyword(ctx, "", 0, 0, 0);
            get_rule_expr(value, ctx, false)?;
            ctx.buf.push_str(" AS ");
            ctx.buf.push_str(&quote_identifier(name.as_string().expect("passing name").sval));
        }
        if ctx.pretty_indent() {
            ctx.indent_level -= PRETTYINDENT_VAR;
        }
    }
    get_json_table_columns(tf, root, ctx, showimplicit)?;
    let on_error = jexpr.on_error.expect("on_error").as_json_behavior().expect("JsonBehavior");
    if on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY {
        get_json_behavior(on_error, ctx, "ERROR")?;
    }
    if ctx.pretty_indent() {
        ctx.indent_level -= PRETTYINDENT_VAR;
    }
    append_context_keyword(ctx, ")", 0, 0, 0);
    Ok(())
}

fn get_xmltable<'mcx>(
    tf: &'mcx types_nodes::primnodes::TableFunc<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    ctx.buf.push_str("XMLTABLE(");
    if !tf.ns_uris.is_nil() {
        ctx.buf.push_str("XMLNAMESPACES (");
        let mut first = true;
        for (expr, ns_node) in tf.ns_uris.iter().zip(tf.ns_names.iter()) {
            if !first {
                ctx.buf.push_str(", ");
            }
            first = false;
            match ns_node {
                Some(n) => {
                    get_rule_expr(expr, ctx, showimplicit)?;
                    ctx.buf.push_str(" AS ");
                    ctx.buf
                        .push_str(&quote_identifier(n.as_string().expect("ns_names cell").sval));
                }
                None => {
                    ctx.buf.push_str("DEFAULT ");
                    get_rule_expr(expr, ctx, showimplicit)?;
                }
            }
        }
        ctx.buf.push_str("), ");
    }
    ctx.buf.push('(');
    get_rule_expr(tf.rowexpr.expect("XMLTABLE has a rowexpr"), ctx, showimplicit)?;
    ctx.buf.push_str(") PASSING (");
    get_rule_expr(tf.docexpr.expect("XMLTABLE has a docexpr"), ctx, showimplicit)?;
    ctx.buf.push(')');
    if !tf.colexprs.is_nil() {
        ctx.buf.push_str(" COLUMNS ");
        for (colnum, (((colname, typid), typmod), (colexpr, coldefexpr))) in tf
            .colnames
            .iter()
            .zip(tf.coltypes.iter())
            .zip(tf.coltypmods.iter())
            .zip(tf.colexprs.iter().zip(tf.coldefexprs.iter()))
            .enumerate()
        {
            let ordinality = tf.ordinalitycol == colnum as i32;
            let notnull = tf.notnulls.is_member(colnum as i32);
            if colnum > 0 {
                ctx.buf.push_str(", ");
            }
            ctx.buf
                .push_str(&quote_identifier(colname.as_string().expect("colnames cell").sval));
            ctx.buf.push(' ');
            if ordinality {
                ctx.buf.push_str("FOR ORDINALITY");
                continue;
            }
            ctx.buf.push_str(&format_type_with_typemod(typid, typmod)?);
            if let Some(defexpr) = coldefexpr {
                ctx.buf.push_str(" DEFAULT (");
                get_rule_expr(defexpr, ctx, showimplicit)?;
                ctx.buf.push(')');
            }
            if let Some(colexpr) = colexpr {
                ctx.buf.push_str(" PATH (");
                get_rule_expr(colexpr, ctx, showimplicit)?;
                ctx.buf.push(')');
            }
            if notnull {
                ctx.buf.push_str(" NOT NULL");
            }
        }
    }
    ctx.buf.push(')');
    Ok(())
}

fn tupdesc_field_name<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>, fieldno: i32) -> PgResult<String> {
    // C funcapi builds a RowExpr(RECORD) tupdesc from colnames; the name is
    // all we need here (drill-through from subquery/CTE output expressions).
    if let Some(r) = expr.as_row_expr() {
        if r.row_typeid == types_core::catalog::RECORDOID
            && fieldno > 0
            && fieldno <= r.colnames.len() as i32
        {
            return Ok(r
                .colnames
                .nth(fieldno as usize - 1)
                .as_string()
                .expect("RowExpr colname")
                .sval
                .to_owned());
        }
    }
    let tupdesc = funcapi::get_expr_result_tupdesc(mcx, Some(expr), false)?
        .expect("composite expression has a tupdesc");
    debug_assert!(fieldno >= 1 && fieldno <= tupdesc.natts as i32);
    Ok(String::from_utf8_lossy(tupdesc.attr(fieldno as usize - 1).attname.name_str())
        .into_owned())
}

fn get_rte_attribute_name_string<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    attnum: i32,
) -> PgResult<String> {
    if attnum == 0 {
        return Ok("*".to_string());
    }
    if let Some(alias) = rte.alias {
        if attnum > 0 && attnum <= alias.colnames.len() as i32 {
            return Ok(alias
                .colnames
                .nth(attnum as usize - 1)
                .as_string()
                .expect("alias colname")
                .sval
                .to_owned());
        }
    }
    if rte.rtekind == RTEKind::RTE_RELATION {
        return Ok(lsyscache::get_attname(mcx, rte.relid, attnum as i16, false)?
            .expect("get_attname missing_ok=false")
            .as_str()
            .to_owned());
    }
    let eref = rte.eref.expect("RTE has eref");
    if attnum > 0 && attnum <= eref.colnames.len() as i32 {
        return Ok(eref
            .colnames
            .nth(attnum as usize - 1)
            .as_string()
            .expect("eref colname")
            .sval
            .to_owned());
    }
    panic!(
        "invalid attnum {attnum} for rangetable entry {}",
        eref.aliasname.unwrap_or("")
    );
}

pub(crate) fn get_name_for_var_field<'mcx>(
    node: Node<'mcx>,
    fieldno: i32,
    levelsup: u32,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<String> {
    if let Some(r) = node.as_row_expr() {
        if fieldno > 0 && fieldno <= r.colnames.len() as i32 {
            return Ok(r
                .colnames
                .nth(fieldno as usize - 1)
                .as_string()
                .expect("RowExpr colname")
                .sval
                .to_owned());
        }
    }
    if let Some(param) = node.as_param() {
        if let Some((expr, dpns, idx)) = crate::plan::find_param_referent(param, ctx) {
            let save = crate::plan::push_ancestor_plan(&dpns, idx);
            let result = get_name_for_var_field(expr, fieldno, 0, ctx);
            crate::plan::pop_ancestor_plan(&dpns, save);
            return result;
        }
    }
    let var = match node.as_var() {
        Some(v) if v.vartype == types_core::catalog::RECORDOID => v,
        _ => return tupdesc_field_name(ctx.mcx, node, fieldno),
    };

    let netlevelsup = (var.varlevelsup + levelsup) as usize;
    if netlevelsup >= ctx.namespaces.len() {
        panic!("bogus varlevelsup: {} offset {levelsup}", var.varlevelsup);
    }
    let dpns = Rc::clone(&ctx.namespaces[netlevelsup]);
    let plan_active = dpns.plan.borrow().plan.is_some();
    let (varno, varattno) = if var.varnosyn > 0 && !plan_active {
        (var.varnosyn as i32, var.varattnosyn)
    } else {
        (var.varno, var.varattno)
    };

    let (rte, attnum) = if varno >= 1 && varno as usize <= dpns.rtable.len() {
        (dpns.rtable[varno as usize - 1], varattno)
    } else {
        let ps = dpns.plan.borrow();
        if varno == types_nodes::primnodes::OUTER_VAR && ps.outer_tlist.is_some() {
            let tle = crate::plan::get_tle_by_resno(ps.outer_tlist.unwrap(), varattno)
                .unwrap_or_else(|| panic!("bogus varattno for OUTER_VAR var: {varattno}"));
            debug_assert!(netlevelsup == 0);
            let outer = ps.outer_plan.unwrap();
            drop(ps);
            let save = crate::plan::push_child_plan(&dpns, outer);
            let result = get_name_for_var_field(tle.expr, fieldno, levelsup, ctx);
            crate::plan::pop_child_plan(&dpns, save);
            return result;
        }
        if varno == types_nodes::primnodes::INNER_VAR && ps.inner_tlist.is_some() {
            let tle = crate::plan::get_tle_by_resno(ps.inner_tlist.unwrap(), varattno)
                .unwrap_or_else(|| panic!("bogus varattno for INNER_VAR var: {varattno}"));
            debug_assert!(netlevelsup == 0);
            let inner = ps.inner_plan.unwrap();
            drop(ps);
            let save = crate::plan::push_child_plan(&dpns, inner);
            let result = get_name_for_var_field(tle.expr, fieldno, levelsup, ctx);
            crate::plan::pop_child_plan(&dpns, save);
            return result;
        }
        if varno == types_nodes::primnodes::INDEX_VAR && ps.index_tlist.is_some() {
            let tle = crate::plan::get_tle_by_resno(ps.index_tlist.unwrap(), varattno)
                .unwrap_or_else(|| panic!("bogus varattno for INDEX_VAR var: {varattno}"));
            debug_assert!(netlevelsup == 0);
            drop(ps);
            return get_name_for_var_field(tle.expr, fieldno, levelsup, ctx);
        }
        panic!("bogus varno: {varno}");
    };

    if attnum == 0 {
        return get_rte_attribute_name_string(ctx.mcx, rte, fieldno);
    }

    let mut expr: Node<'mcx> = node;
    match rte.rtekind {
        RTEKind::RTE_SUBQUERY => {
            if let Some(subquery) = rte.subquery {
                let ste = subquery
                    .targetList
                    .iter()
                    .map(|n| n.as_target_entry().expect("targetlist holds TargetEntries"))
                    .find(|tle| tle.resno == attnum);
                let ste = match ste {
                    Some(s) if !s.resjunk => s,
                    _ => panic!(
                        "subquery {} does not have attribute {attnum}",
                        rte.eref.and_then(|e| e.aliasname).unwrap_or("")
                    ),
                };
                expr = ste.expr;
                if expr.node_tag() == NodeTag::T_Var {
                    let parents: Vec<Rc<DeparseNamespace<'mcx>>> =
                        ctx.namespaces[netlevelsup..].to_vec();
                    let mydpns =
                        Rc::new(query::set_deparse_for_query(ctx.mcx, subquery, &parents)?);
                    let save_ns = std::mem::replace(
                        &mut ctx.namespaces,
                        std::iter::once(mydpns).chain(parents).collect(),
                    );
                    let result = get_name_for_var_field(expr, fieldno, 0, ctx);
                    ctx.namespaces = save_ns;
                    return result;
                }
            } else {
                // Plan tree: drill into the SubqueryScan child tlist; a
                // proven-empty subquery leaves a childless Result — print fN.
                let ps = dpns.plan.borrow();
                if ps.inner_plan.is_none() {
                    return Ok(format!("f{fieldno}"));
                }
                let tle = crate::plan::get_tle_by_resno(
                    ps.inner_tlist.expect("inner_plan implies inner_tlist"),
                    attnum,
                )
                .unwrap_or_else(|| panic!("bogus varattno for subquery var: {attnum}"));
                debug_assert!(netlevelsup == 0);
                let inner = ps.inner_plan.unwrap();
                drop(ps);
                let save = crate::plan::push_child_plan(&dpns, inner);
                let result = get_name_for_var_field(tle.expr, fieldno, levelsup, ctx);
                crate::plan::pop_child_plan(&dpns, save);
                return result;
            }
        }
        RTEKind::RTE_JOIN => {
            if rte.joinaliasvars.is_nil() {
                panic!("cannot decompile join alias var in plan tree");
            }
            debug_assert!(attnum > 0 && attnum as usize <= rte.joinaliasvars.len());
            expr = rte.joinaliasvars.nth(attnum as usize - 1);
            if expr.node_tag() == NodeTag::T_Var {
                return get_name_for_var_field(expr, fieldno, var.varlevelsup + levelsup, ctx);
            }
        }
        RTEKind::RTE_CTE => {
            let ctelevelsup = rte.ctelevelsup as usize + netlevelsup;
            let cte = if ctelevelsup < ctx.namespaces.len() {
                ctx.namespaces[ctelevelsup]
                    .ctes
                    .iter()
                    .copied()
                    .find(|c| c.ctename == rte.ctename)
            } else {
                None
            };
            if let Some(cte) = cte {
                let ctequery = cte
                    .ctequery
                    .and_then(|n| n.as_query())
                    .expect("transformed CTE holds a Query");
                let tl = if ctequery.commandType == types_nodes::nodes_enums::CmdType::CMD_SELECT
                {
                    &ctequery.targetList
                } else {
                    &ctequery.returningList
                };
                let ste = tl
                    .iter()
                    .map(|n| n.as_target_entry().expect("targetlist holds TargetEntries"))
                    .find(|tle| tle.resno == attnum);
                let ste = match ste {
                    Some(s) if !s.resjunk => s,
                    _ => panic!(
                        "CTE {} does not have attribute {attnum}",
                        rte.eref.and_then(|e| e.aliasname).unwrap_or("")
                    ),
                };
                expr = ste.expr;
                if expr.node_tag() == NodeTag::T_Var {
                    let parents: Vec<Rc<DeparseNamespace<'mcx>>> =
                        ctx.namespaces[ctelevelsup..].to_vec();
                    let mydpns =
                        Rc::new(query::set_deparse_for_query(ctx.mcx, ctequery, &parents)?);
                    let save_ns = std::mem::replace(
                        &mut ctx.namespaces,
                        std::iter::once(mydpns).chain(parents).collect(),
                    );
                    let result = get_name_for_var_field(expr, fieldno, 0, ctx);
                    ctx.namespaces = save_ns;
                    return result;
                }
            } else {
                // Plan tree: CteScan/WorkTableScan expose the emitting plan as
                // inner_plan; a proven-empty CTE leaves a Result — print fN.
                let ps = dpns.plan.borrow();
                if ps.inner_plan.is_none() {
                    return Ok(format!("f{fieldno}"));
                }
                let tle = crate::plan::get_tle_by_resno(
                    ps.inner_tlist.expect("inner_plan implies inner_tlist"),
                    attnum,
                )
                .unwrap_or_else(|| panic!("bogus varattno for subquery var: {attnum}"));
                debug_assert!(netlevelsup == 0);
                let inner = ps.inner_plan.unwrap();
                drop(ps);
                let save = crate::plan::push_child_plan(&dpns, inner);
                let result = get_name_for_var_field(tle.expr, fieldno, levelsup, ctx);
                crate::plan::pop_child_plan(&dpns, save);
                return result;
            }
        }
        _ => {}
    }
    tupdesc_field_name(ctx.mcx, expr, fieldno)
}
