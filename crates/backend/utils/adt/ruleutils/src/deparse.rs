//! get_rule_expr slice: Const, Var (single-relation context), OpExpr,
//! BoolExpr, RelabelType, CoerceViaIO, FuncExpr, NullTest. Every other node
//! tag is a loud named panic.

use datum::Datum;
use format_type::format_type_with_typemod;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, BOOLOID, INT4OID, NUMERICOID, UNKNOWNOID};
use types_error::PgResult;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CoercionForm, Const, FuncExpr, NullTest, NullTestType, OpExpr, Var,
};
use types_nodes::{Node, NodeTag};

use crate::{gap, generate_function_name, generate_operator_name, quote_identifier};

struct DeparseContext<'mcx> {
    mcx: Mcx<'mcx>,
    buf: String,
    pretty_flags: i32,
    relid: Oid,
}

impl DeparseContext<'_> {
    fn pretty_paren(&self) -> bool {
        self.pretty_flags & crate::PRETTYFLAG_PAREN != 0
    }
}

pub fn deparse_expression_pretty(
    mcx: Mcx<'_>,
    expr: Node<'_>,
    relid: Oid,
    showimplicit: bool,
    pretty_flags: i32,
) -> PgResult<String> {
    let mut ctx = DeparseContext { mcx, buf: String::new(), pretty_flags, relid };
    get_rule_expr(expr, &mut ctx, showimplicit)?;
    Ok(ctx.buf)
}

pub(crate) fn walk_varnos(node: Node<'_>, f: &mut impl FnMut(i32, u32)) {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            f(v.varno, v.varlevelsup);
        }
        NodeTag::T_Const | NodeTag::T_Param => {}
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                walk_varnos(a, f);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                walk_varnos(a, f);
            }
        }
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                walk_varnos(a, f);
            }
        }
        NodeTag::T_RelabelType => walk_varnos(node.as_relabel_type().unwrap().arg, f),
        NodeTag::T_CoerceViaIO => walk_varnos(node.as_coerce_via_io().unwrap().arg, f),
        NodeTag::T_NullTest => {
            if let Some(arg) = node.as_null_test().unwrap().arg {
                walk_varnos(arg, f);
            }
        }
        NodeTag::T_List => {
            for a in node.as_list().unwrap().iter() {
                walk_varnos(a, f);
            }
        }
        other => gap("pull_varnos", &format!("{other:?} walk arm")),
    }
}

fn get_rule_expr(node: Node<'_>, ctx: &mut DeparseContext<'_>, showimplicit: bool) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => get_variable(node.as_var().unwrap(), ctx),
        NodeTag::T_Const => get_const_expr(node.as_const().unwrap(), ctx, 0),
        NodeTag::T_OpExpr => get_oper_expr(node, node.as_op_expr().unwrap(), ctx),
        NodeTag::T_FuncExpr => get_func_expr(node, node.as_func_expr().unwrap(), ctx, showimplicit),
        NodeTag::T_BoolExpr => get_bool_expr(node, node.as_bool_expr().unwrap(), ctx),
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
        NodeTag::T_NullTest => get_null_test(node, node.as_null_test().unwrap(), ctx),
        other => gap("get_rule_expr", &format!("{other:?} deparse arm")),
    }
}

fn get_rule_expr_paren(
    node: Node<'_>,
    ctx: &mut DeparseContext<'_>,
    showimplicit: bool,
    parent: Option<Node<'_>>,
) -> PgResult<()> {
    let need_paren =
        ctx.pretty_paren() && !is_simple_node(node, parent, ctx.pretty_flags);
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
        | NodeTag::T_FuncExpr => true,

        NodeTag::T_CaseExpr => true,

        NodeTag::T_RelabelType => {
            is_simple_node(node.as_relabel_type().unwrap().arg, Some(node), pretty_flags)
        }
        NodeTag::T_CoerceViaIO => {
            is_simple_node(node.as_coerce_via_io().unwrap().arg, Some(node), pretty_flags)
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
                        BoolExprType::NOT_EXPR | BoolExprType::AND_EXPR => matches!(
                            pty,
                            BoolExprType::AND_EXPR | BoolExprType::OR_EXPR
                        ),
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

fn get_variable(var: &Var<'_>, ctx: &mut DeparseContext<'_>) -> PgResult<()> {
    let (varno, varattno) = if var.varnosyn > 0 {
        (var.varnosyn as i32, var.varattnosyn)
    } else {
        (var.varno, var.varattno)
    };
    if var.varlevelsup != 0 || varno != 1 || ctx.relid == InvalidOid {
        gap("get_variable", "Var outside the single-relation deparse context");
    }
    if varattno <= 0 {
        gap("get_variable", "whole-row or system-column Var");
    }
    let attname = lsyscache::get_attname(ctx.mcx, ctx.relid, varattno, false)?
        .expect("get_attname missing_ok=false");
    ctx.buf.push_str(&quote_identifier(attname.as_str()));
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

fn get_const_expr(c: &Const, ctx: &mut DeparseContext<'_>, showtype: i32) -> PgResult<()> {
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

fn get_const_collation(c: &Const, _ctx: &mut DeparseContext<'_>) -> PgResult<()> {
    if c.constcollid != InvalidOid && c.constcollid != lsyscache::get_typcollation(c.consttype)? {
        gap("get_const_collation", "generate_collation_name (COLLATE clause)");
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

fn get_oper_expr(node: Node<'_>, expr: &OpExpr<'_>, ctx: &mut DeparseContext<'_>) -> PgResult<()> {
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

fn get_func_expr(
    node: Node<'_>,
    expr: &FuncExpr<'_>,
    ctx: &mut DeparseContext<'_>,
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
    if expr.funcformat == CoercionForm::COERCE_SQL_SYNTAX {
        gap("get_func_expr", "COERCE_SQL_SYNTAX (get_func_sql_syntax)");
    }

    let mut argtypes = Vec::with_capacity(expr.args.len());
    for arg in expr.args.iter() {
        if arg.node_tag() == NodeTag::T_NamedArgExpr {
            gap("get_func_expr", "NamedArgExpr arguments");
        }
        argtypes.push(parse_expr::expr_type(arg));
    }
    let funcname = generate_function_name(ctx.mcx, expr.funcid, &argtypes, expr.funcvariadic)?;
    ctx.buf.push_str(&funcname);
    ctx.buf.push('(');
    for (i, arg) in expr.args.iter().enumerate() {
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        get_rule_expr(arg, ctx, true)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_coercion_expr(
    arg: Node<'_>,
    ctx: &mut DeparseContext<'_>,
    resulttype: Oid,
    resulttypmod: i32,
    parent: Node<'_>,
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

fn get_bool_expr(
    node: Node<'_>,
    expr: &BoolExpr<'_>,
    ctx: &mut DeparseContext<'_>,
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

fn get_null_test(
    node: Node<'_>,
    ntest: &NullTest<'_>,
    ctx: &mut DeparseContext<'_>,
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
