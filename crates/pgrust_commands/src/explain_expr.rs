use std::cell::RefCell;

use pgrust_catalog_data::*;
use pgrust_expr::backend::executor::expr_casts::parse_interval_text_value_with_style;
use pgrust_expr::backend::executor::value_io::format_array_value_text_with_config;
use pgrust_expr::backend::libpq::pqformat::{FloatFormatOptions, format_float8_text};
use pgrust_expr::backend::utils::time::date::{format_date_text, parse_date_text};
use pgrust_expr::backend::utils::time::timestamp::{
    format_timestamp_text, format_timestamptz_text, parse_timestamp_text, parse_timestamptz_text,
};
use pgrust_expr::{
    DateOrder, DateStyleFormat, DateTimeConfig, cast_value, explain_similar_pattern,
    format_record_text, render_geometry_text, render_interval_text_with_config, render_jsonb_bytes,
    render_multirange_text, render_range_text, render_tid_text, render_tsquery_text,
    render_tsvector_text, render_uuid_text,
};
use pgrust_nodes::datetime::{DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TimestampTzADT};
use pgrust_nodes::datum::{ArrayValue, Value};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use pgrust_nodes::primnodes::{
    BuiltinScalarFunction, CMAX_ATTR_NO, CMIN_ATTR_NO, CaseExpr, Expr, FuncExpr, INDEX_VAR,
    INNER_VAR, OUTER_VAR, ParamKind, SELF_ITEM_POINTER_ATTR_NO, ScalarFunctionImpl, SubLinkType,
    TABLE_OID_ATTR_NO, Var, XMAX_ATTR_NO, XMIN_ATTR_NO, attrno_index, expr_sql_type_hint,
};

thread_local! {
    static EXPLAIN_DATETIME_CONFIG: RefCell<Vec<DateTimeConfig>> = RefCell::new(Vec::new());
}

pub struct ExplainDateTimeConfigGuard;

pub fn push_explain_datetime_config(config: &DateTimeConfig) -> ExplainDateTimeConfigGuard {
    EXPLAIN_DATETIME_CONFIG.with(|stack| stack.borrow_mut().push(config.clone()));
    ExplainDateTimeConfigGuard
}

impl Drop for ExplainDateTimeConfigGuard {
    fn drop(&mut self) {
        EXPLAIN_DATETIME_CONFIG.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

fn current_explain_datetime_config() -> DateTimeConfig {
    EXPLAIN_DATETIME_CONFIG
        .with(|stack| stack.borrow().last().cloned())
        .unwrap_or_default()
}

pub fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_with_qualifier(expr, None, column_names)
}

pub fn render_explain_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some(rendered) =
        render_range_support_expr(expr, qualifier, column_names).map(|out| out.render_full())
    {
        return rendered;
    }
    if matches!(
        expr,
        Expr::Func(func)
            if matches!(
                (&func.implementation, func.funcname.as_deref()),
                (
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith),
                    Some("starts_with")
                )
            )
    ) {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if let Expr::Func(func) = expr
        && !render_explain_func_expr_is_infix(func)
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if matches!(expr, Expr::Var(_))
        || matches!(
            expr,
            Expr::Bool(bool_expr)
                if matches!(
                    bool_expr.boolop,
                    pgrust_nodes::primnodes::BoolExprType::Not
                ) && bool_expr
                    .args
                    .first()
                    .is_some_and(|inner| {
                        render_explain_negated_bool_comparison(inner, qualifier, column_names)
                            .is_some()
                    })
        )
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    format!(
        "({})",
        render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub fn render_explain_projection_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_projection_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub fn render_explain_join_expr(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_join_expr_inner(expr, outer_names, inner_names)
    )
}

fn render_explain_var_name(var: &Var, column_names: &[String]) -> Option<String> {
    attrno_index(var.varattno)
        .and_then(|index| column_names.get(index).cloned())
        .map(normalize_explain_var_name)
        .or_else(|| render_explain_system_var_name(var.varattno, column_names))
}

fn render_explain_system_var_name(
    attno: pgrust_nodes::primnodes::AttrNumber,
    column_names: &[String],
) -> Option<String> {
    let name = match attno {
        TABLE_OID_ATTR_NO => "tableoid",
        SELF_ITEM_POINTER_ATTR_NO => "ctid",
        XMIN_ATTR_NO => "xmin",
        CMIN_ATTR_NO => "cmin",
        XMAX_ATTR_NO => "xmax",
        CMAX_ATTR_NO => "cmax",
        _ => return None,
    };
    relation_qualifier_from_column_names(column_names)
        .map(|qualifier| format!("{qualifier}.{name}"))
        .or_else(|| Some(name.into()))
}

fn relation_qualifier_from_column_names(column_names: &[String]) -> Option<String> {
    column_names
        .iter()
        .filter_map(|name| name.split_once('.').map(|(qualifier, _)| qualifier))
        .find(|qualifier| !qualifier.is_empty())
        .map(str::to_string)
}

fn normalize_explain_var_name(name: String) -> String {
    let mut inner = name.as_str();
    let mut stripped = false;
    while let Some(next) = inner
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    {
        inner = next;
        stripped = true;
    }
    if !stripped {
        return name;
    }
    if matches!(
        inner.split_once('(').map(|(func, _)| func),
        Some("avg" | "count" | "max" | "min" | "sum")
    ) {
        inner.to_string()
    } else {
        name
    }
}

pub fn normalize_aggregate_operand_parens(rendered: String) -> String {
    let mut chars = rendered.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != '(' || !aggregate_call_starts_at(&chars, index + 1) {
            index += 1;
            continue;
        }
        let mut depth = 0usize;
        let mut end = index;
        while end < chars.len() {
            match chars[end] {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            end += 1;
        }
        if index > 0
            && end < chars.len()
            && chars[index - 1] == '('
            && chars.get(end + 1).is_some_and(|ch| ch.is_whitespace())
        {
            chars.remove(end);
            chars.remove(index);
            index = index.saturating_sub(1);
        } else {
            index = end.saturating_add(1);
        }
    }
    chars.into_iter().collect()
}

fn aggregate_call_starts_at(chars: &[char], index: usize) -> bool {
    ["avg(", "count(", "max(", "min(", "sum("]
        .iter()
        .any(|prefix| {
            let prefix = prefix.chars().collect::<Vec<_>>();
            chars
                .get(index..index.saturating_add(prefix.len()))
                .is_some_and(|candidate| candidate == prefix.as_slice())
        })
}

fn render_explain_expr_inner(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_inner_with_qualifier(expr, None, column_names)
}

pub fn render_explain_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::GroupingKey(grouping_key) => {
            render_explain_expr_inner_with_qualifier(&grouping_key.expr, qualifier, column_names)
        }
        Expr::GroupingFunc(grouping_func) => {
            let args = grouping_func
                .args
                .iter()
                .map(|arg| render_explain_expr_inner_with_qualifier(arg, qualifier, column_names))
                .collect::<Vec<_>>()
                .join(", ");
            format!("GROUPING({args})")
        }
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .or_else(|| attrno_index(var.varattno).map(|index| format!("column{}", index + 1)))
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Param(param) if param.paramkind == ParamKind::Exec => {
            format!("${}", param.paramid)
        }
        Expr::Param(param) if param.paramkind == ParamKind::External => {
            format!("${}", param.paramid)
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_cast(inner, *ty, qualifier, column_names),
        Expr::Collate {
            expr,
            collation_oid,
        } => render_explain_collate(expr, *collation_oid, qualifier, column_names),
        Expr::Op(op) => match op.op {
            pgrust_nodes::primnodes::OpExprKind::UnaryPlus
            | pgrust_nodes::primnodes::OpExprKind::Negate
            | pgrust_nodes::primnodes::OpExprKind::BitNot => {
                let [inner] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    pgrust_nodes::primnodes::OpExprKind::UnaryPlus => "+",
                    pgrust_nodes::primnodes::OpExprKind::Negate => "-",
                    pgrust_nodes::primnodes::OpExprKind::BitNot => "~",
                    _ => unreachable!(),
                };
                format!(
                    "({op_text} {})",
                    render_explain_expr_inner_with_qualifier(inner, qualifier, column_names)
                )
            }
            pgrust_nodes::primnodes::OpExprKind::Add
            | pgrust_nodes::primnodes::OpExprKind::Sub
            | pgrust_nodes::primnodes::OpExprKind::Mul
            | pgrust_nodes::primnodes::OpExprKind::Div
            | pgrust_nodes::primnodes::OpExprKind::Mod
            | pgrust_nodes::primnodes::OpExprKind::BitAnd
            | pgrust_nodes::primnodes::OpExprKind::BitOr
            | pgrust_nodes::primnodes::OpExprKind::BitXor
            | pgrust_nodes::primnodes::OpExprKind::Shl
            | pgrust_nodes::primnodes::OpExprKind::Shr
            | pgrust_nodes::primnodes::OpExprKind::Concat
            | pgrust_nodes::primnodes::OpExprKind::JsonGet
            | pgrust_nodes::primnodes::OpExprKind::JsonGetText => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    pgrust_nodes::primnodes::OpExprKind::Add => "+",
                    pgrust_nodes::primnodes::OpExprKind::Sub => "-",
                    pgrust_nodes::primnodes::OpExprKind::Mul => "*",
                    pgrust_nodes::primnodes::OpExprKind::Div => "/",
                    pgrust_nodes::primnodes::OpExprKind::Mod => "%",
                    pgrust_nodes::primnodes::OpExprKind::BitAnd => "&",
                    pgrust_nodes::primnodes::OpExprKind::BitOr => "|",
                    pgrust_nodes::primnodes::OpExprKind::BitXor => "#",
                    pgrust_nodes::primnodes::OpExprKind::Shl => "<<",
                    pgrust_nodes::primnodes::OpExprKind::Shr => ">>",
                    pgrust_nodes::primnodes::OpExprKind::Concat => "||",
                    pgrust_nodes::primnodes::OpExprKind::JsonGet => "->",
                    pgrust_nodes::primnodes::OpExprKind::JsonGetText => "->>",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand(left, qualifier, column_names),
                    op_text,
                    render_explain_infix_operand(right, qualifier, column_names)
                )
            }
            pgrust_nodes::primnodes::OpExprKind::Eq
            | pgrust_nodes::primnodes::OpExprKind::NotEq
            | pgrust_nodes::primnodes::OpExprKind::Lt
            | pgrust_nodes::primnodes::OpExprKind::LtEq
            | pgrust_nodes::primnodes::OpExprKind::Gt
            | pgrust_nodes::primnodes::OpExprKind::GtEq
            | pgrust_nodes::primnodes::OpExprKind::RegexMatch
            | pgrust_nodes::primnodes::OpExprKind::ArrayOverlap
            | pgrust_nodes::primnodes::OpExprKind::ArrayContains
            | pgrust_nodes::primnodes::OpExprKind::ArrayContained
            | pgrust_nodes::primnodes::OpExprKind::JsonbContains
            | pgrust_nodes::primnodes::OpExprKind::JsonbContained
            | pgrust_nodes::primnodes::OpExprKind::JsonbExists
            | pgrust_nodes::primnodes::OpExprKind::JsonbExistsAny
            | pgrust_nodes::primnodes::OpExprKind::JsonbExistsAll
            | pgrust_nodes::primnodes::OpExprKind::JsonbPathExists
            | pgrust_nodes::primnodes::OpExprKind::JsonbPathMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                if let Some(rendered) =
                    render_explain_bool_comparison(op.op, left, right, qualifier, column_names)
                {
                    return rendered;
                }
                let op_text = infix_operator_text(op.opno, op.op).unwrap_or("~");
                if let (Some(left), Some(right)) = (
                    render_explain_row_comparison_operand(left, qualifier, column_names),
                    render_explain_row_comparison_operand(right, qualifier, column_names),
                ) {
                    return format!("{left} {op_text} {right}");
                }
                let display_type = comparison_display_type(left, right, op.collation_oid);
                let right_collation_oid =
                    if text_pattern_operator_suppresses_explain_collation(op.opno) {
                        None
                    } else {
                        op.collation_oid
                    };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand_with_display_type(
                        left,
                        display_type,
                        None,
                        qualifier,
                        column_names
                    ),
                    op_text,
                    render_explain_infix_operand_with_display_type(
                        right,
                        display_type,
                        right_collation_oid,
                        qualifier,
                        column_names
                    )
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            pgrust_nodes::primnodes::BoolExprType::And => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    pgrust_nodes::primnodes::BoolExprType::And,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_bool_arg(arg, qualifier, column_names))
                    .collect::<Vec<_>>();
                rendered.join(" AND ")
            }
            pgrust_nodes::primnodes::BoolExprType::Or => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    pgrust_nodes::primnodes::BoolExprType::Or,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_bool_arg(arg, qualifier, column_names))
                    .collect::<Vec<_>>();
                rendered.join(" OR ")
            }
            pgrust_nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                if let Some(rendered) =
                    render_explain_negated_bool_comparison(inner, qualifier, column_names)
                {
                    return rendered;
                }
                let rendered =
                    render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
                if explain_bool_arg_is_bare(inner) {
                    format!("NOT {rendered}")
                } else {
                    format!("NOT ({rendered})")
                }
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_expr_inner_with_qualifier(left, qualifier, column_names),
            render_explain_expr_inner_with_qualifier(right, qualifier, column_names)
        ),
        Expr::IsNull(inner) => {
            let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
            if expr_sql_type_is_bool(inner) {
                format!("{rendered} IS UNKNOWN")
            } else {
                format!("{rendered} IS NULL")
            }
        }
        Expr::IsNotNull(inner) => {
            let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
            if expr_sql_type_is_bool(inner) {
                format!("{rendered} IS NOT UNKNOWN")
            } else {
                format!("{rendered} IS NOT NULL")
            }
        }
        Expr::IsDistinctFrom(left, right) => {
            render_explain_distinctness_expr(left, right, true, qualifier, column_names)
        }
        Expr::IsNotDistinctFrom(left, right) => {
            render_explain_distinctness_expr(left, right, false, qualifier, column_names)
        }
        Expr::Func(func) => render_explain_func_expr(func, qualifier, column_names),
        Expr::ScalarArrayOp(saop) => render_explain_scalar_array_op(saop, qualifier, column_names),
        Expr::Case(case_expr) => render_explain_whole_row_case(case_expr, qualifier, column_names)
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => render_explain_array_literal(elements, *array_type, qualifier, column_names),
        Expr::Row { fields, .. } => {
            let fields = fields
                .iter()
                .map(|(_, expr)| {
                    render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROW({fields})")
        }
        Expr::Case(case_expr) => render_explain_case_expr(case_expr, qualifier, column_names),
        Expr::FieldSelect {
            expr: inner, field, ..
        } => render_explain_field_select(inner, field, qualifier, column_names),
        Expr::ArraySubscript { array, subscripts } => {
            render_explain_array_subscript(array, subscripts, qualifier, column_names)
        }
        Expr::SubPlan(subplan) => {
            if subplan.renders_as_initplan() {
                format!("(InitPlan {}).col1", subplan.plan_id + 1)
            } else {
                format!("(SubPlan {})", subplan.plan_id + 1)
            }
        }
        Expr::CurrentCatalog => "CURRENT_CATALOG".into(),
        Expr::CurrentSchema => "CURRENT_SCHEMA".into(),
        Expr::CurrentDate => "CURRENT_DATE".into(),
        Expr::CurrentTime { precision } => {
            render_explain_sql_datetime_keyword("CURRENT_TIME", *precision)
        }
        Expr::CurrentTimestamp { precision } => {
            render_explain_sql_datetime_keyword("CURRENT_TIMESTAMP", *precision)
        }
        Expr::LocalTime { precision } => {
            render_explain_sql_datetime_keyword("LOCALTIME", *precision)
        }
        Expr::LocalTimestamp { precision } => {
            render_explain_sql_datetime_keyword("LOCALTIMESTAMP", *precision)
        }
        Expr::CurrentUser => "CURRENT_USER".into(),
        Expr::User => "USER".into(),
        Expr::CurrentRole => "CURRENT_ROLE".into(),
        Expr::SessionUser => "SESSION_USER".into(),
        Expr::SystemUser => "SYSTEM_USER".into(),
        Expr::Random => "random()".into(),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            ..
        } => render_like_explain_expr(
            expr,
            pattern,
            escape.as_deref(),
            *case_insensitive,
            *negated,
            |expr| render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        ),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
        }),
        other => format!("{other:?}"),
    }
}

fn render_explain_case_expr(
    case_expr: &pgrust_nodes::primnodes::CaseExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut rendered = String::from("CASE");
    if let Some(arg) = &case_expr.arg {
        rendered.push(' ');
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            arg,
            qualifier,
            column_names,
        ));
    }
    for arm in &case_expr.args {
        rendered.push_str(" WHEN ");
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            &arm.expr,
            qualifier,
            column_names,
        ));
        rendered.push_str(" THEN ");
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            &arm.result,
            qualifier,
            column_names,
        ));
    }
    rendered.push_str(" ELSE ");
    rendered.push_str(&render_explain_expr_inner_with_qualifier(
        &case_expr.defresult,
        qualifier,
        column_names,
    ));
    rendered.push_str(" END");
    rendered
}

fn render_explain_sql_datetime_keyword(keyword: &str, precision: Option<i32>) -> String {
    match precision {
        Some(precision) => format!("{keyword}({precision})"),
        None => keyword.into(),
    }
}

fn render_explain_field_select(
    inner: &Expr,
    field: &str,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
    if matches!(inner, Expr::ArraySubscript { .. }) {
        format!("{rendered}.{field}")
    } else {
        format!("({rendered}).{field}")
    }
}

fn render_explain_array_subscript(
    array: &Expr,
    subscripts: &[pgrust_nodes::primnodes::ExprArraySubscript],
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut out = render_explain_expr_inner_with_qualifier(array, qualifier, column_names);
    for subscript in subscripts {
        out.push('[');
        if let Some(lower) = &subscript.lower {
            out.push_str(&render_explain_expr_inner_with_qualifier(
                lower,
                qualifier,
                column_names,
            ));
        }
        if subscript.is_slice {
            out.push(':');
            if let Some(upper) = &subscript.upper {
                out.push_str(&render_explain_expr_inner_with_qualifier(
                    upper,
                    qualifier,
                    column_names,
                ));
            }
        }
        out.push(']');
    }
    out
}

fn render_explain_func_expr_is_infix(func: &FuncExpr) -> bool {
    let render_as_named_call = matches!(
        (&func.implementation, func.funcname.as_deref()),
        (
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith),
            Some("starts_with")
        )
    );
    !render_as_named_call && builtin_scalar_function_infix_operator(func.implementation).is_some()
}

fn render_explain_distinctness_expr(
    left: &Expr,
    right: &Expr,
    distinct: bool,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some((expr, value)) = bool_distinctness_operand(left, right) {
        let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
        return match (distinct, value) {
            (false, true) => format!("{rendered} IS TRUE"),
            (true, true) => format!("{rendered} IS NOT TRUE"),
            (false, false) => format!("{rendered} IS FALSE"),
            (true, false) => format!("{rendered} IS NOT FALSE"),
        };
    }
    let operator = if distinct {
        "IS DISTINCT FROM"
    } else {
        "IS NOT DISTINCT FROM"
    };
    format!(
        "{} {operator} {}",
        render_explain_infix_operand(left, qualifier, column_names),
        render_explain_infix_operand(right, qualifier, column_names)
    )
}

fn bool_distinctness_operand<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a Expr, bool)> {
    match (left, right) {
        (expr, Expr::Const(Value::Bool(value))) => Some((expr, *value)),
        (Expr::Const(Value::Bool(value)), expr) => Some((expr, *value)),
        _ => None,
    }
}

fn render_explain_bool_comparison(
    op: pgrust_nodes::primnodes::OpExprKind,
    left: &Expr,
    right: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let (expr, value) = bool_distinctness_operand(left, right)?;
    if !expr_sql_type_is_bool(expr) {
        return None;
    }
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    match (op, value) {
        (pgrust_nodes::primnodes::OpExprKind::Eq, true)
        | (pgrust_nodes::primnodes::OpExprKind::NotEq, false) => Some(rendered),
        (pgrust_nodes::primnodes::OpExprKind::Eq, false)
        | (pgrust_nodes::primnodes::OpExprKind::NotEq, true) => {
            if explain_bool_arg_is_bare(expr) {
                Some(format!("NOT {rendered}"))
            } else {
                Some(format!("NOT ({rendered})"))
            }
        }
        _ => None,
    }
}

fn render_explain_negated_bool_comparison(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let Expr::Op(op) = expr else {
        return None;
    };
    let [left, right] = op.args.as_slice() else {
        return None;
    };
    let (expr, value) = bool_distinctness_operand(left, right)?;
    if !expr_sql_type_is_bool(expr) {
        return None;
    }
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    match (op.op, value) {
        (pgrust_nodes::primnodes::OpExprKind::Eq, false)
        | (pgrust_nodes::primnodes::OpExprKind::NotEq, true) => Some(rendered),
        (pgrust_nodes::primnodes::OpExprKind::Eq, true)
        | (pgrust_nodes::primnodes::OpExprKind::NotEq, false) => {
            if explain_bool_arg_is_bare(expr) {
                Some(format!("NOT {rendered}"))
            } else {
                Some(format!("NOT ({rendered})"))
            }
        }
        _ => None,
    }
}

fn render_explain_func_expr(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
    ) && func.args.len() == 1
    {
        return render_explain_expr_inner_with_qualifier(&func.args[0], qualifier, column_names);
    }
    if let Some(rendered) =
        render_range_support_func_expr(func, qualifier, column_names).map(|out| out.render_inner())
    {
        return rendered;
    }
    if render_explain_func_expr_is_infix(func)
        && let Some(operator) = builtin_scalar_function_infix_operator(func.implementation)
    {
        if let [left, right] = func.args.as_slice() {
            return format!(
                "{} {} {}",
                render_explain_infix_operand(left, qualifier, column_names),
                operator,
                render_explain_infix_operand(right, qualifier, column_names)
            );
        }
    }
    if let Some(rendered) = render_explain_geometry_subscript_func(func, qualifier, column_names) {
        return rendered;
    }
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Timezone)
    ) {
        return render_explain_timezone_function(func, qualifier, column_names);
    }
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::JsonbPathQueryArray)
    ) {
        return render_explain_jsonb_path_query_array_function(func, qualifier, column_names);
    }
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Length)
    ) && let [arg] = func.args.as_slice()
        && expr_sql_type_hint(arg).is_some_and(|ty| ty.kind == SqlTypeKind::Name)
    {
        let rendered = render_explain_expr_inner_with_qualifier(arg, qualifier, column_names);
        return format!("length(({rendered})::text)");
    }
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPoint) => {
            func.funcname.clone().unwrap_or_else(|| "point".into())
        }
        ScalarFunctionImpl::Builtin(builtin) => builtin_scalar_function_name(builtin),
        ScalarFunctionImpl::UserDefined { proc_oid } => func
            .funcname
            .clone()
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    let args = func
        .args
        .iter()
        .map(|arg| render_explain_expr_inner_with_qualifier(arg, qualifier, column_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn render_explain_geometry_subscript_func(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let index = match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointX) => 0,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointY) => 1,
        _ => return None,
    };
    let arg = func.args.first()?;
    let rendered_arg = render_explain_expr_inner_with_qualifier(arg, qualifier, column_names);
    Some(match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow) => {
            format!("{rendered_arg}[{index}]")
        }
        _ if matches!(
            arg,
            Expr::Func(inner)
                if matches!(
                    inner.implementation,
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
                        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow)
                )
        ) =>
        {
            format!("(({rendered_arg})[{index}])")
        }
        _ => format!("{rendered_arg}[{index}]"),
    })
}

fn render_explain_jsonb_path_query_array_function(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut args = func
        .args
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            if index == 1 {
                render_explain_jsonpath_arg(arg)
            } else {
                let rendered =
                    render_explain_expr_inner_with_qualifier(arg, qualifier, column_names);
                if index == 0 && matches!(arg, Expr::Op(_)) {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }
        })
        .collect::<Vec<_>>();
    if args.len() == 2 {
        args.push("'{}'::jsonb".into());
        args.push("false".into());
    }
    format!("jsonb_path_query_array({})", args.join(", "))
}

fn render_explain_jsonpath_arg(expr: &Expr) -> String {
    match expr {
        Expr::Const(value) if value.as_text().is_some() => {
            let path = value.as_text().unwrap_or_default();
            format!("'{}'::jsonpath", path.replace('\'', "''"))
        }
        Expr::Const(Value::JsonPath(path)) => {
            format!("'{}'::jsonpath", path.to_string().replace('\'', "''"))
        }
        other => render_explain_expr_inner(other, &[]),
    }
}

fn render_explain_scalar_array_op(
    saop: &pgrust_nodes::primnodes::ScalarArrayOpExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let op = match saop.op {
        SubqueryComparisonOp::Eq => "=",
        SubqueryComparisonOp::NotEq => "<>",
        SubqueryComparisonOp::Lt => "<",
        SubqueryComparisonOp::LtEq => "<=",
        SubqueryComparisonOp::Gt => ">",
        SubqueryComparisonOp::GtEq => ">=",
        SubqueryComparisonOp::RegexMatch => "~",
        SubqueryComparisonOp::NotRegexMatch => "!~",
        _ => return format!("{saop:?}"),
    };
    let quantifier = if saop.use_or { "ANY" } else { "ALL" };
    let display_type = if expr_is_ctid_system_var(&saop.left)
        || expr_renders_as_ctid(&saop.left, qualifier, column_names)
    {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Tid)))
    } else if expr_has_bpchar_display_type(&saop.left) {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Char)))
    } else if expr_has_varchar_display_type(&saop.left) {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
    } else if matches!(saop.right.as_ref(), Expr::Const(Value::Null)) {
        pgrust_nodes::primnodes::expr_sql_type_hint(&saop.left).map(SqlType::array_of)
    } else {
        None
    };
    let right_sql = if expr_has_varchar_display_type(&saop.left) {
        render_explain_varchar_array_as_text_array(&saop.right, qualifier, column_names)
    } else if display_type.is_none() {
        pgrust_nodes::primnodes::expr_sql_type_hint(&saop.left).and_then(|left_type| {
            render_explain_context_array_without_outer_cast(
                &saop.right,
                left_type,
                saop.collation_oid,
                qualifier,
                column_names,
            )
        })
    } else {
        None
    }
    .unwrap_or_else(|| {
        render_explain_infix_operand_with_display_type(
            &saop.right,
            display_type,
            saop.collation_oid,
            qualifier,
            column_names,
        )
    });
    if saop.use_or
        && matches!(saop.op, SubqueryComparisonOp::Eq)
        && let Some(element) = scalar_array_singleton_element(&saop.right)
    {
        return format!(
            "{} {op} {}",
            render_explain_infix_operand_with_display_type(
                &saop.left,
                if expr_is_text_cast_from_varchar(&saop.left) {
                    None
                } else {
                    display_type.map(|ty| ty.element_type())
                },
                None,
                qualifier,
                column_names
            ),
            render_explain_infix_operand_with_display_type(
                element,
                display_type.map(|ty| ty.element_type()),
                saop.collation_oid,
                qualifier,
                column_names
            )
        );
    }
    let left_sql = render_explain_infix_operand_with_display_type(
        &saop.left,
        if expr_is_text_cast_from_varchar(&saop.left) {
            None
        } else {
            display_type.map(|ty| ty.element_type())
        },
        None,
        qualifier,
        column_names,
    );
    let right_sql =
        if (left_sql == "ctid" || left_sql.ends_with(".ctid")) && right_sql.ends_with("::text[]") {
            format!("{}::tid[]", right_sql.trim_end_matches("::text[]"))
        } else {
            right_sql
        };
    format!("{left_sql} {op} {quantifier} ({right_sql})")
}

fn expr_is_ctid_system_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varattno == SELF_ITEM_POINTER_ATTR_NO,
        Expr::Cast(inner, _) => expr_is_ctid_system_var(inner),
        _ => false,
    }
}

fn expr_renders_as_ctid(expr: &Expr, qualifier: Option<&str>, column_names: &[String]) -> bool {
    let rendered = render_explain_infix_operand(expr, qualifier, column_names);
    rendered == "ctid" || rendered.ends_with(".ctid")
}

fn scalar_array_singleton_element(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ArrayLiteral { elements, .. } if elements.len() == 1 => elements.first(),
        Expr::Cast(inner, _) => scalar_array_singleton_element(inner),
        _ => None,
    }
}

fn render_explain_varchar_array_as_text_array(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let array_expr = match expr {
        Expr::ArrayLiteral { .. } => expr,
        Expr::Cast(inner, ty) if ty.is_array => inner.as_ref(),
        _ => return None,
    };
    let Expr::ArrayLiteral { elements, .. } = array_expr else {
        return None;
    };
    if elements.iter().all(|expr| {
        render_explain_array_literal_const(expr, SqlType::new(SqlTypeKind::Varchar)).is_some()
    }) {
        return None;
    }
    let varchar_type = SqlType::new(SqlTypeKind::Varchar);
    let varchar_type_name = render_explain_sql_type_name(varchar_type);
    let elements = elements
        .iter()
        .map(|expr| match expr {
            Expr::Const(value) => format!(
                "{}::{varchar_type_name}",
                render_explain_typed_literal(value, varchar_type)
            ),
            _ => format!(
                "({})::{varchar_type_name}",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            ),
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("(ARRAY[{elements}])::text[]"))
}

fn render_explain_context_array_without_outer_cast(
    expr: &Expr,
    context_element_type: SqlType,
    collation_oid: Option<u32>,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let (elements, array_type) = match expr {
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => (elements.as_slice(), *array_type),
        Expr::Cast(inner, array_type) if array_type.is_array => {
            let Expr::ArrayLiteral { elements, .. } = inner.as_ref() else {
                return None;
            };
            (elements.as_slice(), *array_type)
        }
        _ => return None,
    };
    let array_element_type = array_type.element_type();
    if !scalar_array_context_type_matches(context_element_type, array_element_type)
        || elements.iter().all(array_element_is_const_like)
    {
        return None;
    }
    let elements = elements
        .iter()
        .map(|expr| {
            render_explain_infix_operand_with_display_type(
                expr,
                Some(array_element_type),
                collation_oid,
                qualifier,
                column_names,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("ARRAY[{elements}]"))
}

fn scalar_array_context_type_matches(left: SqlType, right: SqlType) -> bool {
    left == right || left.with_typmod(SqlType::NO_TYPEMOD) == right.with_typmod(SqlType::NO_TYPEMOD)
}

fn array_element_is_const_like(expr: &Expr) -> bool {
    match expr {
        Expr::Const(_) => true,
        Expr::Cast(inner, _) => array_element_is_const_like(inner),
        _ => false,
    }
}

pub fn render_verbose_range_support_expr(expr: &Expr, column_names: &[String]) -> Option<String> {
    render_range_support_expr(expr, None, column_names).map(|out| out.render_verbose())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RangeSupportSubtype {
    Date,
    TimestampTz,
    Text,
}

#[derive(Clone)]
enum RangeSupportBoundValue {
    Date(DateADT),
    TimestampTz(TimestampTzADT),
    Text(String),
}

#[derive(Clone)]
struct RangeSupportBound {
    value: RangeSupportBoundValue,
    inclusive: bool,
}

struct RangeSupportBounds {
    subtype: RangeSupportSubtype,
    empty: bool,
    lower: Option<RangeSupportBound>,
    upper: Option<RangeSupportBound>,
}

enum RangeSupportOutput {
    Bool(&'static str),
    Comparison(String),
    And(String, String),
    ElementContainedByRangeLiteral { elem: String, range_literal: String },
}

impl RangeSupportOutput {
    fn render_inner(self) -> String {
        match self {
            Self::Bool(value) => value.into(),
            Self::Comparison(comparison) => comparison,
            Self::And(lower, upper) => format!("({lower}) AND ({upper})"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("{elem} <@ {range_literal}")
            }
        }
    }

    fn render_full(self) -> String {
        match self {
            Self::Bool(value) => format!("({value})"),
            Self::Comparison(comparison) => format!("({comparison})"),
            Self::And(lower, upper) => format!("(({lower}) AND ({upper}))"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("({elem} <@ {range_literal})")
            }
        }
    }

    fn render_verbose(self) -> String {
        match self {
            Self::Bool(value) => value.into(),
            Self::Comparison(comparison) => format!("({comparison})"),
            Self::And(lower, upper) => format!("(({lower}) AND ({upper}))"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("({elem} <@ {range_literal})")
            }
        }
    }
}

fn render_range_support_expr(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<RangeSupportOutput> {
    let Expr::Func(func) = expr else {
        return None;
    };
    render_range_support_func_expr(func, qualifier, column_names)
}

fn render_range_support_func_expr(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<RangeSupportOutput> {
    let (elem, range) = match (func.implementation, func.args.as_slice()) {
        (ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeContainedBy), [elem, range]) => {
            (elem, range)
        }
        (ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeContains), [range, elem]) => {
            (elem, range)
        }
        _ => return None,
    };
    let bounds = range_support_bounds(range)?;
    if bounds.empty {
        return Some(RangeSupportOutput::Bool("false"));
    }
    let elem_expr = elem;
    let elem = render_range_support_elem(elem_expr, qualifier, column_names);
    if bounds.lower.is_none() && bounds.upper.is_none() {
        return Some(RangeSupportOutput::Bool("true"));
    }

    if bounds.subtype == RangeSupportSubtype::TimestampTz
        && range_support_elem_is_clock_timestamp(strip_range_support_casts(elem_expr))
        && bounds.lower.is_some()
        && bounds.upper.is_some()
        && let Some(range_literal) = render_tstzrange_support_literal(&bounds)
    {
        return Some(RangeSupportOutput::ElementContainedByRangeLiteral {
            elem,
            range_literal,
        });
    }

    let mut comparisons = Vec::with_capacity(2);
    if let Some(lower) = &bounds.lower {
        comparisons.push(render_range_bound_comparison(
            &elem,
            RangeSupportBoundSide::Lower,
            lower,
            bounds.subtype,
        )?);
    }
    if let Some(upper) = &bounds.upper {
        comparisons.push(render_range_bound_comparison(
            &elem,
            RangeSupportBoundSide::Upper,
            upper,
            bounds.subtype,
        )?);
    }
    match comparisons.as_slice() {
        [] => Some(RangeSupportOutput::Bool("true")),
        [comparison] => Some(RangeSupportOutput::Comparison(comparison.clone())),
        [lower, upper] => Some(RangeSupportOutput::And(lower.clone(), upper.clone())),
        _ => None,
    }
}

fn range_support_bounds(expr: &Expr) -> Option<RangeSupportBounds> {
    match strip_range_support_casts(expr) {
        Expr::Const(Value::Range(range)) => {
            let subtype = range_support_subtype_for_sql_type(range.range_type.subtype)?;
            Some(RangeSupportBounds {
                subtype,
                empty: range.empty,
                lower: match range.lower.as_ref() {
                    Some(bound) => Some(range_support_bound_from_range_bound(bound, subtype)?),
                    None => None,
                },
                upper: match range.upper.as_ref() {
                    Some(bound) => Some(range_support_bound_from_range_bound(bound, subtype)?),
                    None => None,
                },
            })
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeConstructor)
            ) =>
        {
            let subtype = func
                .funcresulttype
                .and_then(range_support_subtype_for_sql_type)
                .or_else(|| range_support_subtype_from_constructor_args(&func.args))?;
            let lower_arg = func.args.first()?;
            let upper_arg = func.args.get(1)?;
            let (lower_inclusive, upper_inclusive) =
                range_constructor_inclusivity(func.args.get(2));
            Some(RangeSupportBounds {
                subtype,
                empty: false,
                lower: range_support_bound_from_expr(lower_arg, subtype, lower_inclusive)?,
                upper: range_support_bound_from_expr(upper_arg, subtype, upper_inclusive)?,
            })
        }
        _ => None,
    }
}

fn range_support_bound_from_range_bound(
    bound: &pgrust_nodes::datum::RangeBound,
    subtype: RangeSupportSubtype,
) -> Option<RangeSupportBound> {
    Some(RangeSupportBound {
        value: range_support_bound_value_from_value(&bound.value, subtype)?,
        inclusive: bound.inclusive,
    })
}

fn range_support_subtype_for_sql_type(sql_type: SqlType) -> Option<RangeSupportSubtype> {
    match sql_type.kind {
        SqlTypeKind::Date | SqlTypeKind::DateRange => Some(RangeSupportSubtype::Date),
        SqlTypeKind::TimestampTz | SqlTypeKind::TimestampTzRange => {
            Some(RangeSupportSubtype::TimestampTz)
        }
        SqlTypeKind::Text => Some(RangeSupportSubtype::Text),
        SqlTypeKind::Range => match sql_type.range_subtype_oid {
            DATE_TYPE_OID => Some(RangeSupportSubtype::Date),
            TIMESTAMPTZ_TYPE_OID => Some(RangeSupportSubtype::TimestampTz),
            TEXT_TYPE_OID => Some(RangeSupportSubtype::Text),
            _ => None,
        },
        _ => None,
    }
}

fn range_support_subtype_from_constructor_args(args: &[Expr]) -> Option<RangeSupportSubtype> {
    if args.iter().any(|arg| {
        matches!(
            strip_range_support_casts(arg),
            Expr::Const(Value::TimestampTz(_))
        )
    }) {
        return Some(RangeSupportSubtype::TimestampTz);
    }
    if args
        .iter()
        .any(|arg| matches!(strip_range_support_casts(arg), Expr::Const(Value::Date(_))))
    {
        return Some(RangeSupportSubtype::Date);
    }
    if args.iter().any(|arg| {
        matches!(
            strip_range_support_casts(arg),
            Expr::Const(Value::Text(_) | Value::TextRef(_, _))
        )
    }) {
        return Some(RangeSupportSubtype::Text);
    }
    None
}

fn range_support_bound_from_expr(
    expr: &Expr,
    subtype: RangeSupportSubtype,
    inclusive: bool,
) -> Option<Option<RangeSupportBound>> {
    match strip_range_support_casts(expr) {
        Expr::Const(Value::Null) => Some(None),
        Expr::Const(value) => Some(Some(RangeSupportBound {
            value: range_support_bound_value_from_value(value, subtype)?,
            inclusive,
        })),
        _ => None,
    }
}

fn range_support_bound_value_from_value(
    value: &Value,
    subtype: RangeSupportSubtype,
) -> Option<RangeSupportBoundValue> {
    match subtype {
        RangeSupportSubtype::Date => match value {
            Value::Date(value) => Some(RangeSupportBoundValue::Date(*value)),
            Value::Text(_) | Value::TextRef(_, _) => {
                let config = postgres_explain_datetime_config();
                parse_date_text(value.as_text()?, &config)
                    .ok()
                    .map(RangeSupportBoundValue::Date)
            }
            _ => None,
        },
        RangeSupportSubtype::TimestampTz => match value {
            Value::TimestampTz(value) => Some(RangeSupportBoundValue::TimestampTz(*value)),
            Value::Text(_) | Value::TextRef(_, _) => {
                let config = postgres_explain_datetime_config();
                parse_timestamptz_text(value.as_text()?, &config)
                    .ok()
                    .map(RangeSupportBoundValue::TimestampTz)
            }
            _ => None,
        },
        RangeSupportSubtype::Text => match value {
            Value::Text(_) | Value::TextRef(_, _) => {
                Some(RangeSupportBoundValue::Text(value.as_text()?.to_string()))
            }
            _ => None,
        },
    }
}

fn range_constructor_inclusivity(flags: Option<&Expr>) -> (bool, bool) {
    let Some(flags) = flags else {
        return (true, false);
    };
    let Some(text) = range_support_const_text(flags) else {
        return (true, false);
    };
    let mut chars = text.chars();
    let lower = matches!(chars.next(), Some('['));
    let upper = matches!(chars.next_back(), Some(']'));
    (lower, upper)
}

fn range_support_const_text(expr: &Expr) -> Option<&str> {
    match strip_range_support_casts(expr) {
        Expr::Const(value) => value.as_text(),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum RangeSupportBoundSide {
    Lower,
    Upper,
}

fn render_range_bound_comparison(
    elem: &str,
    side: RangeSupportBoundSide,
    bound: &RangeSupportBound,
    subtype: RangeSupportSubtype,
) -> Option<String> {
    let (op, value) = match (side, subtype) {
        (RangeSupportBoundSide::Lower, RangeSupportSubtype::Text) => {
            let op = if bound.inclusive { "~>=~" } else { "~>~" };
            (op, render_text_support_literal(bound)?)
        }
        (RangeSupportBoundSide::Upper, RangeSupportSubtype::Text) => {
            let op = if bound.inclusive { "~<=~" } else { "~<~" };
            (op, render_text_support_literal(bound)?)
        }
        (RangeSupportBoundSide::Lower, _) => {
            let op = if bound.inclusive { ">=" } else { ">" };
            (
                op,
                render_range_support_bound_literal(bound, subtype, false)?,
            )
        }
        (RangeSupportBoundSide::Upper, RangeSupportSubtype::Date)
            if bound.inclusive && range_support_bound_is_finite(bound) =>
        {
            (
                "<",
                render_range_support_bound_literal(bound, subtype, true)?,
            )
        }
        (RangeSupportBoundSide::Upper, _) => {
            let op = if bound.inclusive { "<=" } else { "<" };
            (
                op,
                render_range_support_bound_literal(bound, subtype, false)?,
            )
        }
    };
    Some(format!("{elem} {op} {value}"))
}

fn range_support_bound_is_finite(bound: &RangeSupportBound) -> bool {
    match &bound.value {
        RangeSupportBoundValue::Date(value) => {
            value.0 != DATEVAL_NOBEGIN && value.0 != DATEVAL_NOEND
        }
        RangeSupportBoundValue::TimestampTz(value) => value.is_finite(),
        RangeSupportBoundValue::Text(_) => true,
    }
}

fn render_range_support_bound_literal(
    bound: &RangeSupportBound,
    subtype: RangeSupportSubtype,
    increment_date: bool,
) -> Option<String> {
    let config = postgres_explain_datetime_config();
    match (&bound.value, subtype) {
        (RangeSupportBoundValue::Date(value), RangeSupportSubtype::Date) => {
            let value = if increment_date {
                DateADT(value.0 + 1)
            } else {
                *value
            };
            Some(format!("'{}'::date", format_date_text(value, &config)))
        }
        (RangeSupportBoundValue::TimestampTz(value), RangeSupportSubtype::TimestampTz) => {
            Some(format!(
                "'{}'::timestamp with time zone",
                format_timestamptz_text(*value, &config)
            ))
        }
        (RangeSupportBoundValue::Text(_), RangeSupportSubtype::Text) => {
            render_text_support_literal(bound)
        }
        _ => None,
    }
}

fn render_text_support_literal(bound: &RangeSupportBound) -> Option<String> {
    let RangeSupportBoundValue::Text(value) = &bound.value else {
        return None;
    };
    Some(format!("'{}'::text", value.replace('\'', "''")))
}

fn render_tstzrange_support_literal(bounds: &RangeSupportBounds) -> Option<String> {
    if bounds.subtype != RangeSupportSubtype::TimestampTz {
        return None;
    }
    let lower = bounds.lower.as_ref()?;
    let upper = bounds.upper.as_ref()?;
    let config = postgres_explain_datetime_config();
    let lower_value = match &lower.value {
        RangeSupportBoundValue::TimestampTz(value) => format_timestamptz_text(*value, &config),
        _ => return None,
    };
    let upper_value = match &upper.value {
        RangeSupportBoundValue::TimestampTz(value) => format_timestamptz_text(*value, &config),
        _ => return None,
    };
    let lower_bracket = if lower.inclusive { '[' } else { '(' };
    let upper_bracket = if upper.inclusive { ']' } else { ')' };
    Some(format!(
        "'{lower_bracket}\"{}\",\"{}\"{upper_bracket}'::tstzrange",
        lower_value.replace('"', "\\\""),
        upper_value.replace('"', "\\\"")
    ))
}

fn render_range_support_elem(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let expr = strip_range_support_casts(expr);
    match expr {
        Expr::CurrentDate => "CURRENT_DATE".into(),
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Now)
            ) =>
        {
            "now()".into()
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ClockTimestamp)
            ) =>
        {
            "clock_timestamp()".into()
        }
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

fn range_support_elem_is_clock_timestamp(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ClockTimestamp)
            )
    )
}

fn strip_range_support_casts(mut expr: &Expr) -> &Expr {
    while let Expr::Cast(inner, _) = expr {
        expr = inner;
    }
    expr
}

pub fn postgres_explain_datetime_config() -> DateTimeConfig {
    DateTimeConfig {
        date_style_format: DateStyleFormat::Postgres,
        date_order: DateOrder::Mdy,
        time_zone: "America/Los_Angeles".into(),
        ..DateTimeConfig::default()
    }
}

fn render_explain_timezone_function(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match func.args.as_slice() {
        [value] => format!(
            "timezone({})",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names)
        ),
        [zone, value] if is_local_timezone_marker(zone) => format!(
            "({} AT LOCAL)",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names)
        ),
        [zone, value] => format!(
            "({} AT TIME ZONE {})",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names),
            render_explain_expr_inner_with_qualifier(zone, qualifier, column_names)
        ),
        _ => "timezone()".into(),
    }
}

fn is_local_timezone_marker(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Const(value) if value.as_text() == Some("__pgrust_local_timezone__")
    )
}

fn builtin_scalar_function_name(func: BuiltinScalarFunction) -> String {
    match func {
        BuiltinScalarFunction::Lower => "lower".into(),
        BuiltinScalarFunction::Upper => "upper".into(),
        BuiltinScalarFunction::Length => "length".into(),
        BuiltinScalarFunction::OctetLength => "octet_length".into(),
        BuiltinScalarFunction::JsonBuildArray => "json_build_array".into(),
        BuiltinScalarFunction::JsonBuildObject => "json_build_object".into(),
        BuiltinScalarFunction::JsonbBuildArray => "jsonb_build_array".into(),
        BuiltinScalarFunction::JsonbBuildObject => "jsonb_build_object".into(),
        BuiltinScalarFunction::JsonbPathQueryArray => "jsonb_path_query_array".into(),
        BuiltinScalarFunction::RowToJson => "row_to_json".into(),
        BuiltinScalarFunction::ArrayToJson => "array_to_json".into(),
        BuiltinScalarFunction::ToJson => "to_json".into(),
        BuiltinScalarFunction::ToJsonb => "to_jsonb".into(),
        BuiltinScalarFunction::SqlJsonConstructor => "JSON".into(),
        BuiltinScalarFunction::SqlJsonScalar => "JSON_SCALAR".into(),
        BuiltinScalarFunction::SqlJsonSerialize => "JSON_SERIALIZE".into(),
        BuiltinScalarFunction::SqlJsonObject => "JSON_OBJECT".into(),
        BuiltinScalarFunction::SqlJsonArray => "JSON_ARRAY".into(),
        BuiltinScalarFunction::SqlJsonIsJson => "IS JSON".into(),
        BuiltinScalarFunction::DatePart => "date_part".into(),
        BuiltinScalarFunction::Extract => "extract".into(),
        BuiltinScalarFunction::TextStartsWith => "starts_with".into(),
        BuiltinScalarFunction::Abs => "abs".into(),
        BuiltinScalarFunction::ToChar => "to_char".into(),
        BuiltinScalarFunction::Substring => "substr".into(),
        BuiltinScalarFunction::ToChar => "to_char".into(),
        BuiltinScalarFunction::Left => "\"left\"".into(),
        BuiltinScalarFunction::Right => "\"right\"".into(),
        other => format!("{other:?}"),
    }
}

fn builtin_scalar_function_infix_operator(
    implementation: ScalarFunctionImpl,
) -> Option<&'static str> {
    match implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoSame) => Some("~="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoDistance) => Some("<->"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContainedBy) => Some("<@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverlap) => Some("&&"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoLeft) => Some("<<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverLeft) => Some("&<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoRight) => Some(">>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverRight) => Some("&>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBelow) => Some("<<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverBelow) => Some("&<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoAbove) => Some("|>>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverAbove) => Some("|&>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSubnet) => Some("<<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSubnetEq) => Some("<<="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSupernet) => Some(">>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSupernetEq) => Some(">>="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkOverlap) => Some("&&"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContainedBy) => Some("<@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith) => Some("^@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsMatch) => Some("@@"),
        _ => None,
    }
}

pub fn infix_operator_text(
    opno: u32,
    op: pgrust_nodes::primnodes::OpExprKind,
) -> Option<&'static str> {
    match opno {
        pgrust_catalog_data::TEXT_PATTERN_LT_OPERATOR_OID => return Some("~<~"),
        pgrust_catalog_data::TEXT_PATTERN_LE_OPERATOR_OID => return Some("~<=~"),
        pgrust_catalog_data::TEXT_PATTERN_GE_OPERATOR_OID => return Some("~>=~"),
        pgrust_catalog_data::TEXT_PATTERN_GT_OPERATOR_OID => return Some("~>~"),
        _ => {}
    }
    match op {
        pgrust_nodes::primnodes::OpExprKind::Add => Some("+"),
        pgrust_nodes::primnodes::OpExprKind::Sub => Some("-"),
        pgrust_nodes::primnodes::OpExprKind::Mul => Some("*"),
        pgrust_nodes::primnodes::OpExprKind::Div => Some("/"),
        pgrust_nodes::primnodes::OpExprKind::Mod => Some("%"),
        pgrust_nodes::primnodes::OpExprKind::BitAnd => Some("&"),
        pgrust_nodes::primnodes::OpExprKind::BitOr => Some("|"),
        pgrust_nodes::primnodes::OpExprKind::BitXor => Some("#"),
        pgrust_nodes::primnodes::OpExprKind::Shl => Some("<<"),
        pgrust_nodes::primnodes::OpExprKind::Shr => Some(">>"),
        pgrust_nodes::primnodes::OpExprKind::Concat => Some("||"),
        pgrust_nodes::primnodes::OpExprKind::Eq => Some("="),
        pgrust_nodes::primnodes::OpExprKind::NotEq => Some("<>"),
        pgrust_nodes::primnodes::OpExprKind::Lt => Some("<"),
        pgrust_nodes::primnodes::OpExprKind::LtEq => Some("<="),
        pgrust_nodes::primnodes::OpExprKind::Gt => Some(">"),
        pgrust_nodes::primnodes::OpExprKind::GtEq => Some(">="),
        pgrust_nodes::primnodes::OpExprKind::RegexMatch => Some("~"),
        pgrust_nodes::primnodes::OpExprKind::ArrayOverlap => Some("&&"),
        pgrust_nodes::primnodes::OpExprKind::ArrayContains => Some("@>"),
        pgrust_nodes::primnodes::OpExprKind::ArrayContained => Some("<@"),
        pgrust_nodes::primnodes::OpExprKind::JsonbContains => Some("@>"),
        pgrust_nodes::primnodes::OpExprKind::JsonbContained => Some("<@"),
        pgrust_nodes::primnodes::OpExprKind::JsonbExists => Some("?"),
        pgrust_nodes::primnodes::OpExprKind::JsonbExistsAny => Some("?|"),
        pgrust_nodes::primnodes::OpExprKind::JsonbExistsAll => Some("?&"),
        pgrust_nodes::primnodes::OpExprKind::JsonbPathExists => Some("@?"),
        pgrust_nodes::primnodes::OpExprKind::JsonbPathMatch => Some("@@"),
        pgrust_nodes::primnodes::OpExprKind::JsonGet => Some("->"),
        pgrust_nodes::primnodes::OpExprKind::JsonGetText => Some("->>"),
        _ => None,
    }
}

pub fn text_pattern_operator_suppresses_explain_collation(opno: u32) -> bool {
    matches!(
        opno,
        pgrust_catalog_data::TEXT_PATTERN_LT_OPERATOR_OID
            | pgrust_catalog_data::TEXT_PATTERN_LE_OPERATOR_OID
            | pgrust_catalog_data::TEXT_PATTERN_GE_OPERATOR_OID
            | pgrust_catalog_data::TEXT_PATTERN_GT_OPERATOR_OID
    )
}

fn collect_bool_explain_args<'a>(
    expr: &'a Expr,
    boolop: pgrust_nodes::primnodes::BoolExprType,
    out: &mut Vec<&'a Expr>,
) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == boolop => {
            for arg in &bool_expr.args {
                collect_bool_explain_args(arg, boolop, out);
            }
        }
        other => out.push(other),
    }
}

fn render_explain_bool_arg(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    if explain_bool_arg_is_bare(expr) {
        rendered
    } else {
        format!("({rendered})")
    }
}

fn explain_bool_arg_is_bare(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Var(_)
            | Expr::Param(_)
            | Expr::Const(_)
            | Expr::SubPlan(_)
            | Expr::CurrentCatalog
            | Expr::CurrentSchema
            | Expr::CurrentDate
            | Expr::CurrentUser
            | Expr::CurrentRole
            | Expr::SessionUser
            | Expr::Random
    ) || matches!(expr, Expr::Func(func) if !render_explain_func_expr_is_infix(func))
}

pub fn explain_detail_prefix(indent: usize) -> String {
    if indent == 0 {
        "  ".into()
    } else {
        " ".repeat(2 + indent * 6)
    }
}

pub(crate) fn render_explain_projection_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .or_else(|| attrno_index(var.varattno).map(|index| format!("column{}", index + 1)))
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Const(value) => render_explain_projection_const(value),
        Expr::Op(op) => match op.op {
            pgrust_nodes::primnodes::OpExprKind::Add
            | pgrust_nodes::primnodes::OpExprKind::Sub
            | pgrust_nodes::primnodes::OpExprKind::Mul
            | pgrust_nodes::primnodes::OpExprKind::Div
            | pgrust_nodes::primnodes::OpExprKind::Mod
            | pgrust_nodes::primnodes::OpExprKind::BitAnd
            | pgrust_nodes::primnodes::OpExprKind::BitOr
            | pgrust_nodes::primnodes::OpExprKind::BitXor
            | pgrust_nodes::primnodes::OpExprKind::Shl
            | pgrust_nodes::primnodes::OpExprKind::Shr
            | pgrust_nodes::primnodes::OpExprKind::Concat => {
                let [left, right] = op.args.as_slice() else {
                    return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
                };
                let op_text = match op.op {
                    pgrust_nodes::primnodes::OpExprKind::Add => "+",
                    pgrust_nodes::primnodes::OpExprKind::Sub => "-",
                    pgrust_nodes::primnodes::OpExprKind::Mul => "*",
                    pgrust_nodes::primnodes::OpExprKind::Div => "/",
                    pgrust_nodes::primnodes::OpExprKind::Mod => "%",
                    pgrust_nodes::primnodes::OpExprKind::BitAnd => "&",
                    pgrust_nodes::primnodes::OpExprKind::BitOr => "|",
                    pgrust_nodes::primnodes::OpExprKind::BitXor => "#",
                    pgrust_nodes::primnodes::OpExprKind::Shl => "<<",
                    pgrust_nodes::primnodes::OpExprKind::Shr => ">>",
                    pgrust_nodes::primnodes::OpExprKind::Concat => "||",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_projection_expr_inner_with_qualifier(
                        left,
                        qualifier,
                        column_names,
                    ),
                    op_text,
                    render_explain_projection_expr_inner_with_qualifier(
                        right,
                        qualifier,
                        column_names,
                    )
                )
            }
            _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        },
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

fn render_explain_infix_operand(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    if explain_expr_needs_infix_operand_parens(expr) {
        format!("({rendered})")
    } else {
        rendered
    }
}

pub fn render_explain_infix_operand_with_display_type(
    expr: &Expr,
    display_type: Option<SqlType>,
    collation_oid: Option<u32>,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let expr = if display_type.is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Int8)) {
        strip_bigint_comparison_cast(expr)
    } else if display_type.is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Char)) {
        strip_bpchar_display_cast(expr)
    } else if display_type.is_some_and(|ty| matches!(ty.kind, SqlTypeKind::TimestampTz)) {
        strip_timestamp_timestamptz_display_cast(expr)
    } else {
        expr
    };
    let rendered = match (display_type, expr) {
        (Some(sql_type), Expr::Const(value)) => {
            format!(
                "{}::{}",
                render_explain_typed_literal(value, sql_type),
                render_explain_sql_type_name(sql_type.with_typmod(SqlType::NO_TYPEMOD))
            )
        }
        (Some(sql_type), expr) if let Some(value) = render_negative_numeric_literal_text(expr) => {
            format!(
                "'{value}'::{}",
                render_explain_sql_type_name(sql_type.with_typmod(SqlType::NO_TYPEMOD))
            )
        }
        (Some(sql_type), Expr::Cast(inner, _)) if matches!(inner.as_ref(), Expr::Const(_)) => {
            render_explain_infix_operand_with_display_type(
                inner,
                Some(sql_type),
                None,
                qualifier,
                column_names,
            )
        }
        (Some(sql_type), Expr::Cast(inner, _))
            if sql_type.is_array && matches!(inner.as_ref(), Expr::ArrayLiteral { .. }) =>
        {
            render_explain_infix_operand_with_display_type(
                inner,
                Some(sql_type),
                None,
                qualifier,
                column_names,
            )
        }
        (Some(sql_type), Expr::ArrayLiteral { elements, .. }) if sql_type.is_array => {
            render_explain_array_literal(elements, sql_type, qualifier, column_names)
        }
        (Some(sql_type), expr)
            if matches!(sql_type.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(expr, SqlTypeKind::Varchar) =>
        {
            format!(
                "({})::text",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            )
        }
        (Some(sql_type), expr)
            if matches!(sql_type.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(expr, SqlTypeKind::Text) =>
        {
            format!(
                "({})::text",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            )
        }
        _ => render_explain_infix_operand(expr, qualifier, column_names),
    };
    append_explain_collation(rendered, collation_oid)
}

fn explain_expr_needs_infix_operand_parens(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Op(_)
            | Expr::Bool(_)
            | Expr::ScalarArrayOp(_)
            | Expr::Like { .. }
            | Expr::Similar { .. }
    ) || matches!(expr, Expr::Func(func) if render_explain_func_expr_is_infix(func))
}

pub fn comparison_display_type(
    left: &Expr,
    right: &Expr,
    collation_oid: Option<u32>,
) -> Option<SqlType> {
    if expr_has_bpchar_display_type(left) || expr_has_bpchar_display_type(right) {
        Some(SqlType::new(SqlTypeKind::Char))
    } else if expr_has_varchar_display_type(left) || expr_has_varchar_display_type(right) {
        // :HACK: PostgreSQL's EXPLAIN usually shows varchar comparison
        // operators through their text operator implementation. The executable
        // expression still keeps its original types; this only normalizes the
        // displayed qual while pgrust lacks full operator-family display data.
        Some(SqlType::new(SqlTypeKind::Text))
    } else if collation_oid == Some(POSIX_COLLATION_OID)
        && (expr_sql_type_hint_is(left, SqlTypeKind::Text)
            || expr_sql_type_hint_is(right, SqlTypeKind::Text))
    {
        Some(SqlType::new(SqlTypeKind::Text))
    } else if let Some(sql_type) = comparison_cast_literal_display_type(left, right) {
        Some(sql_type)
    } else if let Some(sql_type) = comparison_numeric_literal_display_type(left, right) {
        Some(sql_type)
    } else if let Some(sql_type) = comparison_text_literal_display_type(left, right) {
        Some(sql_type)
    } else if timestamp_timestamptz_comparison_display_type(left, right) {
        Some(SqlType::new(SqlTypeKind::TimestampTz))
    } else {
        None
    }
}

fn comparison_cast_literal_display_type(left: &Expr, right: &Expr) -> Option<SqlType> {
    let sql_type = match (left, right) {
        (Expr::Cast(_, sql_type), Expr::Const(_)) | (Expr::Const(_), Expr::Cast(_, sql_type)) => {
            *sql_type
        }
        _ => return None,
    };
    matches!(
        sql_type.kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
    )
    .then_some(sql_type)
}

fn comparison_numeric_literal_display_type(left: &Expr, right: &Expr) -> Option<SqlType> {
    fn numeric_type_if_literal(literal: &Expr, typed: &Expr) -> Option<SqlType> {
        if !expr_is_negative_numeric_literal(literal) {
            return None;
        }
        let sql_type = match typed {
            Expr::Var(var) => var.vartype,
            other => pgrust_nodes::primnodes::expr_sql_type_hint(other)?,
        };
        matches!(
            sql_type.kind,
            SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
        )
        .then_some(sql_type)
    }

    numeric_type_if_literal(left, right).or_else(|| numeric_type_if_literal(right, left))
}

fn expr_is_negative_numeric_literal(expr: &Expr) -> bool {
    if render_negative_numeric_literal_text(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Const(Value::Int16(value)) => *value < 0,
        Expr::Const(Value::Int32(value)) => *value < 0,
        Expr::Const(Value::Int64(value)) => *value < 0,
        Expr::Const(Value::Numeric(value)) => value.render().starts_with('-'),
        _ => false,
    }
}

fn render_negative_numeric_literal_text(expr: &Expr) -> Option<String> {
    let Expr::Op(op) = expr else {
        return None;
    };
    if op.op != pgrust_nodes::primnodes::OpExprKind::Negate || op.args.len() != 1 {
        return None;
    }
    let Some(Expr::Const(value)) = op.args.first() else {
        return None;
    };
    matches!(
        value,
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) | Value::Numeric(_)
    )
    .then(|| format!("-{}", render_explain_literal(value).trim_matches('\'')))
}

fn comparison_text_literal_display_type(left: &Expr, right: &Expr) -> Option<SqlType> {
    fn interval_type_if_text_literal(literal: &Expr, typed: &Expr) -> Option<SqlType> {
        if !matches!(literal, Expr::Const(Value::Text(_) | Value::TextRef(_, _))) {
            return None;
        }
        let sql_type = pgrust_nodes::primnodes::expr_sql_type_hint(typed)?;
        matches!(sql_type.kind, SqlTypeKind::Interval).then_some(sql_type)
    }

    interval_type_if_text_literal(left, right)
        .or_else(|| interval_type_if_text_literal(right, left))
}

fn timestamp_timestamptz_comparison_display_type(left: &Expr, right: &Expr) -> bool {
    [left, right].iter().any(|expr| {
        matches!(
            expr,
            Expr::Cast(inner, ty)
                if matches!(ty.kind, SqlTypeKind::TimestampTz)
                    && expr_sql_type_hint_is(inner, SqlTypeKind::Timestamp)
        )
    }) && [left, right]
        .iter()
        .any(|expr| expr_sql_type_hint_is(expr, SqlTypeKind::TimestampTz))
}

fn expr_has_bpchar_display_type(expr: &Expr) -> bool {
    if expr_sql_type_hint_is(expr, SqlTypeKind::Char) {
        return true;
    }
    matches!(
        expr,
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            )
    )
}

fn expr_has_varchar_display_type(expr: &Expr) -> bool {
    expr_sql_type_hint_is(expr, SqlTypeKind::Varchar) || expr_is_text_cast_from_varchar(expr)
}

fn expr_is_text_cast_from_varchar(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(inner, SqlTypeKind::Varchar)
    )
}

fn strip_bpchar_to_text(expr: &Expr) -> &Expr {
    match expr {
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            &func.args[0]
        }
        _ => expr,
    }
}

fn strip_bpchar_display_cast(expr: &Expr) -> &Expr {
    match strip_bpchar_to_text(expr) {
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Char)
                && expr_sql_type_hint_is(inner, SqlTypeKind::Char) =>
        {
            inner
        }
        stripped => stripped,
    }
}

fn strip_bigint_comparison_cast(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, sql_type) if matches!(sql_type.kind, SqlTypeKind::Int8) => inner,
        _ => expr,
    }
}

fn strip_timestamp_timestamptz_display_cast(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, sql_type)
            if matches!(sql_type.kind, SqlTypeKind::TimestampTz)
                && expr_sql_type_hint_is(inner, SqlTypeKind::Timestamp) =>
        {
            // :HACK: PostgreSQL's EXPLAIN prints timestamp-vs-timestamptz
            // partition quals as `a < const::timestamptz`, even though the
            // executable expression keeps the stable cast on the timestamp key.
            // Keep this scoped to rendering; volatility still controls pruning.
            inner
        }
        _ => expr,
    }
}

fn expr_sql_type_is_bool(expr: &Expr) -> bool {
    expr_sql_type_hint_is(expr, SqlTypeKind::Bool)
}

fn expr_sql_type_hint_is(expr: &Expr, kind: SqlTypeKind) -> bool {
    pgrust_nodes::primnodes::expr_sql_type_hint(expr)
        .is_some_and(|ty| !ty.is_array && ty.kind == kind)
}

fn append_explain_collation(rendered: String, collation_oid: Option<u32>) -> String {
    let Some(collation_oid) = collation_oid else {
        return rendered;
    };
    let Some(collation) = explain_collation_name(collation_oid) else {
        return rendered;
    };
    format!("{rendered} COLLATE {collation}")
}

fn explain_collation_name(collation_oid: u32) -> Option<&'static str> {
    match collation_oid {
        DEFAULT_COLLATION_OID | 0 => None,
        C_COLLATION_OID => Some("\"C\""),
        POSIX_COLLATION_OID => Some("\"POSIX\""),
        _ => None,
    }
}

fn render_explain_collate(
    expr: &Expr,
    collation_oid: u32,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    append_explain_collation(
        render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        Some(collation_oid),
    )
}

pub fn render_explain_join_expr_inner(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) if var.varno == OUTER_VAR => {
            render_explain_var_name(var, outer_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INNER_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INDEX_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) => {
            let mut combined_names = outer_names.to_vec();
            combined_names.extend_from_slice(inner_names);
            render_explain_var_name(var, &combined_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_join_cast(inner, *ty, outer_names, inner_names),
        Expr::Op(op) => match op.op {
            pgrust_nodes::primnodes::OpExprKind::Eq
            | pgrust_nodes::primnodes::OpExprKind::NotEq
            | pgrust_nodes::primnodes::OpExprKind::Lt
            | pgrust_nodes::primnodes::OpExprKind::LtEq
            | pgrust_nodes::primnodes::OpExprKind::Gt
            | pgrust_nodes::primnodes::OpExprKind::GtEq
            | pgrust_nodes::primnodes::OpExprKind::RegexMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    pgrust_nodes::primnodes::OpExprKind::Eq => "=",
                    pgrust_nodes::primnodes::OpExprKind::NotEq => "<>",
                    pgrust_nodes::primnodes::OpExprKind::Lt => "<",
                    pgrust_nodes::primnodes::OpExprKind::LtEq => "<=",
                    pgrust_nodes::primnodes::OpExprKind::Gt => ">",
                    pgrust_nodes::primnodes::OpExprKind::GtEq => ">=",
                    pgrust_nodes::primnodes::OpExprKind::RegexMatch => "~",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_join_expr_inner(left, outer_names, inner_names),
                    op_text,
                    render_explain_join_expr_inner(right, outer_names, inner_names)
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            pgrust_nodes::primnodes::BoolExprType::And => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    pgrust_nodes::primnodes::BoolExprType::And,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_join_bool_arg(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                rendered.join(" AND ")
            }
            pgrust_nodes::primnodes::BoolExprType::Or => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    pgrust_nodes::primnodes::BoolExprType::Or,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_join_bool_arg(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                rendered.join(" OR ")
            }
            pgrust_nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                let rendered = render_explain_join_expr_inner(inner, outer_names, inner_names);
                if explain_bool_arg_is_bare(inner) {
                    format!("NOT {rendered}")
                } else {
                    format!("NOT ({rendered})")
                }
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_join_expr_inner(left, outer_names, inner_names),
            render_explain_join_expr_inner(right, outer_names, inner_names)
        ),
        Expr::IsNull(inner) => {
            format!(
                "{} IS NULL",
                render_explain_join_expr_inner(inner, outer_names, inner_names)
            )
        }
        Expr::IsNotNull(inner) => format!(
            "{} IS NOT NULL",
            render_explain_join_expr_inner(inner, outer_names, inner_names)
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            ..
        } => render_like_explain_expr(
            expr,
            pattern,
            escape.as_deref(),
            *case_insensitive,
            *negated,
            |expr| render_explain_join_expr_inner(expr, outer_names, inner_names),
        ),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_join_expr_inner(expr, outer_names, inner_names)
        }),
        Expr::Func(func) => render_explain_join_func_expr(func, outer_names, inner_names),
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExistsSubLink => format!("EXISTS(SubPlan {})", subplan.plan_id + 1),
            _ if subplan.renders_as_initplan() => {
                format!("(InitPlan {}).col1", subplan.plan_id + 1)
            }
            _ => format!("(SubPlan {})", subplan.plan_id + 1),
        },
        Expr::Row { fields, .. } => render_explain_join_whole_row(fields, outer_names, inner_names)
            .unwrap_or_else(|| {
                let fields = fields
                    .iter()
                    .map(|(_, expr)| render_explain_join_expr_inner(expr, outer_names, inner_names))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ROW({fields})")
            }),
        Expr::Case(case_expr) => {
            if let Some(rendered) =
                render_explain_join_whole_row_case(case_expr, outer_names, inner_names)
            {
                rendered
            } else {
                format!("{expr:?}")
            }
        }
        other => format!("{other:?}"),
    }
}

fn render_explain_join_whole_row_case(
    case_expr: &pgrust_nodes::primnodes::CaseExpr,
    outer_names: &[String],
    inner_names: &[String],
) -> Option<String> {
    if case_expr.arg.is_some() || case_expr.args.len() != 1 {
        return None;
    }
    if !matches!(case_expr.args[0].result, Expr::Const(Value::Null)) {
        return None;
    }
    let Expr::Row { fields, .. } = case_expr.defresult.as_ref() else {
        return None;
    };
    render_explain_join_whole_row(fields, outer_names, inner_names)
}

fn render_explain_join_whole_row(
    fields: &[(String, Expr)],
    outer_names: &[String],
    inner_names: &[String],
) -> Option<String> {
    let mut prefix = None::<String>;
    for (_, expr) in fields {
        let Expr::Var(var) = expr else {
            return None;
        };
        let names = match var.varno {
            OUTER_VAR => outer_names,
            INNER_VAR | INDEX_VAR => inner_names,
            _ => return None,
        };
        let name = render_explain_var_name(var, names)?;
        let (candidate, _) = name.rsplit_once('.')?;
        match &prefix {
            Some(existing) if existing != candidate => return None,
            Some(_) => {}
            None => prefix = Some(candidate.to_string()),
        }
    }
    prefix.map(|prefix| format!("{prefix}.*"))
}

fn render_explain_join_func_expr(
    func: &FuncExpr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
    ) && func.args.len() == 1
    {
        return render_explain_join_expr_inner(&func.args[0], outer_names, inner_names);
    }
    if render_explain_func_expr_is_infix(func)
        && let Some(operator) = builtin_scalar_function_infix_operator(func.implementation)
        && let [left, right] = func.args.as_slice()
    {
        return format!(
            "{} {} {}",
            render_explain_join_infix_operand(left, outer_names, inner_names),
            operator,
            render_explain_join_infix_operand(right, outer_names, inner_names)
        );
    }
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => builtin_scalar_function_name(builtin),
        ScalarFunctionImpl::UserDefined { proc_oid } => func
            .funcname
            .clone()
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    let args = func
        .args
        .iter()
        .map(|arg| render_explain_join_expr_inner(arg, outer_names, inner_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn render_explain_join_infix_operand(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    let rendered = render_explain_join_expr_inner(expr, outer_names, inner_names);
    if explain_expr_needs_infix_operand_parens(expr) {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn render_explain_join_bool_arg(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    let rendered = render_explain_join_expr_inner(expr, outer_names, inner_names);
    if explain_bool_arg_is_bare(expr) {
        rendered
    } else {
        format!("({rendered})")
    }
}

fn render_like_explain_expr<F>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    case_insensitive: bool,
    negated: bool,
    render: F,
) -> String
where
    F: Fn(&Expr) -> String,
{
    let op = match (case_insensitive, negated) {
        (false, false) => "~~",
        (false, true) => "!~~",
        (true, false) => "~~*",
        (true, true) => "!~~*",
    };
    let mut out = format!("{} {op} {}", render(expr), render(pattern));
    if let Some(escape) = escape {
        out.push_str(" ESCAPE ");
        out.push_str(&render(escape));
    }
    out
}

fn render_similar_explain_expr<F>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    negated: bool,
    render: F,
) -> String
where
    F: Fn(&Expr) -> String,
{
    let left = render(expr);
    if let Some(regex) = explain_similar_regex(pattern, escape) {
        let op = if negated { "!~" } else { "~" };
        return format!(
            "{} {} {}",
            left,
            op,
            render_explain_const(&Value::Text(regex.into()))
        );
    }

    let keyword = if negated {
        "NOT SIMILAR TO"
    } else {
        "SIMILAR TO"
    };
    let mut out = format!("{} {} {}", left, keyword, render(pattern));
    if let Some(escape) = escape {
        out.push_str(" ESCAPE ");
        out.push_str(&render(escape));
    }
    out
}

fn explain_similar_regex(pattern: &Expr, escape: Option<&Expr>) -> Option<String> {
    let Expr::Const(pattern) = pattern else {
        return None;
    };
    let pattern = pattern.as_text()?;
    let escape = match escape {
        None => None,
        Some(Expr::Const(Value::Null)) => return None,
        Some(Expr::Const(value)) => Some(value.as_text()?),
        Some(_) => return None,
    };
    explain_similar_pattern(pattern, escape).ok()
}

pub fn render_explain_const(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'::text", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_)
        | Value::Uuid(_) => match value.sql_type_hint() {
            Some(sql_type) => format!(
                "{}::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(sql_type)
            ),
            None => render_explain_literal(value),
        },
        Value::Date(date) => format!(
            "'{}'::date",
            format_date_text(*date, &postgres_explain_datetime_config())
        ),
        Value::Timestamp(timestamp) => format!(
            "'{}'::timestamp without time zone",
            format_timestamp_text(*timestamp, &current_explain_datetime_config())
        ),
        Value::TimestampTz(timestamp) => format!(
            "'{}'::timestamp with time zone",
            format_timestamptz_text(*timestamp, &current_explain_datetime_config())
        ),
        Value::Interval(interval) => format!(
            "'{}'::interval",
            render_interval_text_with_config(*interval, &current_explain_datetime_config())
                .replace('\'', "''")
        ),
        Value::Inet(_) | Value::Cidr(_) => match value.sql_type_hint() {
            Some(sql_type) => format!(
                "{}::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(sql_type)
            ),
            None => render_explain_literal(value),
        },
        Value::PgArray(array) => {
            let rendered = if array
                .elements
                .iter()
                .any(|item| matches!(item, Value::Jsonb(_)))
            {
                render_explain_array_items(&array.elements)
            } else {
                format_array_value_text_with_config(array, &postgres_explain_datetime_config())
            };
            match value.sql_type_hint() {
                Some(sql_type) => {
                    format!("'{rendered}'::{}", render_explain_sql_type_name(sql_type))
                }
                None => format!("'{rendered}'"),
            }
        }
        Value::Array(items) => {
            let rendered = if items.iter().any(|item| matches!(item, Value::Jsonb(_))) {
                render_explain_array_items(items)
            } else {
                let array = ArrayValue::from_1d(items.clone());
                format_array_value_text_with_config(&array, &postgres_explain_datetime_config())
            };
            let escaped = rendered.replace('\'', "''");
            let array_type = items
                .iter()
                .find_map(Value::sql_type_hint)
                .map(SqlType::array_of);
            match array_type {
                Some(sql_type) => {
                    format!("'{escaped}'::{}", render_explain_sql_type_name(sql_type))
                }
                None => format!("'{escaped}'"),
            }
        }
        Value::Jsonb(bytes) => {
            let rendered = render_jsonb_bytes(bytes)
                .unwrap_or_else(|_| "null".into())
                .replace('\'', "''");
            format!("'{rendered}'::jsonb")
        }
        Value::JsonPath(path) => {
            let rendered = path.to_string().replace('\'', "''");
            format!("'{rendered}'::jsonpath")
        }
        Value::Record(record) => {
            let rendered = format_record_text(record).replace('\'', "''");
            let record_type = record.sql_type();
            if record_type.type_oid != pgrust_catalog_data::RECORD_TYPE_OID {
                format!(
                    "'{rendered}'::{}",
                    render_explain_sql_type_name(record_type)
                )
            } else {
                format!("'{rendered}'::record")
            }
        }
        Value::Range(_) | Value::Multirange(_) | Value::TsQuery(_) | Value::TsVector(_) => {
            match value.sql_type_hint() {
                Some(sql_type) => format!(
                    "{}::{}",
                    render_explain_literal(value),
                    render_explain_sql_type_name(sql_type)
                ),
                None => render_explain_literal(value),
            }
        }
        Value::Tid(tid) => {
            format!("'{}'::tid", render_tid_text(tid))
        }
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_array_items(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Jsonb(bytes) => {
                let rendered = render_jsonb_bytes(bytes).unwrap_or_else(|_| "null".into());
                push_explain_array_quoted_element(&mut out, &rendered);
            }
            other => out.push_str(&render_explain_const(other)),
        }
    }
    out.push('}');
    out
}

fn push_explain_array_quoted_element(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn render_explain_projection_const(value: &Value) -> String {
    match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        _ => render_explain_const(value),
    }
}

fn render_explain_cast(
    expr: &Expr,
    ty: SqlType,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Expr::Var(var) = expr
        && explain_cast_is_implicit_integer_widening(var.vartype, ty)
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if let Expr::Cast(inner, inner_ty) = expr
        && inner_ty.with_typmod(SqlType::NO_TYPEMOD) == ty.with_typmod(SqlType::NO_TYPEMOD)
    {
        return render_explain_cast(inner, ty, qualifier, column_names);
    }
    if let Expr::ArrayLiteral { array_type, .. } = expr
        && array_type.with_typmod(SqlType::NO_TYPEMOD) == ty.with_typmod(SqlType::NO_TYPEMOD)
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if let Some(rendered) = render_explain_datetime_cast_literal(expr, ty) {
        return rendered;
    }
    if let Some(rendered) = render_explain_array_cast_literal(expr, ty) {
        return rendered;
    }
    if let Expr::Const(value) = expr {
        if matches!(ty.kind, SqlTypeKind::Oid) {
            return format!("'{}'::oid", render_explain_literal(value));
        }
        if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
            return format!(
                "'{}'::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(ty)
            );
        }
        if matches!(ty.kind, SqlTypeKind::Numeric) {
            return format!(
                "{}::{}",
                render_explain_typed_literal(value, ty),
                render_explain_sql_type_name(ty)
            );
        }
        if matches!(
            ty.kind,
            SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
        ) {
            return format!(
                "{}::{}",
                render_explain_typed_literal(value, ty),
                render_explain_sql_type_name(ty)
            );
        }
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

fn explain_cast_is_implicit_integer_widening(from: SqlType, to: SqlType) -> bool {
    use SqlTypeKind::{Int2, Int4, Int8};
    matches!(
        (from.kind, to.kind),
        (Int2, Int4) | (Int2, Int8) | (Int4, Int8)
    )
}

fn render_explain_array_cast_literal(expr: &Expr, ty: SqlType) -> Option<String> {
    if !ty.is_array {
        return None;
    }
    let Expr::Const(value) = expr else {
        return None;
    };
    let element_type = ty.element_type();
    let array = match value {
        Value::Array(items) => {
            let elements = items
                .iter()
                .map(|item| cast_value(item.clone(), element_type).unwrap_or_else(|_| item.clone()))
                .collect::<Vec<_>>();
            ArrayValue::from_1d(elements)
        }
        Value::PgArray(array) => {
            let elements = array
                .elements
                .iter()
                .map(|item| cast_value(item.clone(), element_type).unwrap_or_else(|_| item.clone()))
                .collect::<Vec<_>>();
            ArrayValue::from_dimensions(array.dimensions.clone(), elements)
        }
        _ => return None,
    };
    let rendered = format_array_value_text_with_config(&array, &postgres_explain_datetime_config());
    Some(format!(
        "'{}'::{}",
        rendered.replace('\'', "''"),
        render_explain_sql_type_name(ty)
    ))
}

pub fn render_explain_datetime_cast_literal(expr: &Expr, ty: SqlType) -> Option<String> {
    let Expr::Const(value) = expr else {
        return None;
    };
    let text = value.as_text()?;
    match ty.kind {
        SqlTypeKind::Date => {
            let config = current_explain_datetime_config();
            parse_date_text(text, &config).ok().map(|date| {
                format!(
                    "'{}'::date",
                    format_date_text(date, &config).replace('\'', "''")
                )
            })
        }
        SqlTypeKind::Timestamp => {
            let config = current_explain_datetime_config();
            parse_timestamp_text(text, &config).ok().map(|timestamp| {
                format!(
                    "'{}'::timestamp without time zone",
                    format_timestamp_text(timestamp, &config).replace('\'', "''")
                )
            })
        }
        SqlTypeKind::TimestampTz => {
            let config = current_explain_datetime_config();
            parse_timestamptz_text(text, &config).ok().map(|timestamp| {
                format!(
                    "'{}'::timestamp with time zone",
                    format_timestamptz_text(timestamp, &config).replace('\'', "''")
                )
            })
        }
        SqlTypeKind::Interval => {
            let config = current_explain_datetime_config();
            parse_interval_text_value_with_style(text, config.interval_style)
                .ok()
                .map(|interval| {
                    format!(
                        "'{}'::interval",
                        render_interval_text_with_config(interval, &config).replace('\'', "''")
                    )
                })
        }
        _ => None,
    }
}

fn postgres_utc_datetime_config() -> DateTimeConfig {
    let mut config = DateTimeConfig::default();
    config.date_style_format = DateStyleFormat::Postgres;
    config.date_order = DateOrder::Mdy;
    config.time_zone = "UTC".into();
    config
}

fn render_explain_join_cast(
    expr: &Expr,
    ty: SqlType,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    if let Expr::Const(value) = expr {
        if matches!(ty.kind, SqlTypeKind::Oid) {
            return format!("'{}'::oid", render_explain_literal(value));
        }
        if matches!(
            ty.kind,
            SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
        ) {
            return format!(
                "{}::{}",
                render_explain_typed_literal(value, ty),
                render_explain_sql_type_name(ty)
            );
        }
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_join_expr_inner(expr, outer_names, inner_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

pub fn render_explain_literal(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            let rendered = render_geometry_text(value, FloatFormatOptions::default())
                .unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::Uuid(uuid) => {
            format!("'{}'", render_uuid_text(uuid))
        }
        Value::Range(_) => {
            let rendered = render_range_text(value).unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::Multirange(_) => {
            let rendered = render_multirange_text(value).unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::TsQuery(query) => {
            let rendered = render_tsquery_text(query);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::TsVector(vector) => {
            let rendered = render_tsvector_text(vector);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::JsonPath(path) => format!("'{}'", path.replace('\'', "''")),
        Value::PgArray(array) => {
            let rendered =
                format_array_value_text_with_config(array, &postgres_explain_datetime_config());
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::Array(items) => {
            let array = ArrayValue::from_1d(items.clone());
            let rendered =
                format_array_value_text_with_config(&array, &postgres_explain_datetime_config());
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::Record(record) => {
            let rendered = format_record_text(record);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::Date(date) => {
            format!(
                "'{}'",
                format_date_text(*date, &current_explain_datetime_config())
            )
        }
        Value::Timestamp(timestamp) => {
            format!(
                "'{}'",
                format_timestamp_text(*timestamp, &postgres_utc_datetime_config())
            )
        }
        Value::TimestampTz(timestamp) => {
            format!(
                "'{}'",
                format_timestamptz_text(*timestamp, &postgres_utc_datetime_config())
            )
        }
        Value::Interval(interval) => {
            let config = current_explain_datetime_config();
            format!(
                "'{}'",
                render_interval_text_with_config(*interval, &config).replace('\'', "''")
            )
        }
        Value::Inet(value) => format!("'{}'", value.render_inet()),
        Value::Cidr(value) => format!("'{}'", value.render_cidr()),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

pub fn render_explain_typed_literal(value: &Value, sql_type: SqlType) -> String {
    match sql_type.kind {
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric => {
            format!("'{}'", render_explain_literal(value).trim_matches('\''))
        }
        SqlTypeKind::Timestamp => render_explain_timestamp_typed_literal(value, false)
            .unwrap_or_else(|| render_explain_literal(value)),
        SqlTypeKind::TimestampTz => render_explain_timestamp_typed_literal(value, true)
            .unwrap_or_else(|| render_explain_literal(value)),
        SqlTypeKind::Interval => render_explain_interval_typed_literal(value)
            .unwrap_or_else(|| render_explain_literal(value)),
        _ => render_explain_literal(value),
    }
}

fn render_explain_timestamp_typed_literal(value: &Value, timestamptz: bool) -> Option<String> {
    let config = current_explain_datetime_config();
    if timestamptz {
        let timestamp = match value {
            Value::TimestampTz(timestamp) => Some(*timestamp),
            _ => value
                .as_text()
                .and_then(|text| parse_timestamptz_text(text, &config).ok()),
        }?;
        return Some(format!(
            "'{}'",
            format_timestamptz_text(timestamp, &config).replace('\'', "''")
        ));
    }
    let timestamp = match value {
        Value::Timestamp(timestamp) => Some(*timestamp),
        _ => value
            .as_text()
            .and_then(|text| parse_timestamp_text(text, &config).ok()),
    }?;
    Some(format!(
        "'{}'",
        format_timestamp_text(timestamp, &config).replace('\'', "''")
    ))
}

fn render_explain_interval_typed_literal(value: &Value) -> Option<String> {
    let config = current_explain_datetime_config();
    let interval = match value {
        Value::Interval(interval) => Some(*interval),
        _ => value.as_text().and_then(|text| {
            parse_interval_text_value_with_style(text, config.interval_style).ok()
        }),
    }?;
    Some(format!(
        "'{}'",
        render_interval_text_with_config(interval, &config).replace('\'', "''")
    ))
}

fn render_explain_array_literal(
    elements: &[Expr],
    array_type: SqlType,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let element_type = array_type.element_type();
    let const_elements = elements
        .iter()
        .map(|expr| render_explain_array_literal_const(expr, element_type))
        .collect::<Option<Vec<_>>>();
    if let Some(elements) = const_elements {
        return format!(
            "'{{{}}}'::{}",
            elements.join(","),
            render_explain_sql_type_name(array_type)
        );
    }
    let elements = elements
        .iter()
        .map(|expr| render_explain_expr_inner_with_qualifier(expr, qualifier, column_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "ARRAY[{elements}]::{}",
        render_explain_sql_type_name(array_type)
    )
}

fn render_explain_array_literal_const(expr: &Expr, element_type: SqlType) -> Option<String> {
    match expr {
        Expr::Const(value) => Some(render_explain_array_literal_value(value, element_type)),
        Expr::Cast(inner, _) => render_explain_array_literal_const(inner, element_type),
        _ => None,
    }
}

fn render_explain_array_literal_value(value: &Value, element_type: SqlType) -> String {
    let value = cast_value(value.clone(), element_type).unwrap_or_else(|_| value.clone());
    match &value {
        Value::Record(record) => {
            let rendered = format_record_text(record)
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!("\"{rendered}\"")
        }
        Value::Text(_) | Value::TextRef(_, _)
            if matches!(
                element_type.kind,
                SqlTypeKind::Composite | SqlTypeKind::Record
            ) =>
        {
            let rendered = value
                .as_text()
                .unwrap_or_default()
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!("\"{rendered}\"")
        }
        Value::Text(_) | Value::TextRef(_, _) => {
            let rendered = value
                .as_text()
                .unwrap_or_default()
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            if rendered
                .chars()
                .any(|ch| ch.is_ascii_whitespace() || matches!(ch, '{' | '}'))
            {
                format!("\"{rendered}\"")
            } else {
                rendered.replace(',', "\\,")
            }
        }
        Value::Bool(value) => {
            if *value {
                "t".into()
            } else {
                "f".into()
            }
        }
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Timestamp(timestamp) => render_explain_array_quoted_value(&format_timestamp_text(
            *timestamp,
            &postgres_explain_datetime_config(),
        )),
        Value::TimestampTz(timestamp) => render_explain_array_quoted_value(
            &format_timestamptz_text(*timestamp, &postgres_explain_datetime_config()),
        ),
        Value::Jsonb(bytes) => {
            let rendered = render_jsonb_bytes(bytes)
                .unwrap_or_else(|_| "null".into())
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\'', "''");
            format!("\"{rendered}\"")
        }
        Value::Tid(tid) => {
            format!("\"{}\"", render_tid_text(tid))
        }
        Value::Null => "NULL".into(),
        _ => render_explain_literal(&value),
    }
}

fn render_explain_array_quoted_value(text: &str) -> String {
    let mut out = String::new();
    push_explain_array_quoted_element(&mut out, text);
    out
}

fn render_explain_row_comparison_operand(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    match expr {
        Expr::Row { fields, .. } => Some(format!(
            "ROW({})",
            fields
                .iter()
                .map(|(_, expr)| render_explain_expr_inner_with_qualifier(
                    expr,
                    qualifier,
                    column_names
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Expr::Const(Value::Record(record)) => Some(format!(
            "ROW({})",
            record
                .fields
                .iter()
                .map(|value| render_explain_expr_inner(&Expr::Const(value.clone()), &[]))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Composite | SqlTypeKind::Record) =>
        {
            render_explain_row_comparison_operand(inner, qualifier, column_names)
        }
        _ => None,
    }
}

fn render_explain_whole_row_case(
    case_expr: &CaseExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    if case_expr.arg.is_some() || case_expr.args.len() != 1 {
        return None;
    }
    if !matches!(&case_expr.args.first()?.result, Expr::Const(Value::Null)) {
        return None;
    }
    let Expr::Row { fields, .. } = case_expr.defresult.as_ref() else {
        return None;
    };
    let rendered_fields = fields
        .iter()
        .map(|(_, expr)| {
            let Expr::Var(var) = expr else {
                return None;
            };
            render_explain_var_name(var, column_names)
        })
        .collect::<Option<Vec<_>>>()?;
    if rendered_fields.is_empty() {
        return None;
    }
    if let Some(qualifier) = qualifier {
        return Some(format!("{qualifier}.*"));
    }
    let prefix = rendered_fields
        .iter()
        .filter_map(|name| name.rsplit_once('.').map(|(prefix, _)| prefix))
        .next()?;
    rendered_fields
        .iter()
        .all(|name| {
            name.rsplit_once('.')
                .is_some_and(|(candidate, _)| candidate == prefix)
        })
        .then(|| format!("{prefix}.*"))
}

pub fn render_explain_sql_type_name(ty: SqlType) -> String {
    let element = ty.element_type();
    let base = match element.kind {
        SqlTypeKind::Bool => "boolean".into(),
        SqlTypeKind::Int2 => "smallint".into(),
        SqlTypeKind::Int4 => "integer".into(),
        SqlTypeKind::Int8 => "bigint".into(),
        SqlTypeKind::Float4 => "real".into(),
        SqlTypeKind::Float8 => "double precision".into(),
        SqlTypeKind::Numeric => element
            .numeric_precision_scale()
            .map(|(precision, scale)| format!("numeric({precision},{scale})"))
            .unwrap_or_else(|| "numeric".into()),
        SqlTypeKind::Text => "text".into(),
        SqlTypeKind::Name => "name".into(),
        SqlTypeKind::Oid => "oid".into(),
        SqlTypeKind::Inet => "inet".into(),
        SqlTypeKind::Cidr => "cidr".into(),
        SqlTypeKind::Date => "date".into(),
        SqlTypeKind::Time => "time without time zone".into(),
        SqlTypeKind::TimeTz => "time with time zone".into(),
        SqlTypeKind::Timestamp => "timestamp without time zone".into(),
        SqlTypeKind::TimestampTz => "timestamp with time zone".into(),
        SqlTypeKind::Char => element
            .char_len()
            .map(|len| format!("character({len})"))
            .unwrap_or_else(|| "bpchar".into()),
        SqlTypeKind::Varchar => element
            .char_len()
            .map(|len| format!("character varying({len})"))
            .unwrap_or_else(|| "character varying".into()),
        SqlTypeKind::Json => "json".into(),
        SqlTypeKind::Jsonb => "jsonb".into(),
        SqlTypeKind::JsonPath => "jsonpath".into(),
        SqlTypeKind::TsQuery => "tsquery".into(),
        SqlTypeKind::TsVector => "tsvector".into(),
        SqlTypeKind::Line => "line".into(),
        SqlTypeKind::Lseg => "lseg".into(),
        SqlTypeKind::Path => "path".into(),
        SqlTypeKind::Box => "box".into(),
        SqlTypeKind::Polygon => "polygon".into(),
        SqlTypeKind::Circle => "circle".into(),
        SqlTypeKind::Point => "point".into(),
        SqlTypeKind::Uuid => "uuid".into(),
        SqlTypeKind::Range => match element.type_oid {
            pgrust_catalog_data::INT4RANGE_TYPE_OID => "int4range".into(),
            pgrust_catalog_data::INT8RANGE_TYPE_OID => "int8range".into(),
            pgrust_catalog_data::NUMRANGE_TYPE_OID => "numrange".into(),
            pgrust_catalog_data::DATERANGE_TYPE_OID => "daterange".into(),
            pgrust_catalog_data::TSRANGE_TYPE_OID => "tsrange".into(),
            pgrust_catalog_data::TSTZRANGE_TYPE_OID => "tstzrange".into(),
            _ => "text".into(),
        },
        SqlTypeKind::Int4Range => "int4range".into(),
        SqlTypeKind::Int8Range => "int8range".into(),
        SqlTypeKind::NumericRange => "numrange".into(),
        SqlTypeKind::DateRange => "daterange".into(),
        SqlTypeKind::TimestampRange => "tsrange".into(),
        SqlTypeKind::TimestampTzRange => "tstzrange".into(),
        SqlTypeKind::Int2Vector => "int2vector".into(),
        SqlTypeKind::OidVector => "oidvector".into(),
        _ => "text".into(),
    };
    if ty.is_array {
        format!("{base}[]")
    } else {
        base
    }
}
