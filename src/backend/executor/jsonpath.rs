use std::cmp::Ordering;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};

use crate::backend::executor::ExecError;
use crate::backend::executor::expr_bool::parse_pg_bool_text;
use crate::backend::executor::expr_casts::parse_pg_float;
use crate::backend::executor::expr_ops::parse_numeric_text;
use crate::backend::executor::jsonb::{JsonbValue, compare_jsonb};
use crate::backend::executor::pg_regex::{
    eval_jsonpath_like_regex, validate_jsonpath_like_regex,
};
use crate::include::nodes::datum::NumericValue;
use crate::include::nodes::parsenodes::SqlTypeKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathMode {
    Lax,
    Strict,
}

#[derive(Debug, Clone)]
pub(crate) struct JsonPath {
    pub(crate) mode: PathMode,
    expr: Expr,
}

#[derive(Debug, Clone)]
enum Expr {
    Path {
        base: Base,
        steps: Vec<Step>,
    },
    Literal(JsonbValue),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    StartsWith {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    LikeRegex {
        expr: Box<Expr>,
        pattern: String,
        flags: String,
    },
    Arithmetic {
        op: ArithmeticOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        inner: Box<Expr>,
    },
    MethodCall {
        inner: Box<Expr>,
        method: Method,
    },
    Exists(Box<Expr>),
    Last,
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsUnknown(Box<Expr>),
}

#[derive(Debug, Clone)]
enum Base {
    Root,
    Current,
    Var(String),
}

#[derive(Debug, Clone)]
enum Step {
    Member(String),
    MemberWildcard,
    Recursive {
        min_depth: RecursiveBound,
        max_depth: RecursiveBound,
    },
    Subscripts(Vec<SubscriptSelection>),
    IndexWildcard,
    Method(Method),
    Filter(Box<Expr>),
}

#[derive(Debug, Clone, Copy)]
enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy)]
enum UnaryOp {
    Plus,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecursiveBound {
    Int(i32),
    Last,
}

#[derive(Debug, Clone)]
enum SubscriptExpr {
    Expr(Box<Expr>),
    Filter {
        expr: Box<Expr>,
        predicate: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
enum SubscriptSelection {
    Index(SubscriptExpr),
    Range(Expr, Expr),
}

#[derive(Debug, Clone, Copy)]
enum MethodKind {
    Abs,
    Boolean,
    Ceiling,
    Decimal,
    Double,
    Floor,
    Integer,
    Number,
    Size,
    String,
    Type,
}

#[derive(Debug, Clone)]
struct Method {
    kind: MethodKind,
    args: Vec<NumericValue>,
}

#[derive(Debug, Clone, Copy)]
enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateValue {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone)]
pub(crate) struct EvaluationContext<'a> {
    pub(crate) root: &'a JsonbValue,
    pub(crate) vars: Option<&'a JsonbValue>,
}

#[derive(Debug, Clone)]
struct RuntimeContext<'a> {
    global: &'a EvaluationContext<'a>,
    current: &'a JsonbValue,
    mode: PathMode,
    last_index: Option<i32>,
}

pub(crate) fn validate_jsonpath(text: &str) -> Result<(), ExecError> {
    parse_jsonpath(text).map(|_| ())
}

pub(crate) fn canonicalize_jsonpath(text: &str) -> Result<String, ExecError> {
    let parsed = parse_jsonpath(text)?;
    Ok(render_jsonpath(&parsed))
}

pub(crate) fn parse_jsonpath(text: &str) -> Result<JsonPath, ExecError> {
    Parser::new(text).parse()
}

pub(crate) fn evaluate_jsonpath(
    path: &JsonPath,
    ctx: &EvaluationContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let runtime = RuntimeContext {
        global: ctx,
        current: ctx.root,
        mode: path.mode,
        last_index: None,
    };
    eval_expr(&path.expr, &runtime)
}

fn eval_expr(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<Vec<JsonbValue>, ExecError> {
    match expr {
        Expr::Literal(value) => Ok(vec![value.clone()]),
        Expr::Last => Ok(ctx
            .last_index
            .map(numeric_jsonb_from_i32)
            .into_iter()
            .collect()),
        Expr::Path { base, steps } => {
            let mut values = match base {
                Base::Root => vec![ctx.global.root.clone()],
                Base::Current => vec![ctx.current.clone()],
                Base::Var(name) => vec![lookup_var(ctx, name)?.clone()],
            };
            for step in steps {
                values = apply_step(values, step, ctx)?;
            }
            Ok(values)
        }
        Expr::Compare { .. }
        | Expr::StartsWith { .. }
        | Expr::LikeRegex { .. }
        | Expr::And(..)
        | Expr::Or(..)
        | Expr::Not(..)
        | Expr::IsUnknown(..) => {
            Ok(vec![predicate_value_to_jsonb(eval_predicate(expr, ctx)?)])
        }
        Expr::Exists(..) => Ok(vec![predicate_value_to_jsonb(eval_predicate(expr, ctx)?)]),
        Expr::Arithmetic { op, left, right } => {
            let left_values = eval_expr(left, ctx)?;
            let right_values = eval_expr(right, ctx)?;
            eval_arithmetic_any_pair(&left_values, &right_values, *op)
        }
        Expr::MethodCall { inner, method } => eval_expr(inner, ctx)?
            .into_iter()
            .map(|value| apply_method(&value, method, ctx.mode))
            .collect(),
        Expr::Unary { op, inner } => {
            let values = eval_expr(inner, ctx)?;
            values
                .into_iter()
                .map(|value| eval_unary_value(value, *op))
                .collect()
        }
    }
}

fn eval_predicate(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<PredicateValue, ExecError> {
    match expr {
        Expr::Exists(inner) => Ok(match eval_expr(inner, ctx) {
            Ok(values) if values.is_empty() => PredicateValue::False,
            Ok(_) => PredicateValue::True,
            Err(_) => PredicateValue::Unknown,
        }),
        Expr::Compare { op, left, right } => {
            let left_values = match eval_expr(left, ctx) {
                Ok(values) => values,
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            let right_values = match eval_expr(right, ctx) {
                Ok(values) => values,
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            Ok(compare_any_pair(&left_values, &right_values, *op, ctx.mode))
        }
        Expr::StartsWith { left, right } => {
            let left_values = match eval_expr(left, ctx) {
                Ok(values) => values,
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            let right_values = match eval_expr(right, ctx) {
                Ok(values) => values,
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            Ok(starts_with_any_pair(&left_values, &right_values))
        }
        Expr::LikeRegex {
            expr,
            pattern,
            flags,
        } => {
            let values = match eval_expr(expr, ctx) {
                Ok(values) => values,
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            like_regex_any(&values, pattern, flags)
        }
        Expr::And(left, right) => {
            let left_value = eval_predicate(left, ctx)?;
            if left_value == PredicateValue::False {
                return Ok(PredicateValue::False);
            }
            let right_value = eval_predicate(right, ctx)?;
            Ok(if right_value == PredicateValue::True {
                left_value
            } else {
                right_value
            })
        }
        Expr::Or(left, right) => {
            let left_value = eval_predicate(left, ctx)?;
            if left_value == PredicateValue::True {
                return Ok(PredicateValue::True);
            }
            let right_value = eval_predicate(right, ctx)?;
            Ok(if right_value == PredicateValue::False {
                left_value
            } else {
                right_value
            })
        }
        Expr::Not(inner) => Ok(match eval_predicate(inner, ctx)? {
            PredicateValue::True => PredicateValue::False,
            PredicateValue::False => PredicateValue::True,
            PredicateValue::Unknown => PredicateValue::Unknown,
        }),
        Expr::IsUnknown(inner) => Ok(if eval_predicate(inner, ctx)? == PredicateValue::Unknown {
            PredicateValue::True
        } else {
            PredicateValue::False
        }),
        _ => predicate_value_from_items(expr, ctx),
    }
}

fn predicate_value_from_items(
    expr: &Expr,
    ctx: &RuntimeContext<'_>,
) -> Result<PredicateValue, ExecError> {
    let values = match eval_expr(expr, ctx) {
        Ok(values) => values,
        Err(_) => return Ok(PredicateValue::Unknown),
    };
    if values.is_empty() {
        return Ok(PredicateValue::False);
    }
    if values.len() != 1 {
        return Err(exec_jsonpath_error(
            "predicate expression must return one item",
        ));
    }
    match &values[0] {
        JsonbValue::Bool(true) => Ok(PredicateValue::True),
        JsonbValue::Bool(false) => Ok(PredicateValue::False),
        JsonbValue::Null => Ok(PredicateValue::Unknown),
        _ => Err(exec_jsonpath_error(
            "predicate expression must return boolean",
        )),
    }
}

fn predicate_value_to_jsonb(value: PredicateValue) -> JsonbValue {
    match value {
        PredicateValue::True => JsonbValue::Bool(true),
        PredicateValue::False => JsonbValue::Bool(false),
        PredicateValue::Unknown => JsonbValue::Null,
    }
}

fn lookup_var<'a>(ctx: &'a RuntimeContext<'_>, name: &str) -> Result<&'a JsonbValue, ExecError> {
    let Some(JsonbValue::Object(items)) = ctx.global.vars else {
        return Err(exec_jsonpath_error(
            "jsonpath variables must be a jsonb object",
        ));
    };
    items
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
        .ok_or_else(|| exec_jsonpath_error(&format!("jsonpath variable \"{name}\" not found")))
}

fn apply_step(
    values: Vec<JsonbValue>,
    step: &Step,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let mut out = Vec::new();
    for value in values {
        apply_step_single(&value, step, ctx, &mut out)?;
    }
    Ok(out)
}

fn apply_step_single(
    value: &JsonbValue,
    step: &Step,
    ctx: &RuntimeContext<'_>,
    out: &mut Vec<JsonbValue>,
) -> Result<(), ExecError> {
    match step {
        Step::Member(name) => match value {
            JsonbValue::Object(items) => {
                if let Some((_, found)) = items.iter().find(|(key, _)| key == name) {
                    out.push(found.clone());
                } else if matches!(ctx.mode, PathMode::Strict) {
                    return Err(exec_jsonpath_error("jsonpath member not found"));
                }
            }
            JsonbValue::Array(items) if matches!(ctx.mode, PathMode::Lax) => {
                for item in items {
                    apply_step_single(item, step, ctx, out)?;
                }
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath member access requires object",
                ));
            }
            _ => {}
        },
        Step::MemberWildcard => match value {
            JsonbValue::Object(items) => out.extend(items.iter().map(|(_, item)| item.clone())),
            JsonbValue::Array(items) if matches!(ctx.mode, PathMode::Lax) => {
                for item in items {
                    apply_step_single(item, step, ctx, out)?;
                }
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath wildcard member access requires object",
                ));
            }
            _ => {}
        },
        Step::Recursive {
            min_depth,
            max_depth,
        } => {
            let min_depth = resolve_recursive_bound(value, *min_depth);
            let max_depth = resolve_recursive_bound(value, *max_depth);
            collect_recursive_values(value, min_depth, max_depth, 0, out);
        }
        Step::Subscripts(selections) => match value {
            JsonbValue::Array(items) => {
                apply_subscript_selections(value, items, selections, ctx, out)?;
            }
            _ if matches!(ctx.mode, PathMode::Lax) => {
                apply_scalar_subscript_selections(value, selections, ctx, out)?;
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath array subscript requires array",
                ));
            }
            _ => {}
        },
        Step::IndexWildcard => match value {
            JsonbValue::Array(items) => out.extend(items.iter().cloned()),
            _ if matches!(ctx.mode, PathMode::Lax) => out.push(value.clone()),
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath array wildcard requires array",
                ));
            }
            _ => {}
        },
        Step::Method(kind) => out.push(apply_method(value, kind, ctx.mode)?),
        Step::Filter(expr) => match value {
            JsonbValue::Array(items) if matches!(ctx.mode, PathMode::Lax) => {
                for item in items {
                    let nested = RuntimeContext {
                        global: ctx.global,
                        current: item,
                        mode: ctx.mode,
                        last_index: ctx.last_index,
                    };
                    if eval_predicate(expr, &nested)? == PredicateValue::True {
                        out.push(item.clone());
                    }
                }
            }
            _ => {
                let nested = RuntimeContext {
                    global: ctx.global,
                    current: value,
                    mode: ctx.mode,
                    last_index: ctx.last_index,
                };
                if eval_predicate(expr, &nested)? == PredicateValue::True {
                    out.push(value.clone());
                }
            }
        },
    }
    Ok(())
}

fn array_index(items: &[JsonbValue], index: i32) -> Option<&JsonbValue> {
    let len = items.len() as i32;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 {
        None
    } else {
        items.get(normalized as usize)
    }
}

fn apply_subscript_selections(
    value: &JsonbValue,
    items: &[JsonbValue],
    selections: &[SubscriptSelection],
    ctx: &RuntimeContext<'_>,
    out: &mut Vec<JsonbValue>,
) -> Result<(), ExecError> {
    let subscript_ctx = RuntimeContext {
        global: ctx.global,
        current: value,
        mode: ctx.mode,
        last_index: items
            .len()
            .checked_sub(1)
            .and_then(|last| i32::try_from(last).ok()),
    };
    let mut matched = false;
    let mut had_range = false;
    for selection in selections {
        match selection {
            SubscriptSelection::Index(expr) => match resolve_subscript_expr(expr, &subscript_ctx)? {
                Some(index) => {
                    if let Some(found) = array_index(items, index) {
                        out.push(found.clone());
                        matched = true;
                    } else if matches!(ctx.mode, PathMode::Strict) {
                        return Err(exec_jsonpath_error("jsonpath array subscript is out of bounds"));
                    }
                }
                None => {
                    if matches!(ctx.mode, PathMode::Strict) {
                        return Err(exec_jsonpath_error("jsonpath array subscript is out of bounds"));
                    }
                }
            },
            SubscriptSelection::Range(start, end) => {
                had_range = true;
                let start = resolve_bound_expr(start, &subscript_ctx)?;
                let end = resolve_bound_expr(end, &subscript_ctx)?;
                match (start, end) {
                    (Some(start), Some(end)) => {
                        for index in start..=end {
                            if let Some(found) = array_index(items, index) {
                                out.push(found.clone());
                                matched = true;
                            }
                        }
                    }
                    _ if matches!(ctx.mode, PathMode::Strict) => {
                        return Err(exec_jsonpath_error("jsonpath array subscript is out of bounds"));
                    }
                    _ => {}
                }
            }
        }
    }
    if had_range && !matched && matches!(ctx.mode, PathMode::Strict) {
        return Err(exec_jsonpath_error("jsonpath array range is out of bounds"));
    }
    Ok(())
}

fn apply_scalar_subscript_selections(
    value: &JsonbValue,
    selections: &[SubscriptSelection],
    ctx: &RuntimeContext<'_>,
    out: &mut Vec<JsonbValue>,
) -> Result<(), ExecError> {
    let subscript_ctx = RuntimeContext {
        global: ctx.global,
        current: value,
        mode: ctx.mode,
        last_index: Some(0),
    };
    for selection in selections {
        match selection {
            SubscriptSelection::Index(expr) => {
                if matches!(resolve_subscript_expr(expr, &subscript_ctx)?, Some(0) | Some(-1)) {
                    out.push(value.clone());
                }
            }
            SubscriptSelection::Range(start, end) => {
                let start = resolve_bound_expr(start, &subscript_ctx)?;
                let end = resolve_bound_expr(end, &subscript_ctx)?;
                if let (Some(start), Some(end)) = (start, end) {
                    if (start..=end).any(|index| index == 0 || index == -1) {
                        out.push(value.clone());
                    }
                }
            }
        }
    }
    Ok(())
}

fn apply_method(value: &JsonbValue, method: &Method, mode: PathMode) -> Result<JsonbValue, ExecError> {
    match method.kind {
        MethodKind::Abs => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric.abs())),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .abs() can only be applied to a numeric value",
            )),
        },
        MethodKind::Boolean => apply_boolean_method(value),
        MethodKind::Ceiling => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric_ceiling(numeric))),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .ceiling() can only be applied to a numeric value",
            )),
        },
        MethodKind::Decimal => apply_decimal_method(value, &method.args),
        MethodKind::Double => apply_double_method(value),
        MethodKind::Floor => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric_floor(numeric))),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .floor() can only be applied to a numeric value",
            )),
        },
        MethodKind::Integer => apply_integer_method(value),
        MethodKind::Number => apply_number_method(value, ".number()"),
        MethodKind::Type => Ok(JsonbValue::String(jsonb_type_name(value).to_string())),
        MethodKind::Size => match value {
            JsonbValue::Array(items) => Ok(numeric_jsonb_from_i32(items.len() as i32)),
            _ if matches!(mode, PathMode::Lax) => Ok(numeric_jsonb_from_i32(1)),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .size() can only be applied to an array",
            )),
        },
        MethodKind::String => apply_string_method(value),
    }
}

fn apply_double_method(value: &JsonbValue) -> Result<JsonbValue, ExecError> {
    let text = match value {
        JsonbValue::Numeric(numeric) => numeric.render(),
        JsonbValue::String(text) => text.clone(),
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method .double() can only be applied to a string or numeric value",
            ));
        }
    };
    let parsed = parse_pg_float(&text, SqlTypeKind::Float8).map_err(|_| {
        exec_jsonpath_error(&format!(
            "argument \"{text}\" of jsonpath item method .double() is invalid for type double precision"
        ))
    })?;
    if parsed.is_nan() || parsed.is_infinite() {
        return Err(exec_jsonpath_error(
            "NaN or Infinity is not allowed for jsonpath item method .double()",
        ));
    }
    Ok(JsonbValue::Numeric(NumericValue::from(parsed.to_string())))
}

fn apply_number_method(value: &JsonbValue, method_name: &str) -> Result<JsonbValue, ExecError> {
    match value {
        JsonbValue::Numeric(numeric) => {
            reject_nan_or_infinity(numeric, method_name)?;
            Ok(JsonbValue::Numeric(numeric.clone()))
        }
        JsonbValue::String(text) => {
            let numeric = parse_numeric_text(text).ok_or_else(|| {
                exec_jsonpath_error(&format!(
                    "argument \"{text}\" of jsonpath item method {method_name} is invalid for type numeric"
                ))
            })?;
            reject_nan_or_infinity(&numeric, method_name)?;
            Ok(JsonbValue::Numeric(numeric))
        }
        _ => Err(exec_jsonpath_error(&format!(
            "jsonpath item method {method_name} can only be applied to a string or numeric value"
        ))),
    }
}

fn apply_decimal_method(value: &JsonbValue, args: &[NumericValue]) -> Result<JsonbValue, ExecError> {
    if args.len() > 2 {
        return Err(exec_jsonpath_error("unsupported jsonpath item method"));
    }
    let numeric = match apply_number_method(value, ".decimal()")? {
        JsonbValue::Numeric(numeric) => numeric,
        _ => unreachable!("decimal method returns numeric"),
    };
    if args.is_empty() {
        return Ok(JsonbValue::Numeric(numeric));
    }
    let precision = decimal_arg_to_i32(&args[0], "precision")?;
    let scale = if let Some(arg) = args.get(1) {
        decimal_arg_to_i32(arg, "scale")?
    } else {
        0
    };
    if !(1..=1000).contains(&precision) {
        return Err(exec_jsonpath_error(&format!(
            "NUMERIC precision {precision} must be between 1 and 1000"
        )));
    }
    if !(-1000..=1000).contains(&scale) {
        return Err(exec_jsonpath_error(&format!(
            "NUMERIC scale {scale} must be between -1000 and 1000"
        )));
    }
    let rendered = numeric.render();
    let coerced = coerce_jsonpath_decimal_numeric(numeric, precision, scale).map_err(|_| {
        exec_jsonpath_error(&format!(
            "argument \"{rendered}\" of jsonpath item method .decimal() is invalid for type numeric"
        ))
    })?;
    Ok(JsonbValue::Numeric(coerced))
}

fn decimal_arg_to_i32(value: &NumericValue, label: &str) -> Result<i32, ExecError> {
    value
        .render()
        .parse::<i32>()
        .map_err(|_| exec_jsonpath_error(&format!(
            "{label} of jsonpath item method .decimal() is out of range for type integer"
        )))
}

fn apply_boolean_method(value: &JsonbValue) -> Result<JsonbValue, ExecError> {
    let result = match value {
        JsonbValue::Bool(value) => *value,
        JsonbValue::Numeric(numeric) => {
            let text = numeric.render();
            let parsed = text.parse::<i32>().map_err(|_| {
                exec_jsonpath_error(&format!(
                    "argument \"{text}\" of jsonpath item method .boolean() is invalid for type boolean"
                ))
            })?;
            parsed != 0
        }
        JsonbValue::String(text) => parse_pg_bool_text(text).map_err(|_| {
            exec_jsonpath_error(&format!(
                "argument \"{text}\" of jsonpath item method .boolean() is invalid for type boolean"
            ))
        })?,
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method .boolean() can only be applied to a boolean, string, or numeric value",
            ));
        }
    };
    Ok(JsonbValue::Bool(result))
}

fn apply_integer_method(value: &JsonbValue) -> Result<JsonbValue, ExecError> {
    let rendered = match value {
        JsonbValue::Numeric(numeric) => numeric
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i32>().ok())
            .map(numeric_jsonb_from_i32)
            .ok_or_else(|| {
                exec_jsonpath_error(&format!(
                    "argument \"{}\" of jsonpath item method .integer() is invalid for type integer",
                    numeric.render()
                ))
            })?,
        JsonbValue::String(text) => {
            let parsed = text.parse::<i32>().map_err(|_| {
                exec_jsonpath_error(&format!(
                    "argument \"{text}\" of jsonpath item method .integer() is invalid for type integer"
                ))
            })?;
            numeric_jsonb_from_i32(parsed)
        }
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method .integer() can only be applied to a string or numeric value",
            ));
        }
    };
    Ok(rendered)
}

fn apply_string_method(value: &JsonbValue) -> Result<JsonbValue, ExecError> {
    let text = match value {
        JsonbValue::String(text) => text.clone(),
        JsonbValue::Numeric(numeric) => numeric.render(),
        JsonbValue::Bool(true) => "true".to_string(),
        JsonbValue::Bool(false) => "false".to_string(),
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method .string() can only be applied to a boolean, string, numeric, or datetime value",
            ));
        }
    };
    Ok(JsonbValue::String(text))
}

fn compare_any_pair(
    left: &[JsonbValue],
    right: &[JsonbValue],
    op: CompareOp,
    mode: PathMode,
) -> PredicateValue {
    let mut found = false;
    let mut unknown = false;
    for left_value in left {
        for right_value in right {
            match compare_values(left_value, right_value, op) {
                PredicateValue::True => {
                    if matches!(mode, PathMode::Lax) {
                        return PredicateValue::True;
                    }
                    found = true;
                }
                PredicateValue::Unknown => {
                    if matches!(mode, PathMode::Strict) {
                        return PredicateValue::Unknown;
                    }
                    unknown = true;
                }
                PredicateValue::False => {}
            }
        }
    }
    if found {
        PredicateValue::True
    } else if unknown {
        PredicateValue::Unknown
    } else {
        PredicateValue::False
    }
}

fn starts_with_any_pair(left: &[JsonbValue], right: &[JsonbValue]) -> PredicateValue {
    let mut unknown = false;
    for left_value in left {
        for right_value in right {
            match starts_with_values(left_value, right_value) {
                PredicateValue::True => return PredicateValue::True,
                PredicateValue::Unknown => unknown = true,
                PredicateValue::False => {}
            }
        }
    }
    if unknown {
        PredicateValue::Unknown
    } else {
        PredicateValue::False
    }
}

fn starts_with_values(left: &JsonbValue, right: &JsonbValue) -> PredicateValue {
    match (left, right) {
        (JsonbValue::String(left), JsonbValue::String(right)) => {
            if left.starts_with(right.as_str()) {
                PredicateValue::True
            } else {
                PredicateValue::False
            }
        }
        _ => PredicateValue::Unknown,
    }
}

fn like_regex_any(
    values: &[JsonbValue],
    pattern: &str,
    flags: &str,
) -> Result<PredicateValue, ExecError> {
    let mut unknown = false;
    for value in values {
        match like_regex_value(value, pattern, flags)? {
            PredicateValue::True => return Ok(PredicateValue::True),
            PredicateValue::Unknown => unknown = true,
            PredicateValue::False => {}
        }
    }
    Ok(if unknown {
        PredicateValue::Unknown
    } else {
        PredicateValue::False
    })
}

fn like_regex_value(
    value: &JsonbValue,
    pattern: &str,
    flags: &str,
) -> Result<PredicateValue, ExecError> {
    let JsonbValue::String(text) = value else {
        return Ok(PredicateValue::Unknown);
    };
    Ok(if eval_jsonpath_like_regex(text, pattern, flags)? {
        PredicateValue::True
    } else {
        PredicateValue::False
    })
}

fn compare_values(left: &JsonbValue, right: &JsonbValue, op: CompareOp) -> PredicateValue {
    if !same_jsonb_type(left, right) {
        return PredicateValue::Unknown;
    }
    let ordering = compare_jsonb(left, right);
    if match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::NotEq => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::LtEq => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::GtEq => ordering != Ordering::Less,
    } {
        PredicateValue::True
    } else {
        PredicateValue::False
    }
}

fn same_jsonb_type(left: &JsonbValue, right: &JsonbValue) -> bool {
    matches!(
        (left, right),
        (JsonbValue::Null, JsonbValue::Null)
            | (JsonbValue::String(_), JsonbValue::String(_))
            | (JsonbValue::Numeric(_), JsonbValue::Numeric(_))
            | (JsonbValue::Bool(_), JsonbValue::Bool(_))
            | (JsonbValue::Array(_), JsonbValue::Array(_))
            | (JsonbValue::Object(_), JsonbValue::Object(_))
    )
}

fn eval_arithmetic_any_pair(
    left: &[JsonbValue],
    right: &[JsonbValue],
    op: ArithmeticOp,
) -> Result<Vec<JsonbValue>, ExecError> {
    if left.is_empty() || right.is_empty() {
        return Err(exec_jsonpath_error(
            "jsonpath arithmetic requires numeric operands",
        ));
    }
    let mut out = Vec::new();
    for left_value in left {
        for right_value in right {
            out.push(eval_arithmetic_pair(left_value, right_value, op)?);
        }
    }
    Ok(out)
}

fn eval_arithmetic_pair(
    left: &JsonbValue,
    right: &JsonbValue,
    op: ArithmeticOp,
) -> Result<JsonbValue, ExecError> {
    let left = numeric_from_jsonb(left)?;
    let right = numeric_from_jsonb(right)?;
    let value = match op {
        ArithmeticOp::Add => left.add(&right),
        ArithmeticOp::Sub => left.sub(&right),
        ArithmeticOp::Mul => left.mul(&right),
        ArithmeticOp::Div => left
            .div(&right, 16)
            .ok_or_else(|| exec_jsonpath_error("jsonpath division by zero"))?,
        ArithmeticOp::Mod => numeric_remainder(&left, &right)
            .ok_or_else(|| exec_jsonpath_error("jsonpath division by zero"))?,
    };
    Ok(JsonbValue::Numeric(value))
}

fn eval_unary_value(value: JsonbValue, op: UnaryOp) -> Result<JsonbValue, ExecError> {
    let numeric = numeric_from_jsonb(&value)?;
    Ok(JsonbValue::Numeric(match op {
        UnaryOp::Plus => numeric,
        UnaryOp::Minus => numeric.negate(),
    }))
}

fn numeric_from_jsonb(value: &JsonbValue) -> Result<NumericValue, ExecError> {
    match value {
        JsonbValue::Numeric(numeric) => Ok(numeric.clone()),
        _ => Err(exec_jsonpath_error(
            "jsonpath arithmetic requires numeric operands",
        )),
    }
}

fn resolve_bound_expr(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<Option<i32>, ExecError> {
    resolve_expr_numeric(expr, ctx)
}

fn resolve_subscript_expr(
    expr: &SubscriptExpr,
    ctx: &RuntimeContext<'_>,
) -> Result<Option<i32>, ExecError> {
    match expr {
        SubscriptExpr::Expr(expr) => resolve_expr_numeric(expr, ctx),
        SubscriptExpr::Filter { expr, predicate } => {
            let Some(index) = resolve_expr_numeric(expr, ctx)? else {
                return Ok(None);
            };
            let current = numeric_jsonb_from_i32(index);
            let nested = RuntimeContext {
                global: ctx.global,
                current: &current,
                mode: ctx.mode,
                last_index: ctx.last_index,
            };
            if eval_predicate(predicate, &nested)? == PredicateValue::True {
                Ok(Some(index))
            } else {
                Err(exec_jsonpath_error(
                    "jsonpath array subscript is not a single numeric value",
                ))
            }
        }
    }
}

fn resolve_expr_numeric(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<Option<i32>, ExecError> {
    let values = eval_expr(expr, ctx)?;
    if values.is_empty() {
        return Ok(None);
    }
    if values.len() != 1 {
        return Err(exec_jsonpath_error(
            "jsonpath array subscript is not a single numeric value",
        ));
    }
    match &values[0] {
        JsonbValue::Numeric(numeric) => truncate_numeric_to_i32(numeric).map(Some),
        _ => Err(exec_jsonpath_error(
            "jsonpath array subscript is not a single numeric value",
        )),
    }
}

fn truncate_numeric_to_i32(value: &NumericValue) -> Result<i32, ExecError> {
    match value {
        NumericValue::Finite { coeff, scale, .. } => {
            let truncated = if *scale == 0 {
                coeff.clone()
            } else {
                coeff / num_bigint::BigInt::from(10u8).pow(*scale)
            };
            truncated
                .try_into()
                .map_err(|_| exec_jsonpath_error("jsonpath subscript is out of range"))
        }
        _ => Err(exec_jsonpath_error("jsonpath subscript is out of range")),
    }
}

fn numeric_jsonb_from_i32(value: i32) -> JsonbValue {
    JsonbValue::Numeric(NumericValue::finite(num_bigint::BigInt::from(value), 0))
}

fn reject_nan_or_infinity(value: &NumericValue, method_name: &str) -> Result<(), ExecError> {
    match value {
        NumericValue::NaN | NumericValue::PosInf | NumericValue::NegInf => Err(exec_jsonpath_error(
            &format!("NaN or Infinity is not allowed for jsonpath item method {method_name}"),
        )),
        NumericValue::Finite { .. } => Ok(()),
    }
}

fn coerce_jsonpath_decimal_numeric(
    parsed: NumericValue,
    precision: i32,
    scale: i32,
) -> Result<NumericValue, ()> {
    let rounded = if scale >= 0 {
        parsed.round_to_scale(scale as u32).ok_or(())?
    } else {
        coerce_jsonpath_decimal_negative_scale(parsed, scale)?
    };
    match rounded {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf | NumericValue::NegInf => Err(()),
        NumericValue::Finite { .. }
            if jsonpath_numeric_fits_precision_scale(&rounded, precision, scale) =>
        {
            Ok(rounded)
        }
        NumericValue::Finite { .. } => Err(()),
    }
}

fn coerce_jsonpath_decimal_negative_scale(
    parsed: NumericValue,
    scale: i32,
) -> Result<NumericValue, ()> {
    let shift = scale.unsigned_abs();
    match parsed {
        NumericValue::Finite {
            coeff,
            scale: current_scale,
            ..
        } => {
            let factor = pow10_bigint(current_scale.saturating_add(shift));
            let quotient = &coeff / &factor;
            let remainder = &coeff % &factor;
            let twice = remainder.abs() * 2u8;
            let rounded = if twice >= factor.abs() {
                quotient + coeff.signum()
            } else {
                quotient
            };
            Ok(NumericValue::finite(rounded * pow10_bigint(shift), 0).normalize())
        }
        other => Ok(other),
    }
}

fn jsonpath_numeric_fits_precision_scale(
    value: &NumericValue,
    precision: i32,
    target_scale: i32,
) -> bool {
    match value {
        NumericValue::Finite { coeff, scale, .. } => {
            if coeff.is_zero() {
                return true;
            }
            let limit_exp = precision - target_scale + (*scale as i32);
            if limit_exp <= 0 {
                return false;
            }
            coeff.abs() < pow10_bigint(limit_exp as u32)
        }
        _ => true,
    }
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

fn numeric_ceiling(value: &NumericValue) -> NumericValue {
    match value {
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf => NumericValue::NegInf,
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Finite {
            coeff,
            scale,
            dscale,
        } if *scale == 0 => NumericValue::finite(coeff.clone(), 0).with_dscale(*dscale),
        NumericValue::Finite { coeff, scale, .. } => {
            let factor = num_bigint::BigInt::from(10u8).pow(*scale);
            let quotient = coeff / &factor;
            let remainder = coeff % &factor;
            let adjusted = if coeff.sign() == num_bigint::Sign::Plus && !remainder.is_zero() {
                quotient + 1
            } else {
                quotient
            };
            NumericValue::finite(adjusted, 0).normalize()
        }
    }
}

fn numeric_floor(value: &NumericValue) -> NumericValue {
    match value {
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf => NumericValue::NegInf,
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Finite {
            coeff,
            scale,
            dscale,
        } if *scale == 0 => NumericValue::finite(coeff.clone(), 0).with_dscale(*dscale),
        NumericValue::Finite { coeff, scale, .. } => {
            let factor = num_bigint::BigInt::from(10u8).pow(*scale);
            let quotient = coeff / &factor;
            let remainder = coeff % &factor;
            let adjusted = if coeff.sign() == num_bigint::Sign::Minus && !remainder.is_zero() {
                quotient - 1
            } else {
                quotient
            };
            NumericValue::finite(adjusted, 0).normalize()
        }
    }
}

fn jsonb_type_name(value: &JsonbValue) -> &'static str {
    match value {
        JsonbValue::Null => "null",
        JsonbValue::Bool(_) => "boolean",
        JsonbValue::Numeric(_) => "number",
        JsonbValue::String(_) => "string",
        JsonbValue::Array(_) => "array",
        JsonbValue::Object(_) => "object",
    }
}

fn numeric_remainder(left: &NumericValue, right: &NumericValue) -> Option<NumericValue> {
    match (left, right) {
        (NumericValue::NaN, _) | (_, NumericValue::NaN) => Some(NumericValue::NaN),
        (NumericValue::PosInf | NumericValue::NegInf, _) => Some(NumericValue::NaN),
        (_, NumericValue::PosInf | NumericValue::NegInf) => Some(left.clone()),
        (_, NumericValue::Finite { coeff, .. }) if coeff.is_zero() => None,
        (
            NumericValue::Finite {
                coeff: lcoeff,
                scale: lscale,
                ..
            },
            NumericValue::Finite {
                coeff: rcoeff,
                scale: rscale,
                ..
            },
        ) => {
            let scale = (*lscale).max(*rscale);
            let left = align_numeric_coeff(lcoeff.clone(), *lscale, scale);
            let right = align_numeric_coeff(rcoeff.clone(), *rscale, scale);
            Some(NumericValue::finite(left % right, scale).normalize())
        }
    }
}

fn align_numeric_coeff(
    coeff: num_bigint::BigInt,
    from_scale: u32,
    to_scale: u32,
) -> num_bigint::BigInt {
    if from_scale >= to_scale {
        coeff
    } else {
        coeff * num_bigint::BigInt::from(10u8).pow(to_scale - from_scale)
    }
}

fn resolve_recursive_bound(value: &JsonbValue, bound: RecursiveBound) -> i32 {
    match bound {
        RecursiveBound::Int(value) => value,
        RecursiveBound::Last => recursive_depth(value),
    }
}

fn recursive_depth(value: &JsonbValue) -> i32 {
    match value {
        JsonbValue::Array(items) => items
            .iter()
            .map(|item| 1 + recursive_depth(item))
            .max()
            .unwrap_or(0),
        JsonbValue::Object(items) => items
            .iter()
            .map(|(_, item)| 1 + recursive_depth(item))
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

fn collect_recursive_values(
    value: &JsonbValue,
    min_depth: i32,
    max_depth: i32,
    current_depth: i32,
    out: &mut Vec<JsonbValue>,
) {
    if current_depth >= min_depth && current_depth <= max_depth {
        out.push(value.clone());
    }
    if current_depth >= max_depth {
        return;
    }
    let children: Vec<&JsonbValue> = match value {
        JsonbValue::Array(items) => items.iter().collect(),
        JsonbValue::Object(items) => items.iter().map(|(_, item)| item).collect(),
        _ => Vec::new(),
    };
    for child in children {
        collect_recursive_values(child, min_depth, max_depth, current_depth + 1, out);
    }
}

fn exec_jsonpath_error(message: &str) -> ExecError {
    ExecError::InvalidStorageValue {
        column: "jsonpath".into(),
        details: message.to_string(),
    }
}

fn render_jsonpath(path: &JsonPath) -> String {
    let mut out = String::new();
    if matches!(path.mode, PathMode::Strict) {
        out.push_str("strict ");
    }
    render_expr(&path.expr, &mut out);
    out
}

fn render_expr(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Literal(value) => render_literal(value, out),
        Expr::Last => out.push_str("last"),
        Expr::Path { base, steps } => {
            render_base(base, out);
            for step in steps {
                render_step(step, out);
            }
        }
        Expr::Compare { op, left, right } => {
            render_operand(left, out);
            out.push_str(match op {
                CompareOp::Eq => " == ",
                CompareOp::NotEq => " != ",
                CompareOp::Lt => " < ",
                CompareOp::LtEq => " <= ",
                CompareOp::Gt => " > ",
                CompareOp::GtEq => " >= ",
            });
            render_operand(right, out);
        }
        Expr::StartsWith { left, right } => {
            render_operand(left, out);
            out.push_str(" starts with ");
            render_operand(right, out);
        }
        Expr::LikeRegex {
            expr,
            pattern,
            flags,
        } => {
            render_operand(expr, out);
            out.push_str(" like_regex ");
            render_quoted_string(pattern, out);
            if !flags.is_empty() {
                out.push_str(" flag ");
                render_quoted_string(flags, out);
            }
        }
        Expr::Arithmetic { op, left, right } => {
            render_operand(left, out);
            out.push_str(match op {
                ArithmeticOp::Add => " + ",
                ArithmeticOp::Sub => " - ",
                ArithmeticOp::Mul => " * ",
                ArithmeticOp::Div => " / ",
                ArithmeticOp::Mod => " % ",
            });
            render_operand(right, out);
        }
        Expr::Unary { op, inner } => {
            out.push(match op {
                UnaryOp::Plus => '+',
                UnaryOp::Minus => '-',
            });
            render_operand(inner, out);
        }
        Expr::MethodCall { inner, method } => {
            render_operand(inner, out);
            render_method(method, out);
        }
        Expr::Exists(inner) => {
            out.push_str("exists(");
            render_expr(inner, out);
            out.push(')');
        }
        Expr::And(left, right) => {
            render_operand(left, out);
            out.push_str(" && ");
            render_operand(right, out);
        }
        Expr::Or(left, right) => {
            render_operand(left, out);
            out.push_str(" || ");
            render_operand(right, out);
        }
        Expr::Not(inner) => {
            out.push('!');
            render_operand(inner, out);
        }
        Expr::IsUnknown(inner) => {
            render_operand(inner, out);
            out.push_str(" is unknown");
        }
    }
}

fn render_operand(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Compare { .. }
        | Expr::StartsWith { .. }
        | Expr::LikeRegex { .. }
        | Expr::Arithmetic { .. }
        | Expr::And(..)
        | Expr::Or(..) => {
            out.push('(');
            render_expr(expr, out);
            out.push(')');
        }
        _ => render_expr(expr, out),
    }
}

fn render_base(base: &Base, out: &mut String) {
    match base {
        Base::Root => out.push('$'),
        Base::Current => out.push('@'),
        Base::Var(name) => {
            out.push('$');
            out.push_str(name);
        }
    }
}

fn render_step(step: &Step, out: &mut String) {
    match step {
        Step::Member(name) => {
            out.push('.');
            render_quoted_string(name, out);
        }
        Step::MemberWildcard => out.push_str(".*"),
        Step::Recursive {
            min_depth,
            max_depth,
        } => {
            out.push_str(".**");
            if !matches!(
                (min_depth, max_depth),
                (RecursiveBound::Int(1), RecursiveBound::Last)
            ) {
                out.push('{');
                render_recursive_bound(*min_depth, out);
                if min_depth != max_depth {
                    out.push_str(" to ");
                    render_recursive_bound(*max_depth, out);
                }
                out.push('}');
            }
        }
        Step::Subscripts(selections) => {
            out.push('[');
            for (idx, selection) in selections.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                render_subscript_selection(selection, out);
            }
            out.push(']');
        }
        Step::IndexWildcard => out.push_str("[*]"),
        Step::Method(method) => render_method(method, out),
        Step::Filter(expr) => {
            out.push_str(" ? (");
            render_expr(expr, out);
            out.push(')');
        }
    }
}

fn render_method(method: &Method, out: &mut String) {
    out.push_str(match method.kind {
        MethodKind::Abs => ".abs(",
        MethodKind::Boolean => ".boolean(",
        MethodKind::Ceiling => ".ceiling(",
        MethodKind::Decimal => ".decimal(",
        MethodKind::Double => ".double(",
        MethodKind::Floor => ".floor(",
        MethodKind::Integer => ".integer(",
        MethodKind::Number => ".number(",
        MethodKind::Size => ".size(",
        MethodKind::String => ".string(",
        MethodKind::Type => ".type(",
    });
    for (index, arg) in method.args.iter().enumerate() {
        if index > 0 {
            out.push(',');
            out.push(' ');
        }
        out.push_str(&arg.render());
    }
    out.push(')');
}

fn render_recursive_bound(bound: RecursiveBound, out: &mut String) {
    match bound {
        RecursiveBound::Int(value) => out.push_str(&value.to_string()),
        RecursiveBound::Last => out.push_str("last"),
    }
}

fn render_subscript_selection(selection: &SubscriptSelection, out: &mut String) {
    match selection {
        SubscriptSelection::Index(expr) => render_subscript_expr(expr, out),
        SubscriptSelection::Range(start, end) => {
            render_expr(start, out);
            out.push_str(" to ");
            render_expr(end, out);
        }
    }
}

fn render_subscript_expr(expr: &SubscriptExpr, out: &mut String) {
    match expr {
        SubscriptExpr::Expr(expr) => render_expr(expr, out),
        SubscriptExpr::Filter { expr, predicate } => {
            render_expr(expr, out);
            out.push_str(" ? (");
            render_expr(predicate, out);
            out.push(')');
        }
    }
}

fn render_literal(value: &JsonbValue, out: &mut String) {
    match value {
        JsonbValue::Null => out.push_str("null"),
        JsonbValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonbValue::Numeric(n) => out.push_str(&n.render()),
        JsonbValue::String(s) => render_quoted_string(s, out),
        JsonbValue::Array(_) | JsonbValue::Object(_) => out.push_str("null"),
    }
}

fn subscript_expr_to_expr(expr: SubscriptExpr) -> Result<Expr, ExecError> {
    match expr {
        SubscriptExpr::Expr(expr) => Ok(*expr),
        SubscriptExpr::Filter { .. } => Err(exec_jsonpath_error(
            "jsonpath subscript range bound cannot be filtered",
        )),
    }
}

fn render_quoted_string(text: &str, out: &mut String) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                let code = ch as u32;
                out.push_str("\\u");
                out.push_str(&format!("{code:04x}"));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn parse(mut self) -> Result<JsonPath, ExecError> {
        self.skip_ws();
        let mode = if self.consume_keyword("strict") {
            self.require_ws()?;
            PathMode::Strict
        } else if self.consume_keyword("lax") {
            self.require_ws()?;
            PathMode::Lax
        } else {
            PathMode::Lax
        };
        let expr = self.parse_or_expr()?;
        self.skip_ws();
        if !self.is_eof() {
            return Err(exec_jsonpath_error("unexpected trailing jsonpath input"));
        }
        Ok(JsonPath { mode, expr })
    }

    fn parse_or_expr(&mut self) -> Result<Expr, ExecError> {
        let mut expr = self.parse_and_expr()?;
        loop {
            self.skip_ws();
            if self.consume("||") {
                let right = self.parse_and_expr()?;
                expr = Expr::Or(Box::new(expr), Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_and_expr(&mut self) -> Result<Expr, ExecError> {
        let mut expr = self.parse_not_expr()?;
        loop {
            self.skip_ws();
            if self.consume("&&") {
                let right = self.parse_not_expr()?;
                expr = Expr::And(Box::new(expr), Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_not_expr(&mut self) -> Result<Expr, ExecError> {
        self.skip_ws();
        if self.consume("!") {
            return Ok(Expr::Not(Box::new(self.parse_not_expr()?)));
        }
        self.parse_is_unknown_expr()
    }

    fn parse_is_unknown_expr(&mut self) -> Result<Expr, ExecError> {
        let expr = self.parse_compare_expr()?;
        self.skip_ws();
        if self.consume_keyword("is") {
            self.skip_ws();
            if self.consume_keyword("unknown") {
                return Ok(Expr::IsUnknown(Box::new(expr)));
            }
            return Err(exec_jsonpath_error("expected UNKNOWN after IS"));
        }
        Ok(expr)
    }

    fn parse_compare_expr(&mut self) -> Result<Expr, ExecError> {
        let left = self.parse_additive_expr()?;
        self.skip_ws();
        let saved = self.offset;
        if self.consume_keyword("starts") {
            self.require_ws()?;
            if !self.consume_keyword("with") {
                return Err(exec_jsonpath_error("expected WITH after STARTS"));
            }
            let right = self.parse_additive_expr()?;
            return Ok(Expr::StartsWith {
                left: Box::new(left),
                right: Box::new(right),
            });
        }
        self.offset = saved;
        if self.consume_keyword("like_regex") {
            self.skip_ws();
            let pattern = self
                .parse_string()?
                .ok_or_else(|| exec_jsonpath_error("expected jsonpath like_regex pattern"))?;
            self.skip_ws();
            let flags = if self.consume_keyword("flag") {
                self.skip_ws();
                self.parse_string()?
                    .ok_or_else(|| exec_jsonpath_error("expected jsonpath like_regex flags"))?
            } else {
                String::new()
            };
            validate_jsonpath_like_regex(&pattern, &flags)?;
            return Ok(Expr::LikeRegex {
                expr: Box::new(left),
                pattern,
                flags,
            });
        }
        let op = if self.consume("==") {
            Some(CompareOp::Eq)
        } else if self.consume("!=") {
            Some(CompareOp::NotEq)
        } else if self.consume("<=") {
            Some(CompareOp::LtEq)
        } else if self.consume(">=") {
            Some(CompareOp::GtEq)
        } else if self.consume("<") {
            Some(CompareOp::Lt)
        } else if self.consume(">") {
            Some(CompareOp::Gt)
        } else {
            None
        };
        if let Some(op) = op {
            let right = self.parse_additive_expr()?;
            Ok(Expr::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, ExecError> {
        let mut expr = self.parse_multiplicative_expr()?;
        loop {
            self.skip_ws();
            let op = if self.consume("+") {
                Some(ArithmeticOp::Add)
            } else if self.consume("-") {
                Some(ArithmeticOp::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                return Ok(expr);
            };
            let right = self.parse_multiplicative_expr()?;
            expr = Expr::Arithmetic {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, ExecError> {
        let mut expr = self.parse_unary_expr()?;
        loop {
            self.skip_ws();
            let op = if self.consume("*") {
                Some(ArithmeticOp::Mul)
            } else if self.consume("/") {
                Some(ArithmeticOp::Div)
            } else if self.consume("%") {
                Some(ArithmeticOp::Mod)
            } else {
                None
            };
            let Some(op) = op else {
                return Ok(expr);
            };
            let right = self.parse_unary_expr()?;
            expr = Expr::Arithmetic {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, ExecError> {
        self.skip_ws();
        if self.consume("+") {
            return Ok(Expr::Unary {
                op: UnaryOp::Plus,
                inner: Box::new(self.parse_unary_expr()?),
            });
        }
        if self.consume("-") {
            return Ok(Expr::Unary {
                op: UnaryOp::Minus,
                inner: Box::new(self.parse_unary_expr()?),
            });
        }
        let expr = self.parse_primary()?;
        self.parse_postfix_methods(expr)
    }

    fn parse_postfix_methods(&mut self, mut expr: Expr) -> Result<Expr, ExecError> {
        loop {
            let saved = self.offset;
            self.skip_ws();
            if !self.consume(".") {
                self.offset = saved;
                return Ok(expr);
            }
            let Some(ident) = self.parse_ident() else {
                self.offset = saved;
                return Ok(expr);
            };
            if !self.consume("(") {
                self.offset = saved;
                return Ok(expr);
            }
            let method = self.parse_method(&ident)?;
            expr = Expr::MethodCall {
                inner: Box::new(expr),
                method,
            };
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ExecError> {
        self.skip_ws();
        if self.consume("(") {
            let expr = self.parse_or_expr()?;
            self.skip_ws();
            self.expect(")")?;
            return Ok(expr);
        }
        if self.peek() == Some('$') {
            self.bump();
            let base = if let Some(ident) = self.parse_optional_ident() {
                Base::Var(ident)
            } else {
                Base::Root
            };
            return self.parse_path(base);
        }
        if self.peek() == Some('@') {
            self.bump();
            return self.parse_path(Base::Current);
        }
        if self.consume_keyword("exists") {
            self.skip_ws();
            self.expect("(")?;
            let expr = self.parse_or_expr()?;
            self.skip_ws();
            self.expect(")")?;
            return Ok(Expr::Exists(Box::new(expr)));
        }
        if self.consume_keyword("last") {
            return Ok(Expr::Last);
        }
        if self.consume_keyword("true") {
            return Ok(Expr::Literal(JsonbValue::Bool(true)));
        }
        if self.consume_keyword("false") {
            return Ok(Expr::Literal(JsonbValue::Bool(false)));
        }
        if self.consume_keyword("null") {
            return Ok(Expr::Literal(JsonbValue::Null));
        }
        if let Some(text) = self.parse_string()? {
            return Ok(Expr::Literal(JsonbValue::String(text)));
        }
        if let Some(number) = self.parse_number()? {
            return Ok(Expr::Literal(number));
        }
        Err(exec_jsonpath_error("invalid jsonpath expression"))
    }

    fn parse_path(&mut self, base: Base) -> Result<Expr, ExecError> {
        let mut steps = Vec::new();
        loop {
            self.skip_ws();
            if self.consume(".") {
                if self.consume("*") {
                    if self.consume("*") {
                        let (min_depth, max_depth) = self.parse_recursive_quantifier()?;
                        steps.push(Step::Recursive {
                            min_depth,
                            max_depth,
                        });
                    } else {
                        steps.push(Step::MemberWildcard);
                    }
                } else {
                    if let Some(ident) = self.parse_ident() {
                        if self.consume("(") {
                            steps.push(Step::Method(self.parse_method(&ident)?));
                        } else {
                            steps.push(Step::Member(ident));
                        }
                    } else {
                        let key = self
                            .parse_string()?
                            .ok_or_else(|| exec_jsonpath_error("expected jsonpath member name"))?;
                        steps.push(Step::Member(key));
                    }
                }
            } else if self.consume("[") {
                self.skip_ws();
                if self.consume("*") {
                    self.skip_ws();
                    self.expect("]")?;
                    steps.push(Step::IndexWildcard);
                } else {
                    let mut selections = Vec::new();
                    loop {
                        let start = self.parse_subscript_expr()?;
                        self.skip_ws();
                        if self.consume_keyword("to") {
                            self.skip_ws();
                            let end = self.parse_additive_expr()?;
                            selections.push(SubscriptSelection::Range(
                                subscript_expr_to_expr(start)?,
                                end,
                            ));
                        } else {
                            selections.push(SubscriptSelection::Index(start));
                        }
                        self.skip_ws();
                        if !self.consume(",") {
                            break;
                        }
                        self.skip_ws();
                    }
                    self.expect("]")?;
                    steps.push(Step::Subscripts(selections));
                }
            } else if self.consume("?") {
                self.skip_ws();
                self.expect("(")?;
                let expr = self.parse_or_expr()?;
                self.skip_ws();
                self.expect(")")?;
                steps.push(Step::Filter(Box::new(expr)));
            } else {
                break;
            }
        }
        Ok(Expr::Path { base, steps })
    }

    fn parse_recursive_quantifier(
        &mut self,
    ) -> Result<(RecursiveBound, RecursiveBound), ExecError> {
        self.skip_ws();
        if !self.consume("{") {
            return Ok((RecursiveBound::Int(0), RecursiveBound::Last));
        }
        self.skip_ws();
        let start = self.parse_recursive_bound()?;
        self.skip_ws();
        let end = if self.consume_keyword("to") {
            self.skip_ws();
            self.parse_recursive_bound()?
        } else {
            start
        };
        self.skip_ws();
        self.expect("}")?;
        Ok((start, end))
    }

    fn parse_recursive_bound(&mut self) -> Result<RecursiveBound, ExecError> {
        if self.consume_keyword("last") {
            return Ok(RecursiveBound::Last);
        }
        Ok(RecursiveBound::Int(self.parse_signed_int()?))
    }

    fn parse_subscript_expr(&mut self) -> Result<SubscriptExpr, ExecError> {
        let expr = self.parse_additive_expr()?;
        self.skip_ws();
        if self.consume("?") {
            self.skip_ws();
            self.expect("(")?;
            let predicate = self.parse_or_expr()?;
            self.skip_ws();
            self.expect(")")?;
            return Ok(SubscriptExpr::Filter {
                expr: Box::new(expr),
                predicate: Box::new(predicate),
            });
        }
        Ok(SubscriptExpr::Expr(Box::new(expr)))
    }

    fn method_kind(&self, ident: &str) -> Result<MethodKind, ExecError> {
        match ident {
            "abs" => Ok(MethodKind::Abs),
            "boolean" => Ok(MethodKind::Boolean),
            "ceiling" => Ok(MethodKind::Ceiling),
            "decimal" => Ok(MethodKind::Decimal),
            "double" => Ok(MethodKind::Double),
            "floor" => Ok(MethodKind::Floor),
            "integer" => Ok(MethodKind::Integer),
            "number" => Ok(MethodKind::Number),
            "size" => Ok(MethodKind::Size),
            "string" => Ok(MethodKind::String),
            "type" => Ok(MethodKind::Type),
            _ => Err(exec_jsonpath_error("unsupported jsonpath item method")),
        }
    }

    fn parse_method(&mut self, ident: &str) -> Result<Method, ExecError> {
        let kind = self.method_kind(ident)?;
        let args = self.parse_method_args()?;
        Ok(Method { kind, args })
    }

    fn parse_method_args(&mut self) -> Result<Vec<NumericValue>, ExecError> {
        self.skip_ws();
        if self.consume(")") {
            return Ok(Vec::new());
        }
        let mut args = Vec::new();
        loop {
            args.push(self.parse_method_numeric_arg()?);
            self.skip_ws();
            if self.consume(")") {
                break;
            }
            self.expect(",")?;
            self.skip_ws();
        }
        Ok(args)
    }

    fn parse_method_numeric_arg(&mut self) -> Result<NumericValue, ExecError> {
        self.skip_ws();
        let start = self.offset;
        let _ = self.consume("+") || self.consume("-");
        let Some(_) = self.take_while(|ch| ch.is_ascii_digit()) else {
            self.offset = start;
            return Err(exec_jsonpath_error("expected numeric jsonpath method argument"));
        };
        let mut text = self.input[start..self.offset].to_string();
        if self.consume(".") {
            text.push('.');
            let frac = self
                .take_while(|ch| ch.is_ascii_digit())
                .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))?;
            text.push_str(frac);
        }
        parse_numeric_text(&text).ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))
    }

    fn parse_signed_int(&mut self) -> Result<i32, ExecError> {
        self.skip_ws();
        let negative = self.consume("-");
        let digits = self
            .take_while(|ch| ch.is_ascii_digit())
            .ok_or_else(|| exec_jsonpath_error("expected integer jsonpath subscript"))?;
        let mut value = digits
            .parse::<i32>()
            .map_err(|_| exec_jsonpath_error("jsonpath subscript is out of range"))?;
        if negative {
            value = -value;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<Option<JsonbValue>, ExecError> {
        let start = self.offset;
        let _ = self.consume("-");
        let Some(int_part) = self.take_while(|ch| ch.is_ascii_digit()) else {
            self.offset = start;
            return Ok(None);
        };
        let mut text = String::new();
        if self.input[start..].starts_with('-') {
            text.push('-');
        }
        text.push_str(int_part);
        if self.consume(".") {
            text.push('.');
            let frac = self
                .take_while(|ch| ch.is_ascii_digit())
                .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))?;
            text.push_str(frac);
        }
        let numeric = parse_numeric_text(&text)
            .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))?;
        Ok(Some(JsonbValue::Numeric(numeric)))
    }

    fn parse_string(&mut self) -> Result<Option<String>, ExecError> {
        let quote = match self.peek() {
            Some('"') | Some('\'') => self.peek().unwrap(),
            _ => return Ok(None),
        };
        self.bump();
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.bump();
            if ch == quote {
                return Ok(Some(out));
            }
            if ch == '\\' {
                self.parse_escape_sequence(&mut out)?;
            } else {
                out.push(ch);
            }
        }
        Err(exec_jsonpath_error("unterminated jsonpath string"))
    }

    fn parse_optional_ident(&mut self) -> Option<String> {
        let saved = self.offset;
        let ident = self.parse_ident();
        if ident.is_none() {
            self.offset = saved;
        }
        ident
    }

    fn parse_ident(&mut self) -> Option<String> {
        let start = self.offset;
        let first = self.peek()?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return None;
        }
        self.bump();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.bump();
            } else {
                break;
            }
        }
        Some(self.input[start..self.offset].to_string())
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        let saved = self.offset;
        if self.consume(keyword) {
            let valid_end = self
                .peek()
                .map(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .unwrap_or(true);
            if valid_end {
                return true;
            }
        }
        self.offset = saved;
        false
    }

    fn require_ws(&mut self) -> Result<(), ExecError> {
        let start = self.offset;
        self.skip_ws();
        if self.offset == start {
            Err(exec_jsonpath_error(
                "expected whitespace after jsonpath mode",
            ))
        } else {
            Ok(())
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(ch) if ch.is_whitespace()) {
            self.bump();
        }
    }

    fn parse_escape_sequence(&mut self, out: &mut String) -> Result<(), ExecError> {
        let escaped = self
            .peek()
            .ok_or_else(|| exec_jsonpath_error("unterminated jsonpath string"))?;
        self.bump();
        match escaped {
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            '\'' => out.push('\''),
            '/' => out.push('/'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000C}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                let codepoint = self.parse_unicode_escape()?;
                if (0xD800..=0xDBFF).contains(&codepoint) {
                    self.expect("\\u")?;
                    let low = self.parse_unicode_escape()?;
                    if !(0xDC00..=0xDFFF).contains(&low) {
                        return Err(exec_jsonpath_error(
                            "invalid low surrogate in jsonpath string",
                        ));
                    }
                    let scalar =
                        0x10000 + (((codepoint - 0xD800) as u32) << 10) + (low - 0xDC00) as u32;
                    let ch = char::from_u32(scalar).ok_or_else(|| {
                        exec_jsonpath_error("invalid Unicode scalar value in jsonpath string")
                    })?;
                    out.push(ch);
                } else if (0xDC00..=0xDFFF).contains(&codepoint) {
                    return Err(exec_jsonpath_error(
                        "invalid low surrogate in jsonpath string",
                    ));
                } else if codepoint == 0 {
                    return Err(exec_jsonpath_error("unsupported Unicode escape sequence"));
                } else {
                    let ch = char::from_u32(codepoint as u32).ok_or_else(|| {
                        exec_jsonpath_error("invalid Unicode scalar value in jsonpath string")
                    })?;
                    out.push(ch);
                }
            }
            _ => {
                return Err(exec_jsonpath_error(
                    "invalid escape sequence in jsonpath string",
                ));
            }
        }
        Ok(())
    }

    fn parse_unicode_escape(&mut self) -> Result<u16, ExecError> {
        let mut value = 0u16;
        for _ in 0..4 {
            let ch = self
                .peek()
                .ok_or_else(|| exec_jsonpath_error("invalid Unicode escape sequence"))?;
            self.bump();
            let digit = ch
                .to_digit(16)
                .ok_or_else(|| exec_jsonpath_error("invalid Unicode escape sequence"))?;
            value = (value << 4) | digit as u16;
        }
        Ok(value)
    }

    fn take_while(&mut self, predicate: impl Fn(char) -> bool) -> Option<&'a str> {
        let start = self.offset;
        while matches!(self.peek(), Some(ch) if predicate(ch)) {
            self.bump();
        }
        (self.offset > start).then_some(&self.input[start..self.offset])
    }

    fn expect(&mut self, token: &str) -> Result<(), ExecError> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(exec_jsonpath_error("unexpected jsonpath token"))
        }
    }

    fn consume(&mut self, token: &str) -> bool {
        if self.input[self.offset..].starts_with(token) {
            self.offset += token.len();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn bump(&mut self) {
        if let Some(ch) = self.peek() {
            self.offset += ch.len_utf8();
        }
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.input.len()
    }
}
