use std::cmp::Ordering;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};

use crate::backend::executor::ExecError;
use crate::backend::executor::expr_bool::parse_pg_bool_text;
use crate::backend::executor::expr_casts::{
    cast_text_value_with_config, cast_value_with_config, parse_pg_float,
};
use crate::backend::executor::expr_ops::{mixed_date_timestamp_ordering, parse_numeric_text};
use crate::backend::executor::jsonb::{
    JsonbValue, compare_jsonb, jsonb_nested_encoded_len_at, render_temporal_jsonb_value,
};
use crate::backend::executor::pg_regex::{eval_jsonpath_like_regex, validate_jsonpath_like_regex};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::notices::push_warning;
use crate::backend::utils::time::datetime::timezone_offset_seconds_at_utc;
use crate::include::nodes::datetime::{TimeTzADT, USECS_PER_SEC};
use crate::include::nodes::datum::{NumericValue, Value};
use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};

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
    Access {
        inner: Box<Expr>,
        step: Step,
    },
    Filter {
        inner: Box<Expr>,
        predicate: Box<Expr>,
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
    BigInt,
    Boolean,
    Ceiling,
    Date,
    Decimal,
    Datetime,
    Double,
    Floor,
    Integer,
    KeyValue,
    LTrim,
    Lower,
    Number,
    BTrim,
    InitCap,
    Replace,
    RTrim,
    Size,
    SplitPart,
    String,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Type,
}

#[derive(Debug, Clone)]
struct Method {
    kind: MethodKind,
    args: Vec<MethodArg>,
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
enum MethodArg {
    Numeric(NumericValue),
    String(String),
}

#[derive(Debug, Clone)]
pub(crate) struct EvaluationContext<'a> {
    pub(crate) root: &'a JsonbValue,
    pub(crate) vars: Option<&'a JsonbValue>,
    pub(crate) datetime_config: &'a DateTimeConfig,
    pub(crate) allow_timezone: bool,
    pub(crate) silent: bool,
    pub(crate) preserve_step_prefix: bool,
    pub(crate) preserve_unary_prefix: bool,
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
    if text.is_empty() {
        return Err(exec_jsonpath_error(
            "invalid input syntax for type jsonpath: \"\"",
        ));
    }
    Parser::new(text).parse()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonPathDatatypeStatus {
    NonDateTime,
    DateTimeNonZoned,
    DateTimeZoned,
    UnknownDateTime,
}

pub(crate) fn jsonpath_is_mutable(
    text: &str,
    passing_types: &[(String, SqlType)],
) -> Result<bool, ExecError> {
    let path = parse_jsonpath(text)?;
    let mut ctx = JsonPathMutableContext {
        passing_types,
        current: JsonPathDatatypeStatus::NonDateTime,
        lax: path.mode == PathMode::Lax,
        mutable: false,
    };
    jsonpath_expr_datatype_status(&path.expr, &mut ctx);
    Ok(ctx.mutable)
}

struct JsonPathMutableContext<'a> {
    passing_types: &'a [(String, SqlType)],
    current: JsonPathDatatypeStatus,
    lax: bool,
    mutable: bool,
}

fn jsonpath_expr_datatype_status(
    expr: &Expr,
    ctx: &mut JsonPathMutableContext<'_>,
) -> JsonPathDatatypeStatus {
    if ctx.mutable {
        return JsonPathDatatypeStatus::NonDateTime;
    }

    match expr {
        Expr::Path { base, steps } => {
            let mut status = match base {
                Base::Root => JsonPathDatatypeStatus::NonDateTime,
                Base::Current => ctx.current,
                Base::Var(name) => jsonpath_passing_datatype_status(ctx.passing_types, name),
            };
            for step in steps {
                status = jsonpath_step_datatype_status(step, status, ctx);
                if ctx.mutable {
                    break;
                }
            }
            status
        }
        Expr::Compare { left, right, .. } => {
            let left_status = jsonpath_expr_datatype_status(left, ctx);
            let right_status = jsonpath_expr_datatype_status(right, ctx);
            if left_status != JsonPathDatatypeStatus::NonDateTime
                && right_status != JsonPathDatatypeStatus::NonDateTime
                && (left_status == JsonPathDatatypeStatus::UnknownDateTime
                    || right_status == JsonPathDatatypeStatus::UnknownDateTime
                    || left_status != right_status)
            {
                ctx.mutable = true;
            }
            JsonPathDatatypeStatus::NonDateTime
        }
        Expr::StartsWith { left, right } | Expr::Arithmetic { left, right, .. } => {
            jsonpath_expr_datatype_status(left, ctx);
            jsonpath_expr_datatype_status(right, ctx);
            JsonPathDatatypeStatus::NonDateTime
        }
        Expr::Filter { inner, predicate } => {
            let status = jsonpath_expr_datatype_status(inner, ctx);
            let previous = ctx.current;
            ctx.current = status;
            jsonpath_expr_datatype_status(predicate, ctx);
            ctx.current = previous;
            status
        }
        Expr::LikeRegex { expr, .. }
        | Expr::Unary { inner: expr, .. }
        | Expr::Exists(expr)
        | Expr::Not(expr)
        | Expr::IsUnknown(expr) => {
            jsonpath_expr_datatype_status(expr, ctx);
            JsonPathDatatypeStatus::NonDateTime
        }
        Expr::MethodCall { inner, method } => {
            jsonpath_expr_datatype_status(inner, ctx);
            jsonpath_method_datatype_status(method, ctx)
        }
        Expr::Access { inner, step } => {
            let status = jsonpath_expr_datatype_status(inner, ctx);
            jsonpath_step_datatype_status(step, status, ctx)
        }
        Expr::And(left, right) | Expr::Or(left, right) => {
            jsonpath_expr_datatype_status(left, ctx);
            jsonpath_expr_datatype_status(right, ctx);
            JsonPathDatatypeStatus::NonDateTime
        }
        Expr::Literal(_) | Expr::Last => JsonPathDatatypeStatus::NonDateTime,
    }
}

fn jsonpath_step_datatype_status(
    step: &Step,
    current_status: JsonPathDatatypeStatus,
    ctx: &mut JsonPathMutableContext<'_>,
) -> JsonPathDatatypeStatus {
    match step {
        Step::Filter(predicate) => {
            let previous_current = ctx.current;
            ctx.current = current_status;
            jsonpath_expr_datatype_status(predicate, ctx);
            ctx.current = previous_current;
            current_status
        }
        Step::Subscripts(selections) => {
            for selection in selections {
                match selection {
                    SubscriptSelection::Index(SubscriptExpr::Expr(expr)) => {
                        jsonpath_expr_datatype_status(expr, ctx);
                    }
                    SubscriptSelection::Index(SubscriptExpr::Filter { expr, predicate }) => {
                        jsonpath_expr_datatype_status(expr, ctx);
                        jsonpath_expr_datatype_status(predicate, ctx);
                    }
                    SubscriptSelection::Range(from, to) => {
                        jsonpath_expr_datatype_status(to, ctx);
                        jsonpath_expr_datatype_status(from, ctx);
                    }
                }
            }
            if ctx.lax {
                current_status
            } else {
                JsonPathDatatypeStatus::NonDateTime
            }
        }
        Step::IndexWildcard => {
            if ctx.lax {
                current_status
            } else {
                JsonPathDatatypeStatus::NonDateTime
            }
        }
        Step::Recursive { min_depth, .. } => match min_depth {
            RecursiveBound::Int(value) if *value <= 0 => current_status,
            RecursiveBound::Int(_) | RecursiveBound::Last => JsonPathDatatypeStatus::NonDateTime,
        },
        Step::Method(method) => jsonpath_method_datatype_status(method, ctx),
        Step::Member(_) | Step::MemberWildcard => JsonPathDatatypeStatus::NonDateTime,
    }
}

fn jsonpath_method_datatype_status(
    method: &Method,
    ctx: &mut JsonPathMutableContext<'_>,
) -> JsonPathDatatypeStatus {
    match method.kind {
        MethodKind::Datetime => {
            if let Some(MethodArg::String(template)) = method.args.first() {
                if datetime_template_has_timezone(template) {
                    JsonPathDatatypeStatus::DateTimeZoned
                } else {
                    JsonPathDatatypeStatus::DateTimeNonZoned
                }
            } else {
                JsonPathDatatypeStatus::UnknownDateTime
            }
        }
        MethodKind::Date | MethodKind::Time | MethodKind::Timestamp => {
            ctx.mutable = true;
            JsonPathDatatypeStatus::DateTimeNonZoned
        }
        MethodKind::TimeTz | MethodKind::TimestampTz => {
            ctx.mutable = true;
            JsonPathDatatypeStatus::DateTimeZoned
        }
        MethodKind::Abs
        | MethodKind::BigInt
        | MethodKind::Boolean
        | MethodKind::Ceiling
        | MethodKind::Decimal
        | MethodKind::Double
        | MethodKind::Floor
        | MethodKind::Integer
        | MethodKind::KeyValue
        | MethodKind::LTrim
        | MethodKind::Lower
        | MethodKind::Number
        | MethodKind::BTrim
        | MethodKind::InitCap
        | MethodKind::Replace
        | MethodKind::RTrim
        | MethodKind::Size
        | MethodKind::SplitPart
        | MethodKind::String
        | MethodKind::Type => JsonPathDatatypeStatus::NonDateTime,
    }
}

fn jsonpath_passing_datatype_status(
    passing_types: &[(String, SqlType)],
    name: &str,
) -> JsonPathDatatypeStatus {
    passing_types
        .iter()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, sql_type)| match sql_type.kind {
            SqlTypeKind::Date | SqlTypeKind::Time | SqlTypeKind::Timestamp => {
                JsonPathDatatypeStatus::DateTimeNonZoned
            }
            SqlTypeKind::TimeTz | SqlTypeKind::TimestampTz => JsonPathDatatypeStatus::DateTimeZoned,
            _ => JsonPathDatatypeStatus::NonDateTime,
        })
        .unwrap_or(JsonPathDatatypeStatus::NonDateTime)
}

fn datetime_template_has_timezone(template: &str) -> bool {
    let upper = template.to_ascii_uppercase();
    upper.contains("TZH") || upper.contains("TZM")
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
        | Expr::IsUnknown(..) => Ok(vec![predicate_value_to_jsonb(eval_predicate(expr, ctx)?)]),
        Expr::Exists(..) => Ok(vec![predicate_value_to_jsonb(eval_predicate(expr, ctx)?)]),
        Expr::Arithmetic { op, left, right } => {
            let left_values = eval_expr(left, ctx)?;
            let right_values = eval_expr(right, ctx)?;
            eval_arithmetic_operands(&left_values, &right_values, *op, ctx)
        }
        Expr::MethodCall { inner, method } => {
            eval_expr(inner, ctx)?
                .into_iter()
                .try_fold(Vec::new(), |mut out, value| {
                    out.extend(apply_method_values(&value, method, ctx)?);
                    Ok(out)
                })
        }
        Expr::Access { inner, step } => {
            let values = eval_expr(inner, ctx)?;
            apply_step(values, step, ctx)
        }
        Expr::Filter { inner, predicate } => {
            eval_expr(inner, ctx)?
                .into_iter()
                .try_fold(Vec::new(), |mut out, value| {
                    let nested = RuntimeContext {
                        global: ctx.global,
                        current: &value,
                        mode: ctx.mode,
                        last_index: ctx.last_index,
                    };
                    if eval_predicate(predicate, &nested)? == PredicateValue::True {
                        out.push(value);
                    }
                    Ok(out)
                })
        }
        Expr::Unary { op, inner } => {
            let values = eval_expr(inner, ctx)?;
            eval_unary_values(&values, *op, ctx)
        }
    }
}

fn eval_predicate(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<PredicateValue, ExecError> {
    match expr {
        Expr::Exists(inner) => Ok(match eval_expr(inner, ctx) {
            Ok(values) if values.is_empty() => PredicateValue::False,
            Ok(_) => PredicateValue::True,
            Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
            Err(_) => PredicateValue::Unknown,
        }),
        Expr::Compare { op, left, right } => {
            let left_values = match eval_expr(left, ctx) {
                Ok(values) => values,
                Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            let right_values = match eval_expr(right, ctx) {
                Ok(values) => values,
                Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            compare_any_pair(&left_values, &right_values, *op, ctx)
        }
        Expr::StartsWith { left, right } => {
            let left_values = match eval_expr(left, ctx) {
                Ok(values) => values,
                Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            let right_values = match eval_expr(right, ctx) {
                Ok(values) => values,
                Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            Ok(starts_with_any_pair(&left_values, &right_values, ctx))
        }
        Expr::LikeRegex {
            expr,
            pattern,
            flags,
        } => {
            let values = match eval_expr(expr, ctx) {
                Ok(values) => values,
                Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
                Err(_) => return Ok(PredicateValue::Unknown),
            };
            like_regex_any(&values, pattern, flags, ctx)
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
        Err(err) if is_jsonpath_fatal_error(&err) => return Err(err),
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
    let Some(vars) = ctx.global.vars else {
        return Err(undefined_jsonpath_variable(name));
    };
    let JsonbValue::Object(items) = vars else {
        return Err(jsonpath_vars_not_object_error());
    };
    items
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
        .ok_or_else(|| undefined_jsonpath_variable(name))
}

fn undefined_jsonpath_variable(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("could not find jsonpath variable \"{name}\""),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn jsonpath_vars_not_object_error() -> ExecError {
    ExecError::DetailedError {
        message: "\"vars\" argument is not an object".into(),
        detail: Some(
            "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.".into(),
        ),
        hint: None,
        sqlstate: "22023",
    }
}

fn is_jsonpath_fatal_error(err: &ExecError) -> bool {
    matches!(
        err,
        ExecError::DetailedError { message, .. }
            if message.starts_with("could not find jsonpath variable ")
                || message == "\"vars\" argument is not an object"
    )
}

fn apply_step(
    values: Vec<JsonbValue>,
    step: &Step,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let mut out = Vec::new();
    for value in values {
        if let Err(err) = apply_step_single(&value, step, ctx, &mut out) {
            if ctx.global.preserve_step_prefix && !out.is_empty() {
                return Ok(out);
            }
            return Err(err);
        }
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
                    return Err(exec_jsonpath_error(&format!(
                        "JSON object does not contain key \"{name}\""
                    )));
                }
            }
            JsonbValue::Array(items) if matches!(ctx.mode, PathMode::Lax) => {
                for item in items {
                    apply_step_single(item, step, ctx, out)?;
                }
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath member accessor can only be applied to an object",
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
                    "jsonpath wildcard member accessor can only be applied to an object",
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
                    "jsonpath array accessor can only be applied to an array",
                ));
            }
            _ => {}
        },
        Step::IndexWildcard => match value {
            JsonbValue::Array(items) => out.extend(items.iter().cloned()),
            _ if matches!(ctx.mode, PathMode::Lax) => out.push(value.clone()),
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath wildcard array accessor can only be applied to an array",
                ));
            }
            _ => {}
        },
        Step::Method(kind) => match value {
            JsonbValue::Array(items)
                if matches!(ctx.mode, PathMode::Lax) && method_auto_unwraps_array(kind) =>
            {
                for item in items {
                    out.extend(apply_method_values(item, kind, ctx)?);
                }
            }
            _ => out.extend(apply_method_values(value, kind, ctx)?),
        },
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
            SubscriptSelection::Index(expr) => {
                match resolve_subscript_expr(expr, &subscript_ctx)? {
                    Some(index) => {
                        if let Some(found) = array_index(items, index) {
                            out.push(found.clone());
                            matched = true;
                        } else if matches!(ctx.mode, PathMode::Strict) {
                            return Err(exec_jsonpath_error(
                                "jsonpath array subscript is out of bounds",
                            ));
                        }
                    }
                    None => {
                        if matches!(ctx.mode, PathMode::Strict) {
                            return Err(exec_jsonpath_error(
                                "jsonpath array subscript is out of bounds",
                            ));
                        }
                    }
                }
            }
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
                        return Err(exec_jsonpath_error(
                            "jsonpath array subscript is out of bounds",
                        ));
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
                if matches!(
                    resolve_subscript_expr(expr, &subscript_ctx)?,
                    Some(0) | Some(-1)
                ) {
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

fn apply_method_values(
    value: &JsonbValue,
    method: &Method,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    if matches!(method.kind, MethodKind::KeyValue) {
        return apply_keyvalue_method(value, ctx).map_err(|err| err);
    }
    Ok(vec![apply_method(value, method, ctx)?])
}

fn apply_method(
    value: &JsonbValue,
    method: &Method,
    ctx: &RuntimeContext<'_>,
) -> Result<JsonbValue, ExecError> {
    match method.kind {
        MethodKind::Abs => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric.abs())),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .abs() can only be applied to a numeric value",
            )),
        },
        MethodKind::BigInt => apply_bigint_method(value),
        MethodKind::Boolean => apply_boolean_method(value),
        MethodKind::Ceiling => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric_ceiling(numeric))),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .ceiling() can only be applied to a numeric value",
            )),
        },
        MethodKind::Date => {
            datetime_method_no_args(method)?;
            apply_datetime_cast_method(value, ".date()", None, SqlType::new(SqlTypeKind::Date), ctx)
        }
        MethodKind::Decimal => {
            apply_decimal_method(value, numeric_method_args(method, ".decimal()")?)
        }
        MethodKind::Datetime => apply_datetime_method(value, method, ctx),
        MethodKind::Double => apply_double_method(value),
        MethodKind::Floor => match value {
            JsonbValue::Numeric(numeric) => Ok(JsonbValue::Numeric(numeric_floor(numeric))),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .floor() can only be applied to a numeric value",
            )),
        },
        MethodKind::Integer => apply_integer_method(value),
        MethodKind::KeyValue => unreachable!("handled by apply_method_values"),
        MethodKind::LTrim => apply_trim_method(value, method, TrimSide::Left),
        MethodKind::Lower => {
            apply_string_transform_method(value, ".lower()", |text| text.to_lowercase())
        }
        MethodKind::Number => apply_number_method(value, ".number()"),
        MethodKind::BTrim => apply_trim_method(value, method, TrimSide::Both),
        MethodKind::InitCap => apply_string_transform_method(value, ".initcap()", initcap_text),
        MethodKind::Replace => apply_replace_method(value, method),
        MethodKind::RTrim => apply_trim_method(value, method, TrimSide::Right),
        MethodKind::Type => Ok(JsonbValue::String(jsonb_type_name(value).to_string())),
        MethodKind::Size => match value {
            JsonbValue::Array(items) => Ok(numeric_jsonb_from_i32(items.len() as i32)),
            _ if matches!(ctx.mode, PathMode::Lax) => Ok(numeric_jsonb_from_i32(1)),
            _ => Err(exec_jsonpath_error(
                "jsonpath item method .size() can only be applied to an array",
            )),
        },
        MethodKind::SplitPart => apply_split_part_method(value, method),
        MethodKind::String => apply_string_method(value),
        MethodKind::Time => apply_datetime_cast_method(
            value,
            ".time()",
            datetime_method_precision_arg(method, ".time()")?,
            datetime_sql_type(SqlTypeKind::Time, numeric_method_arg(method, 0, ".time()")?),
            ctx,
        ),
        MethodKind::TimeTz => apply_datetime_cast_method(
            value,
            ".time_tz()",
            datetime_method_precision_arg(method, ".time_tz()")?,
            datetime_sql_type(
                SqlTypeKind::TimeTz,
                numeric_method_arg(method, 0, ".time_tz()")?,
            ),
            ctx,
        ),
        MethodKind::Timestamp => apply_datetime_cast_method(
            value,
            ".timestamp()",
            datetime_method_precision_arg(method, ".timestamp()")?,
            datetime_sql_type(
                SqlTypeKind::Timestamp,
                numeric_method_arg(method, 0, ".timestamp()")?,
            ),
            ctx,
        ),
        MethodKind::TimestampTz => apply_datetime_cast_method(
            value,
            ".timestamp_tz()",
            datetime_method_precision_arg(method, ".timestamp_tz()")?,
            datetime_sql_type(
                SqlTypeKind::TimestampTz,
                numeric_method_arg(method, 0, ".timestamp_tz()")?,
            ),
            ctx,
        ),
    }
}

fn method_auto_unwraps_array(method: &Method) -> bool {
    !matches!(method.kind, MethodKind::Size | MethodKind::Type)
}

fn apply_keyvalue_method(
    value: &JsonbValue,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let JsonbValue::Object(items) = value else {
        return Err(exec_jsonpath_error(
            "jsonpath item method .keyvalue() can only be applied to an object",
        ));
    };
    let id = jsonpath_keyvalue_object_id(ctx.global.root, value).unwrap_or(0);
    Ok(items
        .iter()
        .map(|(key, value)| {
            JsonbValue::Object(vec![
                ("key".to_string(), JsonbValue::String(key.clone())),
                ("value".to_string(), value.clone()),
                ("id".to_string(), numeric_jsonb_from_i64(id)),
            ])
        })
        .collect())
}

fn jsonpath_keyvalue_object_id(root: &JsonbValue, target: &JsonbValue) -> Option<i64> {
    find_jsonb_object_offset(root, target, 0).and_then(|offset| i64::try_from(offset).ok())
}

fn find_jsonb_object_offset(
    value: &JsonbValue,
    target: &JsonbValue,
    container_offset: usize,
) -> Option<usize> {
    if matches!(value, JsonbValue::Object(_)) && value == target {
        return Some(container_offset);
    }
    match value {
        JsonbValue::Array(items) => {
            let mut data_offset = container_offset + 4 + items.len() * 4;
            for item in items {
                if let Some(offset) = find_jsonb_object_offset(
                    item,
                    target,
                    jsonb_child_container_offset(item, data_offset),
                ) {
                    return Some(offset);
                }
                data_offset += jsonb_nested_encoded_len_at(item, data_offset);
            }
            None
        }
        JsonbValue::Object(items) => {
            let mut data_offset = container_offset + 4 + items.len() * 8;
            for (key, _) in items {
                data_offset += key.len();
            }
            for (_, item) in items {
                if let Some(offset) = find_jsonb_object_offset(
                    item,
                    target,
                    jsonb_child_container_offset(item, data_offset),
                ) {
                    return Some(offset);
                }
                data_offset += jsonb_nested_encoded_len_at(item, data_offset);
            }
            None
        }
        _ => None,
    }
}

fn jsonb_child_container_offset(value: &JsonbValue, data_offset: usize) -> usize {
    match value {
        JsonbValue::Array(_) | JsonbValue::Object(_) => align4(data_offset),
        _ => data_offset,
    }
}

fn align4(offset: usize) -> usize {
    (offset + 3) & !3
}

#[derive(Debug, Clone, Copy)]
enum TrimSide {
    Left,
    Right,
    Both,
}

fn apply_trim_method(
    value: &JsonbValue,
    method: &Method,
    side: TrimSide,
) -> Result<JsonbValue, ExecError> {
    let chars = match method.args.as_slice() {
        [] => " ",
        [MethodArg::String(chars)] => chars.as_str(),
        [_] | [_, ..] => return Err(exec_jsonpath_error("unsupported jsonpath item method")),
    };
    apply_string_transform_method(value, trim_method_name(side), |text| {
        trim_text_chars(text, chars, side)
    })
}

fn trim_method_name(side: TrimSide) -> &'static str {
    match side {
        TrimSide::Left => ".ltrim()",
        TrimSide::Right => ".rtrim()",
        TrimSide::Both => ".btrim()",
    }
}

fn trim_text_chars(text: &str, chars: &str, side: TrimSide) -> String {
    let should_trim = |ch: char| chars.chars().any(|trim| trim == ch);
    match side {
        TrimSide::Left => text.trim_start_matches(should_trim).to_string(),
        TrimSide::Right => text.trim_end_matches(should_trim).to_string(),
        TrimSide::Both => text.trim_matches(should_trim).to_string(),
    }
}

fn apply_string_transform_method<F>(
    value: &JsonbValue,
    method_name: &str,
    transform: F,
) -> Result<JsonbValue, ExecError>
where
    F: FnOnce(&str) -> String,
{
    let JsonbValue::String(text) = value else {
        return Err(exec_jsonpath_error(&format!(
            "jsonpath item method {method_name} can only be applied to a string"
        )));
    };
    Ok(JsonbValue::String(transform(text)))
}

fn initcap_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut start_word = true;
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            if start_word {
                out.extend(ch.to_uppercase());
                start_word = false;
            } else {
                out.extend(ch.to_lowercase());
            }
        } else {
            start_word = true;
            out.push(ch);
        }
    }
    out
}

fn apply_replace_method(value: &JsonbValue, method: &Method) -> Result<JsonbValue, ExecError> {
    let [MethodArg::String(from), MethodArg::String(to)] = method.args.as_slice() else {
        return Err(exec_jsonpath_error("unsupported jsonpath item method"));
    };
    apply_string_transform_method(value, ".replace()", |text| text.replace(from, to))
}

fn apply_split_part_method(value: &JsonbValue, method: &Method) -> Result<JsonbValue, ExecError> {
    let [MethodArg::String(delim), MethodArg::Numeric(field)] = method.args.as_slice() else {
        return Err(exec_jsonpath_error("unsupported jsonpath item method"));
    };
    let field = field
        .render()
        .parse::<i32>()
        .map_err(|_| exec_jsonpath_error("unsupported jsonpath item method"))?;
    apply_string_transform_method(value, ".split_part()", |text| {
        split_part_text(text, delim, field)
    })
}

fn split_part_text(text: &str, delim: &str, field: i32) -> String {
    if field == 0 || delim.is_empty() {
        return String::new();
    }
    let parts: Vec<&str> = text.split(delim).collect();
    let index = if field > 0 {
        field - 1
    } else {
        parts.len() as i32 + field
    };
    if index < 0 {
        String::new()
    } else {
        parts.get(index as usize).copied().unwrap_or("").to_string()
    }
}

fn numeric_method_args<'a>(
    method: &'a Method,
    method_name: &str,
) -> Result<Vec<&'a NumericValue>, ExecError> {
    method
        .args
        .iter()
        .map(|arg| match arg {
            MethodArg::Numeric(value) => Ok(value),
            MethodArg::String(_) => Err(exec_jsonpath_error(&format!(
                "jsonpath item method {method_name} expects numeric arguments"
            ))),
        })
        .collect()
}

fn numeric_method_arg<'a>(
    method: &'a Method,
    index: usize,
    method_name: &str,
) -> Result<Option<&'a NumericValue>, ExecError> {
    match method.args.get(index) {
        Some(MethodArg::Numeric(value)) => Ok(Some(value)),
        Some(MethodArg::String(_)) => Err(exec_jsonpath_error(&format!(
            "jsonpath item method {method_name} expects numeric arguments"
        ))),
        None => Ok(None),
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

fn apply_bigint_method(value: &JsonbValue) -> Result<JsonbValue, ExecError> {
    let rendered = match value {
        JsonbValue::Numeric(numeric) => numeric
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i64>().ok())
            .map(NumericValue::from_i64)
            .ok_or_else(|| {
                exec_jsonpath_error(&format!(
                    "argument \"{}\" of jsonpath item method .bigint() is invalid for type bigint",
                    numeric.render()
                ))
            })?,
        JsonbValue::String(text) => {
            let parsed = text.parse::<i64>().map_err(|_| {
                exec_jsonpath_error(&format!(
                    "argument \"{text}\" of jsonpath item method .bigint() is invalid for type bigint"
                ))
            })?;
            NumericValue::from_i64(parsed)
        }
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method .bigint() can only be applied to a string or numeric value",
            ));
        }
    };
    Ok(JsonbValue::Numeric(rendered))
}

fn apply_datetime_method(
    value: &JsonbValue,
    method: &Method,
    ctx: &RuntimeContext<'_>,
) -> Result<JsonbValue, ExecError> {
    let JsonbValue::String(text) = value else {
        return Err(exec_jsonpath_error(
            "jsonpath item method .datetime() can only be applied to a string",
        ));
    };
    if let Some(template) = datetime_method_template_arg(method)? {
        return apply_datetime_template_method(text, template, ctx);
    }
    datetime_method_no_args(method)?;
    parse_datetime_source(text, ctx)
        .and_then(|parsed| datetime_jsonb_from_value(parsed.value, parsed.offset_seconds))
        .map_err(|_| ExecError::DetailedError {
            message: format!("datetime format is not recognized: \"{text}\""),
            detail: None,
            hint: Some("Use a datetime template argument to specify the input data format.".into()),
            sqlstate: "22007",
        })
}

fn apply_datetime_cast_method(
    value: &JsonbValue,
    method_name: &str,
    precision: Option<i32>,
    ty: SqlType,
    ctx: &RuntimeContext<'_>,
) -> Result<JsonbValue, ExecError> {
    let JsonbValue::String(text) = value else {
        return Err(exec_jsonpath_error(&format!(
            "jsonpath item method {method_name} can only be applied to a string"
        )));
    };
    let parsed = parse_datetime_source(text, ctx).map_err(|_| ExecError::DetailedError {
        message: format!(
            "{} format is not recognized: \"{text}\"",
            method_name.trim_matches(&['.', '(', ')'][..])
        ),
        detail: None,
        hint: None,
        sqlstate: "22007",
    })?;
    if datetime_target_rejects_source(parsed.kind, ty.kind) {
        return Err(datetime_format_not_recognized_error(method_name, text));
    }
    validate_datetime_timezone_conversion(&parsed, ty.kind, method_name, ctx)?;
    let mut converted = cast_value_with_config(parsed.value, ty, ctx.global.datetime_config)?;
    if let Some(precision) = precision {
        converted = crate::backend::executor::expr_datetime::apply_time_precision(
            converted,
            Some(precision),
        );
    }
    let offset = datetime_output_offset(&converted, parsed.offset_seconds, ty.kind, ctx);
    datetime_jsonb_from_value(converted, offset)
}

#[derive(Debug, Clone)]
struct ParsedDateTimeValue {
    value: Value,
    kind: SqlTypeKind,
    offset_seconds: Option<i32>,
}

fn parse_datetime_source(
    text: &str,
    ctx: &RuntimeContext<'_>,
) -> Result<ParsedDateTimeValue, ExecError> {
    let trimmed = text.trim();
    let has_date = looks_like_date(trimmed);
    let has_time = trimmed.contains(':');
    let offset_seconds = jsonpath_datetime_offset_seconds(trimmed);
    let kind = match (has_date, has_time, offset_seconds.is_some()) {
        (true, true, true) => SqlTypeKind::TimestampTz,
        (true, true, false) => SqlTypeKind::Timestamp,
        (true, false, _) => SqlTypeKind::Date,
        (false, true, true) => SqlTypeKind::TimeTz,
        (false, true, false) => SqlTypeKind::Time,
        _ => return Err(exec_jsonpath_error("datetime format is not recognized")),
    };
    let value = cast_datetime_source_text(trimmed, kind, offset_seconds, ctx)?;
    Ok(ParsedDateTimeValue {
        value,
        kind,
        offset_seconds,
    })
}

fn cast_datetime_source_text(
    text: &str,
    kind: SqlTypeKind,
    offset_seconds: Option<i32>,
    ctx: &RuntimeContext<'_>,
) -> Result<Value, ExecError> {
    match cast_text_value_with_config(text, SqlType::new(kind), true, ctx.global.datetime_config) {
        Ok(value) => Ok(value),
        Err(err) => {
            let Some(normalized) = normalized_iso_datetime_text(text, kind, offset_seconds) else {
                return Err(err);
            };
            cast_text_value_with_config(
                &normalized,
                SqlType::new(kind),
                true,
                ctx.global.datetime_config,
            )
        }
    }
}

fn normalized_iso_datetime_text(
    text: &str,
    kind: SqlTypeKind,
    offset_seconds: Option<i32>,
) -> Option<String> {
    if !matches!(kind, SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz) {
        return None;
    }
    let mut normalized = text.to_string();
    if normalized.as_bytes().get(10) == Some(&b'T') {
        normalized.replace_range(10..11, " ");
    }
    if matches!(kind, SqlTypeKind::TimestampTz) {
        if normalized.ends_with('Z') {
            normalized.truncate(normalized.len() - 1);
            normalized.push_str(" +00");
        } else if normalized.ends_with("EST") {
            normalized.truncate(normalized.len() - 3);
            normalized.push_str(" -05");
        } else if let Some(offset_seconds) = offset_seconds {
            if let Some(idx) = jsonpath_datetime_offset_start(&normalized) {
                normalized.truncate(idx);
                normalized.push(' ');
                normalized.push_str(&jsonpath_offset_literal(offset_seconds));
            }
        }
    }
    (normalized != text).then_some(normalized)
}

fn jsonpath_datetime_offset_start(text: &str) -> Option<usize> {
    for (idx, ch) in text.char_indices().rev() {
        if ch == '+' || ch == '-' {
            if text[..idx].contains(':') {
                return Some(idx);
            }
            return None;
        }
    }
    None
}

fn jsonpath_offset_literal(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let mut remaining = offset_seconds.abs();
    let hour = remaining / 3600;
    remaining %= 3600;
    let minute = remaining / 60;
    if minute == 0 {
        format!("{sign}{hour}")
    } else {
        format!("{sign}{hour}:{minute:02}")
    }
}

fn looks_like_date(text: &str) -> bool {
    let mut parts = text.splitn(4, '-');
    let Some(year) = parts.next() else {
        return false;
    };
    let Some(month) = parts.next() else {
        return false;
    };
    let Some(day_and_rest) = parts.next() else {
        return false;
    };
    let day = day_and_rest
        .get(..2)
        .filter(|day| day.as_bytes().iter().all(u8::is_ascii_digit));
    !year.is_empty()
        && year.as_bytes().iter().all(u8::is_ascii_digit)
        && month.len() == 2
        && month.as_bytes().iter().all(u8::is_ascii_digit)
        && day.is_some()
}

fn jsonpath_datetime_offset_seconds(text: &str) -> Option<i32> {
    if text.ends_with('Z') && text[..text.len().saturating_sub(1)].contains(':') {
        return Some(0);
    }
    if text.ends_with("EST") && text[..text.len().saturating_sub(3)].contains(':') {
        return Some(-5 * 3600);
    }
    for (idx, ch) in text.char_indices().rev() {
        if ch != '+' && ch != '-' {
            continue;
        }
        if !text[..idx].contains(':') {
            continue;
        }
        let rest = &text[idx + 1..];
        let mut parts = rest.split(':');
        let hour = parts.next()?.parse::<i32>().ok()?;
        let minute = parts
            .next()
            .map(str::parse::<i32>)
            .transpose()
            .ok()?
            .unwrap_or(0);
        if parts.next().is_some() {
            return None;
        }
        let seconds = hour * 3600 + minute * 60;
        return Some(if ch == '-' { -seconds } else { seconds });
    }
    None
}

fn datetime_target_rejects_source(source: SqlTypeKind, target: SqlTypeKind) -> bool {
    match target {
        SqlTypeKind::Date => matches!(source, SqlTypeKind::Time | SqlTypeKind::TimeTz),
        SqlTypeKind::Time => matches!(source, SqlTypeKind::Date),
        SqlTypeKind::TimeTz => matches!(source, SqlTypeKind::Date | SqlTypeKind::Timestamp),
        SqlTypeKind::Timestamp => matches!(source, SqlTypeKind::Time | SqlTypeKind::TimeTz),
        SqlTypeKind::TimestampTz => matches!(source, SqlTypeKind::Time | SqlTypeKind::TimeTz),
        _ => false,
    }
}

fn datetime_format_not_recognized_error(method_name: &str, text: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "{} format is not recognized: \"{text}\"",
            method_name.trim_matches(&['.', '(', ')'][..])
        ),
        detail: None,
        hint: None,
        sqlstate: "22007",
    }
}

fn validate_datetime_timezone_conversion(
    parsed: &ParsedDateTimeValue,
    target: SqlTypeKind,
    _method_name: &str,
    ctx: &RuntimeContext<'_>,
) -> Result<(), ExecError> {
    if ctx.global.allow_timezone {
        return Ok(());
    }
    let source_has_tz = matches!(parsed.kind, SqlTypeKind::TimeTz | SqlTypeKind::TimestampTz);
    let target_has_tz = matches!(target, SqlTypeKind::TimeTz | SqlTypeKind::TimestampTz);
    if source_has_tz && !target_has_tz {
        return Err(timezone_usage_error(parsed.kind, target));
    }
    if !source_has_tz && target_has_tz && parsed.kind != target {
        return Err(timezone_usage_error(parsed.kind, target));
    }
    Ok(())
}

fn timezone_usage_error(source: SqlTypeKind, target: SqlTypeKind) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "cannot convert value from {} to {} without time zone usage",
            datetime_method_target_name(source),
            datetime_method_target_name(target)
        ),
        detail: None,
        hint: Some("Use *_tz() function for time zone support.".into()),
        sqlstate: "2202E",
    }
}

fn datetime_output_offset(
    value: &Value,
    parsed_offset: Option<i32>,
    target: SqlTypeKind,
    ctx: &RuntimeContext<'_>,
) -> Option<i32> {
    if target != SqlTypeKind::TimestampTz {
        return None;
    }
    parsed_offset.or_else(|| match value {
        Value::TimestampTz(v) => Some(timezone_offset_seconds_at_utc(
            ctx.global.datetime_config,
            v.0,
        )),
        _ => None,
    })
}

fn datetime_jsonb_from_value(
    value: Value,
    offset_seconds: Option<i32>,
) -> Result<JsonbValue, ExecError> {
    Ok(match value {
        Value::Date(v) => JsonbValue::Date(v),
        Value::Time(v) => JsonbValue::Time(v),
        Value::TimeTz(v) => JsonbValue::TimeTz(v),
        Value::Timestamp(v) => JsonbValue::Timestamp(v),
        Value::TimestampTz(v) => match offset_seconds {
            Some(offset_seconds) => JsonbValue::TimestampTzWithOffset(v, offset_seconds),
            None => JsonbValue::TimestampTz(v),
        },
        _ => {
            return Err(exec_jsonpath_error(
                "jsonpath item method produced non-datetime result",
            ));
        }
    })
}

fn datetime_method_no_args(method: &Method) -> Result<(), ExecError> {
    if method.args.is_empty() {
        Ok(())
    } else {
        Err(exec_jsonpath_error("unsupported jsonpath item method"))
    }
}

fn datetime_method_precision_arg(
    method: &Method,
    method_name: &str,
) -> Result<Option<i32>, ExecError> {
    match method.args.len() {
        0 => Ok(None),
        1 => Ok(Some(datetime_precision_arg_to_i32(
            numeric_method_arg(method, 0, method_name)?.expect("single arg present"),
            method_name,
        )?)),
        _ => Err(exec_jsonpath_error("unsupported jsonpath item method")),
    }
}

fn datetime_precision_arg_to_i32(
    value: &NumericValue,
    method_name: &str,
) -> Result<i32, ExecError> {
    let parsed = value.render().parse::<i32>().map_err(|_| {
        exec_jsonpath_error(&format!(
            "time precision of jsonpath item method {method_name} is out of range for type integer"
        ))
    })?;
    if parsed > 6 {
        push_datetime_precision_warning(method_name, parsed);
        return Ok(6);
    }
    if parsed < 0 {
        return Err(exec_jsonpath_error(&format!(
            "time precision of jsonpath item method {method_name} is out of range for type integer"
        )));
    }
    Ok(parsed)
}

fn push_datetime_precision_warning(method_name: &str, precision: i32) {
    let type_name = match method_name {
        ".time()" => "TIME",
        ".time_tz()" => "TIME",
        ".timestamp()" => "TIMESTAMP",
        ".timestamp_tz()" => "TIMESTAMP",
        _ => return,
    };
    let tz_suffix = match method_name {
        ".time_tz()" | ".timestamp_tz()" => " WITH TIME ZONE",
        _ => "",
    };
    push_warning(format!(
        "{type_name}({precision}){tz_suffix} precision reduced to maximum allowed, 6"
    ));
}

fn datetime_sql_type(kind: SqlTypeKind, precision: Option<&NumericValue>) -> SqlType {
    match precision.and_then(|value| value.render().parse::<i32>().ok()) {
        Some(precision) => SqlType::with_time_precision(kind, precision),
        None => SqlType::new(kind),
    }
}

fn datetime_method_target_name(kind: SqlTypeKind) -> &'static str {
    match kind {
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time",
        SqlTypeKind::TimeTz => "timetz",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::TimestampTz => "timestamptz",
        _ => unreachable!("datetime target type"),
    }
}

#[derive(Debug, Clone)]
enum DateTimeTemplateItem {
    Year4,
    Month2,
    Day2,
    Hour24,
    Minute,
    Second,
    TzHour,
    TzMinute,
    Literal(String),
}

fn datetime_method_template_arg<'a>(method: &'a Method) -> Result<Option<&'a str>, ExecError> {
    match method.args.as_slice() {
        [] => Ok(None),
        [MethodArg::String(value)] => Ok(Some(value.as_str())),
        [_] => Err(exec_jsonpath_error("unsupported jsonpath item method")),
        _ => Err(exec_jsonpath_error("unsupported jsonpath item method")),
    }
}

fn apply_datetime_template_method(
    text: &str,
    template: &str,
    ctx: &RuntimeContext<'_>,
) -> Result<JsonbValue, ExecError> {
    let items = parse_datetime_template(template)?;
    let mut offset = 0usize;
    let mut year = None;
    let mut month = None;
    let mut day = None;
    let mut hour = None;
    let mut minute = None;
    let mut second = None;
    let mut tz_hour = None;
    let mut tz_minute = None;

    for item in items {
        match item {
            DateTimeTemplateItem::Year4 => {
                year = Some(parse_template_digits(text, &mut offset, 4, "YYYY")?);
            }
            DateTimeTemplateItem::Month2 => {
                month = Some(parse_template_digits(text, &mut offset, 2, "MM")?);
            }
            DateTimeTemplateItem::Day2 => {
                day = Some(parse_template_digits(text, &mut offset, 2, "DD")?);
            }
            DateTimeTemplateItem::Hour24 => {
                hour = Some(parse_template_digits(text, &mut offset, 2, "HH24")?);
            }
            DateTimeTemplateItem::Minute => {
                minute = Some(parse_template_digits(text, &mut offset, 2, "MI")?);
            }
            DateTimeTemplateItem::Second => {
                second = Some(parse_template_digits(text, &mut offset, 2, "SS")?);
            }
            DateTimeTemplateItem::TzHour => {
                let (parsed, consumed) = parse_template_tz_hour(&text[offset..])?;
                tz_hour = Some(parsed);
                offset += consumed;
            }
            DateTimeTemplateItem::TzMinute => {
                tz_minute = Some(parse_template_digits(text, &mut offset, 2, "TZM")?);
            }
            DateTimeTemplateItem::Literal(literal) => {
                if !text[offset..].starts_with(&literal) {
                    if offset >= text.len() {
                        return Err(ExecError::DetailedError {
                            message: "input string is too short for datetime format".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "22007",
                        });
                    }
                    return Err(ExecError::DetailedError {
                        message: format!("unmatched format character \"{literal}\""),
                        detail: None,
                        hint: None,
                        sqlstate: "22007",
                    });
                }
                offset += literal.len();
            }
        }
    }

    if offset != text.len() {
        return Err(ExecError::DetailedError {
            message: "trailing characters remain in input string after datetime format".into(),
            detail: None,
            hint: None,
            sqlstate: "22007",
        });
    }

    let rendered = if let (Some(year), Some(month), Some(day)) = (year, month, day) {
        if hour.is_some()
            || minute.is_some()
            || second.is_some()
            || tz_hour.is_some()
            || tz_minute.is_some()
        {
            let offset = render_template_offset(tz_hour, tz_minute);
            if let Some(offset) = offset {
                (
                    SqlType::new(SqlTypeKind::TimestampTz),
                    format!(
                        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}{offset}",
                        hour.unwrap_or(0),
                        minute.unwrap_or(0),
                        second.unwrap_or(0)
                    ),
                )
            } else {
                (
                    SqlType::new(SqlTypeKind::Timestamp),
                    format!(
                        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
                        hour.unwrap_or(0),
                        minute.unwrap_or(0),
                        second.unwrap_or(0)
                    ),
                )
            }
        } else {
            (
                SqlType::new(SqlTypeKind::Date),
                format!("{year:04}-{month:02}-{day:02}"),
            )
        }
    } else {
        let offset = render_template_offset(tz_hour, tz_minute);
        if let Some(offset) = offset {
            (
                SqlType::new(SqlTypeKind::TimeTz),
                format!(
                    "{:02}:{:02}:{:02}{offset}",
                    hour.unwrap_or(0),
                    minute.unwrap_or(0),
                    second.unwrap_or(0)
                ),
            )
        } else {
            (
                SqlType::new(SqlTypeKind::Time),
                format!(
                    "{:02}:{:02}:{:02}",
                    hour.unwrap_or(0),
                    minute.unwrap_or(0),
                    second.unwrap_or(0)
                ),
            )
        }
    };

    let offset_seconds = template_offset_seconds(tz_hour, tz_minute);
    cast_text_value_with_config(&rendered.1, rendered.0, true, ctx.global.datetime_config)
        .and_then(|value| datetime_jsonb_from_value(value, offset_seconds))
}

fn template_offset_seconds(hour: Option<i32>, minute: Option<i32>) -> Option<i32> {
    let hour = hour?;
    let minute = minute.unwrap_or(0);
    Some(if hour < 0 {
        hour * 3600 - minute * 60
    } else {
        hour * 3600 + minute * 60
    })
}

fn parse_datetime_template(template: &str) -> Result<Vec<DateTimeTemplateItem>, ExecError> {
    let mut items = Vec::new();
    let mut offset = 0usize;
    while offset < template.len() {
        let rest = &template[offset..];
        if let Some(literal) = rest.strip_prefix('"') {
            let Some(end) = literal.find('"') else {
                return Err(exec_jsonpath_error("unterminated jsonpath string"));
            };
            let lit = &literal[..end];
            if !lit.is_empty() {
                items.push(DateTimeTemplateItem::Literal(lit.to_string()));
            }
            offset += end + 2;
            continue;
        }
        if let Some((item, consumed)) = parse_datetime_template_token(rest) {
            items.push(item);
            offset += consumed;
            continue;
        }
        let ch = rest.chars().next().unwrap();
        if ch.is_ascii_alphabetic() {
            return Err(exec_jsonpath_error(&format!(
                "invalid datetime format separator: \"{ch}\""
            )));
        }
        items.push(DateTimeTemplateItem::Literal(ch.to_string()));
        offset += ch.len_utf8();
    }
    Ok(items)
}

fn parse_datetime_template_token(rest: &str) -> Option<(DateTimeTemplateItem, usize)> {
    let upper = rest.to_ascii_uppercase();
    for (name, item) in [
        ("YYYY", DateTimeTemplateItem::Year4),
        ("HH24", DateTimeTemplateItem::Hour24),
        ("TZH", DateTimeTemplateItem::TzHour),
        ("TZM", DateTimeTemplateItem::TzMinute),
        ("DD", DateTimeTemplateItem::Day2),
        ("MM", DateTimeTemplateItem::Month2),
        ("MI", DateTimeTemplateItem::Minute),
        ("SS", DateTimeTemplateItem::Second),
    ] {
        if upper.starts_with(name) {
            return Some((item, name.len()));
        }
    }
    None
}

fn parse_template_digits(
    text: &str,
    offset: &mut usize,
    len: usize,
    token: &str,
) -> Result<i32, ExecError> {
    let end = (*offset).saturating_add(len).min(text.len());
    let raw = &text[*offset..end];
    if raw.len() != len || !raw.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ExecError::DetailedError {
            message: format!("invalid value \"{raw}\" for \"{token}\""),
            detail: Some("Value must be an integer.".into()),
            hint: None,
            sqlstate: "22007",
        });
    }
    *offset += len;
    raw.parse::<i32>().map_err(|_| ExecError::DetailedError {
        message: format!("invalid value \"{raw}\" for \"{token}\""),
        detail: Some("Value must be an integer.".into()),
        hint: None,
        sqlstate: "22007",
    })
}

fn parse_template_tz_hour(text: &str) -> Result<(i32, usize), ExecError> {
    let mut chars = text.chars();
    let Some(sign) = chars.next() else {
        return Err(ExecError::DetailedError {
            message: "invalid value \"\" for \"TZH\"".into(),
            detail: Some("Value must be an integer.".into()),
            hint: None,
            sqlstate: "22007",
        });
    };
    if sign != '+' && sign != '-' {
        return Err(ExecError::DetailedError {
            message: format!("invalid value \"{sign}\" for \"TZH\""),
            detail: Some("Value must be an integer.".into()),
            hint: None,
            sqlstate: "22007",
        });
    }
    let digits: String = chars.take_while(|ch| ch.is_ascii_digit()).take(2).collect();
    if digits.is_empty() {
        return Err(ExecError::DetailedError {
            message: format!("invalid value \"{sign}\" for \"TZH\""),
            detail: Some("Value must be an integer.".into()),
            hint: None,
            sqlstate: "22007",
        });
    }
    let parsed = digits
        .parse::<i32>()
        .map_err(|_| ExecError::DetailedError {
            message: format!("invalid value \"{sign}{digits}\" for \"TZH\""),
            detail: Some("Value must be an integer.".into()),
            hint: None,
            sqlstate: "22007",
        })?;
    let value = if sign == '-' { -parsed } else { parsed };
    Ok((value, 1 + digits.len()))
}

fn render_template_offset(hour: Option<i32>, minute: Option<i32>) -> Option<String> {
    let hour = hour?;
    let sign = if hour < 0 { '-' } else { '+' };
    let hour = hour.abs();
    match minute {
        Some(minute) => Some(format!("{sign}{hour:02}:{minute:02}")),
        None => Some(format!("{sign}{hour:02}")),
    }
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

fn apply_decimal_method(
    value: &JsonbValue,
    args: Vec<&NumericValue>,
) -> Result<JsonbValue, ExecError> {
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
    let precision = decimal_arg_to_i32(args[0], "precision")?;
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
    value.render().parse::<i32>().map_err(|_| {
        exec_jsonpath_error(&format!(
            "{label} of jsonpath item method .decimal() is out of range for type integer"
        ))
    })
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
        JsonbValue::Date(_)
        | JsonbValue::Time(_)
        | JsonbValue::TimeTz(_)
        | JsonbValue::Timestamp(_)
        | JsonbValue::TimestampTz(_)
        | JsonbValue::TimestampTzWithOffset(_, _) => render_temporal_jsonb_value(value),
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
    ctx: &RuntimeContext<'_>,
) -> Result<PredicateValue, ExecError> {
    let mut found = false;
    let mut unknown = false;
    for left_value in left {
        for right_value in right {
            match compare_values(left_value, right_value, op, ctx)? {
                PredicateValue::True => {
                    if matches!(ctx.mode, PathMode::Lax) {
                        return Ok(PredicateValue::True);
                    }
                    found = true;
                }
                PredicateValue::Unknown => {
                    if matches!(ctx.mode, PathMode::Strict) {
                        return Ok(PredicateValue::Unknown);
                    }
                    unknown = true;
                }
                PredicateValue::False => {}
            }
        }
    }
    Ok(if found {
        PredicateValue::True
    } else if unknown {
        PredicateValue::Unknown
    } else {
        PredicateValue::False
    })
}

fn starts_with_any_pair(
    left: &[JsonbValue],
    right: &[JsonbValue],
    ctx: &RuntimeContext<'_>,
) -> PredicateValue {
    let mut found = false;
    let mut unknown = false;
    for left_value in left {
        for right_value in right {
            match starts_with_values(left_value, right_value) {
                PredicateValue::True => {
                    if matches!(ctx.mode, PathMode::Lax) {
                        return PredicateValue::True;
                    }
                    found = true;
                }
                PredicateValue::Unknown => {
                    if matches!(ctx.mode, PathMode::Strict) {
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
    ctx: &RuntimeContext<'_>,
) -> Result<PredicateValue, ExecError> {
    let mut found = false;
    let mut unknown = false;
    for value in values {
        match like_regex_value(value, pattern, flags)? {
            PredicateValue::True => {
                if matches!(ctx.mode, PathMode::Lax) {
                    return Ok(PredicateValue::True);
                }
                found = true;
            }
            PredicateValue::Unknown => {
                if matches!(ctx.mode, PathMode::Strict) {
                    return Ok(PredicateValue::Unknown);
                }
                unknown = true;
            }
            PredicateValue::False => {}
        }
    }
    Ok(if found {
        PredicateValue::True
    } else if unknown {
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

fn compare_values(
    left: &JsonbValue,
    right: &JsonbValue,
    op: CompareOp,
    ctx: &RuntimeContext<'_>,
) -> Result<PredicateValue, ExecError> {
    if matches!(left, JsonbValue::Array(_) | JsonbValue::Object(_))
        || matches!(right, JsonbValue::Array(_) | JsonbValue::Object(_))
    {
        return Ok(PredicateValue::Unknown);
    }
    if matches!((left, right), (JsonbValue::Null, _) | (_, JsonbValue::Null)) {
        return Ok(match (left, right, op) {
            (JsonbValue::Null, JsonbValue::Null, CompareOp::Eq) => PredicateValue::True,
            (JsonbValue::Null, JsonbValue::Null, CompareOp::NotEq) => PredicateValue::False,
            (JsonbValue::Null, JsonbValue::Null, _) => PredicateValue::Unknown,
            (_, _, CompareOp::Eq) => PredicateValue::False,
            (_, _, CompareOp::NotEq) => PredicateValue::True,
            _ => PredicateValue::Unknown,
        });
    }
    if let Some(ordering) = compare_datetime_values(left, right, ctx)? {
        return Ok(predicate_from_ordering(ordering, op));
    }
    if !same_jsonb_type(left, right) {
        return Ok(PredicateValue::Unknown);
    }
    let ordering = compare_jsonb(left, right);
    Ok(predicate_from_ordering(ordering, op))
}

fn predicate_from_ordering(ordering: Ordering, op: CompareOp) -> PredicateValue {
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

fn compare_datetime_values(
    left: &JsonbValue,
    right: &JsonbValue,
    ctx: &RuntimeContext<'_>,
) -> Result<Option<Ordering>, ExecError> {
    let Some(left) = jsonb_temporal_to_value(left) else {
        return Ok(None);
    };
    let Some(right) = jsonb_temporal_to_value(right) else {
        return Ok(None);
    };
    compare_datetime_value_pair(left, right, ctx)
}

fn compare_datetime_value_pair(
    left: Value,
    right: Value,
    ctx: &RuntimeContext<'_>,
) -> Result<Option<Ordering>, ExecError> {
    if let Some(ordering) =
        mixed_date_timestamp_ordering(&left, &right, Some(ctx.global.datetime_config))
    {
        if matches!(
            (&left, &right),
            (Value::Date(_), Value::TimestampTz(_)) | (Value::TimestampTz(_), Value::Date(_))
        ) {
            ensure_datetime_timezone_is_allowed(SqlTypeKind::Date, SqlTypeKind::TimestampTz, ctx)?;
        }
        return Ok(Some(ordering));
    }
    Ok(Some(match (left, right) {
        (Value::Date(left), Value::Date(right)) => left.0.cmp(&right.0),
        (Value::Date(left), Value::Timestamp(right)) => datetime_sort_key(&cast_datetime_value(
            Value::Date(left),
            SqlTypeKind::Timestamp,
            ctx,
        )?)
        .cmp(&datetime_sort_key(&Value::Timestamp(right))),
        (Value::Timestamp(left), Value::Date(right)) => {
            datetime_sort_key(&Value::Timestamp(left)).cmp(&datetime_sort_key(
                &cast_datetime_value(Value::Date(right), SqlTypeKind::Timestamp, ctx)?,
            ))
        }
        (Value::Date(left), Value::TimestampTz(right)) => {
            ensure_datetime_timezone_is_allowed(SqlTypeKind::Date, SqlTypeKind::TimestampTz, ctx)?;
            datetime_sort_key(&cast_datetime_value(
                Value::Date(left),
                SqlTypeKind::TimestampTz,
                ctx,
            )?)
            .cmp(&datetime_sort_key(&Value::TimestampTz(right)))
        }
        (Value::TimestampTz(left), Value::Date(right)) => {
            ensure_datetime_timezone_is_allowed(SqlTypeKind::Date, SqlTypeKind::TimestampTz, ctx)?;
            datetime_sort_key(&Value::TimestampTz(left)).cmp(&datetime_sort_key(
                &cast_datetime_value(Value::Date(right), SqlTypeKind::TimestampTz, ctx)?,
            ))
        }
        (Value::Time(left), Value::Time(right)) => left.0.cmp(&right.0),
        (Value::Time(left), Value::TimeTz(right)) => {
            ensure_datetime_timezone_is_allowed(SqlTypeKind::Time, SqlTypeKind::TimeTz, ctx)?;
            compare_timetz_values(cast_timetz_value(Value::Time(left), ctx)?, right)
        }
        (Value::TimeTz(left), Value::Time(right)) => {
            ensure_datetime_timezone_is_allowed(SqlTypeKind::Time, SqlTypeKind::TimeTz, ctx)?;
            compare_timetz_values(left, cast_timetz_value(Value::Time(right), ctx)?)
        }
        (Value::TimeTz(left), Value::TimeTz(right)) => compare_timetz_values(left, right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.0.cmp(&right.0),
        (Value::Timestamp(left), Value::TimestampTz(right)) => {
            ensure_datetime_timezone_is_allowed(
                SqlTypeKind::Timestamp,
                SqlTypeKind::TimestampTz,
                ctx,
            )?;
            datetime_sort_key(&cast_datetime_value(
                Value::Timestamp(left),
                SqlTypeKind::TimestampTz,
                ctx,
            )?)
            .cmp(&datetime_sort_key(&Value::TimestampTz(right)))
        }
        (Value::TimestampTz(left), Value::Timestamp(right)) => {
            ensure_datetime_timezone_is_allowed(
                SqlTypeKind::Timestamp,
                SqlTypeKind::TimestampTz,
                ctx,
            )?;
            datetime_sort_key(&Value::TimestampTz(left)).cmp(&datetime_sort_key(
                &cast_datetime_value(Value::Timestamp(right), SqlTypeKind::TimestampTz, ctx)?,
            ))
        }
        (Value::TimestampTz(left), Value::TimestampTz(right)) => left.0.cmp(&right.0),
        _ => return Ok(None),
    }))
}

fn ensure_datetime_timezone_is_allowed(
    source: SqlTypeKind,
    target: SqlTypeKind,
    ctx: &RuntimeContext<'_>,
) -> Result<(), ExecError> {
    if ctx.global.allow_timezone {
        Ok(())
    } else {
        Err(timezone_usage_error(source, target))
    }
}

fn cast_datetime_value(
    value: Value,
    target: SqlTypeKind,
    ctx: &RuntimeContext<'_>,
) -> Result<Value, ExecError> {
    cast_value_with_config(value, SqlType::new(target), ctx.global.datetime_config)
}

fn cast_timetz_value(value: Value, ctx: &RuntimeContext<'_>) -> Result<TimeTzADT, ExecError> {
    match cast_datetime_value(value, SqlTypeKind::TimeTz, ctx)? {
        Value::TimeTz(value) => Ok(value),
        _ => Err(exec_jsonpath_error(
            "jsonpath datetime cast produced non-timetz result",
        )),
    }
}

fn datetime_sort_key(value: &Value) -> i64 {
    match value {
        Value::Timestamp(value) => value.0,
        Value::TimestampTz(value) => value.0,
        _ => unreachable!("datetime_sort_key expects timestamp value"),
    }
}

fn compare_timetz_values(left: TimeTzADT, right: TimeTzADT) -> Ordering {
    timetz_sort_key(left)
        .cmp(&timetz_sort_key(right))
        .then_with(|| (-left.offset_seconds).cmp(&(-right.offset_seconds)))
}

fn timetz_sort_key(value: TimeTzADT) -> i64 {
    value.time.0 - i64::from(value.offset_seconds) * USECS_PER_SEC
}

fn same_jsonb_type(left: &JsonbValue, right: &JsonbValue) -> bool {
    matches!(
        (left, right),
        (JsonbValue::Null, JsonbValue::Null)
            | (JsonbValue::String(_), JsonbValue::String(_))
            | (JsonbValue::Numeric(_), JsonbValue::Numeric(_))
            | (JsonbValue::Bool(_), JsonbValue::Bool(_))
            | (JsonbValue::Date(_), JsonbValue::Date(_))
            | (JsonbValue::Time(_), JsonbValue::Time(_))
            | (JsonbValue::TimeTz(_), JsonbValue::TimeTz(_))
            | (JsonbValue::Timestamp(_), JsonbValue::Timestamp(_))
            | (JsonbValue::TimestampTz(_), JsonbValue::TimestampTz(_))
            | (
                JsonbValue::TimestampTz(_),
                JsonbValue::TimestampTzWithOffset(_, _)
            )
            | (
                JsonbValue::TimestampTzWithOffset(_, _),
                JsonbValue::TimestampTz(_)
            )
            | (
                JsonbValue::TimestampTzWithOffset(_, _),
                JsonbValue::TimestampTzWithOffset(_, _),
            )
            | (JsonbValue::Array(_), JsonbValue::Array(_))
            | (JsonbValue::Object(_), JsonbValue::Object(_))
    )
}

fn jsonb_temporal_to_value(value: &JsonbValue) -> Option<Value> {
    match value {
        JsonbValue::Date(v) => Some(Value::Date(*v)),
        JsonbValue::Time(v) => Some(Value::Time(*v)),
        JsonbValue::TimeTz(v) => Some(Value::TimeTz(*v)),
        JsonbValue::Timestamp(v) => Some(Value::Timestamp(*v)),
        JsonbValue::TimestampTz(v) => Some(Value::TimestampTz(*v)),
        JsonbValue::TimestampTzWithOffset(v, _) => Some(Value::TimestampTz(*v)),
        _ => None,
    }
}

fn eval_arithmetic_operands(
    left: &[JsonbValue],
    right: &[JsonbValue],
    op: ArithmeticOp,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let left_values = arithmetic_operand_values(left, ctx);
    let right_values = arithmetic_operand_values(right, ctx);
    let left = singleton_numeric_operand(&left_values, "left", op)?;
    let right = singleton_numeric_operand(&right_values, "right", op)?;
    eval_arithmetic_pair(left, right, op).map(|value| vec![value])
}

fn eval_arithmetic_pair(
    left: NumericValue,
    right: NumericValue,
    op: ArithmeticOp,
) -> Result<JsonbValue, ExecError> {
    let value = match op {
        ArithmeticOp::Add => left.add(&right),
        ArithmeticOp::Sub => left.sub(&right),
        ArithmeticOp::Mul => left.mul(&right),
        ArithmeticOp::Div => left
            .div(&right, 16)
            .ok_or_else(|| exec_jsonpath_error("division by zero"))?,
        ArithmeticOp::Mod => numeric_remainder(&left, &right)
            .ok_or_else(|| exec_jsonpath_error("division by zero"))?,
    };
    Ok(JsonbValue::Numeric(value))
}

fn arithmetic_operand_values(values: &[JsonbValue], ctx: &RuntimeContext<'_>) -> Vec<JsonbValue> {
    if matches!(ctx.mode, PathMode::Strict) {
        return values.to_vec();
    }
    values.iter().fold(Vec::new(), |mut out, value| {
        match value {
            JsonbValue::Array(items) => out.extend(items.iter().cloned()),
            _ => out.push(value.clone()),
        }
        out
    })
}

fn singleton_numeric_operand(
    values: &[JsonbValue],
    side: &str,
    op: ArithmeticOp,
) -> Result<NumericValue, ExecError> {
    let [value] = values else {
        return Err(singleton_numeric_operand_error(side, op));
    };
    match value {
        JsonbValue::Numeric(numeric) => Ok(numeric.clone()),
        _ => Err(singleton_numeric_operand_error(side, op)),
    }
}

fn singleton_numeric_operand_error(side: &str, op: ArithmeticOp) -> ExecError {
    exec_jsonpath_error(&format!(
        "{side} operand of jsonpath operator {} is not a single numeric value",
        arithmetic_op_symbol(op)
    ))
}

fn arithmetic_op_symbol(op: ArithmeticOp) -> &'static str {
    match op {
        ArithmeticOp::Add => "+",
        ArithmeticOp::Sub => "-",
        ArithmeticOp::Mul => "*",
        ArithmeticOp::Div => "/",
        ArithmeticOp::Mod => "%",
    }
}

fn eval_unary_values(
    values: &[JsonbValue],
    op: UnaryOp,
    ctx: &RuntimeContext<'_>,
) -> Result<Vec<JsonbValue>, ExecError> {
    let values = arithmetic_operand_values(values, ctx);
    let mut out = Vec::new();
    for value in values {
        match value {
            JsonbValue::Numeric(numeric) => out.push(JsonbValue::Numeric(match op {
                UnaryOp::Plus => numeric,
                UnaryOp::Minus => numeric.negate(),
            })),
            _ if ctx.global.silent
                && matches!(ctx.mode, PathMode::Lax)
                && ctx.global.preserve_unary_prefix
                && !out.is_empty() =>
            {
                break;
            }
            _ if ctx.global.silent
                && matches!(ctx.mode, PathMode::Lax)
                && !ctx.global.preserve_unary_prefix => {}
            _ => return Err(unary_numeric_operand_error(op)),
        }
    }
    Ok(out)
}

fn unary_numeric_operand_error(op: UnaryOp) -> ExecError {
    exec_jsonpath_error(&format!(
        "operand of unary jsonpath operator {} is not a numeric value",
        unary_op_symbol(op)
    ))
}

fn unary_op_symbol(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Plus => "+",
        UnaryOp::Minus => "-",
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
            truncated.try_into().map_err(|_| {
                exec_jsonpath_error("jsonpath array subscript is out of integer range")
            })
        }
        _ => Err(exec_jsonpath_error(
            "jsonpath array subscript is out of integer range",
        )),
    }
}

fn numeric_jsonb_from_i32(value: i32) -> JsonbValue {
    JsonbValue::Numeric(NumericValue::finite(num_bigint::BigInt::from(value), 0))
}

fn numeric_jsonb_from_i64(value: i64) -> JsonbValue {
    JsonbValue::Numeric(NumericValue::finite(num_bigint::BigInt::from(value), 0))
}

fn reject_nan_or_infinity(value: &NumericValue, method_name: &str) -> Result<(), ExecError> {
    match value {
        NumericValue::NaN | NumericValue::PosInf | NumericValue::NegInf => {
            Err(exec_jsonpath_error(&format!(
                "NaN or Infinity is not allowed for jsonpath item method {method_name}"
            )))
        }
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
        JsonbValue::Date(_) => "date",
        JsonbValue::Time(_) => "time without time zone",
        JsonbValue::TimeTz(_) => "time with time zone",
        JsonbValue::Timestamp(_) => "timestamp without time zone",
        JsonbValue::TimestampTz(_) | JsonbValue::TimestampTzWithOffset(_, _) => {
            "timestamp with time zone"
        }
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
    render_expr_with_brackets(&path.expr, &mut out, true);
    out
}

fn render_expr(expr: &Expr, out: &mut String) {
    render_expr_with_brackets(expr, out, false);
}

fn render_expr_with_brackets(expr: &Expr, out: &mut String, print_brackets: bool) {
    if let Some(value) = folded_unary_numeric(expr) {
        out.push_str(&value.render());
        return;
    }

    let wrap = print_brackets && is_bracketed_operation(expr);
    if wrap {
        out.push('(');
    }

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
            render_binary_left(expr, left, out);
            out.push_str(match op {
                CompareOp::Eq => " == ",
                CompareOp::NotEq => " != ",
                CompareOp::Lt => " < ",
                CompareOp::LtEq => " <= ",
                CompareOp::Gt => " > ",
                CompareOp::GtEq => " >= ",
            });
            render_binary_right(expr, right, out);
        }
        Expr::StartsWith { left, right } => {
            render_binary_left(expr, left, out);
            out.push_str(" starts with ");
            render_binary_right(expr, right, out);
        }
        Expr::LikeRegex {
            expr: regex_expr,
            pattern,
            flags,
        } => {
            render_binary_left(expr, regex_expr, out);
            out.push_str(" like_regex ");
            render_quoted_string(pattern, out);
            let flags = normalize_like_regex_flags(flags);
            if !flags.is_empty() {
                out.push_str(" flag ");
                render_quoted_string(&flags, out);
            }
        }
        Expr::Arithmetic { op, left, right } => {
            render_binary_left(expr, left, out);
            out.push_str(match op {
                ArithmeticOp::Add => " + ",
                ArithmeticOp::Sub => " - ",
                ArithmeticOp::Mul => " * ",
                ArithmeticOp::Div => " / ",
                ArithmeticOp::Mod => " % ",
            });
            render_binary_right(expr, right, out);
        }
        Expr::Unary { op, inner } => {
            out.push(match op {
                UnaryOp::Plus => '+',
                UnaryOp::Minus => '-',
            });
            render_child_expr(expr, inner, out);
        }
        Expr::MethodCall { inner, method } => {
            render_chain_base(inner, out);
            render_method(method, out);
        }
        Expr::Access { inner, step } => {
            render_chain_base(inner, out);
            render_step(step, out);
        }
        Expr::Filter { inner, predicate } => {
            render_chain_base(inner, out);
            render_filter_suffix(predicate, out);
        }
        Expr::Exists(inner) => {
            out.push_str("exists (");
            render_expr(inner, out);
            out.push(')');
        }
        Expr::And(left, right) => {
            render_binary_left(expr, left, out);
            out.push_str(" && ");
            render_binary_right(expr, right, out);
        }
        Expr::Or(left, right) => {
            render_binary_left(expr, left, out);
            out.push_str(" || ");
            render_binary_right(expr, right, out);
        }
        Expr::Not(inner) => {
            out.push_str("!(");
            render_expr(inner, out);
            out.push(')');
        }
        Expr::IsUnknown(inner) => {
            out.push('(');
            render_expr(inner, out);
            out.push_str(") is unknown");
        }
    }

    if wrap {
        out.push(')');
    }
}

fn is_bracketed_operation(expr: &Expr) -> bool {
    match expr {
        Expr::Compare { .. }
        | Expr::StartsWith { .. }
        | Expr::LikeRegex { .. }
        | Expr::Arithmetic { .. }
        | Expr::And(..)
        | Expr::Or(..)
        | Expr::Unary { .. } => true,
        _ => false,
    }
}

fn expr_priority(expr: &Expr) -> i32 {
    match expr {
        Expr::Or(..) => 0,
        Expr::And(..) => 1,
        Expr::Compare { .. } | Expr::StartsWith { .. } | Expr::LikeRegex { .. } => 2,
        Expr::Arithmetic {
            op: ArithmeticOp::Add | ArithmeticOp::Sub,
            ..
        } => 3,
        Expr::Arithmetic {
            op: ArithmeticOp::Mul | ArithmeticOp::Div | ArithmeticOp::Mod,
            ..
        } => 4,
        Expr::Unary { .. } => 5,
        _ => 6,
    }
}

fn render_binary_left(parent: &Expr, child: &Expr, out: &mut String) {
    render_child_expr(parent, child, out);
}

fn render_binary_right(parent: &Expr, child: &Expr, out: &mut String) {
    render_child_expr(parent, child, out);
}

fn render_child_expr(parent: &Expr, child: &Expr, out: &mut String) {
    render_expr_with_brackets(child, out, expr_priority(child) <= expr_priority(parent));
}

fn render_chain_base(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Path { .. } | Expr::Access { .. } | Expr::MethodCall { .. } | Expr::Filter { .. } => {
            render_expr(expr, out)
        }
        Expr::Literal(JsonbValue::String(_))
        | Expr::Literal(JsonbValue::Bool(_))
        | Expr::Literal(JsonbValue::Null) => render_expr(expr, out),
        _ => {
            out.push('(');
            render_expr(expr, out);
            out.push(')');
        }
    }
}

fn folded_unary_numeric(expr: &Expr) -> Option<NumericValue> {
    match expr {
        Expr::Literal(JsonbValue::Numeric(value)) => Some(value.clone()),
        Expr::Unary {
            op: UnaryOp::Plus,
            inner,
        } => folded_unary_numeric(inner),
        Expr::Unary {
            op: UnaryOp::Minus,
            inner,
        } => folded_unary_numeric(inner).map(|value| value.negate()),
        _ => None,
    }
}

fn render_filter_suffix(predicate: &Expr, out: &mut String) {
    out.push_str("?(");
    render_expr(predicate, out);
    out.push(')');
}

fn render_base(base: &Base, out: &mut String) {
    match base {
        Base::Root => out.push('$'),
        Base::Current => out.push('@'),
        Base::Var(name) => {
            out.push('$');
            render_quoted_string(name, out);
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
                (RecursiveBound::Int(0), RecursiveBound::Last)
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
                    out.push(',');
                }
                render_subscript_selection(selection, out);
            }
            out.push(']');
        }
        Step::IndexWildcard => out.push_str("[*]"),
        Step::Method(method) => render_method(method, out),
        Step::Filter(expr) => render_filter_suffix(expr, out),
    }
}

fn render_method(method: &Method, out: &mut String) {
    out.push_str(match method.kind {
        MethodKind::Abs => ".abs(",
        MethodKind::BigInt => ".bigint(",
        MethodKind::Boolean => ".boolean(",
        MethodKind::Ceiling => ".ceiling(",
        MethodKind::Date => ".date(",
        MethodKind::Decimal => ".decimal(",
        MethodKind::Datetime => ".datetime(",
        MethodKind::Double => ".double(",
        MethodKind::Floor => ".floor(",
        MethodKind::Integer => ".integer(",
        MethodKind::KeyValue => ".keyvalue(",
        MethodKind::LTrim => ".ltrim(",
        MethodKind::Lower => ".lower(",
        MethodKind::Number => ".number(",
        MethodKind::BTrim => ".btrim(",
        MethodKind::InitCap => ".initcap(",
        MethodKind::Replace => ".replace(",
        MethodKind::RTrim => ".rtrim(",
        MethodKind::Size => ".size(",
        MethodKind::SplitPart => ".split_part(",
        MethodKind::String => ".string(",
        MethodKind::Time => ".time(",
        MethodKind::TimeTz => ".time_tz(",
        MethodKind::Timestamp => ".timestamp(",
        MethodKind::TimestampTz => ".timestamp_tz(",
        MethodKind::Type => ".type(",
    });
    for (index, arg) in method.args.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match arg {
            MethodArg::Numeric(value) => out.push_str(&value.render()),
            MethodArg::String(value) => render_quoted_string(value, out),
        }
    }
    out.push(')');
}

fn render_recursive_bound(bound: RecursiveBound, out: &mut String) {
    match bound {
        RecursiveBound::Int(value) => out.push_str(&value.to_string()),
        RecursiveBound::Last => out.push_str("last"),
    }
}

fn validate_method_arg_syntax(kind: MethodKind, args: &[MethodArg]) -> Result<(), ExecError> {
    if matches!(kind, MethodKind::Date) && !args.is_empty() {
        return Err(jsonpath_syntax_error_near(method_arg_syntax_token(
            &args[0],
        )));
    }
    if matches!(
        kind,
        MethodKind::Time | MethodKind::TimeTz | MethodKind::Timestamp | MethodKind::TimestampTz
    ) {
        for arg in args {
            if let MethodArg::Numeric(value) = arg {
                let text = value.render();
                if text.starts_with('-') || text.contains('.') {
                    let token = if text.starts_with('-') {
                        "-"
                    } else {
                        text.as_str()
                    };
                    return Err(jsonpath_syntax_error_near(token));
                }
            }
        }
    }
    Ok(())
}

fn method_arg_syntax_token(arg: &MethodArg) -> String {
    match arg {
        MethodArg::Numeric(value) => value.render(),
        MethodArg::String(value) => value.clone(),
    }
}

fn jsonpath_syntax_error_near(token: impl AsRef<str>) -> ExecError {
    exec_jsonpath_error(&format!(
        "syntax error at or near \"{}\" of jsonpath input",
        token.as_ref()
    ))
}

fn jsonpath_syntax_error_end() -> ExecError {
    exec_jsonpath_error("syntax error at end of jsonpath input")
}

fn is_jsonpath_delimiter(ch: char) -> bool {
    matches!(
        ch,
        '(' | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | '.'
            | ','
            | ':'
            | '?'
            | '@'
            | '$'
            | '+'
            | '-'
            | '*'
            | '/'
            | '%'
            | '='
            | '!'
            | '<'
            | '>'
    )
}

fn jsonpath_numeric_trailing_junk_error(token: &str) -> ExecError {
    exec_jsonpath_error(&format!(
        "trailing junk after numeric literal at or near \"{token}\" of jsonpath input"
    ))
}

fn jsonpath_invalid_numeric_literal_error(token: &str) -> ExecError {
    exec_jsonpath_error(&format!(
        "invalid numeric literal at or near \"{token}\" of jsonpath input"
    ))
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
            render_filter_suffix(predicate, out);
        }
    }
}

fn normalize_like_regex_flags(flags: &str) -> String {
    let mut case_insensitive = false;
    let mut dot_all = false;
    let mut multiline = false;
    let mut expanded = false;
    let mut quote = false;

    for flag in flags.chars() {
        match flag {
            'i' => case_insensitive = true,
            's' => dot_all = true,
            'm' => multiline = true,
            'x' => expanded = true,
            'q' => quote = true,
            _ => {}
        }
    }

    let mut out = String::new();
    if case_insensitive {
        out.push('i');
    }
    if dot_all {
        out.push('s');
    }
    if multiline {
        out.push('m');
    }
    if expanded {
        out.push('x');
    }
    if quote {
        out.push('q');
    }
    out
}

fn render_literal(value: &JsonbValue, out: &mut String) {
    match value {
        JsonbValue::Null => out.push_str("null"),
        JsonbValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonbValue::Numeric(n) => out.push_str(&n.render()),
        JsonbValue::String(s) => render_quoted_string(s, out),
        JsonbValue::Date(_)
        | JsonbValue::Time(_)
        | JsonbValue::TimeTz(_)
        | JsonbValue::Timestamp(_)
        | JsonbValue::TimestampTz(_)
        | JsonbValue::TimestampTzWithOffset(_, _) => {
            render_quoted_string(&render_temporal_jsonb_value(value), out)
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonPathNumberKind {
    Decimal,
    NonDecimal,
}

#[derive(Debug, Clone, Copy)]
struct JsonPathNumberMatch {
    end: usize,
    kind: JsonPathNumberKind,
}

fn scan_jsonpath_number(
    input: &str,
    start: usize,
) -> Result<Option<JsonPathNumberMatch>, ExecError> {
    let bytes = input.as_bytes();
    let Some(&first) = bytes.get(start) else {
        return Ok(None);
    };
    if !first.is_ascii_digit()
        && !(first == b'.' && bytes.get(start + 1).is_some_and(u8::is_ascii_digit))
    {
        return Ok(None);
    }

    if bytes.get(start) == Some(&b'0')
        && matches!(
            bytes.get(start + 1).copied(),
            Some(b'b' | b'B' | b'o' | b'O' | b'x' | b'X')
        )
        && bytes.get(start + 2) == Some(&b'_')
    {
        return Err(jsonpath_syntax_error_end());
    }

    let mut best: Option<JsonPathNumberMatch> = None;
    for candidate in [
        scan_jsonpath_real(bytes, start).map(|end| JsonPathNumberMatch {
            end,
            kind: JsonPathNumberKind::Decimal,
        }),
        scan_jsonpath_decimal(bytes, start).map(|end| JsonPathNumberMatch {
            end,
            kind: JsonPathNumberKind::Decimal,
        }),
        scan_jsonpath_decinteger(bytes, start).map(|end| JsonPathNumberMatch {
            end,
            kind: JsonPathNumberKind::Decimal,
        }),
        scan_jsonpath_nondecimal_integer(bytes, start, b'x', b'X', |byte| byte.is_ascii_hexdigit())
            .map(|end| JsonPathNumberMatch {
                end,
                kind: JsonPathNumberKind::NonDecimal,
            }),
        scan_jsonpath_nondecimal_integer(bytes, start, b'o', b'O', |byte| {
            matches!(byte, b'0'..=b'7')
        })
        .map(|end| JsonPathNumberMatch {
            end,
            kind: JsonPathNumberKind::NonDecimal,
        }),
        scan_jsonpath_nondecimal_integer(bytes, start, b'b', b'B', |byte| {
            matches!(byte, b'0' | b'1')
        })
        .map(|end| JsonPathNumberMatch {
            end,
            kind: JsonPathNumberKind::NonDecimal,
        }),
    ]
    .into_iter()
    .flatten()
    {
        if match best {
            Some(current) => candidate.end > current.end,
            None => true,
        } {
            best = Some(candidate);
        }
    }

    let Some(number) = best else {
        return Ok(None);
    };
    if number.kind == JsonPathNumberKind::Decimal
        && let Some(err) = jsonpath_decimal_trailing_error(input, start, number.end)
    {
        return Err(err);
    }
    Ok(Some(number))
}

fn scan_jsonpath_real(bytes: &[u8], start: usize) -> Option<usize> {
    [
        scan_jsonpath_decimal(bytes, start),
        scan_jsonpath_decinteger(bytes, start),
    ]
    .into_iter()
    .flatten()
    .filter_map(|mantissa_end| {
        if !matches!(bytes.get(mantissa_end).copied(), Some(b'e' | b'E')) {
            return None;
        }
        scan_jsonpath_exponent(bytes, mantissa_end + 1)
    })
    .max()
}

fn scan_jsonpath_decimal(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) == Some(&b'.') {
        return scan_jsonpath_digits(bytes, start + 1, |byte| byte.is_ascii_digit());
    }

    let whole_end = scan_jsonpath_decinteger(bytes, start)?;
    if bytes.get(whole_end) != Some(&b'.') {
        return None;
    }
    let fraction_start = whole_end + 1;
    Some(
        scan_jsonpath_digits(bytes, fraction_start, |byte| byte.is_ascii_digit())
            .unwrap_or(fraction_start),
    )
}

fn scan_jsonpath_decinteger(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start).copied()? {
        b'0' => Some(start + 1),
        b'1'..=b'9' => scan_jsonpath_digits(bytes, start, |byte| byte.is_ascii_digit()),
        _ => None,
    }
}

fn scan_jsonpath_nondecimal_integer(
    bytes: &[u8],
    start: usize,
    lower_prefix: u8,
    upper_prefix: u8,
    valid_digit: impl Fn(u8) -> bool + Copy,
) -> Option<usize> {
    if bytes.get(start) != Some(&b'0')
        || !matches!(bytes.get(start + 1).copied(), Some(prefix) if prefix == lower_prefix || prefix == upper_prefix)
    {
        return None;
    }
    scan_jsonpath_digits(bytes, start + 2, valid_digit)
}

fn scan_jsonpath_exponent(bytes: &[u8], mut start: usize) -> Option<usize> {
    if matches!(bytes.get(start).copied(), Some(b'+' | b'-')) {
        start += 1;
    }
    scan_jsonpath_digits(bytes, start, |byte| byte.is_ascii_digit())
}

fn scan_jsonpath_digits(
    bytes: &[u8],
    mut offset: usize,
    valid_digit: impl Fn(u8) -> bool + Copy,
) -> Option<usize> {
    if !bytes.get(offset).copied().is_some_and(valid_digit) {
        return None;
    }
    offset += 1;
    while offset < bytes.len() {
        if bytes[offset] == b'_' {
            if bytes.get(offset + 1).copied().is_some_and(valid_digit) {
                offset += 2;
            } else {
                break;
            }
        } else if valid_digit(bytes[offset]) {
            offset += 1;
        } else {
            break;
        }
    }
    Some(offset)
}

fn jsonpath_decimal_trailing_error(input: &str, start: usize, end: usize) -> Option<ExecError> {
    let next = input[end..].chars().next()?;
    if !is_jsonpath_other_char(next) {
        return None;
    }

    if next == 'e' || next == 'E' {
        let after_e = end + next.len_utf8();
        if let Some(sign @ ('+' | '-')) = input[after_e..].chars().next() {
            let after_sign = after_e + sign.len_utf8();
            if !input[after_sign..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit())
            {
                return Some(jsonpath_invalid_numeric_literal_error(
                    &input[start..after_sign],
                ));
            }
        }
    }

    if next.is_ascii_digit()
        && input[start..end] == *"0"
        && input[end + next.len_utf8()..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    if next == '_'
        && input[end + next.len_utf8()..]
            .chars()
            .next()
            .is_some_and(|ch| ch == '_')
    {
        return None;
    }

    let token_end = end + next.len_utf8();
    Some(jsonpath_numeric_trailing_junk_error(
        &input[start..token_end],
    ))
}

fn is_jsonpath_other_char(ch: char) -> bool {
    !matches!(
        ch,
        '?' | '%'
            | '$'
            | '.'
            | '['
            | ']'
            | '{'
            | '}'
            | '('
            | ')'
            | '|'
            | '&'
            | '!'
            | '='
            | '<'
            | '>'
            | '@'
            | '#'
            | ','
            | '*'
            | ':'
            | '-'
            | '+'
            | '/'
            | '\\'
            | '"'
    ) && !ch.is_whitespace()
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
    allow_postfix_filter: bool,
    allow_current: bool,
    allow_last: bool,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            offset: 0,
            allow_postfix_filter: true,
            allow_current: false,
            allow_last: false,
        }
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
        let expr_end = self.offset;
        self.skip_ws();
        if !self.is_eof() {
            if expr_end == self.offset
                && matches!(&expr, Expr::Literal(JsonbValue::Numeric(_)))
                && self.peek().is_some_and(is_jsonpath_other_char)
            {
                return Err(jsonpath_syntax_error_end());
            }
            return Err(jsonpath_syntax_error_near(self.syntax_error_token()));
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
        self.parse_postfix_expr(expr)
    }

    fn parse_postfix_expr(&mut self, mut expr: Expr) -> Result<Expr, ExecError> {
        loop {
            let saved = self.offset;
            self.skip_ws();
            if self.allow_postfix_filter && self.consume("?") {
                self.skip_ws();
                self.expect("(")?;
                let predicate = self.parse_filter_predicate()?;
                self.skip_ws();
                self.expect(")")?;
                expr = Expr::Filter {
                    inner: Box::new(expr),
                    predicate: Box::new(predicate),
                };
                continue;
            }
            if self.consume("[") {
                expr = Expr::Access {
                    inner: Box::new(expr),
                    step: self.parse_subscript_step()?,
                };
                continue;
            }
            if self.consume(".") {
                if self.consume("*") {
                    let step = if self.consume("*") {
                        let (min_depth, max_depth) = self.parse_recursive_quantifier()?;
                        Step::Recursive {
                            min_depth,
                            max_depth,
                        }
                    } else {
                        Step::MemberWildcard
                    };
                    expr = Expr::Access {
                        inner: Box::new(expr),
                        step,
                    };
                    continue;
                }
                if let Some(ident) = self.parse_unquoted_string()? {
                    if self.consume("(") {
                        let method = self.parse_method(&ident)?;
                        expr = Expr::MethodCall {
                            inner: Box::new(expr),
                            method,
                        };
                    } else {
                        expr = Expr::Access {
                            inner: Box::new(expr),
                            step: Step::Member(ident),
                        };
                    }
                    continue;
                }
                if let Some(key) = self.parse_string()? {
                    expr = Expr::Access {
                        inner: Box::new(expr),
                        step: Step::Member(key),
                    };
                    continue;
                }
            }
            self.offset = saved;
            return Ok(expr);
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
            let base = if let Some(ident) = self.parse_string()? {
                Base::Var(ident)
            } else if let Some(ident) = self.parse_optional_ident() {
                Base::Var(ident)
            } else {
                Base::Root
            };
            return self.parse_path(base);
        }
        if self.peek() == Some('@') {
            if !self.allow_current {
                return Err(exec_jsonpath_error("@ is not allowed in root expressions"));
            }
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
            if !self.allow_last {
                return Err(exec_jsonpath_error(
                    "LAST is allowed only in array subscripts",
                ));
            }
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
        if matches!(self.peek(), Some(ch) if ch == '_' || ch.is_ascii_alphabetic()) {
            let _ = self.take_while(|ch| ch == '_' || ch.is_ascii_alphanumeric());
            if matches!(self.peek(), Some(ch) if ch.is_whitespace()) {
                return Err(jsonpath_syntax_error_near(" "));
            }
            return Err(jsonpath_syntax_error_end());
        }
        if let Some(ch) = self.peek() {
            return Err(jsonpath_syntax_error_near(ch.to_string()));
        }
        Err(jsonpath_syntax_error_end())
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
                    if let Some(ident) = self.parse_unquoted_string()? {
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
                steps.push(self.parse_subscript_step()?);
            } else if self.consume("?") {
                self.skip_ws();
                self.expect("(")?;
                let expr = self.parse_filter_predicate()?;
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

    fn parse_subscript_step(&mut self) -> Result<Step, ExecError> {
        self.skip_ws();
        if self.consume("*") {
            self.skip_ws();
            self.expect("]")?;
            return Ok(Step::IndexWildcard);
        }

        let mut selections = Vec::new();
        loop {
            let start = self.parse_subscript_expr()?;
            self.skip_ws();
            if self.consume_keyword("to") {
                self.skip_ws();
                let allow_postfix_filter = self.allow_postfix_filter;
                let allow_last = self.allow_last;
                self.allow_postfix_filter = false;
                self.allow_last = true;
                let end = self.parse_additive_expr();
                self.allow_postfix_filter = allow_postfix_filter;
                self.allow_last = allow_last;
                let end = end?;
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
        Ok(Step::Subscripts(selections))
    }

    fn parse_recursive_bound(&mut self) -> Result<RecursiveBound, ExecError> {
        if self.consume_keyword("last") {
            return Ok(RecursiveBound::Last);
        }
        Ok(RecursiveBound::Int(self.parse_signed_int()?))
    }

    fn parse_subscript_expr(&mut self) -> Result<SubscriptExpr, ExecError> {
        let allow_postfix_filter = self.allow_postfix_filter;
        let allow_last = self.allow_last;
        self.allow_postfix_filter = false;
        self.allow_last = true;
        let expr = self.parse_additive_expr();
        self.allow_postfix_filter = allow_postfix_filter;
        let expr = expr?;
        self.skip_ws();
        if self.consume("?") {
            self.skip_ws();
            self.expect("(")?;
            let predicate = self.parse_filter_predicate()?;
            self.skip_ws();
            self.expect(")")?;
            self.allow_last = allow_last;
            return Ok(SubscriptExpr::Filter {
                expr: Box::new(expr),
                predicate: Box::new(predicate),
            });
        }
        self.allow_last = allow_last;
        Ok(SubscriptExpr::Expr(Box::new(expr)))
    }

    fn parse_filter_predicate(&mut self) -> Result<Expr, ExecError> {
        let allow_current = self.allow_current;
        self.allow_current = true;
        let predicate = self.parse_or_expr();
        self.allow_current = allow_current;
        predicate
    }

    fn method_kind(&self, ident: &str) -> Result<MethodKind, ExecError> {
        match ident {
            "abs" => Ok(MethodKind::Abs),
            "bigint" => Ok(MethodKind::BigInt),
            "boolean" => Ok(MethodKind::Boolean),
            "ceiling" => Ok(MethodKind::Ceiling),
            "date" => Ok(MethodKind::Date),
            "decimal" => Ok(MethodKind::Decimal),
            "datetime" => Ok(MethodKind::Datetime),
            "double" => Ok(MethodKind::Double),
            "floor" => Ok(MethodKind::Floor),
            "integer" => Ok(MethodKind::Integer),
            "keyvalue" => Ok(MethodKind::KeyValue),
            "ltrim" => Ok(MethodKind::LTrim),
            "lower" => Ok(MethodKind::Lower),
            "number" => Ok(MethodKind::Number),
            "btrim" => Ok(MethodKind::BTrim),
            "initcap" => Ok(MethodKind::InitCap),
            "replace" => Ok(MethodKind::Replace),
            "rtrim" => Ok(MethodKind::RTrim),
            "size" => Ok(MethodKind::Size),
            "split_part" => Ok(MethodKind::SplitPart),
            "string" => Ok(MethodKind::String),
            "time" => Ok(MethodKind::Time),
            "time_tz" => Ok(MethodKind::TimeTz),
            "timestamp" => Ok(MethodKind::Timestamp),
            "timestamp_tz" => Ok(MethodKind::TimestampTz),
            "type" => Ok(MethodKind::Type),
            _ => Err(exec_jsonpath_error("unsupported jsonpath item method")),
        }
    }

    fn parse_method(&mut self, ident: &str) -> Result<Method, ExecError> {
        let kind = self.method_kind(ident)?;
        let args = self.parse_method_args()?;
        validate_method_arg_syntax(kind, &args)?;
        Ok(Method { kind, args })
    }

    fn parse_method_args(&mut self) -> Result<Vec<MethodArg>, ExecError> {
        self.skip_ws();
        if self.consume(")") {
            return Ok(Vec::new());
        }
        let mut args = Vec::new();
        loop {
            args.push(self.parse_method_arg()?);
            self.skip_ws();
            if self.consume(")") {
                break;
            }
            self.expect(",")?;
            self.skip_ws();
        }
        Ok(args)
    }

    fn parse_method_arg(&mut self) -> Result<MethodArg, ExecError> {
        if let Some(text) = self.parse_string()? {
            return Ok(MethodArg::String(text));
        }
        Ok(MethodArg::Numeric(self.parse_method_numeric_arg()?))
    }

    fn parse_method_numeric_arg(&mut self) -> Result<NumericValue, ExecError> {
        self.skip_ws();
        let start = self.offset;
        let _ = self.consume("+") || self.consume("-");
        let Some(_) = self.take_while(|ch| ch.is_ascii_digit()) else {
            self.offset = start;
            return Err(exec_jsonpath_error(
                "expected numeric jsonpath method argument",
            ));
        };
        let mut text = self.input[start..self.offset].to_string();
        if self.consume(".") {
            text.push('.');
            let frac = self
                .take_while(|ch| ch.is_ascii_digit())
                .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))?;
            text.push_str(frac);
        }
        parse_numeric_text(&text)
            .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))
    }

    fn parse_signed_int(&mut self) -> Result<i32, ExecError> {
        self.skip_ws();
        let negative = if self.consume("-") {
            true
        } else {
            let _ = self.consume("+");
            false
        };
        let digits = self
            .take_while(|ch| ch.is_ascii_digit())
            .ok_or_else(|| exec_jsonpath_error("expected integer jsonpath subscript"))?;
        let mut value = digits
            .parse::<i32>()
            .map_err(|_| exec_jsonpath_error("jsonpath array subscript is out of integer range"))?;
        if negative {
            value = -value;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<Option<JsonbValue>, ExecError> {
        let start = self.offset;
        let Some(number) = scan_jsonpath_number(self.input, start)? else {
            return Ok(None);
        };
        let text = &self.input[start..number.end];
        let numeric = parse_numeric_text(&text)
            .ok_or_else(|| exec_jsonpath_error("invalid jsonpath numeric literal"))?;
        self.offset = number.end;
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
            .ok_or_else(|| jsonpath_syntax_error_near("\\"))?;
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
            'v' => out.push('\u{000B}'),
            'u' => self.parse_unicode_escape(out)?,
            'x' => out.push(self.parse_hex_escape()?),
            other => out.push(other),
        }
        Ok(())
    }

    fn parse_unicode_escape(&mut self, out: &mut String) -> Result<(), ExecError> {
        if self.consume("{") {
            let mut value = 0u32;
            let mut digits = 0usize;
            while !self.consume("}") {
                if digits >= 6 {
                    return Err(exec_jsonpath_error("invalid Unicode escape sequence"));
                }
                value = (value << 4) | self.parse_hex_digit("invalid Unicode escape sequence")?;
                digits += 1;
            }
            if digits == 0 {
                return Err(exec_jsonpath_error("invalid Unicode escape sequence"));
            }
            out.push(
                char::from_u32(value)
                    .ok_or_else(|| exec_jsonpath_error("invalid Unicode escape sequence"))?,
            );
            return Ok(());
        }

        let high = self.parse_four_hex_digits()?;
        if (0xD800..=0xDBFF).contains(&high) {
            if !self.consume("\\u") {
                return Err(exec_jsonpath_error("invalid Unicode escape sequence"));
            }
            let low = self.parse_four_hex_digits()?;
            if !(0xDC00..=0xDFFF).contains(&low) {
                return Err(exec_jsonpath_error("invalid Unicode escape sequence"));
            }
            let scalar = 0x10000 + (((high - 0xD800) as u32) << 10) + (low - 0xDC00) as u32;
            out.push(
                char::from_u32(scalar)
                    .ok_or_else(|| exec_jsonpath_error("invalid Unicode escape sequence"))?,
            );
        } else if (0xDC00..=0xDFFF).contains(&high) {
            return Err(exec_jsonpath_error("invalid Unicode escape sequence"));
        } else {
            out.push(
                char::from_u32(high as u32)
                    .ok_or_else(|| exec_jsonpath_error("invalid Unicode escape sequence"))?,
            );
        }
        Ok(())
    }

    fn parse_four_hex_digits(&mut self) -> Result<u16, ExecError> {
        let mut value = 0u16;
        for _ in 0..4 {
            value = (value << 4) | self.parse_hex_digit("invalid Unicode escape sequence")? as u16;
        }
        Ok(value)
    }

    fn parse_hex_escape(&mut self) -> Result<char, ExecError> {
        let high = self.parse_hex_digit("invalid hexadecimal character sequence")?;
        let low = self.parse_hex_digit("invalid hexadecimal character sequence")?;
        char::from_u32((high << 4) | low)
            .ok_or_else(|| exec_jsonpath_error("invalid hexadecimal character sequence"))
    }

    fn parse_hex_digit(&mut self, message: &'static str) -> Result<u32, ExecError> {
        let ch = self.peek().ok_or_else(|| exec_jsonpath_error(message))?;
        self.bump();
        ch.to_digit(16).ok_or_else(|| exec_jsonpath_error(message))
    }

    fn parse_unquoted_string(&mut self) -> Result<Option<String>, ExecError> {
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            if ch == '\\' {
                self.bump();
                self.parse_escape_sequence(&mut out)?;
            } else if is_jsonpath_other_char(ch) {
                self.bump();
                out.push(ch);
            } else {
                break;
            }
        }
        Ok((!out.is_empty()).then_some(out))
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

    fn syntax_error_token(&self) -> String {
        match self.peek() {
            Some(ch) if ch.is_whitespace() => " ".into(),
            Some(ch) if is_jsonpath_delimiter(ch) => ch.to_string(),
            Some(_) => self.input[self.offset..]
                .chars()
                .take_while(|ch| !ch.is_whitespace() && !is_jsonpath_delimiter(*ch))
                .collect(),
            None => String::new(),
        }
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
