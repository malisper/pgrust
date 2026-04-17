use std::cmp::Ordering;

use num_traits::Zero;

use crate::backend::executor::ExecError;
use crate::backend::executor::expr_ops::parse_numeric_text;
use crate::backend::executor::jsonb::{JsonbValue, compare_jsonb};
use crate::include::nodes::datum::NumericValue;

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
    Arithmetic {
        op: ArithmeticOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        inner: Box<Expr>,
    },
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
    Index(SubscriptExpr),
    IndexWildcard,
    Range(SubscriptExpr, SubscriptExpr),
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
    Int(i32),
    Fractional(NumericValue),
    Last,
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
    };
    eval_expr(&path.expr, &runtime)
}

fn eval_expr(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<Vec<JsonbValue>, ExecError> {
    match expr {
        Expr::Literal(value) => Ok(vec![value.clone()]),
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
        Expr::Compare { op, left, right } => {
            let left_values = eval_expr(left, ctx)?;
            let right_values = eval_expr(right, ctx)?;
            Ok(vec![JsonbValue::Bool(compare_any_pair(
                &left_values,
                &right_values,
                *op,
            ))])
        }
        Expr::Arithmetic { op, left, right } => {
            let left_values = eval_expr(left, ctx)?;
            let right_values = eval_expr(right, ctx)?;
            eval_arithmetic_any_pair(&left_values, &right_values, *op)
        }
        Expr::Unary { op, inner } => {
            let values = eval_expr(inner, ctx)?;
            values
                .into_iter()
                .map(|value| eval_unary_value(value, *op))
                .collect()
        }
        Expr::And(left, right) => Ok(vec![JsonbValue::Bool(
            predicate_bool(left, ctx)? && predicate_bool(right, ctx)?,
        )]),
        Expr::Or(left, right) => Ok(vec![JsonbValue::Bool(
            predicate_bool(left, ctx)? || predicate_bool(right, ctx)?,
        )]),
        Expr::Not(inner) => Ok(vec![JsonbValue::Bool(!predicate_bool(inner, ctx)?)]),
        Expr::IsUnknown(inner) => Ok(vec![JsonbValue::Bool(eval_expr(inner, ctx).is_err())]),
    }
}

fn predicate_bool(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<bool, ExecError> {
    let values = eval_expr(expr, ctx)?;
    if values.is_empty() {
        return Ok(false);
    }
    if values.len() != 1 {
        return Err(exec_jsonpath_error(
            "predicate expression must return one item",
        ));
    }
    match &values[0] {
        JsonbValue::Bool(value) => Ok(*value),
        JsonbValue::Null => Ok(false),
        _ => Err(exec_jsonpath_error(
            "predicate expression must return boolean",
        )),
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
            collect_recursive_values(value, min_depth, max_depth, 1, out);
        }
        Step::Index(index) => match value {
            JsonbValue::Array(items) => {
                let index = resolve_subscript_expr(index.clone(), items.len())?;
                if let Some(found) = array_index(items, index) {
                    out.push(found.clone());
                } else if matches!(ctx.mode, PathMode::Strict) {
                    return Err(exec_jsonpath_error("jsonpath array index out of range"));
                }
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
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error(
                    "jsonpath array wildcard requires array",
                ));
            }
            _ => {}
        },
        Step::Range(start, end) => match value {
            JsonbValue::Array(items) => {
                let start = resolve_subscript_expr(start.clone(), items.len())?;
                let end = resolve_subscript_expr(end.clone(), items.len())?;
                for index in start..=end {
                    if let Some(found) = array_index(items, index) {
                        out.push(found.clone());
                    }
                }
                if out.is_empty() && matches!(ctx.mode, PathMode::Strict) {
                    return Err(exec_jsonpath_error("jsonpath array range is out of bounds"));
                }
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error("jsonpath array range requires array"));
            }
            _ => {}
        },
        Step::Filter(expr) => match value {
            JsonbValue::Array(items) if matches!(ctx.mode, PathMode::Lax) => {
                for item in items {
                    let nested = RuntimeContext {
                        global: ctx.global,
                        current: item,
                        mode: ctx.mode,
                    };
                    if predicate_bool(expr, &nested)? {
                        out.push(item.clone());
                    }
                }
            }
            _ => {
                let nested = RuntimeContext {
                    global: ctx.global,
                    current: value,
                    mode: ctx.mode,
                };
                if predicate_bool(expr, &nested)? {
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

fn compare_any_pair(left: &[JsonbValue], right: &[JsonbValue], op: CompareOp) -> bool {
    for left_value in left {
        for right_value in right {
            if compare_values(left_value, right_value, op) {
                return true;
            }
        }
    }
    false
}

fn compare_values(left: &JsonbValue, right: &JsonbValue, op: CompareOp) -> bool {
    let ordering = compare_jsonb(left, right);
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::NotEq => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::LtEq => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::GtEq => ordering != Ordering::Less,
    }
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

fn resolve_subscript_expr(expr: SubscriptExpr, array_len: usize) -> Result<i32, ExecError> {
    match expr {
        SubscriptExpr::Int(value) => Ok(value),
        SubscriptExpr::Fractional(value) => truncate_numeric_to_i32(&value),
        SubscriptExpr::Last => Ok((array_len as i32) - 1),
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
    let children: Vec<&JsonbValue> = match value {
        JsonbValue::Array(items) => items.iter().collect(),
        JsonbValue::Object(items) => items.iter().map(|(_, item)| item).collect(),
        _ => Vec::new(),
    };
    for child in children {
        if current_depth >= min_depth && current_depth <= max_depth {
            out.push(child.clone());
        }
        if current_depth < max_depth {
            collect_recursive_values(child, min_depth, max_depth, current_depth + 1, out);
        }
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
        Expr::Compare { .. } | Expr::Arithmetic { .. } | Expr::And(..) | Expr::Or(..) => {
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
        Step::Index(index) => {
            out.push('[');
            render_subscript_expr(index.clone(), out);
            out.push(']');
        }
        Step::IndexWildcard => out.push_str("[*]"),
        Step::Range(start, end) => {
            out.push('[');
            render_subscript_expr(start.clone(), out);
            out.push_str(" to ");
            render_subscript_expr(end.clone(), out);
            out.push(']');
        }
        Step::Filter(expr) => {
            out.push_str(" ? (");
            render_expr(expr, out);
            out.push(')');
        }
    }
}

fn render_recursive_bound(bound: RecursiveBound, out: &mut String) {
    match bound {
        RecursiveBound::Int(value) => out.push_str(&value.to_string()),
        RecursiveBound::Last => out.push_str("last"),
    }
}

fn render_subscript_expr(expr: SubscriptExpr, out: &mut String) {
    match expr {
        SubscriptExpr::Int(value) => out.push_str(&value.to_string()),
        SubscriptExpr::Fractional(value) => out.push_str(&value.render()),
        SubscriptExpr::Last => out.push_str("last"),
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
        self.parse_primary()
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
                    let key = self
                        .parse_ident()
                        .or_else(|| self.parse_string().ok().flatten())
                        .ok_or_else(|| exec_jsonpath_error("expected jsonpath member name"))?;
                    steps.push(Step::Member(key));
                }
            } else if self.consume("[") {
                self.skip_ws();
                if self.consume("*") {
                    self.skip_ws();
                    self.expect("]")?;
                    steps.push(Step::IndexWildcard);
                } else {
                    let start = self.parse_subscript_expr()?;
                    self.skip_ws();
                    if self.consume_keyword("to") {
                        self.skip_ws();
                        let end = self.parse_subscript_expr()?;
                        self.skip_ws();
                        self.expect("]")?;
                        steps.push(Step::Range(start, end));
                    } else {
                        self.skip_ws();
                        self.expect("]")?;
                        steps.push(Step::Index(start));
                    }
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
            return Ok((RecursiveBound::Int(1), RecursiveBound::Last));
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
        self.skip_ws();
        if self.consume_keyword("last") {
            return Ok(SubscriptExpr::Last);
        }
        let saved = self.offset;
        if let Some(number) = self.parse_number()? {
            let JsonbValue::Numeric(numeric) = number else {
                unreachable!();
            };
            return match &numeric {
                NumericValue::Finite { scale: 0, .. } => {
                    Ok(SubscriptExpr::Int(truncate_numeric_to_i32(&numeric)?))
                }
                _ => Ok(SubscriptExpr::Fractional(numeric)),
            };
        }
        self.offset = saved;
        Ok(SubscriptExpr::Int(self.parse_signed_int()?))
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
