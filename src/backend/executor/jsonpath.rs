use std::cmp::Ordering;

use crate::backend::executor::ExecError;
use crate::backend::executor::jsonb::{JsonbValue, compare_jsonb};

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
    Path { base: Base, steps: Vec<Step> },
    Literal(JsonbValue),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
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
    Index(i32),
    IndexWildcard,
    Range(i32, i32),
    Filter(Box<Expr>),
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
        Expr::And(left, right) => Ok(vec![JsonbValue::Bool(
            predicate_bool(left, ctx)? && predicate_bool(right, ctx)?,
        )]),
        Expr::Or(left, right) => Ok(vec![JsonbValue::Bool(
            predicate_bool(left, ctx)? || predicate_bool(right, ctx)?,
        )]),
        Expr::Not(inner) => Ok(vec![JsonbValue::Bool(!predicate_bool(inner, ctx)?)]),
    }
}

fn predicate_bool(expr: &Expr, ctx: &RuntimeContext<'_>) -> Result<bool, ExecError> {
    let values = eval_expr(expr, ctx)?;
    if values.is_empty() {
        return Ok(false);
    }
    if values.len() != 1 {
        return Err(exec_jsonpath_error("predicate expression must return one item"));
    }
    match &values[0] {
        JsonbValue::Bool(value) => Ok(*value),
        JsonbValue::Null => Ok(false),
        _ => Err(exec_jsonpath_error("predicate expression must return boolean")),
    }
}

fn lookup_var<'a>(ctx: &'a RuntimeContext<'_>, name: &str) -> Result<&'a JsonbValue, ExecError> {
    let Some(JsonbValue::Object(items)) = ctx.global.vars else {
        return Err(exec_jsonpath_error("jsonpath variables must be a jsonb object"));
    };
    items.iter()
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
                return Err(exec_jsonpath_error("jsonpath member access requires object"));
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
                return Err(exec_jsonpath_error("jsonpath wildcard member access requires object"));
            }
            _ => {}
        },
        Step::Index(index) => match value {
            JsonbValue::Array(items) => {
                if let Some(found) = array_index(items, *index) {
                    out.push(found.clone());
                } else if matches!(ctx.mode, PathMode::Strict) {
                    return Err(exec_jsonpath_error("jsonpath array index out of range"));
                }
            }
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error("jsonpath array subscript requires array"));
            }
            _ => {}
        },
        Step::IndexWildcard => match value {
            JsonbValue::Array(items) => out.extend(items.iter().cloned()),
            _ if matches!(ctx.mode, PathMode::Strict) => {
                return Err(exec_jsonpath_error("jsonpath array wildcard requires array"));
            }
            _ => {}
        },
        Step::Range(start, end) => match value {
            JsonbValue::Array(items) => {
                for index in *start..=*end {
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

fn exec_jsonpath_error(message: &str) -> ExecError {
    ExecError::InvalidStorageValue {
        column: "jsonpath".into(),
        details: message.to_string(),
    }
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
        self.parse_compare_expr()
    }

    fn parse_compare_expr(&mut self) -> Result<Expr, ExecError> {
        let left = self.parse_primary()?;
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
            let right = self.parse_primary()?;
            Ok(Expr::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        } else {
            Ok(left)
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
                    steps.push(Step::MemberWildcard);
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
                    let start = self.parse_signed_int()?;
                    self.skip_ws();
                    if self.consume_keyword("to") {
                        self.skip_ws();
                        let end = self.parse_signed_int()?;
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
        let numeric = crate::backend::executor::exec_expr::parse_numeric_text(&text)
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
            Err(exec_jsonpath_error("expected whitespace after jsonpath mode"))
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
                        return Err(exec_jsonpath_error("invalid low surrogate in jsonpath string"));
                    }
                    let scalar = 0x10000 + (((codepoint - 0xD800) as u32) << 10) + (low - 0xDC00) as u32;
                    let ch = char::from_u32(scalar)
                        .ok_or_else(|| exec_jsonpath_error("invalid Unicode scalar value in jsonpath string"))?;
                    out.push(ch);
                } else if (0xDC00..=0xDFFF).contains(&codepoint) {
                    return Err(exec_jsonpath_error("invalid low surrogate in jsonpath string"));
                } else if codepoint == 0 {
                    return Err(exec_jsonpath_error("unsupported Unicode escape sequence"));
                } else {
                    let ch = char::from_u32(codepoint as u32)
                        .ok_or_else(|| exec_jsonpath_error("invalid Unicode scalar value in jsonpath string"))?;
                    out.push(ch);
                }
            }
            _ => {
                return Err(exec_jsonpath_error("invalid escape sequence in jsonpath string"));
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
