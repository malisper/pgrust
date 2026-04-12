use crate::backend::executor::{
    ExecError, StatementResult, TupleSlot, Value, cast_value, eval_plpgsql_expr,
};
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind};

use super::ast::RaiseLevel;
use super::compile::{CompiledBlock, CompiledExpr, CompiledStmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlNotice {
    pub level: RaiseLevel,
    pub message: String,
}

thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<PlpgsqlNotice>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn take_notices() -> Vec<PlpgsqlNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}

pub(crate) fn execute_block(block: &CompiledBlock) -> Result<StatementResult, ExecError> {
    let mut values = vec![Value::Null; block.total_slots];
    exec_block(block, &mut values)?;
    Ok(StatementResult::AffectedRows(0))
}

fn exec_block(block: &CompiledBlock, values: &mut [Value]) -> Result<(), ExecError> {
    for local in &block.local_slots {
        values[local.slot] = match &local.default_expr {
            Some(expr) => cast_value(eval_expr(expr, values)?, local.ty)?,
            None => Value::Null,
        };
    }
    for stmt in &block.statements {
        exec_stmt(stmt, values)?;
    }
    Ok(())
}

fn exec_stmt(stmt: &CompiledStmt, values: &mut [Value]) -> Result<(), ExecError> {
    match stmt {
        CompiledStmt::Block(block) => exec_block(block, values),
        CompiledStmt::Assign { slot, ty, expr } => {
            values[*slot] = cast_value(eval_expr(expr, values)?, *ty)?;
            Ok(())
        }
        CompiledStmt::Null => Ok(()),
        CompiledStmt::If {
            branches,
            else_branch,
        } => {
            for (condition, body) in branches {
                match eval_expr(condition, values)? {
                    Value::Bool(true) => {
                        for stmt in body {
                            exec_stmt(stmt, values)?;
                        }
                        return Ok(());
                    }
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            for stmt in else_branch {
                exec_stmt(stmt, values)?;
            }
            Ok(())
        }
        CompiledStmt::ForInt {
            slot,
            start_expr,
            end_expr,
            body,
        } => {
            let start = match cast_value(
                eval_expr(start_expr, values)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR start expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            let end = match cast_value(
                eval_expr(end_expr, values)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR end expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            if start > end {
                return Ok(());
            }
            for current in start..=end {
                values[*slot] = Value::Int32(current);
                for stmt in body {
                    exec_stmt(stmt, values)?;
                }
            }
            Ok(())
        }
        CompiledStmt::Raise {
            level,
            message,
            params,
        } => {
            let rendered = render_raise_message(message, params, values)?;
            match level {
                RaiseLevel::Exception => Err(ExecError::RaiseException(rendered)),
                RaiseLevel::Notice | RaiseLevel::Warning => {
                    NOTICE_QUEUE.with(|queue| {
                        queue.borrow_mut().push(PlpgsqlNotice {
                            level: level.clone(),
                            message: rendered,
                        })
                    });
                    Ok(())
                }
            }
        }
    }
}

fn eval_expr(expr: &CompiledExpr, values: &[Value]) -> Result<Value, ExecError> {
    let mut slot = TupleSlot::virtual_row(values.to_vec());
    eval_plpgsql_expr(&expr.expr, &mut slot)
}

fn render_raise_message(
    message: &str,
    params: &[CompiledExpr],
    values: &[Value],
) -> Result<String, ExecError> {
    let mut rendered = String::with_capacity(message.len());
    let mut params = params.iter();
    for ch in message.chars() {
        if ch == '%' {
            let value = params.next().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "RAISE parameter",
                    actual: message.to_string(),
                })
            })?;
            rendered.push_str(&render_raise_value(&eval_expr(value, values)?));
        } else {
            rendered.push(ch);
        }
    }
    Ok(rendered)
}

fn render_raise_value(value: &Value) -> String {
    match value {
        Value::Null => "<NULL>".to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bit(v) => crate::backend::executor::render_bit_text(v),
        Value::InternalChar(v) => char::from(*v).to_string(),
        Value::Json(text) | Value::JsonPath(text) => text.to_string(),
        Value::Jsonb(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::Bytea(bytes) => {
            let mut rendered = String::from("\\x");
            for byte in bytes {
                use std::fmt::Write as _;
                let _ = write!(&mut rendered, "{byte:02x}");
            }
            rendered
        }
        Value::Array(values) => {
            let elems = values.iter().map(render_raise_value).collect::<Vec<_>>();
            format!("{{{}}}", elems.join(","))
        }
    }
}
