use super::ExecError;
use crate::compat::include::nodes::datum::Value;

pub fn parse_pg_bool_text(raw: &str) -> Result<bool, ExecError> {
    let trimmed = raw.trim_matches(|ch: char| ch.is_ascii_whitespace());
    if trimmed.is_empty() {
        return Err(ExecError::InvalidBooleanInput {
            value: raw.to_string(),
        });
    }

    let len = trimmed.len();
    let is_prefix = |needle: &str, min_len: usize| {
        len >= min_len && needle.len() >= len && needle[..len].eq_ignore_ascii_case(trimmed)
    };

    let result = match trimmed.as_bytes()[0].to_ascii_lowercase() {
        b't' if is_prefix("true", 1) => Some(true),
        b'f' if is_prefix("false", 1) => Some(false),
        b'y' if is_prefix("yes", 1) => Some(true),
        b'n' if is_prefix("no", 1) => Some(false),
        b'o' if is_prefix("on", 2) => Some(true),
        b'o' if is_prefix("off", 2) => Some(false),
        b'1' if len == 1 => Some(true),
        b'0' if len == 1 => Some(false),
        _ => None,
    };

    result.ok_or_else(|| ExecError::InvalidBooleanInput {
        value: raw.to_string(),
    })
}

pub fn order_bool_values(
    op: &'static str,
    left: &Value,
    right: &Value,
) -> Result<Value, ExecError> {
    let (Value::Bool(left), Value::Bool(right)) = (left, right) else {
        return Err(ExecError::TypeMismatch {
            op,
            left: left.clone(),
            right: right.clone(),
        });
    };
    Ok(Value::Bool(match op {
        "<" => !left && *right,
        "<=" => !left || *right,
        ">" => *left && !right,
        ">=" => *left || !right,
        _ => unreachable!(),
    }))
}

pub fn eval_booleq(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(left == right)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "booleq",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

pub fn eval_boolne(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(left != right)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "boolne",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

pub fn eval_booland_statefunc(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(*left && *right)),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "booland_statefunc",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

pub fn eval_boolor_statefunc(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(*left || *right)),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "boolor_statefunc",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

pub fn cast_integer_to_bool(value: i64) -> Value {
    Value::Bool(value != 0)
}

#[cfg(test)]
mod tests {
    use super::{order_bool_values, parse_pg_bool_text};
    use crate::compat::include::nodes::datum::Value;

    #[test]
    fn parse_pg_bool_text_accepts_postgres_spellings() {
        for (input, expected) in [
            ("t", true),
            ("   f           ", false),
            ("true", true),
            ("FALSE", false),
            ("y", true),
            ("yes", true),
            ("n", false),
            ("no", false),
            ("on", true),
            ("off", false),
            ("of", false),
            ("1", true),
            ("0", false),
        ] {
            assert_eq!(parse_pg_bool_text(input).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn parse_pg_bool_text_rejects_invalid_spellings() {
        for input in [
            "test", "foo", "yeah", "nay", "o", "on_", "off_", "11", "000", "",
        ] {
            assert!(parse_pg_bool_text(input).is_err(), "{input}");
        }
    }

    #[test]
    fn bool_ordering_matches_postgres_order() {
        assert_eq!(
            order_bool_values("<", &Value::Bool(false), &Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            order_bool_values(">=", &Value::Bool(true), &Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
    }
}
