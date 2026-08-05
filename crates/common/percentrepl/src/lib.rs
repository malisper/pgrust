//! src/common/percentrepl.c

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

fn loc() -> ErrorLocation {
    ErrorLocation::new(file!(), line!() as i32, "replace_percent_placeholders")
}

// C varargs (letters, ...) becomes a (letter, value) slice; None values mirror
// C NULL (placeholder present in letters but unsupported at this call site).
pub fn replace_percent_placeholders(
    instr: &str,
    param_name: &str,
    values: &[(char, Option<&str>)],
) -> PgResult<String> {
    let mut result = String::with_capacity(instr.len());
    let mut it = instr.chars();

    while let Some(c) = it.next() {
        if c != '%' {
            result.push(c);
            continue;
        }
        match it.next() {
            Some('%') => result.push('%'),
            None => {
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid value for parameter \"{param_name}\": \"{instr}\""))
                    .errdetail("String ends unexpectedly after escape character \"%\".")
                    .finish(loc())?;
                unreachable!()
            }
            Some(p) => {
                let found = values.iter().find(|(l, _)| *l == p).and_then(|(_, v)| *v);
                match found {
                    Some(val) => result.push_str(val),
                    None => {
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "invalid value for parameter \"{param_name}\": \"{instr}\""
                            ))
                            .errdetail(format!(
                                "String contains unexpected placeholder \"%{p}\"."
                            ))
                            .finish(loc())?;
                        unreachable!()
                    }
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_percent_is_literal() {
        assert_eq!(replace_percent_placeholders("100%%", "p", &[]).unwrap(), "100%");
    }

    #[test]
    fn placeholder_substitution() {
        let v = [('f', Some("file")), ('p', Some("path"))];
        assert_eq!(
            replace_percent_placeholders("cp %p %f", "archive_command", &v).unwrap(),
            "cp path file"
        );
    }

    #[test]
    fn trailing_percent_errors() {
        let err = replace_percent_placeholders("abc%", "p", &[]).unwrap_err();
        assert!(err.message().contains("invalid value for parameter"));
    }

    #[test]
    fn unsupported_placeholder_errors() {
        let err = replace_percent_placeholders("%z", "p", &[('f', Some("x"))]).unwrap_err();
        assert!(err.message().contains("invalid value for parameter"));
    }

    #[test]
    fn null_value_is_unsupported() {
        let err = replace_percent_placeholders("%r", "p", &[('r', None)]).unwrap_err();
        assert!(err.message().contains("invalid value for parameter"));
    }
}
