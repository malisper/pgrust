//! src/common/percentrepl.c, hosted here until common-percentrepl lands.

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

fn loc() -> ErrorLocation {
    ErrorLocation::new("percentrepl.c", 0, "replace_percent_placeholders")
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
