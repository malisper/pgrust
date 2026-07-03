// explain_format.c, text mode only; every XML/JSON/YAML arm is loud.
#![allow(non_snake_case)]

use core::fmt::Write;

use types_error::PgResult;

use crate::state::{ExplainState, EXPLAIN_FORMAT_TEXT};

#[cold]
#[inline(never)]
pub(crate) fn nontext_gap(es: &ExplainState<'_>, c_fn: &str) -> ! {
    panic!(
        "{c_fn} (explain_format.c): {:?} output unported (explain non-text format lane)",
        es.format
    )
}

// StringInfo append is fallible; explain output assembly maps failures to the
// same panic C's palloc OOM raises through.
pub(crate) struct Si<'a, 'b>(pub(crate) &'a mut stringinfo::StringInfo<'b>);

impl Write for Si<'_, '_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.0.append_str(s).map_err(|_| core::fmt::Error)
    }
}

macro_rules! append {
    ($es:expr, $($arg:tt)*) => {{
        use core::fmt::Write as _;
        write!($crate::format::Si(&mut $es.str), $($arg)*).expect("explain output append")
    }};
}
pub(crate) use append;

pub fn ExplainPropertyList(qlabel: &str, data: &[impl AsRef<str>], es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainPropertyList");
    }
    ExplainIndentText(es);
    append!(es, "{qlabel}: ");
    for (i, item) in data.iter().enumerate() {
        if i > 0 {
            append!(es, ", ");
        }
        append!(es, "{}", item.as_ref());
    }
    append!(es, "\n");
}

fn ExplainProperty(qlabel: &str, unit: Option<&str>, value: &str, es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainProperty");
    }
    ExplainIndentText(es);
    match unit {
        Some(u) => append!(es, "{qlabel}: {value} {u}\n"),
        None => append!(es, "{qlabel}: {value}\n"),
    }
}

pub fn ExplainPropertyText(qlabel: &str, value: &str, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, None, value, es);
}

pub fn ExplainPropertyInteger(qlabel: &str, unit: Option<&str>, value: i64, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, unit, &format!("{value}"), es);
}

pub fn ExplainPropertyUInteger(
    qlabel: &str,
    unit: Option<&str>,
    value: u64,
    es: &mut ExplainState<'_>,
) {
    ExplainProperty(qlabel, unit, &format!("{value}"), es);
}

pub fn ExplainPropertyFloat(
    qlabel: &str,
    unit: Option<&str>,
    value: f64,
    ndigits: usize,
    es: &mut ExplainState<'_>,
) {
    ExplainProperty(qlabel, unit, &format!("{value:.ndigits$}"), es);
}

pub fn ExplainPropertyBool(qlabel: &str, value: bool, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, None, if value { "true" } else { "false" }, es);
}

pub fn ExplainOpenGroup(_objtype: &str, _labelname: Option<&str>, _labeled: bool, es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainOpenGroup");
    }
}

pub fn ExplainCloseGroup(_objtype: &str, _labelname: Option<&str>, _labeled: bool, es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainCloseGroup");
    }
}

pub fn ExplainBeginOutput(es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainBeginOutput");
    }
}

pub fn ExplainEndOutput(es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainEndOutput");
    }
}

pub fn ExplainSeparatePlans(es: &mut ExplainState<'_>) -> PgResult<()> {
    if es.format != EXPLAIN_FORMAT_TEXT {
        nontext_gap(es, "ExplainSeparatePlans");
    }
    es.str.append_byte(b'\n')
}

pub fn ExplainIndentText(es: &mut ExplainState<'_>) {
    debug_assert_eq!(es.format, EXPLAIN_FORMAT_TEXT);
    let bytes = es.str.as_bytes();
    if bytes.is_empty() || bytes[bytes.len() - 1] == b'\n' {
        es.str
            .append_spaces(es.indent as usize * 2)
            .expect("explain output append");
    }
}
