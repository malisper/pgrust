// explain_format.c.
#![allow(non_snake_case)]

use core::fmt::Write;

use stringinfo::StringInfo;
use types_error::PgResult;

use crate::state::{
    ExplainState, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_TEXT, EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_YAML,
};

const X_OPENING: i32 = 0;
const X_CLOSING: i32 = 1;
const X_CLOSE_IMMEDIATE: i32 = 2;
const X_NOWHITESPACE: i32 = 4;

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

// escape_json (json.c): quoted string literal with control/quote escapes.
fn escape_json(buf: &mut StringInfo<'_>, s: &str) {
    fn put(buf: &mut StringInfo<'_>, b: &[u8]) {
        buf.append_bytes(b).expect("explain output append");
    }
    put(buf, b"\"");
    for &c in s.as_bytes() {
        match c {
            b'\x08' => put(buf, b"\\b"),
            b'\x0c' => put(buf, b"\\f"),
            b'\n' => put(buf, b"\\n"),
            b'\r' => put(buf, b"\\r"),
            b'\t' => put(buf, b"\\t"),
            b'"' => put(buf, b"\\\""),
            b'\\' => put(buf, b"\\\\"),
            _ if c < b' ' => {
                use core::fmt::Write as _;
                write!(Si(buf), "\\u{:04x}", c).expect("explain output append")
            }
            _ => buf.append_byte(c).expect("explain output append"),
        }
    }
    put(buf, b"\"");
}

// escape_xml (xml.c).
fn escape_xml(buf: &mut StringInfo<'_>, s: &str) {
    fn put(buf: &mut StringInfo<'_>, b: &[u8]) {
        buf.append_bytes(b).expect("explain output append");
    }
    for &c in s.as_bytes() {
        match c {
            b'&' => put(buf, b"&amp;"),
            b'<' => put(buf, b"&lt;"),
            b'>' => put(buf, b"&gt;"),
            b'\r' => put(buf, b"&#x0d;"),
            _ => buf.append_byte(c).expect("explain output append"),
        }
    }
}

// C: YAML quoting rules are a superset headache; everything quotes as JSON.
fn escape_yaml(buf: &mut StringInfo<'_>, s: &str) {
    escape_json(buf, s);
}

pub fn ExplainPropertyList(qlabel: &str, data: &[impl AsRef<str>], es: &mut ExplainState<'_>) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {
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
        EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(qlabel, X_OPENING, es);
            for item in data {
                es.str
                    .append_spaces(es.indent as usize * 2 + 2)
                    .expect("explain output append");
                append!(es, "<Item>");
                escape_xml(&mut es.str, item.as_ref());
                append!(es, "</Item>\n");
            }
            ExplainXMLTag(qlabel, X_CLOSING, es);
        }
        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            es.str.append_spaces(es.indent as usize * 2).expect("explain output append");
            escape_json(&mut es.str, qlabel);
            append!(es, ": [");
            for (i, item) in data.iter().enumerate() {
                if i > 0 {
                    append!(es, ", ");
                }
                escape_json(&mut es.str, item.as_ref());
            }
            append!(es, "]");
        }
        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            append!(es, "{qlabel}: ");
            for item in data {
                append!(es, "\n");
                es.str
                    .append_spaces(es.indent as usize * 2 + 2)
                    .expect("explain output append");
                append!(es, "- ");
                escape_yaml(&mut es.str, item.as_ref());
            }
        }
    }
}

pub fn ExplainPropertyListNested(
    qlabel: &str,
    data: &[impl AsRef<str>],
    es: &mut ExplainState<'_>,
) {
    match es.format {
        EXPLAIN_FORMAT_TEXT | EXPLAIN_FORMAT_XML => ExplainPropertyList(qlabel, data, es),
        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            es.str.append_spaces(es.indent as usize * 2).expect("explain output append");
            append!(es, "[");
            for (i, item) in data.iter().enumerate() {
                if i > 0 {
                    append!(es, ", ");
                }
                escape_json(&mut es.str, item.as_ref());
            }
            append!(es, "]");
        }
        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            append!(es, "- [");
            for (i, item) in data.iter().enumerate() {
                if i > 0 {
                    append!(es, ", ");
                }
                escape_yaml(&mut es.str, item.as_ref());
            }
            append!(es, "]");
        }
    }
}

fn ExplainProperty(
    qlabel: &str,
    unit: Option<&str>,
    value: &str,
    numeric: bool,
    es: &mut ExplainState<'_>,
) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {
            ExplainIndentText(es);
            match unit {
                Some(u) => append!(es, "{qlabel}: {value} {u}\n"),
                None => append!(es, "{qlabel}: {value}\n"),
            }
        }
        EXPLAIN_FORMAT_XML => {
            es.str.append_spaces(es.indent as usize * 2).expect("explain output append");
            ExplainXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, es);
            escape_xml(&mut es.str, value);
            ExplainXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, es);
            append!(es, "\n");
        }
        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            es.str.append_spaces(es.indent as usize * 2).expect("explain output append");
            escape_json(&mut es.str, qlabel);
            append!(es, ": ");
            if numeric {
                append!(es, "{value}");
            } else {
                escape_json(&mut es.str, value);
            }
        }
        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            append!(es, "{qlabel}: ");
            if numeric {
                append!(es, "{value}");
            } else {
                escape_yaml(&mut es.str, value);
            }
        }
    }
}

pub fn ExplainPropertyText(qlabel: &str, value: &str, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, None, value, false, es);
}

pub fn ExplainPropertyInteger(qlabel: &str, unit: Option<&str>, value: i64, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, unit, &format!("{value}"), true, es);
}

pub fn ExplainPropertyUInteger(
    qlabel: &str,
    unit: Option<&str>,
    value: u64,
    es: &mut ExplainState<'_>,
) {
    ExplainProperty(qlabel, unit, &format!("{value}"), true, es);
}

pub fn ExplainPropertyFloat(
    qlabel: &str,
    unit: Option<&str>,
    value: f64,
    ndigits: usize,
    es: &mut ExplainState<'_>,
) {
    ExplainProperty(qlabel, unit, &format!("{value:.ndigits$}"), true, es);
}

pub fn ExplainPropertyBool(qlabel: &str, value: bool, es: &mut ExplainState<'_>) {
    ExplainProperty(qlabel, None, if value { "true" } else { "false" }, true, es);
}

pub fn ExplainOpenGroup(
    objtype: &str,
    labelname: Option<&str>,
    labeled: bool,
    es: &mut ExplainState<'_>,
) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(objtype, X_OPENING, es);
            es.indent += 1;
        }
        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            es.str.append_spaces(2 * es.indent as usize).expect("explain output append");
            if let Some(label) = labelname {
                escape_json(&mut es.str, label);
                append!(es, ": ");
            }
            append!(es, "{}", if labeled { '{' } else { '[' });
            es.grouping_stack.push(0);
            es.indent += 1;
        }
        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            if let Some(label) = labelname {
                append!(es, "{label}: ");
                es.grouping_stack.push(1);
            } else {
                append!(es, "- ");
                es.grouping_stack.push(0);
            }
            es.indent += 1;
        }
    }
}

pub fn ExplainCloseGroup(
    objtype: &str,
    _labelname: Option<&str>,
    labeled: bool,
    es: &mut ExplainState<'_>,
) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => {
            es.indent -= 1;
            ExplainXMLTag(objtype, X_CLOSING, es);
        }
        EXPLAIN_FORMAT_JSON => {
            es.indent -= 1;
            append!(es, "\n");
            es.str.append_spaces(2 * es.indent as usize).expect("explain output append");
            append!(es, "{}", if labeled { '}' } else { ']' });
            es.grouping_stack.pop().expect("JSON grouping stack underflow");
        }
        EXPLAIN_FORMAT_YAML => {
            es.indent -= 1;
            es.grouping_stack.pop().expect("YAML grouping stack underflow");
        }
    }
}

pub fn ExplainOpenSetAsideGroup(
    _objtype: &str,
    labelname: Option<&str>,
    _labeled: bool,
    depth: i32,
    es: &mut ExplainState<'_>,
) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => es.indent += depth,
        EXPLAIN_FORMAT_JSON => {
            es.grouping_stack.push(0);
            es.indent += depth;
        }
        EXPLAIN_FORMAT_YAML => {
            es.grouping_stack.push(if labelname.is_some() { 1 } else { 0 });
            es.indent += depth;
        }
    }
}

pub fn ExplainSaveGroup(es: &mut ExplainState<'_>, depth: i32, state_save: &mut i32) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => es.indent -= depth,
        EXPLAIN_FORMAT_JSON | EXPLAIN_FORMAT_YAML => {
            es.indent -= depth;
            *state_save = es.grouping_stack.pop().expect("grouping stack underflow");
        }
    }
}

pub fn ExplainRestoreGroup(es: &mut ExplainState<'_>, depth: i32, state_save: &i32) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => es.indent += depth,
        EXPLAIN_FORMAT_JSON | EXPLAIN_FORMAT_YAML => {
            es.grouping_stack.push(*state_save);
            es.indent += depth;
        }
    }
}

pub fn ExplainDummyGroup(objtype: &str, labelname: Option<&str>, es: &mut ExplainState<'_>) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es),
        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            es.str.append_spaces(2 * es.indent as usize).expect("explain output append");
            if let Some(label) = labelname {
                escape_json(&mut es.str, label);
                append!(es, ": ");
            }
            escape_json(&mut es.str, objtype);
        }
        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            if let Some(label) = labelname {
                escape_yaml(&mut es.str, label);
                append!(es, ": ");
            } else {
                append!(es, "- ");
            }
            escape_yaml(&mut es.str, objtype);
        }
    }
}

pub fn ExplainBeginOutput(es: &mut ExplainState<'_>) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => {
            es.str
                .append_str("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n")
                .expect("explain output append");
            es.indent += 1;
        }
        EXPLAIN_FORMAT_JSON => {
            // top-level structure is an array of plans
            append!(es, "[");
            es.grouping_stack.push(0);
            es.indent += 1;
        }
        EXPLAIN_FORMAT_YAML => {
            es.grouping_stack.push(0);
        }
    }
}

pub fn ExplainEndOutput(es: &mut ExplainState<'_>) {
    match es.format {
        EXPLAIN_FORMAT_TEXT => {}
        EXPLAIN_FORMAT_XML => {
            es.indent -= 1;
            append!(es, "</explain>");
        }
        EXPLAIN_FORMAT_JSON => {
            es.indent -= 1;
            append!(es, "\n]");
            es.grouping_stack.pop().expect("JSON grouping stack underflow");
        }
        EXPLAIN_FORMAT_YAML => {
            es.grouping_stack.pop().expect("YAML grouping stack underflow");
        }
    }
}

pub fn ExplainSeparatePlans(es: &mut ExplainState<'_>) -> PgResult<()> {
    if es.format == EXPLAIN_FORMAT_TEXT {
        es.str.append_byte(b'\n')?;
    }
    Ok(())
}

// XML tag names are more restricted than the other formats' labels; invalid
// characters become dashes ("I/O Read Time" -> "I-O-Read-Time").
fn ExplainXMLTag(tagname: &str, flags: i32, es: &mut ExplainState<'_>) {
    if flags & X_NOWHITESPACE == 0 {
        es.str.append_spaces(2 * es.indent as usize).expect("explain output append");
    }
    es.str.append_byte(b'<').expect("explain output append");
    if flags & X_CLOSING != 0 {
        es.str.append_byte(b'/').expect("explain output append");
    }
    for &b in tagname.as_bytes() {
        let ok = b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.';
        es.str.append_byte(if ok { b } else { b'-' }).expect("explain output append");
    }
    if flags & X_CLOSE_IMMEDIATE != 0 {
        es.str.append_str(" /").expect("explain output append");
    }
    es.str.append_byte(b'>').expect("explain output append");
    if flags & X_NOWHITESPACE == 0 {
        es.str.append_byte(b'\n').expect("explain output append");
    }
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

// Each JSON property is emitted just after the preceding line break (and
// comma, when the group already has members).
fn ExplainJSONLineEnding(es: &mut ExplainState<'_>) {
    debug_assert_eq!(es.format, EXPLAIN_FORMAT_JSON);
    let top = es.grouping_stack.last_mut().expect("JSON grouping stack underflow");
    if *top != 0 {
        es.str.append_byte(b',').expect("explain output append");
    } else {
        *top = 1;
    }
    es.str.append_byte(b'\n').expect("explain output append");
}

// The first property of an unlabeled YAML group rides the group's own "- "
// line; every later property starts a fresh indented line.
fn ExplainYAMLLineStarting(es: &mut ExplainState<'_>) {
    debug_assert_eq!(es.format, EXPLAIN_FORMAT_YAML);
    let top = es.grouping_stack.last_mut().expect("YAML grouping stack underflow");
    if *top == 0 {
        *top = 1;
    } else {
        es.str.append_byte(b'\n').expect("explain output append");
        es.str
            .append_spaces(es.indent as usize * 2)
            .expect("explain output append");
    }
}
