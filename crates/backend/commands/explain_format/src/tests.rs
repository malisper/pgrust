//! Tests for the `explain_format.c` port.
//!
//! The two genuine externals (`escape_json`, `escape_xml`) are installed through
//! their owners' seam slots. The slots are process-wide `OnceLock`s, so install
//! happens exactly once (under a `OnceLock` guard) before any test runs.

use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgString};
use types_error::PgResult;
use types_explain::{ExplainFormat, ExplainState};

use super::*;

// ---------------------------------------------------------------------------
// seam installation
// ---------------------------------------------------------------------------

/// A faithful, dependency-free `escape_json`: append the JSON string literal,
/// quoting the value and escaping the JSON control/`"`/`\` set, exactly like
/// `utils/adt/json.c`'s `escape_json`.
fn test_escape_json(buf: &mut PgString<'_>, s: &str) -> PgResult<()> {
    buf.try_push('"')?;
    for c in s.chars() {
        match c {
            '\u{8}' => buf.try_push_str("\\b")?,
            '\u{c}' => buf.try_push_str("\\f")?,
            '\n' => buf.try_push_str("\\n")?,
            '\r' => buf.try_push_str("\\r")?,
            '\t' => buf.try_push_str("\\t")?,
            '"' => buf.try_push_str("\\\"")?,
            '\\' => buf.try_push_str("\\\\")?,
            c if (c as u32) < 0x20 => {
                let mut tmp = [0u8; 8];
                use core::fmt::Write;
                let mut w = TmpWrite { buf: &mut tmp, len: 0 };
                let _ = write!(w, "\\u{:04x}", c as u32);
                buf.try_push_str(w.as_str())?;
            }
            c => buf.try_push(c)?,
        }
    }
    buf.try_push('"')?;
    Ok(())
}

struct TmpWrite<'a> {
    buf: &'a mut [u8; 8],
    len: usize,
}
impl TmpWrite<'_> {
    fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap()
    }
}
impl core::fmt::Write for TmpWrite<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let b = s.as_bytes();
        self.buf[self.len..self.len + b.len()].copy_from_slice(b);
        self.len += b.len();
        Ok(())
    }
}

/// A faithful `escape_xml`: replace the XML metacharacters as
/// `utils/adt/xml.c`'s `escape_xml` does.
fn test_escape_xml<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    for c in s.chars() {
        match c {
            '&' => out.try_push_str("&amp;")?,
            '<' => out.try_push_str("&lt;")?,
            '>' => out.try_push_str("&gt;")?,
            '\r' => out.try_push_str("&#x0d;")?,
            c => out.try_push(c)?,
        }
    }
    Ok(out)
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        escape_json::set(test_escape_json);
        escape_xml::set(test_escape_xml);
    });
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn new_es<'mcx>(mcx: Mcx<'mcx>, format: ExplainFormat) -> ExplainState<'mcx> {
    let mut es = ExplainState::new_in(mcx);
    es.format = format;
    es
}

macro_rules! es_test {
    ($name:ident, $fmt:ident, |$es:ident| $body:block, $expect:expr) => {
        #[test]
        fn $name() {
            install_seams();
            let ctx = MemoryContext::new("explain-format-test");
            let mut $es = new_es(ctx.mcx(), ExplainFormat::$fmt);
            (|| -> PgResult<()> { $body Ok(()) })().unwrap();
            assert_eq!($es.str.as_str(), $expect);
        }
    };
}

// ---------------------------------------------------------------------------
// ExplainPropertyText
// ---------------------------------------------------------------------------

es_test!(property_text_text_format, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyText("Node Type", "Seq Scan", &mut es)?;
}, "Node Type: Seq Scan\n");

es_test!(property_text_json_format, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyText("Node Type", "Seq Scan", &mut es)?;
}, "[\n  \"Node Type\": \"Seq Scan\"");

es_test!(property_text_yaml_format, EXPLAIN_FORMAT_YAML, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyText("Node Type", "Seq Scan", &mut es)?;
}, "Node Type: \"Seq Scan\"");

es_test!(property_text_xml_format, EXPLAIN_FORMAT_XML, |es| {
    ExplainPropertyText("Node Type", "Seq Scan", &mut es)?;
}, "<Node-Type>Seq Scan</Node-Type>\n");

// ---------------------------------------------------------------------------
// ExplainProperty* numeric wrappers
// ---------------------------------------------------------------------------

es_test!(property_integer_with_unit_text, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyInteger("Workers", Some("ms"), 42, &mut es)?;
}, "Workers: 42 ms\n");

es_test!(property_integer_no_unit_text, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyInteger("Rows", None, -7, &mut es)?;
}, "Rows: -7\n");

#[test]
fn property_uinteger_text() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_TEXT);
    ExplainPropertyUInteger("Bytes", Some("kB"), u64::MAX, &mut es).unwrap();
    assert_eq!(es.str.as_str(), format!("Bytes: {} kB\n", u64::MAX));
}

es_test!(property_float_text, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyFloat("Total Cost", None, 12.3456, 2, &mut es)?;
}, "Total Cost: 12.35\n");

es_test!(property_float_zero_digits, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyFloat("Rows", None, 3.9, 0, &mut es)?;
}, "Rows: 4\n");

es_test!(property_bool_numeric_json, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyBool("Parallel Aware", true, &mut es)?;
}, "[\n  \"Parallel Aware\": true");

es_test!(property_bool_text, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyBool("Inner Unique", false, &mut es)?;
}, "Inner Unique: false\n");

// ---------------------------------------------------------------------------
// ExplainPropertyList
// ---------------------------------------------------------------------------

es_test!(property_list_text, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyList("Sort Key", &["a", "b", "c"], &mut es)?;
}, "Sort Key: a, b, c\n");

es_test!(property_list_json, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyList("Sort Key", &["a", "b"], &mut es)?;
}, "[\n  \"Sort Key\": [\"a\", \"b\"]");

es_test!(property_list_xml, EXPLAIN_FORMAT_XML, |es| {
    es.indent = 1;
    ExplainPropertyList("Sort Key", &["a"], &mut es)?;
}, "  <Sort-Key>\n    <Item>a</Item>\n  </Sort-Key>\n");

es_test!(property_list_yaml, EXPLAIN_FORMAT_YAML, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyList("Sort Key", &["a", "b"], &mut es)?;
}, "Sort Key: \n  - \"a\"\n  - \"b\"");

es_test!(property_list_nested_text_delegates, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainPropertyListNested("Grouping Sets", &["x", "y"], &mut es)?;
}, "Grouping Sets: x, y\n");

es_test!(property_list_nested_json, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyListNested("Sets", &["x", "y"], &mut es)?;
}, "[\n  [\"x\", \"y\"]");

// ---------------------------------------------------------------------------
// XML tag-name sanitizing
// ---------------------------------------------------------------------------

es_test!(xml_tag_invalid_chars_become_dashes, EXPLAIN_FORMAT_XML, |es| {
    ExplainPropertyText("I/O Read Time", "5", &mut es)?;
}, "<I-O-Read-Time>5</I-O-Read-Time>\n");

// ---------------------------------------------------------------------------
// Begin/End output + grouping_stack semantics
// ---------------------------------------------------------------------------

#[test]
fn json_begin_end_output() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_JSON);
    ExplainBeginOutput(&mut es).unwrap();
    assert_eq!(es.grouping_stack.as_slice(), &[0]);
    assert_eq!(es.indent, 1);
    ExplainEndOutput(&mut es).unwrap();
    assert_eq!(es.indent, 0);
    assert!(es.grouping_stack.is_empty());
    assert_eq!(es.str.as_str(), "[\n]");
}

es_test!(xml_begin_end_output, EXPLAIN_FORMAT_XML, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainEndOutput(&mut es)?;
}, "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n</explain>");

#[test]
fn json_open_close_group_labeled() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_JSON);
    ExplainBeginOutput(&mut es).unwrap();
    ExplainOpenGroup("Plan", Some("Plan"), true, &mut es).unwrap();
    assert_eq!(es.grouping_stack.as_slice(), &[0, 1]);
    assert_eq!(es.indent, 2);
    ExplainPropertyText("Node Type", "Result", &mut es).unwrap();
    ExplainCloseGroup("Plan", Some("Plan"), true, &mut es).unwrap();
    assert_eq!(es.grouping_stack.as_slice(), &[1]);
    assert_eq!(es.indent, 1);
    assert_eq!(
        es.str.as_str(),
        "[\n  \"Plan\": {\n    \"Node Type\": \"Result\"\n  }"
    );
}

es_test!(json_comma_between_two_properties, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyText("A", "1", &mut es)?;
    ExplainPropertyText("B", "2", &mut es)?;
}, "[\n  \"A\": \"1\",\n  \"B\": \"2\"");

#[test]
fn text_open_close_group_noop_output() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_TEXT);
    ExplainOpenGroup("Plan", Some("Plan"), true, &mut es).unwrap();
    ExplainCloseGroup("Plan", Some("Plan"), true, &mut es).unwrap();
    assert_eq!(es.str.as_str(), "");
    assert_eq!(es.indent, 0);
}

// ---------------------------------------------------------------------------
// SetAside / Save / Restore round-trip
// ---------------------------------------------------------------------------

#[test]
fn json_set_aside_save_restore_roundtrip() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_JSON);
    ExplainBeginOutput(&mut es).unwrap();
    let before_stack: Vec<i32> = es.grouping_stack.as_slice().to_vec();
    let before_indent = es.indent;

    ExplainOpenSetAsideGroup("Plan", Some("Plan"), true, 1, &mut es).unwrap();
    assert_eq!(es.indent, before_indent + 1);
    es.grouping_stack[0] = 1;
    let saved = ExplainSaveGroup(&mut es, 1);
    assert_eq!(saved, 1);
    assert_eq!(es.indent, before_indent);
    assert_eq!(es.grouping_stack.as_slice(), before_stack.as_slice());

    ExplainRestoreGroup(&mut es, 1, saved).unwrap();
    assert_eq!(es.indent, before_indent + 1);
    assert_eq!(es.grouping_stack[0], 1);
}

#[test]
fn xml_save_restore_only_indent() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_XML);
    ExplainOpenSetAsideGroup("Plan", None, false, 2, &mut es).unwrap();
    assert_eq!(es.indent, 2);
    let saved = ExplainSaveGroup(&mut es, 2);
    assert_eq!(saved, 0); // unspecified/zero for XML
    assert_eq!(es.indent, 0);
    ExplainRestoreGroup(&mut es, 2, saved).unwrap();
    assert_eq!(es.indent, 2);
}

// ---------------------------------------------------------------------------
// DummyGroup
// ---------------------------------------------------------------------------

es_test!(dummy_group_xml, EXPLAIN_FORMAT_XML, |es| {
    ExplainDummyGroup("Triggers", None, &mut es)?;
}, "<Triggers />\n");

es_test!(dummy_group_json_labeled, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainDummyGroup("Triggers", Some("Trig"), &mut es)?;
}, "[\n  \"Trig\": \"Triggers\"");

es_test!(dummy_group_yaml_unlabeled, EXPLAIN_FORMAT_YAML, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainDummyGroup("Triggers", None, &mut es)?;
}, "- \"Triggers\"");

// ---------------------------------------------------------------------------
// ExplainSeparatePlans
// ---------------------------------------------------------------------------

es_test!(separate_plans_text_adds_blank_line, EXPLAIN_FORMAT_TEXT, |es| {
    ExplainSeparatePlans(&mut es)?;
}, "\n");

es_test!(separate_plans_json_noop, EXPLAIN_FORMAT_JSON, |es| {
    ExplainSeparatePlans(&mut es)?;
}, "");

// ---------------------------------------------------------------------------
// ExplainIndentText "already on a line" behavior
// ---------------------------------------------------------------------------

#[test]
fn indent_text_only_at_line_start() {
    install_seams();
    let ctx = MemoryContext::new("explain-format-test");
    let mut es = new_es(ctx.mcx(), ExplainFormat::EXPLAIN_FORMAT_TEXT);
    es.indent = 2;
    // Empty buffer -> indents (len==0 path).
    ExplainIndentText(&mut es).unwrap();
    assert_eq!(es.str.as_str(), "    ");
    // Now there is non-newline data; another indent is suppressed.
    es.str.try_push('x').unwrap();
    ExplainIndentText(&mut es).unwrap();
    assert_eq!(es.str.as_str(), "    x");
    // After a newline, indents again.
    es.str.try_push('\n').unwrap();
    ExplainIndentText(&mut es).unwrap();
    assert_eq!(es.str.as_str(), "    x\n    ");
}

// ---------------------------------------------------------------------------
// escaping is routed through the seams
// ---------------------------------------------------------------------------

es_test!(json_escapes_special_chars, EXPLAIN_FORMAT_JSON, |es| {
    ExplainBeginOutput(&mut es)?;
    ExplainPropertyText("k", "a\"b\\c", &mut es)?;
}, "[\n  \"k\": \"a\\\"b\\\\c\"");

es_test!(xml_escapes_metacharacters, EXPLAIN_FORMAT_XML, |es| {
    ExplainPropertyText("k", "a<b>&c", &mut es)?;
}, "<k>a&lt;b&gt;&amp;c</k>\n");
