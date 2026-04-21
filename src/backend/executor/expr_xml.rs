use super::exec_expr::eval_expr;
use super::expr_casts::cast_value_with_config;
use super::{ExecError, ExecutorContext, TupleSlot, format_array_value_text};
use crate::backend::utils::misc::guc_xml::XmlBinaryFormat;
use crate::backend::utils::misc::guc_xml::XmlOptionSetting;
use crate::include::nodes::datetime::{DateADT, TimestampADT, TimestampTzADT};
use crate::include::nodes::datum::{ArrayValue, Value};
use crate::include::nodes::primnodes::{XmlExpr, XmlExprOp};
use crate::pgrust::compact_string::CompactString;
use base64::Engine as _;
use quick_xml::Reader;
use quick_xml::events::Event;

fn xml_input_error(
    raw_input: &str,
    message: &str,
    detail: Option<String>,
    sqlstate: &'static str,
) -> ExecError {
    ExecError::XmlInput {
        raw_input: raw_input.to_string(),
        message: message.to_string(),
        detail,
        context: None,
        sqlstate,
    }
}

fn is_xml_whitespace(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .all(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
}

fn xml_validation_error_for_option(
    text: &str,
    option: XmlOptionSetting,
    detail: String,
) -> ExecError {
    let (message, sqlstate) = match option {
        XmlOptionSetting::Document => ("invalid XML document", "2200M"),
        XmlOptionSetting::Content => ("invalid XML content", "2200N"),
    };
    xml_input_error(text, message, Some(detail), sqlstate)
}

fn parse_xml_decl_attributes(body: &str) -> Result<Vec<(String, String)>, String> {
    let mut attrs = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let name_start = i;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b':' | b'-')) {
            i += 1;
        }
        if i == name_start {
            return Err("malformed XML declaration".into());
        }
        let name = &body[name_start..i];
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            return Err("malformed XML declaration".into());
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            return Err("malformed XML declaration".into());
        }
        i += 1;
        let value_start = i;
        while i < bytes.len() && bytes[i] != b'"' {
            i += 1;
        }
        if i >= bytes.len() {
            return Err("malformed XML declaration".into());
        }
        attrs.push((name.to_string(), body[value_start..i].to_string()));
        i += 1;
    }
    Ok(attrs)
}

fn validate_xml_declaration(text: &str, option: XmlOptionSetting) -> Result<(), ExecError> {
    let Some((decl_text, _)) = find_xml_declaration(text) else {
        return Ok(());
    };
    let body = decl_text
        .strip_prefix("<?xml")
        .and_then(|rest| rest.strip_suffix("?>"))
        .map(str::trim)
        .ok_or_else(|| xml_validation_error_for_option(text, option, "malformed XML declaration".into()))?;
    let attrs = parse_xml_decl_attributes(body)
        .map_err(|detail| xml_validation_error_for_option(text, option, detail))?;
    for (name, value) in attrs {
        if name == "standalone" && value != "yes" && value != "no" {
            return Err(xml_validation_error_for_option(
                text,
                option,
                "invalid standalone value in XML declaration".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_xml_input(text: &str, option: XmlOptionSetting) -> Result<(), ExecError> {
    validate_xml_declaration(text, option)?;

    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(false);

    let mut depth = 0usize;
    let mut seen_document_element = false;
    let mut after_document_element = false;
    let mut seen_doctype = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(_)) => {
                if depth == 0 && matches!(option, XmlOptionSetting::Document) {
                    if after_document_element || seen_document_element {
                        return Err(xml_input_error(
                            text,
                            "invalid XML document",
                            Some("XML document must have exactly one top-level element".into()),
                            "2200M",
                        ));
                    }
                    seen_document_element = true;
                }
                depth += 1;
            }
            Ok(Event::Empty(_)) => {
                if depth == 0 && matches!(option, XmlOptionSetting::Document) {
                    if after_document_element || seen_document_element {
                        return Err(xml_input_error(
                            text,
                            "invalid XML document",
                            Some("XML document must have exactly one top-level element".into()),
                            "2200M",
                        ));
                    }
                    seen_document_element = true;
                    after_document_element = true;
                }
            }
            Ok(Event::End(_)) => {
                if depth == 0 {
                    return Err(xml_input_error(
                        text,
                        "invalid XML content",
                        Some("unexpected closing tag".into()),
                        "2200N",
                    ));
                }
                depth -= 1;
                if depth == 0 && seen_document_element {
                    after_document_element = true;
                }
            }
            Ok(Event::Text(text_event)) => {
                if depth == 0
                    && matches!(option, XmlOptionSetting::Document)
                    && !is_xml_whitespace(text_event.as_ref())
                {
                    return Err(xml_input_error(
                        text,
                        "invalid XML document",
                        Some(
                            "non-whitespace text is not allowed outside the document element"
                                .into(),
                        ),
                        "2200M",
                    ));
                }
            }
            Ok(Event::CData(text_event)) => {
                if depth == 0
                    && matches!(option, XmlOptionSetting::Document)
                    && !is_xml_whitespace(text_event.as_ref())
                {
                    return Err(xml_input_error(
                        text,
                        "invalid XML document",
                        Some("CDATA is not allowed outside the document element".into()),
                        "2200M",
                    ));
                }
            }
            Ok(Event::DocType(_)) => {
                if matches!(option, XmlOptionSetting::Document) {
                    if seen_doctype || seen_document_element || after_document_element {
                        return Err(xml_input_error(
                            text,
                            "invalid XML document",
                            Some("DOCTYPE must appear before the document element".into()),
                            "2200M",
                        ));
                    }
                    seen_doctype = true;
                }
            }
            Ok(Event::Decl(_))
            | Ok(Event::PI(_))
            | Ok(Event::Comment(_))
            | Ok(Event::GeneralRef(_)) => {}
            Ok(Event::Eof) => break,
            Err(err) => {
                let (message, sqlstate) = match option {
                    XmlOptionSetting::Document => ("invalid XML document", "2200M"),
                    XmlOptionSetting::Content => ("invalid XML content", "2200N"),
                };
                return Err(xml_input_error(
                    text,
                    message,
                    Some(err.to_string()),
                    sqlstate,
                ));
            }
        }
    }

    if matches!(option, XmlOptionSetting::Document) && !seen_document_element {
        return Err(xml_input_error(
            text,
            "invalid XML document",
            Some("XML document must have a top-level element".into()),
            "2200M",
        ));
    }

    if depth != 0 {
        let (message, sqlstate) = match option {
            XmlOptionSetting::Document => ("invalid XML document", "2200M"),
            XmlOptionSetting::Content => ("invalid XML content", "2200N"),
        };
        return Err(xml_input_error(
            text,
            message,
            Some("unclosed XML element".into()),
            sqlstate,
        ));
    }

    Ok(())
}

pub(crate) fn xml_is_well_formed(text: &str, option: XmlOptionSetting) -> bool {
    validate_xml_input(text, option).is_ok()
}

fn xml_detail_error(message: &str, detail: Option<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}

fn xml_escape(text: &str, attribute: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#x0d;"),
            '"' if attribute => out.push_str("&quot;"),
            '\'' if attribute => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

#[derive(Debug, Clone)]
struct XmlDecl {
    version: Option<String>,
    standalone: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XmlDeclStandalone {
    Yes,
    No,
    NoValue,
}

#[derive(Debug, Clone)]
enum XmlNode {
    Element {
        name: String,
        attrs: Vec<(String, String)>,
        children: Vec<XmlNode>,
    },
    Text(String),
    CData(String),
    Comment(String),
    Pi(String),
    Doctype(String),
}

fn find_xml_declaration(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim_start();
    if let Some(rest) = trimmed.strip_prefix("<?xml")
        && let Some(end) = rest.find("?>")
    {
        let decl_len = 5 + end + 2;
        return Some((&trimmed[..decl_len], &trimmed[decl_len..]));
    }
    None
}

fn split_xml_declaration(text: &str) -> (Option<XmlDecl>, &str) {
    if let Some((decl_text, body)) = find_xml_declaration(text) {
        return (
            Some(XmlDecl {
                version: extract_decl_attr(decl_text, "version"),
                standalone: extract_decl_attr(decl_text, "standalone").map(|value| value == "yes"),
            }),
            body,
        );
    }
    (None, text)
}

fn strip_xml_declaration(text: &str) -> &str {
    find_xml_declaration(text).map_or(text, |(_, body)| body)
}

fn extract_decl_attr(text: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let start = text.find(&needle)? + needle.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn print_xml_decl(version: Option<&str>, standalone: Option<bool>) -> Option<String> {
    if version.is_some_and(|v| v != "1.0") || standalone.is_some() {
        let mut out = String::from("<?xml");
        out.push_str(" version=\"");
        out.push_str(version.unwrap_or("1.0"));
        out.push('"');
        match standalone {
            Some(true) => out.push_str(" standalone=\"yes\""),
            Some(false) => out.push_str(" standalone=\"no\""),
            None => {}
        }
        out.push_str("?>");
        Some(out)
    } else {
        None
    }
}

fn xml_decl_info_for_concat(text: &str) -> (Option<String>, XmlDeclStandalone, &str) {
    if let Some((decl_text, body)) = find_xml_declaration(text) {
        let standalone = match extract_decl_attr(decl_text, "standalone").as_deref() {
            Some("yes") => XmlDeclStandalone::Yes,
            Some("no") => XmlDeclStandalone::No,
            _ => XmlDeclStandalone::NoValue,
        };
        return (extract_decl_attr(decl_text, "version"), standalone, body);
    }
    (None, XmlDeclStandalone::NoValue, text)
}

pub(crate) fn concat_xml_texts<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut body = String::new();
    let mut saw_any = false;
    let mut global_standalone = XmlDeclStandalone::Yes;
    let mut global_version: Option<String> = None;
    let mut global_version_no_value = false;

    for value in values {
        saw_any = true;
        let (version, standalone, content) = xml_decl_info_for_concat(value);
        if standalone == XmlDeclStandalone::No && global_standalone == XmlDeclStandalone::Yes {
            global_standalone = XmlDeclStandalone::No;
        }
        if standalone == XmlDeclStandalone::NoValue {
            global_standalone = XmlDeclStandalone::NoValue;
        }

        match version {
            None => global_version_no_value = true,
            Some(version) => match &global_version {
                None => global_version = Some(version),
                Some(existing) if existing != &version => global_version_no_value = true,
                Some(_) => {}
            },
        }

        body.push_str(content);
    }

    if !saw_any {
        return String::new();
    }

    let standalone = match global_standalone {
        XmlDeclStandalone::Yes => Some(true),
        XmlDeclStandalone::No => Some(false),
        XmlDeclStandalone::NoValue => None,
    };
    let version = (!global_version_no_value)
        .then_some(global_version.as_deref())
        .flatten();

    let mut out = String::new();
    if !global_version_no_value || global_standalone != XmlDeclStandalone::NoValue {
        if let Some(decl) = print_xml_decl(version, standalone) {
            out.push_str(&decl);
        }
    }
    out.push_str(&body);
    out
}

pub(crate) fn xml_comment(text: &str) -> Result<String, ExecError> {
    if text.contains("--") || text.ends_with('-') {
        return Err(xml_detail_error("invalid XML comment", None, "2200S"));
    }
    Ok(format!("<!--{text}-->"))
}

fn is_valid_xml_name_first(ch: char) -> bool {
    ch == ':' || ch == '_' || ch.is_alphabetic()
}

fn is_valid_xml_name_char(ch: char) -> bool {
    is_valid_xml_name_first(ch) || ch.is_ascii_digit() || matches!(ch, '.' | '-')
}

fn map_sql_identifier_to_xml_name(ident: &str, escape_period: bool) -> String {
    let mut out = String::new();
    let chars: Vec<char> = ident.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == ':' && i == 0 {
            out.push_str("_x003A_");
        } else if ch == '_' && chars.get(i + 1) == Some(&'x') {
            out.push_str("_x005F_");
        } else if i == 0 && ident.len() >= 3 && ident[..3].eq_ignore_ascii_case("xml") {
            let encoded = if ch == 'x' { "_x0078_" } else { "_x0058_" };
            out.push_str(encoded);
        } else if escape_period && ch == '.' {
            out.push_str("_x002E_");
        } else if (i == 0 && !is_valid_xml_name_first(ch)) || (i > 0 && !is_valid_xml_name_char(ch))
        {
            out.push_str(&format!("_x{:04X}_", ch as u32));
        } else {
            out.push(ch);
        }
        i += 1;
    }
    out
}

fn render_scalar_text(value: Value, ctx: &ExecutorContext) -> Result<String, ExecError> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Text(text) => Ok(text.to_string()),
        Value::TextRef(ptr, len) => Ok(unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize)).to_string()
        }),
        Value::Xml(text) => Ok(text.to_string()),
        Value::Bool(value) => Ok(if value { "true" } else { "false" }.to_string()),
        Value::Bytea(bytes) => Ok(match ctx.datetime_config.xml.binary {
            XmlBinaryFormat::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
            XmlBinaryFormat::Hex => bytes.iter().map(|b| format!("{b:02X}")).collect(),
        }),
        Value::Date(date) => render_xml_date_text(date),
        Value::Timestamp(timestamp) => render_xml_timestamp_text(timestamp, ctx),
        Value::TimestampTz(timestamp) => render_xml_timestamptz_text(timestamp, ctx),
        Value::Array(array) => Ok(format_array_value_text(&ArrayValue::from_1d(array))),
        Value::PgArray(array) => Ok(format_array_value_text(&array)),
        other => match cast_value_with_config(
            other,
            crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
            &ctx.datetime_config,
        )? {
            Value::Text(text) => Ok(text.to_string()),
            Value::TextRef(ptr, len) => Ok(unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
                    .to_string()
            }),
            Value::Xml(text) => Ok(text.to_string()),
            unexpected => Ok(format!("{unexpected:?}")),
        },
    }
}

fn render_xml_builtin_text(
    value: Value,
    ctx: Option<&ExecutorContext>,
) -> Result<String, ExecError> {
    if let Some(ctx) = ctx {
        return render_scalar_text(value, ctx);
    }
    match value {
        Value::Text(text) => Ok(text.to_string()),
        Value::TextRef(ptr, len) => Ok(unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize)).to_string()
        }),
        Value::Xml(text) => Ok(text.to_string()),
        other => other
            .as_text()
            .map(str::to_string)
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "xml",
                left: other,
                right: Value::Text("".into()),
            }),
    }
}

fn render_xml_date_text(value: DateADT) -> Result<String, ExecError> {
    if !value.is_finite() {
        return Err(xml_detail_error(
            "date out of range",
            Some("XML does not support infinite date values.".into()),
            "22008",
        ));
    }
    Ok(crate::backend::utils::time::date::format_date_text(
        value,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    ))
}

fn render_xml_timestamp_text(
    value: TimestampADT,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if !value.is_finite() {
        return Err(xml_detail_error(
            "timestamp out of range",
            Some("XML does not support infinite timestamp values.".into()),
            "22008",
        ));
    }
    Ok(crate::backend::utils::time::timestamp::format_timestamp_text(value, &ctx.datetime_config))
}

fn render_xml_timestamptz_text(
    value: TimestampTzADT,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if !value.is_finite() {
        return Err(xml_detail_error(
            "timestamp out of range",
            Some("XML does not support infinite timestamp values.".into()),
            "22008",
        ));
    }
    Ok(
        crate::backend::utils::time::timestamp::format_timestamptz_text(
            value,
            &ctx.datetime_config,
        ),
    )
}

fn render_sql_value_to_xml_value(
    value: Value,
    xml_escape_strings: bool,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Xml(text) => Ok(text.to_string()),
        Value::Array(values) => render_array_xml(ArrayValue::from_1d(values), ctx),
        Value::PgArray(array) => render_array_xml(array, ctx),
        other => {
            let text = render_scalar_text(other, ctx)?;
            if xml_escape_strings {
                Ok(xml_escape(&text, false))
            } else {
                Ok(text)
            }
        }
    }
}

fn render_array_xml(array: ArrayValue, ctx: &ExecutorContext) -> Result<String, ExecError> {
    let mut out = String::new();
    for value in array.elements {
        if matches!(value, Value::Null) {
            continue;
        }
        out.push_str("<element>");
        out.push_str(&render_sql_value_to_xml_value(value, true, ctx)?);
        out.push_str("</element>");
    }
    Ok(out)
}

fn render_xml_content_value(value: Value, ctx: &ExecutorContext) -> Result<String, ExecError> {
    render_sql_value_to_xml_value(value, true, ctx)
}

fn render_xml_attribute_value(
    value: Value,
    ctx: &ExecutorContext,
) -> Result<Option<String>, ExecError> {
    match value {
        Value::Null => Ok(None),
        Value::Xml(text) => Ok(Some(text.to_string())),
        Value::Array(values) => Ok(Some(render_array_xml(ArrayValue::from_1d(values), ctx)?)),
        Value::PgArray(array) => Ok(Some(render_array_xml(array, ctx)?)),
        other => Ok(Some(xml_escape(&render_scalar_text(other, ctx)?, true))),
    }
}

fn xml_option_to_setting(option: crate::backend::parser::XmlOption) -> XmlOptionSetting {
    match option {
        crate::backend::parser::XmlOption::Document => XmlOptionSetting::Document,
        crate::backend::parser::XmlOption::Content => XmlOptionSetting::Content,
    }
}

fn eval_xml_element(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let name = xml
        .name
        .as_deref()
        .ok_or_else(|| xml_detail_error("malformed XMLELEMENT expression", None, "XX000"))?;
    let name = map_sql_identifier_to_xml_name(name, false);
    let mut rendered = String::new();
    rendered.push('<');
    rendered.push_str(&name);
    for (arg, attr_name) in xml.named_args.iter().zip(xml.arg_names.iter()) {
        let value = eval_expr(arg, slot, ctx)?;
        if let Some(attr_value) = render_xml_attribute_value(value, ctx)? {
            rendered.push(' ');
            rendered.push_str(&map_sql_identifier_to_xml_name(attr_name, false));
            rendered.push_str("=\"");
            rendered.push_str(&attr_value);
            rendered.push('"');
        }
    }
    let mut content = String::new();
    for arg in &xml.args {
        content.push_str(&render_xml_content_value(eval_expr(arg, slot, ctx)?, ctx)?);
    }
    if content.is_empty() {
        rendered.push_str("/>");
    } else {
        rendered.push('>');
        rendered.push_str(&content);
        rendered.push_str("</");
        rendered.push_str(&name);
        rendered.push('>');
    }
    Ok(Value::Xml(CompactString::from_owned(rendered)))
}

fn eval_xml_forest(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut rendered = String::new();
    for (arg, name) in xml.args.iter().zip(xml.arg_names.iter()) {
        let value = eval_expr(arg, slot, ctx)?;
        if matches!(value, Value::Null) {
            continue;
        }
        let name = map_sql_identifier_to_xml_name(name, false);
        rendered.push('<');
        rendered.push_str(&name);
        rendered.push('>');
        rendered.push_str(&render_xml_content_value(value, ctx)?);
        rendered.push_str("</");
        rendered.push_str(&name);
        rendered.push('>');
    }
    Ok(Value::Xml(CompactString::from_owned(rendered)))
}

fn eval_xml_parse(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = eval_expr(&xml.args[0], slot, ctx)?;
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = render_scalar_text(value, ctx)?;
    validate_xml_input(
        &text,
        xml_option_to_setting(xml.xml_option.expect("XMLPARSE option")),
    )?;
    Ok(Value::Xml(CompactString::from_owned(text)))
}

pub(crate) fn eval_xml_comment_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let value = values.first().cloned().unwrap_or(Value::Null);
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Xml(CompactString::from_owned(xml_comment(
        &render_xml_builtin_text(value, ctx)?,
    )?)))
}

pub(crate) fn eval_xml_is_well_formed_function(
    values: &[Value],
    option: XmlOptionSetting,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let value = values.first().cloned().unwrap_or(Value::Null);
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Bool(xml_is_well_formed(
        &render_xml_builtin_text(value, ctx)?,
        option,
    )))
}

fn eval_xml_pi(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let name = xml
        .name
        .as_deref()
        .ok_or_else(|| xml_detail_error("malformed XMLPI expression", None, "XX000"))?;
    if name.eq_ignore_ascii_case("xml") {
        return Err(xml_detail_error(
            "invalid processing instruction name",
            Some("processing instruction target name cannot be \"xml\"".into()),
            "2200T",
        ));
    }
    let target = map_sql_identifier_to_xml_name(name, false);
    let mut rendered = String::from("<?");
    rendered.push_str(&target);
    if let Some(arg) = xml.args.first() {
        let value = eval_expr(arg, slot, ctx)?;
        if !matches!(value, Value::Null) {
            let text = render_scalar_text(value, ctx)?;
            if text.contains("?>") {
                return Err(xml_detail_error(
                    "invalid XML processing instruction",
                    Some("processing instruction content cannot contain ?>".into()),
                    "2200T",
                ));
            }
            if !text.is_empty() {
                rendered.push(' ');
                rendered.push_str(text.trim_start());
            }
        }
    }
    rendered.push_str("?>");
    Ok(Value::Xml(CompactString::from_owned(rendered)))
}

fn eval_xml_root(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = eval_expr(&xml.args[0], slot, ctx)?;
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = match value {
        Value::Xml(text) => text.to_string(),
        other => render_scalar_text(other, ctx)?,
    };
    let (decl, body) = split_xml_declaration(&text);
    let version = if let Some(version_expr) = xml.args.get(1) {
        let version = eval_expr(version_expr, slot, ctx)?;
        if matches!(version, Value::Null) {
            None
        } else {
            Some(render_scalar_text(version, ctx)?)
        }
    } else {
        None
    };
    let version = version.or_else(|| decl.as_ref().and_then(|decl| decl.version.clone()));
    let standalone = match xml.standalone {
        Some(crate::backend::parser::XmlStandalone::Yes) => Some(true),
        Some(crate::backend::parser::XmlStandalone::No) => Some(false),
        Some(crate::backend::parser::XmlStandalone::NoValue) => None,
        None => decl.as_ref().and_then(|decl| decl.standalone),
    };
    let mut rendered = String::new();
    if let Some(decl_text) = print_xml_decl(version.as_deref(), standalone) {
        rendered.push_str(&decl_text);
    }
    rendered.push_str(body.trim_start());
    Ok(Value::Xml(CompactString::from_owned(rendered)))
}

fn parse_xml_nodes(text: &str) -> Result<(Option<XmlDecl>, Vec<XmlNode>), ExecError> {
    let (decl, _) = split_xml_declaration(text);
    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(false);
    let mut stack: Vec<(String, Vec<(String, String)>, Vec<XmlNode>)> = Vec::new();
    let mut top = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let attrs = e
                    .attributes()
                    .with_checks(false)
                    .map(|attr| {
                        let attr = attr.map_err(|err| {
                            xml_detail_error("invalid XML content", Some(err.to_string()), "2200N")
                        })?;
                        Ok((
                            String::from_utf8_lossy(attr.key.as_ref()).into_owned(),
                            String::from_utf8_lossy(attr.value.as_ref()).into_owned(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;
                stack.push((name, attrs, Vec::new()));
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let attrs = e
                    .attributes()
                    .with_checks(false)
                    .map(|attr| {
                        let attr = attr.map_err(|err| {
                            xml_detail_error("invalid XML content", Some(err.to_string()), "2200N")
                        })?;
                        Ok((
                            String::from_utf8_lossy(attr.key.as_ref()).into_owned(),
                            String::from_utf8_lossy(attr.value.as_ref()).into_owned(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;
                let node = XmlNode::Element {
                    name,
                    attrs,
                    children: Vec::new(),
                };
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::End(_)) => {
                let (name, attrs, children) = stack
                    .pop()
                    .ok_or_else(|| xml_detail_error("invalid XML content", None, "2200N"))?;
                let node = XmlNode::Element {
                    name,
                    attrs,
                    children,
                };
                if let Some((_, _, parent_children)) = stack.last_mut() {
                    parent_children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::Text(e)) => {
                let node = XmlNode::Text(String::from_utf8_lossy(e.as_ref()).into_owned());
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::CData(e)) => {
                let node = XmlNode::CData(String::from_utf8_lossy(e.as_ref()).into_owned());
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::Comment(e)) => {
                let node = XmlNode::Comment(String::from_utf8_lossy(e.as_ref()).into_owned());
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::PI(e)) => {
                let node = XmlNode::Pi(String::from_utf8_lossy(e.as_ref()).into_owned());
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::DocType(e)) => top.push(XmlNode::Doctype(
                String::from_utf8_lossy(e.as_ref()).into_owned(),
            )),
            Ok(Event::Decl(_)) => {}
            Ok(Event::GeneralRef(e)) => {
                let node = XmlNode::Text(format!("&{};", String::from_utf8_lossy(e.as_ref())));
                if let Some((_, _, children)) = stack.last_mut() {
                    children.push(node);
                } else {
                    top.push(node);
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => {
                return Err(xml_detail_error(
                    "invalid XML content",
                    Some(err.to_string()),
                    "2200N",
                ));
            }
        }
    }
    Ok((decl, top))
}

fn node_is_significant_text(node: &XmlNode) -> bool {
    matches!(node, XmlNode::Text(text) if !text.trim().is_empty())
        || matches!(node, XmlNode::CData(text) if !text.trim().is_empty())
}

fn is_mixed_content(children: &[XmlNode]) -> bool {
    let has_element_like = children
        .iter()
        .any(|child| matches!(child, XmlNode::Element { .. }));
    let has_significant_text = children.iter().any(node_is_significant_text);
    has_element_like && has_significant_text
}

fn is_text_only_content(children: &[&XmlNode]) -> bool {
    !children.is_empty()
        && children
            .iter()
            .all(|child| matches!(child, XmlNode::Text(_) | XmlNode::CData(_)))
}

fn render_compact_node(node: &XmlNode, out: &mut String) {
    match node {
        XmlNode::Element {
            name,
            attrs,
            children,
        } => {
            out.push('<');
            out.push_str(name);
            for (key, value) in attrs {
                out.push(' ');
                out.push_str(key);
                out.push_str("=\"");
                out.push_str(value);
                out.push('"');
            }
            if children.is_empty() {
                out.push_str("/>");
            } else {
                out.push('>');
                for child in children {
                    render_compact_node(child, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            }
        }
        XmlNode::Text(text) => out.push_str(text),
        XmlNode::CData(text) => {
            out.push_str("<![CDATA[");
            out.push_str(text);
            out.push_str("]]>");
        }
        XmlNode::Comment(text) => {
            out.push_str("<!--");
            out.push_str(text);
            out.push_str("-->");
        }
        XmlNode::Pi(text) => {
            out.push_str("<?");
            out.push_str(text);
            out.push_str("?>");
        }
        XmlNode::Doctype(text) => {
            out.push_str("<!DOCTYPE ");
            out.push_str(text);
            out.push('>');
        }
    }
}

fn render_pretty_node(node: &XmlNode, depth: usize, out: &mut String) {
    let indent = "  ".repeat(depth);
    match node {
        XmlNode::Element {
            name,
            attrs,
            children,
        } => {
            out.push_str(&indent);
            out.push('<');
            out.push_str(name);
            for (key, value) in attrs {
                out.push(' ');
                out.push_str(key);
                out.push_str("=\"");
                out.push_str(value);
                out.push('"');
            }
            let filtered_children: Vec<&XmlNode> = children
                .iter()
                .filter(|child| !matches!(child, XmlNode::Text(text) if text.trim().is_empty()))
                .collect();
            if filtered_children.is_empty() {
                out.push_str("/>");
            } else if is_text_only_content(&filtered_children) {
                out.push('>');
                for child in filtered_children {
                    render_compact_node(child, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            } else if is_mixed_content(children) {
                out.push('>');
                for child in children {
                    render_compact_node(child, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            } else {
                out.push('>');
                for child in filtered_children {
                    out.push('\n');
                    render_pretty_node(child, depth + 1, out);
                }
                out.push('\n');
                out.push_str(&indent);
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            }
        }
        XmlNode::Text(text) => out.push_str(text),
        XmlNode::CData(text) => {
            out.push_str(&indent);
            out.push_str("<![CDATA[");
            out.push_str(text);
            out.push_str("]]>");
        }
        XmlNode::Comment(text) => {
            out.push_str(&indent);
            out.push_str("<!--");
            out.push_str(text);
            out.push_str("-->");
        }
        XmlNode::Pi(text) => {
            out.push_str(&indent);
            out.push_str("<?");
            out.push_str(text);
            out.push_str("?>");
        }
        XmlNode::Doctype(text) => {
            out.push_str(&indent);
            out.push_str("<!DOCTYPE ");
            out.push_str(text);
            out.push('>');
        }
    }
}

fn top_level_starts_on_new_line(node: &XmlNode) -> bool {
    !matches!(node, XmlNode::Text(_))
}

fn has_top_level_doctype(nodes: &[XmlNode]) -> bool {
    nodes.iter().any(|node| matches!(node, XmlNode::Doctype(_)))
}

fn format_xml_indent(text: &str, option: XmlOptionSetting) -> Result<String, ExecError> {
    let (_, nodes) = parse_xml_nodes(text)?;
    let mut out = String::new();
    if matches!(option, XmlOptionSetting::Document)
        && let Some((decl_text, _)) = find_xml_declaration(text)
    {
        out.push_str(decl_text);
        if !nodes.is_empty() {
            out.push('\n');
        }
    }
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 && top_level_starts_on_new_line(node) {
            out.push('\n');
        }
        render_pretty_node(node, 0, &mut out);
    }
    if matches!(option, XmlOptionSetting::Content) && has_top_level_doctype(&nodes) {
        out.push('\n');
    }
    Ok(out)
}

fn eval_xml_serialize(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = eval_expr(&xml.args[0], slot, ctx)?;
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let raw = match value {
        Value::Xml(text) => text.to_string(),
        other => render_scalar_text(other, ctx)?,
    };
    let option = xml_option_to_setting(xml.xml_option.expect("XMLSERIALIZE option"));
    match option {
        XmlOptionSetting::Document => {
            if validate_xml_input(&raw, XmlOptionSetting::Document).is_err() {
                return Err(xml_detail_error("not an XML document", None, "2200L"));
            }
        }
        XmlOptionSetting::Content => {
            validate_xml_input(&raw, XmlOptionSetting::Content)?;
        }
    }
    let rendered = match (option, xml.indent) {
        (XmlOptionSetting::Document, Some(true)) => {
            format_xml_indent(&raw, XmlOptionSetting::Document)?
        }
        (XmlOptionSetting::Content, Some(true)) => {
            format_xml_indent(&raw, XmlOptionSetting::Content)?
        }
        (XmlOptionSetting::Document, _) => raw,
        (XmlOptionSetting::Content, _) => strip_xml_declaration(&raw).trim_start().to_string(),
    };
    cast_value_with_config(
        Value::Text(CompactString::from_owned(rendered)),
        xml.target_type.expect("XMLSERIALIZE target type"),
        &ctx.datetime_config,
    )
}

pub(crate) fn eval_xml_expr(
    xml: &XmlExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match xml.op {
        XmlExprOp::Element => eval_xml_element(xml, slot, ctx),
        XmlExprOp::Forest => eval_xml_forest(xml, slot, ctx),
        XmlExprOp::Parse => eval_xml_parse(xml, slot, ctx),
        XmlExprOp::Pi => eval_xml_pi(xml, slot, ctx),
        XmlExprOp::Root => eval_xml_root(xml, slot, ctx),
        XmlExprOp::Serialize => eval_xml_serialize(xml, slot, ctx),
        XmlExprOp::IsDocument => {
            let value = eval_expr(&xml.args[0], slot, ctx)?;
            if matches!(value, Value::Null) {
                Ok(Value::Null)
            } else {
                let text = match value {
                    Value::Xml(text) => text.to_string(),
                    other => render_scalar_text(other, ctx)?,
                };
                Ok(Value::Bool(
                    validate_xml_input(&text, XmlOptionSetting::Document).is_ok(),
                ))
            }
        }
        XmlExprOp::Concat => {
            let mut pieces = Vec::new();
            for arg in &xml.args {
                match eval_expr(arg, slot, ctx)? {
                    Value::Null => {}
                    Value::Xml(text) => pieces.push(text.to_string()),
                    other => pieces.push(render_xml_content_value(other, ctx)?),
                }
            }
            Ok(Value::Xml(CompactString::from_owned(concat_xml_texts(
                pieces.iter().map(String::as_str),
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::XmlStandalone;

    #[test]
    fn validates_document_vs_content() {
        assert!(validate_xml_input("<a/>", XmlOptionSetting::Document).is_ok());
        assert!(validate_xml_input("<a/><b/>", XmlOptionSetting::Content).is_ok());
        assert!(validate_xml_input("<a/><b/>", XmlOptionSetting::Document).is_err());
        assert!(validate_xml_input("hello", XmlOptionSetting::Content).is_ok());
        assert!(validate_xml_input("hello", XmlOptionSetting::Document).is_err());
    }

    #[test]
    fn allows_document_misc_around_root() {
        assert!(
            validate_xml_input(
                "<?xml version=\"1.0\"?><!--x--><a/><?pi ok?>",
                XmlOptionSetting::Document,
            )
            .is_ok()
        );
    }

    #[test]
    fn maps_sql_identifier_to_xml_name() {
        assert_eq!(map_sql_identifier_to_xml_name("xml", false), "_x0078_ml");
        assert_eq!(map_sql_identifier_to_xml_name("a.b", true), "a_x002E_b");
        assert_eq!(
            map_sql_identifier_to_xml_name(":one:", false),
            "_x003A_one:"
        );
    }

    #[test]
    fn formats_xml_indent_for_element_only_content() {
        let formatted =
            format_xml_indent("<foo><bar></bar><baz/></foo>", XmlOptionSetting::Document).unwrap();
        assert_eq!(formatted, "<foo>\n  <bar/>\n  <baz/>\n</foo>");
    }

    #[test]
    fn keeps_mixed_content_inline_when_indenting() {
        let formatted =
            format_xml_indent("<foo>text<bar/>more</foo>", XmlOptionSetting::Document).unwrap();
        assert_eq!(formatted, "<foo>text<bar/>more</foo>");
    }

    #[test]
    fn formats_top_level_mixed_content_like_postgres_xml_sql() {
        let formatted = format_xml_indent(
            "text node<foo>73</foo>text node<bar><val x=\"y\">42</val></bar>",
            XmlOptionSetting::Content,
        )
        .unwrap();
        assert_eq!(
            formatted,
            "text node\n<foo>73</foo>text node\n<bar>\n  <val x=\"y\">42</val>\n</bar>"
        );
    }

    #[test]
    fn preserves_original_declaration_text_when_indenting_document() {
        let formatted = format_xml_indent(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><foo><bar><val>73</val></bar></foo>",
            XmlOptionSetting::Document,
        )
        .unwrap();
        assert_eq!(
            formatted,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<foo>\n  <bar>\n    <val>73</val>\n  </bar>\n</foo>"
        );
    }

    #[test]
    fn preserves_content_doctype_trailing_newline() {
        let formatted = format_xml_indent("<!DOCTYPE a><a/>", XmlOptionSetting::Content).unwrap();
        assert_eq!(formatted, "<!DOCTYPE a>\n<a/>\n");
    }

    #[test]
    fn content_indent_comments_and_pi_have_separator_newlines_but_no_trailing_newline() {
        assert_eq!(
            format_xml_indent("<!--a--><foo/>", XmlOptionSetting::Content).unwrap(),
            "<!--a-->\n<foo/>"
        );
        assert_eq!(
            format_xml_indent("<?pi x?><foo/>", XmlOptionSetting::Content).unwrap(),
            "<?pi x?>\n<foo/>"
        );
        assert_eq!(
            format_xml_indent("<!--a--><?pi x?><foo/>", XmlOptionSetting::Content).unwrap(),
            "<!--a-->\n<?pi x?>\n<foo/>"
        );
        assert_eq!(
            format_xml_indent("<foo/><!--a-->", XmlOptionSetting::Content).unwrap(),
            "<foo/>\n<!--a-->"
        );
        assert_eq!(
            format_xml_indent("<foo/><?pi x?>", XmlOptionSetting::Content).unwrap(),
            "<foo/>\n<?pi x?>"
        );
    }

    #[test]
    fn content_indent_keeps_comments_and_pi_attached_to_preceding_top_level_text() {
        assert_eq!(
            format_xml_indent("<!--a-->text<foo/>", XmlOptionSetting::Content).unwrap(),
            "<!--a-->text\n<foo/>"
        );
        assert_eq!(
            format_xml_indent("<?pi x?>text<foo/>", XmlOptionSetting::Content).unwrap(),
            "<?pi x?>text\n<foo/>"
        );
    }

    #[test]
    fn content_indent_does_not_add_trailing_newline_for_comment_or_empty_element_only() {
        assert_eq!(
            format_xml_indent("<!--a-->", XmlOptionSetting::Content).unwrap(),
            "<!--a-->"
        );
        assert_eq!(
            format_xml_indent("<?pi x?>", XmlOptionSetting::Content).unwrap(),
            "<?pi x?>"
        );
        assert_eq!(
            format_xml_indent("<a/>", XmlOptionSetting::Content).unwrap(),
            "<a/>"
        );
    }

    #[test]
    fn print_xml_decl_matches_postgres_rules() {
        assert_eq!(print_xml_decl(Some("1.0"), None), None);
        assert_eq!(
            print_xml_decl(None, Some(true)),
            Some("<?xml version=\"1.0\" standalone=\"yes\"?>".into())
        );
        assert_eq!(
            print_xml_decl(Some("1.1"), None),
            Some("<?xml version=\"1.1\"?>".into())
        );
    }

    #[test]
    fn xmlroot_style_decl_rewrite_uses_default_version_for_standalone() {
        let decl = print_xml_decl(None, Some(matches!(XmlStandalone::Yes, XmlStandalone::Yes)));
        assert_eq!(
            decl,
            Some("<?xml version=\"1.0\" standalone=\"yes\"?>".into())
        );
    }

    #[test]
    fn xmlconcat_merges_declarations_like_postgres() {
        assert_eq!(
            concat_xml_texts(["<foo/>", "<?xml version=\"1.1\" standalone=\"no\"?><bar/>"]),
            "<foo/><bar/>"
        );
        assert_eq!(
            concat_xml_texts([
                "<?xml version=\"1.1\"?><foo/>",
                "<?xml version=\"1.1\" standalone=\"no\"?><bar/>"
            ]),
            "<?xml version=\"1.1\"?><foo/><bar/>"
        );
        assert_eq!(
            concat_xml_texts([
                "<?xml version=\"1.1\" standalone=\"yes\"?><foo/>",
                "<?xml version=\"1.0\" standalone=\"yes\"?><bar/>"
            ]),
            "<?xml version=\"1.0\" standalone=\"yes\"?><foo/><bar/>"
        );
    }

    #[test]
    fn xmlcomment_rejects_invalid_sequences() {
        assert_eq!(xml_comment("test").unwrap(), "<!--test-->");
        assert!(xml_comment("test-").is_err());
        assert!(xml_comment("--test").is_err());
    }

    #[test]
    fn xml_is_well_formed_variants_follow_option_rules() {
        assert!(xml_is_well_formed("abc", XmlOptionSetting::Content));
        assert!(!xml_is_well_formed("abc", XmlOptionSetting::Document));
        assert!(xml_is_well_formed(
            "<foo>bar</foo>",
            XmlOptionSetting::Document
        ));
        assert!(!xml_is_well_formed(
            "<foo>bar</foo",
            XmlOptionSetting::Content
        ));
    }
}
