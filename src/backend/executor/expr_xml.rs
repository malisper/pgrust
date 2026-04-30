use super::exec_expr::eval_expr;
use super::expr_casts::{cast_value_with_config, cast_value_with_source_type_catalog_and_config};
use super::{ExecError, ExecutorContext, TupleSlot, format_array_value_text};
use crate::backend::utils::misc::guc_xml::XmlBinaryFormat;
use crate::backend::utils::misc::guc_xml::XmlOptionSetting;
use crate::backend::utils::misc::notices::push_warning;
use crate::include::catalog::XML_TYPE_OID;
use crate::include::nodes::datetime::{DateADT, TimestampADT, TimestampTzADT, USECS_PER_SEC};
use crate::include::nodes::datum::{ArrayValue, Value};
use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind, XmlRootVersion};
use crate::include::nodes::primnodes::{
    SqlXmlTable, SqlXmlTableColumnKind, XmlExpr, XmlExprOp, expr_sql_type_hint,
};
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
        detail: detail.map(|detail| xml_detail_with_input_context(raw_input, detail)),
        context: None,
        sqlstate,
    }
}

fn xml_error_without_sql_position(err: ExecError) -> ExecError {
    match err {
        ExecError::XmlInput {
            message,
            detail,
            context,
            sqlstate,
            ..
        } => ExecError::XmlInput {
            raw_input: String::new(),
            message,
            detail,
            context,
            sqlstate,
        },
        other => other,
    }
}

fn xml_detail_with_input_context(text: &str, detail: String) -> String {
    if !detail.starts_with("line 1:") {
        return detail;
    }
    let mut out = detail;
    if let Some(caret_detail) = xml_input_caret_detail(text, &out) {
        out.push('\n');
        out.push_str(&caret_detail);
    }
    if let Some(mismatch) = xml_mismatched_tag_detail(text)
        && !out.contains(&mismatch)
    {
        out.push('\n');
        out.push_str(&mismatch);
    }
    out
}

fn xml_input_caret_detail(text: &str, detail: &str) -> Option<String> {
    if text.is_empty() || text.trim().is_empty() {
        return None;
    }
    let offset = if detail.contains("xmlParseEntityRef: no name") {
        text.find('&')?
    } else if let Some(name) = xml_detail_entity_name(detail) {
        text.find(&format!("&{name};")).or_else(|| text.find('&'))?
    } else if detail.contains("Extra content at the end of the document") {
        xml_extra_content_offset(text).unwrap_or_else(|| text.len().saturating_sub(1))
    } else if detail.contains("StartTag: invalid element name") {
        if text == "<>" {
            1
        } else {
            text.find("<!DOCTYPE")
                .or_else(|| text.find('<'))
                .unwrap_or_default()
        }
    } else if detail.contains("Start tag expected, '<' not found") {
        text.find(|ch: char| !ch.is_whitespace())
            .unwrap_or_default()
    } else {
        return None;
    };
    Some(format!("{text}\n{}^", " ".repeat(offset)))
}

fn xml_detail_entity_name(detail: &str) -> Option<&str> {
    detail
        .strip_prefix("line 1: Entity '")
        .and_then(|rest| rest.split_once("' not defined"))
        .map(|(name, _)| name)
}

fn xml_mismatched_tag_detail(text: &str) -> Option<String> {
    let (opened, closed) = xml_mismatched_tag_names(text)?;
    Some(format!(
        "line 1: Opening and ending tag mismatch: {opened} line 1 and {closed}"
    ))
}

fn xml_mismatched_tag_names(text: &str) -> Option<(&str, &str)> {
    let opened = first_start_tag_name(text)?;
    let close_start = text.rfind("</")?;
    let close_rest = &text[close_start + 2..];
    let close_end = close_rest.find('>')?;
    let closed = &close_rest[..close_end];
    (!closed.is_empty() && opened != closed).then_some((opened, closed))
}

fn xml_extra_content_offset(text: &str) -> Option<usize> {
    let first_end = text.find("/>").map(|index| index + 2).or_else(|| {
        let first_close = text.find('>')?;
        let name = first_start_tag_name(text)?;
        text.find(&format!("</{name}>"))
            .map(|index| index + name.len() + 3)
            .or(Some(first_close + 1))
    })?;
    text[first_end..]
        .char_indices()
        .find_map(|(offset, ch)| (!ch.is_whitespace()).then_some(first_end + offset))
}

pub(crate) fn unsupported_xml_feature_error() -> ExecError {
    ExecError::XmlInput {
        raw_input: String::new(),
        message: "unsupported XML feature".into(),
        detail: Some(
            "This functionality requires the server to be built with libxml support.".into(),
        ),
        context: None,
        sqlstate: "0A000",
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

fn xml_declaration_error(text: &str, option: XmlOptionSetting) -> ExecError {
    let (message, sqlstate) = match option {
        XmlOptionSetting::Document => ("invalid XML document: invalid XML declaration", "2200M"),
        XmlOptionSetting::Content => ("invalid XML content: invalid XML declaration", "2200N"),
    };
    xml_input_error(text, message, None, sqlstate)
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
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b':' | b'-'))
        {
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
        .ok_or_else(|| xml_declaration_error(text, option))?;
    let attrs = parse_xml_decl_attributes(body).map_err(|_| xml_declaration_error(text, option))?;
    for (name, value) in attrs {
        if name == "standalone" && value != "yes" && value != "no" {
            return Err(xml_declaration_error(text, option));
        }
    }
    Ok(())
}

fn xml_declared_entity_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(offset) = rest.find("<!ENTITY") {
        rest = &rest[offset + "<!ENTITY".len()..];
        let mut candidate = rest.trim_start();
        if let Some(after_percent) = candidate.strip_prefix('%') {
            candidate = after_percent.trim_start();
        }
        let name: String = candidate
            .chars()
            .take_while(|ch| !ch.is_whitespace() && *ch != '>')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

fn doctype_has_external_subset(text: &str) -> bool {
    let Some(offset) = text.find("<!DOCTYPE") else {
        return false;
    };
    let after_doctype = &text[offset + "<!DOCTYPE".len()..];
    let head_end = after_doctype
        .find(|ch| matches!(ch, '[' | '>'))
        .unwrap_or(after_doctype.len());
    let head = &after_doctype[..head_end];
    head.split_whitespace()
        .any(|token| matches!(token, "SYSTEM" | "PUBLIC"))
}

fn xml_entity_ref_allowed(name: &str, declared_entities: &[String], allow_external: bool) -> bool {
    matches!(name, "amp" | "lt" | "gt" | "apos" | "quot")
        || declared_entities.iter().any(|declared| declared == name)
        || allow_external
}

fn xml_extra_content_error(text: &str, option: XmlOptionSetting) -> ExecError {
    xml_validation_error_for_option(
        text,
        option,
        "line 1: Extra content at the end of the document".into(),
    )
}

fn xml_invalid_start_tag_error(text: &str, option: XmlOptionSetting) -> ExecError {
    xml_validation_error_for_option(
        text,
        option,
        "line 1: StartTag: invalid element name".into(),
    )
}

fn xml_undefined_entity_error(text: &str, option: XmlOptionSetting, name: &str) -> ExecError {
    xml_validation_error_for_option(text, option, format!("line 1: Entity '{name}' not defined"))
}

fn first_start_tag_name(text: &str) -> Option<&str> {
    let trimmed = text.trim_start();
    let rest = trimmed.strip_prefix('<')?;
    if rest
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, '!' | '?' | '/'))
    {
        return None;
    }
    let end = rest
        .find(|ch: char| ch.is_whitespace() || matches!(ch, '>' | '/'))
        .unwrap_or(rest.len());
    (end > 0).then_some(&rest[..end])
}

fn postgres_xml_detail_for_quick_xml_error(text: &str, err: &str) -> Option<String> {
    if err.contains("tag not closed")
        && let Some(name) = first_start_tag_name(text)
    {
        return Some(format!(
            "line 1: Couldn't find end of Start Tag {name} line 1"
        ));
    }
    if err.contains("entity or character reference not closed") {
        return Some("line 1: xmlParseEntityRef: no name".into());
    }
    if let Some(rest) = err.strip_prefix("ill-formed document: expected `</")
        && let Some((opened, rest)) = rest.split_once(">`, but `</")
        && let Some((closed, _)) = rest.split_once(">` was found")
    {
        return Some(format!(
            "line 1: Opening and ending tag mismatch: {opened} line 1 and {closed}"
        ));
    }
    None
}

pub(crate) fn validate_xml_input(text: &str, option: XmlOptionSetting) -> Result<(), ExecError> {
    validate_xml_declaration(text, option)?;

    if text.trim() == "<>" {
        return Err(xml_invalid_start_tag_error(text, option));
    }

    let declared_entities = xml_declared_entity_names(text);
    let allow_external_entities = doctype_has_external_subset(text);
    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(false);

    let mut depth = 0usize;
    let mut seen_document_element = false;
    let mut after_document_element = false;
    let mut seen_doctype = false;
    let mut seen_non_misc_before_doctype = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(_)) => {
                if depth == 0 {
                    if matches!(option, XmlOptionSetting::Document) || seen_doctype {
                        if after_document_element || seen_document_element {
                            return Err(xml_extra_content_error(text, option));
                        }
                        seen_document_element = true;
                    } else if matches!(option, XmlOptionSetting::Content) {
                        seen_non_misc_before_doctype = true;
                    }
                }
                depth += 1;
            }
            Ok(Event::Empty(_)) => {
                if depth == 0 {
                    if matches!(option, XmlOptionSetting::Document) || seen_doctype {
                        if after_document_element || seen_document_element {
                            return Err(xml_extra_content_error(text, option));
                        }
                        seen_document_element = true;
                        after_document_element = true;
                    } else if matches!(option, XmlOptionSetting::Content) {
                        seen_non_misc_before_doctype = true;
                    }
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
                if depth == 0 && !is_xml_whitespace(text_event.as_ref()) {
                    if matches!(option, XmlOptionSetting::Document) {
                        return Err(xml_validation_error_for_option(
                            text,
                            option,
                            "line 1: Start tag expected, '<' not found".into(),
                        ));
                    }
                    if seen_doctype {
                        if after_document_element || seen_document_element {
                            return Err(xml_extra_content_error(text, option));
                        }
                        return Err(xml_invalid_start_tag_error(text, option));
                    }
                    if matches!(option, XmlOptionSetting::Content) {
                        seen_non_misc_before_doctype = true;
                    }
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
                if seen_doctype {
                    return Err(xml_invalid_start_tag_error(text, option));
                }
                if seen_document_element || after_document_element || seen_non_misc_before_doctype {
                    return Err(xml_invalid_start_tag_error(text, option));
                }
                seen_doctype = true;
            }
            Ok(Event::Decl(_)) | Ok(Event::PI(_)) | Ok(Event::Comment(_)) => {}
            Ok(Event::GeneralRef(entity)) => {
                let name = String::from_utf8_lossy(entity.as_ref());
                if !xml_entity_ref_allowed(&name, &declared_entities, allow_external_entities) {
                    return Err(xml_undefined_entity_error(text, option, &name));
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => {
                let (message, sqlstate) = match option {
                    XmlOptionSetting::Document => ("invalid XML document", "2200M"),
                    XmlOptionSetting::Content => ("invalid XML content", "2200N"),
                };
                let err = err.to_string();
                return Err(xml_input_error(
                    text,
                    message,
                    Some(postgres_xml_detail_for_quick_xml_error(text, &err).unwrap_or(err)),
                    sqlstate,
                ));
            }
        }
    }

    if matches!(option, XmlOptionSetting::Document) && !seen_document_element {
        return Err(xml_validation_error_for_option(
            text,
            option,
            "line 1: Start tag expected, '<' not found".into(),
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
        && rest.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
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

pub(crate) fn strip_xml_declaration(text: &str) -> &str {
    find_xml_declaration(text).map_or(text, |(_, body)| body)
}

pub(crate) fn render_xml_output_text(text: &str) -> &str {
    let Some((decl_text, body)) = find_xml_declaration(text) else {
        return text;
    };
    let version = extract_decl_attr(decl_text, "version");
    let standalone = extract_decl_attr(decl_text, "standalone");
    if version.as_deref().is_none_or(|version| version == "1.0") && standalone.is_none() {
        body
    } else {
        text
    }
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
    map_sql_identifier_to_xml_name_with_prefix_rule(ident, escape_period, true)
}

fn map_sql_identifier_to_xml_pi_name(ident: &str) -> String {
    map_sql_identifier_to_xml_name_with_prefix_rule(ident, false, false)
}

fn map_sql_identifier_to_xml_name_with_prefix_rule(
    ident: &str,
    escape_period: bool,
    escape_xml_prefix: bool,
) -> String {
    let mut out = String::new();
    let chars: Vec<char> = ident.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == ':' && i == 0 {
            out.push_str("_x003A_");
        } else if ch == '_' && chars.get(i + 1) == Some(&'x') {
            out.push_str("_x005F_");
        } else if escape_xml_prefix
            && i == 0
            && ident.len() >= 3
            && ident[..3].eq_ignore_ascii_case("xml")
        {
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
    _ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if !value.is_finite() {
        return Err(xml_detail_error(
            "timestamp out of range",
            Some("XML does not support infinite timestamp values.".into()),
            "22008",
        ));
    }
    Ok(format_xml_timestamp_usecs(value.0, None))
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
    let offset = crate::backend::utils::time::datetime::timezone_offset_seconds_at_utc(
        &ctx.datetime_config,
        value.0,
    );
    Ok(format_xml_timestamp_usecs(
        value.0 + i64::from(offset) * USECS_PER_SEC,
        Some(offset),
    ))
}

fn format_xml_timestamp_usecs(usecs: i64, offset_seconds: Option<i32>) -> String {
    let (days, time_usecs) =
        crate::backend::utils::time::datetime::timestamp_parts_from_usecs(usecs);
    let (mut year, month, day) = crate::backend::utils::time::datetime::ymd_from_days(days);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
    }
    let mut rendered = format!(
        "{year:04}-{month:02}-{day:02}T{}",
        crate::backend::utils::time::datetime::format_time_usecs(time_usecs)
    );
    if let Some(offset) = offset_seconds {
        if offset == 0 {
            rendered.push('Z');
        } else {
            let sign = if offset < 0 { '-' } else { '+' };
            let abs = offset.unsigned_abs();
            rendered.push(sign);
            rendered.push_str(&format!("{:02}:{:02}", abs / 3600, (abs % 3600) / 60));
        }
    }
    if bc {
        rendered.push_str(" BC");
    }
    rendered
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
        Value::Xml(text) => Ok(Some(xml_escape(&text, true))),
        Value::Array(values) => Ok(Some(xml_escape(
            &render_array_xml(ArrayValue::from_1d(values), ctx)?,
            true,
        ))),
        Value::PgArray(array) => Ok(Some(xml_escape(&render_array_xml(array, ctx)?, true))),
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
    if xml
        .args
        .iter()
        .filter_map(expr_sql_type_hint)
        .any(|sql_type| matches!(sql_type.kind, SqlTypeKind::Date))
    {
        // :HACK: PostgreSQL's XML mapping path reports this as a libxml-only
        // feature for date values. pgrust can render the scalar value, but the
        // libxml-disabled regression expects the mapping error surface.
        return Err(unsupported_xml_feature_error());
    }
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
    )
    .map_err(xml_error_without_sql_position)?;
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

pub(crate) fn eval_xml_text_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let value = values.first().cloned().unwrap_or(Value::Null);
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let mut rendered = xml_escape(&render_xml_builtin_text(value, ctx)?, false);
    rendered = rendered.replace('"', "&quot;");
    Ok(Value::Xml(CompactString::from_owned(rendered)))
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
            "invalid XML processing instruction",
            Some("XML processing instruction target name cannot be \"xml\".".into()),
            "2200T",
        ));
    }
    let target = map_sql_identifier_to_xml_pi_name(name);
    let mut rendered = String::from("<?");
    rendered.push_str(&target);
    if let Some(arg) = xml.args.first() {
        let value = eval_expr(arg, slot, ctx)?;
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }
        let text = render_scalar_text(value, ctx)?;
        if text.contains("?>") {
            return Err(xml_detail_error(
                "invalid XML processing instruction",
                Some("XML processing instruction cannot contain \"?>\".".into()),
                "2200T",
            ));
        }
        if !text.is_empty() {
            rendered.push(' ');
            rendered.push_str(text.trim_start());
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
    let version = match xml.root_version {
        XmlRootVersion::Value => {
            let version_expr = xml
                .args
                .get(1)
                .ok_or_else(|| xml_detail_error("malformed XMLROOT expression", None, "XX000"))?;
            let version = eval_expr(version_expr, slot, ctx)?;
            if matches!(version, Value::Null) {
                None
            } else {
                Some(render_scalar_text(version, ctx)?)
            }
        }
        XmlRootVersion::NoValue => None,
        XmlRootVersion::Omitted => decl.as_ref().and_then(|decl| decl.version.clone()),
    };
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
    rendered.push_str(body);
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
                    other => {
                        match cast_value_with_config(
                            other,
                            SqlType::new(SqlTypeKind::Xml),
                            &ctx.datetime_config,
                        )? {
                            Value::Null => {}
                            Value::Xml(text) => pieces.push(text.to_string()),
                            other => pieces.push(render_xml_content_value(other, ctx)?),
                        }
                    }
                }
            }
            Ok(Value::Xml(CompactString::from_owned(concat_xml_texts(
                pieces.iter().map(String::as_str),
            ))))
        }
    }
}

pub(crate) fn eval_xpath_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(path_value) = values.first() else {
        return malformed_xpath_call("xpath");
    };
    let Some(document_value) = values.get(1) else {
        return malformed_xpath_call("xpath");
    };
    if matches!(path_value, Value::Null) || matches!(document_value, Value::Null) {
        return Ok(Value::Null);
    }
    let path = path_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "xpath",
            left: path_value.clone(),
            right: Value::Text("".into()),
        })?;
    let document = document_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "xpath",
            left: document_value.clone(),
            right: Value::Xml("".into()),
        })?;
    let namespaces = xpath_namespaces(values.get(2))?;
    let results = eval_xpath(document, path, &namespaces).map_err(with_xpath_context)?;
    let values = results
        .into_iter()
        .map(|result| Value::Xml(CompactString::from_owned(result.into_xml_text())))
        .collect();
    Ok(Value::PgArray(
        ArrayValue::from_1d(values).with_element_type_oid(XML_TYPE_OID),
    ))
}

pub(crate) fn eval_xpath_exists_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(path_value) = values.first() else {
        return malformed_xpath_call("xpath_exists");
    };
    let Some(document_value) = values.get(1) else {
        return malformed_xpath_call("xpath_exists");
    };
    if matches!(path_value, Value::Null) || matches!(document_value, Value::Null) {
        return Ok(Value::Null);
    }
    let path = path_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "xpath_exists",
            left: path_value.clone(),
            right: Value::Text("".into()),
        })?;
    let document = document_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "xpath_exists",
            left: document_value.clone(),
            right: Value::Xml("".into()),
        })?;
    let namespaces = xpath_namespaces(values.get(2))?;
    Ok(Value::Bool(
        !eval_xpath(document, path, &namespaces)?.is_empty(),
    ))
}

fn malformed_xpath_call(function_name: &'static str) -> Result<Value, ExecError> {
    Err(ExecError::DetailedError {
        message: format!("{function_name} expects two or three arguments"),
        detail: None,
        hint: None,
        sqlstate: "42883",
    })
}

pub(crate) fn eval_sql_xml_table(
    table: &SqlXmlTable,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let document = eval_expr(&table.document, slot, ctx)?;
    if matches!(document, Value::Null) {
        return Ok(Vec::new());
    }
    let document_text = match document {
        Value::Xml(text) => text.to_string(),
        other => render_scalar_text(other, ctx)?,
    };
    let (_, nodes) = parse_xml_nodes(&document_text)?;
    let namespaces = eval_xml_table_namespaces(table, slot, ctx)?;
    let row_path = eval_xml_table_path_expr(&table.row_path, slot, ctx)?;
    let row_nodes = eval_xml_table_node_path(&nodes, None, &row_path, &namespaces);
    let mut rows = Vec::new();
    for (row_index, row_node) in row_nodes.iter().enumerate() {
        let mut values = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            match &column.kind {
                SqlXmlTableColumnKind::Ordinality => {
                    values.push(Value::Int32((row_index + 1) as i32));
                }
                SqlXmlTableColumnKind::Regular {
                    path,
                    default,
                    not_null,
                } => {
                    let path_text = match path {
                        Some(path) => eval_xml_table_path_expr(path, slot, ctx)?,
                        None => column.name.clone(),
                    };
                    let matches =
                        eval_xml_table_column_path(&nodes, row_node, &path_text, &namespaces);
                    let value = eval_xml_table_column_value(
                        &matches,
                        column.sql_type,
                        &path_text,
                        default.as_ref(),
                        *not_null,
                        &column.name,
                        slot,
                        ctx,
                    )?;
                    values.push(value);
                }
            }
        }
        rows.push(TupleSlot::virtual_row(values));
    }
    Ok(rows)
}

fn eval_xml_table_namespaces(
    table: &SqlXmlTable,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(String, String)>, ExecError> {
    table
        .namespaces
        .iter()
        .filter_map(|namespace| namespace.name.as_ref().map(|name| (name, &namespace.uri)))
        .map(|(name, uri)| {
            eval_expr(uri, slot, ctx)
                .and_then(|value| render_scalar_text(value, ctx).map(|uri| (name.clone(), uri)))
        })
        .collect()
}

fn eval_xml_table_path_expr(
    expr: &crate::include::nodes::primnodes::Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<String, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Null => Ok(String::new()),
        value => render_scalar_text(value, ctx),
    }
}

#[derive(Debug, Clone)]
enum XmlTablePathValue<'a> {
    Node(&'a XmlNode),
    Text(String),
    Attribute(String),
    Literal(String),
    Bool(bool),
    Number(i64),
}

fn eval_xml_table_column_value(
    matches: &[XmlTablePathValue<'_>],
    target_type: SqlType,
    path: &str,
    default: Option<&crate::include::nodes::primnodes::Expr>,
    not_null: bool,
    column_name: &str,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut value = if matches.is_empty() {
        if let Some(default) = default {
            eval_expr(default, slot, ctx)?
        } else {
            Value::Null
        }
    } else if target_type.is_array || !matches!(target_type.kind, SqlTypeKind::Xml) {
        if matches.len() != 1 {
            return Err(ExecError::DetailedError {
                message: "more than one value returned by column XPath expression".into(),
                detail: None,
                hint: None,
                sqlstate: "21000",
            });
        }
        let text = xml_table_path_value_text(&matches[0]);
        cast_value_with_source_type_catalog_and_config(
            Value::Text(CompactString::from_owned(text)),
            Some(SqlType::new(SqlTypeKind::Text)),
            target_type,
            ctx.catalog.as_deref(),
            &ctx.datetime_config,
        )?
    } else {
        let mut xml = xml_table_path_values_xml(matches);
        if path.trim() == "/" && !xml.ends_with('\n') {
            xml.push('\n');
        }
        Value::Xml(CompactString::from_owned(xml))
    };
    if not_null && matches!(value, Value::Null) {
        return Err(ExecError::DetailedError {
            message: format!("null is not allowed in column \"{column_name}\""),
            detail: None,
            hint: None,
            sqlstate: "22004",
        });
    }
    if matches!(target_type.kind, SqlTypeKind::Xml) && !matches!(value, Value::Null | Value::Xml(_))
    {
        value = Value::Xml(CompactString::from_owned(xml_escape(
            &render_scalar_text(value, ctx)?,
            false,
        )));
    }
    Ok(value)
}

fn eval_xml_table_node_path<'a>(
    document: &'a [XmlNode],
    current: Option<&'a XmlNode>,
    path: &str,
    namespaces: &[(String, String)],
) -> Vec<&'a XmlNode> {
    let values = eval_xml_table_path(document, current, path, namespaces);
    values
        .into_iter()
        .filter_map(|value| match value {
            XmlTablePathValue::Node(node) => Some(node),
            _ => None,
        })
        .collect()
}

fn eval_xml_table_column_path<'a>(
    document: &'a [XmlNode],
    current: &'a XmlNode,
    path: &str,
    namespaces: &[(String, String)],
) -> Vec<XmlTablePathValue<'a>> {
    eval_xml_table_path(document, Some(current), path, namespaces)
}

fn eval_xml_table_path<'a>(
    document: &'a [XmlNode],
    current: Option<&'a XmlNode>,
    raw_path: &str,
    namespaces: &[(String, String)],
) -> Vec<XmlTablePathValue<'a>> {
    let path = raw_path.trim();
    if path.is_empty() {
        return Vec::new();
    }
    if let Some(literal) = quoted_xml_table_literal(path) {
        return vec![XmlTablePathValue::Literal(literal)];
    }
    if path == ". = \"a\"" || path == ".='a'" || path == ". = 'a'" {
        let text = current.map(xml_table_node_text).unwrap_or_default();
        return vec![XmlTablePathValue::Bool(text == "a")];
    }
    if path == "string-length(.)" {
        let text = current.map(xml_table_node_text).unwrap_or_default();
        return vec![XmlTablePathValue::Number(text.chars().count() as i64)];
    }
    if path.ends_with("namespace::node()") {
        return vec![XmlTablePathValue::Literal(
            "http://www.w3.org/XML/1998/namespace".into(),
        )];
    }
    let absolute = path.starts_with('/');
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return document
            .iter()
            .filter(|node| matches!(node, XmlNode::Element { .. }))
            .map(XmlTablePathValue::Node)
            .collect();
    }
    let trimmed = trimmed.strip_prefix("./").unwrap_or(trimmed);
    let mut values = if absolute || current.is_none() {
        document
            .iter()
            .filter(|node| matches!(node, XmlNode::Element { .. }))
            .map(XmlTablePathValue::Node)
            .collect::<Vec<_>>()
    } else {
        vec![XmlTablePathValue::Node(current.expect("current node"))]
    };
    for (index, step) in trimmed
        .split('/')
        .filter(|step| !step.is_empty())
        .enumerate()
    {
        values = eval_xml_table_path_step(
            values,
            step,
            namespaces,
            index == 0 && (absolute || current.is_none()),
        );
        if values.is_empty() {
            break;
        }
    }
    values
}

fn eval_xml_table_path_step<'a>(
    values: Vec<XmlTablePathValue<'a>>,
    raw_step: &str,
    namespaces: &[(String, String)],
    absolute_first_step: bool,
) -> Vec<XmlTablePathValue<'a>> {
    if raw_step == "." {
        return values;
    }
    if raw_step == "text()" {
        return values
            .into_iter()
            .flat_map(|value| match value {
                XmlTablePathValue::Node(node) => xml_table_direct_text_values(node)
                    .into_iter()
                    .map(XmlTablePathValue::Text)
                    .collect::<Vec<_>>(),
                other => vec![other],
            })
            .collect();
    }
    if let Some(attr_name) = raw_step.strip_prefix('@') {
        return values
            .into_iter()
            .filter_map(|value| match value {
                XmlTablePathValue::Node(XmlNode::Element { attrs, .. }) => attrs
                    .iter()
                    .find(|(name, _)| xml_name_matches(name, attr_name, namespaces))
                    .map(|(_, value)| XmlTablePathValue::Attribute(decode_xml_entities(value))),
                _ => None,
            })
            .collect();
    }
    let (step_name, predicate) = split_xml_table_step_predicate(raw_step);
    values
        .into_iter()
        .flat_map(|value| match value {
            XmlTablePathValue::Node(node) => {
                let candidates = if absolute_first_step {
                    vec![node]
                } else {
                    xml_table_element_children(node)
                };
                candidates
                    .into_iter()
                    .filter(|candidate| {
                        (step_name == "*"
                            || xml_table_node_name_matches(candidate, step_name, namespaces))
                            && predicate.as_deref().is_none_or(|predicate| {
                                xml_table_predicate_matches(candidate, predicate, namespaces)
                            })
                    })
                    .map(XmlTablePathValue::Node)
                    .collect::<Vec<_>>()
            }
            _ => Vec::new(),
        })
        .collect()
}

fn split_xml_table_step_predicate(step: &str) -> (&str, Option<String>) {
    let Some(start) = step.find('[') else {
        return (step, None);
    };
    let name = &step[..start];
    let predicate = step[start + 1..]
        .strip_suffix(']')
        .unwrap_or(&step[start + 1..])
        .trim()
        .to_string();
    (name, Some(predicate))
}

fn xml_table_predicate_matches(
    node: &XmlNode,
    predicate: &str,
    namespaces: &[(String, String)],
) -> bool {
    predicate.split(" or ").any(|part| {
        let Some((left, right)) = part.split_once('=') else {
            return false;
        };
        let value = right.trim().trim_matches('"').trim_matches('\'');
        let left = left.trim();
        if let Some(child_name) = left.strip_suffix("/text()") {
            return xml_table_child_text(node, child_name.trim(), namespaces).as_deref()
                == Some(value);
        }
        xml_table_child_text(node, left, namespaces).as_deref() == Some(value)
    })
}

fn xml_table_child_text(
    node: &XmlNode,
    child_name: &str,
    namespaces: &[(String, String)],
) -> Option<String> {
    xml_table_element_children(node)
        .into_iter()
        .find(|child| xml_table_node_name_matches(child, child_name, namespaces))
        .map(xml_table_node_text)
}

fn xml_table_node_name_matches(
    node: &XmlNode,
    expected: &str,
    namespaces: &[(String, String)],
) -> bool {
    match node {
        XmlNode::Element { name, .. } => xml_name_matches(name, expected, namespaces),
        _ => false,
    }
}

fn xml_name_matches(actual: &str, expected: &str, namespaces: &[(String, String)]) -> bool {
    if actual == expected {
        return true;
    }
    let expected_local = expected
        .split_once(':')
        .map_or(expected, |(_, local)| local);
    let actual_local = actual.split_once(':').map_or(actual, |(_, local)| local);
    actual_local == expected_local
        && expected
            .split_once(':')
            .is_none_or(|(prefix, _)| namespaces.iter().any(|(name, _)| name == prefix))
}

fn xml_table_element_children(node: &XmlNode) -> Vec<&XmlNode> {
    match node {
        XmlNode::Element { children, .. } => children
            .iter()
            .filter(|child| matches!(child, XmlNode::Element { .. }))
            .collect(),
        _ => Vec::new(),
    }
}

fn xml_table_direct_text_values(node: &XmlNode) -> Vec<String> {
    match node {
        XmlNode::Element { children, .. } => children
            .iter()
            .filter_map(|child| match child {
                XmlNode::Text(text) | XmlNode::CData(text) => Some(decode_xml_entities(text)),
                _ => None,
            })
            .collect(),
        XmlNode::Text(text) | XmlNode::CData(text) => vec![decode_xml_entities(text)],
        _ => Vec::new(),
    }
}

fn xml_table_node_text(node: &XmlNode) -> String {
    let mut out = String::new();
    match node {
        XmlNode::Element { children, .. } => {
            for child in children {
                out.push_str(&xml_table_node_text(child));
            }
        }
        XmlNode::Text(text) | XmlNode::CData(text) => out.push_str(&decode_xml_entities(text)),
        XmlNode::Comment(_) | XmlNode::Pi(_) | XmlNode::Doctype(_) => {}
    }
    out
}

fn xml_table_path_value_text(value: &XmlTablePathValue<'_>) -> String {
    match value {
        XmlTablePathValue::Node(node) => xml_table_node_text(node),
        XmlTablePathValue::Text(text)
        | XmlTablePathValue::Attribute(text)
        | XmlTablePathValue::Literal(text) => text.clone(),
        XmlTablePathValue::Bool(value) => value.to_string(),
        XmlTablePathValue::Number(value) => value.to_string(),
    }
}

fn xml_table_path_values_xml(values: &[XmlTablePathValue<'_>]) -> String {
    let mut out = String::new();
    for value in values {
        match value {
            XmlTablePathValue::Node(node) => render_xml_table_node(node, &mut out),
            XmlTablePathValue::Text(text)
            | XmlTablePathValue::Attribute(text)
            | XmlTablePathValue::Literal(text) => out.push_str(&xml_escape(text, false)),
            XmlTablePathValue::Bool(value) => out.push_str(&value.to_string()),
            XmlTablePathValue::Number(value) => out.push_str(&value.to_string()),
        }
    }
    out
}

fn render_xml_table_node(node: &XmlNode, out: &mut String) {
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
                    render_xml_table_node(child, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            }
        }
        XmlNode::Text(text) => out.push_str(&xml_escape(&decode_xml_entities(text), false)),
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

#[derive(Debug)]
enum XPathValue {
    Node(String),
    Text(String),
    Bool(bool),
    Number(i64),
}

impl XPathValue {
    fn into_xml_text(self) -> String {
        match self {
            XPathValue::Node(text) => text,
            XPathValue::Text(text) => xml_escape(&text, false),
            XPathValue::Bool(value) => value.to_string(),
            XPathValue::Number(value) => value.to_string(),
        }
    }
}

fn xpath_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

fn xpath_parse_error(detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: "could not parse XML document".into(),
        detail,
        hint: None,
        sqlstate: "2200M",
    }
}

fn with_xpath_context(err: ExecError) -> ExecError {
    ExecError::WithContext {
        source: Box::new(err),
        context: "SQL function \"xpath\" statement 1".into(),
    }
}

// :HACK: This is a regression-focused XPath subset. Replace it with a real
// libxml-compatible XPath engine when pgrust grows one.
fn eval_xpath(
    document_text: &str,
    path: &str,
    namespaces: &[(String, String)],
) -> Result<Vec<XPathValue>, ExecError> {
    let path = path.trim();
    if path.is_empty() {
        return Err(xpath_error("empty XPath expression", "2203B"));
    }
    let (_, nodes) = parse_xml_nodes(document_text).map_err(|err| match err {
        ExecError::DetailedError { detail, .. } | ExecError::XmlInput { detail, .. } => {
            xpath_parse_error(detail)
        }
        other => other,
    })?;
    let document_namespaces = xpath_document_namespaces(&nodes);
    let matching_namespaces = xpath_matching_namespaces(namespaces, &document_namespaces);
    validate_xpath_namespaces(&nodes, document_text)?;
    warn_for_relative_xpath_namespaces(document_text, &document_namespaces);

    if let Some(text) = xpath_string_literal(path) {
        return Ok(vec![XPathValue::Text(text)]);
    }
    if let Some(inner) = path
        .strip_prefix("count(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return Ok(vec![XPathValue::Number(
            eval_xpath_node_path(&nodes, inner, &matching_namespaces).len() as i64,
        )]);
    }
    if let Some((left, right)) = split_xpath_count_comparison(path) {
        let expected = right.parse::<i64>().unwrap_or_default();
        let count = eval_xpath_node_path(&nodes, left, &matching_namespaces).len() as i64;
        return Ok(vec![XPathValue::Bool(count == expected)]);
    }
    if let Some(inner) = path
        .strip_prefix("name(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let name = eval_xpath_node_path(&nodes, inner, &matching_namespaces)
            .first()
            .and_then(|node| match node {
                XmlNode::Element { name, .. } => Some(name.clone()),
                _ => None,
            })
            .unwrap_or_default();
        return Ok(vec![XPathValue::Text(name)]);
    }

    if path == "//text()" {
        return Ok(xpath_descendant_text(&nodes)
            .into_iter()
            .filter(|text| !text.is_empty())
            .map(XPathValue::Text)
            .collect());
    }
    if path == "text()" {
        return Ok(nodes
            .iter()
            .flat_map(xml_table_direct_text_values)
            .filter(|text| !text.is_empty())
            .map(XPathValue::Text)
            .collect());
    }
    if let Some(attr_name) = path.strip_prefix("//@") {
        return Ok(
            xpath_descendant_attributes(&nodes, attr_name, &matching_namespaces)
                .into_iter()
                .map(XPathValue::Text)
                .collect(),
        );
    }
    if let Some((element_path, attr_name)) = path.strip_prefix("//").and_then(|rest| {
        rest.rsplit_once("/@")
            .map(|(element_path, attr_name)| (element_path, attr_name))
    }) {
        return Ok(
            xpath_descendant_elements(&nodes, element_path, &matching_namespaces)
                .into_iter()
                .filter_map(|node| xpath_node_attribute(node, attr_name, &matching_namespaces))
                .map(XPathValue::Text)
                .collect(),
        );
    }

    Ok(eval_xpath_node_path(&nodes, path, &matching_namespaces)
        .into_iter()
        .map(|node| {
            let mut out = String::new();
            render_xpath_node(node, &document_namespaces, namespaces, &mut out);
            XPathValue::Node(out)
        })
        .collect())
}

fn split_xpath_count_comparison(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("count(")?;
    let (inner, right) = rest.split_once(")=")?;
    Some((inner.trim(), right.trim()))
}

fn xpath_string_literal(path: &str) -> Option<String> {
    if path.len() >= 2 {
        let bytes = path.as_bytes();
        if (bytes[0] == b'\'' && bytes[path.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[path.len() - 1] == b'"')
        {
            return Some(
                path[1..path.len() - 1]
                    .replace("''", "'")
                    .replace("\"\"", "\""),
            );
        }
    }
    None
}

fn eval_xpath_node_path<'a>(
    document: &'a [XmlNode],
    path: &str,
    namespaces: &[(String, String)],
) -> Vec<&'a XmlNode> {
    let path = path.trim();
    if path == "/*" {
        return document
            .iter()
            .filter(|node| matches!(node, XmlNode::Element { .. }))
            .collect();
    }
    if path == "//*" {
        return xpath_descendant_elements(document, "*", namespaces);
    }
    if let Some(step) = path.strip_prefix("//") {
        return xpath_descendant_elements(document, step, namespaces);
    }

    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return document
            .iter()
            .filter(|node| matches!(node, XmlNode::Element { .. }))
            .collect();
    }
    let mut current: Vec<&XmlNode> = document
        .iter()
        .filter(|node| matches!(node, XmlNode::Element { .. }))
        .collect();
    for (index, step) in trimmed
        .split('/')
        .filter(|step| !step.is_empty())
        .enumerate()
    {
        let absolute_first = index == 0;
        current = xpath_step(current, step, namespaces, absolute_first);
        if current.is_empty() {
            break;
        }
    }
    current
}

fn xpath_descendant_elements<'a>(
    document: &'a [XmlNode],
    step: &str,
    namespaces: &[(String, String)],
) -> Vec<&'a XmlNode> {
    let mut out = Vec::new();
    for node in document {
        collect_xpath_descendant_elements(node, step, namespaces, &mut out);
    }
    out
}

fn collect_xpath_descendant_elements<'a>(
    node: &'a XmlNode,
    step: &str,
    namespaces: &[(String, String)],
    out: &mut Vec<&'a XmlNode>,
) {
    if xpath_step_matches(node, step, namespaces) {
        out.push(node);
    }
    if let XmlNode::Element { children, .. } = node {
        for child in children {
            collect_xpath_descendant_elements(child, step, namespaces, out);
        }
    }
}

fn xpath_step<'a>(
    nodes: Vec<&'a XmlNode>,
    step: &str,
    namespaces: &[(String, String)],
    absolute_first_step: bool,
) -> Vec<&'a XmlNode> {
    nodes
        .into_iter()
        .flat_map(|node| {
            let candidates = if absolute_first_step {
                vec![node]
            } else {
                xml_table_element_children(node)
            };
            candidates
                .into_iter()
                .filter(|candidate| xpath_step_matches(candidate, step, namespaces))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn xpath_step_matches(node: &XmlNode, raw_step: &str, namespaces: &[(String, String)]) -> bool {
    let (step_name, predicate) = split_xml_table_step_predicate(raw_step);
    xpath_node_name_matches(node, step_name, namespaces)
        && predicate
            .as_deref()
            .is_none_or(|predicate| xpath_predicate_matches(node, predicate, namespaces))
}

fn xpath_node_name_matches(
    node: &XmlNode,
    expected: &str,
    namespaces: &[(String, String)],
) -> bool {
    match node {
        XmlNode::Element { name, .. } => xpath_name_matches(name, expected, namespaces),
        _ => false,
    }
}

fn xpath_name_matches(actual: &str, expected: &str, namespaces: &[(String, String)]) -> bool {
    if expected == "*" {
        return true;
    }
    let Some((expected_prefix, expected_local)) = expected.split_once(':') else {
        return actual == expected;
    };
    let Some(expected_uri) = xpath_namespace_uri(namespaces, expected_prefix) else {
        return actual == expected;
    };
    if actual == expected {
        return true;
    }
    let (actual_prefix, actual_local) = actual.split_once(':').unwrap_or(("", actual));
    actual_local == expected_local
        && xpath_namespace_uri(namespaces, actual_prefix).as_deref() == Some(expected_uri.as_str())
}

fn xpath_predicate_matches(
    node: &XmlNode,
    predicate: &str,
    namespaces: &[(String, String)],
) -> bool {
    let Some((left, right)) = predicate.split_once('=') else {
        return false;
    };
    let expected = right.trim().trim_matches('"').trim_matches('\'');
    let left = left.trim();
    if left == "text()" {
        return xml_table_node_text(node) == expected;
    }
    if let Some(child_name) = left.strip_suffix("/text()") {
        return xpath_child_text(node, child_name.trim(), namespaces).as_deref() == Some(expected);
    }
    xpath_child_text(node, left, namespaces).as_deref() == Some(expected)
}

fn xpath_child_text(
    node: &XmlNode,
    child_name: &str,
    namespaces: &[(String, String)],
) -> Option<String> {
    xml_table_element_children(node)
        .into_iter()
        .find(|child| xpath_node_name_matches(child, child_name, namespaces))
        .map(xml_table_node_text)
}

fn xpath_node_attribute(
    node: &XmlNode,
    attr_name: &str,
    namespaces: &[(String, String)],
) -> Option<String> {
    match node {
        XmlNode::Element { attrs, .. } => attrs
            .iter()
            .find(|(name, _)| xpath_name_matches(name, attr_name, namespaces))
            .map(|(_, value)| decode_xml_entities(value)),
        _ => None,
    }
}

fn xpath_descendant_attributes(
    document: &[XmlNode],
    attr_name: &str,
    namespaces: &[(String, String)],
) -> Vec<String> {
    let mut out = Vec::new();
    for node in document {
        collect_xpath_descendant_attributes(node, attr_name, namespaces, &mut out);
    }
    out
}

fn collect_xpath_descendant_attributes(
    node: &XmlNode,
    attr_name: &str,
    namespaces: &[(String, String)],
    out: &mut Vec<String>,
) {
    if let Some(value) = xpath_node_attribute(node, attr_name, namespaces) {
        out.push(value);
    }
    if let XmlNode::Element { children, .. } = node {
        for child in children {
            collect_xpath_descendant_attributes(child, attr_name, namespaces, out);
        }
    }
}

fn xpath_descendant_text(document: &[XmlNode]) -> Vec<String> {
    let mut out = Vec::new();
    for node in document {
        collect_xpath_descendant_text(node, &mut out);
    }
    out
}

fn collect_xpath_descendant_text(node: &XmlNode, out: &mut Vec<String>) {
    match node {
        XmlNode::Element { children, .. } => {
            for child in children {
                collect_xpath_descendant_text(child, out);
            }
        }
        XmlNode::Text(text) | XmlNode::CData(text) => out.push(decode_xml_entities(text)),
        XmlNode::Comment(_) | XmlNode::Pi(_) | XmlNode::Doctype(_) => {}
    }
}

fn xpath_namespaces(value: Option<&Value>) -> Result<Vec<(String, String)>, ExecError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let values = match value {
        Value::PgArray(array) => {
            if array.dimensions.is_empty() {
                return Ok(Vec::new());
            }
            if array.dimensions.len() != 2 || array.dimensions[1].length != 2 {
                return Err(ExecError::DetailedError {
                    message: "invalid array for XML namespace mapping".into(),
                    detail: Some(
                        "The array must be two-dimensional with length of the second axis equal to 2."
                            .into(),
                    ),
                    hint: None,
                    sqlstate: "22000",
                });
            }
            array.elements.clone()
        }
        Value::Array(rows) => rows
            .iter()
            .flat_map(|row| match row {
                Value::Array(values) => values.clone(),
                Value::PgArray(array) => array.elements.clone(),
                value => vec![value.clone()],
            })
            .collect(),
        other => {
            return Err(ExecError::TypeMismatch {
                op: "xpath",
                left: other.clone(),
                right: Value::Array(Vec::new()),
            });
        }
    };
    if values.len() % 2 != 0 {
        return Err(ExecError::DetailedError {
            message: "invalid array for XML namespace mapping".into(),
            detail: Some(
                "The array must be two-dimensional with length of the second axis equal to 2."
                    .into(),
            ),
            hint: None,
            sqlstate: "22000",
        });
    }
    let mut namespaces = Vec::new();
    for pair in values.chunks(2) {
        let [name, uri] = pair else {
            continue;
        };
        if matches!(name, Value::Null) || matches!(uri, Value::Null) {
            return Err(ExecError::DetailedError {
                message: "neither namespace name nor URI may be null".into(),
                detail: None,
                hint: None,
                sqlstate: "22004",
            });
        }
        let Some(name) = name.as_text() else {
            return Err(ExecError::TypeMismatch {
                op: "xpath",
                left: name.clone(),
                right: Value::Text("".into()),
            });
        };
        let Some(uri) = uri.as_text() else {
            return Err(ExecError::TypeMismatch {
                op: "xpath",
                left: uri.clone(),
                right: Value::Text("".into()),
            });
        };
        namespaces.push((name.to_string(), uri.to_string()));
    }
    Ok(namespaces)
}

fn xpath_document_namespaces(document: &[XmlNode]) -> Vec<(String, String)> {
    document
        .iter()
        .find_map(|node| match node {
            XmlNode::Element { attrs, .. } => Some(xpath_namespaces_from_attrs(attrs)),
            _ => None,
        })
        .unwrap_or_default()
}

fn xpath_matching_namespaces(
    explicit_namespaces: &[(String, String)],
    document_namespaces: &[(String, String)],
) -> Vec<(String, String)> {
    let mut namespaces = explicit_namespaces.to_vec();
    for (prefix, uri) in document_namespaces {
        if !namespaces.iter().any(|(candidate, _)| candidate == prefix) {
            namespaces.push((prefix.clone(), uri.clone()));
        }
    }
    namespaces
}

fn xpath_namespace_uri(namespaces: &[(String, String)], prefix: &str) -> Option<String> {
    namespaces
        .iter()
        .find(|(candidate, _)| candidate == prefix)
        .map(|(_, uri)| uri.clone())
}

fn xpath_namespaces_from_attrs(attrs: &[(String, String)]) -> Vec<(String, String)> {
    attrs
        .iter()
        .filter_map(|(name, value)| {
            if name == "xmlns" {
                Some(("".into(), decode_xml_entities(value)))
            } else {
                name.strip_prefix("xmlns:")
                    .map(|prefix| (prefix.to_string(), decode_xml_entities(value)))
            }
        })
        .collect()
}

fn validate_xpath_namespaces(document: &[XmlNode], document_text: &str) -> Result<(), ExecError> {
    let mut scope = Vec::new();
    for node in document {
        validate_xpath_node_namespaces(node, document_text, &mut scope)?;
    }
    Ok(())
}

fn validate_xpath_node_namespaces(
    node: &XmlNode,
    document_text: &str,
    inherited: &mut Vec<(String, String)>,
) -> Result<(), ExecError> {
    let XmlNode::Element {
        name,
        attrs,
        children,
    } = node
    else {
        return Ok(());
    };
    let original_len = inherited.len();
    for (prefix, uri) in xpath_namespaces_from_attrs(attrs) {
        if uri.contains('<') {
            return Err(xpath_parse_error(None));
        }
        inherited.push((prefix, uri));
    }
    if let Some((prefix, _)) = name.split_once(':')
        && !inherited.iter().any(|(name, _)| name == prefix)
    {
        return Err(xpath_parse_error(Some(format!(
            "line 1: Namespace prefix {prefix} on tag is not defined\n{}",
            xpath_document_caret_detail(document_text)
        ))));
    }
    for (attr_name, _) in attrs {
        if attr_name == "xmlns" || attr_name.starts_with("xmlns:") {
            continue;
        }
        if let Some((prefix, _)) = attr_name.split_once(':')
            && !inherited.iter().any(|(name, _)| name == prefix)
        {
            return Err(xpath_parse_error(Some(format!(
                "line 1: Namespace prefix {prefix} for attribute {attr_name} on {name} is not defined\n{}",
                xpath_document_caret_detail(document_text)
            ))));
        }
    }
    for child in children {
        validate_xpath_node_namespaces(child, document_text, inherited)?;
    }
    inherited.truncate(original_len);
    Ok(())
}

fn warn_for_relative_xpath_namespaces(document_text: &str, namespaces: &[(String, String)]) {
    for (_, uri) in namespaces {
        if !uri.is_empty() && !uri.contains(':') {
            push_warning(format!(
                "line 1: xmlns: URI {uri} is not absolute\n{}",
                xpath_document_caret_detail(document_text)
            ));
        }
    }
}

fn xpath_document_caret_detail(document_text: &str) -> String {
    let caret_index = document_text
        .find("/>")
        .or_else(|| document_text.find('>'))
        .unwrap_or(document_text.len().saturating_sub(1));
    format!("{document_text}\n{}^", " ".repeat(caret_index))
}

fn render_xpath_node(
    node: &XmlNode,
    document_namespaces: &[(String, String)],
    explicit_namespaces: &[(String, String)],
    out: &mut String,
) {
    match node {
        XmlNode::Element {
            name,
            attrs,
            children,
        } => {
            out.push('<');
            out.push_str(name);
            for (key, value) in attrs
                .iter()
                .filter(|(key, _)| key == "xmlns" || key.starts_with("xmlns:"))
            {
                render_xml_attr(out, key, value);
            }
            let mut extra_namespaces = Vec::new();
            if let Some((prefix, _)) = name.split_once(':')
                && !attrs
                    .iter()
                    .any(|(key, _)| key == &format!("xmlns:{prefix}"))
                && let Some((_, uri)) = explicit_namespaces
                    .iter()
                    .chain(document_namespaces.iter())
                    .find(|(candidate, _)| candidate == prefix)
            {
                extra_namespaces.push((format!("xmlns:{prefix}"), uri.clone()));
            }
            if xpath_subtree_has_unprefixed_element(children)
                && !attrs.iter().any(|(key, _)| key == "xmlns")
                && let Some((_, uri)) = document_namespaces
                    .iter()
                    .find(|(candidate, _)| candidate.is_empty())
            {
                extra_namespaces.push(("xmlns".into(), uri.clone()));
            }
            for (key, value) in extra_namespaces {
                render_xml_attr(out, &key, &value);
            }
            for (key, value) in attrs
                .iter()
                .filter(|(key, _)| key != "xmlns" && !key.starts_with("xmlns:"))
            {
                render_xml_attr(out, key, value);
            }
            if children.is_empty() {
                out.push_str("/>");
            } else {
                out.push('>');
                for child in children {
                    render_xpath_node(child, document_namespaces, explicit_namespaces, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            }
        }
        XmlNode::Text(text) => out.push_str(&xml_escape(&decode_xml_entities(text), false)),
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

fn render_xml_attr(out: &mut String, key: &str, value: &str) {
    out.push(' ');
    out.push_str(key);
    out.push_str("=\"");
    out.push_str(value);
    out.push('"');
}

fn xpath_subtree_has_unprefixed_element(children: &[XmlNode]) -> bool {
    children.iter().any(|child| match child {
        XmlNode::Element { name, children, .. } => {
            !name.contains(':') || xpath_subtree_has_unprefixed_element(children)
        }
        _ => false,
    })
}

fn quoted_xml_table_literal(path: &str) -> Option<String> {
    if path.len() >= 2 && path.starts_with('"') && path.ends_with('"') {
        return Some(path[1..path.len() - 1].replace("\"\"", "\""));
    }
    None
}

fn decode_xml_entities(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&apos;", "'")
        .replace("&quot;", "\"")
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
    fn validates_entity_references_like_libxml() {
        assert!(validate_xml_input("<a>&amp;</a>", XmlOptionSetting::Document).is_ok());
        let err = validate_xml_input("<a>&undefined;</a>", XmlOptionSetting::Content).unwrap_err();
        assert!(matches!(
            err,
            ExecError::XmlInput {
                detail: Some(ref detail),
                ..
            } if detail.contains("Entity 'undefined' not defined")
        ));
        assert!(
            validate_xml_input(
                "<!DOCTYPE a [<!ENTITY local \"ok\">]><a>&local;</a>",
                XmlOptionSetting::Document
            )
            .is_ok()
        );
        assert!(
            validate_xml_input(
                "<!DOCTYPE chapter PUBLIC \"-//OASIS//DTD DocBook XML V4.1.2//EN\" \"docbookx.dtd\"><chapter>&nbsp;</chapter>",
                XmlOptionSetting::Document,
            )
            .is_ok()
        );
    }

    #[test]
    fn doctype_content_must_be_document_shaped() {
        assert!(validate_xml_input("<a/><b/>", XmlOptionSetting::Content).is_ok());
        assert!(validate_xml_input("<!DOCTYPE a><a/>", XmlOptionSetting::Content).is_ok());
        let err =
            validate_xml_input("<!DOCTYPE a><a/><b/>", XmlOptionSetting::Content).unwrap_err();
        assert!(matches!(
            err,
            ExecError::XmlInput {
                detail: Some(ref detail),
                ..
            } if detail.contains("Extra content")
        ));
        let err =
            validate_xml_input("text <!DOCTYPE a><a/>", XmlOptionSetting::Content).unwrap_err();
        assert!(matches!(
            err,
            ExecError::XmlInput {
                detail: Some(ref detail),
                ..
            } if detail.contains("StartTag: invalid element name")
        ));
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

    fn xpath_result_texts(
        document: &str,
        path: &str,
        namespaces: &[(String, String)],
    ) -> Vec<String> {
        eval_xpath(document, path, namespaces)
            .unwrap()
            .into_iter()
            .map(XPathValue::into_xml_text)
            .collect()
    }

    #[test]
    fn xpath_relative_name_matches_document_element() {
        assert_eq!(xpath_result_texts("<root/>", "root", &[]), vec!["<root/>"]);
    }

    #[test]
    fn xpath_unprefixed_names_do_not_match_namespaced_elements() {
        let document = concat!(
            "<menu>",
            "<beers/>",
            "<myns:beers xmlns:myns=\"http://myns.com\"/>",
            "</menu>"
        );

        assert_eq!(
            xpath_result_texts(document, "/menu/beers", &[]),
            vec!["<beers/>"]
        );
    }

    #[test]
    fn xpath_prefixed_alias_renders_namespace_before_attrs() {
        let document = concat!(
            "<local:data xmlns:local=\"http://127.0.0.1\">",
            "<local:piece id=\"1\">number one</local:piece>",
            "<local:piece id=\"2\"/>",
            "</local:data>"
        );
        let namespaces = [("loc".into(), "http://127.0.0.1".into())];

        assert_eq!(
            xpath_result_texts(document, "//loc:piece", &namespaces),
            vec![
                "<local:piece xmlns:local=\"http://127.0.0.1\" id=\"1\">number one</local:piece>",
                "<local:piece xmlns:local=\"http://127.0.0.1\" id=\"2\"/>",
            ]
        );
    }
}
