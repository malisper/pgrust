//! xml.c port over the dlopen libxml2 table in [`libxml`] (never a stub:
//! parity is against a with-libxml C oracle). Internal scratch is std
//! Vec/String: cold parse paths, results copied into the result mcx at the
//! wrapper. Not here (loud via fmgr not-ported): table_to_xml family,
//! cursor/database_to_xml, xmlagg.

#![allow(non_snake_case)]

pub mod builtins;
pub mod chvalid;
pub mod errhandler;
pub mod libxml;
pub mod xmltable;
pub mod xpath;
#[cfg(test)]
mod tests;

use core::ffi::{c_char, c_int, c_void};

use ::mbutils::GetDatabaseEncoding;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_XML_COMMENT,
    ERRCODE_INVALID_XML_CONTENT, ERRCODE_INVALID_XML_DOCUMENT,
    ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION, ERRCODE_NOT_AN_XML_DOCUMENT,
    ERRCODE_OUT_OF_MEMORY, WARNING,
};

use errhandler::{
    pg_xml_init, xml_ereport, xml_err_occurred, PG_XML_STRICTNESS_ALL,
    PG_XML_STRICTNESS_WELLFORMED,
};
use libxml::{cstr, xml2, xmlDoc, xmlDocHdr, xmlNode, XML_PARSE_DTDATTR, XML_PARSE_NOBLANKS,
    XML_PARSE_NOENT};

pub const PG_UTF8: i32 = 6;
pub const PG_XML_DEFAULT_VERSION: &[u8] = b"1.0";

const XML_ERR_OK: i32 = 0;
const XML_ERR_INVALID_CHAR: i32 = 9;
const XML_ERR_XMLDECL_NOT_FINISHED: i32 = 57;
const XML_ERR_SPACE_REQUIRED: i32 = 65;
const XML_ERR_STANDALONE_VALUE: i32 = 78;
const XML_ERR_VERSION_MISSING: i32 = 96;
const XML_ERR_MISSING_ENCODING: i32 = 101;

const MAX_MULTIBYTE_CHAR_LEN: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmlOptionType {
    XMLOPTION_DOCUMENT = 0,
    XMLOPTION_CONTENT = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmlBinaryType {
    XMLBINARY_BASE64 = 0,
    XMLBINARY_HEX = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmlStandaloneType {
    XML_STANDALONE_YES = 0,
    XML_STANDALONE_NO = 1,
    XML_STANDALONE_NO_VALUE = 2,
    XML_STANDALONE_OMITTED = 3,
}

use core::cell::Cell;
thread_local! {
    static XMLBINARY: Cell<i32> = const { Cell::new(guc_tables::consts::XMLBINARY_BASE64) };
    static XMLOPTION: Cell<i32> = const { Cell::new(guc_tables::consts::XMLOPTION_CONTENT) };
}

fn get_xmlbinary_guc() -> i32 {
    XMLBINARY.with(|v| v.get())
}
fn set_xmlbinary_guc(v: i32) {
    XMLBINARY.with(|c| c.set(v));
}
fn get_xmloption_guc() -> i32 {
    XMLOPTION.with(|v| v.get())
}
fn set_xmloption_guc(v: i32) {
    XMLOPTION.with(|c| c.set(v));
}

pub fn xmlbinary() -> XmlBinaryType {
    if get_xmlbinary_guc() == guc_tables::consts::XMLBINARY_HEX {
        XmlBinaryType::XMLBINARY_HEX
    } else {
        XmlBinaryType::XMLBINARY_BASE64
    }
}

pub fn xmloption() -> XmlOptionType {
    if get_xmloption_guc() == guc_tables::consts::XMLOPTION_DOCUMENT {
        XmlOptionType::XMLOPTION_DOCUMENT
    } else {
        XmlOptionType::XMLOPTION_CONTENT
    }
}

pub fn init_seams() {
    guc_tables::vars::xmlbinary.install(guc_tables::GucVarAccessors {
        get: get_xmlbinary_guc,
        set: set_xmlbinary_guc,
    });
    guc_tables::vars::xmloption.install(guc_tables::GucVarAccessors {
        get: get_xmloption_guc,
        set: set_xmloption_guc,
    });
}

pub fn no_xml_support() -> PgError {
    PgError::error("unsupported XML feature")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail("This functionality requires the server to be built with libxml support.")
}

fn oom(msg: &str) -> PgError {
    xml_ereport(msg, ERRCODE_OUT_OF_MEMORY)
}

// ===========================================================================
// I/O
// ===========================================================================

/// C `xmlChar_to_encoding` (xml.c:250).
pub fn xmlChar_to_encoding(encoding_name: &str) -> PgResult<i32> {
    let encoding = ::mbutils::pg_char_to_encoding(encoding_name);
    if encoding < 0 {
        return Err(PgError::error(format!(
            "invalid encoding name \"{encoding_name}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    Ok(encoding)
}

/// C `xml_in`. `Ok(None)` = soft parse failure (SQL NULL).
pub fn xml_in(s: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<Option<Vec<u8>>> {
    if !xml_parse_ok(s, xmloption(), true, GetDatabaseEncoding(), escontext)? {
        return Ok(None);
    }
    Ok(Some(s.to_vec()))
}

/// C `xml_out_internal` (xml.c:311).
pub fn xml_out_internal(x: &[u8], target_encoding: i32) -> PgResult<Vec<u8>> {
    let mut work = x.to_vec();
    work.push(0);

    let mut len = x.len();
    let mut version: Option<Vec<u8>> = None;
    let mut standalone: i32 = 0;
    let res_code = parse_xml_decl(
        &work,
        Some(&mut len),
        Some(&mut version),
        None,
        Some(&mut standalone),
    )?;

    if res_code == XML_ERR_OK {
        let mut buf: Vec<u8> = Vec::new();
        if !print_xml_decl(&mut buf, version.as_deref(), target_encoding, standalone) {
            if x.get(len) == Some(&b'\n') {
                len += 1;
            }
        }
        buf.extend_from_slice(&x[len.min(x.len())..]);
        return Ok(buf);
    }

    let _ = elog::ereport(WARNING)
        .errmsg_internal("could not parse XML declaration in stored value")
        .errdetail(errdetail_for_xml_code(res_code))
        .finish(::types_error::ErrorLocation::new(
            "src/backend/utils/adt/xml.c",
            348,
            "xml_out_internal",
        ));
    Ok(x.to_vec())
}

/// C `xml_out` (xml.c:355).
pub fn xml_out(x: &[u8]) -> PgResult<Vec<u8>> {
    xml_out_internal(x, 0)
}

/// C `xml_recv` (xml.c:370). `raw` is the unread remainder of the message.
pub fn xml_recv<'mcx>(mcx: ::mcx::Mcx<'mcx>, raw: &[u8]) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let mut work = raw.to_vec();
    work.push(0);

    let mut encoding_str: Option<Vec<u8>> = None;
    parse_xml_decl(&work, None, None, Some(&mut encoding_str), None)?;

    let encoding = match encoding_str {
        Some(ref e) => xmlChar_to_encoding(&String::from_utf8_lossy(e))?,
        None => PG_UTF8,
    };

    xml_parse_ok(raw, xmloption(), true, encoding, None)?;

    match ::mbutils::pg_any_to_server(mcx, raw, encoding)? {
        Some(converted) => Ok(converted),
        None => ::mcx::slice_in(mcx, raw),
    }
}

/// C `xml_send` (xml.c:438).
pub fn xml_send<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    x: &[u8],
) -> PgResult<::datum::Varlena<'mcx>> {
    let outval = xml_out_internal(x, ::mbutils::pg_get_client_encoding())?;
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, &outval)?;
    Ok(pqformat::pq_endtypsend(buf))
}

// ===========================================================================
// SQL/XML publishing functions
// ===========================================================================

/// C `xmlcomment` (xml.c:490).
pub fn xmlcomment(arg: &[u8]) -> PgResult<Vec<u8>> {
    let len = arg.len();
    for i in 1..len {
        if arg[i] == b'-' && arg[i - 1] == b'-' {
            return Err(PgError::error("invalid XML comment")
                .with_sqlstate(ERRCODE_INVALID_XML_COMMENT)
                .into());
        }
    }
    if len > 0 && arg[len - 1] == b'-' {
        return Err(PgError::error("invalid XML comment")
            .with_sqlstate(ERRCODE_INVALID_XML_COMMENT)
            .into());
    }

    let mut buf: Vec<u8> = Vec::with_capacity(len + 7);
    buf.extend_from_slice(b"<!--");
    buf.extend_from_slice(arg);
    buf.extend_from_slice(b"-->");
    Ok(buf)
}

/// C `xmltext` (xml.c:526) — `xmlEncodeSpecialChars(NULL, arg)`.
pub fn xmltext(arg: &[u8]) -> PgResult<Vec<u8>> {
    let x = xml2();
    let input = cstr(arg);
    // SAFETY: input is NUL-terminated; the returned xmlChar* is ours to free.
    unsafe {
        let out = (x.xmlEncodeSpecialChars)(core::ptr::null(), input.as_ptr());
        if out.is_null() {
            return Err(oom("could not allocate xmlBuffer").into());
        }
        let v = libxml::xmlchar_to_vec(out);
        x.xmlFree(out as *mut c_void);
        Ok(v)
    }
}

/// C `xmlconcat` (xml.c:553).
pub fn xmlconcat(args: &[&[u8]]) -> PgResult<Vec<u8>> {
    let mut global_standalone: i32 = 1;
    let mut global_version: Option<Vec<u8>> = None;
    let mut global_version_no_value = false;
    let mut buf: Vec<u8> = Vec::new();

    for &x in args {
        let mut len = x.len();
        let mut str = x.to_vec();
        str.push(0);

        let mut version: Option<Vec<u8>> = None;
        let mut standalone: i32 = 0;
        parse_xml_decl(
            &str,
            Some(&mut len),
            Some(&mut version),
            None,
            Some(&mut standalone),
        )?;

        if standalone == 0 && global_standalone == 1 {
            global_standalone = 0;
        }
        if standalone < 0 {
            global_standalone = -1;
        }

        match &version {
            None => global_version_no_value = true,
            Some(v) => {
                if global_version.is_none() {
                    global_version = Some(v.clone());
                } else if global_version.as_deref() != Some(v.as_slice()) {
                    global_version_no_value = true;
                }
            }
        }

        buf.extend_from_slice(&x[len.min(x.len())..]);
    }

    if !global_version_no_value || global_standalone >= 0 {
        let mut buf2: Vec<u8> = Vec::new();
        let v = if !global_version_no_value {
            global_version.as_deref()
        } else {
            None
        };
        print_xml_decl(&mut buf2, v, 0, global_standalone);
        buf2.extend_from_slice(&buf);
        buf = buf2;
    }
    Ok(buf)
}

/// C `xmlconcat2` (xml.c:619).
pub fn xmlconcat2(arg1: Option<&[u8]>, arg2: Option<&[u8]>) -> PgResult<Option<Vec<u8>>> {
    match (arg1, arg2) {
        (None, None) => Ok(None),
        (None, Some(a2)) => Ok(Some(a2.to_vec())),
        (Some(a1), None) => Ok(Some(a1.to_vec())),
        (Some(a1), Some(a2)) => Ok(Some(xmlconcat(&[a1, a2])?)),
    }
}

/// C `texttoxml` (xml.c:636).
pub fn texttoxml(data: &[u8]) -> PgResult<Vec<u8>> {
    xmlparse(data, xmloption(), true)
}

/// C `xmltotext` (xml.c:645) — binary compatible.
pub fn xmltotext(data: &[u8]) -> PgResult<Vec<u8>> {
    Ok(data.to_vec())
}

/// C `xmlparse` (xml.c:993).
pub fn xmlparse(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
) -> PgResult<Vec<u8>> {
    xml_parse_ok(
        data,
        xmloption_arg,
        preserve_whitespace,
        GetDatabaseEncoding(),
        None,
    )?;
    Ok(data.to_vec())
}

/// C `xmlpi` (xml.c:1011). Returns `None` for SQL NULL.
pub fn xmlpi(target: &str, arg: Option<&[u8]>) -> PgResult<Option<Vec<u8>>> {
    if target.eq_ignore_ascii_case("xml") {
        return Err(PgError::error("invalid XML processing instruction")
            .with_sqlstate(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION)
            .with_detail(format!(
                "XML processing instruction target name cannot be \"{target}\"."
            ))
            .into());
    }

    // Null check comes after the syntax check (SQL standard).
    let Some(arg_present) = arg else {
        return Ok(None);
    };

    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"<?");
    buf.extend_from_slice(target.as_bytes());

    if find_subslice(arg_present, b"?>").is_some() {
        return Err(PgError::error("invalid XML processing instruction")
            .with_sqlstate(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION)
            .with_detail("XML processing instruction cannot contain \"?>\".")
            .into());
    }
    buf.push(b' ');
    let skip = arg_present.iter().take_while(|&&c| c == b' ').count();
    buf.extend_from_slice(&arg_present[skip..]);
    buf.extend_from_slice(b"?>");
    Ok(Some(buf))
}

/// C `xmlroot` (xml.c:1063).
pub fn xmlroot(
    data: &[u8],
    version: Option<&[u8]>,
    standalone: XmlStandaloneType,
) -> PgResult<Vec<u8>> {
    let mut len = data.len();
    let mut str = data.to_vec();
    str.push(0);

    let mut orig_version: Option<Vec<u8>> = None;
    let mut orig_standalone: i32 = 0;
    parse_xml_decl(
        &str,
        Some(&mut len),
        Some(&mut orig_version),
        None,
        Some(&mut orig_standalone),
    )?;

    let final_version: Option<Vec<u8>> = version.map(|v| v.to_vec());

    match standalone {
        XmlStandaloneType::XML_STANDALONE_YES => orig_standalone = 1,
        XmlStandaloneType::XML_STANDALONE_NO => orig_standalone = 0,
        XmlStandaloneType::XML_STANDALONE_NO_VALUE => orig_standalone = -1,
        XmlStandaloneType::XML_STANDALONE_OMITTED => {}
    }

    let mut buf: Vec<u8> = Vec::new();
    print_xml_decl(&mut buf, final_version.as_deref(), 0, orig_standalone);
    buf.extend_from_slice(&data[len.min(data.len())..]);
    Ok(buf)
}

/// C `xmlvalidate` (xml.c:1118) — removed feature, always errors.
pub fn xmlvalidate() -> PgError {
    PgError::error("xmlvalidate is not implemented").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// C `xml_is_document` (xml.c:1129).
pub fn xml_is_document(arg: &[u8]) -> PgResult<bool> {
    let mut esc = SoftErrorContext::new(false);
    xml_parse_ok(
        arg,
        XmlOptionType::XMLOPTION_DOCUMENT,
        true,
        GetDatabaseEncoding(),
        Some(&mut esc),
    )
}

/// C `xml_is_well_formed` (xml.c:5108) family.
pub fn xml_is_well_formed(data: &[u8]) -> PgResult<bool> {
    wellformed_probe(data, xmloption())
}

pub fn xml_is_well_formed_document(data: &[u8]) -> PgResult<bool> {
    wellformed_probe(data, XmlOptionType::XMLOPTION_DOCUMENT)
}

pub fn xml_is_well_formed_content(data: &[u8]) -> PgResult<bool> {
    wellformed_probe(data, XmlOptionType::XMLOPTION_CONTENT)
}

fn wellformed_probe(data: &[u8], opt: XmlOptionType) -> PgResult<bool> {
    let mut esc = SoftErrorContext::new(false);
    xml_parse_ok(data, opt, true, GetDatabaseEncoding(), Some(&mut esc))
}

/// C `xmltotext_with_options` (xml.c:656) — XMLSERIALIZE's core.
pub fn xmltotext_with_options(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    indent: bool,
) -> PgResult<Vec<u8>> {
    if xmloption_arg != XmlOptionType::XMLOPTION_DOCUMENT && !indent {
        return Ok(data.to_vec());
    }

    let mut esc = SoftErrorContext::new(false);
    let parsed = xml_parse_doc(
        data,
        xmloption_arg,
        !indent,
        GetDatabaseEncoding(),
        Some(&mut esc),
    )?;
    let Some(parsed) = parsed else {
        // A soft error must be failure to conform to XMLOPTION_DOCUMENT.
        return Err(PgError::error("not an XML document")
            .with_sqlstate(ERRCODE_NOT_AN_XML_DOCUMENT)
            .into());
    };

    if !indent {
        parsed.free();
        return Ok(data.to_vec());
    }

    let result = serialize_indented(&parsed, data, xmloption_arg);
    parsed.free();
    result
}

fn serialize_indented(
    parsed: &ParsedXml,
    data: &[u8],
    xmloption_arg: XmlOptionType,
) -> PgResult<Vec<u8>> {
    let x = xml2();
    pg_xml_init(PG_XML_STRICTNESS_ALL);
    // SAFETY: parsed.doc is the live doc from xml_parse_doc; libxml objects
    // created here are freed on every exit path below.
    unsafe {
        let buf = (x.xmlBufferCreate)();
        if buf.is_null() || xml_err_occurred() {
            return Err(oom("could not allocate xmlBuffer").into());
        }

        let mut work = data.to_vec();
        work.push(0);
        let mut decl_len: usize = 0;
        parse_xml_decl(&work, Some(&mut decl_len), None, None, None)?;

        let save_opts = if decl_len == 0 {
            libxml::XML_SAVE_NO_DECL | libxml::XML_SAVE_FORMAT
        } else {
            libxml::XML_SAVE_FORMAT
        };
        let ctxt = (x.xmlSaveToBuffer)(buf, core::ptr::null(), save_opts);
        if ctxt.is_null() || xml_err_occurred() {
            (x.xmlBufferFree)(buf);
            return Err(oom("could not allocate xmlSaveCtxt").into());
        }

        let fail = |msg: &str, code| -> Box<PgError> {
            Box::new(xml_ereport(msg, code))
        };

        if parsed.parsed_as_document() {
            if (x.xmlSaveDoc)(ctxt, parsed.doc) == -1 || xml_err_occurred() {
                (x.xmlSaveClose)(ctxt);
                (x.xmlBufferFree)(buf);
                return Err(fail("could not save document to xmlBuffer", ERRCODE_OUT_OF_MEMORY));
            }
        } else if !parsed.content_nodes.is_null() {
            // Non-singly-rooted XML: fake content-root container, newline
            // text node between non-text children (xml.c:757-816).
            let root = (x.xmlNewNode)(core::ptr::null_mut(), c"content-root".as_ptr() as *const u8);
            if root.is_null() || xml_err_occurred() {
                (x.xmlSaveClose)(ctxt);
                (x.xmlBufferFree)(buf);
                return Err(fail("could not allocate xml node", ERRCODE_OUT_OF_MEMORY));
            }
            let oldroot = (x.xmlDocSetRootElement)(parsed.doc, root);
            if !oldroot.is_null() {
                (x.xmlFreeNode)(oldroot);
            }
            (x.xmlAddChildList)(root, parsed.content_nodes);

            let newline = (x.xmlNewDocText)(core::ptr::null_mut(), c"\n".as_ptr() as *const u8);
            if newline.is_null() || xml_err_occurred() {
                (x.xmlSaveClose)(ctxt);
                (x.xmlBufferFree)(buf);
                return Err(fail("could not allocate xml node", ERRCODE_OUT_OF_MEMORY));
            }

            let mut node = (*(root as *const libxml::xmlNodeHdr)).children;
            while !node.is_null() {
                let hdr = &*(node as *const libxml::xmlNodeHdr);
                if hdr.type_ != libxml::XML_TEXT_NODE
                    && !hdr.prev.is_null()
                    && ((x.xmlSaveTree)(ctxt, newline) == -1 || xml_err_occurred())
                {
                    (x.xmlFreeNode)(newline);
                    (x.xmlSaveClose)(ctxt);
                    (x.xmlBufferFree)(buf);
                    return Err(fail("could not save newline to xmlBuffer", ERRCODE_OUT_OF_MEMORY));
                }
                if (x.xmlSaveTree)(ctxt, node) == -1 || xml_err_occurred() {
                    (x.xmlFreeNode)(newline);
                    (x.xmlSaveClose)(ctxt);
                    (x.xmlBufferFree)(buf);
                    return Err(fail("could not save content to xmlBuffer", ERRCODE_OUT_OF_MEMORY));
                }
                node = hdr.next;
            }
            (x.xmlFreeNode)(newline);
        }

        if (x.xmlSaveClose)(ctxt) == -1 || xml_err_occurred() {
            (x.xmlBufferFree)(buf);
            return Err(fail("could not close xmlSaveCtxtPtr", ERRCODE_INTERNAL_ERROR));
        }

        let mut bytes = libxml::buffer_to_vec(buf);
        (x.xmlBufferFree)(buf);

        // xmlDocContentDumpOutput may add a trailing newline; C trims it only
        // when the REQUESTED xmloption was DOCUMENT (xml.c:822).
        if xmloption_arg == XmlOptionType::XMLOPTION_DOCUMENT {
            while matches!(bytes.last(), Some(&b'\n') | Some(&b'\r')) {
                bytes.pop();
            }
        }
        Ok(bytes)
    }
}

/// C `xmlelement` (xml.c:869), libxml half: the caller has evaluated and
/// mapped all arguments through `map_sql_value_to_xml_value`.
pub fn xmlelement(
    name: &str,
    named_args: &[(String, Option<String>)],
    content: &[String],
) -> PgResult<Vec<u8>> {
    let x = xml2();
    pg_xml_init(PG_XML_STRICTNESS_ALL);
    // SAFETY: all libxml objects freed on every exit path.
    unsafe {
        let buf = (x.xmlBufferCreate)();
        if buf.is_null() || xml_err_occurred() {
            return Err(oom("could not allocate xmlBuffer").into());
        }
        let writer = (x.xmlNewTextWriterMemory)(buf, 0);
        if writer.is_null() || xml_err_occurred() {
            (x.xmlBufferFree)(buf);
            return Err(oom("could not allocate xmlTextWriter").into());
        }

        let name_c = cstr(name.as_bytes());
        (x.xmlTextWriterStartElement)(writer, name_c.as_ptr());

        for (argname, value) in named_args {
            if let Some(v) = value {
                let n = cstr(argname.as_bytes());
                let vc = cstr(v.as_bytes());
                (x.xmlTextWriterWriteAttribute)(writer, n.as_ptr(), vc.as_ptr());
            }
        }
        for s in content {
            let c = cstr(s.as_bytes());
            (x.xmlTextWriterWriteRaw)(writer, c.as_ptr());
        }

        (x.xmlTextWriterEndElement)(writer);
        // Freeing the writer flushes it; must precede the buffer read.
        (x.xmlFreeTextWriter)(writer);

        let result = libxml::buffer_to_vec(buf);
        (x.xmlBufferFree)(buf);
        Ok(result)
    }
}

/// BYTEAOID arm of `map_sql_value_to_xml_value` (xml.c:2615) — base64/binhex.
pub fn encode_binary(bytes: &[u8], binary: XmlBinaryType) -> PgResult<Vec<u8>> {
    let x = xml2();
    pg_xml_init(PG_XML_STRICTNESS_ALL);
    // SAFETY: as xmlelement.
    unsafe {
        let buf = (x.xmlBufferCreate)();
        if buf.is_null() || xml_err_occurred() {
            return Err(oom("could not allocate xmlBuffer").into());
        }
        let writer = (x.xmlNewTextWriterMemory)(buf, 0);
        if writer.is_null() || xml_err_occurred() {
            (x.xmlBufferFree)(buf);
            return Err(oom("could not allocate xmlTextWriter").into());
        }
        let ptr = bytes.as_ptr() as *const c_char;
        let len = bytes.len() as c_int;
        match binary {
            XmlBinaryType::XMLBINARY_BASE64 => {
                (x.xmlTextWriterWriteBase64)(writer, ptr, 0, len);
            }
            XmlBinaryType::XMLBINARY_HEX => {
                (x.xmlTextWriterWriteBinHex)(writer, ptr, 0, len);
            }
        }
        (x.xmlFreeTextWriter)(writer);
        let v = libxml::buffer_to_vec(buf);
        (x.xmlBufferFree)(buf);
        Ok(v)
    }
}

// ===========================================================================
// XML declaration parsing / printing (pure)
// ===========================================================================

pub fn errdetail_for_xml_code(code: i32) -> String {
    match code {
        XML_ERR_INVALID_CHAR => "Invalid character value.".to_string(),
        XML_ERR_SPACE_REQUIRED => "Space required.".to_string(),
        XML_ERR_STANDALONE_VALUE => "standalone accepts only 'yes' or 'no'.".to_string(),
        XML_ERR_VERSION_MISSING => "Malformed declaration: missing version.".to_string(),
        XML_ERR_MISSING_ENCODING => "Missing encoding in text declaration.".to_string(),
        XML_ERR_XMLDECL_NOT_FINISHED => "Parsing XML declaration: '?>' expected.".to_string(),
        _ => format!("Unrecognized libxml error code: {code}."),
    }
}

pub fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// C `xmlIsBlank_ch(c)`.
#[inline]
fn is_blank_ch(c: u8) -> bool {
    c == 0x20 || c == 0x9 || c == 0xA || c == 0xD
}

/// libxml2 `xmlGetUTF8Char` (encoding.c): one codepoint, -1 on decode error.
fn get_utf8_char(utf8: &[u8]) -> i32 {
    if utf8.is_empty() {
        return -1;
    }
    let c0 = utf8[0] as u32;
    if c0 & 0x80 == 0 {
        return c0 as i32;
    }
    let (need, mut val): (usize, u32) = if c0 & 0xE0 == 0xC0 {
        (2, c0 & 0x1F)
    } else if c0 & 0xF0 == 0xE0 {
        (3, c0 & 0x0F)
    } else if c0 & 0xF8 == 0xF0 {
        (4, c0 & 0x07)
    } else {
        return -1;
    };
    if utf8.len() < need {
        return -1;
    }
    for &b in &utf8[1..need] {
        if b & 0xC0 != 0x80 {
            return -1;
        }
        val = (val << 6) | (b as u32 & 0x3F);
    }
    val as i32
}

/// `PG_XMLISNAMECHAR(c)` (xml.c:1366).
fn pg_xml_is_name_char(c: i32) -> bool {
    let Ok(c) = u32::try_from(c) else {
        return false;
    };
    (c < 0x100 && chvalid::xml_is_base_char_ch(c))
        || chvalid::xml_is_ideographic_q(c)
        || (c < 0x100 && chvalid::xml_is_digit_ch(c))
        || c == '.' as u32
        || c == '-' as u32
        || c == '_' as u32
        || c == ':' as u32
        || chvalid::xml_is_combining_q(c)
        || (c < 0x100 && chvalid::xml_is_extender_ch(c))
}

fn starts_with_at(str: &[u8], p: usize, needle: &[u8]) -> bool {
    str.get(p..p + needle.len()).map(|s| s == needle).unwrap_or(false)
}

fn memchr_from(str: &[u8], from: usize, byte: u8) -> Option<usize> {
    str.get(from..)
        .and_then(|s| s.iter().position(|&b| b == byte).map(|i| from + i))
}

fn strnlen(s: &[u8], maxlen: usize) -> usize {
    let mut n = 0;
    while n < maxlen && n < s.len() && s[n] != 0 {
        n += 1;
    }
    n
}

fn skip_xml_space(str: &[u8], p: &mut usize) {
    while is_blank_ch(*str.get(*p).unwrap_or(&0)) {
        *p += 1;
    }
}

/// C `parse_xml_decl` (xml.c:1433, static). `str` is NUL-terminated.
#[allow(clippy::needless_option_as_deref)]
pub fn parse_xml_decl(
    str: &[u8],
    mut lenp: Option<&mut usize>,
    mut version: Option<&mut Option<Vec<u8>>>,
    mut encoding: Option<&mut Option<Vec<u8>>>,
    mut standalone: Option<&mut i32>,
) -> PgResult<i32> {
    if let Some(v) = version.as_deref_mut() {
        *v = None;
    }
    if let Some(e) = encoding.as_deref_mut() {
        *e = None;
    }
    if let Some(s) = standalone.as_deref_mut() {
        *s = -1;
    }

    let mut p: usize = 0;
    let at = |i: usize| -> u8 { *str.get(i).unwrap_or(&0) };

    let mut goto_finished = !starts_with_at(str, p, b"<?xml");
    if !goto_finished {
        // A name char right after `<?xml` = PI, not XMLDecl.
        let after = &str[(p + 5).min(str.len())..];
        let utf8len = strnlen(after, MAX_MULTIBYTE_CHAR_LEN);
        let utf8char = get_utf8_char(&after[..utf8len.min(after.len())]);
        if pg_xml_is_name_char(utf8char) {
            goto_finished = true;
        }
    }

    if !goto_finished {
        p += 5;

        if !is_blank_ch(at(p)) {
            return Ok(XML_ERR_SPACE_REQUIRED);
        }
        skip_xml_space(str, &mut p);
        if !starts_with_at(str, p, b"version") {
            return Ok(XML_ERR_VERSION_MISSING);
        }
        p += 7;
        skip_xml_space(str, &mut p);
        if at(p) != b'=' {
            return Ok(XML_ERR_VERSION_MISSING);
        }
        p += 1;
        skip_xml_space(str, &mut p);

        if at(p) == b'\'' || at(p) == b'"' {
            let quote = at(p);
            match memchr_from(str, p + 1, quote) {
                None => return Ok(XML_ERR_VERSION_MISSING),
                Some(q) => {
                    if let Some(v) = version.as_deref_mut() {
                        *v = Some(str[p + 1..q].to_vec());
                    }
                    p = q + 1;
                }
            }
        } else {
            return Ok(XML_ERR_VERSION_MISSING);
        }

        let save_p = p;
        skip_xml_space(str, &mut p);
        if starts_with_at(str, p, b"encoding") {
            if !is_blank_ch(at(save_p)) {
                return Ok(XML_ERR_SPACE_REQUIRED);
            }
            p += 8;
            skip_xml_space(str, &mut p);
            if at(p) != b'=' {
                return Ok(XML_ERR_MISSING_ENCODING);
            }
            p += 1;
            skip_xml_space(str, &mut p);

            if at(p) == b'\'' || at(p) == b'"' {
                let quote = at(p);
                match memchr_from(str, p + 1, quote) {
                    None => return Ok(XML_ERR_MISSING_ENCODING),
                    Some(q) => {
                        if let Some(e) = encoding.as_deref_mut() {
                            *e = Some(str[p + 1..q].to_vec());
                        }
                        p = q + 1;
                    }
                }
            } else {
                return Ok(XML_ERR_MISSING_ENCODING);
            }
        } else {
            p = save_p;
        }

        let save_p = p;
        skip_xml_space(str, &mut p);
        if starts_with_at(str, p, b"standalone") {
            if !is_blank_ch(at(save_p)) {
                return Ok(XML_ERR_SPACE_REQUIRED);
            }
            p += 10;
            skip_xml_space(str, &mut p);
            if at(p) != b'=' {
                return Ok(XML_ERR_STANDALONE_VALUE);
            }
            p += 1;
            skip_xml_space(str, &mut p);
            if starts_with_at(str, p, b"'yes'") || starts_with_at(str, p, b"\"yes\"") {
                if let Some(s) = standalone.as_deref_mut() {
                    *s = 1;
                }
                p += 5;
            } else if starts_with_at(str, p, b"'no'") || starts_with_at(str, p, b"\"no\"") {
                if let Some(s) = standalone.as_deref_mut() {
                    *s = 0;
                }
                p += 4;
            } else {
                return Ok(XML_ERR_STANDALONE_VALUE);
            }
        } else {
            p = save_p;
        }

        skip_xml_space(str, &mut p);
        if !starts_with_at(str, p, b"?>") {
            return Ok(XML_ERR_XMLDECL_NOT_FINISHED);
        }
        p += 2;
    }

    let len = p;
    for &b in &str[..len.min(str.len())] {
        if b > 127 {
            return Ok(XML_ERR_INVALID_CHAR);
        }
    }
    if let Some(l) = lenp.as_deref_mut() {
        *l = len;
    }
    Ok(XML_ERR_OK)
}

/// C `print_xml_decl` (xml.c:1606, static). `encoding` is a `pg_enc`.
pub fn print_xml_decl(
    buf: &mut Vec<u8>,
    version: Option<&[u8]>,
    encoding: i32,
    standalone: i32,
) -> bool {
    let version_nondefault = version.map(|v| v != PG_XML_DEFAULT_VERSION).unwrap_or(false);

    if version_nondefault || (encoding != 0 && encoding != PG_UTF8) || standalone != -1 {
        buf.extend_from_slice(b"<?xml");
        buf.extend_from_slice(b" version=\"");
        buf.extend_from_slice(version.unwrap_or(PG_XML_DEFAULT_VERSION));
        buf.push(b'"');

        if encoding != 0 && encoding != PG_UTF8 {
            buf.extend_from_slice(b" encoding=\"");
            buf.extend_from_slice(::mbutils::pg_encoding_to_char(encoding).as_bytes());
            buf.push(b'"');
        }

        if standalone == 1 {
            buf.extend_from_slice(b" standalone=\"yes\"");
        } else if standalone == 0 {
            buf.extend_from_slice(b" standalone=\"no\"");
        }
        buf.extend_from_slice(b"?>");
        true
    } else {
        false
    }
}

/// C `xml_doctype_in_content` (xml.c:1672, static).
pub fn xml_doctype_in_content(str: &[u8]) -> bool {
    let mut p: usize = 0;
    let at = |i: usize| -> u8 { *str.get(i).unwrap_or(&0) };

    loop {
        skip_xml_space(str, &mut p);
        if at(p) != b'<' {
            return false;
        }
        p += 1;

        if at(p) == b'!' {
            p += 1;
            if starts_with_at(str, p, b"DOCTYPE") {
                return true;
            }
            if !starts_with_at(str, p, b"--") {
                return false;
            }
            match find_subslice(&str[(p + 2).min(str.len())..], b"--") {
                None => return false,
                Some(off) => {
                    let pos = p + 2 + off;
                    if at(pos + 2) != b'>' {
                        return false;
                    }
                    p = pos + 3;
                    continue;
                }
            }
        }

        if at(p) != b'?' {
            return false;
        }
        p += 1;
        match find_subslice(&str[p.min(str.len())..], b"?>") {
            None => return false,
            Some(off) => {
                p = p + off + 2;
            }
        }
    }
}

// ===========================================================================
// xml_parse (xml.c:1748) — the libxml core
// ===========================================================================

/// The C `xml_parse` outputs: the doc plus how it was parsed. Freed
/// explicitly via [`ParsedXml::free`] — no Drop (owner-managed resource).
pub struct ParsedXml {
    pub doc: *mut xmlDoc,
    parsed_document: bool,
    pub content_nodes: *mut xmlNode,
}

impl ParsedXml {
    pub fn parsed_as_document(&self) -> bool {
        self.parsed_document
    }
    pub fn free(self) {
        if !self.doc.is_null() {
            // SAFETY: doc is live and uniquely owned by self.
            unsafe { (xml2().xmlFreeDoc)(self.doc) };
        }
    }
}

/// C `xml_parse`: `Ok(Some(_))` well-formed, `Ok(None)` soft-saved parse
/// failure, `Err` hard failure. Caller must `free()` the doc.
pub fn xml_parse_doc(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
    encoding: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParsedXml>> {
    let x = xml2();

    let utf8_owned: Option<Vec<u8>> = if encoding == PG_UTF8 || data.is_empty() {
        None
    } else {
        let ctx = ::mcx::MemoryContext::new("xml_parse encoding conversion");
        let conv =
            ::mbutils::pg_do_encoding_conversion(ctx.mcx(), data, encoding, PG_UTF8)?
                .map(|conv| conv.to_vec());
        drop(ctx);
        conv
    };
    let utf8: &[u8] = utf8_owned.as_deref().unwrap_or(data);

    pg_xml_init(PG_XML_STRICTNESS_WELLFORMED);

    let mut work = utf8.to_vec();
    work.push(0);

    let mut parse_as_document = xmloption_arg == XmlOptionType::XMLOPTION_DOCUMENT;
    let mut count: usize = 0;
    let mut version: Option<Vec<u8>> = None;
    let mut standalone: i32 = 0;

    if !parse_as_document {
        let res_code = parse_xml_decl(
            &work,
            Some(&mut count),
            Some(&mut version),
            None,
            Some(&mut standalone),
        )?;
        if res_code != 0 {
            let e = PgError::error("invalid XML content: invalid XML declaration")
                .with_sqlstate(ERRCODE_INVALID_XML_CONTENT)
                .with_detail(errdetail_for_xml_code(res_code));
            return ereturn(escontext, None, e);
        }
        if xml_doctype_in_content(&work[count..]) {
            parse_as_document = true;
        }
    }

    // SAFETY: libxml objects freed on every exit path; work/chunk buffers are
    // NUL-terminated and outlive the calls that read them.
    unsafe {
        if parse_as_document {
            let ctxt = (x.xmlNewParserCtxt)();
            if ctxt.is_null() || xml_err_occurred() {
                if !ctxt.is_null() {
                    (x.xmlFreeParserCtxt)(ctxt);
                }
                return Err(oom("could not allocate parser context").into());
            }
            let options = XML_PARSE_NOENT
                | XML_PARSE_DTDATTR
                | if preserve_whitespace { 0 } else { XML_PARSE_NOBLANKS };
            let doc = (x.xmlCtxtReadDoc)(
                ctxt,
                work.as_ptr(),
                core::ptr::null(),
                c"UTF-8".as_ptr(),
                options,
            );
            let result = if doc.is_null() || xml_err_occurred() {
                if !doc.is_null() {
                    (x.xmlFreeDoc)(doc);
                }
                let (code, msg) = if xmloption_arg == XmlOptionType::XMLOPTION_DOCUMENT {
                    (ERRCODE_INVALID_XML_DOCUMENT, "invalid XML document")
                } else {
                    (ERRCODE_INVALID_XML_CONTENT, "invalid XML content")
                };
                ereturn(escontext.take(), None, xml_ereport(msg, code))
            } else {
                errhandler::flush_xml_warnings();
                Ok(Some(ParsedXml {
                    doc,
                    parsed_document: true,
                    content_nodes: core::ptr::null_mut(),
                }))
            };
            (x.xmlFreeParserCtxt)(ctxt);
            result
        } else {
            let version_c = version.as_ref().map(|v| cstr(v));
            let version_ptr = version_c
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(core::ptr::null());
            let doc = (x.xmlNewDoc)(version_ptr);
            if doc.is_null() || xml_err_occurred() {
                return Err(oom("could not allocate XML document").into());
            }
            let hdr = doc as *mut xmlDocHdr;
            (*hdr).encoding = (x.xmlStrdup)(c"UTF-8".as_ptr() as *const u8);
            if (*hdr).encoding.is_null() || xml_err_occurred() {
                (x.xmlFreeDoc)(doc);
                return Err(oom("could not allocate XML document").into());
            }
            (*hdr).standalone = standalone;

            let save = (x.xmlKeepBlanksDefault)(if preserve_whitespace { 1 } else { 0 });

            let tail = &work[count..];
            let result = if tail.first().copied().unwrap_or(0) != 0 {
                let mut nodes: *mut xmlNode = core::ptr::null_mut();
                let rc = (x.xmlParseBalancedChunkMemory)(
                    doc,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                    0,
                    tail.as_ptr(),
                    &mut nodes,
                );
                if rc != 0 || xml_err_occurred() {
                    (x.xmlKeepBlanksDefault)(save);
                    (x.xmlFreeDoc)(doc);
                    return ereturn(
                        escontext.take(),
                        None,
                        xml_ereport("invalid XML content", ERRCODE_INVALID_XML_CONTENT),
                    );
                }
                errhandler::flush_xml_warnings();
                Ok(Some(ParsedXml {
                    doc,
                    parsed_document: false,
                    content_nodes: nodes,
                }))
            } else {
                errhandler::flush_xml_warnings();
                Ok(Some(ParsedXml {
                    doc,
                    parsed_document: false,
                    content_nodes: core::ptr::null_mut(),
                }))
            };
            (x.xmlKeepBlanksDefault)(save);
            result
        }
    }
}

/// Well-formedness check over [`xml_parse_doc`]; frees the doc.
pub fn xml_parse_ok(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
    encoding: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    match xml_parse_doc(data, xmloption_arg, preserve_whitespace, encoding, escontext)? {
        Some(parsed) => {
            parsed.free();
            Ok(true)
        }
        None => Ok(false),
    }
}

// ===========================================================================
// SQL identifier <-> XML name mapping / escaping
// ===========================================================================

/// C `escape_xml` (xml.c:2593).
pub fn escape_xml(str: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(str.len());
    for &b in str {
        match b {
            b'&' => buf.extend_from_slice(b"&amp;"),
            b'<' => buf.extend_from_slice(b"&lt;"),
            b'>' => buf.extend_from_slice(b"&gt;"),
            b'\r' => buf.extend_from_slice(b"&#x0d;"),
            other => buf.push(other),
        }
    }
    buf
}

/// C `sqlchar_to_unicode` (xml.c:2336, static).
fn sqlchar_to_unicode(s: &[u8]) -> PgResult<u32> {
    if GetDatabaseEncoding() == PG_UTF8 {
        let c = get_utf8_char(&s[..strnlen(s, MAX_MULTIBYTE_CHAR_LEN)]);
        return Ok(if c < 0 { 0 } else { c as u32 });
    }
    let mblen = ::mbutils::pg_mblen(s) as usize;
    let ctx = ::mcx::MemoryContext::new("sqlchar_to_unicode");
    let utf8 = match ::mbutils::pg_do_encoding_conversion(
        ctx.mcx(),
        &s[..mblen.min(s.len())],
        GetDatabaseEncoding(),
        PG_UTF8,
    )? {
        Some(conv) => conv.to_vec(),
        None => s[..mblen.min(s.len())].to_vec(),
    };
    let c = get_utf8_char(&utf8);
    Ok(if c < 0 { 0 } else { c as u32 })
}

fn is_valid_xml_namefirst(c: u32) -> bool {
    chvalid::xml_is_base_char_q(c)
        || chvalid::xml_is_ideographic_q(c)
        || c == '_' as u32
        || c == ':' as u32
}

fn is_valid_xml_namechar(c: u32) -> bool {
    chvalid::xml_is_base_char_q(c)
        || chvalid::xml_is_ideographic_q(c)
        || chvalid::xml_is_digit_q(c)
        || c == '.' as u32
        || c == '-' as u32
        || c == '_' as u32
        || c == ':' as u32
        || chvalid::xml_is_combining_q(c)
        || chvalid::xml_is_extender_q(c)
}

/// C `map_sql_identifier_to_xml_name` (xml.c:2379).
pub fn map_sql_identifier_to_xml_name(
    ident: &[u8],
    fully_escaped: bool,
    escape_period: bool,
) -> PgResult<Vec<u8>> {
    debug_assert!(fully_escaped || !escape_period);

    let mut buf: Vec<u8> = Vec::new();
    let mut p: usize = 0;
    let n = ident.len();

    while p < n && ident[p] != 0 {
        let cur = ident[p];
        let next = if p + 1 < n { ident[p + 1] } else { 0 };

        if cur == b':' && (p == 0 || fully_escaped) {
            buf.extend_from_slice(b"_x003A_");
            p += 1;
        } else if cur == b'_' && next == b'x' {
            buf.extend_from_slice(b"_x005F_");
            p += 1;
        } else if fully_escaped
            && p == 0
            && ident.len() >= 3
            && ident[..3].eq_ignore_ascii_case(b"xml")
        {
            if cur == b'x' {
                buf.extend_from_slice(b"_x0078_");
            } else {
                buf.extend_from_slice(b"_x0058_");
            }
            p += 1;
        } else if escape_period && cur == b'.' {
            buf.extend_from_slice(b"_x002E_");
            p += 1;
        } else {
            let mblen = ::mbutils::pg_mblen(&ident[p..]) as usize;
            let u = sqlchar_to_unicode(&ident[p..])?;
            let valid = if p == 0 {
                is_valid_xml_namefirst(u)
            } else {
                is_valid_xml_namechar(u)
            };
            if !valid {
                buf.extend_from_slice(format!("_x{u:04X}_").as_bytes());
            } else {
                buf.extend_from_slice(&ident[p..(p + mblen).min(n)]);
            }
            p += mblen.max(1);
        }
    }
    Ok(buf)
}

/// C `map_xml_name_to_sql_identifier` (xml.c:2435).
pub fn map_xml_name_to_sql_identifier(name: &[u8]) -> PgResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut p: usize = 0;
    let n = name.len();
    let is_hex = |b: u8| b.is_ascii_hexdigit();

    while p < n && name[p] != 0 {
        let g = |off: usize| -> u8 {
            if p + off < n {
                name[p + off]
            } else {
                0
            }
        };
        if name[p] == b'_'
            && g(1) == b'x'
            && is_hex(g(2))
            && is_hex(g(3))
            && is_hex(g(4))
            && is_hex(g(5))
            && g(6) == b'_'
        {
            let hex = &name[p + 2..p + 6];
            let u = u32::from_str_radix(&String::from_utf8_lossy(hex), 16).unwrap_or(0);
            let ctx = ::mcx::MemoryContext::new("map_xml_name_to_sql_identifier");
            let bytes = ::mbutils::pg_unicode_to_server(ctx.mcx(), u)?;
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            buf.extend_from_slice(&bytes[..end]);
            p += 7;
        } else {
            let mblen = ::mbutils::pg_mblen(&name[p..]) as usize;
            buf.extend_from_slice(&name[p..(p + mblen).min(n)]);
            p += mblen.max(1);
        }
    }
    Ok(buf)
}
