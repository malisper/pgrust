//! Idiomatic port of `src/backend/utils/adt/xml.c` — XML data type support.
//!
//! Every function from `xml.c` is implemented here. Following the C file's own
//! design (see its header comment: "even if [libxml] is not done, the type and
//! all the functions are available, but most of them will fail"), the
//! libxml-dependent functions route their libxml-internal work through the
//! libxml provider seams. When the installed `have_libxml` provider reports
//! `false`, the in-crate code raises the `NO_XML_SUPPORT()` feature error
//! ([`no_xml_support`]) exactly as a `--without-libxml` server would. The
//! substantial body of *pure* string / SQL-XML scaffolding — declaration
//! printing, escaping, identifier and type-name mapping, the
//! table/query/schema/database publishing family, XSD generation — is ported
//! 1:1 in-crate, reaching SPI, the catalog, type output functions, and encoding
//! conversion through their narrow seams.
//!
//! `xmltype` is `struct varlena` (text-compatible), so the ported bodies work
//! at the C-string / byte level; values are `Vec<u8>` / `String` exactly where
//! the C code held `text *` / `char *` (matching the idiomatic
//! `backend-utils-adt-varlena` owned-payload convention).

// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// registers the `xml.c` `xml_is_well_formed*` builtins into the fmgr-core table
// (C: `fmgr_builtins[]`), whose `BuiltinFunction`/raise marshalling uses
// `String`/`std`. (The `extern crate alloc` + `alloc::` imports below stay
// valid under std.)
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `clippy::result_large_err`: every fallible function here returns the shared
// `types_error::PgResult` (== `Result<_, PgError>`). `PgError`'s size is fixed
// by the types crate (a faithful port of `ErrorData`) and the un-boxed
// `PgResult` return type is the project-wide error contract these ports match.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::borrow::ToOwned;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

pub mod chvalid;
pub mod dep_seams;
pub mod fmgr_builtins;

use types_datum::Datum;
use types_error::{
    ERRCODE_DATA_EXCEPTION, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_XML_COMMENT,
    ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION, ERRCODE_NULL_VALUE_NOT_ALLOWED, PgError, PgResult,
    SoftErrorContext, ereturn,
};
use types_tuple::heaptuple::{
    BOOLOID, BPCHAROID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, VARCHAROID, VARHDRSZ,
    XMLOID,
};
use types_core::{InvalidOid, Oid};
/// The unified value carrier (`ByVal`/`ByRef`/`Cstring`) the executor and
/// row-mapping callers hold column values in, distinct from the bare-word
/// [`Datum`] (`types_datum::Datum`) the in-crate scalar formatters use.
use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

pub use types_error::ERRCODE_NOT_AN_XML_DOCUMENT;
pub use types_nodes::primnodes::{XmlExprOp, XmlOptionType};
pub use types_xml::{PgXmlStrictness, XmlBinaryType, XmlStandaloneType};

use backend_utils_adt_datetime::{
    j2date, timestamp2tm, EncodeDateOnly, EncodeDateTime, TIMESTAMP_NOT_FINITE,
};
use types_datetime::{fsec_t, DateADT, Timestamp, POSTGRES_EPOCH_JDATE, USE_XSD_DATES};
use types_pgtime::pgtime::pg_tm;
use types_wchar::encoding::PG_UTF8;

use backend_utils_adt_xml_libxml_seams as seam;
use backend_utils_cache_lsyscache_seams as lsc;
use types_xml::{RelationColumn, SpiColumn, SpiResult};

// ---------------------------------------------------------------------------
// `NO_XML_SUPPORT()` macro and inward seam adapter
// ---------------------------------------------------------------------------

/// C `NO_XML_SUPPORT()` — ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, ...).
fn no_xml_support() -> PgError {
    PgError::error("unsupported XML feature")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail("This functionality requires the server to be built with libxml support.")
}

/// Seam adapter for `backend-utils-adt-xml-seams::escape_xml`: the consumer
/// (explain-format) passes valid UTF-8 text and wants an mcx-allocated string.
fn escape_xml_seam<'mcx>(mcx: mcx::Mcx<'mcx>, s: &str) -> PgResult<mcx::PgString<'mcx>> {
    let escaped = escape_xml(s.as_bytes());
    // `escaped` is XML-escaped bytes of valid UTF-8 input → still valid UTF-8.
    let text = core::str::from_utf8(&escaped).expect("xml-escaped UTF-8 stays UTF-8");
    mcx::PgString::from_str_in(text, mcx)
}

// ---------------------------------------------------------------------------
// GUC variable backing storage (in C: `int xmlbinary = XMLBINARY_BASE64;` and
// `int xmloption = XMLOPTION_CONTENT;`, xml.c:109-110). xml.c owns these module
// globals (declared `extern` in guc_tables.c), read directly from the GUC slot
// — they are NOT sourced from ControlFile. Modelled here as backend-local
// atomics seeded with the PostgreSQL boot defaults; the GUC engine writes them
// at startup through the installed `GucVarAccessors`.
// ---------------------------------------------------------------------------

use core::sync::atomic::{AtomicI32, Ordering};

/// `int xmlbinary = XMLBINARY_BASE64;` (xml.c:109). `XMLBINARY_BASE64 == 0`.
static XMLBINARY: AtomicI32 = AtomicI32::new(XmlBinaryType::XMLBINARY_BASE64 as i32);
/// `int xmloption = XMLOPTION_CONTENT;` (xml.c:110). `XMLOPTION_CONTENT == 1`.
static XMLOPTION: AtomicI32 = AtomicI32::new(XmlOptionType::XMLOPTION_CONTENT as i32);

#[inline]
fn xmlbinary_guc() -> i32 {
    XMLBINARY.load(Ordering::Relaxed)
}
#[inline]
fn set_xmlbinary_guc(v: i32) {
    XMLBINARY.store(v, Ordering::Relaxed);
}
#[inline]
fn xmloption_guc() -> i32 {
    XMLOPTION.load(Ordering::Relaxed)
}
#[inline]
fn set_xmloption_guc(v: i32) {
    XMLOPTION.store(v, Ordering::Relaxed);
}

/// `XmlBinaryType` decode of the `xmlbinary` GUC int — for `seam::xmlbinary`.
#[inline]
fn xmlbinary_seam() -> XmlBinaryType {
    match xmlbinary_guc() {
        x if x == XmlBinaryType::XMLBINARY_HEX as i32 => XmlBinaryType::XMLBINARY_HEX,
        _ => XmlBinaryType::XMLBINARY_BASE64,
    }
}

/// `XmlOptionType` decode of the `xmloption` GUC int — for `seam::xmloption`.
#[inline]
fn xmloption_seam() -> XmlOptionType {
    match xmloption_guc() {
        x if x == XmlOptionType::XMLOPTION_DOCUMENT as i32 => XmlOptionType::XMLOPTION_DOCUMENT,
        _ => XmlOptionType::XMLOPTION_CONTENT,
    }
}

/// Seam adapter for `backend-utils-adt-xml-seams::map_xml_name_to_sql_identifier`:
/// the consumer (ruleutils `T_XmlExpr` deparse) passes a valid UTF-8 XML name
/// and wants the decoded SQL identifier mcx-allocated.
fn map_xml_name_to_sql_identifier_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    name: &str,
) -> PgResult<mcx::PgString<'mcx>> {
    let ident = map_xml_name_to_sql_identifier(name.as_bytes())?;
    mcx::PgString::from_str_in(ident.as_str(), mcx)
}

/// Install this crate's inward seams.
pub fn init_seams() {
    backend_utils_adt_xml_seams::escape_xml::set(escape_xml_seam);
    backend_utils_adt_xml_seams::map_xml_name_to_sql_identifier::set(
        map_xml_name_to_sql_identifier_seam,
    );

    // The `xmlbinary` / `xmloption` GUC slots: install the `conf->variable`
    // accessors (read directly from the GUC slot, never ControlFile) plus the
    // `xmlbinary()` / `xmloption()` reader seams that decode them to enums.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::xmlbinary.install(GucVarAccessors {
            get: xmlbinary_guc,
            set: set_xmlbinary_guc,
        });
        vars::xmloption.install(GucVarAccessors {
            get: xmloption_guc,
            set: set_xmloption_guc,
        });
    }
    seam::xmlbinary::set(xmlbinary_seam);
    seam::xmloption::set(xmloption_seam);

    // Register the `xml_is_well_formed*` SQL-callable builtins into the
    // fmgr-core builtin table (C: `fmgr_builtins[]`).
    fmgr_builtins::register_xml_builtins();

    // Install the cross-subsystem (non-libxml2) dependency seams xml.c reaches
    // through: syscache/lsyscache lookups, namespace resolution, utils/mb
    // encoding conversions — wired from their real ported owners.
    dep_seams::install();
}

// ---------------------------------------------------------------------------
// File-scope constants (xml.c)
// ---------------------------------------------------------------------------

/// `PG_XML_DEFAULT_VERSION "1.0"` (xml.c:301).
pub const PG_XML_DEFAULT_VERSION: &str = "1.0";

/// `NAMESPACE_XSD` (xml.c:243) — SQL/XML:2008 section 4.9.
pub const NAMESPACE_XSD: &str = "http://www.w3.org/2001/XMLSchema";
/// `NAMESPACE_XSI` (xml.c:244).
pub const NAMESPACE_XSI: &str = "http://www.w3.org/2001/XMLSchema-instance";
/// `NAMESPACE_SQLXML` (xml.c:245).
pub const NAMESPACE_SQLXML: &str = "http://standards.iso.org/iso/9075/2003/sqlxml";

/// `ERRCXT_MAGIC` (xml.c:115) — random number identifying a `PgXmlErrorContext`.
pub const ERRCXT_MAGIC: i32 = 68275028;
/// `XMLTABLE_CONTEXT_MAGIC` (xml.c:195) — random number identifying an
/// `XmlTableContext`.
pub const XMLTABLE_CONTEXT_MAGIC: i32 = 46922182;

// ---------------------------------------------------------------------------
// libxml XML_ERR_* codes used by parse_xml_decl / errdetail_for_xml_code.
// (libxml/xmlerror.h enum values.)
// ---------------------------------------------------------------------------

const XML_ERR_OK: i32 = 0;
const XML_ERR_INVALID_CHAR: i32 = 9;
const XML_ERR_XMLDECL_NOT_FINISHED: i32 = 57;
const XML_ERR_SPACE_REQUIRED: i32 = 65;
const XML_ERR_STANDALONE_VALUE: i32 = 78;
const XML_ERR_VERSION_MISSING: i32 = 96;
const XML_ERR_MISSING_ENCODING: i32 = 101;

/// `MAX_MULTIBYTE_CHAR_LEN` (mb/pg_wchar.h) — used to bound `strnlen` in
/// `parse_xml_decl`.
const MAX_MULTIBYTE_CHAR_LEN: usize = 4;

// ===========================================================================
// xmltype constructors / helpers (text-compatible varlena bodies)
// ===========================================================================

/// C `appendStringInfoText` (xml.c:459) — append a text value's payload bytes.
fn append_string_info_text(str: &mut Vec<u8>, t: &[u8]) {
    str.extend_from_slice(t);
}

/// C `stringinfo_to_xmltype` (xml.c:467) — `cstring_to_text_with_len(buf->data,
/// buf->len)`. `xmltype` and `text` share a representation, so this is the
/// identity on payload bytes.
pub fn stringinfo_to_xmltype(buf: &[u8]) -> Vec<u8> {
    buf.to_vec()
}

/// C `cstring_to_xmltype` (xml.c:474) — `cstring_to_text(string)`.
pub fn cstring_to_xmltype(string: &str) -> Vec<u8> {
    string.as_bytes().to_vec()
}

/// C `xmlBuffer_to_xmltype` (xml.c:482) — `cstring_to_text_with_len` over an
/// `xmlBuffer`. The libxml-buffer construction lives in the libxml provider; the
/// bytes it returns are already the payload.
pub fn xmlBuffer_to_xmltype(buf: &[u8]) -> Vec<u8> {
    buf.to_vec()
}

// ===========================================================================
// I/O functions
// ===========================================================================

/// C `xmlChar_to_encoding` (xml.c:250) — map an encoding name to a `pg_enc`.
pub fn xmlChar_to_encoding(encoding_name: &str) -> PgResult<i32> {
    let encoding = common_encnames_seams::pg_char_to_encoding::call(encoding_name);
    if encoding < 0 {
        return Err(
            PgError::error(format!("invalid encoding name \"{encoding_name}\""))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }
    Ok(encoding)
}

/// C `xml_in` (xml.c:272) — `xml` type input function. Parses the input to check
/// well-formedness via libxml.
///
/// `escontext` is the soft `ErrorSaveContext` off the fmgr call frame
/// (C: `(Node *) fcinfo->context`). On a malformed-XML parse the error is routed
/// into the soft context (so `pg_input_is_valid('bad','xml')` returns false) and
/// `Ok(None)` is returned (C: `if (doc == NULL) PG_RETURN_NULL();`); with no soft
/// sink the parse error is thrown as a hard `Err`.
pub fn xml_in(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<Option<Vec<u8>>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let vardata = s.as_bytes().to_vec();
    // Parse to check well-formedness; on a soft failure `xml_parse` saves the
    // error into `escontext` and returns false => return NULL.
    if !xml_parse(
        &vardata,
        seam::xmloption::call(),
        true,
        seam::get_database_encoding::call(),
        escontext,
    )? {
        return Ok(None);
    }
    Ok(Some(vardata))
}

/// C `xml_out_internal` (xml.c:311) — render an `xmltype` to a C string,
/// stripping the encoding from any XML declaration.
pub fn xml_out_internal(x: &[u8], target_encoding: i32) -> PgResult<Vec<u8>> {
    // text_to_cstring((text *) x)
    let str = x.to_vec();

    if !seam::have_libxml::call() {
        return Ok(str);
    }

    let mut len = str.len();
    let mut version: Option<Vec<u8>> = None;
    let mut standalone: i32 = 0;
    let res_code = parse_xml_decl(
        &str,
        Some(&mut len),
        Some(&mut version),
        None,
        Some(&mut standalone),
    )?;

    if res_code == XML_ERR_OK {
        let mut buf: Vec<u8> = Vec::new();
        if !print_xml_decl(&mut buf, version.as_deref(), target_encoding, standalone) {
            // If we are not going to produce an XML declaration, eat a single
            // newline in the original string to prevent empty first lines.
            if str.get(len) == Some(&b'\n') {
                len += 1;
            }
        }
        buf.extend_from_slice(&str[len..]);
        return Ok(buf);
    }

    // ereport(WARNING, errcode(ERRCODE_DATA_CORRUPTED),
    //   errmsg_internal("could not parse XML declaration in stored value"),
    //   errdetail_for_xml_code(res_code));
    backend_utils_error_elog_seams::ereport_msg::call(
        types_error::WARNING,
        "could not parse XML declaration in stored value".to_string(),
        Some(errdetail_for_xml_code(res_code)),
    )?;

    // Returns the unchanged stored string.
    Ok(str)
}

/// C `xml_out` (xml.c:355) — `xml` type output function. Removes the encoding in
/// all cases.
pub fn xml_out(x: &[u8]) -> PgResult<Vec<u8>> {
    xml_out_internal(x, 0)
}

/// C `xml_recv` (xml.c:370) — `xml` type binary receive function.
///
/// `buf` is the unread remainder of the message (already past the cursor).
pub fn xml_recv(buf: &[u8]) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

    // Read the raw bytes; we don't yet know the encoding.
    let nbytes = buf.len();
    // Build a NUL-terminated working copy for parse_xml_decl.
    let mut work = buf.to_vec();
    work.push(0);

    let mut encoding_str: Option<Vec<u8>> = None;
    parse_xml_decl(&work, None, None, Some(&mut encoding_str), None)?;

    // If encoding wasn't explicitly specified, treat as UTF-8.
    let encoding = match encoding_str {
        Some(ref e) => xmlChar_to_encoding(&String::from_utf8_lossy(e))?,
        None => PG_UTF8,
    };

    // Parse to check well-formedness; xml_parse throws ERROR if not.
    let result = &work[..nbytes];
    xml_parse(result, seam::xmloption::call(), true, encoding, None)?;

    // Now that we know what we're dealing with, convert to server encoding:
    //   newstr = pg_any_to_server(str, nbytes, encoding);
    let newstr = seam::any_to_server::call(result, encoding)?;
    Ok(newstr)
}

/// C `xml_send` (xml.c:438) — `xml` type binary send function.
pub fn xml_send(x: &[u8]) -> PgResult<Vec<u8>> {
    // xml_out_internal doesn't convert the encoding, it just prints the right
    // declaration. pq_sendtext does the server->client conversion.
    let outval = xml_out_internal(x, seam::client_encoding::call())?;
    // pq_sendtext(&buf, outval, strlen(outval)) — converts server->client.
    seam::server_to_client::call(&outval)
}

// ===========================================================================
// SQL/XML publishing functions
// ===========================================================================

/// C `xmlcomment` (xml.c:490) — `XMLCOMMENT(text)`.
pub fn xmlcomment(arg: &[u8]) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

    let argdata = arg;
    let len = argdata.len();

    // check for "--" in string or "-" at the end
    for i in 1..len {
        if argdata[i] == b'-' && argdata[i - 1] == b'-' {
            return Err(
                PgError::error("invalid XML comment").with_sqlstate(ERRCODE_INVALID_XML_COMMENT)
            );
        }
    }
    if len > 0 && argdata[len - 1] == b'-' {
        return Err(
            PgError::error("invalid XML comment").with_sqlstate(ERRCODE_INVALID_XML_COMMENT)
        );
    }

    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"<!--");
    append_string_info_text(&mut buf, arg);
    buf.extend_from_slice(b"-->");

    Ok(stringinfo_to_xmltype(&buf))
}

/// C `xmltext` (xml.c:526) — `XMLTEXT(text)`.
pub fn xmltext(arg: &[u8]) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    // xmlEncodeSpecialChars(NULL, xml_text2xmlChar(arg))
    seam::encode_special_chars::call(arg)
}

/// C `xmlconcat` (xml.c:553) — `XMLCONCAT(args)`. `args` are the (text-
/// compatible) xmltype payloads.
pub fn xmlconcat(args: &[Vec<u8>]) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

    let mut global_standalone: i32 = 1;
    let mut global_version: Option<Vec<u8>> = None;
    let mut global_version_no_value = false;
    let mut buf: Vec<u8> = Vec::new();

    for x in args {
        let mut len = x.len();
        // text_to_cstring; ensure a terminating NUL for parse_xml_decl scanning.
        let mut str = x.clone();
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

        // appendStringInfoString(&buf, str + len) — payload after the decl.
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

    Ok(stringinfo_to_xmltype(&buf))
}

/// C `xmlconcat2` (xml.c:619) — two-argument `xmlconcat` SQL wrapper (XMLAGG).
pub fn xmlconcat2(arg1: Option<&[u8]>, arg2: Option<&[u8]>) -> PgResult<Option<Vec<u8>>> {
    match (arg1, arg2) {
        (None, None) => Ok(None),
        (None, Some(a2)) => Ok(Some(a2.to_vec())),
        (Some(a1), None) => Ok(Some(a1.to_vec())),
        (Some(a1), Some(a2)) => {
            let args = [a1.to_vec(), a2.to_vec()];
            Ok(Some(xmlconcat(&args)?))
        }
    }
}

/// C `texttoxml` (xml.c:636) — `text::xml` cast.
pub fn texttoxml(data: &[u8]) -> PgResult<Vec<u8>> {
    xmlparse(data, seam::xmloption::call(), true)
}

/// C `xmltotext` (xml.c:645) — `xml::text` cast. Binary compatible.
pub fn xmltotext(data: &[u8]) -> PgResult<Vec<u8>> {
    Ok(data.to_vec())
}

/// C `xmltotext_with_options` (xml.c:656) — `XMLSERIALIZE`'s core.
pub fn xmltotext_with_options(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    indent: bool,
) -> PgResult<Vec<u8>> {
    if xmloption_arg != XmlOptionType::XMLOPTION_DOCUMENT && !indent {
        // Backwards-compatibility: succeed even without libxml.
        return Ok(data.to_vec());
    }

    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

    seam::serialize_with_options::call(
        data,
        xmloption_arg,
        indent,
        seam::get_database_encoding::call(),
    )
}

/// C `xmlelement` (xml.c:869) — `XMLELEMENT(...)`.
///
/// `name` is the element name. `named_args` are the (attribute-name,
/// already-mapped-value-or-NULL) pairs, and `content` the already-mapped child
/// content strings. The caller has evaluated all arguments and mapped them
/// through [`map_sql_value_to_xml_value`] just as the C code does.
pub fn xmlelement(
    name: &str,
    named_args: &[(String, Option<String>)],
    content: &[String],
) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::build_element::call(name.to_owned(), named_args.to_vec(), content.to_vec())
}

/// C `xmlparse` (xml.c:993) — `XMLPARSE(...)`.
pub fn xmlparse(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    xml_parse(
        data,
        xmloption_arg,
        preserve_whitespace,
        seam::get_database_encoding::call(),
        None,
    )?;
    Ok(data.to_vec())
}

/// C `xmlpi` (xml.c:1011) — `XMLPI(...)`. Returns `(value, is_null)`.
pub fn xmlpi(
    target: &str,
    arg: Option<&[u8]>,
    arg_is_null: bool,
) -> PgResult<(Option<Vec<u8>>, bool)> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

    if target.eq_ignore_ascii_case("xml") {
        return Err(PgError::error("invalid XML processing instruction")
            .with_sqlstate(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION)
            .with_detail(format!(
                "XML processing instruction target name cannot be \"{target}\"."
            )));
    }

    // Null check comes after the syntax check (SQL standard).
    let result_is_null = arg_is_null;
    if result_is_null {
        return Ok((None, true));
    }

    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"<?");
    buf.extend_from_slice(target.as_bytes());

    if let Some(arg) = arg {
        let string = arg; // text_to_cstring(arg)
        if find_subslice(string, b"?>").is_some() {
            return Err(PgError::error("invalid XML processing instruction")
                .with_sqlstate(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION)
                .with_detail("XML processing instruction cannot contain \"?>\"."));
        }

        buf.push(b' ');
        // appendStringInfoString(&buf, string + strspn(string, " "))
        let skip = string.iter().take_while(|&&c| c == b' ').count();
        buf.extend_from_slice(&string[skip..]);
    }
    buf.extend_from_slice(b"?>");

    Ok((Some(stringinfo_to_xmltype(&buf)), false))
}

/// C `xmlroot` (xml.c:1063) — `XMLROOT(...)`.
pub fn xmlroot(
    data: &[u8],
    version: Option<&[u8]>,
    standalone: XmlStandaloneType,
) -> PgResult<Vec<u8>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

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

    // version overrides: if a version arg was given, use it (xml_text2xmlChar);
    // otherwise NULL (matching the C `orig_version = NULL` else branch).
    let final_version: Option<Vec<u8>> = version.map(|v| v.to_vec());

    match standalone {
        XmlStandaloneType::XML_STANDALONE_YES => orig_standalone = 1,
        XmlStandaloneType::XML_STANDALONE_NO => orig_standalone = 0,
        XmlStandaloneType::XML_STANDALONE_NO_VALUE => orig_standalone = -1,
        XmlStandaloneType::XML_STANDALONE_OMITTED => { /* leave original value */ }
    }

    let mut buf: Vec<u8> = Vec::new();
    print_xml_decl(&mut buf, final_version.as_deref(), 0, orig_standalone);
    buf.extend_from_slice(&data[len.min(data.len())..]);

    Ok(stringinfo_to_xmltype(&buf))
}

/// C `xmlvalidate` (xml.c:1118) — removed (security hole); always errors.
pub fn xmlvalidate(_data: Datum, _dtd: Datum) -> PgResult<bool> {
    Err(PgError::error("xmlvalidate is not implemented")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// C `xml_is_document` (xml.c:1129) — `xmlval IS DOCUMENT`.
pub fn xml_is_document(arg: &[u8]) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    // Report "true" if no soft error is reported by xml_parse(DOCUMENT).
    let outcome = seam::xml_parse_libxml::call(
        arg,
        XmlOptionType::XMLOPTION_DOCUMENT,
        true,
        seam::get_database_encoding::call(),
    )?;
    Ok(outcome.is_ok())
}

// ===========================================================================
// libxml lifecycle / error handling
// ===========================================================================

/// C `pg_xml_init_library` (xml.c:1165) — one-time libxml setup. The real
/// `LIBXML_TEST_VERSION` / `xmlChar` check lives in the libxml provider; with no
/// provider this is a no-op (the type still works without libxml).
pub fn pg_xml_init_library() {
    // No-op in the fail-safe configuration; the provider performs the actual
    // library initialization when installed.
}

/// C `pg_xml_init` (xml.c:1211) — set up a `PgXmlErrorContext`.
///
/// The error context is wholly internal to the libxml provider; this entry point
/// exists for API parity and, without a provider, surfaces the
/// `NO_XML_SUPPORT()` feature error.
pub fn pg_xml_init(_strictness: PgXmlStrictness) -> PgResult<()> {
    pg_xml_init_library();
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    Ok(())
}

/// C `pg_xml_done` (xml.c:1292) — tear down a `PgXmlErrorContext`. No-op in the
/// fail-safe configuration (the provider owns libxml's global state).
pub fn pg_xml_done(_is_error: bool) {}

/// C `pg_xml_error_occurred` (xml.c:1340). Without a libxml provider no error
/// context exists, so no error has occurred.
pub fn pg_xml_error_occurred() -> bool {
    false
}

/// C `errdetail_for_xml_code` (xml.c:2276, static) — map a libxml decl-parse
/// error code to an errdetail string.
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

/// C `chopStringInfoNewlines` (xml.c:2313, static) — strip trailing newlines.
pub fn chopStringInfoNewlines(str: &mut Vec<u8>) {
    while !str.is_empty() && *str.last().unwrap() == b'\n' {
        str.pop();
    }
}

/// C `appendStringInfoLineSeparator` (xml.c:2324, static).
pub fn appendStringInfoLineSeparator(str: &mut Vec<u8>) {
    chopStringInfoNewlines(str);
    if !str.is_empty() {
        str.push(b'\n');
    }
}

// ===========================================================================
// XML declaration parsing / printing
// ===========================================================================

/// C `xml_pnstrdup` (xml.c:1375) — `pnstrdup` for xmlChar; returns owned bytes.
pub fn xml_pnstrdup(str: &[u8]) -> Vec<u8> {
    str.to_vec()
}

/// C `pg_xmlCharStrndup` (xml.c:1387, static) — like `xml_pnstrdup` for char*.
pub fn pg_xmlCharStrndup(str: &[u8], len: usize) -> Vec<u8> {
    str[..len.min(str.len())].to_vec()
}

/// C `xml_pstrdup_and_free` (xml.c:1404) — copy an xmlChar string, freeing the
/// input. In Rust ownership handles the free; this is the identity copy.
pub fn xml_pstrdup_and_free(str: Option<&[u8]>) -> Option<Vec<u8>> {
    str.map(|s| s.to_vec())
}

/// `find_subslice` — index of the first occurrence of `needle` in `hay`.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// C `PG_XMLISNAMECHAR(c)` (xml.c:1366) — the macro combining the libxml
/// single-byte / range predicates that
/// `parse_xml_decl` uses to classify the decoded codepoint after `<?xml`:
///
/// ```c
/// #define PG_XMLISNAMECHAR(c) ///     (xmlIsBaseChar_ch(c) || xmlIsIdeographicQ(c) ///             || xmlIsDigit_ch(c) ///             || c == '.' || c == '-' || c == '_' || c == ':' ///             || xmlIsCombiningQ(c) ///             || xmlIsExtender_ch(c))
/// ```
///
/// `c` is libxml's `xmlGetUTF8Char` result (an `int`; negative on a decode
/// error, in which case no class matches). The `_ch` forms only consult the
/// single-byte (< 0x100) part of their class, exactly as the macro does.
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

/// C `parse_xml_decl` (xml.c:1433, static).
///
/// `str` is the NUL-terminated input. `lenp`/`version`/`encoding`/`standalone`
/// are optional outputs. Returns 0 (`XML_ERR_OK`) on success or a libxml error
/// code. The remaining libxml legs (`xmlGetUTF8Char`, `xmlIsBlank_ch`) are
/// reached through the seam; the name-char classification is the in-crate
/// [`pg_xml_is_name_char`].
// `as_deref_mut` is used to reborrow the optional output references that are
// written to in several branches; without it the first use would move them.
#[allow(clippy::needless_option_as_deref)]
pub fn parse_xml_decl(
    str: &[u8],
    mut lenp: Option<&mut usize>,
    mut version: Option<&mut Option<Vec<u8>>>,
    mut encoding: Option<&mut Option<Vec<u8>>>,
    mut standalone: Option<&mut i32>,
) -> PgResult<i32> {
    // Only initialize libxml; no error handling required here.
    pg_xml_init_library();

    // Initialize output arguments to "not present".
    if let Some(v) = version.as_deref_mut() {
        *v = None;
    }
    if let Some(e) = encoding.as_deref_mut() {
        *e = None;
    }
    if let Some(s) = standalone.as_deref_mut() {
        *s = -1;
    }

    // byte index into str
    let mut p: usize = 0;

    // helper closures over `str`
    let at = |i: usize| -> u8 { *str.get(i).unwrap_or(&0) };

    // if (xmlStrncmp(p, "<?xml", 5) != 0) goto finished;
    let mut goto_finished = false;
    if !starts_with_at(str, p, b"<?xml") {
        goto_finished = true;
    }

    if !goto_finished {
        // Determine whether next char is a name char (=> a PI, not an XMLDecl).
        let after = &str[(p + 5).min(str.len())..];
        let utf8len = strnlen(after, MAX_MULTIBYTE_CHAR_LEN);
        let utf8char = seam::get_utf8_char::call(&after[..utf8len.min(after.len())])?;
        if pg_xml_is_name_char(utf8char) {
            goto_finished = true;
        }
    }

    if !goto_finished {
        p += 5;

        // version
        if !seam::is_blank_ch::call(at(p))? {
            return Ok(XML_ERR_SPACE_REQUIRED);
        }
        skip_xml_space(str, &mut p)?;
        if !starts_with_at(str, p, b"version") {
            return Ok(XML_ERR_VERSION_MISSING);
        }
        p += 7;
        skip_xml_space(str, &mut p)?;
        if at(p) != b'=' {
            return Ok(XML_ERR_VERSION_MISSING);
        }
        p += 1;
        skip_xml_space(str, &mut p)?;

        if at(p) == b'\'' || at(p) == b'"' {
            let quote = at(p);
            match memchr_from(str, p + 1, quote) {
                None => return Ok(XML_ERR_VERSION_MISSING),
                Some(q) => {
                    if let Some(v) = version.as_deref_mut() {
                        *v = Some(xml_pnstrdup(&str[p + 1..q]));
                    }
                    p = q + 1;
                }
            }
        } else {
            return Ok(XML_ERR_VERSION_MISSING);
        }

        // encoding
        let save_p = p;
        skip_xml_space(str, &mut p)?;
        if starts_with_at(str, p, b"encoding") {
            if !seam::is_blank_ch::call(at(save_p))? {
                return Ok(XML_ERR_SPACE_REQUIRED);
            }
            p += 8;
            skip_xml_space(str, &mut p)?;
            if at(p) != b'=' {
                return Ok(XML_ERR_MISSING_ENCODING);
            }
            p += 1;
            skip_xml_space(str, &mut p)?;

            if at(p) == b'\'' || at(p) == b'"' {
                let quote = at(p);
                match memchr_from(str, p + 1, quote) {
                    None => return Ok(XML_ERR_MISSING_ENCODING),
                    Some(q) => {
                        if let Some(e) = encoding.as_deref_mut() {
                            *e = Some(xml_pnstrdup(&str[p + 1..q]));
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

        // standalone
        let save_p = p;
        skip_xml_space(str, &mut p)?;
        if starts_with_at(str, p, b"standalone") {
            if !seam::is_blank_ch::call(at(save_p))? {
                return Ok(XML_ERR_SPACE_REQUIRED);
            }
            p += 10;
            skip_xml_space(str, &mut p)?;
            if at(p) != b'=' {
                return Ok(XML_ERR_STANDALONE_VALUE);
            }
            p += 1;
            skip_xml_space(str, &mut p)?;
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

        skip_xml_space(str, &mut p)?;
        if !starts_with_at(str, p, b"?>") {
            return Ok(XML_ERR_XMLDECL_NOT_FINISHED);
        }
        p += 2;
    }

    // finished:
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

/// C `SKIP_XML_SPACE(p)` — advance `p` over XML whitespace.
fn skip_xml_space(str: &[u8], p: &mut usize) -> PgResult<()> {
    while seam::is_blank_ch::call(*str.get(*p).unwrap_or(&0))? {
        *p += 1;
    }
    Ok(())
}

fn starts_with_at(str: &[u8], p: usize, needle: &[u8]) -> bool {
    str.get(p..p + needle.len())
        .map(|s| s == needle)
        .unwrap_or(false)
}

fn memchr_from(str: &[u8], from: usize, byte: u8) -> Option<usize> {
    str.get(from..)
        .and_then(|s| s.iter().position(|&b| b == byte).map(|i| from + i))
        .filter(|&i| str.get(i) == Some(&byte))
}

fn strnlen(s: &[u8], maxlen: usize) -> usize {
    let mut n = 0;
    while n < maxlen && n < s.len() && s[n] != 0 {
        n += 1;
    }
    n
}

/// C `print_xml_decl` (xml.c:1606, static). `encoding` is a `pg_enc`.
pub fn print_xml_decl(
    buf: &mut Vec<u8>,
    version: Option<&[u8]>,
    encoding: i32,
    standalone: i32,
) -> bool {
    let version_nondefault = version
        .map(|v| v != PG_XML_DEFAULT_VERSION.as_bytes())
        .unwrap_or(false);

    if version_nondefault || (encoding != 0 && encoding != PG_UTF8) || standalone != -1 {
        buf.extend_from_slice(b"<?xml");

        match version {
            Some(v) => {
                buf.extend_from_slice(b" version=\"");
                buf.extend_from_slice(v);
                buf.push(b'"');
            }
            None => {
                buf.extend_from_slice(b" version=\"");
                buf.extend_from_slice(PG_XML_DEFAULT_VERSION.as_bytes());
                buf.push(b'"');
            }
        }

        if encoding != 0 && encoding != PG_UTF8 {
            buf.extend_from_slice(b" encoding=\"");
            buf.extend_from_slice(
                common_encnames_seams::pg_encoding_to_char::call(encoding).as_bytes(),
            );
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

/// C `xml_doctype_in_content` (xml.c:1672, static) — true if CONTENT input is
/// led by a DTD (`<!DOCTYPE`), through valid whitespace/comments/PIs.
///
/// This pure-logic helper is ported in full for libxml-provider reuse. In C it
/// is called from inside `xml_parse`'s `#ifdef USE_LIBXML` body to implement the
/// SQL/XML:2006+ behavior of treating CONTENT input whose first node is a
/// DOCTYPE as a DOCUMENT. Because the whole libxml parse lives in the libxml
/// provider, a faithful provider's `xml_parse_libxml` must invoke this function
/// on CONTENT inputs to preserve that DOCTYPE-in-content detection; the
/// fail-safe configuration never parses, so it never reaches this path.
pub fn xml_doctype_in_content(str: &[u8]) -> PgResult<bool> {
    let mut p: usize = 0;
    let at = |i: usize| -> u8 { *str.get(i).unwrap_or(&0) };

    loop {
        skip_xml_space(str, &mut p)?;
        if at(p) != b'<' {
            return Ok(false);
        }
        p += 1;

        if at(p) == b'!' {
            p += 1;

            if starts_with_at(str, p, b"DOCTYPE") {
                return Ok(true);
            }

            if !starts_with_at(str, p, b"--") {
                return Ok(false);
            }
            // find end of comment: -- followed by >
            match find_subslice(&str[(p + 2).min(str.len())..], b"--") {
                None => return Ok(false),
                Some(off) => {
                    let pos = p + 2 + off;
                    if at(pos + 2) != b'>' {
                        return Ok(false);
                    }
                    p = pos + 3;
                    continue;
                }
            }
        }

        if at(p) != b'?' {
            return Ok(false);
        }
        p += 1;

        match find_subslice(&str[p.min(str.len())..], b"?>") {
            None => return Ok(false),
            Some(off) => {
                p = p + off + 2;
            }
        }
    }
}

/// C `xml_parse` (xml.c:1748, static) — parse + validate via libxml.
///
/// Returns `Ok(true)` on success (well-formed). Hard parse failures surface as
/// `Err`. The whole libxml parse lives in the provider.
pub fn xml_parse(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
    encoding: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    // The provider returns `Ok(Err(soft))` for a malformed parse and a genuine
    // hard `Err` only for non-recoverable failures (OOM). A malformed parse is
    // routed through `ereturn`: with a soft context it is saved and `false` is
    // returned (C: the soft `ereturn` makes the caller see `doc == NULL`); with
    // no context it propagates as a hard error, matching the C call sites that
    // pass escontext == NULL.
    match seam::xml_parse_libxml::call(data, xmloption_arg, preserve_whitespace, encoding)? {
        Ok(b) => Ok(b),
        Err(soft) => ereturn(escontext, false, soft),
    }
}

/// C `xml_text2xmlChar` (xml.c:1933, static) — `text_to_cstring(in)` as bytes.
pub fn xml_text2xmlChar(in_: &[u8]) -> Vec<u8> {
    in_.to_vec()
}

// ===========================================================================
// SQL identifier <-> XML name mapping
// ===========================================================================

/// C `sqlchar_to_unicode` (xml.c:2336, static) — codepoint of one server char.
///
/// The full conversion (server→UTF-8 then UTF-8→codepoint, so the result is a
/// *Unicode* codepoint even for non-UTF-8 server encodings) needs the mb
/// subsystem plus the catalog-resident encoding conversion procs, so it crosses
/// [`seam::sqlchar_to_unicode`].
pub fn sqlchar_to_unicode(s: &[u8]) -> PgResult<u32> {
    seam::sqlchar_to_unicode::call(s)
}

/// C `is_valid_xml_namefirst` (xml.c:2353, static) — `(Letter | '_' | ':')`,
/// over the fixed libxml2 [`chvalid`] range tables (ported in-crate).
pub fn is_valid_xml_namefirst(c: u32) -> PgResult<bool> {
    /* (Letter | '_' | ':') */
    Ok(chvalid::xml_is_base_char_q(c)
        || chvalid::xml_is_ideographic_q(c)
        || c == '_' as u32
        || c == ':' as u32)
}

/// C `is_valid_xml_namechar` (xml.c:2362, static) — the SQL/XML 9.1 name-char
/// predicate, over the fixed libxml2 [`chvalid`] range tables (ported
/// in-crate).
pub fn is_valid_xml_namechar(c: u32) -> PgResult<bool> {
    /* Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender */
    Ok(chvalid::xml_is_base_char_q(c)
        || chvalid::xml_is_ideographic_q(c)
        || chvalid::xml_is_digit_q(c)
        || c == '.' as u32
        || c == '-' as u32
        || c == '_' as u32
        || c == ':' as u32
        || chvalid::xml_is_combining_q(c)
        || chvalid::xml_is_extender_q(c))
}

/// C `map_sql_identifier_to_xml_name` (xml.c:2379) — SQL/XML:2008 section 9.1.
pub fn map_sql_identifier_to_xml_name(
    ident: &[u8],
    fully_escaped: bool,
    escape_period: bool,
) -> PgResult<String> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }

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
            && ident[p..].len() >= 3
            && ident[p..p + 3].eq_ignore_ascii_case(b"xml")
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
            let mblen = seam::pg_mblen::call(&ident[p..])? as usize;
            let u = sqlchar_to_unicode(&ident[p..])?;
            let valid = if p == 0 {
                is_valid_xml_namefirst(u)?
            } else {
                is_valid_xml_namechar(u)?
            };
            if !valid {
                buf.extend_from_slice(format!("_x{u:04X}_").as_bytes());
            } else {
                buf.extend_from_slice(&ident[p..p + mblen.min(ident.len() - p)]);
            }
            p += mblen.max(1);
        }
    }

    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// C `map_xml_name_to_sql_identifier` (xml.c:2435) — SQL/XML:2008 section 9.3.
pub fn map_xml_name_to_sql_identifier(name: &[u8]) -> PgResult<String> {
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
            // sscanf(p + 2, "%X", &u)
            let hex = &name[p + 2..p + 6];
            let u = u32::from_str_radix(&String::from_utf8_lossy(hex), 16).unwrap_or(0);
            let bytes = seam::unicode_to_server::call(u)?;
            buf.extend_from_slice(&bytes);
            p += 6;
            p += 1; // for-loop's p += pg_mblen; here the '_' is a single byte
        } else {
            let mblen = seam::pg_mblen::call(&name[p..])? as usize;
            buf.extend_from_slice(&name[p..(p + mblen).min(n)]);
            p += mblen.max(1);
        }
    }

    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// C `map_sql_value_to_xml_value` (xml.c:2477) — SQL/XML:2008 section 9.8.
///
/// `value` is the raw Datum, `type_` its OID. The array/array-domain branch, the
/// `BOOLOID` literal `"true"`/`"false"` arm, and the DATE/TIMESTAMP/TIMESTAMPTZ
/// XSD formatting are all ported in-crate; only the genuinely-external call-outs
/// are seamed: `get_base_element_type` + `deconstruct_array` for the array
/// branch, `getBaseType` / `getTypeOutputInfo`+`OidOutputFunctionCall` for the
/// native text path, and `detoast_bytea` + `encode_binary` for `BYTEAOID`.
pub fn map_sql_value_to_xml_value(
    value: Datum,
    type_: Oid,
    xml_escape_strings: bool,
) -> PgResult<String> {
    // type_is_array_domain(type) == (get_base_element_type(type) != InvalidOid):
    // both plain arrays and domains over arrays.
    if lsc::get_base_element_type::call(type_)? != InvalidOid {
        // array = DatumGetArrayTypeP(value); elmtype = ARR_ELEMTYPE(array);
        // get_typlenbyvalalign(elmtype, ...);
        // deconstruct_array(array, elmtype, ..., &elem_values, &elem_nulls,
        //                   &num_elems);
        let (elmtype, elems) = seam::deconstruct_array::call(datum_bits(value))?;

        let mut buf: Vec<u8> = Vec::new();
        for elem in &elems {
            match elem {
                None => continue, // if (elem_nulls[i]) continue;
                Some(bits) => {
                    buf.extend_from_slice(b"<element>");
                    let mapped =
                        map_sql_value_to_xml_value(Datum::from_usize(*bits as usize), elmtype, true)?;
                    buf.extend_from_slice(mapped.as_bytes());
                    buf.extend_from_slice(b"</element>");
                }
            }
        }

        return Ok(String::from_utf8_lossy(&buf).into_owned());
    }

    // Flatten domains; the special-case treatments below should apply to, eg,
    // domains over boolean not just boolean.
    let type_ = lsc::get_base_type::call(type_)?;

    // Special XSD formatting for some data types.
    match type_ {
        BOOLOID => {
            // DatumGetBool(value) ? "true" : "false"
            if datum_get_bool(value) {
                return Ok("true".to_string());
            } else {
                return Ok("false".to_string());
            }
        }
        DATEOID => {
            let date: DateADT = datum_get_date_adt(value);
            // XSD doesn't support infinite values.
            if backend_utils_adt_datetime::date::DATE_NOT_FINITE(date) {
                return Err(PgError::error("date out of range")
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .with_detail("XML does not support infinite date values."));
            }
            let mut tm = pg_tm::default();
            let (y, mo, d) = j2date(date + POSTGRES_EPOCH_JDATE);
            tm.tm_year = y;
            tm.tm_mon = mo;
            tm.tm_mday = d;
            let mut buf = String::new();
            EncodeDateOnly(&tm, USE_XSD_DATES, &mut buf);
            return Ok(buf);
        }
        TIMESTAMPOID => {
            let timestamp: Timestamp = datum_get_timestamp(value);
            // XSD doesn't support infinite values.
            if TIMESTAMP_NOT_FINITE(timestamp) {
                return Err(PgError::error("timestamp out of range")
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .with_detail("XML does not support infinite timestamp values."));
            }
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
                let mut buf = String::new();
                EncodeDateTime(&mut tm, fsec, false, 0, None, USE_XSD_DATES, &mut buf);
                return Ok(buf);
            } else {
                return Err(PgError::error("timestamp out of range")
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
            }
        }
        TIMESTAMPTZOID => {
            let timestamp: Timestamp = datum_get_timestamp(value);
            // XSD doesn't support infinite values.
            if TIMESTAMP_NOT_FINITE(timestamp) {
                return Err(PgError::error("timestamp out of range")
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .with_detail("XML does not support infinite timestamp values."));
            }
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            let mut tz: i32 = 0;
            let mut tzn: Option<String> = None;
            if timestamp2tm(
                timestamp,
                Some(&mut tz),
                &mut tm,
                &mut fsec,
                Some(&mut tzn),
                None,
            )
            .is_ok()
            {
                let mut buf = String::new();
                EncodeDateTime(
                    &mut tm,
                    fsec,
                    true,
                    tz,
                    tzn.as_deref(),
                    USE_XSD_DATES,
                    &mut buf,
                );
                return Ok(buf);
            } else {
                return Err(PgError::error("timestamp out of range")
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
            }
        }
        BYTEAOID => {
            // #ifdef USE_LIBXML — base64/binhex over the *raw* bytea payload.
            if !seam::have_libxml::call() {
                return Err(no_xml_support());
            }
            // bstr = DatumGetByteaPP(value); the writer consumes VARDATA_ANY(bstr)
            // for VARSIZE_ANY_EXHDR(bstr) bytes (the raw payload, not the text rep).
            let raw = seam::detoast_bytea::call(datum_bits(value))?;
            return seam::encode_binary::call(&raw, seam::xmlbinary::call());
        }
        _ => {}
    }

    // otherwise, just use the type's native text representation
    // getTypeOutputInfo(type, &typeOut, &isvarlena);
    // str = OidOutputFunctionCall(typeOut, value);
    let str = seam::output_function_call::call(type_, datum_bits(value))?;

    // ... exactly as-is for XML, and when escaping is not wanted
    if type_ == XMLOID || !xml_escape_strings {
        Ok(str)
    } else {
        // otherwise, translate special characters as needed
        Ok(String::from_utf8_lossy(&escape_xml(str.as_bytes())).into_owned())
    }
}

/// `map_sql_value_to_xml_value(value, type, xml_escape_strings)` (xml.c:2562)
/// over the unified value carrier ([`TDatum`]), the form the executor
/// (`ExecEvalXmlExpr` for XMLELEMENT/XMLFOREST/XMLCONCAT content) and
/// `query_to_xml` row mapping hold their column values in.
///
/// This is the same algorithm as [`map_sql_value_to_xml_value`] but reaches the
/// genuinely-external call-outs through their **installed** real seams instead
/// of the bare-machine-word `xml`-local seams, which cannot represent a
/// by-reference value: the array branch uses `deconstruct_array_v`, the native
/// text path uses `getTypeOutputInfo` + `OidOutputFunctionCall` over the full
/// `Datum<'mcx>` carrier, and `bytea` consumes the (already detoasted,
/// header-less) `ByRef` payload directly. By-value scalars (bool/date/timestamp)
/// are read off the carrier's machine word exactly as the bare-word form does.
pub fn map_sql_value_to_xml_value_v<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    value: TDatum<'mcx>,
    type_: Oid,
    xml_escape_strings: bool,
) -> PgResult<String> {
    // type_is_array_domain(type): plain arrays and domains over arrays.
    let elmtype = lsc::get_base_element_type::call(type_)?;
    if elmtype != InvalidOid {
        // get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
        let tla = lsc::get_typlenbyvalalign::call(elmtype)?;
        // deconstruct_array(array, elmtype, ..., &elem_values, &elem_nulls, &n);
        let elems = backend_utils_adt_arrayfuncs_seams::deconstruct_array_v::call(
            mcx,
            value,
            elmtype,
            tla.typlen,
            tla.typbyval,
            tla.typalign as core::ffi::c_char,
        )?;

        let mut buf: Vec<u8> = Vec::new();
        for (elem, isnull) in elems.iter() {
            if *isnull {
                continue; // if (elem_nulls[i]) continue;
            }
            buf.extend_from_slice(b"<element>");
            let mapped = map_sql_value_to_xml_value_v(mcx, elem.clone_in(mcx)?, elmtype, true)?;
            buf.extend_from_slice(mapped.as_bytes());
            buf.extend_from_slice(b"</element>");
        }
        return Ok(String::from_utf8_lossy(&buf).into_owned());
    }

    // Flatten domains; the special-case treatments below should apply to, eg,
    // domains over boolean not just boolean.
    let type_ = lsc::get_base_type::call(type_)?;

    // Special XSD formatting for some data types. bool/date/timestamp[tz] are
    // by-value: read the machine word off the carrier and reuse the bare-word
    // formatter (identical XSD output). bytea is by-reference.
    match type_ {
        BOOLOID | DATEOID | TIMESTAMPOID | TIMESTAMPTZOID => {
            let word = Datum::from_usize(value.as_usize());
            return map_sql_value_to_xml_value(word, type_, xml_escape_strings);
        }
        BYTEAOID => {
            // #ifdef USE_LIBXML — base64/binhex over the *raw* bytea payload.
            if !seam::have_libxml::call() {
                return Err(no_xml_support());
            }
            // The carrier is a header-ful varlena image (the canonical by-ref
            // Datum convention); VARDATA_ANY(bstr) is the payload past the 4-byte
            // header, which is exactly what the C path base64/binhex-encodes.
            let img = value.as_ref_bytes();
            let payload = &img[types_datum::varlena::VARHDRSZ.min(img.len())..];
            return seam::encode_binary::call(payload, seam::xmlbinary::call());
        }
        _ => {}
    }

    // otherwise, just use the type's native text representation:
    //   getTypeOutputInfo(type, &typeOut, &isvarlena);
    //   str = OidOutputFunctionCall(typeOut, value);
    let (typeout, _isvarlena) = lsc::get_type_output_info::call(type_)?;
    let str =
        backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(mcx, typeout, value)?;
    let str = str.as_str().to_owned();

    // ... exactly as-is for XML, and when escaping is not wanted.
    if type_ == XMLOID || !xml_escape_strings {
        Ok(str)
    } else {
        // otherwise, translate special characters as needed
        Ok(String::from_utf8_lossy(&escape_xml(str.as_bytes())).into_owned())
    }
}

/// Extract the raw machine-word Datum payload as the C `Datum` bits.
fn datum_bits(value: Datum) -> u64 {
    value.as_usize() as u64
}

/// C `DatumGetBool(X)` — `X != 0`.
fn datum_get_bool(value: Datum) -> bool {
    value.as_usize() != 0
}

/// C `DatumGetDateADT(X)` == `DatumGetInt32(X)` — the low 32 Datum bits.
fn datum_get_date_adt(value: Datum) -> DateADT {
    value.as_u32() as i32
}

/// C `DatumGetTimestamp(X)` == `DatumGetInt64(X)` — the Datum bits as i64.
fn datum_get_timestamp(value: Datum) -> Timestamp {
    value.as_usize() as u64 as i64
}

/// C `escape_xml` (xml.c:2696) — XML-escape a C string into a fresh buffer.
/// NB: intentionally not dependent on libxml.
///
/// Operates on bytes exactly like the C original: only `&`, `<`, `>`, and `\r`
/// are escaped; every other byte (including the continuation bytes of a
/// multibyte character) is copied verbatim, so non-ASCII input is preserved
/// byte-for-byte.
pub fn escape_xml(str: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
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

// ===========================================================================
// SQL -> XML mapping: visibility helpers (SPI / catalog)
// ===========================================================================

/// C `query_to_oid_list` (xml.c:2785, static) — run `query` via SPI, collect the
/// first column as a list of OIDs.
pub fn query_to_oid_list(query: &str) -> PgResult<Vec<Oid>> {
    let result = seam::spi_execute_select::call(query)?;
    let mut list = Vec::new();
    for row in &result.rows {
        if let Some(Some(s)) = row.first() {
            if let Ok(oid) = s.parse::<u32>() {
                list.push(oid as Oid);
            }
        }
    }
    Ok(list)
}

const RELKIND_RELATION: char = 'r';
const RELKIND_MATVIEW: char = 'm';
const RELKIND_VIEW: char = 'v';

const XML_VISIBLE_SCHEMAS_EXCLUDE: &str = "(nspname ~ '^pg_' OR nspname = 'information_schema')";

fn xml_visible_schemas() -> String {
    format!(
        "SELECT oid FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT {XML_VISIBLE_SCHEMAS_EXCLUDE}"
    )
}

/// C `schema_get_xml_visible_tables` (xml.c:2814, static).
pub fn schema_get_xml_visible_tables(nspid: Oid) -> PgResult<Vec<Oid>> {
    let query = format!(
        "SELECT oid FROM pg_catalog.pg_class WHERE relnamespace = {} AND relkind IN ('{}','{}','{}') AND pg_catalog.has_table_privilege (oid, 'SELECT') ORDER BY relname;",
        nspid, RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_VIEW
    );
    query_to_oid_list(&query)
}

/// C `database_get_xml_visible_schemas` (xml.c:2841, static).
pub fn database_get_xml_visible_schemas() -> PgResult<Vec<Oid>> {
    query_to_oid_list(&format!("{} ORDER BY nspname;", xml_visible_schemas()))
}

/// C `database_get_xml_visible_tables` (xml.c:2848, static).
pub fn database_get_xml_visible_tables() -> PgResult<Vec<Oid>> {
    let query = format!(
        "SELECT oid FROM pg_catalog.pg_class WHERE relkind IN ('{}','{}','{}') AND pg_catalog.has_table_privilege(pg_class.oid, 'SELECT') AND relnamespace IN ({});",
        RELKIND_RELATION,
        RELKIND_MATVIEW,
        RELKIND_VIEW,
        xml_visible_schemas()
    );
    query_to_oid_list(&query)
}

// ===========================================================================
// SQL -> XML mapping: data publishing
// ===========================================================================

/// C `xmldata_root_element_start` (xml.c:2966, static).
pub fn xmldata_root_element_start(
    result: &mut Vec<u8>,
    eltname: &str,
    xmlschema: Option<&str>,
    targetns: &str,
    top_level: bool,
) {
    debug_assert!(top_level || xmlschema.is_none());

    result.extend_from_slice(format!("<{eltname}").as_bytes());
    if top_level {
        result.extend_from_slice(format!(" xmlns:xsi=\"{NAMESPACE_XSI}\"").as_bytes());
        if !targetns.is_empty() {
            result.extend_from_slice(format!(" xmlns=\"{targetns}\"").as_bytes());
        }
    }
    if xmlschema.is_some() {
        if !targetns.is_empty() {
            result.extend_from_slice(format!(" xsi:schemaLocation=\"{targetns} #\"").as_bytes());
        } else {
            result.extend_from_slice(b" xsi:noNamespaceSchemaLocation=\"#\"");
        }
    }
    result.extend_from_slice(b">\n");
}

/// C `xmldata_root_element_end` (xml.c:2993, static).
pub fn xmldata_root_element_end(result: &mut Vec<u8>, eltname: &str) {
    result.extend_from_slice(format!("</{eltname}>\n").as_bytes());
}

/// C `query_to_xml_internal` (xml.c:3000, static).
pub fn query_to_xml_internal(
    query: &str,
    tablename: Option<&str>,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<Vec<u8>> {
    let xmltn = match tablename {
        Some(t) => map_sql_identifier_to_xml_name(t.as_bytes(), true, false)?,
        None => "table".to_string(),
    };

    let mut result: Vec<u8> = Vec::new();

    let exec = seam::spi_execute_select::call(query)
        .map_err(|_| PgError::error("invalid query").with_sqlstate(ERRCODE_DATA_EXCEPTION))?;

    if !tableforest {
        xmldata_root_element_start(&mut result, &xmltn, xmlschema, targetns, top_level);
        result.push(b'\n');
    }

    if let Some(schema) = xmlschema {
        result.extend_from_slice(format!("{schema}\n\n").as_bytes());
    }

    for i in 0..exec.rows.len() {
        SPI_sql_row_to_xmlelement(
            i as u64,
            &mut result,
            &exec,
            tablename,
            nulls,
            tableforest,
            targetns,
            top_level,
        )?;
    }

    if !tableforest {
        xmldata_root_element_end(&mut result, &xmltn);
    }

    Ok(result)
}

/// C `table_to_xml_internal` (xml.c:2867, static).
pub fn table_to_xml_internal(
    relid: Oid,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<Vec<u8>> {
    let relname = seam::get_rel_name::call(relid)?;
    let query = format!("SELECT * FROM {relname}");
    query_to_xml_internal(
        &query,
        Some(&relname),
        xmlschema,
        nulls,
        tableforest,
        targetns,
        top_level,
    )
}

/// C `table_to_xml` (xml.c:2883).
pub fn table_to_xml(relid: Oid, nulls: bool, tableforest: bool, targetns: &str) -> PgResult<Vec<u8>> {
    let si = table_to_xml_internal(relid, None, nulls, tableforest, targetns, true)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `query_to_xml` (xml.c:2897).
pub fn query_to_xml(query: &str, nulls: bool, tableforest: bool, targetns: &str) -> PgResult<Vec<u8>> {
    let si = query_to_xml_internal(query, None, None, nulls, tableforest, targetns, true)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `cursor_to_xml` (xml.c:2911).
pub fn cursor_to_xml(
    name: &str,
    count: i32,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let mut result: Vec<u8> = Vec::new();

    if !tableforest {
        xmldata_root_element_start(&mut result, "table", None, targetns, true);
        result.push(b'\n');
    }

    let exec = seam::spi_cursor_fetch::call(name, count)?;
    for i in 0..exec.rows.len() {
        SPI_sql_row_to_xmlelement(
            i as u64,
            &mut result,
            &exec,
            None,
            nulls,
            tableforest,
            targetns,
            true,
        )?;
    }

    if !tableforest {
        xmldata_root_element_end(&mut result, "table");
    }

    Ok(stringinfo_to_xmltype(&result))
}

/// C `table_to_xmlschema` (xml.c:3044).
pub fn table_to_xmlschema(
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let columns = seam::relation_columns::call(relid)?;
    let result = map_sql_table_to_xmlschema(&columns, relid, nulls, tableforest, targetns)?;
    Ok(cstring_to_xmltype(&result))
}

/// C `query_to_xmlschema` (xml.c:3063).
pub fn query_to_xmlschema(
    query: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let columns = seam::spi_query_tupdesc::call(query)?;
    let cols = spi_columns_to_relation_columns(&columns);
    let result = map_sql_table_to_xmlschema(&cols, InvalidOid, nulls, tableforest, targetns)?;
    Ok(cstring_to_xmltype(&result))
}

/// C `cursor_to_xmlschema` (xml.c:3092).
pub fn cursor_to_xmlschema(
    name: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let columns = seam::spi_cursor_tupdesc::call(name)?;
    let cols = spi_columns_to_relation_columns(&columns);
    let result = map_sql_table_to_xmlschema(&cols, InvalidOid, nulls, tableforest, targetns)?;
    Ok(cstring_to_xmltype(&result))
}

/// C `table_to_xml_and_xmlschema` (xml.c:3122).
pub fn table_to_xml_and_xmlschema(
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let columns = seam::relation_columns::call(relid)?;
    let xmlschema = map_sql_table_to_xmlschema(&columns, relid, nulls, tableforest, targetns)?;
    let si = table_to_xml_internal(relid, Some(&xmlschema), nulls, tableforest, targetns, true)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `query_to_xml_and_xmlschema` (xml.c:3144).
pub fn query_to_xml_and_xmlschema(
    query: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let columns = seam::spi_query_tupdesc::call(query)?;
    let cols = spi_columns_to_relation_columns(&columns);
    let xmlschema = map_sql_table_to_xmlschema(&cols, InvalidOid, nulls, tableforest, targetns)?;
    let si = query_to_xml_internal(
        query,
        None,
        Some(&xmlschema),
        nulls,
        tableforest,
        targetns,
        true,
    )?;
    Ok(stringinfo_to_xmltype(&si))
}

fn spi_columns_to_relation_columns(columns: &[SpiColumn]) -> Vec<RelationColumn> {
    columns
        .iter()
        .map(|c| RelationColumn {
            attname: c.name.clone(),
            atttypid: c.typeid,
            is_dropped: c.is_dropped,
        })
        .collect()
}

/// C `schema_to_xml_internal` (xml.c:3180, static).
pub fn schema_to_xml_internal(
    nspid: Oid,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<Vec<u8>> {
    let nspname = seam::get_namespace_name::call(nspid)?;
    let xmlsn = map_sql_identifier_to_xml_name(nspname.as_bytes(), true, false)?;
    let mut result: Vec<u8> = Vec::new();

    xmldata_root_element_start(&mut result, &xmlsn, xmlschema, targetns, top_level);
    result.push(b'\n');

    if let Some(schema) = xmlschema {
        result.extend_from_slice(format!("{schema}\n\n").as_bytes());
    }

    let relid_list = schema_get_xml_visible_tables(nspid)?;

    for relid in relid_list {
        let subres = table_to_xml_internal(relid, None, nulls, tableforest, targetns, false)?;
        result.extend_from_slice(&subres);
        result.push(b'\n');
    }

    xmldata_root_element_end(&mut result, &xmlsn);

    Ok(result)
}

/// C `schema_to_xml` (xml.c:3222).
pub fn schema_to_xml(name: &str, nulls: bool, tableforest: bool, targetns: &str) -> PgResult<Vec<u8>> {
    let nspid = seam::lookup_namespace::call(name)?;
    let si = schema_to_xml_internal(nspid, None, nulls, tableforest, targetns, true)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `xsd_schema_element_start` (xml.c:3245, static).
pub fn xsd_schema_element_start(result: &mut Vec<u8>, targetns: &str) {
    result.extend_from_slice(format!("<xsd:schema\n    xmlns:xsd=\"{NAMESPACE_XSD}\"").as_bytes());
    if !targetns.is_empty() {
        result.extend_from_slice(
            format!("\n    targetNamespace=\"{targetns}\"\n    elementFormDefault=\"qualified\"")
                .as_bytes(),
        );
    }
    result.extend_from_slice(b">\n\n");
}

/// C `xsd_schema_element_end` (xml.c:3262, static).
pub fn xsd_schema_element_end(result: &mut Vec<u8>) {
    result.extend_from_slice(b"</xsd:schema>");
}

/// C `schema_to_xmlschema_internal` (xml.c:3269, static).
pub fn schema_to_xmlschema_internal(
    schemaname: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let mut result: Vec<u8> = Vec::new();

    let nspid = seam::lookup_namespace::call(schemaname)?;

    xsd_schema_element_start(&mut result, targetns);

    let relid_list = schema_get_xml_visible_tables(nspid)?;

    // CreateTupleDescCopy(rel->rd_att) for each relation.
    let mut tupdesc_list: Vec<Vec<RelationColumn>> = Vec::new();
    for &relid in &relid_list {
        tupdesc_list.push(seam::relation_columns::call(relid)?);
    }

    result.extend_from_slice(map_sql_typecoll_to_xmlschema_types(&tupdesc_list)?.as_bytes());
    result.extend_from_slice(
        map_sql_schema_to_xmlschema_types(nspid, &relid_list, nulls, tableforest, targetns)?
            .as_bytes(),
    );

    xsd_schema_element_end(&mut result);

    Ok(result)
}

/// C `schema_to_xmlschema` (xml.c:3314).
pub fn schema_to_xmlschema(
    name: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let si = schema_to_xmlschema_internal(name, nulls, tableforest, targetns)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `schema_to_xml_and_xmlschema` (xml.c:3327).
pub fn schema_to_xml_and_xmlschema(
    name: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let nspid = seam::lookup_namespace::call(name)?;
    let xmlschema = schema_to_xmlschema_internal(name, nulls, tableforest, targetns)?;
    let xmlschema_str = String::from_utf8_lossy(&xmlschema).into_owned();
    let si = schema_to_xml_internal(
        nspid,
        Some(&xmlschema_str),
        nulls,
        tableforest,
        targetns,
        true,
    )?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `database_to_xml_internal` (xml.c:3355, static).
pub fn database_to_xml_internal(
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let dbname = seam::get_database_name::call()?;
    let xmlcn = map_sql_identifier_to_xml_name(dbname.as_bytes(), true, false)?;
    let mut result: Vec<u8> = Vec::new();

    xmldata_root_element_start(&mut result, &xmlcn, xmlschema, targetns, true);
    result.push(b'\n');

    if let Some(schema) = xmlschema {
        result.extend_from_slice(format!("{schema}\n\n").as_bytes());
    }

    let nspid_list = database_get_xml_visible_schemas()?;

    for nspid in nspid_list {
        let subres = schema_to_xml_internal(nspid, None, nulls, tableforest, targetns, false)?;
        result.extend_from_slice(&subres);
        result.push(b'\n');
    }

    xmldata_root_element_end(&mut result, &xmlcn);

    Ok(result)
}

/// C `database_to_xml` (xml.c:3397).
pub fn database_to_xml(nulls: bool, tableforest: bool, targetns: &str) -> PgResult<Vec<u8>> {
    let si = database_to_xml_internal(None, nulls, tableforest, targetns)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `database_to_xmlschema_internal` (xml.c:3410, static).
pub fn database_to_xmlschema_internal(
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let mut result: Vec<u8> = Vec::new();

    xsd_schema_element_start(&mut result, targetns);

    let relid_list = database_get_xml_visible_tables()?;
    let nspid_list = database_get_xml_visible_schemas()?;

    let mut tupdesc_list: Vec<Vec<RelationColumn>> = Vec::new();
    for &relid in &relid_list {
        tupdesc_list.push(seam::relation_columns::call(relid)?);
    }

    result.extend_from_slice(map_sql_typecoll_to_xmlschema_types(&tupdesc_list)?.as_bytes());
    result.extend_from_slice(
        map_sql_catalog_to_xmlschema_types(&nspid_list, nulls, tableforest, targetns)?.as_bytes(),
    );

    xsd_schema_element_end(&mut result);

    Ok(result)
}

/// C `database_to_xmlschema` (xml.c:3453).
pub fn database_to_xmlschema(nulls: bool, tableforest: bool, targetns: &str) -> PgResult<Vec<u8>> {
    let si = database_to_xmlschema_internal(nulls, tableforest, targetns)?;
    Ok(stringinfo_to_xmltype(&si))
}

/// C `database_to_xml_and_xmlschema` (xml.c:3465).
pub fn database_to_xml_and_xmlschema(
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<Vec<u8>> {
    let xmlschema = database_to_xmlschema_internal(nulls, tableforest, targetns)?;
    let xmlschema_str = String::from_utf8_lossy(&xmlschema).into_owned();
    let si = database_to_xml_internal(Some(&xmlschema_str), nulls, tableforest, targetns)?;
    Ok(stringinfo_to_xmltype(&si))
}

// ===========================================================================
// SQL -> XML Schema type mapping (catalog)
// ===========================================================================

/// C `map_multipart_sql_identifier_to_xml_name` (xml.c:3484, static).
pub fn map_multipart_sql_identifier_to_xml_name(
    a: Option<&str>,
    b: Option<&str>,
    c: Option<&str>,
    d: Option<&str>,
) -> PgResult<String> {
    let mut result = String::new();

    if let Some(a) = a {
        result.push_str(&map_sql_identifier_to_xml_name(a.as_bytes(), true, true)?);
    }
    if let Some(b) = b {
        result.push('.');
        result.push_str(&map_sql_identifier_to_xml_name(b.as_bytes(), true, true)?);
    }
    if let Some(c) = c {
        result.push('.');
        result.push_str(&map_sql_identifier_to_xml_name(c.as_bytes(), true, true)?);
    }
    if let Some(d) = d {
        result.push('.');
        result.push_str(&map_sql_identifier_to_xml_name(d.as_bytes(), true, true)?);
    }

    Ok(result)
}

/// C `map_sql_table_to_xmlschema` (xml.c:3515, static).
pub fn map_sql_table_to_xmlschema(
    columns: &[RelationColumn],
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let mut result: Vec<u8> = Vec::new();

    let (xmltn, tabletypename, rowtypename): (String, String, String);

    if relid != InvalidOid {
        let rel = seam::relation_info::call(relid)?;
        let dbname = seam::get_database_name::call()?;
        let nspname = seam::get_namespace_name::call(rel.relnamespace)?;

        xmltn = map_sql_identifier_to_xml_name(rel.relname.as_bytes(), true, false)?;
        tabletypename = map_multipart_sql_identifier_to_xml_name(
            Some("TableType"),
            Some(&dbname),
            Some(&nspname),
            Some(&rel.relname),
        )?;
        rowtypename = map_multipart_sql_identifier_to_xml_name(
            Some("RowType"),
            Some(&dbname),
            Some(&nspname),
            Some(&rel.relname),
        )?;
    } else {
        xmltn = if tableforest { "row" } else { "table" }.to_string();
        tabletypename = "TableType".to_string();
        rowtypename = "RowType".to_string();
    }

    xsd_schema_element_start(&mut result, targetns);

    let single = vec![columns.to_vec()];
    result.extend_from_slice(map_sql_typecoll_to_xmlschema_types(&single)?.as_bytes());

    result.extend_from_slice(
        format!("<xsd:complexType name=\"{rowtypename}\">\n  <xsd:sequence>\n").as_bytes(),
    );

    for att in columns {
        if att.is_dropped {
            continue;
        }
        let name = map_sql_identifier_to_xml_name(att.attname.as_bytes(), true, false)?;
        let typename = map_sql_type_to_xml_name(att.atttypid, -1)?;
        let nillable = if nulls {
            " nillable=\"true\""
        } else {
            " minOccurs=\"0\""
        };
        result.extend_from_slice(
            format!(
                "    <xsd:element name=\"{name}\" type=\"{typename}\"{nillable}></xsd:element>\n"
            )
            .as_bytes(),
        );
    }

    result.extend_from_slice(b"  </xsd:sequence>\n</xsd:complexType>\n\n");

    if !tableforest {
        result.extend_from_slice(
            format!(
                "<xsd:complexType name=\"{tabletypename}\">\n  <xsd:sequence>\n    <xsd:element name=\"row\" type=\"{rowtypename}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n  </xsd:sequence>\n</xsd:complexType>\n\n"
            )
            .as_bytes(),
        );
        result.extend_from_slice(
            format!("<xsd:element name=\"{xmltn}\" type=\"{tabletypename}\"/>\n\n").as_bytes(),
        );
    } else {
        result.extend_from_slice(
            format!("<xsd:element name=\"{xmltn}\" type=\"{rowtypename}\"/>\n\n").as_bytes(),
        );
    }

    xsd_schema_element_end(&mut result);

    Ok(String::from_utf8_lossy(&result).into_owned())
}

/// C `map_sql_schema_to_xmlschema_types` (xml.c:3620, static).
pub fn map_sql_schema_to_xmlschema_types(
    nspid: Oid,
    relid_list: &[Oid],
    _nulls: bool,
    tableforest: bool,
    _targetns: &str,
) -> PgResult<String> {
    let dbname = seam::get_database_name::call()?;
    let nspname = seam::get_namespace_name::call(nspid)?;

    let mut result: Vec<u8> = Vec::new();

    let xmlsn = map_sql_identifier_to_xml_name(nspname.as_bytes(), true, false)?;
    let schematypename = map_multipart_sql_identifier_to_xml_name(
        Some("SchemaType"),
        Some(&dbname),
        Some(&nspname),
        None,
    )?;

    result.extend_from_slice(format!("<xsd:complexType name=\"{schematypename}\">\n").as_bytes());
    if !tableforest {
        result.extend_from_slice(b"  <xsd:all>\n");
    } else {
        result.extend_from_slice(b"  <xsd:sequence>\n");
    }

    for &relid in relid_list {
        let relname = seam::get_rel_name::call(relid)?;
        let xmltn = map_sql_identifier_to_xml_name(relname.as_bytes(), true, false)?;
        let tabletypename = map_multipart_sql_identifier_to_xml_name(
            Some(if tableforest { "RowType" } else { "TableType" }),
            Some(&dbname),
            Some(&nspname),
            Some(&relname),
        )?;

        if !tableforest {
            result.extend_from_slice(
                format!("    <xsd:element name=\"{xmltn}\" type=\"{tabletypename}\"/>\n").as_bytes(),
            );
        } else {
            result.extend_from_slice(
                format!(
                    "    <xsd:element name=\"{xmltn}\" type=\"{tabletypename}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n"
                )
                .as_bytes(),
            );
        }
    }

    if !tableforest {
        result.extend_from_slice(b"  </xsd:all>\n");
    } else {
        result.extend_from_slice(b"  </xsd:sequence>\n");
    }
    result.extend_from_slice(b"</xsd:complexType>\n\n");

    result.extend_from_slice(
        format!("<xsd:element name=\"{xmlsn}\" type=\"{schematypename}\"/>\n\n").as_bytes(),
    );

    Ok(String::from_utf8_lossy(&result).into_owned())
}

/// C `map_sql_catalog_to_xmlschema_types` (xml.c:3693, static).
pub fn map_sql_catalog_to_xmlschema_types(
    nspid_list: &[Oid],
    _nulls: bool,
    _tableforest: bool,
    _targetns: &str,
) -> PgResult<String> {
    let dbname = seam::get_database_name::call()?;

    let mut result: Vec<u8> = Vec::new();

    let xmlcn = map_sql_identifier_to_xml_name(dbname.as_bytes(), true, false)?;
    let catalogtypename =
        map_multipart_sql_identifier_to_xml_name(Some("CatalogType"), Some(&dbname), None, None)?;

    result.extend_from_slice(format!("<xsd:complexType name=\"{catalogtypename}\">\n").as_bytes());
    result.extend_from_slice(b"  <xsd:all>\n");

    for &nspid in nspid_list {
        let nspname = seam::get_namespace_name::call(nspid)?;
        let xmlsn = map_sql_identifier_to_xml_name(nspname.as_bytes(), true, false)?;
        let schematypename = map_multipart_sql_identifier_to_xml_name(
            Some("SchemaType"),
            Some(&dbname),
            Some(&nspname),
            None,
        )?;

        result.extend_from_slice(
            format!("    <xsd:element name=\"{xmlsn}\" type=\"{schematypename}\"/>\n").as_bytes(),
        );
    }

    result.extend_from_slice(b"  </xsd:all>\n");
    result.extend_from_slice(b"</xsd:complexType>\n\n");

    result.extend_from_slice(
        format!("<xsd:element name=\"{xmlcn}\" type=\"{catalogtypename}\"/>\n\n").as_bytes(),
    );

    Ok(String::from_utf8_lossy(&result).into_owned())
}

/// C `map_sql_type_to_xml_name` (xml.c:3750, static).
pub fn map_sql_type_to_xml_name(typeoid: Oid, typmod: i32) -> PgResult<String> {
    let mut result = String::new();
    let vh = VARHDRSZ as i32;

    match typeoid {
        BPCHAROID => {
            if typmod == -1 {
                result.push_str("CHAR");
            } else {
                result.push_str(&format!("CHAR_{}", typmod - vh));
            }
        }
        VARCHAROID => {
            if typmod == -1 {
                result.push_str("VARCHAR");
            } else {
                result.push_str(&format!("VARCHAR_{}", typmod - vh));
            }
        }
        NUMERICOID => {
            if typmod == -1 {
                result.push_str("NUMERIC");
            } else {
                result.push_str(&format!(
                    "NUMERIC_{}_{}",
                    ((typmod - vh) >> 16) & 0xffff,
                    (typmod - vh) & 0xffff
                ));
            }
        }
        INT4OID => result.push_str("INTEGER"),
        INT2OID => result.push_str("SMALLINT"),
        INT8OID => result.push_str("BIGINT"),
        FLOAT4OID => result.push_str("REAL"),
        FLOAT8OID => result.push_str("DOUBLE"),
        BOOLOID => result.push_str("BOOLEAN"),
        TIMEOID => {
            if typmod == -1 {
                result.push_str("TIME");
            } else {
                result.push_str(&format!("TIME_{typmod}"));
            }
        }
        TIMETZOID => {
            if typmod == -1 {
                result.push_str("TIME_WTZ");
            } else {
                result.push_str(&format!("TIME_WTZ_{typmod}"));
            }
        }
        TIMESTAMPOID => {
            if typmod == -1 {
                result.push_str("TIMESTAMP");
            } else {
                result.push_str(&format!("TIMESTAMP_{typmod}"));
            }
        }
        TIMESTAMPTZOID => {
            if typmod == -1 {
                result.push_str("TIMESTAMP_WTZ");
            } else {
                result.push_str(&format!("TIMESTAMP_WTZ_{typmod}"));
            }
        }
        DATEOID => result.push_str("DATE"),
        XMLOID => result.push_str("XML"),
        _ => {
            let typtuple = seam::type_info::call(typeoid)?;
            let dbname = seam::get_database_name::call()?;
            let nspname = seam::get_namespace_name::call(typtuple.typnamespace)?;
            result.push_str(&map_multipart_sql_identifier_to_xml_name(
                Some(if typtuple.is_domain { "Domain" } else { "UDT" }),
                Some(&dbname),
                Some(&nspname),
                Some(&typtuple.typname),
            )?);
        }
    }

    Ok(result)
}

/// C `map_sql_typecoll_to_xmlschema_types` (xml.c:3855, static).
///
/// `tupdesc_list` is a list of tuple descriptors (column lists).
pub fn map_sql_typecoll_to_xmlschema_types(tupdesc_list: &[Vec<RelationColumn>]) -> PgResult<String> {
    let mut uniquetypes: Vec<Oid> = Vec::new();

    // extract all column types
    for tupdesc in tupdesc_list {
        for att in tupdesc {
            if att.is_dropped {
                continue;
            }
            if !uniquetypes.contains(&att.atttypid) {
                uniquetypes.push(att.atttypid);
            }
        }
    }

    // add base types of domains
    let snapshot = uniquetypes.clone();
    for typid in snapshot {
        let basetypid = lsc::get_base_type::call(typid)?;
        if basetypid != typid && !uniquetypes.contains(&basetypid) {
            uniquetypes.push(basetypid);
        }
    }

    let mut result = String::new();
    for typid in uniquetypes {
        result.push_str(&map_sql_type_to_xmlschema_type(typid, -1)?);
        result.push('\n');
    }

    Ok(result)
}

/// C `map_sql_type_to_xmlschema_type` (xml.c:3910, static).
pub fn map_sql_type_to_xmlschema_type(typeoid: Oid, typmod: i32) -> PgResult<String> {
    let mut result = String::new();
    let typename = map_sql_type_to_xml_name(typeoid, typmod)?;
    let vh = VARHDRSZ as i32;

    if typeoid == XMLOID {
        result.push_str(
            "<xsd:complexType mixed=\"true\">\n  <xsd:sequence>\n    <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n  </xsd:sequence>\n</xsd:complexType>\n",
        );
        return Ok(result);
    }

    result.push_str(&format!("<xsd:simpleType name=\"{typename}\">\n"));

    match typeoid {
        BPCHAROID | VARCHAROID | TEXTOID => {
            result.push_str("  <xsd:restriction base=\"xsd:string\">\n");
            if typmod != -1 {
                result.push_str(&format!("    <xsd:maxLength value=\"{}\"/>\n", typmod - vh));
            }
            result.push_str("  </xsd:restriction>\n");
        }
        BYTEAOID => {
            let kind = if seam::xmlbinary::call() == XmlBinaryType::XMLBINARY_BASE64 {
                "base64Binary"
            } else {
                "hexBinary"
            };
            result.push_str(&format!(
                "  <xsd:restriction base=\"xsd:{kind}\">\n  </xsd:restriction>\n"
            ));
        }
        NUMERICOID => {
            if typmod != -1 {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:decimal\">\n    <xsd:totalDigits value=\"{}\"/>\n    <xsd:fractionDigits value=\"{}\"/>\n  </xsd:restriction>\n",
                    ((typmod - vh) >> 16) & 0xffff,
                    (typmod - vh) & 0xffff
                ));
            }
        }
        INT2OID => {
            result.push_str(&format!(
                "  <xsd:restriction base=\"xsd:short\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i16::MAX,
                i16::MIN
            ));
        }
        INT4OID => {
            result.push_str(&format!(
                "  <xsd:restriction base=\"xsd:int\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i32::MAX,
                i32::MIN
            ));
        }
        INT8OID => {
            result.push_str(&format!(
                "  <xsd:restriction base=\"xsd:long\">\n    <xsd:maxInclusive value=\"{}\"/>\n    <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i64::MAX,
                i64::MIN
            ));
        }
        FLOAT4OID => {
            result.push_str("  <xsd:restriction base=\"xsd:float\"></xsd:restriction>\n");
        }
        FLOAT8OID => {
            result.push_str("  <xsd:restriction base=\"xsd:double\"></xsd:restriction>\n");
        }
        BOOLOID => {
            result.push_str("  <xsd:restriction base=\"xsd:boolean\"></xsd:restriction>\n");
        }
        TIMEOID | TIMETZOID => {
            let tz = if typeoid == TIMETZOID {
                "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"
            } else {
                ""
            };
            if typmod == -1 {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{tz}\"/>\n  </xsd:restriction>\n"
                ));
            } else if typmod == 0 {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{tz}\"/>\n  </xsd:restriction>\n"
                ));
            } else {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:time\">\n    <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{tz}\"/>\n  </xsd:restriction>\n",
                    typmod - vh
                ));
            }
        }
        TIMESTAMPOID | TIMESTAMPTZOID => {
            let tz = if typeoid == TIMESTAMPTZOID {
                "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"
            } else {
                ""
            };
            if typmod == -1 {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{tz}\"/>\n  </xsd:restriction>\n"
                ));
            } else if typmod == 0 {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{tz}\"/>\n  </xsd:restriction>\n"
                ));
            } else {
                result.push_str(&format!(
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{tz}\"/>\n  </xsd:restriction>\n",
                    typmod - vh
                ));
            }
        }
        DATEOID => {
            result.push_str(
                "  <xsd:restriction base=\"xsd:date\">\n    <xsd:pattern value=\"\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}\"/>\n  </xsd:restriction>\n",
            );
        }
        _ => {
            if seam::is_domain::call(typeoid)? {
                let (base_typeoid, base_typmod) =
                    seam::get_base_type_and_typmod::call(typeoid, -1)?;
                result.push_str(&format!(
                    "  <xsd:restriction base=\"{}\"/>\n",
                    map_sql_type_to_xml_name(base_typeoid, base_typmod)?
                ));
            }
        }
    }
    result.push_str("</xsd:simpleType>\n");

    Ok(result)
}

/// C `SPI_sql_row_to_xmlelement` (xml.c:4085, static).
///
/// `exec` is the SPI result; `rownum` indexes into its rows.
pub fn SPI_sql_row_to_xmlelement(
    rownum: u64,
    result: &mut Vec<u8>,
    exec: &SpiResult,
    tablename: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<()> {
    let xmltn = match tablename {
        Some(t) => map_sql_identifier_to_xml_name(t.as_bytes(), true, false)?,
        None => {
            if tableforest {
                "row".to_string()
            } else {
                "table".to_string()
            }
        }
    };

    if tableforest {
        xmldata_root_element_start(result, &xmltn, None, targetns, top_level);
    } else {
        result.extend_from_slice(b"<row>\n");
    }

    let row = &exec.rows[rownum as usize];
    for (i, col) in exec.columns.iter().enumerate() {
        let colname = map_sql_identifier_to_xml_name(col.name.as_bytes(), true, false)?;
        match row.get(i).and_then(|v| v.as_ref()) {
            None => {
                if nulls {
                    result.extend_from_slice(format!("  <{colname} xsi:nil=\"true\"/>\n").as_bytes());
                }
            }
            Some(value) => {
                result.extend_from_slice(format!("  <{colname}>{value}</{colname}>\n").as_bytes());
            }
        }
    }

    if tableforest {
        xmldata_root_element_end(result, &xmltn);
        result.push(b'\n');
    } else {
        result.extend_from_slice(b"</row>\n\n");
    }

    Ok(())
}

// ===========================================================================
// XPath support
// ===========================================================================

/// C `xpath_internal`'s namespace-mapping block (xml.c:4347-4382): validate and
/// deconstruct the `text[]` namespace-mapping array into `(name, uri)` pairs.
///
/// `namespaces` is the raw (detoasted) `text[]` array varlena image, or `None`
/// for the SQL NULL / absent argument. An empty array (`ndim == 0`) means "no
/// namespace mappings". A non-empty array must be two-dimensional with the
/// length of its second axis equal to 2 (each subarray is a `[name, uri]`
/// pair); otherwise `ERRCODE_DATA_EXCEPTION` "invalid array for XML namespace
/// mapping". No element of a pair may be NULL (`ERRCODE_NULL_VALUE_NOT_ALLOWED`).
fn deconstruct_xml_namespaces(
    namespaces: Option<&[u8]>,
) -> PgResult<Vec<(String, String)>> {
    let Some(arr) = namespaces else {
        // C: ndim = namespaces ? ARR_NDIM(namespaces) : 0 == 0 -> no mappings.
        return Ok(Vec::new());
    };

    let (ndim, dims, elems) =
        backend_utils_adt_arrayfuncs_seams::deconstruct_text_array_with_dims::call(arr)?;

    if ndim == 0 {
        // 0-dimensional array: no namespace mappings.
        return Ok(Vec::new());
    }

    if ndim != 2 || dims.get(1).copied() != Some(2) {
        return Err(PgError::error("invalid array for XML namespace mapping")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .with_detail(
                "The array must be two-dimensional with length of the second axis equal to 2.",
            ));
    }

    // ns_count = nelems / 2 (checked: dims[1] == 2 so nelems is even).
    let ns_count = elems.len() / 2;
    let mut out: Vec<(String, String)> = Vec::with_capacity(ns_count);
    for i in 0..ns_count {
        // if (ns_names_uris_nulls[i*2] || ns_names_uris_nulls[i*2+1]) ereport.
        let name = elems[i * 2].as_ref();
        let uri = elems[i * 2 + 1].as_ref();
        let (name, uri) = match (name, uri) {
            (Some(n), Some(u)) => (n, u),
            _ => {
                return Err(PgError::error("neither namespace name nor URI may be null")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }
        };
        // TextDatumGetCString(ns_names_uris[..]): the element payload as a
        // C string (the libxml provider re-encodes it for xmlXPathRegisterNs).
        out.push((
            String::from_utf8_lossy(name).into_owned(),
            String::from_utf8_lossy(uri).into_owned(),
        ));
    }
    Ok(out)
}

/// C `xpath` (xml.c:4519) — `xpath(text, xml [, text[]])`. The fmgr-boundary
/// entry: validate/deconstruct the raw `text[]` namespace array, then evaluate.
/// Returns the matched values as serialized `xmltype` byte images (one per
/// array element).
pub fn xpath_fmgr(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: Option<&[u8]>,
) -> PgResult<Vec<Vec<u8>>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let ns = deconstruct_xml_namespaces(namespaces)?;
    xpath_internal(xpath_expr, data, &ns, false)
}

/// C `xpath_exists` (xml.c:4565) — the fmgr-boundary entry: validate/deconstruct
/// the raw `text[]` namespace array, then evaluate for existence.
pub fn xpath_exists_fmgr(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: Option<&[u8]>,
) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let ns = deconstruct_xml_namespaces(namespaces)?;
    let items = xpath_internal(xpath_expr, data, &ns, true)?;
    Ok(!items.is_empty())
}

/// C `xpath` (xml.c:4519) — `xpath(text, xml [, text[]])`. Returns the matched
/// values as serialized `xmltype` byte images (one per array element).
pub fn xpath(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: &[(String, String)],
) -> PgResult<Vec<Vec<u8>>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    xpath_internal(xpath_expr, data, namespaces, false)
}

/// C `xpath_internal` (xml.c:4323, static).
///
/// `namespaces` are the already-deconstructed `(name, uri)` mappings; the C
/// function's raw `text[]` dimension validation (`ndim != 2 || dims[1] != 2` ->
/// `ERRCODE_DATA_EXCEPTION` "invalid array for XML namespace mapping", and the
/// per-pair NULL check) is performed by the array-deconstructing caller that
/// produces this slice, since the array deconstruction itself is a catalog/array
/// call-out.
///
/// `count_only` mirrors C's `astate == NULL` path used by `xmlexists` /
/// `xpath_exists`: it is forwarded to [`seam::xpath_eval`] so a provider may skip
/// per-node serialization when only the match *count* is needed.
pub fn xpath_internal(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: &[(String, String)],
    count_only: bool,
) -> PgResult<Vec<Vec<u8>>> {
    if xpath_expr.is_empty() {
        return Err(PgError::error("empty XPath expression")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY));
    }
    seam::xpath_eval::call(
        xpath_expr,
        data,
        namespaces,
        count_only,
        seam::get_database_encoding::call(),
    )
}

/// C `xmlexists` (xml.c:4542) — `XMLEXISTS(text PASSING xml)`.
pub fn xmlexists(xpath_expr: &[u8], data: &[u8]) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let items = xpath_internal(xpath_expr, data, &[], true)?;
    Ok(!items.is_empty())
}

/// C `xpath_exists` (xml.c:4565) — `xpath_exists(text, xml [, text[]])`.
pub fn xpath_exists(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: &[(String, String)],
) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let items = xpath_internal(xpath_expr, data, namespaces, true)?;
    Ok(!items.is_empty())
}

// ===========================================================================
// Well-formedness predicates
// ===========================================================================

/// C `wellformed_xml` (xml.c:4590, static) — true if `xml_parse` reports no soft
/// error.
pub fn wellformed_xml(data: &[u8], xmloption_arg: XmlOptionType) -> PgResult<bool> {
    let outcome = seam::xml_parse_libxml::call(
        data,
        xmloption_arg,
        true,
        seam::get_database_encoding::call(),
    )?;
    Ok(outcome.is_ok())
}

/// C `xml_is_well_formed` (xml.c:4607).
pub fn xml_is_well_formed(data: &[u8]) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    wellformed_xml(data, seam::xmloption::call())
}

/// C `xml_is_well_formed_document` (xml.c:4620).
pub fn xml_is_well_formed_document(data: &[u8]) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    wellformed_xml(data, XmlOptionType::XMLOPTION_DOCUMENT)
}

/// C `xml_is_well_formed_content` (xml.c:4633).
pub fn xml_is_well_formed_content(data: &[u8]) -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    wellformed_xml(data, XmlOptionType::XMLOPTION_CONTENT)
}

// ===========================================================================
// XMLTABLE support (TableFuncRoutine)
//
// The whole XMLTABLE machinery is `#ifdef USE_LIBXML`; without a libxml provider
// every entry point raises `NO_XML_SUPPORT()`, exactly as a `--without-libxml`
// server's `XmlTableRoutine` would. These thin entry points route to the
// provider when installed (the libxml parser-context lifecycle is wholly
// internal to it).
// ===========================================================================

/// `TYPCATEGORY_NUMERIC` (pg_type.h) — the column-XPath BOOLEAN result casts
/// boolean→number→string when the target column type is in this category.
const TYPCATEGORY_NUMERIC: u8 = b'N';

/// C `XmlTableInitOpaque` (xml.c:4683, static; `XmlTableRoutine.InitOpaque`).
pub fn XmlTableInitOpaque(natts: i32) -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_init_opaque::call(natts)
}

/// C `XmlTableSetDocument` (xml.c:4731, static; `XmlTableRoutine.SetDocument`).
///
/// C does `str = xml_out_internal(DatumGetXmlP(value), 0)` to obtain the
/// encoding-stripped document text, then `xmlCtxtReadMemory` on it. The
/// `xml_out_internal` rendering is pure in-crate work; the libxml parse is the
/// seam. `xml_image` is the document's `xmltype` varlena payload.
pub fn XmlTableSetDocument(xml_image: &[u8]) -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let str = xml_out_internal(xml_image, 0)?;
    seam::xmltable_set_document::call(&str)
}

/// C `XmlTableSetNamespace` (xml.c:4788, static; `XmlTableRoutine.SetNamespace`).
pub fn XmlTableSetNamespace(name: Option<&str>, uri: &str) -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_set_namespace::call(name, uri)
}

/// C `XmlTableSetRowFilter` (xml.c:4814, static; `XmlTableRoutine.SetRowFilter`).
pub fn XmlTableSetRowFilter(path: &str) -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_set_row_filter::call(path)
}

/// C `XmlTableSetColumnFilter` (xml.c:4846, static; `...SetColumnFilter`).
pub fn XmlTableSetColumnFilter(path: &str, colnum: i32) -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_set_column_filter::call(path, colnum)
}

/// C `XmlTableFetchRow` (xml.c:4881, static; `XmlTableRoutine.FetchRow`).
pub fn XmlTableFetchRow() -> PgResult<bool> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_fetch_row::call()
}

/// C `XmlTableGetValue` (xml.c:4926, static; `XmlTableRoutine.GetValue`), minus
/// the trailing `InputFunctionCall` (owned by the executor, which holds
/// `in_functions`/`typioparams`). Returns the column's textual value, or `None`
/// for the C `*isnull = true`. The `get_type_category_preferred` lookup — needed
/// only for the XPATH_BOOLEAN cast — is resolved here and passed to the provider.
pub fn XmlTableGetValue(colnum: i32, typid: Oid) -> PgResult<Option<String>> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    let is_xml = typid == XMLOID;
    // C calls get_type_category_preferred unconditionally before the PG_TRY; we
    // mirror that (the provider only consults it in the BOOLEAN arm).
    let (typcategory, _typispreferred) = lsc::get_type_category_preferred::call(typid)?;
    let is_numeric_category = typcategory == TYPCATEGORY_NUMERIC;
    seam::xmltable_get_value::call(colnum, is_xml, is_numeric_category)
}

/// C `XmlTableDestroyOpaque` (xml.c:5078, static; `...DestroyOpaque`).
pub fn XmlTableDestroyOpaque() -> PgResult<()> {
    if !seam::have_libxml::call() {
        return Err(no_xml_support());
    }
    seam::xmltable_destroy_opaque::call()
}
