//! The libxml2 binding + seam installs (only compiled with `with-libxml`).
//!
//! Mirrors `src/backend/utils/adt/xml.c`'s `#ifdef USE_LIBXML` bodies. Each
//! seam install reproduces one C function's libxml2 call sequence:
//!
//!  * `xml_parse_libxml`      <- `xml_parse`           (xml.c:1748)
//!  * `serialize_with_options`<- `xmltotext_with_options` (xml.c:638)
//!  * `build_element`         <- `xmlelement`          (xml.c:864)
//!  * `encode_special_chars`  <- `xmltext`             (xml.c:526)
//!  * `encode_binary`         <- BYTEAOID arm of `map_sql_value_to_xml_value` (xml.c:2615)
//!  * `xpath_eval`            <- `xpath_internal` + `xml_xpathobjtoxmlarray` (xml.c:4323/4243)
//!  * `is_blank_ch`           <- `xmlIsBlank_ch`
//!  * `get_utf8_char`         <- `xmlGetUTF8Char`
//!  * `have_libxml`           <- the `NO_XML_SUPPORT()` condition (true here)
//!
//! Error handling: xml.c installs a structured libxml error handler via
//! `pg_xml_init` that buffers diagnostics into a `PgXmlErrorContext` and, on
//! failure, attaches them as the ereport `errdetail`. We port that here
//! (`xml_error_handler`/`pg_xml_init`/`xml_ereport` + a thread-local
//! `XmlErrCtx`): a libxml failure becomes a `PgError` carrying the same
//! `DETAIL: line N: <message>` text and the same `err_occurred` escalation
//! (load-bearing — libxml can return a non-NULL doc while having raised a
//! namespace error, and C escalates on `err_occurred`). We also block external
//! entity loading (`xmlSetExternalEntityLoader`) exactly as
//! `pg_xml_init`/`xmlPgEntityLoader` do, so parses are sandboxed identically.
//! Not reproduced: the source-line/caret context lines C appends via
//! `xmlParserPrintFileContext` (its libxml generic-error callback is
//! C-variadic, undefinable in stable Rust) and the immediate `WARNING`/`NOTICE`
//! channel for sub-error-level libxml messages — both are libxml-binding gaps,
//! noted at `xml_error_handler`.

use core::ffi::{c_char, c_int, c_uchar, c_void};

use types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_ARGUMENT_FOR_XQUERY, ERRCODE_INVALID_XML_CONTENT,
    ERRCODE_INVALID_XML_DOCUMENT, ERRCODE_OUT_OF_MEMORY,
};
use ::nodes::primnodes::XmlOptionType;
use ::types_xml::XmlBinaryType;

use adt_xml as owner;
use xml_libxml_seams as seams;

/* ===================================================================== *
 *  Opaque libxml2 types. Only pointers cross the FFI.
 * ===================================================================== */
#[repr(C)]
struct xmlDoc {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlParserCtxt {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlBuffer {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlTextWriter {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlSaveCtxt {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlXPathContext {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlXPathCompExpr {
    _p: [u8; 0],
}
#[repr(C)]
struct xmlXPathObject {
    _p: [u8; 0],
}

/// `xmlNode` — we only read `type` (first int field after two pointers... but we
/// never deref the struct layout directly except through accessor calls, so it
/// stays opaque). Node-set members are read via `xmlXPathNodeSetItem`.
#[repr(C)]
struct xmlNode {
    _p: [u8; 0],
}

/// `xmlNodeSet` — pointed at by `xmlXPathObject.nodesetval`; its fields are read
/// through [`xmlNodeSetHdr`] (the libxml header accessors are macros, not
/// exported symbols).
#[repr(C)]
struct xmlNodeSet {
    _p: [u8; 0],
}

// libxml2 XML node types we must distinguish in `xml_xmlnodetoxmltype`.
const XML_ATTRIBUTE_NODE: c_int = 2;
const XML_TEXT_NODE: c_int = 3;
const XML_DOCUMENT_NODE: c_int = 9;

// `xmlXPathObjectType`.
const XPATH_NODESET: c_int = 1;
const XPATH_BOOLEAN: c_int = 2;
const XPATH_NUMBER: c_int = 3;
const XPATH_STRING: c_int = 4;

// xmlParserOption bits used by xml_parse.
const XML_PARSE_NOENT: c_int = 1 << 1;
const XML_PARSE_DTDATTR: c_int = 1 << 3;
const XML_PARSE_NOBLANKS: c_int = 1 << 8;
const XML_PARSE_NONET: c_int = 1 << 11;

// xmlSaveOption bits used by xmltotext_with_options.
const XML_SAVE_FORMAT: c_int = 1 << 0;
const XML_SAVE_NO_DECL: c_int = 1 << 1;

type xmlExternalEntityLoader = unsafe extern "C" fn(
    URL: *const c_char,
    ID: *const c_char,
    ctxt: *mut c_void,
) -> *mut c_void;

/* ===================================================================== *
 *  libxml2 symbols (the subset xml.c actually invokes through its seams).
 * ===================================================================== */
/// libxml2 `xmlFreeFunc` — the type of the global `xmlFree` deallocator
/// pointer (`xmlmemory.h`: `typedef void (*xmlFreeFunc)(void *mem);`).
type xmlFreeFunc = unsafe extern "C" fn(mem: *mut c_void);

extern "C" {
    fn xmlInitParser();

    /// libxml2 exports `xmlFree` as a GLOBAL VARIABLE holding a function
    /// pointer, NOT as a function:
    ///   `XMLPUBVAR xmlFreeFunc xmlFree;`  (xmlmemory.h)
    /// Binding it as `fn xmlFree(...)` resolves the call to the *address of
    /// the variable* and executes the stored pointer bits as code → SIGBUS.
    /// It must be bound as a data symbol and called through the contained
    /// pointer (see the `xmlFree()` wrapper below).
    #[link_name = "xmlFree"]
    static xmlFreePtr: xmlFreeFunc;

    // parsing
    fn xmlNewParserCtxt() -> *mut xmlParserCtxt;
    fn xmlFreeParserCtxt(ctxt: *mut xmlParserCtxt);
    fn xmlCtxtReadDoc(
        ctxt: *mut xmlParserCtxt,
        cur: *const c_uchar,
        URL: *const c_char,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlDoc;
    fn xmlCtxtReadMemory(
        ctxt: *mut xmlParserCtxt,
        buffer: *const c_char,
        size: c_int,
        URL: *const c_char,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlDoc;
    fn xmlNewDoc(version: *const c_uchar) -> *mut xmlDoc;
    fn xmlFreeDoc(doc: *mut xmlDoc);
    fn xmlParseBalancedChunkMemory(
        doc: *mut xmlDoc,
        sax: *mut c_void,
        user_data: *mut c_void,
        depth: c_int,
        string: *const c_uchar,
        lst: *mut *mut xmlNode,
    ) -> c_int;
    fn xmlKeepBlanksDefault(val: c_int) -> c_int;
    fn xmlDocSetRootElement(doc: *mut xmlDoc, root: *mut xmlNode) -> *mut xmlNode;
    fn xmlNewNode(ns: *mut c_void, name: *const c_uchar) -> *mut xmlNode;
    fn xmlNewDocText(doc: *mut xmlDoc, content: *const c_uchar) -> *mut xmlNode;
    fn xmlAddChildList(parent: *mut xmlNode, cur: *mut xmlNode) -> *mut xmlNode;
    fn xmlFreeNode(node: *mut xmlNode);

    // buffers
    fn xmlBufferCreate() -> *mut xmlBuffer;
    fn xmlBufferFree(buf: *mut xmlBuffer);
    fn xmlBufferContent(buf: *const xmlBuffer) -> *const c_uchar;
    fn xmlBufferLength(buf: *const xmlBuffer) -> c_int;

    // serialization (save)
    fn xmlSaveToBuffer(
        buffer: *mut xmlBuffer,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlSaveCtxt;
    fn xmlSaveDoc(ctxt: *mut xmlSaveCtxt, doc: *mut xmlDoc) -> std::os::raw::c_long;
    fn xmlSaveTree(ctxt: *mut xmlSaveCtxt, node: *mut xmlNode) -> std::os::raw::c_long;
    fn xmlSaveClose(ctxt: *mut xmlSaveCtxt) -> c_int;

    // node dump / copy (xml_xmlnodetoxmltype)
    fn xmlNodeDump(
        buf: *mut xmlBuffer,
        doc: *mut xmlDoc,
        cur: *mut xmlNode,
        level: c_int,
        format: c_int,
    ) -> c_int;
    fn xmlCopyNode(node: *mut xmlNode, extended: c_int) -> *mut xmlNode;

    // text writer (xmlelement / encode_binary / encode_special_chars)
    fn xmlNewTextWriterMemory(buf: *mut xmlBuffer, compression: c_int) -> *mut xmlTextWriter;
    fn xmlFreeTextWriter(writer: *mut xmlTextWriter);
    fn xmlTextWriterStartElement(writer: *mut xmlTextWriter, name: *const c_uchar) -> c_int;
    fn xmlTextWriterEndElement(writer: *mut xmlTextWriter) -> c_int;
    fn xmlTextWriterWriteAttribute(
        writer: *mut xmlTextWriter,
        name: *const c_uchar,
        content: *const c_uchar,
    ) -> c_int;
    fn xmlTextWriterWriteRaw(writer: *mut xmlTextWriter, content: *const c_uchar) -> c_int;
    fn xmlTextWriterWriteBase64(
        writer: *mut xmlTextWriter,
        data: *const c_char,
        start: c_int,
        len: c_int,
    ) -> c_int;
    fn xmlTextWriterWriteBinHex(
        writer: *mut xmlTextWriter,
        data: *const c_char,
        start: c_int,
        len: c_int,
    ) -> c_int;
    fn xmlEncodeSpecialChars(doc: *const xmlDoc, input: *const c_uchar) -> *mut c_uchar;

    // XPath
    fn xmlXPathNewContext(doc: *mut xmlDoc) -> *mut xmlXPathContext;
    fn xmlXPathFreeContext(ctxt: *mut xmlXPathContext);
    fn xmlXPathRegisterNs(
        ctxt: *mut xmlXPathContext,
        prefix: *const c_uchar,
        ns_uri: *const c_uchar,
    ) -> c_int;
    fn xmlXPathCtxtCompile(
        ctxt: *mut xmlXPathContext,
        expr: *const c_uchar,
    ) -> *mut xmlXPathCompExpr;
    fn xmlXPathFreeCompExpr(comp: *mut xmlXPathCompExpr);
    fn xmlXPathCompiledEval(
        comp: *mut xmlXPathCompExpr,
        ctxt: *mut xmlXPathContext,
    ) -> *mut xmlXPathObject;
    fn xmlXPathFreeObject(obj: *mut xmlXPathObject);
    fn xmlXPathCastNodeToString(node: *mut xmlNode) -> *mut c_uchar;
    fn xmlXPathCastNodeSetToString(ns: *mut xmlNodeSet) -> *mut c_uchar;
    fn xmlXPathCastBooleanToString(val: c_int) -> *mut c_uchar;
    fn xmlXPathCastBooleanToNumber(val: c_int) -> f64;
    fn xmlXPathCastNumberToString(val: f64) -> *mut c_uchar;

    // entity loader sandboxing (pg_xml_init / xmlPgEntityLoader)
    fn xmlSetExternalEntityLoader(f: Option<xmlExternalEntityLoader>);
    fn xmlSetStructuredErrorFunc(ctx: *mut c_void, handler: Option<xmlStructuredErrorFunc>);

    fn xmlStrlen(s: *const c_uchar) -> c_int;
}

/// Call the libxml2 global `xmlFree` deallocator through its function-pointer
/// variable. Mirrors C `xmlFree(p)` (which the headers expand to
/// `(*xmlFree)(p)` via the `xmlFreeFunc` global). libxml2 initialises the
/// pointer to its default allocator at library load (and `xmlMemSetup` may
/// override it), so it is non-NULL for the whole process lifetime.
#[inline]
unsafe fn xmlFree(p: *mut c_void) {
    (xmlFreePtr)(p);
}

type xmlStructuredErrorFunc = unsafe extern "C" fn(user_data: *mut c_void, error: *mut c_void);

/* ===================================================================== *
 *  libxml2 structured-error capture — port of `xml_errorHandler`,
 *  `pg_xml_init`, and `xml_ereport` (xml.c). PostgreSQL installs a
 *  structured error handler that buffers libxml diagnostics into a
 *  `PgXmlErrorContext` and, on failure, attaches the collected text as the
 *  ereport `errdetail`. We mirror that here so a libxml error becomes a
 *  `PgError` carrying the same `DETAIL: line N: <message>` text and the same
 *  `err_occurred` escalation (the latter is load-bearing: libxml can return a
 *  non-NULL document while still having raised a namespace error, and C escalates
 *  on `err_occurred`, not just on a NULL result).
 *
 *  Not reproduced: the two source-line/caret context lines that C appends via
 *  `xmlParserPrintFileContext`. That helper drives libxml's *generic* error
 *  callback, which has a C-variadic signature `(void*, const char*, ...)` that
 *  cannot be defined in stable Rust (and reading the parser-context input buffer
 *  directly would require hardcoding the offset of `input` inside the opaque,
 *  version-variable `xmlParserCtxt`). Those context lines remain a libxml-binding
 *  gap; the message/detail text and the pass/fail escalation are faithful.
 * ===================================================================== */

/// Prefix of libxml2's public `struct _xmlError` (`xmlerror.h`), through the
/// fields `xml_errorHandler` reads. ABI-stable across libxml2 2.x, like the
/// other struct prefixes this crate reads.
#[repr(C)]
struct xmlErrorHdr {
    domain: c_int,
    code: c_int,
    message: *mut c_char,
    level: c_int,
    file: *mut c_char,
    line: c_int,
    str1: *mut c_char,
    str2: *mut c_char,
    str3: *mut c_char,
    int1: c_int,
    int2: c_int,
    ctxt: *mut c_void,
    node: *mut c_void,
}

/// Prefix of libxml2's `struct _xmlParserCtxt` (`parser.h`) through the `input`
/// field. The layout up to `input` is ABI-stable across libxml2 2.x:
/// `sax`, `userData`, `myDoc` (pointers), `wellFormed`, `replaceEntities`
/// (ints), `version`, `encoding` (pointers), `standalone`, `html` (ints), then
/// `input`. `error->ctxt` points at one of these when the diagnostic comes from
/// the parser; we read `ctxt->input` to reconstruct the file-context lines.
#[repr(C)]
struct xmlParserCtxtHdr {
    sax: *mut c_void,
    user_data: *mut c_void,
    my_doc: *mut c_void,
    well_formed: c_int,
    replace_entities: c_int,
    version: *const c_uchar,
    encoding: *const c_uchar,
    standalone: c_int,
    html: c_int,
    input: *mut xmlParserInputHdr,
}

/// Prefix of libxml2's `struct _xmlParserInput` (`parser.h`) through the fields
/// `xmlParserPrintFileContextInternal` reads (`base`, `cur`, `end`). ABI-stable.
#[repr(C)]
struct xmlParserInputHdr {
    buf: *mut c_void,
    filename: *const c_char,
    directory: *const c_char,
    base: *const c_uchar,
    cur: *const c_uchar,
    end: *const c_uchar,
    length: c_int,
    line: c_int,
    col: c_int,
}

// `xmlErrorDomain` values used by `xml_errorHandler`.
const XML_FROM_NONE: c_int = 0;
const XML_FROM_PARSER: c_int = 1;
const XML_FROM_NAMESPACE: c_int = 3;
const XML_FROM_IO: c_int = 13;
const XML_FROM_MEMORY: c_int = 15;

// `xmlErrorLevel` values.
const XML_ERR_WARNING: c_int = 1;
const XML_ERR_ERROR: c_int = 2;

// `xmlParserErrors` codes the handler special-cases. These are the *ordinal*
// values of the `xmlParserErrors` enum in libxml2's `xmlerror.h` — which is
// what libxml actually stores in `xmlError.code` at runtime, and what C's
// `xml_errorHandler` compares against via the enum names. They are NOT the
// numbers shown in some online libxml docs (those are a different, scrambled
// numbering). `XML_WAR_UNDECLARED_ENTITY` in particular is 27, not 98: getting
// this wrong made the DocBook `&nbsp;`/unloaded-external-DTD undeclared-entity
// error (which C suppresses to keep the parse non-fatal) escalate to a hard
// "invalid XML document" error. Verified against
// SDKs/MacOSX.sdk/usr/include/libxml2/libxml/xmlerror.h (libxml2 2.9.x).
const XML_ERR_NOT_WELL_BALANCED: c_int = 85;
const XML_WAR_UNDECLARED_ENTITY: c_int = 27;
const XML_WAR_NS_URI: c_int = 99;
const XML_WAR_NS_URI_RELATIVE: c_int = 100;
const XML_ERR_NS_DECL_ERROR: c_int = 35;
const XML_WAR_NS_COLUMN: c_int = 106;
const XML_NS_ERR_XML_NAMESPACE: c_int = 200;
const XML_NS_ERR_UNDEFINED_NAMESPACE: c_int = 201;
const XML_NS_ERR_QNAME: c_int = 202;
const XML_NS_ERR_ATTRIBUTE_REDEFINED: c_int = 203;
const XML_NS_ERR_EMPTY: c_int = 204;

// `XML_ELEMENT_NODE` (tree.h) — the only node type whose `name` we read.
const XML_ELEMENT_NODE: c_int = 1;

// `PgXmlStrictness` (utils/xml.h) — the strictness argument to `pg_xml_init`.
const PG_XML_STRICTNESS_WELLFORMED: i32 = 1;
const PG_XML_STRICTNESS_ALL: i32 = 2;

/// Per-backend libxml error context — the fields of `PgXmlErrorContext`
/// (`utils/xml.h`) that `xml_errorHandler`/`xml_ereport` use. Thread-local
/// because it is per-backend state, never shared across threads (AGENTS.md).
struct XmlErrCtx {
    strictness: i32,
    err_occurred: bool,
    err_buf: String,
    /// Warning-level diagnostics collected during the current libxml operation.
    /// C's `xml_errorHandler` ereport(WARNING)s these immediately, but doing so
    /// from inside an `extern "C"` callback that libxml invoked mid-parse is
    /// unsafe in our unwind model, so we buffer them and `flush_xml_warnings`
    /// emits them at the seam boundary once libxml has returned.
    pending_warnings: Vec<String>,
}

std::thread_local! {
    static XML_ERR_CTX: std::cell::RefCell<XmlErrCtx> = const {
        std::cell::RefCell::new(XmlErrCtx {
            strictness: 0,
            err_occurred: false,
            err_buf: String::new(),
            pending_warnings: Vec::new(),
        })
    };
}

/// Emit any warnings buffered by `xml_error_handler` during the just-completed
/// libxml operation, as real `ereport(WARNING, errmsg_internal("%s", text))`
/// (port of xml.c:2253). Called at each seam boundary after libxml returns.
fn flush_xml_warnings() {
    let warnings = XML_ERR_CTX.with(|c| std::mem::take(&mut c.borrow_mut().pending_warnings));
    for w in warnings {
        let _ = elog_seams::ereport_msg::call(::types_error::WARNING, w, None);
    }
}

/// Append a line separator the way C's `appendStringInfoLineSeparator` does:
/// strip trailing newlines, then add one `\n` if the buffer is non-empty.
fn append_line_separator(buf: &mut String) {
    while buf.ends_with('\n') {
        buf.pop();
    }
    if !buf.is_empty() {
        buf.push('\n');
    }
}

/// Port of libxml2's `xmlParserPrintFileContextInternal` (`error.c`). Builds the
/// two context lines `xmlParserPrintFileContext` would print: the offending
/// source line (window of up to 80 chars ending at the error position) followed
/// by a line of blanks (tabs preserved) and a `^` under the error column.
/// Returns the two lines joined by newlines (no trailing newline kept by the
/// caller's chop), or `None` if the input has no current position.
unsafe fn parser_print_file_context(input: *const xmlParserInputHdr) -> Option<String> {
    let cur0 = (*input).cur;
    let base = (*input).base;
    if cur0.is_null() || base.is_null() {
        return None;
    }

    // The buffer is NUL-terminated; bound forward scans by `end` defensively.
    let end = (*input).end;
    let at = |p: *const c_uchar| -> u8 { *p };

    let mut cur = cur0;
    // skip backwards over any end-of-lines
    while cur > base && (at(cur) == b'\n' || at(cur) == b'\r') {
        cur = cur.sub(1);
    }
    // search backwards for beginning-of-line (to max buffer size: 80)
    let mut n: usize = 0;
    while {
        let cont = n < 80 && cur > base && at(cur) != b'\n' && at(cur) != b'\r';
        n += 1;
        cont
    } {
        cur = cur.sub(1);
    }
    if at(cur) == b'\n' || at(cur) == b'\r' {
        cur = cur.add(1);
    }
    // error column relative to line start
    let col = cur0 as usize - cur as usize;

    // copy selected text (up to 80 chars) to a buffer
    let mut content: Vec<u8> = Vec::with_capacity(81);
    while at(cur) != 0
        && at(cur) != b'\n'
        && at(cur) != b'\r'
        && content.len() < 80
        && (end.is_null() || cur < end)
    {
        content.push(at(cur));
        cur = cur.add(1);
    }

    let mut out = String::new();
    out.push_str(&String::from_utf8_lossy(&content));
    out.push('\n');

    // blank line with the problem pointer: replace each char before `col` with
    // a space (tabs kept as tabs), then a caret.
    let mut caret: Vec<u8> = Vec::with_capacity(col + 1);
    let mut i = 0usize;
    while i < col && i < 79 && i < content.len() {
        caret.push(if content[i] == b'\t' { b'\t' } else { b' ' });
        i += 1;
    }
    caret.push(b'^');
    out.push_str(&String::from_utf8_lossy(&caret));

    Some(out)
}

/// Port of `xml_errorHandler` (xml.c). Buffers the libxml diagnostic into the
/// thread-local error context, applying the same domain/level normalization and
/// strictness filtering, and sets `err_occurred` for errors at `XML_ERR_ERROR`+.
unsafe extern "C" fn xml_error_handler(_user_data: *mut c_void, error: *mut c_void) {
    let err = error as *const xmlErrorHdr;
    if err.is_null() {
        return;
    }
    let code = (*err).code;
    let mut domain = (*err).domain;
    let mut level = (*err).level;

    // Older/newer libxml versions report some errors differently; compensate
    // exactly as xml.c does.
    match code {
        XML_WAR_NS_URI => {
            level = XML_ERR_ERROR;
            domain = XML_FROM_NAMESPACE;
        }
        XML_ERR_NS_DECL_ERROR
        | XML_WAR_NS_URI_RELATIVE
        | XML_WAR_NS_COLUMN
        | XML_NS_ERR_XML_NAMESPACE
        | XML_NS_ERR_UNDEFINED_NAMESPACE
        | XML_NS_ERR_QNAME
        | XML_NS_ERR_ATTRIBUTE_REDEFINED
        | XML_NS_ERR_EMPTY => {
            domain = XML_FROM_NAMESPACE;
        }
        _ => {}
    }

    let strictness = XML_ERR_CTX.with(|c| c.borrow().strictness);
    let already_occurred = XML_ERR_CTX.with(|c| c.borrow().err_occurred);

    // Decide whether to act on the error or not (xml.c domain switch).
    match domain {
        XML_FROM_PARSER => {
            // Suppress XML_ERR_NOT_WELL_BALANCED once we already logged an error
            // (cross-version libxml2 behavior compensation).
            if code == XML_ERR_NOT_WELL_BALANCED && already_occurred {
                return;
            }
            // fall through to the accept-regardless block below.
            if code == XML_WAR_UNDECLARED_ENTITY {
                return;
            }
        }
        XML_FROM_NONE | XML_FROM_MEMORY | XML_FROM_IO => {
            if code == XML_WAR_UNDECLARED_ENTITY {
                return;
            }
        }
        _ => {
            // Ignore error if only doing a well-formedness check.
            if strictness == PG_XML_STRICTNESS_WELLFORMED {
                return;
            }
        }
    }

    // Prepare the error message (xml.c errorBuf).
    let mut msg = String::new();
    let line = (*err).line;
    if line > 0 {
        msg.push_str(&format!("line {line}: "));
    }
    // element name, when the error node is an element node.
    let node = (*err).node;
    if !node.is_null() && node_type(node as *mut xmlNode) == XML_ELEMENT_NODE {
        let name = (*(node as *const xmlNodeHdr)).name;
        if !name.is_null() {
            let nm = xmlchar_to_vec(name);
            msg.push_str(&format!("element {}: ", String::from_utf8_lossy(&nm)));
        }
    }
    if !(*err).message.is_null() {
        // `message` is a plain `char*`; `xmlchar_to_vec` reads to the NUL, which
        // is correct for both `char*` and `xmlChar*`.
        let m = xmlchar_to_vec((*err).message as *const c_uchar);
        msg.push_str(&String::from_utf8_lossy(&m));
    } else {
        msg.push_str("(no message provided)");
    }

    // Append the parser file-context lines (the offending source line plus a
    // caret pointing at the column), exactly as C's xml_errorHandler does via
    // xmlParserPrintFileContext (xml.c:2197-2212). C redirects libxml's generic
    // error handler to appendStringInfo and calls xmlParserPrintFileContext on
    // ctxt->input; we instead reconstruct the same two lines from the parser
    // input buffer (port of libxml2's xmlParserPrintFileContextInternal), which
    // is byte-for-byte identical and avoids registering a C-variadic callback.
    let ctxt = (*err).ctxt;
    if !ctxt.is_null() {
        let input = (*(ctxt as *const xmlParserCtxtHdr)).input;
        if !input.is_null() {
            if let Some(ctx) = parser_print_file_context(input) {
                // appendStringInfoLineSeparator(errorBuf): strip trailing
                // newlines, then add one if non-empty, before the context.
                while msg.ends_with('\n') {
                    msg.pop();
                }
                if !msg.is_empty() {
                    msg.push('\n');
                }
                msg.push_str(&ctx);
            }
        }
    }

    // chopStringInfoNewlines(errorBuf) — strip trailing newlines.
    while msg.ends_with('\n') {
        msg.pop();
    }

    const PG_XML_STRICTNESS_LEGACY_LOCAL: i32 = 0;
    if strictness == PG_XML_STRICTNESS_LEGACY_LOCAL {
        XML_ERR_CTX.with(|c| {
            let mut c = c.borrow_mut();
            append_line_separator(&mut c.err_buf);
            c.err_buf.push_str(&msg);
        });
        return;
    }

    if level >= XML_ERR_ERROR {
        XML_ERR_CTX.with(|c| {
            let mut c = c.borrow_mut();
            append_line_separator(&mut c.err_buf);
            c.err_buf.push_str(&msg);
            c.err_occurred = true;
        });
    } else if level >= XML_ERR_WARNING {
        // C ereport(WARNING, errmsg_internal("%s", errorBuf->data)) immediately
        // (xml.c:2253). We can't ereport from inside the libxml-invoked callback
        // safely, so buffer the text; flush_xml_warnings emits it once libxml
        // has returned to the seam. err_occurred stays unset, so the parse still
        // succeeds — matching the not-an-error outcome.
        XML_ERR_CTX.with(|c| c.borrow_mut().pending_warnings.push(msg));
    } else {
        // notice: dropped (we have no NOTICE channel that survives the callback;
        // notices never affect xml.sql output).
    }
}

/// Reset the thread-local error context for a fresh operation, recording the
/// strictness level — port of `pg_xml_init(strictness)`'s context allocation.
fn xml_err_reset(strictness: i32) {
    XML_ERR_CTX.with(|c| {
        let mut c = c.borrow_mut();
        c.strictness = strictness;
        c.err_occurred = false;
        c.err_buf.clear();
        c.pending_warnings.clear();
    });
}

/// True if a libxml error was buffered since the last [`xml_err_reset`] —
/// port of reading `xmlerrcxt->err_occurred`.
fn xml_err_occurred() -> bool {
    XML_ERR_CTX.with(|c| c.borrow().err_occurred)
}

/// The buffered libxml diagnostic text (the `errdetail` payload), empty if none.
fn xml_err_detail() -> String {
    XML_ERR_CTX.with(|c| c.borrow().err_buf.clone())
}

/// Build the `PgError` for a failed libxml operation, attaching the buffered
/// libxml diagnostics as `errdetail` — port of `xml_ereport`.
fn xml_ereport(msg: &str, sqlstate: ::types_error::SqlState) -> PgError {
    let detail = xml_err_detail();
    let mut e = PgError::error(msg.to_string()).with_sqlstate(sqlstate);
    if !detail.is_empty() {
        e = e.with_detail(detail);
    }
    e
}

/* ===================================================================== *
 *  xmlNode / xmlDoc / xmlXPathObject field accessors.
 *
 *  libxml2's public structs ARE part of its stable ABI, but we only need a
 *  handful of fields. To avoid hand-mirroring the whole layout we read them
 *  through tiny `#[repr(C)]` prefixes that match the documented field order.
 *  These layouts are fixed across all libxml2 2.x releases (xml.c relies on
 *  the same stability, e.g. reading `doc->encoding`, `node->type`,
 *  `node->children`, `node->next`, `node->prev`, `xpathobj->type`).
 * ===================================================================== */

/// Prefix of `struct _xmlNode` (tree.h) up through the fields xml.c reads.
#[repr(C)]
struct xmlNodeHdr {
    _private: *mut c_void,
    type_: c_int,
    name: *const c_uchar,
    children: *mut xmlNode,
    last: *mut xmlNode,
    parent: *mut xmlNode,
    next: *mut xmlNode,
    prev: *mut xmlNode,
    doc: *mut xmlDoc,
}

/// Prefix of `struct _xmlDoc` (tree.h) up through `standalone`/`encoding`.
#[repr(C)]
struct xmlDocHdr {
    _private: *mut c_void,
    type_: c_int,
    name: *const c_char,
    children: *mut xmlNode,
    last: *mut xmlNode,
    parent: *mut xmlNode,
    next: *mut xmlNode,
    prev: *mut xmlNode,
    doc: *mut xmlDoc,
    // doc-specific:
    compression: c_int,
    standalone: c_int,
    int_subset: *mut c_void,
    ext_subset: *mut c_void,
    old_ns: *mut c_void,
    version: *const c_uchar,
    encoding: *const c_uchar,
}

/// `struct _xmlNodeSet` (xpath.h). `xmlXPathNodeSetGetLength` /
/// `xmlXPathNodeSetItem` are header *macros* in libxml2 (not exported
/// symbols), so we read these fields directly, exactly as those macros do.
#[repr(C)]
struct xmlNodeSetHdr {
    node_nr: c_int,
    node_max: c_int,
    node_tab: *mut *mut xmlNode,
}

/// Prefix of `struct _xmlXPathObject` (xpath.h).
#[repr(C)]
struct xmlXPathObjectHdr {
    type_: c_int,
    nodesetval: *mut xmlNodeSet,
    boolval: c_int,
    floatval: f64,
    stringval: *mut c_uchar,
}

#[inline]
unsafe fn node_type(node: *mut xmlNode) -> c_int {
    (*(node as *const xmlNodeHdr)).type_
}
#[inline]
unsafe fn node_children(node: *mut xmlNode) -> *mut xmlNode {
    (*(node as *const xmlNodeHdr)).children
}
#[inline]
unsafe fn node_next(node: *mut xmlNode) -> *mut xmlNode {
    (*(node as *const xmlNodeHdr)).next
}
#[inline]
unsafe fn node_prev(node: *mut xmlNode) -> *mut xmlNode {
    (*(node as *const xmlNodeHdr)).prev
}
#[inline]
unsafe fn doc_set_encoding_utf8(doc: *mut xmlDoc) {
    // doc->encoding = xmlStrdup("UTF-8"); standalone is set separately.
    let hdr = doc as *mut xmlDocHdr;
    (*hdr).encoding = xmlStrdupConst(b"UTF-8\0".as_ptr() as *const c_uchar);
}
#[inline]
unsafe fn doc_set_standalone(doc: *mut xmlDoc, standalone: c_int) {
    (*(doc as *mut xmlDocHdr)).standalone = standalone;
}

extern "C" {
    fn xmlStrdup(cur: *const c_uchar) -> *mut c_uchar;
}
#[inline]
unsafe fn xmlStrdupConst(s: *const c_uchar) -> *const c_uchar {
    xmlStrdup(s) as *const c_uchar
}

/* ===================================================================== *
 *  Small helpers.
 * ===================================================================== */

/// A NUL-terminated copy of `bytes` for passing to libxml as an `xmlChar*`.
fn cstr(bytes: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(bytes.len() + 1);
    v.extend_from_slice(bytes);
    v.push(0);
    v
}

/// Read a NUL-terminated libxml `xmlChar*` into an owned `Vec<u8>` (no trailing
/// NUL), without freeing it.
unsafe fn xmlchar_to_vec(p: *const c_uchar) -> Vec<u8> {
    if p.is_null() {
        return Vec::new();
    }
    let len = xmlStrlen(p) as usize;
    std::slice::from_raw_parts(p as *const u8, len).to_vec()
}

fn oom(msg: &str) -> PgError {
    PgError::error(msg.to_string()).with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Outputs of the owner's `parse_xml_decl` we care about.
struct XmlDecl {
    /// byte length consumed by the declaration (C `*lenp`).
    count: usize,
    /// declared version, if any.
    version: Option<Vec<u8>>,
    /// standalone flag (-1 = not present).
    standalone: i32,
    /// res_code (0 == valid declaration / no declaration).
    res_code: i32,
}

/// Thin wrapper over the owner's `parse_xml_decl(str, &lenp, &version, _, &standalone)`.
fn parse_xml_decl(data: &[u8]) -> PgResult<XmlDecl> {
    let mut count: usize = 0;
    let mut version: Option<Vec<u8>> = None;
    let mut standalone: i32 = 0;
    let res_code = owner::parse_xml_decl(
        data,
        Some(&mut count),
        Some(&mut version),
        None,
        Some(&mut standalone),
    )?;
    Ok(XmlDecl {
        count,
        version,
        standalone,
        res_code,
    })
}

/// libxml entity loader that refuses every external fetch — mirrors
/// `xmlPgEntityLoader` (xml.c), which returns NULL to block external DTDs/URLs.
unsafe extern "C" fn pg_entity_loader(
    _url: *const c_char,
    _id: *const c_char,
    _ctxt: *mut c_void,
) -> *mut c_void {
    core::ptr::null_mut()
}

/// `pg_xml_init(strictness)`: parser init + install our sandboxing handlers and
/// the structured error handler that buffers diagnostics into the thread-local
/// error context (port of xml.c `pg_xml_init`). Resets the per-operation error
/// state; `strictness` selects which libxml message domains are captured.
unsafe fn pg_xml_init(strictness: i32) {
    xmlInitParser();
    xmlSetExternalEntityLoader(Some(pg_entity_loader));
    xmlSetStructuredErrorFunc(core::ptr::null_mut(), Some(xml_error_handler));
    xml_err_reset(strictness);
}

/* ===================================================================== *
 *  Seam bodies.
 * ===================================================================== */

/// C `xml_parse` (xml.c:1748) reduced to its libxml core. `data` is already
/// server-encoded; the in-crate body has handed us the bytes + declared
/// `encoding`. We convert to a NUL-terminated UTF-8 buffer (the in-crate code
/// only calls this on UTF-8-convertible input; non-UTF8 server encodings are
/// converted to UTF-8 by the `any_to_server`/encoding seams before reaching
/// here in xml.c — but since the seam hands us bytes already in the server
/// encoding plus the encoding id, and the only supported path for libxml is
/// UTF-8, we treat the bytes as UTF-8 and let libxml's parser validate).
///
/// Returns `Ok(Ok(true))` when the document/content parses well-formed.
/// A malformed parse becomes `Ok(Err(soft))` so the caller can choose soft vs
/// hard reporting (it currently escalates to a hard error via `?`).
fn xml_parse_libxml(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    preserve_whitespace: bool,
    _encoding: i32,
) -> PgResult<core::result::Result<bool, PgError>> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_WELLFORMED);

        // Decide document vs content (xml.c: parse_xml_decl + xml_doctype_in_content,
        // both ported in the owner crate as pure helpers).
        let mut count: usize = 0;
        let mut parse_as_document = matches!(xmloption_arg, XmlOptionType::XMLOPTION_DOCUMENT);
        let mut version_bytes: Option<Vec<u8>> = None;
        let mut standalone: i32 = 0;

        if !parse_as_document {
            let decl = parse_xml_decl(data)?;
            if decl.res_code != 0 {
                return Ok(Err(PgError::error(
                    "invalid XML content: invalid XML declaration",
                )
                .with_sqlstate(ERRCODE_INVALID_XML_CONTENT)));
            }
            count = decl.count;
            standalone = decl.standalone;
            version_bytes = decl.version;
            let tail = &data[count.min(data.len())..];
            if owner::xml_doctype_in_content(tail)? {
                parse_as_document = true;
            }
        }

        if parse_as_document {
            let ctxt = xmlNewParserCtxt();
            if ctxt.is_null() {
                return Err(oom("could not allocate parser context"));
            }
            let options = XML_PARSE_NOENT
                | XML_PARSE_DTDATTR
                | XML_PARSE_NONET
                | if preserve_whitespace { 0 } else { XML_PARSE_NOBLANKS };
            let buf = cstr(data);
            let doc = xmlCtxtReadDoc(
                ctxt,
                buf.as_ptr() as *const c_uchar,
                core::ptr::null(),
                b"UTF-8\0".as_ptr() as *const c_char,
                options,
            );
            // C: `if (doc == NULL || xmlerrcxt->err_occurred)` — libxml can
            // return a non-NULL doc while having raised an error, so we must
            // honor err_occurred, not just the NULL result.
            let result = if doc.is_null() || xml_err_occurred() {
                let (code, msg) = match xmloption_arg {
                    XmlOptionType::XMLOPTION_DOCUMENT => {
                        (ERRCODE_INVALID_XML_DOCUMENT, "invalid XML document")
                    }
                    _ => (ERRCODE_INVALID_XML_CONTENT, "invalid XML content"),
                };
                if !doc.is_null() {
                    xmlFreeDoc(doc);
                }
                Ok(Err(xml_ereport(msg, code)))
            } else {
                xmlFreeDoc(doc);
                Ok(Ok(true))
            };
            xmlFreeParserCtxt(ctxt);
            result
        } else {
            // content fragment via xmlParseBalancedChunkMemory.
            let version_c = version_bytes.as_ref().map(|v| cstr(v));
            let version_ptr = version_c
                .as_ref()
                .map(|v| v.as_ptr() as *const c_uchar)
                .unwrap_or(core::ptr::null());
            let doc = xmlNewDoc(version_ptr);
            if doc.is_null() {
                return Err(oom("could not allocate XML document"));
            }
            doc_set_encoding_utf8(doc);
            doc_set_standalone(doc, standalone);
            let save = xmlKeepBlanksDefault(if preserve_whitespace { 1 } else { 0 });

            let tail = &data[count.min(data.len())..];
            let result = if !tail.is_empty() {
                let chunk = cstr(tail);
                let mut nodes: *mut xmlNode = core::ptr::null_mut();
                let rc = xmlParseBalancedChunkMemory(
                    doc,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                    0,
                    chunk.as_ptr() as *const c_uchar,
                    &mut nodes,
                );
                if rc != 0 || xml_err_occurred() {
                    Ok(Err(xml_ereport("invalid XML content", ERRCODE_INVALID_XML_CONTENT)))
                } else {
                    Ok(Ok(true))
                }
            } else {
                Ok(Ok(true))
            };

            xmlKeepBlanksDefault(save);
            xmlFreeDoc(doc);
            result
        }
    }
}

/// C `xmltext` (xml.c:526) libxml core: `xmlEncodeSpecialChars(NULL, arg)`.
fn encode_special_chars(arg: &[u8]) -> PgResult<Vec<u8>> {
    unsafe {
        let input = cstr(arg);
        let out = xmlEncodeSpecialChars(core::ptr::null(), input.as_ptr() as *const c_uchar);
        if out.is_null() {
            return Err(oom("could not allocate xmlBuffer"));
        }
        let v = xmlchar_to_vec(out);
        xmlFree(out as *mut c_void);
        Ok(v)
    }
}

/// C `xmltotext_with_options` (xml.c:638) — parse then optionally indent-serialize.
/// The seam is only reached for the `indent` (or DOCUMENT) path; the non-indent
/// content fast-return is handled in the owner crate before the seam call.
fn serialize_with_options(
    data: &[u8],
    xmloption_arg: XmlOptionType,
    indent: bool,
    _encoding: i32,
) -> PgResult<Vec<u8>> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_ALL);

        // Parse (preserve_whitespace = !indent), tracking doc vs content.
        let preserve_whitespace = !indent;
        let mut count: usize = 0;
        let mut parse_as_document = matches!(xmloption_arg, XmlOptionType::XMLOPTION_DOCUMENT);
        let mut version_bytes: Option<Vec<u8>> = None;
        let mut standalone: i32 = 0;

        if !parse_as_document {
            let decl = parse_xml_decl(data)?;
            count = decl.count;
            standalone = decl.standalone;
            version_bytes = decl.version;
            let tail = &data[count.min(data.len())..];
            if owner::xml_doctype_in_content(tail)? {
                parse_as_document = true;
            }
        }

        let doc;
        let mut content_nodes: *mut xmlNode = core::ptr::null_mut();
        if parse_as_document {
            let ctxt = xmlNewParserCtxt();
            if ctxt.is_null() {
                return Err(oom("could not allocate parser context"));
            }
            let options = XML_PARSE_NOENT
                | XML_PARSE_DTDATTR
                | XML_PARSE_NONET
                | if preserve_whitespace { 0 } else { XML_PARSE_NOBLANKS };
            let buf = cstr(data);
            doc = xmlCtxtReadDoc(
                ctxt,
                buf.as_ptr() as *const c_uchar,
                core::ptr::null(),
                b"UTF-8\0".as_ptr() as *const c_char,
                options,
            );
            xmlFreeParserCtxt(ctxt);
            if doc.is_null() {
                return Err(PgError::error("not an XML document")
                    .with_sqlstate(::types_error::ERRCODE_NOT_AN_XML_DOCUMENT));
            }
        } else {
            let version_c = version_bytes.as_ref().map(|v| cstr(v));
            let version_ptr = version_c
                .as_ref()
                .map(|v| v.as_ptr() as *const c_uchar)
                .unwrap_or(core::ptr::null());
            doc = xmlNewDoc(version_ptr);
            if doc.is_null() {
                return Err(oom("could not allocate XML document"));
            }
            doc_set_encoding_utf8(doc);
            doc_set_standalone(doc, standalone);
            let save = xmlKeepBlanksDefault(if preserve_whitespace { 1 } else { 0 });
            let tail = &data[count.min(data.len())..];
            if !tail.is_empty() {
                let chunk = cstr(tail);
                let rc = xmlParseBalancedChunkMemory(
                    doc,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                    0,
                    chunk.as_ptr() as *const c_uchar,
                    &mut content_nodes,
                );
                if rc != 0 {
                    xmlKeepBlanksDefault(save);
                    xmlFreeDoc(doc);
                    return Err(PgError::error("not an XML document")
                        .with_sqlstate(::types_error::ERRCODE_NOT_AN_XML_DOCUMENT));
                }
            }
            xmlKeepBlanksDefault(save);
        }

        // If we weren't asked to indent, the owner returns the input unchanged
        // and never calls us; but guard anyway.
        if !indent {
            let out = data.to_vec();
            xmlFreeDoc(doc);
            return Ok(out);
        }

        // Indent-serialize.  C trims the trailing newline based on the *requested*
        // xmloption (xmloption_arg), NOT the parsed type — so a CONTENT request that
        // parses as a DOCUMENT (e.g. `<!DOCTYPE a><a/>`) keeps the trailing newline.
        let requested_document = matches!(xmloption_arg, XmlOptionType::XMLOPTION_DOCUMENT);
        let result =
            serialize_indented(doc, data, parse_as_document, requested_document, content_nodes);
        xmlFreeDoc(doc);
        result
    }
}

unsafe fn serialize_indented(
    doc: *mut xmlDoc,
    data: &[u8],
    parse_as_document: bool,
    requested_document: bool,
    content_nodes: *mut xmlNode,
) -> PgResult<Vec<u8>> {
    let buf = xmlBufferCreate();
    if buf.is_null() {
        return Err(oom("could not allocate xmlBuffer"));
    }

    // Detect whether the input had an XML declaration (parse_xml_decl, ported).
    let decl_len = parse_xml_decl(data).map(|d| d.count).unwrap_or(0);

    let save_opts = if decl_len == 0 {
        XML_SAVE_NO_DECL | XML_SAVE_FORMAT
    } else {
        XML_SAVE_FORMAT
    };
    let ctxt = xmlSaveToBuffer(buf, core::ptr::null(), save_opts);
    if ctxt.is_null() {
        xmlBufferFree(buf);
        return Err(oom("could not allocate xmlSaveCtxt"));
    }

    if parse_as_document {
        if xmlSaveDoc(ctxt, doc) == -1 {
            xmlSaveClose(ctxt);
            xmlBufferFree(buf);
            return Err(oom("could not save document to xmlBuffer"));
        }
    } else if !content_nodes.is_null() {
        // Build a fake "content-root" container and serialize its children with
        // newlines between non-text nodes (xml.c:776-810).
        let root = xmlNewNode(core::ptr::null_mut(), b"content-root\0".as_ptr() as *const c_uchar);
        if root.is_null() {
            xmlSaveClose(ctxt);
            xmlBufferFree(buf);
            return Err(oom("could not allocate xml node"));
        }
        let oldroot = xmlDocSetRootElement(doc, root);
        if !oldroot.is_null() {
            xmlFreeNode(oldroot);
        }
        xmlAddChildList(root, content_nodes);

        let newline = xmlNewDocText(core::ptr::null_mut(), b"\n\0".as_ptr() as *const c_uchar);
        if newline.is_null() {
            xmlSaveClose(ctxt);
            xmlBufferFree(buf);
            return Err(oom("could not allocate xml node"));
        }

        let mut node = node_children(root);
        while !node.is_null() {
            if node_type(node) != XML_TEXT_NODE && !node_prev(node).is_null()
                && xmlSaveTree(ctxt, newline) == -1
            {
                xmlFreeNode(newline);
                xmlSaveClose(ctxt);
                xmlBufferFree(buf);
                return Err(oom("could not save newline to xmlBuffer"));
            }
            if xmlSaveTree(ctxt, node) == -1 {
                xmlFreeNode(newline);
                xmlSaveClose(ctxt);
                xmlBufferFree(buf);
                return Err(oom("could not save content to xmlBuffer"));
            }
            node = node_next(node);
        }
        xmlFreeNode(newline);
    }

    if xmlSaveClose(ctxt) == -1 {
        xmlBufferFree(buf);
        return Err(PgError::error("could not close xmlSaveCtxtPtr".to_string())
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    let content = xmlBufferContent(buf);
    let len = xmlBufferLength(buf) as usize;
    let bytes = if content.is_null() {
        Vec::new()
    } else {
        std::slice::from_raw_parts(content as *const u8, len).to_vec()
    };
    xmlBufferFree(buf);

    // xmlDocContentDumpOutput may add a trailing newline; C trims it only when the
    // *requested* xmloption was DOCUMENT (xml.c:822 — `xmloption_arg`), not the
    // parsed type.  A CONTENT request keeps the trailing newline via
    // xmlBuffer_to_xmltype.
    let out = if requested_document {
        let mut end = bytes.len();
        while end > 0 && (bytes[end - 1] == b'\n' || bytes[end - 1] == b'\r') {
            end -= 1;
        }
        bytes[..end].to_vec()
    } else {
        bytes
    };
    Ok(out)
}

/// C `xmlelement` (xml.c:864) libxml core via `xmlTextWriter*`.
fn build_element(
    name: String,
    named_args: Vec<(String, Option<String>)>,
    content: Vec<String>,
) -> PgResult<Vec<u8>> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_ALL);
        let buf = xmlBufferCreate();
        if buf.is_null() {
            return Err(oom("could not allocate xmlBuffer"));
        }
        let writer = xmlNewTextWriterMemory(buf, 0);
        if writer.is_null() {
            xmlBufferFree(buf);
            return Err(oom("could not allocate xmlTextWriter"));
        }

        let name_c = cstr(name.as_bytes());
        xmlTextWriterStartElement(writer, name_c.as_ptr() as *const c_uchar);

        for (argname, value) in &named_args {
            if let Some(str) = value {
                let n = cstr(argname.as_bytes());
                let v = cstr(str.as_bytes());
                xmlTextWriterWriteAttribute(
                    writer,
                    n.as_ptr() as *const c_uchar,
                    v.as_ptr() as *const c_uchar,
                );
            }
        }

        for str in &content {
            let c = cstr(str.as_bytes());
            xmlTextWriterWriteRaw(writer, c.as_ptr() as *const c_uchar);
        }

        xmlTextWriterEndElement(writer);
        // MUST flush by freeing the writer before reading the buffer.
        xmlFreeTextWriter(writer);

        let result = buffer_to_vec(buf);
        xmlBufferFree(buf);
        Ok(result)
    }
}

/// C BYTEAOID arm of `map_sql_value_to_xml_value` (xml.c:2615) — base64/binhex.
fn encode_binary(bytes: &[u8], binary: XmlBinaryType) -> PgResult<String> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_ALL);
        let buf = xmlBufferCreate();
        if buf.is_null() {
            return Err(oom("could not allocate xmlBuffer"));
        }
        let writer = xmlNewTextWriterMemory(buf, 0);
        if writer.is_null() {
            xmlBufferFree(buf);
            return Err(oom("could not allocate xmlTextWriter"));
        }
        let data_ptr = bytes.as_ptr() as *const c_char;
        let len = bytes.len() as c_int;
        match binary {
            XmlBinaryType::XMLBINARY_BASE64 => {
                xmlTextWriterWriteBase64(writer, data_ptr, 0, len);
            }
            XmlBinaryType::XMLBINARY_HEX => {
                xmlTextWriterWriteBinHex(writer, data_ptr, 0, len);
            }
        }
        xmlFreeTextWriter(writer);

        let v = buffer_to_vec(buf);
        xmlBufferFree(buf);
        Ok(String::from_utf8_lossy(&v).into_owned())
    }
}

unsafe fn buffer_to_vec(buf: *mut xmlBuffer) -> Vec<u8> {
    let content = xmlBufferContent(buf);
    let len = xmlBufferLength(buf) as usize;
    if content.is_null() {
        Vec::new()
    } else {
        std::slice::from_raw_parts(content as *const u8, len).to_vec()
    }
}

/// C `xpath_internal` + `xml_xpathobjtoxmlarray` (xml.c:4323 / 4243).
fn xpath_eval(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: &[(String, String)],
    count_only: bool,
    database_encoding: i32,
) -> PgResult<Vec<Vec<u8>>> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_ALL);

        // In a UTF8 database, skip any leading xml declaration (xml.c).
        const PG_UTF8: i32 = 6;
        let xmldecl_len = if database_encoding == PG_UTF8 {
            parse_xml_decl(data).map(|d| d.count).unwrap_or(0)
        } else {
            0
        };
        let payload = &data[xmldecl_len.min(data.len())..];

        let ctxt = xmlNewParserCtxt();
        if ctxt.is_null() {
            return Err(oom("could not allocate parser context"));
        }
        let doc = xmlCtxtReadMemory(
            ctxt,
            payload.as_ptr() as *const c_char,
            payload.len() as c_int,
            core::ptr::null(),
            core::ptr::null(),
            0,
        );
        // C `xpath_internal`: `if (doc == NULL || xmlerrcxt->err_occurred)`.
        if doc.is_null() || xml_err_occurred() {
            if !doc.is_null() {
                xmlFreeDoc(doc);
            }
            xmlFreeParserCtxt(ctxt);
            return Err(xml_ereport(
                "could not parse XML document",
                ERRCODE_INVALID_XML_DOCUMENT,
            ));
        }
        // The parse succeeded; emit any buffered libxml WARNINGs now (e.g. the
        // relative-namespace warning), matching C's immediate ereport(WARNING)
        // ordering before the XPath result is produced.
        flush_xml_warnings();

        let xpathctx = xmlXPathNewContext(doc);
        if xpathctx.is_null() {
            xmlFreeDoc(doc);
            xmlFreeParserCtxt(ctxt);
            return Err(oom("could not allocate XPath context"));
        }
        // xml.c:4426 `xpathctx->node = (xmlNodePtr) doc;` — evaluate the XPath
        // expression with the document node as the context node, so relative
        // location paths (e.g. `root`) match against the document's children.
        xpathctx_set_node(xpathctx, doc as *mut xmlNode);

        let cleanup = |xpathobj: *mut xmlXPathObject,
                       xpathcomp: *mut xmlXPathCompExpr,
                       xpathctx: *mut xmlXPathContext,
                       doc: *mut xmlDoc,
                       ctxt: *mut xmlParserCtxt| {
            if !xpathobj.is_null() {
                xmlXPathFreeObject(xpathobj);
            }
            if !xpathcomp.is_null() {
                xmlXPathFreeCompExpr(xpathcomp);
            }
            if !xpathctx.is_null() {
                xmlXPathFreeContext(xpathctx);
            }
            if !doc.is_null() {
                xmlFreeDoc(doc);
            }
            if !ctxt.is_null() {
                xmlFreeParserCtxt(ctxt);
            }
        };

        // Register namespaces.
        for (name, uri) in namespaces {
            let n = cstr(name.as_bytes());
            let u = cstr(uri.as_bytes());
            if xmlXPathRegisterNs(
                xpathctx,
                n.as_ptr() as *const c_uchar,
                u.as_ptr() as *const c_uchar,
            ) != 0
            {
                cleanup(core::ptr::null_mut(), core::ptr::null_mut(), xpathctx, doc, ctxt);
                return Err(PgError::error(format!(
                    "could not register XML namespace with name \"{name}\" and URI \"{uri}\""
                )));
            }
        }

        let expr = cstr(xpath_expr);
        let xpathcomp = xmlXPathCtxtCompile(xpathctx, expr.as_ptr() as *const c_uchar);
        if xpathcomp.is_null() {
            cleanup(core::ptr::null_mut(), core::ptr::null_mut(), xpathctx, doc, ctxt);
            return Err(PgError::error("invalid XPath expression".to_string())
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY));
        }

        let xpathobj = xmlXPathCompiledEval(xpathcomp, xpathctx);
        if xpathobj.is_null() {
            cleanup(core::ptr::null_mut(), xpathcomp, xpathctx, doc, ctxt);
            return Err(PgError::error("could not create XPath object".to_string())
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY));
        }

        let result = xpathobj_to_xmlarray(xpathobj, count_only);
        cleanup(xpathobj, xpathcomp, xpathctx, doc, ctxt);
        result
    }
}

/// C `xml_xpathobjtoxmlarray` (xml.c:4243). When `count_only` (astate==NULL in
/// C) we return one placeholder entry per match so the caller's `.len()` equals
/// the C `result` count without per-node serialization.
unsafe fn xpathobj_to_xmlarray(
    xpathobj: *mut xmlXPathObject,
    count_only: bool,
) -> PgResult<Vec<Vec<u8>>> {
    let hdr = &*(xpathobj as *const xmlXPathObjectHdr);
    match hdr.type_ {
        XPATH_NODESET => {
            if hdr.nodesetval.is_null() {
                return Ok(Vec::new());
            }
            // xmlXPathNodeSetGetLength(ns) == ns ? ns->nodeNr : 0 (header macro).
            let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
            let n = ns.node_nr;
            let mut out = Vec::with_capacity(n.max(0) as usize);
            for i in 0..n {
                if count_only {
                    out.push(Vec::new());
                    continue;
                }
                // xmlXPathNodeSetItem(ns, i) == ns->nodeTab[i] (header macro).
                let node = *ns.node_tab.add(i as usize);
                out.push(xml_xmlnodetoxmltype(node)?);
            }
            Ok(out)
        }
        XPATH_BOOLEAN => {
            if count_only {
                return Ok(vec![Vec::new()]);
            }
            // Float8GetDatum/BoolGetDatum -> map_sql_value_to_xml_value(BOOLOID).
            let s = if hdr.boolval != 0 { "true" } else { "false" };
            Ok(vec![s.as_bytes().to_vec()])
        }
        XPATH_NUMBER => {
            if count_only {
                return Ok(vec![Vec::new()]);
            }
            // map_sql_value_to_xml_value(FLOAT8OID) == float8out(floatval).
            Ok(vec![float8out(hdr.floatval).into_bytes()])
        }
        XPATH_STRING => {
            if count_only {
                return Ok(vec![Vec::new()]);
            }
            // map_sql_value_to_xml_value(CSTRINGOID, escape=true) == escape_xml(str).
            let s = xmlchar_to_vec(hdr.stringval);
            Ok(vec![owner::escape_xml(&s)])
        }
        other => Err(PgError::error(format!(
            "xpath expression result type {other} is unsupported"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

/// C `xml_xmlnodetoxmltype` (xml.c:4151). For attr/text nodes, escape the
/// cast-to-string; otherwise copy + `xmlNodeDump` the subtree.
unsafe fn xml_xmlnodetoxmltype(cur: *mut xmlNode) -> PgResult<Vec<u8>> {
    let t = node_type(cur);
    if t != XML_ATTRIBUTE_NODE && t != XML_TEXT_NODE {
        let buf = xmlBufferCreate();
        if buf.is_null() {
            return Err(oom("could not allocate xmlBuffer"));
        }
        let cur_copy = xmlCopyNode(cur, 1);
        if cur_copy.is_null() {
            xmlBufferFree(buf);
            return Err(oom("could not copy node"));
        }
        let is_doc = node_type(cur_copy) == XML_DOCUMENT_NODE;
        let bytes = xmlNodeDump(buf, core::ptr::null_mut(), cur_copy, 0, 0);
        if bytes == -1 {
            // free per xml.c: xmlFreeDoc for doc-node copies, else xmlFreeNode.
            if is_doc {
                xmlFreeDoc(cur_copy as *mut xmlDoc);
            } else {
                xmlFreeNode(cur_copy);
            }
            xmlBufferFree(buf);
            return Err(oom("could not dump node"));
        }
        let v = buffer_to_vec(buf);
        if is_doc {
            xmlFreeDoc(cur_copy as *mut xmlDoc);
        } else {
            xmlFreeNode(cur_copy);
        }
        xmlBufferFree(buf);
        Ok(v)
    } else {
        let str = xmlXPathCastNodeToString(cur);
        let raw = xmlchar_to_vec(str);
        if !str.is_null() {
            xmlFree(str as *mut c_void);
        }
        Ok(owner::escape_xml(&raw))
    }
}

/// `float8out` equivalent for the XPATH_NUMBER scalar. PostgreSQL with the
/// default `extra_float_digits` (>= 1, PG12+) emits the shortest round-trippable
/// decimal, which is exactly Rust's `{}` float formatting. NaN/Inf map to the
/// SQL spellings PostgreSQL uses.
fn float8out(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v < 0.0 {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        }
    } else {
        let s = format!("{v}");
        // Rust prints integral floats as "1" while PG float8out keeps no ".0"
        // either, so the bare `{}` form matches.
        s
    }
}

/* ===================================================================== *
 *  Tiny per-byte predicates (xmlIsBlank_ch / xmlGetUTF8Char).
 * ===================================================================== */

/// C `xmlIsBlank_ch(c)` — XML S production for a single byte.
fn is_blank_ch(c: u8) -> PgResult<bool> {
    Ok(c == 0x20 || c == 0x9 || c == 0xA || c == 0xD)
}

/// C `xmlGetUTF8Char(utf8, &len)` — decode one UTF-8 codepoint; -1 on error.
/// Faithful reproduction of libxml2's `xmlGetUTF8Char` (encoding.c).
fn get_utf8_char(utf8: &[u8]) -> PgResult<i32> {
    if utf8.is_empty() {
        return Ok(-1);
    }
    let c0 = utf8[0] as u32;
    // 1-byte
    if c0 & 0x80 == 0 {
        return Ok(c0 as i32);
    }
    // multi-byte: determine length
    let (need, mut val): (usize, u32) = if c0 & 0xE0 == 0xC0 {
        (2, c0 & 0x1F)
    } else if c0 & 0xF0 == 0xE0 {
        (3, c0 & 0x0F)
    } else if c0 & 0xF8 == 0xF0 {
        (4, c0 & 0x07)
    } else {
        return Ok(-1);
    };
    if utf8.len() < need {
        return Ok(-1);
    }
    for &b in &utf8[1..need] {
        if b & 0xC0 != 0x80 {
            return Ok(-1);
        }
        val = (val << 6) | (b as u32 & 0x3F);
    }
    Ok(val as i32)
}

/* ===================================================================== *
 *  XMLTABLE table builder — port of xml.c `XmlTable*` (`#ifdef USE_LIBXML`).
 *
 *  C stores `XmlTableBuilderData` in `TableFuncScanState->opaque`; a
 *  TableFuncScan always runs to completion (single pass into a tuplestore)
 *  before any other node, so the builder state is held in a per-backend
 *  thread-local here (set by InitOpaque, cleared by DestroyOpaque). Raw libxml
 *  pointers stay internal to this provider, exactly as in C.
 * ===================================================================== */

/// Prefix of `struct _xmlXPathContext` (xpath.h) up through `node`, the only
/// field xml.c writes (`xpathcxt->node = cur`). `doc` then `node` are the first
/// two pointer fields and stable across libxml2 2.x.
#[repr(C)]
struct xmlXPathContextHdr {
    doc: *mut xmlDoc,
    node: *mut xmlNode,
}

#[inline]
unsafe fn xpathctx_set_node(ctx: *mut xmlXPathContext, node: *mut xmlNode) {
    (*(ctx as *mut xmlXPathContextHdr)).node = node;
}

/// Per-scan XMLTABLE builder state — the libxml half of C's
/// `XmlTableBuilderData` (the `magic`/`natts` bookkeeping is implicit: the
/// thread-local being `Some` is the magic check, and `xpathscomp.len()` is
/// `natts`).
struct XmlTableBuilderData {
    /// `xmlParserCtxtPtr ctxt`
    ctxt: *mut xmlParserCtxt,
    /// `xmlDocPtr doc`
    doc: *mut xmlDoc,
    /// `xmlXPathContextPtr xpathcxt`
    xpathcxt: *mut xmlXPathContext,
    /// `xmlXPathCompExprPtr xpathcomp` — the compiled row filter.
    xpathcomp: *mut xmlXPathCompExpr,
    /// `xmlXPathObjectPtr xpathobj` — the row-filter result node set.
    xpathobj: *mut xmlXPathObject,
    /// `xmlXPathCompExprPtr *xpathscomp` — one compiled XPath per column.
    xpathscomp: Vec<*mut xmlXPathCompExpr>,
    /// `long int row_count` — 1-based cursor into the row node set.
    row_count: i64,
}

thread_local! {
    /// `TableFuncScanState->opaque` — at most one live XMLTABLE scan per backend
    /// (the single-pass invariant the C `XmlTableInitOpaque` comment relies on).
    static XMLTABLE_BUILDER: std::cell::RefCell<Option<XmlTableBuilderData>> =
        const { std::cell::RefCell::new(None) };
}

/// C `GetXmlTableBuilderPrivateData` — the magic check, here the thread-local
/// presence check. The closure runs with the builder borrowed mutably.
fn with_xtcxt<R>(
    fname: &str,
    f: impl FnOnce(&mut XmlTableBuilderData) -> PgResult<R>,
) -> PgResult<R> {
    XMLTABLE_BUILDER.with(|b| {
        let mut b = b.borrow_mut();
        match b.as_mut() {
            Some(xt) => f(xt),
            None => Err(PgError::error(format!(
                "{fname} called with invalid TableFuncScanState"
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
        }
    })
}

/// C `XmlTableInitOpaque` (xml.c:4683).
fn xmltable_init_opaque(natts: i32) -> PgResult<()> {
    unsafe {
        pg_xml_init(PG_XML_STRICTNESS_ALL);
        xmlInitParser();

        let ctxt = xmlNewParserCtxt();
        if ctxt.is_null() || xml_err_occurred() {
            if !ctxt.is_null() {
                xmlFreeParserCtxt(ctxt);
            }
            return Err(xml_ereport(
                "could not allocate parser context",
                ERRCODE_OUT_OF_MEMORY,
            ));
        }

        let xtcxt = XmlTableBuilderData {
            ctxt,
            doc: core::ptr::null_mut(),
            xpathcxt: core::ptr::null_mut(),
            xpathcomp: core::ptr::null_mut(),
            xpathobj: core::ptr::null_mut(),
            xpathscomp: vec![core::ptr::null_mut(); natts.max(0) as usize],
            row_count: 0,
        };
        XMLTABLE_BUILDER.with(|b| {
            // A leftover builder would mean a prior scan was not torn down; the
            // single-pass invariant forbids it, but destroy it defensively.
            if let Some(old) = b.borrow_mut().take() {
                xmltable_free_resources(old);
            }
            *b.borrow_mut() = Some(xtcxt);
        });
        Ok(())
    }
}

/// C `XmlTableSetDocument` (xml.c:4731). `xml_image` is `xml_out_internal`'s
/// encoding-stripped serialization (produced by the consumer crate).
fn xmltable_set_document(xml_image: &[u8]) -> PgResult<()> {
    with_xtcxt("XmlTableSetDocument", |xtcxt| unsafe {
        let doc = xmlCtxtReadMemory(
            xtcxt.ctxt,
            xml_image.as_ptr() as *const c_char,
            xml_image.len() as c_int,
            core::ptr::null(),
            core::ptr::null(),
            0,
        );
        if doc.is_null() || xml_err_occurred() {
            if !doc.is_null() {
                xmlFreeDoc(doc);
            }
            return Err(xml_ereport(
                "could not parse XML document",
                ERRCODE_INVALID_XML_DOCUMENT,
            ));
        }
        let xpathcxt = xmlXPathNewContext(doc);
        if xpathcxt.is_null() || xml_err_occurred() {
            if !xpathcxt.is_null() {
                xmlXPathFreeContext(xpathcxt);
            }
            xmlFreeDoc(doc);
            return Err(xml_ereport(
                "could not allocate XPath context",
                ERRCODE_OUT_OF_MEMORY,
            ));
        }
        // xpathcxt->node = (xmlNodePtr) doc;
        xpathctx_set_node(xpathcxt, doc as *mut xmlNode);

        xtcxt.doc = doc;
        xtcxt.xpathcxt = xpathcxt;
        Ok(())
    })
}

/// C `XmlTableSetNamespace` (xml.c:4788).
fn xmltable_set_namespace(name: Option<&str>, uri: &str) -> PgResult<()> {
    // if (name == NULL) ereport(... "DEFAULT namespace is not supported").
    let name = match name {
        Some(n) => n,
        None => {
            return Err(PgError::error("DEFAULT namespace is not supported".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    };
    with_xtcxt("XmlTableSetNamespace", |xtcxt| unsafe {
        let n = cstr(name.as_bytes());
        let u = cstr(uri.as_bytes());
        if xmlXPathRegisterNs(
            xtcxt.xpathcxt,
            n.as_ptr() as *const c_uchar,
            u.as_ptr() as *const c_uchar,
        ) != 0
        {
            return Err(xml_ereport(
                "could not set XML namespace",
                ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
            ));
        }
        Ok(())
    })
}

/// C `XmlTableSetRowFilter` (xml.c:4814).
fn xmltable_set_row_filter(path: &str) -> PgResult<()> {
    if path.is_empty() {
        return Err(
            PgError::error("row path filter must not be empty string".to_string())
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY),
        );
    }
    with_xtcxt("XmlTableSetRowFilter", |xtcxt| unsafe {
        let xstr = cstr(path.as_bytes());
        let comp = xmlXPathCtxtCompile(xtcxt.xpathcxt, xstr.as_ptr() as *const c_uchar);
        if comp.is_null() || xml_err_occurred() {
            return Err(xml_ereport(
                "invalid XPath expression",
                ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
            ));
        }
        xtcxt.xpathcomp = comp;
        Ok(())
    })
}

/// C `XmlTableSetColumnFilter` (xml.c:4846).
fn xmltable_set_column_filter(path: &str, colnum: i32) -> PgResult<()> {
    if path.is_empty() {
        return Err(
            PgError::error("column path filter must not be empty string".to_string())
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY),
        );
    }
    with_xtcxt("XmlTableSetColumnFilter", |xtcxt| unsafe {
        let xstr = cstr(path.as_bytes());
        let comp = xmlXPathCtxtCompile(xtcxt.xpathcxt, xstr.as_ptr() as *const c_uchar);
        if comp.is_null() || xml_err_occurred() {
            return Err(xml_ereport(
                "invalid XPath expression",
                ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
            ));
        }
        xtcxt.xpathscomp[colnum as usize] = comp;
        Ok(())
    })
}

/// C `XmlTableFetchRow` (xml.c:4881).
fn xmltable_fetch_row() -> PgResult<bool> {
    with_xtcxt("XmlTableFetchRow", |xtcxt| unsafe {
        // xmlSetStructuredErrorFunc(xtCxt->xmlerrcxt, xml_errorHandler) — our
        // handler is process-global (thread-local context), already installed by
        // pg_xml_init; re-assert it as C does.
        xmlSetStructuredErrorFunc(core::ptr::null_mut(), Some(xml_error_handler));

        if xtcxt.xpathobj.is_null() {
            xtcxt.xpathobj = xmlXPathCompiledEval(xtcxt.xpathcomp, xtcxt.xpathcxt);
            if xtcxt.xpathobj.is_null() || xml_err_occurred() {
                return Err(xml_ereport(
                    "could not create XPath object",
                    ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
                ));
            }
            xtcxt.row_count = 0;
        }

        let hdr = &*(xtcxt.xpathobj as *const xmlXPathObjectHdr);
        if hdr.type_ == XPATH_NODESET && !hdr.nodesetval.is_null() {
            let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
            let prev = xtcxt.row_count;
            xtcxt.row_count += 1;
            if prev < ns.node_nr as i64 {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// C `XmlTableGetValue` (xml.c:4926), reduced to its libxml core: evaluate the
/// column XPath against the current row node and return the textual value (or
/// `None` for the C `*isnull = true`). `InputFunctionCall` stays in the executor.
fn xmltable_get_value(
    colnum: i32,
    is_xml: bool,
    is_numeric_category: bool,
) -> PgResult<Option<String>> {
    with_xtcxt("XmlTableGetValue", |xtcxt| unsafe {
        xmlSetStructuredErrorFunc(core::ptr::null_mut(), Some(xml_error_handler));

        debug_assert!(!xtcxt.xpathobj.is_null());
        debug_assert!(!xtcxt.xpathscomp[colnum as usize].is_null());

        // cur = nodesetval->nodeTab[row_count - 1]; xpathcxt->node = cur;
        let obj_hdr = &*(xtcxt.xpathobj as *const xmlXPathObjectHdr);
        let ns = &*(obj_hdr.nodesetval as *const xmlNodeSetHdr);
        let cur = *ns.node_tab.add((xtcxt.row_count - 1) as usize);
        xpathctx_set_node(xtcxt.xpathcxt, cur);

        let xpathobj = xmlXPathCompiledEval(xtcxt.xpathscomp[colnum as usize], xtcxt.xpathcxt);
        if xpathobj.is_null() || xml_err_occurred() {
            if !xpathobj.is_null() {
                xmlXPathFreeObject(xpathobj);
            }
            return Err(xml_ereport(
                "could not create XPath object",
                ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
            ));
        }

        let result = xmltable_value_from_xpathobj(xpathobj, is_xml, is_numeric_category);
        xmlXPathFreeObject(xpathobj);
        result
    })
}

/// The four-case value extraction in `XmlTableGetValue`'s `PG_TRY` body.
unsafe fn xmltable_value_from_xpathobj(
    xpathobj: *mut xmlXPathObject,
    is_xml: bool,
    is_numeric_category: bool,
) -> PgResult<Option<String>> {
    let hdr = &*(xpathobj as *const xmlXPathObjectHdr);
    match hdr.type_ {
        XPATH_NODESET => {
            let count = if hdr.nodesetval.is_null() {
                0
            } else {
                (*(hdr.nodesetval as *const xmlNodeSetHdr)).node_nr
            };
            if hdr.nodesetval.is_null() || count == 0 {
                // *isnull = true
                Ok(None)
            } else if is_xml {
                // Concatenate serialized values.
                let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
                let mut buf: Vec<u8> = Vec::new();
                for i in 0..count {
                    let node = *ns.node_tab.add(i as usize);
                    buf.extend_from_slice(&xml_xmlnodetoxmltype(node)?);
                }
                Ok(Some(string_from_utf8_lossy_owned(buf)))
            } else {
                // For non-XML: one node => content; more than one => error.
                if count > 1 {
                    return Err(PgError::error(
                        "more than one value returned by column XPath expression".to_string(),
                    )
                    .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION));
                }
                let str = xmlXPathCastNodeSetToString(hdr.nodesetval);
                let v = if str.is_null() {
                    String::new()
                } else {
                    let s = string_from_utf8_lossy_owned(xmlchar_to_vec(str));
                    xmlFree(str as *mut c_void);
                    s
                };
                Ok(Some(v))
            }
        }
        XPATH_STRING => {
            // Content should be escaped when the target will be XML.
            let raw = xmlchar_to_vec(hdr.stringval);
            let v = if is_xml {
                string_from_utf8_lossy_owned(owner::escape_xml(&raw))
            } else {
                string_from_utf8_lossy_owned(raw)
            };
            Ok(Some(v))
        }
        XPATH_BOOLEAN => {
            // Allow implicit casting from boolean to numbers.
            let str = if !is_numeric_category {
                xmlXPathCastBooleanToString(hdr.boolval)
            } else {
                xmlXPathCastNumberToString(xmlXPathCastBooleanToNumber(hdr.boolval))
            };
            let v = if str.is_null() {
                String::new()
            } else {
                let s = string_from_utf8_lossy_owned(xmlchar_to_vec(str));
                xmlFree(str as *mut c_void);
                s
            };
            Ok(Some(v))
        }
        XPATH_NUMBER => {
            let str = xmlXPathCastNumberToString(hdr.floatval);
            let v = if str.is_null() {
                String::new()
            } else {
                let s = string_from_utf8_lossy_owned(xmlchar_to_vec(str));
                xmlFree(str as *mut c_void);
                s
            };
            Ok(Some(v))
        }
        other => Err(PgError::error(format!("unexpected XPath object type {other}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

/// Decode libxml UTF-8 output bytes into an owned `String`. libxml always emits
/// UTF-8; a malformed byte would be a libxml bug, so a lossy decode is the safe
/// faithful choice (the executor's InputFunctionCall re-validates per type).
fn string_from_utf8_lossy_owned(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

/// C `XmlTableDestroyOpaque` (xml.c:5078) — free libxml resources and clear the
/// builder state.
fn xmltable_destroy_opaque() -> PgResult<()> {
    XMLTABLE_BUILDER.with(|b| {
        if let Some(xt) = b.borrow_mut().take() {
            unsafe {
                xmlSetStructuredErrorFunc(core::ptr::null_mut(), Some(xml_error_handler));
            }
            xmltable_free_resources(xt);
        }
    });
    Ok(())
}

/// The resource-freeing tail shared by `XmlTableDestroyOpaque` and the defensive
/// re-init cleanup (the libxml-free sequence of xml.c:5078).
fn xmltable_free_resources(xt: XmlTableBuilderData) {
    unsafe {
        for comp in &xt.xpathscomp {
            if !comp.is_null() {
                xmlXPathFreeCompExpr(*comp);
            }
        }
        if !xt.xpathobj.is_null() {
            xmlXPathFreeObject(xt.xpathobj);
        }
        if !xt.xpathcomp.is_null() {
            xmlXPathFreeCompExpr(xt.xpathcomp);
        }
        if !xt.xpathcxt.is_null() {
            xmlXPathFreeContext(xt.xpathcxt);
        }
        if !xt.doc.is_null() {
            xmlFreeDoc(xt.doc);
        }
        if !xt.ctxt.is_null() {
            xmlFreeParserCtxt(xt.ctxt);
        }
    }
}

/* ===================================================================== *
 *  Install.
 * ===================================================================== */
pub fn install() {
    seams::have_libxml::set(|| true);
    seams::is_blank_ch::set(is_blank_ch);
    seams::get_utf8_char::set(get_utf8_char);
    seams::xml_parse_libxml::set(xml_parse_libxml);
    seams::encode_special_chars::set(encode_special_chars);
    seams::serialize_with_options::set(serialize_with_options);
    seams::build_element::set(build_element);
    seams::encode_binary::set(encode_binary);
    seams::xpath_eval::set(xpath_eval);
    seams::xmltable_init_opaque::set(xmltable_init_opaque);
    seams::xmltable_set_document::set(xmltable_set_document);
    seams::xmltable_set_namespace::set(xmltable_set_namespace);
    seams::xmltable_set_row_filter::set(xmltable_set_row_filter);
    seams::xmltable_set_column_filter::set(xmltable_set_column_filter);
    seams::xmltable_fetch_row::set(xmltable_fetch_row);
    seams::xmltable_get_value::set(xmltable_get_value);
    seams::xmltable_destroy_opaque::set(xmltable_destroy_opaque);
}
