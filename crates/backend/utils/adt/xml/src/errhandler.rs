//! pg_xml_init/xml_errorHandler/xml_ereport: libxml diagnostics buffer into
//! a thread-local PgXmlErrorContext analog (String scratch: the handler runs
//! inside a libxml callback, no mcx exists there).

use core::ffi::{c_uchar, c_void};

use ::types_error::{PgError, SqlState, WARNING};

use crate::libxml::{
    self, xml2, xmlErrorHdr, xmlNode, xmlNodeHdr, xmlParserCtxtHdr, xmlParserInputHdr,
    XML_ELEMENT_NODE,
};

pub const PG_XML_STRICTNESS_LEGACY: i32 = 0;
pub const PG_XML_STRICTNESS_WELLFORMED: i32 = 1;
pub const PG_XML_STRICTNESS_ALL: i32 = 2;

// xmlErrorDomain / xmlErrorLevel values (xmlerror.h).
const XML_FROM_NONE: i32 = 0;
const XML_FROM_PARSER: i32 = 1;
const XML_FROM_NAMESPACE: i32 = 3;
const XML_FROM_IO: i32 = 13;
const XML_FROM_MEMORY: i32 = 15;
const XML_ERR_WARNING: i32 = 1;
const XML_ERR_ERROR: i32 = 2;

// xmlParserErrors ORDINALS (xmlerror.h enum order, not the scrambled doc
// numbering: XML_WAR_UNDECLARED_ENTITY is 27, not 98 — wrong value escalates
// C-suppressed DTD entity warnings).
const XML_ERR_NOT_WELL_BALANCED: i32 = 85;
const XML_WAR_UNDECLARED_ENTITY: i32 = 27;
const XML_WAR_NS_URI: i32 = 99;
const XML_WAR_NS_URI_RELATIVE: i32 = 100;
const XML_ERR_NS_DECL_ERROR: i32 = 35;
const XML_WAR_NS_COLUMN: i32 = 106;
const XML_NS_ERR_XML_NAMESPACE: i32 = 200;
const XML_NS_ERR_UNDEFINED_NAMESPACE: i32 = 201;
const XML_NS_ERR_QNAME: i32 = 202;
const XML_NS_ERR_ATTRIBUTE_REDEFINED: i32 = 203;
const XML_NS_ERR_EMPTY: i32 = 204;

struct XmlErrCtx {
    strictness: i32,
    err_occurred: bool,
    err_buf: String,
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

#[track_caller]
fn loc(func: &'static str) -> ::types_error::ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ::types_error::ErrorLocation::new(site.file(), site.line() as i32, func)
}

// C ereports WARNINGs inside the handler (xml.c:2253); elog from a
// libxml-invoked callback is unsafe in our unwind model, so they flush here.
pub fn flush_xml_warnings() {
    let warnings = XML_ERR_CTX.with(|c| std::mem::take(&mut c.borrow_mut().pending_warnings));
    for w in warnings {
        let _ = elog::ereport(WARNING)
            .errmsg_internal(w)
            .finish(loc("xml_errorHandler"));
    }
}

fn append_line_separator(buf: &mut String) {
    while buf.ends_with('\n') {
        buf.pop();
    }
    if !buf.is_empty() {
        buf.push('\n');
    }
}

// libxml2 xmlParserPrintFileContextInternal: offending line + caret line.
unsafe fn parser_print_file_context(input: *const xmlParserInputHdr) -> Option<String> {
    // SAFETY (fn body): input is the live parser-input header of the parser
    // context libxml handed the structured error handler.
    unsafe {
        let cur0 = (*input).cur;
        let base = (*input).base;
        if cur0.is_null() || base.is_null() {
            return None;
        }
        let end = (*input).end;
        let at = |p: *const c_uchar| -> u8 { *p };

        let mut cur = cur0;
        while cur > base && (at(cur) == b'\n' || at(cur) == b'\r') {
            cur = cur.sub(1);
        }
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
        let col = cur0 as usize - cur as usize;

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
}

pub unsafe extern "C" fn xml_error_handler(_user_data: *mut c_void, error: *mut c_void) {
    // SAFETY (fn body): libxml passes a live xmlError; header prefixes match
    // the stable 2.x ABI (libxml.rs).
    unsafe {
        let err = error as *const xmlErrorHdr;
        if err.is_null() {
            return;
        }
        let code = (*err).code;
        let mut domain = (*err).domain;
        let mut level = (*err).level;

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

        match domain {
            XML_FROM_PARSER => {
                if code == XML_ERR_NOT_WELL_BALANCED && already_occurred {
                    return;
                }
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
                if strictness == PG_XML_STRICTNESS_WELLFORMED {
                    return;
                }
            }
        }

        let mut msg = String::new();
        let line = (*err).line;
        if line > 0 {
            msg.push_str(&format!("line {line}: "));
        }
        let node = (*err).node;
        if !node.is_null() && libxml::node_type(node as *mut xmlNode) == XML_ELEMENT_NODE {
            let name = (*(node as *const xmlNodeHdr)).name;
            if !name.is_null() {
                let nm = libxml::xmlchar_to_vec(name);
                msg.push_str(&format!("element {}: ", String::from_utf8_lossy(&nm)));
            }
        }
        if !(*err).message.is_null() {
            let m = libxml::xmlchar_to_vec((*err).message as *const c_uchar);
            msg.push_str(&String::from_utf8_lossy(&m));
        } else {
            msg.push_str("(no message provided)");
        }

        let ctxt = (*err).ctxt;
        if !ctxt.is_null() {
            let input = (*(ctxt as *const xmlParserCtxtHdr)).input;
            if !input.is_null() {
                if let Some(ctx) = parser_print_file_context(input) {
                    append_line_separator(&mut msg);
                    msg.push_str(&ctx);
                }
            }
        }

        while msg.ends_with('\n') {
            msg.pop();
        }

        if strictness == PG_XML_STRICTNESS_LEGACY {
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
            XML_ERR_CTX.with(|c| c.borrow_mut().pending_warnings.push(msg));
        }
    }
}

unsafe extern "C" fn pg_entity_loader(
    _url: *const core::ffi::c_char,
    _id: *const core::ffi::c_char,
    _ctxt: *mut c_void,
) -> *mut c_void {
    core::ptr::null_mut()
}

/// C `pg_xml_init(strictness)`: parser init, external-entity sandboxing
/// (xmlPgEntityLoader), structured error capture, per-operation state reset.
pub fn pg_xml_init(strictness: i32) {
    let x = xml2();
    // SAFETY: process-global libxml setup with our own handlers.
    unsafe {
        (x.xmlInitParser)();
        (x.xmlSetExternalEntityLoader)(Some(pg_entity_loader));
        (x.xmlSetStructuredErrorFunc)(core::ptr::null_mut(), Some(xml_error_handler));
    }
    XML_ERR_CTX.with(|c| {
        let mut c = c.borrow_mut();
        c.strictness = strictness;
        c.err_occurred = false;
        c.err_buf.clear();
        c.pending_warnings.clear();
    });
}

pub fn xml_err_occurred() -> bool {
    XML_ERR_CTX.with(|c| c.borrow().err_occurred)
}

pub fn xml_err_detail() -> String {
    XML_ERR_CTX.with(|c| c.borrow().err_buf.clone())
}

/// C `xml_ereport`: the buffered libxml diagnostics ride as errdetail.
pub fn xml_ereport(msg: &str, sqlstate: SqlState) -> PgError {
    let detail = xml_err_detail();
    let mut e = PgError::error(msg.to_string()).with_sqlstate(sqlstate);
    if !detail.is_empty() {
        e = e.with_detail(detail);
    }
    e
}
