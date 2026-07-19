//! dlopen/dlsym binding to the system libxml2, resolved once (ICU-lane
//! precedent: no build-time dep; pods only guarantee the runtime .so).
//! Unloadable/missing symbol = loud panic, never a no-libxml fallback.

#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]

use core::ffi::{c_char, c_int, c_long, c_void};
use std::sync::OnceLock;

pub type c_uchar = u8;

pub enum xmlDoc {}
pub enum xmlParserCtxt {}
pub enum xmlBuffer {}
pub enum xmlTextWriter {}
pub enum xmlSaveCtxt {}
pub enum xmlXPathContext {}
pub enum xmlXPathCompExpr {}
pub enum xmlXPathObject {}
pub enum xmlNode {}
pub enum xmlNodeSet {}

pub const XML_ELEMENT_NODE: c_int = 1;
pub const XML_ATTRIBUTE_NODE: c_int = 2;
pub const XML_TEXT_NODE: c_int = 3;
pub const XML_DOCUMENT_NODE: c_int = 9;

pub const XPATH_NODESET: c_int = 1;
pub const XPATH_BOOLEAN: c_int = 2;
pub const XPATH_NUMBER: c_int = 3;
pub const XPATH_STRING: c_int = 4;

pub const XML_PARSE_NOENT: c_int = 1 << 1;
pub const XML_PARSE_DTDATTR: c_int = 1 << 3;
pub const XML_PARSE_NOBLANKS: c_int = 1 << 8;
pub const XML_PARSE_NONET: c_int = 1 << 11;

pub const XML_SAVE_FORMAT: c_int = 1 << 0;
pub const XML_SAVE_NO_DECL: c_int = 1 << 1;

pub type xmlExternalEntityLoader =
    unsafe extern "C" fn(URL: *const c_char, ID: *const c_char, ctxt: *mut c_void) -> *mut c_void;
pub type xmlStructuredErrorFunc =
    unsafe extern "C" fn(user_data: *mut c_void, error: *mut c_void);
type xmlFreeFunc = unsafe extern "C" fn(mem: *mut c_void);

// Prefixes of libxml2's public structs through the fields xml.c reads;
// stable across libxml2 2.x — the same ABI contract C's xml.c relies on.
#[repr(C)]
pub struct xmlNodeHdr {
    pub _private: *mut c_void,
    pub type_: c_int,
    pub name: *const c_uchar,
    pub children: *mut xmlNode,
    pub last: *mut xmlNode,
    pub parent: *mut xmlNode,
    pub next: *mut xmlNode,
    pub prev: *mut xmlNode,
    pub doc: *mut xmlDoc,
}

#[repr(C)]
pub struct xmlDocHdr {
    pub _private: *mut c_void,
    pub type_: c_int,
    pub name: *const c_char,
    pub children: *mut xmlNode,
    pub last: *mut xmlNode,
    pub parent: *mut xmlNode,
    pub next: *mut xmlNode,
    pub prev: *mut xmlNode,
    pub doc: *mut xmlDoc,
    pub compression: c_int,
    pub standalone: c_int,
    pub int_subset: *mut c_void,
    pub ext_subset: *mut c_void,
    pub old_ns: *mut c_void,
    pub version: *const c_uchar,
    pub encoding: *const c_uchar,
}

#[repr(C)]
pub struct xmlNodeSetHdr {
    pub node_nr: c_int,
    pub node_max: c_int,
    pub node_tab: *mut *mut xmlNode,
}

#[repr(C)]
pub struct xmlXPathObjectHdr {
    pub type_: c_int,
    pub nodesetval: *mut xmlNodeSet,
    pub boolval: c_int,
    pub floatval: f64,
    pub stringval: *mut c_uchar,
}

#[repr(C)]
pub struct xmlXPathContextHdr {
    pub doc: *mut xmlDoc,
    pub node: *mut xmlNode,
}

#[repr(C)]
pub struct xmlErrorHdr {
    pub domain: c_int,
    pub code: c_int,
    pub message: *mut c_char,
    pub level: c_int,
    pub file: *mut c_char,
    pub line: c_int,
    pub str1: *mut c_char,
    pub str2: *mut c_char,
    pub str3: *mut c_char,
    pub int1: c_int,
    pub int2: c_int,
    pub ctxt: *mut c_void,
    pub node: *mut c_void,
}

#[repr(C)]
pub struct xmlParserCtxtHdr {
    pub sax: *mut c_void,
    pub user_data: *mut c_void,
    pub my_doc: *mut c_void,
    pub well_formed: c_int,
    pub replace_entities: c_int,
    pub version: *const c_uchar,
    pub encoding: *const c_uchar,
    pub standalone: c_int,
    pub html: c_int,
    pub input: *mut xmlParserInputHdr,
}

#[repr(C)]
pub struct xmlParserInputHdr {
    pub buf: *mut c_void,
    pub filename: *const c_char,
    pub directory: *const c_char,
    pub base: *const c_uchar,
    pub cur: *const c_uchar,
    pub end: *const c_uchar,
    pub length: c_int,
    pub line: c_int,
    pub col: c_int,
}

pub struct LibXml2 {
    pub xmlInitParser: unsafe extern "C" fn(),
    xmlFreeVar: *const xmlFreeFunc,
    pub xmlStrdup: unsafe extern "C" fn(cur: *const c_uchar) -> *mut c_uchar,
    pub xmlStrlen: unsafe extern "C" fn(s: *const c_uchar) -> c_int,
    pub xmlNewParserCtxt: unsafe extern "C" fn() -> *mut xmlParserCtxt,
    pub xmlFreeParserCtxt: unsafe extern "C" fn(ctxt: *mut xmlParserCtxt),
    pub xmlCtxtReadDoc: unsafe extern "C" fn(
        ctxt: *mut xmlParserCtxt,
        cur: *const c_uchar,
        URL: *const c_char,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlDoc,
    pub xmlCtxtReadMemory: unsafe extern "C" fn(
        ctxt: *mut xmlParserCtxt,
        buffer: *const c_char,
        size: c_int,
        URL: *const c_char,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlDoc,
    pub xmlNewDoc: unsafe extern "C" fn(version: *const c_uchar) -> *mut xmlDoc,
    pub xmlFreeDoc: unsafe extern "C" fn(doc: *mut xmlDoc),
    pub xmlParseBalancedChunkMemory: unsafe extern "C" fn(
        doc: *mut xmlDoc,
        sax: *mut c_void,
        user_data: *mut c_void,
        depth: c_int,
        string: *const c_uchar,
        lst: *mut *mut xmlNode,
    ) -> c_int,
    pub xmlKeepBlanksDefault: unsafe extern "C" fn(val: c_int) -> c_int,
    pub xmlDocSetRootElement:
        unsafe extern "C" fn(doc: *mut xmlDoc, root: *mut xmlNode) -> *mut xmlNode,
    pub xmlNewNode:
        unsafe extern "C" fn(ns: *mut c_void, name: *const c_uchar) -> *mut xmlNode,
    pub xmlNewDocText:
        unsafe extern "C" fn(doc: *mut xmlDoc, content: *const c_uchar) -> *mut xmlNode,
    pub xmlAddChildList:
        unsafe extern "C" fn(parent: *mut xmlNode, cur: *mut xmlNode) -> *mut xmlNode,
    pub xmlFreeNode: unsafe extern "C" fn(node: *mut xmlNode),
    pub xmlBufferCreate: unsafe extern "C" fn() -> *mut xmlBuffer,
    pub xmlBufferFree: unsafe extern "C" fn(buf: *mut xmlBuffer),
    pub xmlBufferContent: unsafe extern "C" fn(buf: *const xmlBuffer) -> *const c_uchar,
    pub xmlBufferLength: unsafe extern "C" fn(buf: *const xmlBuffer) -> c_int,
    pub xmlSaveToBuffer: unsafe extern "C" fn(
        buffer: *mut xmlBuffer,
        encoding: *const c_char,
        options: c_int,
    ) -> *mut xmlSaveCtxt,
    pub xmlSaveDoc: unsafe extern "C" fn(ctxt: *mut xmlSaveCtxt, doc: *mut xmlDoc) -> c_long,
    pub xmlSaveTree: unsafe extern "C" fn(ctxt: *mut xmlSaveCtxt, node: *mut xmlNode) -> c_long,
    pub xmlSaveClose: unsafe extern "C" fn(ctxt: *mut xmlSaveCtxt) -> c_int,
    pub xmlNodeDump: unsafe extern "C" fn(
        buf: *mut xmlBuffer,
        doc: *mut xmlDoc,
        cur: *mut xmlNode,
        level: c_int,
        format: c_int,
    ) -> c_int,
    pub xmlCopyNode: unsafe extern "C" fn(node: *mut xmlNode, extended: c_int) -> *mut xmlNode,
    pub xmlNewTextWriterMemory:
        unsafe extern "C" fn(buf: *mut xmlBuffer, compression: c_int) -> *mut xmlTextWriter,
    pub xmlFreeTextWriter: unsafe extern "C" fn(writer: *mut xmlTextWriter),
    pub xmlTextWriterStartElement:
        unsafe extern "C" fn(writer: *mut xmlTextWriter, name: *const c_uchar) -> c_int,
    pub xmlTextWriterEndElement: unsafe extern "C" fn(writer: *mut xmlTextWriter) -> c_int,
    pub xmlTextWriterWriteAttribute: unsafe extern "C" fn(
        writer: *mut xmlTextWriter,
        name: *const c_uchar,
        content: *const c_uchar,
    ) -> c_int,
    pub xmlTextWriterWriteRaw:
        unsafe extern "C" fn(writer: *mut xmlTextWriter, content: *const c_uchar) -> c_int,
    pub xmlTextWriterWriteBase64: unsafe extern "C" fn(
        writer: *mut xmlTextWriter,
        data: *const c_char,
        start: c_int,
        len: c_int,
    ) -> c_int,
    pub xmlTextWriterWriteBinHex: unsafe extern "C" fn(
        writer: *mut xmlTextWriter,
        data: *const c_char,
        start: c_int,
        len: c_int,
    ) -> c_int,
    pub xmlEncodeSpecialChars:
        unsafe extern "C" fn(doc: *const xmlDoc, input: *const c_uchar) -> *mut c_uchar,
    pub xmlXPathNewContext: unsafe extern "C" fn(doc: *mut xmlDoc) -> *mut xmlXPathContext,
    pub xmlXPathFreeContext: unsafe extern "C" fn(ctxt: *mut xmlXPathContext),
    pub xmlXPathRegisterNs: unsafe extern "C" fn(
        ctxt: *mut xmlXPathContext,
        prefix: *const c_uchar,
        ns_uri: *const c_uchar,
    ) -> c_int,
    pub xmlXPathCtxtCompile: unsafe extern "C" fn(
        ctxt: *mut xmlXPathContext,
        expr: *const c_uchar,
    ) -> *mut xmlXPathCompExpr,
    pub xmlXPathFreeCompExpr: unsafe extern "C" fn(comp: *mut xmlXPathCompExpr),
    pub xmlXPathCompiledEval: unsafe extern "C" fn(
        comp: *mut xmlXPathCompExpr,
        ctxt: *mut xmlXPathContext,
    ) -> *mut xmlXPathObject,
    pub xmlXPathFreeObject: unsafe extern "C" fn(obj: *mut xmlXPathObject),
    pub xmlXPathCastNodeToString: unsafe extern "C" fn(node: *mut xmlNode) -> *mut c_uchar,
    pub xmlXPathCastNodeSetToString: unsafe extern "C" fn(ns: *mut xmlNodeSet) -> *mut c_uchar,
    pub xmlXPathCastBooleanToString: unsafe extern "C" fn(val: c_int) -> *mut c_uchar,
    pub xmlXPathCastBooleanToNumber: unsafe extern "C" fn(val: c_int) -> f64,
    pub xmlXPathCastNumberToString: unsafe extern "C" fn(val: f64) -> *mut c_uchar,
    pub xmlSetExternalEntityLoader: unsafe extern "C" fn(f: Option<xmlExternalEntityLoader>),
    pub xmlSetStructuredErrorFunc:
        unsafe extern "C" fn(ctx: *mut c_void, handler: Option<xmlStructuredErrorFunc>),
}

impl LibXml2 {
    // xmlFree is a GLOBAL VARIABLE holding a fn pointer (xmlmemory.h);
    // calling the variable's address as code is a SIGBUS — read per call.
    #[inline]
    pub unsafe fn xmlFree(&self, p: *mut c_void) {
        unsafe { (*self.xmlFreeVar)(p) }
    }
}

// SAFETY: one backend = one thread; the table is immutable fn pointers plus
// the xmlFree variable address, valid process-wide once resolved.
unsafe impl Sync for LibXml2 {}
unsafe impl Send for LibXml2 {}

static LIBXML: OnceLock<Result<&'static LibXml2, String>> = OnceLock::new();

pub fn xml2() -> &'static LibXml2 {
    match LIBXML.get_or_init(load) {
        Ok(api) => api,
        Err(e) => panic!("adt_xml libxml2: {e}"),
    }
}

#[cfg(not(target_family = "wasm"))]
fn open_lib() -> Result<*mut c_void, String> {
    for name in [c"libxml2.so.2", c"libxml2.so", c"libxml2.2.dylib"] {
        // SAFETY: literal is NUL-terminated.
        let h = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW) };
        if !h.is_null() {
            return Ok(h);
        }
    }
    Err("could not dlopen libxml2.so.2 / libxml2.so / libxml2.2.dylib".to_string())
}

// wasm32: no dynamic loading on wasm32-wasip1 (wasi-libc ships no dlopen),
// so libxml2 can never be resolved; every xml entry point that touches the
// table panics with this message — same shape as the native no-.so
// environment. A functional wasm xml arm needs a statically linked libxml2
// (later increment; xml is not on the P5 --single serial subset).
#[cfg(target_family = "wasm")]
fn load() -> Result<&'static LibXml2, String> {
    Err("xml is not supported on wasm32-wasip1 (no dynamic loading for libxml2)".to_string())
}

#[cfg(not(target_family = "wasm"))]
fn load() -> Result<&'static LibXml2, String> {
    let handle = open_lib()?;
    macro_rules! resolve {
        ($name:ident) => {{
            let sym = concat!(stringify!($name), "\0");
            // SAFETY: sym is NUL-terminated; handle is a live dlopen handle.
            let p = unsafe { libc::dlsym(handle, sym.as_ptr() as *const c_char) };
            if p.is_null() {
                return Err(format!("libxml2 symbol {} not found", stringify!($name)));
            }
            // SAFETY: non-null dlsym result transmuted to the C signature
            // declared for this stable public libxml2 entry point.
            unsafe { core::mem::transmute(p) }
        }};
    }
    let xmlFreeVar = {
        // SAFETY: as above; xmlFree is a data symbol (see LibXml2::xmlFree).
        let p = unsafe { libc::dlsym(handle, c"xmlFree".as_ptr()) };
        if p.is_null() {
            return Err("libxml2 symbol xmlFree not found".to_string());
        }
        p as *const xmlFreeFunc
    };
    Ok(Box::leak(Box::new(LibXml2 {
        xmlInitParser: resolve!(xmlInitParser),
        xmlFreeVar,
        xmlStrdup: resolve!(xmlStrdup),
        xmlStrlen: resolve!(xmlStrlen),
        xmlNewParserCtxt: resolve!(xmlNewParserCtxt),
        xmlFreeParserCtxt: resolve!(xmlFreeParserCtxt),
        xmlCtxtReadDoc: resolve!(xmlCtxtReadDoc),
        xmlCtxtReadMemory: resolve!(xmlCtxtReadMemory),
        xmlNewDoc: resolve!(xmlNewDoc),
        xmlFreeDoc: resolve!(xmlFreeDoc),
        xmlParseBalancedChunkMemory: resolve!(xmlParseBalancedChunkMemory),
        xmlKeepBlanksDefault: resolve!(xmlKeepBlanksDefault),
        xmlDocSetRootElement: resolve!(xmlDocSetRootElement),
        xmlNewNode: resolve!(xmlNewNode),
        xmlNewDocText: resolve!(xmlNewDocText),
        xmlAddChildList: resolve!(xmlAddChildList),
        xmlFreeNode: resolve!(xmlFreeNode),
        xmlBufferCreate: resolve!(xmlBufferCreate),
        xmlBufferFree: resolve!(xmlBufferFree),
        xmlBufferContent: resolve!(xmlBufferContent),
        xmlBufferLength: resolve!(xmlBufferLength),
        xmlSaveToBuffer: resolve!(xmlSaveToBuffer),
        xmlSaveDoc: resolve!(xmlSaveDoc),
        xmlSaveTree: resolve!(xmlSaveTree),
        xmlSaveClose: resolve!(xmlSaveClose),
        xmlNodeDump: resolve!(xmlNodeDump),
        xmlCopyNode: resolve!(xmlCopyNode),
        xmlNewTextWriterMemory: resolve!(xmlNewTextWriterMemory),
        xmlFreeTextWriter: resolve!(xmlFreeTextWriter),
        xmlTextWriterStartElement: resolve!(xmlTextWriterStartElement),
        xmlTextWriterEndElement: resolve!(xmlTextWriterEndElement),
        xmlTextWriterWriteAttribute: resolve!(xmlTextWriterWriteAttribute),
        xmlTextWriterWriteRaw: resolve!(xmlTextWriterWriteRaw),
        xmlTextWriterWriteBase64: resolve!(xmlTextWriterWriteBase64),
        xmlTextWriterWriteBinHex: resolve!(xmlTextWriterWriteBinHex),
        xmlEncodeSpecialChars: resolve!(xmlEncodeSpecialChars),
        xmlXPathNewContext: resolve!(xmlXPathNewContext),
        xmlXPathFreeContext: resolve!(xmlXPathFreeContext),
        xmlXPathRegisterNs: resolve!(xmlXPathRegisterNs),
        xmlXPathCtxtCompile: resolve!(xmlXPathCtxtCompile),
        xmlXPathFreeCompExpr: resolve!(xmlXPathFreeCompExpr),
        xmlXPathCompiledEval: resolve!(xmlXPathCompiledEval),
        xmlXPathFreeObject: resolve!(xmlXPathFreeObject),
        xmlXPathCastNodeToString: resolve!(xmlXPathCastNodeToString),
        xmlXPathCastNodeSetToString: resolve!(xmlXPathCastNodeSetToString),
        xmlXPathCastBooleanToString: resolve!(xmlXPathCastBooleanToString),
        xmlXPathCastBooleanToNumber: resolve!(xmlXPathCastBooleanToNumber),
        xmlXPathCastNumberToString: resolve!(xmlXPathCastNumberToString),
        xmlSetExternalEntityLoader: resolve!(xmlSetExternalEntityLoader),
        xmlSetStructuredErrorFunc: resolve!(xmlSetStructuredErrorFunc),
    })))
}

#[inline]
pub unsafe fn node_type(node: *mut xmlNode) -> c_int {
    unsafe { (*(node as *const xmlNodeHdr)).type_ }
}

pub fn cstr(bytes: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(bytes.len() + 1);
    v.extend_from_slice(bytes);
    v.push(0);
    v
}

pub unsafe fn xmlchar_to_vec(p: *const c_uchar) -> Vec<u8> {
    if p.is_null() {
        return Vec::new();
    }
    // SAFETY: p is a NUL-terminated libxml string per caller contract.
    unsafe {
        let len = (xml2().xmlStrlen)(p) as usize;
        std::slice::from_raw_parts(p, len).to_vec()
    }
}

pub unsafe fn buffer_to_vec(buf: *mut xmlBuffer) -> Vec<u8> {
    // SAFETY: buf is a live xmlBuffer per caller contract.
    unsafe {
        let content = (xml2().xmlBufferContent)(buf);
        let len = (xml2().xmlBufferLength)(buf) as usize;
        if content.is_null() {
            Vec::new()
        } else {
            std::slice::from_raw_parts(content, len).to_vec()
        }
    }
}
