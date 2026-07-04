//! `xpath()` / `xpath_exists()` / `xmlexists()` — xml.c:4151-4580.

use core::ffi::{c_char, c_void};

use ::types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_ARGUMENT_FOR_XQUERY, ERRCODE_INVALID_XML_DOCUMENT,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_OUT_OF_MEMORY,
};

use crate::errhandler::{pg_xml_init, xml_ereport, xml_err_occurred, PG_XML_STRICTNESS_ALL};
use crate::libxml::{
    self, cstr, xml2, xmlNode, xmlNodeSetHdr, xmlXPathObjectHdr, XML_ATTRIBUTE_NODE,
    XML_DOCUMENT_NODE, XML_TEXT_NODE, XPATH_BOOLEAN, XPATH_NODESET, XPATH_NUMBER, XPATH_STRING,
};
use crate::{escape_xml, parse_xml_decl, PG_UTF8};

/// C `xml_xmlnodetoxmltype` (xml.c:4151): attr/text nodes escape their string
/// cast; everything else copies + dumps the subtree.
pub(crate) unsafe fn node_to_xmltype(cur: *mut xmlNode) -> PgResult<Vec<u8>> {
    let x = xml2();
    // SAFETY (fn body): cur is a live node in the evaluated document.
    unsafe {
        let t = libxml::node_type(cur);
        if t != XML_ATTRIBUTE_NODE && t != XML_TEXT_NODE {
            let buf = (x.xmlBufferCreate)();
            if buf.is_null() {
                return Err(xml_ereport("could not allocate xmlBuffer", ERRCODE_OUT_OF_MEMORY)
                    .into());
            }
            let cur_copy = (x.xmlCopyNode)(cur, 1);
            if cur_copy.is_null() {
                (x.xmlBufferFree)(buf);
                return Err(
                    xml_ereport("could not copy node", ERRCODE_OUT_OF_MEMORY).into()
                );
            }
            let is_doc = libxml::node_type(cur_copy) == XML_DOCUMENT_NODE;
            let bytes = (x.xmlNodeDump)(buf, core::ptr::null_mut(), cur_copy, 0, 0);
            let free_copy = || {
                // SAFETY: cur_copy is the live copy freed exactly once.
                unsafe {
                    if is_doc {
                        (x.xmlFreeDoc)(cur_copy as *mut libxml::xmlDoc);
                    } else {
                        (x.xmlFreeNode)(cur_copy);
                    }
                }
            };
            if bytes == -1 {
                free_copy();
                (x.xmlBufferFree)(buf);
                return Err(xml_ereport("could not dump node", ERRCODE_OUT_OF_MEMORY).into());
            }
            let v = libxml::buffer_to_vec(buf);
            free_copy();
            (x.xmlBufferFree)(buf);
            Ok(v)
        } else {
            let str = (x.xmlXPathCastNodeToString)(cur);
            let raw = libxml::xmlchar_to_vec(str);
            if !str.is_null() {
                x.xmlFree(str as *mut c_void);
            }
            Ok(escape_xml(&raw))
        }
    }
}

fn float8out(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v < 0.0 { "-Infinity".to_string() } else { "Infinity".to_string() }
    } else {
        format!("{v}")
    }
}

/// C `xml_xpathobjtoxmlarray` (xml.c:4243): result count plus, when wanted,
/// the elements' xmltype payloads.
unsafe fn xpathobj_to_xmlarray(
    xpathobj: *mut libxml::xmlXPathObject,
    collect: Option<&mut Vec<Vec<u8>>>,
) -> PgResult<i32> {
    // SAFETY (fn body): xpathobj is the live eval result; header prefix per ABI.
    unsafe {
        let hdr = &*(xpathobj as *const xmlXPathObjectHdr);
        match hdr.type_ {
            XPATH_NODESET => {
                if hdr.nodesetval.is_null() {
                    return Ok(0);
                }
                let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
                let n = ns.node_nr;
                if let Some(out) = collect {
                    for i in 0..n {
                        let node = *ns.node_tab.add(i as usize);
                        out.push(node_to_xmltype(node)?);
                    }
                }
                Ok(n)
            }
            XPATH_BOOLEAN => {
                if let Some(out) = collect {
                    // map_sql_value_to_xml_value(BOOLOID).
                    out.push(if hdr.boolval != 0 { b"true".to_vec() } else { b"false".to_vec() });
                }
                Ok(1)
            }
            XPATH_NUMBER => {
                if let Some(out) = collect {
                    // map_sql_value_to_xml_value(FLOAT8OID) == float8out.
                    out.push(float8out(hdr.floatval).into_bytes());
                }
                Ok(1)
            }
            XPATH_STRING => {
                if let Some(out) = collect {
                    // map_sql_value_to_xml_value(CSTRINGOID, escape=true).
                    let s = libxml::xmlchar_to_vec(hdr.stringval);
                    out.push(escape_xml(&s));
                }
                Ok(1)
            }
            other => Err(PgError::error(format!(
                "xpath expression result type {other} is unsupported"
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
            .into()),
        }
    }
}

/// C `xpath_internal` (xml.c:4323). `namespaces` is the flattened text[]
/// image (None for xmlexists). Returns the match count; fills `collect` with
/// xmltype payloads when given.
pub fn xpath_internal(
    xpath_expr: &[u8],
    data: &[u8],
    namespaces: Option<&[u8]>,
    collect: Option<&mut Vec<Vec<u8>>>,
) -> PgResult<i32> {
    let mut ns_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if let Some(arr) = namespaces {
        let ndim = arrayfuncs::foundation::arr_ndim(arr);
        if ndim != 0 {
            let (nd, dims, _lbs) = arrayfuncs::foundation::read_dims_lbounds(arr);
            if nd != 2 || dims[1] != 2 {
                return Err(PgError::error("invalid array for XML namespace mapping")
                    .with_sqlstate(ERRCODE_DATA_EXCEPTION)
                    .with_detail(
                        "The array must be two-dimensional with length of the second axis equal to 2.",
                    )
                    .into());
            }
            let ctx = ::mcx::MemoryContext::new("xpath namespaces");
            let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(
                ctx.mcx(),
                arr,
                ::types_core::catalog::TEXTOID,
                true,
            )?;
            let mut i = 0;
            while i < elems.len() {
                if nulls[i] || nulls[i + 1] {
                    return Err(PgError::error("neither namespace name nor URI may be null")
                        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                        .into());
                }
                ns_pairs.push((text_datum_payload(elems[i]), text_datum_payload(elems[i + 1])));
                i += 2;
            }
        }
    }

    if xpath_expr.is_empty() {
        return Err(PgError::error("empty XPath expression")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
            .into());
    }

    // In a UTF8 database, skip any xml declaration (declared-encoding
    // assertions); ignore decl-parse failure, letting xmlCtxtReadMemory report.
    let mut xmldecl_len: usize = 0;
    if ::mbutils::GetDatabaseEncoding() == PG_UTF8 {
        let mut work = data.to_vec();
        work.push(0);
        let _ = parse_xml_decl(&work, Some(&mut xmldecl_len), None, None, None);
    }
    let payload = &data[xmldecl_len.min(data.len())..];

    let x = xml2();
    pg_xml_init(PG_XML_STRICTNESS_ALL);
    // SAFETY: libxml objects freed on every exit path via `cleanup`.
    unsafe {
        let ctxt = (x.xmlNewParserCtxt)();
        if ctxt.is_null() || xml_err_occurred() {
            if !ctxt.is_null() {
                (x.xmlFreeParserCtxt)(ctxt);
            }
            return Err(
                xml_ereport("could not allocate parser context", ERRCODE_OUT_OF_MEMORY).into(),
            );
        }
        let doc = (x.xmlCtxtReadMemory)(
            ctxt,
            payload.as_ptr() as *const c_char,
            payload.len() as i32,
            core::ptr::null(),
            core::ptr::null(),
            0,
        );
        if doc.is_null() || xml_err_occurred() {
            if !doc.is_null() {
                (x.xmlFreeDoc)(doc);
            }
            (x.xmlFreeParserCtxt)(ctxt);
            return Err(
                xml_ereport("could not parse XML document", ERRCODE_INVALID_XML_DOCUMENT).into(),
            );
        }
        crate::errhandler::flush_xml_warnings();

        let cleanup = |xpathobj: *mut libxml::xmlXPathObject,
                       xpathcomp: *mut libxml::xmlXPathCompExpr,
                       xpathctx: *mut libxml::xmlXPathContext| {
            // SAFETY: each pointer is live (or null-checked) and freed once.
            unsafe {
                if !xpathobj.is_null() {
                    (x.xmlXPathFreeObject)(xpathobj);
                }
                if !xpathcomp.is_null() {
                    (x.xmlXPathFreeCompExpr)(xpathcomp);
                }
                if !xpathctx.is_null() {
                    (x.xmlXPathFreeContext)(xpathctx);
                }
                (x.xmlFreeDoc)(doc);
                (x.xmlFreeParserCtxt)(ctxt);
            }
        };

        let xpathctx = (x.xmlXPathNewContext)(doc);
        if xpathctx.is_null() || xml_err_occurred() {
            cleanup(core::ptr::null_mut(), core::ptr::null_mut(), xpathctx);
            return Err(
                xml_ereport("could not allocate XPath context", ERRCODE_OUT_OF_MEMORY).into(),
            );
        }
        // xpathctx->node = (xmlNodePtr) doc (xml.c:4426).
        (*(xpathctx as *mut libxml::xmlXPathContextHdr)).node = doc as *mut xmlNode;

        for (name, uri) in &ns_pairs {
            let n = cstr(name);
            let u = cstr(uri);
            if (x.xmlXPathRegisterNs)(xpathctx, n.as_ptr(), u.as_ptr()) != 0 {
                cleanup(core::ptr::null_mut(), core::ptr::null_mut(), xpathctx);
                return Err(PgError::error(format!(
                    "could not register XML namespace with name \"{}\" and URI \"{}\"",
                    String::from_utf8_lossy(name),
                    String::from_utf8_lossy(uri)
                ))
                .into());
            }
        }

        let expr = cstr(xpath_expr);
        let xpathcomp = (x.xmlXPathCtxtCompile)(xpathctx, expr.as_ptr());
        if xpathcomp.is_null() || xml_err_occurred() {
            cleanup(core::ptr::null_mut(), core::ptr::null_mut(), xpathctx);
            return Err(
                xml_ereport("invalid XPath expression", ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                    .into(),
            );
        }

        let xpathobj = (x.xmlXPathCompiledEval)(xpathcomp, xpathctx);
        if xpathobj.is_null() || xml_err_occurred() {
            cleanup(core::ptr::null_mut(), xpathcomp, xpathctx);
            return Err(
                xml_ereport("could not create XPath object", ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                    .into(),
            );
        }

        let result = xpathobj_to_xmlarray(xpathobj, collect);
        cleanup(xpathobj, xpathcomp, xpathctx);
        result
    }
}

// TextDatumGetCString over a deconstructed text[] element.
fn text_datum_payload(d: ::datum::Datum) -> Vec<u8> {
    let p = d.as_usize() as *const u8;
    // SAFETY: the datum points into the live flattened array image.
    unsafe {
        let total = ::types_tuple::varatt::varsize_any(p);
        let image = core::slice::from_raw_parts(p, total);
        match image.first() {
            Some(&h) if h & 0x01 == 0x01 => image[1..].to_vec(),
            _ => image[::datum::VARHDRSZ..].to_vec(),
        }
    }
}
