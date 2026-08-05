//! XMLTABLE builder — C `XmlTableRoutine`: the executor scan state owns an
//! [`XmlTableContext`] where C stows an opaque pointer. Teardown is the
//! explicit [`XmlTableContext::destroy`] (owner-managed resource, no Drop).

use core::ffi::c_char;

use ::types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_ARGUMENT_FOR_XQUERY, ERRCODE_INVALID_XML_DOCUMENT, ERRCODE_OUT_OF_MEMORY,
};

use crate::errhandler::{
    pg_xml_init, xml_error_handler, xml_ereport, xml_err_occurred, PG_XML_STRICTNESS_ALL,
};
use crate::libxml::{
    self, cstr, xml2, xmlDoc, xmlNodeSetHdr, xmlParserCtxt, xmlXPathCompExpr, xmlXPathContext,
    xmlXPathContextHdr, xmlXPathObject, xmlXPathObjectHdr, XPATH_BOOLEAN, XPATH_NODESET,
    XPATH_NUMBER, XPATH_STRING,
};

use ::types_core::catalog::XMLOID;
const TYPCATEGORY_NUMERIC: i8 = b'N' as i8;

pub struct XmlTableContext {
    ctxt: *mut xmlParserCtxt,
    doc: *mut xmlDoc,
    xpathcxt: *mut xmlXPathContext,
    xpathcomp: *mut xmlXPathCompExpr,
    xpathobj: *mut xmlXPathObject,
    xpathscomp: Vec<*mut xmlXPathCompExpr>,
    row_count: i64,
}

impl XmlTableContext {
    /// C `XmlTableInitOpaque` (xml.c:4683).
    pub fn new(natts: i32) -> PgResult<XmlTableContext> {
        let x = xml2();
        pg_xml_init(PG_XML_STRICTNESS_ALL);
        // SAFETY: fresh parser context, freed by destroy().
        unsafe {
            (x.xmlInitParser)();
            let ctxt = (x.xmlNewParserCtxt)();
            if ctxt.is_null() || xml_err_occurred() {
                if !ctxt.is_null() {
                    (x.xmlFreeParserCtxt)(ctxt);
                }
                return Err(
                    xml_ereport("could not allocate parser context", ERRCODE_OUT_OF_MEMORY)
                        .into(),
                );
            }
            Ok(XmlTableContext {
                ctxt,
                doc: core::ptr::null_mut(),
                xpathcxt: core::ptr::null_mut(),
                xpathcomp: core::ptr::null_mut(),
                xpathobj: core::ptr::null_mut(),
                xpathscomp: vec![core::ptr::null_mut(); natts.max(0) as usize],
                row_count: 0,
            })
        }
    }

    /// C `XmlTableSetDocument`: `value` is the xmltype payload; libxml reads
    /// the encoding-stripped `xml_out_internal` rendering.
    pub fn set_document(&mut self, value: &[u8]) -> PgResult<()> {
        let image = crate::xml_out_internal(value, 0)?;
        let x = xml2();
        // SAFETY: self.ctxt is live; doc/xpathcxt ownership moves into self.
        unsafe {
            let doc = (x.xmlCtxtReadMemory)(
                self.ctxt,
                image.as_ptr() as *const c_char,
                image.len() as i32,
                core::ptr::null(),
                core::ptr::null(),
                0,
            );
            if doc.is_null() || xml_err_occurred() {
                if !doc.is_null() {
                    (x.xmlFreeDoc)(doc);
                }
                return Err(
                    xml_ereport("could not parse XML document", ERRCODE_INVALID_XML_DOCUMENT)
                        .into(),
                );
            }
            let xpathcxt = (x.xmlXPathNewContext)(doc);
            if xpathcxt.is_null() || xml_err_occurred() {
                if !xpathcxt.is_null() {
                    (x.xmlXPathFreeContext)(xpathcxt);
                }
                (x.xmlFreeDoc)(doc);
                return Err(
                    xml_ereport("could not allocate XPath context", ERRCODE_OUT_OF_MEMORY)
                        .into(),
                );
            }
            (*(xpathcxt as *mut xmlXPathContextHdr)).node = doc as *mut libxml::xmlNode;
            self.doc = doc;
            self.xpathcxt = xpathcxt;
            Ok(())
        }
    }

    /// C `XmlTableSetNamespace` (xml.c:4788).
    pub fn set_namespace(&mut self, name: Option<&[u8]>, uri: &[u8]) -> PgResult<()> {
        let Some(name) = name else {
            return Err(PgError::error("DEFAULT namespace is not supported")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .into());
        };
        let x = xml2();
        let n = cstr(name);
        let u = cstr(uri);
        // SAFETY: xpathcxt is live (set_document precedes per the routine order).
        unsafe {
            if (x.xmlXPathRegisterNs)(self.xpathcxt, n.as_ptr(), u.as_ptr()) != 0 {
                return Err(xml_ereport(
                    "could not set XML namespace",
                    ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
                )
                .into());
            }
        }
        Ok(())
    }

    /// C `XmlTableSetRowFilter` (xml.c:4814).
    pub fn set_row_filter(&mut self, path: &[u8]) -> PgResult<()> {
        if path.is_empty() {
            return Err(PgError::error("row path filter must not be empty string")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                .into());
        }
        let x = xml2();
        let xstr = cstr(path);
        // SAFETY: xpathcxt live; comp ownership moves into self.
        unsafe {
            let comp = (x.xmlXPathCtxtCompile)(self.xpathcxt, xstr.as_ptr());
            if comp.is_null() || xml_err_occurred() {
                return Err(
                    xml_ereport("invalid XPath expression", ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                        .into(),
                );
            }
            self.xpathcomp = comp;
        }
        Ok(())
    }

    /// C `XmlTableSetColumnFilter` (xml.c:4846).
    pub fn set_column_filter(&mut self, path: &[u8], colnum: i32) -> PgResult<()> {
        if path.is_empty() {
            return Err(PgError::error("column path filter must not be empty string")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                .into());
        }
        let x = xml2();
        let xstr = cstr(path);
        // SAFETY: as set_row_filter.
        unsafe {
            let comp = (x.xmlXPathCtxtCompile)(self.xpathcxt, xstr.as_ptr());
            if comp.is_null() || xml_err_occurred() {
                return Err(
                    xml_ereport("invalid XPath expression", ERRCODE_INVALID_ARGUMENT_FOR_XQUERY)
                        .into(),
                );
            }
            self.xpathscomp[colnum as usize] = comp;
        }
        Ok(())
    }

    /// C `XmlTableFetchRow` (xml.c:4881).
    pub fn fetch_row(&mut self) -> PgResult<bool> {
        let x = xml2();
        // SAFETY: row-filter comp and xpathcxt are live; obj moves into self.
        unsafe {
            (x.xmlSetStructuredErrorFunc)(core::ptr::null_mut(), Some(xml_error_handler));

            if self.xpathobj.is_null() {
                self.xpathobj = (x.xmlXPathCompiledEval)(self.xpathcomp, self.xpathcxt);
                if self.xpathobj.is_null() || xml_err_occurred() {
                    return Err(xml_ereport(
                        "could not create XPath object",
                        ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
                    )
                    .into());
                }
                self.row_count = 0;
            }

            let hdr = &*(self.xpathobj as *const xmlXPathObjectHdr);
            if hdr.type_ == XPATH_NODESET && !hdr.nodesetval.is_null() {
                let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
                let prev = self.row_count;
                self.row_count += 1;
                if prev < ns.node_nr as i64 {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }

    /// C `XmlTableGetValue` (xml.c:4926) minus the trailing
    /// `InputFunctionCall` (the executor owns in_functions/typioparams).
    /// `None` = SQL NULL.
    pub fn get_value(&mut self, colnum: i32, typid: ::types_core::Oid) -> PgResult<Option<Vec<u8>>> {
        let is_xml = typid == XMLOID;
        let (typcategory, _preferred) = ::lsyscache::get_type_category_preferred(typid)?;
        let is_numeric_category = typcategory == TYPCATEGORY_NUMERIC;

        let x = xml2();
        // SAFETY: fetch_row() == true precedes (executor contract), so
        // xpathobj/nodesetval/row_count index a live node.
        unsafe {
            (x.xmlSetStructuredErrorFunc)(core::ptr::null_mut(), Some(xml_error_handler));

            debug_assert!(!self.xpathobj.is_null());
            debug_assert!(!self.xpathscomp[colnum as usize].is_null());

            let obj_hdr = &*(self.xpathobj as *const xmlXPathObjectHdr);
            let ns = &*(obj_hdr.nodesetval as *const xmlNodeSetHdr);
            let cur = *ns.node_tab.add((self.row_count - 1) as usize);
            (*(self.xpathcxt as *mut xmlXPathContextHdr)).node = cur;

            let xpathobj = (x.xmlXPathCompiledEval)(self.xpathscomp[colnum as usize], self.xpathcxt);
            if xpathobj.is_null() || xml_err_occurred() {
                if !xpathobj.is_null() {
                    (x.xmlXPathFreeObject)(xpathobj);
                }
                return Err(xml_ereport(
                    "could not create XPath object",
                    ERRCODE_INVALID_ARGUMENT_FOR_XQUERY,
                )
                .into());
            }

            let result = value_from_xpathobj(xpathobj, is_xml, is_numeric_category);
            (x.xmlXPathFreeObject)(xpathobj);
            result
        }
    }

    /// C `XmlTableDestroyOpaque` (xml.c:5078).
    pub fn destroy(self) {
        let x = xml2();
        // SAFETY: all pointers are owned by self and freed exactly once here.
        unsafe {
            (x.xmlSetStructuredErrorFunc)(core::ptr::null_mut(), Some(xml_error_handler));
            for comp in &self.xpathscomp {
                if !comp.is_null() {
                    (x.xmlXPathFreeCompExpr)(*comp);
                }
            }
            if !self.xpathobj.is_null() {
                (x.xmlXPathFreeObject)(self.xpathobj);
            }
            if !self.xpathcomp.is_null() {
                (x.xmlXPathFreeCompExpr)(self.xpathcomp);
            }
            if !self.xpathcxt.is_null() {
                (x.xmlXPathFreeContext)(self.xpathcxt);
            }
            if !self.doc.is_null() {
                (x.xmlFreeDoc)(self.doc);
            }
            if !self.ctxt.is_null() {
                (x.xmlFreeParserCtxt)(self.ctxt);
            }
        }
    }
}

// The four-case value extraction in XmlTableGetValue's PG_TRY body.
unsafe fn value_from_xpathobj(
    xpathobj: *mut xmlXPathObject,
    is_xml: bool,
    is_numeric_category: bool,
) -> PgResult<Option<Vec<u8>>> {
    let x = xml2();
    // SAFETY (fn body): xpathobj is the live eval result.
    unsafe {
        let hdr = &*(xpathobj as *const xmlXPathObjectHdr);
        let take_xmlchar = |p: *mut u8| -> Vec<u8> {
            // SAFETY: p is a fresh NUL-terminated libxml string (or null).
            unsafe {
                let v = libxml::xmlchar_to_vec(p);
                if !p.is_null() {
                    x.xmlFree(p as *mut core::ffi::c_void);
                }
                v
            }
        };
        match hdr.type_ {
            XPATH_NODESET => {
                let count = if hdr.nodesetval.is_null() {
                    0
                } else {
                    (*(hdr.nodesetval as *const xmlNodeSetHdr)).node_nr
                };
                if hdr.nodesetval.is_null() || count == 0 {
                    Ok(None)
                } else if is_xml {
                    let ns = &*(hdr.nodesetval as *const xmlNodeSetHdr);
                    let mut buf: Vec<u8> = Vec::new();
                    for i in 0..count {
                        let node = *ns.node_tab.add(i as usize);
                        buf.extend_from_slice(&crate::xpath::node_to_xmltype(node)?);
                    }
                    Ok(Some(buf))
                } else if count > 1 {
                    Err(PgError::error(
                        "more than one value returned by column XPath expression",
                    )
                    .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
                    .into())
                } else {
                    Ok(Some(take_xmlchar((x.xmlXPathCastNodeSetToString)(
                        hdr.nodesetval,
                    ))))
                }
            }
            XPATH_STRING => {
                let raw = libxml::xmlchar_to_vec(hdr.stringval);
                Ok(Some(if is_xml { crate::escape_xml(&raw) } else { raw }))
            }
            XPATH_BOOLEAN => {
                let p = if !is_numeric_category {
                    (x.xmlXPathCastBooleanToString)(hdr.boolval)
                } else {
                    (x.xmlXPathCastNumberToString)((x.xmlXPathCastBooleanToNumber)(hdr.boolval))
                };
                Ok(Some(take_xmlchar(p)))
            }
            XPATH_NUMBER => Ok(Some(take_xmlchar((x.xmlXPathCastNumberToString)(
                hdr.floatval,
            )))),
            other => Err(PgError::error(format!("unexpected XPath object type {other}"))
                .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR)
                .into()),
        }
    }
}
