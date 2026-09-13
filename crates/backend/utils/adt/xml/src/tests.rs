use crate::*;

fn decl(s: &[u8]) -> (i32, usize, Option<Vec<u8>>, Option<Vec<u8>>, i32) {
    let mut work = s.to_vec();
    work.push(0);
    let mut len = 0usize;
    let mut version = None;
    let mut encoding = None;
    let mut standalone = 0i32;
    let rc = parse_xml_decl(
        &work,
        Some(&mut len),
        Some(&mut version),
        Some(&mut encoding),
        Some(&mut standalone),
    )
    .unwrap();
    (rc, len, version, encoding, standalone)
}

#[test]
fn parse_xml_decl_full() {
    let (rc, len, version, encoding, standalone) =
        decl(b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><a/>");
    assert_eq!(rc, 0);
    assert_eq!(&decl(b"<?xml version=\"1.0\"?>x").2.unwrap(), b"1.0");
    assert_eq!(version.as_deref(), Some(b"1.0".as_slice()));
    assert_eq!(encoding.as_deref(), Some(b"UTF-8".as_slice()));
    assert_eq!(standalone, 1);
    assert_eq!(len, 55);
}

#[test]
fn parse_xml_decl_absent_and_pi() {
    let (rc, len, version, _, standalone) = decl(b"<a/>");
    assert_eq!((rc, len, standalone), (0, 0, -1));
    assert!(version.is_none());
    // <?xmlfoo ...?> is a PI, not a declaration.
    let (rc, len, ..) = decl(b"<?xmlfoo bar?><a/>");
    assert_eq!((rc, len), (0, 0));
    // PG_XMLISNAMECHAR accepts every codepoint >= 0xF8 (xmlIsBaseChar_ch's
    // last alternative), so a Greek letter also names a PI.
    let (rc, len, ..) = decl("<?xml\u{3b1}?><a/>".as_bytes());
    assert_eq!((rc, len), (0, 0));
}

#[test]
fn xpath_numbers_print_like_float8out() {
    let eval = |expr: &[u8]| {
        let mut out = Vec::new();
        assert_eq!(crate::xpath::xpath_internal(expr, b"<r/>", None, Some(&mut out)).unwrap(), 1);
        String::from_utf8(out.remove(0)).unwrap()
    };
    adt_float::set_extra_float_digits(1);
    assert_eq!(eval(b"100000000000000000000"), "1e+20");
    assert_eq!(eval(b"1000000000000000"), "1e+15");
    assert_eq!(eval(b"1 div 3"), "0.3333333333333333");
    assert_eq!(eval(b"1 div 0"), "Infinity");
    adt_float::set_extra_float_digits(0);
    assert_eq!(eval(b"1 div 3"), "0.333333333333333");
    assert_eq!(eval(b"10000000000000000"), "1e+16");
    adt_float::set_extra_float_digits(1);
}

#[test]
fn parse_xml_decl_errors() {
    assert_eq!(decl(b"<?xml?>").0, 65); // space required
    assert_eq!(decl(b"<?xml standalone=\"yes\"?>").0, 96); // version missing
    assert_eq!(decl(b"<?xml version=\"1.0\" standalone=\"maybe\"?>").0, 78);
    assert_eq!(decl(b"<?xml version=\"1.0\" encoding?>").0, 101);
    assert_eq!(decl(b"<?xml version=\"1.0\" ").0, 57);
}

#[test]
fn print_xml_decl_forms() {
    let mut buf = Vec::new();
    assert!(!print_xml_decl(&mut buf, Some(b"1.0"), 0, -1));
    assert!(buf.is_empty());
    assert!(print_xml_decl(&mut buf, Some(b"1.1"), 0, -1));
    assert_eq!(buf, b"<?xml version=\"1.1\"?>");
    buf.clear();
    assert!(print_xml_decl(&mut buf, None, 0, 0));
    assert_eq!(buf, b"<?xml version=\"1.0\" standalone=\"no\"?>");
}

#[test]
fn doctype_in_content_forms() {
    assert!(xml_doctype_in_content(b"<!DOCTYPE a><a/>\0"));
    assert!(xml_doctype_in_content(b"  <!-- c --> <?pi x?> <!DOCTYPE a><a/>\0"));
    assert!(!xml_doctype_in_content(b"<a/>\0"));
    assert!(!xml_doctype_in_content(b"text\0"));
}

#[test]
fn escape_xml_specials() {
    assert_eq!(escape_xml(b"a<b>&c\rd"), b"a&lt;b&gt;&amp;c&#x0d;d".to_vec());
}

#[test]
fn xmlcomment_validation() {
    assert_eq!(xmlcomment(b"hello").unwrap(), b"<!--hello-->".to_vec());
    assert!(xmlcomment(b"a--b").is_err());
    assert!(xmlcomment(b"tail-").is_err());
    assert_eq!(xmlcomment(b"").unwrap(), b"<!---->".to_vec());
}

#[test]
fn xmlpi_validation() {
    assert!(xmlpi("xml", None).is_err());
    assert!(xmlpi("XmL", None).is_err());
    assert_eq!(xmlpi("php", None).unwrap(), None);
    assert!(xmlpi("php", Some(b"echo ?>")).is_err());
    assert_eq!(
        xmlpi("php", Some(b"  echo 'x';")).unwrap().unwrap(),
        b"<?php echo 'x';?>".to_vec()
    );
}

#[test]
fn xmlroot_rewrites_decl() {
    let out = xmlroot(
        b"<?xml version=\"1.0\"?><a/>",
        Some(b"1.1"),
        XmlStandaloneType::XML_STANDALONE_YES,
    )
    .unwrap();
    assert_eq!(out, b"<?xml version=\"1.1\" standalone=\"yes\"?><a/>".to_vec());
    let out = xmlroot(b"<a/>", None, XmlStandaloneType::XML_STANDALONE_OMITTED).unwrap();
    assert_eq!(out, b"<a/>".to_vec());
}

#[test]
fn map_identifiers_round() {
    assert_eq!(
        map_sql_identifier_to_xml_name(b"foo_xk", true, true).unwrap(),
        b"foo_x005F_xk".to_vec()
    );
    assert_eq!(
        map_sql_identifier_to_xml_name(b"xmlfoo", true, true).unwrap(),
        b"_x0078_mlfoo".to_vec()
    );
    assert_eq!(
        map_xml_name_to_sql_identifier(b"_x0078_mlfoo").unwrap(),
        b"xmlfoo".to_vec()
    );
}

#[test]
fn errdetail_codes() {
    assert_eq!(errdetail_for_xml_code(9), "Invalid character value.");
    assert_eq!(errdetail_for_xml_code(4242), "Unrecognized libxml error code: 4242.");
}

// upstream 4c777d6dd9c9 (18.6): Fix handling of namespace nodes in xpath() (xml)
// A namespace node (XML_NAMESPACE_DECL) is an xmlNs, not an xmlNode: routing
// it through xmlCopyNode + xmlNodeDump returned the raw ` xmlns:foo="..."`
// declaration (18.3 regress: "could not copy node" on other libxml2 builds);
// C 18.6 casts it to its string value like attribute/text nodes.
#[test]
fn xpath_namespace_node_casts_to_string() {
    let mut out = Vec::new();
    let n = crate::xpath::xpath_internal(
        b"//namespace::foo",
        b"<root xmlns:foo=\"http://127.0.0.1\"/>",
        None,
        Some(&mut out),
    )
    .unwrap();
    assert_eq!(n, 1);
    assert_eq!(out, vec![b"http://127.0.0.1".to_vec()]);
}

// xml.c:2243-2262 xml_errorHandler: outside LEGACY strictness, level >=
// XML_ERR_ERROR is buffered for xml_ereport, level >= XML_ERR_WARNING is
// ereport(WARNING)ed, and everything below (libxml's informational level)
// is ereport(NOTICE)ed — never dropped.
#[test]
fn xml_error_handler_reports_below_warning_level_as_notice() {
    use core::ffi::c_void;
    use std::ffi::CString;

    use crate::errhandler::{
        set_strictness_for_test, take_pending_reports, xml_error_handler, PG_XML_STRICTNESS_ALL,
    };
    use crate::libxml::xmlErrorHdr;

    fn raise(level: i32, message: &str) {
        let msg = CString::new(message).unwrap();
        let mut err = xmlErrorHdr {
            domain: 1, // XML_FROM_PARSER
            code: 0,
            message: msg.as_ptr() as *mut _,
            level,
            file: core::ptr::null_mut(),
            line: 0,
            str1: core::ptr::null_mut(),
            str2: core::ptr::null_mut(),
            str3: core::ptr::null_mut(),
            int1: 0,
            int2: 0,
            ctxt: core::ptr::null_mut(),
            node: core::ptr::null_mut(),
        };
        // SAFETY: a fully initialised xmlError header, live for the call.
        unsafe { xml_error_handler(core::ptr::null_mut(), &mut err as *mut _ as *mut c_void) };
    }

    set_strictness_for_test(PG_XML_STRICTNESS_ALL);
    raise(1, "a warning\n"); // XML_ERR_WARNING
    raise(0, "an informational diagnostic\n"); // XML_ERR_NONE
    let reports = take_pending_reports();
    assert_eq!(
        reports,
        vec![
            (types_error::WARNING, "a warning".to_string()),
            (types_error::NOTICE, "an informational diagnostic".to_string()),
        ]
    );
    set_strictness_for_test(0);
}
