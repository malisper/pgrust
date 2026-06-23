//! ABI vocabulary for the XML data type subsystem.
//!
//! Mirrors the C declarations in `src/include/utils/xml.h` and the
//! `XmlOptionType` / `XmlExprOp` enums in `src/include/nodes/primnodes.h`.
//!
//! The `xmltype` on-disk/Datum type is *defined to be* `struct varlena`
//! (`typedef struct varlena xmltype;` in `utils/xml.h`), and the C comments in
//! `xml.c` note that `xmltype` and `text` share an identical representation.
//! We therefore do NOT introduce a second varlena struct here; ported code
//! reuses the varlena vocabulary from `backend-utils-{varlena,adt-varlena}`.
//! What lives here is the small set of plain-`int` enum tags that cross the
//! Datum / catalog / GUC boundary, locked to their C discriminant values.

use core::ffi::c_int;

// ---------------------------------------------------------------------------
// const-assert helper
// ---------------------------------------------------------------------------

/// Compile-time assertion: forces a build error when `$cond` is false.
macro_rules! const_assert {
    ($cond:expr) => {
        const _: [(); 0 - !{
            const ASSERT: bool = $cond;
            ASSERT
        } as usize] = [];
    };
}

// ---------------------------------------------------------------------------
// XmlOptionType  (nodes/primnodes.h)
// ---------------------------------------------------------------------------

/// `XmlOptionType` (`nodes/primnodes.h`): DOCUMENT or CONTENT.
///
/// Carried by `XmlExpr.xmloption` and by the `xmloption` GUC (as an int).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XmlOptionType {
    /// `XMLOPTION_DOCUMENT` == 0.
    Document = 0,
    /// `XMLOPTION_CONTENT` == 1.
    Content = 1,
}

const_assert!(XmlOptionType::Document as c_int == 0);
const_assert!(XmlOptionType::Content as c_int == 1);

// ---------------------------------------------------------------------------
// XmlBinaryType  (utils/xml.h)
// ---------------------------------------------------------------------------

/// `XmlBinaryType` (`utils/xml.h`): how binary values map to XML.
///
/// Carried by the `xmlbinary` GUC (as an int).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XmlBinaryType {
    /// `XMLBINARY_BASE64` == 0.
    Base64 = 0,
    /// `XMLBINARY_HEX` == 1.
    Hex = 1,
}

const_assert!(XmlBinaryType::Base64 as c_int == 0);
const_assert!(XmlBinaryType::Hex as c_int == 1);

// ---------------------------------------------------------------------------
// XmlStandaloneType  (utils/xml.h)
// ---------------------------------------------------------------------------

/// `XmlStandaloneType` (`utils/xml.h`): the `standalone` attribute state of an
/// XML declaration.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XmlStandaloneType {
    /// `XML_STANDALONE_YES` == 0.
    Yes = 0,
    /// `XML_STANDALONE_NO` == 1.
    No = 1,
    /// `XML_STANDALONE_NO_VALUE` == 2.
    NoValue = 2,
    /// `XML_STANDALONE_OMITTED` == 3.
    Omitted = 3,
}

const_assert!(XmlStandaloneType::Yes as c_int == 0);
const_assert!(XmlStandaloneType::No as c_int == 1);
const_assert!(XmlStandaloneType::NoValue as c_int == 2);
const_assert!(XmlStandaloneType::Omitted as c_int == 3);

// ---------------------------------------------------------------------------
// PgXmlStrictness  (utils/xml.h)
// ---------------------------------------------------------------------------

/// `PgXmlStrictness` (`utils/xml.h`): the strictness argument to `pg_xml_init`,
/// controlling which libxml diagnostics are escalated to PostgreSQL errors.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgXmlStrictness {
    /// `PG_XML_STRICTNESS_LEGACY` == 0: ignore errors unless the function
    /// result itself indicates an error condition.
    Legacy = 0,
    /// `PG_XML_STRICTNESS_WELLFORMED` == 1: ignore non-parser messages.
    Wellformed = 1,
    /// `PG_XML_STRICTNESS_ALL` == 2: report all notices/warnings/errors.
    All = 2,
}

const_assert!(PgXmlStrictness::Legacy as c_int == 0);
const_assert!(PgXmlStrictness::Wellformed as c_int == 1);
const_assert!(PgXmlStrictness::All as c_int == 2);

// ---------------------------------------------------------------------------
// XmlExprOp  (nodes/primnodes.h)
// ---------------------------------------------------------------------------

/// `XmlExprOp` (`nodes/primnodes.h`): which SQL/XML construct an `XmlExpr`
/// represents.  Locked to its C discriminant values.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XmlExprOp {
    /// `IS_XMLCONCAT` — `XMLCONCAT(args)`.
    XmlConcat = 0,
    /// `IS_XMLELEMENT` — `XMLELEMENT(name, xml_attributes, args)`.
    XmlElement = 1,
    /// `IS_XMLFOREST` — `XMLFOREST(xml_attributes)`.
    XmlForest = 2,
    /// `IS_XMLPARSE` — `XMLPARSE(text, is_doc, preserve_ws)`.
    XmlParse = 3,
    /// `IS_XMLPI` — `XMLPI(name [, args])`.
    XmlPi = 4,
    /// `IS_XMLROOT` — `XMLROOT(xml, version, standalone)`.
    XmlRoot = 5,
    /// `IS_XMLSERIALIZE` — `XMLSERIALIZE(is_document, xmlval, indent)`.
    XmlSerialize = 6,
    /// `IS_DOCUMENT` — `xmlval IS DOCUMENT`.
    IsDocument = 7,
}

const_assert!(XmlExprOp::XmlConcat as c_int == 0);
const_assert!(XmlExprOp::IsDocument as c_int == 7);

// ---------------------------------------------------------------------------
// GUC default values  (xml.c)
// ---------------------------------------------------------------------------

/// `int xmlbinary = XMLBINARY_BASE64;` (xml.c) — GUC default for `xmlbinary`.
pub const XMLBINARY_DEFAULT: XmlBinaryType = XmlBinaryType::Base64;

/// `int xmloption = XMLOPTION_CONTENT;` (xml.c) — GUC default for `xmloption`.
pub const XMLOPTION_DEFAULT: XmlOptionType = XmlOptionType::Content;
