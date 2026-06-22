//! ABI vocabulary for the XML data type subsystem (`utils/adt/xml.c`).
//!
//! Mirrors the C declarations in `src/include/utils/xml.h`. `XmlOptionType` and
//! `XmlExprOp` live in `types-nodes` (they come from `nodes/primnodes.h`); what
//! lives here is the remaining `utils/xml.h` enum vocabulary plus the owned data
//! shapes the SPI/catalog seams of `xml.c` marshal across crate boundaries.
//!
//! The `xmltype` Datum type is `typedef struct varlena xmltype;`
//! (text-compatible), so ported bodies carry payloads as `Vec<u8>`/`String` like
//! the rest of the varlena family; no separate varlena struct is introduced.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;

/// `XmlBinaryType` (`utils/xml.h`): how binary values map to XML. Carried by the
/// `xmlbinary` GUC (as an int).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum XmlBinaryType {
    /// `XMLBINARY_BASE64` == 0.
    XMLBINARY_BASE64 = 0,
    /// `XMLBINARY_HEX` == 1.
    XMLBINARY_HEX = 1,
}

/// `XmlStandaloneType` (`utils/xml.h`): the `standalone` attribute state of an
/// XML declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum XmlStandaloneType {
    /// `XML_STANDALONE_YES` == 0.
    XML_STANDALONE_YES = 0,
    /// `XML_STANDALONE_NO` == 1.
    XML_STANDALONE_NO = 1,
    /// `XML_STANDALONE_NO_VALUE` == 2.
    XML_STANDALONE_NO_VALUE = 2,
    /// `XML_STANDALONE_OMITTED` == 3.
    XML_STANDALONE_OMITTED = 3,
}

/// `PgXmlStrictness` (`utils/xml.h`): the strictness argument to `pg_xml_init`,
/// controlling which libxml diagnostics are escalated to PostgreSQL errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum PgXmlStrictness {
    /// `PG_XML_STRICTNESS_LEGACY` == 0: ignore errors unless the function result
    /// itself indicates an error condition.
    PG_XML_STRICTNESS_LEGACY = 0,
    /// `PG_XML_STRICTNESS_WELLFORMED` == 1: ignore non-parser messages.
    PG_XML_STRICTNESS_WELLFORMED = 1,
    /// `PG_XML_STRICTNESS_ALL` == 2: report all notices/warnings/errors.
    PG_XML_STRICTNESS_ALL = 2,
}

/// `int xmlbinary = XMLBINARY_BASE64;` (xml.c:109) — GUC default for `xmlbinary`.
pub const XMLBINARY_DEFAULT: XmlBinaryType = XmlBinaryType::XMLBINARY_BASE64;

// ---------------------------------------------------------------------------
// Owned data shapes carried across the SPI / catalog seams of xml.c.
//
// These are idiomatic owned shapes (they own `String`/`Vec`), not on-disk /
// Datum-ABI types, so they carry no `#[repr(C)]`. They are the shared vocabulary
// of the `table_to_xml` / `query_to_xml` / `cursor_to_xml` family and the
// SQL/XML schema mappers, moved across the SPI / syscache / `pg_class` /
// `pg_type` seams.
// ---------------------------------------------------------------------------

/// One column of a tuple descriptor exposed through the SPI/catalog seams.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpiColumn {
    /// `SPI_fname` — the column name.
    pub name: String,
    /// `SPI_gettypeid` — the column type OID.
    pub typeid: Oid,
    /// `attisdropped` — whether the column is a dropped attribute.
    pub is_dropped: bool,
}

/// One row's column values, parallel to the descriptor. `None` is SQL NULL;
/// `Some(s)` is the value rendered to its text image by the column type's output
/// function (the SPI provider's faithful `SPI_getvalue`). The xml consumer
/// applies `map_sql_value_to_xml_value` over these (see the `spi_execute_select`
/// seam note).
pub type SpiRow = Vec<Option<String>>;

/// The raw (`SPI_getbinval`) image of one column value, as needed by the xml
/// `SPI_sql_row_to_xmlelement` path to run `map_sql_value_to_xml_value` (whose
/// XSD special-cases for bool/date/timestamp[tz]/bytea require the raw Datum,
/// not the type's default text rendering). `None` is SQL NULL. `Some` carries
/// the pass-by-value Datum word and — for a pass-by-reference column — the
/// verbatim header-ful varlena byte image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpiRawValue {
    /// The pass-by-value Datum machine word (`0` for a by-reference column).
    pub word: u64,
    /// `Some(image)` for a pass-by-reference column (header-ful varlena bytes);
    /// `None` for a pass-by-value column.
    pub byref: Option<Vec<u8>>,
}

/// One row's raw column values, parallel to [`SpiRow`].
pub type SpiRawRow = Vec<Option<SpiRawValue>>;

/// A SELECT result surfaced through the SPI seam.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SpiResult {
    /// The result tuple descriptor.
    pub columns: Vec<SpiColumn>,
    /// One [`SpiRow`] per processed tuple.
    pub rows: Vec<SpiRow>,
    /// One [`SpiRawRow`] per processed tuple, parallel to `rows` — the raw
    /// `SPI_getbinval` images the xml value mapping consumes.
    pub raw_rows: Vec<SpiRawRow>,
}

/// `pg_class` metadata for a relation, as needed by `map_sql_table_to_xmlschema`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationInfo {
    /// `relname`.
    pub relname: String,
    /// `relnamespace`.
    pub relnamespace: Oid,
    /// The relation's columns (`rd_att`).
    pub columns: Vec<RelationColumn>,
}

/// One relation column, parallel to `Form_pg_attribute`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationColumn {
    /// `attname`.
    pub attname: String,
    /// `atttypid`.
    pub atttypid: Oid,
    /// `attisdropped`.
    pub is_dropped: bool,
}

/// `pg_type` metadata for a type, as needed by `map_sql_type_to_xml_name`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeInfo {
    /// `typname`.
    pub typname: String,
    /// `typnamespace`.
    pub typnamespace: Oid,
    /// `typtype == TYPTYPE_DOMAIN`.
    pub is_domain: bool,
}
