//! Provider seams that `src/backend/utils/adt/xml.c` reaches through into other
//! subsystems that are not ported yet: libxml2, SPI, the syscache/catalog/fmgr,
//! the `xmlbinary`/`xmloption` GUCs, the encoding conversions, and utils/mb.
//!
//! Almost every interesting body in `xml.c` is *pure* string work once its
//! inputs are in hand, and that pure logic is ported 1:1 in
//! `backend-utils-adt-xml`. Only the cross-subsystem call-outs are seamed here.
//! None of these owners is ported yet, so every seam loud-panics until its real
//! provider lands — exactly the `--without-libxml` design of the C file, which
//! `have_libxml` preserves: when the installed provider reports `false`, the
//! in-crate code raises `NO_XML_SUPPORT()` and never touches the parse/serialize
//! seams.
//!
//! This crate intentionally has no name-matched owner crate (`...-libxml`); the
//! seam-install guard exempts it, since these are outward dependency seams of an
//! unported substrate, not `backend-utils-adt-xml`'s own inward contract.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;
use types_error::{PgError, PgResult};
use nodes::primnodes::XmlOptionType;
use types_xml::{
    RelationColumn, RelationInfo, SpiColumn, SpiResult, TypeInfo, XmlBinaryType,
};

// ===========================================================================
// 1. libxml2 seams (utils/adt/xml.c `#ifdef USE_LIBXML`)
// ===========================================================================

seam_core::seam!(
    /// C: the `NO_XML_SUPPORT()` macro condition — whether libxml support is
    /// compiled in at all. A `--without-libxml` server's provider returns `false`.
    pub fn have_libxml() -> bool
);

seam_core::seam!(
    /// C: `xmlIsBlank_ch(c)` — XML S (whitespace) for a single byte.
    pub fn is_blank_ch(c: u8) -> PgResult<bool>
);

seam_core::seam!(
    /// C: `xmlGetUTF8Char(utf8, &len)` — decode one UTF-8 codepoint (used by
    /// `parse_xml_decl` to classify the char after `<?xml`); `-1` on error.
    pub fn get_utf8_char(utf8: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// C: `xml_parse(...)` core — parse `data` (server-encoded, with declared
    /// `encoding`) as a document or content fragment under `xmloption`,
    /// validating well-formedness. The inner `Result<bool, PgError>` selects
    /// soft-error reporting: `Ok(Err(soft))` for malformed input when a soft
    /// context is wanted, else the caller escalates to a hard `Err`.
    pub fn xml_parse_libxml(
        data: &[u8],
        xmloption_arg: XmlOptionType,
        preserve_whitespace: bool,
        encoding: i32,
    ) -> PgResult<core::result::Result<bool, PgError>>
);

seam_core::seam!(
    /// C: the `xmlEncodeSpecialChars` path of `xmltext`.
    pub fn encode_special_chars(arg: &[u8]) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: the libxml serialization in `xmltotext_with_options`.
    pub fn serialize_with_options(
        data: &[u8],
        xmloption_arg: XmlOptionType,
        indent: bool,
        encoding: i32,
    ) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: `xmlelement` — build an element from a name, named (name, value)
    /// attribute pairs, and content fragments, via `xmlTextWriter*`.
    pub fn build_element(
        name: String,
        named_args: Vec<(String, Option<String>)>,
        content: Vec<String>,
    ) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: the `BYTEAOID` arm of `map_sql_value_to_xml_value` —
    /// `xmlTextWriterWriteBase64` / `WriteBinHex` over a bytea value.
    pub fn encode_binary(bytes: &[u8], binary: XmlBinaryType) -> PgResult<String>
);

seam_core::seam!(
    /// C: `xpath_internal` — evaluate `xpath_expr` against `data` with the given
    /// `(name, uri)` namespace mappings, returning the matched values as
    /// serialized `xmltype` byte images. `count_only` mirrors C's
    /// `astate == NULL` path: a provider may skip per-node serialization and
    /// return placeholder entries whose *count* is all the caller inspects.
    pub fn xpath_eval(
        xpath_expr: &[u8],
        data: &[u8],
        namespaces: &[(String, String)],
        count_only: bool,
        database_encoding: i32,
    ) -> PgResult<Vec<Vec<u8>>>
);

// ---------------------------------------------------------------------------
// XMLTABLE table-builder routines (xml.c `XmlTable*`, `#ifdef USE_LIBXML`).
//
// C stores the per-scan `XmlTableBuilderData` (libxml parser ctxt, parsed doc,
// XPath context, compiled row/column expressions, the current node-set object
// and row cursor) in `TableFuncScanState->opaque`. The libxml object lifecycle
// is wholly internal to this provider, and a TableFuncScan node always runs to
// completion (filling a tuplestore in one pass) before any other executor node
// runs — exactly the invariant `XmlTableInitOpaque`'s comment relies on — so
// the provider holds the builder state in a per-backend thread-local set by
// `xmltable_init_opaque` and cleared by `xmltable_destroy_opaque`. These seams
// are therefore parameter-only.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// C: `XmlTableInitOpaque(state, natts)` — allocate the builder state and
    /// the libxml parser context, sized for `natts` columns.
    pub fn xmltable_init_opaque(natts: i32) -> PgResult<()>
);

seam_core::seam!(
    /// C: `XmlTableSetDocument(state, value)` — install the input document.
    /// `xml_image` is the `xml_out_internal` text of the `xmltype` value (the
    /// encoding-stripped serialized form, the consumer crate renders it).
    pub fn xmltable_set_document(xml_image: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// C: `XmlTableSetNamespace(state, name, uri)`.
    pub fn xmltable_set_namespace(name: Option<&str>, uri: &str) -> PgResult<()>
);

seam_core::seam!(
    /// C: `XmlTableSetRowFilter(state, path)` — compile the row-filter XPath.
    pub fn xmltable_set_row_filter(path: &str) -> PgResult<()>
);

seam_core::seam!(
    /// C: `XmlTableSetColumnFilter(state, path, colnum)` — compile a column XPath.
    pub fn xmltable_set_column_filter(path: &str, colnum: i32) -> PgResult<()>
);

seam_core::seam!(
    /// C: `XmlTableFetchRow(state)` — advance the row cursor; `false` at end.
    pub fn xmltable_fetch_row() -> PgResult<bool>
);

seam_core::seam!(
    /// C: `XmlTableGetValue(state, colnum, typid, typmod, &isnull)` reduced to
    /// its libxml core: evaluate the column XPath against the current row node
    /// and return the textual value to feed the column's input function, or
    /// `None` when the result is empty (the C `*isnull = true`). The
    /// `InputFunctionCall` is owned by the executor (it holds `in_functions`/
    /// `typioparams`), so the provider returns the cstring, not the Datum.
    ///
    /// `is_xml` is `typid == XMLOID` (selects the XML-serialization arms of the
    /// node-set and string cases); `is_numeric_category` is
    /// `get_type_category_preferred(typid).0 == TYPCATEGORY_NUMERIC` (the
    /// XPATH_BOOLEAN arm casts boolean→number→string when the target is numeric),
    /// both resolved by the consumer crate which owns the catalog lookups.
    pub fn xmltable_get_value(
        colnum: i32,
        is_xml: bool,
        is_numeric_category: bool,
    ) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// C: `XmlTableDestroyOpaque(state)` — free all libxml resources and clear
    /// the builder state.
    pub fn xmltable_destroy_opaque() -> PgResult<()>
);

// ===========================================================================
// 2. syscache + catalog + fmgr seams
// ===========================================================================

seam_core::seam!(
    /// C: `get_database_name(MyDatabaseId)`.
    pub fn get_database_name() -> PgResult<String>
);

seam_core::seam!(
    /// C: `get_namespace_name(nspid)`.
    pub fn get_namespace_name(nspid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// C: `LookupExplicitNamespace(name, false)`.
    pub fn lookup_namespace(name: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// C: `get_rel_name(relid)`.
    pub fn get_rel_name(relid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// C: `DatumGetCString(DirectFunctionCall1(regclassout, relid))` —
    /// `table_to_xml_internal`'s schema-qualified relation name for the
    /// internally-built `SELECT * FROM <regclassout>` query. Unlike
    /// `get_rel_name` (the unqualified XML element name), this qualifies the
    /// relation with its schema when it is not visible on the search_path, so
    /// the generated query resolves regardless of where the table lives.
    pub fn regclass_name(relid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// C: `get_typtype(typeoid) == TYPTYPE_DOMAIN`.
    pub fn is_domain(typeoid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// C: `getBaseTypeAndTypmod(typeoid, &typmod)`.
    pub fn get_base_type_and_typmod(typeoid: Oid, typmod: i32) -> PgResult<(Oid, i32)>
);

seam_core::seam!(
    /// C: `getTypeOutputInfo(type, &typeOut, &isvarlena)` +
    /// `OidOutputFunctionCall(typeOut, value)`. `value` is the raw Datum bits.
    pub fn output_function_call(typeoid: Oid, value: u64) -> PgResult<String>
);

seam_core::seam!(
    /// C: the array-deconstruction prologue of `map_sql_value_to_xml_value`'s
    /// array branch (`DatumGetArrayTypeP` / `ARR_ELEMTYPE` / `deconstruct_array`).
    /// `value` is the raw Datum bits; returns the element type OID and one entry
    /// per slot (`None` for a NULL element, `Some(bits)` for the raw element
    /// Datum).
    pub fn deconstruct_array(value: u64) -> PgResult<(Oid, Vec<Option<u64>>)>
);

seam_core::seam!(
    /// C: `DatumGetByteaPP(value)` then `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` —
    /// detoast a `bytea` Datum and return its raw payload bytes (not the textual
    /// output-func representation).
    pub fn detoast_bytea(value: u64) -> PgResult<Vec<u8>>
);

// ===========================================================================
// 3. GUC + encoding seams
// ===========================================================================

seam_core::seam!(
    /// C: `int xmlbinary` GUC.
    pub fn xmlbinary() -> XmlBinaryType
);

seam_core::seam!(
    /// C: `int xmloption` GUC.
    pub fn xmloption() -> XmlOptionType
);

seam_core::seam!(
    /// C: `pg_get_client_encoding()` — the client encoding id (for `xml_send`).
    pub fn client_encoding() -> i32
);

seam_core::seam!(
    /// C: `GetDatabaseEncoding()` — the server/database encoding id.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    /// C: `pg_unicode_to_server(u, buf)` — encode one Unicode codepoint into the
    /// server encoding (used by `map_xml_name_to_sql_identifier`).
    pub fn unicode_to_server(codepoint: u32) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: `pg_any_to_server(s, len, encoding)` — convert `bytes` from `encoding`
    /// to the server (database) encoding (used by `xml_recv`).
    pub fn any_to_server(bytes: &[u8], encoding: i32) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: `pg_server_to_any(s, len, encoding)` — convert `bytes` from the server
    /// encoding to `encoding` (used by `sqlchar_to_unicode`, target `PG_UTF8`).
    pub fn server_to_any(bytes: &[u8], encoding: i32) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// C: `pq_sendtext`'s `pg_server_to_client(str, len)` (used by `xml_send`).
    pub fn server_to_client(bytes: &[u8]) -> PgResult<Vec<u8>>
);

// ===========================================================================
// 4. utils/mb seams
// ===========================================================================

seam_core::seam!(
    /// C: `pg_mblen(mbstr)` (mbutils.c) — byte length of the leading encoded
    /// character in the server encoding. Used by the identifier-mapping family.
    pub fn pg_mblen(bytes: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// C: `sqlchar_to_unicode(s)` (xml.c, static) — the Unicode codepoint of the
    /// first server-encoding character of `s`.
    pub fn sqlchar_to_unicode(s: &[u8]) -> PgResult<u32>
);

// ===========================================================================
// 5. SPI seams (the table/query/cursor-to-xml family)
// ===========================================================================

seam_core::seam!(
    /// C: `SPI_connect(); SPI_execute(query, true, 0); ...; SPI_finish();`
    /// returning the produced rows, each value rendered to its text image via
    /// the column type's output function (`SPI_getvalue` =
    /// `getTypeOutputInfo` + `OidOutputFunctionCall`), or a soft error.
    ///
    /// NOTE (contract): the SPI provider renders each value with its *default*
    /// output function, the faithful `SPI_getvalue` behaviour. C's
    /// `SPI_sql_row_to_xmlelement` additionally runs `map_sql_value_to_xml_value`
    /// over each value (ISO datetime, `true`/`false` for bool, base64 for bytea,
    /// XML escaping), which lives in xml.c — i.e. in this consumer crate, not in
    /// the SPI owner. The consumer therefore owns applying
    /// `map_sql_value_to_xml_value` to the returned strings; for the common
    /// catalog columns (oid / name / text) the two renderings coincide.
    pub fn spi_execute_select(query: &str) -> PgResult<SpiResult>
);

seam_core::seam!(
    /// C: `SPI_cursor_find(name)` + `SPI_cursor_fetch(portal, true, count)`.
    /// Values are output-function-rendered; see [`spi_execute_select`] for the
    /// `map_sql_value_to_xml_value` ownership note.
    pub fn spi_cursor_fetch(name: &str, count: i32) -> PgResult<SpiResult>
);

seam_core::seam!(
    /// C: the tuple descriptor of a prepared/opened query.
    pub fn spi_query_tupdesc(query: &str) -> PgResult<Vec<SpiColumn>>
);

seam_core::seam!(
    /// C: `SPI_cursor_find(name)->tupDesc`.
    pub fn spi_cursor_tupdesc(name: &str) -> PgResult<Vec<SpiColumn>>
);

// ===========================================================================
// 6. catalog seams naming owned metadata shapes
// ===========================================================================

seam_core::seam!(
    /// C: `SearchSysCache1(RELOID, relid)` `pg_class` metadata.
    pub fn relation_info(relid: Oid) -> PgResult<RelationInfo>
);

seam_core::seam!(
    /// C: `table_open(relid, AccessShareLock)->rd_att` columns.
    pub fn relation_columns(relid: Oid) -> PgResult<Vec<RelationColumn>>
);

seam_core::seam!(
    /// C: `SearchSysCache1(TYPEOID, typeoid)` `pg_type` metadata.
    pub fn type_info(typeoid: Oid) -> PgResult<TypeInfo>
);
