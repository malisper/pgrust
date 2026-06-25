# Audit: backend-utils-adt-xml (`src/backend/utils/adt/xml.c`)

Result: **PASS**

Method: function-by-function comparison of `crates/backend-utils-adt-xml/src/lib.rs`
against the C (`postgres-18.3/src/backend/utils/adt/xml.c`) and the idiomatic
reference port (`pgrust/src-idiomatic/.../xml.c`). The port is a faithful copy of
the src-idiomatic port with import-path / seam-model / enum-variant-name
reconciliation only — verified by a normalized diff (seam prefixes folded) that
shows ZERO logic changes, only:
  - import paths re-homed to the per-owner crates,
  - C-faithful enum variant names (`XMLOPTION_DOCUMENT`, `XML_STANDALONE_YES`,
    `XMLBINARY_BASE64`, …) replacing the reference's short names,
  - the crate-local `seam::` module replaced by `backend-utils-adt-xml-libxml-seams`,
  - `seam::no_xml_support()` replaced by a local `no_xml_support()` helper with
    identical message/sqlstate/detail,
  - the single `emit_report(WARNING …)` in `xml_out_internal` routed through
    `backend-utils-error-elog-seams::ereport_msg`,
  - added `escape_xml_seam` adapter + `init_seams()` installer.

## Function inventory

Every function present in the C file (and the reference port) is present here.
Normalized diff of the function-name lists shows the port adds exactly three
helpers (`no_xml_support`, `escape_xml_seam`, `init_seams`) and drops nothing.

Pure-logic functions (ported 1:1, no libxml): `append_string_info_text`,
`stringinfo_to_xmltype`, `cstring_to_xmltype`, `xmlBuffer_to_xmltype`,
`xmlChar_to_encoding`, `xmlconcat2`, `texttoxml`, `xmltotext`,
`chopStringInfoNewlines`, `appendStringInfoLineSeparator`, `xml_pnstrdup`,
`pg_xmlCharStrndup`, `xml_pstrdup_and_free`, `find_subslice`,
`pg_xml_is_name_char`, `parse_xml_decl`, `skip_xml_space`, `starts_with_at`,
`memchr_from`, `strnlen`, `print_xml_decl`, `xml_doctype_in_content`,
`xml_text2xmlChar`, `is_valid_xml_namefirst`/`is_valid_xml_namechar` (via the
in-crate `chvalid` range tables), `escape_xml`, the `xsd_schema_element_*` /
`xmldata_root_element_*` emitters, the whole table/query/cursor/schema/database
publishing family, the `map_sql_*_to_xmlschema_type(s)` mappers,
`SPI_sql_row_to_xmlelement`, `errdetail_for_xml_code`, and the `datum_*`
extractors. `chvalid.rs` carries the libxml2 v2.9.14 chvalid range tables verbatim
(XML 1.0 Appendix B productions [85]–[89]).

libxml/SPI/catalog/GUC/encoding/mb-dependent functions (`xml_in`,
`xml_out_internal`, `xml_recv`, `xml_send`, `xmlcomment`, `xmltext`, `xmlconcat`,
`xmltotext_with_options`, `xmlelement`, `xmlparse`, `xmlpi`, `xmlroot`,
`xmlvalidate`, `xml_is_document`, `xml_parse`, `map_sql_identifier_to_xml_name`,
`map_xml_name_to_sql_identifier`, `map_sql_value_to_xml_value`,
`sqlchar_to_unicode`, `xpath`/`xpath_internal`/`xmlexists`/`xpath_exists`,
`wellformed_xml`, `xml_is_well_formed{,_document,_content}`, the `XmlTable*`
family, `pg_xml_init*`/`pg_xml_done`/`pg_xml_error_occurred`, and the
SPI/catalog visibility helpers) route their cross-subsystem work through the
`backend-utils-adt-xml-libxml-seams` provider seams, preserving the C file's own
`--without-libxml` design: `have_libxml() == false` raises `NO_XML_SUPPORT()`
exactly as a `--without-libxml` server would, and the parse/serialize seams are
never touched.

## Parity spot-checks (vs C)

- `DatumGetBool` = `(X != 0)` (postgres.h:95) → `value.as_usize() != 0`. OK.
- `DatumGetDateADT`/`DatumGetTimestamp` are the low 32 / full 64 Datum bits. OK.
- `XmlStandaloneType` mapping to `orig_standalone` 1/0/-1/(unchanged) matches
  `xml_out_internal` (xml.c). OK.
- `XMLBINARY_BASE64` vs `XMLBINARY_HEX` branch in `map_sql_value_to_xml_value`
  bytea arm matches xml.c:2636. OK.
- enum discriminants (`types-xml`): `XML_STANDALONE_*` 0–3, `XMLBINARY_*` 0/1,
  `PG_XML_STRICTNESS_*` 0–2 verified vs `include/utils/xml.h:27-45`. OK.
- `XMLBINARY_DEFAULT == XMLBINARY_BASE64`, GUC default verified vs xml.c:109. OK.
- `BYTEAOID == 17` verified vs `catalog/pg_type.dat`. OK.
- file-scope consts (`PG_XML_DEFAULT_VERSION`, `NAMESPACE_XSD/XSI/SQLXML`,
  `ERRCXT_MAGIC`, `XMLTABLE_CONTEXT_MAGIC`, `XML_ERR_*`, `MAX_MULTIBYTE_CHAR_LEN`)
  unchanged from the reference. OK.
- SQLSTATEs in error paths (`ERRCODE_INVALID_PARAMETER_VALUE`,
  `ERRCODE_INVALID_XML_COMMENT`, `ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION`,
  `ERRCODE_FEATURE_NOT_SUPPORTED`, `ERRCODE_NOT_AN_XML_DOCUMENT`,
  `ERRCODE_DATA_EXCEPTION`, `ERRCODE_DATETIME_VALUE_OUT_OF_RANGE`,
  `ERRCODE_INVALID_ARGUMENT_FOR_XQUERY`) preserved. OK.

## Seams

- INWARD (owned): `backend-utils-adt-xml-seams::escape_xml(mcx, &str) -> PgString`
  is INSTALLED via `init_seams()` (adapter `escape_xml_seam` over the byte-level
  `escape_xml`). Consumer: `backend-commands-explain-format`.
- OUTWARD provider seams (`backend-utils-adt-xml-libxml-seams`): libxml, SPI,
  syscache/catalog/fmgr, GUC, encoding, mb. Owners unported → loud-panic until a
  provider lands (mirror-PG-and-panic). No name-matched owner crate, so the
  seam-install guard correctly exempts them.

## Notes / accepted divergences

- The WARNING in `xml_out_internal` is emitted through `ereport_msg`, whose seam
  has no SQLSTATE slot, so `ERRCODE_DATA_CORRUPTED` is dropped at the seam (the
  message + detail are preserved). Acceptable until elog gains a sqlstate-bearing
  emit seam.
- Bare-word `PGFunction` registry (the `PG_FUNCTION_INFO_V1` SQL entry points)
  deferred, per task scope.
- `mod tests;` from the reference (old central-types model) intentionally not
  carried.
