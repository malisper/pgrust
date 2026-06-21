//! Seam declarations for the `backend-utils-adt-xml` unit
//! (`utils/adt/xml.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `char *escape_xml(const char *str)` — return a freshly allocated,
    /// XML-escaped copy of `str` (`&` → `&amp;`, `<` → `&lt;`, `>` → `&gt;`,
    /// CR → `&#x0d;`). The C `palloc`s in the current context; the seam takes
    /// the target context and the result carries `'mcx`. `Err` is the
    /// allocation's out-of-memory `ereport(ERROR)`.
    pub fn escape_xml<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        str: &str,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `char *map_xml_name_to_sql_identifier(const char *name)` (xml.c:2435) —
    /// SQL/XML:2008 section 9.3: decode `_xHHHH_` escapes in an XML Name back to
    /// the original SQL identifier text. Used by `ruleutils.c`'s `T_XmlExpr`
    /// deparser to render `XMLELEMENT`/`XMLFOREST`/`XMLATTRIBUTES` names. C
    /// `palloc`s the result in the current context; the seam takes the target
    /// context and the result carries `'mcx`. `Err` carries the
    /// `unicode_to_server` / `pg_mblen` conversion errors and OOM.
    pub fn map_xml_name_to_sql_identifier<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        name: &str,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);
