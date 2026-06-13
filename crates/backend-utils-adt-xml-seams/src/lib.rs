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
