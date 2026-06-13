//! Seam declarations for the `backend-utils-adt-json` unit
//! (`utils/adt/json.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `escape_json(StringInfo buf, const char *str)` — append the JSON string
    /// literal for `str` (surrounding double quotes plus the JSON escaping of
    /// each character) to `buf`. `Err` is the append's out-of-memory
    /// `ereport(ERROR)`.
    pub fn escape_json<'mcx>(
        buf: &mut mcx::PgString<'mcx>,
        str: &str,
    ) -> types_error::PgResult<()>
);
