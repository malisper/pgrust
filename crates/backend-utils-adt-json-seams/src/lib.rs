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

seam_core::seam!(
    /// `escape_json_with_len(StringInfo buf, const char *str, int len)`
    /// (json.c:1630) — append the JSON string literal for the `len`-byte buffer
    /// `str` to `buf`. Used by `jsonb`'s string rendering. `Err` is the append's
    /// out-of-memory `ereport(ERROR)`.
    pub fn escape_json_with_len<'mcx>(
        buf: &mut mcx::PgString<'mcx>,
        str: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `JsonEncodeDateTime(buf, value, typid, tzp)` (json.c:309) — encode a
    /// date/time `Datum` into ISO format (forcing XSD date style), returning the
    /// formatted string. `tzp`, if `Some`, is the time-zone offset in seconds
    /// for `timestamptz`. The cycle partner `jsonb` calls this when rendering a
    /// datetime value. `Err` carries the datetime `ereport(ERROR, "... out of
    /// range")`.
    pub fn json_encode_datetime(
        value: types_datum::Datum,
        typid: types_core::Oid,
        tzp: Option<i32>,
    ) -> types_error::PgResult<String>
);
