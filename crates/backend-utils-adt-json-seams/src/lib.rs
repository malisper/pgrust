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
    /// `char *JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int
    /// *tzp)` (json.c) — render a date/time `value` of type `typid` to its
    /// canonical JSON text. Reached from `jsonb_util.c`'s `convertJsonbScalar`
    /// `jbvDatetime` arm, which always passes a non-NULL `&tz`, so `tzp`
    /// marshals to `Some(tz)`. The C writes into a caller buffer and returns
    /// it; here the rendered text is returned as an allocated `String`. `Err`
    /// carries the encode `ereport(ERROR)`.
    pub fn json_encode_datetime(
        value: types_datum::Datum,
        typid: types_core::Oid,
        tzp: Option<i32>,
    ) -> types_error::PgResult<String>
);
