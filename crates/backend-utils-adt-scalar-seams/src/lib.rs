//! Seam declarations for the `backend-utils-adt-scalar` unit
//! (`utils/adt/datum.c`, `utils/adt/bool.c`, et al.).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `datumCopy(value, typByVal, typLen)` (`utils/adt/datum.c`): make a copy
    /// of a non-NULL datum. By-value datums are returned verbatim; by-reference
    /// datums are deep-copied into `mcx` (C: `palloc` in the current context).
    /// The byte-model `value`/result carry the verbatim datum bytes. `Err`
    /// carries OOM and the expanded-object flatten `ereport(ERROR)` surface.
    pub fn datum_copy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
        typbyval: bool,
        typlen: i16,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>
);

seam_core::seam!(
    /// `parse_bool(value, *result)` (`utils/adt/bool.c`) — parse a boolean GUC
    /// string ("true"/"false"/"on"/"off"/"yes"/"no"/"1"/"0", case-insensitive,
    /// unambiguous prefixes accepted). `Some(b)` on success (C returns `true`
    /// with `*result` set), `None` when the value is not a valid boolean (C
    /// returns `false`). Infallible at the ereport level.
    pub fn parse_bool(value: &str) -> Option<bool>
);

seam_core::seam!(
    /// `enum_out(enumval Oid)` (`utils/adt/enum.c`): render an enum value's OID
    /// to its label text. C looks the label up via `SearchSysCache1(ENUMOID)`
    /// and `ereport(ERROR)`s with `ERRCODE_INVALID_PARAMETER_VALUE` for an
    /// invalid enum value, so the seam is fallible. The label is a fresh
    /// `pstrdup` in the caller's context (a `cstring`).
    pub fn enum_out<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        enumval: types_core::primitive::Oid,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);
