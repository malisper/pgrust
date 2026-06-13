//! Seam declarations for the `backend-utils-adt-scalar` unit's `datum.c`
//! (`utils/adt/datum.c`).
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
        value: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
        typbyval: bool,
        typlen: i16,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::TupleValue<'mcx>>
);
