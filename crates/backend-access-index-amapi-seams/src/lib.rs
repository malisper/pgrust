//! Seam declarations for the `backend-access-index-amapi` unit
//! (`access/index/amapi.c`), expressed as caller-shaped projections of the
//! `IndexAmRoutine` the handler returns.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetIndexAmRoutineByAmId(amoid, noerror = false)->amcanbackward`
    /// (amapi.c): look up the index AM's handler in the syscache, call it, and
    /// project `amcanbackward` out of the returned `IndexAmRoutine` (the
    /// installer owns the routine's allocation and `pfree`). Errors with the
    /// C `noerror = false` lookups/validation (`cache lookup failed for access
    /// method %u`, `index access method "%s" does not have a handler`, ...).
    pub fn index_am_canbackward(amoid: types_core::Oid) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `amoptions(reloptions, validate)` — the index AM's options parser
    /// (`IndexAmRoutine.amoptions`), invoked by `index_reloptions`
    /// (reloptions.c). `amoptions` is dispatched by the AM option-parser
    /// function's OID. `reloptions` is the verbatim `text[]` catalog bytes;
    /// the result is the AM-defined option `bytea` payload (the AM owns its
    /// layout), allocated in `mcx`. `Err` carries the AM's option-validation
    /// `ereport(ERROR)`s; `None` mirrors the AM returning a NULL bytea.
    pub fn am_reloptions<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        amoptions: types_core::Oid,
        reloptions: &[u8],
        validate: bool,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>
);
