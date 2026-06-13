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
    /// `IndexAmTranslateStrategy(strategy, amoid, opfamily, missing_ok)`
    /// (amapi.c): translate an AM-specific strategy number into the
    /// AM-independent `CompareType` (returned as its `i32` value, e.g.
    /// `COMPARE_EQ`). The owning unit reaches the AM's
    /// `amtranslatestrategy`. Errors carry the C lookup/validation `ereport`s
    /// when `missing_ok = false`.
    pub fn index_am_translate_strategy(
        strategy: i32,
        amoid: types_core::Oid,
        opfamily: types_core::Oid,
        missing_ok: bool,
    ) -> types_error::PgResult<i32>
);
