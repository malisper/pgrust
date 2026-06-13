//! Seam declarations for the `backend-access-index-amapi` unit
//! (`access/index/amapi.c`), expressed as caller-shaped projections of the
//! `IndexAmRoutine` the handler returns.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetIndexAmRoutineByAmId(amoid, false)` (amapi.c): look up the index
    /// AM's handler, call it, and project the scalar `IndexAmRoutine` fields
    /// opclasscmds.c reads (the installer owns and frees the routine). `Err`
    /// carries the C `cache lookup failed` / `does not have a handler`
    /// validation.
    pub fn get_index_am_info(
        amoid: types_core::Oid,
    ) -> types_error::PgResult<types_opclass::IndexAmInfo>
);

seam_core::seam!(
    /// `amroutine->amadjustmembers(opfamilyoid, opclassoid, operators,
    /// procedures)` (the AM's member-adjustment callback): set dependency
    /// strength and optionally validate. The C callback mutates the lists in
    /// place; here it returns the (possibly mutated) `(operators,
    /// procedures)`, reallocated in `mcx`. `opclassoid` is `InvalidOid` for
    /// the ALTER OPERATOR FAMILY case (no specific opclass). `Err` carries the
    /// AM's validation `ereport(ERROR)`s.
    pub fn am_adjust_members<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        amoid: types_core::Oid,
        opfamilyoid: types_core::Oid,
        opclassoid: types_core::Oid,
        operators: mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
        procedures: mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
    ) -> types_error::PgResult<(
        mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
        mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
    )>
);

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

seam_core::seam!(
    /// `IndexAmTranslateCompareType(cmptype, amoid, opfamily, missing_ok)`
    /// (amapi.c): translate an AM-independent `CompareType` (its `i32` value)
    /// into the AM-specific `StrategyNumber` (returned as `i16`; `0` when there
    /// is no mapping). Used by lsyscache.c's `get_opfamily_member_for_cmptype`.
    /// The owner reaches the AM's `amtranslatecmptype`. Errors carry the C
    /// lookup/validation `ereport`s when `missing_ok = false`.
    pub fn index_am_translate_cmptype(
        cmptype: i32,
        amoid: types_core::Oid,
        opfamily: types_core::Oid,
        missing_ok: bool,
    ) -> types_error::PgResult<i16>
);

seam_core::seam!(
    /// `GetIndexAmRoutineByAmId(amoid, false)->amconsistentequality`
    /// (lsyscache.c `equality_ops_are_compatible`): whether the index access
    /// method guarantees consistent equality semantics across its opclasses.
    /// The owning amapi unit loads the AM handler and reads the flag (the C
    /// `pfree(amroutine)` is folded into the seam). `Err` carries the
    /// handler-load `ereport`s.
    pub fn index_am_consistent_equality(amoid: types_core::Oid) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `GetIndexAmRoutineByAmId(amoid, false)->amconsistentordering`
    /// (lsyscache.c `comparison_ops_are_compatible`): whether the index access
    /// method guarantees consistent ordering semantics across its opclasses.
    /// `Err` carries the handler-load `ereport`s.
    pub fn index_am_consistent_ordering(amoid: types_core::Oid) -> types_error::PgResult<bool>
);
