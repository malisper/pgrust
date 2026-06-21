//! Inward seam declarations for `backend-utils-adt-ri-triggers`
//! (`utils/adt/ri_triggers.c`): the syscache invalidation callback the
//! catalog-cache owner fires on `pg_constraint` changes.
//!
//! The owning unit (`backend-utils-adt-ri-triggers`) installs this from its
//! `init_seams()`; the syscache owner (which registers the callback) invokes it
//! through this seam to avoid a dependency cycle. Until RI is installed, a call
//! panics loudly.

seam_core::seam!(
    /// `InvalidateConstraintCacheCallBack(arg, cacheid, hashvalue)`
    /// (ri_triggers.c): invalidate any `ri_constraint_cache` entry whose
    /// syscache hash value matches `hashvalue` (or all entries if
    /// `hashvalue == 0`). Infallible (mutates only process-local state).
    pub fn invalidate_constraint_cache_callback(hashvalue: u32)
);

seam_core::seam!(
    /// `RI_Initial_Check(trigger, fk_rel, pk_rel)` (ri_triggers.c) — try to
    /// validate a (newly-added or being-enforced) FK against all existing rows
    /// with a single set-based `SELECT ... FROM fk LEFT JOIN pk ... WHERE pk IS
    /// NULL AND fk IS NOT NULL` SPI query. Returns `true` if the check ran (and
    /// raised on any orphan row); `false` if it could not run (insufficient
    /// SELECT permission / RLS), in which case the caller falls back to firing
    /// `RI_FKey_check_ins` per row. The `trigger` handle resolves off the
    /// current-trigger side-channel (its `tgconstraint` identifies the FK).
    /// **Installed by `backend-utils-adt-ri-triggers`.**
    pub fn ri_initial_check<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: types_ri_triggers::TriggerRef,
        fk_rel: &types_rel::Relation<'mcx>,
        pk_rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `RI_FKey_check_ins(fcinfo)` (ri_triggers.c) — the FK check-on-INSERT
    /// trigger, fired per row by the phase-3 validation scan as if the row had
    /// just been inserted. Raises the standard FK-violation error if the row's
    /// key has no matching PK row. The `trigdata` handle resolves off the
    /// current-trigger side-channel (its `tg_trigslot` is the scanned row).
    /// **Installed by `backend-utils-adt-ri-triggers`.**
    pub fn ri_fkey_check_ins<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigdata: types_ri_triggers::TriggerDataRef,
    ) -> types_error::PgResult<()>
);
