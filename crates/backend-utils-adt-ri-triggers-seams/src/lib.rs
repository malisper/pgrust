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

seam_core::seam!(
    /// `RI_FKey_trigger_type(tgfoid)` (ri_triggers.c) — classify a trigger
    /// function OID as an RI PK-side trigger (`RI_TRIGGER_PK`), an RI FK-side
    /// trigger (`RI_TRIGGER_FK`), or a non-RI trigger (`RI_TRIGGER_NONE`).
    /// Pure (no allocation, no error path). Used by `AfterTriggerSaveEvent` to
    /// decide whether the FK-enforcement skip applies to a candidate trigger.
    /// **Installed by `backend-utils-adt-ri-triggers`.**
    pub fn ri_fkey_trigger_type(tgfoid: types_core::Oid) -> i32
);

seam_core::seam!(
    /// `RI_FKey_pk_upd_check_required(trigger, pk_rel, oldslot, newslot)`
    /// (ri_triggers.c) — decide whether the PK-side FK-enforcement AFTER event
    /// must be queued for an UPDATE/DELETE on the referenced (PK) table. Returns
    /// `false` when the change cannot possibly orphan a referencing row (key
    /// columns went NULL, or old==new key). The `trigger`/`pk_rel` handles
    /// resolve off the current-trigger side-channel the caller installs;
    /// `newslot == None` for a DELETE. **Installed by
    /// `backend-utils-adt-ri-triggers`.**
    pub fn ri_fkey_pk_upd_check_required<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: types_ri_triggers::TriggerRef,
        pk_rel: types_ri_triggers::TriggerDataRef,
        oldslot: types_ri_triggers::TupleTableSlotRef,
        newslot: Option<types_ri_triggers::TupleTableSlotRef>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `RI_FKey_fk_upd_check_required(trigger, fk_rel, oldslot, newslot)`
    /// (ri_triggers.c) — decide whether the FK-side check AFTER event must be
    /// queued for an UPDATE on the referencing (FK) table. Returns `false` when
    /// the new key is NULL (SIMPLE/FULL handling), or the old==new key and the
    /// old row was not inserted by the current transaction. The
    /// `trigger`/`fk_rel` handles resolve off the current-trigger side-channel
    /// the caller installs. **Installed by `backend-utils-adt-ri-triggers`.**
    pub fn ri_fkey_fk_upd_check_required<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: types_ri_triggers::TriggerRef,
        fk_rel: types_ri_triggers::TriggerDataRef,
        oldslot: types_ri_triggers::TupleTableSlotRef,
        newslot: types_ri_triggers::TupleTableSlotRef,
    ) -> types_error::PgResult<bool>
);
