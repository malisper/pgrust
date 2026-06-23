//! Seam declarations for the `backend-access-common-relation` unit
//! (`access/common/relation.c`): the generic relation open/close routines
//! shared by tables and indexes.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. An open relation crosses as a
//! [`rel::Relation`] handle: the owner copies the consumed slice of
//! the relcache entry into `mcx` and arms the handle with its own close
//! function, so release goes through the handle (`close(lockmode)` is the C
//! `relation_close`; `Drop` is the abort path) — there is no bare-OID close
//! seam.

seam_core::seam!(
    /// `relation_open(relationId, lockmode)` (relation.c): lock and open a
    /// relation by OID. `Err` carries the C `ereport(ERROR)`s: lock
    /// acquisition failure, `could not open relation with OID %u`, or the
    /// `cannot access temporary tables of other sessions` check.
    pub fn relation_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<rel::Relation<'mcx>>
);

seam_core::seam!(
    /// `try_relation_open(relationId, lockmode)` (relation.c): same as
    /// `relation_open`, except `Ok(None)` (the C NULL) instead of failing if
    /// the relation does not exist.
    pub fn try_relation_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<Option<rel::Relation<'mcx>>>
);

seam_core::seam!(
    /// `relation_openrv(relation, lockmode)` (relation.c): open a relation
    /// specified by a `RangeVar`. `Err` carries name-lookup failure
    /// (`relation "%s" does not exist`) and the `relation_open` errors.
    pub fn relation_openrv<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &types_tuple::access::RangeVar,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<rel::Relation<'mcx>>
);

seam_core::seam!(
    /// `relation_openrv_extended(relation, lockmode, missing_ok)`
    /// (relation.c): as `relation_openrv`, but with `missing_ok` true a
    /// missing relation yields `Ok(None)` instead of an error.
    pub fn relation_openrv_extended<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &types_tuple::access::RangeVar,
        lockmode: types_storage::lock::LOCKMODE,
        missing_ok: bool,
    ) -> types_error::PgResult<Option<rel::Relation<'mcx>>>
);
