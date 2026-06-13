//! Seam declarations for the `backend-access-common-relation` unit
//! (`access/common/relation.c`): the generic relation open/close routines
//! shared by tables and indexes.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. The C `Relation` crosses as an owned
//! [`types_tuple::rel::RelationData`] carrier: open copies the consumed
//! relcache fields into the caller's `mcx`; close consumes the carrier (the
//! C pointer is dead after `relation_close`).

seam_core::seam!(
    /// `relation_open(relationId, lockmode)` (relation.c): lock and open a
    /// relation by OID. The returned carrier is allocated in `mcx`. `Err`
    /// carries the C `ereport(ERROR)`s: lock acquisition failure, `could not
    /// open relation with OID %u`, the `cannot access temporary tables of
    /// other sessions` check, or OOM copying the entry.
    pub fn relation_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_tuple::rel::RelationData<'mcx>>
);

seam_core::seam!(
    /// `try_relation_open(relationId, lockmode)` (relation.c): same as
    /// `relation_open`, except `Ok(None)` (the C NULL) instead of failing if
    /// the relation does not exist.
    pub fn try_relation_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<Option<types_tuple::rel::RelationData<'mcx>>>
);

seam_core::seam!(
    /// `relation_openrv(relation, lockmode)` (relation.c): open a relation
    /// specified by a `RangeVar`. `Err` carries name-lookup failure
    /// (`relation "%s" does not exist`) and the `relation_open` errors.
    pub fn relation_openrv<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &types_tuple::access::RangeVar,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_tuple::rel::RelationData<'mcx>>
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
    ) -> types_error::PgResult<Option<types_tuple::rel::RelationData<'mcx>>>
);

seam_core::seam!(
    /// `relation_close(relation, lockmode)` (relation.c): close the relation
    /// (decrement the relcache refcount) and, if `lockmode` is not `NoLock`,
    /// release the lock. Consumes the carrier. `LockRelease` can
    /// `elog(ERROR)`, carried on `Err`.
    pub fn relation_close(
        relation: types_tuple::rel::RelationData<'_>,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);
