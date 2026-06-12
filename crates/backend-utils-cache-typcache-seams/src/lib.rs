//! Seam declarations for the `backend-utils-cache-typcache` unit
//! (`utils/cache/typcache.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `lookup_rowtype_tupdesc(type_id, typmod)` (typcache.c): the tuple
    /// descriptor of a composite rowtype, cloned out of the typcache into
    /// `mcx` (the C returns a refcounted pointer into the cache; the safe
    /// port copies, so the C `ReleaseTupleDesc` pairing becomes drop). `Err`
    /// carries the C `ereport(ERROR)`s (type is not composite / record type
    /// not registered) and OOM from the copy.
    pub fn lookup_rowtype_tupdesc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_id: types_core::primitive::Oid,
        typmod: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>
);
