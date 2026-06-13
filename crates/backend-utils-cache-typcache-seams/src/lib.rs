//! Seam declarations for the `backend-utils-cache-typcache` unit
//! (`utils/cache/typcache.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `lookup_type_cache(type_id, flags)` (typcache.c): fetch (creating if
    /// necessary) the `TypeCacheEntry` for `type_id`. `flags` selects which
    /// optional fields to compute (`TYPECACHE_*`); callers needing only the
    /// `pg_type` storage fields pass `0`. The returned entry is copied out (C
    /// returns a long-lived cache pointer; the safe port hands back the trimmed
    /// row by value). `Err` carries `ereport(ERROR, ERRCODE_UNDEFINED_OBJECT,
    /// "type ... does not exist")` and the catalog-lookup surface.
    pub fn lookup_type_cache(
        type_id: types_core::primitive::Oid,
        flags: i32,
    ) -> types_error::PgResult<types_typcache::TypeCacheEntry>
);

seam_core::seam!(
    /// `lookup_type_cache(type_id, TYPECACHE_HASH_PROC_FINFO |
    /// TYPECACHE_HASH_EXTENDED_PROC_FINFO)` then read `hash_proc_finfo.fn_oid` /
    /// `hash_extended_proc_finfo.fn_oid` (the `hash_range` / `hash_range_extended`
    /// element-type fallback re-lookup, rangetypes.c:1419 / :1482): resolve the
    /// element type's (extended, when `extended`) hash support function and
    /// return its OID. `Err` carries the C `ereport(ERROR,
    /// ERRCODE_UNDEFINED_FUNCTION, "could not identify a hash function for type
    /// %s")` raised when no hash function exists.
    pub fn lookup_range_elem_hash_proc(
        elem_type_id: types_core::primitive::Oid,
        extended: bool,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

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

seam_core::seam!(
    /// `AtEOXact_TypeCache()`.
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    /// `AtEOSubXact_TypeCache()`.
    pub fn at_eosubxact_type_cache()
);
