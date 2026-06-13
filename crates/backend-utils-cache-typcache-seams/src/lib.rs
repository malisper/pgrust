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
    /// `lookup_type_cache(type_id, TYPECACHE_HASH[_EXTENDED]_PROC_FINFO)` then
    /// read `->hash_proc_finfo.fn_oid` (or `->hash_extended_proc_finfo.fn_oid`
    /// when `extended`) (typcache.c): the OID of `type_id`'s standard or
    /// extended hash support function, used by `hash_multirange` /
    /// `hash_multirange_extended` when the range element's cached entry has not
    /// yet computed it. Returns `InvalidOid` (`0`) when the type has no such
    /// hash function. `Err` carries the catalog-lookup surface.
    pub fn lookup_type_hash_proc(
        type_id: types_core::primitive::Oid,
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
