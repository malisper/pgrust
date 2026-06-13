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
    /// `lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO)->eq_opr_finfo.fn_oid`
    /// (typcache.c): resolve the OID of `element_type`'s default equality
    /// operator's underlying function (the cached `eq_opr_finfo`), as
    /// `array_eq` / `arrayoverlap` / `array_contain_compare` use it. Returns
    /// `InvalidOid` (0) when the type has no equality operator (the C then
    /// `ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION)`; the caller does that
    /// check). `Err` carries the typcache lookup surface
    /// (`ERRCODE_UNDEFINED_OBJECT`, "type ... does not exist").
    pub fn lookup_element_eq_opr(
        element_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO)->cmp_proc_finfo.fn_oid`
    /// (typcache.c): resolve the OID of `element_type`'s btree comparison
    /// support function (the cached `cmp_proc_finfo`), as `array_cmp` /
    /// `btarraycmp` use it. Returns `InvalidOid` (0) when the type has no
    /// comparison function (the C then `ereport(ERROR,
    /// ERRCODE_UNDEFINED_FUNCTION)`; the caller does that check). `Err` carries
    /// the typcache lookup surface.
    pub fn lookup_element_cmp_proc(
        element_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `lookup_type_cache(element_type, TYPECACHE_HASH_PROC_FINFO)->hash_proc_finfo.fn_oid`
    /// (typcache.c): resolve the OID of `element_type`'s hash support function
    /// (the cached `hash_proc_finfo`), as `hash_array` uses it. Returns
    /// `InvalidOid` (0) when the type has no hash function; `hash_array`'s
    /// `RECORDOID` special case substitutes `F_HASH_RECORD` itself. `Err`
    /// carries the typcache lookup surface.
    pub fn lookup_element_hash_proc(
        element_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `lookup_type_cache(element_type, TYPECACHE_HASH_EXTENDED_PROC_FINFO)->hash_extended_proc_finfo.fn_oid`
    /// (typcache.c): resolve the OID of `element_type`'s extended (64-bit,
    /// seeded) hash support function (the cached `hash_extended_proc_finfo`),
    /// as `hash_array_extended` uses it. Returns `InvalidOid` (0) when the type
    /// has no extended hash function (the C then `ereport(ERROR,
    /// ERRCODE_UNDEFINED_FUNCTION)`; the caller does that check). `Err` carries
    /// the typcache lookup surface.
    pub fn lookup_element_hash_extended_proc(
        element_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `AtEOXact_TypeCache()`.
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    /// `AtEOSubXact_TypeCache()`.
    pub fn at_eosubxact_type_cache()
);
