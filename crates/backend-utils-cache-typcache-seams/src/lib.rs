//! Seam declarations for the `backend-utils-cache-typcache` unit
//! (`utils/cache/typcache.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

// ---------------------------------------------------------------------------
// Extensions for the `backend-utils-adt-array-typanalyze` unit
// (`utils/adt/array_typanalyze.c`).
//
// `array_typanalyze` calls `lookup_type_cache(element_typeid, TYPECACHE_EQ_OPR
// | TYPECACHE_CMP_PROC_FINFO | TYPECACHE_HASH_PROC_FINFO)` and reads
// `eq_opr`, `cmp_proc_finfo.fn_oid`, `hash_proc_finfo.fn_oid`, plus the element
// type's `typbyval`/`typlen`/`typalign` — fields the trimmed
// `lookup_type_cache` projection below does not carry. `compute_array_stats`
// then invokes the element type's hash / compare support functions by OID
// through `FunctionCall1Coll` / `FunctionCall2Coll`. All three cross into the
// typcache owner. The owning unit installs them from its `init_seams()` when it
// lands; until then a call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `lookup_type_cache(element_typeid, TYPECACHE_EQ_OPR |
    /// TYPECACHE_CMP_PROC_FINFO | TYPECACHE_HASH_PROC_FINFO)` then project the
    /// element-type metadata `array_typanalyze` needs
    /// (array_typanalyze.c:124-143): returns the populated
    /// [`types_statistics::ArrayAnalyzeExtraData`] (with `coll_id` set to
    /// `collid`), or `None` when one of the required equality / compare / hash
    /// operators is missing (the C `OidIsValid` guard that takes the
    /// standard-stats-only `PG_RETURN_BOOL(true)` path). `cmp` / `hash` carry
    /// the support functions' proc OIDs. `Err` carries the catalog-lookup
    /// `ereport(ERROR)` surface.
    pub fn array_typanalyze_element_typcache(
        element_typeid: types_core::primitive::Oid,
        collid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_statistics::ArrayAnalyzeExtraData>>
);

seam_core::seam!(
    /// `DatumGetUInt32(FunctionCall1Coll(hash, coll, value))`
    /// (array_typanalyze.c:715, `element_hash`): invoke the element type's hash
    /// support function (resolved from `hash_proc` by a fresh `FmgrInfo` in the
    /// owner) with collation `coll` and return the 32-bit hash of `value`. `Err`
    /// carries the support function's `ereport(ERROR)` surface.
    pub fn array_element_hash<'mcx>(
        hash_proc: types_core::primitive::Oid,
        coll: types_core::primitive::Oid,
        value: types_tuple::Datum<'mcx>,
    ) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// `DatumGetInt32(FunctionCall2Coll(cmp, coll, a, b))`
    /// (array_typanalyze.c:746, `element_compare`): invoke the element type's
    /// btree compare support function (resolved from `cmp_proc` by a fresh
    /// `FmgrInfo` in the owner) with collation `coll`, returning the sign of
    /// `a <=> b`. `Err` carries the support function's `ereport(ERROR)` surface.
    pub fn array_element_compare<'mcx>(
        cmp_proc: types_core::primitive::Oid,
        coll: types_core::primitive::Oid,
        a: types_tuple::Datum<'mcx>,
        b: types_tuple::Datum<'mcx>,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `compare_values_of_enum(tcache, arg1, arg2)` (typcache.c): the
    /// enum-value comparison engine `enum_cmp_internal` (utils/adt/enum.c)
    /// defers the odd-OID case to. `type_id` is the enum type OID (C resolves
    /// the `TypeCacheEntry *` from it; the safe port keys the cache by OID).
    /// Returns negative / zero / positive for `arg1` `<` / `=` / `>` `arg2` in
    /// the enum's declared sort order. `Err` carries the cache-load /
    /// catalog-scan `ereport(ERROR)` surface.
    pub fn compare_values_of_enum(
        type_id: types_core::primitive::Oid,
        arg1: types_core::primitive::Oid,
        arg2: types_core::primitive::Oid,
    ) -> types_error::PgResult<i32>
);

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
    /// `lookup_type_cache(type_id, flags)` (typcache.c), range/multirange-ADT
    /// view: same as [`lookup_type_cache`] but hands back the
    /// `types_cache::TypeCacheEntry` shape the range/multirange ports use
    /// (with the `hash_proc_finfo` / `hash_extended_proc_finfo` support
    /// fields). `hash_multirange` calls this to resolve the subtype's hash
    /// support function when it was not already cached. `Err` carries the
    /// catalog-lookup `ereport(ERROR)` surface.
    pub fn lookup_type_cache_entry(
        type_id: types_core::primitive::Oid,
        flags: i32,
    ) -> types_error::PgResult<types_cache::TypeCacheEntry>
);

seam_core::seam!(
    /// `lookup_type_cache(type_id, TYPECACHE_HASH_PROC_FINFO |
    /// TYPECACHE_HASH_EXTENDED_PROC_FINFO)` then read `hash_proc_finfo.fn_oid` /
    /// `hash_extended_proc_finfo.fn_oid` (the `hash_range` / `hash_range_extended`
    /// element-type fallback re-lookup, rangetypes.c:1419 / :1482; also the
    /// `hash_multirange` / `hash_multirange_extended` subtype fallback): resolve
    /// the element type's (extended, when `extended`) hash support function and
    /// return its OID. `Err` carries the C `ereport(ERROR,
    /// ERRCODE_UNDEFINED_FUNCTION, "could not identify a hash function for type
    /// %s")` raised when no hash function exists.
    pub fn lookup_range_elem_hash_proc(
        elem_type_id: types_core::primitive::Oid,
        extended: bool,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

/// The subset of `lookup_type_cache(type_id, TYPECACHE_TUPDESC |
/// TYPECACHE_DOMAIN_BASE_INFO)` that `expandedrecord.c`'s builders read out of
/// the returned `TypeCacheEntry`: the `typtype`, the `domainBaseType` (the
/// resolved base of a domain over composite), the resolved composite tuple
/// descriptor (`tupDesc`, `None` if the type is not composite — the caller then
/// raises "type is not composite"), and the `tupDesc_identifier`. C reads these
/// straight off the long-lived cache entry; the safe port copies the descriptor
/// into `mcx` and hands back the scalar fields by value.
pub struct ExpandedRecordTypeCacheView<'mcx> {
    /// `typentry->typtype`.
    pub typtype: i8,
    /// `typentry->domainBaseType`.
    pub domain_base_type: types_core::primitive::Oid,
    /// `typentry->tupDesc`, cloned into `mcx`; `None` when the type is not
    /// composite (the caller raises ERRCODE_WRONG_OBJECT_TYPE).
    pub tup_desc: Option<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    /// `typentry->tupDesc_identifier`.
    pub tup_desc_identifier: u64,
    /// Whether the cache's `tupDesc->tdrefcount >= 0` (the C path acquires its
    /// own refcount via a memory-context callback). The safe port holds its own
    /// deep copy; this records the flag the builder mirrors into
    /// `er_tupdesc_refcounted`.
    pub tup_desc_refcounted: bool,
}

seam_core::seam!(
    /// `lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO)`
    /// then, if the result is a domain, the chained
    /// `lookup_type_cache(domainBaseType, TYPECACHE_TUPDESC)`
    /// (expandedrecord.c:84-101) — the composite/domain-over-composite resolution
    /// `make_expanded_record_from_typeid` performs. Returns the
    /// [`ExpandedRecordTypeCacheView`] (typtype of the *original* type, the
    /// domain base OID, the resolved composite `tupDesc` cloned into `mcx`, and
    /// its `tupDesc_identifier`). `Err` carries the typcache lookup surface;
    /// `tup_desc == None` signals the not-composite case the caller turns into
    /// `ERRCODE_WRONG_OBJECT_TYPE`.
    pub fn lookup_type_cache_expanded_record<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<ExpandedRecordTypeCacheView<'mcx>>
);

seam_core::seam!(
    /// `lookup_type_cache(type_id, TYPECACHE_TUPDESC)` reading `tupDesc` and
    /// `tupDesc_identifier` (expandedrecord.c:226-236) — the named-composite
    /// path of `make_expanded_record_from_tupdesc`. Returns the resolved
    /// composite `tupDesc` cloned into `mcx` (`None` when not composite) plus
    /// its identifier. `Err` carries the typcache lookup surface.
    pub fn lookup_type_cache_tupdesc_view<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<ExpandedRecordTypeCacheView<'mcx>>
);

seam_core::seam!(
    /// `assign_record_type_identifier(type_id, typmod)` (typcache.c): return a
    /// unique identifier for the (possibly anonymous) RECORD type/typmod pair,
    /// assigning a fresh one if necessary. `Err` carries the cache-insert /
    /// allocation surface.
    pub fn assign_record_type_identifier(
        type_id: types_core::primitive::Oid,
        typmod: i32,
    ) -> types_error::PgResult<u64>
);

seam_core::seam!(
    /// `assign_record_type_typmod(tupDesc)` (typcache.c): for an anonymous
    /// RECORD `TupleDesc`, find or create the matching entry in the record-type
    /// cache and stamp its assigned `tdtypmod` (and `tdtypeid = RECORDOID`) back
    /// into the descriptor in place, so it can be used to build composite
    /// rowtype Datums (`BlessTupleDesc`). `Err` carries the cache-insert /
    /// allocation surface.
    pub fn assign_record_type_typmod(
        tup_desc: &mut types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<()>
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

/// The base-type I/O info `domain_state_setup` (utils/adt/domains.c) pulls out
/// of the typcache for a domain type: the result of
/// `lookup_type_cache(domainType, TYPECACHE_DOMAIN_BASE_INFO)` (which also
/// validates that the OID really is a domain) combined with the base type's
/// input/receive function lookup (`getTypeInputInfo` / `getTypeBinaryInputInfo`).
#[derive(Clone, Copy, Debug)]
pub struct DomainBaseInputInfo {
    /// `typiofunc` -- OID of the base type's `typinput` (text) or `typreceive`
    /// (binary) function, to be dispatched by [`super`]-side fmgr seams.
    pub typiofunc: types_core::primitive::Oid,
    /// `typioparam` -- the I/O parameter OID passed to that function.
    pub typioparam: types_core::primitive::Oid,
    /// `typtypmod` -- the domain's `domainBaseTypmod`.
    pub typtypmod: i32,
}

seam_core::seam!(
    /// `domain_state_setup`'s typcache half (utils/adt/domains.c): run
    /// `lookup_type_cache(domainType, TYPECACHE_DOMAIN_BASE_INFO)` and look up
    /// the base type's I/O function. `binary` selects
    /// `getTypeBinaryInputInfo` over `getTypeInputInfo`. `Err` carries the
    /// `ereport(ERROR, "type %s is not a domain")` (ERRCODE_DATATYPE_MISMATCH)
    /// for a non-domain OID plus the bad-OID cache lookup error.
    pub fn domain_get_base_input_info(
        domain_type: types_core::primitive::Oid,
        binary: bool,
    ) -> types_error::PgResult<DomainBaseInputInfo>
);

seam_core::seam!(
    /// `domain_check_input` (utils/adt/domains.c): validate `value`/`isnull`
    /// against every cached constraint of `domain_type`. Drives the typcache
    /// `DomainConstraintRef` (`InitDomainConstraintRef` /
    /// `UpdateDomainConstraintRef`), evaluating each `DOM_CONSTRAINT_CHECK`
    /// with `ExecCheck` in a standalone `ExprContext` and rejecting nulls for
    /// `DOM_CONSTRAINT_NOTNULL`. Hard-error variant only (escontext == NULL):
    /// `Err` carries the NOT NULL violation (ERRCODE_NOT_NULL_VIOLATION,
    /// "domain %s does not allow null values"), the CHECK violation
    /// (ERRCODE_CHECK_VIOLATION, "value for domain %s violates check
    /// constraint \"%s\"") with the schema/datatype/constraint diagnostic
    /// fields attached, and anything the CHECK expression itself raises.
    pub fn domain_check_input<'mcx>(
        value: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        isnull: bool,
        domain_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// rowtypes.c `record_cmp` per-column step: `lookup_type_cache(coltype,
    /// TYPECACHE_CMP_PROC_FINFO)` then `FunctionCallInvoke` of the type's
    /// three-way `cmp` support function on the pair of column values, returning
    /// the sign of `v1 <=> v2`. Encapsulates the typcache lookup, the
    /// `OidIsValid(cmp_proc_finfo.fn_oid)` validity check that raises
    /// `errcode(ERRCODE_UNDEFINED_FUNCTION)` ("could not identify a comparison
    /// function for type %s"), and the support function's own
    /// `ereport(ERROR)`s. `collation` is the column collation (or `InvalidOid`
    /// when the two records disagree). Both values are non-null (the caller
    /// handles the NULL-ordering rules).
    pub fn record_column_cmp(
        coltype: types_core::primitive::Oid,
        collation: types_core::primitive::Oid,
        v1: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
        v2: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// rowtypes.c `record_eq` per-column step: `lookup_type_cache(coltype,
    /// TYPECACHE_EQ_OPR_FINFO)` then `FunctionCallInvoke` of the type's
    /// equality operator on the pair of column values. Encapsulates the
    /// typcache lookup, the `OidIsValid(eq_opr_finfo.fn_oid)` validity check
    /// that raises `errcode(ERRCODE_UNDEFINED_FUNCTION)` ("could not identify
    /// an equality operator for type %s"), and the operator's own
    /// `ereport(ERROR)`s. Returns the equality result; C treats a null
    /// operator result as `false`, which the owner folds in. Both values are
    /// non-null.
    pub fn record_column_eq(
        coltype: types_core::primitive::Oid,
        collation: types_core::primitive::Oid,
        v1: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
        v2: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// rowtypes.c `hash_record` per-column step: `lookup_type_cache(coltype,
    /// TYPECACHE_HASH_PROC_FINFO)` then `FunctionCallInvoke` of the type's
    /// standard hash support function on the (non-null) column value, with the
    /// column collation. Encapsulates the typcache lookup, the
    /// `OidIsValid(hash_proc_finfo.fn_oid)` validity check that raises
    /// `errcode(ERRCODE_UNDEFINED_FUNCTION)` ("could not identify a hash
    /// function for type %s"), and the support function's own
    /// `ereport(ERROR)`s.
    pub fn record_column_hash(
        coltype: types_core::primitive::Oid,
        collation: types_core::primitive::Oid,
        v: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
    ) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// rowtypes.c `hash_record_extended` per-column step:
    /// `lookup_type_cache(coltype, TYPECACHE_HASH_EXTENDED_PROC_FINFO)` then
    /// `FunctionCallInvoke` of the type's extended hash support function on the
    /// (non-null) column value with the given `seed`, with the column
    /// collation. Encapsulates the typcache lookup, the
    /// `OidIsValid(hash_extended_proc_finfo.fn_oid)` validity check that raises
    /// `errcode(ERRCODE_UNDEFINED_FUNCTION)` ("could not identify an extended
    /// hash function for type %s"), and the support function's own
    /// `ereport(ERROR)`s.
    pub fn record_column_hash_extended(
        coltype: types_core::primitive::Oid,
        collation: types_core::primitive::Oid,
        v: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
        seed: u64,
    ) -> types_error::PgResult<u64>
);

seam_core::seam!(
    /// `lookup_rowtype_tupdesc_copy(type_id, typmod)` (typcache.c): like
    /// `lookup_rowtype_tupdesc`, but returns an independent
    /// (`CreateTupleDescCopyConstr`) copy with no refcount bookkeeping —
    /// `TypeGetTupleDesc` renames its attributes and re-stamps the rowtype, so
    /// it needs a freestanding descriptor. Cloned into `mcx`. `Err` carries the
    /// C `ereport(ERROR)`s (type is not composite / record type not registered)
    /// and OOM from the copy.
    pub fn lookup_rowtype_tupdesc_copy<'mcx>(
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
    /// `lookup_type_cache(type_id, TYPECACHE_EQ_OPR)->eq_opr` (typcache.c) — the
    /// equality OPERATOR oid of a type (not the function). `analyzeCTE`
    /// (parse_cte.c) calls this to resolve the cycle-mark column's `=` operator
    /// so it can then take its negator (the `<>` operator) for cycle detection.
    /// Returns `InvalidOid` (0) when the type has no equality operator (the
    /// caller then `ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION)`). The trimmed
    /// `TypeCacheEntry` returned by [`lookup_type_cache`] does not carry
    /// `eq_opr`, so this dedicated accessor reads it from the full cache row.
    /// `Err` carries the typcache lookup surface.
    pub fn lookup_type_cache_eq_opr(
        type_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `lookup_type_cache(type_id, TYPECACHE_LT_OPR)->lt_opr` (typcache.c) — the
    /// "less than" btree OPERATOR oid of a type. `CreateStatistics`
    /// (statscmds.c) calls this to reject columns/expressions whose type has no
    /// default btree operator class. Returns `InvalidOid` (0) when the type has
    /// no less-than operator. The trimmed `TypeCacheEntry` returned by
    /// [`lookup_type_cache`] does not carry `lt_opr`, so this dedicated accessor
    /// reads it from the full cache row. `Err` carries the typcache lookup
    /// surface.
    pub fn lookup_type_cache_lt_opr(
        type_id: types_core::primitive::Oid,
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

seam_core::seam!(
    /// The `get_sort_group_operators` (parse_oper.c) typcache leg: run
    /// `lookup_type_cache(argtype, TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR |
    /// TYPECACHE_GT_OPR [| TYPECACHE_HASH_PROC])` and return the resolved
    /// default sorting/grouping operators by value: `(lt_opr, eq_opr, gt_opr,
    /// is_hashable)`. `is_hashable` is `OidIsValid(hash_proc)`, only computed
    /// when `want_hashable` is true (the C non-NULL `isHashable` toggling
    /// `TYPECACHE_HASH_PROC`); otherwise `false`. Encapsulated on the owner
    /// because the trimmed [`types_typcache::TypeCacheEntry`] copy-out does not
    /// carry the operator/proc fields. `Err` carries the cache-lookup
    /// `ereport(ERROR)` surface (e.g. "type ... does not exist").
    pub fn sort_group_operators(
        argtype: types_core::primitive::Oid,
        want_hashable: bool,
    ) -> types_error::PgResult<(
        types_core::primitive::Oid,
        types_core::primitive::Oid,
        types_core::primitive::Oid,
        bool,
    )>
);

seam_core::seam!(
    /// `lookup_type_cache(atttypid, 0)->typtype` (typcache.c): the `typtype`
    /// classification byte (`TYPTYPE_RANGE` / `TYPTYPE_MULTIRANGE` / ...) of a
    /// type, loading the cache entry by OID. Used by `ExecWithoutOverlapsNotEmpty`
    /// (execIndexing.c) to dispatch a WITHOUT OVERLAPS key value to the range
    /// vs. multirange emptiness check. `Err` carries the cache-load surface.
    pub fn type_cache_typtype(atttypid: types_core::primitive::Oid) -> types_error::PgResult<i8>
);

seam_core::seam!(
    /// `DomainHasConstraints(type_id)` (typcache.c): does the domain type
    /// `type_id` have any CHECK / NOT NULL constraints? Loads the typcache entry
    /// (`TYPECACHE_DOMAIN_CONSTRAINT_INFO`) and reports whether the constraint
    /// list is non-empty. `ExecInitJsonCoercion` uses it to decide whether a
    /// JSON_EXISTS coercion to a domain over integer must run domain checks.
    /// `Err` carries the cache-load surface.
    pub fn domain_has_constraints(
        type_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `InitDomainConstraintRef(type_id, ref, ctx, need_exprstate=false)`
    /// (typcache.c) — the constraint list `ExecInitCoerceToDomain` bakes into the
    /// `ExprState`. Loads the typcache entry (`TYPECACHE_DOMAIN_CONSTRAINT_INFO`),
    /// returning each constraint's `constrainttype` / `name` / planned CHECK
    /// `check_expr` (the executor compiles `check_expr` itself via
    /// `ExecInitExprRec`, so `need_exprstate` is false and no executor
    /// `ExprState` is produced here). Parent-first ordering as the typcache
    /// emits it. `Err` carries the cache-load surface.
    pub fn domain_constraint_list(
        type_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<
        std::vec::Vec<types_cache::typcache::DomainConstraintState>,
    >
);
