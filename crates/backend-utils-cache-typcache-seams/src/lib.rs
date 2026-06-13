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
    pub fn domain_check_input(
        value: types_datum::Datum,
        isnull: bool,
        domain_type: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_TypeCache()`.
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    /// `AtEOSubXact_TypeCache()`.
    pub fn at_eosubxact_type_cache()
);
