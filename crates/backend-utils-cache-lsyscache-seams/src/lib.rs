//! Seam declarations for the `backend-utils-cache-lsyscache` unit
//! (`utils/cache/lsyscache.c` convenience catalog lookups).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_opfamily_name(opfid, missing_ok)` (lsyscache.c): the opfamily's
    /// name, copied out of the syscache into `mcx` (C: `pstrdup` in the
    /// current context). With `missing_ok = false` a missing opfamily raises
    /// (`Err`); with `missing_ok = true` it is `Ok(None)`. `Err` includes OOM
    /// from the copy.
    pub fn get_opfamily_name<'mcx>(
        mcx: Mcx<'mcx>,
        opfid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_opclass_input_type(opclass)` (lsyscache.c): the opclass's
    /// `opcintype`. A missing opclass is the C `elog(ERROR, "cache lookup
    /// failed for opclass %u")`, carried on `Err`.
    pub fn get_opclass_input_type(opclass: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_rel_name(relid)` (lsyscache.c): the relation's name, copied out
    /// of the syscache into `mcx` (C: `pstrdup`). A missing relation is
    /// `Ok(None)` (C: NULL). `Err` includes OOM from the copy.
    pub fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `getBaseType(typid)` (lsyscache.c): resolve a domain type to its base
    /// type (the identity for non-domains). A missing pg_type row along the
    /// domain chain is the C `elog(ERROR, "cache lookup failed for type
    /// %u")`, carried on `Err`.
    pub fn get_base_type(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_namespace_name(nspid)` (lsyscache.c): the namespace's name,
    /// copied out of the syscache into `mcx` (C: `pstrdup`). A missing
    /// namespace is `Ok(None)` (C: NULL). `Err` includes OOM from the copy.
    pub fn get_namespace_name<'mcx>(
        mcx: Mcx<'mcx>,
        nspid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_rel_relkind(relid)` (lsyscache.c): the relation's `relkind`, or 0
    /// when there is no such pg_class row (the C `'\0'` return). `Err`
    /// carries the syscache machinery's `ereport(ERROR)`s.
    pub fn get_rel_relkind(relid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_attname(relid, attnum, missing_ok)` (lsyscache.c): the
    /// attribute's name, copied out of the syscache into `mcx` (C: `pstrdup`
    /// in the current context). With `missing_ok = false` a missing attribute
    /// is the C `elog(ERROR, "cache lookup failed for attribute %d of
    /// relation %u")`, carried on `Err`; with `missing_ok = true` it is
    /// `Ok(None)`. `Err` includes OOM from the copy.
    pub fn get_attname<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: types_core::AttrNumber,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_attnum(relid, attname)` (lsyscache.c): the attribute's number, or
    /// `InvalidAttrNumber` (0) if no such attribute. `Err` carries the
    /// syscache machinery's `ereport(ERROR)`s.
    pub fn get_attnum(relid: Oid, attname: &str) -> PgResult<types_core::AttrNumber>
);

seam_core::seam!(
    /// `get_relname_relid(relname, relnamespace)` (lsyscache.c):
    /// `GetSysCacheOid2(RELNAMENSP, ...)` — the relation's OID or
    /// `InvalidOid`. `Err` carries catcache-path `ereport(ERROR)`s.
    pub fn get_relname_relid(relname: &str, relnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_am_name(amOid)` (lsyscache.c): the access method's name, copied
    /// out of the syscache into `mcx` (C: `pstrdup`). A missing AM is
    /// `Ok(None)` (C: NULL). `Err` includes OOM from the copy.
    pub fn get_am_name<'mcx>(mcx: Mcx<'mcx>, am_oid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_func_signature(funcid, &argtypes, &nargs)` (lsyscache.c): the
    /// function's argument type OIDs (length `nargs`), palloc'd in `mcx`. A
    /// missing pg_proc row is the C `elog(ERROR, "cache lookup failed for
    /// function %u")`, carried on `Err` (also OOM from the copy).
    pub fn get_func_signature<'mcx>(
        mcx: Mcx<'mcx>,
        func_oid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `op_input_types(opno, &lefttype, &righttype)` (lsyscache.c): the
    /// operator's input type OIDs. A missing pg_operator row is the C
    /// `elog(ERROR, "cache lookup failed for operator %u")`, carried on `Err`.
    pub fn op_input_types(opno: Oid) -> PgResult<(Oid, Oid)>
);

seam_core::seam!(
    /// `get_op_opfamily_properties(opno, opfamily, missing_ok, &strategy,
    /// &lefttype, &righttype)` (lsyscache.c): look up the operator's membership
    /// in the opfamily, returning its `(strategy, op_lefttype, op_righttype)`.
    /// With `missing_ok = false` a missing pg_amop row is the C `elog(ERROR,
    /// "operator %u is not a member of opfamily %u")`, carried on `Err`; with
    /// `missing_ok = true` a miss returns `Ok(None)`.
    pub fn get_op_opfamily_properties(
        opno: Oid,
        opfamily: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<(i32, Oid, Oid)>>
);

seam_core::seam!(
    /// `get_opfamily_method(opfid)` (lsyscache.c): the access-method OID
    /// (`opfmethod`) of the opfamily. A missing opfamily is the C `elog(ERROR,
    /// "cache lookup failed for operator family %u")`, carried on `Err`.
    pub fn get_opfamily_method(opfid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opfamily_proc(opfamily, lefttype, righttype, procnum)`
    /// (lsyscache.c): the support function OID registered for the given
    /// procnum/type pair, or `InvalidOid` (0) if none. `Err` carries the
    /// syscache machinery's `ereport(ERROR)`s.
    pub fn get_opfamily_proc(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        procnum: i16,
    ) -> PgResult<Oid>
);
