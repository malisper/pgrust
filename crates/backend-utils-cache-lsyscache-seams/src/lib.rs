//! Seam declarations for the `backend-utils-cache-lsyscache` unit
//! (`utils/cache/lsyscache.c` convenience catalog lookups).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_core::{AttrNumber, Oid};
use types_datum::Datum;
// Canonical unified value (the Datum-unification keystone) for the by-reference
// `attoptions` array, which cannot ride the bare scalar word.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;
use types_selfuncs::{AttStatsSlot, StatsTuple};
use types_array::{ArrayElementIoData, ArrayIoFuncSelector};

/// `OpIndexInterpretation` (`utils/lsyscache.h`): one entry of the list
/// `get_op_index_interpretation` returns — an amcanorder opfamily the operator
/// belongs to and its properties within that family.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpIndexInterpretation {
    /// `opfamily_id` — the opfamily containing the operator.
    pub opfamily_id: Oid,
    /// `cmptype` — the operator's generic `CompareType` (carried as `i32`,
    /// matching the rest of this unit's cmptype vocabulary; `COMPARE_NE` for
    /// the `<>`-via-negator case).
    pub cmptype: i32,
    /// `oplefttype` — the operator's declared left input datatype.
    pub oplefttype: Oid,
    /// `oprighttype` — the operator's declared right input datatype.
    pub oprighttype: Oid,
}

seam_core::seam!(
    /// `get_commutator(opno)` (lsyscache.c): the commutator operator of `opno`,
    /// or `InvalidOid` (0) if none. `Err` carries catcache-path
    /// `ereport(ERROR)`s.
    pub fn get_commutator(opno: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_attstatsslot(&sslot, statstuple, reqkind, reqop, flags)`
    /// (lsyscache.c): extract the first `pg_statistic` slot of the given kind
    /// (and optional operator), detoasting the requested `values` / `numbers`
    /// arrays into `mcx`. Returns `None` when no such slot exists (C: `false`);
    /// the C `free_attstatsslot` is the returned slot's `Drop`. `Err` carries
    /// the syscache / detoast `ereport(ERROR)`s and OOM.
    pub fn get_attstatsslot<'mcx>(
        mcx: Mcx<'mcx>,
        stats_tuple: StatsTuple,
        reqkind: i32,
        reqop: Oid,
        flags: i32,
    ) -> PgResult<Option<AttStatsSlot<'mcx>>>
);

/// The `(typlen, typbyval, typalign)` triple `get_typlenbyvalalign` reports.
#[derive(Clone, Copy, Debug, Default)]
pub struct TypLenByValAlign {
    /// `typlen` — `pg_type.typlen`.
    pub typlen: i16,
    /// `typbyval` — `pg_type.typbyval`.
    pub typbyval: bool,
    /// `typalign` — `pg_type.typalign`.
    pub typalign: i8,
}

seam_core::seam!(
    /// `get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign)`
    /// (lsyscache.c): the type's length, by-value flag, and alignment from
    /// its `pg_type` row. A missing type is the C `elog(ERROR, "cache lookup
    /// failed for type %u")`, carried on `Err`.
    pub fn get_typlenbyvalalign(typid: Oid) -> PgResult<TypLenByValAlign>
);

/// `IOFuncSelector` (`lsyscache.h`): which I/O function `get_type_io_data`
/// resolves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IOFuncSelector {
    /// `IOFunc_input`
    Input,
    /// `IOFunc_output`
    Output,
    /// `IOFunc_receive`
    Receive,
    /// `IOFunc_send`
    Send,
}

/// The output of `get_type_io_data` (lsyscache.c): the `pg_type` storage
/// parameters plus the resolved I/O proc OID and its I/O parameter OID.
#[derive(Clone, Copy, Debug, Default)]
pub struct TypeIoData {
    /// `typlen` — `pg_type.typlen`.
    pub typlen: i16,
    /// `typbyval` — `pg_type.typbyval`.
    pub typbyval: bool,
    /// `typalign` — `pg_type.typalign`.
    pub typalign: i8,
    /// `typdelim` — `pg_type.typdelim`.
    pub typdelim: i8,
    /// `typioparam` — `getTypeIOParam(typeTuple)`.
    pub typioparam: Oid,
    /// `func` — the resolved I/O function OID (`InvalidOid` when none, which
    /// can only happen for receive/send).
    pub func: Oid,
}

seam_core::seam!(
    /// `get_type_io_data(typid, which_func, &typlen, &typbyval, &typalign,
    /// &typdelim, &typioparam, &func)` (lsyscache.c): the storage parameters,
    /// I/O parameter OID and selected I/O proc OID of a type, from its
    /// `pg_type` row. `Err` carries the `elog(ERROR, "cache lookup failed for
    /// type %u")` and the catcache machinery's `ereport(ERROR)`s.
    pub fn get_type_io_data(typid: Oid, which_func: IOFuncSelector) -> PgResult<TypeIoData>
);

seam_core::seam!(
    /// `get_rel_relispartition(relid)` (lsyscache.c): whether the relation is
    /// a partition (`pg_class.relispartition`); `false` if there is no such
    /// pg_class row. `Err` carries the syscache machinery's
    /// `ereport(ERROR)`s.
    pub fn get_rel_relispartition(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCache3(STATRELATTINH, relid, attnum, inherit)` +
    /// `get_attstatsslot(&sslot, statsTuple, STATISTIC_KIND_MCV, InvalidOid,
    /// ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)` (the
    /// `ExecHashBuildSkewHash` skew-MCV probe). Returns the MCV slot's
    /// `(values, numbers)` arrays copied into `mcx` (the only `AttStatsSlot`
    /// fields the skew build reads), or `Ok(None)` when there is no
    /// `pg_statistic` row (`!HeapTupleIsValid`) or no MCV slot
    /// (`get_attstatsslot` returns false). The owner does the matching
    /// `free_attstatsslot` / `ReleaseSysCache`. `Err` carries the catcache /
    /// detoast `ereport(ERROR)`s plus OOM from the copy.
    pub fn get_attstatsslot_mcv<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: AttrNumber,
        inherit: bool,
    ) -> PgResult<Option<(PgVec<'mcx, Datum>, PgVec<'mcx, f32>)>>
);

seam_core::seam!(
    /// `getTypeOutputInfo(type, &typOutput, &typIsVarlena)` (lsyscache.c):
    /// the type's text output function OID and whether it is varlena,
    /// returned as `(typoutput, typisvarlena)`. A non-output type is the C
    /// `ereport(ERROR, ...cannot display a value of type...)`; cache lookup
    /// failure is `elog(ERROR)`. Both carried on `Err`.
    pub fn get_type_output_info(typid: Oid) -> PgResult<(Oid, bool)>
);

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
    /// `getTypeInputInfo(type, &typInput, &typIOParam)` (lsyscache.c): look up
    /// the type's input conversion function OID and its typioparam, returned as
    /// `(typInput, typIOParam)`. A missing type or one without a usable input
    /// function is the C `ereport(ERROR)` (`cache lookup failed` / `no input
    /// function available for type`), carried on `Err`.
    pub fn get_type_input_info(typ: Oid) -> PgResult<(Oid, Oid)>
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
    /// `get_namespace_name_or_temp(nspid)` (lsyscache.c): like
    /// [`get_namespace_name`], but returns the literal `"pg_temp"` when
    /// `nspid` is the calling backend's own temp namespace. A missing
    /// namespace is `Ok(None)`.
    /// `get_namespace_name_or_temp(nspid)` (lsyscache.c): `"pg_temp"` if
    /// `isTempNamespace(nspid)`, else `get_namespace_name(nspid)`, copied into
    /// `mcx` (C: `pstrdup`). A missing namespace is `Ok(None)`. `Err` is OOM.
    pub fn get_namespace_name_or_temp<'mcx>(
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
    /// `getTypeBinaryOutputInfo(type, &typSend, &typIsVarlena)` (lsyscache.c):
    /// the type's binary send-function OID and varlena flag, with the C cache-
    /// lookup and "no binary output function" `ereport`s carried on `Err`.
    /// Returns `(typsend, typisvarlena)`.
    pub fn get_type_binary_output_info(type_oid: Oid) -> PgResult<(Oid, bool)>
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
    /// `get_ordering_op_properties(opno, &opfamily, &opcintype, &cmptype)`
    /// (lsyscache.c): given an ordering operator (a btree "<" or ">"
    /// operator), return its containing opfamily, the opclass input type, and
    /// the comparison type. `Some((opfamily, opcintype, cmptype))` is the C
    /// `true`; `None` is the C `false` (the operator is not a valid ordering
    /// operator). `Err` carries the syscache machinery's `ereport(ERROR)`s.
    pub fn get_ordering_op_properties(
        opno: Oid,
    ) -> PgResult<Option<(Oid, Oid, i32)>>
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

seam_core::seam!(
    /// `get_func_rettype(funcid)` (lsyscache.c): the return type OID of the
    /// `pg_proc` entry. `elog(ERROR)` on cache lookup failure, carried on
    /// `Err`.
    pub fn get_func_rettype(funcid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_op_hash_functions(opno, &lhs_procno, &rhs_procno)` (lsyscache.c):
    /// resolve the LHS and RHS hash support functions of a hashable equality
    /// operator. Returns `Some((lhs, rhs))` when found (the C `true`), `None`
    /// when not (the C `false`). `Err` carries catcache-path `ereport(ERROR)`s.
    pub fn get_op_hash_functions(opno: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `op_strict(opno)` (lsyscache.c): whether the operator's underlying
    /// function is strict. `Err` carries catcache-path `ereport(ERROR)`s.
    pub fn op_strict(opno: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_compatible_hash_operators(opno, &lhs_opno, &rhs_opno)`
    /// (lsyscache.c): find the single-type `=` hash operators compatible with
    /// `opno` (which may be cross-type). Returns `Some((lhs_opno, rhs_opno))`
    /// when `opno` is registered as the `=` operator of some hash opfamily (the
    /// C `true`), `None` when not (the C `false`, both out-params left
    /// `InvalidOid`). `Err` carries the `pg_amop` syscache-list `ereport(ERROR)`s.
    pub fn get_compatible_hash_operators(opno: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `get_opfamily_member(opfamily, lefttype, righttype, strategy)`
    /// (lsyscache.c): the operator OID registered for the given strategy/type
    /// pair, or `InvalidOid` (0) if none. `Err` carries the syscache
    /// machinery's `ereport(ERROR)`s.
    pub fn get_opfamily_member(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Oid>
);

/* ---- additional reads consumed by the typcache port ----------------------- */

seam_core::seam!(
    /// `GetDefaultOpClass(type_id, am_id)` (indexing.c via lsyscache surface):
    /// default operator-class OID for the type in the given access method, or
    /// `InvalidOid`. `Err` carries the ambiguity `ereport(ERROR)` and the
    /// catalog-scan failure surface.
    pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opclass_family(opclass)` (lsyscache.c): the opclass's `opcfamily`.
    /// A missing opclass is the C `elog(ERROR, "cache lookup failed for
    /// opclass %u")`, carried on `Err`.
    pub fn get_opclass_family(opclass: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opcode(opno)` (lsyscache.c): the function OID implementing an
    /// operator. A missing operator is the C `elog(ERROR, "cache lookup
    /// failed for operator %u")`, carried on `Err`.
    pub fn get_opcode(opno: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `getBaseTypeAndTypmod(type_id, &typmod)` (lsyscache.c): walk the domain
    /// chain to the base type, returning `(basetype, typmod)`. `Err` carries
    /// the syscache failure surface.
    pub fn get_base_type_and_typmod(type_id: Oid) -> PgResult<(Oid, i32)>
);

seam_core::seam!(
    /// `get_base_element_type(type_id)` (lsyscache.c): the element type of the
    /// base type of `type_id`, or `InvalidOid`. `Err` carries the syscache
    /// failure surface.
    pub fn get_base_element_type(type_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_multirange_range(multirange_type_id)` (lsyscache.c): the range
    /// type OID of a multirange, or `InvalidOid`. `Err` carries the syscache
    /// failure surface.
    pub fn get_multirange_range(multirange_type_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(RANGETYPE, ...)` → `Form_pg_range` fields read by
    /// `load_rangetype_info`, or `None` when no such row. `Err` carries the
    /// catcache failure surface.
    pub fn lookup_pg_range(
        range_type_id: Oid,
    ) -> PgResult<Option<types_cache::typcache::PgRangeRow>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, ...)` → the `Form_pg_type` fields the
    /// typcache reads when building a `TypeCacheEntry`, or `None` when the
    /// type OID does not exist. The `typname` (error-message only) rides as an
    /// owned `String`. `Err` carries the catcache failure surface and OOM.
    pub fn lookup_pg_type(
        type_id: Oid,
    ) -> PgResult<Option<types_cache::typcache::PgTypeRow>>
);

seam_core::seam!(
    /// `GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(type_id))` — the
    /// syscache hash value stored as `TypeCacheEntry.type_id_hash`. `Err`
    /// carries the catcache failure surface.
    pub fn syscache_hash_value_typeoid(type_id: Oid) -> PgResult<u32>
);

seam_core::seam!(
    /// `get_index_isclustered(indexOid)` (lsyscache.c) — used by CLUSTER.
    pub fn get_index_isclustered(index_oid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `get_rel_namespace(relid)` (lsyscache.c) — used by CLUSTER.
    pub fn get_rel_namespace(relid: Oid) -> PgResult<Oid>
);

// ---------------------------------------------------------------------------
// Element-type metadata seams driven by `utils/adt/arrayfuncs.c`
// (backend-utils-adt-arrayfuncs). All are lsyscache.c lookups.
// ---------------------------------------------------------------------------

// NOTE: `get_typlenbyvalalign` is already declared above (returns
// `TypLenByValAlign`); arrayfuncs reuses that seam rather than redeclaring it.

seam_core::seam!(
    /// `get_element_type(array_type)` (lsyscache.c): the element type OID of a
    /// true array type, or `None` (C `InvalidOid`) if `array_type` is not an
    /// array.
    pub fn get_element_type(array_type: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `get_array_type(input_type)` (lsyscache.c): the `typarray` of
    /// `input_type`, or `None` (C `InvalidOid`) if it has no array type.
    pub fn get_array_type(input_type: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `get_type_io_data(typid, which, &typlen, &typbyval, &typalign,
    /// &typdelim, &typioparam, &func)` (lsyscache.c): the element type's
    /// storage metadata plus the OID of its selected I/O function (input /
    /// output / receive / send per `which`).
    ///
    /// Array-element-typed projection of the same `get_type_io_data` C entry
    /// point used by arrayfuncs.c; named distinctly from the canonical
    /// [`get_type_io_data`] seam above (which other callers consume with the
    /// `IOFuncSelector` / `TypeIoData` shape) to keep one symbol per decl.
    pub fn get_array_element_io_data(
        element_type: Oid,
        which: ArrayIoFuncSelector,
    ) -> PgResult<ArrayElementIoData>
);

// ===========================================================================
// Remaining lsyscache.c entry points (PG 18.3) — completing C-source coverage.
// ===========================================================================

// ---- opfamily / operator (pg_amop, pg_operator) ---------------------------

seam_core::seam!(
    /// `op_in_opfamily(opno, opfamily)` (lsyscache.c): whether `opno` is a
    /// (search) member of `opfamily`. `Err` carries the catcache surface.
    pub fn op_in_opfamily(opno: Oid, opfamily: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_op_opfamily_strategy(opno, opfamily)` (lsyscache.c): the operator's
    /// (search) strategy number within the opfamily, or `0` if not a member.
    pub fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_op_opfamily_sortfamily(opno, opfamily)` (lsyscache.c): the
    /// `amopsortfamily` of the operator as an ordering member of `opfamily`, or
    /// `InvalidOid`.
    pub fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opfamily_member_for_cmptype(opfamily, lefttype, righttype, cmptype)`
    /// (lsyscache.c): the operator implementing the given comparison type, or
    /// `InvalidOid`. `cmptype` is the `CompareType` `i32`.
    pub fn get_opfamily_member_for_cmptype(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        cmptype: i32,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_equality_op_for_ordering_op(opno, &reverse)` (lsyscache.c): the
    /// datatype-specific equality operator for an ordering operator. Returns
    /// `Some((eqop, reverse))` (the C result OID and `*reverse` flag) when the
    /// operator is a valid ordering operator, `None` otherwise (the C
    /// `InvalidOid` with `*reverse` untouched).
    pub fn get_equality_op_for_ordering_op(opno: Oid) -> PgResult<Option<(Oid, bool)>>
);

seam_core::seam!(
    /// `get_ordering_op_for_equality_op(opno, use_lhs_type)` (lsyscache.c): a
    /// datatype-specific "<" ordering operator compatible with an equality
    /// operator, or `InvalidOid`.
    pub fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_mergejoin_opfamilies(opno)` (lsyscache.c): the list of amcanorder
    /// opfamily OIDs in which `opno` represents equality, allocated in `mcx`
    /// (empty list == the C `NIL`).
    pub fn get_mergejoin_opfamilies<'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `get_op_index_interpretation(opno)` (lsyscache.c): the amcanorder
    /// opfamilies `opno` belongs to and its properties within each, as a list
    /// allocated in `mcx` (empty == the C `NIL`).
    pub fn get_op_index_interpretation<'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
    ) -> PgResult<PgVec<'mcx, OpIndexInterpretation>>
);

seam_core::seam!(
    /// `equality_ops_are_compatible(opno1, opno2)` (lsyscache.c): whether two
    /// equality operators have compatible equality semantics.
    pub fn equality_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `comparison_ops_are_compatible(opno1, opno2)` (lsyscache.c): whether two
    /// comparison operators have compatible ordering semantics.
    pub fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_opname(opno)` (lsyscache.c): the operator's name copied into `mcx`
    /// (C: `pstrdup`), or `Ok(None)` if no such operator. `Err` is OOM.
    pub fn get_opname<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_op_rettype(opno)` (lsyscache.c): the operator's result type, or
    /// `InvalidOid` if no such operator.
    pub fn get_op_rettype(opno: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `op_mergejoinable(opno, inputtype)` (lsyscache.c): whether the operator
    /// is potentially mergejoinable (rep. for `array_eq`/`record_eq` via the
    /// typcache; otherwise `pg_operator.oprcanmerge`).
    pub fn op_mergejoinable(opno: Oid, inputtype: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `op_hashjoinable(opno, inputtype)` (lsyscache.c): whether the operator is
    /// hashjoinable (rep. for `array_eq`/`record_eq` via the typcache; otherwise
    /// `pg_operator.oprcanhash`).
    pub fn op_hashjoinable(opno: Oid, inputtype: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `op_volatile(opno)` (lsyscache.c): the `provolatile` of the operator's
    /// underlying function (`i`/`s`/`v`). `Err` carries the "operator does not
    /// exist" / cache-lookup `elog`.
    pub fn op_volatile(opno: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_negator(opno)` (lsyscache.c): the operator's negator, or
    /// `InvalidOid`.
    pub fn get_negator(opno: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_oprrest(opno)` (lsyscache.c): the operator's restriction selectivity
    /// estimator OID, or `InvalidOid`.
    pub fn get_oprrest(opno: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_oprjoin(opno)` (lsyscache.c): the operator's join selectivity
    /// estimator OID, or `InvalidOid`.
    pub fn get_oprjoin(opno: Oid) -> PgResult<Oid>
);

// ---- opclass (pg_opclass) -------------------------------------------------

seam_core::seam!(
    /// `get_opclass_opfamily_and_input_type(opclass, &opfamily, &opcintype)`
    /// (lsyscache.c): `Some((opfamily, opcintype))` (the C `true`) or `None`
    /// (the C `false`, missing opclass).
    pub fn get_opclass_opfamily_and_input_type(opclass: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `get_opclass_method(opclass)` (lsyscache.c): the index AM OID
    /// (`opcmethod`) of the opclass; missing opclass is `elog(ERROR)`.
    pub fn get_opclass_method(opclass: Oid) -> PgResult<Oid>
);

// ---- attribute (pg_attribute) ---------------------------------------------

seam_core::seam!(
    /// `get_attgenerated(relid, attnum)` (lsyscache.c): the `attgenerated`
    /// char; missing attribute is `elog(ERROR)`.
    pub fn get_attgenerated(relid: Oid, attnum: AttrNumber) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_atttype(relid, attnum)` (lsyscache.c): the attribute's type OID, or
    /// `InvalidOid` if absent.
    pub fn get_atttype(relid: Oid, attnum: AttrNumber) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid)`
    /// (lsyscache.c): `(atttypid, atttypmod, attcollation)`; missing attribute
    /// is `elog(ERROR)`.
    pub fn get_atttypetypmodcoll(relid: Oid, attnum: AttrNumber) -> PgResult<(Oid, i32, Oid)>
);

seam_core::seam!(
    /// `get_attoptions(relid, attnum)` (lsyscache.c): the attribute's
    /// `attoptions` `text[]` Datum copied into `mcx`, or `Ok(None)` for SQL
    /// null (the C `(Datum) 0`). Missing attribute is `elog(ERROR)`.
    pub fn get_attoptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<DatumV<'mcx>>>
);

// ---- function (pg_proc) ---------------------------------------------------

seam_core::seam!(
    /// `get_func_name(funcid)` (lsyscache.c): the function's name copied into
    /// `mcx` (C: `pstrdup`), or `Ok(None)` if absent.
    pub fn get_func_name<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_func_namespace(funcid)` (lsyscache.c): the function's schema OID, or
    /// `InvalidOid` if absent.
    pub fn get_func_namespace(funcid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_func_nargs(funcid)` (lsyscache.c): the number of arguments; missing
    /// function is `elog(ERROR)`.
    pub fn get_func_nargs(funcid: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_func_variadictype(funcid)` (lsyscache.c): `provariadic`; missing
    /// function is `elog(ERROR)`.
    pub fn get_func_variadictype(funcid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_func_retset(funcid)` (lsyscache.c): `proretset`; missing function is
    /// `elog(ERROR)`.
    pub fn get_func_retset(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `func_volatile(funcid)` (lsyscache.c): `provolatile` (`i`/`s`/`v`);
    /// missing function is `elog(ERROR)`.
    pub fn func_volatile(funcid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `func_parallel(funcid)` (lsyscache.c): `proparallel` (`s`/`r`/`u`);
    /// missing function is `elog(ERROR)`.
    pub fn func_parallel(funcid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_func_prokind(funcid)` (lsyscache.c): `prokind` (`f`/`p`/`a`/`w`);
    /// missing function is `elog(ERROR)`.
    pub fn get_func_prokind(funcid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_func_leakproof(funcid)` (lsyscache.c): `proleakproof`; missing
    /// function is `elog(ERROR)`.
    pub fn get_func_leakproof(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `func_strict(funcid)` (lsyscache.c): `proisstrict`; missing function is
    /// `elog(ERROR)`.
    pub fn func_strict(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_func_support(funcid)` (lsyscache.c): the planner support function
    /// OID, or `InvalidOid` if absent.
    pub fn get_func_support(funcid: Oid) -> PgResult<Oid>
);

// ---- relation (pg_class) --------------------------------------------------

seam_core::seam!(
    /// `get_relnatts(relid)` (lsyscache.c): the number of attributes, or
    /// `InvalidAttrNumber` (0) if absent.
    pub fn get_relnatts(relid: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_rel_type_id(relid)` (lsyscache.c): the relation's composite type
    /// OID (`reltype`), or `InvalidOid`.
    pub fn get_rel_type_id(relid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_rel_tablespace(relid)` (lsyscache.c): the relation's tablespace OID
    /// (`reltablespace`), or `InvalidOid`.
    pub fn get_rel_tablespace(relid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_rel_persistence(relid)` (lsyscache.c): `relpersistence`
    /// (`p`/`u`/`t`); missing relation is `elog(ERROR)`.
    pub fn get_rel_persistence(relid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_rel_relam(relid)` (lsyscache.c): the relation's access method OID
    /// (`relam`); missing relation is `elog(ERROR)`.
    pub fn get_rel_relam(relid: Oid) -> PgResult<Oid>
);

// ---- type (pg_type) -------------------------------------------------------

seam_core::seam!(
    /// `get_typisdefined(typid)` (lsyscache.c): `typisdefined`, or `false` if
    /// absent.
    pub fn get_typisdefined(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_typlen(typid)` (lsyscache.c): `typlen`, or `0` if absent.
    pub fn get_typlen(typid: Oid) -> PgResult<i16>
);

seam_core::seam!(
    /// `get_typbyval(typid)` (lsyscache.c): `typbyval`, or `false` if absent.
    pub fn get_typbyval(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_typlenbyval(typid, &typlen, &typbyval)` (lsyscache.c): `(typlen,
    /// typbyval)`; missing type is `elog(ERROR)`.
    pub fn get_typlenbyval(typid: Oid) -> PgResult<(i16, bool)>
);

seam_core::seam!(
    /// `get_typstorage(typid)` (lsyscache.c): `typstorage`, or `TYPSTORAGE_PLAIN`
    /// (`'p'`) if absent.
    pub fn get_typstorage(typid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `get_typtype(typid)` (lsyscache.c): `typtype`, or `'\0'` if absent.
    pub fn get_typtype(typid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `type_is_rowtype(typid)` (lsyscache.c): whether the type is RECORD or a
    /// (possibly domain-over) named composite type.
    pub fn type_is_rowtype(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `type_is_enum(typid)` (lsyscache.c).
    pub fn type_is_enum(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `type_is_range(typid)` (lsyscache.c).
    pub fn type_is_range(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `type_is_multirange(typid)` (lsyscache.c).
    pub fn type_is_multirange(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_type_category_preferred(typid, &typcategory, &typispreferred)`
    /// (lsyscache.c): `(typcategory, typispreferred)`; missing type is
    /// `elog(ERROR)`.
    pub fn get_type_category_preferred(typid: Oid) -> PgResult<(u8, bool)>
);

seam_core::seam!(
    /// `get_typ_typrelid(typid)` (lsyscache.c): `typrelid`, or `InvalidOid` if
    /// absent or not a complex type.
    pub fn get_typ_typrelid(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_promoted_array_type(typid)` (lsyscache.c): the "true" array type of
    /// a scalar, or the type itself if already a true array, else `InvalidOid`.
    pub fn get_promoted_array_type(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `getTypeBinaryInputInfo(type, &typReceive, &typIOParam)` (lsyscache.c):
    /// `(typreceive, typioparam)`; shell/undefined or no-receive-function types
    /// raise `ereport(ERROR)`.
    pub fn get_type_binary_input_info(typ: Oid) -> PgResult<(Oid, Oid)>
);

seam_core::seam!(
    /// `get_typmodin(typid)` (lsyscache.c): `typmodin`, or `InvalidOid`.
    pub fn get_typmodin(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_typmodout(typid)` (lsyscache.c): `typmodout`, or `InvalidOid`.
    pub fn get_typmodout(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_typcollation(typid)` (lsyscache.c): `typcollation`, or `InvalidOid`.
    pub fn get_typcollation(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `type_is_collatable(typid)` (lsyscache.c): `OidIsValid(get_typcollation)`.
    pub fn type_is_collatable(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_typsubscript(typid, &typelem)` (lsyscache.c): `(typsubscript,
    /// typelem)`; `(InvalidOid, InvalidOid)` if absent.
    pub fn get_typsubscript(typid: Oid) -> PgResult<(Oid, Oid)>
);

seam_core::seam!(
    /// `getSubscriptingRoutines(typid, &typelem)` (lsyscache.c): the type's
    /// subscripting routines pointer (the `OidFunctionCall0` result, kept opaque
    /// — see the fmgr `oid_function_call0` seam) and its `typelem`. `None` means
    /// the type is not subscriptable (the C NULL). `Datum` carries the
    /// `const SubscriptRoutines *`.
    pub fn get_subscripting_routines(typid: Oid) -> PgResult<Option<(Datum, Oid)>>
);

seam_core::seam!(
    /// `get_typavgwidth(typid, typmod)` (lsyscache.c): the planner's estimated
    /// average value width for the type.
    pub fn get_typavgwidth(typid: Oid, typmod: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_typdefault(typid)` (lsyscache.c): the type's default-value
    /// expression node tree (`stringToNode(typdefaultbin)` if present, else a
    /// `makeConst` over the literal `typdefault`), allocated in `mcx`, or
    /// `Ok(None)` when the type has no default (the C `NULL`). A missing type is
    /// `elog(ERROR, "cache lookup failed for type %u")`.
    pub fn get_typdefault<'mcx>(
        mcx: Mcx<'mcx>,
        typid: Oid,
    ) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>>
);

// ---- statistics (pg_statistic) --------------------------------------------

seam_core::seam!(
    /// `get_attavgwidth(relid, attnum)` (lsyscache.c): the average stored width
    /// of the column from `pg_statistic`, or `0` if no data.
    pub fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> PgResult<i32>
);

seam_core::seam!(
    /// `free_attstatsslot(sslot)` (lsyscache.c): release a slot obtained from
    /// [`get_attstatsslot`]. In the owned model the slot's storage is freed by
    /// its `Drop`; this named entry point consumes the slot to make that point
    /// explicit (the C frees `sslot->values_arr` / `sslot->numbers_arr`).
    pub fn free_attstatsslot<'mcx>(sslot: AttStatsSlot<'mcx>)
);

// ---- range (pg_range) -----------------------------------------------------

seam_core::seam!(
    /// `get_range_subtype(rangeOid)` (lsyscache.c): the range's subtype, or
    /// `InvalidOid` if not a range type.
    pub fn get_range_subtype(range_oid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_range_collation(rangeOid)` (lsyscache.c): the range's collation, or
    /// `InvalidOid`.
    pub fn get_range_collation(range_oid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_range_multirange(rangeOid)` (lsyscache.c): the range's multirange
    /// type (`rngmultitypid`), or `InvalidOid`.
    pub fn get_range_multirange(range_oid: Oid) -> PgResult<Oid>
);

// ---- index (pg_index) -----------------------------------------------------

seam_core::seam!(
    /// `get_index_column_opclass(index_oid, attno)` (lsyscache.c): the opclass
    /// of the index's `attno`th column, or `InvalidOid` if the index was not
    /// found or `attno` is a non-key column.
    pub fn get_index_column_opclass(index_oid: Oid, attno: i32) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_index_isreplident(index_oid)` (lsyscache.c): `indisreplident`, or
    /// `false` if absent.
    pub fn get_index_isreplident(index_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_index_isvalid(index_oid)` (lsyscache.c): `indisvalid`; missing
    /// index is `elog(ERROR)`.
    pub fn get_index_isvalid(index_oid: Oid) -> PgResult<bool>
);

// ---- publication / subscription -------------------------------------------

seam_core::seam!(
    /// `get_publication_oid(pubname, missing_ok)` (lsyscache.c): the
    /// publication's OID; with `missing_ok = false` a miss raises
    /// `ereport(ERROR)`, else returns `InvalidOid`.
    pub fn get_publication_oid(pubname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_publication_name(pubid, missing_ok)` (lsyscache.c): the
    /// publication's name copied into `mcx` (C: `pstrdup`). With
    /// `missing_ok = false` a miss raises `elog(ERROR)`, else `Ok(None)`.
    pub fn get_publication_name<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_subscription_oid(subname, missing_ok)` (lsyscache.c): the
    /// subscription's OID in the current database; with `missing_ok = false` a
    /// miss raises `ereport(ERROR)`, else returns `InvalidOid`.
    pub fn get_subscription_oid(subname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_subscription_name(subid, missing_ok)` (lsyscache.c): the
    /// subscription's name copied into `mcx` (C: `pstrdup`). With
    /// `missing_ok = false` a miss raises `elog(ERROR)`, else `Ok(None)`.
    pub fn get_subscription_name<'mcx>(
        mcx: Mcx<'mcx>,
        subid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);
