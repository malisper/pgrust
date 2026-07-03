use types_core::Oid;
use types_error::PgResult;
use types_storage::PgClassShape;
use types_tuple::{HeapTupleData, PgTypeShape};

seam_core::seam!(
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // SearchSysCacheExists1(DATABASEOID, dboid).
    pub fn search_syscache_exists_databaseoid(dboid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    pub fn sys_cache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()>
);

seam_core::seam!(
    // RelationInvalidatesSnapshotsOnly (syscache.c).
    pub fn relation_invalidates_snapshots_only(relid: Oid) -> bool
);

seam_core::seam!(
    // SearchSysCache1(RELOID, relid) projected to (oid, relisshared);
    // None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_class_by_relid(relid: Oid) -> PgResult<Option<PgClassShape>>
);

seam_core::seam!(
    // GETSTRUCT(tuple) as Form_pg_class, projected to (oid, relisshared).
    pub fn pg_class_shape(tuple: &HeapTupleData<'_>) -> PgClassShape
);

seam_core::seam!(
    pub fn pg_attribute_attrelid(tuple: &HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    pub fn pg_index_indexrelid(tuple: &HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    // Some(conrelid) iff contype == CONSTRAINT_FOREIGN && OidIsValid(conrelid).
    pub fn pg_constraint_fk_target(tuple: &HeapTupleData<'_>) -> Option<Oid>
);

seam_core::seam!(
    // SearchSysCache1(TYPEOID, typid) projected to TupleDescInitEntry's reads;
    // None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_type_shape(typid: Oid) -> PgResult<Option<PgTypeShape>>
);

seam_core::seam!(
    // SearchSysCache1(AUTHOID, roleid) projected to pg_authid.rolname
    // (GetUserNameFromId's single-field read); None mirrors !HeapTupleIsValid.
    pub fn lookup_authid_rolname<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        roleid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    // SearchSysCache1(AUTHNAME, rolname) projected to (oid, rolsuper) — the
    // check_session_authorization/check_role read; None mirrors !HeapTupleIsValid.
    pub fn lookup_authid_by_rolname(rolname: &str) -> PgResult<Option<(Oid, bool)>>
);

#[derive(Clone, Copy, Debug)]
pub struct AuthIdSessionShape {
    pub roleid: Oid,
    pub rolname: types_tuple::NameData,
    pub rolsuper: bool,
    pub rolcanlogin: bool,
    pub rolconnlimit: i32,
}

seam_core::seam!(
    // SearchSysCache1(AUTHNAME, rolename) projected to the five fields
    // InitializeSessionUserId reads; None mirrors !HeapTupleIsValid.
    pub fn lookup_authid_session_by_rolname(
        rolname: &str,
    ) -> PgResult<Option<AuthIdSessionShape>>
);

seam_core::seam!(
    // SearchSysCache1(AUTHOID, roleid), same projection.
    pub fn lookup_authid_session_by_oid(roleid: Oid) -> PgResult<Option<AuthIdSessionShape>>
);

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::AttrNumber;
use types_tuple::NameData;


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAmopShape {
    pub amopstrategy: i16,
    pub amopsortfamily: Oid,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAmopMemberShape {
    pub amopfamily: Oid,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
    pub amopstrategy: i16,
    pub amopmethod: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAttributeLsShape {
    pub attname: NameData,
    pub atttypid: Oid,
    pub atttypmod: i32,
    pub attcollation: Oid,
    pub attgenerated: i8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgCollationShape {
    pub collname: NameData,
    pub collisdeterministic: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgConstraintShape {
    pub conname: NameData,
    pub contype: i8,
    pub conindid: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOpclassShape {
    pub opcmethod: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOpfamilyShape {
    pub opfmethod: Oid,
    pub opfname: NameData,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOperatorShape {
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub oprcom: Oid,
    pub oprnegate: Oid,
    pub oprcode: Oid,
    pub oprrest: Oid,
    pub oprjoin: Oid,
    pub oprcanmerge: bool,
    pub oprcanhash: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgProcShape {
    pub pronamespace: Oid,
    pub prorettype: Oid,
    pub provariadic: Oid,
    pub prosupport: Oid,
    pub pronargs: i16,
    pub prokind: i8,
    pub provolatile: i8,
    pub proparallel: i8,
    pub proretset: bool,
    pub proisstrict: bool,
    pub proleakproof: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgClassLsShape {
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub relam: Oid,
    pub reltablespace: Oid,
    pub relnatts: i16,
    pub relkind: i8,
    pub relpersistence: i8,
    pub relispartition: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTransformShape {
    pub trffromsql: Oid,
    pub trftosql: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTypeElementShape {
    pub typelem: Oid,
    pub typsubscript: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTypeBaseShape {
    pub typtype: i8,
    pub typbasetype: Oid,
    pub typtypmod: i32,
    pub typelem: Oid,
    pub typsubscript: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTypeIoShape {
    pub oid: Oid,
    pub typinput: Oid,
    pub typoutput: Oid,
    pub typreceive: Oid,
    pub typsend: Oid,
    pub typmodin: Oid,
    pub typmodout: Oid,
    pub typelem: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typdelim: i8,
    pub typisdefined: bool,
}

#[derive(Debug)]
pub struct PgTypeDefaultShape<'mcx> {
    pub typdefaultbin: Option<PgString<'mcx>>,
    pub typdefault: Option<PgString<'mcx>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgRangeShape {
    pub rngsubtype: Oid,
    pub rngcollation: Oid,
    pub rngmultitypid: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgIndexLsShape {
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisreplident: bool,
    pub indisvalid: bool,
    pub indisclustered: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgStatisticSlotShape {
    pub stakind: [i16; 5],
    pub staop: [Oid; 5],
    pub stacoll: [Oid; 5],
}

seam_core::seam!(
    // SearchSysCache3(AMOPOPID, opno, purpose, opfamily); purpose is
    // AMOP_SEARCH b's' / AMOP_ORDER b'o'. None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_amop_by_operator(
        opno: Oid,
        purpose: u8,
        opfamily: Oid,
    ) -> PgResult<Option<PgAmopShape>>
);

seam_core::seam!(
    pub fn lookup_pg_amop_by_strategy(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_amop_members_by_operator<'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
    ) -> PgResult<PgVec<'mcx, PgAmopMemberShape>>
);

seam_core::seam!(
    pub fn lookup_pg_amproc(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        procnum: i16,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_attribute_shape(
        relid: Oid,
        attnum: AttrNumber,
    ) -> PgResult<Option<PgAttributeLsShape>>
);

seam_core::seam!(
    pub fn lookup_pg_attribute_attnum_by_name(relid: Oid, attname: &str) -> PgResult<AttrNumber>
);

seam_core::seam!(
    // SysCacheGetAttr(ATTNUM tuple, attoptions) + datumCopy into mcx.
    // Outer None mirrors !HeapTupleIsValid; inner None mirrors isnull.
    pub fn pg_attribute_attoptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<Option<Datum>>>
);

seam_core::seam!(
    pub fn lookup_pg_cast_oid(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Oid>
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgCastShape {
    pub oid: Oid,
    pub castfunc: Oid,
    pub castcontext: i8,
    pub castmethod: i8,
}

seam_core::seam!(
    // SearchSysCache2(CASTSOURCETARGET, src, tgt); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_cast_shape(
        sourcetypeid: Oid,
        targettypeid: Oid,
    ) -> PgResult<Option<PgCastShape>>
);

seam_core::seam!(
    // SearchSysCache4(OPERNAMENSP, oprname, oprleft, oprright, oprnamespace)
    // projected to the operator oid; InvalidOid on a miss.
    pub fn lookup_pg_operator_oid_exact(
        opername: &str,
        oprleft: Oid,
        oprright: Oid,
        oprnamespace: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // SearchSysCacheList3(OPERNAMENSP, oprname, oprleft, oprright) projected
    // to (oid, oprnamespace) per member, catcache list order.
    pub fn lookup_pg_operator_candidates<'mcx>(
        mcx: Mcx<'mcx>,
        opername: &str,
        oprleft: Oid,
        oprright: Oid,
    ) -> PgResult<PgVec<'mcx, (Oid, Oid)>>
);

seam_core::seam!(
    // SearchSysCacheList1(OPERNAMENSP, oprname): any member with this oprkind
    // (OpernameGetCandidates' existence question, pre-visibility).
    pub fn pg_operator_name_candidates_exist(opername: &str, oprkind: i8) -> PgResult<bool>
);

seam_core::seam!(
    pub fn lookup_pg_collation_shape(colloid: Oid) -> PgResult<Option<PgCollationShape>>
);

seam_core::seam!(
    pub fn lookup_pg_constraint_shape(conoid: Oid) -> PgResult<Option<PgConstraintShape>>
);

seam_core::seam!(
    pub fn lookup_pg_language_name(langoid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_opclass_shape(opclass: Oid) -> PgResult<Option<PgOpclassShape>>
);

seam_core::seam!(
    pub fn lookup_pg_opfamily_shape(opfid: Oid) -> PgResult<Option<PgOpfamilyShape>>
);

seam_core::seam!(
    pub fn lookup_pg_operator_shape(opno: Oid) -> PgResult<Option<PgOperatorShape>>
);

seam_core::seam!(
    pub fn pg_operator_oprname(opno: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_proc_shape(funcid: Oid) -> PgResult<Option<PgProcShape>>
);

seam_core::seam!(
    pub fn pg_proc_proname(funcid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_proc_signature<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<(Oid, PgVec<'mcx, Oid>)>>
);

#[derive(Debug)]
pub struct PgProcResultArraysShape<'mcx> {
    pub proallargtypes: Option<PgVec<'mcx, Oid>>,
    pub proargmodes: Option<PgVec<'mcx, i8>>,
    pub proargnames: Option<PgVec<'mcx, PgString<'mcx>>>,
}

seam_core::seam!(
    // SysCacheGetAttr(PROCOID tuple, proallargtypes/proargmodes/proargnames),
    // arrays deconstructed; the 1-D/no-null/elemtype/equal-length elogs are the
    // installer's. Inner None per field mirrors attisnull; outer None mirrors
    // !HeapTupleIsValid.
    pub fn pg_proc_result_arrays<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<PgProcResultArraysShape<'mcx>>>
);

seam_core::seam!(
    pub fn lookup_pg_class_relid_by_name(relname: &str, relnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_class_ls_shape(relid: Oid) -> PgResult<Option<PgClassLsShape>>
);

seam_core::seam!(
    pub fn pg_class_relname(relid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_transform_shape(typid: Oid, langid: Oid) -> PgResult<Option<PgTransformShape>>
);

seam_core::seam!(
    pub fn pg_type_isdefined(typid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    pub fn pg_type_typtype(typid: Oid) -> PgResult<Option<i8>>
);

seam_core::seam!(
    pub fn pg_type_category(typid: Oid) -> PgResult<Option<(i8, bool)>>
);

seam_core::seam!(
    pub fn pg_type_typrelid(typid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    pub fn pg_type_element_shape(typid: Oid) -> PgResult<Option<PgTypeElementShape>>
);

seam_core::seam!(
    pub fn pg_type_typarray(typid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    pub fn pg_type_base_shape(typid: Oid) -> PgResult<Option<PgTypeBaseShape>>
);

seam_core::seam!(
    pub fn pg_type_io_shape(typid: Oid) -> PgResult<Option<PgTypeIoShape>>
);

seam_core::seam!(
    pub fn pg_type_default_strings<'mcx>(
        mcx: Mcx<'mcx>,
        typid: Oid,
    ) -> PgResult<Option<PgTypeDefaultShape<'mcx>>>
);

seam_core::seam!(
    pub fn lookup_pg_range_shape(range_oid: Oid) -> PgResult<Option<PgRangeShape>>
);

seam_core::seam!(
    pub fn lookup_pg_range_by_multirange(multirange_oid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    pub fn lookup_pg_index_ls_shape(index_oid: Oid) -> PgResult<Option<PgIndexLsShape>>
);

seam_core::seam!(
    // SysCacheGetAttrNotNull(INDEXRELID tuple, indclass).values[index];
    // None mirrors !HeapTupleIsValid. PRECONDITION: index < indclass->dim1.
    pub fn pg_index_indclass_element(index_oid: Oid, index: i32) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    pub fn lookup_pg_publication_oid(pubname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn pg_publication_pubname(pubid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    // GetSysCacheOid2(SUBSCRIPTIONNAME, dbid, subname); InvalidOid on miss.
    pub fn lookup_pg_subscription_oid(dbid: Oid, subname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn pg_subscription_subname(subid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn pg_statistic_stawidth(relid: Oid, attnum: AttrNumber, inh: bool) -> PgResult<Option<i32>>
);

pub struct PgProcCostShape {
    pub procost: f32,
    pub prosupport: Oid,
}

seam_core::seam!(
    // SearchSysCache1(PROCOID) projection for add_function_cost/get_function_rows.
    pub fn pg_proc_cost_shape(funcid: Oid) -> PgResult<Option<PgProcCostShape>>
);

pub struct PgStatisticShape {
    pub stanullfrac: f32,
    pub stawidth: i32,
    pub stadistinct: f32,
}

seam_core::seam!(
    // SearchSysCache3(STATRELATTINH) scalar projection; None mirrors
    // !HeapTupleIsValid. Slot arrays stay behind pg_statistic_slot_shape.
    pub fn lookup_pg_statistic_shape(
        relid: Oid,
        attnum: AttrNumber,
        inh: bool,
    ) -> PgResult<Option<PgStatisticShape>>
);

seam_core::seam!(
    pub fn pg_statistic_slot_shape(tuple: &HeapTupleData<'_>) -> PgStatisticSlotShape
);

seam_core::seam!(
    pub fn pg_namespace_nspname(nspid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    // RelationHasSysCache (syscache.c).
    pub fn relation_has_sys_cache(relid: Oid) -> bool
);

// The pg_type columns lookup_type_cache copies into a TypeCacheEntry, plus
// the typisdefined/typname pair its shell-type ereport needs.
#[derive(Clone, Copy, Debug)]
pub struct PgTypeTypcacheShape {
    pub typname: NameData,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typstorage: i8,
    pub typtype: i8,
    pub typisdefined: bool,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typcollation: Oid,
}

seam_core::seam!(
    pub fn lookup_pg_type_typcache_shape(typid: Oid) -> PgResult<Option<PgTypeTypcacheShape>>
);

seam_core::seam!(
    // GetSysCacheHashValue1(TYPEOID, typid).
    pub fn syscache_hash_value_typeoid(typid: Oid) -> PgResult<u32>
);

seam_core::seam!(
    // GetSysCacheOid1(NAMESPACENAME, Anum_pg_namespace_oid, nspname);
    // InvalidOid on a miss.
    pub fn lookup_pg_namespace_oid_by_name(nspname: &str) -> PgResult<Oid>
);

// The pg_collation columns pg_locale.c's create arms read off one COLLOID
// probe, decoded once.
#[derive(Debug)]
pub struct PgCollationLocaleRow<'mcx> {
    pub collname: NameData,
    pub collnamespace: Oid,
    pub collprovider: u8,
    pub collisdeterministic: bool,
    pub collcollate: Option<PgString<'mcx>>,
    pub collctype: Option<PgString<'mcx>>,
    pub colllocale: Option<PgString<'mcx>>,
    pub collversion: Option<PgString<'mcx>>,
}

seam_core::seam!(
    // SearchSysCache1(COLLOID, collid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_collation_locale_row<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<Option<PgCollationLocaleRow<'mcx>>>
);

// The pg_aggregate columns parse_func/prepagg/ExecInitAgg read off one
// AGGFNOID probe, decoded once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAggregateShape {
    pub aggkind: i8,
    pub aggnumdirectargs: i16,
    pub aggtransfn: Oid,
    pub aggfinalfn: Oid,
    pub aggcombinefn: Oid,
    pub aggserialfn: Oid,
    pub aggdeserialfn: Oid,
    pub aggfinalextra: bool,
    pub aggfinalmodify: i8,
    pub aggtranstype: Oid,
    pub aggtransspace: i32,
}

seam_core::seam!(
    // SearchSysCache1(AGGFNOID, aggfnoid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_aggregate_shape(aggfnoid: Oid) -> PgResult<Option<PgAggregateShape>>
);

seam_core::seam!(
    // SysCacheGetAttr(AGGFNOID tuple, agginitval) as text; outer None mirrors
    // !HeapTupleIsValid, inner None mirrors attisnull.
    pub fn pg_aggregate_agginitval<'mcx>(
        mcx: Mcx<'mcx>,
        aggfnoid: Oid,
    ) -> PgResult<Option<Option<PgString<'mcx>>>>
);

// One SearchSysCacheList1(PROCNAMEARGSNSP, proname) member, projected to
// FuncnameGetCandidates' reads (proargtypes.values only — the variadic/
// defaults expansions read the fields and panic upstream).
#[derive(Debug)]
pub struct PgProcCandidate<'mcx> {
    pub oid: Oid,
    pub pronamespace: Oid,
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub provariadic: Oid,
    pub proargtypes: PgVec<'mcx, Oid>,
}

seam_core::seam!(
    // SearchSysCacheList1(PROCNAMEARGSNSP, proname), catcache list order.
    pub fn lookup_pg_proc_name_candidates<'mcx>(
        mcx: Mcx<'mcx>,
        proname: &str,
    ) -> PgResult<PgVec<'mcx, PgProcCandidate<'mcx>>>
);
