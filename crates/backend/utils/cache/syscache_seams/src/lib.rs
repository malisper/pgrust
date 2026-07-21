use types_core::Oid;
use types_error::PgResult;
use types_storage::PgClassShape;
use types_tuple::{HeapTupleData, PgTypeShape};

seam_core::seam!(
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // InitCatalogCachePhase2 (syscache.c): opens every syscache's catalog +
    // index so first-plan buffer counts match C (relcache.c needNewCacheFile).
    pub fn init_catalog_cache_phase2() -> PgResult<()>
);

seam_core::seam!(
    // SearchSysCacheExists1(PROCOID, funcid).
    pub fn search_syscache_exists_procoid(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // SearchSysCacheExists2(ATTNUM, relid, attnum).
    pub fn search_syscache_exists_attnum(relid: Oid, attnum: i16) -> PgResult<bool>
);

seam_core::seam!(
    // SearchSysCacheExists1(DATABASEOID, dboid).
    pub fn search_syscache_exists_databaseoid(dboid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // SearchSysCacheExists1(TABLESPACEOID, tblspcoid).
    pub fn search_syscache_exists_tablespaceoid(tblspcoid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    pub fn sys_cache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()>
);

seam_core::seam!(
    // RelationInvalidatesSnapshotsOnly (syscache.c).
    pub fn relation_invalidates_snapshots_only(relid: Oid) -> bool
);

seam_core::seam!(
    // SearchSysCache1(RELOID, relid) projected to PgClassShape;
    // None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_class_by_relid(relid: Oid) -> PgResult<Option<PgClassShape>>
);

seam_core::seam!(
    // GETSTRUCT(tuple) as Form_pg_class, projected to PgClassShape.
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

#[derive(Clone, Copy, Debug)]
pub struct PgSequenceForm {
    pub seqtypid: Oid,
    pub seqstart: i64,
    pub seqincrement: i64,
    pub seqmax: i64,
    pub seqmin: i64,
    pub seqcache: i64,
    pub seqcycle: bool,
}

seam_core::seam!(
    // SearchSysCache1(SEQRELID, relid) decoded whole (nextval reads 5 of 8
    // fields per miss); None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_sequence_form(relid: Oid) -> PgResult<Option<PgSequenceForm>>
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
    // lookup_authid_rolname, NameData-by-value shape (no allocation; the
    // executor's CURRENT_USER family reads it per eval).
    pub fn lookup_authid_rolname_data(roleid: Oid) -> PgResult<Option<types_tuple::NameData>>
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

#[derive(Debug)]
pub struct AuthIdPasswordShape<'mcx> {
    pub rolpassword: Option<mcx::PgString<'mcx>>,
    pub rolvaliduntil: Option<types_core::TimestampTz>,
}

seam_core::seam!(
    // SearchSysCache1(AUTHNAME, role) projected to the two nullable columns
    // get_role_password reads; None mirrors !HeapTupleIsValid.
    pub fn lookup_authid_rolpassword<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rolname: &str,
    ) -> PgResult<Option<AuthIdPasswordShape<'mcx>>>
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
pub struct PgAmprocMemberShape {
    pub amprocrighttype: Oid,
    pub amprocnum: i16,
    pub amproc: Oid,
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
    pub collnamespace: Oid,
    pub collisdeterministic: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgConstraintShape {
    pub conname: NameData,
    pub contype: i8,
    pub conindid: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgConstraintDescShape {
    pub conname: NameData,
    pub connamespace: Oid,
    pub conrelid: Oid,
    pub contypid: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOpclassShape {
    pub opcmethod: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
    pub opckeytype: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOpfamilyShape {
    pub opfmethod: Oid,
    pub opfname: NameData,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOperatorShape {
    pub oprnamespace: Oid,
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
    pub prolang: Oid,
    pub pronargs: i16,
    pub prokind: i8,
    pub provolatile: i8,
    pub proparallel: i8,
    pub proretset: bool,
    pub proisstrict: bool,
    pub proleakproof: bool,
    pub prosecdef: bool,
    pub proconfig_isnull: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgProcFmgrShape {
    pub prolang: Oid,
    pub prorettype: Oid,
    pub pronargs: i16,
    pub proisstrict: bool,
    pub proretset: bool,
    pub prosecdef: bool,
    pub proconfig_isnull: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgProcSecdefShape {
    pub proowner: Oid,
    pub prosecdef: bool,
    // "name=value" proconfig entries; None = NULL proconfig.
    pub proconfig: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgLanguageFmgrShape {
    pub lanplcallfoid: Oid,
    pub laninline: Oid,
    pub lanvalidator: Oid,
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
    pub relhassubclass: bool,
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
    pub rngmultitypid: Oid,
    pub rngcollation: Oid,
    pub rngsubopc: Oid,
    pub rngcanonical: Oid,
    pub rngsubdiff: Oid,
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
    pub fn lookup_pg_amproc_members<'mcx>(
        mcx: Mcx<'mcx>,
        opfamily: Oid,
        lefttype: Oid,
    ) -> PgResult<PgVec<'mcx, PgAmprocMemberShape>>
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOperatorNameCandidate {
    pub oid: Oid,
    pub oprnamespace: Oid,
    pub oprkind: i8,
    pub oprleft: Oid,
    pub oprright: Oid,
}

seam_core::seam!(
    // SearchSysCacheList1(OPERNAMENSP, oprname), catcache list order.
    pub fn lookup_pg_operator_name_candidates<'mcx>(
        mcx: Mcx<'mcx>,
        opername: &str,
    ) -> PgResult<PgVec<'mcx, PgOperatorNameCandidate>>
);

seam_core::seam!(
    pub fn lookup_pg_collation_shape(colloid: Oid) -> PgResult<Option<PgCollationShape>>
);

seam_core::seam!(
    // SearchSysCache1(OPEROID, opno) projected to (oprname, oprnamespace);
    // None mirrors !HeapTupleIsValid.
    pub fn pg_operator_oprnamensp(opno: Oid) -> PgResult<Option<(NameData, Oid)>>
);

// The (name, namespace) pair regconfigout/regdictionaryout read off one
// TSCONFIGOID/TSDICTOID probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTsObjectRow {
    pub name: NameData,
    pub namespace_oid: Oid,
}

seam_core::seam!(
    // GetSysCacheOid2(TSCONFIGNAMENSP, cfgname, cfgnamespace); InvalidOid on a miss.
    pub fn lookup_pg_ts_config_oid_by_name_nsp(cfgname: &str, cfgnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // SearchSysCache1(TSCONFIGOID, cfgid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_ts_config_row(cfgid: Oid) -> PgResult<Option<PgTsObjectRow>>
);

seam_core::seam!(
    // GetSysCacheOid2(TSDICTNAMENSP, dictname, dictnamespace); InvalidOid on a miss.
    pub fn lookup_pg_ts_dict_oid_by_name_nsp(dictname: &str, dictnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // GetSysCacheOid2(CONNAMENSP, conname, connamespace); InvalidOid on a miss.
    pub fn lookup_pg_conversion_oid_by_name_nsp(conname: &str, connamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // GetSysCacheOid2(STATEXTNAMENSP, stxname, stxnamespace); InvalidOid on a miss.
    pub fn lookup_pg_statistic_ext_oid_by_name_nsp(stxname: &str, stxnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // SearchSysCache1(TSDICTOID, dictid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_ts_dict_row(dictid: Oid) -> PgResult<Option<PgTsObjectRow>>
);


seam_core::seam!(
    pub fn lookup_pg_constraint_shape(conoid: Oid) -> PgResult<Option<PgConstraintShape>>
);

seam_core::seam!(
    pub fn lookup_pg_constraint_desc_shape(conoid: Oid) -> PgResult<Option<PgConstraintDescShape>>
);

seam_core::seam!(
    // SearchSysCache1(TYPEOID) -> (typname, typnamespace).
    pub fn pg_type_name_namespace(typid: Oid) -> PgResult<Option<(NameData, Oid)>>
);

seam_core::seam!(
    pub fn lookup_pg_language_name(langoid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_opclass_shape(opclass: Oid) -> PgResult<Option<PgOpclassShape>>
);

// SearchSysCache1(CLAOID) projected to (opcname, opcnamespace, opcmethod);
// parse_utilcmd get_opclass's qualified-name emission.
seam_core::seam!(
    pub fn pg_opclass_name_namespace_method(
        opclass: Oid,
    ) -> PgResult<Option<(types_tuple::NameData, Oid, Oid)>>
);

// GetSysCacheOid3(CLAAMNAMENSP): pg_opclass by (opcmethod, opcname,
// opcnamespace); InvalidOid when absent.
seam_core::seam!(
    pub fn lookup_pg_opclass_oid_by_name(amid: Oid, opcname: &str, opcnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_opfamily_shape(opfid: Oid) -> PgResult<Option<PgOpfamilyShape>>
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAmopRow {
    pub amopfamily: Oid,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
    pub amopstrategy: i16,
    pub amoppurpose: i8,
    pub amopopr: Oid,
    pub amopsortfamily: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgAmprocRow {
    pub amprocfamily: Oid,
    pub amproclefttype: Oid,
    pub amprocrighttype: Oid,
    pub amprocnum: i16,
    pub amproc: Oid,
}

seam_core::seam!(
    // SearchSysCacheList1(AMOPSTRATEGY, opfamily): (rows, list.ordered).
    pub fn lookup_pg_amop_rows<'mcx>(
        mcx: Mcx<'mcx>,
        opfamily: Oid,
    ) -> PgResult<(PgVec<'mcx, PgAmopRow>, bool)>
);

seam_core::seam!(
    // SearchSysCacheList1(AMPROCNUM, opfamily): (rows, list.ordered).
    pub fn lookup_pg_amproc_rows<'mcx>(
        mcx: Mcx<'mcx>,
        opfamily: Oid,
    ) -> PgResult<(PgVec<'mcx, PgAmprocRow>, bool)>
);

seam_core::seam!(
    // SearchSysCacheList1(CLAAMNAMENSP, amoid): (oid, opcfamily, opcintype,
    // opcdefault, opcname) per opclass of the AM, catcache list order.
    pub fn lookup_pg_opclass_rows_by_am<'mcx>(
        mcx: Mcx<'mcx>,
        amoid: Oid,
    ) -> PgResult<PgVec<'mcx, (Oid, Oid, Oid, bool, NameData)>>
);

seam_core::seam!(
    pub fn pg_opclass_opcname(opclass: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn lookup_pg_opfamily_oid_exact(amoid: Oid, opfname: &str, nsp: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_operator_shape(opno: Oid) -> PgResult<Option<PgOperatorShape>>
);

seam_core::seam!(
    pub fn pg_operator_oprname(opno: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    // fmgr_info's non-builtin leg: the pg_proc fields FmgrInfo needs.
    pub fn lookup_pg_proc_fmgr(funcid: Oid) -> PgResult<Option<PgProcFmgrShape>>
);

seam_core::seam!(
    // fmgr_security_definer's pg_proc fetch (fmgr.c:646-682).
    pub fn lookup_pg_proc_secdef(funcid: Oid) -> PgResult<Option<PgProcSecdefShape>>
);

seam_core::seam!(
    // fmgr_info_other_lang's pg_language read (fmgr.c).
    pub fn lookup_pg_language_fmgr(langoid: Oid) -> PgResult<Option<PgLanguageFmgrShape>>
);

seam_core::seam!(
    pub fn lookup_pg_proc_prosrc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    pub fn lookup_pg_proc_probin<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
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
    // GetSysCacheOid2(TYPENAMENSP): InvalidOid on miss.
    pub fn lookup_pg_type_oid_by_name(typname: &str, typnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_class_ls_shape(relid: Oid) -> PgResult<Option<PgClassLsShape>>
);

seam_core::seam!(
    pub fn pg_class_relname(relid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    pub fn pg_class_reloftype(relid: Oid) -> PgResult<Option<Oid>>
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
    pub fn pg_type_typnamespace(typid: Oid) -> PgResult<Option<Oid>>
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
    // SysCacheGetAttr(PROCOID, proargdefaults): the nodeToString'd default
    // expression list; inner None = attisnull, outer None = no such proc.
    pub fn pg_proc_proargdefaults<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<Option<PgString<'mcx>>>>
);

seam_core::seam!(
    // get_primary_key_attnos (pg_constraint.c) projected for parse-time
    // callers (check_functional_grouping): parser rigs have no storage, so
    // the systable scan cannot run below them — same projection rationale as
    // pg_proc_proargdefaults. None = no (usable) primary key; the deferrable
    // bail lives in the real impl (pg_constraint::get_primary_key_attnos).
    pub fn pg_constraint_primary_key_attnos<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        deferrable_ok: bool,
    ) -> PgResult<Option<(PgVec<'mcx, i16>, Oid)>>
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
    // SysCacheGetAttrNotNull(INDEXRELID tuple, indoption).values[index];
    // None mirrors !HeapTupleIsValid. PRECONDITION: index < indoption->dim1.
    pub fn pg_index_indoption_element(index_oid: Oid, index: i32) -> PgResult<Option<i16>>
);

seam_core::seam!(
    // pg_am.amtype by AMOID; None mirrors !HeapTupleIsValid.
    pub fn pg_am_amtype(amoid: Oid) -> PgResult<Option<i8>>
);

seam_core::seam!(
    // pg_am.amhandler by AMOID (RelationInitTableAccessMethod's lookup);
    // None mirrors !HeapTupleIsValid.
    pub fn pg_am_amhandler(amoid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    // pg_am.amname by AMOID (pgrcolumnar registration probe); None mirrors
    // !HeapTupleIsValid.
    pub fn pg_am_amname(amoid: Oid) -> PgResult<Option<String>>
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
    pub prorows: f32,
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

// One pg_statistic slot. Array images are fetched on first access via
// lookup_pg_statistic_slot_images and decoded on first values()/numbers()
// (C's get_attstatsslot laziness: the unique-column eq path never touches
// the arrays at all). Byref `values` datums point into the fetched
// values_image, whose heap buffer is stable across moves of this struct.
pub struct PgStatisticSlotData<'mcx> {
    pub kind: i16,
    pub staop: Oid,
    pub stacoll: Oid,
    mcx: Mcx<'mcx>,
    key: (Oid, AttrNumber, bool, i32),
    images: core::cell::OnceCell<PgStatisticSlotImages<'mcx>>,
    values: core::cell::OnceCell<PgVec<'mcx, Datum>>,
    numbers: core::cell::OnceCell<PgVec<'mcx, f32>>,
}

pub struct PgStatisticSlotImages<'mcx> {
    pub valuetype: Oid,
    // Empty images mirror SQL NULL (decode to empty slices).
    pub values_image: PgVec<'mcx, u8>,
    pub numbers_image: PgVec<'mcx, u8>,
}

impl<'mcx> PgStatisticSlotData<'mcx> {
    pub fn lazy(
        kind: i16,
        staop: Oid,
        stacoll: Oid,
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: AttrNumber,
        inh: bool,
        pos: i32,
    ) -> Self {
        PgStatisticSlotData {
            kind,
            staop,
            stacoll,
            mcx,
            key: (relid, attnum, inh, pos),
            images: core::cell::OnceCell::new(),
            values: core::cell::OnceCell::new(),
            numbers: core::cell::OnceCell::new(),
        }
    }

    pub fn from_decoded(
        kind: i16,
        staop: Oid,
        stacoll: Oid,
        valuetype: Oid,
        values: PgVec<'mcx, Datum>,
        numbers: PgVec<'mcx, f32>,
        values_image: PgVec<'mcx, u8>,
    ) -> Self {
        let mcx = *values.allocator();
        let s = PgStatisticSlotData::lazy(kind, staop, stacoll, mcx, 0, 0, false, 0);
        let _ = s.images.set(PgStatisticSlotImages {
            valuetype,
            values_image,
            numbers_image: PgVec::new_in(mcx),
        });
        let _ = s.values.set(values);
        let _ = s.numbers.set(numbers);
        s
    }

    fn images(&self) -> PgResult<&PgStatisticSlotImages<'mcx>> {
        if let Some(i) = self.images.get() {
            return Ok(i);
        }
        let (relid, attnum, inh, pos) = self.key;
        let img = lookup_pg_statistic_slot_images::call(self.mcx, relid, attnum, inh, pos)?;
        Ok(self.images.get_or_init(|| img))
    }

    /// By-ref stavalues Datums borrow this bundle's values_image: reading a
    /// copied-out Datum past the bundle's life is the detoast drop-order UAF.
    pub fn values(&self) -> PgResult<&[Datum]> {
        if let Some(v) = self.values.get() {
            return Ok(v);
        }
        let img = self.images()?;
        let v = decode_pg_statistic_values::call(self.mcx, img.valuetype, &img.values_image)?;
        Ok(self.values.get_or_init(|| v))
    }

    pub fn numbers(&self) -> PgResult<&[f32]> {
        if let Some(n) = self.numbers.get() {
            return Ok(n);
        }
        let img = self.images()?;
        let n = decode_pg_statistic_numbers::call(self.mcx, &img.numbers_image)?;
        Ok(self.numbers.get_or_init(|| n))
    }

    pub fn values_image(&self) -> PgResult<&[u8]> {
        Ok(&self.images()?.values_image)
    }

    pub fn valuetype(&self) -> PgResult<Oid> {
        Ok(self.images()?.valuetype)
    }
}

seam_core::seam!(
    // get_attstatsslot's deferred array fetch: re-probe STATRELATTINH and
    // extract one slot's stavalues/stanumbers images. The row existed at
    // bundle probe time; a vanished row is loud (C reads its still-pinned
    // original tuple).
    pub fn lookup_pg_statistic_slot_images<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: AttrNumber,
        inh: bool,
        pos: i32,
    ) -> PgResult<PgStatisticSlotImages<'mcx>>
);

seam_core::seam!(
    // stavalues image decode (get_attstatsslot's deconstruct_array arm);
    // empty image mirrors SQL NULL. Byref datums point into `image`.
    pub fn decode_pg_statistic_values<'mcx>(
        mcx: Mcx<'mcx>,
        valuetype: Oid,
        image: &[u8],
    ) -> PgResult<PgVec<'mcx, Datum>>
);

seam_core::seam!(
    // stanumbers (float4[]) image decode; empty image mirrors SQL NULL.
    pub fn decode_pg_statistic_numbers<'mcx>(
        mcx: Mcx<'mcx>,
        image: &[u8],
    ) -> PgResult<PgVec<'mcx, f32>>
);

pub struct PgStatisticBundle<'mcx> {
    pub stanullfrac: f32,
    pub stawidth: i32,
    pub stadistinct: f32,
    pub slots: PgVec<'mcx, PgStatisticSlotData<'mcx>>,
}

// The planner's per-cycle attribute-stats memo (replanfix2 T1) arena-leaks
// bundles via mcx::forget_box_in: drops reclaim only arena bytes (PgVecs and
// OnceCell contents are all mcx-backed), so forgetting them loses nothing
// beyond the arena the planning cycle resets anyway.
mcx::forget_safe_struct!(
    PgStatisticSlotImages<'_> { valuetype, values_image, numbers_image },
    PgStatisticSlotData<'_> { kind, staop, stacoll, mcx, key, images, values, numbers },
    PgStatisticBundle<'_> { stanullfrac, stawidth, stadistinct, slots },
);

// The planner's per-cycle syscache memos (syscache-memo lane) arena-leak
// these Copy PODs the same way; !needs_drop is the whole proof.
mcx::forget_safe_nodrop!(PgOperatorShape, PgAmopShape, PgAmopMemberShape);

seam_core::seam!(
    // SearchSysCache3(STATRELATTINH) + full slot decode (the planner's
    // examine_variable keeps the C statsTuple pinned; here the decode-once
    // bundle replaces the tuple + repeated get_attstatsslot calls).
    pub fn lookup_pg_statistic_bundle<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: AttrNumber,
        inh: bool,
    ) -> PgResult<Option<PgStatisticBundle<'mcx>>>
);

seam_core::seam!(
    // SysCacheGetAttr(ATTNUM, ..., Anum_pg_attribute_attstattarget); Ok(None)
    // mirrors SQL NULL, a missing pg_attribute row is an error.
    pub fn lookup_pg_attribute_stattarget(relid: Oid, attnum: AttrNumber) -> PgResult<Option<i16>>
);

seam_core::seam!(
    // pg_type.typanalyze regproc for examine_attribute; InvalidOid = none.
    pub fn pg_type_typanalyze(typid: Oid) -> PgResult<Oid>
);

pub struct StatExtForm<'mcx> {
    pub stxrelid: Oid,
    pub keys: PgVec<'mcx, i16>,
    pub kinds: PgVec<'mcx, u8>,
    pub stattarget: i32,
    pub has_exprs: bool,
}

seam_core::seam!(
    // SearchSysCache1(STATEXTOID) projected to the planner's reads.
    pub fn statext_form<'mcx>(mcx: Mcx<'mcx>, statoid: Oid) -> PgResult<Option<StatExtForm<'mcx>>>
);

seam_core::seam!(
    // SearchSysCache2(STATEXTDATASTXOID) per-kind non-null flags
    // (ndistinct, dependencies, mcv, expressions); None = no data row.
    pub fn statext_data_kinds(statoid: Oid, inh: bool) -> PgResult<Option<(bool, bool, bool, bool)>>
);

seam_core::seam!(
    // SysCacheGetAttr(STATEXTDATASTXOID, anum) detoasted varlena image
    // (4-byte header included); None = NULL or no data row.
    pub fn statext_data_blob<'mcx>(
        mcx: Mcx<'mcx>,
        statoid: Oid,
        inh: bool,
        anum: i32,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    // SysCacheGetAttr(STATEXTOID, stxexprs) as nodeToString text; None = NULL.
    pub fn statext_exprs_src<'mcx>(
        mcx: Mcx<'mcx>,
        statoid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    // statext_expressions_load (extended_stats.c): stxdexpr pg_statistic[]
    // element `idx` decoded to a bundle. Slots decode eagerly — the lazy
    // refetch key targets pg_statistic rows, which these are not.
    pub fn statext_expressions_load<'mcx>(
        mcx: Mcx<'mcx>,
        statoid: Oid,
        inh: bool,
        idx: i32,
    ) -> PgResult<PgStatisticBundle<'mcx>>
);

seam_core::seam!(
    pub fn pg_namespace_nspname(nspid: Oid) -> PgResult<Option<NameData>>
);

seam_core::seam!(
    // RelationHasSysCache (syscache.c).
    pub fn relation_has_sys_cache(relid: Oid) -> bool
);

seam_core::seam!(
    // RelationSupportsSysCache (syscache.c): rel OR supporting-index oid.
    pub fn relation_supports_sys_cache(relid: Oid) -> bool
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

// The pg_type columns typcache's domain-constraint crawl reads per stack level.
#[derive(Clone, Copy, Debug)]
pub struct PgTypeDomainShape {
    pub typname: NameData,
    pub typnamespace: Oid,
    pub typtype: i8,
    pub typnotnull: bool,
    pub typbasetype: Oid,
}

seam_core::seam!(
    pub fn pg_type_domain_shape(typid: Oid) -> PgResult<Option<PgTypeDomainShape>>
);

seam_core::seam!(
    // GetSysCacheHashValue1(TYPEOID, typid).
    pub fn syscache_hash_value_typeoid(typid: Oid) -> PgResult<u32>
);

seam_core::seam!(
    // GetSysCacheHashValue1(PROCOID, funcid).
    pub fn syscache_hash_value_procoid(funcid: Oid) -> PgResult<u32>
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
    pub collencoding: i32,
    pub collcollate: Option<PgString<'mcx>>,
    pub collctype: Option<PgString<'mcx>>,
    pub colllocale: Option<PgString<'mcx>>,
    pub collicurules: Option<PgString<'mcx>>,
    pub collversion: Option<PgString<'mcx>>,
}

seam_core::seam!(
    // SearchSysCache1(COLLOID, collid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_collation_locale_row<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<Option<PgCollationLocaleRow<'mcx>>>
);

// The pg_collation columns namespace.c's lookup_collation reads off one
// COLLNAMEENCNSP probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgCollationNameEncNspRow {
    pub oid: Oid,
    pub collprovider: u8,
}

seam_core::seam!(
    // SearchSysCache3(COLLNAMEENCNSP, collname, encoding, collnamespace);
    // None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_collation_by_name_enc_nsp(
        collname: &str,
        encoding: i32,
        collnamespace: Oid,
    ) -> PgResult<Option<PgCollationNameEncNspRow>>
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
    pub aggmtransfn: Oid,
    pub aggminvtransfn: Oid,
    pub aggmfinalfn: Oid,
    pub aggfinalextra: bool,
    pub aggmfinalextra: bool,
    pub aggfinalmodify: i8,
    pub aggmfinalmodify: i8,
    pub aggsortop: Oid,
    pub aggtranstype: Oid,
    pub aggtransspace: i32,
    pub aggmtranstype: Oid,
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

seam_core::seam!(
    // SysCacheGetAttr(AGGFNOID tuple, aggminitval) as text; same contract as
    // pg_aggregate_agginitval.
    pub fn pg_aggregate_aggminitval<'mcx>(
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

// Form_pg_enum + the tuple-header xmin facts check_safe_enum_use reads.
#[derive(Clone, Copy)]
pub struct PgEnumShape {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumlabel: NameData,
    pub xmin: types_core::TransactionId,
    pub xmin_committed: bool,
}

seam_core::seam!(
    // SearchSysCache1(ENUMOID, enum_oid); None mirrors !HeapTupleIsValid.
    pub fn lookup_pg_enum_by_oid(enum_oid: Oid) -> PgResult<Option<PgEnumShape>>
);

seam_core::seam!(
    // SearchSysCache2(ENUMTYPOIDNAME, typid, label).
    pub fn lookup_pg_enum_by_typid_label(typid: Oid, label: &str) -> PgResult<Option<PgEnumShape>>
);

#[derive(Clone, Copy)]
pub struct PgTsParserShape {
    pub prsstart: Oid,
    pub prstoken: Oid,
    pub prsend: Oid,
    pub prsheadline: Oid,
    pub prslextype: Oid,
}

seam_core::seam!(
    pub fn lookup_pg_ts_parser_shape(prsid: Oid) -> PgResult<Option<PgTsParserShape>>
);

pub struct PgTsDictShape<'mcx> {
    pub dictname: NameData,
    pub dictnamespace: Oid,
    pub dicttemplate: Oid,
    // dictinitoption text payload (detoasted, header stripped); None = SQL NULL.
    pub dictinitoption: Option<mcx::PgVec<'mcx, u8>>,
}

seam_core::seam!(
    pub fn lookup_pg_ts_dict_shape<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dictid: Oid,
    ) -> PgResult<Option<PgTsDictShape<'mcx>>>
);

#[derive(Clone, Copy)]
pub struct PgTsTemplateShape {
    pub tmplinit: Oid,
    pub tmpllexize: Oid,
}

seam_core::seam!(
    pub fn lookup_pg_ts_template_shape(tmplid: Oid) -> PgResult<Option<PgTsTemplateShape>>
);

#[derive(Clone, Copy)]
pub struct PgTsConfigShape {
    pub cfgparser: Oid,
    pub cfgnamespace: Oid,
    pub cfgname: NameData,
}

seam_core::seam!(
    pub fn lookup_pg_ts_config_shape(cfgid: Oid) -> PgResult<Option<PgTsConfigShape>>
);

seam_core::seam!(
    // GetSysCacheOid2(TSCONFIGNAMENSP); InvalidOid on a miss.
    pub fn lookup_pg_ts_config_oid_by_name(cfgname: &str, cfgnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn lookup_pg_ts_dict_oid_by_name(dictname: &str, dictnamespace: Oid) -> PgResult<Oid>
);

#[derive(Clone, Copy)]
pub struct PgTsConfigMapShape {
    pub maptokentype: i32,
    pub mapseqno: i32,
    pub mapdict: Oid,
}

seam_core::seam!(
    // All pg_ts_config_map rows for cfgid, sorted (maptokentype, mapseqno) —
    // the TSConfigMapIndexId scan order ts_cache relies on.
    pub fn pg_ts_config_map_shapes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        cfgid: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, PgTsConfigMapShape>>
);
