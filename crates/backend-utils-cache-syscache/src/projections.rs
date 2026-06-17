//! Implementations of the caller-shaped projected-row seams declared in
//! `backend-utils-cache-syscache-seams`: catcache lookup + attribute
//! extraction + field projection, copied into the caller's `mcx`.

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgString, PgVec};
use types_cache::SysCacheKey;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
// `types_datum::Datum` (the bare-word shim) survives only at the unmigrated
// cross-crate contract edge `SysCacheKey::Value`'s search-key word (C:
// `Datum key1..key4`), audited `types-cache` vocabulary not in this batch.
use types_datum::Datum as KeyDatum;

use crate::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SearchSysCache2, SearchSysCache3,
    SearchSysCacheAttName,
    SearchSysCacheExists, SearchSysCacheList, SearchSysCacheList1, SysCacheGetAttr,
    SysCacheGetAttrNotNull, AGGFNOID, AMOID, AMOPSTRATEGY, AMPROCNUM, ATTNAME, ATTNUM, AUTHNAME, AUTHOID,
    CASTSOURCETARGET, CLAAMNAMENSP, CLAOID, COLLOID, CONSTROID, DATABASEOID, ENUMOID, ENUMTYPOIDNAME,
    FOREIGNDATAWRAPPERNAME,
    FOREIGNDATAWRAPPEROID, FOREIGNSERVERNAME, FOREIGNSERVEROID, FOREIGNTABLEREL, INDEXRELID, LANGNAME,
    LANGOID, NAMESPACEOID, OPEROID, PARAMETERACLNAME, PARAMETERACLOID, PROCOID, RELOID,
    RULERELNAME, STATRELATTINH, TYPEOID,
    USERMAPPINGOID, USERMAPPINGUSERSERVER,
};
use types_statistics::{
    Anum_pg_statistic_stacoll1, Anum_pg_statistic_stadistinct, Anum_pg_statistic_stakind1,
    Anum_pg_statistic_stanullfrac, Anum_pg_statistic_staop1,
};
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use types_core::AttrNumber;
use types_fmgr::{LangInfo, ProcInfo, ProcLanguage, ProcResultInfo};
use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seams;
use backend_utils_adt_amutils_seams as amutils_seams;
use backend_utils_misc_guc_seams as guc_seams;
use backend_utils_error::ereport;
use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, WARNING};
use types_core::primitive::OidIsValid;
use types_tuple::heaptuple::{HeapTupleHeaderGetRawXmin, HeapTupleHeaderGetXmin,
    HeapTupleHeaderXminCommitted};
use types_catalog::pg_enum::{Anum_pg_enum_enumlabel, Anum_pg_enum_enumtypid, Anum_pg_enum_oid,
    EnumTupleData};
use backend_utils_cache_syscache_seams::{PgClassFullForm, PgOperatorForm, PgProcForm};
use types_cache::AuthIdRow;
use types_tuple::backend_access_common_tupdesc::PgTypeInfo;
use backend_utils_cache_syscache_seams::CastRow;
use types_cache::syscache::{ForeignDataWrapperFormRow, ForeignServerFormRow};
use backend_nodes_read_seams as nodes_read_seams;
use backend_utils_adt_varlena_seams as varlena_seams;
use types_catalog::pg_aggregate::{
    AggFormData, AggRow, Anum_pg_aggregate_aggcombinefn, Anum_pg_aggregate_aggdeserialfn,
    Anum_pg_aggregate_aggfinalextra, Anum_pg_aggregate_aggfinalfn, Anum_pg_aggregate_aggfinalmodify,
    Anum_pg_aggregate_aggfnoid, Anum_pg_aggregate_agginitval,
    Anum_pg_aggregate_aggmfinalextra, Anum_pg_aggregate_aggmfinalfn, Anum_pg_aggregate_aggmfinalmodify,
    Anum_pg_aggregate_aggminitval, Anum_pg_aggregate_aggminvtransfn, Anum_pg_aggregate_aggmtransfn,
    Anum_pg_aggregate_aggmtransspace, Anum_pg_aggregate_aggmtranstype,
    Anum_pg_aggregate_aggserialfn, Anum_pg_aggregate_aggsortop,
    Anum_pg_aggregate_aggtransfn, Anum_pg_aggregate_aggtransspace, Anum_pg_aggregate_aggtranstype,
};
use types_catalog::pg_language::FormData_pg_language;
use types_nodes::nodes::{Node, NodePtr};

/// `Anum_pg_class_relam` (`catalog/pg_class.h`).
const Anum_pg_class_relam: i32 = 7;
/// `Anum_pg_class_reloftype` (`catalog/pg_class.h`).
const Anum_pg_class_reloftype: i32 = 5;

// `catalog/pg_rewrite.h` attribute numbers.
const Anum_pg_rewrite_oid: i32 = 1;
const Anum_pg_rewrite_ev_class: i32 = 3;

// `catalog/pg_cast.h` attribute numbers.
const Anum_pg_cast_oid: i32 = 1;
const Anum_pg_cast_castfunc: i32 = 4;
const Anum_pg_cast_castcontext: i32 = 5;
const Anum_pg_cast_castmethod: i32 = 6;

// `catalog/pg_opclass.h` attribute numbers.
const Anum_pg_opclass_opcname: i32 = 3;
const Anum_pg_opclass_opcfamily: i32 = 6;
const Anum_pg_opclass_opcintype: i32 = 7;
const Anum_pg_opclass_opckeytype: i32 = 9;

// `catalog/pg_operator.h` attribute numbers.
const Anum_pg_operator_oprname: i32 = 2;
const Anum_pg_operator_oprkind: i32 = 5;
const Anum_pg_operator_oprcanmerge: i32 = 6;
const Anum_pg_operator_oprcanhash: i32 = 7;
const Anum_pg_operator_oprleft: i32 = 8;
const Anum_pg_operator_oprright: i32 = 9;
const Anum_pg_operator_oprresult: i32 = 10;
const Anum_pg_operator_oprcom: i32 = 11;
const Anum_pg_operator_oprnegate: i32 = 12;
const Anum_pg_operator_oprcode: i32 = 13;
const Anum_pg_operator_oprrest: i32 = 14;
const Anum_pg_operator_oprjoin: i32 = 15;

// `catalog/pg_amop.h` attribute numbers.
const Anum_pg_amop_amoplefttype: i32 = 3;
const Anum_pg_amop_amoprighttype: i32 = 4;
const Anum_pg_amop_amopstrategy: i32 = 5;
const Anum_pg_amop_amoppurpose: i32 = 6;
const Anum_pg_amop_amopopr: i32 = 7;
const Anum_pg_amop_amopsortfamily: i32 = 9;

// `catalog/pg_proc.h` attribute numbers.
const Anum_pg_proc_proargdefaults: i32 = 24;

// `catalog/pg_aggregate.h` attribute numbers.
const Anum_pg_aggregate_aggkind: i32 = 2;
const Anum_pg_aggregate_aggnumdirectargs: i32 = 3;

// `catalog/pg_foreign_*.h` `*options` `text[]` attribute numbers.
const Anum_pg_foreign_data_wrapper_fdwoptions: i32 = 7;
const Anum_pg_foreign_server_srvoptions: i32 = 8;
const Anum_pg_user_mapping_umoptions: i32 = 4;

// `catalog/pg_foreign_data_wrapper.h` scalar attribute numbers.
const Anum_pg_foreign_data_wrapper_oid: i32 = 1;
const Anum_pg_foreign_data_wrapper_fdwname: i32 = 2;
const Anum_pg_foreign_data_wrapper_fdwowner: i32 = 3;
const Anum_pg_foreign_data_wrapper_fdwhandler: i32 = 4;
const Anum_pg_foreign_data_wrapper_fdwvalidator: i32 = 5;

// `catalog/pg_foreign_server.h` scalar attribute numbers.
const Anum_pg_foreign_server_oid: i32 = 1;
const Anum_pg_foreign_server_srvname: i32 = 2;
const Anum_pg_foreign_server_srvowner: i32 = 3;
const Anum_pg_foreign_server_srvfdw: i32 = 4;

// `catalog/pg_foreign_table.h` attribute numbers.
const Anum_pg_foreign_table_ftserver: i32 = 2;
const Anum_pg_foreign_table_ftoptions: i32 = 3;

// `catalog/pg_user_mapping.h` attribute numbers.
const Anum_pg_user_mapping_oid: i32 = 1;

// `catalog/pg_attribute.h` attribute number.
const Anum_pg_attribute_attfdwoptions: i32 = 24;

// `catalog/pg_amproc.h` attribute numbers.
const Anum_pg_amproc_amproclefttype: i32 = 3;
const Anum_pg_amproc_amprocrighttype: i32 = 4;
const Anum_pg_amproc_amprocnum: i32 = 5;
const Anum_pg_amproc_amproc: i32 = 6;

fn byval<'mcx>(value: Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    match value {
        Datum::ByVal(_) => Ok(value),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error(
            "syscache projection: expected a by-value attribute",
        )),
    }
}

fn getattr_oid(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<Oid> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_oid())
}

fn getattr_i16(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<i16> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_i16())
}

fn getattr_char(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<i8> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_char())
}

/// A `name` attribute (`NameData` bytes) as an owned string in `mcx`.
fn getattr_name<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<PgString<'mcx>> {
    let value = SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?;
    let bytes = match &value {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error("syscache projection: name attribute is by-value"))
        }
    };
    // NameStr(): the NUL-padded fixed-size buffer up to the first NUL.
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    PgString::from_str_in(&String::from_utf8_lossy(&bytes[..len]), mcx)
}

/// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
/// `Form_pg_class.relam` field. `Ok(None)` on a cache miss
/// (`!HeapTupleIsValid`). The projection is by-value, so the tuple copy
/// lives in a scratch context dropped before returning.
pub(crate) fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("syscache relam projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relam = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relam)?;
    ReleaseSysCache(tup);
    Ok(Some(relam))
}

/// `SearchSysCache1(AMOID, ObjectIdGetDatum(amoid))` + `GETSTRUCT(Form_pg_am)
/// ->amhandler` (amapi.c `GetIndexAmRoutineByAmId` / `GetTableAmRoutineByAmId`):
/// the access method's handler-function OID. `Ok(None)` on a cache miss
/// (`!HeapTupleIsValid`); the caller raises `cache lookup failed for access
/// method %u`. The projection is by-value, so the tuple copy lives in a scratch
/// context dropped before returning.
pub(crate) fn search_am_handler(amoid: Oid) -> PgResult<Option<Oid>> {
    // Anum_pg_am_amhandler (pg_am.h): the `regproc amhandler` column, attno 3.
    const ANUM_PG_AM_AMHANDLER: i32 = 3;
    let scratch = MemoryContext::new("syscache am handler projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, AMOID, SysCacheKey::Value(KeyDatum::from_oid(amoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let amhandler = getattr_oid(mcx, AMOID, &tup, ANUM_PG_AM_AMHANDLER)?;
    ReleaseSysCache(tup);
    Ok(Some(amhandler))
}

/// `SearchSysCache2(RULERELNAME, ObjectIdGetDatum(relid),
/// PointerGetDatum(rulename))` projected to the `Form_pg_rewrite`
/// `(oid, ev_class)` fields (rewriteSupport.c `get_rewrite_oid`). `Ok(None)`
/// on a cache miss (`!HeapTupleIsValid`); the caller keeps the missing_ok /
/// `Assert(relid == ev_class)` logic.
pub(crate) fn search_rewrite_oid(relid: Oid, rulename: &str) -> PgResult<Option<(Oid, Oid)>> {
    let scratch = MemoryContext::new("syscache pg_rewrite oid projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache2(
        mcx,
        crate::RULERELNAME,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Str(rulename),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let ruleoid = getattr_oid(mcx, crate::RULERELNAME, &tup, Anum_pg_rewrite_oid)?;
    let ev_class = getattr_oid(mcx, crate::RULERELNAME, &tup, Anum_pg_rewrite_ev_class)?;
    ReleaseSysCache(tup);
    Ok(Some((ruleoid, ev_class)))
}

/// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
/// `Form_pg_class.reloftype` field (the OF-type for a typed table; `InvalidOid`
/// otherwise). `Ok(None)` on a cache miss (`!HeapTupleIsValid`).
pub(crate) fn search_relation_reloftype(relid: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("syscache reloftype projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let reloftype = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reloftype)?;
    ReleaseSysCache(tup);
    Ok(Some(reloftype))
}

/// `SearchSysCache2(CASTSOURCETARGET, srctype, targettype)` projected to the
/// [`CastRow`] fields (`Form_pg_cast`). `Ok(None)` on a cache miss (no cast).
pub(crate) fn cast_by_source_target(
    sourcetypeid: Oid,
    targettypeid: Oid,
) -> PgResult<Option<CastRow>> {
    let scratch = MemoryContext::new("syscache pg_cast projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache2(
        mcx,
        CASTSOURCETARGET,
        SysCacheKey::Value(KeyDatum::from_oid(sourcetypeid)),
        SysCacheKey::Value(KeyDatum::from_oid(targettypeid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = CastRow {
        oid: getattr_oid(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_oid)?,
        castfunc: getattr_oid(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castfunc)?,
        castcontext: getattr_char(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castcontext)?,
        castmethod: getattr_char(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castmethod)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` projected to the
/// `Form_pg_opclass` fields the hash validator reads. `Ok(None)` on a cache
/// miss (`!HeapTupleIsValid`).
pub(crate) fn search_opclass<'mcx>(
    mcx: Mcx<'mcx>,
    opclassoid: Oid,
) -> PgResult<Option<OpclassForm<'mcx>>> {
    let tuple = SearchSysCache1(mcx, CLAOID, SysCacheKey::Value(KeyDatum::from_oid(opclassoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = OpclassForm {
        opcfamily: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opcfamily)?,
        opcintype: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opcintype)?,
        opckeytype: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opckeytype)?,
        opcname: getattr_name(mcx, CLAOID, &tup, Anum_pg_opclass_opcname)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `SearchSysCache1(OPEROID, ObjectIdGetDatum(opno))` + `GETSTRUCT` of the
/// fixed-width `Form_pg_operator` columns. `Ok(None)` on a cache miss.
pub(crate) fn pg_operator_form<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
) -> PgResult<Option<PgOperatorForm>> {
    let tuple = SearchSysCache1(mcx, OPEROID, SysCacheKey::Value(KeyDatum::from_oid(opno)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let oprname = getattr_name(mcx, OPEROID, &tup, Anum_pg_operator_oprname)?;
    let form = PgOperatorForm {
        oprname: oprname.as_str().to_owned(),
        oprkind: getattr_char(mcx, OPEROID, &tup, Anum_pg_operator_oprkind)?,
        oprcanmerge: getattr_bool(mcx, OPEROID, &tup, Anum_pg_operator_oprcanmerge)?,
        oprcanhash: getattr_bool(mcx, OPEROID, &tup, Anum_pg_operator_oprcanhash)?,
        oprleft: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprleft)?,
        oprright: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprright)?,
        oprresult: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprresult)?,
        oprcom: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprcom)?,
        oprnegate: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprnegate)?,
        oprcode: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprcode)?,
        oprrest: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprrest)?,
        oprjoin: getattr_oid(mcx, OPEROID, &tup, Anum_pg_operator_oprjoin)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))` member
/// rows, projected.
pub(crate) fn search_amop_list<'mcx>(
    mcx: Mcx<'mcx>,
    opfamilyoid: Oid,
) -> PgResult<PgVec<'mcx, AmopRow>> {
    let members = SearchSysCacheList1(
        mcx,
        AMOPSTRATEGY,
        SysCacheKey::Value(KeyDatum::from_oid(opfamilyoid)),
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push(AmopRow {
            amopstrategy: getattr_i16(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopstrategy)?,
            amoppurpose: getattr_char(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoppurpose)?,
            amopopr: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopopr)?,
            amopsortfamily: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopsortfamily)?,
            amoplefttype: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoplefttype)?,
            amoprighttype: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoprighttype)?,
        });
    }
    Ok(rows)
}

/// `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))` member
/// rows, projected.
pub(crate) fn search_amproc_list<'mcx>(
    mcx: Mcx<'mcx>,
    opfamilyoid: Oid,
) -> PgResult<PgVec<'mcx, AmprocRow>> {
    let members = SearchSysCacheList1(
        mcx,
        AMPROCNUM,
        SysCacheKey::Value(KeyDatum::from_oid(opfamilyoid)),
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push(AmprocRow {
            amproclefttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproclefttype)?,
            amprocrighttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocrighttype)?,
            amprocnum: getattr_i16(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocnum)?,
            amproc: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproc)?,
        });
    }
    Ok(rows)
}

/// `SearchSysCacheList2(AMPROCNUM, ObjectIdGetDatum(opfamilyoid),
/// ObjectIdGetDatum(lefttype))` member rows, projected (the partial-key list
/// keyed by opfamily + amproclefttype).
pub(crate) fn search_amproc_list2<'mcx>(
    mcx: Mcx<'mcx>,
    opfamilyoid: Oid,
    lefttype: Oid,
) -> PgResult<PgVec<'mcx, AmprocRow>> {
    let members = SearchSysCacheList(
        mcx,
        AMPROCNUM,
        2,
        SysCacheKey::Value(KeyDatum::from_oid(opfamilyoid)),
        SysCacheKey::Value(KeyDatum::from_oid(lefttype)),
        SysCacheKey::UNUSED,
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push(AmprocRow {
            amproclefttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproclefttype)?,
            amprocrighttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocrighttype)?,
            amprocnum: getattr_i16(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocnum)?,
            amproc: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproc)?,
        });
    }
    Ok(rows)
}

/// `SearchSysCacheList1(CLAAMNAMENSP, ObjectIdGetDatum(amoid))` member rows,
/// projected to `(oid, opcfamily, opcintype)` (`Form_pg_opclass`). Consumed by
/// amvalidate.c's `opclass_for_family_datatype`.
pub(crate) fn search_opclass_list_by_am<'mcx>(
    mcx: Mcx<'mcx>,
    amoid: Oid,
) -> PgResult<PgVec<'mcx, (Oid, Oid, Oid)>> {
    let members = SearchSysCacheList1(
        mcx,
        CLAAMNAMENSP,
        SysCacheKey::Value(KeyDatum::from_oid(amoid)),
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push((
            getattr_oid(mcx, CLAAMNAMENSP, tup, Anum_pg_opclass_oid)?,
            getattr_oid(mcx, CLAAMNAMENSP, tup, Anum_pg_opclass_opcfamily)?,
            getattr_oid(mcx, CLAAMNAMENSP, tup, Anum_pg_opclass_opcintype)?,
        ));
    }
    Ok(rows)
}

/// `func_get_detail`'s default-argument extraction (`parse_func.c`):
///
/// ```c
/// proargdefaults = SysCacheGetAttrNotNull(PROCOID, ftup, Anum_pg_proc_proargdefaults);
/// str = TextDatumGetCString(proargdefaults);
/// defaults = castNode(List, stringToNode(str));
/// ```
///
/// `SearchSysCache1(PROCOID, funcid)` then the `proargdefaults` `pg_node_tree`
/// column projected to its deserialized default-expression list (the elements
/// of the `castNode(List, ...)`), each node allocated in `mcx`. The C call site
/// only reaches this on `ndargs > 0`, where the column is `SysCacheGetAttrNotNull`
/// (non-null) — so a SQL-null column or a cache miss is an `Err` here.
pub(crate) fn proc_argdefaults<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let tuple = SearchSysCache1(mcx, PROCOID, SysCacheKey::Value(KeyDatum::from_oid(funcid)))?;
    let Some(tup) = tuple else {
        return Err(PgError::error(format!(
            "cache lookup failed for function {funcid}"
        )));
    };
    // SysCacheGetAttrNotNull(PROCOID, ftup, Anum_pg_proc_proargdefaults).
    let datum = SysCacheGetAttrNotNull(mcx, PROCOID, &tup, Anum_pg_proc_proargdefaults)?;
    // TextDatumGetCString(proargdefaults).
    let s = varlena_seams::text_to_cstring_v::call(mcx, &datum)?;
    // castNode(List, stringToNode(str)).
    let node = nodes_read_seams::string_to_node::call(mcx, s.as_str())?;
    ReleaseSysCache(tup);
    match mcx::PgBox::into_inner(node) {
        Node::List(elems) => Ok(elems),
        _ => Err(PgError::error(
            "proargdefaults: stringToNode did not yield a List",
        )),
    }
}

/// `SysCacheGetAttr(cacheId, tup, attnum)` for a `text[]` (`*options`) column:
/// `Some(Some(bytes))` with the detoasted varlena, `Some(None)` when the column
/// is SQL NULL. (The caller maps the outer `Option` to "tuple present".)
fn getattr_option_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let (value, is_null) = SysCacheGetAttr(mcx, cache_id, tup, attnum)?;
    if is_null {
        return Ok(None);
    }
    match &value {
        Datum::ByRef(b) => Ok(Some(mcx::slice_in(mcx, &b[..])?)),
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error(
            "syscache projection: *options attribute is by-value",
        )),
    }
}

/// `SearchSysCache1(FOREIGNDATAWRAPPEROID, fdwid)` then
/// `SysCacheGetAttr(Anum_pg_foreign_data_wrapper_fdwoptions)` (the raw
/// `fdwoptions` `text[]`). `Ok(None)` on a cache miss.
pub(crate) fn foreign_data_wrapper_options<'mcx>(
    mcx: Mcx<'mcx>,
    fdwid: Oid,
) -> PgResult<Option<Option<PgVec<'mcx, u8>>>> {
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNDATAWRAPPEROID,
        SysCacheKey::Value(KeyDatum::from_oid(fdwid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let bytes = getattr_option_bytes(
        mcx,
        FOREIGNDATAWRAPPEROID,
        &tup,
        Anum_pg_foreign_data_wrapper_fdwoptions,
    )?;
    ReleaseSysCache(tup);
    Ok(Some(bytes))
}

/// `SearchSysCache1(FOREIGNSERVEROID, serverid)` then
/// `SysCacheGetAttr(Anum_pg_foreign_server_srvoptions)`. `Ok(None)` on a cache
/// miss.
pub(crate) fn foreign_server_options<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
) -> PgResult<Option<Option<PgVec<'mcx, u8>>>> {
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNSERVEROID,
        SysCacheKey::Value(KeyDatum::from_oid(serverid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let bytes = getattr_option_bytes(
        mcx,
        FOREIGNSERVEROID,
        &tup,
        Anum_pg_foreign_server_srvoptions,
    )?;
    ReleaseSysCache(tup);
    Ok(Some(bytes))
}

/// `SearchSysCache1(USERMAPPINGOID, umid)` then
/// `SysCacheGetAttr(Anum_pg_user_mapping_umoptions)`. `Ok(None)` on a cache
/// miss.
pub(crate) fn user_mapping_options_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    umid: Oid,
) -> PgResult<Option<Option<PgVec<'mcx, u8>>>> {
    let tuple = SearchSysCache1(
        mcx,
        USERMAPPINGOID,
        SysCacheKey::Value(KeyDatum::from_oid(umid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let bytes = getattr_option_bytes(mcx, USERMAPPINGOID, &tup, Anum_pg_user_mapping_umoptions)?;
    ReleaseSysCache(tup);
    Ok(Some(bytes))
}

/// `SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid))` projected
/// to `Form_pg_foreign_data_wrapper`'s `(fdwname, fdwowner, fdwhandler,
/// fdwvalidator)`. `Ok(None)` on a cache miss.
pub(crate) fn foreign_data_wrapper_form<'mcx>(
    mcx: Mcx<'mcx>,
    fdwid: Oid,
) -> PgResult<Option<ForeignDataWrapperFormRow<'mcx>>> {
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNDATAWRAPPEROID,
        SysCacheKey::Value(KeyDatum::from_oid(fdwid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = ForeignDataWrapperFormRow {
        fdwname: getattr_name(
            mcx,
            FOREIGNDATAWRAPPEROID,
            &tup,
            Anum_pg_foreign_data_wrapper_fdwname,
        )?,
        fdwowner: getattr_oid(
            mcx,
            FOREIGNDATAWRAPPEROID,
            &tup,
            Anum_pg_foreign_data_wrapper_fdwowner,
        )?,
        fdwhandler: getattr_oid(
            mcx,
            FOREIGNDATAWRAPPEROID,
            &tup,
            Anum_pg_foreign_data_wrapper_fdwhandler,
        )?,
        fdwvalidator: getattr_oid(
            mcx,
            FOREIGNDATAWRAPPEROID,
            &tup,
            Anum_pg_foreign_data_wrapper_fdwvalidator,
        )?,
    };
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid))` projected to
/// `Form_pg_foreign_server`'s `(srvname, srvowner, srvfdw)`. `Ok(None)` on a
/// cache miss.
pub(crate) fn foreign_server_form<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
) -> PgResult<Option<ForeignServerFormRow<'mcx>>> {
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNSERVEROID,
        SysCacheKey::Value(KeyDatum::from_oid(serverid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = ForeignServerFormRow {
        srvname: getattr_name(mcx, FOREIGNSERVEROID, &tup, Anum_pg_foreign_server_srvname)?,
        srvowner: getattr_oid(mcx, FOREIGNSERVEROID, &tup, Anum_pg_foreign_server_srvowner)?,
        srvfdw: getattr_oid(mcx, FOREIGNSERVEROID, &tup, Anum_pg_foreign_server_srvfdw)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `GetSysCacheOid1(FOREIGNDATAWRAPPERNAME, Anum_pg_foreign_data_wrapper_oid,
/// CStringGetDatum(fdwname))`: the FDW's OID, or `InvalidOid` when no row
/// matches.
pub(crate) fn foreign_data_wrapper_oid_by_name(fdwname: &str) -> PgResult<Oid> {
    let scratch = MemoryContext::new("syscache fdw oid-by-name");
    GetSysCacheOid(
        scratch.mcx(),
        FOREIGNDATAWRAPPERNAME,
        Anum_pg_foreign_data_wrapper_oid as i16,
        SysCacheKey::Str(fdwname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `GetSysCacheOid1(FOREIGNSERVERNAME, Anum_pg_foreign_server_oid,
/// CStringGetDatum(servername))`: the server's OID, or `InvalidOid` when no row
/// matches.
pub(crate) fn foreign_server_oid_by_name(servername: &str) -> PgResult<Oid> {
    let scratch = MemoryContext::new("syscache foreign-server oid-by-name");
    GetSysCacheOid(
        scratch.mcx(),
        FOREIGNSERVERNAME,
        Anum_pg_foreign_server_oid as i16,
        SysCacheKey::Str(servername),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid))` projected to
/// `Form_pg_foreign_table`'s `ftserver`. `Ok(None)` on a cache miss.
pub(crate) fn foreign_table_server_by_relid(relid: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("syscache foreign-table ftserver");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNTABLEREL,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let ftserver = getattr_oid(mcx, FOREIGNTABLEREL, &tup, Anum_pg_foreign_table_ftserver)?;
    ReleaseSysCache(tup);
    Ok(Some(ftserver))
}

/// `SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid))` projected to
/// `(ftserver, ftoptions)`: the foreign server OID plus the raw `ftoptions`
/// `text[]` (`Some(bytes)`), or `None` when SQL NULL. `Ok(None)` on a cache
/// miss.
pub(crate) fn foreign_table_form<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<(Oid, Option<PgVec<'mcx, u8>>)>> {
    let tuple = SearchSysCache1(
        mcx,
        FOREIGNTABLEREL,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let ftserver = getattr_oid(mcx, FOREIGNTABLEREL, &tup, Anum_pg_foreign_table_ftserver)?;
    let bytes = getattr_option_bytes(mcx, FOREIGNTABLEREL, &tup, Anum_pg_foreign_table_ftoptions)?;
    ReleaseSysCache(tup);
    Ok(Some((ftserver, bytes)))
}

/// `SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(userid),
/// ObjectIdGetDatum(serverid))` projected to the mapping OID
/// (`Form_pg_user_mapping.oid`) plus the raw `umoptions` `text[]`
/// (`Some(bytes)`), or `None` when SQL NULL. `Ok(None)` on a cache miss.
pub(crate) fn user_mapping_form<'mcx>(
    mcx: Mcx<'mcx>,
    userid: Oid,
    serverid: Oid,
) -> PgResult<Option<(Oid, Option<PgVec<'mcx, u8>>)>> {
    let tuple = SearchSysCache2(
        mcx,
        USERMAPPINGUSERSERVER,
        SysCacheKey::Value(KeyDatum::from_oid(userid)),
        SysCacheKey::Value(KeyDatum::from_oid(serverid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let umid = getattr_oid(mcx, USERMAPPINGUSERSERVER, &tup, Anum_pg_user_mapping_oid)?;
    let bytes = getattr_option_bytes(
        mcx,
        USERMAPPINGUSERSERVER,
        &tup,
        Anum_pg_user_mapping_umoptions,
    )?;
    ReleaseSysCache(tup);
    Ok(Some((umid, bytes)))
}

/// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
/// then `SysCacheGetAttr(Anum_pg_attribute_attfdwoptions)`: the raw
/// `attfdwoptions` `text[]` (`Some(bytes)`), or `None` when SQL NULL.
/// `Ok(None)` on a cache miss.
pub(crate) fn attribute_fdwoptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<Option<PgVec<'mcx, u8>>>> {
    let tuple = SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let bytes = getattr_option_bytes(mcx, ATTNUM, &tup, Anum_pg_attribute_attfdwoptions)?;
    ReleaseSysCache(tup);
    Ok(Some(bytes))
}

// Fixed-width `Form_pg_attribute` column numbers (catalog/pg_attribute.h),
// plus the nullable `attoptions` (`CATALOG_VARLEN`).
const Anum_pg_attribute_attrelid: i32 = 1;
const Anum_pg_attribute_attname: i32 = 2;
const Anum_pg_attribute_attlen: i32 = 4;
const Anum_pg_attribute_atttypmod: i32 = 6;
const Anum_pg_attribute_attndims: i32 = 7;
const Anum_pg_attribute_attbyval: i32 = 8;
const Anum_pg_attribute_attalign: i32 = 9;
const Anum_pg_attribute_attstorage: i32 = 10;
const Anum_pg_attribute_attcompression: i32 = 11;
const Anum_pg_attribute_attnotnull: i32 = 12;
const Anum_pg_attribute_atthasdef: i32 = 13;
const Anum_pg_attribute_atthasmissing: i32 = 14;
const Anum_pg_attribute_attidentity: i32 = 15;
const Anum_pg_attribute_attgenerated: i32 = 16;
const Anum_pg_attribute_attisdropped: i32 = 17;
const Anum_pg_attribute_attislocal: i32 = 18;
const Anum_pg_attribute_attinhcount: i32 = 19;
const Anum_pg_attribute_attcollation: i32 = 20;
const Anum_pg_attribute_attoptions: i32 = 23;

/// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
/// + `GETSTRUCT(Form_pg_attribute)` projected to the fixed-width
/// `FormData_pg_attribute` row (the lsyscache `get_attgenerated` /
/// `get_atttype` / `get_atttypetypmodcoll` helpers' `GETSTRUCT`). `Ok(None)`
/// on a cache miss; the projection owns the `ReleaseSysCache`.
pub(crate) fn pg_attribute_form(
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<types_tuple::heaptuple::FormData_pg_attribute>> {
    // GETSTRUCT projects the fixed-width part by value, so the tuple copy lives
    // in a scratch context dropped before returning.
    let scratch = MemoryContext::new("pg_attribute_form projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = types_tuple::heaptuple::FormData_pg_attribute {
        attrelid: getattr_oid(mcx, ATTNUM, &tup, Anum_pg_attribute_attrelid)?,
        attname: types_tuple::heaptuple::NameData {
            data: getattr_namedata(mcx, ATTNUM, &tup, Anum_pg_attribute_attname)?,
        },
        atttypid: getattr_oid(mcx, ATTNUM, &tup, Anum_pg_attribute_atttypid)?,
        attlen: getattr_i16(mcx, ATTNUM, &tup, Anum_pg_attribute_attlen)?,
        attnum: getattr_i16(mcx, ATTNUM, &tup, Anum_pg_attribute_attnum)?,
        atttypmod: getattr_i32(mcx, ATTNUM, &tup, Anum_pg_attribute_atttypmod)?,
        attndims: getattr_i16(mcx, ATTNUM, &tup, Anum_pg_attribute_attndims)?,
        attbyval: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_attbyval)?,
        attalign: getattr_char(mcx, ATTNUM, &tup, Anum_pg_attribute_attalign)?,
        attstorage: getattr_char(mcx, ATTNUM, &tup, Anum_pg_attribute_attstorage)?,
        attcompression: getattr_char(mcx, ATTNUM, &tup, Anum_pg_attribute_attcompression)?,
        attnotnull: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_attnotnull)?,
        atthasdef: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_atthasdef)?,
        atthasmissing: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_atthasmissing)?,
        attidentity: getattr_char(mcx, ATTNUM, &tup, Anum_pg_attribute_attidentity)?,
        attgenerated: getattr_char(mcx, ATTNUM, &tup, Anum_pg_attribute_attgenerated)?,
        attisdropped: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_attisdropped)?,
        attislocal: getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_attislocal)?,
        attinhcount: getattr_i16(mcx, ATTNUM, &tup, Anum_pg_attribute_attinhcount)?,
        attcollation: getattr_oid(mcx, ATTNUM, &tup, Anum_pg_attribute_attcollation)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
/// + `SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &isnull)`
/// + `datumCopy` (lsyscache.c `get_attoptions`): the attribute's `attoptions`
/// `text[]` Datum copied into `mcx` (`Some(Some(datum))`), `Some(None)` for SQL
/// NULL (the C `(Datum) 0`), or `Ok(None)` on a cache miss (the caller raises
/// its own `cache lookup failed for attribute` error). The raw varlena bytes
/// ride in the canonical `Datum::ByRef` arm.
pub(crate) fn pg_attribute_attoptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<Option<Datum<'mcx>>>> {
    let tuple = SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // SysCacheGetAttr + datumCopy: `None` is the C `isnull` (`(Datum) 0`).
    let attopts =
        getattr_option_bytes(mcx, ATTNUM, &tup, Anum_pg_attribute_attoptions)?.map(Datum::ByRef);
    ReleaseSysCache(tup);
    Ok(Some(attopts))
}

/// `SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid))` projected to the
/// [`AggRow`] fields (`Form_pg_aggregate` `aggkind` / `aggnumdirectargs`).
/// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller (`func_get_detail`)
/// raises its own `cache lookup failed for aggregate %u` `elog(ERROR)`.
pub(crate) fn agg_row_by_oid<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<AggRow>> {
    let tuple = SearchSysCache1(mcx, AGGFNOID, SysCacheKey::Value(KeyDatum::from_oid(funcid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = AggRow {
        aggkind: getattr_char(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggkind)?,
        aggnumdirectargs: getattr_i16(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggnumdirectargs)?
            as i32,
    };
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
/// aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple)` plus the two nullable
/// `CATALOG_VARLEN` `agginitval` / `aggminitval` `SysCacheGetAttr` text columns,
/// projected to the full [`AggFormData`] for `ExecInitAgg`'s `fetch_agg_form`
/// (`nodeAgg.c`). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller
/// raises its own `cache lookup failed for aggregate %u` `elog(ERROR)`, as in C.
pub(crate) fn agg_form_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
) -> PgResult<Option<AggFormData>> {
    let tuple = SearchSysCache1(mcx, AGGFNOID, SysCacheKey::Value(KeyDatum::from_oid(aggfnoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // GETSTRUCT(aggTuple) — the fixed-part Form_pg_aggregate columns.
    let form = AggFormData {
        aggfnoid: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggfnoid as i32)?,
        aggkind: getattr_char(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggkind as i32)?,
        aggnumdirectargs: getattr_i16(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggnumdirectargs as i32)?,
        aggtransfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggtransfn as i32)?,
        aggfinalfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggfinalfn as i32)?,
        aggcombinefn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggcombinefn as i32)?,
        aggserialfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggserialfn as i32)?,
        aggdeserialfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggdeserialfn as i32)?,
        aggmtransfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmtransfn as i32)?,
        aggminvtransfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggminvtransfn as i32)?,
        aggmfinalfn: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmfinalfn as i32)?,
        aggfinalextra: getattr_bool(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggfinalextra as i32)?,
        aggmfinalextra: getattr_bool(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmfinalextra as i32)?,
        aggfinalmodify: getattr_char(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggfinalmodify as i32)?,
        aggmfinalmodify: getattr_char(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmfinalmodify as i32)?,
        aggsortop: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggsortop as i32)?,
        aggtranstype: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggtranstype as i32)?,
        aggtransspace: getattr_i32(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggtransspace as i32)?,
        aggmtranstype: getattr_oid(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmtranstype as i32)?,
        aggmtransspace: getattr_i32(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggmtransspace as i32)?,
        // SysCacheGetAttr(AGGFNOID, tup, Anum_pg_aggregate_agginitval, &isnull):
        // the nullable text initial-transition-value columns.
        agginitval: getattr_option_text(mcx, AGGFNOID, &tup, Anum_pg_aggregate_agginitval as i32)?,
        aggminitval: getattr_option_text(mcx, AGGFNOID, &tup, Anum_pg_aggregate_aggminitval as i32)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `text = SysCacheGetAttr(cacheId, tup, attnum, &isnull)`: `None` when the
/// column is SQL NULL, otherwise `TextDatumGetCString(text)` as an owned
/// `String` in `mcx`.
fn getattr_option_text(
    mcx: Mcx<'_>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<Option<String>> {
    let (value, is_null) = SysCacheGetAttr(mcx, cache_id, tup, attnum)?;
    if is_null {
        return Ok(None);
    }
    // TextDatumGetCString(value).
    let s = varlena_seams::text_to_cstring_v::call(mcx, &value)?;
    Ok(Some(String::from(s.as_str())))
}

/* ---------------------------------------------------------------------------
 * Additional fixed-width / name projection seams (lsyscache.c, fmgr.c,
 * superuser.c, acl.c, ri_triggers.c, relcache.c, tupdesc.c, catalog.c
 * consumers). Each mirrors the C `SearchSysCache*` + GETSTRUCT/SysCacheGetAttr
 * + project + ReleaseSysCache pattern.
 * ------------------------------------------------------------------------- */

// `catalog/pg_class.h` attribute numbers (1-based; PG 18 column order).
const Anum_pg_class_relname: i32 = 2;
const Anum_pg_class_relnamespace: i32 = 3;
const Anum_pg_class_reltype: i32 = 4;
const Anum_pg_class_relowner: i32 = 6;
const Anum_pg_class_relfilenode: i32 = 8;
const Anum_pg_class_reltablespace: i32 = 9;
const Anum_pg_class_relpages: i32 = 10;
const Anum_pg_class_reltuples: i32 = 11;
const Anum_pg_class_relallvisible: i32 = 12;
const Anum_pg_class_reltoastrelid: i32 = 14;
const Anum_pg_class_relhasindex: i32 = 15;
const Anum_pg_class_relisshared: i32 = 16;
const Anum_pg_class_relpersistence: i32 = 17;
const Anum_pg_class_relkind: i32 = 18;
const Anum_pg_class_relnatts: i32 = 19;
const Anum_pg_class_relchecks: i32 = 20;
const Anum_pg_class_relhasrules: i32 = 21;
const Anum_pg_class_relhastriggers: i32 = 22;
const Anum_pg_class_relhassubclass: i32 = 23;
const Anum_pg_class_relrowsecurity: i32 = 24;
const Anum_pg_class_relforcerowsecurity: i32 = 25;
const Anum_pg_class_relispopulated: i32 = 26;
const Anum_pg_class_relreplident: i32 = 27;
const Anum_pg_class_relispartition: i32 = 28;
const Anum_pg_class_relrewrite: i32 = 29;
const Anum_pg_class_relfrozenxid: i32 = 30;
const Anum_pg_class_relminmxid: i32 = 31;

// `catalog/pg_type.h` attribute numbers.
const Anum_pg_type_oid: i32 = 1;
const Anum_pg_type_typname: i32 = 2;
const Anum_pg_type_typnamespace: i32 = 3;
const Anum_pg_type_typowner: i32 = 4;
const Anum_pg_type_typlen: i32 = 5;
const Anum_pg_type_typbyval: i32 = 6;
const Anum_pg_type_typtype: i32 = 7;
const Anum_pg_type_typcategory: i32 = 8;
const Anum_pg_type_typispreferred: i32 = 9;
const Anum_pg_type_typisdefined: i32 = 10;
const Anum_pg_type_typdelim: i32 = 11;
const Anum_pg_type_typrelid: i32 = 12;
const Anum_pg_type_typsubscript: i32 = 13;
const Anum_pg_type_typelem: i32 = 14;
const Anum_pg_type_typarray: i32 = 15;
const Anum_pg_type_typinput: i32 = 16;
const Anum_pg_type_typoutput: i32 = 17;
const Anum_pg_type_typreceive: i32 = 18;
const Anum_pg_type_typsend: i32 = 19;
const Anum_pg_type_typmodin: i32 = 20;
const Anum_pg_type_typmodout: i32 = 21;
const Anum_pg_type_typanalyze: i32 = 22;
const Anum_pg_type_typalign: i32 = 23;
const Anum_pg_type_typstorage: i32 = 24;
const Anum_pg_type_typnotnull: i32 = 25;
const Anum_pg_type_typbasetype: i32 = 26;
const Anum_pg_type_typtypmod: i32 = 27;
const Anum_pg_type_typndims: i32 = 28;
const Anum_pg_type_typcollation: i32 = 29;

// `catalog/pg_proc.h` attribute numbers.
const Anum_pg_proc_proname: i32 = 2;
const Anum_pg_proc_pronamespace: i32 = 3;
const Anum_pg_proc_provariadic: i32 = 8;
const Anum_pg_proc_prosupport: i32 = 9;
const Anum_pg_proc_prokind: i32 = 10;
const Anum_pg_proc_proleakproof: i32 = 12;
const Anum_pg_proc_proisstrict: i32 = 13;
const Anum_pg_proc_proretset: i32 = 14;
const Anum_pg_proc_provolatile: i32 = 15;
const Anum_pg_proc_proparallel: i32 = 16;
const Anum_pg_proc_proowner: i32 = 4;
const Anum_pg_proc_prolang: i32 = 5;
const Anum_pg_proc_prosecdef: i32 = 11;
const Anum_pg_proc_pronargs: i32 = 17;
const Anum_pg_proc_prorettype: i32 = 19;
const Anum_pg_proc_proargtypes: i32 = 20;
const Anum_pg_proc_prosrc: i32 = 26;
const Anum_pg_proc_probin: i32 = 27;
const Anum_pg_proc_proconfig: i32 = 29;

// Language OIDs the `prolang` switch matches (`catalog/pg_language_d.h`).
const INTERNAL_LANGUAGE_ID: u32 = 12;
const C_LANGUAGE_ID: u32 = 13;
const SQL_LANGUAGE_ID: u32 = 14;

// `catalog/pg_authid.h` attribute numbers.
const Anum_pg_authid_oid: i32 = 1;
const Anum_pg_authid_rolname: i32 = 2;
const Anum_pg_authid_rolsuper: i32 = 3;
const Anum_pg_authid_rolcanlogin: i32 = 7;
const Anum_pg_authid_rolreplication: i32 = 8;
const Anum_pg_authid_rolbypassrls: i32 = 9;
const Anum_pg_authid_rolconnlimit: i32 = 10;

// `catalog/pg_language.h` attribute numbers.
const Anum_pg_language_lanname: i32 = 2;
const Anum_pg_language_lanplcallfoid: i32 = 6;
const Anum_pg_language_lanvalidator: i32 = 8;

// `catalog/pg_collation.h` attribute numbers.
const Anum_pg_collation_collname: i32 = 2;
const Anum_pg_collation_collnamespace: i32 = 3;

// `catalog/pg_index.h` attribute numbers.
const Anum_pg_index_indexrelid: i32 = 1;
const Anum_pg_index_indrelid: i32 = 2;
const Anum_pg_index_indnatts: i32 = 3;
const Anum_pg_index_indnkeyatts: i32 = 4;
const Anum_pg_index_indisunique: i32 = 5;
const Anum_pg_index_indnullsnotdistinct: i32 = 6;
const Anum_pg_index_indisprimary: i32 = 7;
const Anum_pg_index_indisexclusion: i32 = 8;
const Anum_pg_index_indimmediate: i32 = 9;
const Anum_pg_index_indisclustered: i32 = 10;
const Anum_pg_index_indisvalid: i32 = 11;
const Anum_pg_index_indcheckxmin: i32 = 12;
const Anum_pg_index_indisready: i32 = 13;
const Anum_pg_index_indislive: i32 = 14;
const Anum_pg_index_indisreplident: i32 = 15;
const Anum_pg_index_indkey: i32 = 16;
const Anum_pg_index_indcollation: i32 = 17;
const Anum_pg_index_indclass: i32 = 18;
const Anum_pg_index_indoption: i32 = 19;
const Anum_pg_index_indpred: i32 = 21;

// `catalog/pg_attribute.h` attribute numbers.
const Anum_pg_attribute_attnum: i32 = 5;
const Anum_pg_attribute_atttypid: i32 = 3;

// `catalog/pg_opclass.h` `oid` for `get_opclass_oid`.
const Anum_pg_opclass_oid: i32 = 1;

/// A padding key for the unused key slots of [`SearchSysCache`]/[`GetSysCacheOid`]
/// (the catcache ignores keys beyond `cc_nkeys`).
const UNUSED_KEY: SysCacheKey<'static> = SysCacheKey::Value(KeyDatum::null());

/* ---------------------------------------------------------------------------
 * pg_constraint projections for backend-catalog-pg-constraint
 * ------------------------------------------------------------------------- */

use types_catalog::pg_constraint::{
    ConKeyArray, ConstraintFormCopy, FkArrayProjection, FormData_pg_constraint, OidArray,
    Anum_pg_constraint_conname,
    Anum_pg_constraint_connamespace, Anum_pg_constraint_contype, Anum_pg_constraint_condeferrable,
    Anum_pg_constraint_condeferred, Anum_pg_constraint_conenforced, Anum_pg_constraint_convalidated,
    Anum_pg_constraint_conrelid, Anum_pg_constraint_contypid, Anum_pg_constraint_conindid,
    Anum_pg_constraint_conparentid, Anum_pg_constraint_confrelid, Anum_pg_constraint_confupdtype,
    Anum_pg_constraint_confdeltype, Anum_pg_constraint_confmatchtype, Anum_pg_constraint_conislocal,
    Anum_pg_constraint_coninhcount, Anum_pg_constraint_connoinherit, Anum_pg_constraint_conperiod,
    Anum_pg_constraint_conkey, Anum_pg_constraint_confkey, Anum_pg_constraint_conpfeqop,
    Anum_pg_constraint_conppeqop, Anum_pg_constraint_conffeqop, Anum_pg_constraint_confdelsetcols,
};
use types_tuple::heaptuple::{INT2OID, OIDOID};
use backend_access_common_detoast_seams as detoast_seams;

/// `Anum_pg_constraint_oid` (`catalog/pg_constraint.h`).
const Anum_pg_constraint_oid: i32 = 1;
/// `NAMEDATALEN` (`pg_config_manual.h`).
const NAMEDATALEN: usize = 64;

/// A `name` attribute (`NameData` bytes) as the NUL-padded fixed 64-byte image
/// (`(Form_pg_constraint)->conname`). The deformed `name` column is a 64-byte
/// by-reference value; we copy up to `NAMEDATALEN` into a zero-padded buffer.
fn getattr_namedata(
    mcx: Mcx<'_>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<[u8; NAMEDATALEN]> {
    let value = SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?;
    let mut n = [0u8; NAMEDATALEN];
    if let Datum::ByRef(b) = &value {
        let take = core::cmp::min(NAMEDATALEN, b.len());
        n[..take].copy_from_slice(&b[..take]);
    }
    Ok(n)
}

/// `(Form_pg_constraint) GETSTRUCT(tup)` — the fixed-width scalar columns
/// (Anum 1..=20) projected off a held `pg_constraint` `FormedTuple` via the
/// `CONSTROID` cache tuple descriptor (`heap_getattr`).
fn deform_constraint_form(
    mcx: Mcx<'_>,
    tup: &FormedTuple<'_>,
) -> PgResult<FormData_pg_constraint> {
    Ok(FormData_pg_constraint {
        oid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_oid)?,
        conname: getattr_namedata(mcx, CONSTROID, tup, Anum_pg_constraint_conname as i32)?,
        connamespace: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_connamespace as i32)?,
        contype: getattr_char(mcx, CONSTROID, tup, Anum_pg_constraint_contype as i32)?,
        condeferrable: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_condeferrable as i32)?,
        condeferred: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_condeferred as i32)?,
        conenforced: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_conenforced as i32)?,
        convalidated: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_convalidated as i32)?,
        conrelid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_conrelid as i32)?,
        contypid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_contypid as i32)?,
        conindid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_conindid as i32)?,
        conparentid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_conparentid as i32)?,
        confrelid: getattr_oid(mcx, CONSTROID, tup, Anum_pg_constraint_confrelid as i32)?,
        confupdtype: getattr_char(mcx, CONSTROID, tup, Anum_pg_constraint_confupdtype as i32)?,
        confdeltype: getattr_char(mcx, CONSTROID, tup, Anum_pg_constraint_confdeltype as i32)?,
        confmatchtype: getattr_char(mcx, CONSTROID, tup, Anum_pg_constraint_confmatchtype as i32)?,
        conislocal: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_conislocal as i32)?,
        coninhcount: getattr_i16(mcx, CONSTROID, tup, Anum_pg_constraint_coninhcount as i32)?,
        connoinherit: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_connoinherit as i32)?,
        conperiod: getattr_bool(mcx, CONSTROID, tup, Anum_pg_constraint_conperiod as i32)?,
    })
}

/// `bool` attribute (`as_bool` of the by-value datum).
fn getattr_bool(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<bool> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_bool())
}

fn getattr_i32(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<i32> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_i32())
}

fn getattr_u32(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<u32> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_u32())
}

fn getattr_f32(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<f32> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_f32())
}

/// A `name` attribute (`NameStr` bytes) as raw bytes in `mcx` (no trailing NUL),
/// matching the C `pstrdup(NameStr(..))` consumers that want the bytes.
fn getattr_name_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let value = SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?;
    let bytes = match &value {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error("syscache projection: name attribute is by-value"))
        }
    };
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    mcx::slice_in(mcx, &bytes[..len])
}

/// `RelationSupportsSysCache(relid)` (syscache.c).
pub(crate) fn relation_supports_syscache(relid: Oid) -> bool {
    crate::RelationSupportsSysCache(relid)
}

/// `InitCatalogCachePhase2()` (syscache.c).
pub(crate) fn init_catalog_cache_phase2() -> PgResult<()> {
    crate::InitCatalogCachePhase2()
}

/// `SearchSysCacheExists1(RELOID, indexOid)` (syscache.c).
pub(crate) fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool> {
    let scratch = MemoryContext::new("syscache exists reloid");
    let mcx = scratch.mcx();
    SearchSysCacheExists(
        mcx,
        RELOID,
        SysCacheKey::Value(KeyDatum::from_oid(reloid)),
        UNUSED_KEY,
        UNUSED_KEY,
        UNUSED_KEY,
    )
}

/// `SearchSysCache1(RELOID, relid)` -> `Form_pg_class.relkind` (`get_rel_relkind`).
pub(crate) fn rel_relkind(relid: Oid) -> PgResult<Option<u8>> {
    let scratch = MemoryContext::new("syscache relkind projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relkind = getattr_char(mcx, RELOID, &tup, Anum_pg_class_relkind)? as u8;
    ReleaseSysCache(tup);
    Ok(Some(relkind))
}

/// `GetSysCacheHashValue1(CONSTROID, ObjectIdGetDatum(oid))` — the catcache
/// hash value for a `pg_constraint` row.
pub(crate) fn get_syscache_hash_value_constroid(oid: Oid) -> PgResult<u32> {
    crate::GetSysCacheHashValue1(CONSTROID, SysCacheKey::Value(KeyDatum::from_oid(oid)))
}

/// `GetSysCacheHashValue1(DATABASEOID, ObjectIdGetDatum(dbid))` (acl.c
/// `initialize_acl`) — the catcache hash value for a `pg_database` row, cached
/// to filter `DATABASEOID` invalidations for other databases.
pub(crate) fn database_syscache_hash_value(dbid: Oid) -> PgResult<u32> {
    crate::GetSysCacheHashValue1(DATABASEOID, SysCacheKey::Value(KeyDatum::from_oid(dbid)))
}

// pg_locale.c catalog-read seams: the locale-relevant pg_database / pg_collation
// columns the `create_pg_locale` / `init_database_collation` / libc-default
// paths consult. Fixed column numbers (catalog/pg_database.h, pg_collation.h).
const Anum_pg_database_datlocprovider_loc: i32 = 5;
const Anum_pg_database_datcollate_loc: i32 = 13;
const Anum_pg_database_datctype_loc: i32 = 14;
const Anum_pg_collation_collname_loc: i32 = 2;
const Anum_pg_collation_collnamespace_loc: i32 = 3;
const Anum_pg_collation_collprovider_loc: i32 = 5;
const Anum_pg_collation_collcollate_loc: i32 = 8;
const Anum_pg_collation_collctype_loc: i32 = 9;
const Anum_pg_collation_colllocale_loc: i32 = 10;
const Anum_pg_collation_collversion_loc: i32 = 12;

/// `SearchSysCache1(DATABASEOID, MyDatabaseId)` + the locale-relevant
/// `pg_database` columns (`init_database_collation` / libc default path,
/// pg_locale.c): `datlocprovider` (`char`) + `datcollate` / `datctype`
/// (`SysCacheGetAttrNotNull` text). `Ok(None)` on a cache miss (the caller's
/// "cache lookup failed for database" path). MyDatabaseId comes from the
/// init-small globals owner.
pub(crate) fn database_locale_row(
) -> PgResult<Option<backend_utils_adt_pg_locale_catalog_seams::DatabaseLocaleRow>> {
    let dbid = backend_utils_init_small_seams::my_database_id::call();
    let scratch = MemoryContext::new("database_locale_row projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, DATABASEOID, SysCacheKey::Value(KeyDatum::from_oid(dbid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let provider = getattr_char(mcx, DATABASEOID, &tup, Anum_pg_database_datlocprovider_loc)?;
    // datcollate / datctype are BKI_FORCE_NOT_NULL (SysCacheGetAttrNotNull).
    let collate = getattr_option_text(mcx, DATABASEOID, &tup, Anum_pg_database_datcollate_loc)?
        .ok_or_else(|| PgError::error(format!("null datcollate for database {dbid}")))?;
    let ctype = getattr_option_text(mcx, DATABASEOID, &tup, Anum_pg_database_datctype_loc)?
        .ok_or_else(|| PgError::error(format!("null datctype for database {dbid}")))?;
    ReleaseSysCache(tup);
    Ok(Some(backend_utils_adt_pg_locale_catalog_seams::DatabaseLocaleRow {
        provider,
        collate,
        ctype,
    }))
}

/// `SearchSysCache1(COLLOID, collid)` + the locale-relevant `pg_collation`
/// columns (`create_pg_locale`, pg_locale.c): `collprovider` (`char`) +
/// `collname` (`NameStr`) + `collnamespace` + nullable `collcollate` /
/// `collctype` / `colllocale` / `collversion` text. `Ok(None)` on a cache miss
/// (the "cache lookup failed for collation" path).
pub(crate) fn collation_locale_row(
    collid: Oid,
) -> PgResult<Option<backend_utils_adt_pg_locale_catalog_seams::CollationLocaleRow>> {
    let scratch = MemoryContext::new("collation_locale_row projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, COLLOID, SysCacheKey::Value(KeyDatum::from_oid(collid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let provider = getattr_char(mcx, COLLOID, &tup, Anum_pg_collation_collprovider_loc)?;
    let name = getattr_name(mcx, COLLOID, &tup, Anum_pg_collation_collname_loc)?
        .as_str()
        .to_string();
    let namespace = getattr_oid(mcx, COLLOID, &tup, Anum_pg_collation_collnamespace_loc)?;
    let collate = getattr_option_text(mcx, COLLOID, &tup, Anum_pg_collation_collcollate_loc)?;
    let ctype = getattr_option_text(mcx, COLLOID, &tup, Anum_pg_collation_collctype_loc)?;
    let locale = getattr_option_text(mcx, COLLOID, &tup, Anum_pg_collation_colllocale_loc)?;
    let version = getattr_option_text(mcx, COLLOID, &tup, Anum_pg_collation_collversion_loc)?;
    ReleaseSysCache(tup);
    Ok(Some(backend_utils_adt_pg_locale_catalog_seams::CollationLocaleRow {
        provider,
        name,
        namespace,
        collate,
        ctype,
        locale,
        version,
    }))
}

/// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` projected to the
/// scalar [`FormData_pg_constraint`] columns plus the heap TID (`tup->t_self`),
/// then `ReleaseSysCache`. `Ok(None)` on a cache miss (`!HeapTupleIsValid`);
/// the caller raises `cache lookup failed for constraint %u`.
///
/// The `conkey` array column is not materialized here: none of this seam's
/// consumers (`RemoveConstraintById` / `RenameConstraintById` /
/// `ConstraintSetParentConstraint` / `get_ri_constraint_root` /
/// `constraint_type_oids`) read `conkey`, and C never detoasts it on these
/// paths — so `conkey` is `None`, matching the C reads and avoiding spurious
/// detoast `ereport`s the C code never performs.
pub(crate) fn search_constraint_form_by_oid(
    conoid: Oid,
) -> PgResult<Option<ConstraintFormCopy>> {
    let scratch = MemoryContext::new("syscache pg_constraint form projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, CONSTROID, SysCacheKey::Value(KeyDatum::from_oid(conoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = deform_constraint_form(mcx, &tup)?;
    let tid = tup.tuple.t_self;
    ReleaseSysCache(tup);
    Ok(Some(ConstraintFormCopy {
        form,
        conkey: None,
        tid,
    }))
}

// ---------------------------------------------------------------------------
// `pg_constraint` array-column reads (`DatumGetArrayTypeP` + `ARR_*`).
//
// The smallint / oid array columns of a `pg_constraint` row are 1-D inline
// catalog arrays. The deformed by-reference column value is the verbatim array
// varlena (header included); `DatumGetArrayTypeP` (C) detoasts it first. We
// mirror that with `detoast_attr`, then read the `ArrayType` header fields
// (`array.h` accessor macros) straight off the bytes.
// ---------------------------------------------------------------------------

/// `sizeof(ArrayType)` — `vl_len_`(4) + `ndim`(4) + `dataoffset`(4) +
/// `elemtype`(4).
const ARRAYTYPE_HDRSZ: usize = 16;
/// `MAXIMUM_ALIGNOF` (`pg_config.h`): 8 on all supported platforms.
const MAXIMUM_ALIGNOF: usize = 8;

#[inline]
fn arr_read_i32(a: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes([a[off], a[off + 1], a[off + 2], a[off + 3]])
}

#[inline]
fn arr_maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `ARR_DATA_OFFSET(a)` — `ARR_HASNULL ? dataoffset : MAXALIGN(sizeof(ArrayType)
/// + 2*sizeof(int)*ndim)`.
fn arr_data_offset(_a: &[u8], ndim: i32, dataoffset: i32) -> usize {
    if dataoffset != 0 {
        dataoffset as usize
    } else {
        arr_maxalign(ARRAYTYPE_HDRSZ + 2 * 4 * ndim as usize)
    }
}

/// Common decode of an array varlena into `(ndim, hasnull, elemtype, dim0,
/// data_offset)`. `DatumGetArrayTypeP` (detoast) then `ARR_NDIM` / `ARR_HASNULL`
/// / `ARR_ELEMTYPE` / `ARR_DIMS(arr)[0]` / `ARR_DATA_PTR`.
fn detoast_array_header<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &[u8],
) -> PgResult<(PgVec<'mcx, u8>, i32, bool, Oid, i32, usize)> {
    let arr = detoast_seams::detoast_attr::call(mcx, raw)?;
    let ndim = arr_read_i32(&arr, 4);
    let dataoffset = arr_read_i32(&arr, 8);
    let hasnull = dataoffset != 0;
    let elemtype = u32::from_ne_bytes([arr[12], arr[13], arr[14], arr[15]]);
    // ARR_DIMS(arr)[0] is the first int after the fixed header.
    let dim0 = if ndim >= 1 {
        arr_read_i32(&arr, ARRAYTYPE_HDRSZ)
    } else {
        0
    };
    let data_off = arr_data_offset(&arr, ndim, dataoffset);
    Ok((arr, ndim, hasnull, elemtype, dim0, data_off))
}

/// Read a 1-D `int2[]` array column into a [`ConKeyArray`]. No validation
/// (1-D / elemtype / hasnull) is performed here — the caller (pg_constraint)
/// raises the C error messages; we faithfully report whatever the header says,
/// and the element data is read only when the header is a non-null 1-D array.
fn read_conkey_array(mcx: Mcx<'_>, raw: &[u8]) -> PgResult<ConKeyArray> {
    let (arr, ndim, hasnull, elemtype, dim0, data_off) = detoast_array_header(mcx, raw)?;
    let mut data: Vec<i16> = Vec::new();
    if ndim == 1 && !hasnull {
        let n = dim0.max(0) as usize;
        data.reserve(n);
        for i in 0..n {
            let off = data_off + i * 2;
            data.push(i16::from_ne_bytes([arr[off], arr[off + 1]]));
        }
    }
    Ok(ConKeyArray {
        ndim,
        hasnull,
        elemtype,
        dim0,
        data,
    })
}

/// Read a 1-D `oid[]` array column into an [`OidArray`].
fn read_oid_array(mcx: Mcx<'_>, raw: &[u8]) -> PgResult<OidArray> {
    let (arr, ndim, hasnull, elemtype, dim0, data_off) = detoast_array_header(mcx, raw)?;
    let mut data: Vec<Oid> = Vec::new();
    if ndim == 1 && !hasnull {
        let n = dim0.max(0) as usize;
        data.reserve(n);
        for i in 0..n {
            let off = data_off + i * 4;
            data.push(u32::from_ne_bytes([
                arr[off],
                arr[off + 1],
                arr[off + 2],
                arr[off + 3],
            ]));
        }
    }
    Ok(OidArray {
        ndim,
        hasnull,
        elemtype,
        dim0,
        data,
    })
}

/// `(Form_pg_constraint) GETSTRUCT(tup)` of a held `pg_constraint` tuple
/// (`read_constraint_form` seam — `AdjustNotNullInheritance`).
pub(crate) fn read_constraint_form(
    tup: &FormedTuple<'_>,
) -> PgResult<FormData_pg_constraint> {
    let scratch = MemoryContext::new("syscache read_constraint_form");
    deform_constraint_form(scratch.mcx(), tup)
}

/// `SysCacheGetAttrNotNull(CONSTROID, tup, Anum_pg_constraint_conkey)` +
/// `DatumGetArrayTypeP` (`get_conkey_array` seam — `extractNotNullColumn`).
pub(crate) fn get_conkey_array(tup: &FormedTuple<'_>) -> PgResult<ConKeyArray> {
    let scratch = MemoryContext::new("syscache get_conkey_array");
    let mcx = scratch.mcx();
    let value = SysCacheGetAttrNotNull(mcx, CONSTROID, tup, Anum_pg_constraint_conkey as i32)?;
    match &value {
        Datum::ByRef(b) => read_conkey_array(mcx, b),
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("conkey is not a by-reference array")),
    }
}

/// `heap_getattr(tup, Anum_pg_constraint_conkey, RelationGetDescr(pg_constraint),
/// &isNull)` + `DatumGetArrayTypeP` (`heap_get_conkey` seam —
/// `get_primary_key_attnos`). `Ok(None)` when the column is SQL NULL.
pub(crate) fn heap_get_conkey(
    rel: &types_rel::RelationData<'_>,
    tup: &FormedTuple<'_>,
) -> PgResult<Option<ConKeyArray>> {
    let scratch = MemoryContext::new("syscache heap_get_conkey");
    let mcx = scratch.mcx();
    let (value, isnull) = crate::heap_getattr(
        mcx,
        tup,
        Anum_pg_constraint_conkey as i32,
        &rel.rd_att,
    )?;
    if isnull {
        return Ok(None);
    }
    match &value {
        Datum::ByRef(b) => Ok(Some(read_conkey_array(mcx, b)?)),
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("conkey is not a by-reference array")),
    }
}

/// `DeconstructFkConstraintRow`'s `SysCacheGetAttrNotNull` / `SysCacheGetAttr`
/// reads of the six FK array columns + `DatumGetArrayTypeP`
/// (`deconstruct_fk_arrays` seam). `confdelsetcols` is `None` for a SQL NULL.
pub(crate) fn deconstruct_fk_arrays(
    tup: &FormedTuple<'_>,
) -> PgResult<FkArrayProjection> {
    let scratch = MemoryContext::new("syscache deconstruct_fk_arrays");
    let mcx = scratch.mcx();

    let read_conkey = |attnum: i16| -> PgResult<ConKeyArray> {
        let value = SysCacheGetAttrNotNull(mcx, CONSTROID, tup, attnum as i32)?;
        match &value {
            Datum::ByRef(b) => read_conkey_array(mcx, b),
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => Err(PgError::error("FK array column is not by-reference")),
        }
    };
    let read_oid = |attnum: i16| -> PgResult<OidArray> {
        let value = SysCacheGetAttrNotNull(mcx, CONSTROID, tup, attnum as i32)?;
        match &value {
            Datum::ByRef(b) => read_oid_array(mcx, b),
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => Err(PgError::error("FK array column is not by-reference")),
        }
    };

    let conkey = read_conkey(Anum_pg_constraint_conkey)?;
    let confkey = read_conkey(Anum_pg_constraint_confkey)?;
    let conpfeqop = read_oid(Anum_pg_constraint_conpfeqop)?;
    let conppeqop = read_oid(Anum_pg_constraint_conppeqop)?;
    let conffeqop = read_oid(Anum_pg_constraint_conffeqop)?;

    // confdelsetcols may be SQL NULL.
    let (value, isnull) =
        SysCacheGetAttr(mcx, CONSTROID, tup, Anum_pg_constraint_confdelsetcols as i32)?;
    let confdelsetcols = if isnull {
        None
    } else {
        match &value {
            Datum::ByRef(b) => Some(read_conkey_array(mcx, b)?),
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => {
                return Err(PgError::error("confdelsetcols is not a by-reference array"))
            }
        }
    };

    let _ = (INT2OID, OIDOID); // element types validated by the caller.

    Ok(FkArrayProjection {
        conkey,
        confkey,
        conpfeqop,
        conppeqop,
        conffeqop,
        confdelsetcols,
    })
}

/// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` + `heap_copytuple`
/// (`search_constraint_tuple_by_oid` seam — `DeconstructFkConstraintRow` via
/// `FindFkPeriodOpersForConstraint`). `Ok(None)` on a cache miss; the returned
/// tuple is the held `FormedTuple` copied into `mcx`.
pub(crate) fn search_constraint_tuple_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCache1(mcx, CONSTROID, SysCacheKey::Value(KeyDatum::from_oid(conoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let copy = tup.clone_in(mcx)?;
    ReleaseSysCache(tup);
    Ok(Some(copy))
}

/// `relTup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` +
/// `((Form_pg_class) GETSTRUCT(relTup))->relchecks` (RemoveConstraintById).
/// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller raises `cache
/// lookup failed for relation %u`.
pub(crate) fn fetch_relchecks(relid: Oid) -> PgResult<Option<i16>> {
    let scratch = MemoryContext::new("syscache pg_class relchecks projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relchecks = getattr_i16(mcx, RELOID, &tup, Anum_pg_class_relchecks)?;
    ReleaseSysCache(tup);
    Ok(Some(relchecks))
}

/// `SearchSysCache1(PROCOID, funcid)` + `GETSTRUCT` of the fixed-width
/// `Form_pg_proc` columns the scalar lsyscache helpers read (`get_func_*`).
pub(crate) fn pg_proc_form<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgProcForm>> {
    let tuple = SearchSysCache1(mcx, PROCOID, SysCacheKey::Value(KeyDatum::from_oid(funcid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let proname = getattr_name(mcx, PROCOID, &tup, Anum_pg_proc_proname)?;
    let form = PgProcForm {
        proname: proname.as_str().to_owned(),
        pronamespace: getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_pronamespace)?,
        provariadic: getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_provariadic)?,
        prosupport: getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_prosupport)?,
        prokind: getattr_char(mcx, PROCOID, &tup, Anum_pg_proc_prokind)?,
        proleakproof: getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proleakproof)?,
        proisstrict: getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proisstrict)?,
        proretset: getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proretset)?,
        provolatile: getattr_char(mcx, PROCOID, &tup, Anum_pg_proc_provolatile)?,
        proparallel: getattr_char(mcx, PROCOID, &tup, Anum_pg_proc_proparallel)?,
        pronargs: getattr_i16(mcx, PROCOID, &tup, Anum_pg_proc_pronargs)?,
        prorettype: getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_prorettype)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `SearchSysCache1(TYPEOID, oidtypeid)` projected to the type-dependent
/// attribute fields `TupleDescInitEntry` stamps (`tupdesc.c`).
pub(crate) fn search_type_attr_info(oidtypeid: Oid) -> PgResult<Option<PgTypeInfo>> {
    let scratch = MemoryContext::new("syscache pg_type projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(KeyDatum::from_oid(oidtypeid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let info = PgTypeInfo {
        typlen: getattr_i16(mcx, TYPEOID, &tup, Anum_pg_type_typlen)?,
        typbyval: getattr_bool(mcx, TYPEOID, &tup, Anum_pg_type_typbyval)?,
        typalign: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typalign)?,
        typstorage: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typstorage)?,
        typcollation: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typcollation)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(info))
}

/// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid))` + `GETSTRUCT(Form_pg_type)`
/// (`utils/cache/lsyscache.c` reads). Projects the fixed-width `pg_type` columns
/// by value (every field through `typcollation`), `Ok(None)` on a cache miss so
/// the caller raises its own `cache lookup failed for type %u` `elog(ERROR)`.
pub(crate) fn pg_type_form(
    typid: Oid,
) -> PgResult<Option<types_tuple::pg_type::FormData_pg_type>> {
    let scratch = MemoryContext::new("pg_type_form projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(KeyDatum::from_oid(typid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = types_tuple::pg_type::FormData_pg_type {
        oid: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_oid)?,
        typname: types_tuple::heaptuple::NameData {
            data: getattr_namedata(mcx, TYPEOID, &tup, Anum_pg_type_typname)?,
        },
        typnamespace: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typnamespace)?,
        typowner: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typowner)?,
        typlen: getattr_i16(mcx, TYPEOID, &tup, Anum_pg_type_typlen)?,
        typbyval: getattr_bool(mcx, TYPEOID, &tup, Anum_pg_type_typbyval)?,
        typtype: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typtype)?,
        typcategory: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typcategory)?,
        typispreferred: getattr_bool(mcx, TYPEOID, &tup, Anum_pg_type_typispreferred)?,
        typisdefined: getattr_bool(mcx, TYPEOID, &tup, Anum_pg_type_typisdefined)?,
        typdelim: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typdelim)?,
        typrelid: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typrelid)?,
        typsubscript: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typsubscript)?,
        typelem: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typelem)?,
        typarray: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typarray)?,
        typinput: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typinput)?,
        typoutput: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typoutput)?,
        typreceive: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typreceive)?,
        typsend: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typsend)?,
        typmodin: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typmodin)?,
        typmodout: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typmodout)?,
        typanalyze: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typanalyze)?,
        typalign: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typalign)?,
        typstorage: getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typstorage)?,
        typnotnull: getattr_bool(mcx, TYPEOID, &tup, Anum_pg_type_typnotnull)?,
        typbasetype: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typbasetype)?,
        typtypmod: getattr_i32(mcx, TYPEOID, &tup, Anum_pg_type_typtypmod)?,
        typndims: getattr_i32(mcx, TYPEOID, &tup, Anum_pg_type_typndims)?,
        typcollation: getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typcollation)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `GetSysCacheOid3(CLAAMNAMENSP, Anum_pg_opclass_oid, amid, opcname, nsp)`.
pub(crate) fn get_opclass_oid(amid: Oid, opcname: &str, namespace_id: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("syscache get_opclass_oid");
    let mcx = scratch.mcx();
    GetSysCacheOid(
        mcx,
        CLAAMNAMENSP,
        Anum_pg_opclass_oid as AttrNumber,
        SysCacheKey::Value(KeyDatum::from_oid(amid)),
        SysCacheKey::Str(opcname),
        SysCacheKey::Value(KeyDatum::from_oid(namespace_id)),
        UNUSED_KEY,
    )
}

/// `SearchSysCacheAttName(relid, attname)` + `GETSTRUCT` -> `(attnum, atttypid)`.
pub(crate) fn search_syscache_attname(
    relid: Oid,
    attname: &str,
) -> PgResult<Option<(AttrNumber, Oid)>> {
    let scratch = MemoryContext::new("syscache attname projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCacheAttName(mcx, relid, attname)?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let attnum = getattr_i16(mcx, ATTNAME, &tup, Anum_pg_attribute_attnum)? as AttrNumber;
    let atttypid = getattr_oid(mcx, ATTNAME, &tup, Anum_pg_attribute_atttypid)?;
    ReleaseSysCache(tup);
    Ok(Some((attnum, atttypid)))
}

/// `SearchSysCache1(AUTHOID, roleid)` projected to the role-identity fields.
pub(crate) fn lookup_authid_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
) -> PgResult<Option<AuthIdRow<'mcx>>> {
    let tuple = SearchSysCache1(mcx, AUTHOID, SysCacheKey::Value(KeyDatum::from_oid(roleid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = project_authid(mcx, &tup)?;
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache1(AUTHNAME, rolename)` projected to the role-identity fields.
pub(crate) fn lookup_authid_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    rolename: &str,
) -> PgResult<Option<AuthIdRow<'mcx>>> {
    let tuple = SearchSysCache1(mcx, AUTHNAME, SysCacheKey::Str(rolename))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = project_authid(mcx, &tup)?;
    ReleaseSysCache(tup);
    Ok(Some(row))
}

fn project_authid<'mcx>(mcx: Mcx<'mcx>, tup: &FormedTuple<'_>) -> PgResult<AuthIdRow<'mcx>> {
    Ok(AuthIdRow {
        oid: getattr_oid(mcx, AUTHOID, tup, Anum_pg_authid_oid)?,
        rolname: getattr_name(mcx, AUTHOID, tup, Anum_pg_authid_rolname)?,
        rolsuper: getattr_bool(mcx, AUTHOID, tup, Anum_pg_authid_rolsuper)?,
        rolcanlogin: getattr_bool(mcx, AUTHOID, tup, Anum_pg_authid_rolcanlogin)?,
        rolreplication: getattr_bool(mcx, AUTHOID, tup, Anum_pg_authid_rolreplication)?,
        rolbypassrls: getattr_bool(mcx, AUTHOID, tup, Anum_pg_authid_rolbypassrls)?,
        rolconnlimit: getattr_i32(mcx, AUTHOID, tup, Anum_pg_authid_rolconnlimit)?,
    })
}

/// Project one `pg_enum` `FormedTuple` to an [`EnumTupleData`]: the
/// `(Form_pg_enum) GETSTRUCT(tup)` columns enum.c reads plus the tuple-header
/// `xmin`/`xmin_committed` `check_safe_enum_use` needs.
fn project_enum(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>) -> PgResult<EnumTupleData> {
    let header = tup
        .tuple
        .t_data
        .as_ref()
        .expect("pg_enum tuple has a header");
    Ok(EnumTupleData {
        oid: getattr_oid(mcx, cache_id, tup, Anum_pg_enum_oid as i32)?,
        enumtypid: getattr_oid(mcx, cache_id, tup, Anum_pg_enum_enumtypid as i32)?,
        enumlabel: getattr_namedata(mcx, cache_id, tup, Anum_pg_enum_enumlabel as i32)?,
        xmin_committed: HeapTupleHeaderXminCommitted(header),
        xmin: HeapTupleHeaderGetXmin(header),
    })
}

/// `SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumval))` projected to an
/// [`EnumTupleData`].
pub(crate) fn lookup_enum_by_oid(enumval: Oid) -> PgResult<Option<EnumTupleData>> {
    let scratch = MemoryContext::new("syscache enum-by-oid projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, ENUMOID, SysCacheKey::Value(KeyDatum::from_oid(enumval)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = project_enum(mcx, ENUMOID, &tup)?;
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache2(ENUMTYPOIDNAME, ObjectIdGetDatum(enumtypoid),
/// CStringGetDatum(name))` projected to an [`EnumTupleData`].
pub(crate) fn lookup_enum_by_typoid_name(
    enumtypoid: Oid,
    name: &str,
) -> PgResult<Option<EnumTupleData>> {
    let scratch = MemoryContext::new("syscache enum-by-typoid-name projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache2(
        mcx,
        ENUMTYPOIDNAME,
        SysCacheKey::Value(KeyDatum::from_oid(enumtypoid)),
        SysCacheKey::Str(name),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = project_enum(mcx, ENUMTYPOIDNAME, &tup)?;
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache1(LANGOID, language_id)` projected to the language facts
/// `fmgr_info_other_lang` / `CheckFunctionValidatorAccess` read.
pub(crate) fn lookup_language<'mcx>(
    mcx: Mcx<'mcx>,
    language_id: Oid,
) -> PgResult<Option<LangInfo<'mcx>>> {
    let tuple = SearchSysCache1(mcx, LANGOID, SysCacheKey::Value(KeyDatum::from_oid(language_id)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let info = LangInfo {
        lanplcallfoid: getattr_oid(mcx, LANGOID, &tup, Anum_pg_language_lanplcallfoid)?,
        lanvalidator: getattr_oid(mcx, LANGOID, &tup, Anum_pg_language_lanvalidator)?,
        lanname: getattr_name(mcx, LANGOID, &tup, Anum_pg_language_lanname)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(info))
}

/// `GetSysCacheOid1(LANGNAME, Anum_pg_language_oid, CStringGetDatum(langname))`
/// (the syscache leg of `get_language_oid`, proclang.c): the language's OID by
/// name, or `InvalidOid` on a cache miss. The `proclang` owner wraps this with
/// the `missing_ok` error decision.
pub(crate) fn language_oid_by_name(langname: &str) -> PgResult<Oid> {
    let scratch = MemoryContext::new("syscache language oid-by-name");
    GetSysCacheOid(
        scratch.mcx(),
        LANGNAME,
        Anum_pg_language_oid as i16,
        SysCacheKey::Str(langname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchSysCache1(LANGNAME, PointerGetDatum(languageName))` (proclang.c
/// pre-existing-definition check): the writable `pg_language` tuple by name
/// plus its `oid`/`lanowner`, or `None` if no such language exists.
pub(crate) fn language_tuple_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    langname: &str,
) -> PgResult<Option<(FormedTuple<'mcx>, FormData_pg_language)>> {
    let tuple = SearchSysCache1(mcx, LANGNAME, SysCacheKey::Str(langname))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // Form_pg_language oldform = (Form_pg_language) GETSTRUCT(oldtup);
    let form = FormData_pg_language {
        oid: getattr_oid(mcx, LANGNAME, &tup, Anum_pg_language_oid)?,
        lanowner: getattr_oid(mcx, LANGNAME, &tup, Anum_pg_language_lanowner)?,
    };
    // The caller (replace branch) keeps the tuple for heap_modify_tuple; the
    // C `ReleaseSysCache(oldtup)` releases the catcache pin, but the repo's
    // SearchSysCache1 already returns an owned (mcx-allocated) FormedTuple, so
    // no pin is held past return.
    Ok(Some((tup, form)))
}

const Anum_pg_language_oid: i32 = 1;
const Anum_pg_language_lanowner: i32 = 3;

/// `SearchSysCache2(RULERELNAME, ev_class, rulename)` projected to the writable
/// `pg_rewrite` tuple plus its deformed fixed columns (rewriteDefine.c). `None`
/// on a cache miss.
pub(crate) fn rule_tuple_by_relname<'mcx>(
    mcx: Mcx<'mcx>,
    ev_class: Oid,
    rulename: &str,
) -> PgResult<Option<(FormedTuple<'mcx>, types_catalog::pg_rewrite::FormData_pg_rewrite)>> {
    use types_catalog::pg_rewrite::{
        Anum_pg_rewrite_ev_class, Anum_pg_rewrite_ev_enabled, Anum_pg_rewrite_ev_type,
        Anum_pg_rewrite_is_instead, Anum_pg_rewrite_oid, Anum_pg_rewrite_rulename,
        FormData_pg_rewrite,
    };
    let tuple = SearchSysCache2(
        mcx,
        RULERELNAME,
        SysCacheKey::Value(KeyDatum::from_oid(ev_class)),
        SysCacheKey::Str(rulename),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // (Form_pg_rewrite) GETSTRUCT(tup): the fixed-width scalar columns.
    let name_bytes =
        SysCacheGetAttrNotNull(mcx, RULERELNAME, &tup, Anum_pg_rewrite_rulename as i32)?;
    let mut rulename_image = [0u8; 64];
    if let Datum::ByRef(b) = &name_bytes {
        let n = b.len().min(64);
        rulename_image[..n].copy_from_slice(&b[..n]);
    }
    let form = FormData_pg_rewrite {
        oid: getattr_oid(mcx, RULERELNAME, &tup, Anum_pg_rewrite_oid as i32)?,
        rulename: rulename_image,
        ev_class: getattr_oid(mcx, RULERELNAME, &tup, Anum_pg_rewrite_ev_class as i32)?,
        ev_type: getattr_char(mcx, RULERELNAME, &tup, Anum_pg_rewrite_ev_type as i32)? as u8,
        ev_enabled: getattr_char(mcx, RULERELNAME, &tup, Anum_pg_rewrite_ev_enabled as i32)? as u8,
        is_instead: getattr_bool(mcx, RULERELNAME, &tup, Anum_pg_rewrite_is_instead as i32)?,
    };
    Ok(Some((tup, form)))
}

/// `SearchSysCache1(RELOID, relid)` projected to `(relkind, relnamespace)`,
/// `None` on a cache miss (rewriteDefine.c RangeVarCallbackForRenameRule).
pub(crate) fn class_relkind_namespace(relid: Oid) -> PgResult<Option<(u8, Oid)>> {
    const Anum_pg_class_relnamespace: i32 = 3;
    const Anum_pg_class_relkind: i32 = 18;
    let scratch = MemoryContext::new("syscache rule-class projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relkind = getattr_char(mcx, RELOID, &tup, Anum_pg_class_relkind)? as u8;
    let relnamespace = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relnamespace)?;
    Ok(Some((relkind, relnamespace)))
}

/// `SearchSysCache1(INDEXRELID, index_oid)` then whether `indpred` is non-null
/// (`!heap_attisnull(rd_indextuple, Anum_pg_index_indpred, NULL)`).
pub(crate) fn pg_index_has_predicate(index_oid: Oid) -> PgResult<Option<bool>> {
    let scratch = MemoryContext::new("syscache indpred projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, INDEXRELID, SysCacheKey::Value(KeyDatum::from_oid(index_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let (_value, is_null) = SysCacheGetAttr(mcx, INDEXRELID, &tup, Anum_pg_index_indpred)?;
    ReleaseSysCache(tup);
    Ok(Some(!is_null))
}

/// `ParseLongOption` + skip-with-WARNING over the deconstructed `proconfig`
/// element strings — the `TransformGUCArray(array, &names, &values)` body
/// (guc.c), driven off the already-decoded `"name=value"` element strings.
/// `value == NULL` (a bare `name`) raises the C
/// `ereport(WARNING, "could not parse setting for parameter \"%s\"")` and skips
/// the entry (C: `continue`). The split name/value pieces are copied into `mcx`.
fn transform_guc_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[PgString<'mcx>],
) -> PgResult<(PgVec<'mcx, PgString<'mcx>>, PgVec<'mcx, PgString<'mcx>>)> {
    let mut names = vec_with_capacity_in::<PgString<'mcx>>(mcx, elems.len())?;
    let mut values = vec_with_capacity_in::<PgString<'mcx>>(mcx, elems.len())?;
    for s in elems {
        // ParseLongOption(s, &name, &value).
        let (name, value) = guc_seams::parse_long_option::call(mcx, s.as_str())?;
        let Some(value) = value else {
            // ereport(WARNING, errcode(ERRCODE_SYNTAX_ERROR),
            //         errmsg("could not parse setting for parameter \"%s\"", name));
            ereport(WARNING)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "could not parse setting for parameter \"{}\"",
                    name.as_str()
                ))
                .finish(ErrorLocation::new("utils/misc/guc.c", 0, "TransformGUCArray"))?;
            continue;
        };
        names.push(name);
        values.push(value);
    }
    Ok((names, values))
}

/// `SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId))` projected to the
/// catalog facts `fmgr_info_cxt_security` reads (`fmgr.c`):
/// `pronargs`/`proisstrict`/`proretset`/`prolang`/`prosrc`/`probin`/
/// `prosecdef`/`proowner`/`proname`, the `TransformGUCArray`'d `proconfig`
/// names+values, and the tuple's raw xmin + TID (the C-function cache key). The
/// folded `security_definer` predicate is `prosecdef || proconfig-not-null`
/// (the C `FmgrHookIsNeeded(functionId)` term stays with the fmgr consumer,
/// which folds its hook check in). `Ok(None)` on a cache miss.
pub(crate) fn lookup_proc<'mcx>(mcx: Mcx<'mcx>, function_id: Oid) -> PgResult<Option<ProcInfo<'mcx>>> {
    let tuple = SearchSysCache1(mcx, PROCOID, SysCacheKey::Value(KeyDatum::from_oid(function_id)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    // procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple).
    let nargs = getattr_i16(mcx, PROCOID, &tup, Anum_pg_proc_pronargs)?;
    let strict = getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proisstrict)?;
    let retset = getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proretset)?;
    let prolang = getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_prolang)?;
    let prosecdef = getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_prosecdef)?;
    let proowner = getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_proowner)?;
    let proname = getattr_name(mcx, PROCOID, &tup, Anum_pg_proc_proname)?;

    // prolang switch (INTERNAL/C/SQL/else).
    let language = match prolang {
        INTERNAL_LANGUAGE_ID => ProcLanguage::Internal,
        C_LANGUAGE_ID => ProcLanguage::C,
        SQL_LANGUAGE_ID => ProcLanguage::Sql,
        _ => ProcLanguage::Other,
    };

    // prosrc = SysCacheGetAttrNotNull(PROCOID, ftup, Anum_pg_proc_prosrc)
    //          (BKI_FORCE_NOT_NULL); TextDatumGetCString(prosrcdatum). The
    // internal-by-name leg reads it; carry it always (it is non-null).
    let prosrc_datum = SysCacheGetAttrNotNull(mcx, PROCOID, &tup, Anum_pg_proc_prosrc)?;
    let prosrc = Some(varlena_seams::text_to_cstring_v::call(mcx, &prosrc_datum)?);

    // probin = SysCacheGetAttr(PROCOID, ftup, Anum_pg_proc_probin, &isnull):
    // only set for a C-language function; NULL otherwise.
    let (probin_datum, probin_isnull) = SysCacheGetAttr(mcx, PROCOID, &tup, Anum_pg_proc_probin)?;
    let probin = if probin_isnull {
        None
    } else {
        Some(varlena_seams::text_to_cstring_v::call(mcx, &probin_datum)?)
    };

    // datum = SysCacheGetAttr(PROCOID, ftup, Anum_pg_proc_proconfig, &isnull);
    // !isnull feeds the security_definer predicate, and TransformGUCArray(datum)
    // splits the text[] into the SET name/value lists.
    let (proconfig_datum, proconfig_isnull) =
        SysCacheGetAttr(mcx, PROCOID, &tup, Anum_pg_proc_proconfig)?;
    let (proconfig_names, proconfig_values) = if proconfig_isnull {
        (vec_with_capacity_in(mcx, 0)?, vec_with_capacity_in(mcx, 0)?)
    } else {
        let bytes = match &proconfig_datum {
            Datum::ByRef(b) => &b[..],
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => {
                return Err(PgError::error(
                    "syscache projection: proconfig attribute is by-value",
                ))
            }
        };
        // deconstruct the text[] image, then TransformGUCArray over the entries.
        let elems = arrayfuncs_seams::text_array_to_strings_bytes::call(mcx, bytes)?;
        transform_guc_array(mcx, &elems)?
    };

    // C: prosecdef || !proconfig-is-null routes through fmgr_security_definer.
    // FmgrHookIsNeeded(functionId) is the fmgr consumer's term (it folds its
    // plugin-hook check in).
    let security_definer = prosecdef || !proconfig_isnull;

    // HeapTupleHeaderGetRawXmin(procedureTuple->t_data) + procedureTuple->t_self.
    let xmin = HeapTupleHeaderGetRawXmin(
        tup.tuple
            .t_data
            .as_ref()
            .expect("pg_proc tuple has a header"),
    );
    let tid = tup.tuple.t_self;

    ReleaseSysCache(tup);
    Ok(Some(ProcInfo {
        nargs,
        strict,
        retset,
        language,
        prosrc,
        probin,
        security_definer,
        prosecdef,
        prolang,
        proname: Some(proname),
        proowner,
        proconfig_names,
        proconfig_values,
        xmin,
        tid,
    }))
}

/// `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))` projected to the
/// `pg_proc` facts `internal_get_result_type` (funcapi.c) reads:
/// `prorettype`/`proretset`/`pronargs`/`proargtypes` (the declared input-type
/// `oidvector`) and `NameStr(proname)`. `Ok(None)` on a cache miss.
pub(crate) fn lookup_proc_result_info<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<ProcResultInfo<'mcx>>> {
    let tuple = SearchSysCache1(mcx, PROCOID, SysCacheKey::Value(KeyDatum::from_oid(funcid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    let prorettype = getattr_oid(mcx, PROCOID, &tup, Anum_pg_proc_prorettype)?;
    let proretset = getattr_bool(mcx, PROCOID, &tup, Anum_pg_proc_proretset)?;
    let pronargs = getattr_i16(mcx, PROCOID, &tup, Anum_pg_proc_pronargs)?;
    let proname = getattr_name(mcx, PROCOID, &tup, Anum_pg_proc_proname)?;

    // procedureStruct->proargtypes is an oidvector (BKI_FORCE_NOT_NULL); read
    // its element OIDs directly off the on-disk image (== C's vec->values).
    let proargtypes_datum = SysCacheGetAttrNotNull(mcx, PROCOID, &tup, Anum_pg_proc_proargtypes)?;
    let bytes = match &proargtypes_datum {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error(
                "syscache projection: proargtypes attribute is by-value",
            ))
        }
    };
    let proargtypes = arrayfuncs_seams::oidvector_to_oids_bytes::call(mcx, bytes)?;

    ReleaseSysCache(tup);
    Ok(Some(ProcResultInfo {
        prorettype,
        proretset,
        pronargs,
        proargtypes,
        proname,
    }))
}

/// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` then
/// `Form_pg_index.indnatts`/`indnkeyatts` + `SysCacheGetAttrNotNull(INDEXRELID,
/// tuple, Anum_pg_index_indclass)` projected to the per-column opclass
/// `oidvector` (`get_index_column_opclass`, lsyscache.c). Returns `(indnatts,
/// indnkeyatts, indclass)` with the opclass OIDs copied into `mcx`. `Ok(None)`
/// on a cache miss (the C `return InvalidOid`).
pub(crate) fn pg_index_indclass<'mcx>(
    mcx: Mcx<'mcx>,
    index_oid: Oid,
) -> PgResult<Option<(i16, i16, PgVec<'mcx, Oid>)>> {
    let tuple = SearchSysCache1(mcx, INDEXRELID, SysCacheKey::Value(KeyDatum::from_oid(index_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    let indnatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnatts)?;
    let indnkeyatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnkeyatts)?;

    // datum = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indclass);
    // indclass = (oidvector *) DatumGetPointer(datum); read ->values[0..dim1].
    let indclass_datum = SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indclass)?;
    let bytes = match &indclass_datum {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error(
                "syscache projection: indclass attribute is by-value",
            ))
        }
    };
    let indclass = arrayfuncs_seams::oidvector_to_oids_bytes::call(mcx, bytes)?;

    ReleaseSysCache(tup);
    Ok(Some((indnatts, indnkeyatts, indclass)))
}

/// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId))` then `heap_copytuple`
/// projected to the `pg_index` row the relcache's `RelationInitIndexAccessInfo`
/// (relcache.c) consumes off `rd_indextuple`: the fixed `Form_pg_index` scalars
/// plus the variable-length `indkey`/`indcollation`/`indclass`/`indoption`
/// arrays (which the C reads with `fastgetattr`/`heap_getattr` from the copied
/// tuple). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller raises
/// `cache lookup failed for index %u`.
pub(crate) fn search_pg_index_info<'mcx>(
    mcx: Mcx<'mcx>,
    index_oid: Oid,
) -> PgResult<Option<types_cache::PgIndexInfo<'mcx>>> {
    let tuple = SearchSysCache1(mcx, INDEXRELID, SysCacheKey::Value(KeyDatum::from_oid(index_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    // Fixed Form_pg_index scalars (GETSTRUCT in C).
    let indexrelid = getattr_oid(mcx, INDEXRELID, &tup, Anum_pg_index_indexrelid)?;
    let indrelid = getattr_oid(mcx, INDEXRELID, &tup, Anum_pg_index_indrelid)?;
    let indnatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnatts)?;
    let indnkeyatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnkeyatts)?;
    let indisunique = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisunique)?;
    let indnullsnotdistinct =
        getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indnullsnotdistinct)?;
    let indisprimary = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisprimary)?;
    let indisexclusion = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisexclusion)?;
    let indimmediate = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indimmediate)?;
    let indisclustered = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisclustered)?;
    let indisvalid = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisvalid)?;
    let indcheckxmin = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indcheckxmin)?;
    let indisready = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisready)?;
    let indislive = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indislive)?;
    let indisreplident = getattr_bool(mcx, INDEXRELID, &tup, Anum_pg_index_indisreplident)?;

    // Variable-length vectors. The C reads these off rd_indextuple with
    // fastgetattr (indkey is BKI_FORCE_NOT_NULL and accessed directly; the
    // CATALOG_VARLEN trio indcollation/indclass/indoption with heap_getattr).
    // All four are BKI_FORCE_NOT_NULL, so SysCacheGetAttrNotNull is faithful.
    let by_ref_bytes = |d: &Datum<'mcx>, name: &str| -> PgResult<PgVec<'mcx, u8>> {
        match d {
            Datum::ByRef(b) => {
                let mut out = vec_with_capacity_in::<u8>(mcx, b.len())?;
                out.extend_from_slice(b);
                Ok(out)
            }
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => Err(PgError::error(format!(
                "syscache projection: {name} attribute is by-value"
            ))),
        }
    };

    // indkey int2vector -> AttrNumber (i16) values.
    let indkey_datum = SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indkey)?;
    let indkey_bytes = by_ref_bytes(&indkey_datum, "indkey")?;
    let indkey = arrayfuncs_seams::int2vector_to_i16s_bytes::call(mcx, &indkey_bytes)?;

    // indcollation oidvector -> Oid values.
    let indcollation_datum =
        SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indcollation)?;
    let indcollation_bytes = by_ref_bytes(&indcollation_datum, "indcollation")?;
    let indcollation =
        arrayfuncs_seams::oidvector_to_oids_bytes::call(mcx, &indcollation_bytes)?;

    // indclass oidvector -> Oid values.
    let indclass_datum = SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indclass)?;
    let indclass_bytes = by_ref_bytes(&indclass_datum, "indclass")?;
    let indclass = arrayfuncs_seams::oidvector_to_oids_bytes::call(mcx, &indclass_bytes)?;

    // indoption int2vector -> i16 values.
    let indoption_datum =
        SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indoption)?;
    let indoption_bytes = by_ref_bytes(&indoption_datum, "indoption")?;
    let indoption = arrayfuncs_seams::int2vector_to_i16s_bytes::call(mcx, &indoption_bytes)?;

    ReleaseSysCache(tup);
    Ok(Some(types_cache::PgIndexInfo {
        indexrelid,
        indrelid,
        indnatts,
        indnkeyatts,
        indisunique,
        indnullsnotdistinct,
        indisprimary,
        indisexclusion,
        indimmediate,
        indisclustered,
        indisvalid,
        indcheckxmin,
        indisready,
        indislive,
        indisreplident,
        indkey,
        indcollation,
        indclass,
        indoption,
    }))
}

/// amutils.c `indexam_property`: `SearchSysCache1(RELOID,
/// ObjectIdGetDatum(index_oid))` + `GETSTRUCT` projected to `(relkind, relam,
/// relnatts)`. `Ok(None)` on a cache miss (the C `!HeapTupleIsValid` →
/// `PG_RETURN_NULL`).
pub(crate) fn amutils_index_relation(
    index_oid: Oid,
) -> PgResult<Option<amutils_seams::IndexRelationInfo>> {
    // The fixed-width Form_pg_class fields amutils reads are scalar — a throwaway
    // context suffices for the catcache marshal (the projected struct is Copy).
    let ctx = mcx::MemoryContext::new("amutils_index_relation");
    let mcx = ctx.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(index_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let info = amutils_seams::IndexRelationInfo {
        relkind: getattr_char(mcx, RELOID, &tup, Anum_pg_class_relkind)? as u8,
        relam: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relam)?,
        relnatts: getattr_i16(mcx, RELOID, &tup, Anum_pg_class_relnatts)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(info))
}

/// amutils.c `indexam_property`: `SearchSysCache1(INDEXRELID,
/// ObjectIdGetDatum(index_oid))` + `GETSTRUCT` + `SysCacheGetAttrNotNull(...
/// Anum_pg_index_indoption)` projected to `(indexrelid, indnatts, indnkeyatts,
/// indoption)`. `Ok(None)` on a cache miss.
pub(crate) fn amutils_index_form(
    index_oid: Oid,
) -> PgResult<Option<amutils_seams::IndexFormInfo>> {
    let ctx = mcx::MemoryContext::new("amutils_index_form");
    let mcx = ctx.mcx();
    let tuple = SearchSysCache1(mcx, INDEXRELID, SysCacheKey::Value(KeyDatum::from_oid(index_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    let indexrelid = getattr_oid(mcx, INDEXRELID, &tup, Anum_pg_index_indexrelid)?;
    let indnatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnatts)?;
    let indnkeyatts = getattr_i16(mcx, INDEXRELID, &tup, Anum_pg_index_indnkeyatts)?;

    // datum = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indoption);
    // indoption = ((int2vector *) DatumGetPointer(datum)); read ->values[..].
    let indoption_datum =
        SysCacheGetAttrNotNull(mcx, INDEXRELID, &tup, Anum_pg_index_indoption)?;
    let bytes = match &indoption_datum {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error(
                "syscache projection: indoption attribute is by-value",
            ))
        }
    };
    let indoption_vec = arrayfuncs_seams::int2vector_to_i16s_bytes::call(mcx, bytes)?;
    // The projected struct outlives the throwaway context, so copy out to a
    // std Vec (the seam's IndexFormInfo carries an owning Vec, not an mcx slice).
    let indoption = indoption_vec.iter().copied().collect();

    ReleaseSysCache(tup);
    Ok(Some(amutils_seams::IndexFormInfo {
        indexrelid,
        indnatts,
        indnkeyatts,
        indoption,
    }))
}

/// `SearchSysCache1(COLLOID, collation)` then
/// `(get_namespace_name(collnamespace), NameStr(collname))`, both as raw name
/// bytes copied into `mcx` (`ri_GenerateQualCollation`).
pub(crate) fn collation_qualified_name<'mcx>(
    mcx: Mcx<'mcx>,
    collation: Oid,
) -> PgResult<Option<(PgVec<'mcx, u8>, PgVec<'mcx, u8>)>> {
    let tuple = SearchSysCache1(mcx, COLLOID, SysCacheKey::Value(KeyDatum::from_oid(collation)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let collnamespace = getattr_oid(mcx, COLLOID, &tup, Anum_pg_collation_collnamespace)?;
    let collname = getattr_name_bytes(mcx, COLLOID, &tup, Anum_pg_collation_collname)?;
    ReleaseSysCache(tup);
    let nspname = match lsyscache_seams::get_namespace_name::call(mcx, collnamespace)? {
        Some(s) => mcx::slice_in(mcx, s.as_str().as_bytes())?,
        None => return Ok(None),
    };
    Ok(Some((nspname, collname)))
}

/// `SearchSysCache1(RELOID, relid)` + `GETSTRUCT` of the full `Form_pg_class`
/// tuple (relcache Phase3 nailed-entry refill).
pub(crate) fn search_pg_class_full_form<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<PgClassFullForm<'mcx>>> {
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = PgClassFullForm {
        relname: getattr_name(mcx, RELOID, &tup, Anum_pg_class_relname)?,
        relnamespace: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relnamespace)?,
        reltype: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reltype)?,
        reloftype: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reloftype)?,
        relowner: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relowner)?,
        relam: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relam)?,
        relfilenode: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relfilenode)?,
        reltablespace: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reltablespace)?,
        relpages: getattr_i32(mcx, RELOID, &tup, Anum_pg_class_relpages)?,
        reltuples: getattr_f32(mcx, RELOID, &tup, Anum_pg_class_reltuples)?,
        relallvisible: getattr_i32(mcx, RELOID, &tup, Anum_pg_class_relallvisible)?,
        reltoastrelid: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reltoastrelid)?,
        relhasindex: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relhasindex)?,
        relisshared: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relisshared)?,
        relpersistence: getattr_char(mcx, RELOID, &tup, Anum_pg_class_relpersistence)?,
        relkind: getattr_char(mcx, RELOID, &tup, Anum_pg_class_relkind)?,
        relnatts: getattr_i16(mcx, RELOID, &tup, Anum_pg_class_relnatts)?,
        relchecks: getattr_i16(mcx, RELOID, &tup, Anum_pg_class_relchecks)?,
        relhasrules: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relhasrules)?,
        relhastriggers: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relhastriggers)?,
        relhassubclass: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relhassubclass)?,
        relrowsecurity: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relrowsecurity)?,
        relforcerowsecurity: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relforcerowsecurity)?,
        relispopulated: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relispopulated)?,
        relreplident: getattr_char(mcx, RELOID, &tup, Anum_pg_class_relreplident)?,
        relispartition: getattr_bool(mcx, RELOID, &tup, Anum_pg_class_relispartition)?,
        relrewrite: getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relrewrite)?,
        relfrozenxid: getattr_u32(mcx, RELOID, &tup, Anum_pg_class_relfrozenxid)?,
        relminmxid: getattr_u32(mcx, RELOID, &tup, Anum_pg_class_relminmxid)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/* ===========================================================================
 * ACL / owner catalog-row projections (the aclmask/aclcheck family in
 * catalog/aclchk.c — the F0 keystone). Each reads the object's owner OID and
 * its `aclitem[]` ACL off `SearchSysCache*` + `GETSTRUCT` + `SysCacheGetAttr`,
 * decoding the ACL column into its [`AclItem`] elements (the `Acl *` /
 * `ArrayType` payload C's `aclmask()` consumes). A SQL-null ACL column crosses
 * as `None` (where aclchk builds the hardwired `acldefault`); a cache miss is
 * `Ok(None)`.
 * ======================================================================== */

use types_acl::AclItem;
use types_cache::syscache::{ClassOwnerAcl, NamespaceOwnerAcl, ObjectOwnerAcl, TypeOwnerAcl};

// `catalog/pg_namespace.h` attribute numbers.
const Anum_pg_namespace_nspowner: i32 = 3;
const Anum_pg_namespace_nspacl: i32 = 4;

// `catalog/pg_class.h` ACL attribute number.
const Anum_pg_class_relacl: i32 = 13;

// `catalog/pg_attribute.h` ACL + attisdropped attribute numbers.
const Anum_pg_attribute_attacl: i32 = 22;
const Anum_pg_attribute_attisdropped_acl: i32 = 17;

// `catalog/pg_type.h` attribute numbers (ACL path). The shared columns
// (`oid`/`typowner`/`typtype`/`typsubscript`/`typelem`) are defined once in the
// consolidated `pg_type.h` block above.
const Anum_pg_type_typacl: i32 = 32;

// `catalog/pg_parameter_acl.h` ACL attribute number (`paracl[1]`).
const Anum_pg_parameter_acl_paracl: i32 = 3;

/// `TYPTYPE_MULTIRANGE` (`catalog/pg_type.h`).
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
/// `F_ARRAY_SUBSCRIPT_HANDLER` (`fmgroids.h`, `pg_proc.dat` oid 6179) — the
/// `typsubscript` value that, with a valid `typelem`, marks a true array type.
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

/// `sizeof(AclItem)` (`utils/acl.h`) — 16 bytes on every platform (hardcoded in
/// `pg_type.dat`: `aclitem` `typlen => 16`).
const SIZEOF_ACLITEM: usize = 16;

/// `IsTrueArrayType(typeForm)` (`catalog/pg_type.h`):
/// `OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER`.
fn is_true_array_type(typelem: Oid, typsubscript: Oid) -> bool {
    OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

/// `DatumGetAclP(aclDatum)` then walk the `aclitem[]` elements: detoast the
/// stored ACL varlena (C `DatumGetAclP` == `DatumGetArrayTypePCopy`), then read
/// `ACL_NUM(acl) = ARR_DIMS(acl)[0]` fixed-16-byte items from `ACL_DAT(acl) =
/// ARR_DATA_PTR(acl)`. A stored ACL is always a well-formed 1-D no-nulls
/// `aclitem` array (`check_acl`); a 0-dimension/empty image yields an empty
/// vector. Allocated in the caller's `mcx`.
fn decode_acl<'mcx>(mcx: Mcx<'mcx>, raw: &[u8]) -> PgResult<PgVec<'mcx, AclItem>> {
    // DatumGetArrayTypeP + ARR_NDIM/ARR_DIMS[0]/ARR_DATA_PTR (reusing the
    // pg_constraint array-header decoder above).
    let (arr, ndim, _hasnull, _elemtype, dim0, data_off) = detoast_array_header(mcx, raw)?;
    let n = if ndim >= 1 { dim0.max(0) as usize } else { 0 };
    let mut items: PgVec<'mcx, AclItem> = vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let off = data_off + i * SIZEOF_ACLITEM;
        let b = arr.get(off..off + SIZEOF_ACLITEM).ok_or_else(|| {
            PgError::error("syscache ACL projection: truncated aclitem array data")
        })?;
        // AclItem { ai_grantee: Oid, ai_grantor: Oid, ai_privs: AclMode (u64) }.
        let ai_grantee = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        let ai_grantor = u32::from_ne_bytes([b[4], b[5], b[6], b[7]]);
        let ai_privs = u64::from_ne_bytes([
            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
        ]);
        items.push(AclItem {
            ai_grantee,
            ai_grantor,
            ai_privs,
        });
    }
    Ok(items)
}

/// Read an ACL column off a held tuple: `SysCacheGetAttr(cacheId, tup, attnum)`
/// then, when not SQL-null, [`decode_acl`]. `Some(items)` for a present ACL,
/// `None` for a SQL-null column.
fn getattr_acl<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<Option<PgVec<'mcx, AclItem>>> {
    let (value, is_null) = SysCacheGetAttr(mcx, cache_id, tup, attnum)?;
    if is_null {
        return Ok(None);
    }
    match &value {
        Datum::ByRef(b) => Ok(Some(decode_acl(mcx, &b[..])?)),
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error(
            "syscache ACL projection: aclitem[] column is by-value",
        )),
    }
}

/// `pg_class_aclmask_ext`'s catalog read (aclchk.c): `SearchSysCache1(RELOID,
/// table_oid)` -> `relowner`/`relkind`/`relnamespace` + the decoded `relacl`.
pub(crate) fn pg_class_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
) -> PgResult<Option<ClassOwnerAcl<'mcx>>> {
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(table_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relowner = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relowner)?;
    let relkind = getattr_char(mcx, RELOID, &tup, Anum_pg_class_relkind)?;
    let relnamespace = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relnamespace)?;
    let acl = getattr_acl(mcx, RELOID, &tup, Anum_pg_class_relacl)?;
    ReleaseSysCache(tup);
    Ok(Some(ClassOwnerAcl {
        relowner,
        relkind,
        relnamespace,
        acl,
    }))
}

/// `pg_attribute_aclmask_ext`'s column read (aclchk.c): `SearchSysCache2(ATTNUM,
/// table_oid, attnum)` -> `(attisdropped, decoded attacl)`. `Ok(None)` on a
/// cache miss (no such pg_attribute row); the relation owner is fetched
/// separately by the caller (`pg_class_owner_acl`).
pub(crate) fn pg_attribute_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    attnum: i16,
) -> PgResult<Option<(bool, Option<PgVec<'mcx, AclItem>>)>> {
    let tuple = SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(table_oid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // attributeForm->attisdropped.
    let attisdropped = getattr_bool(mcx, ATTNUM, &tup, Anum_pg_attribute_attisdropped_acl)?;
    let acl = getattr_acl(mcx, ATTNUM, &tup, Anum_pg_attribute_attacl)?;
    ReleaseSysCache(tup);
    Ok(Some((attisdropped, acl)))
}

/// `pg_namespace_aclmask_ext`'s catalog read (aclchk.c):
/// `SearchSysCache1(NAMESPACEOID, nsp_oid)` -> `nspowner` + decoded `nspacl`.
pub(crate) fn pg_namespace_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    nsp_oid: Oid,
) -> PgResult<Option<NamespaceOwnerAcl<'mcx>>> {
    let tuple = SearchSysCache1(mcx, NAMESPACEOID, SysCacheKey::Value(KeyDatum::from_oid(nsp_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let nspowner = getattr_oid(mcx, NAMESPACEOID, &tup, Anum_pg_namespace_nspowner)?;
    let acl = getattr_acl(mcx, NAMESPACEOID, &tup, Anum_pg_namespace_nspacl)?;
    ReleaseSysCache(tup);
    Ok(Some(NamespaceOwnerAcl { nspowner, acl }))
}

/// `pg_type_aclmask_ext`'s catalog read (aclchk.c):
/// `SearchSysCache1(TYPEOID, type_oid)` -> `typowner`/`typacl`, resolving the
/// true-array-element redirect (`IsTrueArrayType` -> `typelem`) and the
/// multirange redirect (`typtype == TYPTYPE_MULTIRANGE` ->
/// `get_multirange_range`) before projecting the effective type's `(owner,
/// acl)`. Each redirect re-fetches by OID; a miss at any step is `Ok(None)`.
pub(crate) fn pg_type_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
) -> PgResult<Option<TypeOwnerAcl<'mcx>>> {
    let tuple = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(KeyDatum::from_oid(type_oid)))?;
    let Some(mut tup) = tuple else {
        return Ok(None);
    };

    // "True" array types don't manage their own permissions; consult the
    // element type instead.
    let typelem = getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typelem)?;
    let typsubscript = getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typsubscript)?;
    if is_true_array_type(typelem, typsubscript) {
        ReleaseSysCache(tup);
        let elt = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(KeyDatum::from_oid(typelem)))?;
        let Some(elt_tup) = elt else {
            return Ok(None);
        };
        tup = elt_tup;
    }

    // Likewise, multirange types consult the associated range type. (After the
    // array step, to get the right answer for arrays of multiranges.)
    let typtype = getattr_char(mcx, TYPEOID, &tup, Anum_pg_type_typtype)?;
    if typtype == TYPTYPE_MULTIRANGE {
        let oid = getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_oid)?;
        let rangetype = lsyscache_seams::get_multirange_range::call(oid)?;
        ReleaseSysCache(tup);
        let rng = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(KeyDatum::from_oid(rangetype)))?;
        let Some(rng_tup) = rng else {
            return Ok(None);
        };
        tup = rng_tup;
    }

    let typowner = getattr_oid(mcx, TYPEOID, &tup, Anum_pg_type_typowner)?;
    let acl = getattr_acl(mcx, TYPEOID, &tup, Anum_pg_type_typacl)?;
    ReleaseSysCache(tup);
    Ok(Some(TypeOwnerAcl { typowner, acl }))
}

/// `object_aclmask_ext`'s generic catalog read (aclchk.c):
/// `SearchSysCache1(cacheid, objectid)` -> `SysCacheGetAttrNotNull(owner_attnum)`
/// (owner) + decoded `SysCacheGetAttr(acl_attnum)` (ACL). The caller resolves
/// `cacheid`/`owner_attnum`/`acl_attnum` for its `classid`.
pub(crate) fn object_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    cacheid: i32,
    objectid: Oid,
    owner_attnum: i16,
    acl_attnum: i16,
) -> PgResult<Option<ObjectOwnerAcl<'mcx>>> {
    let tuple = SearchSysCache1(mcx, cacheid, SysCacheKey::Value(KeyDatum::from_oid(objectid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    // ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(cacheid, tuple,
    //                            get_object_attnum_owner(classid))).
    let owner = getattr_oid(mcx, cacheid, &tup, owner_attnum as i32)?;
    let acl = getattr_acl(mcx, cacheid, &tup, acl_attnum as i32)?;
    ReleaseSysCache(tup);
    Ok(Some(ObjectOwnerAcl { owner, acl }))
}

/// `pg_parameter_aclmask`'s catalog read (aclchk.c):
/// `SearchSysCache1(PARAMETERACLNAME, CStringGetTextDatum(parname))` -> decoded
/// `paracl`. Outer `Option`: `None` = cache miss (no entry — the C
/// `ACL_NO_RIGHTS` case); inner `Option`: `None` = SQL-null `paracl`.
pub(crate) fn parameter_acl_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    parname: &str,
) -> PgResult<Option<Option<PgVec<'mcx, AclItem>>>> {
    let tuple = SearchSysCache1(mcx, PARAMETERACLNAME, SysCacheKey::Str(parname))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let acl = getattr_acl(mcx, PARAMETERACLNAME, &tup, Anum_pg_parameter_acl_paracl)?;
    ReleaseSysCache(tup);
    Ok(Some(acl))
}

/// `pg_parameter_acl_aclmask`'s catalog read (aclchk.c):
/// `SearchSysCache1(PARAMETERACLOID, acl_oid)` -> decoded `paracl`. Outer
/// `Option`: `None` = cache miss (caller raises "parameter ACL with OID %u does
/// not exist"); inner `Option`: `None` = SQL-null `paracl`.
pub(crate) fn parameter_acl_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    acl_oid: Oid,
) -> PgResult<Option<Option<PgVec<'mcx, AclItem>>>> {
    let tuple = SearchSysCache1(mcx, PARAMETERACLOID, SysCacheKey::Value(KeyDatum::from_oid(acl_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let acl = getattr_acl(mcx, PARAMETERACLOID, &tup, Anum_pg_parameter_acl_paracl)?;
    ReleaseSysCache(tup);
    Ok(Some(acl))
}

/* ===========================================================================
 * pg_statistic syscache reads (STATRELATTINH).
 *
 * selfuncs.c / lsyscache.c hold a `pg_statistic` row pinned across several
 * reads as a `HeapTuple` (`VariableStatData.statsTuple`,
 * `get_attstatsslot`'s `statstuple`). In C that pointer lives in the syscache
 * until `ReleaseSysCache`. The owned model returns a `FormedTuple` by value, so
 * to honor the C "pin a tuple, read it repeatedly, then release it" contract we
 * stash the owned tuple — alongside the `MemoryContext` its arena lives in —
 * in a heap box and hand its address out as the [`StatsTuple`] pointer (the
 * exact C `HeapTuple` carrier). The field-read seams reborrow it; the release
 * seam reclaims the box (dropping the context = `ReleaseSysCache`).
 * ========================================================================= */

/// The owner-side backing of a [`StatsTuple`]: the pinned `pg_statistic`
/// [`FormedTuple`] plus the [`MemoryContext`] whose arena holds it. Leaked to a
/// raw pointer so the tuple survives across the selectivity code's repeated
/// reads, exactly as the C syscache keeps the `HeapTuple` alive until
/// `ReleaseSysCache`.
struct StatsTupleHolder {
    /// The arena the tuple's bytes live in. Kept alive (not dropped) for as
    /// long as the holder is leaked; dropped on `release_stats_tuple`.
    _ctx: MemoryContext,
    /// The pinned `pg_statistic` tuple. Its `'static` lifetime is sound only
    /// because `_ctx` outlives every borrow (the holder owns both, and is only
    /// dropped wholesale in `release_stats_tuple`).
    tuple: FormedTuple<'static>,
}

/// Reborrow the [`StatsTupleHolder`] behind a [`StatsTuple`] pointer. Safe given
/// the contract: the pointer came from [`search_statrelattinh`] (a leaked
/// `Box<StatsTupleHolder>`) and has not yet been passed to
/// [`release_stats_tuple`].
fn stats_holder<'a>(stats_tuple: types_selfuncs::StatsTuple) -> &'a StatsTupleHolder {
    debug_assert!(!stats_tuple.ptr.is_null());
    unsafe { &*(stats_tuple.ptr as *const StatsTupleHolder) }
}

/// `SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(relid), Int16GetDatum(attnum),
/// BoolGetDatum(inherit))` (selfuncs.c `examine_simple_variable`, lsyscache.c
/// `get_attstatsslot_mcv`). Pins the `pg_statistic` row and returns it as a
/// [`StatsTuple`] (the C `HeapTuple`); the caller pairs it with
/// [`release_stats_tuple`] (`ReleaseSysCache`). `Ok(None)` on a cache miss.
pub(crate) fn search_statrelattinh<'mcx>(
    _mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    inherit: bool,
) -> PgResult<Option<types_selfuncs::StatsTuple>> {
    // The pinned tuple must outlive the search's transient `mcx`, so it gets its
    // own arena (held by the leaked holder, freed in release_stats_tuple).
    let ctx = MemoryContext::new("pg_statistic syscache pin");
    // Extend the tuple's lifetime to `'static` as it crosses out of the search's
    // borrow of `ctx`: it lives in `ctx`, which the holder owns and keeps alive
    // until `release_stats_tuple`. Decoupling the borrow here also lets `ctx` be
    // moved into the holder below.
    let result: Option<FormedTuple<'static>> = {
        let mcx = ctx.mcx();
        let found = SearchSysCache3(
            mcx,
            STATRELATTINH,
            SysCacheKey::Value(KeyDatum::from_oid(relid)),
            SysCacheKey::Value(KeyDatum::from_i16(attnum)),
            SysCacheKey::Value(KeyDatum::from_bool(inherit)),
        )?;
        found.map(|tup| unsafe {
            core::mem::transmute::<FormedTuple<'_>, FormedTuple<'static>>(tup)
        })
    };
    let Some(tuple) = result else {
        // !HeapTupleIsValid: drop the empty arena and report the miss.
        return Ok(None);
    };
    let holder = Box::new(StatsTupleHolder { _ctx: ctx, tuple });
    let ptr = Box::into_raw(holder) as *mut core::ffi::c_void;
    Ok(Some(types_selfuncs::StatsTuple { ptr }))
}

/// `ReleaseSysCache(tuple)` for a [`StatsTuple`] from [`search_statrelattinh`]:
/// reclaim the leaked holder (dropping its tuple and arena).
pub(crate) fn release_stats_tuple(stats_tuple: types_selfuncs::StatsTuple) {
    if stats_tuple.ptr.is_null() {
        return;
    }
    // Reconstruct and drop the box: frees the FormedTuple and its arena.
    let holder = unsafe { Box::from_raw(stats_tuple.ptr as *mut StatsTupleHolder) };
    drop(holder);
}

/// `((Form_pg_statistic) GETSTRUCT(statstuple))->stanullfrac` (pg_statistic.h):
/// the fraction of NULLs, read off the pinned `pg_statistic` tuple.
pub(crate) fn pg_statistic_stanullfrac(stats_tuple: types_selfuncs::StatsTuple) -> f32 {
    let holder = stats_holder(stats_tuple);
    let scratch = MemoryContext::new("pg_statistic stanullfrac");
    let mcx = scratch.mcx();
    // stanullfrac is a NOT NULL fixed-width float4 column, so heap_getattr never
    // returns null here (Form_pg_statistic GETSTRUCT in C reads it directly).
    let (value, isnull) = getattr_with_cache_tupdesc_stats(
        mcx,
        &holder.tuple,
        Anum_pg_statistic_stanullfrac as i32,
    );
    debug_assert!(!isnull);
    value.as_f32()
}

/// `((Form_pg_statistic) GETSTRUCT(statstuple))->stadistinct` (pg_statistic.h):
/// the distinct-value estimate, read off the pinned `pg_statistic` tuple.
pub(crate) fn pg_statistic_stadistinct(stats_tuple: types_selfuncs::StatsTuple) -> f32 {
    let holder = stats_holder(stats_tuple);
    let scratch = MemoryContext::new("pg_statistic stadistinct");
    let mcx = scratch.mcx();
    let (value, isnull) = getattr_with_cache_tupdesc_stats(
        mcx,
        &holder.tuple,
        Anum_pg_statistic_stadistinct as i32,
    );
    debug_assert!(!isnull);
    value.as_f32()
}

/// `((Form_pg_statistic) GETSTRUCT(statstuple))` projected to the fixed-width
/// slot metadata (`stakindN` / `staopN` / `stacollN`) for `get_attstatsslot`.
pub(crate) fn pg_statistic_slot_meta(
    stats_tuple: types_selfuncs::StatsTuple,
) -> PgResult<backend_utils_cache_syscache_seams::PgStatisticSlotMeta> {
    use backend_utils_cache_syscache_seams::{PgStatisticSlotMeta, STATISTIC_NUM_SLOTS};
    let holder = stats_holder(stats_tuple);
    let scratch = MemoryContext::new("pg_statistic slot meta");
    let mcx = scratch.mcx();
    let mut meta = PgStatisticSlotMeta::default();
    // The stakind1..5 / staop1..5 / stacoll1..5 columns are contiguous fixed
    // attributes; (&stats->stakind1)[i] == column Anum_pg_statistic_stakind1+i.
    for i in 0..STATISTIC_NUM_SLOTS {
        let (kind, kn) = getattr_with_cache_tupdesc_stats(
            mcx,
            &holder.tuple,
            Anum_pg_statistic_stakind1 as i32 + i as i32,
        );
        let (op, on) = getattr_with_cache_tupdesc_stats(
            mcx,
            &holder.tuple,
            Anum_pg_statistic_staop1 as i32 + i as i32,
        );
        let (coll, cn) = getattr_with_cache_tupdesc_stats(
            mcx,
            &holder.tuple,
            Anum_pg_statistic_stacoll1 as i32 + i as i32,
        );
        debug_assert!(!kn && !on && !cn);
        meta.stakind[i] = kind.as_i16();
        meta.staop[i] = op.as_oid();
        meta.stacoll[i] = coll.as_oid();
    }
    Ok(meta)
}

/// `SysCacheGetAttrNotNull(STATRELATTINH, statstuple, attnum)`
/// (lsyscache.c `get_attstatsslot`): the (guaranteed non-null) `stavaluesN` /
/// `stanumbersN` array Datum, still pointing into the pinned tuple.
pub(crate) fn syscache_get_attr_not_null_statistic(
    stats_tuple: types_selfuncs::StatsTuple,
    attnum: AttrNumber,
) -> PgResult<types_tuple::Datum<'static>> {
    let holder = stats_holder(stats_tuple);
    // The detoast/copy the caller performs (DatumGetArrayTypePCopy) needs the
    // array bytes to outlive this call's scratch arena, so deform into the
    // holder's own arena (held alive until release_stats_tuple). The borrow is
    // promoted to `'static` because the holder is leaked for the pin's lifetime.
    let mcx: Mcx<'static> =
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(holder._ctx.mcx()) };
    let value = SysCacheGetAttrNotNull(mcx, STATRELATTINH, &holder.tuple, attnum as i32)?;
    Ok(value)
}

/// `heap_getattr(tup, attnum, SysCache[STATRELATTINH]->cc_tupdesc)` for a held
/// `pg_statistic` tuple. The cache's tupdesc is initialized (phase 2) if needed,
/// mirroring `SysCacheGetAttr`'s lazy init.
fn getattr_with_cache_tupdesc_stats<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> (Datum<'mcx>, bool) {
    match SysCacheGetAttr(mcx, STATRELATTINH, tup, attnum) {
        Ok(col) => col,
        Err(_) => (Datum::null(), true),
    }
}

/// `SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(relid), Int16GetDatum(attnum),
/// BoolGetDatum(false))` + `((Form_pg_statistic) GETSTRUCT(tp))->stawidth`
/// (lsyscache.c `get_attavgwidth`): the non-inherited average stored width.
/// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the search owns the
/// `ReleaseSysCache`.
pub(crate) fn pg_statistic_stawidth(
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<Option<i32>> {
    use types_statistics::Anum_pg_statistic_stawidth;
    let scratch = MemoryContext::new("pg_statistic stawidth projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache3(
        mcx,
        STATRELATTINH,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
        SysCacheKey::Value(KeyDatum::from_bool(false)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let stawidth = getattr_i32(mcx, STATRELATTINH, &tup, Anum_pg_statistic_stawidth as i32)?;
    ReleaseSysCache(tup);
    Ok(Some(stawidth))
}
