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
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SearchSysCache2, SearchSysCacheAttName,
    SearchSysCacheExists, SearchSysCacheList, SearchSysCacheList1, SysCacheGetAttr,
    SysCacheGetAttrNotNull, AGGFNOID, AMOPSTRATEGY, AMPROCNUM, ATTNAME, AUTHNAME, AUTHOID,
    CASTSOURCETARGET, CLAAMNAMENSP, CLAOID, COLLOID, FOREIGNDATAWRAPPEROID, FOREIGNSERVEROID,
    INDEXRELID, LANGOID, PROCOID, RELOID, TYPEOID, USERMAPPINGOID,
};
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use types_core::AttrNumber;
use types_fmgr::LangInfo;
use backend_utils_cache_syscache_seams::{PgClassFullForm, PgProcForm};
use types_cache::AuthIdRow;
use types_tuple::backend_access_common_tupdesc::PgTypeInfo;
use backend_utils_cache_syscache_seams::CastRow;
use backend_nodes_read_seams as nodes_read_seams;
use backend_utils_adt_varlena_seams as varlena_seams;
use types_catalog::pg_aggregate::AggRow;
use types_nodes::nodes::{Node, NodePtr};

/// `Anum_pg_class_relam` (`catalog/pg_class.h`).
const Anum_pg_class_relam: i32 = 7;
/// `Anum_pg_class_reloftype` (`catalog/pg_class.h`).
const Anum_pg_class_reloftype: i32 = 5;

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

// `catalog/pg_amproc.h` attribute numbers.
const Anum_pg_amproc_amproclefttype: i32 = 3;
const Anum_pg_amproc_amprocrighttype: i32 = 4;
const Anum_pg_amproc_amprocnum: i32 = 5;
const Anum_pg_amproc_amproc: i32 = 6;

fn byval<'mcx>(value: Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    match value {
        Datum::ByVal(_) => Ok(value),
        Datum::ByRef(_) => Err(PgError::error(
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
        Datum::ByVal(_) => {
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
        Datum::ByVal(_) => Err(PgError::error(
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
const Anum_pg_type_typlen: i32 = 5;
const Anum_pg_type_typbyval: i32 = 6;
const Anum_pg_type_typalign: i32 = 23;
const Anum_pg_type_typstorage: i32 = 24;
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
const Anum_pg_proc_pronargs: i32 = 17;
const Anum_pg_proc_prorettype: i32 = 19;

// `catalog/pg_authid.h` attribute numbers.
const Anum_pg_authid_oid: i32 = 1;
const Anum_pg_authid_rolname: i32 = 2;
const Anum_pg_authid_rolsuper: i32 = 3;
const Anum_pg_authid_rolcanlogin: i32 = 7;
const Anum_pg_authid_rolreplication: i32 = 8;
const Anum_pg_authid_rolconnlimit: i32 = 10;

// `catalog/pg_language.h` attribute numbers.
const Anum_pg_language_lanname: i32 = 2;
const Anum_pg_language_lanplcallfoid: i32 = 6;
const Anum_pg_language_lanvalidator: i32 = 8;

// `catalog/pg_collation.h` attribute numbers.
const Anum_pg_collation_collname: i32 = 2;
const Anum_pg_collation_collnamespace: i32 = 3;

// `catalog/pg_index.h` attribute numbers.
const Anum_pg_index_indpred: i32 = 21;

// `catalog/pg_attribute.h` attribute numbers.
const Anum_pg_attribute_attnum: i32 = 5;
const Anum_pg_attribute_atttypid: i32 = 3;

// `catalog/pg_opclass.h` `oid` for `get_opclass_oid`.
const Anum_pg_opclass_oid: i32 = 1;

/// A padding key for the unused key slots of [`SearchSysCache`]/[`GetSysCacheOid`]
/// (the catcache ignores keys beyond `cc_nkeys`).
const UNUSED_KEY: SysCacheKey<'static> = SysCacheKey::Value(KeyDatum::null());

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
        Datum::ByVal(_) => {
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
        rolconnlimit: getattr_i32(mcx, AUTHOID, tup, Anum_pg_authid_rolconnlimit)?,
    })
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
