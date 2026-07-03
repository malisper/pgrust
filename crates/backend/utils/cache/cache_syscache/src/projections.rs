//! syscache_seams installs: the projections lsyscache/tupdesc/relation/inval
//! consume (C: `SearchSysCache*` + `GETSTRUCT` member loads).

use datum::Datum;
use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_storage::PgClassShape;
use syscache_seams::PgTypeTypcacheShape;
use types_tuple::{HeapTupleData, NameData, PgTypeShape, TupleDescData};

use mcx::PgVec;
use syscache_seams::PgCastShape;

use crate::{
    GetSysCacheOid, ReleaseSysCache, ReleaseSysCacheList, SearchSysCache1, SearchSysCache2,
    SearchSysCache3, SearchSysCache4, SearchSysCacheExists, SearchSysCacheList, SearchSysCacheList1,
    SysCacheKey,
};
use crate::cacheinfo::{AGGFNOID, AMOPOPID, AMOPSTRATEGY, AMPROCNUM, ATTNUM, CLAOID, OPFAMILYOID, PROCNAMEARGSNSP, AUTHNAME, AUTHOID, CASTSOURCETARGET, CONSTROID, INDEXRELID, NAMESPACENAME, NAMESPACEOID, OPERNAMENSP, TYPENAMENSP, OPEROID, PROCOID, RELNAMENSP, RELOID, STATRELATTINH, TYPEOID};

const ANUM_PG_CLASS_OID: i32 = 1;
const ANUM_PG_CLASS_RELISSHARED: i32 = 16;
const ANUM_PG_TYPE_OID: i32 = 1;
const ANUM_PG_TYPE_TYPNAME: i32 = 2;
const ANUM_PG_TYPE_TYPLEN: i32 = 5;
const ANUM_PG_TYPE_TYPBYVAL: i32 = 6;
const ANUM_PG_TYPE_TYPTYPE: i32 = 7;
const ANUM_PG_TYPE_TYPCATEGORY: i32 = 8;
const ANUM_PG_TYPE_TYPISPREFERRED: i32 = 9;
const ANUM_PG_TYPE_TYPISDEFINED: i32 = 10;
const ANUM_PG_TYPE_TYPRELID: i32 = 12;
const ANUM_PG_TYPE_TYPSUBSCRIPT: i32 = 13;
const ANUM_PG_TYPE_TYPELEM: i32 = 14;
const ANUM_PG_TYPE_TYPARRAY: i32 = 15;
const ANUM_PG_TYPE_TYPALIGN: i32 = 23;
const ANUM_PG_TYPE_TYPSTORAGE: i32 = 24;
const ANUM_PG_TYPE_TYPCOLLATION: i32 = 29;
const ANUM_PG_ATTRIBUTE_ATTRELID: i32 = 1;
const ANUM_PG_INDEX_INDEXRELID: i32 = 1;
const ANUM_PG_CONSTRAINT_CONTYPE: i32 = 4;
const ANUM_PG_CONSTRAINT_CONRELID: i32 = 9;
const ANUM_PG_AUTHID_OID: i32 = 1;
const ANUM_PG_AUTHID_ROLNAME: i32 = 2;
const ANUM_PG_AUTHID_ROLSUPER: i32 = 3;
const ANUM_PG_AUTHID_ROLCANLOGIN: i32 = 7;
const ANUM_PG_AUTHID_ROLCONNLIMIT: i32 = 10;
const ANUM_PG_NAMESPACE_OID: i32 = 1;
const ANUM_PG_NAMESPACE_NSPNAME: i32 = 2;
const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
const ANUM_PG_OPERATOR_OID: i32 = 1;
const ANUM_PG_OPERATOR_OPRNAME: i32 = 2;
const ANUM_PG_OPERATOR_OPRNAMESPACE: i32 = 3;
const ANUM_PG_OPERATOR_OPRKIND: i32 = 5;
const ANUM_PG_OPERATOR_OPRCANMERGE: i32 = 6;
const ANUM_PG_OPERATOR_OPRCANHASH: i32 = 7;
const ANUM_PG_OPERATOR_OPRLEFT: i32 = 8;
const ANUM_PG_OPERATOR_OPRRIGHT: i32 = 9;
const ANUM_PG_OPERATOR_OPRRESULT: i32 = 10;
const ANUM_PG_OPERATOR_OPRCOM: i32 = 11;
const ANUM_PG_OPERATOR_OPRNEGATE: i32 = 12;
const ANUM_PG_OPERATOR_OPRCODE: i32 = 13;
const ANUM_PG_OPERATOR_OPRREST: i32 = 14;
const ANUM_PG_OPERATOR_OPRJOIN: i32 = 15;
const ANUM_PG_PROC_PROCOST: i32 = 6;
const ANUM_PG_PROC_PROROWS: i32 = 7;
const ANUM_PG_PROC_PROSUPPORT: i32 = 9;
const ANUM_PG_STATISTIC_STANULLFRAC: i32 = 4;
const ANUM_PG_STATISTIC_STAWIDTH: i32 = 5;
const ANUM_PG_STATISTIC_STADISTINCT: i32 = 6;
const ANUM_PG_STATISTIC_STAKIND1: i32 = 7;
const ANUM_PG_STATISTIC_STAOP1: i32 = 12;
const ANUM_PG_STATISTIC_STACOLL1: i32 = 17;
const ANUM_PG_STATISTIC_STANUMBERS1: i32 = 22;
const ANUM_PG_STATISTIC_STAVALUES1: i32 = 27;
const STATISTIC_NUM_SLOTS: i32 = 5;
const ANUM_PG_ATTRIBUTE_ATTSTATTARGET: i32 = 21;
const ANUM_PG_TYPE_TYPANALYZE: i32 = 22;
const FLOAT4OID: Oid = 700;
const ANUM_PG_AGGREGATE_AGGKIND: i32 = 2;
const ANUM_PG_AGGREGATE_AGGNUMDIRECTARGS: i32 = 3;
const ANUM_PG_AGGREGATE_AGGTRANSFN: i32 = 4;
const ANUM_PG_AGGREGATE_AGGFINALFN: i32 = 5;
const ANUM_PG_AGGREGATE_AGGCOMBINEFN: i32 = 6;
const ANUM_PG_AGGREGATE_AGGSERIALFN: i32 = 7;
const ANUM_PG_AGGREGATE_AGGDESERIALFN: i32 = 8;
const ANUM_PG_AGGREGATE_AGGFINALEXTRA: i32 = 12;
const ANUM_PG_AGGREGATE_AGGFINALMODIFY: i32 = 14;
const ANUM_PG_AGGREGATE_AGGTRANSTYPE: i32 = 17;
const ANUM_PG_AGGREGATE_AGGTRANSSPACE: i32 = 18;
const ANUM_PG_AGGREGATE_AGGINITVAL: i32 = 21;
const ANUM_PG_CAST_OID: i32 = 1;
const ANUM_PG_CAST_CASTFUNC: i32 = 4;
const ANUM_PG_CAST_CASTCONTEXT: i32 = 5;
const ANUM_PG_CAST_CASTMETHOD: i32 = 6;

fn tupdesc_for(cache_id: i32) -> &'static TupleDescData<'static> {
    match catcache::cache_tupdesc(cache_id) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(cache_id, false)
                .expect("catcache phase-2 init for projection");
            catcache::cache_tupdesc(cache_id).expect("phase-2 init left no tupdesc")
        }
    }
}

/// GETSTRUCT-style fixed-column read off a raw catalog tuple.
fn getattr(tuple: &HeapTupleData<'_>, cache_id: i32, attnum: i32) -> Datum {
    let td = tupdesc_for(cache_id);
    let mut isnull = false;
    // SAFETY: caller passes a tuple of this catalog's row type; the read
    // columns are fixed-width NOT NULL leading columns.
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, td, &mut isnull) };
    debug_assert!(!isnull);
    d
}

fn pg_class_shape(tuple: &HeapTupleData<'_>) -> PgClassShape {
    PgClassShape {
        oid: getattr(tuple, RELOID, ANUM_PG_CLASS_OID).as_oid(),
        relisshared: getattr(tuple, RELOID, ANUM_PG_CLASS_RELISSHARED).as_bool(),
    }
}

fn pg_attribute_attrelid(tuple: &HeapTupleData<'_>) -> Oid {
    getattr(tuple, ATTNUM, ANUM_PG_ATTRIBUTE_ATTRELID).as_oid()
}

fn pg_index_indexrelid(tuple: &HeapTupleData<'_>) -> Oid {
    getattr(tuple, INDEXRELID, ANUM_PG_INDEX_INDEXRELID).as_oid()
}

fn pg_constraint_fk_target(tuple: &HeapTupleData<'_>) -> Option<Oid> {
    if getattr(tuple, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPE).as_i8() != CONSTRAINT_FOREIGN {
        return None;
    }
    let conrelid = getattr(tuple, CONSTROID, ANUM_PG_CONSTRAINT_CONRELID).as_oid();
    if conrelid == 0 {
        None
    } else {
        Some(conrelid)
    }
}

fn lookup_pg_class_by_relid(relid: Oid) -> PgResult<Option<PgClassShape>> {
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let shape = pg_class_shape(&tuple.tuple());
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_CLASS_RELNAME: i32 = 2;

fn pg_class_relname(relid: Oid) -> PgResult<Option<types_tuple::NameData>> {
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let d = getattr(&tuple.tuple(), RELOID, ANUM_PG_CLASS_RELNAME);
    // SAFETY: relname is a NameData column; the datum points at its 64-byte
    // buffer inside the pinned tuple image, copied out before release.
    let name = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn lookup_pg_type_shape(typid: Oid) -> PgResult<Option<PgTypeShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = PgTypeShape {
        typlen: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPLEN).as_i16(),
        typbyval: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPBYVAL).as_bool(),
        typalign: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPALIGN).as_i8(),
        typstorage: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSTORAGE).as_i8(),
        typcollation: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPCOLLATION).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_type_isdefined(typid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let isdefined = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPISDEFINED).as_bool();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(isdefined))
}

fn pg_type_typtype(typid: Oid) -> PgResult<Option<i8>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let typtype = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPTYPE).as_i8();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(typtype))
}

fn pg_type_category(typid: Oid) -> PgResult<Option<(i8, bool)>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let category = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPCATEGORY).as_i8();
    let preferred = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPISPREFERRED).as_bool();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some((category, preferred)))
}

fn pg_type_element_shape(
    typid: Oid,
) -> PgResult<Option<syscache_seams::PgTypeElementShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgTypeElementShape {
        typelem: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPELEM).as_oid(),
        typsubscript: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSUBSCRIPT).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_OPCLASS_OPCMETHOD: i32 = 2;
const ANUM_PG_OPCLASS_OPCFAMILY: i32 = 6;
const ANUM_PG_OPCLASS_OPCINTYPE: i32 = 7;

fn lookup_pg_opclass_shape(
    opclass: Oid,
) -> PgResult<Option<syscache_seams::PgOpclassShape>> {
    let Some(tuple) = SearchSysCache1(CLAOID, SysCacheKey::Value(Datum::from_oid(opclass)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgOpclassShape {
        opcmethod: getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCMETHOD).as_oid(),
        opcfamily: getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCFAMILY).as_oid(),
        opcintype: getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCINTYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_authid_by_rolname(rolname: &str) -> PgResult<Option<(Oid, bool)>> {
    let Some(tuple) = SearchSysCache1(AUTHNAME, SysCacheKey::Str(rolname))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let oid = getattr(&t, AUTHNAME, ANUM_PG_AUTHID_OID).as_oid();
    let rolsuper = getattr(&t, AUTHNAME, ANUM_PG_AUTHID_ROLSUPER).as_bool();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some((oid, rolsuper)))
}

fn authid_session_shape(
    tuple: &HeapTupleData<'_>,
    cache_id: i32,
) -> syscache_seams::AuthIdSessionShape {
    let d = getattr(tuple, cache_id, ANUM_PG_AUTHID_ROLNAME);
    // SAFETY: rolname is a NameData column; the datum points at its 64-byte
    // NUL-padded buffer inside the pinned tuple image.
    let rolname = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    syscache_seams::AuthIdSessionShape {
        roleid: getattr(tuple, cache_id, ANUM_PG_AUTHID_OID).as_oid(),
        rolname,
        rolsuper: getattr(tuple, cache_id, ANUM_PG_AUTHID_ROLSUPER).as_bool(),
        rolcanlogin: getattr(tuple, cache_id, ANUM_PG_AUTHID_ROLCANLOGIN).as_bool(),
        rolconnlimit: getattr(tuple, cache_id, ANUM_PG_AUTHID_ROLCONNLIMIT).as_i32(),
    }
}

fn lookup_authid_session_by_rolname(
    rolname: &str,
) -> PgResult<Option<syscache_seams::AuthIdSessionShape>> {
    let Some(tuple) = SearchSysCache1(AUTHNAME, SysCacheKey::Str(rolname))? else {
        return Ok(None);
    };
    let shape = authid_session_shape(&tuple.tuple(), AUTHNAME);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_authid_session_by_oid(
    roleid: Oid,
) -> PgResult<Option<syscache_seams::AuthIdSessionShape>> {
    let Some(tuple) = SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? else {
        return Ok(None);
    };
    let shape = authid_session_shape(&tuple.tuple(), AUTHOID);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_authid_rolname<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let Some(tuple) = SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? else {
        return Ok(None);
    };
    let d = getattr(&tuple.tuple(), AUTHOID, ANUM_PG_AUTHID_ROLNAME);
    // SAFETY: rolname is a NameData column; the datum points at its
    // NUL-terminated 64-byte buffer inside the pinned tuple image.
    let name = unsafe {
        let p = d.as_usize() as *const u8;
        let mut len = 0usize;
        while len < 64 && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len))
    };
    let s = PgString::from_str_in(name, mcx)?;
    ReleaseSysCache(tuple);
    Ok(Some(s))
}

fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool> {
    SearchSysCacheExists(
        RELOID,
        SysCacheKey::Value(Datum::from_oid(reloid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn search_syscache_exists_attnum(relid: Oid, attnum: i16) -> PgResult<bool> {
    SearchSysCacheExists(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn sys_cache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()> {
    crate::SysCacheInvalidate(cache_id, hash_value);
    Ok(())
}

fn getattr_name(tuple: &HeapTupleData<'_>, cache_id: i32, attnum: i32) -> NameData {
    let d = getattr(tuple, cache_id, attnum);
    let mut name = NameData::default();
    // SAFETY: a NameData column's datum points at its 64-byte in-tuple buffer.
    unsafe {
        core::ptr::copy_nonoverlapping(
            d.as_usize() as *const u8,
            name.data.as_mut_ptr(),
            name.data.len(),
        );
    }
    name
}

fn lookup_pg_type_typcache_shape(typid: Oid) -> PgResult<Option<PgTypeTypcacheShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = PgTypeTypcacheShape {
        typname: getattr_name(&t, TYPEOID, ANUM_PG_TYPE_TYPNAME),
        typlen: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPLEN).as_i16(),
        typbyval: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPBYVAL).as_bool(),
        typalign: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPALIGN).as_i8(),
        typstorage: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSTORAGE).as_i8(),
        typtype: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPTYPE).as_i8(),
        typisdefined: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPISDEFINED).as_bool(),
        typrelid: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPRELID).as_oid(),
        typsubscript: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSUBSCRIPT).as_oid(),
        typelem: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPELEM).as_oid(),
        typarray: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPARRAY).as_oid(),
        typcollation: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPCOLLATION).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_class_relid_by_name(relname: &str, relnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        RELNAMENSP,
        ANUM_PG_CLASS_OID,
        SysCacheKey::Str(relname),
        SysCacheKey::Value(Datum::from_oid(relnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_type_oid_by_name(typname: &str, typnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        TYPENAMENSP,
        ANUM_PG_TYPE_OID,
        SysCacheKey::Str(typname),
        SysCacheKey::Value(Datum::from_oid(typnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_namespace_oid_by_name(nspname: &str) -> PgResult<Oid> {
    GetSysCacheOid(
        NAMESPACENAME,
        ANUM_PG_NAMESPACE_OID,
        SysCacheKey::Str(nspname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn pg_namespace_nspname(nspid: Oid) -> PgResult<Option<NameData>> {
    let Some(tuple) = SearchSysCache1(NAMESPACEOID, SysCacheKey::Value(Datum::from_oid(nspid)))?
    else {
        return Ok(None);
    };
    let name = getattr_name(&tuple.tuple(), NAMESPACEOID, ANUM_PG_NAMESPACE_NSPNAME);
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn syscache_hash_value_typeoid(typid: Oid) -> PgResult<u32> {
    crate::GetSysCacheHashValue(
        TYPEOID,
        SysCacheKey::Value(Datum::from_oid(typid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn syscache_hash_value_procoid(funcid: Oid) -> PgResult<u32> {
    crate::GetSysCacheHashValue(
        PROCOID,
        SysCacheKey::Value(Datum::from_oid(funcid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn pg_operator_oprname(opno: Oid) -> PgResult<Option<NameData>> {
    let Some(tuple) = SearchSysCache1(OPEROID, SysCacheKey::Value(Datum::from_oid(opno)))? else {
        return Ok(None);
    };
    let d = getattr(&tuple.tuple(), OPEROID, ANUM_PG_OPERATOR_OPRNAME);
    // SAFETY: oprname is a NameData column; the datum points at its 64-byte
    // buffer inside the pinned tuple image, copied out before release.
    let name = unsafe { *(d.as_usize() as *const NameData) };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn lookup_pg_operator_shape(opno: Oid) -> PgResult<Option<syscache_seams::PgOperatorShape>> {
    let Some(tuple) = SearchSysCache1(OPEROID, SysCacheKey::Value(Datum::from_oid(opno)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgOperatorShape {
        oprleft: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRLEFT).as_oid(),
        oprright: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRRIGHT).as_oid(),
        oprresult: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRRESULT).as_oid(),
        oprcom: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRCOM).as_oid(),
        oprnegate: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRNEGATE).as_oid(),
        oprcode: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRCODE).as_oid(),
        oprrest: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRREST).as_oid(),
        oprjoin: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRJOIN).as_oid(),
        oprcanmerge: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRCANMERGE).as_bool(),
        oprcanhash: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRCANHASH).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_TYPE_TYPDELIM: i32 = 11;
const ANUM_PG_TYPE_TYPINPUT: i32 = 16;
const ANUM_PG_TYPE_TYPOUTPUT: i32 = 17;
const ANUM_PG_TYPE_TYPRECEIVE: i32 = 18;
const ANUM_PG_TYPE_TYPSEND: i32 = 19;
const ANUM_PG_TYPE_TYPMODIN: i32 = 20;
const ANUM_PG_TYPE_TYPMODOUT: i32 = 21;
const ANUM_PG_TYPE_TYPBASETYPE: i32 = 26;
const ANUM_PG_TYPE_TYPTYPMOD: i32 = 27;
const ANUM_PG_PROC_PRONAME: i32 = 2;
const ANUM_PG_PROC_PRONAMESPACE: i32 = 3;
const ANUM_PG_PROC_PROVARIADIC: i32 = 8;
const ANUM_PG_PROC_PROKIND: i32 = 10;
const ANUM_PG_PROC_PROLEAKPROOF: i32 = 12;
const ANUM_PG_PROC_PROISSTRICT: i32 = 13;
const ANUM_PG_PROC_PRORETSET: i32 = 14;
const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
const ANUM_PG_PROC_PROPARALLEL: i32 = 16;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
const ANUM_PG_PROC_OID: i32 = 1;
const ANUM_PG_PROC_PRONARGDEFAULTS: i32 = 18;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
const ANUM_PG_AMPROC_AMPROC: i32 = 6;

// get_opfamily_proc (lsyscache.c): GetSysCacheOid4(AMPROCNUM, Anum_pg_amproc_amproc, ...).
const ANUM_PG_AMOP_AMOPFAMILY: i32 = 2;
const ANUM_PG_AMOP_AMOPLEFTTYPE: i32 = 3;
const ANUM_PG_AMOP_AMOPRIGHTTYPE: i32 = 4;
const ANUM_PG_AMOP_AMOPSTRATEGY: i32 = 5;
const ANUM_PG_AMOP_AMOPOPR: i32 = 7;
const ANUM_PG_AMOP_AMOPMETHOD: i32 = 8;
const ANUM_PG_AMOP_AMOPSORTFAMILY: i32 = 9;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELTYPE: i32 = 4;
const ANUM_PG_CLASS_RELAM: i32 = 7;
const ANUM_PG_CLASS_RELTABLESPACE: i32 = 9;
const ANUM_PG_CLASS_RELPERSISTENCE: i32 = 17;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
const ANUM_PG_CLASS_RELNATTS: i32 = 19;
const ANUM_PG_CLASS_RELISPARTITION: i32 = 28;
const ANUM_PG_OPFAMILY_OPFMETHOD: i32 = 2;
const ANUM_PG_OPFAMILY_OPFNAME: i32 = 3;

fn lookup_pg_class_ls_shape(relid: Oid) -> PgResult<Option<syscache_seams::PgClassLsShape>> {
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgClassLsShape {
        relnamespace: getattr(&t, RELOID, ANUM_PG_CLASS_RELNAMESPACE).as_oid(),
        reltype: getattr(&t, RELOID, ANUM_PG_CLASS_RELTYPE).as_oid(),
        relam: getattr(&t, RELOID, ANUM_PG_CLASS_RELAM).as_oid(),
        reltablespace: getattr(&t, RELOID, ANUM_PG_CLASS_RELTABLESPACE).as_oid(),
        relnatts: getattr(&t, RELOID, ANUM_PG_CLASS_RELNATTS).as_i16(),
        relkind: getattr(&t, RELOID, ANUM_PG_CLASS_RELKIND).as_i8(),
        relpersistence: getattr(&t, RELOID, ANUM_PG_CLASS_RELPERSISTENCE).as_i8(),
        relispartition: getattr(&t, RELOID, ANUM_PG_CLASS_RELISPARTITION).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_amop_by_operator(
    opno: Oid,
    purpose: u8,
    opfamily: Oid,
) -> PgResult<Option<syscache_seams::PgAmopShape>> {
    let Some(tuple) = SearchSysCache3(
        AMOPOPID,
        SysCacheKey::Value(Datum::from_oid(opno)),
        SysCacheKey::Value(Datum::from_char(purpose as i8)),
        SysCacheKey::Value(Datum::from_oid(opfamily)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgAmopShape {
        amopstrategy: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPSTRATEGY).as_i16(),
        amopsortfamily: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPSORTFAMILY).as_oid(),
        amoplefttype: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPLEFTTYPE).as_oid(),
        amoprighttype: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPRIGHTTYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_amop_by_strategy(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> PgResult<Oid> {
    crate::GetSysCacheOid(
        AMOPSTRATEGY,
        ANUM_PG_AMOP_AMOPOPR,
        SysCacheKey::Value(Datum::from_oid(opfamily)),
        SysCacheKey::Value(Datum::from_oid(lefttype)),
        SysCacheKey::Value(Datum::from_oid(righttype)),
        SysCacheKey::Value(Datum::from_i16(strategy)),
    )
}

fn lookup_pg_amop_members_by_operator<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
) -> PgResult<PgVec<'mcx, syscache_seams::PgAmopMemberShape>> {
    let list = SearchSysCacheList1(AMOPOPID, SysCacheKey::Value(Datum::from_oid(opno)))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgAmopMemberShape {
            amopfamily: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPFAMILY).as_oid(),
            amoplefttype: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPLEFTTYPE).as_oid(),
            amoprighttype: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPRIGHTTYPE).as_oid(),
            amopstrategy: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPSTRATEGY).as_i16(),
            amopmethod: getattr(&t, AMOPOPID, ANUM_PG_AMOP_AMOPMETHOD).as_oid(),
        });
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

fn lookup_pg_opfamily_shape(
    opfid: Oid,
) -> PgResult<Option<syscache_seams::PgOpfamilyShape>> {
    let Some(tuple) =
        SearchSysCache1(OPFAMILYOID, SysCacheKey::Value(Datum::from_oid(opfid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let d = getattr(&t, OPFAMILYOID, ANUM_PG_OPFAMILY_OPFNAME);
    // SAFETY: opfname is a NameData column; the datum points at its 64-byte
    // in-tuple image.
    let opfname = unsafe { *(d.as_usize() as *const NameData) };
    let shape = syscache_seams::PgOpfamilyShape {
        opfmethod: getattr(&t, OPFAMILYOID, ANUM_PG_OPFAMILY_OPFMETHOD).as_oid(),
        opfname,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_amproc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> PgResult<Oid> {
    crate::GetSysCacheOid(
        AMPROCNUM,
        ANUM_PG_AMPROC_AMPROC,
        SysCacheKey::Value(Datum::from_oid(opfamily)),
        SysCacheKey::Value(Datum::from_oid(lefttype)),
        SysCacheKey::Value(Datum::from_oid(righttype)),
        SysCacheKey::Value(Datum::from_i16(procnum)),
    )
}

fn pg_type_base_shape(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeBaseShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgTypeBaseShape {
        typtype: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPTYPE).as_i8(),
        typbasetype: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPBASETYPE).as_oid(),
        typtypmod: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPTYPMOD).as_i32(),
        typelem: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPELEM).as_oid(),
        typsubscript: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSUBSCRIPT).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_type_io_shape(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeIoShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgTypeIoShape {
        oid: typid,
        typinput: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPINPUT).as_oid(),
        typoutput: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPOUTPUT).as_oid(),
        typreceive: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPRECEIVE).as_oid(),
        typsend: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPSEND).as_oid(),
        typmodin: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPMODIN).as_oid(),
        typmodout: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPMODOUT).as_oid(),
        typelem: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPELEM).as_oid(),
        typlen: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPLEN).as_i16(),
        typbyval: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPBYVAL).as_bool(),
        typalign: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPALIGN).as_i8(),
        typdelim: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPDELIM).as_i8(),
        typisdefined: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPISDEFINED).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_type_typarray(typid: Oid) -> PgResult<Option<Oid>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let arr = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPARRAY).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(arr))
}

fn pg_proc_proname(funcid: Oid) -> PgResult<Option<types_tuple::NameData>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let d = getattr(&tuple.tuple(), PROCOID, ANUM_PG_PROC_PRONAME);
    // SAFETY: proname is a NameData column; the datum points at its 64-byte
    // buffer inside the pinned tuple image, copied out before release.
    let name = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn lookup_pg_proc_shape(funcid: Oid) -> PgResult<Option<syscache_seams::PgProcShape>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgProcShape {
        pronamespace: getattr(&t, PROCOID, ANUM_PG_PROC_PRONAMESPACE).as_oid(),
        prorettype: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETTYPE).as_oid(),
        provariadic: getattr(&t, PROCOID, ANUM_PG_PROC_PROVARIADIC).as_oid(),
        prosupport: getattr(&t, PROCOID, ANUM_PG_PROC_PROSUPPORT).as_oid(),
        pronargs: getattr(&t, PROCOID, ANUM_PG_PROC_PRONARGS).as_i16(),
        prokind: getattr(&t, PROCOID, ANUM_PG_PROC_PROKIND).as_i8(),
        provolatile: getattr(&t, PROCOID, ANUM_PG_PROC_PROVOLATILE).as_i8(),
        proparallel: getattr(&t, PROCOID, ANUM_PG_PROC_PROPARALLEL).as_i8(),
        proretset: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETSET).as_bool(),
        proisstrict: getattr(&t, PROCOID, ANUM_PG_PROC_PROISSTRICT).as_bool(),
        proleakproof: getattr(&t, PROCOID, ANUM_PG_PROC_PROLEAKPROOF).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_proc_name_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    proname: &str,
) -> PgResult<PgVec<'mcx, syscache_seams::PgProcCandidate<'mcx>>> {
    let list = SearchSysCacheList1(PROCNAMEARGSNSP, SysCacheKey::Str(proname))?;
    let n = list.n_members() as usize;
    // PgVec::new_in, not vec_with_capacity_in: the element embeds a PgVec
    // (proargtypes), so the no-drop const gate rejects it (slots precedent).
    let mut out: PgVec<'mcx, syscache_seams::PgProcCandidate<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve_exact(n).map_err(|_| mcx.oom(n))?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        let pronargs = getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_PRONARGS).as_i16();
        let argv = getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_PROARGTYPES);
        // SAFETY: proargtypes is a not-null plain-storage oidvector; values
        // tail follows the 24-byte header in place, dim1 == pronargs.
        let args = unsafe {
            let p = argv.as_usize() as *const array::oidvector;
            core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
        };
        let mut proargtypes = mcx::vec_with_capacity_in(mcx, args.len())?;
        proargtypes.extend_from_slice(args);
        out.push(syscache_seams::PgProcCandidate {
            oid: getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_OID).as_oid(),
            pronamespace: getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_PRONAMESPACE).as_oid(),
            pronargs,
            pronargdefaults: getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_PRONARGDEFAULTS).as_i16(),
            provariadic: getattr(&t, PROCNAMEARGSNSP, ANUM_PG_PROC_PROVARIADIC).as_oid(),
            proargtypes,
        });
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

fn lookup_pg_operator_oid_exact(
    opername: &str,
    oprleft: Oid,
    oprright: Oid,
    oprnamespace: Oid,
) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache4(
        OPERNAMENSP,
        SysCacheKey::Str(opername),
        SysCacheKey::Value(Datum::from_oid(oprleft)),
        SysCacheKey::Value(Datum::from_oid(oprright)),
        SysCacheKey::Value(Datum::from_oid(oprnamespace)),
    )?
    else {
        return Ok(0);
    };
    let oid = getattr(&tuple.tuple(), OPERNAMENSP, ANUM_PG_OPERATOR_OID).as_oid();
    ReleaseSysCache(tuple);
    Ok(oid)
}

fn lookup_pg_operator_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    opername: &str,
    oprleft: Oid,
    oprright: Oid,
) -> PgResult<PgVec<'mcx, (Oid, Oid)>> {
    let list = SearchSysCacheList(
        OPERNAMENSP,
        3,
        SysCacheKey::Str(opername),
        SysCacheKey::Value(Datum::from_oid(oprleft)),
        SysCacheKey::Value(Datum::from_oid(oprright)),
    )?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push((
            getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OID).as_oid(),
            getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OPRNAMESPACE).as_oid(),
        ));
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

fn lookup_pg_operator_name_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    opername: &str,
) -> PgResult<PgVec<'mcx, syscache_seams::PgOperatorNameCandidate>> {
    let list = SearchSysCacheList1(OPERNAMENSP, SysCacheKey::Str(opername))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgOperatorNameCandidate {
            oid: getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OID).as_oid(),
            oprnamespace: getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OPRNAMESPACE).as_oid(),
            oprkind: getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OPRKIND).as_i8(),
            oprleft: getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OPRLEFT).as_oid(),
            oprright: getattr(&t, OPERNAMENSP, ANUM_PG_OPERATOR_OPRRIGHT).as_oid(),
        });
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

fn pg_operator_name_candidates_exist(opername: &str, oprkind: i8) -> PgResult<bool> {
    let list = SearchSysCacheList1(OPERNAMENSP, SysCacheKey::Str(opername))?;
    let n = list.n_members() as usize;
    let mut found = false;
    for i in 0..n {
        let m = list.member(i);
        if getattr(&m.tuple(), OPERNAMENSP, ANUM_PG_OPERATOR_OPRKIND).as_i8() == oprkind {
            found = true;
            break;
        }
    }
    ReleaseSysCacheList(list);
    Ok(found)
}

fn lookup_pg_cast_shape(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Option<PgCastShape>> {
    let Some(tuple) = SearchSysCache2(
        CASTSOURCETARGET,
        SysCacheKey::Value(Datum::from_oid(sourcetypeid)),
        SysCacheKey::Value(Datum::from_oid(targettypeid)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = PgCastShape {
        oid: getattr(&t, CASTSOURCETARGET, ANUM_PG_CAST_OID).as_oid(),
        castfunc: getattr(&t, CASTSOURCETARGET, ANUM_PG_CAST_CASTFUNC).as_oid(),
        castcontext: getattr(&t, CASTSOURCETARGET, ANUM_PG_CAST_CASTCONTEXT).as_i8(),
        castmethod: getattr(&t, CASTSOURCETARGET, ANUM_PG_CAST_CASTMETHOD).as_i8(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_proc_cost_shape(funcid: Oid) -> PgResult<Option<syscache_seams::PgProcCostShape>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgProcCostShape {
        procost: getattr(&t, PROCOID, ANUM_PG_PROC_PROCOST).as_f32(),
        prorows: getattr(&t, PROCOID, ANUM_PG_PROC_PROROWS).as_f32(),
        prosupport: getattr(&t, PROCOID, ANUM_PG_PROC_PROSUPPORT).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

/// Nullable-column read off a raw catalog tuple; None mirrors SQL NULL.
fn getattr_nullable(tuple: &HeapTupleData<'_>, cache_id: i32, attnum: i32) -> Option<Datum> {
    let td = tupdesc_for(cache_id);
    let mut isnull = false;
    // SAFETY: caller passes a tuple of this catalog's row type.
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, td, &mut isnull) };
    if isnull { None } else { Some(d) }
}

fn lookup_pg_attribute_stattarget(
    relid: Oid,
    attnum: types_core::AttrNumber,
) -> PgResult<Option<i16>> {
    let Some(tuple) = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        return Err(types_error::PgError::error(format!(
            "cache lookup failed for attribute {attnum} of relation {relid}"
        ))
        .into());
    };
    let out = getattr_nullable(&tuple.tuple(), ATTNUM, ANUM_PG_ATTRIBUTE_ATTSTATTARGET)
        .map(|d| d.as_i16());
    ReleaseSysCache(tuple);
    Ok(out)
}

fn pg_type_typanalyze(typid: Oid) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Err(types_error::PgError::error(format!(
            "cache lookup failed for type {typid}"
        ))
        .into());
    };
    let out = getattr(&tuple.tuple(), TYPEOID, ANUM_PG_TYPE_TYPANALYZE).as_oid();
    ReleaseSysCache(tuple);
    Ok(out)
}

// Owned copy of a varlena attr's full image; None mirrors SQL NULL.
fn varlena_image<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    cache_id: i32,
    attnum: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(d) = getattr_nullable(tuple, cache_id, attnum) else {
        return Ok(None);
    };
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum points into the live tuple.
    let b0 = unsafe { *p };
    assert!(
        b0 != 0x01 && (b0 & 0x03) != 0x02,
        "pg_statistic array attr is toasted/compressed: detoast (heaptoast) gap in stats decode"
    );
    if b0 & 0x01 != 0 {
        // PG_DETOAST_DATUM's short-header expansion to a 4-byte-header image.
        let raw = (b0 as usize >> 1) & 0x7F;
        let total = raw - 1 + 4;
        let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
        out.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
        // SAFETY: short varlena addresses `raw` in-tuple bytes.
        out.extend_from_slice(unsafe { core::slice::from_raw_parts(p.add(1), raw - 1) });
        return Ok(Some(out));
    }
    let len = {
        // SAFETY: 4-byte varlena header verified above.
        let w = unsafe { u32::from_ne_bytes(*(p as *const [u8; 4])) };
        (w >> 2) as usize
    };
    // SAFETY: the datum addresses `len` in-tuple bytes.
    let src = unsafe { core::slice::from_raw_parts(p, len) };
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(src);
    Ok(Some(out))
}

fn pg_statistic_stawidth(
    relid: Oid,
    attnum: types_core::AttrNumber,
    inh: bool,
) -> PgResult<Option<i32>> {
    let Some(tuple) = SearchSysCache3(
        STATRELATTINH,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let stawidth = getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAWIDTH).as_i32();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(stawidth))
}

fn lookup_pg_statistic_bundle<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: types_core::AttrNumber,
    inh: bool,
) -> PgResult<Option<syscache_seams::PgStatisticBundle<'mcx>>> {
    let Some(tuple) = SearchSysCache3(
        STATRELATTINH,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let mut slots = PgVec::new_in(mcx);
    for i in 0..STATISTIC_NUM_SLOTS {
        let kind = getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAKIND1 + i).as_i16();
        if kind == 0 {
            continue;
        }
        let numbers = match varlena_image(mcx, &t, STATRELATTINH, ANUM_PG_STATISTIC_STANUMBERS1 + i)? {
            Some(img) => {
                let elems = datum::array_build::deconstruct_array_image(mcx, &img, 4, true, b'i')?;
                let mut nums: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(mcx, elems.len())?;
                nums.extend(elems.iter().map(|d| d.as_f32()));
                nums
            }
            None => PgVec::new_in(mcx),
        };
        let (values, values_image, valuetype) =
            match varlena_image(mcx, &t, STATRELATTINH, ANUM_PG_STATISTIC_STAVALUES1 + i)? {
                Some(img) => {
                    let elemtype = datum::array_build::array_image_elemtype(&img);
                    let ty = syscache_seams::lookup_pg_type_shape::call(elemtype)?
                        .expect("stavalues element type");
                    let values = datum::array_build::deconstruct_array_image(
                        mcx, &img, ty.typlen, ty.typbyval, ty.typalign as u8,
                    )?;
                    (values, img, elemtype)
                }
                None => (PgVec::new_in(mcx), PgVec::new_in(mcx), InvalidOid),
            };
        slots.push(syscache_seams::PgStatisticSlotData {
            kind,
            staop: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAOP1 + i).as_oid(),
            stacoll: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STACOLL1 + i).as_oid(),
            valuetype,
            values,
            numbers,
            values_image,
        });
    }
    let bundle = syscache_seams::PgStatisticBundle {
        stanullfrac: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STANULLFRAC).as_f32(),
        stawidth: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAWIDTH).as_i32(),
        stadistinct: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STADISTINCT).as_f32(),
        slots,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(bundle))
}

fn lookup_pg_aggregate_shape(
    aggfnoid: Oid,
) -> PgResult<Option<syscache_seams::PgAggregateShape>> {
    let Some(tuple) = SearchSysCache1(AGGFNOID, SysCacheKey::Value(Datum::from_oid(aggfnoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgAggregateShape {
        aggkind: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGKIND).as_i8(),
        aggnumdirectargs: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGNUMDIRECTARGS).as_i16(),
        aggtransfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGTRANSFN).as_oid(),
        aggfinalfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGFINALFN).as_oid(),
        aggcombinefn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGCOMBINEFN).as_oid(),
        aggserialfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGSERIALFN).as_oid(),
        aggdeserialfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGDESERIALFN).as_oid(),
        aggfinalextra: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGFINALEXTRA).as_bool(),
        aggfinalmodify: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGFINALMODIFY).as_i8(),
        aggtranstype: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGTRANSTYPE).as_oid(),
        aggtransspace: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGTRANSSPACE).as_i32(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_aggregate_agginitval<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
) -> PgResult<Option<Option<PgString<'mcx>>>> {
    let Some(tuple) = SearchSysCache1(AGGFNOID, SysCacheKey::Value(Datum::from_oid(aggfnoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = match varlena_image(mcx, &t, AGGFNOID, ANUM_PG_AGGREGATE_AGGINITVAL)? {
        Some(img) => {
            let s = core::str::from_utf8(&img[4..]).expect("agginitval is server-encoding text");
            Some(PgString::from_str_in(s, mcx)?)
        }
        None => None,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(out))
}

fn lookup_pg_statistic_shape(
    relid: Oid,
    attnum: types_core::AttrNumber,
    inh: bool,
) -> PgResult<Option<syscache_seams::PgStatisticShape>> {
    let Some(tuple) = SearchSysCache3(
        STATRELATTINH,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgStatisticShape {
        stanullfrac: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STANULLFRAC).as_f32(),
        stawidth: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAWIDTH).as_i32(),
        stadistinct: getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STADISTINCT).as_f32(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

pub(crate) fn install() {
    syscache_seams::search_syscache_exists_reloid::set(search_syscache_exists_reloid);
    syscache_seams::search_syscache_exists_attnum::set(search_syscache_exists_attnum);
    syscache_seams::sys_cache_invalidate::set(sys_cache_invalidate);
    syscache_seams::relation_invalidates_snapshots_only::set(crate::RelationInvalidatesSnapshotsOnly);
    syscache_seams::lookup_pg_class_by_relid::set(lookup_pg_class_by_relid);
    syscache_seams::pg_class_shape::set(pg_class_shape);
    syscache_seams::pg_class_relname::set(pg_class_relname);
    syscache_seams::pg_attribute_attrelid::set(pg_attribute_attrelid);
    syscache_seams::pg_index_indexrelid::set(pg_index_indexrelid);
    syscache_seams::pg_constraint_fk_target::set(pg_constraint_fk_target);
    syscache_seams::lookup_pg_type_shape::set(lookup_pg_type_shape);
    syscache_seams::pg_type_isdefined::set(pg_type_isdefined);
    syscache_seams::pg_type_typtype::set(pg_type_typtype);
    syscache_seams::pg_type_category::set(pg_type_category);
    syscache_seams::pg_type_element_shape::set(pg_type_element_shape);
    syscache_seams::lookup_pg_opclass_shape::set(lookup_pg_opclass_shape);
    syscache_seams::lookup_authid_rolname::set(lookup_authid_rolname);
    syscache_seams::lookup_authid_by_rolname::set(lookup_authid_by_rolname);
    syscache_seams::lookup_authid_session_by_rolname::set(lookup_authid_session_by_rolname);
    syscache_seams::lookup_authid_session_by_oid::set(lookup_authid_session_by_oid);
    syscache_seams::lookup_pg_type_typcache_shape::set(lookup_pg_type_typcache_shape);
    syscache_seams::syscache_hash_value_typeoid::set(syscache_hash_value_typeoid);
    syscache_seams::syscache_hash_value_procoid::set(syscache_hash_value_procoid);
    syscache_seams::lookup_pg_class_relid_by_name::set(lookup_pg_class_relid_by_name);
    syscache_seams::lookup_pg_type_oid_by_name::set(lookup_pg_type_oid_by_name);
    syscache_seams::pg_namespace_nspname::set(pg_namespace_nspname);
    syscache_seams::lookup_pg_namespace_oid_by_name::set(lookup_pg_namespace_oid_by_name);
    syscache_seams::lookup_pg_operator_shape::set(lookup_pg_operator_shape);
    syscache_seams::pg_operator_oprname::set(pg_operator_oprname);
    syscache_seams::lookup_pg_operator_oid_exact::set(lookup_pg_operator_oid_exact);
    syscache_seams::lookup_pg_amproc::set(lookup_pg_amproc);
    syscache_seams::lookup_pg_class_ls_shape::set(lookup_pg_class_ls_shape);
    syscache_seams::lookup_pg_amop_by_operator::set(lookup_pg_amop_by_operator);
    syscache_seams::lookup_pg_amop_by_strategy::set(lookup_pg_amop_by_strategy);
    syscache_seams::lookup_pg_amop_members_by_operator::set(lookup_pg_amop_members_by_operator);
    syscache_seams::lookup_pg_opfamily_shape::set(lookup_pg_opfamily_shape);
    syscache_seams::pg_type_base_shape::set(pg_type_base_shape);
    syscache_seams::pg_type_io_shape::set(pg_type_io_shape);
    syscache_seams::pg_type_typarray::set(pg_type_typarray);
    syscache_seams::lookup_pg_proc_shape::set(lookup_pg_proc_shape);
    syscache_seams::pg_proc_proname::set(pg_proc_proname);
    syscache_seams::lookup_pg_proc_name_candidates::set(lookup_pg_proc_name_candidates);
    syscache_seams::lookup_pg_operator_candidates::set(lookup_pg_operator_candidates);
    syscache_seams::pg_operator_name_candidates_exist::set(pg_operator_name_candidates_exist);
    syscache_seams::lookup_pg_operator_name_candidates::set(lookup_pg_operator_name_candidates);
    syscache_seams::lookup_pg_cast_shape::set(lookup_pg_cast_shape);
    syscache_seams::pg_proc_cost_shape::set(pg_proc_cost_shape);
    syscache_seams::lookup_pg_attribute_stattarget::set(lookup_pg_attribute_stattarget);
    syscache_seams::pg_type_typanalyze::set(pg_type_typanalyze);
    syscache_seams::lookup_pg_aggregate_shape::set(lookup_pg_aggregate_shape);
    syscache_seams::pg_aggregate_agginitval::set(pg_aggregate_agginitval);
    install_pg_statistic();
}

// Fixture rigs that mock the other catalogs still install the real
// pg_statistic decode (set-once seams forbid override-after-install).
pub(crate) fn install_pg_statistic() {
    syscache_seams::lookup_pg_statistic_shape::set(lookup_pg_statistic_shape);
    syscache_seams::lookup_pg_statistic_bundle::set(lookup_pg_statistic_bundle);
    syscache_seams::pg_statistic_stawidth::set(pg_statistic_stawidth);
}
