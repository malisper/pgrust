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
    SysCacheGetAttr, SysCacheKey,
};
use crate::cacheinfo::{AMOID, COLLNAMEENCNSP, COLLOID, AGGFNOID, AMOPOPID, AMOPSTRATEGY, AMPROCNUM, ATTNUM, CLAAMNAMENSP, OPFAMILYAMNAMENSP, CLAOID, OPFAMILYOID, PROCNAMEARGSNSP, AUTHNAME, ENUMOID, ENUMTYPOIDNAME, AUTHOID, CASTSOURCETARGET, CONSTROID, INDEXRELID, NAMESPACENAME, NAMESPACEOID, OPERNAMENSP, TSCONFIGNAMENSP, TSCONFIGOID, TSDICTNAMENSP, TSDICTOID, TYPENAMENSP, ATTNAME, OPEROID, PROCOID, RANGEMULTIRANGE, RANGETYPE, RELNAMENSP, RELOID, SEQRELID, STATEXTDATASTXOID, STATEXTOID, STATRELATTINH, TYPEOID};

const ANUM_PG_CLASS_OID: i32 = 1;
const ANUM_PG_CLASS_RELFILENODE: i32 = 8;
const ANUM_PG_CLASS_RELISSHARED: i32 = 16;
const ANUM_PG_TYPE_OID: i32 = 1;
const ANUM_PG_TYPE_TYPNAME: i32 = 2;
const ANUM_PG_TYPE_TYPNAMESPACE: i32 = 3;
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
const ANUM_PG_TYPE_TYPDEFAULTBIN: i32 = 30;
const ANUM_PG_TYPE_TYPDEFAULT: i32 = 31;
const ANUM_PG_SEQUENCE_SEQTYPID: i32 = 2;
const ANUM_PG_SEQUENCE_SEQSTART: i32 = 3;
const ANUM_PG_SEQUENCE_SEQINCREMENT: i32 = 4;
const ANUM_PG_SEQUENCE_SEQMAX: i32 = 5;
const ANUM_PG_SEQUENCE_SEQMIN: i32 = 6;
const ANUM_PG_SEQUENCE_SEQCACHE: i32 = 7;
const ANUM_PG_SEQUENCE_SEQCYCLE: i32 = 8;
const ANUM_PG_ATTRIBUTE_ATTRELID: i32 = 1;
const ANUM_PG_ATTRIBUTE_ATTNUM: i32 = 5;
const ANUM_PG_INDEX_INDEXRELID: i32 = 1;
const ANUM_PG_INDEX_INDCLASS: i32 = 18;
const ANUM_PG_INDEX_INDNATTS: i32 = 3;
const ANUM_PG_INDEX_INDNKEYATTS: i32 = 4;
const ANUM_PG_INDEX_INDISCLUSTERED: i32 = 10;
const ANUM_PG_INDEX_INDISVALID: i32 = 11;
const ANUM_PG_INDEX_INDISREPLIDENT: i32 = 15;
const ANUM_PG_CONSTRAINT_CONNAME: i32 = 2;
const ANUM_PG_CONSTRAINT_CONNAMESPACE: i32 = 3;
const ANUM_PG_CONSTRAINT_CONTYPE: i32 = 4;
const ANUM_PG_CONSTRAINT_CONTYPID: i32 = 10;
const ANUM_PG_CONSTRAINT_CONRELID: i32 = 9;
const ANUM_PG_CONSTRAINT_CONINDID: i32 = 11;
const ANUM_PG_AUTHID_OID: i32 = 1;
const ANUM_PG_AUTHID_ROLNAME: i32 = 2;
const ANUM_PG_AUTHID_ROLSUPER: i32 = 3;
const ANUM_PG_AUTHID_ROLCANLOGIN: i32 = 7;
const ANUM_PG_AUTHID_ROLCONNLIMIT: i32 = 10;
const ANUM_PG_AUTHID_ROLPASSWORD: i32 = 11;
const ANUM_PG_AUTHID_ROLVALIDUNTIL: i32 = 12;
const ANUM_PG_NAMESPACE_OID: i32 = 1;
const ANUM_PG_NAMESPACE_NSPNAME: i32 = 2;
const ANUM_PG_COLLATION_OID: i32 = 1;
const ANUM_PG_COLLATION_COLLNAME: i32 = 2;
const ANUM_PG_COLLATION_COLLNAMESPACE: i32 = 3;
const ANUM_PG_COLLATION_COLLPROVIDER: i32 = 5;
const ANUM_PG_COLLATION_COLLISDETERMINISTIC: i32 = 6;
const ANUM_PG_COLLATION_COLLENCODING: i32 = 7;
const ANUM_PG_COLLATION_COLLCOLLATE: i32 = 8;
const ANUM_PG_COLLATION_COLLCTYPE: i32 = 9;
const ANUM_PG_COLLATION_COLLLOCALE: i32 = 10;
const ANUM_PG_COLLATION_COLLICURULES: i32 = 11;
const ANUM_PG_COLLATION_COLLVERSION: i32 = 12;
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
const ANUM_PG_AGGREGATE_AGGMTRANSFN: i32 = 9;
const ANUM_PG_AGGREGATE_AGGMINVTRANSFN: i32 = 10;
const ANUM_PG_AGGREGATE_AGGMFINALFN: i32 = 11;
const ANUM_PG_AGGREGATE_AGGFINALEXTRA: i32 = 12;
const ANUM_PG_AGGREGATE_AGGMFINALEXTRA: i32 = 13;
const ANUM_PG_AGGREGATE_AGGFINALMODIFY: i32 = 14;
const ANUM_PG_AGGREGATE_AGGMFINALMODIFY: i32 = 15;
const ANUM_PG_AGGREGATE_AGGSORTOP: i32 = 16;
const ANUM_PG_AGGREGATE_AGGTRANSTYPE: i32 = 17;
const ANUM_PG_AGGREGATE_AGGTRANSSPACE: i32 = 18;
const ANUM_PG_AGGREGATE_AGGMTRANSTYPE: i32 = 19;
const ANUM_PG_AGGREGATE_AGGINITVAL: i32 = 21;
const ANUM_PG_AGGREGATE_AGGMINITVAL: i32 = 22;
const ANUM_PG_CAST_OID: i32 = 1;
const ANUM_PG_CAST_CASTFUNC: i32 = 4;
const ANUM_PG_CAST_CASTCONTEXT: i32 = 5;
const ANUM_PG_CAST_CASTMETHOD: i32 = 6;

// Decode-once carriers for the hottest fixed-column projections: warm hit is
// one FxHash probe, no catcache pin / per-column fetch. Coarse invalidation:
// ANY catcache invalidation (catcache::inval_epoch) clears all memos — the
// only channel through which a syscache answer can change.
struct ShapeMemos {
    #[allow(dead_code)]
    mcx: Mcx<'static>,
    epoch: u64,
    type_shape: mcx::PgHashMap<'static, Oid, Option<PgTypeShape>>,
    type_base: mcx::PgHashMap<'static, Oid, Option<syscache_seams::PgTypeBaseShape>>,
    proc: mcx::PgHashMap<'static, Oid, Option<syscache_seams::PgProcShape>>,
    cast: mcx::PgHashMap<'static, u64, Option<PgCastShape>>,
}

thread_local! {
    static MEMOS: core::cell::RefCell<Option<ShapeMemos>> =
        const { core::cell::RefCell::new(None) };
}

// INVARIANT: `f` must not re-enter syscache/catcache (probe or insert only;
// the decode itself runs outside the borrow).
fn with_memos<R>(f: impl FnOnce(&mut ShapeMemos) -> R) -> R {
    MEMOS.with(|cell| {
        let mut slot = cell.borrow_mut();
        let m = slot.get_or_insert_with(|| {
            let mcx = mcx::session_root("SysCacheShapeMemos").mcx();
            // LIFO: empty the droppy TLS memos before the context is freed.
            mcx::register_session_cleanup(Box::new(|| {
                MEMOS.with(|c| drop(c.borrow_mut().take()));
            }));
            ShapeMemos {
                mcx,
                epoch: catcache::inval_epoch(),
                type_shape: mcx::PgHashMap::with_capacity_in(64, mcx),
                type_base: mcx::PgHashMap::with_capacity_in(64, mcx),
                proc: mcx::PgHashMap::with_capacity_in(64, mcx),
                cast: mcx::PgHashMap::with_capacity_in(64, mcx),
            }
        });
        let e = catcache::inval_epoch();
        if m.epoch != e {
            m.epoch = e;
            m.type_shape.clear();
            m.type_base.clear();
            m.proc.clear();
            m.cast.clear();
        }
        f(m)
    })
}

fn tupdesc_for(cache_id: i32) -> &'static TupleDescData<'static> {
    use core::cell::Cell;
    use crate::cacheinfo::SYS_CACHE_SIZE;
    thread_local! {
        // cc_tupdesc is written once at phase-2 init and never replaced, so
        // this flat memo cannot go stale (getattr calls this per column).
        static TDS: [Cell<Option<&'static TupleDescData<'static>>>; SYS_CACHE_SIZE] =
            const { [const { Cell::new(None) }; SYS_CACHE_SIZE] };
    }
    TDS.with(|a| match a[cache_id as usize].get() {
        Some(td) => td,
        None => {
            let td = tupdesc_for_slow(cache_id);
            a[cache_id as usize].set(Some(td));
            td
        }
    })
}

#[cold]
fn tupdesc_for_slow(cache_id: i32) -> &'static TupleDescData<'static> {
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
    // SAFETY: caller passes a tuple of this catalog's row type; the read
    // columns are fixed-width NOT NULL leading columns (GETSTRUCT invariant).
    unsafe { types_tuple::fastgetattr_fixed(tuple, attnum, td) }
}

fn pg_class_shape(tuple: &HeapTupleData<'_>) -> PgClassShape {
    PgClassShape {
        oid: getattr(tuple, RELOID, ANUM_PG_CLASS_OID).as_oid(),
        relnamespace: getattr(tuple, RELOID, ANUM_PG_CLASS_RELNAMESPACE).as_oid(),
        relfilenode: getattr(tuple, RELOID, ANUM_PG_CLASS_RELFILENODE).as_oid(),
        reltablespace: getattr(tuple, RELOID, ANUM_PG_CLASS_RELTABLESPACE).as_oid(),
        relisshared: getattr(tuple, RELOID, ANUM_PG_CLASS_RELISSHARED).as_bool(),
        relpersistence: getattr(tuple, RELOID, ANUM_PG_CLASS_RELPERSISTENCE).as_i8(),
        relkind: getattr(tuple, RELOID, ANUM_PG_CLASS_RELKIND).as_i8(),
    }
}

fn pg_attribute_attrelid(tuple: &HeapTupleData<'_>) -> Oid {
    getattr(tuple, ATTNUM, ANUM_PG_ATTRIBUTE_ATTRELID).as_oid()
}

fn pg_index_indexrelid(tuple: &HeapTupleData<'_>) -> Oid {
    getattr(tuple, INDEXRELID, ANUM_PG_INDEX_INDEXRELID).as_oid()
}

fn lookup_pg_index_ls_shape(
    index_oid: Oid,
) -> PgResult<Option<syscache_seams::PgIndexLsShape>> {
    let Some(tuple) =
        SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(index_oid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgIndexLsShape {
        indnatts: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDNATTS).as_i16(),
        indnkeyatts: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDNKEYATTS).as_i16(),
        indisreplident: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDISREPLIDENT).as_bool(),
        indisvalid: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDISVALID).as_bool(),
        indisclustered: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDISCLUSTERED).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_index_indclass_element(index_oid: Oid, index: i32) -> PgResult<Option<Oid>> {
    let Some(tuple) =
        SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(index_oid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let d = getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDCLASS);
    // SAFETY: not-null plain-storage oidvector column of the held tuple
    // (24-byte header, values in place); the seam's precondition bounds
    // `index` under dim1.
    let elem = unsafe {
        let p = d.as_usize() as *const u8;
        let dim1 = *(p.add(16) as *const i32);
        debug_assert!(index >= 0 && index < dim1);
        *(p.add(24) as *const Oid).add(index as usize)
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(elem))
}

fn pg_index_indoption_element(index_oid: Oid, index: i32) -> PgResult<Option<i16>> {
    const ANUM_PG_INDEX_INDOPTION: i32 = 19;
    let Some(tuple) =
        SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(index_oid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let d = getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDOPTION);
    // SAFETY: not-null plain-storage int2vector column of the held tuple
    // (24-byte header, values in place); the seam's precondition bounds
    // `index` under dim1.
    let elem = unsafe {
        let p = d.as_usize() as *const u8;
        let dim1 = *(p.add(16) as *const i32);
        debug_assert!(index >= 0 && index < dim1);
        *(p.add(24) as *const i16).add(index as usize)
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(elem))
}

fn pg_am_amtype_lookup(amoid: Oid) -> PgResult<Option<i8>> {
    const ANUM_PG_AM_AMTYPE: i32 = 4;
    let Some(tuple) = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let amtype = getattr(&t, AMOID, ANUM_PG_AM_AMTYPE).as_i8();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(amtype))
}

fn pg_am_amhandler_lookup(amoid: Oid) -> PgResult<Option<Oid>> {
    const ANUM_PG_AM_AMHANDLER: i32 = 3;
    let Some(tuple) = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let amhandler = getattr(&t, AMOID, ANUM_PG_AM_AMHANDLER).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(amhandler))
}

fn pg_am_amname_lookup(amoid: Oid) -> PgResult<Option<String>> {
    const ANUM_PG_AM_AMNAME: i32 = 2;
    let Some(tuple) = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let d = getattr(&t, AMOID, ANUM_PG_AM_AMNAME);
    // SAFETY: amname is a NameData column; the datum points at its
    // NUL-terminated 64-byte buffer inside the pinned tuple image.
    let name = unsafe {
        let p = d.as_usize() as *const u8;
        let mut len = 0usize;
        while len < 64 && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len)).to_owned()
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(name))
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
    if let Some(hit) = with_memos(|m| m.type_shape.get(&typid).copied()) {
        return Ok(hit);
    }
    let shape = lookup_pg_type_shape_uncached(typid)?;
    with_memos(|m| m.type_shape.insert(typid, shape));
    Ok(shape)
}

fn lookup_pg_type_shape_uncached(typid: Oid) -> PgResult<Option<PgTypeShape>> {
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

fn lookup_pg_sequence_form(relid: Oid) -> PgResult<Option<syscache_seams::PgSequenceForm>> {
    let Some(tuple) = SearchSysCache1(SEQRELID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let form = syscache_seams::PgSequenceForm {
        seqtypid: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQTYPID).as_oid(),
        seqstart: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQSTART).as_i64(),
        seqincrement: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQINCREMENT).as_i64(),
        seqmax: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQMAX).as_i64(),
        seqmin: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQMIN).as_i64(),
        seqcache: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQCACHE).as_i64(),
        seqcycle: getattr(&t, SEQRELID, ANUM_PG_SEQUENCE_SEQCYCLE).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(form))
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

const ANUM_PG_OPCLASS_OID: i32 = 1;
const ANUM_PG_OPCLASS_OPCMETHOD: i32 = 2;
const ANUM_PG_OPCLASS_OPCFAMILY: i32 = 6;
const ANUM_PG_OPCLASS_OPCINTYPE: i32 = 7;
const ANUM_PG_OPCLASS_OPCKEYTYPE: i32 = 9;

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
        opckeytype: getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCKEYTYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_amop_rows<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    opfamily: Oid,
) -> PgResult<(mcx::PgVec<'mcx, syscache_seams::PgAmopRow>, bool)> {
    const ANUM_PG_AMOP_AMOPPURPOSE: i32 = 6;
    const ANUM_PG_AMOP_AMOPSORTFAMILY: i32 = 9;
    let list = SearchSysCacheList1(AMOPSTRATEGY, SysCacheKey::Value(Datum::from_oid(opfamily)))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgAmopRow {
            amopfamily: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPFAMILY).as_oid(),
            amoplefttype: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPLEFTTYPE).as_oid(),
            amoprighttype: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPRIGHTTYPE).as_oid(),
            amopstrategy: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPSTRATEGY).as_i16(),
            amoppurpose: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPPURPOSE).as_i8(),
            amopopr: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPOPR).as_oid(),
            amopsortfamily: getattr(&t, AMOPSTRATEGY, ANUM_PG_AMOP_AMOPSORTFAMILY).as_oid(),
        });
    }
    let ordered = list.ordered;
    ReleaseSysCacheList(list);
    Ok((out, ordered))
}

fn lookup_pg_amproc_rows<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    opfamily: Oid,
) -> PgResult<(mcx::PgVec<'mcx, syscache_seams::PgAmprocRow>, bool)> {
    const ANUM_PG_AMPROC_AMPROCFAMILY: i32 = 2;
    const ANUM_PG_AMPROC_AMPROCLEFTTYPE: i32 = 3;
    let list = SearchSysCacheList1(AMPROCNUM, SysCacheKey::Value(Datum::from_oid(opfamily)))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgAmprocRow {
            amprocfamily: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCFAMILY).as_oid(),
            amproclefttype: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCLEFTTYPE).as_oid(),
            amprocrighttype: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCRIGHTTYPE).as_oid(),
            amprocnum: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCNUM).as_i16(),
            amproc: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROC).as_oid(),
        });
    }
    let ordered = list.ordered;
    ReleaseSysCacheList(list);
    Ok((out, ordered))
}

fn lookup_pg_opclass_rows_by_am<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    amoid: Oid,
) -> PgResult<mcx::PgVec<'mcx, (Oid, Oid, Oid, bool, types_tuple::NameData)>> {
    const ANUM_PG_OPCLASS_OID: i32 = 1;
    const ANUM_PG_OPCLASS_OPCNAME: i32 = 3;
    const ANUM_PG_OPCLASS_OPCDEFAULT: i32 = 8;
    let list = SearchSysCacheList1(CLAAMNAMENSP, SysCacheKey::Value(Datum::from_oid(amoid)))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        // SAFETY: opcname datum points at the row's inline NameData column.
        let name = unsafe {
            *(getattr(&t, CLAAMNAMENSP, ANUM_PG_OPCLASS_OPCNAME).as_usize()
                as *const types_tuple::NameData)
        };
        out.push((
            getattr(&t, CLAAMNAMENSP, ANUM_PG_OPCLASS_OID).as_oid(),
            getattr(&t, CLAAMNAMENSP, ANUM_PG_OPCLASS_OPCFAMILY).as_oid(),
            getattr(&t, CLAAMNAMENSP, ANUM_PG_OPCLASS_OPCINTYPE).as_oid(),
            getattr(&t, CLAAMNAMENSP, ANUM_PG_OPCLASS_OPCDEFAULT).as_bool(),
            name,
        ));
    }
    ReleaseSysCacheList(list);
    Ok(out)
}

fn pg_opclass_opcname(opclass: Oid) -> PgResult<Option<types_tuple::NameData>> {
    let Some(tuple) = SearchSysCache1(CLAOID, SysCacheKey::Value(Datum::from_oid(opclass)))? else {
        return Ok(None);
    };
    const ANUM_PG_OPCLASS_OPCNAME: i32 = 3;
    let d = getattr(&tuple.tuple(), CLAOID, ANUM_PG_OPCLASS_OPCNAME);
    // SAFETY: opcname is a NameData column; the datum points at its 64-byte
    // inline image inside the held tuple.
    let name = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn pg_opclass_name_namespace_method(
    opclass: Oid,
) -> PgResult<Option<(types_tuple::NameData, Oid, Oid)>> {
    let Some(tuple) = SearchSysCache1(CLAOID, SysCacheKey::Value(Datum::from_oid(opclass)))? else {
        return Ok(None);
    };
    const ANUM_PG_OPCLASS_OPCNAME: i32 = 3;
    const ANUM_PG_OPCLASS_OPCNAMESPACE: i32 = 4;
    let t = tuple.tuple();
    let d = getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCNAME);
    // SAFETY: opcname is a NameData column; the datum points at its 64-byte
    // inline image inside the held tuple.
    let name = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    let nsp = getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCNAMESPACE).as_oid();
    let method = getattr(&t, CLAOID, ANUM_PG_OPCLASS_OPCMETHOD).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some((name, nsp, method)))
}

fn lookup_pg_opfamily_oid_exact(amoid: Oid, opfname: &str, nsp: Oid) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache3(
        OPFAMILYAMNAMENSP,
        SysCacheKey::Value(Datum::from_oid(amoid)),
        SysCacheKey::Str(opfname),
        SysCacheKey::Value(Datum::from_oid(nsp)),
    )?
    else {
        return Ok(0);
    };
    let oid = getattr(&tuple.tuple(), OPFAMILYAMNAMENSP, 1).as_oid();
    ReleaseSysCache(tuple);
    Ok(oid)
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

fn lookup_authid_rolpassword<'mcx>(
    mcx: Mcx<'mcx>,
    rolname: &str,
) -> PgResult<Option<syscache_seams::AuthIdPasswordShape<'mcx>>> {
    let Some(tuple) = SearchSysCache1(AUTHNAME, SysCacheKey::Str(rolname))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::AuthIdPasswordShape {
        rolpassword: text_attr(mcx, &t, AUTHNAME, ANUM_PG_AUTHID_ROLPASSWORD)?,
        rolvaliduntil: getattr_nullable(&t, AUTHNAME, ANUM_PG_AUTHID_ROLVALIDUNTIL)
            .map(|d| d.as_i64()),
    };
    drop(t);
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

fn lookup_authid_rolname_data(roleid: Oid) -> PgResult<Option<types_tuple::NameData>> {
    let Some(tuple) = SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? else {
        return Ok(None);
    };
    let d = getattr(&tuple.tuple(), AUTHOID, ANUM_PG_AUTHID_ROLNAME);
    // SAFETY: rolname is a NameData column; the datum points at its 64-byte
    // buffer inside the pinned tuple image.
    let name = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
    ReleaseSysCache(tuple);
    Ok(Some(name))
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

fn search_syscache_exists_procoid(funcid: Oid) -> PgResult<bool> {
    SearchSysCacheExists(
        PROCOID,
        SysCacheKey::Value(Datum::from_oid(funcid)),
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

fn search_syscache_exists_databaseoid(dboid: Oid) -> PgResult<bool> {
    SearchSysCacheExists(
        crate::cacheinfo::DATABASEOID,
        SysCacheKey::Value(Datum::from_oid(dboid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn search_syscache_exists_tablespaceoid(tblspcoid: Oid) -> PgResult<bool> {
    SearchSysCacheExists(
        crate::cacheinfo::TABLESPACEOID,
        SysCacheKey::Value(Datum::from_oid(tblspcoid)),
        SysCacheKey::UNUSED,
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

const ANUM_PG_TYPE_TYPNOTNULL: i32 = 25;

fn pg_type_domain_shape(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeDomainShape>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgTypeDomainShape {
        typname: getattr_name(&t, TYPEOID, ANUM_PG_TYPE_TYPNAME),
        typnamespace: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNAMESPACE).as_oid(),
        typtype: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPTYPE).as_i8(),
        typnotnull: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNOTNULL).as_bool(),
        typbasetype: getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPBASETYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_type_name_namespace(typid: Oid) -> PgResult<Option<(NameData, Oid)>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let name = getattr_name(&t, TYPEOID, ANUM_PG_TYPE_TYPNAME);
    let nsp = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNAMESPACE).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some((name, nsp)))
}

fn lookup_pg_constraint_shape(
    conoid: Oid,
) -> PgResult<Option<syscache_seams::PgConstraintShape>> {
    let Some(tuple) = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(conoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgConstraintShape {
        conname: getattr_name(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNAME),
        contype: getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPE).as_i8(),
        conindid: getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONINDID).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_constraint_desc_shape(
    conoid: Oid,
) -> PgResult<Option<syscache_seams::PgConstraintDescShape>> {
    let Some(tuple) = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(conoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgConstraintDescShape {
        conname: getattr_name(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNAME),
        connamespace: getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNAMESPACE).as_oid(),
        conrelid: getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONRELID).as_oid(),
        contypid: getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPID).as_oid(),
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

const ANUM_PG_ATTRIBUTE_ATTNAME: i32 = 2;
const ANUM_PG_ATTRIBUTE_ATTTYPID: i32 = 3;
const ANUM_PG_ATTRIBUTE_ATTTYPMOD: i32 = 6;
const ANUM_PG_ATTRIBUTE_ATTGENERATED: i32 = 16;
const ANUM_PG_ATTRIBUTE_ATTISDROPPED2: i32 = 17;
const ANUM_PG_ATTRIBUTE_ATTCOLLATION: i32 = 20;

fn lookup_pg_attribute_shape(
    relid: Oid,
    attnum: types_core::AttrNumber,
) -> PgResult<Option<syscache_seams::PgAttributeLsShape>> {
    let Some(tuple) = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    if getattr(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTISDROPPED2).as_bool() {
        drop(t);
        ReleaseSysCache(tuple);
        return Ok(None);
    }
    let shape = syscache_seams::PgAttributeLsShape {
        attname: getattr_name(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTNAME),
        atttypid: getattr(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTTYPID).as_oid(),
        atttypmod: getattr(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTTYPMOD).as_i32(),
        attcollation: getattr(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTCOLLATION).as_oid(),
        attgenerated: getattr(&t, ATTNUM, ANUM_PG_ATTRIBUTE_ATTGENERATED).as_i8(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

// get_attnum (lsyscache.c): InvalidAttrNumber when no such column.
fn lookup_pg_attribute_attnum_by_name(
    relid: Oid,
    attname: &str,
) -> PgResult<types_core::AttrNumber> {
    let Some(tuple) = SearchSysCache2(
        ATTNAME,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Str(attname),
    )?
    else {
        return Ok(0);
    };
    let t = tuple.tuple();
    // SearchSysCacheAttName: dropped columns don't match.
    let attnum = if getattr(&t, ATTNAME, ANUM_PG_ATTRIBUTE_ATTISDROPPED2).as_bool() {
        0
    } else {
        getattr(&t, ATTNAME, ANUM_PG_ATTRIBUTE_ATTNUM).as_i16()
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(attnum)
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

fn lookup_pg_cast_oid(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Oid> {
    const ANUM_PG_CAST_OID: i32 = 1;
    GetSysCacheOid(
        crate::cacheinfo::CASTSOURCETARGET,
        ANUM_PG_CAST_OID,
        SysCacheKey::Value(Datum::from_oid(sourcetypeid)),
        SysCacheKey::Value(Datum::from_oid(targettypeid)),
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
        oprnamespace: getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRNAMESPACE).as_oid(),
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
const ANUM_PG_PROC_PROSECDEF: i32 = 11;
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
const ANUM_PG_PROC_PROALLARGTYPES: i32 = 21;
const ANUM_PG_PROC_PROARGMODES: i32 = 22;
const ANUM_PG_PROC_PROARGNAMES: i32 = 23;
const ANUM_PG_RANGE_RNGTYPID: i32 = 1;
const ANUM_PG_RANGE_RNGSUBTYPE: i32 = 2;
const ANUM_PG_RANGE_RNGMULTITYPID: i32 = 3;
const ANUM_PG_RANGE_RNGCOLLATION: i32 = 4;
const ANUM_PG_RANGE_RNGSUBOPC: i32 = 5;
const ANUM_PG_RANGE_RNGCANONICAL: i32 = 6;
const ANUM_PG_RANGE_RNGSUBDIFF: i32 = 7;
const ANUM_PG_PROC_PROARGDEFAULTS: i32 = 24;
const ANUM_PG_PROC_PROLANG: i32 = 5;
const ANUM_PG_PROC_PROSRC: i32 = 26;
const ANUM_PG_PROC_PROCONFIG: i32 = 29;
const ANUM_PG_AMPROC_AMPROCRIGHTTYPE: i32 = 4;
const ANUM_PG_AMPROC_AMPROCNUM: i32 = 5;
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
const ANUM_PG_CLASS_RELHASSUBCLASS: i32 = 23;
const ANUM_PG_CLASS_RELOFTYPE: i32 = 5;
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
        relhassubclass: getattr(&t, RELOID, ANUM_PG_CLASS_RELHASSUBCLASS).as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_class_reloftype(relid: Oid) -> PgResult<Option<Oid>> {
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let reloftype = getattr(&t, RELOID, ANUM_PG_CLASS_RELOFTYPE).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(reloftype))
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

fn lookup_pg_amproc_members<'mcx>(
    mcx: Mcx<'mcx>,
    opfamily: Oid,
    lefttype: Oid,
) -> PgResult<PgVec<'mcx, syscache_seams::PgAmprocMemberShape>> {
    let list = SearchSysCacheList(
        AMPROCNUM,
        2,
        SysCacheKey::Value(Datum::from_oid(opfamily)),
        SysCacheKey::Value(Datum::from_oid(lefttype)),
        SysCacheKey::UNUSED,
    )?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgAmprocMemberShape {
            amprocrighttype: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCRIGHTTYPE).as_oid(),
            amprocnum: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROCNUM).as_i16(),
            amproc: getattr(&t, AMPROCNUM, ANUM_PG_AMPROC_AMPROC).as_oid(),
        });
    }
    ReleaseSysCacheList(list);
    Ok(out)
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
    if let Some(hit) = with_memos(|m| m.type_base.get(&typid).copied()) {
        return Ok(hit);
    }
    let shape = pg_type_base_shape_uncached(typid)?;
    with_memos(|m| m.type_base.insert(typid, shape));
    Ok(shape)
}

fn pg_type_base_shape_uncached(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeBaseShape>> {
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

// Decode-once Form cache (AGENTS rule 6): C hands back the cached FormData
// pointer for free; the 13-field decode below must not run per probe. TYPEOID
// invalidation (targeted or full-reset hash 0) flushes the whole memo.
mod io_shape_memo {
    use core::cell::RefCell;

    use datum::Datum;
    use mcx::{MemoryContext, PgHashMap};
    use types_core::{InvalidOid, Oid};

    use crate::cacheinfo::TYPEOID;

    thread_local! {
        static MEMO: RefCell<Option<PgHashMap<'static, Oid, syscache_seams::PgTypeIoShape>>> =
            const { RefCell::new(None) };
    }

    fn flush(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
        MEMO.with(|m| {
            if let Some(map) = m.borrow_mut().as_mut() {
                map.clear();
            }
        });
    }

    pub(super) fn get(typid: Oid) -> Option<syscache_seams::PgTypeIoShape> {
        MEMO.with(|m| m.borrow().as_ref().and_then(|map| map.get(&typid).copied()))
    }

    pub(super) fn insert(typid: Oid, shape: syscache_seams::PgTypeIoShape) {
        MEMO.with(|m| {
            let mut slot = m.borrow_mut();
            if slot.is_none() {
                let registered = inval::invalidate::CacheRegisterSyscacheCallback(
                    TYPEOID,
                    flush,
                    Datum::from_oid(InvalidOid),
                );
                if registered.is_err() {
                    return; // out of callback slots: run unmemoized
                }
                let mcx = ::mcx::session_root("TypeIoShapeMemo").mcx();
                // LIFO: empty the droppy TLS memo before the context is freed.
                ::mcx::register_session_cleanup(Box::new(|| {
                    MEMO.with(|c| drop(c.borrow_mut().take()));
                }));
                *slot = Some(PgHashMap::with_capacity_in(16, mcx));
            }
            slot.as_mut().unwrap().insert(typid, shape);
        });
    }
}

fn pg_type_io_shape(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeIoShape>> {
    if let Some(shape) = io_shape_memo::get(typid) {
        return Ok(Some(shape));
    }
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
    io_shape_memo::insert(typid, shape);
    Ok(Some(shape))
}

fn pg_type_default_strings<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
) -> PgResult<Option<syscache_seams::PgTypeDefaultShape<'mcx>>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let text_attr = |anum: i32| -> PgResult<Option<mcx::PgString<'mcx>>> {
        match varlena_image(mcx, &t, TYPEOID, anum)? {
            None => Ok(None),
            Some(img) => {
                let s = core::str::from_utf8(&img[4..]).expect("pg_type default is text");
                Ok(Some(mcx::PgString::from_str_in(s, mcx)?))
            }
        }
    };
    let shape = syscache_seams::PgTypeDefaultShape {
        typdefaultbin: text_attr(ANUM_PG_TYPE_TYPDEFAULTBIN)?,
        typdefault: text_attr(ANUM_PG_TYPE_TYPDEFAULT)?,
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

fn pg_type_typrelid(typid: Oid) -> PgResult<Option<Oid>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let relid = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPRELID).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(relid))
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
    if let Some(hit) = with_memos(|m| m.proc.get(&funcid).copied()) {
        return Ok(hit);
    }
    let shape = lookup_pg_proc_shape_uncached(funcid)?;
    with_memos(|m| m.proc.insert(funcid, shape));
    Ok(shape)
}

fn lookup_pg_proc_shape_uncached(funcid: Oid) -> PgResult<Option<syscache_seams::PgProcShape>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgProcShape {
        pronamespace: getattr(&t, PROCOID, ANUM_PG_PROC_PRONAMESPACE).as_oid(),
        prorettype: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETTYPE).as_oid(),
        provariadic: getattr(&t, PROCOID, ANUM_PG_PROC_PROVARIADIC).as_oid(),
        prosupport: getattr(&t, PROCOID, ANUM_PG_PROC_PROSUPPORT).as_oid(),
        prolang: getattr(&t, PROCOID, ANUM_PG_PROC_PROLANG).as_oid(),
        pronargs: getattr(&t, PROCOID, ANUM_PG_PROC_PRONARGS).as_i16(),
        prokind: getattr(&t, PROCOID, ANUM_PG_PROC_PROKIND).as_i8(),
        provolatile: getattr(&t, PROCOID, ANUM_PG_PROC_PROVOLATILE).as_i8(),
        proparallel: getattr(&t, PROCOID, ANUM_PG_PROC_PROPARALLEL).as_i8(),
        proretset: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETSET).as_bool(),
        proisstrict: getattr(&t, PROCOID, ANUM_PG_PROC_PROISSTRICT).as_bool(),
        proleakproof: getattr(&t, PROCOID, ANUM_PG_PROC_PROLEAKPROOF).as_bool(),
        prosecdef: getattr(&t, PROCOID, ANUM_PG_PROC_PROSECDEF).as_bool(),
        proconfig_isnull: getattr_nullable(&t, PROCOID, ANUM_PG_PROC_PROCONFIG).is_none(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_LANGUAGE_LANPLCALLFOID: i32 = 6;
const ANUM_PG_LANGUAGE_LANINLINE: i32 = 7;
const ANUM_PG_LANGUAGE_LANVALIDATOR: i32 = 8;

fn lookup_pg_language_fmgr(
    langoid: Oid,
) -> PgResult<Option<syscache_seams::PgLanguageFmgrShape>> {
    use crate::cacheinfo::LANGOID;
    let Some(tuple) = SearchSysCache1(LANGOID, SysCacheKey::Value(Datum::from_oid(langoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgLanguageFmgrShape {
        lanplcallfoid: getattr(&t, LANGOID, ANUM_PG_LANGUAGE_LANPLCALLFOID).as_oid(),
        laninline: getattr(&t, LANGOID, ANUM_PG_LANGUAGE_LANINLINE).as_oid(),
        lanvalidator: getattr(&t, LANGOID, ANUM_PG_LANGUAGE_LANVALIDATOR).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_LANGUAGE_LANNAME: i32 = 2;

fn lookup_pg_language_name(langoid: Oid) -> PgResult<Option<NameData>> {
    use crate::cacheinfo::LANGOID;
    let Some(tuple) = SearchSysCache1(LANGOID, SysCacheKey::Value(Datum::from_oid(langoid)))?
    else {
        return Ok(None);
    };
    let name = getattr_name(&tuple.tuple(), LANGOID, ANUM_PG_LANGUAGE_LANNAME);
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

fn lookup_pg_proc_fmgr(funcid: Oid) -> PgResult<Option<syscache_seams::PgProcFmgrShape>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgProcFmgrShape {
        prolang: getattr(&t, PROCOID, ANUM_PG_PROC_PROLANG).as_oid(),
        prorettype: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETTYPE).as_oid(),
        pronargs: getattr(&t, PROCOID, ANUM_PG_PROC_PRONARGS).as_i16(),
        proisstrict: getattr(&t, PROCOID, ANUM_PG_PROC_PROISSTRICT).as_bool(),
        proretset: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETSET).as_bool(),
        prosecdef: getattr(&t, PROCOID, ANUM_PG_PROC_PROSECDEF).as_bool(),
        proconfig_isnull: getattr_nullable(&t, PROCOID, ANUM_PG_PROC_PROCONFIG).is_none(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

const ANUM_PG_PROC_PROOWNER: i32 = 4;

fn lookup_pg_proc_secdef(funcid: Oid) -> PgResult<Option<syscache_seams::PgProcSecdefShape>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let cx = mcx::MemoryContext::new("lookup_pg_proc_secdef");
    let proconfig = match varlena_image(cx.mcx(), &t, PROCOID, ANUM_PG_PROC_PROCONFIG)? {
        Some(img) => {
            let elems =
                datum::array_build::deconstruct_array_image(cx.mcx(), &img, -1, false, b'i')?;
            let mut out = Vec::with_capacity(elems.len());
            for e in elems.iter() {
                let ep = e.as_usize() as *const u8;
                // SAFETY: by-ref text element datum inside the detoasted image.
                let payload = unsafe {
                    if types_tuple::varatt::varatt_is_1b(ep) {
                        let raw = types_tuple::varatt::varsize_1b(ep);
                        core::slice::from_raw_parts(ep.add(1), raw - 1)
                    } else {
                        let raw = types_tuple::varatt::varsize_4b(ep);
                        core::slice::from_raw_parts(ep.add(4), raw - 4)
                    }
                };
                out.push(String::from_utf8_lossy(payload).into_owned());
            }
            Some(out)
        }
        None => None,
    };
    let shape = syscache_seams::PgProcSecdefShape {
        proowner: getattr(&t, PROCOID, ANUM_PG_PROC_PROOWNER).as_oid(),
        prosecdef: getattr(&t, PROCOID, ANUM_PG_PROC_PROSECDEF).as_bool(),
        proconfig,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_proc_prosrc<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROSRC)? {
        Some(img) => {
            let s = core::str::from_utf8(&img[4..]).expect("prosrc is server-encoding text");
            Some(PgString::from_str_in(s, mcx)?)
        }
        None => None,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(out)
}

fn lookup_pg_proc_probin<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    const ANUM_PG_PROC_PROBIN: i32 = 27;
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROBIN)? {
        Some(img) => {
            let s = core::str::from_utf8(&img[4..]).expect("probin is server-encoding text");
            Some(PgString::from_str_in(s, mcx)?)
        }
        None => None,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(out)
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

fn lookup_pg_opclass_oid_by_name(amid: Oid, opcname: &str, opcnamespace: Oid) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache3(
        CLAAMNAMENSP,
        SysCacheKey::Value(Datum::from_oid(amid)),
        SysCacheKey::Str(opcname),
        SysCacheKey::Value(Datum::from_oid(opcnamespace)),
    )?
    else {
        return Ok(0);
    };
    let oid = getattr(&tuple.tuple(), CLAAMNAMENSP, ANUM_PG_OPCLASS_OID).as_oid();
    ReleaseSysCache(tuple);
    Ok(oid)
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

fn pg_operator_oprnamensp(opno: Oid) -> PgResult<Option<(NameData, Oid)>> {
    let Some(tuple) = SearchSysCache1(OPEROID, SysCacheKey::Value(Datum::from_oid(opno)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let row = (
        getattr_name(&t, OPEROID, ANUM_PG_OPERATOR_OPRNAME),
        getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRNAMESPACE).as_oid(),
    );
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(row))
}

fn lookup_pg_ts_config_oid_by_name_nsp(cfgname: &str, cfgnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        TSCONFIGNAMENSP,
        ANUM_PG_TS_CONFIG_OID,
        SysCacheKey::Str(cfgname),
        SysCacheKey::Value(Datum::from_oid(cfgnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_ts_config_row(cfgid: Oid) -> PgResult<Option<syscache_seams::PgTsObjectRow>> {
    let Some(tuple) = SearchSysCache1(TSCONFIGOID, SysCacheKey::Value(Datum::from_oid(cfgid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let row = syscache_seams::PgTsObjectRow {
        name: getattr_name(&t, TSCONFIGOID, ANUM_PG_TS_CONFIG_CFGNAME),
        namespace_oid: getattr(&t, TSCONFIGOID, ANUM_PG_TS_CONFIG_CFGNAMESPACE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(row))
}

fn lookup_pg_conversion_oid_by_name_nsp(conname: &str, connamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        crate::cacheinfo::CONNAMENSP,
        ANUM_PG_CONVERSION_OID,
        SysCacheKey::Str(conname),
        SysCacheKey::Value(Datum::from_oid(connamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

const ANUM_PG_STATISTIC_EXT_OID: i32 = 1;

fn lookup_pg_statistic_ext_oid_by_name_nsp(stxname: &str, stxnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        crate::cacheinfo::STATEXTNAMENSP,
        ANUM_PG_STATISTIC_EXT_OID,
        SysCacheKey::Str(stxname),
        SysCacheKey::Value(Datum::from_oid(stxnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_ts_dict_oid_by_name_nsp(dictname: &str, dictnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        TSDICTNAMENSP,
        ANUM_PG_TS_DICT_OID,
        SysCacheKey::Str(dictname),
        SysCacheKey::Value(Datum::from_oid(dictnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_ts_dict_row(dictid: Oid) -> PgResult<Option<syscache_seams::PgTsObjectRow>> {
    let Some(tuple) = SearchSysCache1(TSDICTOID, SysCacheKey::Value(Datum::from_oid(dictid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let row = syscache_seams::PgTsObjectRow {
        name: getattr_name(&t, TSDICTOID, ANUM_PG_TS_DICT_DICTNAME),
        namespace_oid: getattr(&t, TSDICTOID, ANUM_PG_TS_DICT_DICTNAMESPACE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(row))
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
    let key = ((sourcetypeid as u64) << 32) | targettypeid as u64;
    if let Some(hit) = with_memos(|m| m.cast.get(&key).copied()) {
        return Ok(hit);
    }
    let shape = lookup_pg_cast_shape_uncached(sourcetypeid, targettypeid)?;
    with_memos(|m| m.cast.insert(key, shape));
    Ok(shape)
}

fn lookup_pg_cast_shape_uncached(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Option<PgCastShape>> {
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

fn lookup_pg_collation_shape(
    colloid: Oid,
) -> PgResult<Option<syscache_seams::PgCollationShape>> {
    let Some(tuple) = SearchSysCache1(COLLOID, SysCacheKey::Value(Datum::from_oid(colloid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgCollationShape {
        // SAFETY: collname is a NameData column; the datum points at its
        // 64-byte in-tuple block.
        collname: unsafe {
            *(getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLNAME).as_usize() as *const NameData)
        },
        collnamespace: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLNAMESPACE).as_oid(),
        collisdeterministic: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLISDETERMINISTIC)
            .as_bool(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
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
    // SAFETY: non-null varlena attr datum points into the live tuple; the
    // image spans exactly its header-declared size (external = 2 + tag size,
    // short = 7-bit length, else the 4-byte word).
    let src = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    // PG_DETOAST_DATUM: fetch/decompress/unpack to a plain 4B-header image.
    Ok(Some(detoast::detoast_attr(mcx, src)?))
}

const ANUM_PG_STATISTIC_EXT_STXRELID: i32 = 2;
const ANUM_PG_STATISTIC_EXT_STXKEYS: i32 = 6;
const ANUM_PG_STATISTIC_EXT_STXSTATTARGET: i32 = 7;
const ANUM_PG_STATISTIC_EXT_STXKIND: i32 = 8;
const ANUM_PG_STATISTIC_EXT_STXEXPRS: i32 = 9;
const ANUM_PG_STATISTIC_EXT_DATA_STXDNDISTINCT: i32 = 3;
const ANUM_PG_STATISTIC_EXT_DATA_STXDDEPENDENCIES: i32 = 4;
const ANUM_PG_STATISTIC_EXT_DATA_STXDMCV: i32 = 5;
const ANUM_PG_STATISTIC_EXT_DATA_STXDEXPR: i32 = 6;

fn statext_form<'mcx>(
    mcx: Mcx<'mcx>,
    statoid: Oid,
) -> PgResult<Option<syscache_seams::StatExtForm<'mcx>>> {
    let Some(tuple) = SearchSysCache1(STATEXTOID, SysCacheKey::Value(Datum::from_oid(statoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let stxrelid = getattr(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXRELID).as_oid();
    let keys_img = varlena_image(mcx, &t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXKEYS)?
        .expect("stxkeys is not null");
    let kinds_img = varlena_image(mcx, &t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXKIND)?
        .expect("stxkind is not null");
    let (target_d, target_null) =
        SysCacheGetAttr(STATEXTOID, &tuple, ANUM_PG_STATISTIC_EXT_STXSTATTARGET)?;
    let stattarget = if target_null { -1 } else { target_d.as_i16() as i32 };
    let (_, exprs_null) = SysCacheGetAttr(STATEXTOID, &tuple, ANUM_PG_STATISTIC_EXT_STXEXPRS)?;

    let nkeys = datum::array_build::array_image_nelems(&keys_img);
    let mut keys: PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, nkeys)?;
    for i in 0..nkeys {
        let off = 4 + 20 + i * 2;
        keys.push(i16::from_ne_bytes(keys_img[off..off + 2].try_into().unwrap()));
    }
    let nkinds = datum::array_build::array_image_nelems(&kinds_img);
    let mut kinds: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, nkinds)?;
    for i in 0..nkinds {
        kinds.push(kinds_img[4 + 20 + i]);
    }

    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(syscache_seams::StatExtForm {
        stxrelid,
        keys,
        kinds,
        stattarget,
        has_exprs: !exprs_null,
    }))
}

fn statext_data_kinds(statoid: Oid, inh: bool) -> PgResult<Option<(bool, bool, bool, bool)>> {
    let Some(tuple) = SearchSysCache2(
        STATEXTDATASTXOID,
        SysCacheKey::Value(Datum::from_oid(statoid)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    else {
        return Ok(None);
    };
    let mut flags = [false; 4];
    for (i, anum) in [
        ANUM_PG_STATISTIC_EXT_DATA_STXDNDISTINCT,
        ANUM_PG_STATISTIC_EXT_DATA_STXDDEPENDENCIES,
        ANUM_PG_STATISTIC_EXT_DATA_STXDMCV,
        ANUM_PG_STATISTIC_EXT_DATA_STXDEXPR,
    ]
    .into_iter()
    .enumerate()
    {
        let (_, isnull) = SysCacheGetAttr(STATEXTDATASTXOID, &tuple, anum)?;
        flags[i] = !isnull;
    }
    ReleaseSysCache(tuple);
    Ok(Some((flags[0], flags[1], flags[2], flags[3])))
}

fn statext_data_blob<'mcx>(
    mcx: Mcx<'mcx>,
    statoid: Oid,
    inh: bool,
    anum: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(tuple) = SearchSysCache2(
        STATEXTDATASTXOID,
        SysCacheKey::Value(Datum::from_oid(statoid)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let img = varlena_image(mcx, &t, STATEXTDATASTXOID, anum)?;
    drop(t);
    ReleaseSysCache(tuple);
    Ok(img)
}

fn statext_exprs_src<'mcx>(mcx: Mcx<'mcx>, statoid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let Some(tuple) = SearchSysCache1(STATEXTOID, SysCacheKey::Value(Datum::from_oid(statoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = text_attr(mcx, &t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXEXPRS)?;
    drop(t);
    ReleaseSysCache(tuple);
    Ok(out)
}

fn statext_expressions_load<'mcx>(
    mcx: Mcx<'mcx>,
    statoid: Oid,
    inh: bool,
    idx: i32,
) -> PgResult<syscache_seams::PgStatisticBundle<'mcx>> {
    let img = statext_data_blob(mcx, statoid, inh, ANUM_PG_STATISTIC_EXT_DATA_STXDEXPR)?
        .unwrap_or_else(|| {
            panic!(
                "requested statistics kind \"e\" is not yet built for statistics object {statoid}"
            )
        });
    let elems = datum::array_build::deconstruct_array_image(mcx, &img, -1, false, b'd')?;
    let d = elems[idx as usize];
    let p = d.as_usize() as *const u8;
    // SAFETY: composite datum inside the detoasted stxdexpr image; its
    // varlena word (HeapTupleHeaderGetDatumLength) declares the image size.
    let tup = unsafe {
        let t_len = u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2;
        types_tuple::HeapTupleData::from_raw_parts(
            p,
            t_len,
            types_tuple::ItemPointerData::invalid(),
            InvalidOid,
        )
    };
    let mut slots = PgVec::new_in(mcx);
    for i in 0..STATISTIC_NUM_SLOTS {
        let kind = getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STAKIND1 + i).as_i16();
        if kind == 0 {
            continue;
        }
        let numbers_image =
            varlena_image(mcx, &tup, STATRELATTINH, ANUM_PG_STATISTIC_STANUMBERS1 + i)?
                .unwrap_or(PgVec::new_in(mcx));
        let numbers = decode_pg_statistic_numbers(mcx, &numbers_image)?;
        let (values_image, valuetype) =
            match varlena_image(mcx, &tup, STATRELATTINH, ANUM_PG_STATISTIC_STAVALUES1 + i)? {
                Some(vimg) => {
                    let elemtype = datum::array_build::array_image_elemtype(&vimg);
                    (vimg, elemtype)
                }
                None => (PgVec::new_in(mcx), InvalidOid),
            };
        let values = decode_pg_statistic_values(mcx, valuetype, &values_image)?;
        slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
            kind,
            getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STAOP1 + i).as_oid(),
            getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STACOLL1 + i).as_oid(),
            valuetype,
            values,
            numbers,
            values_image,
        ));
    }
    Ok(syscache_seams::PgStatisticBundle {
        stanullfrac: getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STANULLFRAC).as_f32(),
        stawidth: getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STAWIDTH).as_i32(),
        stadistinct: getattr(&tup, STATRELATTINH, ANUM_PG_STATISTIC_STADISTINCT).as_f32(),
        slots,
    })
}

fn text_attr<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    cache_id: i32,
    attnum: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    match varlena_image(mcx, tuple, cache_id, attnum)? {
        Some(img) => {
            let s = core::str::from_utf8(&img[4..]).expect("server-encoding text attr");
            Ok(Some(PgString::from_str_in(s, mcx)?))
        }
        None => Ok(None),
    }
}

fn lookup_pg_collation_locale_row<'mcx>(
    mcx: Mcx<'mcx>,
    collid: Oid,
) -> PgResult<Option<syscache_seams::PgCollationLocaleRow<'mcx>>> {
    let Some(tuple) = SearchSysCache1(COLLOID, SysCacheKey::Value(Datum::from_oid(collid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = syscache_seams::PgCollationLocaleRow {
        collname: getattr_name(&t, COLLOID, ANUM_PG_COLLATION_COLLNAME),
        collnamespace: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLNAMESPACE).as_oid(),
        collprovider: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLPROVIDER).as_i8() as u8,
        collisdeterministic: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLISDETERMINISTIC)
            .as_bool(),
        collencoding: getattr(&t, COLLOID, ANUM_PG_COLLATION_COLLENCODING).as_i32(),
        collcollate: text_attr(mcx, &t, COLLOID, ANUM_PG_COLLATION_COLLCOLLATE)?,
        collctype: text_attr(mcx, &t, COLLOID, ANUM_PG_COLLATION_COLLCTYPE)?,
        colllocale: text_attr(mcx, &t, COLLOID, ANUM_PG_COLLATION_COLLLOCALE)?,
        collicurules: text_attr(mcx, &t, COLLOID, ANUM_PG_COLLATION_COLLICURULES)?,
        collversion: text_attr(mcx, &t, COLLOID, ANUM_PG_COLLATION_COLLVERSION)?,
    };
    ReleaseSysCache(tuple);
    Ok(Some(out))
}

fn lookup_pg_collation_by_name_enc_nsp(
    collname: &str,
    encoding: i32,
    collnamespace: Oid,
) -> PgResult<Option<syscache_seams::PgCollationNameEncNspRow>> {
    let Some(tuple) = SearchSysCache3(
        COLLNAMEENCNSP,
        SysCacheKey::Str(collname),
        SysCacheKey::Value(Datum::from_i32(encoding)),
        SysCacheKey::Value(Datum::from_oid(collnamespace)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = syscache_seams::PgCollationNameEncNspRow {
        oid: getattr(&t, COLLNAMEENCNSP, ANUM_PG_COLLATION_OID).as_oid(),
        collprovider: getattr(&t, COLLNAMEENCNSP, ANUM_PG_COLLATION_COLLPROVIDER).as_i8() as u8,
    };
    ReleaseSysCache(tuple);
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
        // Array images stay in the tuple until a consumer asks (C's
        // get_attstatsslot laziness via lookup_pg_statistic_slot_images).
        slots.push(syscache_seams::PgStatisticSlotData::lazy(
            kind,
            getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STAOP1 + i).as_oid(),
            getattr(&t, STATRELATTINH, ANUM_PG_STATISTIC_STACOLL1 + i).as_oid(),
            mcx,
            relid,
            attnum,
            inh,
            i,
        ));
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

fn lookup_pg_statistic_slot_images<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: types_core::AttrNumber,
    inh: bool,
    pos: i32,
) -> PgResult<syscache_seams::PgStatisticSlotImages<'mcx>> {
    let tuple = SearchSysCache3(
        STATRELATTINH,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
        SysCacheKey::Value(Datum::from_bool(inh)),
    )?
    .unwrap_or_else(|| {
        panic!("pg_statistic row ({relid},{attnum},{inh}) vanished between bundle probe and slot fetch")
    });
    let t = tuple.tuple();
    let numbers_image = varlena_image(mcx, &t, STATRELATTINH, ANUM_PG_STATISTIC_STANUMBERS1 + pos)?
        .unwrap_or(PgVec::new_in(mcx));
    let (values_image, valuetype) =
        match varlena_image(mcx, &t, STATRELATTINH, ANUM_PG_STATISTIC_STAVALUES1 + pos)? {
            Some(img) => {
                let elemtype = datum::array_build::array_image_elemtype(&img);
                (img, elemtype)
            }
            None => (PgVec::new_in(mcx), InvalidOid),
        };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(syscache_seams::PgStatisticSlotImages { valuetype, values_image, numbers_image })
}

fn decode_pg_statistic_values<'mcx>(
    mcx: Mcx<'mcx>,
    valuetype: Oid,
    image: &[u8],
) -> PgResult<PgVec<'mcx, Datum>> {
    if image.is_empty() {
        return Ok(PgVec::new_in(mcx));
    }
    // Via the seam, not the local impl: pg_statistic-only rigs mock TYPEOID.
    let ty = syscache_seams::lookup_pg_type_shape::call(valuetype)?
        .expect("stavalues element type has a pg_type row");
    datum::array_build::deconstruct_array_image(
        mcx,
        image,
        ty.typlen,
        ty.typbyval,
        ty.typalign as u8,
    )
}

fn decode_pg_statistic_numbers<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<PgVec<'mcx, f32>> {
    if image.is_empty() {
        return Ok(PgVec::new_in(mcx));
    }
    let elems = datum::array_build::deconstruct_array_image(mcx, image, 4, true, b'i')?;
    let mut out: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    out.extend(elems.iter().map(|d| d.as_f32()));
    Ok(out)
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
        aggmtransfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMTRANSFN).as_oid(),
        aggminvtransfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMINVTRANSFN).as_oid(),
        aggmfinalfn: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMFINALFN).as_oid(),
        aggfinalextra: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGFINALEXTRA).as_bool(),
        aggmfinalextra: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMFINALEXTRA).as_bool(),
        aggfinalmodify: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGFINALMODIFY).as_i8(),
        aggmfinalmodify: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMFINALMODIFY).as_i8(),
        aggsortop: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGSORTOP).as_oid(),
        aggtranstype: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGTRANSTYPE).as_oid(),
        aggtransspace: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGTRANSSPACE).as_i32(),
        aggmtranstype: getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGMTRANSTYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn pg_aggregate_aggminitval<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
) -> PgResult<Option<Option<PgString<'mcx>>>> {
    pg_aggregate_initval_attr(mcx, aggfnoid, ANUM_PG_AGGREGATE_AGGMINITVAL)
}

fn pg_aggregate_agginitval<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
) -> PgResult<Option<Option<PgString<'mcx>>>> {
    pg_aggregate_initval_attr(mcx, aggfnoid, ANUM_PG_AGGREGATE_AGGINITVAL)
}

fn pg_aggregate_initval_attr<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
    attnum: i32,
) -> PgResult<Option<Option<PgString<'mcx>>>> {
    let Some(tuple) = SearchSysCache1(AGGFNOID, SysCacheKey::Value(Datum::from_oid(aggfnoid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = match varlena_image(mcx, &t, AGGFNOID, attnum)? {
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

fn pg_proc_proargdefaults<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<Option<PgString<'mcx>>>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let out = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROARGDEFAULTS)? {
        Some(img) => {
            let s = core::str::from_utf8(&img[4..]).expect("proargdefaults is pg_node_tree text");
            Some(PgString::from_str_in(s, mcx)?)
        }
        None => None,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(out))
}

fn lookup_pg_range_shape(
    range_oid: Oid,
) -> PgResult<Option<syscache_seams::PgRangeShape>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::RANGETYPE,
        SysCacheKey::Value(Datum::from_oid(range_oid)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let shape = syscache_seams::PgRangeShape {
        rngsubtype: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGSUBTYPE).as_oid(),
        rngmultitypid: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGMULTITYPID)
            .as_oid(),
        rngcollation: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGCOLLATION)
            .as_oid(),
        rngsubopc: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGSUBOPC).as_oid(),
        rngcanonical: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGCANONICAL)
            .as_oid(),
        rngsubdiff: getattr(&t, crate::cacheinfo::RANGETYPE, ANUM_PG_RANGE_RNGSUBDIFF).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_proc_signature<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<(Oid, PgVec<'mcx, Oid>)>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let rettype = getattr(&t, PROCOID, ANUM_PG_PROC_PRORETTYPE).as_oid();
    let argv = getattr(&t, PROCOID, ANUM_PG_PROC_PROARGTYPES);
    // SAFETY: proargtypes is a not-null plain-storage oidvector; values tail
    // follows the 24-byte header in place, dim1 == pronargs.
    let args = unsafe {
        let p = argv.as_usize() as *const array::oidvector;
        core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
    };
    let mut proargtypes = mcx::vec_with_capacity_in(mcx, args.len())?;
    proargtypes.extend_from_slice(args);
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some((rettype, proargtypes)))
}

fn lookup_pg_range_by_multirange(multirange_oid: Oid) -> PgResult<Option<Oid>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::RANGEMULTIRANGE,
        SysCacheKey::Value(Datum::from_oid(multirange_oid)),
    )?
    else {
        return Ok(None);
    };
    let oid = getattr(&tuple.tuple(), crate::cacheinfo::RANGEMULTIRANGE, ANUM_PG_RANGE_RNGTYPID)
        .as_oid();
    ReleaseSysCache(tuple);
    Ok(Some(oid))
}

// Minimal flat-array reads for the pg_proc result arrays (1-D, no nulls).
fn flat_array_meta(img: &[u8]) -> (i32, bool, usize, usize) {
    let ndim = i32::from_ne_bytes(img[4..8].try_into().unwrap());
    let dataoffset = i32::from_ne_bytes(img[8..12].try_into().unwrap());
    let nelems = if ndim == 1 {
        i32::from_ne_bytes(img[16..20].try_into().unwrap()) as usize
    } else {
        0
    };
    // No-null arrays have dataoffset 0: data starts after ndim/lbound words.
    let data_off = if dataoffset == 0 { 16 + 8 * ndim as usize } else { dataoffset as usize };
    (ndim, dataoffset != 0, nelems, data_off)
}

fn pg_proc_result_arrays<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<syscache_seams::PgProcResultArraysShape<'mcx>>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();

    let proallargtypes = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROALLARGTYPES)? {
        None => None,
        Some(img) => {
            let (ndim, hasnull, nelems, off) = flat_array_meta(&img);
            assert!(ndim == 1 && !hasnull, "proallargtypes is not a 1-D Oid array or it contains nulls");
            let mut v: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nelems)?;
            for i in 0..nelems {
                v.push(Oid::from_ne_bytes(img[off + 4 * i..off + 4 * i + 4].try_into().unwrap()));
            }
            Some(v)
        }
    };
    let proargmodes = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROARGMODES)? {
        None => None,
        Some(img) => {
            let (ndim, hasnull, nelems, off) = flat_array_meta(&img);
            assert!(ndim == 1 && !hasnull, "proargmodes is not a 1-D char array or it contains nulls");
            let mut v: PgVec<'mcx, i8> = mcx::vec_with_capacity_in(mcx, nelems)?;
            for i in 0..nelems {
                v.push(img[off + i] as i8);
            }
            Some(v)
        }
    };
    let proargnames = match varlena_image(mcx, &t, PROCOID, ANUM_PG_PROC_PROARGNAMES)? {
        None => None,
        Some(img) => {
            let (ndim, hasnull, nelems, mut off) = flat_array_meta(&img);
            assert!(ndim == 1 && !hasnull, "proargnames is not a 1-D text array or it contains nulls");
            let mut v: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
            v.try_reserve_exact(nelems).map_err(|_| mcx.oom(nelems))?;
            for _ in 0..nelems {
                // text elements: short (1B) or 4B headers, 'i' alignment for
                // the 4B form only when the preceding byte run requires it —
                // PG packs short varlenas unaligned.
                let (payload, adv): (&[u8], usize) = if img[off] & 0x01 == 0x01 {
                    let total = ((img[off] >> 1) & 0x7F) as usize;
                    (&img[off + 1..off + total], total)
                } else {
                    let aligned = (off + 3) & !3;
                    let total =
                        (u32::from_ne_bytes(img[aligned..aligned + 4].try_into().unwrap()) >> 2)
                            as usize;
                    off = aligned;
                    (&img[off + 4..off + total], total)
                };
                let s = core::str::from_utf8(payload).expect("proargnames is server-encoding text");
                v.push(PgString::from_str_in(s, mcx)?);
                off += adv;
            }
            Some(v)
        }
    };

    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(syscache_seams::PgProcResultArraysShape {
        proallargtypes,
        proargmodes,
        proargnames,
    }))
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

// SysCacheGetAttr(ATTNUM, attoptions) + datumCopy: the tuple is released,
// so the varlena image is copied into the caller's mcx.
fn pg_attribute_attoptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<Option<Datum>>> {
    const ANUM_PG_ATTRIBUTE_ATTOPTIONS: i32 = 23;
    let Some(tuple) = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        return Ok(None);
    };
    let out = match getattr_nullable(&tuple.tuple(), ATTNUM, ANUM_PG_ATTRIBUTE_ATTOPTIONS) {
        None => None,
        Some(d) => {
            let src = d.as_usize() as *const u8;
            // SAFETY: non-null by-ref varlena datum inside the live tuple.
            let bytes = unsafe {
                core::slice::from_raw_parts(src, types_tuple::varatt::varsize_any(src))
            };
            Some(Datum::from_usize(mcx::slice_in(mcx, bytes)?.leak().as_ptr() as usize))
        }
    };
    ReleaseSysCache(tuple);
    Ok(Some(out))
}

fn pg_type_typnamespace(typid: Oid) -> PgResult<Option<Oid>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let nsp = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNAMESPACE).as_oid();
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(nsp))
}

const ANUM_PG_ENUM_OID: i32 = 1;
const ANUM_PG_ENUM_ENUMTYPID: i32 = 2;
const ANUM_PG_ENUM_ENUMLABEL: i32 = 4;

fn pg_enum_shape(tuple: &crate::CatCTuple, cache_id: i32) -> syscache_seams::PgEnumShape {
    let t = tuple.tuple();
    let hdr = t.t_data();
    syscache_seams::PgEnumShape {
        oid: getattr(&t, cache_id, ANUM_PG_ENUM_OID).as_oid(),
        enumtypid: getattr(&t, cache_id, ANUM_PG_ENUM_ENUMTYPID).as_oid(),
        enumlabel: getattr_name(&t, cache_id, ANUM_PG_ENUM_ENUMLABEL),
        xmin: hdr.xmin(),
        xmin_committed: hdr.xmin_committed(),
    }
}

fn lookup_pg_enum_by_oid(enum_oid: Oid) -> PgResult<Option<syscache_seams::PgEnumShape>> {
    let Some(tuple) = SearchSysCache1(ENUMOID, SysCacheKey::Value(Datum::from_oid(enum_oid)))?
    else {
        return Ok(None);
    };
    let shape = pg_enum_shape(&tuple, ENUMOID);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_enum_by_typid_label(
    typid: Oid,
    label: &str,
) -> PgResult<Option<syscache_seams::PgEnumShape>> {
    let Some(tuple) = SearchSysCache2(
        ENUMTYPOIDNAME,
        SysCacheKey::Value(Datum::from_oid(typid)),
        SysCacheKey::Str(label),
    )?
    else {
        return Ok(None);
    };
    let shape = pg_enum_shape(&tuple, ENUMTYPOIDNAME);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

pub(crate) fn install() {
    syscache_seams::lookup_pg_ts_parser_shape::set(lookup_pg_ts_parser_shape);
    syscache_seams::lookup_pg_ts_dict_shape::set(lookup_pg_ts_dict_shape);
    syscache_seams::lookup_pg_ts_template_shape::set(lookup_pg_ts_template_shape);
    syscache_seams::lookup_pg_ts_config_shape::set(lookup_pg_ts_config_shape);
    syscache_seams::lookup_pg_ts_config_oid_by_name::set(lookup_pg_ts_config_oid_by_name);
    syscache_seams::lookup_pg_ts_dict_oid_by_name::set(lookup_pg_ts_dict_oid_by_name);
    syscache_seams::pg_ts_config_map_shapes::set(pg_ts_config_map_shapes);
    syscache_seams::lookup_pg_enum_by_oid::set(lookup_pg_enum_by_oid);
    syscache_seams::lookup_pg_enum_by_typid_label::set(lookup_pg_enum_by_typid_label);
    syscache_seams::pg_attribute_attoptions::set(pg_attribute_attoptions);
    syscache_seams::pg_type_typnamespace::set(pg_type_typnamespace);
    syscache_seams::search_syscache_exists_reloid::set(search_syscache_exists_reloid);
    syscache_seams::search_syscache_exists_procoid::set(search_syscache_exists_procoid);
    syscache_seams::search_syscache_exists_attnum::set(search_syscache_exists_attnum);
    syscache_seams::search_syscache_exists_databaseoid::set(search_syscache_exists_databaseoid);
    syscache_seams::search_syscache_exists_tablespaceoid::set(search_syscache_exists_tablespaceoid);
    syscache_seams::sys_cache_invalidate::set(sys_cache_invalidate);
    syscache_seams::relation_invalidates_snapshots_only::set(crate::RelationInvalidatesSnapshotsOnly);
    syscache_seams::lookup_pg_class_by_relid::set(lookup_pg_class_by_relid);
    syscache_seams::pg_class_shape::set(pg_class_shape);
    syscache_seams::pg_class_relname::set(pg_class_relname);
    syscache_seams::pg_attribute_attrelid::set(pg_attribute_attrelid);
    syscache_seams::pg_index_indexrelid::set(pg_index_indexrelid);
    syscache_seams::statext_form::set(statext_form);
    syscache_seams::statext_data_kinds::set(statext_data_kinds);
    syscache_seams::statext_data_blob::set(statext_data_blob);
    syscache_seams::statext_exprs_src::set(statext_exprs_src);
    syscache_seams::statext_expressions_load::set(statext_expressions_load);
    syscache_seams::pg_constraint_fk_target::set(pg_constraint_fk_target);
    syscache_seams::lookup_pg_type_shape::set(lookup_pg_type_shape);
    syscache_seams::lookup_pg_sequence_form::set(lookup_pg_sequence_form);
    syscache_seams::lookup_pg_attribute_shape::set(lookup_pg_attribute_shape);
    syscache_seams::lookup_pg_attribute_attnum_by_name::set(lookup_pg_attribute_attnum_by_name);
    syscache_seams::pg_type_isdefined::set(pg_type_isdefined);
    syscache_seams::pg_type_typtype::set(pg_type_typtype);
    syscache_seams::pg_type_category::set(pg_type_category);
    syscache_seams::pg_type_element_shape::set(pg_type_element_shape);
    syscache_seams::lookup_pg_opclass_shape::set(lookup_pg_opclass_shape);
    syscache_seams::lookup_authid_rolname::set(lookup_authid_rolname);
    syscache_seams::lookup_authid_rolname_data::set(lookup_authid_rolname_data);
    syscache_seams::lookup_authid_by_rolname::set(lookup_authid_by_rolname);
    syscache_seams::lookup_authid_session_by_rolname::set(lookup_authid_session_by_rolname);
    syscache_seams::lookup_authid_session_by_oid::set(lookup_authid_session_by_oid);
    syscache_seams::lookup_authid_rolpassword::set(lookup_authid_rolpassword);
    syscache_seams::lookup_pg_type_typcache_shape::set(lookup_pg_type_typcache_shape);
    syscache_seams::lookup_pg_range_by_multirange::set(lookup_pg_range_by_multirange);
    syscache_seams::pg_proc_result_arrays::set(pg_proc_result_arrays);
    syscache_seams::pg_proc_proargdefaults::set(pg_proc_proargdefaults);
    syscache_seams::lookup_pg_range_shape::set(lookup_pg_range_shape);
    syscache_seams::lookup_pg_proc_signature::set(lookup_pg_proc_signature);
    syscache_seams::syscache_hash_value_typeoid::set(syscache_hash_value_typeoid);
    syscache_seams::syscache_hash_value_procoid::set(syscache_hash_value_procoid);
    syscache_seams::lookup_pg_class_relid_by_name::set(lookup_pg_class_relid_by_name);
    syscache_seams::lookup_pg_type_oid_by_name::set(lookup_pg_type_oid_by_name);
    syscache_seams::pg_namespace_nspname::set(pg_namespace_nspname);
    syscache_seams::pg_type_name_namespace::set(pg_type_name_namespace);
    syscache_seams::lookup_pg_constraint_shape::set(lookup_pg_constraint_shape);
    syscache_seams::lookup_pg_constraint_desc_shape::set(lookup_pg_constraint_desc_shape);
    syscache_seams::lookup_pg_namespace_oid_by_name::set(lookup_pg_namespace_oid_by_name);
    syscache_seams::lookup_pg_cast_oid::set(lookup_pg_cast_oid);
    syscache_seams::lookup_pg_operator_shape::set(lookup_pg_operator_shape);
    syscache_seams::pg_operator_oprname::set(pg_operator_oprname);
    syscache_seams::lookup_pg_opclass_oid_by_name::set(lookup_pg_opclass_oid_by_name);
    syscache_seams::lookup_pg_operator_oid_exact::set(lookup_pg_operator_oid_exact);
    syscache_seams::pg_operator_oprnamensp::set(pg_operator_oprnamensp);
    syscache_seams::lookup_pg_ts_config_oid_by_name_nsp::set(lookup_pg_ts_config_oid_by_name_nsp);
    syscache_seams::lookup_pg_ts_config_row::set(lookup_pg_ts_config_row);
    syscache_seams::lookup_pg_ts_dict_oid_by_name_nsp::set(lookup_pg_ts_dict_oid_by_name_nsp);
    syscache_seams::lookup_pg_conversion_oid_by_name_nsp::set(lookup_pg_conversion_oid_by_name_nsp);
    syscache_seams::lookup_pg_statistic_ext_oid_by_name_nsp::set(lookup_pg_statistic_ext_oid_by_name_nsp);
    syscache_seams::lookup_pg_ts_dict_row::set(lookup_pg_ts_dict_row);
    syscache_seams::lookup_pg_amproc::set(lookup_pg_amproc);
    syscache_seams::lookup_pg_amproc_members::set(lookup_pg_amproc_members);
    syscache_seams::lookup_pg_class_ls_shape::set(lookup_pg_class_ls_shape);
    syscache_seams::pg_class_reloftype::set(pg_class_reloftype);
    syscache_seams::lookup_pg_index_ls_shape::set(lookup_pg_index_ls_shape);
    syscache_seams::pg_index_indclass_element::set(pg_index_indclass_element);
    syscache_seams::pg_index_indoption_element::set(pg_index_indoption_element);
    syscache_seams::pg_am_amtype::set(pg_am_amtype_lookup);
    syscache_seams::pg_am_amhandler::set(pg_am_amhandler_lookup);
    syscache_seams::pg_am_amname::set(pg_am_amname_lookup);
    syscache_seams::lookup_pg_amop_by_operator::set(lookup_pg_amop_by_operator);
    syscache_seams::lookup_pg_amop_by_strategy::set(lookup_pg_amop_by_strategy);
    syscache_seams::lookup_pg_amop_members_by_operator::set(lookup_pg_amop_members_by_operator);
    syscache_seams::lookup_pg_opfamily_shape::set(lookup_pg_opfamily_shape);
    syscache_seams::pg_opclass_opcname::set(pg_opclass_opcname);
    syscache_seams::pg_opclass_name_namespace_method::set(pg_opclass_name_namespace_method);
    syscache_seams::lookup_pg_amop_rows::set(lookup_pg_amop_rows);
    syscache_seams::lookup_pg_amproc_rows::set(lookup_pg_amproc_rows);
    syscache_seams::lookup_pg_opclass_rows_by_am::set(lookup_pg_opclass_rows_by_am);
    syscache_seams::lookup_pg_opfamily_oid_exact::set(lookup_pg_opfamily_oid_exact);
    syscache_seams::pg_type_base_shape::set(pg_type_base_shape);
    syscache_seams::pg_type_domain_shape::set(pg_type_domain_shape);
    syscache_seams::pg_type_io_shape::set(pg_type_io_shape);
    syscache_seams::pg_type_typarray::set(pg_type_typarray);
    syscache_seams::pg_type_default_strings::set(pg_type_default_strings);
    syscache_seams::pg_type_typrelid::set(pg_type_typrelid);
    syscache_seams::pg_proc_proname::set(pg_proc_proname);
    syscache_seams::lookup_pg_proc_shape::set(lookup_pg_proc_shape);
    syscache_seams::lookup_pg_proc_fmgr::set(lookup_pg_proc_fmgr);
    syscache_seams::lookup_pg_proc_secdef::set(lookup_pg_proc_secdef);
    syscache_seams::lookup_pg_language_fmgr::set(lookup_pg_language_fmgr);
    syscache_seams::lookup_pg_language_name::set(lookup_pg_language_name);
    syscache_seams::lookup_pg_proc_prosrc::set(lookup_pg_proc_prosrc);
    syscache_seams::lookup_pg_proc_probin::set(lookup_pg_proc_probin);
    syscache_seams::lookup_pg_proc_name_candidates::set(lookup_pg_proc_name_candidates);
    syscache_seams::lookup_pg_operator_candidates::set(lookup_pg_operator_candidates);
    syscache_seams::pg_operator_name_candidates_exist::set(pg_operator_name_candidates_exist);
    syscache_seams::lookup_pg_operator_name_candidates::set(lookup_pg_operator_name_candidates);
    syscache_seams::lookup_pg_cast_shape::set(lookup_pg_cast_shape);
    syscache_seams::pg_proc_cost_shape::set(pg_proc_cost_shape);
    syscache_seams::lookup_pg_attribute_stattarget::set(lookup_pg_attribute_stattarget);
    syscache_seams::lookup_pg_collation_shape::set(lookup_pg_collation_shape);
    syscache_seams::pg_type_typanalyze::set(pg_type_typanalyze);
    syscache_seams::lookup_pg_collation_locale_row::set(lookup_pg_collation_locale_row);
    syscache_seams::lookup_pg_collation_by_name_enc_nsp::set(lookup_pg_collation_by_name_enc_nsp);
    syscache_seams::lookup_pg_aggregate_shape::set(lookup_pg_aggregate_shape);
    syscache_seams::pg_aggregate_agginitval::set(pg_aggregate_agginitval);
    syscache_seams::pg_aggregate_aggminitval::set(pg_aggregate_aggminitval);
    install_pg_statistic();
}

// Fixture rigs that mock the other catalogs still install the real
// pg_statistic decode (set-once seams forbid override-after-install).
pub(crate) fn install_pg_statistic() {
    syscache_seams::lookup_pg_statistic_shape::set(lookup_pg_statistic_shape);
    syscache_seams::lookup_pg_statistic_bundle::set(lookup_pg_statistic_bundle);
    syscache_seams::lookup_pg_statistic_slot_images::set(lookup_pg_statistic_slot_images);
    syscache_seams::decode_pg_statistic_values::set(decode_pg_statistic_values);
    syscache_seams::decode_pg_statistic_numbers::set(decode_pg_statistic_numbers);
    syscache_seams::pg_statistic_stawidth::set(pg_statistic_stawidth);
}

const ANUM_PG_TS_PARSER_PRSSTART: i32 = 4;
const ANUM_PG_TS_PARSER_PRSTOKEN: i32 = 5;
const ANUM_PG_TS_PARSER_PRSEND: i32 = 6;
const ANUM_PG_TS_PARSER_PRSHEADLINE: i32 = 7;
const ANUM_PG_TS_PARSER_PRSLEXTYPE: i32 = 8;
const ANUM_PG_TS_DICT_DICTNAME: i32 = 2;
const ANUM_PG_TS_DICT_DICTNAMESPACE: i32 = 3;
const ANUM_PG_TS_DICT_DICTTEMPLATE: i32 = 5;
const ANUM_PG_TS_DICT_DICTINITOPTION: i32 = 6;
const ANUM_PG_TS_TEMPLATE_TMPLINIT: i32 = 4;
const ANUM_PG_TS_TEMPLATE_TMPLLEXIZE: i32 = 5;
const ANUM_PG_TS_CONFIG_OID: i32 = 1;
const ANUM_PG_TS_CONFIG_CFGNAME: i32 = 2;
const ANUM_PG_TS_CONFIG_CFGNAMESPACE: i32 = 3;
const ANUM_PG_TS_CONFIG_CFGPARSER: i32 = 5;
const ANUM_PG_TS_DICT_OID: i32 = 1;
const ANUM_PG_CONVERSION_OID: i32 = 1;
const ANUM_PG_TS_CONFIG_MAP_MAPTOKENTYPE: i32 = 2;
const ANUM_PG_TS_CONFIG_MAP_MAPSEQNO: i32 = 3;
const ANUM_PG_TS_CONFIG_MAP_MAPDICT: i32 = 4;

fn lookup_pg_ts_parser_shape(
    prsid: Oid,
) -> PgResult<Option<syscache_seams::PgTsParserShape>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::TSPARSEROID,
        SysCacheKey::Value(Datum::from_oid(prsid)),
    )?
    else {
        return Ok(None);
    };
    let t = tuple.tuple();
    let cid = crate::cacheinfo::TSPARSEROID;
    let shape = syscache_seams::PgTsParserShape {
        prsstart: getattr(&t, cid, ANUM_PG_TS_PARSER_PRSSTART).as_oid(),
        prstoken: getattr(&t, cid, ANUM_PG_TS_PARSER_PRSTOKEN).as_oid(),
        prsend: getattr(&t, cid, ANUM_PG_TS_PARSER_PRSEND).as_oid(),
        prsheadline: getattr(&t, cid, ANUM_PG_TS_PARSER_PRSHEADLINE).as_oid(),
        prslextype: getattr(&t, cid, ANUM_PG_TS_PARSER_PRSLEXTYPE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_ts_dict_shape<'mcx>(
    mcx: Mcx<'mcx>,
    dictid: Oid,
) -> PgResult<Option<syscache_seams::PgTsDictShape<'mcx>>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::TSDICTOID,
        SysCacheKey::Value(Datum::from_oid(dictid)),
    )?
    else {
        return Ok(None);
    };
    let cid = crate::cacheinfo::TSDICTOID;
    let t = tuple.tuple();
    let dn = getattr(&t, cid, ANUM_PG_TS_DICT_DICTNAME);
    // SAFETY: dictname is a NameData column; the datum points at its 64-byte
    // in-tuple image.
    let dictname = unsafe { *(dn.as_usize() as *const NameData) };
    let dictnamespace = getattr(&t, cid, ANUM_PG_TS_DICT_DICTNAMESPACE).as_oid();
    let dicttemplate = getattr(&t, cid, ANUM_PG_TS_DICT_DICTTEMPLATE).as_oid();
    let dictinitoption = match getattr_nullable(&t, cid, ANUM_PG_TS_DICT_DICTINITOPTION) {
        None => None,
        Some(d) => {
            let p = d.as_usize() as *const u8;
            // SAFETY: non-null varlena attr datum inside the live tuple.
            let src = unsafe {
                core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
            };
            let flat = detoast::detoast_attr(mcx, src)?;
            let mut out = mcx::vec_with_capacity_in(mcx, flat.len() - 4)?;
            out.extend_from_slice(&flat[4..]);
            Some(out)
        }
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(syscache_seams::PgTsDictShape { dictname, dictnamespace, dicttemplate, dictinitoption }))
}

fn lookup_pg_ts_template_shape(
    tmplid: Oid,
) -> PgResult<Option<syscache_seams::PgTsTemplateShape>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::TSTEMPLATEOID,
        SysCacheKey::Value(Datum::from_oid(tmplid)),
    )?
    else {
        return Ok(None);
    };
    let cid = crate::cacheinfo::TSTEMPLATEOID;
    let t = tuple.tuple();
    let shape = syscache_seams::PgTsTemplateShape {
        tmplinit: getattr(&t, cid, ANUM_PG_TS_TEMPLATE_TMPLINIT).as_oid(),
        tmpllexize: getattr(&t, cid, ANUM_PG_TS_TEMPLATE_TMPLLEXIZE).as_oid(),
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_ts_config_shape(
    cfgid: Oid,
) -> PgResult<Option<syscache_seams::PgTsConfigShape>> {
    let Some(tuple) = SearchSysCache1(
        crate::cacheinfo::TSCONFIGOID,
        SysCacheKey::Value(Datum::from_oid(cfgid)),
    )?
    else {
        return Ok(None);
    };
    let cid = crate::cacheinfo::TSCONFIGOID;
    let t = tuple.tuple();
    let d = getattr(&t, cid, ANUM_PG_TS_CONFIG_CFGNAME);
    // SAFETY: cfgname is a NameData column; the datum points at its 64-byte
    // in-tuple image.
    let cfgname = unsafe { *(d.as_usize() as *const NameData) };
    let shape = syscache_seams::PgTsConfigShape {
        cfgparser: getattr(&t, cid, ANUM_PG_TS_CONFIG_CFGPARSER).as_oid(),
        cfgnamespace: getattr(&t, cid, ANUM_PG_TS_CONFIG_CFGNAMESPACE).as_oid(),
        cfgname,
    };
    drop(t);
    ReleaseSysCache(tuple);
    Ok(Some(shape))
}

fn lookup_pg_ts_config_oid_by_name(cfgname: &str, cfgnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        crate::cacheinfo::TSCONFIGNAMENSP,
        ANUM_PG_TS_CONFIG_OID,
        SysCacheKey::Str(cfgname),
        SysCacheKey::Value(Datum::from_oid(cfgnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn lookup_pg_ts_dict_oid_by_name(dictname: &str, dictnamespace: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        crate::cacheinfo::TSDICTNAMENSP,
        ANUM_PG_TS_DICT_OID,
        SysCacheKey::Str(dictname),
        SysCacheKey::Value(Datum::from_oid(dictnamespace)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn pg_ts_config_map_shapes<'mcx>(
    mcx: Mcx<'mcx>,
    cfgid: Oid,
) -> PgResult<PgVec<'mcx, syscache_seams::PgTsConfigMapShape>> {
    let cid = crate::cacheinfo::TSCONFIGMAP;
    let list = SearchSysCacheList1(cid, SysCacheKey::Value(Datum::from_oid(cfgid)))?;
    let n = list.n_members() as usize;
    let mut out = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let m = list.member(i);
        let t = m.tuple();
        out.push(syscache_seams::PgTsConfigMapShape {
            maptokentype: getattr(&t, cid, ANUM_PG_TS_CONFIG_MAP_MAPTOKENTYPE).as_i32(),
            mapseqno: getattr(&t, cid, ANUM_PG_TS_CONFIG_MAP_MAPSEQNO).as_i32(),
            mapdict: getattr(&t, cid, ANUM_PG_TS_CONFIG_MAP_MAPDICT).as_oid(),
        });
    }
    ReleaseSysCacheList(list);
    out.sort_unstable_by_key(|r| (r.maptokentype, r.mapseqno));
    Ok(out)
}
