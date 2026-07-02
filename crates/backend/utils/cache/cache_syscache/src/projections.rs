//! syscache_seams installs: the projections lsyscache/tupdesc/relation/inval
//! consume (C: `SearchSysCache*` + `GETSTRUCT` member loads).

use datum::Datum;
use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_storage::PgClassShape;
use types_tuple::{HeapTupleData, PgTypeShape, TupleDescData};

use crate::{ReleaseSysCache, SearchSysCache1, SearchSysCacheExists, SysCacheKey};
use crate::cacheinfo::{ATTNUM, AUTHOID, CONSTROID, INDEXRELID, RELOID, TYPEOID};

const ANUM_PG_CLASS_OID: i32 = 1;
const ANUM_PG_CLASS_RELISSHARED: i32 = 16;
const ANUM_PG_TYPE_TYPLEN: i32 = 5;
const ANUM_PG_TYPE_TYPBYVAL: i32 = 6;
const ANUM_PG_TYPE_TYPALIGN: i32 = 23;
const ANUM_PG_TYPE_TYPSTORAGE: i32 = 24;
const ANUM_PG_TYPE_TYPCOLLATION: i32 = 29;
const ANUM_PG_ATTRIBUTE_ATTRELID: i32 = 1;
const ANUM_PG_INDEX_INDEXRELID: i32 = 1;
const ANUM_PG_CONSTRAINT_CONTYPE: i32 = 4;
const ANUM_PG_CONSTRAINT_CONRELID: i32 = 9;
const ANUM_PG_AUTHID_ROLNAME: i32 = 2;
const CONSTRAINT_FOREIGN: i8 = b'f' as i8;

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

fn sys_cache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()> {
    crate::SysCacheInvalidate(cache_id, hash_value);
    Ok(())
}

pub(crate) fn install() {
    syscache_seams::search_syscache_exists_reloid::set(search_syscache_exists_reloid);
    syscache_seams::sys_cache_invalidate::set(sys_cache_invalidate);
    syscache_seams::relation_invalidates_snapshots_only::set(crate::RelationInvalidatesSnapshotsOnly);
    syscache_seams::lookup_pg_class_by_relid::set(lookup_pg_class_by_relid);
    syscache_seams::pg_class_shape::set(pg_class_shape);
    syscache_seams::pg_attribute_attrelid::set(pg_attribute_attrelid);
    syscache_seams::pg_index_indexrelid::set(pg_index_indexrelid);
    syscache_seams::pg_constraint_fk_target::set(pg_constraint_fk_target);
    syscache_seams::lookup_pg_type_shape::set(lookup_pg_type_shape);
    syscache_seams::lookup_authid_rolname::set(lookup_authid_rolname);
}
