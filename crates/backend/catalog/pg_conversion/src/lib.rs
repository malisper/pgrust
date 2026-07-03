#![allow(non_snake_case)]

use cache_syscache::cacheinfo::CONDEFAULT;
use cache_syscache::{ReleaseSysCacheList, SearchSysCacheList, SysCacheKey};
use datum::Datum;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_tuple::HeapTupleData;

const ANUM_PG_CONVERSION_CONPROC: i32 = 7;
const ANUM_PG_CONVERSION_CONDEFAULT: i32 = 8;

fn getattr(tuple: &HeapTupleData<'_>, attnum: i32) -> Datum {
    let td = match catcache::cache_tupdesc(CONDEFAULT) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(CONDEFAULT, false)
                .expect("catcache phase-2 init for pg_conversion");
            catcache::cache_tupdesc(CONDEFAULT).expect("phase-2 init left no tupdesc")
        }
    };
    let mut isnull = false;
    // SAFETY: caller passes a pg_conversion tuple; conproc/condefault are
    // fixed-width NOT NULL columns.
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, td, &mut isnull) };
    debug_assert!(!isnull);
    d
}

/// C `FindDefaultConversion`: default conversion proc for the triple, or InvalidOid.
pub fn FindDefaultConversion(
    name_space: Oid,
    for_encoding: i32,
    to_encoding: i32,
) -> PgResult<Oid> {
    let catlist = SearchSysCacheList(
        CONDEFAULT,
        3,
        SysCacheKey::Value(Datum::from_oid(name_space)),
        SysCacheKey::Value(Datum::from_i32(for_encoding)),
        SysCacheKey::Value(Datum::from_i32(to_encoding)),
    )?;
    let mut proc = InvalidOid;
    for i in 0..catlist.n_members() as usize {
        let member = catlist.member(i);
        let tuple = member.tuple();
        if getattr(&tuple, ANUM_PG_CONVERSION_CONDEFAULT).as_bool() {
            proc = getattr(&tuple, ANUM_PG_CONVERSION_CONPROC).as_oid();
            break;
        }
    }
    ReleaseSysCacheList(catlist);
    Ok(proc)
}
