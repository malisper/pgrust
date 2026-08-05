#![allow(non_snake_case, non_upper_case_globals)]

use cache_syscache::cacheinfo::{CONDEFAULT, CONNAMENSP};
use cache_syscache::{ReleaseSysCacheList, SearchSysCacheList, SysCacheKey};
use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{InvalidOid, Oid, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT};
use types_rel::RowExclusiveLock;
use types_tuple::{HeapTupleData, NameData};

pub const ConversionRelationId: Oid = 2607;
pub const ConversionOidIndexId: Oid = 2670;

const Natts_pg_conversion: usize = 8;
const Anum_pg_conversion_oid: types_core::AttrNumber = 1;
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

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DUPLICATE_OBJECT))
}

/// C `ConversionCreate`: pg_conversion row insert + dependency records.
pub fn ConversionCreate<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    connamespace: Oid,
    conowner: Oid,
    conforencoding: i32,
    contoencoding: i32,
    conproc: Oid,
    def: bool,
) -> PgResult<ObjectAddress> {
    if let Some(tuple) = cache_syscache::SearchSysCache2(
        CONNAMENSP,
        SysCacheKey::Str(conname),
        SysCacheKey::Value(Datum::from_oid(connamespace)),
    )? {
        cache_syscache::ReleaseSysCache(tuple);
        return Err(duplicate(format!("conversion \"{conname}\" already exists")));
    }

    if def && FindDefaultConversion(connamespace, conforencoding, contoencoding)? != InvalidOid {
        return Err(duplicate(format!(
            "default conversion for {} to {} already exists",
            mbutils::pg_encoding_to_char(conforencoding),
            mbutils::pg_encoding_to_char(contoencoding)
        )));
    }

    let rel = table::table_open(mcx, ConversionRelationId, RowExclusiveLock)?;

    let mut cname = NameData::default();
    cname.namestrcpy(conname);
    let oid = catalog::GetNewOidWithIndex(mcx, &rel, ConversionOidIndexId, Anum_pg_conversion_oid)?;

    let values: [Datum; Natts_pg_conversion] = [
        Datum::from_oid(oid),
        Datum::from_usize(cname.data.as_ptr() as usize),
        Datum::from_oid(connamespace),
        Datum::from_oid(conowner),
        Datum::from_i32(conforencoding),
        Datum::from_i32(contoencoding),
        Datum::from_oid(conproc),
        Datum::from_bool(def),
    ];
    let nulls = [false; Natts_pg_conversion];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(ConversionRelationId, oid);
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PROCEDURE_RELATION_ID, conproc),
        DependencyType::Normal,
    )?;
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(NAMESPACE_RELATION_ID, connamespace),
        DependencyType::Normal,
    )?;
    pg_depend::recordDependencyOnOwner(mcx, ConversionRelationId, oid, conowner)?;
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    rel.close(RowExclusiveLock)?;
    Ok(myself)
}
