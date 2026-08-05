// pg_cast.c CastCreate: pg_cast row insert + dependency records.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{Oid, OidIsValid, TYPE_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT};
use types_rel::RowExclusiveLock;

pub const CastRelationId: Oid = 2605;
pub const CastOidIndexId: Oid = 2660;
pub const CastSourceTargetIndexId: Oid = 2661;

const Natts_pg_cast: usize = 6;
const Anum_pg_cast_oid: i16 = 1;

const PROCEDURE_RELATION_ID: Oid = 1255;

#[cold]
#[inline(never)]
fn cast_exists(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Box<PgError>> {
    Ok(Box::new(
        PgError::error(format!(
            "cast from type {} to type {} already exists",
            format_type::format_type_be(sourcetypeid)?,
            format_type::format_type_be(targettypeid)?
        ))
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn CastCreate<'mcx>(
    mcx: Mcx<'mcx>,
    sourcetypeid: Oid,
    targettypeid: Oid,
    funcid: Oid,
    incastid: Oid,
    outcastid: Oid,
    castcontext: i8,
    castmethod: i8,
    behavior: DependencyType,
) -> PgResult<ObjectAddress> {
    let relation = table::table_open(mcx, CastRelationId, RowExclusiveLock)?;

    // Duplicate check is for the friendly message only; the unique index
    // catches it anyway.
    if let Some(tuple) = cache_syscache::SearchSysCache2(
        cache_syscache::cacheinfo::CASTSOURCETARGET,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(sourcetypeid)),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(targettypeid)),
    )? {
        cache_syscache::ReleaseSysCache(tuple);
        return Err(cast_exists(sourcetypeid, targettypeid)?);
    }

    let castid = catalog::GetNewOidWithIndex(mcx, &relation, CastOidIndexId, Anum_pg_cast_oid)?;
    let values: [Datum; Natts_pg_cast] = [
        Datum::from_oid(castid),
        Datum::from_oid(sourcetypeid),
        Datum::from_oid(targettypeid),
        Datum::from_oid(funcid),
        Datum::from_char(castcontext),
        Datum::from_char(castmethod),
    ];
    let nulls = [false; Natts_pg_cast];
    let mut tup = heaptuple::heap_form_tuple(mcx, relation.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &relation, &mut tup)?;

    let myself = ObjectAddress::set(CastRelationId, castid);

    let mut referenced: [ObjectAddress; 5] = [myself; 5];
    let mut n = 0;
    referenced[n] = ObjectAddress::set(TYPE_RELATION_ID, sourcetypeid);
    n += 1;
    referenced[n] = ObjectAddress::set(TYPE_RELATION_ID, targettypeid);
    n += 1;
    if OidIsValid(funcid) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, funcid);
        n += 1;
    }
    if OidIsValid(incastid) {
        referenced[n] = ObjectAddress::set(CastRelationId, incastid);
        n += 1;
    }
    if OidIsValid(outcastid) {
        referenced[n] = ObjectAddress::set(CastRelationId, outcastid);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(mcx, &myself, &mut referenced[..n], behavior)?;

    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    relation.close(RowExclusiveLock)?;

    Ok(myself)
}
