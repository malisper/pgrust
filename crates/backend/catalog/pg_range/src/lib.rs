// pg_range.c.
#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use pg_depend::{
    record_object_address_dependencies, recordDependencyOn, DependencyType, ObjectAddress,
};
use types_core::{
    AttrNumber, Oid, OidIsValid, COLLATION_RELATION_ID, OPERATOR_CLASS_RELATION_ID,
    PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::PgResult;
use types_rel::RowExclusiveLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const RangeRelationId: Oid = 3541;
pub const RangeTypidIndexId: Oid = 3542;
pub const RangeMultirangeTypidIndexId: Oid = 2228;

pub const Anum_pg_range_rngtypid: AttrNumber = 1;
pub const Anum_pg_range_rngsubtype: AttrNumber = 2;
pub const Anum_pg_range_rngmultitypid: AttrNumber = 3;
pub const Anum_pg_range_rngcollation: AttrNumber = 4;
pub const Anum_pg_range_rngsubopc: AttrNumber = 5;
pub const Anum_pg_range_rngcanonical: AttrNumber = 6;
pub const Anum_pg_range_rngsubdiff: AttrNumber = 7;
const Natts_pg_range: usize = 7;

#[allow(clippy::too_many_arguments)]
pub fn RangeCreate<'mcx>(
    mcx: Mcx<'mcx>,
    rangeTypeOid: Oid,
    rangeSubType: Oid,
    rangeCollation: Oid,
    rangeSubOpclass: Oid,
    rangeCanonical: Oid,
    rangeSubDiff: Oid,
    multirangeTypeOid: Oid,
) -> PgResult<()> {
    let pg_range = table::table_open(mcx, RangeRelationId, RowExclusiveLock)?;

    let mut values = [Datum::null(); Natts_pg_range];
    let nulls = [false; Natts_pg_range];
    values[Anum_pg_range_rngtypid as usize - 1] = Datum::from_oid(rangeTypeOid);
    values[Anum_pg_range_rngsubtype as usize - 1] = Datum::from_oid(rangeSubType);
    values[Anum_pg_range_rngcollation as usize - 1] = Datum::from_oid(rangeCollation);
    values[Anum_pg_range_rngsubopc as usize - 1] = Datum::from_oid(rangeSubOpclass);
    values[Anum_pg_range_rngcanonical as usize - 1] = Datum::from_oid(rangeCanonical);
    values[Anum_pg_range_rngsubdiff as usize - 1] = Datum::from_oid(rangeSubDiff);
    values[Anum_pg_range_rngmultitypid as usize - 1] = Datum::from_oid(multirangeTypeOid);

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_range.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_range, &mut tup)?;

    let myself = ObjectAddress::set(TYPE_RELATION_ID, rangeTypeOid);
    let mut referenced = [ObjectAddress::set(types_core::InvalidOid, types_core::InvalidOid); 5];
    let mut n = 0;
    referenced[n] = ObjectAddress::set(TYPE_RELATION_ID, rangeSubType);
    n += 1;
    referenced[n] = ObjectAddress::set(OPERATOR_CLASS_RELATION_ID, rangeSubOpclass);
    n += 1;
    if OidIsValid(rangeCollation) {
        referenced[n] = ObjectAddress::set(COLLATION_RELATION_ID, rangeCollation);
        n += 1;
    }
    if OidIsValid(rangeCanonical) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, rangeCanonical);
        n += 1;
    }
    if OidIsValid(rangeSubDiff) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, rangeSubDiff);
        n += 1;
    }
    record_object_address_dependencies(mcx, &myself, &mut referenced[..n], DependencyType::Normal)?;

    let referencing = ObjectAddress::set(TYPE_RELATION_ID, multirangeTypeOid);
    recordDependencyOn(mcx, &referencing, &myself, DependencyType::Internal)?;

    pg_range.close(RowExclusiveLock)
}

pub fn RangeDelete<'mcx>(mcx: Mcx<'mcx>, rangeTypeOid: Oid) -> PgResult<()> {
    let pg_range = table::table_open(mcx, RangeRelationId, RowExclusiveLock)?;

    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_range_rngtypid;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(rangeTypeOid);

    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_range,
        RangeTypidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&pg_range, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    pg_range.close(RowExclusiveLock)
}
