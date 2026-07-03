#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_rel::{Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const SharedDependRelationId: Oid = 1214;
pub const SharedDependDependerIndexId: Oid = 1232;
pub const SharedDependReferenceIndexId: Oid = 1233;

pub const Natts_pg_shdepend: usize = 7;
pub const Anum_pg_shdepend_dbid: i32 = 1;
pub const Anum_pg_shdepend_classid: i32 = 2;
pub const Anum_pg_shdepend_objid: i32 = 3;
pub const Anum_pg_shdepend_objsubid: i32 = 4;
pub const Anum_pg_shdepend_refclassid: i32 = 5;
pub const Anum_pg_shdepend_refobjid: i32 = 6;
pub const Anum_pg_shdepend_deptype: i32 = 7;

fn oid_eq_key(attno: i32, arg: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info({F_OIDEQ}) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(arg);
    key
}

// C batches through multi-insert slots; per-row inserts write the same page
// image.
pub fn copyTemplateDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    templateDbId: Oid,
    newDbId: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, SharedDependRelationId, RowExclusiveLock)?;
    let mut indstate = None;

    let keys = [oid_eq_key(Anum_pg_shdepend_dbid, templateDbId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, SharedDependDependerIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let td = rel.descr();
        let mut isnull = false;
        let mut att = |attno: i32| -> Datum {
            // SAFETY: pg_shdepend row under this relation's descriptor.
            unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) }
        };
        let values = [
            Datum::from_oid(newDbId),
            att(Anum_pg_shdepend_classid),
            att(Anum_pg_shdepend_objid),
            att(Anum_pg_shdepend_objsubid),
            att(Anum_pg_shdepend_refclassid),
            att(Anum_pg_shdepend_refobjid),
            att(Anum_pg_shdepend_deptype),
        ];
        let nulls = [false; Natts_pg_shdepend];
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
        }
        let mut copy = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsertWithInfo(mcx, &rel, &mut copy, indstate.as_mut().unwrap())?;
    }
    genam::systable_endscan(mcx, scan)?;

    if let Some(st) = indstate {
        catalog_indexing::CatalogCloseIndexes(st)?;
    }
    rel.close(RowExclusiveLock)
}

fn delete_matching(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    keys: &[ScanKeyData],
) -> PgResult<()> {
    let mut scan = genam::systable_beginscan(mcx, rel, SharedDependDependerIndexId, true, None, keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        catalog_indexing::CatalogTupleDelete(rel, &tup.t_self)?;
    }
    genam::systable_endscan(mcx, scan)
}

// A shared object's own rows carry dbid = 0 (shdepDropDependency drop-all arm).
pub fn dropDatabaseDependencies(mcx: Mcx<'_>, databaseId: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, SharedDependRelationId, RowExclusiveLock)?;

    let keys = [oid_eq_key(Anum_pg_shdepend_dbid, databaseId)];
    delete_matching(mcx, &rel, &keys)?;

    let keys = [
        oid_eq_key(Anum_pg_shdepend_dbid, 0),
        oid_eq_key(Anum_pg_shdepend_classid, DATABASE_RELATION_ID),
        oid_eq_key(Anum_pg_shdepend_objid, databaseId),
    ];
    delete_matching(mcx, &rel, &keys)?;

    rel.close(RowExclusiveLock)
}
