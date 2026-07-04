// pg_largeobject.c: pg_largeobject + pg_largeobject_metadata manipulation.
#![allow(non_snake_case, non_upper_case_globals)]

use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_rel::{AccessShareLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::SnapshotData;
use types_tuple::{HeapTupleData, TupleDescData};

pub const LargeObjectRelationId: Oid = 2613;
pub const LargeObjectLOidPNIndexId: Oid = 2683;
pub const LargeObjectMetadataRelationId: Oid = 2995;
pub const LargeObjectMetadataOidIndexId: Oid = 2996;

pub const Anum_pg_largeobject_loid: AttrNumber = 1;
pub const Anum_pg_largeobject_pageno: AttrNumber = 2;
pub const Anum_pg_largeobject_data: AttrNumber = 3;
pub const Natts_pg_largeobject: usize = 3;

pub const Anum_pg_largeobject_metadata_oid: AttrNumber = 1;
pub const Anum_pg_largeobject_metadata_lomowner: AttrNumber = 2;
pub const Anum_pg_largeobject_metadata_lomacl: AttrNumber = 3;
pub const Natts_pg_largeobject_metadata: usize = 3;

pub type Snapshot = Rc<SnapshotData<'static>>;

pub fn oid_key(attno: AttrNumber, value: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(value);
    key
}

fn getattr(tup: &HeapTupleData<'_>, attnum: AttrNumber, desc: &TupleDescData<'_>) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: fixed catalog column under the relation's own descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    (d, isnull)
}

pub fn LargeObjectCreate<'mcx>(mcx: Mcx<'mcx>, loid: Oid) -> PgResult<Oid> {
    let pg_lo_meta = table::table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;

    let loid_new = if OidIsValid(loid) {
        loid
    } else {
        catalog::GetNewOidWithIndex(
            mcx,
            &pg_lo_meta,
            LargeObjectMetadataOidIndexId,
            Anum_pg_largeobject_metadata_oid,
        )?
    };
    let ownerId = miscinit::GetUserId();
    let lomacl = aclchk_seams::get_user_default_acl::call(mcx, b'L', ownerId, 0)?;
    let mut values = [
        Datum::from_oid(loid_new),
        Datum::from_oid(ownerId),
        Datum::null(),
    ];
    let mut nulls = [false, false, true];
    if let Some(img) = lomacl.as_deref() {
        values[2] = Datum::from_usize(img.as_ptr() as usize);
        nulls[2] = false;
    }
    let mut ntup = heaptuple::heap_form_tuple(mcx, pg_lo_meta.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_lo_meta, &mut ntup)?;

    pg_lo_meta.close(RowExclusiveLock)?;

    if let Some(img) = lomacl.as_deref() {
        aclchk_seams::record_dependency_on_new_acl::call(
            mcx,
            LargeObjectRelationId,
            loid_new,
            0,
            ownerId,
            img,
        )?;
    }
    Ok(loid_new)
}

pub fn LargeObjectDrop<'mcx>(mcx: Mcx<'mcx>, loid: Oid) -> PgResult<()> {
    let pg_lo_meta = table::table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;
    let pg_largeobject = table::table_open(mcx, LargeObjectRelationId, RowExclusiveLock)?;

    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, loid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        None,
        &skey,
    )?;
    let tid = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tuple) => tuple.t_self,
        None => {
            return Err(Box::new(
                PgError::new(ERROR, format!("large object {loid} does not exist"))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    };
    catalog_indexing::CatalogTupleDelete(&pg_lo_meta, &tid)?;
    genam::systable_endscan(mcx, scan)?;

    let skey = [oid_key(Anum_pg_largeobject_loid, loid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_largeobject,
        LargeObjectLOidPNIndexId,
        true,
        None,
        &skey,
    )?;
    while let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tuple.t_self;
        catalog_indexing::CatalogTupleDelete(&pg_largeobject, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    pg_largeobject.close(RowExclusiveLock)?;
    pg_lo_meta.close(RowExclusiveLock)
}

pub fn LargeObjectExists<'mcx>(mcx: Mcx<'mcx>, loid: Oid) -> PgResult<bool> {
    LargeObjectExistsWithSnapshot(mcx, loid, None)
}

pub fn LargeObjectExistsWithSnapshot<'mcx>(
    mcx: Mcx<'mcx>,
    loid: Oid,
    snapshot: Option<Snapshot>,
) -> PgResult<bool> {
    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, loid)];
    let pg_lo_meta = table::table_open(mcx, LargeObjectMetadataRelationId, AccessShareLock)?;
    let mut sd = genam::systable_beginscan(
        mcx,
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        snapshot,
        &skey,
    )?;
    let retval = genam::systable_getnext(mcx, &mut sd)?.is_some();
    genam::systable_endscan(mcx, sd)?;
    pg_lo_meta.close(AccessShareLock)?;
    Ok(retval)
}

/// Catalog read for aclchk.c's `pg_largeobject_aclmask_snapshot` /
/// `object_ownercheck` (pg_largeobject_metadata has no syscache). Returns
/// `(lomowner, detoasted lomacl image or None)`; `Ok(None)` = missing object.
pub fn largeobject_owner_acl<'mcx>(
    mcx: Mcx<'mcx>,
    lobj_oid: Oid,
    snapshot: Option<Snapshot>,
) -> PgResult<Option<(Oid, Option<mcx::PgVec<'mcx, u8>>)>> {
    let pg_lo_meta = table::table_open(mcx, LargeObjectMetadataRelationId, AccessShareLock)?;
    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, lobj_oid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        snapshot,
        &skey,
    )?;

    let result = match genam::systable_getnext(mcx, &mut scan)? {
        None => None,
        Some(tuple) => {
            let desc = pg_lo_meta.descr();
            let (owner, owner_null) =
                getattr(tuple, Anum_pg_largeobject_metadata_lomowner, desc);
            debug_assert!(!owner_null);
            let (acl, acl_null) = getattr(tuple, Anum_pg_largeobject_metadata_lomacl, desc);
            let acl = if acl_null {
                None
            } else {
                // SAFETY: non-null lomacl is a live varlena inside the held tuple.
                let image = unsafe {
                    let p = acl.as_usize() as *const u8;
                    core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
                };
                Some(detoast::detoast_attr(mcx, image)?)
            };
            Some((owner.as_oid(), acl))
        }
    };

    genam::systable_endscan(mcx, scan)?;
    pg_lo_meta.close(AccessShareLock)?;
    Ok(result)
}
