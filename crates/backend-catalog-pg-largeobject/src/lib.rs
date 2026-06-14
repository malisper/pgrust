#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`, the
// project-wide error contract; we accept the large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]
// The C declares locals up top and assigns later; keep that decl-then-assign
// shape so the port reads 1:1 against pg_largeobject.c.
#![allow(clippy::needless_late_init)]

//! Port of `src/backend/catalog/pg_largeobject.c` — routines that support
//! manipulation of the `pg_largeobject` and `pg_largeobject_metadata` system
//! catalogs.
//!
//! Exported (non-static) C functions — there are no file-static functions — all
//! present with their original names, branch order, lock levels, scan order,
//! and error code / message / SQLSTATE preserved:
//!
//!   * [`LargeObjectCreate`] (pg_largeobject.c:35-88) — create a large object by
//!     inserting a `pg_largeobject_metadata` row with no data pages (a size-0
//!     object): pick the OID (`OidIsValid(loid) ? loid :
//!     GetNewOidWithIndex(...)`), look up the owner (`GetUserId`) and the
//!     default ACL (`get_user_default_acl`), form + insert the metadata row
//!     (`heap_form_tuple` / `CatalogTupleInsert`), then record dependencies on
//!     the roles named in the default ACL (`recordDependencyOnNewAcl`);
//!   * [`LargeObjectDrop`] (pg_largeobject.c:94-152) — delete the
//!     `pg_largeobject_metadata` row (by OID), erroring with
//!     `ERRCODE_UNDEFINED_OBJECT` if it is absent, then delete every associated
//!     `pg_largeobject` data-page row (by loid);
//!   * [`LargeObjectExists`] (pg_largeobject.c:166-170) — existence check via an
//!     up-to-date snapshot (delegates to [`LargeObjectExistsWithSnapshot`] with
//!     a `None`/`NULL` snapshot);
//!   * [`LargeObjectExistsWithSnapshot`] (pg_largeobject.c:175-205) — the same
//!     existence check with a caller-supplied snapshot (used for read-only
//!     opens).
//!
//! ## Seam crossings
//!
//! The relation is opened directly through
//! `backend-access-table-table::table_open`, returning the owned `Relation`
//! handle (mirrors the merged `pg_namespace`/`pg_depend` ports). `GetUserId`
//! crosses `backend-utils-init-miscinit-seams`; `get_user_default_acl` /
//! `recordDependencyOnNewAcl` cross `backend-catalog-aclchk-seams`;
//! `GetNewOidWithIndex` is a direct call into the merged
//! `backend-catalog-catalog`; the `heap_form_tuple` + `CatalogTupleInsert`
//! value layer crosses the `catalog/indexing.c`-owned
//! `catalog_tuple_insert_pg_largeobject_metadata` seam (panics until indexing
//! lands, exactly as the merged `pg_namespace`/`pg_am` inserts do); the two
//! keyed `systable_beginscan`/`systable_getnext`/`systable_endscan` scans cross
//! `backend-access-index-genam-seams`, and `CatalogTupleDelete` the existing
//! `backend-catalog-indexing-seams::catalog_tuple_delete`.
//!
//! The default ACL (`Acl *` = `ArrayType`) crosses opaquely from its producer
//! (`get_user_default_acl`) to its consumers (the row-form value layer and
//! `recordDependencyOnNewAcl`); `None` is the C `lomacl == NULL`. The
//! caller's snapshot crosses as `Option<Rc<SnapshotData>>`; `None` is the C
//! `NULL` (the up-to-date current snapshot).
//!
//! This crate installs its one inward seam,
//! [`backend_catalog_pg_largeobject_seams::large_object_exists_with_snapshot`],
//! from [`init_seams`].

use std::rc::Rc;

use mcx::MemoryContext;
use types_catalog::catalog::{
    ANUM_PG_LARGEOBJECT_LOID, ANUM_PG_LARGEOBJECT_METADATA_OID, LARGE_OBJECT_LOID_PN_INDEX_ID,
    LARGE_OBJECT_METADATA_OID_INDEX_ID, LARGE_OBJECT_METADATA_RELATION_ID, LARGE_OBJECT_RELATION_ID,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{AttrNumber, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::SnapshotData;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::table_open;
use backend_catalog_aclchk_seams::{get_user_default_acl, record_dependency_on_new_acl};
use backend_catalog_catalog::GetNewOidWithIndex;
use backend_catalog_indexing_seams::{
    catalog_tuple_delete, catalog_tuple_insert_pg_largeobject_metadata,
};
use backend_utils_error::ereport;
use backend_utils_init_miscinit_seams::get_user_id;
use types_nodes::parsenodes::ObjectType;
use types_storage::lock::{AccessShareLock, RowExclusiveLock};

/* ===========================================================================
 * Catalog OID / Anum aliases, spelled as the C `...RelationId` /
 * `...IndexId` macros / `Anum_...` so the port reads 1:1 against
 * pg_largeobject.c.
 * ========================================================================= */

/// `LargeObjectMetadataRelationId` — `pg_largeobject_metadata`.
const LargeObjectMetadataRelationId: Oid = LARGE_OBJECT_METADATA_RELATION_ID;
/// `LargeObjectRelationId` — `pg_largeobject`.
const LargeObjectRelationId: Oid = LARGE_OBJECT_RELATION_ID;
/// `LargeObjectMetadataOidIndexId` — `pg_largeobject_metadata_oid_index`.
const LargeObjectMetadataOidIndexId: Oid = LARGE_OBJECT_METADATA_OID_INDEX_ID;
/// `LargeObjectLOidPNIndexId` — `pg_largeobject_loid_pn_index`.
const LargeObjectLOidPNIndexId: Oid = LARGE_OBJECT_LOID_PN_INDEX_ID;

/// `Anum_pg_largeobject_metadata_oid` — the metadata OID column number.
const Anum_pg_largeobject_metadata_oid: AttrNumber = ANUM_PG_LARGEOBJECT_METADATA_OID;
/// `Anum_pg_largeobject_loid` — the data-page `loid` column number.
const Anum_pg_largeobject_loid: AttrNumber = ANUM_PG_LARGEOBJECT_LOID;

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`. The eager fmgr resolution crosses the fmgr seam
/// (panics until fmgr lands, exactly where C does the lookup).
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/* ===========================================================================
 * LargeObjectCreate (pg_largeobject.c:35-88)
 * ========================================================================= */

/// Create a large object having the given LO identifier.
///
/// We create a new large object by inserting an entry into
/// `pg_largeobject_metadata` without any data pages, so that the object will
/// appear to exist with size 0.
pub fn LargeObjectCreate(loid: Oid) -> PgResult<Oid> {
    let loid_new: Oid;
    // values[Natts_pg_largeobject_metadata]; nulls[Natts_pg_largeobject_metadata];
    // (the Datum value layer is built inside catalog_tuple_insert_pg_largeobject_metadata)
    let ownerId: Oid;

    /* The C `CurrentMemoryContext` for `table_open`. */
    let ctx = MemoryContext::new("LargeObjectCreate");
    let mcx = ctx.mcx();

    let pg_lo_meta = table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;

    /*
     * Insert metadata of the largeobject
     */
    // memset(values, 0, sizeof(values)); memset(nulls, false, sizeof(nulls));

    if OidIsValid(loid) {
        loid_new = loid;
    } else {
        loid_new = GetNewOidWithIndex(
            &pg_lo_meta,
            LargeObjectMetadataOidIndexId,
            Anum_pg_largeobject_metadata_oid,
        )?;
    }
    ownerId = get_user_id::call();
    let lomacl = get_user_default_acl::call(ObjectType::Largeobject, ownerId, types_core::primitive::InvalidOid)?;

    // values[Anum_pg_largeobject_metadata_oid - 1] = ObjectIdGetDatum(loid_new);
    // values[Anum_pg_largeobject_metadata_lomowner - 1] = ObjectIdGetDatum(ownerId);
    //
    // if (lomacl != NULL)
    //     values[Anum_pg_largeobject_metadata_lomacl - 1] = PointerGetDatum(lomacl);
    // else
    //     nulls[Anum_pg_largeobject_metadata_lomacl - 1] = true;
    //
    // ntup = heap_form_tuple(RelationGetDescr(pg_lo_meta), values, nulls);
    // CatalogTupleInsert(pg_lo_meta, ntup);
    // heap_freetuple(ntup);  -- reclaimed at memory-context reset.
    catalog_tuple_insert_pg_largeobject_metadata::call(
        &pg_lo_meta,
        loid_new,
        ownerId,
        lomacl.clone(),
    )?;

    pg_lo_meta.close(RowExclusiveLock)?;

    /* dependencies on roles mentioned in default ACL */
    // recordDependencyOnNewAcl(LargeObjectRelationId, loid_new, 0, ownerId, lomacl);
    record_dependency_on_new_acl::call(LargeObjectRelationId, loid_new, 0, ownerId, lomacl)?;

    Ok(loid_new)
}

/* ===========================================================================
 * LargeObjectDrop (pg_largeobject.c:94-152)
 * ========================================================================= */

/// Drop a large object having the given LO identifier.  Both the data pages and
/// metadata must be dropped.
pub fn LargeObjectDrop(loid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("LargeObjectDrop");
    let mcx = ctx.mcx();

    let pg_lo_meta = table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;

    let pg_largeobject = table_open(mcx, LargeObjectRelationId, RowExclusiveLock)?;

    /*
     * Delete an entry from pg_largeobject_metadata
     */
    // ScanKeyInit(&skey[0], Anum_pg_largeobject_metadata_oid,
    //             BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));
    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, loid)?];

    // scan = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true,
    //                           NULL, 1, skey);
    let mut scan = genam::systable_beginscan::call(
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        None,
        &skey,
    )?;

    // tuple = systable_getnext(scan);
    // if (!HeapTupleIsValid(tuple)) ereport(ERROR, ...);
    let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    let Some(tuple) = tuple else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {loid} does not exist"))
            .into_error());
    };

    // CatalogTupleDelete(pg_lo_meta, &tuple->t_self);
    catalog_tuple_delete::call(&pg_lo_meta, tuple.tuple.t_self)?;

    // systable_endscan(scan);
    scan.end()?;

    /*
     * Delete all the associated entries from pg_largeobject
     */
    // ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(loid));
    let skey = [oid_key(Anum_pg_largeobject_loid, loid)?];

    // scan = systable_beginscan(pg_largeobject, LargeObjectLOidPNIndexId, true,
    //                           NULL, 1, skey);
    let mut scan = genam::systable_beginscan::call(
        &pg_largeobject,
        LargeObjectLOidPNIndexId,
        true,
        None,
        &skey,
    )?;

    // while (HeapTupleIsValid(tuple = systable_getnext(scan)))
    //     CatalogTupleDelete(pg_largeobject, &tuple->t_self);
    while let Some(tuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        catalog_tuple_delete::call(&pg_largeobject, tuple.tuple.t_self)?;
    }

    // systable_endscan(scan);
    scan.end()?;

    // table_close(pg_largeobject, RowExclusiveLock);
    pg_largeobject.close(RowExclusiveLock)?;

    // table_close(pg_lo_meta, RowExclusiveLock);
    pg_lo_meta.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * LargeObjectExists (pg_largeobject.c:166-170)
 * ========================================================================= */

/// LargeObjectExists
///
/// We don't use the system cache for large object metadata, for fear of using
/// too much local memory.
///
/// This function always scans the system catalog using an up-to-date snapshot,
/// so it should not be used when a large object is opened in read-only mode
/// (because large objects opened in read only mode are supposed to be viewed
/// relative to the caller's snapshot, whereas in read-write mode they are
/// relative to a current snapshot).
pub fn LargeObjectExists(loid: Oid) -> PgResult<bool> {
    LargeObjectExistsWithSnapshot(loid, None)
}

/* ===========================================================================
 * LargeObjectExistsWithSnapshot (pg_largeobject.c:175-205)
 * ========================================================================= */

/// Same as [`LargeObjectExists`], except the snapshot to read with can be
/// specified.
///
/// `snapshot == None` is the C `NULL` (the up-to-date current snapshot);
/// `Some(handle)` is the caller-supplied registered snapshot for a read-only
/// open.
pub fn LargeObjectExistsWithSnapshot(
    loid: Oid,
    snapshot: Option<Rc<SnapshotData>>,
) -> PgResult<bool> {
    let mut retval: bool = false;

    let ctx = MemoryContext::new("LargeObjectExistsWithSnapshot");
    let mcx = ctx.mcx();

    // ScanKeyInit(&skey[0], Anum_pg_largeobject_metadata_oid,
    //             BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));
    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, loid)?];

    // pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock);
    let pg_lo_meta = table_open(mcx, LargeObjectMetadataRelationId, AccessShareLock)?;

    // sd = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true,
    //                         snapshot, 1, skey);
    let mut sd = genam::systable_beginscan::call(
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        snapshot.as_deref(),
        &skey,
    )?;

    // tuple = systable_getnext(sd);
    // if (HeapTupleIsValid(tuple)) retval = true;
    let tuple = genam::systable_getnext::call(mcx, sd.desc_mut())?;
    if tuple.is_some() {
        retval = true;
    }

    // systable_endscan(sd);
    sd.end()?;

    // table_close(pg_lo_meta, AccessShareLock);
    pg_lo_meta.close(AccessShareLock)?;

    Ok(retval)
}

/// Install this unit's inward seam(s). Wired into `seams-init`'s `init_all`.
pub fn init_seams() {
    backend_catalog_pg_largeobject_seams::large_object_exists_with_snapshot::set(
        |loid, snapshot| LargeObjectExistsWithSnapshot(loid, snapshot),
    );
}
