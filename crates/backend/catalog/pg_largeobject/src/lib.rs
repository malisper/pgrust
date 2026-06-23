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
//! [`pg_largeobject_seams::large_object_exists_with_snapshot`],
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
use snapshot::SnapshotData;
use types_tuple::heaptuple::Datum;

use scankey::ScanKeyInit;
use genam_seams as genam;
use table::table_open;
use aclchk_seams::{get_user_default_acl, record_dependency_on_new_acl};
use catalog_catalog::GetNewOidWithIndex;
use indexing_seams::{
    catalog_tuple_delete, catalog_tuple_insert_pg_largeobject_metadata,
};
use utils_error::ereport;
use miscinit_seams::get_user_id;
use nodes::parsenodes::ObjectType;
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
    let lomacl = get_user_default_acl::call(mcx, ObjectType::Largeobject, ownerId, types_core::primitive::InvalidOid)?;
    let lomacl_bytes: Option<&[u8]> = match &lomacl {
        Some(types_tuple::heaptuple::Datum::ByRef(b)) => Some(&b[..]),
        _ => None,
    };

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
        lomacl_bytes,
    )?;

    pg_lo_meta.close(RowExclusiveLock)?;

    /* dependencies on roles mentioned in default ACL */
    // recordDependencyOnNewAcl(LargeObjectRelationId, loid_new, 0, ownerId, lomacl);
    record_dependency_on_new_acl::call(mcx, LargeObjectRelationId, loid_new, 0, ownerId, lomacl)?;

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

/* ===========================================================================
 * largeobject_owner_acl — the catalog read backing aclchk.c's
 * `pg_largeobject_aclmask_snapshot` (pg_largeobject_metadata has no syscache,
 * so the projection lives here in the snapshot-scanning domain).
 * ========================================================================= */

/// `Anum_pg_largeobject_metadata_lomowner` — the owner column number.
const Anum_pg_largeobject_metadata_lomowner: AttrNumber = 2;
/// `Anum_pg_largeobject_metadata_lomacl` — the ACL column number.
const Anum_pg_largeobject_metadata_lomacl: AttrNumber = 3;

/// `sizeof(AclItem)` (`utils/acl.h`) — 16 bytes (`pg_type.dat` `aclitem`
/// `typlen => 16`).
const SIZEOF_ACLITEM: usize = 16;
/// `ARR_HDRSZ` fixed `ArrayType` header (ndim/dataoffset/elemtype precede the
/// dims), in bytes.
const ARRAYTYPE_HDRSZ: usize = 16;

fn arr_read_i32(a: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes([a[off], a[off + 1], a[off + 2], a[off + 3]])
}

fn arr_maxalign(len: usize) -> usize {
    // MAXALIGN to 8 bytes.
    (len + 7) & !7
}

/// `DatumGetAclP(aclDatum)` + walk the `aclitem[]` elements: detoast the stored
/// ACL varlena, then read `ACL_NUM(acl) = ARR_DIMS(acl)[0]` fixed-16-byte items
/// from `ACL_DAT(acl) = ARR_DATA_PTR(acl)`. A stored ACL is always a
/// well-formed 1-D no-nulls `aclitem` array; a 0-dimension image yields the
/// empty vector. (Mirrors the syscache ACL projection's `decode_acl`.)
fn decode_acl<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    raw: &[u8],
) -> PgResult<mcx::PgVec<'mcx, types_acl::AclItem>> {
    let arr = detoast_seams::detoast_attr::call(mcx, raw)?;
    let ndim = arr_read_i32(&arr, 4);
    let dataoffset = arr_read_i32(&arr, 8);
    let data_off = if dataoffset != 0 {
        dataoffset as usize
    } else {
        arr_maxalign(ARRAYTYPE_HDRSZ + 2 * 4 * ndim as usize)
    };
    let dim0 = if ndim >= 1 {
        arr_read_i32(&arr, ARRAYTYPE_HDRSZ)
    } else {
        0
    };
    let n = if ndim >= 1 { dim0.max(0) as usize } else { 0 };
    let mut items: mcx::PgVec<'mcx, types_acl::AclItem> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let off = data_off + i * SIZEOF_ACLITEM;
        let b = arr.get(off..off + SIZEOF_ACLITEM).ok_or_else(|| {
            types_error::PgError::error("largeobject ACL projection: truncated aclitem array data")
        })?;
        // AclItem { ai_grantee: Oid, ai_grantor: Oid, ai_privs: AclMode (u64) }.
        let ai_grantee = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        let ai_grantor = u32::from_ne_bytes([b[4], b[5], b[6], b[7]]);
        let ai_privs =
            u64::from_ne_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
        items.push(types_acl::AclItem {
            ai_grantee,
            ai_grantor,
            ai_privs,
        });
    }
    Ok(items)
}

/// Catalog read for `pg_largeobject_aclmask_snapshot` (aclchk.c): the
/// `table_open` + snapshot `systable_beginscan` over
/// `pg_largeobject_metadata`, then `GETSTRUCT(lomowner)` +
/// `heap_getattr(Anum_pg_largeobject_metadata_lomacl)`. Returns `(lomowner,
/// decoded lomacl)`; `None` ACL is the C SQL-null column (caller builds
/// `acldefault`). `Ok(None)` for a missing object (caller raises). `snapshot ==
/// None` is the C `NULL`.
pub fn largeobject_owner_acl<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    lobj_oid: Oid,
    snapshot: Option<Rc<SnapshotData>>,
) -> PgResult<Option<(Oid, Option<mcx::PgVec<'mcx, types_acl::AclItem>>)>> {
    // pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock);
    let pg_lo_meta = table_open(mcx, LargeObjectMetadataRelationId, AccessShareLock)?;

    // ScanKeyInit(&entry[0], Anum_pg_largeobject_metadata_oid,
    //             BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(lobj_oid));
    let skey = [oid_key(Anum_pg_largeobject_metadata_oid, lobj_oid)?];

    // scan = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true,
    //                           snapshot, 1, entry);
    let mut scan = genam::systable_beginscan::call(
        &pg_lo_meta,
        LargeObjectMetadataOidIndexId,
        true,
        snapshot.as_deref(),
        &skey,
    )?;

    // tuple = systable_getnext(scan);
    // if (!HeapTupleIsValid(tuple)) -> caller raises "large object %u does not exist".
    let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    let Some(tuple) = tuple else {
        scan.end()?;
        pg_lo_meta.close(AccessShareLock)?;
        return Ok(None);
    };

    // ownerId = ((Form_pg_largeobject_metadata) GETSTRUCT(tuple))->lomowner;
    // aclDatum = heap_getattr(tuple, Anum_pg_largeobject_metadata_lomacl,
    //                         RelationGetDescr(pg_lo_meta), &isNull);
    let cols = heaptuple::heap_deform_tuple(
        mcx,
        &tuple.tuple,
        &pg_lo_meta.rd_att,
        &tuple.data,
    )?;

    let (owner_val, owner_null) = &cols[(Anum_pg_largeobject_metadata_lomowner - 1) as usize];
    if *owner_null {
        return Err(types_error::PgError::error(
            "pg_largeobject_metadata.lomowner is null",
        ));
    }
    let ownerId: Oid = match owner_val {
        Datum::ByVal(v) => *v as u32,
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(types_error::PgError::error(
                "pg_largeobject_metadata.lomowner is by-reference",
            ))
        }
    };

    let (acl_val, acl_null) = &cols[(Anum_pg_largeobject_metadata_lomacl - 1) as usize];
    let acl = if *acl_null {
        None
    } else {
        match acl_val {
            Datum::ByRef(b) => Some(decode_acl(mcx, &b[..])?),
            Datum::ByVal(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => {
                return Err(types_error::PgError::error(
                    "pg_largeobject_metadata.lomacl is by-value",
                ))
            }
        }
    };

    // systable_endscan(scan); table_close(pg_lo_meta, AccessShareLock);
    scan.end()?;
    pg_lo_meta.close(AccessShareLock)?;

    Ok(Some((ownerId, acl)))
}

/// Install this unit's inward seam(s). Wired into `seams-init`'s `init_all`.
pub fn init_seams() {
    pg_largeobject_seams::large_object_exists_with_snapshot::set(
        |loid, snapshot| LargeObjectExistsWithSnapshot(loid, snapshot),
    );
    pg_largeobject_seams::large_object_exists::set(LargeObjectExists);
    pg_largeobject_seams::LargeObjectDrop::set(LargeObjectDrop);
    pg_largeobject_seams::largeobject_owner_acl::set(largeobject_owner_acl);
}
