//! `src/backend/catalog/pg_shdepend.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the pg_shdepend (shared-dependency) relation.
//!
//! Signature mapping:
//! * The open `Relation sdepRel` crosses to the scan/mutate seams as
//!   `&RelationData`; `table_open`..`table_close` spans are `Relation` guard
//!   scopes (the explicit `close(lockmode)` is the C `table_close`, and any
//!   `?` inside the span releases through `Drop`).
//! * pg_shdepend rows cross to the indexing seams as the deformed
//!   `FormData_pg_shdepend` (the caller-shaped projection precedent); the
//!   scan seam returns each row deformed as `SysScanRow`.
//! * The `char deptype` byte is the `i8` of [`SharedDependencyType::as_char`]
//!   / `FormData_pg_shdepend::deptype`.
//! * `checkSharedDependencies`' two `char **` out-params are
//!   `Option<PgString>` (the C NULL when no dependents are found); it takes
//!   `Mcx` since the strings are caller-context allocations.
//! * `updateAclDependencies` / `updateInitAclDependencies` consume their input
//!   `PgVec<Oid>` arrays (the C pfrees them before return).
//! * The DROP OWNED `ObjectAddresses *deleteobjs` collection is an in-crate
//!   `PgVec<ObjectAddress>`; the dependency.c-owned ops (lock/recheck/sort/
//!   performMultipleDeletions) cross as seams over that list.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgString, PgVec};
use types_tuple::heaptuple::ItemPointerData;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_catalog::catalog::{
    AUTH_ID_RELATION_ID, AUTH_MEM_RELATION_ID, COLLATION_RELATION_ID, CONVERSION_RELATION_ID,
    DATABASE_RELATION_ID, DEFAULTTABLESPACE_OID, DEFAULT_ACL_RELATION_ID, EVENT_TRIGGER_RELATION_ID,
    EXTENSION_RELATION_ID, FOREIGN_DATA_WRAPPER_RELATION_ID, FOREIGN_SERVER_RELATION_ID,
    LANGUAGE_RELATION_ID, LARGE_OBJECT_RELATION_ID, NAMESPACE_RELATION_ID,
    OPERATOR_CLASS_RELATION_ID, OPERATOR_FAMILY_RELATION_ID, OPERATOR_RELATION_ID,
    PROCEDURE_RELATION_ID, PUBLICATION_RELATION_ID, RELATION_RELATION_ID, STATISTIC_EXT_RELATION_ID,
    SUBSCRIPTION_RELATION_ID, TABLE_SPACE_RELATION_ID, TS_CONFIG_RELATION_ID,
    TS_DICTIONARY_RELATION_ID, TYPE_RELATION_ID, USER_MAPPING_RELATION_ID,
};
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::catalog_shdepend::{
    Anum_pg_shdepend_classid, Anum_pg_shdepend_dbid, Anum_pg_shdepend_deptype,
    Anum_pg_shdepend_objid, Anum_pg_shdepend_objsubid, Anum_pg_shdepend_refclassid,
    Anum_pg_shdepend_refobjid, FormData_pg_shdepend, Natts_pg_shdepend,
    SharedDependDependerIndexId, SharedDependReferenceIndexId, SharedDependencyType,
    SHARED_DEPENDENCY_ACL, SHARED_DEPENDENCY_INITACL, SHARED_DEPENDENCY_INVALID,
    SHARED_DEPENDENCY_OWNER, SHARED_DEPENDENCY_POLICY, SHARED_DEPENDENCY_TABLESPACE,
    SHARED_DEPEND_RELATION_ID,
};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
// Migrated onto the canonical `Datum<'mcx>` enum. The bare-word newtype
// survives only as `ScalarWord` at the external `ScanKeyData.sk_argument` ABI
// edge (types-scan's `sk_argument` is still a bare word; the scankey crate is
// unmigrated), reached via the enum's `from_oid`/`from_i32` -> bare-word
// conversion.
use types_error::{
    PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_UNDEFINED_OBJECT,
};
use types_nodes::parsenodes::DropBehavior;
use types_rel::{Relation, RelationData};
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, RowExclusiveLock, LOCKMODE};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_access_transam_xact_seams as xact_seams;
use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_catalog_seams as catalog_seams;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_objectaddress_seams as objectaddress_seams;
use backend_commands_alter_seams as alter_seams;
use backend_commands_dbcommands_seams as dbcommands_seams;
use backend_commands_event_trigger_seams as event_trigger_seams;
use backend_commands_foreigncmds_seams as foreigncmds_seams;
use backend_commands_policy_seams as policy_seams;
use backend_commands_publicationcmds_seams as publicationcmds_seams;
use backend_commands_schemacmds_seams as schemacmds_seams;
use backend_commands_subscriptioncmds_seams as subscriptioncmds_seams;
use backend_commands_tablecmds_seams as tablecmds_seams;
use backend_commands_tablespace_seams as tablespace_seams;
use backend_commands_typecmds_seams as typecmds_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as miscadmin_seams;

/// `MAX_CATALOG_MULTI_INSERT_BYTES` (`catalog/indexing.h`).
const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

/// `MAX_REPORTED_DEPS` (pg_shdepend.c).
const MAX_REPORTED_DEPS: i32 = 100;

/// `SharedDependencyObjectType` (pg_shdepend.c, file-private enum).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SharedDependencyObjectType {
    LOCAL_OBJECT,
    SHARED_OBJECT,
    REMOTE_OBJECT,
}
use SharedDependencyObjectType::*;

/// `ShDependObjectInfo` (pg_shdepend.c, file-private struct).
#[derive(Clone, Copy, Debug)]
struct ShDependObjectInfo {
    object: ObjectAddress,
    /// `char deptype`. Stored values are ASCII `< 128`, so comparing as `i8`
    /// matches the C signed-char comparison.
    deptype: i8,
    objtype: SharedDependencyObjectType,
}

/// `remoteDep` (pg_shdepend.c, file-private struct).
struct RemoteDep {
    dbOid: Oid,
    count: i32,
}

/// `ObjectAddressSet(addr, class, object)` — fills classId/objectId, subId=0.
#[inline]
fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

/// `table_open(SharedDependRelationId, lockmode)` — the guard's `Drop` is the
/// error-path `table_close`; the success path closes explicitly. The opened
/// carrier is copied into a short-lived `mcx`, mirroring the relcache-owned
/// lifetime of the C entry.
fn open_shdepend(mcx: Mcx<'_>, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    table::table_open(mcx, SHARED_DEPEND_RELATION_ID, lockmode)
}

/// `ScanKeyInit(&key[n], attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        // `ScanKeyData.sk_argument` is the canonical unified `Datum<'mcx>`
        // (the Datum-unification keystone flipped this edge).
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key[n], attno, BTEqualStrategyNumber, F_INT4EQ,
/// Int32GetDatum(value))`.
fn int4_key<'mcx>(attno: AttrNumber, value: i32) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        // `ScanKeyData.sk_argument` is the canonical unified `Datum<'mcx>`
        // (the Datum-unification keystone flipped this edge).
        Datum::from_i32(value),
    )?;
    Ok(key)
}

/// One scanned pg_shdepend row: the heap TID (`tup->t_self`, for
/// delete/update legs) plus the `heap_deform_tuple` projection of the whole
/// row.
struct SysScanRow<'a> {
    tid: ItemPointerData,
    values: &'a [Datum<'a>],
    isnull: &'a [bool],
}

/// `systable_beginscan(rel, indexId, true, NULL, nkeys, key)` + the
/// `while ((tup = systable_getnext(scan)))` loop + `systable_endscan(scan)`
/// (the genam iterator): invoke `body` once per matching row, in scan order.
/// `body` returning `Ok(true)` continues, `Ok(false)` stops early (the C
/// `break`); an `Err` propagates after the scan is ended (the [`SysScanGuard`]
/// `Drop` covers the error path). The deformed columns / null flags land in
/// a scratch context dropped at the end of each iteration.
fn systable_scan_foreach(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&SysScanRow<'_>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("systable_scan_foreach row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        // GETSTRUCT(tup): the whole row, deformed (every pg_shdepend column is
        // fixed-width and NOT NULL, so by-value).
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum> = vec_with_capacity_in(smcx, cols.len())?;
        let mut isnull: PgVec<'_, bool> = vec_with_capacity_in(smcx, cols.len())?;
        for (value, null) in cols.iter() {
            // Every pg_shdepend column is fixed-width and by-value.
            if let Datum::ByRef(_) = value {
                return Err(PgError::error("pg_shdepend column is not by-value"));
            }
            values.push(value.clone());
            isnull.push(*null);
        }
        let row = SysScanRow {
            tid: tup.tuple.t_self,
            values: &values,
            isnull: &isnull,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
        // The deformed-row scratch context drops at the end of each
        // iteration (declared before the borrows of it, so it outlives them).
    }
    scan.end()
}

/// As [`systable_scan_foreach`], but `body` additionally receives a `recheck`
/// closure standing in for `systable_recheck_tuple(scan, tup)` (genam.c) on
/// the current row: after acquiring a lock on a candidate object, the caller
/// rechecks that the row it is processing is still live, returning `Ok(false)`
/// (the C `false`) if it should be skipped. `Err` from `body`, `recheck`, or
/// the scan machinery propagates after the scan is ended.
fn systable_scan_foreach_recheckable(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&SysScanRow<'_>, &mut dyn FnMut() -> PgResult<bool>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("systable_scan_foreach_recheckable row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum> = vec_with_capacity_in(smcx, cols.len())?;
        let mut isnull: PgVec<'_, bool> = vec_with_capacity_in(smcx, cols.len())?;
        for (value, null) in cols.iter() {
            // Every pg_shdepend column is fixed-width and by-value.
            if let Datum::ByRef(_) = value {
                return Err(PgError::error("pg_shdepend column is not by-value"));
            }
            values.push(value.clone());
            isnull.push(*null);
        }
        let row = SysScanRow {
            tid: tup.tuple.t_self,
            values: &values,
            isnull: &isnull,
        };
        // `systable_recheck_tuple(scan, tuple)`: rechecks the scan's current
        // (most-recently-fetched) row under a fresh catalog snapshot.
        let mut recheck = || genam_seams::systable_recheck_tuple::call(scan.desc_mut());
        let keep_going = body(&row, &mut recheck)?;
        if !keep_going {
            break;
        }
    }
    scan.end()
}

/// `(Form_pg_shdepend) GETSTRUCT(tup)` — interpret one deformed pg_shdepend
/// row. Every pg_shdepend column is fixed-width and NOT NULL.
fn form_pg_shdepend(row: &SysScanRow<'_>) -> FormData_pg_shdepend {
    debug_assert_eq!(row.values.len(), Natts_pg_shdepend);
    debug_assert!(row.isnull.iter().all(|&null| !null));
    let col = |attno: AttrNumber| &row.values[attno as usize - 1];
    FormData_pg_shdepend {
        dbid: col(Anum_pg_shdepend_dbid).as_oid(),
        classid: col(Anum_pg_shdepend_classid).as_oid(),
        objid: col(Anum_pg_shdepend_objid).as_oid(),
        objsubid: col(Anum_pg_shdepend_objsubid).as_i32(),
        refclassid: col(Anum_pg_shdepend_refclassid).as_oid(),
        refobjid: col(Anum_pg_shdepend_refobjid).as_oid(),
        deptype: col(Anum_pg_shdepend_deptype).as_char(),
    }
}

/// `getObjectDescription(object, false)` — `None` if dropped concurrently
/// (the per-class format function returns an empty buffer, i.e. the C NULL).
fn getObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<Option<PgString<'mcx>>> {
    objectaddress_seams::get_object_description::call(mcx, object, false)
}

/// `snprintf` renders a NULL `%s` argument as `"(null)"`; `getObjectDescription`
/// can return NULL.
fn desc_or_null<'a>(desc: &'a Option<PgString<'_>>) -> &'a str {
    match desc {
        Some(d) => d.as_str(),
        None => "(null)",
    }
}

/* ---------------------------------------------------------------------------
 * recordSharedDependencyOn  (C lines 124-156)
 * ------------------------------------------------------------------------- */

/// Record a dependency between 2 objects via their respective ObjectAddresses.
/// The first argument is the dependent object, the second the one it
/// references (which must be a shared object).
pub fn recordSharedDependencyOn(
    depender: &ObjectAddress,
    referenced: &ObjectAddress,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    /* Objects in pg_shdepend can't have SubIds. */
    debug_assert_eq!(depender.objectSubId, 0);
    debug_assert_eq!(referenced.objectSubId, 0);

    /*
     * During bootstrap, do nothing since pg_shdepend may not exist yet.
     * initdb will fill in appropriate pg_shdepend entries after bootstrap.
     */
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    /* If the referenced object is pinned, do nothing. */
    if !catalog_seams::is_pinned_object::call(referenced.classId, referenced.objectId) {
        shdepAddDependency(
            &sdepRel,
            depender.classId,
            depender.objectId,
            depender.objectSubId,
            referenced.classId,
            referenced.objectId,
            deptype,
        )?;
    }

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * recordDependencyOnOwner  (C lines 167-182)
 * ------------------------------------------------------------------------- */

/// A convenient wrapper of recordSharedDependencyOn -- register the specified
/// user as owner of the given object.
pub fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) -> PgResult<()> {
    let myself = ObjectAddress {
        classId,
        objectId,
        objectSubId: 0,
    };
    let referenced = ObjectAddress {
        classId: AUTH_ID_RELATION_ID,
        objectId: owner,
        objectSubId: 0,
    };

    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_OWNER)
}

/* ---------------------------------------------------------------------------
 * shdepChangeDep  (C lines 205-305)
 * ------------------------------------------------------------------------- */

/// Update shared dependency records to account for an updated referenced
/// object.  Internal workhorse for operations such as changing an object's
/// owner.
fn shdepChangeDep(
    sdepRel: &RelationData<'_>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
    refclassid: Oid,
    refobjid: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    let dbid = classIdGetDbId(classid);

    /*
     * Make sure the new referenced object doesn't go away while we record the
     * dependency.
     */
    shdepLockAndCheckObject(refclassid, refobjid)?;

    /* Look for a previous entry */
    let key = [
        oid_key(Anum_pg_shdepend_dbid, dbid)?,
        oid_key(Anum_pg_shdepend_classid, classid)?,
        oid_key(Anum_pg_shdepend_objid, objid)?,
        int4_key(Anum_pg_shdepend_objsubid, objsubid)?,
    ];

    let mut oldtup: Option<(ItemPointerData, FormData_pg_shdepend)> = None;
    let mut dup_err: Option<PgError> = None;

    systable_scan_foreach(
        sdepRel,
        SharedDependDependerIndexId,
        &key,
        |row| {
            let form = form_pg_shdepend(row);
            /* Ignore if not of the target dependency type */
            if form.deptype != deptype.as_char() {
                return Ok(true);
            }
            /* Caller screwed up if multiple matches */
            if oldtup.is_some() {
                dup_err = Some(PgError::error(format!(
                    "multiple pg_shdepend entries for object {}/{}/{} deptype {}",
                    classid, objid, objsubid, deptype.as_char() as u8 as char
                )));
                return Ok(false);
            }
            oldtup = Some((row.tid, form));
            Ok(true)
        },
    )?;

    if let Some(err) = dup_err {
        return Err(err);
    }

    if catalog_seams::is_pinned_object::call(refclassid, refobjid) {
        /* No new entry needed, so just delete existing entry if any */
        if let Some((tid, _)) = oldtup {
            indexing_seams::catalog_tuple_delete::call(sdepRel, tid)?;
        }
    } else if let Some((tid, mut form)) = oldtup {
        /* Need to update existing entry */
        form.refclassid = refclassid;
        form.refobjid = refobjid;
        indexing_seams::catalog_tuple_update_pg_shdepend::call(sdepRel, tid, &form)?;
    } else {
        /* Need to insert new entry */
        let form = FormData_pg_shdepend {
            dbid,
            classid,
            objid,
            objsubid,
            refclassid,
            refobjid,
            deptype: deptype.as_char(),
        };
        indexing_seams::catalog_tuple_insert_pg_shdepend::call(sdepRel, &form)?;
    }

    Ok(())
}

/* ---------------------------------------------------------------------------
 * changeDependencyOnOwner  (C lines 315-358)
 * ------------------------------------------------------------------------- */

/// Update the shared dependencies to account for the new owner.
pub fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    /* Adjust the SHARED_DEPENDENCY_OWNER entry */
    shdepChangeDep(
        &sdepRel,
        classId,
        objectId,
        0,
        AUTH_ID_RELATION_ID,
        newOwnerId,
        SHARED_DEPENDENCY_OWNER,
    )?;

    /*
     * There should never be a SHARED_DEPENDENCY_ACL entry for the owner, so
     * get rid of it if there is one.  This can happen if the new owner was
     * previously granted some rights to the object.
     */
    shdepDropDependency(
        &sdepRel,
        classId,
        objectId,
        0,
        true,
        AUTH_ID_RELATION_ID,
        newOwnerId,
        SHARED_DEPENDENCY_ACL,
    )?;

    /*
     * However, nothing need be done about SHARED_DEPENDENCY_INITACL entries,
     * since those exist whether or not the role is the object's owner, and
     * ALTER OWNER does not modify the underlying pg_init_privs entry.
     */

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * recordDependencyOnTablespace  (C lines 369-380)
 * ------------------------------------------------------------------------- */

/// A convenient wrapper of recordSharedDependencyOn -- register the specified
/// tablespace as default for the given object.
pub fn recordDependencyOnTablespace(classId: Oid, objectId: Oid, tablespace: Oid) -> PgResult<()> {
    let mut myself = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    ObjectAddressSet(&mut myself, classId, objectId);
    ObjectAddressSet(&mut referenced, TABLE_SPACE_RELATION_ID, tablespace);

    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_TABLESPACE)
}

/* ---------------------------------------------------------------------------
 * changeDependencyOnTablespace  (C lines 390-410)
 * ------------------------------------------------------------------------- */

/// Update the shared dependencies to account for the new tablespace.
pub fn changeDependencyOnTablespace(
    classId: Oid,
    objectId: Oid,
    newTablespaceId: Oid,
) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    if newTablespaceId != DEFAULTTABLESPACE_OID && newTablespaceId != InvalidOid {
        shdepChangeDep(
            &sdepRel,
            classId,
            objectId,
            0,
            TABLE_SPACE_RELATION_ID,
            newTablespaceId,
            SHARED_DEPENDENCY_TABLESPACE,
        )?;
    } else {
        shdepDropDependency(
            &sdepRel,
            classId,
            objectId,
            0,
            true,
            InvalidOid,
            InvalidOid,
            SHARED_DEPENDENCY_INVALID,
        )?;
    }

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * getOidListDiff  (C lines 420-463)
 * ------------------------------------------------------------------------- */

/// Takes two Oid arrays and removes elements that are common to both arrays,
/// leaving just those that are in one input but not the other.  Both arrays
/// are assumed sorted and de-duped.  The lengths are updated in-place; the
/// surviving elements are compacted to the front of each slice.
fn getOidListDiff(list1: &mut [Oid], nlist1: &mut usize, list2: &mut [Oid], nlist2: &mut usize) {
    let mut in1: usize = 0;
    let mut in2: usize = 0;
    let mut out1: usize = 0;
    let mut out2: usize = 0;

    while in1 < *nlist1 && in2 < *nlist2 {
        if list1[in1] == list2[in2] {
            /* skip over duplicates */
            in1 += 1;
            in2 += 1;
        } else if list1[in1] < list2[in2] {
            /* list1[in1] is not in list2 */
            list1[out1] = list1[in1];
            out1 += 1;
            in1 += 1;
        } else {
            /* list2[in2] is not in list1 */
            list2[out2] = list2[in2];
            out2 += 1;
            in2 += 1;
        }
    }

    /* any remaining list1 entries are not in list2 */
    while in1 < *nlist1 {
        list1[out1] = list1[in1];
        out1 += 1;
        in1 += 1;
    }

    /* any remaining list2 entries are not in list1 */
    while in2 < *nlist2 {
        list2[out2] = list2[in2];
        out2 += 1;
        in2 += 1;
    }

    *nlist1 = out1;
    *nlist2 = out2;
}

/* ---------------------------------------------------------------------------
 * updateAclDependencies  (C lines 490-500)
 * ------------------------------------------------------------------------- */

/// Update the pg_shdepend info for an object's ACL during GRANT/REVOKE.
///
/// NOTE: Both input arrays must be sorted and de-duped.  (The C pfrees the
/// arrays before return; here ownership of the `PgVec`s is consumed.)
pub fn updateAclDependencies(
    mcx: Mcx<'_>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    oldmembers: PgVec<'_, Oid>,
    newmembers: PgVec<'_, Oid>,
) -> PgResult<()> {
    updateAclDependenciesWorker(
        mcx,
        classId,
        objectId,
        objsubId,
        ownerId,
        SHARED_DEPENDENCY_ACL,
        oldmembers,
        newmembers,
    )
}

/* ---------------------------------------------------------------------------
 * updateInitAclDependencies  (C lines 511-521)
 * ------------------------------------------------------------------------- */

/// Update the pg_shdepend info for a pg_init_privs entry.  Like
/// updateAclDependencies, but considering a pg_init_privs ACL.
pub fn updateInitAclDependencies(
    mcx: Mcx<'_>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    oldmembers: PgVec<'_, Oid>,
    newmembers: PgVec<'_, Oid>,
) -> PgResult<()> {
    updateAclDependenciesWorker(
        mcx,
        classId,
        objectId,
        objsubId,
        InvalidOid, /* ownerId will not be consulted */
        SHARED_DEPENDENCY_INITACL,
        oldmembers,
        newmembers,
    )
}

/* ---------------------------------------------------------------------------
 * updateAclDependenciesWorker  (C lines 524-595)
 * ------------------------------------------------------------------------- */

/// Common code for the above two functions.
fn updateAclDependenciesWorker(
    mcx: Mcx<'_>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    deptype: SharedDependencyType,
    mut oldmembers: PgVec<'_, Oid>,
    mut newmembers: PgVec<'_, Oid>,
) -> PgResult<()> {
    /*
     * Remove entries that are common to both lists; those represent existing
     * dependencies we don't need to change.
     *
     * OK to overwrite the inputs since we'll free them anyway.
     */
    let mut noldmembers = oldmembers.len();
    let mut nnewmembers = newmembers.len();
    getOidListDiff(
        &mut oldmembers,
        &mut noldmembers,
        &mut newmembers,
        &mut nnewmembers,
    );

    if noldmembers > 0 || nnewmembers > 0 {
        let ctx = MemoryContext::new("pg_shdepend");
        let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

        /* Add new dependencies that weren't already present */
        for &roleid in newmembers.iter().take(nnewmembers) {
            /*
             * For SHARED_DEPENDENCY_ACL entries, skip the owner: she has an
             * OWNER shdep entry instead.  But for INITACL entries, we record
             * the owner too.
             */
            if deptype == SHARED_DEPENDENCY_ACL && roleid == ownerId {
                continue;
            }

            /* Skip pinned roles; they don't need dependency entries */
            if catalog_seams::is_pinned_object::call(AUTH_ID_RELATION_ID, roleid) {
                continue;
            }

            shdepAddDependency(
                &sdepRel,
                classId,
                objectId,
                objsubId,
                AUTH_ID_RELATION_ID,
                roleid,
                deptype,
            )?;
        }

        /* Drop no-longer-used old dependencies */
        for &roleid in oldmembers.iter().take(noldmembers) {
            /* Skip the owner for ACL entries, same as above */
            if deptype == SHARED_DEPENDENCY_ACL && roleid == ownerId {
                continue;
            }

            /* Skip pinned roles */
            if catalog_seams::is_pinned_object::call(AUTH_ID_RELATION_ID, roleid) {
                continue;
            }

            shdepDropDependency(
                &sdepRel,
                classId,
                objectId,
                objsubId,
                false, /* exact match on objsubId */
                AUTH_ID_RELATION_ID,
                roleid,
                deptype,
            )?;
        }

        sdepRel.close(RowExclusiveLock)?;
    }

    let _ = mcx;
    /* C pfrees oldmembers / newmembers here; Rust drops them at scope end. */
    drop(oldmembers);
    drop(newmembers);
    Ok(())
}

/* ---------------------------------------------------------------------------
 * shared_dependency_comparator  (C lines 609-654)
 * ------------------------------------------------------------------------- */

/// qsort comparator for ShDependObjectInfo items.
fn shared_dependency_comparator(
    obja: &ShDependObjectInfo,
    objb: &ShDependObjectInfo,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    /* Primary sort key is OID ascending. */
    if obja.object.objectId < objb.object.objectId {
        return Ordering::Less;
    }
    if obja.object.objectId > objb.object.objectId {
        return Ordering::Greater;
    }

    /* Next sort on catalog ID, in case identical OIDs appear in different
     * catalogs. */
    if obja.object.classId < objb.object.classId {
        return Ordering::Less;
    }
    if obja.object.classId > objb.object.classId {
        return Ordering::Greater;
    }

    /* Sort on object subId, as an unsigned int so that 0 (the whole object)
     * comes first. */
    if (obja.object.objectSubId as u32) < (objb.object.objectSubId as u32) {
        return Ordering::Less;
    }
    if (obja.object.objectSubId as u32) > (objb.object.objectSubId as u32) {
        return Ordering::Greater;
    }

    /* Last, sort on deptype. */
    if obja.deptype < objb.deptype {
        return Ordering::Less;
    }
    if obja.deptype > objb.deptype {
        return Ordering::Greater;
    }

    Ordering::Equal
}

/* ---------------------------------------------------------------------------
 * checkSharedDependencies  (C lines 675-885)
 * ------------------------------------------------------------------------- */

/// Check whether there are shared dependency entries for a given shared
/// object; return `true` if so.  On success returns the
/// `(detail_msg, detail_log_msg)` strings (each `None` when no dependents are
/// found).
pub fn checkSharedDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
) -> PgResult<(bool, Option<PgString<'mcx>>, Option<PgString<'mcx>>)> {
    let mut numReportedDeps: i32 = 0;
    let mut numNotReportedDeps: i32 = 0;
    let mut numNotReportedDbs: i32 = 0;
    let mut remDeps: PgVec<'_, RemoteDep> = PgVec::new_in(mcx);
    let mut object = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    /* This case can be dispatched quickly */
    if catalog_seams::is_pinned_object::call(classId, objectId) {
        object.classId = classId;
        object.objectId = objectId;
        object.objectSubId = 0;
        let desc = objectaddress_seams::get_object_description::call(mcx, &object, false)?;
        return Err(PgError::error(format!(
            "cannot drop {} because it is required by the database system",
            desc_or_null(&desc)
        ))
        .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST));
    }

    let mut objects: PgVec<'_, ShDependObjectInfo> = PgVec::new_in(mcx);
    let mut descs = PgString::new_in(mcx);
    let mut alldescs = PgString::new_in(mcx);

    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_shdepend_refclassid, classId)?,
        oid_key(Anum_pg_shdepend_refobjid, objectId)?,
    ];

    let my_database_id = miscadmin_seams::my_database_id::call();

    systable_scan_foreach(
        &sdepRel,
        SharedDependReferenceIndexId,
        &key,
        |row| {
            let sdepForm = form_pg_shdepend(row);

            object.classId = sdepForm.classid;
            object.objectId = sdepForm.objid;
            object.objectSubId = sdepForm.objsubid;

            /*
             * If it's a dependency local to this database or it's a shared
             * object, add it to the objects array.  If remote, keep track of
             * count.
             */
            if sdepForm.dbid == my_database_id || sdepForm.dbid == InvalidOid {
                objects
                    .try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<ShDependObjectInfo>()))?;
                objects.push(ShDependObjectInfo {
                    object,
                    deptype: sdepForm.deptype,
                    objtype: if sdepForm.dbid == my_database_id {
                        LOCAL_OBJECT
                    } else {
                        SHARED_OBJECT
                    },
                });
            } else {
                /* It's not local nor shared, so it must be remote. */
                let mut stored = false;
                for dep in remDeps.iter_mut() {
                    if dep.dbOid == sdepForm.dbid {
                        dep.count += 1;
                        stored = true;
                        break;
                    }
                }
                if !stored {
                    remDeps
                        .try_reserve(1)
                        .map_err(|_| mcx.oom(core::mem::size_of::<RemoteDep>()))?;
                    remDeps.push(RemoteDep {
                        dbOid: sdepForm.dbid,
                        count: 1,
                    });
                }
            }
            Ok(true)
        },
    )?;

    sdepRel.close(AccessShareLock)?;

    /* Sort and report local and shared objects. */
    if objects.len() > 1 {
        objects.sort_by(shared_dependency_comparator);
    }

    for i in 0..objects.len() {
        let obj = objects[i];
        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeObjectDescription(
                mcx,
                &mut descs,
                obj.objtype,
                &obj.object,
                SharedDependencyType(obj.deptype),
                0,
            )?;
        } else {
            numNotReportedDeps += 1;
        }
        storeObjectDescription(
            mcx,
            &mut alldescs,
            obj.objtype,
            &obj.object,
            SharedDependencyType(obj.deptype),
            0,
        )?;
    }

    /* Summarize dependencies in remote databases. */
    for i in 0..remDeps.len() {
        let count = remDeps[i].count;
        let db_oid = remDeps[i].dbOid;
        object.classId = DATABASE_RELATION_ID;
        object.objectId = db_oid;
        object.objectSubId = 0;

        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeObjectDescription(
                mcx,
                &mut descs,
                REMOTE_OBJECT,
                &object,
                SHARED_DEPENDENCY_INVALID,
                count,
            )?;
        } else {
            numNotReportedDbs += 1;
        }
        storeObjectDescription(
            mcx,
            &mut alldescs,
            REMOTE_OBJECT,
            &object,
            SHARED_DEPENDENCY_INVALID,
            count,
        )?;
    }

    if descs.is_empty() {
        return Ok((false, None, None));
    }

    if numNotReportedDeps > 0 {
        descs.try_push_str(&ngettext_format(
            "\nand %d other object (see server log for list)",
            "\nand %d other objects (see server log for list)",
            numNotReportedDeps,
        ))?;
    }
    if numNotReportedDbs > 0 {
        descs.try_push_str(&ngettext_format(
            "\nand objects in %d other database (see server log for list)",
            "\nand objects in %d other databases (see server log for list)",
            numNotReportedDbs,
        ))?;
    }

    Ok((true, Some(descs), Some(alldescs)))
}

/// `ngettext(singular, plural, n)` then `appendStringInfo(..., n)`: select the
/// form by the C plural rule (n == 1 -> singular) and substitute the single
/// `%d` with `n`.
fn ngettext_format(singular: &str, plural: &str, n: i32) -> String {
    let template = if n == 1 { singular } else { plural };
    template.replacen("%d", &n.to_string(), 1)
}

/* ---------------------------------------------------------------------------
 * copyTemplateDependencies  (C lines 894-990)
 * ------------------------------------------------------------------------- */

/// Routine to create the initial shared dependencies of a new database.  We
/// simply copy the dependencies from the template database.
pub fn copyTemplateDependencies(templateDbId: Oid, newDbId: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    /* max_slots = MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_shdepend) */
    let max_slots = MAX_CATALOG_MULTI_INSERT_BYTES / core::mem::size_of::<FormData_pg_shdepend>();

    /* Scan all entries with dbid = templateDbId */
    let key = [oid_key(Anum_pg_shdepend_dbid, templateDbId)?];

    /*
     * Copy the entries of the original database, changing the database Id to
     * that of the new database.  Rows with dbId == 0 are not returned by the
     * dbid=templateDbId scan, so they are naturally skipped (matching the C).
     */
    let mut batch: PgVec<'_, FormData_pg_shdepend> = vec_with_capacity_in(ctx.mcx(), max_slots)?;

    systable_scan_foreach(
        &sdepRel,
        SharedDependDependerIndexId,
        &key,
        |row| {
            let shdep = form_pg_shdepend(row);

            batch.push(FormData_pg_shdepend {
                dbid: newDbId,
                classid: shdep.classid,
                objid: shdep.objid,
                objsubid: shdep.objsubid,
                refclassid: shdep.refclassid,
                refobjid: shdep.refobjid,
                deptype: shdep.deptype,
            });

            /* If slots are full, insert a batch of tuples */
            if batch.len() == max_slots {
                indexing_seams::catalog_tuples_multi_insert_pg_shdepend::call(ctx.mcx(), &sdepRel, &batch)?;
                batch.clear();
            }
            Ok(true)
        },
    )?;

    /* Insert any tuples left in the buffer */
    if !batch.is_empty() {
        indexing_seams::catalog_tuples_multi_insert_pg_shdepend::call(ctx.mcx(), &sdepRel, &batch)?;
    }

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * dropDatabaseDependencies  (C lines 998-1034)
 * ------------------------------------------------------------------------- */

/// Delete pg_shdepend entries corresponding to a database that's being
/// dropped.
pub fn dropDatabaseDependencies(databaseId: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    /*
     * First, delete all the entries that have the database Oid in the dbid
     * field.
     */
    let key = [oid_key(Anum_pg_shdepend_dbid, databaseId)?];
    /* We leave the other index fields unspecified */

    systable_scan_foreach(
        &sdepRel,
        SharedDependDependerIndexId,
        &key,
        |row| {
            indexing_seams::catalog_tuple_delete::call(&sdepRel, row.tid)?;
            Ok(true)
        },
    )?;

    /* Now delete all entries corresponding to the database itself */
    shdepDropDependency(
        &sdepRel,
        DATABASE_RELATION_ID,
        databaseId,
        0,
        true,
        InvalidOid,
        InvalidOid,
        SHARED_DEPENDENCY_INVALID,
    )?;

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * deleteSharedDependencyRecordsFor  (C lines 1046-1059)
 * ------------------------------------------------------------------------- */

/// Delete all pg_shdepend entries corresponding to an object that's being
/// dropped or modified.
pub fn deleteSharedDependencyRecordsFor(
    classId: Oid,
    objectId: Oid,
    objectSubId: i32,
) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    shdepDropDependency(
        &sdepRel,
        classId,
        objectId,
        objectSubId,
        objectSubId == 0,
        InvalidOid,
        InvalidOid,
        SHARED_DEPENDENCY_INVALID,
    )?;

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * shdepAddDependency  (C lines 1068-1105)
 * ------------------------------------------------------------------------- */

/// Internal workhorse for inserting into pg_shdepend.
fn shdepAddDependency(
    sdepRel: &RelationData<'_>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    refclassId: Oid,
    refobjId: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    /*
     * Make sure the object doesn't go away while we record the dependency on
     * it.  DROP routines should lock the object exclusively before they check
     * shared dependencies.
     */
    shdepLockAndCheckObject(refclassId, refobjId)?;

    /* Form the new tuple and record the dependency. */
    let form = FormData_pg_shdepend {
        dbid: classIdGetDbId(classId),
        classid: classId,
        objid: objectId,
        objsubid: objsubId,
        refclassid: refclassId,
        refobjid: refobjId,
        deptype: deptype.as_char(),
    };

    indexing_seams::catalog_tuple_insert_pg_shdepend::call(sdepRel, &form)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * shdepDropDependency  (C lines 1123-1180)
 * ------------------------------------------------------------------------- */

/// Internal workhorse for deleting entries from pg_shdepend.
fn shdepDropDependency(
    sdepRel: &RelationData<'_>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    drop_subobjects: bool,
    refclassId: Oid,
    refobjId: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    /* Scan for entries matching the dependent object */
    let key = [
        oid_key(Anum_pg_shdepend_dbid, classIdGetDbId(classId))?,
        oid_key(Anum_pg_shdepend_classid, classId)?,
        oid_key(Anum_pg_shdepend_objid, objectId)?,
        int4_key(Anum_pg_shdepend_objsubid, objsubId)?,
    ];
    let nkeys = if drop_subobjects { 3 } else { 4 };

    /*
     * When drop_subobjects, the C leaves key[3] unset and passes nkeys=3; we
     * slice off key[3] here so it is never read, matching the C.
     */
    systable_scan_foreach(
        sdepRel,
        SharedDependDependerIndexId,
        &key[..nkeys],
        |row| {
            let shdepForm = form_pg_shdepend(row);

            /* Filter entries according to additional parameters */
            if OidIsValid(refclassId) && shdepForm.refclassid != refclassId {
                return Ok(true);
            }
            if OidIsValid(refobjId) && shdepForm.refobjid != refobjId {
                return Ok(true);
            }
            if deptype != SHARED_DEPENDENCY_INVALID && shdepForm.deptype != deptype.as_char() {
                return Ok(true);
            }

            /* OK, delete it */
            indexing_seams::catalog_tuple_delete::call(sdepRel, row.tid)?;
            Ok(true)
        },
    )?;

    Ok(())
}

/* ---------------------------------------------------------------------------
 * classIdGetDbId  (C lines 1189-1200)
 * ------------------------------------------------------------------------- */

/// Get the database Id that should be used in pg_shdepend, given the OID of
/// the catalog containing the object.  For shared objects, it's InvalidOid;
/// for all other objects, it's the current database Id.
fn classIdGetDbId(classId: Oid) -> Oid {
    if catalog_seams::is_shared_relation::call(classId) {
        InvalidOid
    } else {
        miscadmin_seams::my_database_id::call()
    }
}

/* ---------------------------------------------------------------------------
 * shdepLockAndCheckObject  (C lines 1210-1258)
 * ------------------------------------------------------------------------- */

/// Lock the object that we are about to record a dependency on.  After it's
/// locked, verify that it hasn't been dropped while we weren't looking.  If
/// the object has been dropped, this function returns an Err (C does not
/// return).
pub fn shdepLockAndCheckObject(classId: Oid, objectId: Oid) -> PgResult<()> {
    /* AccessShareLock should be OK, since we are not modifying the object */
    /* The lock is held until end of transaction (the C default). */
    lmgr_seams::lock_shared_object::call(classId, objectId, 0, AccessShareLock)?.keep();

    if classId == AUTH_ID_RELATION_ID {
        if !syscache_seams::auth_oid_exists::call(objectId)? {
            return Err(
                PgError::error(format!("role {} was concurrently dropped", objectId))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            );
        }
    } else if classId == TABLE_SPACE_RELATION_ID {
        /* For lack of a syscache on pg_tablespace, do this: */
        let ctx = MemoryContext::new("pg_shdepend");
        let tablespace = tablespace_seams::get_tablespace_name::call(ctx.mcx(), objectId)?;
        if tablespace.is_none() {
            return Err(PgError::error(format!(
                "tablespace {} was concurrently dropped",
                objectId
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    } else if classId == DATABASE_RELATION_ID {
        /* For lack of a syscache on pg_database, do this: */
        let ctx = MemoryContext::new("pg_shdepend");
        let database = dbcommands_seams::get_database_name::call(ctx.mcx(), objectId)?;
        if database.is_none() {
            return Err(PgError::error(format!(
                "database {} was concurrently dropped",
                objectId
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    } else {
        return Err(PgError::error(format!(
            "unrecognized shared classId: {}",
            classId
        )));
    }

    Ok(())
}

/* ---------------------------------------------------------------------------
 * storeObjectDescription  (C lines 1275-1326)
 * ------------------------------------------------------------------------- */

/// Append the description of a dependent object to "descs".
fn storeObjectDescription(
    mcx: Mcx<'_>,
    descs: &mut PgString<'_>,
    type_: SharedDependencyObjectType,
    object: &ObjectAddress,
    deptype: SharedDependencyType,
    count: i32,
) -> PgResult<()> {
    let objdesc = getObjectDescription(mcx, object)?;

    /* An object being dropped concurrently doesn't need to be reported. */
    let objdesc = match objdesc {
        Some(s) => s,
        None => return Ok(()),
    };

    /* separate entries with a newline */
    if !descs.is_empty() {
        descs.try_push('\n')?;
    }

    match type_ {
        LOCAL_OBJECT | SHARED_OBJECT => {
            if deptype == SHARED_DEPENDENCY_OWNER {
                descs.try_push_str(&format!("owner of {}", objdesc.as_str()))?;
            } else if deptype == SHARED_DEPENDENCY_ACL {
                descs.try_push_str(&format!("privileges for {}", objdesc.as_str()))?;
            } else if deptype == SHARED_DEPENDENCY_INITACL {
                descs.try_push_str(&format!("initial privileges for {}", objdesc.as_str()))?;
            } else if deptype == SHARED_DEPENDENCY_POLICY {
                descs.try_push_str(&format!("target of {}", objdesc.as_str()))?;
            } else if deptype == SHARED_DEPENDENCY_TABLESPACE {
                descs.try_push_str(&format!("tablespace for {}", objdesc.as_str()))?;
            } else {
                return Err(PgError::error(format!(
                    "unrecognized dependency type: {}",
                    deptype.as_char() as i32
                )));
            }
        }
        REMOTE_OBJECT => {
            /* translator: %s will always be "database %s" */
            descs.try_push_str(&ngettext_format_s(
                "%d object in %s",
                "%d objects in %s",
                count,
                objdesc.as_str(),
            ))?;
        }
    }

    Ok(())
}

/// `ngettext("%d object in %s", "%d objects in %s", count)` then
/// `appendStringInfo(..., count, objdesc)`.
fn ngettext_format_s(singular: &str, plural: &str, count: i32, s: &str) -> String {
    let template = if count == 1 { singular } else { plural };
    template
        .replacen("%d", &count.to_string(), 1)
        .replacen("%s", s, 1)
}

/* ---------------------------------------------------------------------------
 * shdepDropOwned  (C lines 1341-1521)
 * ------------------------------------------------------------------------- */

/// Drop the objects owned by any one of the given RoleIds.  Grants are removed
/// while scanning; drops are saved up and done all at once with
/// performMultipleDeletions.
pub fn shdepDropOwned(roleids: &[Oid], behavior: DropBehavior) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");
    /* `deleteobjs` — the C `ObjectAddresses` collection. */
    let mut deleteobjs: PgVec<'_, ObjectAddress> = PgVec::new_in(ctx.mcx());

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock failures.
     */
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    let my_database_id = miscadmin_seams::my_database_id::call();

    /*
     * For each role, find the dependent objects and drop them using the
     * regular (non-shared) dependency management.
     */
    for &roleid in roleids.iter() {
        /* Doesn't work for pinned objects */
        if catalog_seams::is_pinned_object::call(AUTH_ID_RELATION_ID, roleid) {
            let obj = ObjectAddress {
                classId: AUTH_ID_RELATION_ID,
                objectId: roleid,
                objectSubId: 0,
            };
            let desc = objectaddress_seams::get_object_description::call(ctx.mcx(), &obj, false)?;
            return Err(PgError::error(format!(
                "cannot drop objects owned by {} because they are required by the database system",
                desc_or_null(&desc)
            ))
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST));
        }

        let key = [
            oid_key(Anum_pg_shdepend_refclassid, AUTH_ID_RELATION_ID)?,
            oid_key(Anum_pg_shdepend_refobjid, roleid)?,
        ];

        systable_scan_foreach_recheckable(
            &sdepRel,
            SharedDependReferenceIndexId,
            &key,
            |row, recheck| {
                let sdepForm = form_pg_shdepend(row);

                /*
                 * We only operate on shared objects and objects in the current
                 * database
                 */
                if sdepForm.dbid != my_database_id && sdepForm.dbid != InvalidOid {
                    return Ok(true);
                }

                let deptype = SharedDependencyType(sdepForm.deptype);

                if deptype == SHARED_DEPENDENCY_INVALID {
                    /* Shouldn't happen */
                    return Err(PgError::error("unexpected dependency type".to_string()));
                } else if deptype == SHARED_DEPENDENCY_POLICY {
                    /*
                     * Try to remove role from policy; if unable to, remove
                     * policy.
                     */
                    if !policy_seams::remove_role_from_object_policy::call(
                        roleid,
                        sdepForm.classid,
                        sdepForm.objid,
                    )? {
                        let obj = ObjectAddress {
                            classId: sdepForm.classid,
                            objectId: sdepForm.objid,
                            objectSubId: sdepForm.objsubid,
                        };
                        /*
                         * Acquire lock on object, then verify this dependency
                         * is still relevant.  If not, ignore the object.
                         */
                        dependency_seams::acquire_deletion_lock::call(&obj, 0)?;
                        if !recheck()? {
                            dependency_seams::release_deletion_lock::call(&obj)?;
                            return Ok(true);
                        }
                        deleteobjs
                            .try_reserve(1)
                            .map_err(|_| ctx.mcx().oom(core::mem::size_of::<ObjectAddress>()))?;
                        deleteobjs.push(obj);
                    }
                } else if deptype == SHARED_DEPENDENCY_ACL {
                    /*
                     * Dependencies on role grants are recorded using
                     * SHARED_DEPENDENCY_ACL, but unlike a regular ACL list
                     * there's a separate catalog row for each grant - so
                     * removing the grant just means removing the entire row.
                     */
                    if sdepForm.classid != AUTH_MEM_RELATION_ID {
                        aclchk_seams::remove_role_from_object_acl::call(
                            roleid,
                            sdepForm.classid,
                            sdepForm.objid,
                        )?;
                        return Ok(true);
                    }
                    /* FALLTHROUGH to SHARED_DEPENDENCY_OWNER */
                    shdepDropOwned_owner_branch(
                        ctx.mcx(),
                        &sdepForm,
                        my_database_id,
                        recheck,
                        &mut deleteobjs,
                    )?;
                } else if deptype == SHARED_DEPENDENCY_OWNER {
                    shdepDropOwned_owner_branch(
                        ctx.mcx(),
                        &sdepForm,
                        my_database_id,
                        recheck,
                        &mut deleteobjs,
                    )?;
                } else if deptype == SHARED_DEPENDENCY_INITACL {
                    /*
                     * Any mentions of the role that remain in pg_init_privs
                     * entries are just dropped.
                     */
                    /* Shouldn't see a role grant here */
                    debug_assert!(sdepForm.classid != AUTH_MEM_RELATION_ID);
                    aclchk_seams::remove_role_from_init_priv::call(
                        roleid,
                        sdepForm.classid,
                        sdepForm.objid,
                        sdepForm.objsubid,
                    )?;
                }
                /* (No default branch in the C switch.) */
                Ok(true)
            },
        )?;
    }

    /*
     * For stability of deletion-report ordering, sort the objects into
     * approximate reverse creation order before deletion.
     */
    dependency_seams::sort_object_addresses::call(&mut deleteobjs);

    /* the dependency mechanism does the actual work */
    dependency_seams::perform_multiple_deletions::call(&deleteobjs, behavior, 0)?;

    sdepRel.close(RowExclusiveLock)?;

    /* free_object_addresses(deleteobjs) — owned vec, dropped here */
    Ok(())
}

/// The `SHARED_DEPENDENCY_OWNER` case body of `shdepDropOwned` (also reached by
/// the `SHARED_DEPENDENCY_ACL` FALLTHROUGH for role grants).  C lines
/// 1463-1485.
fn shdepDropOwned_owner_branch(
    mcx: Mcx<'_>,
    sdepForm: &FormData_pg_shdepend,
    my_database_id: Oid,
    recheck: &mut dyn FnMut() -> PgResult<bool>,
    deleteobjs: &mut PgVec<'_, ObjectAddress>,
) -> PgResult<()> {
    /*
     * Save it for deletion below, if it's a local object or a role grant.
     * Other shared objects, such as databases, should not be removed here.
     */
    if sdepForm.dbid == my_database_id || sdepForm.classid == AUTH_MEM_RELATION_ID {
        let obj = ObjectAddress {
            classId: sdepForm.classid,
            objectId: sdepForm.objid,
            objectSubId: sdepForm.objsubid,
        };
        /* as above */
        dependency_seams::acquire_deletion_lock::call(&obj, 0)?;
        if !recheck()? {
            dependency_seams::release_deletion_lock::call(&obj)?;
            return Ok(());
        }
        deleteobjs
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<ObjectAddress>()))?;
        deleteobjs.push(obj);
    }
    Ok(())
}

/* ---------------------------------------------------------------------------
 * shdepReassignOwned  (C lines 1529-1639)
 * ------------------------------------------------------------------------- */

/// Change the owner of objects owned by any of the roles in roleids to
/// newrole.  Grants are not touched.
pub fn shdepReassignOwned(roleids: &[Oid], newrole: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_shdepend");

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock problems.
     */
    let sdepRel = open_shdepend(ctx.mcx(), RowExclusiveLock)?;

    let my_database_id = miscadmin_seams::my_database_id::call();

    for &roleid in roleids.iter() {
        /* Refuse to work on pinned roles */
        if catalog_seams::is_pinned_object::call(AUTH_ID_RELATION_ID, roleid) {
            let obj = ObjectAddress {
                classId: AUTH_ID_RELATION_ID,
                objectId: roleid,
                objectSubId: 0,
            };
            let desc = objectaddress_seams::get_object_description::call(ctx.mcx(), &obj, false)?;
            return Err(PgError::error(format!(
                "cannot reassign ownership of objects owned by {} because they are required by the database system",
                desc_or_null(&desc)
            ))
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST));
        }

        let key = [
            oid_key(Anum_pg_shdepend_refclassid, AUTH_ID_RELATION_ID)?,
            oid_key(Anum_pg_shdepend_refobjid, roleid)?,
        ];

        systable_scan_foreach(
            &sdepRel,
            SharedDependReferenceIndexId,
            &key,
            |row| {
                let sdepForm = form_pg_shdepend(row);

                /*
                 * We only operate on shared objects and objects in the current
                 * database
                 */
                if sdepForm.dbid != my_database_id && sdepForm.dbid != InvalidOid {
                    return Ok(true);
                }

                /*
                 * The C runs each call in a short-lived memory context to
                 * bound the leak; the Rust port relies on Rust ownership
                 * instead, so the AllocSetContextCreate/Switch/Delete
                 * bracketing is a no-op here.
                 */

                /* Perform the appropriate processing */
                let deptype = SharedDependencyType(sdepForm.deptype);
                if deptype == SHARED_DEPENDENCY_OWNER {
                    shdepReassignOwned_Owner(&sdepForm, newrole)?;
                } else if deptype == SHARED_DEPENDENCY_INITACL {
                    shdepReassignOwned_InitAcl(&sdepForm, roleid, newrole)?;
                } else if deptype == SHARED_DEPENDENCY_ACL
                    || deptype == SHARED_DEPENDENCY_POLICY
                    || deptype == SHARED_DEPENDENCY_TABLESPACE
                {
                    /* Nothing to do for these entry types */
                } else {
                    return Err(PgError::error(format!(
                        "unrecognized dependency type: {}",
                        sdepForm.deptype as i32
                    )));
                }

                /* Make sure the next iteration will see my changes */
                xact_seams::command_counter_increment::call()?;
                Ok(true)
            },
        )?;
    }

    sdepRel.close(RowExclusiveLock)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * shdepReassignOwned_Owner  (C lines 1646-1726)
 * ------------------------------------------------------------------------- */

/// shdepReassignOwned's processing of SHARED_DEPENDENCY_OWNER entries.
fn shdepReassignOwned_Owner(sdepForm: &FormData_pg_shdepend, newrole: Oid) -> PgResult<()> {
    let classid = sdepForm.classid;
    let objid = sdepForm.objid;

    /* Issue the appropriate ALTER OWNER call */
    if classid == TYPE_RELATION_ID {
        typecmds_seams::alter_type_owner_oid::call(objid, newrole, true)?;
    } else if classid == NAMESPACE_RELATION_ID {
        schemacmds_seams::alter_schema_owner_oid::call(objid, newrole)?;
    } else if classid == RELATION_RELATION_ID {
        /*
         * Pass recursing = true so that we don't fail on indexes, owned
         * sequences, etc when we happen to visit them before their parent
         * table.
         */
        tablecmds_seams::at_exec_change_owner::call(objid, newrole, true, AccessExclusiveLock)?;
    } else if classid == DEFAULT_ACL_RELATION_ID {
        /*
         * Ignore default ACLs; they should be handled by DROP OWNED, not
         * REASSIGN OWNED.
         */
    } else if classid == USER_MAPPING_RELATION_ID {
        /* ditto */
    } else if classid == FOREIGN_SERVER_RELATION_ID {
        foreigncmds_seams::alter_foreign_server_owner_oid::call(objid, newrole)?;
    } else if classid == FOREIGN_DATA_WRAPPER_RELATION_ID {
        foreigncmds_seams::alter_foreign_data_wrapper_owner_oid::call(objid, newrole)?;
    } else if classid == EVENT_TRIGGER_RELATION_ID {
        event_trigger_seams::alter_event_trigger_owner_oid::call(objid, newrole)?;
    } else if classid == PUBLICATION_RELATION_ID {
        publicationcmds_seams::alter_publication_owner_oid::call(objid, newrole)?;
    } else if classid == SUBSCRIPTION_RELATION_ID {
        subscriptioncmds_seams::alter_subscription_owner_oid::call(objid, newrole)?;
    } else if classid == COLLATION_RELATION_ID
        || classid == CONVERSION_RELATION_ID
        || classid == OPERATOR_RELATION_ID
        || classid == PROCEDURE_RELATION_ID
        || classid == LANGUAGE_RELATION_ID
        || classid == LARGE_OBJECT_RELATION_ID
        || classid == OPERATOR_FAMILY_RELATION_ID
        || classid == OPERATOR_CLASS_RELATION_ID
        || classid == EXTENSION_RELATION_ID
        || classid == STATISTIC_EXT_RELATION_ID
        || classid == TABLE_SPACE_RELATION_ID
        || classid == DATABASE_RELATION_ID
        || classid == TS_CONFIG_RELATION_ID
        || classid == TS_DICTIONARY_RELATION_ID
    {
        /* Generic alter owner cases */
        alter_seams::alter_object_owner_internal::call(classid, objid, newrole)?;
    } else {
        return Err(PgError::error(format!("unexpected classid {}", classid)));
    }

    Ok(())
}

/* ---------------------------------------------------------------------------
 * shdepReassignOwned_InitAcl  (C lines 1733-1759)
 * ------------------------------------------------------------------------- */

/// shdepReassignOwned's processing of SHARED_DEPENDENCY_INITACL entries.
fn shdepReassignOwned_InitAcl(
    sdepForm: &FormData_pg_shdepend,
    oldrole: Oid,
    newrole: Oid,
) -> PgResult<()> {
    aclchk_seams::replace_role_in_init_priv::call(
        oldrole,
        newrole,
        sdepForm.classid,
        sdepForm.objid,
        sdepForm.objsubid,
    )
}

/// Install this crate's implementations into
/// `backend-catalog-pg-shdepend-seams`.
pub fn init_seams() {
    use backend_catalog_pg_shdepend_seams as seams;

    seams::recordSharedDependencyOn::set(recordSharedDependencyOn);
    seams::recordDependencyOnOwner::set(recordDependencyOnOwner);
    seams::changeDependencyOnOwner::set(changeDependencyOnOwner);
    seams::recordDependencyOnTablespace::set(recordDependencyOnTablespace);
    seams::changeDependencyOnTablespace::set(changeDependencyOnTablespace);
    seams::updateAclDependencies::set(updateAclDependencies);
    seams::updateInitAclDependencies::set(updateInitAclDependencies);
    seams::checkSharedDependencies::set(checkSharedDependencies);
    seams::copyTemplateDependencies::set(copyTemplateDependencies);
    seams::dropDatabaseDependencies::set(dropDatabaseDependencies);
    seams::deleteSharedDependencyRecordsFor::set(deleteSharedDependencyRecordsFor);
    seams::shdepLockAndCheckObject::set(shdepLockAndCheckObject);
    seams::shdepDropOwned::set(shdepDropOwned);
    seams::shdepReassignOwned::set(shdepReassignOwned);

    // user.c DROP/REASSIGN ROLE shared-dependency seams (all on
    // AuthIdRelationId). check_shared_dependencies returns Some((detail,
    // detail_log)) when the role still has dependents, else None.
    backend_commands_user_seams::shdep_lock_and_check_object::set(shdepLockAndCheckObject);
    backend_commands_user_seams::check_shared_dependencies::set(|roleid| {
        let ctx = mcx::MemoryContext::new("checkSharedDependencies");
        let (has_deps, detail, detail_log) =
            checkSharedDependencies(ctx.mcx(), types_core::AUTH_ID_RELATION_ID, roleid)?;
        if has_deps {
            Ok(Some((
                detail.map(|s| s.to_string()).unwrap_or_default(),
                detail_log.map(|s| s.to_string()).unwrap_or_default(),
            )))
        } else {
            Ok(None)
        }
    });
    backend_commands_user_seams::shdep_drop_owned::set(|role_ids, behavior| {
        shdepDropOwned(&role_ids, behavior)
    });
    backend_commands_user_seams::shdep_reassign_owned::set(|role_ids, newrole| {
        shdepReassignOwned(&role_ids, newrole)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_list_diff_removes_common_keeps_distinct() {
        let mut l1 = vec![1u32, 2, 3, 5];
        let mut n1 = 4usize;
        let mut l2 = vec![2u32, 4, 5, 6];
        let mut n2 = 4usize;
        getOidListDiff(&mut l1, &mut n1, &mut l2, &mut n2);
        assert_eq!(n1, 2);
        assert_eq!(&l1[..n1], &[1, 3]);
        assert_eq!(n2, 2);
        assert_eq!(&l2[..n2], &[4, 6]);
    }

    #[test]
    fn oid_list_diff_identical_lists_empty() {
        let mut l1 = vec![10u32, 20, 30];
        let mut n1 = 3usize;
        let mut l2 = vec![10u32, 20, 30];
        let mut n2 = 3usize;
        getOidListDiff(&mut l1, &mut n1, &mut l2, &mut n2);
        assert_eq!(n1, 0);
        assert_eq!(n2, 0);
    }

    fn info(class_id: Oid, object_id: Oid, sub_id: i32, deptype: i8) -> ShDependObjectInfo {
        ShDependObjectInfo {
            object: ObjectAddress {
                classId: class_id,
                objectId: object_id,
                objectSubId: sub_id,
            },
            deptype,
            objtype: LOCAL_OBJECT,
        }
    }

    #[test]
    fn comparator_orders_by_oid_first() {
        let a = info(100, 5, 0, b'o' as i8);
        let b = info(100, 9, 0, b'o' as i8);
        assert_eq!(
            shared_dependency_comparator(&a, &b),
            core::cmp::Ordering::Less
        );
    }

    #[test]
    fn comparator_subid_unsigned_zero_first() {
        let whole = info(50, 7, 0, b'o' as i8);
        let neg = info(50, 7, -1, b'o' as i8);
        assert_eq!(
            shared_dependency_comparator(&whole, &neg),
            core::cmp::Ordering::Less
        );
    }

    #[test]
    fn ngettext_singular_vs_plural() {
        assert_eq!(
            ngettext_format(
                "\nand %d other object (see server log for list)",
                "\nand %d other objects (see server log for list)",
                1
            ),
            "\nand 1 other object (see server log for list)"
        );
        assert_eq!(
            ngettext_format(
                "\nand %d other object (see server log for list)",
                "\nand %d other objects (see server log for list)",
                3
            ),
            "\nand 3 other objects (see server log for list)"
        );
    }

    #[test]
    fn ngettext_with_string_substitution() {
        assert_eq!(
            ngettext_format_s("%d object in %s", "%d objects in %s", 1, "database mydb"),
            "1 object in database mydb"
        );
        assert_eq!(
            ngettext_format_s("%d object in %s", "%d objects in %s", 5, "database mydb"),
            "5 objects in database mydb"
        );
    }

    #[test]
    fn shared_dependency_type_chars() {
        assert_eq!(SHARED_DEPENDENCY_OWNER.as_char(), b'o' as i8);
        assert_eq!(SHARED_DEPENDENCY_ACL.as_char(), b'a' as i8);
        assert_eq!(SHARED_DEPENDENCY_INITACL.as_char(), b'i' as i8);
        assert_eq!(SHARED_DEPENDENCY_POLICY.as_char(), b'r' as i8);
        assert_eq!(SHARED_DEPENDENCY_TABLESPACE.as_char(), b't' as i8);
        assert_eq!(SHARED_DEPENDENCY_INVALID.as_char(), 0);
    }

    #[test]
    fn catalog_oids_match_postgres() {
        assert_eq!(SHARED_DEPEND_RELATION_ID, 1214);
        assert_eq!(SharedDependDependerIndexId, 1232);
        assert_eq!(SharedDependReferenceIndexId, 1233);
        assert_eq!(AUTH_ID_RELATION_ID, 1260);
        assert_eq!(AUTH_MEM_RELATION_ID, 1261);
        assert_eq!(TABLE_SPACE_RELATION_ID, 1213);
        assert_eq!(DATABASE_RELATION_ID, 1262);
        assert_eq!(DEFAULTTABLESPACE_OID, 1663);
        assert_eq!(Natts_pg_shdepend, 7);
    }
}
