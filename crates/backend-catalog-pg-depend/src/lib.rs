//! `src/backend/catalog/pg_depend.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the pg_depend relation.
//!
//! Signature mapping:
//! * C `long` record counts are `i64`.
//! * C `List *` of OIDs is `PgVec<'mcx, Oid>` allocated in the caller's `mcx`
//!   (the C `NIL` is an empty vec).
//! * `recordMultipleDependencies`' `referenced`/`nreferenced` pair is one
//!   slice (callers pass the filled prefix of the C array).
//! * `sequenceIsOwned`'s `bool` + `*tableId`/`*colId` out-params are
//!   `Option<(Oid, i32)>` (`None` == the C `false`).
//! * `getIdentitySequence`'s open `Relation rel` crosses as `&RelationData`;
//!   the `relispartition` field is read directly off `rd_rel` (the decided
//!   `RelationData` carrier holds the `rd_rel` Form).
//! * The catalog `deptype` byte is the `i8` of
//!   [`FormData_pg_depend::deptype`] / [`DependencyType::as_char`].
//! * `table_open`..`table_close` spans are `OpenRelation` guard scopes: the
//!   explicit `close(lockmode)` is the C `table_close`, and any `?` inside
//!   the span releases through `Drop`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgString, PgVec};
use types_catalog::catalog::{
    CONSTRAINT_RELATION_ID, EXTENSION_RELATION_ID, RELATION_RELATION_ID, RELKIND_SEQUENCE,
    TYPE_RELATION_ID,
};
use types_catalog::catalog_dependency::{
    Anum_pg_depend_classid, Anum_pg_depend_deptype, Anum_pg_depend_objid,
    Anum_pg_depend_objsubid, Anum_pg_depend_refclassid, Anum_pg_depend_refobjid,
    Anum_pg_depend_refobjsubid, DependDependerIndexId, DependReferenceIndexId, DependencyType,
    FormData_pg_depend, Natts_pg_depend, ObjectAddress, DEPENDENCY_AUTO,
    DEPENDENCY_AUTO_EXTENSION, DEPENDENCY_EXTENSION, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
    DEPEND_RELATION_ID,
};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, InvalidAttrNumber, InvalidOid, Oid, OidIsValid};
use types_datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERROR,
};
use types_rel::RelationData;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, RowExclusiveLock, LOCKMODE};
use types_tuple::backend_access_common_heaptuple::TupleValue;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use types_rel::Relation;
use backend_catalog_catalog_seams as catalog_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_objectaddress_seams as objectaddress_seams;
use backend_catalog_partition_seams as partition_seams;
use backend_commands_extension_seams as extension_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;

/// `MAX_CATALOG_MULTI_INSERT_BYTES` (`catalog/indexing.h`).
const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

/// `table_open(DependRelationId, lockmode)` — the guard's `Drop` is the
/// error-path `table_close`; the success path closes explicitly. The opened
/// carrier is copied into `mcx` (callers hold a short-lived local context,
/// mirroring the relcache-owned lifetime of the C entry).
fn open_depend(mcx: Mcx<'_>, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    table::table_open(mcx, DEPEND_RELATION_ID, lockmode)
}

/// `ScanKeyInit(&key[n], attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`. The eager fmgr resolution crosses the fmgr
/// seam (panics until fmgr lands, exactly where C does the lookup).
fn oid_key(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData> {
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

/// `ScanKeyInit(&key[n], attno, BTEqualStrategyNumber, F_INT4EQ,
/// Int32GetDatum(value))`.
fn int4_key(attno: AttrNumber, value: i32) -> PgResult<ScanKeyData> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Datum::from_i32(value),
    )?;
    Ok(key)
}

/// One scanned pg_depend row: the heap TID (`tup->t_self`, for delete/update
/// legs) plus the `heap_deform_tuple` projection of the whole row.
struct SysScanRow<'a> {
    tid: ItemPointerData,
    values: &'a [Datum],
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
        // GETSTRUCT(tup): the whole row, deformed (every pg_depend column is
        // fixed-width and NOT NULL, so by-value).
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum> = vec_with_capacity_in(smcx, cols.len())?;
        let mut isnull: PgVec<'_, bool> = vec_with_capacity_in(smcx, cols.len())?;
        for (value, null) in cols.iter() {
            values.push(match value {
                TupleValue::ByVal(d) => *d,
                TupleValue::ByRef(_) => {
                    return Err(PgError::error("pg_depend column is not by-value"))
                }
            });
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

/// `(Form_pg_depend) GETSTRUCT(tup)` — interpret one deformed pg_depend row.
/// Every pg_depend column is fixed-width and NOT NULL.
fn form_pg_depend(row: &SysScanRow<'_>) -> FormData_pg_depend {
    debug_assert_eq!(row.values.len(), Natts_pg_depend);
    debug_assert!(row.isnull.iter().all(|&null| !null));
    let col = |attno: AttrNumber| row.values[attno as usize - 1];
    FormData_pg_depend {
        classid: col(Anum_pg_depend_classid).as_oid(),
        objid: col(Anum_pg_depend_objid).as_oid(),
        objsubid: col(Anum_pg_depend_objsubid).as_i32(),
        refclassid: col(Anum_pg_depend_refclassid).as_oid(),
        refobjid: col(Anum_pg_depend_refobjid).as_oid(),
        refobjsubid: col(Anum_pg_depend_refobjsubid).as_i32(),
        deptype: col(Anum_pg_depend_deptype).as_char(),
    }
}

/// PostgreSQL's own `snprintf` renders a NULL `%s` argument as `"(null)"`
/// (src/port/snprintf.c); `get_extension_name` can return NULL.
fn name_or_null<'a>(name: &'a Option<PgString<'_>>) -> &'a str {
    match name {
        Some(n) => n.as_str(),
        None => "(null)",
    }
}

/// `getObjectDescription` can return NULL (empty buffer for a vanished
/// object); `snprintf` renders that `%s` as `"(null)"`.
fn desc_or_null<'a>(desc: &'a Option<PgString<'_>>) -> &'a str {
    match desc {
        Some(d) => d.as_str(),
        None => "(null)",
    }
}

/// Record a dependency between 2 objects via their respective ObjectAddress.
/// The first argument is the dependent object, the second the one it
/// references.
///
/// This simply creates an entry in pg_depend, without any other processing.
pub fn recordDependencyOn(
    mcx: Mcx<'_>,
    depender: &ObjectAddress,
    referenced: &ObjectAddress,
    behavior: DependencyType,
) -> PgResult<()> {
    recordMultipleDependencies(mcx, depender, core::slice::from_ref(referenced), behavior)
}

/// Record multiple dependencies (of the same kind) for a single dependent
/// object.  This has a little less overhead than recording each separately.
pub fn recordMultipleDependencies(
    mcx: Mcx<'_>,
    depender: &ObjectAddress,
    referenced: &[ObjectAddress],
    behavior: DependencyType,
) -> PgResult<()> {
    if referenced.is_empty() {
        return Ok(()); /* nothing to do */
    }

    /*
     * During bootstrap, do nothing since pg_depend may not exist yet.
     *
     * Objects created during bootstrap are most likely pinned, and the few
     * that are not do not have dependencies on each other, so that there
     * would be no need to make a pg_depend entry anyway.
     */
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    let dep_ctx = MemoryContext::new("pg_depend");

    let dependDesc = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    /*
     * Allocate the slots to use, but delay costly initialization until we
     * know that they will be used.
     *
     * The C slots hold formed virtual tuples; here the batch accumulates the
     * row values and each flush forms-and-inserts through the indexing seam
     * (which owns CatalogOpenIndexes/CatalogTuplesMultiInsertWithInfo/
     * CatalogCloseIndexes).
     */
    let max_slots = core::cmp::min(
        referenced.len(),
        MAX_CATALOG_MULTI_INSERT_BYTES / core::mem::size_of::<FormData_pg_depend>(),
    );
    let mut slot: PgVec<'_, FormData_pg_depend> = vec_with_capacity_in(mcx, max_slots)?;

    /* number of slots currently storing tuples */
    let mut slot_stored_count = 0usize;
    for referenced in referenced {
        /*
         * If the referenced object is pinned by the system, there's no real
         * need to record dependencies on it.  This saves lots of space in
         * pg_depend, so it's worth the time taken to check.
         */
        if isObjectPinned(referenced) {
            continue;
        }

        /*
         * Record the dependency.  Note we don't bother to check for duplicate
         * dependencies; there's no harm in them.
         */
        slot.push(FormData_pg_depend {
            refclassid: referenced.classId,
            refobjid: referenced.objectId,
            refobjsubid: referenced.objectSubId,
            deptype: behavior.as_char(),
            classid: depender.classId,
            objid: depender.objectId,
            objsubid: depender.objectSubId,
        });
        slot_stored_count += 1;

        /* If slots are full, insert a batch of tuples */
        if slot_stored_count == max_slots {
            indexing_seams::catalog_tuples_multi_insert_pg_depend::call(&dependDesc, &slot)?;
            slot.clear();
            slot_stored_count = 0;
        }
    }

    /* Insert any tuples left in the buffer */
    if slot_stored_count > 0 {
        indexing_seams::catalog_tuples_multi_insert_pg_depend::call(&dependDesc, &slot)?;
    }

    dependDesc.close(RowExclusiveLock)?;

    Ok(())
}

/// If we are executing a CREATE EXTENSION operation, mark the given object as
/// being a member of the extension, or check that it already is one.
/// Otherwise, do nothing.
///
/// This must be called during creation of any user-definable object type that
/// could be a member of an extension.
///
/// `isReplace` must be true if the object already existed, and false if it is
/// newly created.  In the former case we insist that it already be a member
/// of the current extension.  In the latter case we can skip checking whether
/// it is already a member of any extension.
///
/// Note: isReplace = true is typically used when updating an object in CREATE
/// OR REPLACE and similar commands.  We used to allow the target object to
/// not already be an extension member, instead silently absorbing it into the
/// current extension.  However, this was both error-prone (extensions might
/// accidentally overwrite free-standing objects) and a security hazard (since
/// the object would retain its previous ownership).
pub fn recordDependencyOnCurrentExtension(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    isReplace: bool,
) -> PgResult<()> {
    /* Only whole objects can be extension members */
    debug_assert!(object.objectSubId == 0);

    if extension_seams::creating_extension::call() {
        /* Only need to check for existing membership if isReplace */
        if isReplace {
            /*
             * Side note: these catalog lookups are safe only because the
             * object is a pre-existing one.  In the not-isReplace case, the
             * caller has most likely not yet done a CommandCounterIncrement
             * that would make the new object visible.
             */
            let oldext = getExtensionOfObject(object.classId, object.objectId)?;
            if OidIsValid(oldext) {
                /* If already a member of this extension, nothing to do */
                if oldext == extension_seams::current_extension_object::call() {
                    return Ok(());
                }
                /* Already a member of some other extension, so reject */
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "{} is already a member of extension \"{}\"",
                        desc_or_null(&objectaddress_seams::get_object_description::call(mcx, object, false)?),
                        name_or_null(&extension_seams::get_extension_name::call(mcx, oldext)?),
                    ),
                )
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
            }
            /* It's a free-standing object, so reject */
            return Err(PgError::new(
                ERROR,
                format!(
                    "{} is not a member of extension \"{}\"",
                    desc_or_null(&objectaddress_seams::get_object_description::call(mcx, object, false)?),
                    name_or_null(&extension_seams::get_extension_name::call(
                        mcx,
                        extension_seams::current_extension_object::call(),
                    )?),
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail(
                "An extension is not allowed to replace an object that it does not own.",
            ));
        }

        /* OK, record it as a member of CurrentExtensionObject */
        let extension = ObjectAddress {
            classId: EXTENSION_RELATION_ID,
            objectId: extension_seams::current_extension_object::call(),
            objectSubId: 0,
        };

        recordDependencyOn(mcx, object, &extension, DEPENDENCY_EXTENSION)?;
    }

    Ok(())
}

/// If we are executing a CREATE EXTENSION operation, check that the given
/// object is a member of the extension, and throw an error if it isn't.
/// Otherwise, do nothing.
///
/// This must be called whenever a CREATE IF NOT EXISTS operation (for an
/// object type that can be an extension member) has found that an object of
/// the desired name already exists.  It is insecure for an extension to use
/// IF NOT EXISTS except when the conflicting object is already an extension
/// member; otherwise a hostile user could substitute an object with arbitrary
/// properties.
pub fn checkMembershipInCurrentExtension(mcx: Mcx<'_>, object: &ObjectAddress) -> PgResult<()> {
    /*
     * This is actually the same condition tested in
     * recordDependencyOnCurrentExtension; but we want to issue a
     * differently-worded error, and anyway it would be pretty confusing to
     * call recordDependencyOnCurrentExtension in these circumstances.
     */

    /* Only whole objects can be extension members */
    debug_assert!(object.objectSubId == 0);

    if extension_seams::creating_extension::call() {
        let oldext = getExtensionOfObject(object.classId, object.objectId)?;
        /* If already a member of this extension, OK */
        if oldext == extension_seams::current_extension_object::call() {
            return Ok(());
        }
        /* Else complain */
        return Err(PgError::new(
            ERROR,
            format!(
                "{} is not a member of extension \"{}\"",
                desc_or_null(&objectaddress_seams::get_object_description::call(mcx, object, false)?),
                name_or_null(&extension_seams::get_extension_name::call(
                    mcx,
                    extension_seams::current_extension_object::call(),
                )?),
            ),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_detail(
            "An extension may only use CREATE ... IF NOT EXISTS to skip object creation if the conflicting object is one that it already owns.",
        ));
    }

    Ok(())
}

/// `deleteDependencyRecordsFor` -- delete all records with given depender
/// classId/objectId.  Returns the number of records deleted.
///
/// This is used when redefining an existing object.  Links leading to the
/// object do not change, and links leading from it will be recreated
/// (possibly with some differences from before).
///
/// If `skipExtensionDeps` is true, we do not delete any dependencies that
/// show that the given object is a member of an extension.  This avoids
/// needing a lot of extra logic to fetch and recreate that dependency.
pub fn deleteDependencyRecordsFor(
    classId: Oid,
    objectId: Oid,
    skipExtensionDeps: bool,
) -> PgResult<i64> {
    let mut count: i64 = 0;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        if skipExtensionDeps && form_pg_depend(row).deptype == DEPENDENCY_EXTENSION.as_char() {
            return Ok(true);
        }

        indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
        count += 1;
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// `deleteDependencyRecordsForClass` -- delete all records with given
/// depender classId/objectId, dependee classId, and deptype.  Returns the
/// number of records deleted.
///
/// This is a variant of deleteDependencyRecordsFor, useful when revoking an
/// object property that is expressed by a dependency record (such as
/// extension membership).
pub fn deleteDependencyRecordsForClass(
    classId: Oid,
    objectId: Oid,
    refclassId: Oid,
    deptype: i8,
) -> PgResult<i64> {
    let mut count: i64 = 0;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == refclassId && depform.deptype == deptype {
            indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
            count += 1;
        }
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// `deleteDependencyRecordsForSpecific` -- delete all records with given
/// depender classId/objectId, dependee classId/objectId, of the given
/// deptype.  Returns the number of records deleted.
pub fn deleteDependencyRecordsForSpecific(
    classId: Oid,
    objectId: Oid,
    deptype: i8,
    refclassId: Oid,
    refobjectId: Oid,
) -> PgResult<i64> {
    let mut count: i64 = 0;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == refclassId
            && depform.refobjid == refobjectId
            && depform.deptype == deptype
        {
            indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
            count += 1;
        }
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// Adjust dependency record(s) to point to a different object of the same
/// type.
///
/// `classId`/`objectId` specify the referencing object.
/// `refClassId`/`oldRefObjectId` specify the old referenced object.
/// `newRefObjectId` is the new referenced object (must be of class
/// `refClassId`).
///
/// Note the lack of objsubid parameters.  If there are subobject references
/// they will all be readjusted.  Also, there is an expectation that we are
/// dealing with NORMAL dependencies: if we have to replace an (implicit)
/// dependency on a pinned object with an explicit dependency on an unpinned
/// one, the new one will be NORMAL.
///
/// Returns the number of records updated -- zero indicates a problem.
pub fn changeDependencyFor(
    mcx: Mcx<'_>,
    classId: Oid,
    objectId: Oid,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> PgResult<i64> {
    let mut count: i64 = 0;

    /*
     * Check to see if either oldRefObjectId or newRefObjectId is pinned.
     * Pinned objects should not have any dependency entries pointing to them,
     * so in these cases we should add or remove a pg_depend entry, or do
     * nothing at all, rather than update an entry as in the normal case.
     */
    let mut objAddr = ObjectAddress {
        classId: refClassId,
        objectId: oldRefObjectId,
        objectSubId: 0,
    };

    let oldIsPinned = isObjectPinned(&objAddr);

    objAddr.objectId = newRefObjectId;

    let newIsPinned = isObjectPinned(&objAddr);

    if oldIsPinned {
        /*
         * If both are pinned, we need do nothing.  However, return 1 not 0,
         * else callers will think this is an error case.
         */
        if newIsPinned {
            return Ok(1);
        }

        /*
         * There is no old dependency record, but we should insert a new one.
         * Assume a normal dependency is wanted.
         */
        let depAddr = ObjectAddress {
            classId,
            objectId,
            objectSubId: 0,
        };
        recordDependencyOn(mcx, &depAddr, &objAddr, DEPENDENCY_NORMAL)?;

        return Ok(1);
    }

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    /* There should be existing dependency record(s), so search. */
    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == refClassId && depform.refobjid == oldRefObjectId {
            if newIsPinned {
                indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
            } else {
                /* make a modifiable copy */
                let mut newform = depform;

                newform.refobjid = newRefObjectId;

                indexing_seams::catalog_tuple_update_pg_depend::call(&depRel, row.tid, &newform)?;
            }

            count += 1;
        }
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// Adjust all dependency records to come from a different object of the same
/// type.
///
/// `classId`/`oldObjectId` specify the old referencing object.
/// `newObjectId` is the new referencing object (must be of class `classId`).
///
/// Returns the number of records updated.
pub fn changeDependenciesOf(classId: Oid, oldObjectId: Oid, newObjectId: Oid) -> PgResult<i64> {
    let mut count: i64 = 0;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, oldObjectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        /* make a modifiable copy */
        let mut newform = form_pg_depend(row);

        newform.objid = newObjectId;

        indexing_seams::catalog_tuple_update_pg_depend::call(&depRel, row.tid, &newform)?;

        count += 1;
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// Adjust all dependency records to point to a different object of the same
/// type.
///
/// `refClassId`/`oldRefObjectId` specify the old referenced object.
/// `newRefObjectId` is the new referenced object (must be of class
/// `refClassId`).
///
/// Returns the number of records updated.
pub fn changeDependenciesOn(
    mcx: Mcx<'_>,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> PgResult<i64> {
    let mut count: i64 = 0;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), RowExclusiveLock)?;

    /*
     * If oldRefObjectId is pinned, there won't be any dependency entries on
     * it --- we can't cope in that case.  (This isn't really worth expending
     * code to fix, in current usage; it just means you can't rename stuff out
     * of pg_catalog, which would likely be a bad move anyway.)
     */
    let mut objAddr = ObjectAddress {
        classId: refClassId,
        objectId: oldRefObjectId,
        objectSubId: 0,
    };

    if isObjectPinned(&objAddr) {
        return Err(PgError::new(
            ERROR,
            format!(
                "cannot remove dependency on {} because it is a system object",
                desc_or_null(&objectaddress_seams::get_object_description::call(mcx, &objAddr, false)?)
            ),
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    /*
     * We can handle adding a dependency on something pinned, though, since
     * that just means deleting the dependency entry.
     */
    objAddr.objectId = newRefObjectId;

    let newIsPinned = isObjectPinned(&objAddr);

    /* Now search for dependency records */
    let key = [
        oid_key(Anum_pg_depend_refclassid, refClassId)?,
        oid_key(Anum_pg_depend_refobjid, oldRefObjectId)?,
    ];

    systable_scan_foreach(&depRel, DependReferenceIndexId, &key, |row| {
        if newIsPinned {
            indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
        } else {
            /* make a modifiable copy */
            let mut newform = form_pg_depend(row);

            newform.refobjid = newRefObjectId;

            indexing_seams::catalog_tuple_update_pg_depend::call(&depRel, row.tid, &newform)?;
        }

        count += 1;
        Ok(true)
    })?;

    depRel.close(RowExclusiveLock)?;

    Ok(count)
}

/// Test if an object is required for basic database functionality.
///
/// The passed subId, if any, is ignored; we assume that only whole objects
/// are pinned (and that this implies pinning their components).
fn isObjectPinned(object: &ObjectAddress) -> bool {
    catalog_seams::is_pinned_object::call(object.classId, object.objectId)
}

/*
 * Various special-purpose lookups and manipulations of pg_depend.
 */

/// Find the extension containing the specified object, if any.
///
/// Returns the OID of the extension, or `InvalidOid` if the object does not
/// belong to any extension.
///
/// Extension membership is marked by an EXTENSION dependency from the object
/// to the extension.  Note that the result will be indeterminate if pg_depend
/// contains links from this object to more than one extension ... but that
/// should never happen.
pub fn getExtensionOfObject(classId: Oid, objectId: Oid) -> PgResult<Oid> {
    let mut result = InvalidOid;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == EXTENSION_RELATION_ID
            && depform.deptype == DEPENDENCY_EXTENSION.as_char()
        {
            result = depform.refobjid;
            return Ok(false); /* no need to keep scanning */
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(result)
}

/// Return (possibly empty) list of extensions that the given object depends
/// on in `DEPENDENCY_AUTO_EXTENSION` mode.
pub fn getAutoExtensionsOfObject<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, classId)?,
        oid_key(Anum_pg_depend_objid, objectId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == EXTENSION_RELATION_ID
            && depform.deptype == DEPENDENCY_AUTO_EXTENSION.as_char()
        {
            /* lappend_oid */
            result
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
            result.push(depform.refobjid);
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(result)
}

/// Look up a type belonging to an extension.
///
/// Returns the type's OID, or `InvalidOid` if not found.
///
/// Notice that the type is specified by name only, without a schema.  That's
/// because this will typically be used by relocatable extensions which can't
/// make a-priori assumptions about which schema their objects are in.  As
/// long as the extension only defines one type of this name, the answer is
/// unique anyway.
///
/// We might later add the ability to look up functions, operators, etc.
pub fn getExtensionType(mcx: Mcx<'_>, extensionOid: Oid, typname: &str) -> PgResult<Oid> {
    let mut result = InvalidOid;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_refclassid, EXTENSION_RELATION_ID)?,
        oid_key(Anum_pg_depend_refobjid, extensionOid)?,
        int4_key(Anum_pg_depend_refobjsubid, 0)?,
    ];

    systable_scan_foreach(&depRel, DependReferenceIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.classid == TYPE_RELATION_ID && depform.deptype == DEPENDENCY_EXTENSION.as_char()
        {
            let typoid = depform.objid;

            let Some(found_name) = syscache_seams::search_type_name::call(mcx, typoid)? else {
                return Ok(true); /* should we throw an error? */
            };
            if found_name.as_str() == typname {
                result = typoid;
                return Ok(false); /* no need to keep searching */
            }
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(result)
}

/// Detect whether a sequence is marked as "owned" by a column.
///
/// An ownership marker is an AUTO or INTERNAL dependency from the sequence to
/// the column.  If we find one, return `Some((tableId, colId))` identifying
/// the owning column; else return `None`.
///
/// Note: if there's more than one such pg_depend entry then you get a random
/// one of them returned.  This should not happen, though.
pub fn sequenceIsOwned(seqId: Oid, deptype: i8) -> PgResult<Option<(Oid, i32)>> {
    let mut ret: Option<(Oid, i32)> = None;

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, RELATION_RELATION_ID)?,
        oid_key(Anum_pg_depend_objid, seqId)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let depform = form_pg_depend(row);

        if depform.refclassid == RELATION_RELATION_ID && depform.deptype == deptype {
            ret = Some((depform.refobjid, depform.refobjsubid));
            return Ok(false); /* no need to keep scanning */
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(ret)
}

/// Collect a list of OIDs of all sequences owned by the specified relation,
/// and column if specified.  If `deptype` is not zero, then only find
/// sequences with the specified dependency type.
fn getOwnedSequences_internal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    deptype: i8,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);

    let dep_ctx = MemoryContext::new("pg_depend");

    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_refclassid, RELATION_RELATION_ID)?,
        oid_key(Anum_pg_depend_refobjid, relid)?,
        int4_key(Anum_pg_depend_refobjsubid, attnum as i32)?,
    ];
    /* the C nkeys is `attnum ? 3 : 2` */
    let key = if attnum != 0 { &key[..] } else { &key[..2] };

    systable_scan_foreach(&depRel, DependReferenceIndexId, key, |row| {
        let deprec = form_pg_depend(row);

        /*
         * We assume any auto or internal dependency of a sequence on a column
         * must be what we are looking for.  (We need the relkind test because
         * indexes can also have auto dependencies on columns.)
         */
        if deprec.classid == RELATION_RELATION_ID
            && deprec.objsubid == 0
            && deprec.refobjsubid != 0
            && (deprec.deptype == DEPENDENCY_AUTO.as_char()
                || deprec.deptype == DEPENDENCY_INTERNAL.as_char())
            && lsyscache_seams::get_rel_relkind::call(deprec.objid)? == RELKIND_SEQUENCE
        {
            if deptype == 0 || deprec.deptype == deptype {
                result
                    .try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
                result.push(deprec.objid);
            }
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(result)
}

/// Collect a list of OIDs of all sequences owned (identity or serial) by the
/// specified relation.
pub fn getOwnedSequences<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    getOwnedSequences_internal(mcx, relid, 0, 0)
}

/// Get owned identity sequence, error if not exactly one.
///
/// `rel` is the caller's open relation; the `relispartition` field is read
/// directly off `rd_rel`.
pub fn getIdentitySequence(
    mcx: Mcx<'_>,
    rel: &RelationData<'_>,
    mut attnum: AttrNumber,
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut relid = rel.rd_id; /* RelationGetRelid(rel) */

    /*
     * The identity sequence is associated with the topmost partitioned table,
     * which might have column order different than the given partition.
     */
    if rel.rd_rel.relispartition {
        let ancestors = partition_seams::get_partition_ancestors::call(mcx, relid)?;
        /*
         * The C get_attname elogs ERROR before it can return NULL when
         * missing_ok is false (lsyscache.c), so Ok(None) is
         * contract-impossible on this call; the expect is the Assert
         * analogue of that seam contract.
         */
        let attname = lsyscache_seams::get_attname::call(mcx, relid, attnum, false)?
            .expect("get_attname(missing_ok = false) returned no name");

        /* llast_oid(ancestors) — C llast asserts the list is non-empty */
        relid = *ancestors
            .last()
            .expect("get_partition_ancestors returned an empty list for a partition");
        attnum = lsyscache_seams::get_attnum::call(relid, attname.as_str())?;
        if attnum == InvalidAttrNumber {
            return Err(PgError::error(format!(
                "cache lookup failed for attribute \"{}\" of relation {}",
                attname.as_str(),
                relid
            )));
        }
        /* list_free(ancestors) — owned vec, dropped here */
    }

    let seqlist = getOwnedSequences_internal(mcx, relid, attnum, DEPENDENCY_INTERNAL.as_char())?;
    if seqlist.len() > 1 {
        return Err(PgError::error("more than one owned sequence found"));
    } else if seqlist.is_empty() {
        if missing_ok {
            return Ok(InvalidOid);
        } else {
            return Err(PgError::error("no owned sequence found"));
        }
    }

    Ok(seqlist[0]) /* linitial_oid(seqlist) */
}

/// `get_index_constraint`
///
/// Given the OID of an index, return the OID of the owning unique,
/// primary-key, or exclusion constraint, or `InvalidOid` if there is no
/// owning constraint.
pub fn get_index_constraint(indexId: Oid) -> PgResult<Oid> {
    let mut constraintId = InvalidOid;

    /* Search the dependency table for the index */
    let dep_ctx = MemoryContext::new("pg_depend");
    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_classid, RELATION_RELATION_ID)?,
        oid_key(Anum_pg_depend_objid, indexId)?,
        int4_key(Anum_pg_depend_objsubid, 0)?,
    ];

    systable_scan_foreach(&depRel, DependDependerIndexId, &key, |row| {
        let deprec = form_pg_depend(row);

        /*
         * We assume any internal dependency on a constraint must be what we
         * are looking for.
         */
        if deprec.refclassid == CONSTRAINT_RELATION_ID
            && deprec.refobjsubid == 0
            && deprec.deptype == DEPENDENCY_INTERNAL.as_char()
        {
            constraintId = deprec.refobjid;
            return Ok(false);
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(constraintId)
}

/// `get_index_ref_constraints`
///
/// Given the OID of an index, return the OID of all foreign key constraints
/// which reference the index.
pub fn get_index_ref_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    indexId: Oid,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);

    /* Search the dependency table for the index */
    let dep_ctx = MemoryContext::new("pg_depend");
    let depRel = open_depend(dep_ctx.mcx(), AccessShareLock)?;

    let key = [
        oid_key(Anum_pg_depend_refclassid, RELATION_RELATION_ID)?,
        oid_key(Anum_pg_depend_refobjid, indexId)?,
        int4_key(Anum_pg_depend_refobjsubid, 0)?,
    ];

    systable_scan_foreach(&depRel, DependReferenceIndexId, &key, |row| {
        let deprec = form_pg_depend(row);

        /*
         * We assume any normal dependency from a constraint must be what we
         * are looking for.
         */
        if deprec.classid == CONSTRAINT_RELATION_ID
            && deprec.objsubid == 0
            && deprec.deptype == DEPENDENCY_NORMAL.as_char()
        {
            result
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
            result.push(deprec.objid);
        }
        Ok(true)
    })?;

    depRel.close(AccessShareLock)?;

    Ok(result)
}

/// Install this crate's implementations into `backend-catalog-pg-depend-seams`.
pub fn init_seams() {
    use backend_catalog_pg_depend_seams as seams;

    seams::recordDependencyOn::set(recordDependencyOn);
    seams::recordMultipleDependencies::set(recordMultipleDependencies);
    seams::recordDependencyOnCurrentExtension::set(recordDependencyOnCurrentExtension);
    seams::checkMembershipInCurrentExtension::set(checkMembershipInCurrentExtension);
    seams::deleteDependencyRecordsFor::set(deleteDependencyRecordsFor);
    seams::deleteDependencyRecordsForClass::set(deleteDependencyRecordsForClass);
    seams::deleteDependencyRecordsForSpecific::set(deleteDependencyRecordsForSpecific);
    seams::changeDependencyFor::set(changeDependencyFor);
    seams::changeDependenciesOf::set(changeDependenciesOf);
    seams::changeDependenciesOn::set(changeDependenciesOn);
    seams::getExtensionOfObject::set(getExtensionOfObject);
    seams::getAutoExtensionsOfObject::set(getAutoExtensionsOfObject);
    seams::getExtensionType::set(getExtensionType);
    seams::sequenceIsOwned::set(sequenceIsOwned);
    seams::getOwnedSequences::set(getOwnedSequences);
    seams::getIdentitySequence::set(getIdentitySequence);
    seams::get_index_constraint::set(get_index_constraint);
    seams::get_index_ref_constraints::set(get_index_ref_constraints);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deptype_bytes_match_postgres() {
        /* pg_depend stores DependencyType as the ASCII code in a `char` column */
        assert_eq!(DEPENDENCY_NORMAL.as_char(), b'n' as i8);
        assert_eq!(DEPENDENCY_AUTO.as_char(), b'a' as i8);
        assert_eq!(DEPENDENCY_INTERNAL.as_char(), b'i' as i8);
        assert_eq!(DEPENDENCY_EXTENSION.as_char(), b'e' as i8);
        assert_eq!(DEPENDENCY_AUTO_EXTENSION.as_char(), b'x' as i8);
    }

    #[test]
    fn catalog_oids_match_postgres() {
        /* genbki-assigned catalog OIDs the scan-key construction relies on */
        assert_eq!(DEPEND_RELATION_ID, 2608);
        assert_eq!(DependDependerIndexId, 2673);
        assert_eq!(DependReferenceIndexId, 2674);
        assert_eq!(RELATION_RELATION_ID, 1259);
        assert_eq!(TYPE_RELATION_ID, 1247);
        assert_eq!(CONSTRAINT_RELATION_ID, 2606);
        assert_eq!(EXTENSION_RELATION_ID, 3079);
        assert_eq!(RELKIND_SEQUENCE, b'S');
        /* RowExclusiveLock for mutators, AccessShareLock for readers */
        assert_eq!(AccessShareLock, 1);
        assert_eq!(RowExclusiveLock, 3);
    }

    #[test]
    fn scan_key_vocabulary_matches_postgres() {
        /* ScanKeyInit arguments (stratnum.h, pg_proc.dat) */
        assert_eq!(BTEqualStrategyNumber, 3);
        assert_eq!(F_INT4EQ, 65);
        assert_eq!(F_OIDEQ, 184);
        /* pg_depend attribute numbers follow the CATALOG field order */
        assert_eq!(Anum_pg_depend_classid, 1);
        assert_eq!(Anum_pg_depend_objid, 2);
        assert_eq!(Anum_pg_depend_objsubid, 3);
        assert_eq!(Anum_pg_depend_refclassid, 4);
        assert_eq!(Anum_pg_depend_refobjid, 5);
        assert_eq!(Anum_pg_depend_refobjsubid, 6);
        assert_eq!(Anum_pg_depend_deptype, 7);
        assert_eq!(Natts_pg_depend, 7);
    }
}
