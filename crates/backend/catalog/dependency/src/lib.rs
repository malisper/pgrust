#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
#![allow(clippy::collapsible_if)]

//! `backend/catalog/dependency.c` — the inter-object dependency recorder and
//! cascaded-drop engine.
//!
//! Faithful 1:1 port: every C function (public and file-static) is present with
//! its original name, branch order, scan order, `DEPFLAG_*` accumulation, error
//! codes/messages/SQLSTATE, lock levels, and dependency-recording order:
//!
//!  * deletion driver — `performDeletion`, `performMultipleDeletions`,
//!    `deleteObjectsInList`, `findDependentObjects` (+ its `ObjectAddressStack`
//!    cycle detection), `reportDependentObjects`, `deleteOneObject`,
//!    `doDeletion` (the per-class dispatch), `DropObjectById`, `DeleteInitPrivs`;
//!  * lock helpers `AcquireDeletionLock` / `ReleaseDeletionLock`;
//!  * the `ObjectAddresses` collection API — `new_object_addresses`,
//!    `add_object_address`, `add_exact_object_address`,
//!    `add_exact_object_address_extra`, `object_address_present`,
//!    `object_address_present_add_flags`, `stack_address_present_add_flags`,
//!    `record_object_address_dependencies`, `sort_object_addresses`,
//!    `eliminate_duplicate_dependencies`, `object_address_comparator`;
//!  * the expression-dependency recorders `recordDependencyOnExpr` /
//!    `recordDependencyOnSingleRelExpr` and the `find_expr_references_walker`
//!    engine (see [`find_expr`]).
//!
//! ## pg_depend scans
//!
//! Where dependency.c keeps an open `Relation depRel` and runs `systable`
//! scans on it, this port opens `pg_depend` with `table_open` (an owned
//! [`Relation`] guard) and scans via the genam seams + `heap_deform_tuple`,
//! exactly as the sibling `backend-catalog-pg-depend` port does. `Mcx<'mcx>`
//! threads the deformation/allocation.
//!
//! ## Inward seams — the `ObjectAddresses` collection
//!
//! Cross-crate callers (pg_constraint/pg_cast/pg_range/…) pass the owned
//! [`ObjectAddresses`] value directly across the collection seams, mirroring
//! C's `ObjectAddresses *` pointer thread. Within this crate the engine uses
//! owned `ObjectAddresses` values too.

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};

use types_catalog::catalog::{
    ACCESS_METHOD_OPERATOR_RELATION_ID as AccessMethodOperatorRelationId,
    ACCESS_METHOD_PROCEDURE_RELATION_ID as AccessMethodProcedureRelationId,
    ACCESS_METHOD_RELATION_ID as AccessMethodRelationId,
    ATTR_DEFAULT_RELATION_ID as AttrDefaultRelationId, AUTH_ID_RELATION_ID as AuthIdRelationId,
    AUTH_MEM_RELATION_ID as AuthMemRelationId, CAST_RELATION_ID as CastRelationId,
    COLLATION_RELATION_ID as CollationRelationId, CONSTRAINT_RELATION_ID as ConstraintRelationId,
    CONVERSION_RELATION_ID as ConversionRelationId, DATABASE_RELATION_ID as DatabaseRelationId,
    DEFAULT_ACL_RELATION_ID as DefaultAclRelationId,
    EVENT_TRIGGER_RELATION_ID as EventTriggerRelationId,
    EXTENSION_RELATION_ID as ExtensionRelationId,
    FOREIGN_DATA_WRAPPER_RELATION_ID as ForeignDataWrapperRelationId,
    FOREIGN_SERVER_RELATION_ID as ForeignServerRelationId, INIT_PRIVS_OBJ_INDEX_ID,
    INIT_PRIVS_RELATION_ID, LANGUAGE_RELATION_ID as LanguageRelationId,
    LARGE_OBJECT_RELATION_ID as LargeObjectRelationId, NAMESPACE_RELATION_ID as NamespaceRelationId,
    OPERATOR_CLASS_RELATION_ID as OperatorClassRelationId,
    OPERATOR_FAMILY_RELATION_ID as OperatorFamilyRelationId,
    OPERATOR_RELATION_ID as OperatorRelationId,
    PARAMETER_ACL_RELATION_ID as ParameterAclRelationId, POLICY_RELATION_ID as PolicyRelationId,
    PROCEDURE_RELATION_ID as ProcedureRelationId,
    PUBLICATION_NAMESPACE_RELATION_ID as PublicationNamespaceRelationId,
    PUBLICATION_RELATION_ID as PublicationRelationId,
    PUBLICATION_REL_RELATION_ID as PublicationRelRelationId,
    RELATION_RELATION_ID as RelationRelationId, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
    RELKIND_SEQUENCE, REWRITE_RELATION_ID as RewriteRelationId,
    STATISTIC_EXT_RELATION_ID as StatisticExtRelationId,
    SUBSCRIPTION_RELATION_ID as SubscriptionRelationId,
    TABLESPACE_RELATION_ID as TableSpaceRelationId, TRANSFORM_RELATION_ID as TransformRelationId,
    TRIGGER_RELATION_ID as TriggerRelationId, TS_CONFIG_RELATION_ID as TSConfigRelationId,
    TS_DICTIONARY_RELATION_ID as TSDictionaryRelationId,
    TS_PARSER_RELATION_ID as TSParserRelationId, TS_TEMPLATE_RELATION_ID as TSTemplateRelationId,
    TYPE_RELATION_ID as TypeRelationId, USER_MAPPING_RELATION_ID as UserMappingRelationId,
    Anum_pg_init_privs_classoid, Anum_pg_init_privs_objoid, Anum_pg_init_privs_objsubid,
};
use types_catalog::catalog_dependency::{
    Anum_pg_depend_classid, Anum_pg_depend_deptype, Anum_pg_depend_objid, Anum_pg_depend_objsubid,
    Anum_pg_depend_refclassid, Anum_pg_depend_refobjid, Anum_pg_depend_refobjsubid,
    DependDependerIndexId, DependReferenceIndexId, DependencyType, FormData_pg_depend,
    Natts_pg_depend, ObjectAddress, ObjectAddressExtra, ObjectAddresses, DEPENDENCY_AUTO,
    DEPENDENCY_AUTO_EXTENSION, DEPENDENCY_EXTENSION, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
    DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC, DEPFLAG_AUTO, DEPFLAG_EXTENSION,
    DEPFLAG_INTERNAL, DEPFLAG_IS_PART, DEPFLAG_NORMAL, DEPFLAG_ORIGINAL, DEPFLAG_PARTITION,
    DEPFLAG_REVERSE, DEPFLAG_SUBOBJECT, DEPEND_RELATION_ID,
};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, DEBUG2, ERROR, NOTICE,
};
use nodes::parsenodes::{DropBehavior, DROP_CASCADE, DROP_RESTRICT};
use rel::{Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{
    AccessExclusiveLock, RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use heaptuple::heap_deform_tuple;
use scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use utils_error::{ereport, elog};
use types_error::ErrorLocation;

// Outward owner seams.
use transam_xact_seams as xact_seams;
use catalog_seams as catalog_seams;
use heap_seams as heap_seams;
use index_seams as index_seams;
use indexing_seams as indexing_seams;
use objectaccess_seams as objectaccess_seams;
use objectaddress_seams as objectaddress_seams;
use pg_attrdef_seams as attrdef_seams;
use pg_constraint_seams as constraint_seams;
use pg_depend_seams as pg_depend_seams;
use pg_largeobject_seams as largeobject_seams;
use pg_operator_seams as operator_seams;
use pg_shdepend_seams as shdepend_seams;
use comment_seams as comment_seams;
use event_trigger_seams as event_trigger_seams;
use extension_seams as extension_seams;
use functioncmds_seams as functioncmds_seams;
use policy_seams as policy_seams;
use publicationcmds_seams as publicationcmds_seams;
use seclabel_seams as seclabel_seams;
use sequence_seams_2 as sequence_seams;
use statscmds_seams as statscmds_seams;
use trigger_seams as trigger_seams;
use tsearchcmds_seams as tsearchcmds_seams;
use typecmds_seams as typecmds_seams;
use lmgr_seams as lmgr_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use stack_depth_seams as stack_depth_seams;

pub mod find_expr;
pub use find_expr::{find_expr_references_walker, FindExprReferencesContext};

mod seams;

/* ===========================================================================
 * Tiny helpers mirroring postgres macros.
 * ========================================================================= */

/// `OidIsValid(objectId)` (`postgres_ext.h`).
#[inline]
pub(crate) fn OidIsValid(object_id: Oid) -> bool {
    object_id != InvalidOid
}

/// Mirrors C's `__FILE__`/`__LINE__`/`__func__` triple for non-error finishes.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("dependency.c", 0, funcname)
}

/// `getObjectDescription(object, missing_ok)` (`catalog/objectaddress.c`).
/// `None` only when `missing_ok` and the object was concurrently dropped.
fn getObjectDescription(object: &ObjectAddress, missing_ok: bool) -> PgResult<Option<String>> {
    let ctx = MemoryContext::new("getObjectDescription");
    let s = objectaddress_seams::get_object_description::call(ctx.mcx(), object, missing_ok)?;
    Ok(s.map(|p| p.as_str().to_string()))
}

/// `getObjectDescription` for `missing_ok = false` callers that embed the
/// description in a message: it never returns NULL there.
fn getObjectDescription_required(object: &ObjectAddress) -> PgResult<String> {
    Ok(getObjectDescription(object, false)?.unwrap_or_default())
}

/* ===========================================================================
 * pg_depend scan helpers (mirroring backend-catalog-pg-depend's idiom).
 * ========================================================================= */

/// `table_open(DependRelationId, lockmode)` — owned guard; `Drop` is the
/// error-path `table_close`, the success path closes explicitly.
fn open_depend(mcx: Mcx<'_>, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    table::table_open(mcx, DEPEND_RELATION_ID, lockmode)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum)`.
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

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum)`.
fn int4_key<'mcx>(attno: AttrNumber, value: i32) -> PgResult<ScanKeyData<'mcx>> {
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

/// One scanned pg_depend row: the heap TID (`tup->t_self`) + its deformed row.
struct DependRow {
    tid: ItemPointerData,
    form: FormData_pg_depend,
}

/// `(Form_pg_depend) GETSTRUCT(tup)` — every pg_depend column is fixed-width,
/// NOT NULL.
fn form_pg_depend(values: &[Datum<'_>]) -> FormData_pg_depend {
    debug_assert_eq!(values.len(), Natts_pg_depend);
    let col = |attno: AttrNumber| &values[attno as usize - 1];
    FormData_pg_depend {
        classid: col(Anum_pg_depend_classid).as_oid(),
        objid: col(Anum_pg_depend_objid).as_oid(),
        objsubid: col(Anum_pg_depend_objsubid).as_i32(),
        refclassid: col(Anum_pg_depend_refclassid).as_oid(),
        refobjid: col(Anum_pg_depend_refobjid).as_oid(),
        refobjsubid: col(Anum_pg_depend_refobjsubid).as_i32(),
        deptype: col(Anum_pg_depend_deptype).as_i8(),
    }
}

/// Collect all matching pg_depend rows (TID + deformed Form) into an owned Vec.
/// The C `systable_beginscan`/getnext/endscan loop materialised eagerly so the
/// caller can release locks / recheck / break without holding the scan open
/// (matching the C control flow which closes the scan before recursing).
fn scan_depend_rows(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
) -> PgResult<Vec<DependRow>> {
    let mut out: Vec<DependRow> = Vec::new();
    // The scan slot stores each fetched heap tuple (cloned by the AM's
    // getnextslot) in this context, and that tuple lives until the next
    // getnext replaces it or `systable_endscan` drops the slot. The scratch
    // context must therefore outlive the whole scan, not be recreated per row
    // (a per-row context, dropped while the slot still holds its tuple, would
    // leave the slot's tuple uncharged and the next store would double-free).
    // Each `DependRow` we keep is fully owned (fixed-width OIDs), so nothing in
    // `out` references this context after the loop.
    let scratch = MemoryContext::new("scan_depend_rows");
    let smcx = scratch.mcx();
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = vec_with_capacity_in(smcx, cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        out.push(DependRow {
            tid: tup.tuple.t_self,
            form: form_pg_depend(&values),
        });
    }
    scan.end()?;
    Ok(out)
}

/* ===========================================================================
 * ObjectAddressAndFlags — temporary storage in findDependentObjects.
 * ========================================================================= */

#[derive(Clone, Copy)]
struct ObjectAddressAndFlags {
    obj: ObjectAddress,
    subflags: i32,
}

/* ===========================================================================
 * ObjectAddressStack — threaded list for recursion detection.
 *
 * A Rust shared-borrow chain whose lifetime nests with the recursion; `flags`
 * is interior-mutable because `stack_address_present_add_flags` ORs into outer
 * levels.
 * ========================================================================= */

struct ObjectAddressStack<'a> {
    object: ObjectAddress,
    flags: core::cell::Cell<i32>,
    next: Option<&'a ObjectAddressStack<'a>>,
}

/* ===========================================================================
 * deleteObjectsInList (dependency.c:184)
 * ========================================================================= */

fn deleteObjectsInList(targetObjects: &ObjectAddresses, flags: i32) -> PgResult<()> {
    if event_trigger_seams::trackDroppedObjectsNeeded::call()?
        && (flags & seams::PERFORM_DELETION_INTERNAL) == 0
    {
        for i in 0..targetObjects.numrefs {
            let thisobj = &targetObjects.refs[i as usize];
            let extra = &targetObjects.extras[i as usize];
            let mut original = false;
            let mut normal = false;

            if extra.flags & DEPFLAG_ORIGINAL != 0 {
                original = true;
            }
            if extra.flags & DEPFLAG_NORMAL != 0 {
                normal = true;
            }
            if extra.flags & DEPFLAG_REVERSE != 0 {
                normal = true;
            }

            if event_trigger_seams::EventTriggerSupportsObject::call(thisobj)? {
                event_trigger_seams::EventTriggerSQLDropAddObject::call(thisobj, original, normal)?;
            }
        }
    }

    for i in 0..targetObjects.numrefs {
        let thisobj = targetObjects.refs[i as usize];
        let thisextra = targetObjects.extras[i as usize];

        if (flags & seams::PERFORM_DELETION_SKIP_ORIGINAL) != 0
            && (thisextra.flags & DEPFLAG_ORIGINAL) != 0
        {
            continue;
        }

        deleteOneObject(&thisobj, flags)?;
    }

    Ok(())
}

/* ===========================================================================
 * performDeletion (dependency.c:272)
 * ========================================================================= */

/// performDeletion: drop `object`, cascading to (or erroring on) dependents.
pub fn performDeletion(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let depRel = open_depend(mcx, RowExclusiveLock)?;

    AcquireDeletionLock(object, 0)?;

    let mut targetObjects = new_object_addresses();

    findDependentObjects(
        &depRel,
        object,
        DEPFLAG_ORIGINAL,
        flags,
        None,
        &mut targetObjects,
        None,
    )?;

    reportDependentObjects(&targetObjects, behavior, flags, Some(object))?;

    // For a concurrent drop, the object-deletion subroutine commits the current
    // transaction (DROP INDEX CONCURRENTLY), so we must not keep pg_depend open
    // across deleteObjectsInList — that would leak the relcache reference past
    // the commit (dependency.c closes/reopens depRel around doDeletion()). The
    // Rust deleteOneObject opens its own scratch pg_depend after doDeletion, so
    // we simply close ours here and don't reopen.
    if (flags & seams::PERFORM_DELETION_CONCURRENTLY) != 0 {
        depRel.close(RowExclusiveLock)?;
        return deleteObjectsInList(&targetObjects, flags);
    }

    deleteObjectsInList(&targetObjects, flags)?;

    depRel.close(RowExclusiveLock)
}

/* ===========================================================================
 * performMultipleDeletions (dependency.c:331)
 * ========================================================================= */

/// performMultipleDeletions: like performDeletion but for a set of objects.
pub fn performMultipleDeletions(
    mcx: Mcx<'_>,
    objects: &ObjectAddresses,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    if objects.numrefs <= 0 {
        return Ok(());
    }

    let depRel = open_depend(mcx, RowExclusiveLock)?;

    let mut targetObjects = new_object_addresses();

    for i in 0..objects.numrefs {
        let thisobj = objects.refs[i as usize];

        AcquireDeletionLock(&thisobj, flags)?;

        findDependentObjects(
            &depRel,
            &thisobj,
            DEPFLAG_ORIGINAL,
            flags,
            None,
            &mut targetObjects,
            Some(objects),
        )?;
    }

    let orig = if objects.numrefs == 1 {
        Some(&objects.refs[0])
    } else {
        None
    };
    reportDependentObjects(&targetObjects, behavior, flags, orig)?;

    // See performDeletion: a concurrent drop commits the transaction inside
    // doDeletion(), so pg_depend must not stay open across deleteObjectsInList.
    if (flags & seams::PERFORM_DELETION_CONCURRENTLY) != 0 {
        depRel.close(RowExclusiveLock)?;
        return deleteObjectsInList(&targetObjects, flags);
    }

    deleteObjectsInList(&targetObjects, flags)?;

    depRel.close(RowExclusiveLock)
}

/* ===========================================================================
 * findDependentObjects (dependency.c:431)
 * ========================================================================= */

fn findDependentObjects(
    depRel: &RelationData<'_>,
    object: &ObjectAddress,
    mut objflags: i32,
    flags: i32,
    stack: Option<&ObjectAddressStack>,
    targetObjects: &mut ObjectAddresses,
    pendingObjects: Option<&ObjectAddresses>,
) -> PgResult<()> {
    if stack_address_present_add_flags(object, objflags, stack) {
        return Ok(());
    }

    stack_depth_seams::check_stack_depth::call()?;

    if object_address_present_add_flags(object, objflags, targetObjects) {
        return Ok(());
    }

    if catalog_seams::is_pinned_object::call(object.classId, object.objectId) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .errmsg(format!(
                "cannot drop {} because it is required by the database system",
                getObjectDescription_required(object)?
            ))
            .into_error());
    }

    /*
     * Scan pg_depend entries showing what this object depends on, by the
     * depender index. (Materialised, so we can release locks / recheck / break
     * without holding the scan open, matching the C control flow.)
     */
    let keys = [
        oid_key(Anum_pg_depend_classid, object.classId)?,
        oid_key(Anum_pg_depend_objid, object.objectId)?,
        int4_key(Anum_pg_depend_objsubid, object.objectSubId)?,
    ];
    let nkeys = if object.objectSubId != 0 { 3 } else { 2 };
    let scan = scan_depend_rows(depRel, DependDependerIndexId, &keys[..nkeys])?;

    let mut owningObject = ObjectAddress {
        classId: 0,
        objectId: 0,
        objectSubId: 0,
    };
    let mut partitionObject = ObjectAddress {
        classId: 0,
        objectId: 0,
        objectSubId: 0,
    };

    let mut recurse_target: Option<ObjectAddress> = None;

    'depender: for row in scan.iter() {
        let foundDep = &row.form;

        let otherObject = ObjectAddress {
            classId: foundDep.refclassid,
            objectId: foundDep.refobjid,
            objectSubId: foundDep.refobjsubid,
        };

        if otherObject.classId == object.classId
            && otherObject.objectId == object.objectId
            && object.objectSubId == 0
        {
            continue;
        }

        let deptype = DependencyType(foundDep.deptype);
        let mut handle_internal = false;
        if deptype == DEPENDENCY_NORMAL
            || deptype == DEPENDENCY_AUTO
            || deptype == DEPENDENCY_AUTO_EXTENSION
        {
            /* no problem */
        } else if deptype == DEPENDENCY_EXTENSION {
            if flags & seams::PERFORM_DELETION_SKIP_EXTENSIONS != 0 {
                continue;
            }

            if extension_seams::creating_extension::call()
                && otherObject.classId == ExtensionRelationId
                && otherObject.objectId == extension_seams::current_extension_object::call()
            {
                continue;
            }

            /* FALL THRU to internal */
            handle_internal = true;
        } else if deptype == DEPENDENCY_INTERNAL {
            handle_internal = true;
        } else if deptype == DEPENDENCY_PARTITION_PRI {
            objflags |= DEPFLAG_IS_PART;
            partitionObject = otherObject;
        } else if deptype == DEPENDENCY_PARTITION_SEC {
            if objflags & DEPFLAG_IS_PART == 0 {
                partitionObject = otherObject;
            }
            objflags |= DEPFLAG_IS_PART;
        } else {
            return Err(elog_unrecognized_dependency_type(foundDep.deptype, object)?);
        }

        if handle_internal {
            /* 1. Outermost level: disallow the DROP. */
            if stack.is_none() {
                if let Some(pending) = pendingObjects {
                    if object_address_present(&otherObject, pending) {
                        ReleaseDeletionLock(object)?;
                        return Ok(());
                    }
                }

                if !OidIsValid(owningObject.classId) || deptype == DEPENDENCY_EXTENSION {
                    owningObject = otherObject;
                }
                continue;
            }

            /* 2. Recursing from the other end is OK. */
            if stack_address_present_add_flags(&otherObject, 0, stack) {
                continue;
            }

            /* 3. Transform into a delete of the owning object. */
            ReleaseDeletionLock(object)?;
            AcquireDeletionLock(&otherObject, 0)?;

            /* The owner might have been deleted while we waited. */
            if !recheck_pg_depend(depRel, row)? {
                ReleaseDeletionLock(&otherObject)?;
                return Ok(());
            }

            recurse_target = Some(otherObject);
            break 'depender;
        }
    }

    if let Some(otherObject) = recurse_target {
        findDependentObjects(
            depRel,
            &otherObject,
            DEPFLAG_REVERSE,
            flags,
            stack,
            targetObjects,
            pendingObjects,
        )?;

        if !object_address_present_add_flags(object, objflags, targetObjects) {
            return Err(elog(
                ERROR,
                format!(
                    "deletion of owning object {} failed to delete {}",
                    getObjectDescription_required(&otherObject)?,
                    getObjectDescription_required(object)?
                ),
            )
            .unwrap_err());
        }

        return Ok(());
    }

    if OidIsValid(owningObject.classId) {
        let otherObjDesc = if OidIsValid(partitionObject.classId) {
            getObjectDescription_required(&partitionObject)?
        } else {
            getObjectDescription_required(&owningObject)?
        };

        return Err(ereport(ERROR)
            .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .errmsg(format!(
                "cannot drop {} because {} requires it",
                getObjectDescription_required(object)?,
                otherObjDesc
            ))
            .errhint(format!("You can drop {otherObjDesc} instead."))
            .into_error());
    }

    /*
     * Identify all objects that directly depend on the current object, by the
     * reference index.
     */
    let mut dependentObjects: Vec<ObjectAddressAndFlags> = Vec::new();

    let keys = [
        oid_key(Anum_pg_depend_refclassid, object.classId)?,
        oid_key(Anum_pg_depend_refobjid, object.objectId)?,
        int4_key(Anum_pg_depend_refobjsubid, object.objectSubId)?,
    ];
    let nkeys = if object.objectSubId != 0 { 3 } else { 2 };
    let scan = scan_depend_rows(depRel, DependReferenceIndexId, &keys[..nkeys])?;

    for row in scan.iter() {
        let foundDep = &row.form;

        let otherObject = ObjectAddress {
            classId: foundDep.classid,
            objectId: foundDep.objid,
            objectSubId: foundDep.objsubid,
        };

        if otherObject.classId == object.classId
            && otherObject.objectId == object.objectId
            && object.objectSubId == 0
        {
            continue;
        }

        AcquireDeletionLock(&otherObject, 0)?;

        if !recheck_pg_depend(depRel, row)? {
            ReleaseDeletionLock(&otherObject)?;
            continue;
        }

        let deptype = DependencyType(foundDep.deptype);
        let subflags;
        if deptype == DEPENDENCY_NORMAL {
            subflags = DEPFLAG_NORMAL;
        } else if deptype == DEPENDENCY_AUTO || deptype == DEPENDENCY_AUTO_EXTENSION {
            subflags = DEPFLAG_AUTO;
        } else if deptype == DEPENDENCY_INTERNAL {
            subflags = DEPFLAG_INTERNAL;
        } else if deptype == DEPENDENCY_PARTITION_PRI || deptype == DEPENDENCY_PARTITION_SEC {
            subflags = DEPFLAG_PARTITION;
        } else if deptype == DEPENDENCY_EXTENSION {
            subflags = DEPFLAG_EXTENSION;
        } else {
            return Err(elog_unrecognized_dependency_type(foundDep.deptype, object)?);
        }

        dependentObjects.push(ObjectAddressAndFlags {
            obj: otherObject,
            subflags,
        });
    }

    if dependentObjects.len() > 1 {
        dependentObjects.sort_by(|a, b| object_address_comparator(&a.obj, &b.obj));
    }

    let mystack = ObjectAddressStack {
        object: *object,
        flags: core::cell::Cell::new(objflags),
        next: stack,
    };

    for depObj in &dependentObjects {
        findDependentObjects(
            depRel,
            &depObj.obj,
            depObj.subflags,
            flags,
            Some(&mystack),
            targetObjects,
            pendingObjects,
        )?;
    }

    let extra_flags = mystack.flags.get();
    let dependee = if extra_flags & DEPFLAG_IS_PART != 0 {
        partitionObject
    } else if let Some(s) = stack {
        s.object
    } else {
        ObjectAddress {
            classId: 0,
            objectId: 0,
            objectSubId: 0,
        }
    };
    let extra = ObjectAddressExtra {
        flags: extra_flags,
        dependee,
    };
    add_exact_object_address_extra(object, &extra, targetObjects);

    Ok(())
}

/// `systable_recheck_tuple(scan, tup)` — recheck a specific previously-found
/// pg_depend row by re-scanning its exact key (classid/objid/objsubid +
/// refclassid/refobjid/refobjsubid + deptype) and confirming it still exists.
/// The genam `systable_recheck_tuple` seam operates on a live scan descriptor;
/// since this port materialises the rows up front (the C scan is already
/// closed before recursion), we re-fetch by an exact-match scan instead, which
/// the C recheck logically performs (visibility recheck after lock).
fn recheck_pg_depend(depRel: &RelationData<'_>, row: &DependRow) -> PgResult<bool> {
    let f = &row.form;
    // The depender index (DependDependerIndexId) only covers the
    // classid/objid/objsubid columns, so only those three can be index scan
    // keys; the reference columns and deptype are confirmed by the predicate
    // below (the C `systable_recheck_tuple` likewise rechecks the exact row).
    let keys = [
        oid_key(Anum_pg_depend_classid, f.classid)?,
        oid_key(Anum_pg_depend_objid, f.objid)?,
        int4_key(Anum_pg_depend_objsubid, f.objsubid)?,
    ];
    let found = scan_depend_rows(depRel, DependDependerIndexId, &keys)?;
    Ok(found.iter().any(|r| {
        r.form.refclassid == f.refclassid
            && r.form.refobjid == f.refobjid
            && r.form.refobjsubid == f.refobjsubid
            && r.form.deptype == f.deptype
    }))
}

fn elog_unrecognized_dependency_type(
    deptype: i8,
    object: &ObjectAddress,
) -> PgResult<types_error::PgError> {
    Ok(elog(
        ERROR,
        format!(
            "unrecognized dependency type '{}' for {}",
            deptype as u8 as char,
            getObjectDescription_required(object)?
        ),
    )
    .unwrap_err())
}

/* ===========================================================================
 * reportDependentObjects (dependency.c:979)
 * ========================================================================= */

const MAX_REPORTED_DEPS: i32 = 100;

fn reportDependentObjects(
    targetObjects: &ObjectAddresses,
    behavior: DropBehavior,
    flags: i32,
    origObject: Option<&ObjectAddress>,
) -> PgResult<()> {
    let msglevel = if flags & seams::PERFORM_DELETION_QUIETLY != 0 {
        DEBUG2
    } else {
        NOTICE
    };
    let mut ok = true;
    let mut clientdetail = String::new();
    let mut logdetail = String::new();
    let mut numReportedClient: i32 = 0;
    let mut numNotReportedClient: i32 = 0;

    for i in 0..targetObjects.numrefs {
        let extra = &targetObjects.extras[i as usize];

        if (extra.flags & DEPFLAG_IS_PART) != 0 && (extra.flags & DEPFLAG_PARTITION) == 0 {
            let object = &targetObjects.refs[i as usize];
            let otherObjDesc = getObjectDescription_required(&extra.dependee)?;

            return Err(ereport(ERROR)
                .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                .errmsg(format!(
                    "cannot drop {} because {} requires it",
                    getObjectDescription_required(object)?,
                    otherObjDesc
                ))
                .errhint(format!("You can drop {otherObjDesc} instead."))
                .into_error());
        }
    }

    if behavior == DROP_CASCADE
        && !utils_error::message_level_is_interesting(msglevel)
    {
        return Ok(());
    }

    let mut i = targetObjects.numrefs - 1;
    while i >= 0 {
        let obj = &targetObjects.refs[i as usize];
        let extra = &targetObjects.extras[i as usize];

        if extra.flags & DEPFLAG_ORIGINAL != 0 {
            i -= 1;
            continue;
        }
        if extra.flags & DEPFLAG_SUBOBJECT != 0 {
            i -= 1;
            continue;
        }

        let objDesc = match getObjectDescription(obj, false)? {
            Some(s) => s,
            None => {
                i -= 1;
                continue;
            }
        };

        if extra.flags & (DEPFLAG_AUTO | DEPFLAG_INTERNAL | DEPFLAG_PARTITION | DEPFLAG_EXTENSION)
            != 0
        {
            ereport(DEBUG2)
                .errmsg_internal(format!("drop auto-cascades to {objDesc}"))
                .finish(here("reportDependentObjects"))?;
        } else if behavior == DROP_RESTRICT {
            let otherDesc = getObjectDescription(&extra.dependee, false)?;

            if let Some(otherDesc) = otherDesc {
                if numReportedClient < MAX_REPORTED_DEPS {
                    if !clientdetail.is_empty() {
                        clientdetail.push('\n');
                    }
                    clientdetail.push_str(&format!("{objDesc} depends on {otherDesc}"));
                    numReportedClient += 1;
                } else {
                    numNotReportedClient += 1;
                }
                if !logdetail.is_empty() {
                    logdetail.push('\n');
                }
                logdetail.push_str(&format!("{objDesc} depends on {otherDesc}"));
            } else {
                numNotReportedClient += 1;
            }
            ok = false;
        } else {
            if numReportedClient < MAX_REPORTED_DEPS {
                if !clientdetail.is_empty() {
                    clientdetail.push('\n');
                }
                clientdetail.push_str(&format!("drop cascades to {objDesc}"));
                numReportedClient += 1;
            } else {
                numNotReportedClient += 1;
            }
            if !logdetail.is_empty() {
                logdetail.push('\n');
            }
            logdetail.push_str(&format!("drop cascades to {objDesc}"));
        }

        i -= 1;
    }

    if numNotReportedClient > 0 {
        if numNotReportedClient == 1 {
            clientdetail.push_str(&format!(
                "\nand {numNotReportedClient} other object (see server log for list)"
            ));
        } else {
            clientdetail.push_str(&format!(
                "\nand {numNotReportedClient} other objects (see server log for list)"
            ));
        }
    }

    if !ok {
        if let Some(origObject) = origObject {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                .errmsg(format!(
                    "cannot drop {} because other objects depend on it",
                    getObjectDescription_required(origObject)?
                ))
                .errdetail_internal(clientdetail)
                .errdetail_log(logdetail)
                .errhint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                .errmsg("cannot drop desired object(s) because other objects depend on them")
                .errdetail_internal(clientdetail)
                .errdetail_log(logdetail)
                .errhint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into_error());
        }
    } else if numReportedClient > 1 {
        let n = numReportedClient + numNotReportedClient;
        ereport(msglevel)
            .errmsg_plural(
                format!("drop cascades to {n} other object"),
                format!("drop cascades to {n} other objects"),
                n as u64,
            )
            .errdetail_internal(clientdetail)
            .errdetail_log(logdetail)
            .finish(here("reportDependentObjects"))?;
    } else if numReportedClient == 1 {
        ereport(msglevel)
            .errmsg_internal(clientdetail)
            .finish(here("reportDependentObjects"))?;
    }

    Ok(())
}

/* ===========================================================================
 * DropObjectById (dependency.c:1188)
 * ========================================================================= */

/// Generic drop-by-OID for catalogs needing no special processing
/// (dependency.c:1188). Deletes the single catalog row keyed by `object`'s OID,
/// finding it either through a by-OID syscache or, when the catalog has none,
/// through its OID index via a systable scan.
fn DropObjectById(object: &ObjectAddress) -> PgResult<()> {
    /* cacheId = get_object_catcache_oid(object->classId); */
    let cache_id = objectaddress_seams::get_object_catcache_oid::call(object.classId)?;

    let ctx = MemoryContext::new("DropObjectById");
    /* rel = table_open(object->classId, RowExclusiveLock); */
    let rel = table::table_open(ctx.mcx(), object.classId, RowExclusiveLock)?;

    /*
     * Use the syscache if the class has one, else look it up by the OID index.
     */
    if cache_id >= 0 {
        /*
         * tup = SearchSysCache1(cacheId, ObjectIdGetDatum(object->objectId));
         * if (!HeapTupleIsValid(tup)) elog(ERROR, "cache lookup failed ...");
         * CatalogTupleDelete(rel, &tup->t_self); ReleaseSysCache(tup);
         */
        match syscache_seams::search_syscache1_tid::call(cache_id, object.objectId)? {
            Some(tid) => {
                indexing_seams::catalog_tuple_delete::call(&rel, tid)?;
            }
            None => {
                let descr = objectaddress_seams::get_object_class_descr::call(object.classId)?;
                rel.close(RowExclusiveLock)?;
                return Err(elog(
                    ERROR,
                    format!(
                        "cache lookup failed for {} {}",
                        descr, object.objectId
                    ),
                )
                .unwrap_err());
            }
        }
    } else {
        /*
         * ScanKeyInit(&skey[0], get_object_attnum_oid(classId),
         *             BTEqualStrategyNumber, F_OIDEQ,
         *             ObjectIdGetDatum(object->objectId));
         * scan = systable_beginscan(rel, get_object_oid_index(classId), true,
         *                           NULL, 1, skey);
         * tup = systable_getnext(scan);
         * if (!HeapTupleIsValid(tup)) elog(ERROR, "could not find tuple ...");
         * CatalogTupleDelete(rel, &tup->t_self);
         * systable_endscan(scan);
         */
        let attnum = objectaddress_seams::get_object_attnum_oid::call(object.classId)?;
        let oid_index = objectaddress_seams::get_object_oid_index::call(object.classId)?;
        let keys = [oid_key(attnum, object.objectId)?];

        let mut scan =
            genam_seams::systable_beginscan::call(&rel, oid_index, true, None, &keys)?;
        let scratch = MemoryContext::new("DropObjectById row");
        let tid = match genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? {
            Some(tup) => tup.tuple.t_self,
            None => {
                scan.end()?;
                let descr = objectaddress_seams::get_object_class_descr::call(object.classId)?;
                rel.close(RowExclusiveLock)?;
                return Err(elog(
                    ERROR,
                    format!(
                        "could not find tuple for {} {}",
                        descr, object.objectId
                    ),
                )
                .unwrap_err());
            }
        };
        indexing_seams::catalog_tuple_delete::call(&rel, tid)?;
        scan.end()?;
    }

    /* table_close(rel, RowExclusiveLock); */
    rel.close(RowExclusiveLock)
}

/* ===========================================================================
 * deleteOneObject (dependency.c:1245)
 * ========================================================================= */

fn deleteOneObject(object: &ObjectAddress, flags: i32) -> PgResult<()> {
    objectaccess_seams::InvokeObjectDropHookArg::call(
        object.classId,
        object.objectId,
        object.objectSubId,
        flags,
    )?;

    /*
     * dependency.c closes/reopens depRel for a concurrent drop. The owned
     * Relation guard scoping makes a per-object scratch open the faithful
     * equivalent: each scan below opens pg_depend fresh, so the concurrent
     * close/reopen is implicit (we never hold the relation across doDeletion).
     */
    doDeletion(object, flags)?;

    /* Remove pg_depend records linking from this object to others. */
    let ctx = MemoryContext::new("deleteOneObject");
    let depRel = open_depend(ctx.mcx(), RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_depend_classid, object.classId)?,
        oid_key(Anum_pg_depend_objid, object.objectId)?,
        int4_key(Anum_pg_depend_objsubid, object.objectSubId)?,
    ];
    let nkeys = if object.objectSubId != 0 { 3 } else { 2 };
    let rows = scan_depend_rows(&depRel, DependDependerIndexId, &keys[..nkeys])?;
    for row in rows.iter() {
        indexing_seams::catalog_tuple_delete::call(&depRel, row.tid)?;
    }
    depRel.close(RowExclusiveLock)?;

    shdepend_seams::deleteSharedDependencyRecordsFor::call(
        object.classId,
        object.objectId,
        object.objectSubId,
    )?;

    comment_seams::DeleteComments::call(object.objectId, object.classId, object.objectSubId)?;
    {
        let sctx = MemoryContext::new("DeleteSecurityLabel");
        seclabel_seams::DeleteSecurityLabel::call(sctx.mcx(), object)?;
    }
    DeleteInitPrivs(object)?;

    xact_seams::command_counter_increment::call()?;

    Ok(())
}

/* ===========================================================================
 * doDeletion (dependency.c:1351)
 * ========================================================================= */

fn doDeletion(object: &ObjectAddress, flags: i32) -> PgResult<()> {
    let classId = object.classId;

    if classId == RelationRelationId {
        let relKind = lsyscache_seams::get_rel_relkind::call(object.objectId)?;

        if relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX {
            let concurrent = (flags & seams::PERFORM_DELETION_CONCURRENTLY) != 0;
            let concurrent_lock_mode = (flags & seams::PERFORM_DELETION_CONCURRENT_LOCK) != 0;

            debug_assert!(object.objectSubId == 0);
            index_seams::index_drop::call(object.objectId, concurrent, concurrent_lock_mode)?;
        } else if object.objectSubId != 0 {
            heap_seams::RemoveAttributeById::call(object.objectId, object.objectSubId)?;
        } else {
            heap_seams::heap_drop_with_catalog::call(object.objectId)?;
        }

        if relKind == RELKIND_SEQUENCE {
            sequence_seams::DeleteSequenceTuple::call(object.objectId)?;
        }
    } else if classId == ProcedureRelationId {
        functioncmds_seams::remove_function_tuple::call(object.objectId)?;
    } else if classId == TypeRelationId {
        typecmds_seams::RemoveTypeById::call(object.objectId)?;
    } else if classId == ConstraintRelationId {
        constraint_seams::RemoveConstraintById::call(object.objectId)?;
    } else if classId == AttrDefaultRelationId {
        attrdef_seams::RemoveAttrDefaultById::call(object.objectId)?;
    } else if classId == LargeObjectRelationId {
        largeobject_seams::LargeObjectDrop::call(object.objectId)?;
    } else if classId == OperatorRelationId {
        operator_seams::RemoveOperatorById::call(object.objectId)?;
    } else if classId == RewriteRelationId {
        rewriteRemove_seams::RemoveRewriteRuleById::call(object.objectId)?;
    } else if classId == TriggerRelationId {
        trigger_seams::RemoveTriggerById::call(object.objectId)?;
    } else if classId == StatisticExtRelationId {
        statscmds_seams::RemoveStatisticsById::call(object.objectId)?;
    } else if classId == TSConfigRelationId {
        tsearchcmds_seams::RemoveTSConfigurationById::call(object.objectId)?;
    } else if classId == ExtensionRelationId {
        extension_seams::RemoveExtensionById::call(object.objectId)?;
    } else if classId == PolicyRelationId {
        policy_seams::RemovePolicyById::call(object.objectId)?;
    } else if classId == PublicationNamespaceRelationId {
        publicationcmds_seams::RemovePublicationSchemaById::call(object.objectId)?;
    } else if classId == PublicationRelRelationId {
        publicationcmds_seams::RemovePublicationRelById::call(object.objectId)?;
    } else if classId == PublicationRelationId {
        publicationcmds_seams::RemovePublicationById::call(object.objectId)?;
    } else if classId == CastRelationId
        || classId == CollationRelationId
        || classId == ConversionRelationId
        || classId == LanguageRelationId
        || classId == OperatorClassRelationId
        || classId == OperatorFamilyRelationId
        || classId == AccessMethodRelationId
        || classId == AccessMethodOperatorRelationId
        || classId == AccessMethodProcedureRelationId
        || classId == NamespaceRelationId
        || classId == TSParserRelationId
        || classId == TSDictionaryRelationId
        || classId == TSTemplateRelationId
        || classId == ForeignDataWrapperRelationId
        || classId == ForeignServerRelationId
        || classId == UserMappingRelationId
        || classId == DefaultAclRelationId
        || classId == EventTriggerRelationId
        || classId == TransformRelationId
        || classId == AuthMemRelationId
    {
        DropObjectById(object)?;
    } else if classId == AuthIdRelationId
        || classId == DatabaseRelationId
        || classId == TableSpaceRelationId
        || classId == SubscriptionRelationId
        || classId == ParameterAclRelationId
    {
        return Err(elog(ERROR, "global objects cannot be deleted by doDeletion").unwrap_err());
    } else {
        return Err(elog(ERROR, format!("unsupported object class: {classId}")).unwrap_err());
    }

    Ok(())
}

/* ===========================================================================
 * AcquireDeletionLock / ReleaseDeletionLock (dependency.c:1495)
 * ========================================================================= */

/// AcquireDeletionLock - acquire a suitable lock for deleting an object.
/// The lock is held to end of transaction (C never releases these except via
/// `ReleaseDeletionLock` for objects it decides not to delete) — the lmgr
/// guard's `keep()` mirrors that.
pub fn AcquireDeletionLock(object: &ObjectAddress, flags: i32) -> PgResult<()> {
    if object.classId == RelationRelationId {
        if flags & seams::PERFORM_DELETION_CONCURRENTLY != 0 {
            lmgr_seams::lock_relation_oid::call(object.objectId, ShareUpdateExclusiveLock)?.keep();
        } else {
            lmgr_seams::lock_relation_oid::call(object.objectId, AccessExclusiveLock)?.keep();
        }
    } else if object.classId == AuthMemRelationId {
        lmgr_seams::lock_shared_object::call(object.classId, object.objectId, 0, AccessExclusiveLock)?
            .keep();
    } else {
        lmgr_seams::lock_database_object::call(
            object.classId,
            object.objectId,
            0,
            AccessExclusiveLock,
        )?
        .keep();
    }

    Ok(())
}

/// ReleaseDeletionLock - release an object deletion lock.
pub fn ReleaseDeletionLock(object: &ObjectAddress) -> PgResult<()> {
    if object.classId == RelationRelationId {
        lmgr_seams::unlock_relation_oid::call(object.objectId, AccessExclusiveLock)?;
    } else {
        lmgr_seams::unlock_database_object::call(
            object.classId,
            object.objectId,
            0,
            AccessExclusiveLock,
        )?;
    }

    Ok(())
}

/* ===========================================================================
 * eliminate_duplicate_dependencies (dependency.c:2397)
 * ========================================================================= */

pub(crate) fn eliminate_duplicate_dependencies(addrs: &mut ObjectAddresses) {
    debug_assert!(addrs.extras.is_empty());

    if addrs.numrefs <= 1 {
        return;
    }

    let numrefs = addrs.numrefs as usize;
    addrs.refs[..numrefs].sort_by(object_address_comparator);

    let mut priorobj: usize = 0;
    let mut newrefs: i32 = 1;
    for oldref in 1..numrefs {
        let thisobj = addrs.refs[oldref];
        let prior = &mut addrs.refs[priorobj];

        if prior.classId == thisobj.classId && prior.objectId == thisobj.objectId {
            if prior.objectSubId == thisobj.objectSubId {
                continue;
            }
            if prior.objectSubId == 0 {
                prior.objectSubId = thisobj.objectSubId;
                continue;
            }
        }
        priorobj += 1;
        addrs.refs[priorobj] = thisobj;
        newrefs += 1;
    }

    addrs.refs.truncate(newrefs as usize);
    addrs.numrefs = newrefs;
}

/* ===========================================================================
 * object_address_comparator (dependency.c:2457)
 * ========================================================================= */

pub(crate) fn object_address_comparator(
    obja: &ObjectAddress,
    objb: &ObjectAddress,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    if obja.objectId > objb.objectId {
        return Ordering::Less;
    }
    if obja.objectId < objb.objectId {
        return Ordering::Greater;
    }
    if obja.classId < objb.classId {
        return Ordering::Less;
    }
    if obja.classId > objb.classId {
        return Ordering::Greater;
    }
    if (obja.objectSubId as u32) < (objb.objectSubId as u32) {
        return Ordering::Less;
    }
    if (obja.objectSubId as u32) > (objb.objectSubId as u32) {
        return Ordering::Greater;
    }
    Ordering::Equal
}

/* ===========================================================================
 * new_object_addresses (dependency.c:2501)
 * ========================================================================= */

pub fn new_object_addresses() -> ObjectAddresses {
    ObjectAddresses {
        refs: Vec::new(),
        extras: Vec::new(),
        numrefs: 0,
        maxrefs: 32,
    }
}

/* ===========================================================================
 * add_object_address (dependency.c:2520)
 * ========================================================================= */

pub fn add_object_address(classId: Oid, objectId: Oid, subId: i32, addrs: &mut ObjectAddresses) {
    debug_assert!(addrs.extras.is_empty());
    addrs.refs.push(ObjectAddress {
        classId,
        objectId,
        objectSubId: subId,
    });
    addrs.numrefs += 1;
}

/* ===========================================================================
 * add_exact_object_address (dependency.c:2547)
 * ========================================================================= */

pub fn add_exact_object_address(object: &ObjectAddress, addrs: &mut ObjectAddresses) {
    debug_assert!(addrs.extras.is_empty());
    addrs.refs.push(*object);
    addrs.numrefs += 1;
}

/* ===========================================================================
 * add_exact_object_address_extra (dependency.c:2572)
 * ========================================================================= */

fn add_exact_object_address_extra(
    object: &ObjectAddress,
    extra: &ObjectAddressExtra,
    addrs: &mut ObjectAddresses,
) {
    addrs.refs.push(*object);
    addrs.extras.push(*extra);
    addrs.numrefs += 1;
}

/* ===========================================================================
 * object_address_present (dependency.c:2607)
 * ========================================================================= */

pub fn object_address_present(object: &ObjectAddress, addrs: &ObjectAddresses) -> bool {
    let mut i = addrs.numrefs - 1;
    while i >= 0 {
        let thisobj = &addrs.refs[i as usize];

        if object.classId == thisobj.classId && object.objectId == thisobj.objectId {
            if object.objectSubId == thisobj.objectSubId || thisobj.objectSubId == 0 {
                return true;
            }
        }
        i -= 1;
    }

    false
}

/* ===========================================================================
 * object_address_present_add_flags (dependency.c:2633)
 * ========================================================================= */

fn object_address_present_add_flags(
    object: &ObjectAddress,
    flags: i32,
    addrs: &mut ObjectAddresses,
) -> bool {
    let mut result = false;

    let mut i = addrs.numrefs - 1;
    while i >= 0 {
        let thisobj = addrs.refs[i as usize];

        if object.classId == thisobj.classId && object.objectId == thisobj.objectId {
            if object.objectSubId == thisobj.objectSubId {
                let thisextra = &mut addrs.extras[i as usize];
                thisextra.flags |= flags;
                result = true;
            } else if thisobj.objectSubId == 0 {
                result = true;
            } else if object.objectSubId == 0 {
                let thisextra = &mut addrs.extras[i as usize];
                if flags != 0 {
                    thisextra.flags |= flags | DEPFLAG_SUBOBJECT;
                }
            }
        }
        i -= 1;
    }

    result
}

/* ===========================================================================
 * stack_address_present_add_flags (dependency.c:2706)
 * ========================================================================= */

fn stack_address_present_add_flags(
    object: &ObjectAddress,
    flags: i32,
    stack: Option<&ObjectAddressStack>,
) -> bool {
    let mut result = false;

    let mut stackptr = stack;
    while let Some(s) = stackptr {
        let thisobj = &s.object;

        if object.classId == thisobj.classId && object.objectId == thisobj.objectId {
            if object.objectSubId == thisobj.objectSubId {
                s.flags.set(s.flags.get() | flags);
                result = true;
            } else if thisobj.objectSubId == 0 {
                result = true;
            } else if object.objectSubId == 0 {
                if flags != 0 {
                    s.flags.set(s.flags.get() | flags | DEPFLAG_SUBOBJECT);
                }
            }
        }
        stackptr = s.next;
    }

    result
}

/* ===========================================================================
 * record_object_address_dependencies (dependency.c:2756)
 * ========================================================================= */

pub fn record_object_address_dependencies(
    depender: &ObjectAddress,
    referenced: &mut ObjectAddresses,
    behavior: DependencyType,
) -> PgResult<()> {
    eliminate_duplicate_dependencies(referenced);
    record_multiple_dependencies(depender, &referenced.refs, referenced.numrefs, behavior)
}

/// `recordMultipleDependencies(depender, refs, nrefs, behavior)` — the sibling
/// pg_depend writer.
fn record_multiple_dependencies(
    depender: &ObjectAddress,
    refs: &[ObjectAddress],
    nrefs: i32,
    behavior: DependencyType,
) -> PgResult<()> {
    let n = nrefs.max(0) as usize;
    let ctx = MemoryContext::new("recordMultipleDependencies");
    pg_depend_seams::recordMultipleDependencies::call(ctx.mcx(), depender, &refs[..n], behavior)
}

/* ===========================================================================
 * sort_object_addresses (dependency.c:2775)
 * ========================================================================= */

pub fn sort_object_addresses(addrs: &mut ObjectAddresses) {
    if addrs.numrefs > 1 {
        let n = addrs.numrefs as usize;
        addrs.refs[..n].sort_by(object_address_comparator);
    }
}

/* ===========================================================================
 * recordDependencyOnExpr (dependency.c:1552)
 * ========================================================================= */

/// recordDependencyOnExpr — find and record an expression's object references.
pub fn recordDependencyOnExpr(
    depender: &ObjectAddress,
    expr: &nodes::nodes::Node<'_>,
    rtable: &[nodes::parsenodes::RangeTblEntry<'_>],
    behavior: DependencyType,
) -> PgResult<()> {
    let ctx = MemoryContext::new("recordDependencyOnExpr");
    let mut context = FindExprReferencesContext::new(ctx.mcx());
    let mut owned_rtable: Vec<nodes::parsenodes::RangeTblEntry<'_>> =
        Vec::with_capacity(rtable.len());
    for rte in rtable.iter() {
        owned_rtable.push(rte.clone_in(ctx.mcx())?);
    }
    context.rtables.push(owned_rtable);

    find_expr::find_expr_references_walker(expr, &mut context)?;

    eliminate_duplicate_dependencies(&mut context.addrs);

    record_multiple_dependencies(
        depender,
        &context.addrs.refs,
        context.addrs.numrefs,
        behavior,
    )
}

/* ===========================================================================
 * recordDependencyOnSingleRelExpr (dependency.c:1595)
 * ========================================================================= */

/// recordDependencyOnSingleRelExpr — as above but for one relation (varno=1).
pub fn recordDependencyOnSingleRelExpr(
    depender: &ObjectAddress,
    expr: &nodes::nodes::Node<'_>,
    rel_id: Oid,
    behavior: DependencyType,
    self_behavior: DependencyType,
    reverse_self: bool,
) -> PgResult<()> {
    let ctx = MemoryContext::new("recordDependencyOnSingleRelExpr");
    let mut context = FindExprReferencesContext::new(ctx.mcx());
    context
        .rtables
        .push(find_expr::bogus_single_rel_rtable(ctx.mcx(), rel_id));

    find_expr::find_expr_references_walker(expr, &mut context)?;

    eliminate_duplicate_dependencies(&mut context.addrs);

    if (behavior != self_behavior || reverse_self) && context.addrs.numrefs > 0 {
        let mut self_addrs = new_object_addresses();

        let oldrefs = core::mem::take(&mut context.addrs.refs);
        let mut kept: Vec<ObjectAddress> = Vec::new();
        for thisobj in oldrefs.into_iter() {
            if thisobj.classId == RelationRelationId && thisobj.objectId == rel_id {
                add_exact_object_address(&thisobj, &mut self_addrs);
            } else {
                kept.push(thisobj);
            }
        }
        context.addrs.numrefs = kept.len() as i32;
        context.addrs.refs = kept;

        if !reverse_self {
            record_multiple_dependencies(
                depender,
                &self_addrs.refs,
                self_addrs.numrefs,
                self_behavior,
            )?;
        } else {
            let ctx = MemoryContext::new("recordDependencyOn");
            for selfref in 0..self_addrs.numrefs {
                let thisobj = &self_addrs.refs[selfref as usize];
                pg_depend_seams::recordDependencyOn::call(
                    ctx.mcx(),
                    thisobj,
                    depender,
                    self_behavior,
                )?;
            }
        }
    }

    record_multiple_dependencies(
        depender,
        &context.addrs.refs,
        context.addrs.numrefs,
        behavior,
    )
}

/* ===========================================================================
 * DeleteInitPrivs (dependency.c:2799)
 * ========================================================================= */

/// Delete pg_init_privs rows for an object.
fn DeleteInitPrivs(object: &ObjectAddress) -> PgResult<()> {
    let ctx = MemoryContext::new("DeleteInitPrivs");
    let relPriv = table::table_open(ctx.mcx(), INIT_PRIVS_RELATION_ID, RowExclusiveLock)?;

    let keys = [
        oid_key(Anum_pg_init_privs_objoid, object.objectId)?,
        oid_key(Anum_pg_init_privs_classoid, object.classId)?,
        int4_key(Anum_pg_init_privs_objsubid, object.objectSubId)?,
    ];
    let nkeys = if object.objectSubId != 0 { 3 } else { 2 };

    let mut scan =
        genam_seams::systable_beginscan::call(&relPriv, INIT_PRIVS_OBJ_INDEX_ID, true, None, &keys[..nkeys])?;
    let mut tids: Vec<ItemPointerData> = Vec::new();
    loop {
        let scratch = MemoryContext::new("DeleteInitPrivs row");
        let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? else {
            break;
        };
        tids.push(tup.tuple.t_self);
    }
    scan.end()?;
    for tid in tids {
        indexing_seams::catalog_tuple_delete::call(&relPriv, tid)?;
    }

    relPriv.close(RowExclusiveLock)
}

pub use seams::init_seams;
