// dependency.c deletion half plus recordDependencyOnExpr, bounded to plain
// tables/views and the objects their INTERNAL/AUTO closure reaches (rowtype +
// array type, toast table + toast index, pg_attrdef/pg_constraint entries,
// pg_rewrite rules); the DROP RESTRICT 2BP01 report is live, every other
// object class or report arm is loud with its C symbol.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod description;
mod find_expr;

pub use description::getObjectDescription;
pub use find_expr::{eliminate_duplicate_dependencies, find_expr_references, recordDependencyOnExpr};

use datum::Datum;
use mcx::Mcx;
use pg_depend::{object_address_comparator, ObjectAddress};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID, TYPE_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST};
use types_rel::{AccessExclusiveLock, Relation, RowExclusiveLock, RELKIND_INDEX, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

pub use types_nodes::parsenodes::DropBehavior;

pub const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
pub const PERFORM_DELETION_CONCURRENTLY: i32 = 0x0002;
pub const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
pub const PERFORM_DELETION_SKIP_ORIGINAL: i32 = 0x0008;
pub const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;
pub const PERFORM_DELETION_CONCURRENT_LOCK: i32 = 0x0020;

const DEPFLAG_ORIGINAL: i32 = 0x0001;
const DEPFLAG_NORMAL: i32 = 0x0002;
const DEPFLAG_AUTO: i32 = 0x0004;
const DEPFLAG_INTERNAL: i32 = 0x0008;
const DEPFLAG_PARTITION: i32 = 0x0010;
const DEPFLAG_EXTENSION: i32 = 0x0020;
const DEPFLAG_REVERSE: i32 = 0x0040;
const DEPFLAG_IS_PART: i32 = 0x0080;
const DEPFLAG_SUBOBJECT: i32 = 0x0100;

const Anum_pg_depend_classid: usize = 1;
const Anum_pg_depend_objid: usize = 2;
const Anum_pg_depend_objsubid: usize = 3;
const Anum_pg_depend_refclassid: usize = 4;
const Anum_pg_depend_refobjid: usize = 5;
const Anum_pg_depend_refobjsubid: usize = 6;
const Anum_pg_depend_deptype: usize = 7;

const DescriptionRelationId: Oid = 2609;
const DescriptionObjIndexId: Oid = 2675;
const InitPrivsRelationId: Oid = 3394;
const InitPrivsObjIndexId: Oid = 3395;
const AttrDefaultRelationId: Oid = 2604;
const PolicyRelationId: Oid = 3256;
const DefaultAclRelationId: Oid = 826;
const DefaultAclOidIndexId: Oid = 828;
const RewriteRelationId: Oid = 2618;
const ConstraintRelationId: Oid = 2606;
const AuthMemRelationId: Oid = 1261;
const TriggerRelationId: Oid = 2620;
const EventTriggerRelationId: Oid = 3466;
const EventTriggerOidIndexId: Oid = 3468;
const PublicationRelationId: Oid = 6104;
const PublicationRelRelationId: Oid = 6106;
const PublicationNamespaceRelationId: Oid = 6237;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: dependency.c {what}")
}

#[derive(Clone, Copy)]
struct ObjectAddressExtra {
    flags: i32,
    dependee: ObjectAddress,
}

impl Default for ObjectAddressExtra {
    fn default() -> Self {
        ObjectAddressExtra { flags: 0, dependee: ObjectAddress::set(InvalidOid, InvalidOid) }
    }
}

pub struct ObjectAddresses {
    refs: Vec<ObjectAddress>,
    extras: Vec<ObjectAddressExtra>,
}

impl ObjectAddresses {
    pub fn new() -> Self {
        ObjectAddresses { refs: Vec::new(), extras: Vec::new() }
    }

    pub fn add_exact_object_address(&mut self, obj: ObjectAddress) {
        self.refs.push(obj);
        self.extras.push(ObjectAddressExtra::default());
    }

    fn add_exact_object_address_extra(&mut self, obj: ObjectAddress, extra: ObjectAddressExtra) {
        self.refs.push(obj);
        self.extras.push(extra);
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.refs.len()
    }
}

impl Default for ObjectAddresses {
    fn default() -> Self {
        Self::new()
    }
}

pub fn object_address_present(object: &ObjectAddress, addrs: &ObjectAddresses) -> bool {
    addrs.refs.iter().rev().any(|thisobj| {
        object.classId == thisobj.classId
            && object.objectId == thisobj.objectId
            && (object.objectSubId == thisobj.objectSubId || thisobj.objectSubId == 0)
    })
}

fn object_address_present_add_flags(
    object: &ObjectAddress,
    flags: i32,
    addrs: &mut ObjectAddresses,
) -> bool {
    let mut result = false;
    for i in (0..addrs.refs.len()).rev() {
        let thisobj = addrs.refs[i];
        if object.classId == thisobj.classId && object.objectId == thisobj.objectId {
            if object.objectSubId == thisobj.objectSubId {
                addrs.extras[i].flags |= flags;
                result = true;
            } else if thisobj.objectSubId == 0 {
                result = true;
            } else if object.objectSubId == 0 && flags != 0 {
                addrs.extras[i].flags |= flags | DEPFLAG_SUBOBJECT;
            }
        }
    }
    result
}

struct StackEntry {
    object: ObjectAddress,
    flags: i32,
}

fn stack_address_present_add_flags(
    object: &ObjectAddress,
    flags: i32,
    stack: &mut [StackEntry],
) -> bool {
    let mut result = false;
    for entry in stack.iter_mut() {
        let thisobj = entry.object;
        if object.classId == thisobj.classId && object.objectId == thisobj.objectId {
            if object.objectSubId == thisobj.objectSubId {
                entry.flags |= flags;
                result = true;
            } else if thisobj.objectSubId == 0 {
                result = true;
            } else if object.objectSubId == 0 && flags != 0 {
                entry.flags |= flags | DEPFLAG_SUBOBJECT;
            }
        }
    }
    result
}

pub fn AcquireDeletionLock(object: &ObjectAddress, flags: i32) -> PgResult<()> {
    if object.classId == RELATION_RELATION_ID {
        if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
            lmgr::LockRelationOid(object.objectId, types_rel::ShareUpdateExclusiveLock)
        } else {
            lmgr::LockRelationOid(object.objectId, AccessExclusiveLock)
        }
    } else if object.classId == AuthMemRelationId {
        lmgr::LockSharedObject(object.classId, object.objectId, 0, AccessExclusiveLock)
    } else {
        lmgr::LockDatabaseObject(object.classId, object.objectId, 0, AccessExclusiveLock)
    }
}

pub fn ReleaseDeletionLock(object: &ObjectAddress) -> PgResult<()> {
    if object.classId == RELATION_RELATION_ID {
        lmgr::UnlockRelationOid(object.objectId, AccessExclusiveLock)
    } else {
        lmgr::UnlockDatabaseObject(object.classId, object.objectId, 0, AccessExclusiveLock)
    }
}

fn scankey(attno: usize, func: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

pub(crate) fn oid_key(attno: usize, oid: Oid) -> ScanKeyData {
    scankey(attno, types_core::fmgr::F_OIDEQ, Datum::from_oid(oid))
}

fn int4_key(attno: usize, v: i32) -> ScanKeyData {
    scankey(attno, types_core::fmgr::F_INT4EQ, Datum::from_i32(v))
}

fn getattr(tup: &HeapTupleData<'_>, attnum: usize, desc: &TupleDescData<'_>) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed NOT NULL catalog column under the relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

pub fn performDeletion<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let mut depRel = Some(table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?);
    AcquireDeletionLock(object, 0)?;
    let mut targetObjects = ObjectAddresses::new();
    let mut stack: Vec<StackEntry> = Vec::new();
    findDependentObjects(
        mcx,
        object,
        DEPFLAG_ORIGINAL,
        flags,
        &mut stack,
        &mut targetObjects,
        None,
        depRel.as_ref().expect("pg_depend open"),
    )?;
    reportDependentObjects(mcx, &targetObjects, behavior, flags, Some(object))?;
    deleteObjectsInList(mcx, &targetObjects, &mut depRel, flags)?;
    depRel.take().expect("pg_depend open").close(RowExclusiveLock)
}

pub fn performMultipleDeletions<'mcx>(
    mcx: Mcx<'mcx>,
    objects: &ObjectAddresses,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    if objects.is_empty() {
        return Ok(());
    }
    let mut depRel = Some(table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?);
    let mut targetObjects = ObjectAddresses::new();
    for thisobj in objects.refs.iter() {
        AcquireDeletionLock(thisobj, flags)?;
        let mut stack: Vec<StackEntry> = Vec::new();
        findDependentObjects(
            mcx,
            thisobj,
            DEPFLAG_ORIGINAL,
            flags,
            &mut stack,
            &mut targetObjects,
            Some(objects),
            depRel.as_ref().expect("pg_depend open"),
        )?;
    }
    let origObject = if objects.refs.len() == 1 { Some(&objects.refs[0]) } else { None };
    reportDependentObjects(mcx, &targetObjects, behavior, flags, origObject)?;
    deleteObjectsInList(mcx, &targetObjects, &mut depRel, flags)?;
    depRel.take().expect("pg_depend open").close(RowExclusiveLock)
}

#[allow(clippy::too_many_arguments)]
fn findDependentObjects<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    objflags: i32,
    flags: i32,
    stack: &mut Vec<StackEntry>,
    targetObjects: &mut ObjectAddresses,
    pendingObjects: Option<&ObjectAddresses>,
    depRel: &Relation<'mcx>,
) -> PgResult<()> {
    let mut objflags = objflags;

    if stack_address_present_add_flags(object, objflags, stack) {
        return Ok(());
    }
    if object_address_present_add_flags(object, objflags, targetObjects) {
        return Ok(());
    }
    if catalog::IsPinnedObject(object.classId, object.objectId) {
        let desc = getObjectDescription(mcx, object)?.expect("pinned objects are describable");
        return Err(Box::new(
            PgError::error(format!(
                "cannot drop {desc} because it is required by the database system"
            ))
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
        ));
    }

    // Scan what this object depends on (owner detection).
    let mut owningObject = ObjectAddress::set(InvalidOid, InvalidOid);
    let mut partitionObject = ObjectAddress::set(InvalidOid, InvalidOid);
    {
        let mut keys: Vec<ScanKeyData> = vec![
            oid_key(Anum_pg_depend_classid, object.classId),
            oid_key(Anum_pg_depend_objid, object.objectId),
        ];
        if object.objectSubId != 0 {
            keys.push(int4_key(Anum_pg_depend_objsubid, object.objectSubId));
        }
        let mut scan = genam::systable_beginscan(
            mcx,
            depRel,
            pg_depend::DependDependerIndexId,
            true,
            None,
            &keys,
        )?;
        let desc = depRel.descr();
        loop {
            let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
            // SAFETY: aliases the slot-held image for the recheck call below.
            let view = unsafe {
                HeapTupleData::from_raw_parts(tup.header_ptr(), tup.t_len, tup.t_self, tup.t_tableOid)
            };
            let tup = &view;
            let otherObject = ObjectAddress::sub_set(
                getattr(tup, Anum_pg_depend_refclassid, desc).as_oid(),
                getattr(tup, Anum_pg_depend_refobjid, desc).as_oid(),
                getattr(tup, Anum_pg_depend_refobjsubid, desc).as_i32(),
            );
            let deptype = getattr(tup, Anum_pg_depend_deptype, desc).as_i8() as u8;

            if otherObject.classId == object.classId
                && otherObject.objectId == object.objectId
                && object.objectSubId == 0
            {
                continue;
            }

            match deptype {
                b'n' | b'a' | b'x' => {}
                b'e' | b'i' => {
                    if deptype == b'e' && flags & PERFORM_DELETION_SKIP_EXTENSIONS != 0 {
                        continue;
                    }
                    // Scripts of the extension being created/altered may drop
                    // its own member objects.
                    if deptype == b'e'
                        && pg_depend::creating_extension()
                        && otherObject.classId == types_core::EXTENSION_RELATION_ID
                        && otherObject.objectId == pg_depend::CurrentExtensionObject()
                    {
                        continue;
                    }
                    if stack.is_empty() {
                        if let Some(pending) = pendingObjects {
                            if object_address_present(&otherObject, pending) {
                                genam::systable_endscan(mcx, scan)?;
                                ReleaseDeletionLock(object)?;
                                return Ok(());
                            }
                        }
                        if owningObject.classId == InvalidOid || deptype == b'e' {
                            owningObject = otherObject;
                        }
                        continue;
                    }
                    if stack_address_present_add_flags(&otherObject, 0, stack) {
                        continue;
                    }
                    // Recurse to the owning object instead.
                    ReleaseDeletionLock(object)?;
                    AcquireDeletionLock(&otherObject, 0)?;
                    if !genam::systable_recheck_tuple(mcx, &mut scan, tup)? {
                        genam::systable_endscan(mcx, scan)?;
                        ReleaseDeletionLock(&otherObject)?;
                        return Ok(());
                    }
                    genam::systable_endscan(mcx, scan)?;
                    findDependentObjects(
                        mcx,
                        &otherObject,
                        DEPFLAG_REVERSE,
                        flags,
                        stack,
                        targetObjects,
                        pendingObjects,
                        depRel,
                    )?;
                    if !object_address_present_add_flags(object, objflags, targetObjects) {
                        panic!(
                            "deletion of owning object {:?} failed to delete {:?}",
                            otherObject, object
                        );
                    }
                    return Ok(());
                }
                // After the scan we complain unless some partition dependency
                // of this object is also being deleted.
                b'P' => {
                    objflags |= DEPFLAG_IS_PART;
                    partitionObject = otherObject;
                }
                b'S' => {
                    if objflags & DEPFLAG_IS_PART == 0 {
                        partitionObject = otherObject;
                    }
                    objflags |= DEPFLAG_IS_PART;
                }
                other => panic!(
                    "unrecognized dependency type '{}' for {:?}",
                    other as char, object
                ),
            }
        }
        genam::systable_endscan(mcx, scan)?;
    }

    if owningObject.classId != InvalidOid {
        // A found PARTITION dependency is preferred in the report.
        let other = if partitionObject.classId != InvalidOid {
            &partitionObject
        } else {
            &owningObject
        };
        let otherObjDesc = getObjectDescription(mcx, other)?
            .expect("owning object was just read from pg_depend");
        let objDesc = getObjectDescription(mcx, object)?
            .expect("drop target exists");
        return Err(cannot_drop_required(&objDesc, &otherObjDesc));
    }

    // Scan what depends on this object.
    let mut dependentObjects: Vec<(ObjectAddress, i32)> = Vec::new();
    {
        let mut keys: Vec<ScanKeyData> = vec![
            oid_key(Anum_pg_depend_refclassid, object.classId),
            oid_key(Anum_pg_depend_refobjid, object.objectId),
        ];
        if object.objectSubId != 0 {
            keys.push(int4_key(Anum_pg_depend_refobjsubid, object.objectSubId));
        }
        let mut scan = genam::systable_beginscan(
            mcx,
            depRel,
            pg_depend::DependReferenceIndexId,
            true,
            None,
            &keys,
        )?;
        let desc = depRel.descr();
        loop {
            let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
            // SAFETY: aliases the slot-held image for the recheck call below.
            let view = unsafe {
                HeapTupleData::from_raw_parts(tup.header_ptr(), tup.t_len, tup.t_self, tup.t_tableOid)
            };
            let tup = &view;
            let otherObject = ObjectAddress::sub_set(
                getattr(tup, Anum_pg_depend_classid, desc).as_oid(),
                getattr(tup, Anum_pg_depend_objid, desc).as_oid(),
                getattr(tup, Anum_pg_depend_objsubid, desc).as_i32(),
            );
            let deptype = getattr(tup, Anum_pg_depend_deptype, desc).as_i8() as u8;

            if otherObject.classId == object.classId
                && otherObject.objectId == object.objectId
                && object.objectSubId == 0
            {
                continue;
            }

            AcquireDeletionLock(&otherObject, 0)?;
            if !genam::systable_recheck_tuple(mcx, &mut scan, tup)? {
                ReleaseDeletionLock(&otherObject)?;
                continue;
            }

            let subflags = match deptype {
                b'n' => DEPFLAG_NORMAL,
                b'a' | b'x' => DEPFLAG_AUTO,
                b'i' => DEPFLAG_INTERNAL,
                b'P' | b'S' => DEPFLAG_PARTITION,
                b'e' => DEPFLAG_EXTENSION,
                other => panic!(
                    "unrecognized dependency type '{}' for {:?}",
                    other as char, object
                ),
            };
            dependentObjects.push((otherObject, subflags));
        }
        genam::systable_endscan(mcx, scan)?;
    }

    dependentObjects.sort_by(|a, b| object_address_comparator(&a.0, &b.0));

    stack.push(StackEntry { object: *object, flags: objflags });
    for (depObj, subflags) in dependentObjects.iter() {
        findDependentObjects(
            mcx,
            depObj,
            *subflags,
            flags,
            stack,
            targetObjects,
            pendingObjects,
            depRel,
        )?;
    }
    let top = stack.pop().expect("stack imbalance");
    objflags = top.flags;

    let extra = ObjectAddressExtra {
        flags: objflags,
        dependee: if objflags & DEPFLAG_IS_PART != 0 {
            partitionObject
        } else if let Some(prev) = stack.last() {
            prev.object
        } else {
            ObjectAddress::set(InvalidOid, InvalidOid)
        },
    };
    targetObjects.add_exact_object_address_extra(*object, extra);
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_drop_required(obj_desc: &str, other_desc: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot drop {obj_desc} because {other_desc} requires it"))
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .with_hint(format!("You can drop {other_desc} instead.")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn dependent_objects_exist(
    orig_desc: Option<String>,
    clientdetail: String,
    logdetail: String,
) -> Box<PgError> {
    let msg = match orig_desc {
        Some(desc) => format!("cannot drop {desc} because other objects depend on it"),
        None => "cannot drop desired object(s) because other objects depend on them".into(),
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .with_detail(clientdetail)
            .with_detail_log(logdetail)
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
    )
}

const MAX_REPORTED_DEPS: i32 = 100;

// The auto/internal cascade arm stays a silent DEBUG2 no-op.
fn reportDependentObjects<'mcx>(
    mcx: Mcx<'mcx>,
    targetObjects: &ObjectAddresses,
    behavior: DropBehavior,
    flags: i32,
    origObject: Option<&ObjectAddress>,
) -> PgResult<()> {
    // A partition-dependent object may be deleted only alongside one of its
    // partition dependencies (i.e. it was reached via a PARTITION dep).
    for i in 0..targetObjects.refs.len() {
        let extra = &targetObjects.extras[i];
        if extra.flags & DEPFLAG_IS_PART != 0 && extra.flags & DEPFLAG_PARTITION == 0 {
            let otherDesc = getObjectDescription(mcx, &extra.dependee)?
                .expect("partition dependee was just read from pg_depend");
            let objDesc = getObjectDescription(mcx, &targetObjects.refs[i])?
                .expect("drop target exists");
            return Err(cannot_drop_required(&objDesc, &otherDesc));
        }
    }

    let mut clientdetail = String::new();
    let mut logdetail = String::new();
    let mut numReportedClient: i32 = 0;
    let mut numNotReportedClient: i32 = 0;
    let mut ok = true;

    // Back to front: dependency order, not deletion order.
    for i in (0..targetObjects.refs.len()).rev() {
        let obj = &targetObjects.refs[i];
        let extra = &targetObjects.extras[i];
        if extra.flags & DEPFLAG_ORIGINAL != 0 {
            continue;
        }
        if extra.flags & DEPFLAG_SUBOBJECT != 0 {
            continue;
        }
        if extra.flags & (DEPFLAG_AUTO | DEPFLAG_INTERNAL | DEPFLAG_PARTITION | DEPFLAG_EXTENSION)
            != 0
        {
            // drop auto-cascades: DEBUG2, not client-visible. C builds the
            // object description before this arm for that log line; deferred
            // into the reporting arms so unported-class descriptions (e.g. a
            // table's own pg_type rowtype) stay unreached.
        } else if behavior == DropBehavior::DROP_RESTRICT {
            let Some(objDesc) = getObjectDescription(mcx, obj)? else { continue };
            if let Some(otherDesc) = getObjectDescription(mcx, &extra.dependee)? {
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
        } else if flags & PERFORM_DELETION_QUIETLY != 0 {
            // QUIETLY drops msglevel to DEBUG2: nothing client-visible.
        } else {
            let Some(objDesc) = getObjectDescription(mcx, obj)? else { continue };
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
    }

    if numNotReportedClient > 0 {
        let noun = if numNotReportedClient == 1 { "object" } else { "objects" };
        clientdetail.push_str(&format!(
            "\nand {numNotReportedClient} other {noun} (see server log for list)"
        ));
    }

    if !ok {
        let orig_desc = match origObject {
            Some(orig) => getObjectDescription(mcx, orig)?,
            None => None,
        };
        return Err(dependent_objects_exist(orig_desc, clientdetail, logdetail));
    }

    if numReportedClient > 1 {
        let total = numReportedClient + numNotReportedClient;
        let noun = if total == 1 { "object" } else { "objects" };
        elog_seams::ereport_msg::call(
            types_error::NOTICE,
            format!("drop cascades to {total} other {noun}"),
            Some(clientdetail),
        )?;
    } else if numReportedClient == 1 {
        elog_seams::ereport_msg::call(types_error::NOTICE, clientdetail, None)?;
    }
    Ok(())
}

fn deleteObjectsInList<'mcx>(
    mcx: Mcx<'mcx>,
    targetObjects: &ObjectAddresses,
    depRel: &mut Option<Relation<'mcx>>,
    flags: i32,
) -> PgResult<()> {
    if event_trigger_seams::track_dropped_objects_needed::call(mcx)?
        && flags & PERFORM_DELETION_INTERNAL == 0
    {
        for i in 0..targetObjects.refs.len() {
            let thisobj = &targetObjects.refs[i];
            let extra = &targetObjects.extras[i];
            let original = extra.flags & DEPFLAG_ORIGINAL != 0;
            let normal =
                extra.flags & DEPFLAG_NORMAL != 0 || extra.flags & DEPFLAG_REVERSE != 0;
            if event_trigger_seams::event_trigger_supports_object::call(thisobj) {
                event_trigger_seams::event_trigger_sql_drop_add_object::call(
                    mcx, thisobj, original, normal,
                )?;
            }
        }
    }

    for i in 0..targetObjects.refs.len() {
        let thisobj = &targetObjects.refs[i];
        let thisextra = &targetObjects.extras[i];
        if flags & PERFORM_DELETION_SKIP_ORIGINAL != 0 && thisextra.flags & DEPFLAG_ORIGINAL != 0 {
            continue;
        }
        deleteOneObject(mcx, thisobj, depRel, flags)?;
    }
    Ok(())
}

fn deleteOneObject<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    depRel: &mut Option<Relation<'mcx>>,
    flags: i32,
) -> PgResult<()> {
    // doDeletion commits the transaction in the concurrent case; pg_depend
    // cannot stay open across it.
    if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
        depRel.take().expect("pg_depend open").close(RowExclusiveLock)?;
    }

    doDeletion(mcx, object, flags)?;

    if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
        *depRel = Some(table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?);
    }
    let depRel = depRel.as_ref().expect("pg_depend open");

    let mut keys: Vec<ScanKeyData> = vec![
        oid_key(Anum_pg_depend_classid, object.classId),
        oid_key(Anum_pg_depend_objid, object.objectId),
    ];
    if object.objectSubId != 0 {
        keys.push(int4_key(Anum_pg_depend_objsubid, object.objectSubId));
    }
    let mut scan = genam::systable_beginscan(
        mcx,
        depRel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(depRel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    pg_shdepend::deleteSharedDependencyRecordsFor(
        mcx,
        object.classId,
        object.objectId,
        object.objectSubId,
    )?;

    DeleteComments(mcx, object.objectId, object.classId, object.objectSubId)?;
    seclabel::DeleteSecurityLabel(mcx, object)?;
    DeleteInitPrivs(mcx, object)?;

    xact::CommandCounterIncrement()?;
    Ok(())
}

fn doDeletion<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress, flags: i32) -> PgResult<()> {
    match object.classId {
        RELATION_RELATION_ID => {
            let relKind = lsyscache::get_rel_relkind(object.objectId)? as u8;
            if relKind == RELKIND_INDEX || relKind == types_rel::RELKIND_PARTITIONED_INDEX {
                debug_assert!(object.objectSubId == 0);
                catalog_index::index_drop(
                    mcx,
                    object.objectId,
                    flags & PERFORM_DELETION_CONCURRENTLY != 0,
                    flags & PERFORM_DELETION_CONCURRENT_LOCK != 0,
                )?;
            } else if object.objectSubId != 0 {
                catalog_heap::RemoveAttributeById(
                    mcx,
                    object.objectId,
                    object.objectSubId as types_core::AttrNumber,
                )?;
            } else if matches!(
                relKind,
                RELKIND_RELATION
                    | RELKIND_TOASTVALUE
                    | RELKIND_SEQUENCE
                    | RELKIND_VIEW
                    | types_rel::RELKIND_MATVIEW
                    | types_rel::RELKIND_PARTITIONED_TABLE
                    | types_rel::RELKIND_COMPOSITE_TYPE
                    | types_rel::RELKIND_FOREIGN_TABLE
            ) {
                catalog_heap::heap_drop_with_catalog(mcx, object.objectId)?;
                if relKind == RELKIND_SEQUENCE {
                    sequence_seams::delete_sequence_tuple::call(object.objectId)?;
                }
            } else {
                unported("doDeletion: non-table relkind (foreign-table/composite lanes)");
            }
        }
        TYPE_RELATION_ID => pg_type::RemoveTypeById(mcx, object.objectId)?,
        PolicyRelationId => policy_seams::remove_policy_by_id::call(mcx, object.objectId)?,
        PublicationRelationId => {
            publicationcmds_seams::remove_publication_by_id::call(mcx, object.objectId)?
        }
        PublicationRelRelationId => {
            publicationcmds_seams::remove_publication_rel_by_id::call(mcx, object.objectId)?
        }
        PublicationNamespaceRelationId => {
            publicationcmds_seams::remove_publication_schema_by_id::call(mcx, object.objectId)?
        }
        pg_largeobject::LargeObjectRelationId => {
            pg_largeobject::LargeObjectDrop(mcx, object.objectId)?
        }
        types_core::PROCEDURE_RELATION_ID => {
            functioncmds::RemoveFunctionById(mcx, object.objectId)?
        }
        types_core::EXTENSION_RELATION_ID => {
            extension::RemoveExtensionById(mcx, object.objectId)?
        }
        AttrDefaultRelationId => pg_attrdef::RemoveAttrDefaultById(mcx, object.objectId)?,
        ConstraintRelationId => pg_constraint::RemoveConstraintById(mcx, object.objectId)?,
        TriggerRelationId => trigger::RemoveTriggerById(mcx, object.objectId)?,
        statscmds::StatisticExtRelationId => statscmds::RemoveStatisticsById(mcx, object.objectId)?,
        types_core::NAMESPACE_RELATION_ID => {
            pg_namespace::RemoveSchemaById(mcx, object.objectId)?
        }
        RewriteRelationId => {
            rewrite_define_seams::remove_rewrite_rule_by_id::call(mcx, object.objectId)?
        }
        types_core::OPERATOR_RELATION_ID => {
            dependency_seams::remove_operator_by_id::call(mcx, object.objectId)?
        }
        types_core::OPERATOR_CLASS_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::OPERATOR_CLASS_RELATION_ID,
            types_core::OPCLASS_OID_INDEX_ID,
            object.objectId,
        )?,
        types_core::OPERATOR_FAMILY_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::OPERATOR_FAMILY_RELATION_ID,
            types_core::OPFAMILY_OID_INDEX_ID,
            object.objectId,
        )?,
        types_core::ACCESS_METHOD_OPERATOR_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::ACCESS_METHOD_OPERATOR_RELATION_ID,
            types_core::ACCESS_METHOD_OPERATOR_OID_INDEX_ID,
            object.objectId,
        )?,
        types_core::ACCESS_METHOD_PROCEDURE_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::ACCESS_METHOD_PROCEDURE_RELATION_ID,
            types_core::ACCESS_METHOD_PROCEDURE_OID_INDEX_ID,
            object.objectId,
        )?,
        // C routes pg_ts_dict through generic DropObjectById and pg_ts_config
        // through RemoveTSConfigurationById (tsearchcmds.c) — hosted here
        // because dependency deletion cannot call back into tsearchcmds.
        TSDictionaryRelationId => {
            drop_row_by_oid(mcx, TSDictionaryRelationId, TSDictionaryOidIndexId, object.objectId)?
        }
        // RemoveCollationById (pg_collation.c): plain row delete.
        CollationRelationId_dep => {
            drop_row_by_oid(mcx, CollationRelationId_dep, CollationOidIndexId_dep, object.objectId)?
        }
        TSConfigRelationId => {
            drop_row_by_oid(mcx, TSConfigRelationId, TSConfigOidIndexId, object.objectId)?;
            let rel = table::table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;
            let keys = [oid_key(1, object.objectId)];
            let mut scan =
                genam::systable_beginscan(mcx, &rel, TSConfigMapIndexId, true, None, &keys)?;
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                let tid = tup.t_self;
                catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
            }
            genam::systable_endscan(mcx, scan)?;
            rel.close(RowExclusiveLock)?;
        }
        DefaultAclRelationId => {
            drop_row_by_oid(mcx, DefaultAclRelationId, DefaultAclOidIndexId, object.objectId)?
        }
        AccessMethodRelationId => {
            drop_row_by_oid(mcx, AccessMethodRelationId, AmOidIndexId, object.objectId)?
        }
        CastRelationId => drop_row_by_oid(mcx, CastRelationId, CastOidIndexId, object.objectId)?,
        ConversionRelationId => {
            drop_row_by_oid(mcx, ConversionRelationId, ConversionOidIndexId, object.objectId)?
        }
        LanguageRelationId => {
            drop_row_by_oid(mcx, LanguageRelationId, LanguageOidIndexId, object.objectId)?
        }
        TransformRelationId => {
            drop_row_by_oid(mcx, TransformRelationId, TransformOidIndexId, object.objectId)?
        }
        TSParserRelationId => {
            drop_row_by_oid(mcx, TSParserRelationId, TSParserOidIndexId, object.objectId)?
        }
        TSTemplateRelationId => {
            drop_row_by_oid(mcx, TSTemplateRelationId, TSTemplateOidIndexId, object.objectId)?
        }
        AuthMemRelationId => {
            drop_row_by_oid(mcx, AuthMemRelationId, AuthMemOidIndexId, object.objectId)?
        }
        // DropObjectById (objectaddress.c) takes the EVENTTRIGGEROID catcache
        // branch; the unique-index scan reaches the same tuple.
        EventTriggerRelationId => {
            let rel = table::table_open(mcx, EventTriggerRelationId, RowExclusiveLock)?;
            let keys = [oid_key(1, object.objectId)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &rel,
                EventTriggerOidIndexId,
                true,
                None,
                &keys,
            )?;
            let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
                panic!("cache lookup failed for event trigger {}", object.objectId)
            });
            let tid = tup.t_self;
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
            genam::systable_endscan(mcx, scan)?;
            rel.close(RowExclusiveLock)?;
        }
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
            types_core::FOREIGN_DATA_WRAPPER_OID_INDEX_ID,
            object.objectId,
        )?,
        types_core::FOREIGN_SERVER_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::FOREIGN_SERVER_RELATION_ID,
            types_core::FOREIGN_SERVER_OID_INDEX_ID,
            object.objectId,
        )?,
        types_core::USER_MAPPING_RELATION_ID => drop_row_by_oid(
            mcx,
            types_core::USER_MAPPING_RELATION_ID,
            types_core::USER_MAPPING_OID_INDEX_ID,
            object.objectId,
        )?,
        other => panic!("unported: doDeletion object class {other}"),
    }
    Ok(())
}

const AccessMethodRelationId: Oid = 2601;
const AmOidIndexId: Oid = 2652;
const CastRelationId: Oid = 2605;
const CastOidIndexId: Oid = 2660;
const ConversionRelationId: Oid = 2607;
const ConversionOidIndexId: Oid = 2670;
const LanguageRelationId: Oid = 2612;
const LanguageOidIndexId: Oid = 2682;
const TransformRelationId: Oid = 3576;
const TransformOidIndexId: Oid = 3574;
const TSDictionaryRelationId: Oid = 3600;
const TSParserRelationId: Oid = 3601;
const TSParserOidIndexId: Oid = 3607;
const TSTemplateRelationId: Oid = 3764;
const TSTemplateOidIndexId: Oid = 3767;
const AuthMemOidIndexId: Oid = 6303;
const CollationRelationId_dep: Oid = 3456;
const CollationOidIndexId_dep: Oid = 3085;
const TSDictionaryOidIndexId: Oid = 3605;
const TSConfigRelationId: Oid = 3602;
const TSConfigOidIndexId: Oid = 3712;
const TSConfigMapRelationId: Oid = 3603;
const TSConfigMapIndexId: Oid = 3609;

// deleteDependencyRecordsFor (pg_depend.c).
pub fn deleteDependencyRecordsFor<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    skipExtensionDeps: bool,
) -> PgResult<i64> {
    let mut count: i64 = 0;
    let depRel = table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_depend_classid, classId),
        oid_key(Anum_pg_depend_objid, objectId),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &depRel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = depRel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        // SAFETY: aliases the slot-held image for the deptype read below.
        let view = unsafe {
            HeapTupleData::from_raw_parts(tup.header_ptr(), tup.t_len, tup.t_self, tup.t_tableOid)
        };
        if skipExtensionDeps
            && getattr(&view, Anum_pg_depend_deptype, desc).as_i8() as u8 == b'e'
        {
            continue;
        }
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&depRel, &tid)?;
        count += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    depRel.close(RowExclusiveLock)?;
    Ok(count)
}

// DeleteComments (comment.c).
fn DeleteComments<'mcx>(mcx: Mcx<'mcx>, oid: Oid, classoid: Oid, subid: i32) -> PgResult<()> {
    let rel = table::table_open(mcx, DescriptionRelationId, RowExclusiveLock)?;
    let mut keys: Vec<ScanKeyData> = vec![oid_key(1, oid), oid_key(2, classoid)];
    if subid != 0 {
        keys.push(int4_key(3, subid));
    }
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DescriptionObjIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// DeleteInitPrivs (aclchk.c).
fn DeleteInitPrivs<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress) -> PgResult<()> {
    let rel = table::table_open(mcx, InitPrivsRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(1, object.objectId),
        oid_key(2, object.classId),
        int4_key(3, object.objectSubId),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InitPrivsObjIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

fn seam_perform_deletion(
    mcx: Mcx<'_>,
    class_id: Oid,
    object_id: Oid,
    object_sub_id: i32,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let object = ObjectAddress::sub_set(class_id, object_id, object_sub_id);
    performDeletion(mcx, &object, behavior, flags)
}

fn seam_acquire_deletion_lock(class_id: Oid, object_id: Oid, flags: i32) -> PgResult<()> {
    AcquireDeletionLock(&ObjectAddress::set(class_id, object_id), flags)
}

fn seam_release_deletion_lock(class_id: Oid, object_id: Oid) -> PgResult<()> {
    ReleaseDeletionLock(&ObjectAddress::set(class_id, object_id))
}

// sort_object_addresses + performMultipleDeletions tail of shdepDropOwned.
fn seam_perform_multiple_deletions(
    mcx: Mcx<'_>,
    objects: &[(Oid, Oid, i32)],
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let mut addrs = ObjectAddresses::new();
    for &(class_id, object_id, object_sub_id) in objects {
        addrs.add_exact_object_address(ObjectAddress::sub_set(
            class_id,
            object_id,
            object_sub_id,
        ));
    }
    addrs.refs.sort_by(object_address_comparator);
    performMultipleDeletions(mcx, &addrs, behavior, flags)
}

pub fn init_seams() {
    dependency_seams::perform_deletion::set(seam_perform_deletion);
    dependency_seams::record_dependency_on_expr::set(recordDependencyOnExpr);
    pg_shdepend::acquire_deletion_lock::set(seam_acquire_deletion_lock);
    pg_shdepend::release_deletion_lock::set(seam_release_deletion_lock);
    pg_shdepend::perform_multiple_deletions::set(seam_perform_multiple_deletions);
    pg_shdepend::command_counter_increment::set(xact::CommandCounterIncrement);
}

// DropObjectById (dependency.c) reduced to the oid-indexed catalogs above:
// delete the catalog row addressed by its oid.
fn drop_row_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    oid_index_id: Oid,
    oid: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relation_id, RowExclusiveLock)?;
    let keys = [oid_key(1, oid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, oid_index_id, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        panic!("could not find tuple for object {oid} in catalog {relation_id}");
    };
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}
